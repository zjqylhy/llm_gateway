package gateway

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// 显式使用隔离的真实 Grafana/Prometheus，默认单元测试不依赖 Docker。
func TestGrafanaRealIntegration(t *testing.T) {
	base := os.Getenv("GW_TEST_GRAFANA_URL")
	if base == "" {
		t.Skip("set GW_TEST_GRAFANA_URL for real Grafana integration")
	}
	e := testEngine(t, MockProvider)
	_, err := e.Store.Update(context.Background(), "test", "cost-user", func(cfg *Config) error {
		h, _ := Password("test-password-123")
		cfg.Users["cost"] = User{"cost", h, "cost"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	a := NewAdmin(e, false, "")
	a.Assets = http.FileServer(http.Dir("../../web/dist"))
	a.Grafana, err = newGrafanaProxy(base, "console-viewer", os.Getenv("GW_TEST_GRAFANA_PASSWORD"))
	if err != nil {
		t.Fatal(err)
	}
	handler := a.Handler()
	server := httptest.NewUnstartedServer(handler)
	server.Listener, err = net.Listen("tcp", "0.0.0.0:18081")
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	defer server.Close()
	e.Metrics.APICalls.WithLabelValues("app", "/v1/chat/completions", "false", "success", "200", "").Add(123)
	e.Metrics.APICalls.WithLabelValues("other", "/v1/chat/completions", "false", "success", "200", "").Add(999)
	e.Metrics.APICalls.WithLabelValues("app", "/v1/chat/completions", "false", "error", "429", "app_qps_exceeded").Add(7)
	for _, id := range []string{"app", "other"} {
		e.Metrics.APILatency.WithLabelValues(id, "/v1/chat/completions", "false").Observe(.03)
		e.Metrics.Tokens.WithLabelValues(id, "primary", "input", "reported").Add(100)
		e.Metrics.Tokens.WithLabelValues(id, "primary", "output", "reported").Add(20)
	}
	// 等待真实 collector 至少抓取两次，保证 rate 和 counter 查询均有样本。
	time.Sleep(11 * time.Second)
	for _, role := range []string{"dev", "cost", "admin"} {
		cookie, csrf := adminLogin(t, a, role)
		dashboard := applicationDashboard
		if role == "admin" {
			dashboard = "llm-gateway"
		}
		if w := adminCall(a, cookie, csrf, "GET", grafanaPrefix+"api/dashboards/uid/"+dashboard, ""); w.Code != 200 {
			t.Fatal(role, w.Code, w.Body.String())
		}
		if w := adminCall(a, cookie, csrf, "GET", grafanaPrefix+"d/"+dashboard+"/monitoring", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "grafana") {
			t.Fatal(role, "HTML", w.Code, w.Body.String())
		}
		expression := `sum(gateway_api_calls_total{outcome="success"})`
		w := adminCall(a, cookie, csrf, "POST", grafanaPrefix+"api/ds/query", testGrafanaBody(expression, "prometheus"))
		if w.Code != 200 {
			t.Fatal(role, w.Code, w.Body.String())
		}
		var decoded struct {
			Results map[string]struct {
				Error  string
				Frames []struct{ Data struct{ Values [][]any } }
			}
		}
		if json.Unmarshal(w.Body.Bytes(), &decoded) != nil || len(decoded.Results["A"].Frames) == 0 || decoded.Results["A"].Error != "" {
			t.Fatal(role, w.Body.String())
		}
		value := decoded.Results["A"].Frames[0].Data.Values[1]
		expected := float64(1122)
		if role == "dev" {
			expected = 123
		}
		if len(value) == 0 || value[len(value)-1] != expected {
			t.Fatal(role, "cross application result", value, expected)
		}
		if role == "dev" {
			w = adminCall(a, cookie, csrf, "POST", grafanaPrefix+"api/ds/query", testGrafanaBody(`gateway_api_calls_total{app="other"}`, "prometheus"))
			if w.Code != 200 || strings.Contains(w.Body.String(), `"app":"other"`) {
				t.Fatal("foreign query leaked", w.Body.String())
			}
			w = adminCall(a, cookie, csrf, "GET", grafanaPrefix+"api/datasources/uid/prometheus/resources/api/v1/label/app/values?match%5B%5D="+url.QueryEscape("gateway_api_calls_total"), "")
			if w.Code != 200 || strings.Contains(w.Body.String(), "other") || !strings.Contains(w.Body.String(), "app") {
				t.Fatal("foreign label leaked", w.Code, w.Body.String())
			}
		}
	}
	// 自动查询面板及变量的表达式，使用实际 JSON 而非复制实现。
	for _, file := range []string{"../../deploy/observability/dashboard.json", "../../deploy/observability/application-dashboard.json"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var dashboard struct {
			Panels []struct {
				Title   string
				Targets []struct{ Expr string }
			}
		}
		if json.Unmarshal(raw, &dashboard) != nil {
			t.Fatal("invalid dashboard JSON")
		}
		user := "dev"
		if strings.HasSuffix(file, "/dashboard.json") {
			user = "admin"
		}
		cookie, csrf := adminLogin(t, a, user)
		replacer := strings.NewReplacer("$__rate_interval", "1m", "$__interval", "1m", "$app", ".*", "$instance", ".*", "$api", ".*", "$stream", ".*", "$model", ".*", "$error_code", ".*")
		for _, panel := range dashboard.Panels {
			for _, target := range panel.Targets {
				if target.Expr == "" {
					continue
				}
				w := adminCall(a, cookie, csrf, "POST", grafanaPrefix+"api/ds/query", testGrafanaBody(replacer.Replace(target.Expr), "prometheus"))
				var decoded struct {
					Results map[string]struct{ Error string }
				}
				if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &decoded) != nil || decoded.Results["A"].Error != "" {
					t.Error(file, panel.Title, w.Code, w.Body.String())
				}
			}
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	if os.Getenv("GW_TEST_GRAFANA_UI") == "true" {
		t.Log("local UI available at http://localhost:18081; test users admin/dev/cost")
		// 浏览器验收显式启用，最长十分钟，普通集成测试直接结束。
		deadline := time.Now().Add(10 * time.Minute)
		for time.Now().Before(deadline) {
			if stop := os.Getenv("GW_TEST_GRAFANA_UI_STOP"); stop != "" {
				if _, err := os.Stat(stop); err == nil {
					break
				}
			}
			// 仅浏览器验收的合成指标，不调用模型，也不构成吞吐压测。
			e.Metrics.APICalls.WithLabelValues("app", "/v1/chat/completions", "false", "success", "200", "").Inc()
			e.Metrics.APILatency.WithLabelValues("app", "/v1/chat/completions", "false").Observe(.03)
			e.Metrics.Tokens.WithLabelValues("app", "primary", "input", "reported").Add(100)
			e.Metrics.Tokens.WithLabelValues("app", "primary", "output", "reported").Add(20)
			e.Metrics.APICalls.WithLabelValues("other", "/v1/chat/completions", "false", "success", "200", "").Add(3)
			time.Sleep(time.Second)
		}
	}
}

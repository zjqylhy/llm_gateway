package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestScopedPromQLAllInputs(t *testing.T) {
	queries := []string{
		`sum(rate(gateway_api_calls_total[1m]))`,
		`sum(gateway_api_calls_total{app=~".*"}) / sum(gateway_calls_total{app!=""})`,
		`gateway_api_calls_total{app="other"} or gateway_calls_total`,
		`max_over_time((sum(gateway_api_calls_total) + sum(gateway_calls_total))[1h:10s])`,
		`gateway_api_calls_total * on(app) group_left(app_name) gateway_app_info`,
	}
	for _, query := range queries {
		rewritten, err := scopedPromQL(query, []string{"own.a", "own+b"}, false)
		if err != nil {
			t.Fatal(query, err)
		}
		expr, _ := parser.ParseExpr(rewritten)
		count := 0
		parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
			if selector, ok := n.(*parser.VectorSelector); ok {
				count++
				for _, app := range []string{"foreign", "ownXa", "ownbb", ""} {
					permitted := true
					for _, m := range selector.LabelMatchers {
						if m.Name == "app" && !m.Matches(app) {
							permitted = false
						}
					}
					if permitted {
						t.Errorf("foreign app %q survives %s", app, rewritten)
					}
				}
			}
			return nil
		})
		if count == 0 {
			t.Fatal("no selectors tested")
		}
	}
	for _, query := range []string{`up`, `{__name__=~".*"}`, `gateway_metering_outbox_bytes`, `gateway_api_calls_total @ 1`, `gateway_calls_total offset -1h`, `gateway_calls_total[25h]`, `sum_over_time(gateway_calls_total[2h:1s])`, `info(gateway_api_calls_total)`, `not valid`} {
		if _, err := scopedPromQL(query, []string{"own"}, false); err == nil {
			t.Error("accepted unsafe expression", query)
		}
	}
	if _, err := scopedPromQL(`gateway_calls_total`, nil, false); err == nil {
		t.Fatal("empty scope allowed")
	}
}

func testGrafanaBody(expr, uid string) string {
	now := time.Now()
	return fmt.Sprintf(`{"from":"%d","to":"%d","queries":[{"refId":"A","datasource":{"type":"prometheus","uid":%q},"expr":%q,"intervalMs":1000}]}`, now.Add(-time.Hour).UnixMilli(), now.UnixMilli(), uid, expr)
}

func TestGrafanaProxySessionScopeAndReadonly(t *testing.T) {
	var requests atomic.Int32
	var query string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		user, password, ok := r.BasicAuth()
		if !ok || user != "viewer" || password != "viewer-secret" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Grafana-User") != "" {
			t.Error("identity forwarding failed")
		}
		if r.URL.Path == grafanaPrefix+"api/ds/query" {
			raw, _ := io.ReadAll(r.Body)
			query = string(raw)
		}
		w.Header().Set("Set-Cookie", "grafana_session=private")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":{}}`))
	}))
	defer upstream.Close()
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	a.Grafana, _ = newGrafanaProxy(upstream.URL, "viewer", "viewer-secret")
	c, csrf := adminLogin(t, a, "dev")
	w := adminCall(a, c, csrf, "POST", grafanaPrefix+"api/ds/query", testGrafanaBody(`gateway_calls_total{app="other"} or gateway_calls_total`, "prometheus"))
	if w.Code != 200 || !strings.Contains(query, `app=~\"app\"`) || w.Header().Get("Set-Cookie") != "" || w.Header().Get("X-Frame-Options") != "SAMEORIGIN" {
		t.Fatal(w.Code, w.Body.String(), query)
	}
	before := requests.Load()
	for _, p := range []string{"api/dashboards/uid/llm-gateway", "d/llm-gateway/gateway", "api/datasources", "api/admin/users", "api/datasources/proxy/1/api/v1/query?query=up", "apis/query.grafana.app/v0alpha1/query", "public/build/../api/datasources", "api/live/ws"} {
		if w := adminCall(a, c, csrf, "GET", grafanaPrefix+p, ""); w.Code != 403 {
			t.Error(p, w.Code)
		}
	}
	for _, body := range []string{testGrafanaBody(`up`, "prometheus"), testGrafanaBody(`gateway_calls_total`, "__expr__"), testGrafanaBody(`gateway_calls_total`, "other")} {
		if w := adminCall(a, c, csrf, "POST", grafanaPrefix+"api/ds/query", body); w.Code != 403 {
			t.Error("query accepted", w.Code)
		}
	}
	r := httptest.NewRequest("POST", "http://console"+grafanaPrefix+"api/ds/query", strings.NewReader(testGrafanaBody(`gateway_calls_total`, "prometheus")))
	r.AddCookie(c)
	r.Header.Set("Origin", "https://foreign")
	recorder := httptest.NewRecorder()
	a.Handler().ServeHTTP(recorder, r)
	if recorder.Code != 403 {
		t.Fatal("foreign origin accepted")
	}
	if requests.Load() != before {
		t.Fatal("denied requests reached Grafana")
	}
	_, err := e.Store.Update(context.Background(), "test", "transfer", func(cfg *Config) error {
		app := cfg.Apps["app"]
		app.Owner = "admin"
		cfg.Apps["app"] = app
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if w := adminCall(a, c, csrf, "POST", grafanaPrefix+"api/ds/query", testGrafanaBody(`gateway_calls_total`, "prometheus")); w.Code != 403 {
		t.Fatal("former owner still authorized")
	}
	adminCall(a, c, csrf, "POST", "/admin/logout", "")
	if w := adminCall(a, c, csrf, "GET", grafanaPrefix+"public/build/a.js", ""); w.Code != 401 {
		t.Fatal("logout did not revoke assets")
	}
}

func TestGrafanaMetadataAndDashboardIsolation(t *testing.T) {
	for _, resource := range []string{"api/v1/series", "api/v1/labels", "api/v1/label/app/values"} {
		q, err := scopedPrometheusParams(resource, url.Values{}, []string{"app"}, false, time.Now())
		if err != nil || !strings.Contains(q.Get("match[]"), `app=~"app"`) {
			t.Fatal(resource, q, err)
		}
		if _, err := scopedPrometheusParams(resource, url.Values{"match[]": {"up"}}, []string{"app"}, false, time.Now()); err == nil {
			t.Fatal("metadata leaked global metric")
		}
	}
	for _, resource := range []string{"api/v1/metadata", "api/v1/targets", "api/v1/rules", "api/v1/query_exemplars"} {
		if _, err := scopedPrometheusParams(resource, url.Values{}, []string{"app"}, false, time.Now()); err == nil {
			t.Error("resource allowed", resource)
		}
	}
	cfg := Config{Apps: map[string]App{"a": {Name: "Own", Owner: "dev"}, "b": {Name: "PrivateName", Owner: "other"}}}
	data, err := scopedDashboard([]byte(`{"dashboard":{"templating":{"list":[{"name":"app","query":"global"}]}}}`), User{Name: "dev", Role: "developer"}, cfg)
	if err != nil || strings.Contains(string(data), "PrivateName") || !strings.Contains(string(data), "Own") {
		t.Fatal(string(data), err)
	}
	var decoded map[string]any
	if json.Unmarshal(data, &decoded) != nil {
		t.Fatal("invalid scoped JSON")
	}
}

func TestAppInfoRenameAndRemoval(t *testing.T) {
	m := NewMetrics()
	cfg := Config{Apps: map[string]App{"app": {Name: "First"}}}
	m.refreshApps(cfg)
	cfg.Apps["app"] = App{Name: "Second"}
	m.refreshApps(cfg)
	if testutil.CollectAndCount(m.AppInfo) != 1 {
		t.Fatal("obsolete name series retained")
	}
	delete(cfg.Apps, "app")
	m.refreshApps(cfg)
	if testutil.CollectAndCount(m.AppInfo) != 0 {
		t.Fatal("deleted app retained")
	}
}

func TestGrafanaMacroQueryAndDTOIsolation(t *testing.T) {
	body := testGrafanaBody(`rate(gateway_api_calls_total[$__rate_interval]) + rate(gateway_calls_total[$__interval])`, "prometheus")
	result, err := grafanaQueryBody([]byte(body), []string{"own"}, false, time.Now())
	if err != nil || strings.Contains(string(result), "$__") || !strings.Contains(string(result), `app=~\"own\"`) {
		t.Fatal(string(result), err)
	}
	cfg := Config{Apps: map[string]App{"a": {Name: "Own", Owner: "dev"}, "b": {Name: "PrivateName", Owner: "other"}}}
	data, err := scopedDashboard([]byte(`{"spec":{"variables":[{"kind":"QueryVariable","spec":{"name":"app","query":{}}}]}}`), User{Name: "dev", Role: "developer"}, cfg)
	if err != nil || strings.Contains(string(data), "PrivateName") || !strings.Contains(string(data), "CustomVariable") {
		t.Fatal(string(data), err)
	}
}

func TestGrafanaIndependentConcurrencyAndOutage(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { JSON(w, map[string]any{"ok": true}) }))
	a.Grafana, _ = newGrafanaProxy(upstream.URL, "viewer", "secret")
	c, csrf := adminLogin(t, a, "dev")
	a.MaxConcurrent = 1
	for i := 0; i < cap(a.admission.slots); i++ {
		a.admission.slots <- struct{}{}
	}
	if w := adminCall(a, c, csrf, "GET", grafanaPrefix+"api/health", ""); w.Code != 200 {
		t.Fatal("Grafana consumed regular admin admission", w.Code)
	}
	for len(a.admission.slots) > 0 {
		<-a.admission.slots
	}
	for i := 0; i < cap(a.Grafana.requests); i++ {
		a.Grafana.requests <- struct{}{}
	}
	if w := adminCall(a, c, csrf, "GET", grafanaPrefix+"api/health", ""); w.Code != 503 {
		t.Fatal("unbounded proxy admission", w.Code)
	}
	if w := adminCall(a, c, csrf, "GET", "/admin/apps", ""); w.Code != 200 {
		t.Fatal("monitoring saturation blocked admin", w.Code)
	}
	for len(a.Grafana.requests) > 0 {
		<-a.Grafana.requests
	}
	upstream.Close()
	if w := adminCall(a, c, csrf, "GET", grafanaPrefix+"api/health", ""); w.Code != 502 {
		t.Fatal("outage not expressed", w.Code)
	}
	if len(a.Grafana.requests) != 0 {
		t.Fatal("outage leaked slot")
	}
}

func TestGrafanaDropsBackendRewriteFields(t *testing.T) {
	body := testGrafanaBody(`gateway_calls_total`, "prometheus")
	body = strings.Replace(body, `"intervalMs":1000`, `"intervalMs":1000,"datasourceId":999,"adhocFilters":[{"key":"app","value":"other"}],"exemplar":true`, 1)
	result, err := grafanaQueryBody([]byte(body), []string{"own"}, false, time.Now())
	if err != nil || strings.Contains(string(result), "adhocFilters") || strings.Contains(string(result), "datasourceId") || strings.Contains(string(result), `"exemplar":true`) {
		t.Fatal(string(result), err)
	}
}

func TestGrafanaCompressedStaticAndRedirect(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _, _ := r.BasicAuth()
		if user != "viewer" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Grafana-User") != "" {
			t.Error("client identity reached Grafana")
		}
		if r.URL.Path == grafanaPrefix+"api/health" {
			w.Header().Set("Location", "https://foreign.example")
			w.WriteHeader(302)
			return
		}
		if r.Header.Get("Accept-Encoding") != "gzip" {
			t.Error("static compression not negotiated")
		}
		w.Header().Set("Content-Type", "application/javascript")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Set-Cookie", "grafana_session=secret")
		compressed := gzip.NewWriter(w)
		_, _ = compressed.Write([]byte("export default 'ok'"))
		_ = compressed.Close()
	}))
	defer upstream.Close()
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	a.Grafana, _ = newGrafanaProxy(upstream.URL, "viewer", "secret")
	c, csrf := adminLogin(t, a, "dev")
	r := httptest.NewRequest("GET", "http://console"+grafanaPrefix+"public/build/test.js", nil)
	r.AddCookie(c)
	r.AddCookie(&http.Cookie{Name: "grafana_session", Value: "administrator"})
	r.SetBasicAuth("admin", "forged")
	r.Header.Set("X-Grafana-User", "admin")
	r.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, r)
	gz, err := gzip.NewReader(bytes.NewReader(w.Body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	plain, _ := io.ReadAll(gz)
	if string(plain) != "export default 'ok'" || w.Header().Get("Content-Encoding") != "gzip" || w.Header().Get("Set-Cookie") != "" || w.Header().Get("Vary") != "Accept-Encoding" {
		t.Fatal("compressed static response invalid", w.Header())
	}
	w = adminCall(a, c, csrf, "GET", grafanaPrefix+"api/health", "")
	if w.Code != 502 || w.Header().Get("Location") != "" {
		t.Fatal("external redirect allowed", w.Code)
	}
}

// 使用交付 JSON 验证表达式与角色策略，避免 UI 改动留下无效或越权面板。
func TestGrafanaDashboardExpressions(t *testing.T) {
	for _, file := range []string{"../../deploy/observability/dashboard.json", "../../deploy/observability/application-dashboard.json"} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var dashboard struct {
			Panels []struct {
				Title   string
				Targets []struct{ Expr string }
			}
		}
		if json.Unmarshal(data, &dashboard) != nil {
			t.Fatal(file, "invalid JSON")
		}
		admin := strings.HasSuffix(file, "/dashboard.json")
		replacer := strings.NewReplacer("$__rate_interval", "1m", "$__interval", "1m", "$app", ".*", "$instance", ".*", "$api", ".*", "$stream", ".*", "$model", ".*", "$error_code", ".*")
		for _, panel := range dashboard.Panels {
			for _, target := range panel.Targets {
				if target.Expr == "" {
					continue
				}
				if _, err := scopedPromQL(replacer.Replace(target.Expr), []string{"app"}, admin); err != nil {
					t.Error(file, panel.Title, err)
				}
			}
		}
	}
}

func TestGrafanaStatusShowsOutageWithoutBlockingApps(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { JSON(w, map[string]any{"database": "ok"}) }))
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	a.Grafana, _ = newGrafanaProxy(upstream.URL, "viewer", "secret")
	c, csrf := adminLogin(t, a, "dev")
	if w := adminCall(a, c, csrf, "GET", "/admin/monitoring", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"available":true`) {
		t.Fatal(w.Code, w.Body.String())
	}
	upstream.Close()
	if w := adminCall(a, c, csrf, "GET", "/admin/monitoring", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"available":false`) {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := adminCall(a, c, csrf, "GET", "/admin/apps", ""); w.Code != 200 {
		t.Fatal("Grafana outage blocked apps", w.Code)
	}
}

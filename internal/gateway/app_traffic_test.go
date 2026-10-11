package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func callCount(t *testing.T, e *Engine, app, outcome, status, code string) float64 {
	t.Helper()
	rows, err := e.Metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range rows {
		if family.GetName() != "gateway_calls_total" {
			continue
		}
		for _, row := range family.Metric {
			labels := map[string]string{}
			for _, label := range row.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["app"] == app && labels["outcome"] == outcome && labels["http_status"] == status && labels["error_code"] == code {
				return row.Counter.GetValue()
			}
		}
	}
	return 0
}

func TestApplicationCallMetricsIncludeEarlyAndStreamFailures(t *testing.T) {
	e := testEngine(t, MockProvider)
	if w := call(t, e, "[fail-primary] hello", false); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if got := callCount(t, e, "app", "success", "200", ""); got != 1 {
		t.Fatal("fallback counted client call more than once", got)
	}
	for _, test := range []struct{ body, key, code, status string }{
		{`{}`, "test-key", "invalid_request_error", "400"},
		{`{"model":"forbidden","messages":[{"role":"user","content":"x"}]}`, "test-key", "model_not_allowed", "403"},
		{`{}`, "unknown-key", "invalid_api_key", "401"},
	} {
		w := rerankCall(e, "/v1/chat/completions", test.key, test.body)
		app := "app"
		if test.key == "unknown-key" {
			app = "_unknown"
		}
		if callCount(t, e, app, "error", test.status, test.code) != 1 {
			t.Fatal(w.Code, w.Body.String(), test.code)
		}
	}
	w := call(t, e, "[break-stream]", true)
	if w.Code != 200 || callCount(t, e, "app", "error", "200", "stream_interrupted") != 1 {
		t.Fatal(w.Code, w.Body.String())
	}
	if callCount(t, e, "app", "success", "200", "") != 1 {
		t.Fatal("stream failure counted as success")
	}
	if w := rerankCall(e, "/v1/embeddings", "test-key", `{"model":"embedding","input":"x"}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if callCount(t, e, "app", "success", "200", "") != 2 {
		t.Fatal("embedding was not counted")
	}
}

func TestApplicationQPSErrorAndAppliedLimit(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, err := e.Store.Update(context.Background(), "test", "limit", func(c *Config) error { app := c.Apps["app"]; app.QPS = 1; c.Apps["app"] = app; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if w := call(t, e, "first", false); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w := call(t, e, "second", false)
	if w.Code != 429 || !strings.Contains(w.Body.String(), "app_qps_exceeded") || callCount(t, e, "app", "error", "429", "app_qps_exceeded") != 1 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	e.Metrics.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), `gateway_app_qps_limit{app="app"} 1`) {
		t.Fatal("applied limit not exported")
	}
}

func TestApplicationStreamMeteringErrorUsesClientCode(t *testing.T) {
	e := testEngine(t, MockProvider)
	e.Bus = failedBus{e.Bus, true}
	w := call(t, e, "hello", true)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "metering_unavailable") || callCount(t, e, "app", "error", "200", "metering_unavailable") != 1 {
		t.Fatal(w.Code, w.Body.String())
	}
	if callCount(t, e, "app", "success", "200", "") != 0 {
		t.Fatal("unacknowledged metering counted as success")
	}
}

func TestApplicationRerankCallsCountOnce(t *testing.T) {
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
		JSON(w, map[string]any{"results": []any{map[string]any{"index": 0, "relevance_score": 0.9}, map[string]any{"index": 1, "relevance_score": 0.1}}, "usage": map[string]int{"total_tokens": 16}})
	})
	configureRerank(t, e, rerankTestModel("rank", "compatible"))
	for _, path := range []string{"/v1/rerank", "/v1/reranks"} {
		w := rerankCall(e, path, "test-key", rerankTestBody)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if callCount(t, e, "app", "success", "200", "") != 2 {
		t.Fatal("rerank entry points did not count once each")
	}
}

func TestApplicationOverloadMetricsDoNotReadBody(t *testing.T) {
	e := testEngine(t, MockProvider)
	e.MaxConcurrent = 1
	if !e.admission.enter(1, 1) {
		t.Fatal("cannot occupy slot")
	}
	defer e.admission.leave()
	r := httptest.NewRequest("POST", "/v1/rerank", &unreadBody{t})
	r.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	e.Handler().ServeHTTP(w, r)
	if w.Code != 503 || callCount(t, e, "app", "error", "503", "gateway_overloaded") != 1 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestApplicationTrafficTimeWindow(t *testing.T) {
	now := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		from, to time.Time
		valid    bool
	}{
		{now.Add(-24 * time.Hour), now, true},
		{now.Add(-22 * time.Hour), now.Add(-20 * time.Hour), true},
		{now.Add(-time.Hour), now.Add(-time.Hour), false},
		{now.Add(-25 * time.Hour), now, false},
		{now.Add(-26 * time.Hour), now.Add(-25 * time.Hour), false},
		{now, now.Add(time.Hour), false},
	} {
		q := url.Values{"from": {test.from.Format(time.RFC3339)}, "to": {test.to.Format(time.RFC3339)}}
		_, _, err := trafficWindow(q, now)
		if (err == nil) != test.valid {
			t.Fatal(q, err)
		}
	}
	if _, _, err := trafficWindow(url.Values{"from": {"bad-time"}}, now); err == nil {
		t.Fatal("invalid time accepted")
	}
}

func TestApplicationTrafficFractionalTimestamp(t *testing.T) {
	at := time.Date(2026, 10, 11, 10, 0, 0, 123000000, time.UTC)
	raw, _ := json.Marshal([]any{float64(at.UnixMilli()) / 1000, "1.5"})
	var sample []json.RawMessage
	_ = json.Unmarshal(raw, &sample)
	point, err := parseTrafficPoint(sample)
	if err != nil || !point.At.Equal(at) {
		t.Fatal(point, err)
	}
	result := trafficResponse{From: at, To: at.Add(time.Hour), Apps: []trafficApp{{ID: "app"}}}
	if err := mergeTraffic(&result, []prometheusSeries{{Metric: map[string]string{"app": "app", "kind": "success"}, Values: [][]json.RawMessage{sample}}}, false); err != nil {
		t.Fatal(err)
	}
}

func trafficTestPath(from, to time.Time, app string) string {
	return "/admin/app-traffic?" + url.Values{"from": {from.Format(time.RFC3339)}, "to": {to.Format(time.RFC3339)}, "app_id": {app}}.Encode()
}

func TestApplicationTrafficPrometheusQueriesAndRBAC(t *testing.T) {
	var requests atomic.Int32
	to := time.Now().UTC().Truncate(time.Second)
	from := to.Add(-22 * time.Hour)
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		query := r.Form.Get("query")
		if !strings.Contains(query, `app="app"`) || strings.Contains(query, "other") {
			t.Error("query escaped authorization", query)
		}
		point := []any{to.Unix(), "12.5"}
		if r.URL.Path == "/api/v1/query_range" {
			if r.Form.Get("step") != "60s" || r.Form.Get("start") != from.Format(time.RFC3339) || r.Form.Get("end") != to.Format(time.RFC3339) || !strings.Contains(query, "rate(") || !strings.Contains(query, "sum by(app,http_status,error_code)") || !strings.Contains(query, "@ end()") || !strings.Contains(query, "and on(app,http_status,error_code)") {
				t.Error("invalid multi-instance range query", r.Form)
			}
			rows := []any{}
			for _, kind := range []string{"success", "limit"} {
				rows = append(rows, map[string]any{"metric": map[string]string{"app": "app", "kind": kind}, "values": []any{[]any{from.Unix(), "2"}, point}})
			}
			for _, labels := range []map[string]string{
				{"app": "app", "kind": "failure", "http_status": "400", "error_code": "invalid_request_error"},
				{"app": "app", "kind": "failure", "http_status": "429", "error_code": "app_qps_exceeded"},
			} {
				rows = append(rows, map[string]any{"metric": labels, "values": []any{[]any{from.Unix(), "0"}, point}})
			}
			JSON(w, map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": rows}})
		} else {
			if r.Form.Get("time") != to.Format(time.RFC3339) || !strings.Contains(query, "increase(") || !strings.Contains(query, "79200000ms") || !strings.Contains(query, "max by(app)") {
				t.Error("invalid selected-window summary query", r.Form)
			}
			rows := []any{
				map[string]any{"metric": map[string]string{"app": "app", "kind": "success"}, "value": point},
				map[string]any{"metric": map[string]string{"app": "app", "kind": "failure", "http_status": "429", "error_code": "app_qps_exceeded"}, "value": []any{to.Unix(), "0.5"}},
				map[string]any{"metric": map[string]string{"app": "app", "kind": "failure", "http_status": "400", "error_code": "invalid_request_error"}, "value": []any{to.Unix(), "0.25"}},
				map[string]any{"metric": map[string]string{"app": "app", "kind": "limit"}, "value": []any{to.Unix(), "100"}},
			}
			JSON(w, map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": rows}})
		}
	}))
	defer prom.Close()
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	a.Prometheus = &TrafficPrometheus{URL: prom.URL, Client: prom.Client()}
	cookie, csrf := adminLogin(t, a, "dev")
	w := adminCall(a, cookie, csrf, "GET", trafficTestPath(from, to, "app"), "")
	var result trafficResponse
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || !result.Available || len(result.Apps) != 1 || *result.Apps[0].SuccessQPS != 12.5 || *result.Apps[0].FailureQPS != .75 || *result.Apps[0].LimitAtEnd != 100 || len(result.Apps[0].Failures) != 2 || len(result.Apps[0].Series) != 4 || requests.Load() != 2 {
		t.Fatal(w.Code, w.Body.String(), requests.Load())
	}
	lines := result.Apps[0].Series
	if lines[2].HTTPStatus != "400" || lines[2].ErrorCode != "invalid_request_error" || lines[3].HTTPStatus != "429" || lines[3].ErrorCode != "app_qps_exceeded" || lines[2].Points[0].Value != 0 {
		t.Fatal("error dimensions or zero samples lost", lines)
	}
	for _, id := range []string{"other", "", "missing"} {
		if w := adminCall(a, cookie, csrf, "GET", trafficTestPath(from, to, id), ""); w.Code != 403 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if requests.Load() != 2 {
		t.Fatal("unauthorized request queried Prometheus")
	}
	if w := adminCall(a, cookie, csrf, "GET", trafficTestPath(to.Add(-25*time.Hour), to, "app"), ""); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if selector := trafficSelector([]string{"a.*", `b"|other`}); !strings.Contains(selector, `a\\.\\*`) || strings.Contains(selector, `app=~"(a.*`) {
		t.Fatal("unescaped selector", selector)
	}
}

func TestApplicationTrafficMissingOrFailedPrometheus(t *testing.T) {
	for _, mode := range []string{"unconfigured", "empty", "unavailable", "partial", "invalid", "oversized", "cross-app", "nan"} {
		t.Run(mode, func(t *testing.T) {
			to := time.Now().UTC().Truncate(time.Second)
			prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				kind := "vector"
				if r.URL.Path == "/api/v1/query_range" {
					kind = "matrix"
				}
				if mode == "unavailable" {
					w.WriteHeader(503)
					return
				}
				if mode == "invalid" {
					_, _ = fmt.Fprint(w, `{}`)
					return
				}
				if mode == "oversized" {
					_, _ = fmt.Fprint(w, strings.Repeat("x", (8<<20)+1))
					return
				}
				body := map[string]any{"status": "success", "data": map[string]any{"resultType": kind, "result": []any{}}}
				if mode == "partial" {
					body["warnings"] = []string{"partial-data"}
				}
				if mode == "cross-app" || mode == "nan" {
					app, value := "other", "1"
					if mode == "nan" {
						app, value = "app", "NaN"
					}
					row := map[string]any{"metric": map[string]string{"app": app, "kind": "success"}, "value": []any{to.Unix(), value}}
					if kind == "matrix" {
						row["values"] = []any{[]any{to.Unix(), value}}
					}
					body["data"] = map[string]any{"resultType": kind, "result": []any{row}}
				}
				JSON(w, body)
			}))
			defer prom.Close()
			e := testEngine(t, MockProvider)
			a := NewAdmin(e, false, "")
			if mode != "unconfigured" {
				a.Prometheus = &TrafficPrometheus{URL: prom.URL, Client: prom.Client()}
			}
			cookie, csrf := adminLogin(t, a, "dev")
			w := adminCall(a, cookie, csrf, "GET", trafficTestPath(to.Add(-time.Hour), to, "app"), "")
			var result trafficResponse
			_ = json.Unmarshal(w.Body.Bytes(), &result)
			if w.Code != 200 || result.Available || result.Reason == "" || result.Apps[0].SuccessQPS != nil || result.Apps[0].FailureQPS != nil || len(result.Apps[0].Series) != 0 {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
	for _, raw := range []string{"file:///local", "http://user:secret@prometheus:9090", "http://prometheus:9090?query=all", "bad"} {
		if _, err := newTrafficPrometheus(raw); err == nil {
			t.Fatal("invalid deployment URL accepted", raw)
		}
	}
}

func TestApplicationTrafficFailureSeriesLabels(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Second)
	row := func(status, code string) prometheusSeries {
		point := []json.RawMessage{json.RawMessage(fmt.Sprint(at.Unix())), json.RawMessage(`"0.1"`)}
		return prometheusSeries{Metric: map[string]string{"app": "app", "kind": "failure", "http_status": status, "error_code": code}, Value: point, Values: [][]json.RawMessage{point}}
	}
	for _, summary := range []bool{false, true} {
		for _, test := range []struct {
			name  string
			rows  []prometheusSeries
			valid bool
		}{
			{"stream and pre-header metering", []prometheusSeries{row("200", "metering_unavailable"), row("503", "metering_unavailable")}, true},
			{"distinct business errors", []prometheusSeries{row("400", "invalid_request_error"), row("400", "unsupported_capability")}, true},
			{"missing labels", []prometheusSeries{row("", "")}, false},
			{"unknown code", []prometheusSeries{row("400", "unbounded_upstream_code")}, false},
			{"wrong status", []prometheusSeries{row("200", "app_qps_exceeded")}, false},
			{"duplicate pair", []prometheusSeries{row("400", "invalid_request_error"), row("400", "invalid_request_error")}, false},
		} {
			t.Run(fmt.Sprintf("%s/summary=%t", test.name, summary), func(t *testing.T) {
				result := trafficResponse{From: at.Add(-time.Hour), To: at, Apps: []trafficApp{{ID: "app"}}}
				err := mergeTraffic(&result, test.rows, summary)
				if (err == nil) != test.valid {
					t.Fatal(err, result)
				}
				if test.valid && !summary && len(result.Apps[0].Series) != 2 {
					t.Fatal("distinct failure lines collapsed", result)
				}
			})
		}
	}
}

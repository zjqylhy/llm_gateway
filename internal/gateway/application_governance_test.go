package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestApplicationRouteOverrideInheritanceAndAudit(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	cookie, csrf := adminLogin(t, a, "admin")
	path := "/admin/apps/app/routes/chat"
	body := `{"alias":"chat","kind":"chat","strategy":"priority","candidates":[{"model_id":"backup","weight":100}]}`
	if w := adminCall(a, cookie, csrf, "PUT", path, body); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := e.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []bool{false, true} {
		w := call(t, e, "hello", stream)
		if w.Code != 200 || w.Header().Get("X-Gateway-Model") != "backup" {
			t.Fatal(w.Code, w.Body.String())
		}
		rows, err := e.Store.Records(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
		if err != nil || len(rows) != 2 {
			t.Fatal(rows, err)
		}
		for _, row := range rows {
			if row.AppID != "app" || row.AppName != "test" || row.RouteScope != "application:app" || row.RouteStrategy != "priority" || row.ConfigVersion == 0 {
				t.Fatalf("missing snapshot: %+v", row)
			}
		}
	}
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"chat","messages":[{"role":"user","content":"hello"}]}`))
	r.Header.Set("Authorization", "Bearer other-key")
	w := httptest.NewRecorder()
	e.Handler().ServeHTTP(w, r)
	if w.Code != 200 || w.Header().Get("X-Gateway-Model") != "primary" {
		t.Fatal("other application changed", w.Code)
	}
	if w := adminCall(a, cookie, csrf, "DELETE", path, ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	_ = e.Refresh(context.Background())
	if w := call(t, e, "hello", false); w.Header().Get("X-Gateway-Model") != "primary" {
		t.Fatal("inheritance not restored")
	}
}

func TestApplicationRoutePermissionsValidationAndNoGlobalEscape(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	dev, csrf := adminLogin(t, a, "dev")
	for _, req := range []struct {
		method, path string
		code         int
	}{
		{"GET", "/admin/apps/app/routes", 200},
		{"GET", "/admin/apps/other/routes", 403},
		{"PUT", "/admin/apps/app/routes/chat", 403},
		{"DELETE", "/admin/apps/app/routes/chat", 403},
	} {
		if w := adminCall(a, dev, csrf, req.method, req.path, `{}`); w.Code != req.code {
			t.Fatal(req, w.Code, w.Body.String())
		}
	}
	admin, csrf := adminLogin(t, a, "admin")
	for _, body := range []string{
		`{"alias":"embedding","kind":"embedding","strategy":"priority","candidates":[{"model_id":"embedding","weight":100}]}`,
		`{"alias":"chat","kind":"embedding","strategy":"priority","candidates":[{"model_id":"embedding","weight":100}]}`,
		`{"alias":"chat","kind":"chat","strategy":"weight","candidates":[{"model_id":"backup","weight":90}]}`,
	} {
		if w := adminCall(a, admin, csrf, "PUT", "/admin/apps/app/routes/chat", body); w.Code != 400 {
			t.Fatal("invalid override accepted", w.Code)
		}
	}
	valid := `{"alias":"chat","kind":"chat","strategy":"priority","candidates":[{"model_id":"backup","weight":100}]}`
	if w := adminCall(a, admin, csrf, "PUT", "/admin/apps/app/routes/chat", valid); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	_, err := e.Store.Update(context.Background(), "test", "ineligible", func(c *Config) error { m := c.Models["backup"]; m.Context = 1; c.Models[m.ID] = m; return nil })
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Refresh(context.Background())
	if w := call(t, e, "hello", false); w.Code != 400 || !strings.Contains(w.Body.String(), "unsupported_capability") {
		t.Fatal("escaped to global", w.Code, w.Body.String())
	}
	// 修改模型能力时必须同时校验专属配置；失败不发布半有效的快照。
	cfg, _ := e.Store.Load(context.Background())
	m := cfg.Models["backup"]
	m.Capabilities = []string{"embedding"}
	m.MaxOutput = 0
	m.Dimensions = 32
	// 全局仍保留 backup，所以移除全局候选后验证专属候选仍阻止改型。
	base := `{"alias":"chat","kind":"chat","strategy":"priority","candidates":[{"model_id":"primary","weight":100}]}`
	if w := adminCall(a, admin, csrf, "PUT", "/admin/routes", base); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	b, _ := json.Marshal(m)
	if w := adminCall(a, admin, csrf, "PUT", "/admin/models/backup", string(b)); w.Code != 400 {
		t.Fatal("invalidated override accepted", w.Code)
	}
}

func TestApplicationGraySamplesAndRollbackAreScoped(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, err := e.Store.Update(context.Background(), "test", "gray", func(c *Config) error {
		r := c.Routes["chat"]
		r.Gray = Gray{RunID: "same-run", State: "running", Candidate: "backup", Percent: 100, MinSamples: 10, MaxError: .2, MaxLatencyMS: 1000}
		c.Routes["chat"] = r
		putScopedRoute(c, "application:app", r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		for _, scope := range []string{"application:app", ""} {
			status := "success"
			if scope != "" {
				status = "failed"
			}
			x := Event{ID: fmt.Sprintf("%s-%d:2", scope, i), AttemptID: fmt.Sprintf("%s-%d", scope, i), RequestID: "gray-request", AppID: "app", Alias: "chat", Model: Model{ID: "backup"}, At: time.Now(), Kind: "completed", Purpose: "inference", Revision: 2, Gray: true, GrayRun: "same-run", RouteScope: scope, Status: status}
			if err := e.Store.Apply(context.Background(), x); err != nil {
				t.Fatal(err)
			}
		}
	}
	cfg, _ := e.Store.Load(context.Background())
	if err := RollbackGray(context.Background(), e, cfg); err != nil {
		t.Fatal(err)
	}
	cfg, _ = e.Store.Load(context.Background())
	if cfg.Routes["chat"].Gray.State != "running" || cfg.AppRoutes["app"]["chat"].Gray.State != "rolled_back" {
		t.Fatal("gray rollback crossed scope")
	}
	// 新运行 ID 不使用上一轮失败样本。
	a := NewAdmin(e, false, "")
	cookie, csrf := adminLogin(t, a, "admin")
	w := adminCall(a, cookie, csrf, "POST", "/admin/gray-release", `{"app_id":"app","alias":"chat","policy":{"state":"running","candidate":"backup","percent":100,"min_samples":10,"max_error":0.2,"max_latency_ms":1000}}`)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	cfg, _ = e.Store.Load(context.Background())
	if cfg.AppRoutes["app"]["chat"].Gray.RunID == "same-run" {
		t.Fatal("run id reused")
	}
	if err := RollbackGray(context.Background(), e, cfg); err != nil {
		t.Fatal(err)
	}
	cfg, _ = e.Store.Load(context.Background())
	if cfg.AppRoutes["app"]["chat"].Gray.State != "running" {
		t.Fatal("old samples rolled back new release")
	}
	_ = e.Refresh(context.Background())
	w = call(t, e, "hello", false)
	rows, _ := e.Store.Records(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
	for _, row := range rows {
		if row.Purpose == "inference" && row.GrayRun != cfg.AppRoutes["app"]["chat"].Gray.RunID {
			t.Fatal("attempt used global run")
		}
	}
}

type businessMeteringStore struct {
	*MemoryStore
	global   string
	controls map[string]appMeteringControl
}

func (s businessMeteringStore) applicationMeteringControls(_ context.Context, ids []string) (string, map[string]appMeteringControl, error) {
	out := map[string]appMeteringControl{}
	for _, id := range ids {
		out[id] = s.controls[id]
	}
	return s.global, out, nil
}

func TestApplicationMeteringRBACAndBusinessImpact(t *testing.T) {
	e := testEngine(t, MockProvider)
	now, passed := time.Now().UTC(), true
	s := businessMeteringStore{e.Store.(*MemoryStore), "healthy", map[string]appMeteringControl{"app": {State: "healthy", CheckedAt: &now, ReportPassed: &passed, ReportAt: &now}, "other": {State: "frozen"}}}
	e.Store = s
	a := NewAdmin(e, false, "")
	dev, csrf := adminLogin(t, a, "dev")
	w := adminCall(a, dev, csrf, "GET", "/admin/app-metering", "")
	var page struct {
		Applications []appMeteringView
		Next         string
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Applications) != 1 || page.Applications[0].AppID != "app" || page.Applications[0].ReportPassed == nil || !*page.Applications[0].ReportPassed {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, path := range []string{"/admin/app-metering?app_id=other", "/admin/instances", "/admin/metering"} {
		if w := adminCall(a, dev, csrf, "GET", path, ""); w.Code != 403 {
			t.Fatal("private status exposed", path, w.Code)
		}
	}
	s.global = "frozen"
	e.Store = s
	w = adminCall(a, dev, csrf, "GET", "/admin/app-metering?app_id=app", "")
	if !strings.Contains(w.Body.String(), "平台计量异常") || strings.Contains(w.Body.String(), "generation") || strings.Contains(w.Body.String(), "other") {
		t.Fatal(w.Body.String())
	}
	s.global = "healthy"
	s.controls["app"] = appMeteringControl{State: "quiescing"}
	e.Store = s
	w = adminCall(a, dev, csrf, "GET", "/admin/app-metering?app_id=app", "")
	if !strings.Contains(w.Body.String(), "暂缓新调用") {
		t.Fatal(w.Body.String())
	}
	delete(s.controls, "app")
	e.Store = s
	w = adminCall(a, dev, csrf, "GET", "/admin/app-metering?app_id=app", "")
	if !strings.Contains(w.Body.String(), `"state":"unknown"`) {
		t.Fatal("missing app marked healthy", w.Body.String())
	}
	s.global = "unavailable"
	e.Store = s
	w = adminCall(a, dev, csrf, "GET", "/admin/app-metering?app_id=app", "")
	if !strings.Contains(w.Body.String(), "计量许可不可用") {
		t.Fatal("permit outage impact missing", w.Body.String())
	}
	_, _ = e.Store.Update(context.Background(), "test", "cost-user", func(c *Config) error {
		u := c.Users["dev"]
		u.Name, u.Role = "cost", "cost"
		c.Users[u.Name] = u
		return nil
	})
	cost, costCSRF := adminLogin(t, a, "cost")
	w = adminCall(a, cost, costCSRF, "GET", "/admin/app-metering", "")
	if json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Applications) != 2 {
		t.Fatal("cost role lost existing cross-app visibility", w.Body.String())
	}
	if w := adminCall(a, cost, costCSRF, "GET", "/admin/instances", ""); w.Code != 403 {
		t.Fatal("cost role exposed operations")
	}
}

func TestApplicationMeteringPaginationAndOfflineMode(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, err := e.Store.Update(context.Background(), "test", "apps", func(c *Config) error {
		for i := range 105 {
			id := fmt.Sprintf("many-%03d", i)
			c.Apps[id] = App{ID: id, Owner: "admin"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	a := NewAdmin(e, false, "")
	cookie, csrf := adminLogin(t, a, "admin")
	var first, second struct {
		Applications []appMeteringView
		Next         string
	}
	w := adminCall(a, cookie, csrf, "GET", "/admin/app-metering", "")
	_ = json.Unmarshal(w.Body.Bytes(), &first)
	if len(first.Applications) != 100 || first.Next == "" || first.Applications[0].State != "offline" {
		t.Fatal(w.Body.String())
	}
	w = adminCall(a, cookie, csrf, "GET", "/admin/app-metering?after="+first.Next, "")
	_ = json.Unmarshal(w.Body.Bytes(), &second)
	if len(second.Applications) != 7 || second.Next != "" {
		t.Fatal(w.Body.String())
	}
}

func TestInstanceStatusSeparatesHeartbeatAndConfiguration(t *testing.T) {
	e := testEngine(t, MockProvider)
	s := e.Store.(*MemoryStore)
	cfg, _ := s.Load(context.Background())
	s.mu.Lock()
	s.instances = map[string]Instance{"gateway:new": {cfg.Version, time.Now()}, "worker:old": {cfg.Version, time.Now().Add(-time.Minute)}, "admin:lag": {cfg.Version - 1, time.Now()}, "legacy": {cfg.Version, time.Now()}}
	s.mu.Unlock()
	a := NewAdmin(e, false, "")
	cookie, csrf := adminLogin(t, a, "admin")
	w := adminCall(a, cookie, csrf, "GET", "/admin/instances", "")
	var rows map[string]struct {
		Status  string
		Service string
	}
	if json.Unmarshal(w.Body.Bytes(), &rows) != nil || rows["gateway:new"].Status != "ready" || rows["gateway:new"].Service != "gateway" || rows["worker:old"].Status != "stale" || rows["admin:lag"].Status != "config_pending" || rows["legacy"].Service != "unknown" {
		t.Fatal(w.Body.String())
	}
}

func TestAuditSnapshotsSurviveAppRenameAndLegacyJSON(t *testing.T) {
	e := testEngine(t, MockProvider)
	w := call(t, e, "hello", false)
	request := w.Header().Get("X-Request-Id")
	_, _ = e.Store.Update(context.Background(), "test", "rename", func(c *Config) error { a := c.Apps["app"]; a.Name = "renamed"; c.Apps[a.ID] = a; return nil })
	rows, _ := e.Store.Records(context.Background(), Filter{RequestID: request})
	for _, row := range rows {
		if row.AppName != "test" {
			t.Fatal("historical name changed")
		}
	}
	legacy := Event{AppID: "app", AttemptID: "old", Revision: 2}
	raw, _ := json.Marshal(legacy)
	for _, field := range []string{"app_name", "config_version", "route_scope", "route_strategy"} {
		if strings.Contains(string(raw), field) {
			t.Fatal("legacy event hash would change")
		}
	}
}

func TestApplicationRoutesForEmbeddingAndRerank(t *testing.T) {
	t.Run("embedding", func(t *testing.T) {
		e := testEngine(t, MockProvider)
		_, err := e.Store.Update(context.Background(), "test", "embedding-override", func(c *Config) error {
			m := c.Models["embedding"]
			m.ID = "embedding-backup"
			c.Models[m.ID] = m
			putScopedRoute(c, "application:app", Route{Alias: "embedding", Kind: "embedding", Strategy: "priority", Candidates: []Candidate{{m.ID, 100}}})
			return validateAppRoutes(*c)
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = e.Refresh(context.Background())
		w := rerankCall(e, "/v1/embeddings", "test-key", `{"model":"embedding","input":"hello"}`)
		if w.Code != 200 || w.Header().Get("X-Gateway-Model") != "embedding-backup" {
			t.Fatal(w.Code, w.Body.String())
		}
	})
	t.Run("rerank", func(t *testing.T) {
		e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
			var body struct{ Model string }
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Model != "vendor-rank-backup" {
				t.Error("incorrect rerank route", body.Model)
			}
			JSON(w, map[string]any{"results": []any{map[string]any{"index": 0, "relevance_score": .9}, map[string]any{"index": 1, "relevance_score": .1}}, "usage": map[string]int{"total_tokens": 20}})
		})
		configureRerank(t, e, rerankTestModel("rank", "compatible"), rerankTestModel("rank-backup", "compatible"))
		_, err := e.Store.Update(context.Background(), "test", "rerank-override", func(c *Config) error {
			putScopedRoute(c, "application:app", Route{Alias: "rerank", Kind: "rerank", Strategy: "priority", Candidates: []Candidate{{"rank-backup", 100}}})
			return validateAppRoutes(*c)
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = e.Refresh(context.Background())
		w := rerankCall(e, "/v1/reranks", "test-key", rerankTestBody)
		if w.Code != 200 || w.Header().Get("X-Gateway-Model") != "rank-backup" {
			t.Fatal(w.Code, w.Body.String())
		}
	})
}

func TestApplicationCacheAuditAndRouteVersionIsolation(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, err := e.Store.Update(context.Background(), "test", "cache-override", func(c *Config) error {
		app := c.Apps["app"]
		app.Cache = true
		c.Apps[app.ID] = app
		putScopedRoute(c, "application:app", Route{Alias: "chat", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{"backup", 100}}})
		// 内部向量化始终标为应用缓存配置，不误认成恰好同名的外部路由。
		c.Routes["internal"] = c.Routes["embedding"]
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Refresh(context.Background())
	if w := call(t, e, "hello", false); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w := call(t, e, "hello", false)
	if w.Header().Get("X-Gateway-Cache") != "hit" {
		t.Fatal("expected cache hit", w.Code)
	}
	rows, _ := e.Store.Records(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
	for _, row := range rows {
		if row.AppName != "test" || row.ConfigVersion == 0 {
			t.Fatal("cache provenance missing")
		}
		if row.Purpose == "cache_embedding" && row.RouteScope != "internal" {
			t.Fatal("internal route misattributed")
		}
		if row.Purpose == "request" && row.RouteScope != "application:app" {
			t.Fatal("cache hit source missing")
		}
	}
	_, _ = e.Store.Update(context.Background(), "test", "change-override", func(c *Config) error { delete(c.AppRoutes["app"], "chat"); return nil })
	_ = e.Refresh(context.Background())
	w = call(t, e, "hello", false)
	if w.Header().Get("X-Gateway-Cache") == "hit" || w.Header().Get("X-Gateway-Model") != "primary" {
		t.Fatal("old strategy cache reused")
	}
}

func TestApplicationFallbackKeepsAuditScopeOnEveryAttempt(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, err := e.Store.Update(context.Background(), "test", "override", func(c *Config) error {
		putScopedRoute(c, "application:app", c.Routes["chat"])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Refresh(context.Background())
	w := call(t, e, "[fail-primary] hello", false)
	if w.Code != 200 || w.Header().Get("X-Gateway-Model") != "backup" {
		t.Fatal(w.Code, w.Body.String())
	}
	rows, _ := e.Store.Records(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
	if len(rows) != 3 {
		t.Fatal("missing separate fallback attempt")
	}
	for _, row := range rows {
		if row.RouteScope != "application:app" || row.AppName != "test" {
			t.Fatal("fallback provenance missing")
		}
	}
}

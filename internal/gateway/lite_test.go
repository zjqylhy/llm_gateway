package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLiteOptionsRejectInvalidValuesBeforeConnecting(t *testing.T) {
	for _, item := range []struct{ name, value string }{
		{"GW_CACHE_ENABLED", "invalid"}, {"GW_MYSQL_MAX_OPEN_CONNS", "1"},
		{"GW_MYSQL_MAX_IDLE_CONNS", "31"}, {"GW_REDIS_POOL_SIZE", "0"},
		{"GW_REDIS_POOL_SIZE", "1025"}, {"GW_MYSQL_MAX_IDLE_CONNS", "-1"},
	} {
		t.Run(item.name+"="+item.value, func(t *testing.T) {
			for _, name := range []string{"GW_CACHE_ENABLED", "GW_MYSQL_MAX_OPEN_CONNS", "GW_MYSQL_MAX_IDLE_CONNS", "GW_REDIS_POOL_SIZE"} {
				t.Setenv(name, "")
			}
			t.Setenv(item.name, item.value)
			if _, err := OpenRuntime(context.Background(), false); err == nil || !strings.Contains(err.Error(), item.name) {
				t.Fatalf("expected actionable validation before opening dependencies: %v", err)
			}
		})
	}
}

func TestLitePoolsAndCacheDefaults(t *testing.T) {
	t.Setenv("GW_CACHE_ENABLED", "")
	t.Setenv("GW_MYSQL_MAX_OPEN_CONNS", "")
	t.Setenv("GW_MYSQL_MAX_IDLE_CONNS", "")
	t.Setenv("GW_REDIS_POOL_SIZE", "")
	if enabled, err := semanticCacheEnabled(); err != nil || !enabled {
		t.Fatal("full deployment cache default changed", enabled, err)
	}
	open, idle, err := mysqlPoolOptions()
	if err != nil || open != 30 || idle != 10 {
		t.Fatal("full deployment SQL pool default changed", open, idle, err)
	}
	t.Setenv("GW_MYSQL_MAX_OPEN_CONNS", "12")
	t.Setenv("GW_MYSQL_MAX_IDLE_CONNS", "2")
	t.Setenv("GW_REDIS_POOL_SIZE", "8")
	open, idle, err = mysqlPoolOptions()
	if err != nil || open != 12 || idle != 2 {
		t.Fatal(open, idle, err)
	}
	quota, err := quotaRedisOptions("redis://localhost:6379/0")
	if err != nil || quota.PoolSize != 8 || quota.MaxActiveConns != 8 || !quota.ContextTimeoutEnabled {
		t.Fatal(quota, err)
	}
	if _, _, err = isolatedRedisOptions("redis://localhost:6379/0", "redis://localhost:6379/1"); err == nil {
		t.Fatal("lower pool size must not weaken cache/quota isolation")
	}
	t.Setenv("GW_CACHE_ENABLED", "false")
	t.Setenv("GW_ADMIN_PASSWORD", "lite-test-password")
	r, err := OpenRuntime(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Engine.Cache != nil || !r.Engine.CacheDisabled {
		t.Fatal("disabled runtime allocated a cache")
	}
}

func TestGlobalCacheDisabledSkipsExistingPolicyButPreservesEmbedding(t *testing.T) {
	var embeddingCalls atomic.Int32
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/embeddings" {
			embeddingCalls.Add(1)
		}
		MockProvider(w, r)
	})
	_, err := e.Store.Update(context.Background(), "test", "cache", func(c *Config) error {
		a := c.Apps["app"]
		a.Cache = true
		c.Apps[a.ID] = a
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Refresh(context.Background())
	e.CacheDisabled, e.Cache = true, nil
	for _, stream := range []bool{false, true} {
		w := call(t, e, "same question", stream)
		if w.Code != 200 || w.Header().Get("X-Gateway-Cache") == "hit" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if embeddingCalls.Load() != 0 {
		t.Fatal("disabled cache still called the embedding provider")
	}
	r := httptest.NewRequest("POST", "/v1/embeddings", strings.NewReader(`{"model":"embedding","input":"hello"}`))
	r.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	e.Handler().ServeHTTP(w, r)
	if w.Code != 200 || embeddingCalls.Load() != 1 {
		t.Fatal("direct embedding API disabled", w.Code, w.Body.String())
	}
	cfg, _ := e.Store.Load(context.Background())
	if !cfg.Apps["app"].Cache {
		t.Fatal("existing cache policy was overwritten")
	}
	e.CacheDisabled, e.Cache = false, NewMemoryCache()
	_ = call(t, e, "same question", false)
	if w := call(t, e, "same question", false); w.Header().Get("X-Gateway-Cache") != "hit" {
		t.Fatal("cache policy did not resume after re-enabling", w.Body.String())
	}
}

func TestCacheCapabilityAndManagementGate(t *testing.T) {
	e := testEngine(t, MockProvider)
	e.CacheDisabled = true
	a := NewAdmin(e, false, "")
	cookie, csrf := adminLogin(t, a, "admin")
	w := adminCall(a, cookie, csrf, "GET", "/admin/config", "")
	var view struct {
		Config
		Capabilities map[string]bool `json:"runtime_capabilities"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil || w.Code != 200 || view.Capabilities["cache_enabled"] {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	app := view.Apps["app"]
	app.Cache = true
	body, _ := json.Marshal(app)
	w = adminCall(a, cookie, csrf, "PUT", "/admin/apps/app", string(body))
	if w.Code != 400 || !strings.Contains(w.Body.String(), "cache is disabled") {
		t.Fatal("new cache enable accepted", w.Code, w.Body.String())
	}
	_, _ = e.Store.Update(context.Background(), "test", "existing-cache", func(c *Config) error {
		c.Apps["app"] = app
		return nil
	})
	app.Name = "edited while cache disabled"
	body, _ = json.Marshal(app)
	if w = adminCall(a, cookie, csrf, "PUT", "/admin/apps/app", string(body)); w.Code != 200 {
		t.Fatal("existing policy cannot be preserved during edit", w.Code, w.Body.String())
	}
	w = adminCall(a, cookie, csrf, "GET", "/admin/config", "")
	if b, _ := io.ReadAll(w.Result().Body); !strings.Contains(string(b), "edited while cache disabled") {
		t.Fatal("configuration shape changed")
	}
}

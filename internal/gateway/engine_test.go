package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testEngine(t *testing.T, upstream http.HandlerFunc) *Engine {
	t.Helper()
	server := httptest.NewServer(upstream)
	t.Cleanup(server.Close)
	s := NewMemoryStore()
	g := NewMemoryGuard()
	v, _ := NewVault([]byte(strings.Repeat("k", 32)))
	e := &Engine{Store: s, Guard: g, Bus: LocalBus{s, g}, Cache: NewMemoryCache(), Publisher: LocalPublisher{s}, Vault: v, Client: NewHTTPClient(true), Dev: true, InstanceID: "test", Metrics: NewMetrics()}
	_, err := s.Update(context.Background(), "test", "seed", func(c *Config) error {
		h, err := Password("test-password-123")
		if err != nil {
			return err
		}
		c.Users["admin"] = User{"admin", h, "admin"}
		c.Users["dev"] = User{"dev", h, "developer"}
		for _, id := range []string{"primary", "backup", "embedding"} {
			caps := []string{"chat", "stream", "tools", "json_object", "json_schema"}
			max := int64(2000)
			dim := 0
			if id == "embedding" {
				caps = []string{"embedding"}
				max = 0
				dim = 32
			}
			c.Providers[id] = Provider{id, "mock", server.URL + "/v1", v.Encrypt(id, "test")}
			c.Models[id] = Model{ID: id, ProviderID: id, Upstream: "mock-" + id, Version: "v1", Capabilities: caps, Context: 16000, MaxOutput: max, Dimensions: dim, InputMicro: 1000000, OutputMicro: 2000000, PriceVersion: "v1", Currency: "CNY", FXPPM: 1000000}
		}
		c.Routes["chat"] = Route{Alias: "chat", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{"primary", 100}, {"backup", 0}}}
		c.Routes["embedding"] = Route{Alias: "embedding", Kind: "embedding", Strategy: "priority", Candidates: []Candidate{{"embedding", 100}}}
		c.Apps["app"] = App{ID: "app", Name: "test", Active: true, Owner: "dev", Models: []string{"chat", "embedding"}, BudgetAction: "warn", Similarity: .97, CacheTTL: 300, EmbeddingModel: "embedding"}
		c.Apps["other"] = App{ID: "other", Active: true, Owner: "admin", Models: []string{"chat"}, BudgetAction: "warn"}
		c.Keys["key"] = Key{ID: "key", AppID: "app", Hash: Hash("test-key"), State: "active"}
		c.Keys["other-key"] = Key{ID: "other-key", AppID: "other", Hash: Hash("other-key"), State: "active"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go e.RunRefresh(ctx)
	return e
}
func call(t *testing.T, e *Engine, text string, stream bool) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"model": "chat", "messages": []any{map[string]string{"role": "user", "content": text}}, "stream": stream, "stream_options": map[string]bool{"include_usage": true}})
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(b)))
	r.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	e.Handler().ServeHTTP(w, r)
	return w
}
func TestFallbackHasSeparateAttempts(t *testing.T) {
	e := testEngine(t, MockProvider)
	w := call(t, e, "[fail-primary] hello", false)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	events, _ := e.Store.Records(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
	ids := map[string]bool{}
	for _, x := range events {
		if x.Purpose == "inference" {
			ids[x.AttemptID] = true
		}
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 attempts: %+v", events)
	}
	c, _ := e.Guard.Counters(context.Background(), "app", month(time.Now()))
	// 主模型失败的费用未知，回退成功不应释放上一 attempt 的暴露额度。
	if c.ReservedTokens <= 0 || c.ReservedMicro <= 0 || c.Tokens <= 0 {
		t.Fatal(c)
	}
}
func TestStreamFailureNeverFallsBack(t *testing.T) {
	var backup atomic.Int32
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
		b := new(strings.Builder)
		_, _ = io.Copy(b, r.Body)
		if strings.Contains(b.String(), "mock-backup") {
			backup.Add(1)
		}
		r.Body = ioBody(b.String())
		MockProvider(w, r)
	})
	w := call(t, e, "[break-stream]", true)
	if !strings.Contains(w.Body.String(), "stream_interrupted") || strings.Contains(w.Body.String(), "[DONE]") || backup.Load() != 0 {
		t.Fatalf("mixed or falsely completed stream: %s", w.Body.String())
	}
}
func ioBody(s string) *stringBody { return &stringBody{strings.NewReader(s)} }

type stringBody struct{ *strings.Reader }

func (*stringBody) Close() error { return nil }
func TestStreamReportsUsageAndCompletes(t *testing.T) {
	e := testEngine(t, MockProvider)
	w := call(t, e, "hello", true)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"total_tokens":32`) || !strings.HasSuffix(w.Body.String(), "data: [DONE]\n\n") {
		t.Fatal(w.Body.String())
	}
}

type failedBus struct {
	Delegate   Bus
	Completion bool
}

func (b failedBus) Publish(c context.Context, e Event) error {
	if !b.Completion || e.Kind == "completed" {
		return errors.New("injected queue failure")
	}
	return b.Delegate.Publish(c, e)
}
func (b failedBus) Close() error { return nil }
func TestMeteringFailureDoesNotCallProvider(t *testing.T) {
	var calls atomic.Int32
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); MockProvider(w, r) })
	e.Bus = failedBus{e.Bus, false}
	w := call(t, e, "hello", false)
	if w.Code != 503 || calls.Load() != 0 {
		t.Fatalf("unmetered call: %d %d", w.Code, calls.Load())
	}
}
func TestStreamNoDoneBeforeDurableCompletion(t *testing.T) {
	e := testEngine(t, MockProvider)
	e.Bus = failedBus{e.Bus, true}
	w := call(t, e, "hello", true)
	if strings.Contains(w.Body.String(), "[DONE]") || !strings.Contains(w.Body.String(), "metering_unavailable") {
		t.Fatal(w.Body.String())
	}
}
func TestCacheIsolationAndRealtimeBypass(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, _ = e.Store.Update(context.Background(), "test", "enable-cache", func(c *Config) error { a := c.Apps["app"]; a.Cache = true; c.Apps[a.ID] = a; return nil })
	_ = e.Refresh(context.Background())
	w := call(t, e, "same question", false)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w = call(t, e, "same question", false)
	if w.Header().Get("X-Gateway-Cache") != "hit" {
		t.Fatal("cache miss", w.Body.String())
	}
	_, _ = e.Store.Update(context.Background(), "test", "model-upgrade", func(c *Config) error { m := c.Models["primary"]; m.Version = "v2"; c.Models[m.ID] = m; return nil })
	_ = e.Refresh(context.Background())
	w = call(t, e, "same question", false)
	if w.Header().Get("X-Gateway-Cache") == "hit" {
		t.Fatal("stale model response reused")
	}
	w = call(t, e, "other question", false)
	if w.Header().Get("X-Gateway-Cache") == "hit" {
		t.Fatal("unrelated question hit")
	}
}
func TestRevokedKeyAndAppPermissions(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, _ = e.Store.Update(context.Background(), "test", "revoke", func(c *Config) error { k := c.Keys["key"]; k.State = "revoked"; c.Keys[k.ID] = k; return nil })
	_ = e.Refresh(context.Background())
	if w := call(t, e, "hello", false); w.Code != 401 {
		t.Fatal(w.Code)
	}
}
func TestEmbeddingIncompatibleRouteRejected(t *testing.T) {
	c := EmptyConfig()
	c.Models["a"] = Model{ID: "a", Upstream: "a", Version: "1", Dimensions: 32, Capabilities: []string{"embedding"}}
	c.Models["b"] = Model{ID: "b", Upstream: "b", Version: "1", Dimensions: 32, Capabilities: []string{"embedding"}}
	if ValidateRoute(c, Route{Alias: "emb", Kind: "embedding", Strategy: "priority", Candidates: []Candidate{{"a", 50}, {"b", 50}}}) == nil {
		t.Fatal("different vector spaces accepted")
	}
}
func TestGrayAutomaticallyRollsBack(t *testing.T) {
	e := testEngine(t, MockProvider)
	cfg, _ := e.Store.Update(context.Background(), "test", "gray", func(c *Config) error {
		r := c.Routes["chat"]
		r.Gray = Gray{Percent: 100, Candidate: "backup", State: "running", MinSamples: 10, MaxError: .1, MaxLatencyMS: 1000}
		c.Routes[r.Alias] = r
		return nil
	})
	for i := 0; i < 10; i++ {
		_ = e.Store.Apply(context.Background(), Event{ID: ID("evt_"), RequestID: ID("req_"), AttemptID: ID("att_"), Revision: 2, Kind: "completed", Purpose: "inference", Alias: "chat", Model: cfg.Models["backup"], Gray: true, Status: "failed", At: time.Now()})
	}
	if err := RollbackGray(context.Background(), e, cfg); err != nil {
		t.Fatal(err)
	}
	cfg, _ = e.Store.Load(context.Background())
	if cfg.Routes["chat"].Gray.State != "rolled_back" {
		t.Fatal(cfg.Routes["chat"])
	}
}

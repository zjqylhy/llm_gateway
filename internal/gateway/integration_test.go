package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
)

// 验收复用真实依赖，但数据库、消息、配置、向量和 Redis 均与运行中的应用隔离。
// 只有测试客户端允许访问可控本机上游，不改变生产传输的 SSRF 限制。
func TestInfrastructureIntegration(t *testing.T) {
	if os.Getenv("GW_RUN_INFRA_TESTS") != "1" {
		t.Skip("run scripts/verify-infrastructure.ps1 with the local dependency stack")
	}
	run := os.Getenv("GW_INFRA_RUN_ID")
	if !regexp.MustCompile(`^gw_acceptance_[a-f0-9]{12}$`).MatchString(run) {
		t.Fatal("dedicated acceptance run ID required")
	}
	dsn, err := mysql.ParseDSN(os.Getenv("GW_MYSQL_DSN"))
	if err != nil || dsn.DBName != run || os.Getenv("GW_KAFKA_TOPIC") != run || os.Getenv("GW_MILVUS_COLLECTION") != run {
		t.Fatal("database, topic and collection must match the dedicated run ID")
	}
	if os.Getenv("GW_KAFKA_GROUP") != run || os.Getenv("GW_ETCD_PREFIX") != "/"+run {
		t.Fatal("consumer group and configuration prefix must match the dedicated run ID")
	}
	for _, name := range []string{"GW_MYSQL_DSN", "GW_REDIS_URL", "GW_KAFKA_BROKERS", "GW_ETCD_ENDPOINTS", "GW_MILVUS_URL"} {
		if !strings.Contains(os.Getenv(name), "127.0.0.1") {
			t.Fatalf("%s must point to local services", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	r, err := OpenRuntime(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if size, err := r.Redis.DBSize(ctx).Result(); err != nil || size != 0 {
		t.Fatal("acceptance requires a new, empty Redis instance", err)
	}
	s := r.Engine.Store.(*SQLStore)
	publisher := r.Engine.Publisher.(EtcdPublisher)
	if r.Reader != nil {
		t.Fatal("runtime without metering must not join the consumer group")
	}
	readerConfig := r.OpenMeteringReader().Config()
	defer func() { _ = r.Reader.Close() }()
	cache := r.Engine.Cache.(MilvusCache)
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, err := publisher.Client.Delete(cleanup, publisher.Prefix+"/config"); err != nil {
			t.Errorf("acceptance config cleanup: %v", err)
		}
		if err := cache.call(cleanup, "collections/drop", map[string]any{"collectionName": run}, nil); err != nil {
			t.Errorf("acceptance collection cleanup: %v", err)
		}
	}()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = Bootstrap(ctx, r.Engine); err != nil {
		t.Fatal(err)
	}
	if err = cache.Init(ctx, 32); err != nil {
		t.Fatal("cache initialization", err)
	}
	var upstreamCalls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		upstreamCalls.Add(1)
		MockProvider(w, req)
	}))
	defer upstream.Close()
	e := r.Engine
	e.Client = NewHTTPClient(true)
	checkT := t
	update := func(action string, fn func(*Config) error) Config {
		checkT.Helper()
		cfg, err := s.Update(ctx, "acceptance", action, fn)
		if err != nil {
			checkT.Fatal(err)
		}
		if err = e.Publisher.PublishConfig(ctx, cfg); err != nil {
			checkT.Fatal(err)
		}
		return cfg
	}
	update("acceptance.seed", func(c *Config) error {
		for _, id := range []string{"primary", "backup", "embedding"} {
			caps := []string{"chat", "stream", "tools", "json_object", "json_schema"}
			dim := 0
			if id == "embedding" {
				caps, dim = []string{"embedding"}, 32
			}
			c.Providers[id] = Provider{id, "mock", upstream.URL + "/v1", e.Vault.Encrypt(id, "acceptance")}
			c.Models[id] = Model{ID: id, ProviderID: id, Upstream: "mock-" + id, Version: "v1", Capabilities: caps, Dimensions: dim, Context: 16000, MaxOutput: 128, InputMicro: 1000000, OutputMicro: 2000000, PriceVersion: "acceptance-v1", Currency: "CNY", FXPPM: 1000000}
		}
		c.Routes["chat"] = Route{Alias: "chat", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{"primary", 100}, {"backup", 0}}}
		c.Routes["embedding"] = Route{Alias: "embedding", Kind: "embedding", Strategy: "priority", Candidates: []Candidate{{"embedding", 100}}}
		for _, id := range []string{"app", "cache-a", "cache-b", "budget", "integration"} {
			a := App{ID: id, Active: true, Owner: "admin", Models: []string{"chat", "embedding"}, BudgetAction: "warn"}
			if strings.HasPrefix(id, "cache-") {
				a.Cache, a.CacheTTL, a.Similarity, a.EmbeddingModel = true, 300, .97, "embedding"
			}
			if id == "budget" {
				a.BudgetAction, a.BudgetMicro = "reject", 1
			}
			c.Apps[id] = a
			c.Keys[id] = Key{ID: id, AppID: id, Hash: Hash(id + "-key"), State: "active"}
		}
		return nil
	})
	var workerCancel context.CancelFunc
	var workerDone chan error
	startWorker := func() {
		workerCtx, stop := context.WithCancel(ctx)
		workerCancel, workerDone = stop, make(chan error, 1)
		go func() { workerDone <- Consume(workerCtx, r.Reader, s, e.Guard) }()
	}
	stopWorker := func() {
		checkT.Helper()
		if workerCancel == nil {
			return
		}
		workerCancel()
		if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
			checkT.Fatal("worker failed", err)
		}
		workerCancel = nil
		_ = r.Reader.Close()
		r.Reader = kafka.NewReader(readerConfig)
	}
	startWorker()
	defer func() {
		if workerCancel != nil {
			workerCancel()
			<-workerDone
		}
	}()
	wait := func(label string, check func() bool) {
		checkT.Helper()
		deadline := time.Now().Add(35 * time.Second)
		for !check() {
			if workerCancel != nil {
				select {
				case err := <-workerDone:
					workerCancel = nil
					checkT.Fatal("worker stopped", err)
				default:
				}
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				checkT.Fatal("timeout: " + label)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	drain := func() {
		checkT.Helper()
		wait("Kafka committed through topic end", func() bool {
			ok, err := KafkaCaughtUp(ctx, r)
			return err == nil && ok
		})
	}
	publish := func(event Event) {
		checkT.Helper()
		if err := e.Bus.Publish(ctx, event); err != nil {
			checkT.Fatal(err)
		}
	}
	request := func(app, path string, body any) *httptest.ResponseRecorder {
		checkT.Helper()
		if err := e.Refresh(ctx); err != nil {
			checkT.Fatal(err)
		}
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", path, strings.NewReader(string(raw))).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+app+"-key")
		w := httptest.NewRecorder()
		e.Handler().ServeHTTP(w, req)
		return w
	}
	chat := func(app, text string, stream bool) *httptest.ResponseRecorder {
		return request(app, "/v1/chat/completions", map[string]any{"model": "chat", "messages": []any{map[string]string{"role": "user", "content": text}}, "max_tokens": 64, "stream": stream, "stream_options": map[string]bool{"include_usage": true}})
	}
	records := func(w *httptest.ResponseRecorder) []Event {
		checkT.Helper()
		drain()
		rows, err := s.Records(ctx, Filter{RequestID: w.Header().Get("X-Request-Id")})
		if err != nil {
			checkT.Fatal(err)
		}
		return rows
	}
	reconcile := func(app string) Counters {
		checkT.Helper()
		var tokens, micro int64
		if err := s.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(tokens),0),COALESCE(SUM(cost_micro),0) FROM gw_attempts WHERE app_id=? AND purpose<>'request'", app).Scan(&tokens, &micro); err != nil {
			checkT.Fatal(err)
		}
		counts, err := e.Guard.Counters(ctx, app, month(time.Now()))
		if err != nil || counts.Tokens != tokens || counts.Micro != micro || counts.ReservedTokens != 0 || counts.ReservedMicro != 0 {
			checkT.Fatalf("SQL/Redis mismatch for %s: SQL=%d/%d Redis=%+v error=%v", app, tokens, micro, counts, err)
		}
		return counts
	}

	runCase := func(name string, fn func(*testing.T)) {
		if !t.Run(name, func(sub *testing.T) {
			checkT = sub
			defer func() { checkT = t }()
			fn(sub)
		}) {
			t.FailNow()
		}
	}
	runCase("configuration_CAS", func(t *testing.T) {
		cfg, err := s.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		old := cfg
		old.Version--
		if err = publisher.PublishConfig(ctx, old); err != nil {
			t.Fatal(err)
		}
		actual, err := publisher.Current(ctx)
		if err != nil || actual.Version != cfg.Version {
			t.Fatal("configuration regressed", err)
		}
	})
	event := Event{ID: ID("event_"), RequestID: ID("request_"), AttemptID: ID("attempt_"), Revision: 1, Kind: "started", Purpose: "inference", AppID: "integration", At: time.Now().UTC(), Month: month(time.Now()), Status: "started", ReserveTokens: 100, ReserveMicro: 100, Model: Model{ID: "integration"}}
	runCase("duplicate_out_of_order_and_correction", func(t *testing.T) {
		if err := e.Guard.Reserve(ctx, App{ID: event.AppID}, Reservation{ID: event.AttemptID, AppID: event.AppID, Month: event.Month, Tokens: 100, Micro: 100}); err != nil {
			t.Fatal(err)
		}
		publish(event)
		started := event
		event.ID, event.Revision, event.Kind, event.Status, event.Quality = ID("event_"), 2, "completed", "success", "reported"
		event.Usage, event.CostMicro = Usage{Input: 10, Output: 20, Total: 30}, 40
		for i := 0; i < 3; i++ {
			publish(event)
		}
		publish(started)
		event.ID, event.Revision, event.Usage, event.CostMicro = ID("event_"), 3, Usage{Input: 20, Output: 30, Total: 50}, 70
		publish(event)
		publish(event)
		drain()
		counts := reconcile("integration")
		if counts.Tokens != 50 || counts.Micro != 70 {
			t.Fatal("correction was not applied exactly once", counts)
		}
		reports, err := s.Report(ctx, Filter{RequestID: event.RequestID})
		if err != nil || len(reports) != 1 || reports[0].Tokens != 50 || reports[0].Micro != 70 {
			t.Fatal("duplicate ledger posting", reports, err)
		}
	})
	runCase("protocol_fallback_and_stream_interrupt", func(t *testing.T) {
		w := chat("app", "[fail-primary] acceptance", false)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		attempts, success, failed := 0, 0, 0
		for _, row := range records(w) {
			if row.Purpose == "inference" {
				attempts++
				if row.Status == "success" && row.Usage.Total == 32 && row.CostMicro == 52 && row.Quality == "reported" {
					success++
				}
				if row.Status == "failed" && row.Quality != "reported" && row.Usage.Total > 0 {
					failed++
				}
			}
		}
		if attempts != 2 || success != 1 || failed != 1 {
			t.Fatal("fallback billing not separated", attempts, success, failed)
		}
		w = chat("app", "[break-stream]", true)
		if !strings.Contains(w.Body.String(), "stream_interrupted") || strings.Contains(w.Body.String(), "[DONE]") {
			t.Fatal(w.Body.String())
		}
		attempts = 0
		for _, row := range records(w) {
			if row.Purpose == "inference" {
				attempts++
			}
		}
		if attempts != 1 {
			t.Fatal("interrupted stream fell back", attempts)
		}
		w = chat("app", "stream acceptance", true)
		if w.Code != 200 || !strings.HasSuffix(w.Body.String(), "data: [DONE]\n\n") || !strings.Contains(w.Body.String(), `"total_tokens":32`) {
			t.Fatal(w.Code, w.Body.String())
		}
		w = request("app", "/v1/embeddings", map[string]any{"model": "embedding", "input": []string{"first", "second"}})
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"index":1`) {
			t.Fatal(w.Code, w.Body.String())
		}
		drain()
		reconcile("app")
	})
	runCase("semantic_cache_isolation_and_billing", func(t *testing.T) {
		w := chat("cache-a", "same acceptance question", false)
		if w.Code != 200 || w.Header().Get("X-Gateway-Cache") == "hit" {
			t.Fatal("first call unexpectedly cached", w.Code, w.Body.String())
		}
		wait("Milvus cache searchable", func() bool {
			w = chat("cache-a", "same acceptance question", false)
			return w.Code == 200 && w.Header().Get("X-Gateway-Cache") == "hit"
		})
		embeddings, inference := 0, 0
		for _, row := range records(w) {
			if row.Purpose == "cache_embedding" && row.Usage.Total == 8 {
				embeddings++
			}
			if row.Purpose == "inference" {
				inference++
			}
		}
		if embeddings != 1 || inference != 0 {
			t.Fatal("cache hit billing incorrect", embeddings, inference)
		}
		w = chat("cache-b", "same acceptance question", false)
		if w.Code != 200 || w.Header().Get("X-Gateway-Cache") == "hit" {
			t.Fatal("cache crossed application boundary", w.Code)
		}
		update("acceptance.model-upgrade", func(c *Config) error {
			m := c.Models["primary"]
			m.Version = "v2"
			c.Models[m.ID] = m
			return nil
		})
		w = chat("cache-a", "same acceptance question", false)
		if w.Code != 200 || w.Header().Get("X-Gateway-Cache") == "hit" {
			t.Fatal("cache reused outdated model version", w.Code)
		}
		drain()
		reconcile("cache-a")
		reconcile("cache-b")
	})
	runCase("budget_rejects_before_upstream", func(t *testing.T) {
		before := upstreamCalls.Load()
		w := chat("budget", "budget acceptance", false)
		if w.Code != 429 || upstreamCalls.Load() != before {
			t.Fatal("over-budget request called upstream", w.Code, w.Body.String())
		}
		drain()
		reconcile("budget")
	})
	runCase("worker_restart_replays_backlog", func(t *testing.T) {
		drain()
		stopWorker()
		pending := event
		pending.ID, pending.RequestID, pending.AttemptID, pending.Revision = ID("event_"), ID("request_"), ID("attempt_"), 2
		publish(pending)
		publish(pending)
		rows, err := s.Records(ctx, Filter{RequestID: pending.RequestID})
		if err != nil || len(rows) != 0 {
			t.Fatal("stopped worker consumed backlog", rows, err)
		}
		startWorker()
		drain()
		if counts := reconcile("integration"); counts.Tokens != 100 || counts.Micro != 140 {
			t.Fatal("backlog replay was not idempotent", counts)
		}
	})
	runCase("lost_quota_fails_closed_and_rebuild_drains_Kafka", func(t *testing.T) {
		drain()
		stopWorker()
		if err := r.Redis.Del(ctx, "gw:quota:state").Err(); err != nil {
			t.Fatal(err)
		}
		before := upstreamCalls.Load()
		w := chat("app", "quota unavailable", false)
		if w.Code != 503 || upstreamCalls.Load() != before {
			t.Fatal("uninitialized quota allowed a paid call", w.Code, w.Body.String())
		}
		if err := Bootstrap(ctx, e); err == nil {
			t.Fatal("bootstrap silently reset existing quota")
		}
		// 专用临时 Redis 完整丢失；保留 SQL 账本，并制造尚未落库的持久化消息。
		if err := r.Redis.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		pending := event
		pending.ID, pending.RequestID, pending.AttemptID, pending.Revision = ID("event_"), ID("request_"), ID("attempt_"), 2
		publish(pending)
		publish(pending)
		t.Setenv("GW_QUOTA_REBUILD_ACK", "writers-stopped")
		if err := RebuildQuota(ctx, r); err != nil {
			t.Fatal("quota recovery failed", err)
		}
		for _, app := range []string{"app", "cache-a", "cache-b", "budget", "integration"} {
			reconcile(app)
		}
		if counts := reconcile("integration"); counts.Tokens != 150 || counts.Micro != 210 {
			t.Fatal("pending Kafka billing lost during recovery", counts)
		}
		if caught, err := KafkaCaughtUp(ctx, r); err != nil || !caught {
			t.Fatal("quota reopened before messages drained", err)
		}
		if state, err := r.Redis.Get(ctx, "gw:quota:state").Result(); err != nil || state != "ready" {
			t.Fatal("quota did not become ready", state, err)
		}
		startWorker()
		w = chat("app", "after recovery", false)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		drain()
		reconcile("app")
	})
	t.Logf("isolated acceptance completed: %s, upstream calls=%d", run, upstreamCalls.Load())
}

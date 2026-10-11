package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	for _, name := range []string{"GW_MYSQL_DSN", "GW_REDIS_URL", "GW_CACHE_REDIS_URL", "GW_KAFKA_BROKERS", "GW_ETCD_ENDPOINTS", "GW_MILVUS_URL"} {
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

	cache := r.Engine.Cache.(MilvusCache)
	if cache.Redis.Options().Addr == r.Redis.Options().Addr {
		t.Fatal("acceptance cache and quota must use separate Redis instances")
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, err := publisher.Client.Delete(cleanup, publisher.Prefix+"/", clientv3.WithPrefix()); err != nil {
			t.Errorf("acceptance config cleanup: %v", err)
		}
		if err := cache.call(cleanup, "collections/drop", map[string]any{"collectionName": run}, nil); err != nil {
			t.Errorf("acceptance collection cleanup: %v", err)
		}
	}()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = InitializeCheckpoints(ctx, r, false); err != nil {
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
	// 手工事件属于重放 fixture，真实 HTTP 请求仍经过完整许可与入口登记。
	fixtureGuard := RedisGuard{Client: r.Redis, RequireReady: true, EnforceMemory: true, Replay: true}
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
	if _, _, err := r.nextAuditApp(ctx, ""); err != nil {
		t.Fatal("initialize empty application controls", err)
	}
	var workerDone chan error
	startWorker := func() {
		workerCtx, stop := context.WithCancel(ctx)
		workerCancel, workerDone = stop, make(chan error, 1)
		go func() { workerDone <- r.ConsumeMetering(workerCtx) }()
	}
	stopWorker := func() {
		checkT.Helper()
		if workerCancel == nil {
			return
		}
		workerCancel()
		workerErr := <-workerDone
		workerCancel = nil
		if err := workerErr; err != nil && !errors.Is(err, context.Canceled) {
			checkT.Fatal("worker failed", err)
		}

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
		_, _, _ = r.nextAuditApp(ctx, "")
		// 旧用例会主动损坏依赖；不能在辅助方法里修复其控制状态。
		_ = r.renewPermit(ctx)
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
		if err := fixtureGuard.Reserve(ctx, App{ID: event.AppID}, Reservation{ID: event.AttemptID, AppID: event.AppID, Month: event.Month, Tokens: 100, Micro: 100}); err != nil {
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
	runCase("metering_probe_SQL_pool_timeout_is_not_checkpoint_corruption", func(t *testing.T) {
		drain()
		pool := s.DB.Stats().MaxOpenConnections
		s.DB.SetMaxOpenConns(1)
		defer s.DB.SetMaxOpenConns(pool)
		conn, err := s.DB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		before := s.DB.Stats().WaitCount
		probe, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, _, err = r.permitEvidence(probe)
		if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrCheckpoint) || s.DB.Stats().WaitCount <= before {
			t.Fatal("pool pressure misclassified as permanent corruption", err, s.DB.Stats())
		}
	})
	runCase("short_permit_online_barrier_partial_loss_and_explicit_repair", func(t *testing.T) { drain(); testOnlineMetering(t, r, ctx, stopWorker, startWorker); drain() })
	for _, dependency := range []string{"sql", "kafka"} {
		runCase("completion_"+dependency+"_real_connection_loss", func(t *testing.T) {
			drain()
			testCompletionNetworkFailure(t, r, ctx, dependency, stopWorker, startWorker)
			drain()
		})
	}
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
		if err := r.Redis.Set(ctx, "gw:{app:rebuild-orphan}:attempt:orphan", "corrupt", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if err := r.Redis.Set(ctx, "acceptance:preserve", "unrelated", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if err := RebuildQuota(ctx, r); err != nil {
			t.Fatal("quota recovery failed", err)
		}
		if n, err := r.Redis.Exists(ctx, "gw:{app:rebuild-orphan}:attempt:orphan").Result(); err != nil || n != 0 {
			t.Fatal("rebuild retained orphan quota key", n, err)
		}
		if v, err := r.Redis.Get(ctx, "acceptance:preserve").Result(); err != nil || v != "unrelated" {
			t.Fatal("rebuild removed unrelated key", v, err)
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
		t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
		if _, err := ResumeMetering(ctx, r); err != nil {
			t.Fatal("audited quota resume", err)
		}
		e.Bus.(*DurableBus).stopped.Store(false)
		startWorker()
		w = chat("app", "after recovery", false)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		drain()
		reconcile("app")
	})
	runCase("bounded_configuration_and_report", func(t *testing.T) {
		before, err := s.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Update(ctx, "acceptance", "oversized", func(c *Config) error {
			c.Apps["large"] = App{ID: "large", Name: strings.Repeat("x", maxConfigBytes)}
			return nil
		})
		if err == nil {
			t.Fatal("oversized SQL configuration accepted")
		}
		after, err := s.Load(ctx)
		if err != nil || after.Version != before.Version || len(after.Apps) != len(before.Apps) {
			t.Fatal("failed update changed SQL configuration", err)
		}
		defer func() {
			_, err := s.DB.ExecContext(ctx, `DELETE FROM gw_attempts WHERE model_id IN ('acceptance-capacity','acceptance-record-capacity')`)
			if err != nil {
				t.Error(err)
			}
		}()
		for base := 0; base < 10001; base += 250 {
			values := []string{}
			args := []any{}
			for i := base; i < base+250 && i < 10001; i++ {
				id := fmt.Sprintf("capacity-%d", i)
				values = append(values, `(?,?,?,'capacity','acceptance-capacity','success','inference',2,1,1,?,JSON_OBJECT('quality','reported'))`)
				args = append(args, id, id, id, time.Now().UTC())
			}
			if _, err := s.DB.ExecContext(ctx, `INSERT INTO gw_attempts(attempt_id,request_id,app_id,department,model_id,status,purpose,revision,tokens,cost_micro,at,payload) VALUES `+strings.Join(values, ","), args...); err != nil {
				t.Fatal(err)
			}
		}
		if rows, err := s.Report(ctx, Filter{ModelID: "acceptance-capacity"}); !errors.Is(err, ErrReportCapacity) || rows != nil {
			t.Fatal("oversized report silently truncated or retained", len(rows), err)
		}
		if rows, err := s.Report(ctx, Filter{AppID: "capacity-0"}); err != nil || len(rows) != 1 || rows[0].Tokens != 1 {
			t.Fatal("filtered report unavailable", rows, err)
		}
		_, err = s.DB.ExecContext(ctx, `INSERT INTO gw_attempts(attempt_id,request_id,app_id,department,model_id,status,purpose,revision,tokens,cost_micro,at,payload) SELECT CONCAT('large-',attempt_id),request_id,app_id,department,'acceptance-record-capacity',status,purpose,revision,tokens,cost_micro,at,JSON_OBJECT('payload_ciphertext',REPEAT('x',900*1024)) FROM gw_attempts WHERE model_id='acceptance-capacity' LIMIT 20`)
		if err != nil {
			t.Fatal(err)
		}
		if rows, err := s.Records(ctx, Filter{ModelID: "acceptance-record-capacity", Limit: 20}); !errors.Is(err, ErrRecordCapacity) || rows != nil {
			t.Fatal("large record page was not bounded", len(rows), err)
		}
		if rows, err := s.Records(ctx, Filter{ModelID: "acceptance-record-capacity", Limit: 1}); err != nil || len(rows) != 1 {
			t.Fatal("small record page unavailable", len(rows), err)
		}
	})
	runCase("expired_vector_cleanup", func(t *testing.T) {
		vector := make([]float32, 32)
		vector[0] = 1
		if err := cache.call(ctx, "entities/upsert", map[string]any{"collectionName": run, "data": []map[string]any{{"id": "expired-acceptance", "vector": vector, "scope": "expired", "expire": time.Now().Add(-time.Hour).Unix()}}}, nil); err != nil {
			t.Fatal(err)
		}
		query := func() int {
			var out struct {
				Data []map[string]any `json:"data"`
			}
			if err := cache.call(ctx, "entities/query", map[string]any{"collectionName": run, "filter": `id == "expired-acceptance"`, "outputFields": []string{"id"}, "limit": 1, "consistencyLevel": "Strong"}, &out); err != nil {
				t.Fatal(err)
			}
			return len(out.Data)
		}
		wait("expired entity stored", func() bool { return query() == 1 })
		if err := cache.Prune(ctx); err != nil {
			t.Fatal(err)
		}
		wait("expired entity deleted", func() bool { return query() == 0 })
	})
	runCase("cache_eviction_preserves_quota", func(t *testing.T) {
		drain()
		before, err := e.Guard.Counters(ctx, "app", month(time.Now()))
		if err != nil {
			t.Fatal(err)
		}
		payload := strings.Repeat("x", 64<<10)
		for i := 0; i < 256; i++ {
			if err := cache.Redis.Set(ctx, fmt.Sprintf("acceptance:pressure:%d", i), payload, time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
		}
		stats, err := cache.Redis.Info(ctx, "stats").Result()
		if err != nil {
			t.Fatal(err)
		}
		evicted := false
		for _, line := range strings.Split(stats, "\n") {
			if strings.HasPrefix(line, "evicted_keys:") && strings.TrimSpace(strings.TrimPrefix(line, "evicted_keys:")) != "0" {
				evicted = true
			}
		}
		if !evicted {
			t.Fatal("cache pressure did not trigger real Redis eviction")
		}
		after, err := e.Guard.Counters(ctx, "app", month(time.Now()))
		if err != nil || before != after {
			t.Fatal("cache eviction changed quota", before, after, err)
		}
		if state, err := r.Redis.Get(ctx, "gw:quota:state").Result(); err != nil || state != "ready" {
			t.Fatal("cache eviction lost quota ready state", err)
		}
		if w := chat("app", "after eviction", false); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		drain()
		reconcile("app")
	})
	runCase("quota_watermark_preserves_completion_and_replay", func(t *testing.T) {
		drain()
		app := App{ID: "acceptance-headroom", BudgetAction: "warn"}
		reservation := Reservation{ID: "headroom", AppID: app.ID, Month: month(time.Now()), Tokens: 10, Micro: 10}
		if err := fixtureGuard.Reserve(ctx, app, reservation); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Redis.ConfigSet(ctx, "maxmemory", "8mb").Err(); err != nil {
				t.Error(err)
			}
			if err := r.Redis.ConfigSet(ctx, "maxmemory-policy", "noeviction").Err(); err != nil {
				t.Error(err)
			}
			keys := quotaKeys(app.ID, reservation.Month, reservation.ID)
			if err := r.Redis.Del(ctx, keys...).Err(); err != nil {
				t.Error(err)
			}
		}()
		info, err := r.Redis.Info(ctx, "memory").Result()
		if err != nil {
			t.Fatal(err)
		}
		used := int64(0)
		for _, line := range strings.Split(info, "\n") {
			if strings.HasPrefix(line, "used_memory:") {
				used, err = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "used_memory:")), 10, 64)
			}
		}
		if err != nil || used <= 0 {
			t.Fatal("memory not measured", err)
		}
		if err := r.Redis.ConfigSet(ctx, "maxmemory", strconv.FormatInt(used*105/100, 10)).Err(); err != nil {
			t.Fatal(err)
		}
		if err := fixtureGuard.Reserve(ctx, app, reservation); err != nil {
			t.Fatal("existing reserve replay blocked at watermark", err)
		}
		calls := upstreamCalls.Load()
		if w := chat("app", "watermark", false); w.Code != 503 || upstreamCalls.Load() != calls {
			t.Fatal("watermark allowed paid call", w.Code)
		}
		complete := Event{AttemptID: reservation.ID, AppID: app.ID, Month: reservation.Month, Revision: 2, Usage: Usage{Total: 5}, CostMicro: 7}
		if err := e.Guard.Settle(ctx, complete); err != nil {
			t.Fatal("completion lost headroom", err)
		}
		if counts, err := e.Guard.Counters(ctx, app.ID, reservation.Month); err != nil || counts.Tokens != 5 || counts.Micro != 7 || counts.ReservedTokens != 0 || counts.ReservedMicro != 0 {
			t.Fatal("headroom settlement incorrect", counts, err)
		}
		if err := r.Redis.ConfigSet(ctx, "maxmemory", "8mb").Err(); err != nil {
			t.Fatal(err)
		}
		if err := r.Redis.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Err(); err != nil {
			t.Fatal(err)
		}
		if err := fixtureGuard.Reserve(ctx, app, Reservation{ID: "unsafe-policy", AppID: app.ID, Month: reservation.Month, Tokens: 1}); !errors.Is(err, ErrUnavailable) {
			t.Fatal("unsafe quota eviction policy accepted", err)
		}
	})
	runCase("quota_memory_exhaustion_fails_closed", func(t *testing.T) {
		drain()
		stopWorker()
		before, err := e.Guard.Counters(ctx, "app", month(time.Now()))
		if err != nil {
			t.Fatal(err)
		}
		keys := []string{}
		defer func() {
			if len(keys) > 0 {
				if err := r.Redis.Del(ctx, keys...).Err(); err != nil {
					t.Error("pressure cleanup", err)
				}
			}
			startWorker()
		}()
		exhausted := false
		for i := 0; i < 2048; i++ {
			key := fmt.Sprintf("acceptance:pressure:%d", i)
			keys = append(keys, key)
			if err := r.Redis.Set(ctx, key, strings.Repeat("x", 64<<10), 0).Err(); err != nil {
				if !strings.Contains(err.Error(), "OOM") {
					t.Fatal(err)
				}
				exhausted = true
				break
			}
		}
		if !exhausted {
			t.Fatal("quota Redis did not enforce maxmemory")
		}
		calls := upstreamCalls.Load()
		if w := chat("app", "memory exhausted", false); w.Code != 503 || upstreamCalls.Load() != calls {
			t.Fatal("OOM permitted a paid request", w.Code, w.Body.String())
		}
		after, err := e.Guard.Counters(ctx, "app", month(time.Now()))
		if err != nil || before != after {
			t.Fatal("OOM corrupted quota", before, after, err)
		}
		if err := r.Redis.Ping(ctx).Err(); err != nil {
			t.Fatal("Redis process unavailable after OOM", err)
		}
	})
	runCase("SQL_UTF8_parameter_and_JSON_roundtrip", func(t *testing.T) {
		text := "研发🚀 '引号' \\路径\x00; DROP TABLE gw_config; --"
		cfg, err := s.Update(ctx, "acceptance", "quoted-config", func(c *Config) error {
			app := c.Apps["app"]
			app.Name = text
			c.Apps["app"] = app
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := s.Load(ctx)
		if err != nil || loaded.Apps["app"].Name != text || loaded.Version != cfg.Version {
			t.Fatal("JSON parameter changed after SQL roundtrip", err)
		}
		alert := Alert{ID: "quoted-alert", AppID: "app", Kind: "test", Message: text, At: time.Now().UTC()}
		if err := s.SaveAlert(ctx, alert); err != nil {
			t.Fatal(err)
		}
		alerts, err := s.Alerts(ctx, "app")
		found := false
		for _, a := range alerts {
			if a.ID == alert.ID {
				found = a.Message == text
			}
		}
		if err != nil || !found {
			t.Fatal("alert JSON or parameter escaping changed", err)
		}
	})
	runCase("outbox_capacity_and_consumer_ownership", func(t *testing.T) {
		drain()
		stopWorker()
		defer startWorker()
		base := Event{ID: "outbox-a:1", AttemptID: "outbox-a", RequestID: "outbox-request", AppID: "outbox-capacity", Kind: "started", Revision: 1, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), ReserveTokens: 100, ReserveMicro: 100}
		if err := s.SaveOutbox(ctx, base, 8<<20, 2<<20); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_attempts WHERE attempt_id=?`, base.AttemptID).Scan(&n); err != nil || n != 0 {
			t.Fatal("producer wrote ledger", n, err)
		}
		another := base
		another.ID = "outbox-a2:1"
		another.AttemptID = "outbox-a2"
		if err := s.SaveOutbox(ctx, another, 8<<20, 2<<20); !errors.Is(err, ErrOutboxCapacity) {
			t.Fatal("application capacity not enforced", err)
		}
		other := base
		other.ID = "outbox-b:1"
		other.AttemptID = "outbox-b"
		other.AppID = "outbox-other"
		if err := fixtureGuard.Reserve(ctx, App{ID: other.AppID}, Reservation{other.AttemptID, other.AppID, other.Month, 100, 100}); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveOutbox(ctx, other, 8<<20, 2<<20); err != nil {
			t.Fatal("one application blocked another", err)
		}
		completed := base
		completed.ID = "outbox-a:2"
		completed.Kind = "completed"
		completed.Revision = 2
		completed.Usage = Usage{Input: 10, Output: 20, Total: 30}
		completed.CostMicro = 40
		// 真实持有容量行锁；已预留的完成事件仍须保存，不能被新接单竞争拖住。
		lock, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Rollback()
		if _, err := lock.ExecContext(ctx, `UPDATE gw_metering_capacity SET used_bytes=used_bytes WHERE id='global'`); err != nil {
			t.Fatal(err)
		}
		completionCtx, done := context.WithTimeout(ctx, time.Second)
		err = s.SaveOutbox(completionCtx, completed, 8<<20, 2<<20)
		done()
		if err != nil {
			t.Fatal("reserved completion waited for global capacity lock", err)
		}
		if err := lock.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveOutbox(ctx, completed, 8<<20, 2<<20); err != nil {
			t.Fatal("duplicate completion consumed capacity", err)
		}
		wrongApp := completed
		wrongApp.ID = "wrong-application:2"
		wrongApp.AppID = other.AppID
		if err := s.SaveOutbox(ctx, wrongApp, 8<<20, 2<<20); !errors.Is(err, ErrEventConflict) {
			t.Fatal("completion used another application's reservation", err)
		}
		conflict := completed
		conflict.CostMicro++
		if err := s.SaveOutbox(ctx, conflict, 8<<20, 2<<20); !errors.Is(err, ErrEventConflict) {
			t.Fatal("outbox conflict ignored", err)
		}
		if err := fixtureGuard.Reserve(ctx, App{ID: base.AppID}, Reservation{base.AttemptID, base.AppID, base.Month, 100, 100}); err != nil {
			t.Fatal(err)
		}
		for _, item := range []Event{base, completed, other} {
			if err := s.Apply(ctx, item); err != nil {
				t.Fatal(err)
			}
			if item.Kind == "started" {
				if err := stampReservation(ctx, fixtureGuard, item); err != nil {
					t.Fatal(err)
				}
			}
			if item.Kind == "completed" {
				if err := e.Guard.Settle(ctx, item); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.DeliveredOutbox(ctx, item); err != nil {
				t.Fatal(err)
			}
			if err := s.DeliveredOutbox(ctx, item); err != nil {
				t.Fatal("capacity release not idempotent", err)
			}
		}
		if err := s.Apply(ctx, conflict); !errors.Is(err, ErrEventConflict) {
			t.Fatal("ledger conflict ignored", err)
		}
		if err := s.DB.QueryRowContext(ctx, `SELECT used_bytes FROM gw_metering_capacity WHERE id='app:outbox-capacity'`).Scan(&n); err != nil || n != 0 {
			t.Fatal("capacity leaked", n, err)
		}
		// 未完成 other 仍保留完整 completed 空间，不把 started 消费当成已结算。
		if err := s.DB.QueryRowContext(ctx, `SELECT used_bytes FROM gw_metering_capacity WHERE id='app:outbox-other'`).Scan(&n); err != nil || n != maxEventBytes {
			t.Fatal("pending completion space released", n, err)
		}
	})
	runCase("outbox_restart_and_lost_ACK", func(t *testing.T) {
		drain()
		stopWorker()
		original := e.Bus.(*DurableBus)
		bus := &DurableBus{Store: s, Transport: failedBus{original.Transport, true}, GlobalBytes: 512 << 20, AppBytes: 64 << 20}
		started := Event{ID: "relay-attempt:1", AttemptID: "relay-attempt", RequestID: "relay-request", AppID: "relay-app", Kind: "started", Revision: 1, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), ReserveTokens: 100, ReserveMicro: 100}
		if err := fixtureGuard.Reserve(ctx, App{ID: started.AppID}, Reservation{started.AttemptID, started.AppID, started.Month, 100, 100}); err != nil {
			t.Fatal(err)
		}
		if err := bus.Publish(ctx, started); err != nil {
			t.Fatal(err)
		}
		completed := started
		completed.ID = "relay-attempt:2"
		completed.Kind = "completed"
		completed.Revision = 2
		completed.Usage = Usage{Input: 10, Output: 20, Total: 30}
		completed.CostMicro = 40
		if err := bus.Publish(ctx, completed); err == nil {
			t.Fatal("failed publication reported success")
		}
		restarted := &DurableBus{Store: s, Transport: original.Transport}
		if err := restarted.RelayOnce(ctx); err != nil {
			t.Fatal("persistent compensation failed", err)
		}
		// 模拟 ACK 后、标记发送前崩溃，重新领取同一事件。
		if _, err := s.DB.ExecContext(ctx, `UPDATE gw_metering_outbox SET published=0,next_retry_at=UTC_TIMESTAMP(6),lease_until=NULL WHERE event_id=?`, completed.ID); err != nil {
			t.Fatal(err)
		}
		if err := restarted.RelayOnce(ctx); err != nil {
			t.Fatal(err)
		}
		startWorker()
		drain()
		counts := reconcile(started.AppID)
		if counts.Tokens != 30 || counts.Micro != 40 {
			t.Fatal("relay duplicated cost", counts)
		}
		var charged int64
		if err := s.DB.QueryRowContext(ctx, `SELECT used_bytes FROM gw_metering_capacity WHERE id=?`, "app:"+started.AppID).Scan(&charged); err != nil || charged != 0 {
			t.Fatal("relay capacity leaked", charged, err)
		}
		// 仅测试资源恢复；故障已完整核验，不修改现有部署的控制状态。
		if _, err := s.DB.ExecContext(ctx, `UPDATE gw_metering_control SET state='healthy',reason='' WHERE id=1`); err != nil {
			t.Fatal(err)
		}
	})
	runCase("outbox_correction_atomic_idempotency", func(t *testing.T) {
		rows, err := s.Records(ctx, Filter{RequestID: "relay-request"})
		if err != nil || len(rows) != 1 {
			t.Fatal(rows, err)
		}
		fixed := rows[0]
		fixed.Revision = 3
		fixed.Usage = Usage{Input: 20, Output: 30, Total: 50}
		fixed.CostMicro = 70
		fixed.Status = "reconciled"
		fixed.Quality = "reconciled"
		a, err := s.CorrectOutbox(ctx, "acceptance", "same-operation", fixed, 512<<20, 64<<20)
		if err != nil {
			t.Fatal(err)
		}
		b, err := s.CorrectOutbox(ctx, "acceptance", "same-operation", fixed, 512<<20, 64<<20)
		if err != nil || a.ID != b.ID || a.Revision != b.Revision {
			t.Fatal("correction allocated twice", a, b, err)
		}
		fixed.CostMicro++ // 实际幂等输入以 usage 为准；下行用不同 usage 验证冲突。
		fixed.Usage.Input++
		fixed.Usage.Total++
		if _, err := s.CorrectOutbox(ctx, "acceptance", "same-operation", fixed, 512<<20, 64<<20); !errors.Is(err, ErrEventConflict) {
			t.Fatal("correction key reused", err)
		}
		if err := e.Bus.(*DurableBus).RelayOnce(ctx); err != nil {
			t.Fatal(err)
		}
		drain()
		counts := reconcile(a.AppID)
		if counts.Tokens != 50 || counts.Micro != 70 {
			t.Fatal("correction not applied by consumer", counts)
		}
	})
	runCase("completed_without_outbox_releases_reserved_capacity", func(t *testing.T) {
		original := e.Bus.(*DurableBus)
		bus := &DurableBus{Store: failedCompletionOutbox{s}, Transport: original.Transport, GlobalBytes: 512 << 20, AppBytes: 64 << 20}
		started := Event{ID: "kafka-only:1", AttemptID: "kafka-only", RequestID: "kafka-only-request", AppID: "kafka-only-app", Kind: "started", Revision: 1, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), ReserveTokens: 100, ReserveMicro: 100}
		if err := fixtureGuard.Reserve(ctx, App{ID: started.AppID}, Reservation{started.AttemptID, started.AppID, started.Month, 100, 100}); err != nil {
			t.Fatal(err)
		}
		if err := bus.Publish(ctx, started); err != nil {
			t.Fatal(err)
		}
		completed := started
		completed.ID = "kafka-only:2"
		completed.Kind = "completed"
		completed.Revision = 2
		completed.Usage = Usage{Input: 10, Output: 20, Total: 30}
		completed.CostMicro = 40
		if err := bus.Publish(ctx, completed); err != nil {
			t.Fatal("Kafka fallback was short-circuited", err)
		}
		drain()
		if counts := reconcile(started.AppID); counts.Tokens != 30 || counts.Micro != 40 {
			t.Fatal("consumer failed to settle Kafka-only event", counts)
		}
		var n int64
		if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_outbox WHERE event_id=?`, completed.ID).Scan(&n); err != nil || n != 0 {
			t.Fatal("unnecessary outbox backfill", n, err)
		}
		if err := s.DB.QueryRowContext(ctx, `SELECT used_bytes FROM gw_metering_capacity WHERE id=?`, "app:"+started.AppID).Scan(&n); err != nil || n != 0 {
			t.Fatal("Kafka-only completion leaked reservation", n, err)
		}
		// 只重置隔离 fixture 的故障标记，实际恢复通过 metering-resume 核验。
		if _, err := s.DB.ExecContext(ctx, `UPDATE gw_metering_control SET state='healthy',reason='' WHERE id=1`); err != nil {
			t.Fatal(err)
		}
	})
	runCase("outbox_concurrent_capacity_serialization", func(t *testing.T) {
		const n = 64
		events := make([]Event, n)
		failures := make(chan error, n)
		var tasks sync.WaitGroup
		for i := range events {
			id := fmt.Sprintf("capacity-concurrent-%d", i)
			events[i] = Event{ID: id + ":1", AttemptID: id, RequestID: id, AppID: "outbox-concurrent", Kind: "started", Revision: 1, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), ReserveTokens: 100, ReserveMicro: 100}
			tasks.Add(1)
			go func(event Event) {
				defer tasks.Done()
				if err := fixtureGuard.Reserve(ctx, App{ID: event.AppID}, Reservation{event.AttemptID, event.AppID, event.Month, 100, 100}); err != nil {
					failures <- err
					return
				}
				failures <- s.SaveOutbox(ctx, event, 512<<20, 128<<20)
			}(events[i])
		}
		tasks.Wait()
		close(failures)
		for err := range failures {
			if err != nil {
				t.Fatal("concurrent capacity failed", err)
			}
		}
		// 多个生产者同时保存同一完成事件，预留只扣一次。
		duplicate := events[0]
		duplicate.ID = duplicate.AttemptID + ":2"
		duplicate.Kind, duplicate.Revision, duplicate.Status, duplicate.Quality = "completed", 2, "cancelled", "reported"
		duplicateErrors := make(chan error, 16)
		for i := 0; i < 16; i++ {
			go func() { duplicateErrors <- s.SaveOutbox(ctx, duplicate, 512<<20, 128<<20) }()
		}
		for i := 0; i < 16; i++ {
			if err := <-duplicateErrors; err != nil {
				t.Fatal("concurrent reserved completion failed", err)
			}
		}
		for _, event := range events {
			// 手工 fixture 未调用厂商，用已知零费用完成，不用猜测真实 pending。
			event.ID = event.AttemptID + ":2"
			event.Kind = "completed"
			event.Revision = 2
			event.Status = "cancelled"
			event.Quality = "reported"
			if err := e.Bus.Publish(ctx, event); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 3; i++ {
			if err := e.Bus.(*DurableBus).RelayOnce(ctx); err != nil {
				t.Fatal(err)
			}
		}
		drain()
		var used int64
		if err := s.DB.QueryRowContext(ctx, `SELECT used_bytes FROM gw_metering_capacity WHERE id='app:outbox-concurrent'`).Scan(&used); err != nil || used != 0 {
			t.Fatal("capacity accounting leaked under concurrency", used, err)
		}
		if counts := reconcile("outbox-concurrent"); counts != (Counters{}) {
			t.Fatal("late started recreated reservations", counts)
		}
	})
	runCase("offline_audit_requires_evidence_before_resume", func(t *testing.T) {
		drain()
		stopWorker()
		if _, err := AuditMetering(ctx, r); err == nil {
			t.Fatal("audit did not require writer acknowledgement")
		}
		t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
		audit, err := AuditMetering(ctx, r)
		if err != nil || audit.Pending != 1 {
			t.Fatal("pending audit failed", audit, err)
		}
		if err := s.FreezeMetering(ctx, "metering_delivery_failure"); err != nil {
			t.Fatal(err)
		}
		if err := r.Redis.Set(ctx, "gw:metering:frozen", "delivery_failure", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := ResumeMetering(ctx, r); err == nil {
			t.Fatal("unknown started was automatically released")
		}
		key := quotaKeys("relay-app", month(time.Now()), "relay-attempt")[1]
		fields, err := r.Redis.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if err = r.Redis.Del(ctx, key).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err = AuditMetering(ctx, r); err == nil {
			t.Fatal("missing idempotency key not detected")
		}
		if err = r.Redis.HSet(ctx, key, fields).Err(); err != nil {
			t.Fatal(err)
		}
		rows, err := s.Records(ctx, Filter{RequestID: "outbox-request"})
		if err != nil {
			t.Fatal(err)
		}
		// outbox-b 是测试手工构造且从未调用上游的 fixture，已知费用为零。
		var fixed Event
		for _, event := range rows {
			if event.AttemptID == "outbox-b" {
				fixed = event
			}
		}
		if fixed.AttemptID == "" {
			t.Fatal("pending fixture missing")
		}
		fixed.Kind = "completed"
		fixed.Revision = 3
		fixed.Status = "reconciled"
		fixed.Quality = "reconciled"
		fixed.Usage = Usage{}
		fixed.CostMicro = 0
		if _, err = s.CorrectOutbox(ctx, "acceptance", "resolve-known-fixture", fixed, 512<<20, 64<<20); err != nil {
			t.Fatal(err)
		}
		if err = e.Bus.(*DurableBus).RelayOnce(ctx); err != nil {
			t.Fatal(err)
		}
		startWorker()
		drain()
		stopWorker()
		audit, err = ResumeMetering(ctx, r)
		if err != nil || audit.Pending != 0 {
			t.Fatal("verified resume failed", audit, err)
		}
		if err = s.MeteringHealthy(ctx); err != nil {
			t.Fatal("control remained frozen", err)
		}
		if n, err := r.Redis.Exists(ctx, "gw:metering:frozen").Result(); err != nil || n != 0 {
			t.Fatal("distributed freeze not cleared", err)
		}
		startWorker()
	})
	runCase("quota_rebuild_includes_unpublished_outbox", func(t *testing.T) {
		drain()
		stopWorker()
		started := Event{ID: "rebuild-outbox:1", AttemptID: "rebuild-outbox", RequestID: "rebuild-outbox-request", AppID: "rebuild-outbox-app", Kind: "started", Revision: 1, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), ReserveTokens: 100, ReserveMicro: 100}
		if err := fixtureGuard.Reserve(ctx, App{ID: started.AppID}, Reservation{started.AttemptID, started.AppID, started.Month, 100, 100}); err != nil {
			t.Fatal(err)
		}
		completed := started
		completed.ID = "rebuild-outbox:2"
		completed.Kind = "completed"
		completed.Revision = 2
		completed.Usage = Usage{Input: 10, Output: 20, Total: 30}
		completed.CostMicro = 40
		for _, event := range []Event{started, completed} {
			if err := s.SaveOutbox(ctx, event, 512<<20, 64<<20); err != nil {
				t.Fatal(err)
			}
		}
		t.Setenv("GW_QUOTA_REBUILD_ACK", "writers-stopped")
		if err := RebuildQuota(ctx, r); err != nil {
			t.Fatal("unpublished compensation omitted", err)
		}
		if counts := reconcile(started.AppID); counts.Tokens != 30 || counts.Micro != 40 {
			t.Fatal("rebuild lost compensation", counts)
		}
		t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
		if _, err := AuditMetering(ctx, r); err != nil {
			t.Fatal("rebuild did not leave a consistent snapshot", err)
		}
		if _, err := ResumeMetering(ctx, r); err != nil {
			t.Fatal(err)
		}
		e.Bus.(*DurableBus).stopped.Store(false)
		startWorker()
	})
	runCase("checkpoint_atomicity_and_old_owner_fencing", func(t *testing.T) {
		drain()
		stopWorker()
		_, before, err := s.Checkpoints(ctx)
		if err != nil {
			t.Fatal(err)
		}
		old, err := s.ClaimPartition(ctx, 0, before[0].Generation+100, "old-owner", false)
		if err != nil {
			t.Fatal(err)
		}
		current, err := s.ClaimPartition(ctx, 0, old.Generation+1, "new-owner", false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.ClaimPartition(ctx, 0, old.Generation, "old-owner", false); !errors.Is(err, ErrFenced) {
			t.Fatal("late old generation reclaimed ownership", err)
		}
		event := Event{ID: "checkpoint-test:2", AttemptID: "checkpoint-test", RequestID: "checkpoint-request", AppID: "checkpoint-app", Kind: "completed", Revision: 2, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), Usage: Usage{Total: 5}, CostMicro: 7}
		raw, _ := eventBytes(event)
		msg := kafka.Message{Partition: 0, Offset: current.Next, Value: raw}
		if err = s.ApplyPartition(ctx, old, old.Next, msg, event, false); !errors.Is(err, ErrFenced) {
			t.Fatal("old owner wrote ledger", err)
		}
		var n int
		_ = s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_events WHERE id=?`, event.ID).Scan(&n)
		if n != 0 {
			t.Fatal("fenced event persisted")
		}
		// 制造已有 ID 的载荷矛盾，确认回滚事件和检查点，而非提前推进水位。
		if err = s.Apply(ctx, event); err != nil {
			t.Fatal(err)
		}
		event.Usage.Total++
		raw, _ = eventBytes(event)
		msg.Value = raw
		if err = s.ApplyPartition(ctx, current, current.Next, msg, event, false); !errors.Is(err, ErrEventConflict) {
			t.Fatal("payload conflict not rejected", err)
		}
		_, after, err := s.Checkpoints(ctx)
		if err != nil || after[0].Next != before[0].Next {
			t.Fatal("failed transaction advanced checkpoint", after, err)
		}
		if _, err = s.DB.ExecContext(ctx, `DELETE FROM gw_events WHERE id=?`, event.ID); err != nil {
			t.Fatal(err)
		}
		if _, err = s.DB.ExecContext(ctx, `DELETE FROM gw_attempts WHERE attempt_id=?`, event.AttemptID); err != nil {
			t.Fatal(err)
		}
		if _, err = s.DB.ExecContext(ctx, `UPDATE gw_metering_checkpoint SET kafka_generation=-1,owner_id='' WHERE partition_id=0`); err != nil {
			t.Fatal(err)
		}
		startWorker()
	})
	runCase("old_sql_backup_replays_before_current_group_offset", func(t *testing.T) {
		drain()
		stopWorker()
		_, backup, err := s.Checkpoints(ctx)
		if err != nil {
			t.Fatal(err)
		}
		startWorker()
		completed := Event{ID: "backup-replay:2", AttemptID: "backup-replay", RequestID: "backup-replay-request", AppID: "backup-replay-app", Kind: "completed", Revision: 2, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), Usage: Usage{Input: 31, Output: 11, Total: 42}, CostMicro: 53}
		publish(completed)
		drain()
		stopWorker()
		_, latest, err := s.Checkpoints(ctx)
		if err != nil {
			t.Fatal(err)
		}
		partition := -1
		for i, p := range latest {
			if p.Next != backup[i].Next {
				partition = i
			}
		}
		if partition < 0 {
			t.Fatal("event did not advance a checkpoint")
		}
		// 专用测试库回滚该次消息涉及的账本和 SQL checkpoint，Kafka/Redis 保持最新。
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM gw_events WHERE id=?`, completed.ID); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM gw_attempts WHERE attempt_id=?`, completed.AttemptID); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		p := backup[partition]
		if _, err = tx.ExecContext(ctx, `UPDATE gw_metering_checkpoint SET next_offset=?,anchor_offset=?,anchor_hash=? WHERE partition_id=?`, p.Next, p.Anchor, p.Hash, partition); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = r.ConsumeMetering(ctx); !errors.Is(err, ErrCheckpoint) {
			t.Fatal("normal worker accepted group ahead of restored SQL", err)
		}
		t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
		plan, err := PlanMeteringRecovery(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Ranges[partition].Committed <= plan.Checkpoints[partition].Next {
			t.Fatal("restore fixture lacks offset gap")
		}
		_, release, err := s.maintenanceLock(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ApplyMeteringRecovery(ctx, r, plan); err == nil {
			release()
			t.Fatal("parallel recovery bypassed exclusive lock")
		}
		release()
		// 在回放事务前中断，任务与 recovering 已持久化；同 manifest 可继续。
		conn, err := s.DB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = conn.ExecContext(ctx, `LOCK TABLES gw_metering_checkpoint READ`); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		interrupt, done := context.WithTimeout(ctx, 1500*time.Millisecond)
		_, err = ApplyMeteringRecovery(interrupt, r, plan)
		done()
		_, _ = conn.ExecContext(ctx, `UNLOCK TABLES`)
		conn.Close()
		if err == nil {
			t.Fatal("blocked checkpoint write unexpectedly succeeded")
		}
		if _, err = ResumeMetering(ctx, r); err == nil {
			t.Fatal("unfinished recovery resumed admission")
		}
		newPlan, err := PlanMeteringRecovery(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if newPlan.Hash != plan.Hash {
			if _, err = ApplyMeteringRecovery(ctx, r, newPlan); err == nil {
				t.Fatal("new manifest bypassed unfinished recovery")
			}
		}
		if _, err = ApplyMeteringRecovery(ctx, r, plan); err != nil {
			t.Fatal("recovery retry failed", err)
		}
		if counts := reconcile(completed.AppID); counts.Tokens != 42 || counts.Micro != 53 {
			t.Fatal("old backup lost usage", counts)
		}
		if _, err = ApplyMeteringRecovery(ctx, r, plan); err != nil {
			t.Fatal("completed recovery not idempotent", err)
		}
		if err = s.MeteringHealthy(ctx); err == nil {
			t.Fatal("recovery silently resumed admission")
		}
		if _, err = ResumeMetering(ctx, r); err != nil {
			t.Fatal(err)
		}
		e.Bus.(*DurableBus).stopped.Store(false)
		startWorker()
	})
	runCase("recovery_source_retention_and_stale_plan_rejected", func(t *testing.T) {
		drain()
		stopWorker()
		t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
		plan, err := PlanMeteringRecovery(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		partition := 0
		for i, p := range plan.Checkpoints {
			if p.Anchor >= 0 {
				partition = i
				break
			}
		}
		p := plan.Checkpoints[partition]
		if _, err = s.DB.ExecContext(ctx, `UPDATE gw_metering_checkpoint SET anchor_hash=? WHERE partition_id=?`, Hash("different-topic-record"), partition); err != nil {
			t.Fatal(err)
		}
		if _, err = PlanMeteringRecovery(ctx, r); !errors.Is(err, ErrCheckpoint) {
			t.Fatal("changed source anchor accepted", err)
		}
		if _, err = s.DB.ExecContext(ctx, `UPDATE gw_metering_checkpoint SET anchor_hash=?,next_offset=? WHERE partition_id=?`, p.Hash, plan.Ranges[partition].End+1, partition); err != nil {
			t.Fatal(err)
		}
		if _, err = PlanMeteringRecovery(ctx, r); !errors.Is(err, ErrCheckpoint) {
			t.Fatal("checkpoint outside retention accepted", err)
		}
		if _, err = s.DB.ExecContext(ctx, `UPDATE gw_metering_checkpoint SET next_offset=? WHERE partition_id=?`, p.Next, partition); err != nil {
			t.Fatal(err)
		}
		publish(Event{ID: "stale-plan:2", AttemptID: "stale-plan", RequestID: "stale-request", AppID: "stale-app", Kind: "completed", Revision: 2, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now())})
		if _, err = ApplyMeteringRecovery(ctx, r, plan); err == nil {
			t.Fatal("stale fixed-tail plan accepted")
		}
		fresh, err := PlanMeteringRecovery(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ApplyMeteringRecovery(ctx, r, fresh); err != nil {
			t.Fatal(err)
		}
		if _, err = ResumeMetering(ctx, r); err != nil {
			t.Fatal(err)
		}
		e.Bus.(*DurableBus).stopped.Store(false)
		startWorker()
	})
	runCase("existing_checkpoint_initialization_requires_audited_barrier", func(t *testing.T) {
		drain()
		stopWorker()
		t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
		if _, err := s.DB.ExecContext(ctx, `DELETE FROM gw_metering_checkpoint`); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.ExecContext(ctx, `DELETE FROM gw_metering_stream`); err != nil {
			t.Fatal(err)
		}
		if err := InitializeCheckpoints(ctx, r, false); err == nil {
			t.Fatal("nonempty ledger silently initialized")
		}
		if err := InitializeCheckpoints(ctx, r, true); err != nil {
			t.Fatal("audited legacy baseline failed", err)
		}
		_, parts, err := s.Checkpoints(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range parts {
			if p.Base != p.Next {
				t.Fatal("legacy baseline has wrong header boundary", p)
			}
		}
		if err = s.MeteringHealthy(ctx); err == nil {
			t.Fatal("baseline initialization reopened admission")
		}
		if _, err = ResumeMetering(ctx, r); err != nil {
			t.Fatal(err)
		}
		e.Bus.(*DurableBus).Transport.(*KafkaBus).stream.Store(nil)
		e.Bus.(*DurableBus).stopped.Store(false)
		startWorker()
	})
	runCase("live_rebalance_preserves_partition_checkpoints", func(t *testing.T) {
		drain()
		second, err := OpenRuntime(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close()
		secondCtx, secondCancel := context.WithCancel(ctx)
		secondDone := make(chan error, 1)
		go func() { secondDone <- second.ConsumeMetering(secondCtx) }()
		defer func() {
			secondCancel()
			if err := <-secondDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("rebalanced worker failed: %v", err)
			}
		}()
		for i := 0; i < 24; i++ {
			e := Event{ID: fmt.Sprintf("rebalance-%d:2", i), AttemptID: fmt.Sprintf("rebalance-%d", i), RequestID: fmt.Sprintf("rebalance-request-%d", i), AppID: fmt.Sprintf("rebalance-app-%d", i%6), Kind: "completed", Revision: 2, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), Usage: Usage{Total: 3}, CostMicro: 5}
			publish(e)
		}
		drain()
		for i := 0; i < 6; i++ {
			if counts := reconcile(fmt.Sprintf("rebalance-app-%d", i)); counts.Tokens != 12 || counts.Micro != 20 {
				t.Fatal("rebalance lost billing", i, counts)
			}
		}
		stopWorker()
		publish(Event{ID: "rebalance-survivor:2", AttemptID: "rebalance-survivor", RequestID: "rebalance-survivor-request", AppID: "rebalance-app-0", Kind: "completed", Revision: 2, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now()), Usage: Usage{Total: 7}, CostMicro: 11})
		drain()
		if counts := reconcile("rebalance-app-0"); counts.Tokens != 19 || counts.Micro != 31 {
			t.Fatal("surviving owner lost billing", counts)
		}
	})
	runCase("recreated_topic_refuses_old_source_replay", func(t *testing.T) {
		t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
		plan, err := PlanMeteringRecovery(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		deleted, err := r.KafkaClient.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{run}})
		if err != nil {
			t.Fatal(err)
		}
		if deleted.Errors[run] != nil {
			t.Fatal(deleted.Errors[run])
		}
		created, err := r.KafkaClient.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: run, NumPartitions: 6, ReplicationFactor: 1}}})
		if err != nil {
			t.Fatal(err)
		}
		if created.Errors[run] != nil {
			t.Fatal(created.Errors[run])
		}
		if _, err = PlanMeteringRecovery(ctx, r); err == nil {
			t.Fatal("recreated topic accepted old checkpoint")
		}
		if _, err = ApplyMeteringRecovery(ctx, r, plan); err == nil {
			t.Fatal("recreated topic was replayed")
		}
		if err = s.MeteringHealthy(ctx); err == nil {
			t.Fatal("source gap recovery left admission healthy")
		}
	})
	t.Logf("isolated acceptance completed: %s, upstream calls=%d", run, upstreamCalls.Load())
}

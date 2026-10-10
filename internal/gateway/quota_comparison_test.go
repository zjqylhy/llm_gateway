package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

// SQL 基线只用于实验；不向生产运行时提供切换入口。
type comparisonSQLQuota struct{ db *sql.DB }

func (g comparisonSQLQuota) reserve(ctx context.Context, app App, r Reservation) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	// 汇总行已承担额度串行化；READ COMMITTED 避免不存在的 attempt 查询锁住插入间隙。
	tx, err := g.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 重复键直接获取排他锁，避免 INSERT IGNORE 的共享锁随后升级造成死锁。
	if _, err = tx.ExecContext(ctx, "INSERT INTO bench_quota(app,month) VALUES(?,?) ON DUPLICATE KEY UPDATE app=app", app.ID, r.Month); err != nil {
		return err
	}
	var c Counters
	if err = tx.QueryRowContext(ctx, "SELECT tokens,micro,rt,rm FROM bench_quota WHERE app=? AND month=? FOR UPDATE", app.ID, r.Month).Scan(&c.Tokens, &c.Micro, &c.ReservedTokens, &c.ReservedMicro); err != nil {
		return err
	}
	var revision int64
	err = tx.QueryRowContext(ctx, "SELECT revision FROM bench_reservations WHERE id=? FOR UPDATE", r.ID).Scan(&revision)
	if err == nil {
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if app.MonthlyTokens > 0 && c.Tokens+c.ReservedTokens+r.Tokens > app.MonthlyTokens {
		return ErrLimit
	}
	if app.BudgetMicro > 0 && app.BudgetAction == "reject" && c.Micro+c.ReservedMicro+r.Micro > app.BudgetMicro {
		return ErrLimit
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO bench_reservations(id,app,month,rt,rm) VALUES(?,?,?,?,?)", r.ID, app.ID, r.Month, r.Tokens, r.Micro); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE bench_quota SET rt=rt+?,rm=rm+? WHERE app=? AND month=?", r.Tokens, r.Micro, app.ID, r.Month); err != nil {
		return err
	}
	return tx.Commit()
}

func comparisonApply(ctx context.Context, tx *sql.Tx, e Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT IGNORE INTO gw_events(id,attempt_id,revision,payload,at) VALUES(?,?,?,?,?)", e.ID, e.AttemptID, e.Revision, b, e.At); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT IGNORE INTO gw_attempts(attempt_id,request_id,app_id,department,model_id,status,purpose,revision,tokens,cost_micro,at,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", e.AttemptID, e.RequestID, e.AppID, e.Department, e.Model.ID, e.Status, e.Purpose, e.Revision, e.Usage.Total, e.CostMicro, e.At, b); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE gw_attempts SET status=?,revision=?,tokens=?,cost_micro=?,payload=? WHERE attempt_id=? AND revision<?", e.Status, e.Revision, e.Usage.Total, e.CostMicro, b, e.AttemptID, e.Revision)
	return err
}

func (g comparisonSQLQuota) settle(ctx context.Context, e Event) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	tx, err := g.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 与预占保持同样的锁顺序，避免汇总行和 attempt 行之间产生反向锁等待。
	var marker string
	if err = tx.QueryRowContext(ctx, "SELECT app FROM bench_quota WHERE app=? AND month=? FOR UPDATE", e.AppID, e.Month).Scan(&marker); err != nil {
		return err
	}
	var revision, tokens, micro, rt, rm int64
	if err = tx.QueryRowContext(ctx, "SELECT revision,tokens,micro,rt,rm FROM bench_reservations WHERE id=? AND app=? AND month=? FOR UPDATE", e.AttemptID, e.AppID, e.Month).Scan(&revision, &tokens, &micro, &rt, &rm); err != nil {
		return err
	}
	if revision >= e.Revision {
		return tx.Commit()
	}
	if revision > 0 {
		rt, rm = 0, 0
	}
	if err = comparisonApply(ctx, tx, e); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE bench_quota SET tokens=tokens+?,micro=micro+?,rt=rt-?,rm=rm-? WHERE app=? AND month=?", e.Usage.Total-tokens, e.CostMicro-micro, rt, rm, e.AppID, e.Month); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE bench_reservations SET revision=?,tokens=?,micro=? WHERE id=?", e.Revision, e.Usage.Total, e.CostMicro, e.AttemptID); err != nil {
		return err
	}
	return tx.Commit()
}

func (g comparisonSQLQuota) counters(ctx context.Context, app, month string) (Counters, error) {
	var c Counters
	err := g.db.QueryRowContext(ctx, "SELECT tokens,micro,rt,rm FROM bench_quota WHERE app=? AND month=?", app, month).Scan(&c.Tokens, &c.Micro, &c.ReservedTokens, &c.ReservedMicro)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return c, err
}

type comparisonBackend struct {
	name     string
	reserve  func(context.Context, App, Reservation) error
	settle   func(context.Context, Event) error
	counters func(context.Context, string, string) (Counters, error)
}

type comparisonSample struct {
	Backend         string  `json:"backend"`
	Concurrency     int     `json:"concurrency"`
	Apps            int     `json:"apps"`
	Repeat          int     `json:"repeat"`
	Completed       int     `json:"completed"`
	Errors          int     `json:"errors"`
	FirstError      string  `json:"first_error,omitempty"`
	Seconds         float64 `json:"seconds"`
	CyclesPerSecond float64 `json:"cycles_per_second"`
	P50MS           float64 `json:"p50_ms"`
	P95MS           float64 `json:"p95_ms"`
	P99MS           float64 `json:"p99_ms"`
	ReserveP50MS    float64 `json:"reserve_p50_ms"`
	ReserveP99MS    float64 `json:"reserve_p99_ms"`
	PoolWaitCount   int64   `json:"pool_wait_count"`
	PoolWaitMS      float64 `json:"pool_wait_ms"`
	ReservedTokens  int64   `json:"remaining_reserved_tokens"`
	TokenDelta      int64   `json:"settled_token_delta_vs_ledger"`
	MicroDelta      int64   `json:"settled_micro_delta_vs_ledger"`
}

func comparisonPercentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	i := int(float64(len(values)-1) * p / 100)
	return values[i]
}

func TestQuotaStorageComparison(t *testing.T) {
	if os.Getenv("GW_RUN_QUOTA_COMPARISON") != "1" {
		t.Skip("run scripts/compare-quota-storage.ps1")
	}
	run := os.Getenv("GW_QUOTA_COMPARISON_ID")
	if !regexp.MustCompile(`^gw_quota_[a-f0-9]{12}$`).MatchString(run) {
		t.Fatal("isolated comparison ID required")
	}
	dsn, err := mysql.ParseDSN(os.Getenv("GW_MYSQL_DSN"))
	if err != nil || dsn.DBName != run || dsn.Addr == "127.0.0.1:13306" || !regexp.MustCompile(`^127\.0\.0\.1:\d+$`).MatchString(dsn.Addr) {
		t.Fatal("new dedicated local database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s, err := NewSQLStore(ctx, os.Getenv("GW_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.DB.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"CREATE TABLE bench_quota(app VARCHAR(64),month VARCHAR(7),tokens BIGINT NOT NULL DEFAULT 0,micro BIGINT NOT NULL DEFAULT 0,rt BIGINT NOT NULL DEFAULT 0,rm BIGINT NOT NULL DEFAULT 0,PRIMARY KEY(app,month)) ENGINE=InnoDB",
		"CREATE TABLE bench_reservations(id VARCHAR(64) PRIMARY KEY,app VARCHAR(64),month VARCHAR(7),revision BIGINT NOT NULL DEFAULT 0,tokens BIGINT NOT NULL DEFAULT 0,micro BIGINT NOT NULL DEFAULT 0,rt BIGINT NOT NULL,rm BIGINT NOT NULL) ENGINE=InnoDB",
	} {
		if _, err = s.DB.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	ro, err := redis.ParseURL(os.Getenv("GW_REDIS_URL"))
	if err != nil || ro.Addr == "127.0.0.1:16379" || !regexp.MustCompile(`^127\.0\.0\.1:\d+$`).MatchString(ro.Addr) {
		t.Fatal("new local Redis required")
	}
	ro.ContextTimeoutEnabled = true
	ro.MaxRetries = 1
	ro.PoolSize = 32
	ro.MaxActiveConns = 32
	ro.PoolTimeout = time.Second
	ro.ReadTimeout = 250 * time.Millisecond
	ro.WriteTimeout = 250 * time.Millisecond
	rc := redis.NewClient(ro)
	defer rc.Close()
	if n, err := rc.DBSize(ctx).Result(); err != nil || n != 0 {
		t.Fatal("empty comparison Redis required", err)
	}
	if err := rc.Set(ctx, "gw:quota:state", "ready", 0).Err(); err != nil {
		t.Fatal(err)
	}
	rg := RedisGuard{Client: rc, RequireReady: true, EnforceMemory: true}
	sg := comparisonSQLQuota{s.DB}
	backends := []comparisonBackend{
		{"redis", rg.Reserve, func(c context.Context, e Event) error {
			if err := s.Apply(c, e); err != nil {
				return err
			}
			return rg.Settle(c, e)
		}, rg.Counters},
		{"mysql", sg.reserve, sg.settle, sg.counters},
	}
	var flush, syncBinlog int
	var version string
	if err = s.DB.QueryRowContext(ctx, "SELECT @@innodb_flush_log_at_trx_commit,@@sync_binlog,@@version").Scan(&flush, &syncBinlog, &version); err != nil {
		t.Fatal(err)
	}
	if flush != 1 {
		t.Fatal("comparison requires durable MySQL commits")
	}
	// 先验证基线具备相同的配额边界、重复消费和差额修正语义。
	for _, b := range backends {
		comparisonCorrectness(t, ctx, s, b)
	}
	var samples []comparisonSample
	for _, apps := range []int{1, 32} {
		for _, vus := range []int{1, 16, 64} {
			for repeat := 1; repeat <= 3; repeat++ {
				order := []int{0, 1}
				if repeat%2 == 0 {
					order = []int{1, 0}
				}
				for _, index := range order {
					b := backends[index]
					prefix := fmt.Sprintf("%s-a%d-v%d-r%d", b.name, apps, vus, repeat)
					sample := comparisonLoad(t, ctx, s, b, prefix, apps, vus, repeat)
					samples = append(samples, sample)
					t.Logf("%s apps=%d concurrency=%d repeat=%d completed=%d errors=%d cycles/s=%.1f p99=%.2fms reserve-p99=%.2fms", b.name, apps, vus, repeat, sample.Completed, sample.Errors, sample.CyclesPerSecond, sample.P99MS, sample.ReserveP99MS)
				}
			}
		}
	}
	output := struct {
		Run              string             `json:"run"`
		At               time.Time          `json:"at"`
		MySQLVersion     string             `json:"mysql_version"`
		Flush            int                `json:"innodb_flush_log_at_trx_commit"`
		SyncBinlog       int                `json:"sync_binlog"`
		RedisPersistence string             `json:"redis_persistence"`
		Scope            string             `json:"scope"`
		Samples          []comparisonSample `json:"samples"`
	}{run, time.Now().UTC(), version, flush, syncBinlog, "AOF appendfsync always", "reserve + started SQL ledger + completed SQL ledger and settle; no Kafka/HTTP/provider", samples}
	raw, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(os.Getenv("GW_QUOTA_COMPARISON_DIRECTORY"), "results.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func comparisonEvent(id, app string) Event {
	return Event{ID: id + ":1", AttemptID: id, RequestID: id, AppID: app, Month: "2026-10", At: time.Now().UTC(), Kind: "started", Revision: 1, Purpose: "inference", Status: "started", ReserveTokens: 160, ReserveMicro: 260}
}

func comparisonComplete(e Event) Event {
	e.ID = e.AttemptID + ":2"
	e.Revision = 2
	e.Kind = "completed"
	e.Status = "success"
	e.Quality = "reported"
	e.Usage = Usage{Input: 100, Output: 20, Total: 120}
	e.CostMicro = 180
	return e
}

func comparisonCorrectness(t *testing.T, ctx context.Context, s *SQLStore, b comparisonBackend) {
	t.Helper()
	a := App{ID: "correct-" + b.name, MonthlyTokens: 160, BudgetMicro: 260, BudgetAction: "reject"}
	e := comparisonEvent("correct-"+b.name, a.ID)
	r := Reservation{e.AttemptID, a.ID, e.Month, 160, 260}
	if err := b.reserve(ctx, a, r); err != nil {
		t.Fatal(err)
	}
	if err := b.reserve(ctx, a, r); err != nil {
		t.Fatal("duplicate reservation", err)
	}
	if err := s.Apply(ctx, e); err != nil {
		t.Fatal(err)
	}
	finished := comparisonComplete(e)
	for i := 0; i < 2; i++ {
		if err := b.settle(ctx, finished); err != nil {
			t.Fatal(err)
		}
	}
	finished.Revision = 3
	finished.ID = finished.AttemptID + ":3"
	finished.Usage.Total = 130
	finished.CostMicro = 190
	for i := 0; i < 2; i++ {
		if err := b.settle(ctx, finished); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.settle(ctx, comparisonComplete(e)); err != nil {
		t.Fatal("old revision", err)
	}
	if c, err := b.counters(ctx, a.ID, e.Month); err != nil || c != (Counters{Tokens: 130, Micro: 190}) {
		t.Fatal("revision correction", c, err)
	}
	for _, budget := range []bool{false, true} {
		name := fmt.Sprintf("race-%s-%t", b.name, budget)
		a := App{ID: name, MonthlyTokens: 800}
		if budget {
			a.MonthlyTokens = 0
			a.BudgetMicro = 1300
			a.BudgetAction = "reject"
		}
		var wg sync.WaitGroup
		var allowed, rejected, failed atomic.Int64
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := b.reserve(ctx, a, Reservation{fmt.Sprintf("%s-%d", name, i), a.ID, "2026-10", 160, 260})
				switch {
				case err == nil:
					allowed.Add(1)
				case errors.Is(err, ErrLimit):
					rejected.Add(1)
				default:
					failed.Add(1)
				}
			}(i)
		}
		wg.Wait()
		if allowed.Load() != 5 || rejected.Load() != 27 || failed.Load() != 0 {
			t.Fatalf("%s quota race allowed=%d rejected=%d failed=%d", name, allowed.Load(), rejected.Load(), failed.Load())
		}
	}
}

func comparisonLoad(t *testing.T, ctx context.Context, s *SQLStore, b comparisonBackend, prefix string, apps, vus, repeat int) comparisonSample {
	t.Helper()
	var sequence atomic.Int64
	cycle := func(worker int) (float64, error) {
		n := sequence.Add(1)
		id := fmt.Sprintf("%s-%d", prefix, n)
		app := fmt.Sprintf("%s-%d", prefix, (n-1)%int64(apps))
		e := comparisonEvent(id, app)
		start := time.Now()
		if err := b.reserve(ctx, App{ID: app}, Reservation{id, app, e.Month, 160, 260}); err != nil {
			return float64(time.Since(start).Microseconds()) / 1000, err
		}
		reserveMS := float64(time.Since(start).Microseconds()) / 1000
		if err := s.Apply(ctx, e); err != nil {
			return reserveMS, err
		}
		return reserveMS, b.settle(ctx, comparisonComplete(e))
	}
	for i := 0; i < 32; i++ {
		if _, err := cycle(i); err != nil {
			t.Fatal("warmup", err)
		}
	}
	before := s.DB.Stats()
	start := time.Now()
	deadline := start.Add(3 * time.Second)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var latency, reserveLatency []float64
	failures := 0
	firstError := ""
	for worker := 0; worker < vus; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				begin := time.Now()
				r, err := cycle(worker)
				elapsed := float64(time.Since(begin).Microseconds()) / 1000
				mu.Lock()
				if err != nil {
					failures++
					if firstError == "" {
						firstError = err.Error()
					}
				} else {
					latency = append(latency, elapsed)
					reserveLatency = append(reserveLatency, r)
				}
				mu.Unlock()
			}
		}(worker)
	}
	wg.Wait()
	seconds := time.Since(start).Seconds()
	after := s.DB.Stats()
	sample := comparisonSample{Backend: b.name, Concurrency: vus, Apps: apps, Repeat: repeat, Completed: len(latency), Errors: failures, FirstError: firstError, Seconds: seconds, CyclesPerSecond: float64(len(latency)) / seconds, P50MS: comparisonPercentile(latency, 50), P95MS: comparisonPercentile(latency, 95), P99MS: comparisonPercentile(latency, 99), ReserveP50MS: comparisonPercentile(reserveLatency, 50), ReserveP99MS: comparisonPercentile(reserveLatency, 99), PoolWaitCount: after.WaitCount - before.WaitCount, PoolWaitMS: float64((after.WaitDuration - before.WaitDuration).Microseconds()) / 1000}
	// 饱和负载允许触发超时，保留预占及账本差异供报告分析，不把失败算作成功吞吐。
	for app := 0; app < apps; app++ {
		id := fmt.Sprintf("%s-%d", prefix, app)
		var expected Counters
		if err := s.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(tokens),0),COALESCE(SUM(cost_micro),0) FROM gw_attempts WHERE app_id=?", id).Scan(&expected.Tokens, &expected.Micro); err != nil {
			t.Fatal(err)
		}
		actual, err := b.counters(ctx, id, "2026-10")
		if err != nil {
			t.Fatal(err)
		}
		sample.ReservedTokens += actual.ReservedTokens
		sample.TokenDelta += actual.Tokens - expected.Tokens
		sample.MicroDelta += actual.Micro - expected.Micro
		if failures == 0 && actual != expected {
			t.Errorf("%s error-free ledger mismatch actual=%+v expected=%+v", id, actual, expected)
		}
	}
	if len(latency) == 0 {
		t.Errorf("%s has no successful cycles", prefix)
	}
	return sample
}

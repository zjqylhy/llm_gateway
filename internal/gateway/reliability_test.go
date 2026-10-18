package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 测试角色只编译到验收二进制；不向生产命令增加 mock 开关。
func TestReliabilityProcess(t *testing.T) {
	role := os.Getenv("GW_RELIABILITY_ROLE")
	if role == "" {
		t.Skip("container role for isolated reliability acceptance")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if role == "upstream" {
		server := &http.Server{Addr: ":8080", Handler: newReliabilityUpstream(), ReadHeaderTimeout: 5 * time.Second}
		go func() { <-ctx.Done(); _ = server.Close() }()
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			t.Fatal(err)
		}
		return
	}
	startup, done := context.WithTimeout(ctx, 20*time.Second)
	r, err := OpenRuntime(startup, false)
	if err == nil {
		err = r.Engine.Refresh(startup)
	}
	if err == nil {
		if bus, ok := r.Engine.Bus.(*DurableBus); ok {
			err = bus.Transport.(*KafkaBus).PrepareSource(startup)
		}
	}
	done()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	e := r.Engine
	e.Client = NewHTTPClient(true)
	var boundaryPaused atomic.Bool
	if strings.HasPrefix(role, "gateway-") {
		stage := strings.TrimPrefix(role, "gateway-")
		if stage == "before-http" {
			e.Client.Transport = boundaryTransport{e.Client.Transport, &boundaryPaused}
		} else if stage == "before-reserve" || stage == "after-reserve" {
			e.Guard = boundaryGuard{e.Guard, stage, &boundaryPaused}
		} else {
			t.Fatal("unknown boundary role", role)
		}
		role = "gateway"
	}
	go e.RunRefresh(ctx)
	if role == "admin" {
		server := &http.Server{Addr: ":8081", Handler: r.Admin.Handler(), ReadHeaderTimeout: 5 * time.Second}
		defer server.Close()
		go func() { <-ctx.Done(); _ = server.Close() }()
		if err = serveBounded(server, r.MaxConnections); err != nil && err != http.ErrServerClosed {
			t.Fatal(err)
		}
		return
	}
	mux := http.NewServeMux()
	if role == "gateway" && r.capacity != nil {
		r.capacity.start()
	}
	mux.HandleFunc("GET /__acceptance/paused", func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, map[string]bool{"paused": boundaryPaused.Load()})
	})
	mux.Handle("GET /metrics", e.Metrics.Handler())
	mux.Handle("/", e.Handler())
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	go func() { <-ctx.Done(); _ = server.Close() }()
	if role == "worker" {
		if e.CacheDisabled {
			go RunMaintenance(ctx, e)
		}
		go r.RunMeteringMonitor(ctx)
		go r.RunAttemptMaintenance(ctx)
		if bus, ok := e.Bus.(*DurableBus); ok {
			go bus.RunRelay(ctx)
		}
		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				cancel()
			}
		}()
		if err = r.ConsumeMetering(ctx); err != nil && ctx.Err() == nil {
			t.Fatal(err)
		}
		return
	}
	if role != "gateway" {
		t.Fatal("unknown acceptance role")
	}
	if err = serveBounded(server, r.MaxConnections); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}

type reliabilityUpstream struct {
	calls atomic.Int64
	mu    sync.Mutex
	gates map[string]chan struct{}
	held  map[string]int
}

func newReliabilityUpstream() *reliabilityUpstream {
	return &reliabilityUpstream{gates: map[string]chan struct{}{}, held: map[string]int{}}
}

func (u *reliabilityUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/readyz" {
		JSON(w, map[string]bool{"ready": true})
		return
	}
	if r.URL.Path == "/stats" {
		u.mu.Lock()
		defer u.mu.Unlock()
		JSON(w, map[string]any{"calls": u.calls.Load(), "held": u.held})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/release/") {
		key := strings.TrimPrefix(r.URL.Path, "/release/")
		u.mu.Lock()
		if gate, ok := u.gates[key]; ok {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
		u.mu.Unlock()
		JSON(w, map[string]bool{"released": true})
		return
	}
	u.calls.Add(1)
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return
	}
	var body struct {
		Messages []struct{ Content string } `json:"messages"`
	}
	_ = json.Unmarshal(raw, &body)
	text := ""
	if len(body.Messages) != 0 {
		text = body.Messages[0].Content
	}
	if strings.HasPrefix(text, "hold-") {
		u.mu.Lock()
		gate := u.gates[text]
		if gate == nil {
			gate = make(chan struct{})
			u.gates[text] = gate
		}
		u.held[text]++
		u.mu.Unlock()
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
	}
	if text == "slow-stream" {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"},\"finish_reason\":null}]}\n\n")
		_ = http.NewResponseController(w).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(20 * time.Second):
		}
		return
	}
	if text == "delayed-probe" {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	MockProvider(w, r)
}

type reliabilityResult struct {
	Status    int
	RequestID string
	Body      string
	ElapsedMS float64
	FirstMS   float64
	DoneMS    float64
	Error     string
}

type reliabilitySample struct {
	Concurrency   int     `json:"concurrency"`
	Requests      int     `json:"requests"`
	Stream        bool    `json:"stream"`
	Failed        int     `json:"failed"`
	Rejected      int     `json:"protected_rejections"`
	Accepted      int     `json:"accepted"`
	AcceptedRPS   float64 `json:"accepted_rps"`
	AcceptedP95MS float64 `json:"accepted_p95_ms,omitempty"`
	AcceptedP99MS float64 `json:"accepted_p99_ms,omitempty"`
	Seconds       float64 `json:"seconds"`
	RPS           float64 `json:"rps"`
	P50MS         float64 `json:"p50_ms"`
	P95MS         float64 `json:"p95_ms"`
	P99MS         float64 `json:"p99_ms"`
	FirstP95MS    float64 `json:"accepted_first_event_p95_ms,omitempty"`
	DrainSeconds  float64 `json:"ledger_drain_seconds"`
}

func TestReliabilityIntegration(t *testing.T) {
	if os.Getenv("GW_RUN_RELIABILITY_TESTS") != "1" {
		t.Skip("run scripts/verify-infrastructure.ps1 -Reliability")
	}
	run := os.Getenv("GW_INFRA_RUN_ID")
	if !regexp.MustCompile(`^gw_acceptance_[a-f0-9]{12}$`).MatchString(run) || os.Getenv("GW_KAFKA_TOPIC") != run || os.Getenv("GW_KAFKA_GROUP") != run || os.Getenv("GW_ETCD_PREFIX") != "/"+run || os.Getenv("GW_RELIABILITY_REDIS_NAME") != run {
		t.Fatal("isolated acceptance resource names required")
	}
	dsn, err := mysql.ParseDSN(os.Getenv("GW_MYSQL_DSN"))
	if err != nil || dsn.DBName != run || !strings.HasPrefix(dsn.Addr, "127.0.0.1:") {
		t.Fatal("dedicated local acceptance database required")
	}
	sustainedSeconds, _ := strconv.Atoi(os.Getenv("GW_RELIABILITY_SUSTAINED_SECONDS"))
	if sustainedSeconds < 0 || sustainedSeconds > 600 {
		t.Fatal("invalid sustained duration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute+time.Duration(sustainedSeconds)*time.Second)
	defer cancel()
	r, err := OpenRuntime(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.Engine.Store.(*SQLStore)
	if size, err := r.Redis.DBSize(ctx).Result(); err != nil || size != 0 {
		t.Fatal("fresh Redis required", err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = InitializeCheckpoints(ctx, r, false); err != nil {
		t.Fatal(err)
	}
	if err = Bootstrap(ctx, r.Engine); err != nil {
		t.Fatal(err)
	}
	publisher := r.Engine.Publisher.(EtcdPublisher)
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_, _ = publisher.Client.Delete(cleanup, publisher.Prefix+"/", clientv3.WithPrefix())
	}()
	dir := os.Getenv("GW_RELIABILITY_DIRECTORY")
	docker := func(args ...string) (string, error) {
		commandCtx, done := context.WithTimeout(ctx, 30*time.Second)
		defer done()
		out, err := exec.CommandContext(commandCtx, "docker", args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	checkT := t
	acceptanceT := t
	mustDocker := func(args ...string) string {
		checkT.Helper()
		out, err := docker(args...)
		if err != nil {
			checkT.Fatalf("docker %s: %v: %s", args[0], err, out)
		}
		return out
	}
	startRole := func(suffix, role, cpus, memory string) string {
		checkT.Helper()
		name := run + "-" + suffix
		// 自动发布的 Docker 端口会在重启后改变；选择空闲端口后显式绑定，保持故障客户端地址稳定。
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			checkT.Fatal(err)
		}
		binding := listener.Addr().String()
		_ = listener.Close()
		mustDocker("run", "--detach", "--name", name, "--label", "llm-gateway.acceptance="+run,
			"--network", os.Getenv("GW_RELIABILITY_NETWORK"), "--cpus", cpus, "--memory", memory,
			"--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
			"--env-file", os.Getenv("GW_RELIABILITY_ENV_FILE"), "-e", "GW_RELIABILITY_ROLE="+role,
			"-e", "HOSTNAME="+name, "-p", binding+":8080",
			"--mount", "type=bind,src="+dir+",dst=/acceptance,readonly",
			"--entrypoint", "/acceptance/gateway.test", os.Getenv("GW_RELIABILITY_IMAGE"),
			"-test.run=^TestReliabilityProcess$", "-test.timeout="+strconv.Itoa(12+sustainedSeconds/60)+"m", "-test.v")
		binding = mustDocker("port", name, "8080/tcp")
		if !regexp.MustCompile(`^127\.0\.0\.1:[0-9]+$`).MatchString(binding) {
			checkT.Fatal("unexpected role port", binding)
		}
		return "http://" + binding
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: 250, MaxIdleConnsPerHost: 150, Proxy: nil}, Timeout: 35 * time.Second}
	defer client.CloseIdleConnections()
	wait := func(label string, timeout time.Duration, check func() bool) {
		checkT.Helper()
		deadline := time.Now().Add(timeout)
		for !check() {
			if time.Now().After(deadline) || ctx.Err() != nil {
				probe, cancel := context.WithTimeout(context.Background(), time.Second)
				var state, reason string
				err := s.DB.QueryRowContext(probe, `SELECT state,reason FROM gw_metering_control WHERE id=1`).Scan(&state, &reason)
				cancel()
				checkT.Log("metering diagnostic", state, reason, err)
				checkT.Fatal("timeout: " + label)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	ready := func(url string) {
		checkT.Helper()
		wait("role readiness", 30*time.Second, func() bool {
			res, err := client.Get(url + "/readyz")
			if err != nil {
				return false
			}
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
			return res.StatusCode == 200
		})
	}
	upstreamURL := startRole("upstream", "upstream", "1", "128m")
	ready(upstreamURL)
	chatBody := func(text string, stream bool) []byte {
		b, _ := json.Marshal(map[string]any{"model": "chat", "messages": []any{map[string]string{"role": "user", "content": text}}, "stream": stream, "max_tokens": 64, "stream_options": map[string]bool{"include_usage": true}})
		return b
	}
	tokenRequest, _ := DecodeRequest(chatBody("hold-token", false), "chat")
	budgetRequest, _ := DecodeRequest(chatBody("hold-budget", false), "chat")
	cfg, err := s.Update(ctx, "acceptance", "reliability.seed", func(c *Config) error {
		c.Providers["primary"] = Provider{"primary", "mock", "http://" + run + "-upstream:8080/v1", r.Engine.Vault.Encrypt("primary", "acceptance")}
		model := Model{ID: "primary", ProviderID: "primary", Upstream: "mock-primary", Version: "v1", Capabilities: []string{"chat", "stream"}, Context: 16000, MaxOutput: 128, InputMicro: 1000000, OutputMicro: 2000000, PriceVersion: "acceptance-v1", Currency: "CNY", FXPPM: 1000000}
		c.Models[model.ID] = model
		c.Routes["chat"] = Route{Alias: "chat", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{"primary", 100}}}
		for _, id := range []string{"token", "budget", "load", "load-a", "load-b", "redis-fault", "worker-fault", "gateway-fault", "before-reserve", "after-reserve", "before-http"} {
			a := App{ID: id, Active: true, Owner: "admin", Models: []string{"chat"}, BudgetAction: "warn"}
			if id == "token" {
				a.MonthlyTokens = 5 * (tokenRequest.InputEstimate + tokenRequest.MaxOutput)
			}
			if id == "budget" {
				a.BudgetAction = "reject"
				a.BudgetMicro = 5 * Cost(model, Usage{Input: budgetRequest.InputEstimate, Output: budgetRequest.MaxOutput})
			}
			c.Apps[id] = a
			c.Keys[id] = Key{ID: id, AppID: id, Hash: Hash(id + "-key"), State: "active"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.PublishConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	workerURL := startRole("worker", "worker", "1", "256m")
	ready(workerURL)
	gateways := []string{startRole("gateway-1", "gateway", "2", "256m"), startRole("gateway-2", "gateway", "2", "256m")}
	for _, url := range gateways {
		ready(url)
	}
	call := func(url, app, text string, stream bool) reliabilityResult {
		start := time.Now()
		req, _ := http.NewRequestWithContext(ctx, "POST", url+"/v1/chat/completions", bytes.NewReader(chatBody(text, stream)))
		req.Header.Set("Authorization", "Bearer "+app+"-key")
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		result := reliabilityResult{}
		if err == nil {
			result.Status, result.RequestID = res.StatusCode, res.Header.Get("X-Request-Id")
			reader := bufio.NewReader(res.Body)
			var first []byte
			if stream {
				first, err = reader.ReadBytes('\n')
				result.FirstMS = float64(time.Since(start).Microseconds()) / 1000
			}
			rest, readErr := io.ReadAll(reader)
			if err == nil {
				err = readErr
			}
			result.Body = string(append(first, rest...))
			_ = res.Body.Close()
		}
		if err != nil {
			result.Error = err.Error()
		}
		result.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
		return result
	}
	stats := func() (int64, map[string]int) {
		checkT.Helper()
		res, err := client.Get(upstreamURL + "/stats")
		if err != nil {
			checkT.Fatal(err)
		}
		defer res.Body.Close()
		var result struct {
			Calls int64          `json:"calls"`
			Held  map[string]int `json:"held"`
		}
		if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
			checkT.Fatal(err)
		}
		return result.Calls, result.Held
	}
	drain := func() {
		checkT.Helper()
		wait("SQL/Redis consumer catches up", 3*time.Minute, func() bool {
			caught, err := KafkaCaughtUp(ctx, r)
			return err == nil && caught
		})
	}
	reconcile := func(app string) Counters {
		checkT.Helper()
		groups, _, projectionErr := expectedQuotaFor(ctx, s, app)
		if projectionErr != nil {
			checkT.Fatal(projectionErr)
		}
		expected := groups[quotaKeys(app, month(time.Now()), "")[0]]
		actual, err := r.Engine.Guard.Counters(ctx, app, month(time.Now()))
		if err != nil || expected != actual {
			checkT.Fatalf("ledger/quota mismatch for %s: SQL=%+v Redis=%+v error=%v", app, expected, actual, err)
		}
		return actual
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
	singleConsumer := func() {
		wait("single active metering consumer", 45*time.Second, func() bool {
			groups, err := r.KafkaClient.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{run}})
			return err == nil && len(groups.Groups) == 1 && groups.Groups[0].Error == nil && groups.Groups[0].GroupState == "Stable" && len(groups.Groups[0].Members) == 1
		})
	}
	runCase("only_worker_joins_metering_group", func(t *testing.T) {
		singleConsumer()
	})
	runCase("worker_periodic_application_audit_persists_report", func(t *testing.T) {
		if _, err := s.DB.ExecContext(ctx, `UPDATE gw_metering_apps SET checked_at=UTC_TIMESTAMP(6)-INTERVAL 61 SECOND WHERE app_id='redis-fault'`); err != nil {
			t.Fatal(err)
		}
		wait("periodic successful audit report", 30*time.Second, func() bool {
			var count int
			err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_reports r JOIN gw_metering_apps a ON a.report_id=r.id WHERE r.app_id='redis-fault' AND r.actor='system' AND r.reason='periodic' AND r.passed=1 AND a.state='healthy'`).Scan(&count)
			return err == nil && count > 0
		})
	})
	for _, app := range []string{"token", "budget"} {
		runCase("two_instances_compete_for_"+app, func(t *testing.T) {
			results := make(chan reliabilityResult, 100)
			barrier := make(chan struct{})
			for i := 0; i < 100; i++ {
				go func(node int) {
					<-barrier
					results <- call(gateways[node%2], app, "hold-"+app, false)
				}(i)
			}
			close(barrier)
			rejected := 0
			for rejected < 95 {
				select {
				case result := <-results:
					if (result.Status != 429 && result.Status != 503) || result.Error != "" {
						t.Fatal("reservation race unexpectedly admitted or failed", result)
					}
					rejected++
				case <-time.After(20 * time.Second):
					t.Fatal("reservation race did not reject excess requests")
				}
			}
			wait("five reserved calls reach upstream", 10*time.Second, func() bool {
				_, held := stats()
				return held["hold-"+app] >= 1
			})
			_, held := stats()
			if held["hold-"+app] > 5 {
				t.Fatal("global quota exceeded", held)
			}
			res, err := client.Get(upstreamURL + "/release/hold-" + app)
			if err != nil {
				t.Fatal(err)
			}
			_ = res.Body.Close()
			successes := 0
			for range 5 {
				result := <-results
				if result.Error != "" || !Contains([]string{"200", "429", "503"}, strconv.Itoa(result.Status)) {
					t.Fatal(result)
				}
				if result.Status == 200 {
					successes++
				} else {
					rejected++
				}
			}
			if successes < 1 || successes > 5 {
				t.Fatal("budget oversold or no progress", successes)
			}
			drain()
			wait("cancel compensation drained", 40*time.Second, func() bool {
				var n int
				err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_attempts WHERE app_id=? AND (execution_phase='prepared' OR compensation_state='pending')`, app).Scan(&n)
				return err == nil && n == 0
			})
			if counts := reconcile(app); counts.Tokens != int64(32*successes) || counts.Micro != int64(52*successes) || counts.ReservedTokens != 0 {
				t.Fatal("successful attempts not settled exactly once", counts)
			}
		})
	}
	samples := []reliabilitySample{}
	acceptedTotal := 0
	runCase("bounded_pressure_and_stream_protocol", func(t *testing.T) {
		if os.Getenv("GW_RELIABILITY_SKIP_PRESSURE") == "1" {
			t.Skip("fault-only iteration; full acceptance must include pressure")
		}
		for _, stage := range []struct {
			vus, requests int
			stream        bool
		}{{10, 500, false}, {50, 1000, false}, {100, 2000, false}, {50, 200, true}} {
			ready(gateways[0])
			wait("load application exits audit barrier", time.Minute, func() bool {
				var state string
				err := s.DB.QueryRowContext(ctx, `SELECT state FROM gw_metering_apps WHERE app_id='load'`).Scan(&state)
				return err == nil && state == "healthy"
			})
			results := make(chan reliabilityResult, stage.requests)
			var next atomic.Int64
			start := time.Now()
			for i := 0; i < stage.vus; i++ {
				go func(node int) {
					for next.Add(1) <= int64(stage.requests) {
						results <- call(gateways[node%2], "load", "bounded pressure probe", stage.stream)
					}
				}(i)
			}
			latencies, firsts := []float64{}, []float64{}
			acceptedLatency := []float64{}
			failed, rejected := 0, 0
			for i := 0; i < stage.requests; i++ {
				result := <-results
				latencies = append(latencies, result.ElapsedMS)
				if result.Error == "" && result.Status == 503 && (strings.Contains(result.Body, `"governance_unavailable"`) || strings.Contains(result.Body, `"overloaded"`)) {
					rejected++
					continue
				}
				if result.Status != 200 || result.Error != "" || (stage.stream && !strings.HasSuffix(result.Body, "data: [DONE]\n\n")) || (!stage.stream && !strings.Contains(result.Body, `"choices"`)) {
					failed++
					if failed <= 3 {
						t.Logf("pressure failure: %+v", result)
					}
				} else {
					acceptedLatency = append(acceptedLatency, result.ElapsedMS)
					if stage.stream {
						firsts = append(firsts, result.FirstMS)
					}
				}
			}
			seconds := time.Since(start).Seconds()
			// 延迟必须区分已接受调用和保护性拒绝，不能把快速 503 当成容量成绩。
			sort.Float64s(latencies)
			sort.Float64s(firsts)
			percentile := func(values []float64, percent int) float64 { return values[(len(values)-1)*percent/100] }
			sample := reliabilitySample{Concurrency: stage.vus, Requests: stage.requests, Stream: stage.stream, Failed: failed, Seconds: seconds, RPS: float64(stage.requests) / seconds, P50MS: percentile(latencies, 50), P95MS: percentile(latencies, 95), P99MS: percentile(latencies, 99)}
			sample.Rejected = rejected
			sample.Accepted = stage.requests - rejected - failed
			acceptedTotal += sample.Accepted
			if sample.Accepted == 0 {
				t.Error("profile accepted no paid requests; cannot verify successful protocol under load")
			}
			sample.AcceptedRPS = float64(sample.Accepted) / seconds
			if len(acceptedLatency) > 0 {
				sort.Float64s(acceptedLatency)
				sample.AcceptedP95MS = percentile(acceptedLatency, 95)
				sample.AcceptedP99MS = percentile(acceptedLatency, 99)
			}
			if len(firsts) > 0 {
				sample.FirstP95MS = percentile(firsts, 95)
			}
			if failed != 0 {
				t.Errorf("%d protocol failures at concurrency %d", failed, stage.vus)
			}
			drainStart := time.Now()
			drain()
			sample.DrainSeconds = time.Since(drainStart).Seconds()
			samples = append(samples, sample)
			t.Logf("pressure: %+v", sample)
			reconcile("load")
			var paid int
			if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_attempts WHERE app_id='load' AND purpose<>'request' AND status<>'cancelled'`).Scan(&paid); err != nil || paid != acceptedTotal {
				t.Fatal("protected rejection generated paid usage", paid, acceptedTotal, err)
			}
			counts := reconcile("load")
			if counts.Tokens != int64(acceptedTotal)*32 || counts.Micro != int64(acceptedTotal)*52 || counts.ReservedTokens != 0 {
				t.Fatal("protected rejection incurred usage", counts, acceptedTotal)
			}
		}
		b, _ := json.MarshalIndent(samples, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "pressure.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
		for i, url := range append(append([]string{}, gateways...), workerURL) {
			res, err := client.Get(url + "/metrics")
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			name := "gateway-" + strconv.Itoa(i+1) + "-metrics.txt"
			if i == len(gateways) {
				name = "worker-metrics.txt"
			}
			if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
				t.Fatal(err)
			}
		}
	})
	runCase("fixed_arrival_multi_application_load", func(t *testing.T) {
		if os.Getenv("GW_RELIABILITY_SKIP_PRESSURE") == "1" {
			t.Skip("fault-only iteration; fixed-arrival profiles skipped")
		}
		profiles := []fixedArrivalSample{}
		accepted := 0
		type profileConfig struct {
			rate         int
			stream, slow bool
			duration     time.Duration
		}
		configs := []profileConfig{{2, false, false, 20 * time.Second}, {5, false, false, 20 * time.Second}, {10, false, false, 20 * time.Second}, {2, true, false, 20 * time.Second}, {2, false, true, 20 * time.Second}}
		if sustainedSeconds > 0 {
			configs = append(configs, profileConfig{2, false, false, time.Duration(sustainedSeconds) * time.Second})
		}
		metricURLs := append(append([]string{}, gateways...), workerURL)
		if err := captureAcceptanceMetrics(client, metricURLs, dir, "sustained-before"); err != nil {
			t.Fatal(err)
		}
		for profileIndex, profile := range configs {
			text := "fixed-arrival probe"
			if profile.slow {
				text = "delayed-probe"
			}
			stopSampling := sampleAcceptanceResources(ctx, dir, profileIndex, append(append([]string{}, gateways...), workerURL), []string{run + "-gateway-1", run + "-gateway-2", run + "-worker", run})
			sample := runFixedArrival(t, ctx, profile.rate, profile.duration, profile.stream, profile.slow, func(i int) reliabilityResult {
				app := "load-a"
				if i%2 == 1 {
					app = "load-b"
				}
				return call(gateways[i%2], app, text, profile.stream)
			})
			if err := stopSampling(); err != nil {
				t.Fatal(err)
			}
			accepted += sample.Accepted
			if sample.Failed != 0 || sample.Accepted == 0 {
				t.Fatalf("fixed-arrival protocol failed: %+v", sample)
			}
			start := time.Now()
			drain()
			sample.DrainSeconds = time.Since(start).Seconds()
			left, right := reconcile("load-a"), reconcile("load-b")
			if left.Tokens+right.Tokens != int64(accepted)*32 || left.Micro+right.Micro != int64(accepted)*52 || left.ReservedTokens+right.ReservedTokens != 0 || left.ReservedMicro+right.ReservedMicro != 0 {
				t.Fatal("fixed-arrival ledger/quota mismatch", left, right, accepted)
			}
			profiles = append(profiles, sample)
			if err := captureAcceptanceMetrics(client, metricURLs, dir, "profile-"+strconv.Itoa(profileIndex)+"-after"); err != nil {
				t.Fatal(err)
			}
			t.Logf("fixed-arrival: %+v", sample)
		}
		writeFixedArrivalSamples(t, dir, profiles)
		if err := s.AuditCapacity(ctx); err != nil {
			t.Fatal(err)
		}
	})
	runCase("killed_worker_replays_durable_backlog", func(t *testing.T) {
		drain()
		mustDocker("kill", run+"-worker")
		requests := []string{}
		before, _ := stats()
		for i := 0; i < 20; i++ {
			result := call(gateways[i%2], "worker-fault", "while worker is down", false)
			if result.Status == 503 && result.Error == "" {
				continue
			}
			if result.Status != 200 || result.Error != "" {
				t.Fatal(result)
			}
			requests = append(requests, result.RequestID)
		}
		time.Sleep(4 * time.Second)
		for i := 0; i < 4; i++ {
			result := call(gateways[i%2], "worker-fault", "expired permit", false)
			if result.Status != 503 || result.Error != "" {
				t.Fatal("worker loss did not expire admission", result)
			}
		}
		if after, _ := stats(); after-before != int64(len(requests)) {
			t.Fatal("expired permit called provider", before, after, len(requests))
		}
		for _, id := range requests {
			rows, err := s.Records(ctx, Filter{RequestID: id})
			if err != nil || len(rows) != 1 || rows[0].Kind != "started" || rows[0].Revision != 1 {
				t.Fatal("Arm must persist start; killed worker must not apply completion", rows, err)
			}
		}
		mustDocker("start", run+"-worker")
		ready(workerURL)
		singleConsumer()
		drain()
		if counts := reconcile("worker-fault"); counts.Tokens != int64(len(requests))*32 || counts.Micro != int64(len(requests))*52 || counts.ReservedTokens != 0 {
			t.Fatal("worker replay lost or duplicated billing", counts)
		}
	})
	runCase("Redis_process_loss_fails_closed_and_AOF_recovers", func(t *testing.T) {
		drain()
		before, _ := stats()
		mustDocker("kill", run)
		for i := 0; i < 4; i++ {
			result := call(gateways[i%2], "redis-fault", "Redis is down", false)
			if result.Status != 503 || result.Error != "" || result.ElapsedMS > 3000 {
				t.Fatal("Redis outage did not fail closed", result)
			}
		}
		if after, _ := stats(); after != before {
			t.Fatal("Redis outage called upstream", before, after)
		}
		mustDocker("start", run)
		wait("Redis AOF restores quota state", 15*time.Second, func() bool {
			state, err := r.Redis.Get(ctx, "gw:quota:state").Result()
			return err == nil && state == "ready"
		})
		wait("Redis restoration completes cancellation compensation", 60*time.Second, func() bool {
			var pending int
			err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_attempts WHERE execution_phase='prepared' OR compensation_state='pending'`).Scan(&pending)
			return err == nil && pending == 0
		})
		for _, app := range []string{"token", "budget", "load", "worker-fault"} {
			reconcile(app)
		}
		// AOF 恢复数据不等于重新获得接单许可；停写核验并切换代次后才恢复入口。
		mustDocker("stop", "--time", "5", run+"-gateway-1", run+"-gateway-2")
		drain()
		mustDocker("stop", "--time", "5", run+"-worker")
		wait("worker leaves group for audited resume", 30*time.Second, func() bool { return r.ensureGroupStopped(ctx) == nil })
		// 实际恢复 CLI 建立新连接，不沿用故障前 Redis 池中的失效 socket。
		fresh, err := OpenRuntime(ctx, false)
		if err != nil {
			t.Fatal("open fresh recovery runtime", err)
		}
		acceptanceT.Cleanup(func() { fresh.Close() })
		r = fresh
		t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
		t.Setenv("GW_METERING_ACTOR", "acceptance")
		t.Setenv("GW_METERING_REASON", "verify AOF restore before admission")
		if _, err := ResumeMetering(ctx, r); err != nil {
			t.Fatal("AOF evidence failed explicit resume", err)
		}
		if n, err := r.Redis.Exists(ctx, "gw:metering:permit").Result(); err != nil || n != 0 {
			t.Fatal("resume minted an admission permit without a worker")
		}
		mustDocker("start", run+"-worker")
		ready(workerURL)
		mustDocker("start", run+"-gateway-1", run+"-gateway-2")
		ready(gateways[0])
		ready(gateways[1])
		result := call(gateways[0], "redis-fault", "Redis recovered", false)
		if result.Status != 200 || result.Error != "" {
			t.Fatal(result)
		}
		drain()
		reconcile("redis-fault")
	})
	runCase("killed_gateway_interrupts_stream_and_preserves_pending", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, "POST", gateways[0]+"/v1/chat/completions", bytes.NewReader(chatBody("slow-stream", true)))
		req.Header.Set("Authorization", "Bearer gateway-fault-key")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		reader := bufio.NewReader(res.Body)
		first, err := reader.ReadString('\n')
		if err != nil || !strings.Contains(first, "first") {
			t.Fatal("stream did not start", first, err)
		}
		mustDocker("kill", run+"-gateway-1")
		rest, _ := io.ReadAll(reader)
		if strings.Contains(string(rest), "[DONE]") {
			t.Fatal("killed stream falsely completed")
		}
		drain()
		rows, err := s.Records(ctx, Filter{RequestID: res.Header.Get("X-Request-Id")})
		if err != nil || len(rows) != 1 || rows[0].Status != "started" || rows[0].Quality != "pending" {
			t.Fatal("crashed paid attempt was lost or falsely settled", rows, err)
		}
		if counts := reconcile("gateway-fault"); counts.ReservedTokens <= 0 || counts.Tokens != 0 {
			t.Fatal("pending usage was silently released", counts)
		}
		mustDocker("start", run+"-gateway-1")
		ready(gateways[0])
		result := call(gateways[0], "gateway-fault", "gateway recovered", false)
		if result.Status != 200 || result.Error != "" {
			t.Fatal(result)
		}
		drain()
		reconcile("gateway-fault")
	})
	for _, stage := range []string{"before-reserve", "after-reserve", "before-http"} {
		runCase("killed_gateway_"+stage, func(t *testing.T) {
			drain()
			probe := startRole("gateway-probe", "gateway-"+stage, "1", "256m")
			ready(probe)
			before, _ := stats()
			result := make(chan reliabilityResult, 1)
			go func() { result <- call(probe, stage, "boundary probe", false) }()
			wait("boundary reached", 10*time.Second, func() bool {
				res, err := client.Get(probe + "/__acceptance/paused")
				if err != nil {
					return false
				}
				defer res.Body.Close()
				var state struct {
					Paused bool `json:"paused"`
				}
				return json.NewDecoder(res.Body).Decode(&state) == nil && state.Paused
			})
			var raw []byte
			var phase string
			if err := s.DB.QueryRowContext(ctx, `SELECT start_payload,execution_phase FROM gw_attempts WHERE app_id=?`, stage).Scan(&raw, &phase); err != nil {
				t.Fatal(err)
			}
			start, err := decodeAttempt(raw)
			if err != nil {
				t.Fatal(err)
			}
			want := "prepared"
			if stage == "before-http" {
				want = "inflight"
			}
			if phase != want {
				t.Fatal("incorrect crash boundary", phase, want)
			}
			mustDocker("kill", run+"-gateway-probe")
			crashed := <-result
			if crashed.Status == 200 {
				t.Fatal("killed request falsely succeeded", crashed)
			}
			if after, _ := stats(); after != before {
				t.Fatal("paused HTTP reached provider", before, after)
			}
			if stage != "before-http" {
				// 真实后台轮询等到 Prepare 到期，再按 CAS 取消；不由测试直接清理。
				wait("prepared crash compensated", 45*time.Second, func() bool {
					var state string
					err := s.DB.QueryRowContext(ctx, `SELECT compensation_state FROM gw_attempts WHERE attempt_id=?`, start.AttemptID).Scan(&state)
					return err == nil && state == "done"
				})
				if counts := reconcile(stage); counts != (Counters{}) {
					t.Fatal("uncalled crash retained budget", counts)
				}
			} else {
				if err := r.CompensateOnce(ctx); err != nil {
					t.Fatal(err)
				}
				counts := reconcile(stage)
				if counts.ReservedTokens != start.ReserveTokens || counts.ReservedMicro != start.ReserveMicro {
					t.Fatal("Arm crash automatically refunded", counts)
				}
				// 测试 Transport 证明未发 HTTP，作为外部核实证据发布零费用完成。
				if err := r.Engine.Bus.Publish(ctx, cancelEvent(start)); err != nil {
					t.Fatal(err)
				}
				drain()
				if counts := reconcile(stage); counts != (Counters{}) {
					t.Fatal("verified correction did not release", counts)
				}
			}
			if err := s.AuditCapacity(ctx); err != nil {
				t.Fatal(err)
			}
			mustDocker("rm", "-f", "-v", run+"-gateway-probe")
		})
	}
	resources := mustDocker("stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}} {{.MemUsage}}", run+"-gateway-1", run+"-gateway-2", run+"-worker", run+"-upstream", run)
	if err := os.WriteFile(filepath.Join(dir, "resources.txt"), []byte(resources), 0600); err != nil {
		t.Fatal(err)
	}
}

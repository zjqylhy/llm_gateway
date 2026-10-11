package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type liteAcceptanceReport struct {
	Minutes       int                 `json:"requested_minutes"`
	Seconds       float64             `json:"sustained_seconds"`
	Accepted      int                 `json:"accepted"`
	Rejected      int                 `json:"protected_rejections"`
	Failures      int                 `json:"unexpected_failures"`
	AuditReports  int                 `json:"successful_periodic_reports"`
	DrainSeconds  float64             `json:"ledger_drain_seconds"`
	Counters      map[string]Counters `json:"final_counters"`
	ResourceScope string              `json:"resource_scope"`
	MemoryPeaks   map[string]int64    `json:"cgroup_memory_peak_bytes"`
	SQLPoolWaits  map[string]float64  `json:"sql_pool_wait_total"`
	Passed        bool                `json:"passed"`
}

// 只编入测试二进制；峰值包含初始化、文件缓存和采样进程本身的开销。
func TestLiteContainerSnapshot(t *testing.T) {
	if os.Getenv("GW_LITE_SNAPSHOT") != "1" {
		t.Skip("isolated acceptance resource sampler")
	}
	raw, err := os.ReadFile("/sys/fs/cgroup/memory.peak")
	if err != nil {
		t.Fatal("cgroup v2 memory.peak required", err)
	}
	peak, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("LITE_MEMORY_PEAK=%d\n", peak)
}

// 此验收只能由生成随机项目、独立卷和凭据的脚本启动；不连接既有部署。
func TestLiteInfrastructure(t *testing.T) {
	if os.Getenv("GW_RUN_LITE_TESTS") != "1" {
		t.Skip("run scripts/verify-lite.ps1")
	}
	project := os.Getenv("GW_LITE_PROJECT")
	if !regexp.MustCompile(`^gw_lite_[a-f0-9]{12}$`).MatchString(project) {
		t.Fatal("isolated lite project required")
	}
	minutes, err := strconv.Atoi(os.Getenv("GW_LITE_MINUTES"))
	if err != nil || minutes < 1 || minutes > 60 {
		t.Fatal("GW_LITE_MINUTES must be 1..60")
	}
	for _, name := range []string{"GW_MYSQL_DSN", "GW_REDIS_URL", "GW_KAFKA_BROKERS", "GW_ETCD_ENDPOINTS"} {
		if !strings.Contains(os.Getenv(name), "127.0.0.1:") {
			t.Fatal("local acceptance endpoint required", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(minutes+15)*time.Minute)
	defer cancel()
	dir := os.Getenv("GW_LITE_RUN_DIR")
	root := filepath.Clean(filepath.Join(dir, "..", "..", ".."))
	report := liteAcceptanceReport{Minutes: minutes, Counters: map[string]Counters{}, MemoryPeaks: map[string]int64{}, SQLPoolWaits: map[string]float64{}, ResourceScope: "individual container caps; Docker host is not constrained to 2 vCPU / 4 GiB; peak sampling adds a temporary Go process"}
	defer func() {
		raw, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "report.json"), raw, 0600); err != nil {
			t.Error(err)
		}
	}()
	docker := func(args ...string) string {
		t.Helper()
		commandCtx, done := context.WithTimeout(ctx, 90*time.Second)
		defer done()
		command := exec.CommandContext(commandCtx, "docker", args...)
		command.Dir = root
		raw, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s failed: %v: %s", args[0], err, raw)
		}
		return strings.TrimSpace(string(raw))
	}
	compose := func(args ...string) string {
		base := []string{"compose", "--env-file", os.Getenv("GW_LITE_ENV_FILE"), "-p", project, "-f", "deploy/lite/compose.yaml", "-f", "deploy/lite/acceptance.yaml"}
		return docker(append(base, args...)...)
	}
	// 每个故障操作之前核对容器确实属于本次随机项目。
	container := func(service string) string {
		t.Helper()
		id := compose("ps", "-aq", service)
		if id == "" || strings.Contains(id, "\n") || docker("inspect", "--format", `{{index .Config.Labels "com.docker.compose.project"}}`, id) != project {
			t.Fatal("unexpected fault target", service)
		}
		return id
	}
	_ = os.WriteFile(filepath.Join(dir, "host.txt"), []byte(docker("info", "--format", "cpu={{.NCPU}} memory_bytes={{.MemTotal}}")), 0600)
	r, err := OpenRuntime(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { r.Close() }()
	if !r.Engine.CacheDisabled || r.Engine.Cache != nil {
		t.Fatal("lite runtime constructed cache infrastructure")
	}
	s := r.Engine.Store.(*SQLStore)
	if s.DB.Stats().MaxOpenConnections != 12 || r.Redis.Options().PoolSize != 8 {
		t.Fatal("lite pools not applied")
	}
	cfg, err := s.Update(ctx, "acceptance", "lite.seed", func(c *Config) error {
		c.Providers["primary"] = Provider{"primary", "mock", "http://upstream:8080/v1", r.Engine.Vault.Encrypt("primary", "acceptance")}
		for _, id := range []string{"primary", "embedding"} {
			caps, dimensions := []string{"chat", "stream"}, 0
			if id == "embedding" {
				caps, dimensions = []string{"embedding"}, 32
			}
			c.Models[id] = Model{ID: id, ProviderID: "primary", Upstream: "mock-" + id, Version: "v1", Capabilities: caps, Context: 262144, MaxOutput: 65536, Dimensions: dimensions, InputMicro: 1000000, OutputMicro: 2000000, PriceVersion: "acceptance-v1", Currency: "CNY", FXPPM: 1000000}
		}
		c.Routes["chat"] = Route{Alias: "chat", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{"primary", 100}}}
		c.Routes["embedding"] = Route{Alias: "embedding", Kind: "embedding", Strategy: "priority", Candidates: []Candidate{{"embedding", 100}}}
		for _, id := range []string{"lite-a", "lite-b", "fault"} {
			c.Apps[id] = App{ID: id, Owner: "admin", Active: true, Models: []string{"chat", "embedding"}, BudgetAction: "warn", Cache: true, CacheTTL: 300, Similarity: .97, EmbeddingModel: "embedding"}
			c.Keys[id] = Key{ID: id, AppID: id, Hash: Hash(id + "-key"), State: "active"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Engine.Publisher.PublishConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	compose("up", "-d", "gateway", "admin", "worker", "edge-local")
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 35 * time.Second}
	defer client.CloseIdleConnections()
	url := "http://127.0.0.1:" + os.Getenv("GW_LITE_GATEWAY_PORT")
	callURL, callClient := url, client
	upstream := "http://127.0.0.1:" + os.Getenv("GW_LITE_UPSTREAM_PORT")
	wait := func(label string, timeout time.Duration, check func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for !check() {
			if time.Now().After(deadline) || ctx.Err() != nil {
				var state, reason string
				_ = s.DB.QueryRowContext(ctx, `SELECT state,reason FROM gw_metering_control WHERE id=1`).Scan(&state, &reason)
				t.Fatal("timeout:", label, "metering:", state, reason)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	ready := func() {
		wait("gateway readiness", time.Minute, func() bool {
			res, err := client.Get(url + "/readyz")
			if err != nil {
				return false
			}
			defer res.Body.Close()
			return res.StatusCode == 200
		})
	}
	ready()
	drain := func() {
		wait("ledger catches up", 3*time.Minute, func() bool {
			caught, err := KafkaCaughtUp(ctx, r)
			var pending int
			se := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_outbox WHERE delivered=0`).Scan(&pending)
			return err == nil && se == nil && caught && pending == 0
		})
	}
	reconcile := func(app string) Counters {
		t.Helper()
		var expected Counters
		err := s.DB.QueryRowContext(ctx, `SELECT COALESCE(SUM(tokens),0),COALESCE(SUM(cost_micro),0),COALESCE(SUM(IF(status='started',CAST(JSON_EXTRACT(payload,'$.reserve_tokens') AS SIGNED),0)),0),COALESCE(SUM(IF(status='started',CAST(JSON_EXTRACT(payload,'$.reserve_micro') AS SIGNED),0)),0) FROM gw_attempts WHERE app_id=? AND purpose<>'request'`, app).Scan(&expected.Tokens, &expected.Micro, &expected.ReservedTokens, &expected.ReservedMicro)
		actual, ge := r.Engine.Guard.Counters(ctx, app, month(time.Now()))
		if err != nil || ge != nil || expected != actual {
			t.Fatal("SQL/Redis mismatch", app, expected, actual, err, ge)
		}
		return actual
	}
	call := func(app, text string, stream bool) reliabilityResult {
		start := time.Now()
		raw, _ := json.Marshal(map[string]any{"model": "chat", "messages": []any{map[string]string{"role": "user", "content": text}}, "stream": stream, "max_tokens": 32768, "stream_options": map[string]bool{"include_usage": true}})
		req, _ := http.NewRequestWithContext(ctx, "POST", callURL+"/v1/chat/completions", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+app+"-key")
		req.Header.Set("Content-Type", "application/json")
		res, err := callClient.Do(req)
		result := reliabilityResult{}
		if err == nil {
			result.Status, result.RequestID = res.StatusCode, res.Header.Get("X-Request-Id")
			body, re := io.ReadAll(res.Body)
			_ = res.Body.Close()
			result.Body, err = string(body), re
			if res.Header.Get("X-Gateway-Cache") == "hit" {
				result.Error = "disabled cache returned hit"
			}
		}
		if err != nil {
			result.Error = err.Error()
		}
		result.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
		return result
	}
	check := func(result reliabilityResult, stream bool) {
		t.Helper()
		if result.Status != 200 || result.Error != "" || stream && !strings.HasSuffix(result.Body, "data: [DONE]\n\n") || !stream && !strings.Contains(result.Body, `"choices"`) {
			t.Fatalf("request failed: status=%d error=%s body=%.500s", result.Status, result.Error, result.Body)
		}
	}
	// 验证 Caddy、本地 CA、Secure Cookie 和新增能力信息，避免只检查 API 进程。
	wait("Caddy local CA", 30*time.Second, func() bool {
		cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "--env-file", os.Getenv("GW_LITE_ENV_FILE"), "-p", project, "-f", "deploy/lite/compose.yaml", "-f", "deploy/lite/acceptance.yaml"}, "exec", "-T", "edge-local", "cat", "/data/caddy/pki/authorities/local/root.crt")...)
		cmd.Dir = root
		raw, err := cmd.Output()
		return err == nil && os.WriteFile(filepath.Join(dir, "local-ca.crt"), raw, 0600) == nil
	})
	ca, _ := os.ReadFile(filepath.Join(dir, "local-ca.crt"))
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid local CA")
	}
	jar, _ := cookiejar.New(nil)
	console := &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Jar: jar, Timeout: 35 * time.Second}
	defer console.CloseIdleConnections()
	origin := os.Getenv("GW_ADMIN_ORIGIN")
	callURL, callClient = origin, console
	login, _ := json.Marshal(map[string]string{"username": "admin", "password": os.Getenv("GW_ADMIN_PASSWORD")})
	req, _ := http.NewRequest("POST", origin+"/admin/login", bytes.NewReader(login))
	req.Header.Set("Origin", origin)
	res, err := console.Do(req)
	if err != nil {
		t.Fatal("HTTPS login", err)
	}
	_ = res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal("HTTPS login", res.StatusCode)
	}
	res, err = console.Get(origin + "/admin/config")
	if err != nil {
		t.Fatal(err)
	}
	var view struct {
		Config
		Capabilities map[string]bool `json:"runtime_capabilities"`
	}
	err = json.NewDecoder(res.Body).Decode(&view)
	_ = res.Body.Close()
	if err != nil || res.StatusCode != 200 || view.Capabilities["cache_enabled"] || !view.Apps["lite-a"].Cache {
		t.Fatal("admin capability/preserved policy", err)
	}
	res, err = console.Get(origin + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if res.StatusCode != 200 || !bytes.Contains(page, []byte("/assets/")) {
		t.Fatal("frontend assets unavailable")
	}
	for _, stream := range []bool{false, true} {
		check(call("lite-a", strings.Repeat("large input ", 6000), stream), stream)
	}
	req, _ = http.NewRequest("POST", callURL+"/v1/embeddings", strings.NewReader(`{"model":"embedding","input":"direct embedding"}`))
	req.Header.Set("Authorization", "Bearer lite-a-key")
	res, err = callClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	embedding, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if res.StatusCode != 200 || !bytes.Contains(embedding, []byte(`"embedding"`)) {
		t.Fatal("direct embedding unavailable", string(embedding))
	}
	// 两路持有请求占满门禁，第三路必须在调用上游之前拒绝。
	results := make(chan reliabilityResult, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- call("fault", "hold-lite", false) }()
	}
	wait("two held upstream calls", 20*time.Second, func() bool {
		res, err := client.Get(upstream + "/stats")
		if err != nil {
			return false
		}
		defer res.Body.Close()
		var stats struct {
			Held map[string]int `json:"held"`
		}
		return json.NewDecoder(res.Body).Decode(&stats) == nil && stats.Held["hold-lite"] == 2
	})
	if result := call("fault", "excess", false); result.Status != 503 || !strings.Contains(result.Body, "overloaded") {
		t.Fatal("overload gate failed", result)
	}
	res, err = client.Get(upstream + "/release/hold-lite")
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	for i := 0; i < 2; i++ {
		check(<-results, false)
	}
	// 中途断开流不能出现成功终止标记，后台仍需保存已发生的估算计量。
	req, _ = http.NewRequest("POST", callURL+"/v1/chat/completions", strings.NewReader(`{"model":"chat","messages":[{"role":"user","content":"slow-stream"}],"stream":true,"max_tokens":64}`))
	req.Header.Set("Authorization", "Bearer fault-key")
	res, err = callClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(res.Body).ReadString('\n')
	_ = res.Body.Close()
	if err != nil || !strings.Contains(line, "first") {
		t.Fatal("disconnect stream did not start", line, err)
	}
	drain()
	resources, err := os.OpenFile(filepath.Join(dir, "resources.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Close()
	services := []string{"mysql", "redis", "kafka", "etcd", "gateway", "admin", "worker", "edge-local"}
	ids := []string{}
	for _, name := range services {
		ids = append(ids, container(name))
	}
	sample := func() {
		_, _ = fmt.Fprintln(resources, docker(append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, ids...)...))
		for _, id := range ids {
			state := docker("inspect", "--format", "{{.State.Running}} {{.State.OOMKilled}} {{.RestartCount}}", id)
			if state != "true false 0" {
				t.Fatal("unexpected restart or OOM", id, state)
			}
		}
	}
	t.Log("sustained phase: two clients, ordinary/SSE and delayed upstream; resource snapshots every 30 seconds")
	start, nextSample := time.Now(), time.Now()
	for time.Since(start) < time.Duration(minutes)*time.Minute {
		if time.Now().After(nextSample) {
			sample()
			nextSample = time.Now().Add(30 * time.Second)
		}
		var wg sync.WaitGroup
		batch := make(chan struct {
			result reliabilityResult
			stream bool
		}, 2)
		for i, app := range []string{"lite-a", "lite-b"} {
			wg.Add(1)
			go func(app string, stream bool) {
				defer wg.Done()
				batch <- struct {
					result reliabilityResult
					stream bool
				}{call(app, "delayed-probe", stream), stream}
			}(app, i == 1)
		}
		wg.Wait()
		close(batch)
		for item := range batch {
			if item.result.Status == 503 && item.result.Error == "" && strings.Contains(item.result.Body, "governance_unavailable") {
				var frozen int
				if err := s.DB.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM gw_metering_control WHERE state='frozen')+(SELECT COUNT(*) FROM gw_metering_apps WHERE state='frozen')`).Scan(&frozen); err != nil || frozen != 0 {
					t.Fatal("sustained metering unexpectedly frozen", frozen, err)
				}
				if count, err := r.Redis.Exists(ctx, "gw:metering:frozen").Result(); err != nil || count != 0 {
					t.Fatal("sustained Redis freeze", count, err)
				}
				report.Rejected++
			} else if item.result.Status != 200 || item.result.Error != "" || item.stream && !strings.HasSuffix(item.result.Body, "data: [DONE]\n\n") {
				report.Failures++
				t.Fatalf("sustained unexpected failure: %+v", item.result)
			} else {
				report.Accepted++
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	report.Seconds = time.Since(start).Seconds()
	sample()
	for i, id := range ids {
		out := docker("exec", "-e", "GW_LITE_SNAPSHOT=1", id, "/acceptance/gateway.test", "-test.run=^TestLiteContainerSnapshot$", "-test.v")
		matches := regexp.MustCompile(`LITE_MEMORY_PEAK=(\d+)`).FindStringSubmatch(out)
		if len(matches) != 2 {
			t.Fatal("resource sample unavailable", services[i], out)
		}
		peak, _ := strconv.ParseInt(matches[1], 10, 64)
		report.MemoryPeaks[services[i]] = peak
	}
	_ = os.WriteFile(filepath.Join(dir, "host-meminfo.txt"), []byte(docker("run", "--rm", "--network", "none", "alpine:3.23", "cat", "/proc/meminfo")), 0600)
	for role, endpoint := range map[string]string{"gateway": "gateway:8080", "admin": "admin:8081", "worker": "worker:8080"} {
		metrics := compose("exec", "-T", "edge-local", "wget", "-q", "-O", "-", "http://"+endpoint+"/metrics")
		if err := os.WriteFile(filepath.Join(dir, role+".prom"), []byte(metrics), 0600); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(metrics, "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[0] == "gateway_sql_pool_wait_total" {
				report.SQLPoolWaits[role], err = strconv.ParseFloat(fields[1], 64)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, ok := report.SQLPoolWaits[role]; !ok {
			t.Fatal("SQL pool diagnostic missing", role)
		}
	}
	drainStart := time.Now()
	drain()
	report.DrainSeconds = time.Since(drainStart).Seconds()
	for _, app := range []string{"lite-a", "lite-b", "fault"} {
		report.Counters[app] = reconcile(app)
	}
	var internalEmbeddings int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_attempts WHERE purpose='cache_embedding'`).Scan(&internalEmbeddings); err != nil || internalEmbeddings != 0 {
		t.Fatal("cache embedding was executed", internalEmbeddings, err)
	}
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_reports WHERE actor='system' AND reason='periodic' AND passed=1`).Scan(&report.AuditReports); err != nil {
		t.Fatal(err)
	}
	if minutes >= 2 && report.AuditReports == 0 {
		t.Fatal("periodic audits did not execute")
	}
	if report.Accepted == 0 {
		t.Fatal("no sustained successful requests")
	}
	t.Logf("sustained seconds=%.1f accepted=%d audit rejections=%d reports=%d", report.Seconds, report.Accepted, report.Rejected, report.AuditReports)
	// 强杀 worker 时已确认事件留在 Kafka；重启后严格对账并确认不重复计费。
	docker("kill", container("worker"))
	before := reconcile("fault")
	accepted := 0
	for i := 0; i < 4; i++ {
		result := call("fault", "worker stopped", false)
		if result.Status == 200 {
			accepted++
			check(result, false)
		} else if result.Status != 503 {
			t.Fatal(result)
		}
	}
	time.Sleep(4 * time.Second)
	if result := call("fault", "expired permit", false); result.Status != 503 {
		t.Fatal("worker loss admitted request", result)
	}
	compose("start", "worker")
	ready()
	drain()
	after := reconcile("fault")
	if after.Tokens != before.Tokens+int64(accepted)*32 || after.Micro != before.Micro+int64(accepted)*52 {
		t.Fatal("worker replay lost or doubled usage", before, after, accepted)
	}
	// Redis 故障在停写窗口触发；恢复 AOF 后执行生产停写核验流程。
	compose("stop", "-t", "5", "gateway", "admin")
	drain()
	compose("stop", "-t", "5", "worker")
	wait("worker leaves group", 45*time.Second, func() bool { return r.ensureGroupStopped(ctx) == nil })
	docker("kill", container("redis"))
	compose("start", "redis")
	wait("AOF ready", 30*time.Second, func() bool {
		state, err := r.Redis.Get(ctx, "gw:quota:state").Result()
		return err == nil && state == "ready"
	})
	r.Close()
	r, err = OpenRuntime(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	s = r.Engine.Store.(*SQLStore)
	t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
	t.Setenv("GW_METERING_ACTOR", "acceptance")
	t.Setenv("GW_METERING_REASON", "lite AOF restart validation")
	if _, err = ResumeMetering(ctx, r); err != nil {
		t.Fatal("restart audit/resume", err)
	}
	compose("start", "worker", "gateway", "admin")
	ready()
	check(call("fault", "restart preserved configuration", false), false)
	drain()
	for _, app := range []string{"lite-a", "lite-b", "fault"} {
		report.Counters[app] = reconcile(app)
	}
	// 最后用真正的三个生产入口运行历史配置，验证启动和健康，不调用私网模拟上游。
	compose("stop", "-t", "5", "gateway", "admin", "worker")
	production := []string{"compose", "--env-file", os.Getenv("GW_LITE_ENV_FILE"), "-p", project, "-f", "deploy/lite/compose.yaml"}
	docker(append(production, "up", "-d", "gateway", "admin", "worker")...)
	for _, endpoint := range []string{"gateway:8080", "admin:8081", "worker:8082"} {
		wait("production readiness "+endpoint, time.Minute, func() bool {
			command := exec.CommandContext(ctx, "docker", append(production, "exec", "-T", "edge-local", "wget", "-q", "-O", "-", "http://"+endpoint+"/readyz")...)
			command.Dir = root
			return command.Run() == nil
		})
	}
	wait("production role health", time.Minute, func() bool {
		res, err := console.Get(origin + "/healthz")
		if err != nil {
			return false
		}
		defer res.Body.Close()
		return res.StatusCode == 200
	})
	compose("stop", "-t", "10", "gateway", "admin", "worker")
	report.Passed = true
	t.Log("PASS: cache-free runtime, HTTPS admin, protocol, ledger, periodic audits and isolated restarts; whole-machine and cloud acceptance still required")
}

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
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type capacityLoadSample struct {
	Mode         string  `json:"mode"`
	Scope        string  `json:"scope"`
	AppMiB       int64   `json:"application_mib"`
	GlobalMiB    int64   `json:"global_mib"`
	Phase        string  `json:"phase"`
	Rate         int     `json:"offered_rps"`
	Seconds      int     `json:"offered_seconds"`
	Requests     int     `json:"requests"`
	Accepted     int     `json:"accepted"`
	Rejected     int     `json:"rejected"`
	Failed       int     `json:"failed"`
	Dropped      int     `json:"scheduler_dropped"`
	P50          float64 `json:"accepted_p50_ms"`
	P95          float64 `json:"accepted_p95_ms"`
	P99          float64 `json:"accepted_p99_ms"`
	FirstP95     float64 `json:"accepted_first_event_p95_ms,omitempty"`
	DoneP95      float64 `json:"accepted_done_p95_ms,omitempty"`
	Wall         float64 `json:"wall_seconds"`
	SuccessRPS   float64 `json:"accepted_per_wall_second"`
	Drain        float64 `json:"drain_seconds"`
	Peak         int64   `json:"peak_global_bytes"`
	PeakApp      int64   `json:"peak_application_bytes"`
	Over         int64   `json:"peak_over_global_bytes"`
	OverApp      int64   `json:"peak_over_application_bytes"`
	OverSeconds  float64 `json:"sampled_over_limit_seconds"`
	BacklogFirst int64   `json:"first_half_mean_bytes"`
	BacklogLast  int64   `json:"second_half_mean_bytes"`
	Passed       bool    `json:"meets_comparison_threshold"`
}

// 容器、schema、topic 和角色环境都由 verify-capacity.ps1 随机创建。
func TestCapacityIntegration(t *testing.T) {
	if os.Getenv("GW_RUN_CAPACITY_TESTS") != "1" {
		t.Skip("run scripts/verify-capacity.ps1")
	}
	run := os.Getenv("GW_INFRA_RUN_ID")
	dsn, err := mysql.ParseDSN(os.Getenv("GW_MYSQL_DSN"))
	if err != nil || !regexp.MustCompile(`^gw_capacity_[a-f0-9]{12}_[a-z]+$`).MatchString(run) || dsn.DBName != run || !strings.HasPrefix(dsn.Addr, "127.0.0.1:") || os.Getenv("GW_KAFKA_TOPIC") != run || os.Getenv("GW_ETCD_PREFIX") != "/"+run {
		t.Fatal("dedicated capacity resources required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	r, err := OpenRuntime(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.Engine.Store.(*SQLStore)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; ; attempt++ {
		err = InitializeCheckpoints(ctx, r, false)
		if err == nil {
			err = Bootstrap(ctx, r.Engine)
		}
		if err == nil {
			break
		}
		if attempt >= 7 || !(strings.Contains(err.Error(), "Coordinator") || strings.Contains(err.Error(), "coordinator")) {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}
	dir := os.Getenv("GW_RELIABILITY_DIRECTORY")
	t.Run("SQL_snapshot_and_indexes", func(t *testing.T) { testCapacitySnapshotSQL(t, ctx, s, dir) })
	t.Run("mode_switch_and_rollback", func(t *testing.T) { testCapacityModeSQL(t, ctx, s) })
	if t.Failed() {
		return
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 256, Proxy: nil}, Timeout: 65 * time.Second}
	defer client.CloseIdleConnections()
	docker := func(args ...string) string {
		t.Helper()
		c, done := context.WithTimeout(ctx, 40*time.Second)
		defer done()
		raw, e := exec.CommandContext(c, "docker", args...).CombinedOutput()
		if e != nil {
			t.Fatalf("docker %s: %v: %s", args[0], e, raw)
		}
		return strings.TrimSpace(string(raw))
	}
	roles := map[string]string{}
	stop := func(suffix string) {
		if name := roles[suffix]; name != "" {
			raw, _ := exec.Command("docker", "logs", name).CombinedOutput()
			_ = os.WriteFile(filepath.Join(dir, suffix+".log"), raw, 0600)
			docker("stop", "--time", "5", name)
			docker("rm", "-v", name)
			delete(roles, suffix)
		}
	}
	defer func() {
		for suffix := range roles {
			stop(suffix)
		}
	}()
	start := func(suffix, role string, global int64) string {
		listener, e := net.Listen("tcp4", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		binding := listener.Addr().String()
		listener.Close()
		name := run + "-" + suffix
		nodeAppLimit := int64(256 << 20)
		if raw := os.Getenv("GW_CAPACITY_CASE_INDEX"); raw != "" {
			if i, e := strconv.Atoi(raw); e == nil && i >= 0 && i < 6 {
				nodeAppLimit = []int64{8, 64, 256, 256, 256, 256}[i] << 20
			}
		}
		cpus, memory := "2", "256m"
		if role == "worker" {
			cpus = "1"
		}
		args := []string{"run", "--detach", "--name", name, "--label", "llm-gateway.capacity=" + run, "--network", os.Getenv("GW_RELIABILITY_NETWORK"), "--cpus", cpus, "--memory", memory, "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--env-file", os.Getenv("GW_RELIABILITY_ENV_FILE"), "-e", "GW_RELIABILITY_ROLE=" + role, "-e", "HOSTNAME=" + name, "-e", "GW_OUTBOX_GLOBAL_BYTES=" + strconv.FormatInt(global, 10), "-e", "GW_OUTBOX_APP_BYTES=" + strconv.FormatInt(nodeAppLimit, 10), "-p", binding + ":8080", "--mount", "type=bind,src=" + dir + ",dst=/acceptance,readonly", "--entrypoint", "/acceptance/gateway.test", os.Getenv("GW_RELIABILITY_IMAGE"), "-test.run=^TestReliabilityProcess$", "-test.timeout=90m", "-test.v"}
		docker(args...)
		roles[suffix] = name
		return "http://" + binding
	}
	wait := func(label string, deadline time.Duration, check func() bool) {
		t.Helper()
		until := time.Now().Add(deadline)
		for !check() {
			if time.Now().After(until) || ctx.Err() != nil {
				t.Fatal("timeout: " + label)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	ready := func(url string) {
		wait("ready "+url, 45*time.Second, func() bool {
			res, e := client.Get(url + "/readyz")
			if e != nil {
				return false
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			return res.StatusCode == 200
		})
	}
	upstream := start("upstream", "upstream", 1024<<20)
	ready(upstream)
	apps := []string{"cap-a", "cap-b", "cap-c", "cap-d", "cap-e", "cap-f", "cap-g", "cap-h"}
	seedCfg, err := s.Update(ctx, "capacity-test", "seed", func(c *Config) error {
		c.Providers["primary"] = Provider{"primary", "mock", "http://" + roles["upstream"] + ":8080/v1", r.Engine.Vault.Encrypt("primary", "capacity-test")}
		c.Models["primary"] = Model{ID: "primary", ProviderID: "primary", Upstream: "mock-primary", Version: "v1", Capabilities: []string{"chat", "stream"}, Context: 16000, MaxOutput: 128, InputMicro: 1000000, OutputMicro: 2000000, PriceVersion: "capacity-v1", Currency: "CNY", FXPPM: 1000000}
		c.Routes["chat"] = Route{Alias: "chat", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{"primary", 100}}}
		for _, app := range apps {
			c.Apps[app] = App{ID: app, Active: true, Owner: "admin", Models: []string{"chat"}, BudgetAction: "warn", OutboxBytes: 256 << 20}
			c.Keys[app] = Key{ID: app, AppID: app, Hash: Hash(app + "-key"), State: "active"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Engine.Publisher.PublishConfig(ctx, seedCfg); err != nil {
		t.Fatal(err)
	}
	workerLimit := int64(1024 << 20)
	if raw := os.Getenv("GW_CAPACITY_CASE_INDEX"); raw != "" {
		if index, e := strconv.Atoi(raw); e == nil && index >= 0 && index < 6 {
			workerLimit = []int64{1024, 1024, 1024, 64, 512, 1024}[index] << 20
		}
	}
	worker := start("worker", "worker", workerLimit)
	ready(worker)
	wait("application controls", 20*time.Second, func() bool {
		var n int
		err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_apps WHERE app_id LIKE 'cap-%' AND state='healthy'`).Scan(&n)
		return err == nil && n == len(apps)
	})
	drain := func() float64 {
		at := time.Now()
		wait("capacity drained", 40*time.Second, func() bool {
			snap, e := s.sampleCapacity(ctx, nil)
			if e != nil || snap.Global != 0 {
				return false
			}
			var n int
			_ = s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_outbox WHERE delivered=0`).Scan(&n)
			return n == 0
		})
		return time.Since(at).Seconds()
	}
	mode := os.Getenv("GW_CAPACITY_TEST_MODE")
	seconds, _ := strconv.Atoi(os.Getenv("GW_CAPACITY_TEST_SECONDS"))
	if seconds < 1 || seconds > 60 {
		t.Fatal("invalid measurement duration")
	}
	quick := os.Getenv("GW_CAPACITY_TEST_QUICK") == "1"
	all := []capacityLoadSample{}
	write := func() {
		raw, e := json.MarshalIndent(all, "", "  ")
		if e == nil {
			e = os.WriteFile(filepath.Join(dir, "capacity-results.json"), raw, 0600)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	configs := []struct {
		scope        string
		app, global  int64
		applications int
	}{{"application", 8, 1024, 1}, {"application", 64, 1024, 1}, {"application", 256, 1024, 1}, {"global", 256, 64, 8}, {"global", 256, 512, 8}, {"global", 256, 1024, 8}}
	if quick {
		configs = configs[:1]
	}
	selectedCase := -1
	if raw := os.Getenv("GW_CAPACITY_CASE_INDEX"); raw != "" {
		selectedCase, err = strconv.Atoi(raw)
		if err != nil || selectedCase < 0 || selectedCase >= 6 {
			t.Fatal("invalid capacity case index")
		}
	}
	for ci, cfg := range configs {
		if selectedCase >= 0 && ci != selectedCase {
			continue
		}
		if ci > 0 {
			stop("gateway-1")
			stop("gateway-2")
		}
		drain()
		nextCfg, err := s.Update(ctx, "capacity-test", "set-limit", func(c *Config) error {
			for _, id := range apps {
				a := c.Apps[id]
				a.OutboxBytes = cfg.app << 20
				c.Apps[id] = a
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = r.Engine.Publisher.PublishConfig(ctx, nextCfg); err != nil {
			t.Fatal(err)
		}
		gateways := []string{start("gateway-1", "gateway", cfg.global<<20), start("gateway-2", "gateway", cfg.global<<20)}
		for _, url := range gateways {
			ready(url)
		}
		call := func(i int, stream, slow bool) reliabilityResult {
			at := time.Now()
			text := "capacity probe"
			if slow {
				text = "delayed-probe"
			}
			raw, _ := json.Marshal(map[string]any{"model": "chat", "messages": []map[string]string{{"role": "user", "content": text}}, "max_tokens": 64, "stream": stream, "stream_options": map[string]bool{"include_usage": true}})
			req, _ := http.NewRequestWithContext(ctx, "POST", gateways[i%2]+"/v1/chat/completions", bytes.NewReader(raw))
			req.Header.Set("Authorization", "Bearer "+apps[i%cfg.applications]+"-key")
			req.Header.Set("Content-Type", "application/json")
			res, e := client.Do(req)
			if e != nil {
				return reliabilityResult{Error: e.Error(), ElapsedMS: float64(time.Since(at).Microseconds()) / 1000}
			}
			result := reliabilityResult{Status: res.StatusCode}
			if stream && res.StatusCode == 200 {
				reader := bufio.NewReader(res.Body)
				var body strings.Builder
				for {
					line, readErr := reader.ReadString('\n')
					body.WriteString(line)
					if strings.HasPrefix(line, "data:") && result.FirstMS == 0 {
						result.FirstMS = float64(time.Since(at).Microseconds()) / 1000
					}
					if strings.TrimSpace(line) == "data: [DONE]" {
						result.DoneMS = float64(time.Since(at).Microseconds()) / 1000
					}
					if readErr != nil {
						if readErr != io.EOF {
							result.Error = readErr.Error()
						}
						break
					}
				}
				result.Body = body.String()
			} else {
				body, readErr := io.ReadAll(res.Body)
				result.Body = string(body)
				if readErr != nil {
					result.Error = readErr.Error()
				}
			}
			res.Body.Close()
			result.ElapsedMS = float64(time.Since(at).Microseconds()) / 1000
			return result
		}
		var runLoad func(int, int, string, bool, bool) capacityLoadSample
		loadIndex := ci * 100
		runLoad = func(rate, duration int, phase string, stream, slow bool) capacityLoadSample {
			result := capacityLoadSample{Mode: mode, Scope: cfg.scope, AppMiB: cfg.app, GlobalMiB: cfg.global, Phase: phase, Rate: rate, Seconds: duration, Requests: rate * duration}
			prefix := fmt.Sprintf("case-%d-%s-%d", ci, phase, rate)
			metricURLs := append(append([]string{}, gateways...), worker)
			if err := captureAcceptanceMetrics(client, metricURLs, dir, prefix+"-before"); err != nil {
				t.Fatal(err)
			}
			stopResources := sampleAcceptanceResources(ctx, dir, loadIndex, metricURLs, []string{roles["gateway-1"], roles["gateway-2"], roles["worker"], roles["upstream"]})
			loadIndex++
			firstN, lastN := int64(0), int64(0)
			startTime := time.Now()
			sampleCtx, endSample := context.WithCancel(ctx)
			sampleDone := make(chan struct{})
			go func() {
				defer close(sampleDone)
				for {
					c, done := context.WithTimeout(sampleCtx, time.Second)
					snap, e := s.sampleCapacity(c, nil)
					done()
					if e == nil {
						elapsed := time.Since(startTime)
						if elapsed < time.Duration(duration)*time.Second/2 {
							result.BacklogFirst += snap.Global
							firstN++
						} else if elapsed < time.Duration(duration)*time.Second {
							result.BacklogLast += snap.Global
							lastN++
						}
						result.Peak = max(result.Peak, snap.Global)
						var appPeak int64
						for _, v := range snap.Apps {
							appPeak = max(appPeak, v)
						}
						result.PeakApp = max(result.PeakApp, appPeak)
						if snap.Global > cfg.global<<20 || appPeak > cfg.app<<20 {
							result.OverSeconds += .2
						}
					}
					select {
					case <-sampleCtx.Done():
						return
					case <-time.After(200 * time.Millisecond):
					}
				}
			}()
			responses := make(chan reliabilityResult, result.Requests)
			slots := make(chan struct{}, 256)
			for i := 0; i < result.Requests; i++ {
				due := startTime.Add(time.Duration(i) * time.Second / time.Duration(rate))
				if d := time.Until(due); d > 0 {
					time.Sleep(d)
				}
				select {
				case slots <- struct{}{}:
					go func(i int) { defer func() { <-slots }(); responses <- call(i, stream, slow) }(i)
				default:
					result.Dropped++
				}
			}
			latency := []float64{}
			firsts, dones := []float64{}, []float64{}
			for i := 0; i < result.Requests-result.Dropped; i++ {
				v := <-responses
				if v.Error != "" {
					result.Failed++
				} else if v.Status == 503 {
					result.Rejected++
				} else if v.Status != 200 || !stream && !strings.Contains(v.Body, `"choices"`) || stream && !strings.HasSuffix(v.Body, "data: [DONE]\n\n") {
					result.Failed++
					if result.Failed <= 3 {
						t.Logf("capacity request failure: status=%d error=%s body=%s", v.Status, v.Error, v.Body)
					}
				} else {
					result.Accepted++
					latency = append(latency, v.ElapsedMS)
					if stream {
						firsts = append(firsts, v.FirstMS)
						dones = append(dones, v.DoneMS)
					}
				}
			}
			result.Wall = time.Since(startTime).Seconds()
			result.SuccessRPS = float64(result.Accepted) / result.Wall
			endSample()
			<-sampleDone
			result.Drain = drain()
			if err := stopResources(); err != nil {
				t.Fatal(err)
			}
			if len(latency) > 0 {
				sort.Float64s(latency)
				result.P50 = latency[(len(latency)-1)*50/100]
				result.P95 = latency[(len(latency)-1)*95/100]
				result.P99 = latency[(len(latency)-1)*99/100]
			}
			if len(firsts) > 0 {
				sort.Float64s(firsts)
				sort.Float64s(dones)
				result.FirstP95 = firsts[(len(firsts)-1)*95/100]
				result.DoneP95 = dones[(len(dones)-1)*95/100]
			}
			if firstN > 0 {
				result.BacklogFirst /= firstN
			}
			if lastN > 0 {
				result.BacklogLast /= lastN
			}
			result.Over = max(0, result.Peak-(cfg.global<<20))
			result.OverApp = max(0, result.PeakApp-(cfg.app<<20))
			result.Passed = float64(result.Accepted) >= .99*float64(result.Requests) && result.Failed == 0 && result.Dropped == 0 && result.P95 <= 500 && result.P99 <= 1000 && result.Drain <= 5 && result.BacklogLast <= result.BacklogFirst+maxEventBytes
			all = append(all, result)
			write()
			if result.Failed > 0 || result.Accepted == 0 {
				t.Fatalf("capacity protocol/smoke failure: %+v", result)
			}
			t.Logf("capacity mode=%s scope=%s app=%d global=%d phase=%s rate=%d accepted=%d/%d p95=%.1f p99=%.1f peak=%.1fMiB appPeak=%.1fMiB passed=%v", mode, cfg.scope, cfg.app, cfg.global, phase, rate, result.Accepted, result.Requests, result.P95, result.P99, float64(result.Peak)/(1<<20), float64(result.PeakApp)/(1<<20), result.Passed)
			if err := captureAcceptanceMetrics(client, metricURLs, dir, prefix+"-after"); err != nil {
				t.Fatal(err)
			}
			return result
		}
		rates := []int{2, 5, 10, 20, 40, 80}
		if quick {
			rates = []int{2, 20}
		}
		best := 0
		passedRates := map[int]bool{}
		for ri := 0; ri < len(rates); ri++ {
			rate := rates[ri]
			warm := 5
			if quick {
				warm = 1
			}
			runLoad(rate, warm, "warmup", false, false)
			sample := runLoad(rate, seconds, "coarse", false, false)
			if sample.Passed {
				best = rate
				passedRates[rate] = true
			}
			if !quick && ri == len(rates)-1 && sample.Passed && rate < 1024 {
				rates = append(rates, 2*rate)
			}
		}
		if !quick && best > 0 {
			upper := 0
			for _, rate := range rates {
				if rate > best && (upper == 0 || rate < upper) {
					upper = rate
				}
			}
			for probe := 0; probe < 3 && upper-best > 1; probe++ {
				rate := (best + upper) / 2
				v := runLoad(rate, seconds, "refine", false, false)
				if v.Passed {
					best = rate
					passedRates[rate] = true
				} else {
					upper = rate
				}
			}
			candidates := []int{}
			for rate := range passedRates {
				candidates = append(candidates, rate)
			}
			sort.Sort(sort.Reverse(sort.IntSlice(candidates)))
			confirmed := 0
			for _, rate := range candidates {
				passed := true
				for repeat := 1; repeat <= 3; repeat++ {
					if !runLoad(rate, 60, fmt.Sprintf("confirm-%d", repeat), false, false).Passed {
						passed = false
						break
					}
				}
				if passed {
					confirmed = rate
					break
				}
			}
			if ci == 2 && confirmed > 0 {
				runLoad(confirmed, 300, "sustained", false, false)
				runLoad(2, 20, "stream", true, false)
				runLoad(2, 20, "slow", false, true)
			}
		}
		if err := s.AuditCapacity(ctx); err != nil {
			t.Fatal(err)
		}
		if ci == 0 {
			t.Run("full_capacity_cancel_and_recovery", func(t *testing.T) {
				testCapacityBackpressure(t, ctx, r, client, gateways, upstream, roles["worker"], dir, call, docker)
			})
			if t.Failed() {
				return
			}
			drain()
		}
		// 全部已接受请求都必须在 SQL/Redis 一致结算，不只检查 HTTP。
		for _, app := range apps {
			var tokens, micro int64
			if err := s.DB.QueryRowContext(ctx, `SELECT COALESCE(SUM(tokens),0),COALESCE(SUM(cost_micro),0) FROM gw_attempts WHERE app_id=? AND purpose<>'request'`, app).Scan(&tokens, &micro); err != nil {
				t.Fatal(err)
			}
			quota, e := r.Engine.Guard.Counters(ctx, app, month(time.Now()))
			if e != nil || quota.Tokens != tokens || quota.Micro != micro || quota.ReservedTokens != 0 || quota.ReservedMicro != 0 {
				t.Fatalf("quota mismatch %s SQL=%d/%d Redis=%+v error=%v", app, tokens, micro, quota, e)
			}
		}
	}
}

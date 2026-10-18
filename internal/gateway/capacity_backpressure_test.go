package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testCapacityBackpressure(t *testing.T, ctx context.Context, r *Runtime, client *http.Client, gateways []string, upstream, workerName, dir string, call func(int, bool, bool) reliabilityResult, docker func(...string) string) {
	s := r.Engine.Store.(*SQLStore)
	cfg, err := s.Update(ctx, "capacity-test", "backpressure-limit", func(c *Config) error { a := c.Apps["cap-a"]; a.OutboxBytes = 2 << 20; c.Apps[a.ID] = a; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Engine.Publisher.PublishConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	for _, url := range gateways {
		until := time.Now().Add(5 * time.Second)
		for {
			res, e := client.Get(url + "/metrics")
			if e != nil {
				t.Fatal(e)
			}
			raw, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if strings.Contains(string(raw), "\ngateway_config_version "+strconv.FormatInt(cfg.Version, 10)+"\n") {
				break
			}
			if time.Now().After(until) {
				t.Fatal("capacity limit config did not propagate")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	untilHealthy := time.Now().Add(10 * time.Second)
	for {
		var state string
		if err = s.DB.QueryRowContext(ctx, `SELECT state FROM gw_metering_apps WHERE app_id='cap-a'`).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "healthy" {
			break
		}
		if time.Now().After(untilHealthy) {
			t.Fatal("application audit did not finish before fault probe")
		}
		time.Sleep(100 * time.Millisecond)
	}
	before := func() int64 {
		res, e := client.Get(upstream + "/stats")
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		var v struct {
			Calls int64 `json:"calls"`
		}
		if e = json.NewDecoder(res.Body).Decode(&v); e != nil {
			t.Fatal(e)
		}
		return v.Calls
	}
	docker("pause", workerName)
	paused := true
	defer func() {
		if paused {
			docker("unpause", workerName)
		}
	}()
	first := call(0, false, false)
	if first.Status != 200 || first.Error != "" {
		t.Fatalf("call before capacity fill failed: %+v", first)
	}
	// 一笔预留加请求汇总占用略高于 1 MiB，2 MiB 上限已不足容纳下一笔。
	time.Sleep(450 * time.Millisecond)
	calls := before()
	waitCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	raw := []byte(`{"model":"chat","messages":[{"role":"user","content":"blocked capacity probe"}],"max_tokens":64}`)
	req, _ := http.NewRequestWithContext(waitCtx, "POST", gateways[1]+"/v1/chat/completions", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer cap-a-key")
	req.Header.Set("Content-Type", "application/json")
	at := time.Now()
	res, requestErr := client.Do(req)
	if res != nil {
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}
	cancel()
	elapsed := time.Since(at)
	if s.CapacityWait && (requestErr == nil || elapsed < 200*time.Millisecond) {
		t.Fatalf("full capacity did not wait for cancellation: elapsed=%v error=%v", elapsed, requestErr)
	}
	if after := before(); after != calls {
		t.Fatal("blocked request called provider", calls, after)
	}
	docker("unpause", workerName)
	paused = false
	deadline := time.Now().Add(10 * time.Second)
	for {
		snap, e := s.sampleCapacity(ctx, nil)
		if e == nil && snap.Global == 0 {
			res, probeErr := client.Get(gateways[0] + "/readyz")
			if probeErr == nil {
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode == 200 {
					break
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not release capacity")
		}
		time.Sleep(100 * time.Millisecond)
	}
	resumed := call(0, false, false)
	if resumed.Status != 200 || resumed.Error != "" {
		t.Fatalf("request did not resume after release: %+v", resumed)
	}
	raw, _ = json.MarshalIndent(map[string]any{"full_capacity_blocks": s.CapacityWait, "blocked_wait_ms": float64(elapsed.Microseconds()) / 1000, "cancelled_request_did_not_call_provider": true, "worker_release_restores_admission": true}, "", "  ")
	if err = os.WriteFile(filepath.Join(dir, "capacity-backpressure.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

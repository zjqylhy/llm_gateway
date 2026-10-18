package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testCapacityWorkerRestart(t *testing.T, ctx context.Context, r *Runtime, client *http.Client, gateway, upstream, worker, dir string, docker func(...string) string) {
	s := r.Engine.Store.(*SQLStore)
	wait := func(label string, check func() bool) {
		t.Helper()
		until := time.Now().Add(35 * time.Second)
		for !check() {
			if time.Now().After(until) || ctx.Err() != nil {
				t.Fatal("timeout: " + label)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	// 选择刚完成核验的应用，避免把既有核验中断冻结误当作容量故障。
	var app string
	wait("recent healthy application", func() bool {
		return s.DB.QueryRowContext(ctx, `SELECT app_id FROM gw_metering_apps WHERE app_id LIKE 'cap-%' AND state='healthy' AND checked_at>UTC_TIMESTAMP(6)-INTERVAL 20 SECOND ORDER BY checked_at DESC LIMIT 1`).Scan(&app) == nil
	})
	wait("fresh permit", func() bool {
		p, err := r.readPermit(ctx)
		return err == nil && p.Expires-time.Now().UnixMilli() > 2300
	})
	call := func() int {
		req, _ := http.NewRequestWithContext(ctx, "POST", gateway+"/v1/chat/completions", bytes.NewBufferString(`{"model":"chat","messages":[{"role":"user","content":"capacity restart probe"}],"max_tokens":64}`))
		req.Header.Set("Authorization", "Bearer "+app+"-key")
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode == 200 && !bytes.Contains(body, []byte(`"choices"`)) {
			t.Fatal("invalid successful response", string(body))
		}
		return res.StatusCode
	}
	calls := func() int64 {
		res, err := client.Get(upstream + "/stats")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var stats struct {
			Calls int64 `json:"calls"`
		}
		if err := json.NewDecoder(res.Body).Decode(&stats); err != nil {
			t.Fatal(err)
		}
		return stats.Calls
	}
	docker("pause", worker)
	paused, stopped := true, false
	defer func() {
		if paused {
			docker("unpause", worker)
		}
		if stopped {
			docker("start", worker)
		}
	}()
	if status := call(); status != 200 {
		t.Fatal("fresh permit failed before worker restart", status)
	}
	before, err := s.sampleCapacity(ctx, nil)
	if err != nil || before.Global == 0 {
		t.Fatal("paused worker did not retain capacity", before.Global, err)
	}
	docker("unpause", worker)
	paused = false
	docker("kill", worker)
	stopped = true
	time.Sleep(4 * time.Second)
	providerCalls := calls()
	if status := call(); status != 503 || calls() != providerCalls {
		t.Fatal("expired worker permit reached provider", status)
	}
	docker("start", worker)
	stopped = false
	wait("worker releases capacity and renews permit", func() bool {
		snap, err := s.sampleCapacity(ctx, nil)
		_, permitErr := r.readPermit(ctx)
		return err == nil && snap.Global == 0 && permitErr == nil
	})
	if status := call(); status != 200 {
		t.Fatal("admission did not recover after worker restart", status)
	}
	wait("restart probe settled", func() bool {
		snap, err := s.sampleCapacity(ctx, nil)
		return err == nil && snap.Global == 0
	})
	states := map[string]string{}
	rows, err := s.DB.QueryContext(ctx, `SELECT app_id,state FROM gw_metering_apps WHERE app_id LIKE 'cap-%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, state string
		if err := rows.Scan(&id, &state); err != nil {
			t.Fatal(err)
		}
		states[id] = state
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.MarshalIndent(map[string]any{"application": app, "occupied_before_restart_bytes": before.Global, "expired_permit_did_not_call_provider": true, "restart_drained_capacity": true, "admission_recovered": true, "application_states_after_restart": states}, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "capacity-worker-restart.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

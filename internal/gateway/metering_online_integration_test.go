package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testOnlineMetering(t *testing.T, r *Runtime, parent context.Context, stopWorker, startWorker func()) {
	t.Helper()
	s := r.Engine.Store.(*SQLStore)
	e := r.Engine
	var calls atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		raw, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(raw))
		if strings.Contains(string(raw), "barrier-held") {
			close(entered)
			<-release
		}
		MockProvider(w, req)
	}))
	defer upstream.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	cfg, err := s.Update(parent, "acceptance", "metering.online.seed", func(c *Config) error {
		c.Providers["barrier"] = Provider{"barrier", "mock", upstream.URL + "/v1", e.Vault.Encrypt("barrier", "test")}
		c.Models["barrier"] = Model{ID: "barrier", ProviderID: "barrier", Upstream: "mock-barrier", Capabilities: []string{"chat", "stream"}, Context: 16000, MaxOutput: 128, InputMicro: 1000000, OutputMicro: 2000000, PriceVersion: "test", Currency: "CNY", FXPPM: 1000000}
		c.Routes["barrier"] = Route{Alias: "barrier", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{"barrier", 100}}}
		for _, app := range []string{"barrier-a", "barrier-b"} {
			c.Apps[app] = App{ID: app, Active: true, Owner: "admin", Models: []string{"barrier"}, BudgetAction: "warn"}
			c.Keys[app] = Key{ID: app, AppID: app, Hash: Hash(app + "-key"), State: "active"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Publisher.PublishConfig(parent, cfg); err != nil {
		t.Fatal(err)
	}
	if err = e.Refresh(parent); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.nextAuditApp(parent, ""); err != nil {
		t.Fatal(err)
	}
	if err = r.renewPermit(parent); err != nil {
		t.Fatal(err)
	}
	call := func(app, text string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{"model": "barrier", "messages": []any{map[string]string{"role": "user", "content": text}}, "max_tokens": 64, "stream": true})
		req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(body)).WithContext(parent)
		req.Header.Set("Authorization", "Bearer "+app+"-key")
		w := httptest.NewRecorder()
		e.Handler().ServeHTTP(w, req)
		return w
	}
	if _, err = r.Engine.Publisher.(EtcdPublisher).Client.Delete(parent, r.permitKey()); err != nil {
		t.Fatal(err)
	}
	if w := call("barrier-a", "no permit"); w.Code != 503 || calls.Load() != 0 {
		t.Fatal("missing lease called provider", w.Code, w.Body.String())
	}
	if err = r.renewPermit(parent); err != nil {
		t.Fatal(err)
	}
	finished := make(chan *httptest.ResponseRecorder, 1)
	go func() { finished <- call("barrier-a", "barrier-held") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("held call did not enter provider")
	}
	if len(r.admissionSlots) != 1 {
		t.Fatal("metering concurrency slot released before completion", len(r.admissionSlots))
	}
	audited := make(chan error, 1)
	go func() {
		report, err := r.AuditApplication(parent, "barrier-a", "acceptance", "overlap stream")
		if err == nil && !report.Passed {
			err = ErrUnavailable
		}
		audited <- err
	}()
	if err = waitMetering(parent, func() (bool, error) {
		var state string
		err := s.DB.QueryRowContext(parent, `SELECT state FROM gw_metering_apps WHERE app_id='barrier-a'`).Scan(&state)
		return state == "quiescing", err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-audited:
		t.Fatal("audit compared an in-flight attempt", err)
	default:
	}
	if w := call("barrier-a", "blocked by barrier"); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call("barrier-b", "other application"); w.Code != 200 {
		t.Fatal("barrier crossed application boundary", w.Code, w.Body.String())
	}
	close(release)
	if w := <-finished; w.Code != 200 || !strings.Contains(w.Body.String(), "[DONE]") {
		t.Fatal(w.Code, w.Body.String())
	}
	if err = <-audited; err != nil {
		t.Fatal("normal asynchronous lag misclassified", err)
	}
	if len(r.admissionSlots) != 0 {
		t.Fatal("metering concurrency slot leaked", len(r.admissionSlots))
	}
	var raw []byte
	var event Event
	if err = s.DB.QueryRowContext(parent, `SELECT payload FROM gw_attempts WHERE app_id='barrier-a' AND purpose<>'request' LIMIT 1`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	if err = r.Redis.Del(parent, quotaKeys(event.AppID, event.Month, event.AttemptID)[1]).Err(); err != nil {
		t.Fatal(err)
	}
	if report, err := r.AuditApplication(parent, "barrier-a", "acceptance", "deleted attempt key"); err == nil || report.Passed {
		t.Fatal("partial loss was accepted", report, err)
	}
	if err = r.renewPermit(parent); err != nil {
		t.Fatal(err)
	}
	if w := call("barrier-a", "frozen application"); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if w := call("barrier-b", "healthy application"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	// 旧快照保留 ready，但结算水位倒退，许可续期必须拒绝。
	watermarks, err := r.Redis.HGetAll(parent, "gw:metering:watermarks").Result()
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Redis.HSet(parent, "gw:metering:watermarks", "0", -1).Err()
	if err = r.renewPermit(parent); err == nil {
		t.Fatal("old Redis progress received a fresh permit")
	}
	if err = r.Redis.HSet(parent, "gw:metering:watermarks", watermarks).Err(); err != nil {
		t.Fatal(err)
	}
	stopWorker()
	t.Setenv("GW_QUOTA_REBUILD_ACK", "writers-stopped")
	t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
	if err = RebuildQuota(parent, r); err != nil {
		t.Fatal("partial loss repair", err)
	}
	if _, err = ResumeMetering(parent, r); err != nil {
		t.Fatal("audited explicit resume", err)
	}
	e.Bus.(*DurableBus).stopped.Store(false)
	startWorker()
	if err = r.renewPermit(parent); err != nil {
		t.Fatal(err)
	}
	if w := call("barrier-a", "repaired application"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

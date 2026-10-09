package gateway

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestVaultBindsCiphertextToProvider(t *testing.T) {
	v, e := NewVault([]byte(strings.Repeat("x", 32)))
	if e != nil {
		t.Fatal(e)
	}
	sealed := v.Encrypt("p1", "secret")
	if x, e := v.Decrypt("p1", sealed); e != nil || x != "secret" {
		t.Fatalf("decrypt: %q %v", x, e)
	}
	if _, e = v.Decrypt("p2", sealed); e == nil {
		t.Fatal("cross-provider ciphertext accepted")
	}
}
func TestConcurrentReservationsAndCorrections(t *testing.T) {
	g := NewMemoryGuard()
	a := App{ID: "a", MonthlyTokens: 100, BudgetMicro: 100, BudgetAction: "reject"}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := Reservation{ID("att_"), a.ID, "2026-10", 10, 10}
			if g.Reserve(context.Background(), a, r) == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 10 {
		t.Fatalf("accepted %d; oversubscribed quota", accepted.Load())
	}
	r := Reservation{"settle", a.ID, "2026-11", 40, 40}
	if e := g.Reserve(context.Background(), a, r); e != nil {
		t.Fatal(e)
	}
	event := Event{AttemptID: r.ID, AppID: a.ID, Month: r.Month, Revision: 2, Usage: Usage{Total: 20}, CostMicro: 20}
	for i := 0; i < 3; i++ {
		_ = g.Settle(context.Background(), event)
	}
	event.Revision = 3
	event.Usage.Total = 15
	event.CostMicro = 15
	_ = g.Settle(context.Background(), event)
	event.Revision = 2
	event.Usage.Total = 20
	_ = g.Settle(context.Background(), event)
	c, _ := g.Counters(context.Background(), a.ID, r.Month)
	if c.Tokens != 15 || c.Micro != 15 || c.ReservedTokens != 0 {
		t.Fatalf("non-idempotent settlement: %+v", c)
	}
}
func TestEventReplayDoesNotRegress(t *testing.T) {
	s := NewMemoryStore()
	e := Event{AttemptID: "a", Revision: 2, Status: "success", At: time.Now()}
	_ = s.Apply(context.Background(), e)
	e.Revision = 1
	e.Status = "started"
	_ = s.Apply(context.Background(), e)
	rows, _ := s.Records(context.Background(), Filter{})
	if len(rows) != 1 || rows[0].Status != "success" {
		t.Fatal(rows)
	}
}
func TestSSEMultilineAndTruncation(t *testing.T) {
	s := NewSSEReader(strings.NewReader(": heartbeat\n\ndata: {\ndata: \"usage\":null}\n\ndata: [DONE]\n\n"))
	x, e := s.Next()
	if e != nil || string(x) != "{\n\"usage\":null}" {
		t.Fatalf("%s %v", x, e)
	}
	x, e = s.Next()
	if e != nil || string(x) != "[DONE]" {
		t.Fatal(string(x), e)
	}
}
func TestDecodeRejectsUnsupportedAndMultimodal(t *testing.T) {
	for _, raw := range []string{`{"model":"x","messages":[{"role":"user","content":[{"type":"image_url"}]}]}`, `{"model":"x","messages":[{"role":"user","content":"ok"}],"unknown":true}`, `{"model":"x","messages":[]}`} {
		if _, e := DecodeRequest([]byte(raw), "chat"); e == nil {
			t.Fatal("invalid request accepted:", raw)
		}
	}
}
func TestCostCachedPriceAndFX(t *testing.T) {
	m := Model{InputMicro: 1000000, CachedInputMicro: 100000, OutputMicro: 2000000, FXPPM: 7000000}
	u := Usage{Input: 100, Output: 10, Cached: 50}
	if got := Cost(m, u); got != 525 {
		t.Fatalf("cost=%d", got)
	}
}

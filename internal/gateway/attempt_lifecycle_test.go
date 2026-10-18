package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestArmAndCancellationHaveOneWinner(t *testing.T) {
	for range 100 {
		ctx := context.Background()
		store := NewMemoryStore()
		e := Event{ID: "attempt:1", AttemptID: "attempt", RequestID: "request", AppID: "app", Month: "2026-10", Kind: "started", Revision: 1, Quality: "pending", At: time.Now().UTC()}
		if err := store.PrepareAttempt(ctx, e); err != nil {
			t.Fatal(err)
		}
		var arm, cancel error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); arm = store.ArmAttempt(ctx, e) }()
		go func() { defer wg.Done(); cancel = store.CancelAttempt(ctx, e) }()
		wg.Wait()
		if (arm == nil) == (cancel == nil) {
			t.Fatalf("expected exactly one transition: arm=%v cancel=%v", arm, cancel)
		}
		if arm != nil && !errors.Is(arm, ErrFenced) {
			t.Fatal(arm)
		}
		if cancel != nil && !errors.Is(cancel, ErrFenced) {
			t.Fatal(cancel)
		}
		if err := store.ArmAttempt(ctx, e); !errors.Is(err, ErrFenced) {
			t.Fatalf("retry permitted another model invocation: %v", err)
		}
		if arm == nil {
			rows, _ := store.Records(ctx, Filter{})
			if len(rows) != 1 || rows[0].Kind != "started" {
				t.Fatal("Arm lost start evidence", rows)
			}
		} else {
			if err := store.FinishCancellation(ctx, e); err != nil {
				t.Fatal(err)
			}
			if err := store.FinishCancellation(ctx, e); err != nil {
				t.Fatal("cancel retry", err)
			}
		}
	}
}

func TestNewRequestsNeverPublishStarted(t *testing.T) {
	e, _, transport, calls := durableTestEngine(t)
	w := call(t, e, "hello", false)
	if w.Code != 200 || calls.Load() != 1 {
		t.Fatal(w.Code, calls.Load())
	}
	for _, event := range transport.events {
		if event.Kind == "started" {
			t.Fatal("new started Kafka publication", event)
		}
	}
	rows, _ := e.Store.Records(context.Background(), Filter{})
	found := false
	for _, event := range rows {
		if event.Kind == "started" && event.Purpose == "inference" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing synchronous start ledger before worker")
	}
}

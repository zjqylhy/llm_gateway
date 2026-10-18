package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func capacityEvent(id, app string) Event {
	return Event{ID: id + ":1", AttemptID: id, AppID: app, Kind: "started", Revision: 1}
}

func awaitCapacityQueue(t *testing.T, g *capacityGate, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		count := 0
		for _, q := range g.queues {
			count += len(q)
		}
		g.mu.Unlock()
		if count == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("capacity queue did not reach %d", n)
}

func TestCapacityGateCoverageAndWake(t *testing.T) {
	g := newCapacityGate(64<<20, nil, nil)
	defer g.close()
	g.publish(capacitySnapshot{At: time.Now(), Global: 60 << 20, Apps: map[string]int64{"a": 60 << 20}})
	for i := 0; i < 4; i++ {
		if err := g.acquire(context.Background(), capacityEvent(fmt.Sprint(i), "a"), 64<<20); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- g.acquire(ctx, capacityEvent("waiting", "a"), 64<<20) }()
	awaitCapacityQueue(t, g, 1)
	// 覆盖登记后不能把新快照与同一份临时预留重复相加。
	g.publish(capacitySnapshot{At: time.Now(), Global: 64 << 20, Apps: map[string]int64{"a": 64 << 20}, Covered: []string{"0", "1", "2", "3"}})
	g.mu.Lock()
	n := len(g.tickets)
	g.mu.Unlock()
	if n != 0 {
		t.Fatalf("covered tickets remain: %d", n)
	}
	select {
	case err := <-result:
		t.Fatalf("full capacity admitted: %v", err)
	default:
	}
	g.publish(capacitySnapshot{At: time.Now(), Global: 61 << 20, Apps: map[string]int64{"a": 61 << 20}})
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("release did not wake waiter")
	}
}

func TestCapacityGateConcurrentLimitAndCancellation(t *testing.T) {
	g := newCapacityGate(8<<20, nil, nil)
	defer g.close()
	g.publish(capacitySnapshot{At: time.Now(), Apps: map[string]int64{}})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	results := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); results <- g.acquire(ctx, capacityEvent(fmt.Sprint(i), "a"), 8<<20) }(i)
	}
	awaitCapacityQueue(t, g, 24)
	g.mu.Lock()
	n := len(g.tickets)
	g.mu.Unlock()
	if n != 8 {
		t.Fatalf("local admission exceeded capacity: %d", n)
	}
	cancel()
	wg.Wait()
	accepted := 0
	for i := 0; i < 32; i++ {
		if err := <-results; err == nil {
			accepted++
		} else if !errors.Is(err, ErrCapacityTimeout) {
			t.Fatal(err)
		}
	}
	if accepted > 8 {
		t.Fatal("over-admission", accepted)
	}
	awaitCapacityQueue(t, g, 0)
}

func TestCapacityGateApplicationFIFOAndIsolation(t *testing.T) {
	g := newCapacityGate(10<<20, nil, nil)
	defer g.close()
	g.publish(capacitySnapshot{At: time.Now(), Global: 2 << 20, Apps: map[string]int64{"a": 2 << 20}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- g.acquire(ctx, capacityEvent("first", "a"), 2<<20) }()
	awaitCapacityQueue(t, g, 1)
	go func() { second <- g.acquire(ctx, capacityEvent("second", "a"), 2<<20) }()
	awaitCapacityQueue(t, g, 2)
	if err := g.acquire(ctx, capacityEvent("other", "b"), 2<<20); err != nil {
		t.Fatal(err)
	}
	g.publish(capacitySnapshot{At: time.Now(), Global: 1 << 20, Apps: map[string]int64{"a": 1 << 20}})
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first waiter not admitted")
	}
	select {
	case err := <-second:
		t.Fatalf("second overtook: %v", err)
	default:
	}
	g.release("first")
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second waiter not admitted")
	}
}

func TestCapacityGateStaleSnapshotAndClosed(t *testing.T) {
	g := newCapacityGate(8<<20, nil, nil)
	g.publish(capacitySnapshot{At: time.Now().Add(-2 * time.Second), Apps: map[string]int64{}})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := g.acquire(ctx, capacityEvent("stale", "a"), 8<<20); !errors.Is(err, ErrCapacityTimeout) {
		t.Fatal("stale snapshot admitted", err)
	}
	awaitCapacityQueue(t, g, 0)
	g.close()
	if err := g.acquire(context.Background(), capacityEvent("closed", "a"), 8<<20); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestCapacityGateRetainsNewAndUnobservedTickets(t *testing.T) {
	g := newCapacityGate(8<<20, nil, nil)
	defer g.close()
	g.publish(capacitySnapshot{At: time.Now(), Apps: map[string]int64{}})
	for _, id := range []string{"observed", "late", "unknown"} {
		if err := g.acquire(context.Background(), capacityEvent(id, "a"), 8<<20); err != nil {
			t.Fatal(err)
		}
	}
	g.publish(capacitySnapshot{At: time.Now(), Apps: map[string]int64{}, Covered: []string{"observed"}})
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.tickets) != 2 || g.localApps["a"] != 2*maxEventBytes {
		t.Fatal("unobserved ticket lost", g.tickets, g.localApps)
	}
}

func TestCapacityFailedSampleDoesNotPublishPartialCoverage(t *testing.T) {
	called := make(chan struct{}, 1)
	g := newCapacityGate(8<<20, nil, func(ctx context.Context, tickets map[string]capacityTicket) (capacitySnapshot, error) {
		select {
		case called <- struct{}{}:
		default:
		}
		return capacitySnapshot{At: time.Now(), Apps: map[string]int64{}, Covered: []string{"kept"}}, errors.New("later receipt batch failed")
	})
	defer g.close()
	old := capacitySnapshot{At: time.Now(), Global: 1 << 20, Apps: map[string]int64{"a": 1 << 20}}
	g.publish(old)
	if err := g.acquire(context.Background(), capacityEvent("kept", "a"), 8<<20); err != nil {
		t.Fatal(err)
	}
	g.start()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("sampler not started")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.snapshot.At.Equal(old.At) || len(g.tickets) != 1 {
		t.Fatal("failed sample changed snapshot or local receipts")
	}
}

func TestCapacityGateQueuedRequestsUseLatestApplicationLimits(t *testing.T) {
	g := newCapacityGate(10<<20, nil, nil)
	defer g.close()
	var limit atomic.Int64
	var active atomic.Bool
	limit.Store(2 << 20)
	active.Store(true)
	g.limits = func(string) (int64, bool) { return limit.Load(), active.Load() }
	snapshot := capacitySnapshot{At: time.Now(), Global: 2 << 20, Apps: map[string]int64{"a": 2 << 20}}
	g.publish(snapshot)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- g.acquire(ctx, capacityEvent("updated", "a"), 100<<20) }()
	awaitCapacityQueue(t, g, 1)
	// 排队时扩容无需重新发请求；调用者携带的旧限额不能覆盖最新配置。
	limit.Store(3 << 20)
	snapshot.At = time.Now()
	g.publish(snapshot)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request did not observe increased limit")
	}
	go func() { result <- g.acquire(ctx, capacityEvent("disabled", "a"), 100<<20) }()
	awaitCapacityQueue(t, g, 1)
	active.Store(false)
	snapshot.At = time.Now()
	g.publish(snapshot)
	select {
	case err := <-result:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatal("disabled application admitted", err)
		}
	case <-time.After(time.Second):
		t.Fatal("disabled application remained queued")
	}
}

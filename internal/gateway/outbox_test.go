package gateway

import (
	"context"
	"database/sql"
	"errors"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testOutbox struct {
	mu        sync.Mutex
	events    map[string]Event
	published map[string]bool
	saveError func(Event) error
	frozen    bool
}

func newTestOutbox() *testOutbox {
	return &testOutbox{events: map[string]Event{}, published: map[string]bool{}}
}
func (s *testOutbox) SaveOutbox(_ context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveError != nil {
		if err := s.saveError(e); err != nil {
			return err
		}
	}
	if old, ok := s.events[e.ID]; ok {
		a, _ := eventBytes(old)
		b, _ := eventBytes(e)
		if string(a) != string(b) {
			return ErrEventConflict
		}
		return nil
	}
	s.events[e.ID] = e
	return nil
}
func (s *testOutbox) PublishedOutbox(_ context.Context, e Event, _ int64, success bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if success {
		s.published[e.ID] = true
	}
	return nil
}
func (s *testOutbox) FreezeMetering(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frozen = true
	return nil
}
func (s *testOutbox) ClaimOutbox(context.Context) ([]OutboxDelivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []OutboxDelivery
	for id, e := range s.events {
		if !s.published[id] {
			out = append(out, OutboxDelivery{e, 1})
		}
	}
	return out, nil
}

type recordingBus struct {
	mu     sync.Mutex
	events []Event
	fail   func(Event) bool
}

// 只存在于测试代码，模拟 completed 的 outbox 写入失败而 Kafka 可用。
type failedCompletionOutbox struct{ *SQLStore }

func (s failedCompletionOutbox) SaveOutbox(c context.Context, e Event) error {
	if e.Kind == "completed" && e.Purpose == "inference" {
		return errors.New("injected outbox failure")
	}
	return s.SQLStore.SaveOutbox(c, e)
}

func (b *recordingBus) Publish(_ context.Context, e Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail != nil && b.fail(e) {
		return errors.New("injected Kafka failure")
	}
	b.events = append(b.events, e)
	return nil
}
func (b *recordingBus) Close() error { return nil }

func durableTestEngine(t *testing.T, configure ...func(*Engine)) (*Engine, *testOutbox, *recordingBus, *atomic.Int32) {
	calls := new(atomic.Int32)
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); MockProvider(w, r) }, configure...)
	s, transport := newTestOutbox(), &recordingBus{}
	e.Bus = &DurableBus{Store: s, Transport: transport}
	return e, s, transport, calls
}

func TestArmFailureRejectsWithoutProviderOrStartedKafka(t *testing.T) {
	e, _, transport, calls := durableTestEngine(t, func(e *Engine) {
		e.Store = failedArmStore{e.Store.(*MemoryStore)}
	})
	w := call(t, e, "hello", false)
	if w.Code != 503 || calls.Load() != 0 {
		t.Fatal("unpersisted started invoked provider", w.Code, calls.Load())
	}
	for _, event := range transport.events {
		if event.Kind == "started" {
			t.Fatal("started bypassed SQL requirement")
		}
	}
	counts, _ := e.Guard.Counters(context.Background(), "app", month(time.Now()))
	if counts.ReservedTokens != 0 || counts.Tokens != 0 {
		t.Fatal("known uncalled attempt was not cancelled", counts)
	}
}

func TestOutboxCompletionSQLFailureStillPublishesKafka(t *testing.T) {
	e, s, transport, calls := durableTestEngine(t)
	s.saveError = func(e Event) error {
		if e.Kind == "completed" && e.Purpose == "inference" {
			return errors.New("SQL unavailable")
		}
		return nil
	}
	w := call(t, e, "hello", false)
	if w.Code != 200 || calls.Load() != 1 {
		t.Fatal(w.Code, calls.Load(), w.Body.String())
	}
	found := false
	for _, event := range transport.events {
		if event.Kind == "completed" && event.Purpose == "inference" {
			found = event.Usage.Total == 32
		}
	}
	if !found || !s.frozen {
		t.Fatal("fallback or freeze missing")
	}
	if again := call(t, e, "again", false); again.Code != 503 || calls.Load() != 1 {
		t.Fatal("dependency failure permitted new paid call")
	}
}

func TestOutboxKafkaFailureSurvivesRelayWithoutProviderRetry(t *testing.T) {
	e, s, transport, calls := durableTestEngine(t)
	transport.fail = func(e Event) bool { return e.Kind == "completed" && e.Purpose == "inference" }
	w := call(t, e, "hello", true)
	if strings.Contains(w.Body.String(), "[DONE]") || calls.Load() != 1 {
		t.Fatal("metering failure retried provider or returned DONE", w.Body.String())
	}
	var completion Event
	for _, event := range s.events {
		if event.Kind == "completed" && event.Purpose == "inference" {
			completion = event
		}
	}
	if completion.ID == "" || completion.Usage.Total != 32 {
		t.Fatal("completion not saved")
	}
	rows, _ := e.Store.Records(context.Background(), Filter{})
	if len(rows) != 1 || rows[0].Kind != "started" || rows[0].Revision != 1 {
		t.Fatal("Arm must persist start before provider; completion must wait for consumption", rows)
	}
	transport.fail = nil
	restarted := &DurableBus{Store: s, Transport: transport}
	if err := restarted.RelayOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !s.published[completion.ID] || calls.Load() != 1 {
		t.Fatal("relay lost event or invoked provider")
	}
	if err := e.Store.Apply(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	if err := e.Guard.Settle(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	if err := e.Guard.Settle(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	counts, _ := e.Guard.Counters(context.Background(), "app", completion.Month)
	if counts.Tokens != 32 || counts.ReservedTokens != 0 {
		t.Fatal("duplicate settlement", counts)
	}
}

func TestOutboxBothCompletionPathsFailPreservesReservation(t *testing.T) {
	e, s, transport, calls := durableTestEngine(t)
	s.saveError = func(e Event) error {
		if e.Kind == "completed" && e.Purpose == "inference" {
			return errors.New("SQL unavailable")
		}
		return nil
	}
	transport.fail = func(e Event) bool { return e.Kind == "completed" && e.Purpose == "inference" }
	w := call(t, e, "hello", true)
	if strings.Contains(w.Body.String(), "[DONE]") || calls.Load() != 1 || !s.frozen {
		t.Fatal("double failure was treated as success")
	}
	counts, _ := e.Guard.Counters(context.Background(), "app", month(time.Now()))
	if counts.ReservedTokens <= 0 || counts.Tokens != 0 {
		t.Fatal("unknown cost was released", counts)
	}
}

func TestOutboxConflictNeverPublishesAndFreezes(t *testing.T) {
	s, transport := newTestOutbox(), &recordingBus{}
	b := &DurableBus{Store: s, Transport: transport}
	e := Event{ID: "e", AttemptID: "a", RequestID: "r", AppID: "app", Kind: "completed", Revision: 2}
	if err := b.Publish(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	e.CostMicro = 9
	if err := b.Publish(context.Background(), e); err == nil || len(transport.events) != 1 || !s.frozen {
		t.Fatal("conflicting event was delivered")
	}
}

func TestOutboxOversizedSamplePreservesMetering(t *testing.T) {
	s, transport := newTestOutbox(), &recordingBus{}
	b := &DurableBus{Store: s, Transport: transport}
	event := Event{ID: "sample:2", AttemptID: "sample", RequestID: "request", AppID: "app", Kind: "completed", Revision: 2, Usage: Usage{Input: 10, Output: 20, Total: 30}, CostMicro: 40, PayloadCipher: strings.Repeat("x", maxEventBytes)}
	if err := b.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(transport.events) != 1 {
		t.Fatal("event missing")
	}
	actual := transport.events[0]
	if actual.PayloadCipher != "" || actual.Usage != event.Usage || actual.CostMicro != event.CostMicro {
		t.Fatal("financial fields dropped with sample")
	}
}

func TestFrozenQuotaRejectsAdmissionButAllowsSettlementReplay(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	server.Set("gw:quota:state", "ready")
	server.Set("gw:metering:frozen", "failure")
	guard := RedisGuard{Client: client, RequireReady: true}
	res := Reservation{ID: "pending", AppID: "app", Month: "2026-10", Tokens: 100, Micro: 100}
	if err := guard.Reserve(ctx, App{ID: "app"}, res); !errors.Is(err, ErrUnavailable) {
		t.Fatal("frozen quota admitted paid attempt", err)
	}
	guard.Replay = true
	if err := guard.Reserve(ctx, App{ID: "app"}, res); err != nil {
		t.Fatal("durable replay was blocked", err)
	}
	event := Event{AttemptID: res.ID, AppID: res.AppID, Month: res.Month, Revision: 2, Usage: Usage{Total: 30}, CostMicro: 40}
	if err := guard.Settle(ctx, event); err != nil {
		t.Fatal("settlement blocked by freeze", err)
	}
	counts, err := guard.Counters(ctx, res.AppID, res.Month)
	if err != nil || counts.Tokens != 30 || counts.Micro != 40 || counts.ReservedTokens != 0 {
		t.Fatal(counts, err)
	}
}

func TestCancelledCallerDoesNotFreezeHealthyMetering(t *testing.T) {
	s, transport := newTestOutbox(), &recordingBus{}
	b := &DurableBus{Store: s, Transport: transport}
	s.saveError = func(Event) error { return context.Canceled }
	c, cancel := context.WithCancel(context.Background())
	cancel()
	event := Event{ID: "cancelled:2", AttemptID: "cancelled", RequestID: "request", AppID: "app", Kind: "completed", Revision: 2}
	if err := b.Publish(c, event); err == nil {
		t.Fatal("cancelled caller admitted")
	}
	if s.frozen || b.stopped.Load() {
		t.Fatal("caller cancellation froze all applications")
	}
	s.saveError = nil
	if err := b.Publish(context.Background(), event); err != nil {
		t.Fatal("healthy request was blocked", err)
	}
}

func TestApplicationBarrierDoesNotFreezeGlobalMetering(t *testing.T) {
	s, transport := newTestOutbox(), &recordingBus{}
	s.saveError = func(Event) error { return errMeteringBarrier }
	b := &DurableBus{Store: s, Transport: transport}
	e := Event{ID: "barrier:1", AttemptID: "barrier", RequestID: "request", AppID: "app", Kind: "started", Revision: 1}
	if err := b.Publish(context.Background(), e); err == nil {
		t.Fatal("barrier admitted paid attempt")
	}
	if s.frozen || b.stopped.Load() || len(transport.events) != 0 {
		t.Fatal("application pause caused global freeze or publication")
	}
}

// 新 started 从 Arm 持久化，不能通过发布接口重新启用旧协议。
func TestStartedPublicationIsRejectedWithoutBlockingCompletion(t *testing.T) {
	store, transport := newTestOutbox(), &recordingBus{}
	bus := &DurableBus{Store: store, Transport: transport}
	start := Event{ID: "one:1", AttemptID: "one", RequestID: "request", AppID: "app", Kind: "started", Revision: 1}
	if err := bus.Publish(context.Background(), start); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if len(store.events) != 0 || len(transport.events) != 0 {
		t.Fatal("started reached old outbox or Kafka path")
	}
	if err := bus.Publish(context.Background(), cancelEvent(start)); err != nil {
		t.Fatal(err)
	}
}

// 旧 started 仅由迁移/重放 fixture 输入；正常网关不能调用此发布路径。
func publishLegacyFixture(ctx context.Context, bus *DurableBus, event Event) error {
	if event.Kind != "started" {
		return bus.Publish(ctx, event)
	}
	var saveErr error
	switch store := bus.Store.(type) {
	case *SQLStore:
		saveErr = saveFixtureOutbox(ctx, store, event)
	case failedCompletionOutbox:
		saveErr = saveFixtureOutbox(ctx, store.SQLStore, event)
	default:
		saveErr = bus.Store.SaveOutbox(ctx, event)
	}
	if err := saveErr; err != nil {
		return err
	}
	if err := bus.Transport.Publish(ctx, event); err != nil {
		return err
	}
	return bus.Store.PublishedOutbox(ctx, event, 0, true)
}

// 只为旧 Kafka 格式的验收 fixture 构造旧 Outbox；生产保存接口仅接受 completed。
func saveFixtureOutbox(c context.Context, s *SQLStore, e Event) error {
	if e.Kind != "started" {
		return s.SaveOutbox(c, e)
	}
	raw, err := eventBytes(e)
	if err != nil {
		return err
	}
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = checkAppBarrier(c, tx, e.AppID, false); err != nil {
		return err
	}
	var hash string
	err = tx.QueryRowContext(c, `SELECT payload_hash FROM gw_metering_outbox WHERE event_id=?`, e.ID).Scan(&hash)
	if err == nil {
		if hash != Hash(string(raw)) {
			return ErrEventConflict
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = applyEventTx(c, tx, e); err != nil {
		return err
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_attempts SET reserved_remaining_bytes=?,start_payload=?,start_hash=? WHERE attempt_id=?`, maxEventBytes, string(raw), Hash(string(raw)), e.AttemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_metering_outbox(event_id,attempt_id,app_id,revision,payload,payload_hash,charged_bytes,next_retry_at) VALUES(?,?,?,?,?,?,?,UTC_TIMESTAMP(6))`, e.ID, e.AttemptID, e.AppID, e.Revision, raw, Hash(string(raw)), len(raw)); err != nil {
		return err
	}
	return tx.Commit()
}

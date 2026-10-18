package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type failedCancelGuard struct{ Guard }

func (failedCancelGuard) Cancel(context.Context, Event) error {
	return errors.New("injected Redis cancellation outage")
}

func testAttemptLifecycleInfrastructure(t *testing.T, r *Runtime, c context.Context) {
	s := r.Engine.Store.(*SQLStore)
	var generation string
	if err := s.DB.QueryRowContext(c, `SELECT generation FROM gw_metering_control WHERE id=1`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	app := "lifecycle-fixture"
	if _, err := s.DB.ExecContext(c, `INSERT INTO gw_metering_apps(app_id,state,generation,reason,report_id) VALUES(?,'healthy','fixture','','')`, app); err != nil {
		t.Fatal(err)
	}
	guard := RedisGuard{Client: r.Redis, RequireReady: true, Replay: true}
	prepare := func() (context.Context, Event) {
		ctx := context.WithValue(c, admissionProofKey{}, admissionProof{Permit: meteringPermit{generation, time.Now().Add(time.Minute).UnixMilli(), "fixture"}, AppGeneration: "fixture"})
		id := ID("lifecycle_")
		e := Event{ID: id + ":1", AttemptID: id, RequestID: "lifecycle", AppID: app, Kind: "started", Revision: 1, Purpose: "inference", Quality: "pending", ReserveTokens: 100, ReserveMicro: 100, Month: month(time.Now()), At: time.Now().UTC()}
		ctx = context.WithValue(ctx, reservationHashKey{}, eventDigest(e))
		if err := s.PrepareAttempt(ctx, e); err != nil {
			t.Fatal("Prepare", err)
		}
		return ctx, e
	}
	check := func(e Event, phase, compensation string, remaining int64) {
		t.Helper()
		var actualPhase, actualComp string
		var actualRemaining int64
		if err := s.DB.QueryRowContext(c, `SELECT execution_phase,compensation_state,reserved_remaining_bytes FROM gw_attempts WHERE attempt_id=?`, e.AttemptID).Scan(&actualPhase, &actualComp, &actualRemaining); err != nil {
			t.Fatal(err)
		}
		if actualPhase != phase || actualComp != compensation || actualRemaining != remaining {
			t.Fatalf("durable state: %s %s %d", actualPhase, actualComp, actualRemaining)
		}
	}
	ctx, e := prepare()
	if rows, err := s.Records(c, Filter{RequestID: e.RequestID}); err != nil || len(rows) != 0 {
		t.Fatal("prepared intent appeared as paid invocation", rows, err)
	}
	if err := guard.Reserve(ctx, App{ID: app}, Reservation{e.AttemptID, app, e.Month, 100, 100}); err != nil {
		t.Fatal(err)
	}
	if err := compensateAttempt(ctx, s, failedCancelGuard{guard}, e); err == nil {
		t.Fatal("cancellation outage hidden")
	}
	check(e, "cancelled", "pending", maxEventBytes)
	if err := s.ArmAttempt(ctx, e); !errors.Is(err, ErrFenced) {
		t.Fatal("cancelled attempt armed", err)
	}
	if err := r.CompensateOnce(c); err != nil {
		t.Fatal("restart compensation", err)
	}
	check(e, "cancelled", "done", 0)
	late := RedisGuard{Client: r.Redis}
	if err := late.Reserve(ctx, App{ID: app}, Reservation{e.AttemptID, app, e.Month, 100, 100}); !errors.Is(err, ErrFenced) {
		t.Fatal("late reserve revived", err)
	}

	for range 12 {
		ctx, e = prepare()
		if err := guard.Reserve(ctx, App{ID: app}, Reservation{e.AttemptID, app, e.Month, 100, 100}); err != nil {
			t.Fatal(err)
		}
		var arm, cancel error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); arm = s.ArmAttempt(ctx, e) }()
		go func() { defer wg.Done(); cancel = s.CancelAttempt(ctx, e) }()
		wg.Wait()
		if (arm == nil) == (cancel == nil) {
			t.Fatalf("Arm/cancel both won or failed: %v %v", arm, cancel)
		}
		if arm == nil {
			check(e, "inflight", "none", maxEventBytes)
			if err := r.CompensateOnce(c); err != nil {
				t.Fatal(err)
			}
			check(e, "inflight", "none", maxEventBytes)
			if err := s.ArmAttempt(ctx, e); !errors.Is(err, ErrFenced) {
				t.Fatal("Arm replay allowed a second call", err)
			}
			// Fixture never called a vendor; verified zero completion is legitimate only here.
			completion := cancelEvent(e)
			if err := s.SaveOutbox(ctx, completion); err != nil {
				t.Fatal(err)
			}
			if err := s.Apply(ctx, completion); err != nil {
				t.Fatal(err)
			}
			if err := guard.Settle(ctx, completion); err != nil {
				t.Fatal(err)
			}
			if err := s.DeliveredOutbox(ctx, completion); err != nil {
				t.Fatal(err)
			}
		} else if err := compensateAttempt(ctx, s, guard, e); err != nil {
			t.Fatal(err)
		}
	}
	// 一条 Redis 失败不能让同批尚未执行的取消再等待完整崩溃租约。
	for range 4 {
		ctx, pending := prepare()
		if err := s.CancelAttempt(ctx, pending); err != nil {
			t.Fatal(err)
		}
	}
	fault := &Runtime{Engine: &Engine{Store: s, Guard: failedCancelGuard{guard}}}
	if err := fault.CompensateOnce(c); err == nil {
		t.Fatal("injected failure hidden")
	}
	var leased int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_attempts WHERE app_id=? AND compensation_state='pending' AND compensation_lease_until IS NOT NULL`, app).Scan(&leased); err != nil || leased != 0 {
		t.Fatal("unstarted claims remained leased after live exit", leased, err)
	}
	if _, err := s.DB.ExecContext(c, `UPDATE gw_attempts SET next_retry_at=UTC_TIMESTAMP(6) WHERE app_id=? AND compensation_state='pending'`, app); err != nil {
		t.Fatal(err)
	}
	if err := r.CompensateOnce(c); err != nil {
		t.Fatal(err)
	}
	ctx, e = prepare()
	if err := guard.Reserve(ctx, App{ID: app}, Reservation{e.AttemptID, app, e.Month, 100, 100}); err != nil {
		t.Fatal(err)
	}
	if err := s.ArmAttempt(ctx, e); err != nil {
		t.Fatal(err)
	}
	completion := cancelEvent(e)
	if err := s.SaveOutbox(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if err := guard.Settle(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if err := s.DeliveredOutbox(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(c, `UPDATE gw_metering_outbox SET delivered_at=TIMESTAMPADD(HOUR,-2,UTC_TIMESTAMP(6)) WHERE event_id=?`, completion.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := s.GCOutbox(c, time.Hour); err != nil || count != 1 {
		t.Fatal("GC", count, err)
	}
	if err := s.SaveOutbox(ctx, completion); err != nil {
		t.Fatal("late save after GC", err)
	}
	var count int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_outbox WHERE event_id=?`, completion.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("GC duplicate rebuilt occupancy", count, err)
	}
	completion.CostMicro++
	if err := s.SaveOutbox(ctx, completion); !errors.Is(err, ErrEventConflict) {
		t.Fatal("GC removed conflict proof", err)
	}
	if err := s.AuditCapacity(c); err != nil {
		t.Fatal("capacity invariant", err)
	}
}

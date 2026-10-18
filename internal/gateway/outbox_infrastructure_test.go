package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 在随机验收库验证合并后的保存事务，不向 Kafka 或 Redis 注入人工财务状态。
func testUnifiedOutboxSQL(t *testing.T, c context.Context, s *SQLStore) {
	const app = "unified-outbox-fixture"
	defer func() {
		if _, err := s.DB.ExecContext(c, `DELETE FROM gw_events WHERE attempt_id IN ('unified-attempt','unified-summary')`); err != nil {
			t.Error(err)
		}
		for _, table := range []string{"gw_metering_outbox", "gw_attempts"} {
			if _, err := s.DB.ExecContext(c, "DELETE FROM "+table+" WHERE app_id=?", app); err != nil {
				t.Error(err)
			}
		}
	}()
	start := Event{ID: "unified-attempt:1", AttemptID: "unified-attempt", RequestID: "unified-request", AppID: app, Kind: "started", Revision: 1, Purpose: "inference", At: time.Now().UTC(), Month: month(time.Now())}
	if err := s.Apply(c, start); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(c, `UPDATE gw_attempts SET reserved_remaining_bytes=1048576 WHERE attempt_id=?`, start.AttemptID); err != nil {
		t.Fatal(err)
	}
	completed := start
	completed.ID, completed.Kind, completed.Revision = "unified-attempt:2", "completed", 2
	completed.Status, completed.Quality = "cancelled", "reported"
	parallelSave := func(event Event) {
		t.Helper()
		results := make(chan error, 16)
		for range 16 {
			go func() { results <- s.SaveOutbox(c, event) }()
		}
		for range 16 {
			if err := <-results; err != nil {
				t.Error("concurrent save", err)
			}
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	parallelSave(completed)
	var total int64
	var phase string
	if err := s.DB.QueryRowContext(c, `SELECT a.reserved_remaining_bytes+o.charged_bytes,a.execution_phase FROM gw_attempts a JOIN gw_metering_outbox o ON a.attempt_id=o.attempt_id WHERE o.event_id=?`, completed.ID).Scan(&total, &phase); err != nil || total != maxEventBytes || phase != "completed" {
		t.Fatal("reservation was not converted exactly once", total, phase, err)
	}
	conflict := completed
	conflict.CostMicro++
	if err := s.SaveOutbox(c, conflict); !errors.Is(err, ErrEventConflict) {
		t.Fatal("payload conflict accepted", err)
	}
	wrong := completed
	wrong.ID, wrong.AppID = "unified-wrong:2", "another-application"
	if err := s.SaveOutbox(c, wrong); !errors.Is(err, ErrEventConflict) {
		t.Fatal("another application used reservation", err)
	}
	// 汇总没有 Prepare 记录，也必须在并发重放时保持单份 payload。
	summary := completed
	summary.ID, summary.AttemptID, summary.Purpose = "unified-summary:2", "unified-summary", "request"
	parallelSave(summary)
	for _, event := range []Event{completed, summary} {
		if err := s.Apply(c, event); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if err := s.DeliveredOutbox(c, event); err != nil {
				t.Fatal("delivery replay", err)
			}
		}
	}
	if err := s.AuditCapacity(c); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(c, `UPDATE gw_metering_outbox SET delivered_at=UTC_TIMESTAMP(6)-INTERVAL 2 HOUR WHERE app_id=?`, app); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GCOutbox(c, time.Hour); err != nil {
		t.Fatal(err)
	}
	parallelSave(completed)
	parallelSave(summary)
	var n int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_outbox WHERE app_id=?`, app).Scan(&n); err != nil || n != 0 {
		t.Fatal("GC replay recreated occupancy", n, err)
	}
	if err := s.SaveOutbox(c, conflict); !errors.Is(err, ErrEventConflict) {
		t.Fatal("GC replay ignored conflict", err)
	}
}

package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// Prepare 登记未获准调用的尝试；只有 Arm 明确提交才能进入上游。
type attemptLifecycle interface {
	PrepareAttempt(context.Context, Event, int64, int64) error
	ArmAttempt(context.Context, Event) error
	CancelAttempt(context.Context, Event) error
	FinishCancellation(context.Context, Event) error
}

func (s *SQLStore) admissionTx(c context.Context, tx *sql.Tx, app string) error {
	proof, ok := c.Value(admissionProofKey{}).(admissionProof)
	if !ok || proof.Permit.Expires <= time.Now().UnixMilli() {
		return errMeteringBarrier
	}
	var state, gen string
	var protocol int
	if err := tx.QueryRowContext(c, `SELECT state,generation,metering_protocol FROM gw_metering_control WHERE id=1 FOR SHARE`).Scan(&state, &gen, &protocol); err != nil {
		return err
	}
	if protocol != s.meteringProtocol() || state != "healthy" || gen != proof.Permit.Generation {
		return errMeteringBarrier
	}
	if err := tx.QueryRowContext(c, `SELECT state,generation FROM gw_metering_apps WHERE app_id=? FOR SHARE`, app).Scan(&state, &gen); err != nil {
		return err
	}
	if state != "healthy" || gen != proof.AppGeneration {
		return errMeteringBarrier
	}
	return nil
}

func (s *SQLStore) PrepareAttempt(c context.Context, e Event, globalLimit, appLimit int64) (result error) {
	start := time.Now()
	defer func() { s.Metrics.meteringStage("prepare", "started", start, result) }()
	raw, err := eventBytes(e)
	if err != nil {
		return err
	}
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.admissionTx(c, tx, e.AppID); err != nil {
		return err
	}
	global, app, err := s.outboxCapacity(c, tx, e.AppID)
	if err != nil {
		return err
	}
	if !s.backgroundCapacity() && (maxEventBytes > globalLimit-global || maxEventBytes > appLimit-app) {
		return ErrOutboxCapacity
	}
	proof := c.Value(admissionProofKey{}).(admissionProof)
	_, err = tx.ExecContext(c, `INSERT INTO gw_attempts(attempt_id,request_id,app_id,department,model_id,status,purpose,revision,tokens,cost_micro,at,payload,execution_phase,billing_state,reserved_remaining_bytes,prepare_generation,app_generation,start_hash,start_payload,prepare_deadline) VALUES(?,?,?,?,?,?,?,0,0,0,?,?,'prepared','pending',?,?,?,?,?,TIMESTAMPADD(SECOND,30,UTC_TIMESTAMP(6)))`, e.AttemptID, e.RequestID, e.AppID, e.Department, e.Model.ID, "prepared", e.Purpose, e.At, string(raw), maxEventBytes, proof.Permit.Generation, proof.AppGeneration, Hash(string(raw)), string(raw))
	if err != nil {
		return err
	}
	if err = s.adjustCapacity(c, tx, e.AppID, maxEventBytes); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return errors.Join(err, errPrepareCommitUnknown)
	}
	return nil
}

func (s *SQLStore) ArmAttempt(c context.Context, e Event) (result error) {
	start := time.Now()
	defer func() { s.Metrics.meteringStage("arm", "started", start, result) }()
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.admissionTx(c, tx, e.AppID); err != nil {
		return err
	}
	// 账本事件先于 attempt 加锁，与消费、修正保持统一顺序；CAS 失败全部回滚。
	if err = applyEventTx(c, tx, e); err != nil {
		return err
	}
	proof := c.Value(admissionProofKey{}).(admissionProof)
	res, err := tx.ExecContext(c, `UPDATE gw_attempts SET execution_phase='inflight' WHERE attempt_id=? AND execution_phase='prepared' AND prepare_generation=? AND app_generation=? AND start_hash=? AND prepare_deadline>UTC_TIMESTAMP(6)`, e.AttemptID, proof.Permit.Generation, proof.AppGeneration, eventDigest(e))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrFenced
	}
	return tx.Commit()
}

func (s *SQLStore) CancelAttempt(c context.Context, e Event) error {
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 与 Arm 争用同一条 attempt；不允许取消可能已经调用的 inflight。
	var phase, hash string
	if err = tx.QueryRowContext(c, `SELECT execution_phase,start_hash FROM gw_attempts WHERE attempt_id=? FOR UPDATE`, e.AttemptID).Scan(&phase, &hash); err != nil {
		return err
	}
	if hash != eventDigest(e) {
		return ErrEventConflict
	}
	if phase == "cancelled" {
		return tx.Commit()
	}
	if phase != "prepared" {
		return ErrFenced
	}
	_, err = tx.ExecContext(c, `UPDATE gw_attempts SET execution_phase='cancelled',compensation_state='pending',next_retry_at=UTC_TIMESTAMP(6),prepare_deadline=NULL WHERE attempt_id=? AND execution_phase='prepared'`, e.AttemptID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLStore) FinishCancellation(c context.Context, e Event) error {
	// Redis 已确认取消后再释放 SQL。失败只暂时多占空间，不提前退未知预算。
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, _, err = s.outboxCapacity(c, tx, e.AppID); err != nil {
		return err
	}
	if err = applyEventTx(c, tx, cancelEvent(e)); err != nil {
		return err
	}
	var phase, hash string
	var remaining int64
	if err = tx.QueryRowContext(c, `SELECT execution_phase,start_hash,reserved_remaining_bytes FROM gw_attempts WHERE attempt_id=? FOR UPDATE`, e.AttemptID).Scan(&phase, &hash, &remaining); err != nil {
		return err
	}
	if phase != "cancelled" || hash != eventDigest(e) {
		return ErrFenced
	}
	if err = s.adjustCapacity(c, tx, e.AppID, -remaining); err != nil {
		return err
	}
	_, err = tx.ExecContext(c, `UPDATE gw_attempts SET compensation_state='done',redis_cancel_confirmed=1,reserved_remaining_bytes=0,compensation_lease_until=NULL WHERE attempt_id=?`, e.AttemptID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func cancelReservation(c context.Context, g Guard, e Event) error {
	canceller, ok := g.(interface {
		Cancel(context.Context, Event) error
	})
	if !ok {
		return ErrUnavailable
	}
	return canceller.Cancel(c, e)
}

func compensateAttempt(c context.Context, store attemptLifecycle, g Guard, e Event) error {
	if err := store.CancelAttempt(c, e); err != nil {
		return err
	}
	if err := cancelReservation(c, g, e); err != nil {
		return err
	}
	return store.FinishCancellation(c, e)
}

type memoryAttempt struct {
	Start     Event
	Phase     string
	Remaining int64
}

func (s *MemoryStore) PrepareAttempt(_ context.Context, e Event, globalLimit, appLimit int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]memoryAttempt{}
	}
	if _, exists := s.attempts[e.AttemptID]; exists {
		return ErrEventConflict
	}
	if len(s.attempts) >= maxMemoryAttempts {
		return ErrUnavailable
	}
	var global, app int64
	global, app = s.reservedGlobal, s.reservedApps[e.AppID]
	if globalLimit > 0 && maxEventBytes > globalLimit-global || appLimit > 0 && maxEventBytes > appLimit-app {
		return ErrOutboxCapacity
	}
	s.attempts[e.AttemptID] = memoryAttempt{Start: e, Phase: "prepared", Remaining: maxEventBytes}
	if s.reservedApps == nil {
		s.reservedApps = map[string]int64{}
	}
	s.reservedGlobal += maxEventBytes
	s.reservedApps[e.AppID] += maxEventBytes
	return nil
}
func (s *MemoryStore) ArmAttempt(_ context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempt, ok := s.attempts[e.AttemptID]
	if !ok || attempt.Phase != "prepared" {
		return ErrFenced
	}
	if eventDigest(attempt.Start) != eventDigest(e) {
		return ErrEventConflict
	}
	if err := s.applyLocked(e); err != nil {
		return err
	}
	attempt.Phase = "inflight"
	s.attempts[e.AttemptID] = attempt
	return nil
}
func (s *MemoryStore) CancelAttempt(_ context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempt, ok := s.attempts[e.AttemptID]
	if !ok {
		return errors.New("unknown prepared attempt")
	}
	if eventDigest(attempt.Start) != eventDigest(e) {
		return ErrEventConflict
	}
	if attempt.Phase != "prepared" && attempt.Phase != "cancelled" {
		return ErrFenced
	}
	attempt.Phase = "cancelled"
	s.attempts[e.AttemptID] = attempt
	return nil
}
func (s *MemoryStore) FinishCancellation(_ context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempt, ok := s.attempts[e.AttemptID]
	if !ok || attempt.Phase != "cancelled" {
		return ErrFenced
	}
	if err := s.applyLocked(cancelEvent(e)); err != nil {
		return err
	}
	s.reservedGlobal -= attempt.Remaining
	s.reservedApps[e.AppID] -= attempt.Remaining
	attempt.Remaining = 0
	s.attempts[e.AttemptID] = attempt
	return nil
}

func decodeAttempt(raw []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(raw, &e)
	return e, err
}

package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

func (s *SQLStore) FreezeMetering(c context.Context, reason string) error {
	_, err := s.execMetering(c, `UPDATE gw_metering_control SET state='frozen',reason=?,updated_at=UTC_TIMESTAMP(6) WHERE id=1`, reason)
	return err
}

func (s *SQLStore) MeteringHealthy(c context.Context) error {
	var state string
	if err := s.DB.QueryRowContext(c, `SELECT state FROM gw_metering_control WHERE id=1`).Scan(&state); err != nil {
		return err
	}
	if state != "healthy" {
		return ErrUnavailable
	}
	return nil
}

func outboxCapacity(c context.Context, tx *sql.Tx, app string) (int64, int64, error) {
	var values [2]int64
	for i, id := range []string{"global", "app:" + app} {
		// 重复键直接取排他锁，避免 INSERT IGNORE 的共享锁升级形成死锁。
		if _, err := tx.ExecContext(c, `INSERT INTO gw_metering_capacity(id,used_bytes) VALUES(?,0) ON DUPLICATE KEY UPDATE id=id`, id); err != nil {
			return 0, 0, err
		}
		if err := tx.QueryRowContext(c, `SELECT used_bytes FROM gw_metering_capacity WHERE id=? FOR UPDATE`, id).Scan(&values[i]); err != nil {
			return 0, 0, err
		}
	}
	return values[0], values[1], nil
}

func adjustCapacity(c context.Context, tx *sql.Tx, app string, delta int64) error {
	for _, id := range []string{"global", "app:" + app} {
		result, err := tx.ExecContext(c, `UPDATE gw_metering_capacity SET used_bytes=used_bytes+? WHERE id=? AND used_bytes+?>=0`, delta, id, delta)
		if err != nil {
			return err
		}
		n, _ := result.RowsAffected()
		if n != 1 && delta != 0 {
			return errors.New("outbox capacity invariant violated")
		}
	}
	return nil
}

func saveOutbox(c context.Context, tx *sql.Tx, e Event, globalLimit, appLimit int64) error {
	raw, err := eventBytes(e)
	if err != nil {
		return err
	}
	globalUsed, appUsed, err := outboxCapacity(c, tx, e.AppID)
	if err != nil {
		return err
	}
	var hash string
	err = tx.QueryRowContext(c, `SELECT payload_hash FROM gw_metering_outbox WHERE event_id=? FOR UPDATE`, e.ID).Scan(&hash)
	if err == nil {
		if hash != Hash(string(raw)) {
			return ErrEventConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if e.Kind == "started" {
		var state string
		if err = tx.QueryRowContext(c, `SELECT state FROM gw_metering_control WHERE id=1 FOR SHARE`).Scan(&state); err != nil {
			return err
		}
		if state != "healthy" {
			return ErrUnavailable
		}
	}
	var remaining int64
	var slotApp string
	err = tx.QueryRowContext(c, `SELECT app_id,remaining_bytes FROM gw_outbox_slots WHERE attempt_id=? FOR UPDATE`, e.AttemptID).Scan(&slotApp, &remaining)
	hasSlot := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if hasSlot && slotApp != e.AppID {
		return ErrEventConflict
	}
	delta := int64(len(raw))
	if e.Kind == "started" {
		if hasSlot {
			return ErrEventConflict
		}
		delta += maxEventBytes
	} else if e.Revision == 2 && hasSlot && remaining >= int64(len(raw)) {
		remaining -= int64(len(raw))
		delta = 0
		if _, err = tx.ExecContext(c, `UPDATE gw_outbox_slots SET remaining_bytes=? WHERE attempt_id=?`, remaining, e.AttemptID); err != nil {
			return err
		}
	}
	if delta > 0 && (delta > globalLimit-globalUsed || delta > appLimit-appUsed) {
		return ErrOutboxCapacity
	}
	if err = adjustCapacity(c, tx, e.AppID, delta); err != nil {
		return err
	}
	if e.Kind == "started" {
		if _, err = tx.ExecContext(c, `INSERT INTO gw_outbox_slots(attempt_id,app_id,remaining_bytes) VALUES(?,?,?)`, e.AttemptID, e.AppID, maxEventBytes); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(c, `INSERT INTO gw_metering_outbox(event_id,attempt_id,app_id,revision,payload,payload_hash,charged_bytes,next_retry_at) VALUES(?,?,?,?,?,?,?,UTC_TIMESTAMP(6))`, e.ID, e.AttemptID, e.AppID, e.Revision, raw, Hash(string(raw)), len(raw))
	return err
}

func (s *SQLStore) SaveOutbox(c context.Context, e Event, globalLimit, appLimit int64) error {
	if e.Kind == "started" {
		cfg, err := s.Load(c)
		if err != nil {
			return err
		}
		if n := cfg.Apps[e.AppID].OutboxBytes; n > 0 {
			appLimit = n
		}
	}
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = saveOutbox(c, tx, e, globalLimit, appLimit); err != nil {
		return err
	}
	return tx.Commit()
}

// 修正版本、审计与补偿同事务提交；调用方重试同一操作不会再分配版本。
func (s *SQLStore) CorrectOutbox(c context.Context, actor, key string, e Event, globalLimit, appLimit int64) (Event, error) {
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(c, `SELECT payload FROM gw_config WHERE id=1 FOR UPDATE`).Scan(&raw); err != nil {
		return Event{}, err
	}
	var cfg Config
	if err = json.Unmarshal(raw, &cfg); err != nil {
		return Event{}, err
	}
	op := Hash(actor + "\x00" + key)
	input, _ := json.Marshal(struct {
		Attempt string
		App     string
		Usage   Usage
	}{e.AttemptID, e.AppID, e.Usage})
	var previousHash string
	err = tx.QueryRowContext(c, `SELECT input_hash,payload FROM gw_usage_corrections WHERE operation_id=?`, op).Scan(&previousHash, &raw)
	if err == nil {
		if previousHash != Hash(string(input)) {
			return Event{}, ErrEventConflict
		}
		if err = json.Unmarshal(raw, &e); err != nil {
			return Event{}, err
		}
		return e, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Event{}, err
	}
	if cfg.CorrectionRevisions == nil {
		cfg.CorrectionRevisions = map[string]int64{}
	}
	if last := cfg.CorrectionRevisions[e.AttemptID]; last >= e.Revision {
		e.Revision = last + 1
	}
	e.ID = e.AttemptID + ":" + strconv.FormatInt(e.Revision, 10)
	cfg.CorrectionRevisions[e.AttemptID] = e.Revision
	cfg.Version++
	raw, err = json.Marshal(cfg)
	if err != nil || len(raw) > maxConfigBytes {
		return Event{}, errors.New("correction configuration exceeds capacity")
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_config SET payload=?,version=? WHERE id=1`, raw, cfg.Version); err != nil {
		return Event{}, err
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_audit(id,actor,action,version,at) VALUES(?,?,?,?,?)`, ID("aud_"), actor, "usage.correct", cfg.Version, time.Now().UTC()); err != nil {
		return Event{}, err
	}
	if n := cfg.Apps[e.AppID].OutboxBytes; n > 0 {
		appLimit = n
	}
	if err = saveOutbox(c, tx, e, globalLimit, appLimit); err != nil {
		return Event{}, err
	}
	raw, err = eventBytes(e)
	if err != nil {
		return Event{}, err
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_usage_corrections(operation_id,input_hash,payload) VALUES(?,?,?)`, op, Hash(string(input)), raw); err != nil {
		return Event{}, err
	}
	return e, tx.Commit()
}

func (s *SQLStore) PublishedOutbox(c context.Context, e Event, epoch int64, success bool) error {
	if success {
		_, err := s.execMetering(c, `UPDATE gw_metering_outbox SET published=1,lease_until=NULL WHERE event_id=? AND (?=0 OR claim_epoch=?)`, e.ID, epoch, epoch)
		return err
	}
	_, err := s.execMetering(c, `UPDATE gw_metering_outbox SET retry_count=retry_count+1,lease_until=NULL,next_retry_at=TIMESTAMPADD(SECOND,LEAST(30,POW(2,LEAST(retry_count,5)))+MOD(claim_epoch,3),UTC_TIMESTAMP(6)) WHERE event_id=? AND claim_epoch=? AND published=0`, e.ID, epoch)
	return err
}

func (s *SQLStore) ClaimOutbox(c context.Context) ([]OutboxDelivery, error) {
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(c, `SELECT payload,claim_epoch FROM gw_metering_outbox WHERE published=0 AND delivered=0 AND next_retry_at<=UTC_TIMESTAMP(6) AND (lease_until IS NULL OR lease_until<UTC_TIMESTAMP(6)) ORDER BY next_retry_at,event_id LIMIT 32 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return nil, err
	}
	items := []OutboxDelivery{}
	bytes := 0
	for rows.Next() {
		var raw []byte
		var item OutboxDelivery
		if err = rows.Scan(&raw, &item.Epoch); err != nil {
			rows.Close()
			return nil, err
		}
		if bytes+len(raw) > 4<<20 {
			break
		}
		if err = json.Unmarshal(raw, &item.Event); err != nil {
			rows.Close()
			return nil, err
		}
		if _, err = eventBytes(item.Event); err != nil {
			rows.Close()
			return nil, err
		}
		item.Epoch++
		items = append(items, item)
		bytes += len(raw)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if _, err = tx.ExecContext(c, `UPDATE gw_metering_outbox SET claim_epoch=?,lease_until=TIMESTAMPADD(SECOND,15,UTC_TIMESTAMP(6)) WHERE event_id=?`, item.Epoch, item.Event.ID); err != nil {
			return nil, err
		}
	}
	return items, tx.Commit()
}

// 消费者实际读到事件并完成结算，足以证明交付；ACK 标记丢失不会泄漏容量。
func (s *SQLStore) DeliveredOutbox(c context.Context, e Event) error {
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, _, err = outboxCapacity(c, tx, e.AppID); err != nil {
		return err
	}
	var charged int64
	var hash string
	err = tx.QueryRowContext(c, `SELECT charged_bytes,payload_hash FROM gw_metering_outbox WHERE event_id=? FOR UPDATE`, e.ID).Scan(&charged, &hash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		raw, encErr := eventBytes(e)
		if encErr != nil {
			return encErr
		}
		if hash != Hash(string(raw)) {
			return ErrEventConflict
		}
		if _, err = tx.ExecContext(c, `UPDATE gw_metering_outbox SET published=1,delivered=1,charged_bytes=0,payload=NULL,lease_until=NULL WHERE event_id=?`, e.ID); err != nil {
			return err
		}
	}
	if e.Kind == "completed" && e.Purpose != "request" {
		var remaining int64
		var app string
		err = tx.QueryRowContext(c, `SELECT app_id,remaining_bytes FROM gw_outbox_slots WHERE attempt_id=? FOR UPDATE`, e.AttemptID).Scan(&app, &remaining)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if app != e.AppID {
				return fmt.Errorf("%w: attempt application", ErrEventConflict)
			}
			charged += remaining
			if _, err = tx.ExecContext(c, `UPDATE gw_outbox_slots SET remaining_bytes=0 WHERE attempt_id=?`, e.AttemptID); err != nil {
				return err
			}
		}
	}
	if err = adjustCapacity(c, tx, e.AppID, -charged); err != nil {
		return err
	}
	return tx.Commit()
}

package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
)

func (s *SQLStore) FreezeMetering(c context.Context, reason string) error {
	_, err := s.execMetering(c, `UPDATE gw_metering_control SET state='frozen',reason=?,updated_at=UTC_TIMESTAMP(6) WHERE id=1`, reason)
	return err
}

func (s *SQLStore) MeteringHealthy(c context.Context) error {
	var state string
	var protocol int
	if err := s.DB.QueryRowContext(c, `SELECT state,metering_protocol FROM gw_metering_control WHERE id=1`).Scan(&state, &protocol); err != nil {
		return err
	}
	if state != "healthy" || protocol != meteringProtocol {
		return ErrUnavailable
	}
	return nil
}

// 保存只转换 attempt 预留与 payload 占用；所有容量准入已在请求入口完成。
func (s *SQLStore) saveOutbox(c context.Context, tx *sql.Tx, e Event) error {
	raw, err := eventBytes(e)
	if err != nil {
		return err
	}
	hash := Hash(string(raw))
	checkExisting := func(lock bool) (bool, error) {
		query := `SELECT payload_hash FROM gw_metering_outbox WHERE event_id=?`
		if lock {
			query += ` FOR UPDATE`
		}
		var previous string
		err := tx.QueryRowContext(c, query, e.ID).Scan(&previous)
		if errors.Is(err, sql.ErrNoRows) {
			if lock {
				return false, nil
			}
			return ledgerDelivered(c, tx, e)
		}
		if err != nil {
			return false, err
		}
		if previous != hash {
			return false, ErrEventConflict
		}
		return true, nil
	}
	if exists, err := checkExisting(true); exists || err != nil {
		return err
	}
	var app string
	var remaining int64
	err = tx.QueryRowContext(c, `SELECT app_id,reserved_remaining_bytes FROM gw_attempts WHERE attempt_id=? FOR UPDATE`, e.AttemptID).Scan(&app, &remaining)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	hasAttempt := err == nil
	if hasAttempt && app != e.AppID {
		return ErrEventConflict
	}
	// READ COMMITTED 下不存在的事件行不提供互斥；等待 attempt 后重新核验并发保存及交付。
	if exists, err := checkExisting(false); exists || err != nil {
		return err
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_metering_outbox(event_id,attempt_id,app_id,revision,payload,payload_hash,charged_bytes,next_retry_at) VALUES(?,?,?,?,?,?,?,UTC_TIMESTAMP(6))`, e.ID, e.AttemptID, e.AppID, e.Revision, raw, hash, len(raw)); err != nil {
		// 请求汇总没有 attempt 锁；唯一键冲突后核对内容，不能吞掉不同 payload。
		var duplicate *mysql.MySQLError
		if errors.As(err, &duplicate) && duplicate.Number == 1062 {
			if exists, checkErr := checkExisting(false); exists || checkErr != nil {
				return checkErr
			}
		}
		return err
	}
	if e.Revision == 2 && remaining >= int64(len(raw)) {
		if _, err = tx.ExecContext(c, `UPDATE gw_attempts SET reserved_remaining_bytes=reserved_remaining_bytes-? WHERE attempt_id=?`, len(raw), e.AttemptID); err != nil {
			return err
		}
	}
	if hasAttempt && e.Revision == 2 && e.Purpose != "request" {
		_, err = tx.ExecContext(c, `UPDATE gw_attempts SET execution_phase='completed' WHERE attempt_id=? AND execution_phase='inflight'`, e.AttemptID)
	}
	return err
}

func (s *SQLStore) SaveOutbox(c context.Context, e Event) error {
	if e.Kind != "completed" {
		return ErrUnavailable
	}
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.saveOutbox(c, tx, e); err != nil {
		return err
	}
	return tx.Commit()
}

// 修正版本、审计与补偿同事务提交；调用方重试同一操作不会再分配版本。
func (s *SQLStore) CorrectOutbox(c context.Context, actor, key string, e Event) (Event, error) {
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback()
	if err = checkAppBarrier(c, tx, e.AppID, true); err != nil {
		return Event{}, err
	}
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
	if _, err = tx.ExecContext(c, `UPDATE gw_config SET payload=?,version=? WHERE id=1`, string(raw), cfg.Version); err != nil {
		return Event{}, err
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_audit(id,actor,action,version,at) VALUES(?,?,?,?,?)`, ID("aud_"), actor, "usage.correct", cfg.Version, time.Now().UTC()); err != nil {
		return Event{}, err
	}
	if err = s.saveOutbox(c, tx, e); err != nil {
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
	var hash string
	err = tx.QueryRowContext(c, `SELECT payload_hash FROM gw_metering_outbox WHERE event_id=? FOR UPDATE`, e.ID).Scan(&hash)
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
		if _, err = tx.ExecContext(c, `UPDATE gw_metering_outbox SET published=1,delivered=1,charged_bytes=0,payload=NULL,lease_until=NULL,delivered_at=COALESCE(delivered_at,UTC_TIMESTAMP(6)) WHERE event_id=?`, e.ID); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_events SET delivered_at=COALESCE(delivered_at,UTC_TIMESTAMP(6)) WHERE id=?`, e.ID); err != nil {
		return err
	}
	if e.Kind == "completed" && e.Purpose != "request" {
		var app string
		err = tx.QueryRowContext(c, `SELECT app_id FROM gw_attempts WHERE attempt_id=? FOR UPDATE`, e.AttemptID).Scan(&app)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if app != e.AppID {
				return fmt.Errorf("%w: attempt application", ErrEventConflict)
			}
			if _, err = tx.ExecContext(c, `UPDATE gw_attempts SET reserved_remaining_bytes=0 WHERE attempt_id=?`, e.AttemptID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// 清理 Outbox 后仍保留账本交付凭据；只入账而未结算的事件不能走此去重路径。
func ledgerDelivered(c context.Context, tx *sql.Tx, e Event) (bool, error) {
	var raw []byte
	err := tx.QueryRowContext(c, `SELECT payload FROM gw_events WHERE id=? AND delivered_at IS NOT NULL`, e.ID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	previous, err := decodeAttempt(raw)
	if err != nil {
		return false, err
	}
	if eventDigest(previous) != eventDigest(e) {
		return false, ErrEventConflict
	}
	return true, nil
}

func (s *SQLStore) GCOutbox(c context.Context, retain time.Duration) (int, error) {
	if retain < time.Hour {
		return 0, errors.New("outbox GC retention must be at least one hour")
	}
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(c, `SELECT o.event_id FROM gw_metering_outbox o WHERE o.delivered=1 AND o.charged_bytes=0 AND o.payload IS NULL AND o.delivered_at<TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6)) AND EXISTS(SELECT 1 FROM gw_events e WHERE e.id=o.event_id AND e.delivered_at IS NOT NULL) ORDER BY o.delivered_at,o.event_id LIMIT 100 FOR UPDATE SKIP LOCKED`, -int64(retain.Seconds()))
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		if _, err = tx.ExecContext(c, `DELETE FROM gw_metering_outbox WHERE event_id=? AND delivered=1 AND charged_bytes=0`, id); err != nil {
			return 0, err
		}
	}
	return len(ids), tx.Commit()
}

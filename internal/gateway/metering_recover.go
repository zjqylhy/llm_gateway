package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type RecoveryManifest struct {
	Version     int                   `json:"version"`
	Source      StreamIdentity        `json:"source"`
	Checkpoints []PartitionCheckpoint `json:"checkpoints"`
	Ranges      []PartitionRange      `json:"ranges"`
	Generation  string                `json:"generation"`
	Events      int64                 `json:"ledger_events"`
	Outbox      int64                 `json:"outbox_pending"`
	OutboxHash  string                `json:"outbox_hash"`
	Hash        string                `json:"manifest_hash"`
}

type recoveryConnectionKey struct{}

// 恢复写入复用持有 GET_LOCK 的连接，连接丢失后不会通过连接池继续提交。
func (s *SQLStore) beginMeteringTx(c context.Context, options *sql.TxOptions) (*sql.Tx, error) {
	if conn, ok := c.Value(recoveryConnectionKey{}).(*sql.Conn); ok {
		return conn.BeginTx(c, options)
	}
	return s.DB.BeginTx(c, options)
}
func (s *SQLStore) execMetering(c context.Context, q string, args ...any) (sql.Result, error) {
	if conn, ok := c.Value(recoveryConnectionKey{}).(*sql.Conn); ok {
		return conn.ExecContext(c, q, args...)
	}
	return s.DB.ExecContext(c, q, args...)
}
func checkRecoveryConnection(c context.Context) error {
	if conn, ok := c.Value(recoveryConnectionKey{}).(*sql.Conn); ok {
		return conn.PingContext(c)
	}
	return nil
}

func manifestHash(m RecoveryManifest) string {
	m.Hash = ""
	b, _ := json.Marshal(m)
	return Hash(string(b))
}

func (s *SQLStore) maintenanceLock(c context.Context) (*sql.Conn, func(), error) {
	conn, err := s.DB.Conn(c)
	if err != nil {
		return nil, nil, err
	}
	var db string
	if err = conn.QueryRowContext(c, `SELECT DATABASE()`).Scan(&db); err != nil {
		conn.Close()
		return nil, nil, err
	}
	name := "gw:recovery:" + Hash(db)[:40]
	var acquired int
	if err = conn.QueryRowContext(c, `SELECT GET_LOCK(?,0)`, name).Scan(&acquired); err != nil || acquired != 1 {
		conn.Close()
		return nil, nil, errors.New("another metering maintenance command holds the recovery lock")
	}
	release := func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_, _ = conn.ExecContext(ctx, `SELECT RELEASE_LOCK(?)`, name)
		_ = conn.Close()
	}
	return conn, release, nil
}

// plan 不加入消费组，不更改 SQL、Redis 或 Kafka offset；输出仅含位置与摘要。
func PlanMeteringRecovery(c context.Context, r *Runtime) (RecoveryManifest, error) {
	m := RecoveryManifest{Version: 1}
	s := r.Engine.Store.(*SQLStore)
	var err error
	m.Source, m.Checkpoints, err = s.Checkpoints(c)
	if err != nil {
		return m, err
	}
	actual, ranges, err := r.KafkaRanges(c)
	if err != nil {
		return m, err
	}
	m.Ranges = ranges
	if !sameSource(m.Source, actual) {
		return m, fmt.Errorf("%w: source cluster/topic/group/partitions changed", ErrCheckpoint)
	}
	for i, p := range m.Checkpoints {
		if p.Partition != ranges[i].Partition {
			return m, ErrCheckpoint
		}
		if err = r.verifyAnchor(c, p, ranges[i]); err != nil {
			return m, err
		}
	}
	if err = s.DB.QueryRowContext(c, `SELECT generation FROM gw_metering_control WHERE id=1`).Scan(&m.Generation); err != nil {
		return m, err
	}
	if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_events`).Scan(&m.Events); err != nil {
		return m, err
	}
	rows, err := s.DB.QueryContext(c, `SELECT event_id,payload_hash FROM gw_metering_outbox WHERE delivered=0 ORDER BY event_id LIMIT 10001`)
	if err != nil {
		return m, err
	}
	defer rows.Close()
	var fingerprint strings.Builder
	for rows.Next() {
		var id, hash string
		if err = rows.Scan(&id, &hash); err != nil {
			return m, err
		}
		m.Outbox++
		if m.Outbox > 10000 {
			return m, errors.New("recovery plan exceeds 10000 pending outbox events")
		}
		fingerprint.WriteString(id)
		fingerprint.WriteByte(0)
		fingerprint.WriteString(hash)
		fingerprint.WriteByte(0)
	}
	if err = rows.Err(); err != nil {
		return m, err
	}
	m.OutboxHash = Hash(fingerprint.String())
	m.Hash = manifestHash(m)
	return m, nil
}

// 新部署只在 SQL/Kafka 都为空时自动创建基线；存量基线必须停写并核验。
func InitializeCheckpoints(c context.Context, r *Runtime, existing bool) error {
	s := r.Engine.Store.(*SQLStore)
	var count int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_stream`).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		_, _, err := s.Checkpoints(c)
		return err
	}
	if existing && os.Getenv("GW_METERING_RECOVERY_ACK") != "writers-stopped" {
		return errors.New("checkpoint initialization requires GW_METERING_RECOVERY_ACK=writers-stopped")
	}
	lock, release, err := s.maintenanceLock(c)
	if err != nil {
		return err
	}
	defer release()
	c = context.WithValue(c, recoveryConnectionKey{}, lock)
	if existing {
		if err = r.ensureGroupStopped(c); err != nil {
			return err
		}
		if err = s.FreezeMetering(c, "checkpoint_initialization"); err != nil {
			return err
		}
		if err = r.Redis.Set(c, "gw:metering:frozen", "checkpoint_initialization", 0).Err(); err != nil {
			return err
		}
	}
	source, ranges, err := r.KafkaRanges(c)
	if err != nil {
		return err
	}
	source.ID = ID("stream_")
	var events, outbox int
	if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_events`).Scan(&events); err != nil {
		return err
	}
	if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_outbox WHERE delivered=0`).Scan(&outbox); err != nil {
		return err
	}
	empty := events == 0 && outbox == 0
	for _, p := range ranges {
		empty = empty && p.End == 0 && p.First == 0
	}
	if !empty && !existing {
		return errors.New("nonempty deployment requires offline metering-checkpoint-init")
	}
	if !empty {
		if _, err = auditMetering(c, r, true); err != nil {
			return err
		}
	}
	checkpoints := []PartitionCheckpoint{}
	for _, limits := range ranges {
		p := PartitionCheckpoint{Partition: limits.Partition, Base: limits.End, Next: limits.End, Anchor: -1, Generation: -1}
		if limits.End > limits.First {
			reader, err := r.partitionReader(p.Partition, limits.First)
			if err != nil {
				return err
			}
			cursor := limits.First
			for cursor < limits.End {
				fetch, done := context.WithTimeout(c, 5*time.Second)
				msg, err := reader.FetchMessage(fetch)
				done()
				if err != nil {
					reader.Close()
					return err
				}
				if msg.Offset >= limits.End {
					reader.Close()
					return ErrCheckpoint
				}
				e, err := decodeMetering(msg)
				if err != nil {
					reader.Close()
					return err
				}
				var raw []byte
				if err = s.DB.QueryRowContext(c, `SELECT payload FROM gw_events WHERE id=?`, e.ID).Scan(&raw); err != nil {
					reader.Close()
					return errors.New("retained Kafka event missing from ledger; cannot initialize baseline")
				}
				var saved Event
				if json.Unmarshal(raw, &saved) != nil {
					reader.Close()
					return ErrEventConflict
				}
				a, _ := eventBytes(saved)
				b, _ := eventBytes(e)
				if Hash(string(a)) != Hash(string(b)) {
					reader.Close()
					return ErrEventConflict
				}
				p.Anchor, p.Hash = msg.Offset, recordHash(msg)
				cursor = msg.Offset + 1
			}
			reader.Close()
		}
		if p.Next > 0 && p.Anchor < 0 {
			return errors.New("partition has no retained anchor; restore backup provenance first")
		}
		checkpoints = append(checkpoints, p)
	}
	tx, err := s.beginMeteringTx(c, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(c, `INSERT INTO gw_metering_stream(id,cluster_id,topic,group_id,stream_id,partitions) VALUES(1,?,?,?,?,?)`, source.Cluster, source.Topic, source.Group, source.ID, source.Partitions)
	if err != nil {
		return err
	}
	for _, p := range checkpoints {
		if _, err = tx.ExecContext(c, `INSERT INTO gw_metering_checkpoint(partition_id,base_offset,next_offset,anchor_offset,anchor_hash,owner_epoch,kafka_generation,owner_id) VALUES(?,?,?,?,?,0,-1,'')`, p.Partition, p.Base, p.Next, p.Anchor, p.Hash); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_audit(id,actor,action,version,at) SELECT ?,'recovery-command','metering.checkpoint.initialize',version,UTC_TIMESTAMP(6) FROM gw_config WHERE id=1`, ID("aud_")); err != nil {
		return err
	}
	return tx.Commit()
}

func validateManifest(m RecoveryManifest) error {
	if m.Version != 1 || m.Hash == "" || m.Hash != manifestHash(m) || m.Source.Partitions < 1 || m.Source.Partitions > 128 || len(m.Checkpoints) != m.Source.Partitions || len(m.Ranges) != m.Source.Partitions {
		return errors.New("invalid recovery manifest hash or shape")
	}
	for i, p := range m.Checkpoints {
		limits := m.Ranges[i]
		if p.Partition != i || limits.Partition != i || p.Base < 0 || p.Next < p.Base || p.Next < 0 || p.Next < limits.First || p.Next > limits.End {
			return ErrCheckpoint
		}
	}
	return nil
}
func LoadRecoveryManifest(path string) (RecoveryManifest, error) {
	var m RecoveryManifest
	file, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer file.Close()
	b, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return m, err
	}
	if len(b) > 1<<20 {
		return m, errors.New("manifest exceeds 1 MiB")
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		return m, err
	}
	if d.Decode(new(any)) != io.EOF {
		return m, errors.New("manifest contains trailing content")
	}
	return m, validateManifest(m)
}

func (r *Runtime) replayPartition(c context.Context, p PartitionCheckpoint, end int64) error {
	if p.Next == end {
		return nil
	}
	reader, err := r.partitionReader(p.Partition, p.Next)
	if err != nil {
		return err
	}
	defer reader.Close()
	cursor := p.Next
	s := r.Engine.Store.(*SQLStore)
	source, _, err := s.Checkpoints(c)
	if err != nil {
		return err
	}
	for cursor < end {
		fetch, done := context.WithTimeout(c, 5*time.Second)
		m, err := reader.FetchMessage(fetch)
		done()
		if err != nil {
			return err
		}
		if m.Offset >= end {
			return errors.New("captured replay boundary reached without accounting all records")
		}
		e, err := decodeMetering(m)
		if err != nil {
			return err
		}
		if err = verifyRecordSource(source, p, m); err != nil {
			return err
		}
		if err = s.ApplyPartition(c, p, cursor, m, e, true); err != nil {
			return err
		}
		cursor = m.Offset + 1
	}
	return nil
}

func rebuildQuotaLedger(c context.Context, r *Runtime) error {
	if err := checkRecoveryConnection(c); err != nil {
		return err
	}
	if err := r.Redis.Set(c, "gw:quota:state", "rebuilding", 0).Err(); err != nil {
		return err
	}
	var cursor uint64
	for {
		if err := checkRecoveryConnection(c); err != nil {
			return err
		}
		keys, next, err := r.Redis.Scan(c, cursor, "gw:{app:*}:*", 500).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err = r.Redis.Del(c, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	s := r.Engine.Store.(*SQLStore)
	rows, err := s.DB.QueryContext(c, `SELECT payload FROM gw_attempts WHERE purpose<>'request'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	g := RedisGuard{Client: r.Redis}
	for rows.Next() {
		if err = checkRecoveryConnection(c); err != nil {
			return err
		}
		var raw []byte
		var e Event
		if err = rows.Scan(&raw); err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if err = CheckEvent(e); err != nil {
			return err
		}
		if e.Kind == "started" {
			err = g.Reserve(c, App{ID: e.AppID}, Reservation{e.AttemptID, e.AppID, e.Month, e.ReserveTokens, e.ReserveMicro})
			if err == nil {
				err = stampReservation(c, g, e)
			}
		} else {
			err = g.Settle(c, e)
		}
		if err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	// 入账成功且重建完成后，释放 outbox；仅按该条消息的账本内容确认交付。
	// 停写恢复可取消未获准调用的准备态；inflight 始终保留未知，不能按年龄退款。
	for {
		pending, readErr := s.DB.QueryContext(c, `SELECT start_payload FROM gw_attempts WHERE execution_phase='prepared' OR compensation_state='pending' LIMIT 32`)
		if readErr != nil {
			return readErr
		}
		var prepared []Event
		for pending.Next() {
			var raw []byte
			if readErr = pending.Scan(&raw); readErr != nil {
				break
			}
			e, decodeErr := decodeAttempt(raw)
			if decodeErr != nil {
				readErr = decodeErr
				break
			}
			prepared = append(prepared, e)
		}
		if readErr == nil {
			readErr = pending.Err()
		}
		pending.Close()
		if readErr != nil {
			return readErr
		}
		if len(prepared) == 0 {
			break
		}
		for _, e := range prepared {
			if err = checkRecoveryConnection(c); err != nil {
				return err
			}
			if err = compensateAttempt(c, s, g, e); err != nil {
				return err
			}
		}
	}
	for {
		items, err := s.DB.QueryContext(c, `SELECT o.event_id,e.payload FROM gw_metering_outbox o JOIN gw_events e ON e.id=o.event_id WHERE o.delivered=0 LIMIT 32`)
		if err != nil {
			return err
		}
		batch := []Event{}
		for items.Next() {
			var id string
			var raw []byte
			if err = items.Scan(&id, &raw); err != nil {
				items.Close()
				return err
			}
			var e Event
			if err = json.Unmarshal(raw, &e); err != nil {
				items.Close()
				return err
			}
			batch = append(batch, e)
		}
		err = items.Err()
		items.Close()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		for _, e := range batch {
			if err = s.DeliveredOutbox(c, e); err != nil {
				return err
			}
		}
	}
	// completed 未进入 outbox 的 Kafka 成功路径，也必须释放 started 的容量预留。
	rows, err = s.DB.QueryContext(c, `SELECT payload FROM gw_attempts WHERE reserved_remaining_bytes>0 AND revision>=2 AND purpose<>'request' AND execution_phase<>'cancelled'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var e Event
		if err = rows.Scan(&raw); err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if err = s.DeliveredOutbox(c, e); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if err = initializeMeteringEvidence(c, s, RedisGuard{Client: r.Redis}); err != nil {
		return err
	}
	return r.Redis.Set(c, "gw:quota:state", "ready", 0).Err()
}

// apply 不解除接单冻结。SQL 进度与账本原子提交，失败后以同一 manifest 重试。
func ApplyMeteringRecovery(c context.Context, r *Runtime, m RecoveryManifest) (result MeteringAudit, err error) {
	if os.Getenv("GW_METERING_RECOVERY_ACK") != "writers-stopped" {
		return result, errors.New("recovery requires GW_METERING_RECOVERY_ACK=writers-stopped")
	}
	return applyMeteringRecovery(c, r, m)
}
func applyMeteringRecovery(c context.Context, r *Runtime, m RecoveryManifest) (result MeteringAudit, err error) {
	if err = validateManifest(m); err != nil {
		return result, err
	}
	s := r.Engine.Store.(*SQLStore)
	lock, release, err := s.maintenanceLock(c)
	if err != nil {
		return result, err
	}
	defer release()
	c = context.WithValue(c, recoveryConnectionKey{}, lock)
	if err = r.ensureGroupStopped(c); err != nil {
		return result, err
	}
	var saved []byte
	var phase string
	err = s.DB.QueryRowContext(c, `SELECT manifest,phase FROM gw_metering_recovery WHERE id=?`, m.Hash).Scan(&saved, &phase)
	if errors.Is(err, sql.ErrNoRows) {
		var unfinished int
		if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_recovery WHERE phase<>'complete'`).Scan(&unfinished); err != nil {
			return result, err
		}
		if unfinished > 0 {
			return result, errors.New("unfinished recovery exists; retry its original manifest")
		}
		if err = s.FreezeMetering(c, "offline_replay_preflight"); err != nil {
			return result, err
		}
		if err = r.Redis.Set(c, "gw:metering:frozen", "offline_replay_preflight", 0).Err(); err != nil {
			return result, err
		}
		current, e := PlanMeteringRecovery(c, r)
		if e != nil {
			return result, e
		}
		if current.Hash != m.Hash {
			return result, errors.New("plan is stale; stop writers and create a new plan")
		}
		tx, e := lock.BeginTx(c, nil)
		if e != nil {
			return result, e
		}
		defer tx.Rollback()
		if _, e = tx.ExecContext(c, `UPDATE gw_metering_control SET state='recovering',reason='offline_replay',generation=?,updated_at=UTC_TIMESTAMP(6) WHERE id=1`, ID("gen_")); e != nil {
			return result, e
		}
		raw, _ := json.Marshal(m)
		if _, e = tx.ExecContext(c, `INSERT INTO gw_metering_recovery(id,manifest,phase,updated_at) VALUES(?,?,'replay',UTC_TIMESTAMP(6))`, m.Hash, raw); e != nil {
			return result, e
		}
		if e = tx.Commit(); e != nil {
			return result, e
		}
		phase = "replay"
	} else if err != nil {
		return result, err
	} else {
		var original RecoveryManifest
		if json.Unmarshal(saved, &original) != nil || original.Hash != m.Hash || manifestHash(original) != m.Hash {
			return result, ErrCheckpoint
		}
		var state string
		if err = s.DB.QueryRowContext(c, `SELECT state FROM gw_metering_control WHERE id=1`).Scan(&state); err != nil {
			return result, err
		}
		if phase == "complete" {
			return auditMetering(c, r, false)
		}
		if state != "recovering" {
			return result, errors.New("unfinished recovery lost its maintenance state")
		}
	}
	if err = r.Redis.Set(c, "gw:metering:frozen", "offline_replay", 0).Err(); err != nil {
		return result, err
	}
	if err = r.Redis.Set(c, "gw:quota:state", "rebuilding", 0).Err(); err != nil {
		return result, err
	}
	// 连接级锁失效时立即停止；事务仍检查 recovering 与 owner epoch。
	if err = lock.PingContext(c); err != nil {
		return result, err
	}
	source, parts, e := s.Checkpoints(c)
	if e != nil {
		return result, e
	}
	actual, ranges, e := r.KafkaRanges(c)
	if e != nil {
		return result, e
	}
	if !sameSource(source, m.Source) || source.ID != m.Source.ID || !sameSource(source, actual) {
		return result, ErrCheckpoint
	}
	for i, p := range parts {
		if p.Next < m.Checkpoints[i].Next {
			return result, ErrCheckpoint
		}
		if phase == "replay" && p.Next > m.Ranges[i].End {
			return result, ErrCheckpoint
		}
		if phase == "replay" && ranges[i].End != m.Ranges[i].End {
			return result, errors.New("Kafka changed after the recovery plan; writers must remain stopped")
		}
		if err = r.verifyAnchor(c, p, ranges[i]); err != nil {
			return result, err
		}
	}
	if phase == "replay" {
		for i, p := range parts {
			claim, e := s.ClaimPartition(c, p.Partition, -1, "recovery:"+m.Hash, true)
			if e != nil {
				return result, e
			}
			if e = r.replayPartition(c, claim, m.Ranges[i].End); e != nil {
				return result, e
			}
		}
		if _, err = s.execMetering(c, `UPDATE gw_metering_recovery SET phase='outbox',updated_at=UTC_TIMESTAMP(6) WHERE id=?`, m.Hash); err != nil {
			return result, err
		}
		phase = "outbox"
	}
	if phase == "outbox" {
		bus, ok := r.Engine.Bus.(*DurableBus)
		if !ok {
			return result, ErrUnavailable
		}
		if err = DrainOutbox(c, s, bus); err != nil {
			return result, err
		}
		// 补发可能 ACK 后崩溃，重试允许重复消息，按最新 SQL 位置继续。
		_, ranges, e = r.KafkaRanges(c)
		if e != nil {
			return result, e
		}
		_, parts, e = s.Checkpoints(c)
		if e != nil {
			return result, e
		}
		for i, p := range parts {
			if err = r.verifyAnchor(c, p, ranges[i]); err != nil {
				return result, err
			}
			claim, e := s.ClaimPartition(c, p.Partition, -1, "recovery:"+m.Hash, true)
			if e != nil {
				return result, e
			}
			if e = r.replayPartition(c, claim, ranges[i].End); e != nil {
				return result, e
			}
		}
		if _, err = s.execMetering(c, `UPDATE gw_metering_recovery SET phase='rebuild',updated_at=UTC_TIMESTAMP(6) WHERE id=?`, m.Hash); err != nil {
			return result, err
		}
		phase = "rebuild"
	}
	if phase != "rebuild" {
		return result, errors.New("unknown recovery phase")
	}
	if err = rebuildQuotaLedger(c, r); err != nil {
		return result, err
	}
	_, parts, e = s.Checkpoints(c)
	if e != nil {
		return result, e
	}
	_, ranges, e = r.KafkaRanges(c)
	if e != nil {
		return result, e
	}
	offsets := []kafka.OffsetCommit{}
	for i, p := range parts {
		if p.Next != ranges[i].End {
			return result, errors.New("new Kafka records appeared during recovery")
		}
		offsets = append(offsets, kafka.OffsetCommit{Partition: p.Partition, Offset: p.Next, Metadata: "recovery:" + m.Hash})
	}
	if err = lock.PingContext(c); err != nil {
		return result, err
	}
	if err = r.ensureGroupStopped(c); err != nil {
		return result, err
	}
	if err = r.commitPositions(c, -1, "", offsets); err != nil {
		return result, err
	}
	if result, err = auditMetering(c, r, false); err != nil {
		return result, err
	}
	tx, e := lock.BeginTx(c, nil)
	if e != nil {
		return result, e
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(c, `UPDATE gw_metering_checkpoint SET kafka_generation=-1,owner_id=''`); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_metering_control SET state='frozen',reason='recovery_audited',updated_at=UTC_TIMESTAMP(6) WHERE id=1`); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_metering_recovery SET phase='complete',updated_at=UTC_TIMESTAMP(6) WHERE id=?`, m.Hash); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_audit(id,actor,action,version,at) SELECT ?,'recovery-command',?,version,UTC_TIMESTAMP(6) FROM gw_config WHERE id=1`, ID("aud_"), "metering.recover:"+m.Hash); err != nil {
		return result, err
	}
	return result, tx.Commit()
}

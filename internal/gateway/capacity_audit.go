package gateway

import (
	"context"
	"database/sql"
	"errors"
)

// 同一快照核验逻辑容量；历史元数据、索引和磁盘页不属于这一额度。
func (s *SQLStore) AuditCapacity(c context.Context) error {
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	expected := map[string]int64{"global": 0}
	rows, err := tx.QueryContext(c, `SELECT app_id,SUM(bytes) FROM (SELECT app_id,charged_bytes AS bytes FROM gw_metering_outbox UNION ALL SELECT app_id,reserved_remaining_bytes AS bytes FROM gw_attempts) occupancy GROUP BY app_id LIMIT 10001`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var app string
		var used int64
		if err = rows.Scan(&app, &used); err != nil {
			break
		}
		if used < 0 {
			err = errors.New("negative logical occupancy")
			break
		}
		expected["app:"+app] = used
		expected["global"] += used
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	if len(expected) > 10001 {
		return errors.New("capacity audit exceeds application bound")
	}
	rows, err = tx.QueryContext(c, `SELECT id,used_bytes FROM gw_metering_capacity ORDER BY id LIMIT 10002`)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for rows.Next() {
		var id string
		var used int64
		if err = rows.Scan(&id, &used); err != nil {
			break
		}
		if used != expected[id] {
			err = errors.New("outbox payload and attempt reservation disagree with capacity")
			break
		}
		seen[id] = true
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	for id, used := range expected {
		if used != 0 && !seen[id] {
			return errors.New("missing nonzero capacity row")
		}
	}
	return tx.Commit()
}

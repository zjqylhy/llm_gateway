package gateway

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"time"
)

type capacityTicket struct {
	App, Hash string
}

type capacitySnapshot struct {
	At      time.Time
	Global  int64
	Rows    int64
	Apps    map[string]int64
	Covered []string
}

// 聚合与归属回查共用 MVCC 视图，避免清除尚未被总量覆盖的本地预留。
func (s *SQLStore) sampleCapacity(c context.Context, tickets map[string]capacityTicket) (snap capacitySnapshot, err error) {
	snap.At, snap.Apps = time.Now(), map[string]int64{}
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return snap, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(c, `SELECT app_id,SUM(bytes),COUNT(*) FROM (SELECT app_id,reserved_remaining_bytes AS bytes FROM gw_attempts WHERE reserved_remaining_bytes>0 UNION ALL SELECT app_id,charged_bytes AS bytes FROM gw_metering_outbox WHERE charged_bytes>0) occupancy GROUP BY app_id LIMIT 10001`)
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		var app string
		var used, count int64
		if err = rows.Scan(&app, &used, &count); err != nil {
			break
		}
		if used < 0 || used > math.MaxInt64-snap.Global || len(snap.Apps) >= 10000 {
			err = errors.New("invalid or oversized capacity snapshot")
			break
		}
		snap.Apps[app], snap.Global = used, snap.Global+used
		snap.Rows += count
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return snap, err
	}
	ids := make([]string, 0, len(tickets))
	for id := range tickets {
		ids = append(ids, id)
	}
	for offset := 0; offset < len(ids); offset += 128 {
		end := min(offset+128, len(ids))
		args := make([]any, end-offset)
		for i, id := range ids[offset:end] {
			args[i] = id
		}
		rows, err = tx.QueryContext(c, `SELECT attempt_id,app_id,start_hash FROM gw_attempts WHERE attempt_id IN (`+strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")+`)`, args...)
		if err != nil {
			return snap, err
		}
		for rows.Next() {
			var id, app, hash string
			if err = rows.Scan(&id, &app, &hash); err != nil {
				break
			}
			if ticket := tickets[id]; ticket.App != app || ticket.Hash != hash {
				err = ErrEventConflict
				break
			}
			snap.Covered = append(snap.Covered, id)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return snap, err
		}
	}
	err = tx.Commit()
	return snap, err
}

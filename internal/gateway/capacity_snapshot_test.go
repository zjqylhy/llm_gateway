package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testCapacitySnapshotSQL(t *testing.T, ctx context.Context, s *SQLStore, dir string) {
	const app = "capacity-snapshot-fixture"
	defer func() {
		if _, err := s.DB.ExecContext(ctx, `DELETE FROM gw_metering_outbox WHERE app_id=?`, app); err != nil {
			t.Error(err)
		}
		if _, err := s.DB.ExecContext(ctx, `DELETE FROM gw_attempts WHERE app_id=?`, app); err != nil {
			t.Error(err)
		}
	}()
	insert := `INSERT INTO gw_attempts(attempt_id,request_id,app_id,department,model_id,status,purpose,revision,tokens,cost_micro,at,payload,execution_phase,billing_state,reserved_remaining_bytes,start_hash) VALUES('snapshot-probe','probe',?,'','fixture','prepared','inference',0,0,0,UTC_TIMESTAMP(6),'{}','prepared','pending',1048576,'fixture-hash')`
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, insert, app); err != nil {
		t.Fatal(err)
	}
	tickets := map[string]capacityTicket{"snapshot-probe": {App: app, Hash: "fixture-hash"}}
	snap, err := s.sampleCapacity(ctx, tickets)
	if err != nil || snap.Global != 0 || len(snap.Covered) != 0 {
		t.Fatal("uncommitted reservation incorrectly covered", snap, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snap, err = s.sampleCapacity(ctx, tickets)
	if err != nil || snap.Global != maxEventBytes || len(snap.Covered) != 1 {
		t.Fatal("committed reservation not covered", snap, err)
	}
	tx, err = s.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE gw_attempts SET reserved_remaining_bytes=reserved_remaining_bytes-4096 WHERE attempt_id='snapshot-probe'`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO gw_metering_outbox(event_id,attempt_id,app_id,revision,payload,payload_hash,charged_bytes,next_retry_at) VALUES('snapshot-probe:2','snapshot-probe',?,2,?,'fixture',4096,UTC_TIMESTAMP(6))`, app, strings.Repeat("x", 4096)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snap, err = s.sampleCapacity(ctx, tickets)
	if err != nil || snap.Global != maxEventBytes {
		t.Fatal("reservation/payload conversion changed total", snap, err)
	}
	tx, err = s.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE gw_attempts SET reserved_remaining_bytes=0 WHERE attempt_id='snapshot-probe'`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE gw_metering_outbox SET charged_bytes=0,payload=NULL,delivered=1 WHERE attempt_id='snapshot-probe'`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snap, err = s.sampleCapacity(ctx, tickets)
	if err != nil || snap.Global != 0 || len(snap.Covered) != 1 {
		t.Fatal("settled ticket not removable", snap, err)
	}
	tickets["snapshot-probe"] = capacityTicket{App: app, Hash: "wrong"}
	if _, err = s.sampleCapacity(ctx, tickets); !errors.Is(err, ErrEventConflict) {
		t.Fatal("hash conflict ignored", err)
	}
	// 在大量零占用历史中验证索引范围扫描，不只验证空表查询计划。
	ids := make([]int, 10000)
	for i := range ids {
		ids[i] = i
	}
	raw, _ := json.Marshal(ids)
	_, err = s.DB.ExecContext(ctx, `INSERT INTO gw_attempts(attempt_id,request_id,app_id,department,model_id,status,purpose,revision,tokens,cost_micro,at,payload,execution_phase,billing_state,reserved_remaining_bytes) SELECT CONCAT('snapshot-history-',n),'probe',?,'','fixture','completed','inference',2,0,0,UTC_TIMESTAMP(6),'{}','completed','settled',0 FROM JSON_TABLE(?, '$[*]' COLUMNS(n INT PATH '$')) numbers`, app, string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, `INSERT INTO gw_metering_outbox(event_id,attempt_id,app_id,revision,payload,payload_hash,charged_bytes,delivered,next_retry_at) SELECT CONCAT(attempt_id,':2'),attempt_id,app_id,2,NULL,'fixture',0,1,UTC_TIMESTAMP(6) FROM gw_attempts WHERE app_id=? AND attempt_id LIKE 'snapshot-history-%'`, app); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, `UPDATE gw_attempts SET reserved_remaining_bytes=1048576 WHERE attempt_id='snapshot-probe'`); err != nil {
		t.Fatal(err)
	}
	batchTickets := map[string]capacityTicket{}
	for i := 0; i < 130; i++ {
		batchTickets[fmt.Sprintf("snapshot-history-%d", i)] = capacityTicket{App: app}
	}
	snap, err = s.sampleCapacity(ctx, batchTickets)
	if err != nil || len(snap.Covered) != 130 || snap.Global != maxEventBytes {
		t.Fatal("batched receipt lookup failed", len(snap.Covered), snap.Global, err)
	}
	plans := map[string][]map[string]string{}
	for _, table := range []struct{ name, field string }{{"gw_attempts", "reserved_remaining_bytes"}, {"gw_metering_outbox", "charged_bytes"}} {
		rows, e := s.DB.QueryContext(ctx, "EXPLAIN SELECT app_id,SUM("+table.field+") FROM "+table.name+" WHERE "+table.field+">0 GROUP BY app_id")
		if e != nil {
			t.Fatal(e)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			v := make([][]byte, len(cols))
			args := make([]any, len(cols))
			for i := range args {
				args[i] = &v[i]
			}
			if e = rows.Scan(args...); e != nil {
				t.Fatal(e)
			}
			entry := map[string]string{}
			for i, col := range cols {
				entry[col] = string(v[i])
			}
			plans[table.name] = append(plans[table.name], entry)
			if entry["type"] != "range" {
				t.Errorf("capacity query did not use index range: %+v", entry)
			}
		}
		if e = rows.Err(); e != nil {
			t.Error(e)
		}
		rows.Close()
	}
	raw, _ = json.MarshalIndent(plans, "", "  ")
	if err = os.WriteFile(filepath.Join(dir, "capacity-explain.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func testCapacityModeSQL(t *testing.T, ctx context.Context, s *SQLStore) {
	if err := s.validateCapacityMode(ctx); err != nil {
		t.Fatal(err)
	}
	other := &SQLStore{DB: s.DB, CapacityMode: "background", CapacityWait: true}
	if s.backgroundCapacity() {
		other.CapacityMode = "locked"
		other.CapacityWait = false
	}
	if err := other.validateCapacityMode(ctx); err == nil {
		t.Fatal("mixed capacity mode accepted")
	}
	t.Setenv("GW_METERING_MIGRATION_ACK", "writers-stopped")
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO gw_instances(id,version,seen) VALUES('capacity-live-fixture',0,UTC_TIMESTAMP(6))`); err != nil {
		t.Fatal(err)
	}
	defer s.DB.ExecContext(ctx, `DELETE FROM gw_instances WHERE id='capacity-live-fixture'`)
	if err := other.migrateCapacityMode(ctx); err == nil {
		t.Fatal("mode switched while a writer was live")
	}
	if _, err := s.DB.ExecContext(ctx, `DELETE FROM gw_instances WHERE id='capacity-live-fixture'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO gw_metering_outbox(event_id,attempt_id,app_id,revision,payload,payload_hash,charged_bytes,next_retry_at) VALUES('capacity-mode-block','mode','fixture',2,'x','fixture',1,UTC_TIMESTAMP(6))`); err != nil {
		t.Fatal(err)
	}
	defer s.DB.ExecContext(ctx, `DELETE FROM gw_metering_outbox WHERE event_id='capacity-mode-block'`)
	if err := other.migrateCapacityMode(ctx); err == nil {
		t.Fatal("capacity switched with unconsumed state")
	}
	if _, err := s.DB.ExecContext(ctx, `DELETE FROM gw_metering_outbox WHERE event_id='capacity-mode-block'`); err != nil {
		t.Fatal(err)
	}
	if err := other.migrateCapacityMode(ctx); err != nil {
		t.Fatal("drained mode switch failed", err)
	}
	if err := s.validateCapacityMode(ctx); err == nil {
		t.Fatal("old writer accepted new mode")
	}
	if err := s.migrateCapacityMode(ctx); err != nil {
		t.Fatal("capacity rollback failed", err)
	}
	if err := s.validateCapacityMode(ctx); err != nil {
		t.Fatal(err)
	}
}

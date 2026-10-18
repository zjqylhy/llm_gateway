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

// 隔离数据库模拟 v1 元数据和 DDL 中断，不改业务历史。
func testCapacityProtocolSQL(t *testing.T, ctx context.Context, s *SQLStore) {
	exec := func(query string) {
		t.Helper()
		if _, err := s.DB.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	reject := func(reason string) {
		t.Helper()
		if err := s.migrateCapacityProtocol(ctx); err == nil {
			t.Fatal(reason)
		}
	}
	validate := func() {
		t.Helper()
		if err := s.validateCapacityProtocol(ctx); err != nil {
			t.Fatal(err)
		}
	}
	validate()
	exec(`ALTER TABLE gw_capacity_protocol ADD COLUMN mode VARCHAR(16) NOT NULL DEFAULT 'background'`)
	exec(`UPDATE gw_capacity_protocol SET version=1,mode='locked'`)
	exec(`UPDATE gw_metering_control SET metering_protocol=2 WHERE id=1`)
	exec(`CREATE TABLE gw_metering_capacity(id VARCHAR(80) PRIMARY KEY,used_bytes BIGINT NOT NULL)`)
	exec(`INSERT INTO gw_metering_capacity VALUES('global',0)`)
	exec(`INSERT INTO gw_metering_outbox(event_id,attempt_id,app_id,revision,payload,payload_hash,charged_bytes,delivered,next_retry_at) VALUES('capacity-legacy-history','legacy','fixture',2,NULL,'fixture',0,1,UTC_TIMESTAMP(6))`)
	defer s.DB.ExecContext(ctx, `DELETE FROM gw_metering_outbox WHERE event_id='capacity-legacy-history'`)
	if err := s.validateCapacityProtocol(ctx); err == nil {
		t.Fatal("old version accepted")
	}
	t.Setenv("GW_METERING_MIGRATION_ACK", "")
	reject("history migrated without stopped-writers confirmation")
	exec(`DELETE FROM gw_capacity_protocol WHERE id=1`)
	reject("missing marker bypassed history confirmation")
	exec(`INSERT INTO gw_capacity_protocol(id,mode,version) VALUES(1,'locked',1)`)
	t.Setenv("GW_METERING_MIGRATION_ACK", "writers-stopped")
	exec(`INSERT INTO gw_instances(id,version,seen) VALUES('capacity-live-fixture',0,UTC_TIMESTAMP(6))`)
	defer s.DB.ExecContext(ctx, `DELETE FROM gw_instances WHERE id='capacity-live-fixture'`)
	reject("live writers accepted")
	exec(`DELETE FROM gw_instances WHERE id='capacity-live-fixture'`)
	exec(`INSERT INTO gw_metering_outbox(event_id,attempt_id,app_id,revision,payload,payload_hash,charged_bytes,next_retry_at) VALUES('capacity-upgrade-block','upgrade','fixture',2,'x','fixture',1,UTC_TIMESTAMP(6))`)
	defer s.DB.ExecContext(ctx, `DELETE FROM gw_metering_outbox WHERE event_id='capacity-upgrade-block'`)
	reject("undelivered state discarded")
	exec(`DELETE FROM gw_metering_outbox WHERE event_id='capacity-upgrade-block'`)
	exec(`UPDATE gw_metering_capacity SET used_bytes=1`)
	reject("unreconciled legacy counters discarded")
	exec(`UPDATE gw_metering_capacity SET used_bytes=0`)
	exec(`UPDATE gw_capacity_protocol SET version=99`)
	reject("future capacity version accepted")
	exec(`UPDATE gw_capacity_protocol SET version=1`)
	if err := s.migrateCapacityProtocol(ctx); err != nil {
		t.Fatal("legacy upgrade", err)
	}
	validate()
	var n int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='gw_metering_capacity'`).Scan(&n); err != nil || n != 0 {
		t.Fatal("legacy table retained", n, err)
	}
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gw_capacity_protocol' AND column_name='mode'`).Scan(&n); err != nil || n != 0 {
		t.Fatal("legacy mode retained", n, err)
	}
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_outbox WHERE event_id='capacity-legacy-history' AND payload_hash='fixture' AND delivered=1`).Scan(&n); err != nil || n != 1 {
		t.Fatal("history changed", n, err)
	}
	// 模拟标记已提交、DDL 尚未完成及 DDL 已完成但最终版本未发布两种崩溃点。
	for _, beforeDDL := range []bool{true, false} {
		exec(`UPDATE gw_capacity_protocol SET version=0`)
		if beforeDDL {
			exec(`ALTER TABLE gw_capacity_protocol ADD COLUMN mode VARCHAR(16) NOT NULL DEFAULT 'background'`)
			exec(`CREATE TABLE gw_metering_capacity(id VARCHAR(80) PRIMARY KEY,used_bytes BIGINT NOT NULL)`)
		}
		if err := s.validateCapacityProtocol(ctx); err == nil {
			t.Fatal("incomplete DDL accepted")
		}
		if err := s.migrateCapacityProtocol(ctx); err != nil {
			t.Fatal("interrupted upgrade", err)
		}
		validate()
	}
	t.Setenv("GW_METERING_MIGRATION_ACK", "")
	if err := s.migrateCapacityProtocol(ctx); err != nil {
		t.Fatal("idempotent migration", err)
	}
	validate()
}

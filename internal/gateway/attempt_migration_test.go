package gateway

import (
	"context"
	"testing"
	"time"
)

// 仅在脚本创建的随机验收数据库里模拟旧状态，只清理本次随机数据库内的旧表。
func testLifecycleMigration(t *testing.T, r *Runtime, c context.Context) {
	s := r.Engine.Store.(*SQLStore)
	for _, query := range []string{
		`CREATE TABLE gw_outbox_slots(attempt_id VARCHAR(64) PRIMARY KEY,app_id VARCHAR(64) NOT NULL,remaining_bytes BIGINT NOT NULL)`,
		`CREATE TABLE gw_metering_admissions(attempt_id VARCHAR(64) PRIMARY KEY,app_id VARCHAR(64) NOT NULL,at DATETIME(6) NOT NULL)`,
		`UPDATE gw_metering_control SET metering_protocol=1 WHERE id=1`,
	} {
		if _, err := s.DB.ExecContext(c, query); err != nil {
			t.Fatal(err)
		}
	}
	var known, app string
	if err := s.DB.QueryRowContext(c, `SELECT attempt_id,app_id FROM gw_attempts WHERE purpose<>'request' AND reserved_remaining_bytes=0 LIMIT 1`).Scan(&known, &app); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(c, `INSERT INTO gw_outbox_slots VALUES(?,?,0)`, known, app); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(c, `INSERT INTO gw_metering_admissions VALUES('legacy-orphan','legacy-fixture',UTC_TIMESTAMP(6))`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(c); err == nil {
		t.Fatal("live old state migrated without stop acknowledgement")
	}
	t.Setenv("GW_METERING_MIGRATION_ACK", "writers-stopped")
	if err := s.Migrate(c); err == nil {
		t.Fatal("orphan was silently discarded")
	}
	var archives int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN ('gw_metering_admissions','gw_outbox_slots')`).Scan(&archives); err != nil || archives != 2 {
		t.Fatal("migration destroyed rollback evidence", archives, err)
	}
	var protocol int
	if err := s.DB.QueryRowContext(c, `SELECT metering_protocol FROM gw_metering_control WHERE id=1`).Scan(&protocol); err != nil || protocol != 1 {
		t.Fatal("partial migration marked complete", protocol, err)
	}
	// 外部已核实的 fixture 没有调用厂商；补齐证据后允许继续，不将真实未知清零。
	e := Event{ID: "legacy-orphan:2", AttemptID: "legacy-orphan", RequestID: "legacy-known-uncalled", AppID: "legacy-fixture", Kind: "completed", Revision: 2, Purpose: "inference", Status: "cancelled", Quality: "reported", Month: month(time.Now()), At: time.Now().UTC()}
	if err := s.Apply(c, e); err != nil {
		t.Fatal(err)
	}
	if err := r.Engine.Guard.Settle(c, e); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(c); err != nil {
		t.Fatal("verified migration", err)
	}
	if err := s.MeteringHealthy(c); err == nil {
		t.Fatal("migration reopened admission without budget rebuild")
	}
	if err := s.DB.QueryRowContext(c, `SELECT metering_protocol FROM gw_metering_control WHERE id=1`).Scan(&protocol); err != nil || protocol != 2 {
		t.Fatal(protocol, err)
	}
	// 失败路径保留证据，成功迁移只保留新架构依赖。
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN ('gw_metering_admissions','gw_outbox_slots')`).Scan(&archives); err != nil || archives != 0 {
		t.Fatal("obsolete tables retained", archives, err)
	}
	if err := s.Migrate(c); err != nil {
		t.Fatal("idempotent migration", err)
	}
	var remaining int64
	if err := s.DB.QueryRowContext(c, `SELECT reserved_remaining_bytes FROM gw_attempts WHERE attempt_id=?`, known).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatal("released reservation changed", remaining, err)
	}
	if err := s.AuditCapacity(c); err != nil {
		t.Fatal(err)
	}
}

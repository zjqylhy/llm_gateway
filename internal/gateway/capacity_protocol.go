package gateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
)

const (
	capacityProtocol = 2
	meteringProtocol = 3
)

func (s *SQLStore) validateCapacityProtocol(c context.Context) error {
	var version, protocol int
	if err := s.DB.QueryRowContext(c, `SELECT p.version,c.metering_protocol FROM gw_capacity_protocol p JOIN gw_metering_control c ON c.id=p.id WHERE p.id=1`).Scan(&version, &protocol); err != nil {
		return err
	}
	if version != capacityProtocol || protocol != meteringProtocol {
		return errors.New("capacity protocol mismatch; stop writers, drain and migrate")
	}
	return nil
}

// DDL 会隐式提交。先持久化未完成版本，再删除旧派生结构，最后发布新版本。
func (s *SQLStore) migrateCapacityProtocol(c context.Context) error {
	if _, err := s.DB.ExecContext(c, `CREATE TABLE IF NOT EXISTS gw_capacity_protocol(id INT PRIMARY KEY,version INT NOT NULL)`); err != nil {
		return err
	}
	var legacyMode, legacyTable int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gw_capacity_protocol' AND column_name='mode'`).Scan(&legacyMode); err != nil {
		return err
	}
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='gw_metering_capacity'`).Scan(&legacyTable); err != nil {
		return err
	}
	tx, err := s.beginMeteringTx(c, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var protocol int
	if err = tx.QueryRowContext(c, `SELECT metering_protocol FROM gw_metering_control WHERE id=1 FOR UPDATE`).Scan(&protocol); err != nil {
		return err
	}
	var version int
	err = tx.QueryRowContext(c, `SELECT version FROM gw_capacity_protocol WHERE id=1 FOR UPDATE`).Scan(&version)
	missing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !missing {
		return err
	}
	if version < 0 || version > capacityProtocol || protocol > meteringProtocol {
		return errors.New("unsupported capacity or metering protocol")
	}
	if !missing && version == capacityProtocol && legacyMode == 0 && legacyTable == 0 {
		_, err = tx.ExecContext(c, `UPDATE gw_metering_control SET metering_protocol=? WHERE id=1`, meteringProtocol)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	var occupied, live, history int
	if err = tx.QueryRowContext(c, `SELECT (SELECT COUNT(*) FROM gw_attempts WHERE execution_phase IN ('prepared','inflight') OR compensation_state='pending' OR reserved_remaining_bytes<>0)+(SELECT COUNT(*) FROM gw_metering_outbox WHERE charged_bytes<>0 OR delivered=0)`).Scan(&occupied); err != nil {
		return err
	}
	if err = tx.QueryRowContext(c, `SELECT COUNT(*) FROM gw_instances WHERE seen>UTC_TIMESTAMP(6)-INTERVAL 5 SECOND`).Scan(&live); err != nil {
		return err
	}
	if err = tx.QueryRowContext(c, `SELECT EXISTS(SELECT 1 FROM gw_attempts LIMIT 1) OR EXISTS(SELECT 1 FROM gw_metering_outbox LIMIT 1) OR EXISTS(SELECT 1 FROM gw_events LIMIT 1)`).Scan(&history); err != nil {
		return err
	}
	if legacyTable != 0 {
		var nonzero int
		if err = tx.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_capacity WHERE used_bytes<>0`).Scan(&nonzero); err != nil {
			return err
		}
		occupied += nonzero
	}
	fresh := missing && history == 0
	if occupied != 0 || live != 0 || !fresh && os.Getenv("GW_METERING_MIGRATION_ACK") != "writers-stopped" {
		return fmt.Errorf("capacity migration requires stopped writers and drained state: occupied=%d live=%d", occupied, live)
	}
	marker := `INSERT INTO gw_capacity_protocol(id,version) VALUES(1,0) ON DUPLICATE KEY UPDATE version=0`
	if legacyMode != 0 {
		marker = `INSERT INTO gw_capacity_protocol(id,mode,version) VALUES(1,'background',0) ON DUPLICATE KEY UPDATE version=0`
	}
	if _, err = tx.ExecContext(c, marker); err != nil {
		return err
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_metering_control SET metering_protocol=? WHERE id=1`, meteringProtocol); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if _, err = s.DB.ExecContext(c, `DROP TABLE IF EXISTS gw_metering_capacity`); err != nil {
		return err
	}
	if legacyMode != 0 {
		if _, err = s.DB.ExecContext(c, `ALTER TABLE gw_capacity_protocol DROP COLUMN mode`); err != nil {
			return err
		}
	}
	for _, index := range []struct{ table, name, fields string }{
		{"gw_attempts", "attempt_capacity", "reserved_remaining_bytes,app_id"},
		{"gw_metering_outbox", "outbox_capacity", "charged_bytes,app_id"},
	} {
		var n int
		if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? AND index_name=?`, index.table, index.name).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			if _, err = s.DB.ExecContext(c, "CREATE INDEX "+index.name+" ON "+index.table+"("+index.fields+")"); err != nil {
				return err
			}
		}
	}
	_, err = s.DB.ExecContext(c, `UPDATE gw_capacity_protocol SET version=? WHERE id=1 AND version=0`, capacityProtocol)
	return err
}

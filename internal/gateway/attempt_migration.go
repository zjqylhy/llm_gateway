package gateway

import (
	"context"
	"errors"
	"os"
)

// 显式 migrate 执行；运行角色不执行 DDL。已有财务数据必须停写并重建 Redis 后再开放。
func (s *SQLStore) migrateAttemptLifecycle(c context.Context) error {
	columns := []struct{ Name, DDL string }{
		{"execution_phase", "VARCHAR(16) NOT NULL DEFAULT ''"},
		{"billing_state", "VARCHAR(16) NOT NULL DEFAULT 'pending'"},
		{"reserved_remaining_bytes", "BIGINT NOT NULL DEFAULT 0"},
		{"prepare_generation", "VARCHAR(64) NOT NULL DEFAULT ''"},
		{"app_generation", "VARCHAR(64) NOT NULL DEFAULT ''"},
		{"start_hash", "CHAR(64) NOT NULL DEFAULT ''"},
		{"start_payload", "JSON NULL"},
		{"prepare_deadline", "DATETIME(6) NULL"},
		{"compensation_state", "VARCHAR(16) NOT NULL DEFAULT 'none'"},
		{"redis_cancel_confirmed", "BOOLEAN NOT NULL DEFAULT 0"},
		{"next_retry_at", "DATETIME(6) NULL"},
		{"compensation_epoch", "BIGINT NOT NULL DEFAULT 0"},
		{"compensation_lease_until", "DATETIME(6) NULL"},
	}
	var migrated int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gw_attempts' AND column_name='execution_phase'`).Scan(&migrated); err != nil {
		return err
	}
	var records int
	var protocolColumn int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gw_metering_control' AND column_name='metering_protocol'`).Scan(&protocolColumn); err != nil {
		return err
	}
	if protocolColumn == 0 {
		if _, err := s.DB.ExecContext(c, `ALTER TABLE gw_metering_control ADD COLUMN metering_protocol INT NOT NULL DEFAULT 1`); err != nil {
			return err
		}
	}
	var protocol int
	if err := s.DB.QueryRowContext(c, `SELECT metering_protocol FROM gw_metering_control WHERE id=1`).Scan(&protocol); err != nil {
		return err
	}
	if migrated == 0 || protocol < 2 {
		if err := s.DB.QueryRowContext(c, `SELECT (SELECT COUNT(*) FROM gw_attempts)+(SELECT COUNT(*) FROM gw_metering_outbox)`).Scan(&records); err != nil {
			return err
		}
		if records > 0 && os.Getenv("GW_METERING_MIGRATION_ACK") != "writers-stopped" {
			return errors.New("lifecycle migration requires backup, stopped writers and GW_METERING_MIGRATION_ACK=writers-stopped")
		}
	}
	if records > 0 {
		if err := s.FreezeMetering(c, "lifecycle_migration_requires_rebuild"); err != nil {
			return err
		}
	}
	for _, column := range columns {
		var exists int
		if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gw_attempts' AND column_name=?`, column.Name).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			if _, err := s.DB.ExecContext(c, "ALTER TABLE gw_attempts ADD COLUMN "+column.Name+" "+column.DDL); err != nil {
				return err
			}
		}
	}
	for _, index := range []struct{ Name, DDL string }{
		{"attempt_compensation", "(compensation_state,next_retry_at,attempt_id)"},
		{"attempt_prepared", "(execution_phase,prepare_deadline,attempt_id)"},
		{"attempt_application_phase", "(app_id,execution_phase,compensation_state)"},
		{"attempt_billing", "(billing_state,at)"},
	} {
		var exists int
		if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='gw_attempts' AND index_name=?`, index.Name).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			if _, err := s.DB.ExecContext(c, "CREATE INDEX "+index.Name+" ON gw_attempts "+index.DDL); err != nil {
				return err
			}
		}
	}
	var delivered int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gw_metering_outbox' AND column_name='delivered_at'`).Scan(&delivered); err != nil {
		return err
	}
	if delivered == 0 {
		if _, err := s.DB.ExecContext(c, `ALTER TABLE gw_metering_outbox ADD COLUMN delivered_at DATETIME(6) NULL, ADD INDEX outbox_gc(delivered,delivered_at,event_id)`); err != nil {
			return err
		}
		if _, err := s.DB.ExecContext(c, `UPDATE gw_metering_outbox SET delivered_at=UTC_TIMESTAMP(6) WHERE delivered=1`); err != nil {
			return err
		}
	}
	var ledgerDelivery int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gw_events' AND column_name='delivered_at'`).Scan(&ledgerDelivery); err != nil {
		return err
	}
	if ledgerDelivery == 0 {
		if _, err := s.DB.ExecContext(c, `ALTER TABLE gw_events ADD COLUMN delivered_at DATETIME(6) NULL`); err != nil {
			return err
		}
		if _, err := s.DB.ExecContext(c, `UPDATE gw_events e JOIN gw_metering_outbox o ON o.event_id=e.id SET e.delivered_at=o.delivered_at WHERE o.delivered=1`); err != nil {
			return err
		}
	}
	// DDL 可能被中断；空 execution_phase 使迁移可按同一规则继续，不能视为正常新状态。
	if _, err := s.DB.ExecContext(c, `UPDATE gw_attempts SET execution_phase=IF(revision>=2,'completed','inflight'),billing_state=CASE WHEN revision<2 THEN 'pending' WHEN JSON_UNQUOTE(JSON_EXTRACT(payload,'$.quality'))='estimated' THEN 'held' WHEN JSON_UNQUOTE(JSON_EXTRACT(payload,'$.quality'))='reconciled' THEN 'reconciled' ELSE 'settled' END,start_payload=IF(revision=1,payload,NULL) WHERE execution_phase=''`); err != nil {
		return err
	}
	legacyTables := false
	for _, table := range []string{"gw_outbox_slots", "gw_metering_admissions"} {
		var exists int
		if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?`, table).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			continue
		}
		legacyTables = true
		if os.Getenv("GW_METERING_MIGRATION_ACK") != "writers-stopped" {
			return errors.New("legacy table retirement requires backup and stopped writers acknowledgement")
		}
		if err := s.FreezeMetering(c, "lifecycle_migration_requires_rebuild"); err != nil {
			return err
		}
		// 上次完成核验后 DDL 中断，只重试删除，不能重新导入旧额度。
		if protocol >= 2 {
			continue
		}
		var count int
		if err := s.DB.QueryRowContext(c, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			if os.Getenv("GW_METERING_MIGRATION_ACK") != "writers-stopped" {
				return errors.New("old lifecycle tables contain state: stop writers and acknowledge migration")
			}
			if err := s.FreezeMetering(c, "lifecycle_migration_requires_rebuild"); err != nil {
				return err
			}
			if err := s.importLegacyAttempts(c); err != nil {
				return err
			}
			var orphan int
			if err := s.DB.QueryRowContext(c, "SELECT COUNT(*) FROM "+table+" o LEFT JOIN gw_attempts a ON a.attempt_id=o.attempt_id WHERE a.attempt_id IS NULL OR a.app_id<>o.app_id").Scan(&orphan); err != nil {
				return err
			}
			if orphan > 0 {
				return errors.New("legacy admission/slot lacks immutable attempt evidence; preserve tables and reconcile before migration")
			}
			if table == "gw_outbox_slots" {
				var invalid int
				if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_outbox_slots WHERE remaining_bytes<0 OR remaining_bytes>?`, maxEventBytes).Scan(&invalid); err != nil {
					return err
				}
				if invalid != 0 {
					return errors.New("legacy slot reservation outside supported event bound")
				}
				if _, err := s.DB.ExecContext(c, `UPDATE gw_attempts a JOIN gw_outbox_slots o ON o.attempt_id=a.attempt_id SET a.reserved_remaining_bytes=o.remaining_bytes`); err != nil {
					return err
				}
			}
			records += count
		}
	}
	// 两张旧表全部核验后才删除；先提交协议标记，使 DDL 重试不复活旧额度。
	if err := s.AuditCapacity(c); err != nil {
		return err
	}
	if _, err := s.DB.ExecContext(c, `UPDATE gw_metering_control SET metering_protocol=2 WHERE id=1`); err != nil {
		return err
	}
	if legacyTables {
		if _, err := s.DB.ExecContext(c, `DROP TABLE IF EXISTS gw_outbox_slots,gw_metering_admissions`); err != nil {
			return err
		}
	}
	if records > 0 || legacyTables {
		return s.FreezeMetering(c, "lifecycle_migration_requires_rebuild")
	}
	return nil
}

func (s *SQLStore) importLegacyAttempts(c context.Context) error {
	var cursor string
	for {
		rows, err := s.DB.QueryContext(c, `SELECT event_id,payload FROM gw_metering_outbox WHERE revision=1 AND payload IS NOT NULL AND event_id>? ORDER BY event_id LIMIT 32`, cursor)
		if err != nil {
			return err
		}
		var events []Event
		for rows.Next() {
			var raw []byte
			if err = rows.Scan(&cursor, &raw); err != nil {
				break
			}
			e, decodeErr := decodeAttempt(raw)
			if decodeErr != nil {
				err = decodeErr
				break
			}
			events = append(events, e)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return err
		}
		if len(events) == 0 {
			break
		}
		for _, e := range events {
			if err := s.Apply(c, e); err != nil {
				return err
			}
		}
	}
	_, err := s.DB.ExecContext(c, `UPDATE gw_attempts a JOIN gw_events e ON e.attempt_id=a.attempt_id AND e.revision=1 SET a.start_payload=e.payload WHERE a.start_payload IS NULL`)
	return err
}

package gateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/go-sql-driver/mysql"
)

const capacityProtocol = 1

func capacityOptions() (mode string, wait bool, err error) {
	mode = env("GW_OUTBOX_CAPACITY_MODE", "locked")
	if mode != "locked" && mode != "background" {
		return "", false, errors.New("GW_OUTBOX_CAPACITY_MODE requires locked or background")
	}
	wait = mode == "background"
	if raw := os.Getenv("GW_OUTBOX_CAPACITY_WAIT"); raw != "" {
		wait, err = strconv.ParseBool(raw)
		if err != nil || mode == "background" && !wait {
			return "", false, errors.New("GW_OUTBOX_CAPACITY_WAIT requires a boolean; background requires true")
		}
	}
	return
}

func (s *SQLStore) backgroundCapacity() bool { return s.CapacityMode == "background" }
func (s *SQLStore) meteringProtocol() int {
	if s.backgroundCapacity() {
		return 3
	}
	return 2
}

func (s *SQLStore) validateCapacityMode(c context.Context) error {
	var mode string
	var version int
	err := s.DB.QueryRowContext(c, `SELECT mode,version FROM gw_capacity_protocol WHERE id=1`).Scan(&mode, &version)
	var dbErr *mysql.MySQLError
	// 旧数据库只允许原锁定模式；background 必须先显式迁移。
	if errors.As(err, &dbErr) && dbErr.Number == 1146 && !s.backgroundCapacity() {
		return nil
	}
	if err != nil {
		return err
	}
	if mode != s.CapacityMode || version != capacityProtocol {
		return errors.New("capacity mode/protocol mismatch; stop writers and migrate before switching")
	}
	return nil
}

func (s *SQLStore) migrateCapacityMode(c context.Context) error {
	for _, index := range []struct{ table, name, fields string }{
		{"gw_attempts", "attempt_capacity", "reserved_remaining_bytes,app_id"},
		{"gw_metering_outbox", "outbox_capacity", "charged_bytes,app_id"},
	} {
		var n int
		if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? AND index_name=?`, index.table, index.name).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			if _, err := s.DB.ExecContext(c, "CREATE INDEX "+index.name+" ON "+index.table+"("+index.fields+")"); err != nil {
				return err
			}
		}
	}
	if _, err := s.DB.ExecContext(c, `CREATE TABLE IF NOT EXISTS gw_capacity_protocol(id INT PRIMARY KEY,mode VARCHAR(16) NOT NULL,version INT NOT NULL)`); err != nil {
		return err
	}
	tx, err := s.beginMeteringTx(c, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 控制行与准入共锁，模式切换不能跨过正在提交的 Prepare/Arm。
	var control string
	if err = tx.QueryRowContext(c, `SELECT state FROM gw_metering_control WHERE id=1 FOR UPDATE`).Scan(&control); err != nil {
		return err
	}
	mode := "locked"
	var version int
	err = tx.QueryRowContext(c, `SELECT mode,version FROM gw_capacity_protocol WHERE id=1 FOR UPDATE`).Scan(&mode, &version)
	missing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !missing {
		return err
	}
	if !missing && version != capacityProtocol {
		return errors.New("unsupported capacity protocol")
	}
	if mode != s.CapacityMode {
		var occupied, live int64
		if err = tx.QueryRowContext(c, `SELECT (SELECT COUNT(*) FROM gw_attempts WHERE execution_phase IN ('prepared','inflight') OR compensation_state='pending' OR reserved_remaining_bytes<>0)+(SELECT COUNT(*) FROM gw_metering_outbox WHERE charged_bytes<>0 OR delivered=0)`).Scan(&occupied); err != nil {
			return err
		}
		if err = tx.QueryRowContext(c, `SELECT COUNT(*) FROM gw_instances WHERE seen>UTC_TIMESTAMP(6)-INTERVAL 5 SECOND`).Scan(&live); err != nil {
			return err
		}
		if occupied != 0 || live != 0 || os.Getenv("GW_METERING_MIGRATION_ACK") != "writers-stopped" && !missing {
			return fmt.Errorf("capacity switch requires stopped writers and drained state: occupied=%d live=%d", occupied, live)
		}
		if !s.backgroundCapacity() {
			if _, err = tx.ExecContext(c, `UPDATE gw_metering_capacity SET used_bytes=0`); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_capacity_protocol(id,mode,version) VALUES(1,?,?) ON DUPLICATE KEY UPDATE mode=VALUES(mode),version=VALUES(version)`, s.CapacityMode, capacityProtocol); err != nil {
		return err
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_metering_control SET metering_protocol=? WHERE id=1`, s.meteringProtocol()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLStore) outboxCapacity(c context.Context, tx *sql.Tx, app string) (int64, int64, error) {
	if s.backgroundCapacity() {
		return 0, 0, nil
	}
	return outboxCapacity(c, tx, app, s.Metrics)
}

func (s *SQLStore) adjustCapacity(c context.Context, tx *sql.Tx, app string, delta int64) error {
	if s.backgroundCapacity() {
		return nil
	}
	return adjustCapacity(c, tx, app, delta)
}

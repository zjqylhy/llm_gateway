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
	for _, query := range []string{
		`SELECT COUNT(*) FROM gw_attempts WHERE reserved_remaining_bytes<0 OR reserved_remaining_bytes>1048576 OR (compensation_state='done' AND reserved_remaining_bytes<>0)`,
		`SELECT COUNT(*) FROM gw_metering_outbox WHERE charged_bytes<0 OR (delivered=1 AND (charged_bytes<>0 OR payload IS NOT NULL)) OR (delivered=0 AND (payload IS NULL OR charged_bytes<>OCTET_LENGTH(payload)))`,
		`SELECT COUNT(*) FROM gw_attempts a JOIN gw_metering_outbox o ON a.attempt_id=o.attempt_id AND o.revision=2 WHERE a.purpose<>'request' AND a.reserved_remaining_bytes+o.charged_bytes>1048576`,
	} {
		var invalid int
		if err = tx.QueryRowContext(c, query).Scan(&invalid); err != nil {
			return err
		}
		if invalid != 0 {
			return errors.New("invalid outbox payload or attempt reservation")
		}
	}
	return tx.Commit()
}

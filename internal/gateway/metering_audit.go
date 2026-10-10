package gateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func DrainOutbox(c context.Context, s *SQLStore, b *DurableBus) error {
	ctx, done := context.WithTimeout(c, 2*time.Minute)
	defer done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		var pending int
		if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_outbox WHERE published=0 AND delivered=0`).Scan(&pending); err != nil {
			return err
		}
		if pending == 0 {
			return nil
		}
		if err := b.RelayOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.New("outbox drain timed out; quota remains unavailable")
		case <-tick.C:
		}
	}
}

type MeteringAudit struct {
	Groups   int `json:"groups"`
	Attempts int `json:"attempts"`
	Pending  int `json:"pending"`
}

// 财务核验只允许停写执行，动态 SQL/Redis 快照不能直接比较。
func AuditMetering(c context.Context, r *Runtime) (MeteringAudit, error) {
	var result MeteringAudit
	if os.Getenv("GW_METERING_RECOVERY_ACK") != "writers-stopped" {
		return result, errors.New("stop all metering writers, then set GW_METERING_RECOVERY_ACK=writers-stopped")
	}
	s, ok := r.Engine.Store.(*SQLStore)
	if !ok || r.Redis == nil {
		return result, errors.New("SQL and Redis required")
	}
	var undelivered int
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_outbox WHERE delivered=0`).Scan(&undelivered); err != nil {
		return result, err
	}
	if undelivered > 0 {
		return result, errors.New("outbox still has unconsumed events; run offline quota-rebuild first")
	}
	if state, err := r.Redis.Get(c, "gw:quota:state").Result(); err != nil || state != "ready" {
		return result, errors.New("quota must be rebuilt before audit")
	}
	caught, err := KafkaCaughtUp(c, r)
	if err != nil {
		return result, err
	}
	if !caught {
		return result, errors.New("Kafka has unconsumed events")
	}
	rows, err := s.DB.QueryContext(c, `SELECT app_id,JSON_UNQUOTE(JSON_EXTRACT(payload,'$.month')),SUM(IF(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.kind'))='started',0,tokens)),SUM(IF(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.kind'))='started',0,cost_micro)),SUM(IF(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.kind'))='started',JSON_EXTRACT(payload,'$.reserve_tokens'),0)),SUM(IF(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.kind'))='started',JSON_EXTRACT(payload,'$.reserve_micro'),0)),SUM(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.kind'))='started') FROM gw_attempts WHERE purpose<>'request' GROUP BY app_id,JSON_UNQUOTE(JSON_EXTRACT(payload,'$.month')) LIMIT 10001`)
	if err != nil {
		return result, err
	}
	expected := map[string]Counters{}
	for rows.Next() {
		var app, month string
		var v Counters
		var pending int
		if err = rows.Scan(&app, &month, &v.Tokens, &v.Micro, &v.ReservedTokens, &v.ReservedMicro, &pending); err != nil {
			rows.Close()
			return result, err
		}
		result.Groups++
		result.Pending += pending
		if result.Groups > 10000 {
			rows.Close()
			return result, errors.New("audit exceeds 10000 application-month groups")
		}
		expected[quotaKeys(app, month, "")[0]] = v
		actual, readErr := r.Engine.Guard.Counters(c, app, month)
		if readErr != nil || actual != v {
			rows.Close()
			return result, fmt.Errorf("quota mismatch for application %s month %s", app, month)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	// 双向检查，识别 SQL 中没有的 Redis 月度计数、预占或已结算记录。
	var cursor uint64
	visited := 0
	for {
		keys, next, scanErr := r.Redis.Scan(c, cursor, "gw:{app:*}:*", 500).Result()
		if scanErr != nil {
			return result, scanErr
		}
		for _, key := range keys {
			visited++
			if visited > 100000 {
				return result, errors.New("audit exceeds 100000 Redis keys")
			}
			fields, readErr := r.Redis.HGetAll(c, key).Result()
			if readErr != nil {
				return result, readErr
			}
			for _, name := range []string{"revision", "tokens", "micro", "rt", "rm"} {
				if value, exists := fields[name]; exists {
					if _, err := strconv.ParseInt(value, 10, 64); err != nil {
						return result, errors.New("Redis quota contains invalid integers")
					}
				}
			}
			index := strings.Index(key, "}:attempt:")
			if index >= 0 {
				attempt := key[index+len("}:attempt:"):]
				app := strings.TrimPrefix(key[:index], "gw:{app:")
				var ledgerApp, kind string
				var revision, tokens, cost, rt, rm int64
				readErr = s.DB.QueryRowContext(c, `SELECT app_id,JSON_UNQUOTE(JSON_EXTRACT(payload,'$.kind')),revision,tokens,cost_micro,JSON_EXTRACT(payload,'$.reserve_tokens'),JSON_EXTRACT(payload,'$.reserve_micro') FROM gw_attempts WHERE attempt_id=?`, attempt).Scan(&ledgerApp, &kind, &revision, &tokens, &cost, &rt, &rm)
				if readErr != nil || ledgerApp != app {
					return result, fmt.Errorf("unmatched Redis attempt %s", attempt)
				}
				value := func(name string) int64 { n, _ := strconv.ParseInt(fields[name], 10, 64); return n }
				if kind == "started" {
					if value("revision") != 0 || fields["reserved"] != "1" || value("rt") != rt || value("rm") != rm {
						return result, fmt.Errorf("pending reservation mismatch %s", attempt)
					}
				} else {
					if value("revision") != revision || value("tokens") != tokens || value("micro") != cost {
						return result, fmt.Errorf("settled attempt mismatch %s", attempt)
					}
				}
				result.Attempts++
			} else if _, exists := expected[key]; !exists {
				for _, name := range []string{"tokens", "micro", "rt", "rm"} {
					if fields[name] != "" && fields[name] != "0" {
						return result, errors.New("Redis contains quota without SQL ledger")
					}
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	var ledgerCount int
	if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_attempts WHERE purpose<>'request'`).Scan(&ledgerCount); err != nil {
		return result, err
	}
	if ledgerCount != result.Attempts {
		return result, errors.New("SQL attempts are missing Redis idempotency records")
	}
	return result, nil
}

func ResumeMetering(c context.Context, r *Runtime) (MeteringAudit, error) {
	result, err := AuditMetering(c, r)
	if err != nil {
		return result, err
	}
	if result.Pending > 0 {
		return result, errors.New("unresolved started attempts require verified usage corrections before resume")
	}
	s := r.Engine.Store.(*SQLStore)
	tx, err := s.DB.BeginTx(c, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(c, `UPDATE gw_metering_control SET state='healthy',reason='',generation=?,updated_at=UTC_TIMESTAMP(6) WHERE id=1`, ID("gen_")); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_audit(id,actor,action,version,at) SELECT ?,'recovery-command','metering.resume',version,UTC_TIMESTAMP(6) FROM gw_config WHERE id=1`, ID("aud_")); err != nil {
		return result, err
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	if err = r.Redis.Del(c, "gw:metering:frozen").Err(); err != nil {
		freeze, finish := context.WithTimeout(context.WithoutCancel(c), time.Second)
		_ = s.FreezeMetering(freeze, "resume_publish_failure")
		finish()
		return result, err
	}
	return result, nil
}

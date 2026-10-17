package gateway

import (
	"context"
	"encoding/json"
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
	if os.Getenv("GW_METERING_RECOVERY_ACK") != "writers-stopped" {
		return MeteringAudit{}, errors.New("stop all metering writers, then set GW_METERING_RECOVERY_ACK=writers-stopped")
	}
	s, ok := r.Engine.Store.(*SQLStore)
	if !ok {
		return MeteringAudit{}, errors.New("SQL required")
	}
	lock, release, err := s.maintenanceLock(c)
	if err != nil {
		return MeteringAudit{}, err
	}
	defer release()
	c = context.WithValue(c, recoveryConnectionKey{}, lock)
	if err = r.ensureGroupStopped(c); err != nil {
		return MeteringAudit{}, err
	}
	return auditMetering(c, r, false)
}
func auditMetering(c context.Context, r *Runtime, initializing bool) (MeteringAudit, error) {
	var result MeteringAudit
	s, ok := r.Engine.Store.(*SQLStore)
	if !ok || r.Redis == nil {
		return result, errors.New("SQL and Redis required")
	}
	if err := s.AuditCapacity(c); err != nil {
		return result, err
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
	if !initializing {
		manifest, err := PlanMeteringRecovery(c, r)
		if err != nil {
			return result, err
		}
		for i, p := range manifest.Checkpoints {
			if p.Next != manifest.Ranges[i].End || manifest.Ranges[i].Committed != p.Next {
				return result, ErrCheckpoint
			}
		}
	}
	expected, pending, err := expectedQuota(c, s)
	result.Pending = pending
	if err != nil {
		return result, err
	}
	result.Groups = len(expected)
	for key, v := range expected {
		app, month, decodeErr := decodeQuotaGroup(key)
		if decodeErr != nil {
			return result, decodeErr
		}
		actual, readErr := r.Engine.Guard.Counters(c, app, month)
		if readErr != nil || actual != v {
			return result, fmt.Errorf("quota mismatch for application %s month %s", app, month)
		}
	}
	// 双向检查，识别 SQL 中没有的 Redis 月度计数、预占或已结算记录。
	var cursor uint64
	visited := 0
	seen := make(map[string]struct{})
	for {
		keys, next, scanErr := r.Redis.Scan(c, cursor, "gw:{app:*}:*", 500).Result()
		if scanErr != nil {
			return result, scanErr
		}
		for _, key := range keys {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
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
				if !initializing {
					var raw []byte
					var event Event
					if err = s.DB.QueryRowContext(c, `SELECT payload FROM gw_attempts WHERE attempt_id=?`, attempt).Scan(&raw); err != nil {
						return result, err
					}
					if err = json.Unmarshal(raw, &event); err != nil {
						return result, err
					}
					if fields["hash"] != eventDigest(event) {
						return result, errors.New("Redis event digest does not match SQL")
					}
				}
				value := func(name string) int64 { n, _ := strconv.ParseInt(fields[name], 10, 64); return n }
				var raw []byte
				if readErr = s.DB.QueryRowContext(c, `SELECT payload FROM gw_attempts WHERE attempt_id=?`, attempt).Scan(&raw); readErr != nil {
					return result, readErr
				}
				event, decodeErr := decodeAttempt(raw)
				if decodeErr != nil {
					return result, decodeErr
				}
				_, applied := billingExposure(event)
				if kind == "started" {
					if value("revision") != 0 || fields["reserved"] != "1" || value("rt") != applied.ReservedTokens || value("rm") != applied.ReservedMicro {
						return result, fmt.Errorf("pending reservation mismatch %s", attempt)
					}
				} else if value("revision") != revision || value("tokens") != applied.Tokens || value("micro") != applied.Micro || value("rt") != applied.ReservedTokens || value("rm") != applied.ReservedMicro {
					return result, fmt.Errorf("settlement or held exposure mismatch %s", attempt)
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
	if os.Getenv("GW_METERING_RECOVERY_ACK") != "writers-stopped" {
		return MeteringAudit{}, errors.New("resume requires stopped writers")
	}
	s := r.Engine.Store.(*SQLStore)
	lock, release, err := s.maintenanceLock(c)
	if err != nil {
		return MeteringAudit{}, err
	}
	defer release()
	c = context.WithValue(c, recoveryConnectionKey{}, lock)
	if err = r.ensureGroupStopped(c); err != nil {
		return MeteringAudit{}, err
	}
	var unfinished int
	if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_recovery WHERE phase<>'complete'`).Scan(&unfinished); err != nil {
		return MeteringAudit{}, err
	}
	if unfinished > 0 {
		return MeteringAudit{}, errors.New("finish recovery apply before resuming admission")
	}
	result, err := auditMetering(c, r, false)
	if err != nil {
		return result, err
	}
	if result.Pending > 0 {
		return result, errors.New("unresolved started attempts require verified usage corrections before resume")
	}
	tx, err := s.beginMeteringTx(c, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	reportID := ID("report_")
	actor, reason := env("GW_METERING_ACTOR", "recovery-command"), env("GW_METERING_REASON", "verified offline recovery")
	raw, _ := json.Marshal(result)
	if _, err = tx.ExecContext(c, `INSERT INTO gw_metering_reports(id,app_id,actor,reason,passed,at,payload) VALUES(?,'*',?,?,1,UTC_TIMESTAMP(6),?)`, reportID, actor, reason, raw); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_metering_apps SET state='healthy',reason='',generation=?,report_id=?,checked_at=UTC_TIMESTAMP(6)`, ID("appgen_"), reportID); err != nil {
		return result, err
	}
	cfg, err := s.Load(c)
	if err != nil {
		return result, err
	}
	for app := range cfg.Apps {
		if _, err = tx.ExecContext(c, `INSERT IGNORE INTO gw_metering_apps(app_id,state,generation,reason,report_id,checked_at) VALUES(?,'healthy',?,'',?,UTC_TIMESTAMP(6))`, app, ID("appgen_"), reportID); err != nil {
			return result, err
		}
	}
	if _, err = tx.ExecContext(c, `UPDATE gw_metering_control SET state='healthy',reason='',generation=?,updated_at=UTC_TIMESTAMP(6) WHERE id=1`, ID("gen_")); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(c, `INSERT INTO gw_audit(id,actor,action,version,at) SELECT ?,?, ?,version,UTC_TIMESTAMP(6) FROM gw_config WHERE id=1`, ID("aud_"), actor, "metering.resume:"+reportID); err != nil {
		return result, err
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	if err = initializeMeteringEvidence(c, s, RedisGuard{Client: r.Redis}); err == nil {
		err = syncApplicationEvidence(c, r)
	}
	if err == nil {
		err = r.Redis.Del(c, "gw:metering:frozen").Err()
	}
	if err != nil {
		freeze, finish := context.WithTimeout(context.WithoutCancel(c), time.Second)
		_ = s.FreezeMetering(freeze, "resume_publish_failure")
		finish()
		return result, err
	}
	return result, nil
}

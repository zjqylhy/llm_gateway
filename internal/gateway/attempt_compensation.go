package gateway

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"time"
)

type cancellationClaim struct {
	Start Event
	Epoch int64
}

func (s *SQLStore) claimCancellations(c context.Context) ([]cancellationClaim, error) {
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// 两个索引范围分别查询，避免 OR 让正常流量的历史 attempt 全表扫描。
	var claims []cancellationClaim
	for _, query := range []string{
		`SELECT start_payload,compensation_epoch FROM gw_attempts WHERE execution_phase='prepared' AND prepare_deadline<=UTC_TIMESTAMP(6) ORDER BY prepare_deadline,attempt_id LIMIT 32 FOR UPDATE SKIP LOCKED`,
		`SELECT start_payload,compensation_epoch FROM gw_attempts WHERE compensation_state='pending' AND next_retry_at<=UTC_TIMESTAMP(6) AND (compensation_lease_until IS NULL OR compensation_lease_until<=UTC_TIMESTAMP(6)) ORDER BY next_retry_at,attempt_id LIMIT 32 FOR UPDATE SKIP LOCKED`,
	} {
		rows, readErr := tx.QueryContext(c, query)
		if readErr != nil {
			return nil, readErr
		}
		for rows.Next() {
			var raw []byte
			var claim cancellationClaim
			if readErr = rows.Scan(&raw, &claim.Epoch); readErr != nil {
				break
			}
			claim.Start, readErr = decodeAttempt(raw)
			if readErr != nil {
				break
			}
			claim.Epoch++
			claims = append(claims, claim)
		}
		if readErr == nil {
			readErr = rows.Err()
		}
		rows.Close()
		if readErr != nil {
			return nil, readErr
		}
	}
	for _, claim := range claims {
		// 领取同时阻断 Arm；迟到的原请求不能靠进程恢复重新获得调用资格。
		_, err = tx.ExecContext(c, `UPDATE gw_attempts SET execution_phase='cancelled',compensation_state='pending',prepare_deadline=NULL,compensation_epoch=?,compensation_lease_until=TIMESTAMPADD(SECOND,60,UTC_TIMESTAMP(6)),next_retry_at=TIMESTAMPADD(SECOND,60,UTC_TIMESTAMP(6)) WHERE attempt_id=? AND execution_phase IN ('prepared','cancelled')`, claim.Epoch, claim.Start.AttemptID)
		if err != nil {
			return nil, err
		}
	}
	return claims, tx.Commit()
}

func (r *Runtime) CompensateOnce(c context.Context) (result error) {
	s := r.Engine.Store.(*SQLStore)
	claims, err := s.claimCancellations(c)
	if err != nil {
		return err
	}
	remaining := claims
	defer func() {
		if len(remaining) == 0 {
			return
		}
		// 活进程提前结束一轮时，未开始处理的领取不必等待崩溃租约。
		// epoch 条件避免迟到释放覆盖后来 worker 的领取。
		cleanup, done := context.WithTimeout(context.WithoutCancel(c), time.Second)
		defer done()
		pairs := make([]string, len(remaining))
		args := make([]any, 0, 2*len(remaining))
		for i, claim := range remaining {
			pairs[i] = "(?,?)"
			args = append(args, claim.Start.AttemptID, claim.Epoch)
		}
		_, releaseErr := s.DB.ExecContext(cleanup, `UPDATE gw_attempts SET compensation_lease_until=NULL,next_retry_at=TIMESTAMPADD(SECOND,5,UTC_TIMESTAMP(6)) WHERE compensation_state='pending' AND (attempt_id,compensation_epoch) IN (`+strings.Join(pairs, ",")+`)`, args...)
		result = errors.Join(result, releaseErr)
	}()
	for i, claim := range claims {
		if c.Err() != nil {
			return c.Err()
		}
		remaining = claims[i+1:]
		operation, done := context.WithTimeout(c, 3*time.Second)
		started := time.Now()
		err = compensateAttempt(operation, s, r.Engine.Guard, claim.Start)
		s.Metrics.meteringStage("compensation", "started", started, err)
		done()
		if err != nil {
			// SQL 成功、Redis 或释放失败均保留 pending；旧领取的失败不得覆盖新进度。
			_, retryErr := s.DB.ExecContext(c, `UPDATE gw_attempts SET compensation_lease_until=NULL,next_retry_at=TIMESTAMPADD(SECOND,LEAST(300,5*POW(2,LEAST(compensation_epoch,6))),UTC_TIMESTAMP(6)) WHERE attempt_id=? AND compensation_epoch=? AND compensation_state='pending'`, claim.Start.AttemptID, claim.Epoch)
			if retryErr != nil {
				return retryErr
			}
			return err
		}
	}
	return nil
}

func (r *Runtime) RunAttemptMaintenance(c context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	round := 0
	for {
		ctx, done := context.WithTimeout(c, 4*time.Second)
		if err := r.CompensateOnce(ctx); err != nil && c.Err() == nil {
			slog.Warn("attempt compensation deferred", "error", err)
		}
		done()
		if round%12 == 0 {
			ctx, done = context.WithTimeout(c, time.Second)
			removed, err := r.Engine.Store.(*SQLStore).GCOutbox(ctx, 24*time.Hour)
			if err != nil && c.Err() == nil {
				slog.Warn("outbox cleanup deferred", "error", err)
				r.Engine.Metrics.MeteringGCFailures.Inc()
			} else if err == nil {
				r.Engine.Metrics.MeteringGC.Add(float64(removed))
			}
			done()
			ctx, done = context.WithTimeout(c, time.Second)
			if err := r.Engine.Store.(*SQLStore).refreshLifecycleMetrics(ctx); err != nil && c.Err() == nil {
				slog.Warn("lifecycle metric snapshot deferred", "error", err)
			}
			done()
		}
		round++
		select {
		case <-c.Done():
			return
		case <-tick.C:
		}
	}
}

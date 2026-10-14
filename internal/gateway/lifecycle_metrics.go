package gateway

import (
	"context"
	"database/sql"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) registerSQLPool(db *sql.DB) {
	m.Registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "gateway_sql_pool_open", Help: "Open SQL pool connections"}, func() float64 { return float64(db.Stats().OpenConnections) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "gateway_sql_pool_in_use", Help: "SQL connections currently in use"}, func() float64 { return float64(db.Stats().InUse) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "gateway_sql_pool_wait_seconds_total", Help: "Cumulative wait for a SQL pool connection"}, func() float64 { return db.Stats().WaitDuration.Seconds() }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "gateway_sql_pool_wait_total", Help: "Number of SQL connection pool waits"}, func() float64 { return float64(db.Stats().WaitCount) }),
	)
}

func (s *SQLStore) refreshLifecycleMetrics(c context.Context) error {
	metrics := s.Metrics
	if metrics == nil {
		return nil
	}
	values := map[string]float64{}
	// 每个状态查询自己的索引范围，不扫描已结束的全部历史来统计活动状态。
	for _, state := range []string{"prepared", "inflight"} {
		var count int64
		if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_attempts WHERE execution_phase=?`, state).Scan(&count); err != nil {
			return err
		}
		values[state] = float64(count)
	}
	var pending int64
	var age float64
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*),COALESCE(TIMESTAMPDIFF(MICROSECOND,MIN(at),UTC_TIMESTAMP(6))/1000000,0) FROM gw_attempts WHERE compensation_state='pending'`).Scan(&pending, &age); err != nil {
		return err
	}
	var held int64
	var exposure float64
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*),COALESCE(SUM(GREATEST(cost_micro,CAST(JSON_EXTRACT(payload,'$.reserve_micro') AS SIGNED))),0) FROM gw_attempts WHERE billing_state='held'`).Scan(&held, &exposure); err != nil {
		return err
	}
	for state, count := range values {
		metrics.MeteringStates.WithLabelValues(state).Set(count)
	}
	metrics.MeteringStates.WithLabelValues("compensation_pending").Set(float64(pending))
	metrics.MeteringStates.WithLabelValues("held").Set(float64(held))
	metrics.MeteringCompensationAge.Set(max(0, age))
	metrics.MeteringHeldMicro.Set(exposure)
	metrics.MeteringSnapshot.Set(float64(time.Now().Unix()))
	return nil
}

package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ApplicationAudit struct {
	ID       string  `json:"id"`
	App      string  `json:"app_id"`
	Passed   bool    `json:"passed"`
	Attempts int     `json:"attempts"`
	Months   int     `json:"months"`
	Pending  int     `json:"pending"`
	Error    string  `json:"error,omitempty"`
	Seconds  float64 `json:"seconds"`
}

// 只暂停目标应用；已经产生的费用继续入账。等待超时或差账保持冻结，禁止自动放行。
func (r *Runtime) AuditApplication(parent context.Context, app, actor, reason string) (report ApplicationAudit, result error) {
	c, cancel := context.WithTimeout(parent, 150*time.Second)
	defer cancel()
	s := r.Engine.Store.(*SQLStore)
	report = ApplicationAudit{ID: ID("report_"), App: app}
	start := time.Now()
	res, err := s.DB.ExecContext(c, `UPDATE gw_metering_apps SET state='quiescing',reason='audit_barrier' WHERE app_id=? AND state='healthy'`, app)
	if err != nil {
		return report, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return report, ErrUnavailable
	}
	defer func() {
		report.Passed = result == nil
		report.Seconds = time.Since(start).Seconds()
		if result != nil {
			report.Error = result.Error()
		}
		finish, done := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
		defer done()
		raw, _ := json.Marshal(report)
		state, why := "healthy", ""
		if result != nil {
			state, why = "frozen", "audit_failed"
		}
		tx, e := s.DB.BeginTx(finish, nil)
		if e == nil {
			defer tx.Rollback()
			_, e = tx.ExecContext(finish, `INSERT INTO gw_metering_reports(id,app_id,actor,reason,passed,at,payload) VALUES(?,?,?,?,?,UTC_TIMESTAMP(6),?)`, report.ID, app, actor, reason, report.Passed, raw)
			if e == nil {
				_, e = tx.ExecContext(finish, `UPDATE gw_metering_apps SET state=?,reason=?,report_id=?,checked_at=UTC_TIMESTAMP(6) WHERE app_id=? AND state='quiescing'`, state, why, report.ID, app)
			}
			if e == nil {
				e = tx.Commit()
			}
		}
		if e == nil {
			e = r.Redis.HSet(finish, appControlKey(app), "state", state).Err()
		}
		if e != nil {
			_, _ = s.DB.ExecContext(finish, `UPDATE gw_metering_apps SET state='frozen',reason='audit_publish_failure' WHERE app_id=?`, app)
			if result == nil {
				result = e
				report.Passed = false
			}
		}
		r.Engine.Metrics.MeteringAuditSeconds.Observe(report.Seconds)
		if result == nil {
			r.Engine.Metrics.MeteringAuditLast.Set(float64(time.Now().Unix()))
		} else {
			r.Engine.Metrics.MeteringAuditFailures.Inc()
		}
	}()
	if err = r.Redis.HSet(c, appControlKey(app), "state", "quiescing").Err(); err != nil {
		return report, err
	}
	if err = waitMetering(c, func() (bool, error) {
		var active int
		err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_admissions WHERE app_id=?`, app).Scan(&active)
		return active == 0, err
	}); err != nil {
		return report, err
	}
	_, ranges, err := r.KafkaRanges(c)
	if err != nil {
		return report, err
	}
	if err = waitMetering(c, func() (bool, error) {
		var pending int
		if e := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_outbox WHERE app_id=? AND delivered=0 AND JSON_UNQUOTE(JSON_EXTRACT(CONVERT(payload USING utf8mb4),'$.purpose'))<>'request'`, app).Scan(&pending); e != nil {
			return false, e
		}
		_, now, e := r.KafkaRanges(c)
		if e != nil {
			return false, e
		}
		for i, p := range ranges {
			if now[i].Committed < p.End {
				return false, nil
			}
		}
		return pending == 0, nil
	}); err != nil {
		return report, err
	}
	return r.compareApplication(c, report)
}

func waitMetering(c context.Context, probe func() (bool, error)) error {
	for {
		ok, err := probe()
		if err != nil || ok {
			return err
		}
		select {
		case <-c.Done():
			return c.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// 键集分页读取，单应用最多 100000 attempt/10000 月份；超过边界显式冻结，不截断核验。
func (r *Runtime) compareApplication(c context.Context, report ApplicationAudit) (ApplicationAudit, error) {
	s := r.Engine.Store.(*SQLStore)
	expected := map[string]Counters{}
	var generation string
	if err := s.DB.QueryRowContext(c, `SELECT generation FROM gw_metering_apps WHERE app_id=?`, report.App).Scan(&generation); err != nil {
		return report, err
	}
	meta, err := r.Redis.HMGet(c, appControlKey(report.App), "state", "generation").Result()
	if err != nil || meta[0] != "quiescing" || meta[1] != generation {
		return report, errors.New("application control generation mismatch")
	}
	keys := map[string]bool{}
	last := ""
	for {
		rows, err := s.DB.QueryContext(c, `SELECT attempt_id,payload FROM gw_attempts WHERE app_id=? AND purpose<>'request' AND attempt_id>? ORDER BY attempt_id LIMIT 500`, report.App, last)
		if err != nil {
			return report, err
		}
		batch := []Event{}
		for rows.Next() {
			var raw []byte
			var e Event
			if err = rows.Scan(&last, &raw); err != nil {
				break
			}
			if err = json.Unmarshal(raw, &e); err != nil {
				break
			}
			batch = append(batch, e)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return report, err
		}
		if len(batch) == 0 {
			break
		}
		for _, e := range batch {
			report.Attempts++
			if report.Attempts > 100000 {
				return report, errors.New("application audit exceeds 100000 attempts")
			}
			q := quotaKeys(e.AppID, e.Month, e.AttemptID)
			fields, err := r.Redis.HGetAll(c, q[1]).Result()
			if err != nil {
				return report, err
			}
			value := func(name string) int64 { n, _ := strconv.ParseInt(fields[name], 10, 64); return n }
			for _, name := range []string{"revision", "tokens", "micro", "rt", "rm"} {
				if v, exists := fields[name]; exists {
					if _, err := strconv.ParseInt(v, 10, 64); err != nil {
						return report, errors.New("invalid quota integer")
					}
				}
			}
			if fields["hash"] != eventDigest(e) {
				return report, errors.New("attempt hash mismatch")
			}
			v := expected[e.Month]
			if e.Kind == "started" {
				report.Pending++
				if fields["reserved"] != "1" || value("revision") != 0 || value("rt") != e.ReserveTokens || value("rm") != e.ReserveMicro {
					return report, errors.New("pending reservation mismatch")
				}
				v.ReservedTokens += e.ReserveTokens
				v.ReservedMicro += e.ReserveMicro
			} else {
				if value("revision") != e.Revision || value("tokens") != e.Usage.Total || value("micro") != e.CostMicro {
					return report, errors.New("settled usage mismatch")
				}
				v.Tokens += e.Usage.Total
				v.Micro += e.CostMicro
			}
			expected[e.Month] = v
			keys[q[1]] = true
		}
	}
	if len(expected) > 10000 {
		return report, errors.New("application audit exceeds 10000 months")
	}
	for month, v := range expected {
		actual, err := r.Engine.Guard.Counters(c, report.App, month)
		if err != nil || actual != v {
			return report, errors.New("application monthly quota mismatch")
		}
		keys[quotaKeys(report.App, month, "")[0]] = true
	}
	// SCAN 可以重复返回，单独去重。非预期 attempt 即使金额为零也须保留调查证据。
	var cursor uint64
	seen := map[string]bool{}
	for {
		batch, next, err := r.Redis.Scan(c, cursor, "gw:{app:"+report.App+"}:*", 500).Result()
		if err != nil {
			return report, err
		}
		for _, key := range batch {
			seen[key] = true
			if len(seen) > 110000 {
				return report, errors.New("application Redis audit exceeds 110000 keys")
			}
			if keys[key] {
				continue
			}
			if strings.Contains(key, "}:attempt:") {
				return report, errors.New("orphan Redis reservation or settlement")
			}
			fields, err := r.Redis.HGetAll(c, key).Result()
			if err != nil {
				return report, err
			}
			for _, n := range []string{"tokens", "micro", "rt", "rm"} {
				if fields[n] != "" && fields[n] != "0" {
					return report, errors.New("orphan monthly quota")
				}
			}
		}
		cursor = next
		if next == 0 {
			break
		}
	}
	report.Months = len(expected)
	if report.Pending != 0 {
		return report, errors.New("unresolved started attempts require verified usage")
	}
	return report, nil
}

func (a *Admin) meteringStatus(w http.ResponseWriter, r *http.Request) {
	u, _, _, ok := a.identity(r)
	if !ok || u.Role != "admin" {
		APIError(w, 403, "forbidden", "administrator role required")
		return
	}
	s, ok := a.Engine.Store.(*SQLStore)
	if !ok {
		JSON(w, map[string]string{"state": "offline"})
		return
	}
	c, done := context.WithTimeout(r.Context(), 3*time.Second)
	defer done()
	var state, gen, reason string
	if err := s.DB.QueryRowContext(c, `SELECT state,generation,reason FROM gw_metering_control WHERE id=1`).Scan(&state, &gen, &reason); err != nil {
		APIError(w, 503, "not_ready", "metering status unavailable")
		return
	}
	controls, err := s.DB.QueryContext(c, `SELECT app_id,state,reason,report_id,checked_at FROM gw_metering_apps WHERE app_id>? ORDER BY app_id LIMIT 101`, r.URL.Query().Get("after"))
	if err != nil {
		APIError(w, 503, "not_ready", "application metering status unavailable")
		return
	}
	apps := []map[string]any{}
	next := ""
	for controls.Next() {
		var app, st, why, report string
		var checked sql.NullTime
		if err = controls.Scan(&app, &st, &why, &report, &checked); err != nil {
			break
		}
		if len(apps) == 100 {
			next = apps[99]["app_id"].(string)
			break
		}
		apps = append(apps, map[string]any{"app_id": app, "state": st, "reason": why, "report_id": report, "checked_at": checked})
	}
	if err == nil {
		err = controls.Err()
	}
	controls.Close()
	if err != nil {
		APIError(w, 503, "not_ready", "application metering status unavailable")
		return
	}
	rows, err := s.DB.QueryContext(c, `SELECT id,app_id,actor,reason,passed,at,payload FROM gw_metering_reports ORDER BY at DESC LIMIT 100`)
	if err != nil {
		APIError(w, 503, "not_ready", "metering reports unavailable")
		return
	}
	defer rows.Close()
	reports := []map[string]any{}
	for rows.Next() {
		var raw []byte
		var id, app, actor, why string
		var passed bool
		var at time.Time
		if rows.Scan(&id, &app, &actor, &why, &passed, &at, &raw) != nil {
			APIError(w, 503, "not_ready", "metering reports unavailable")
			return
		}
		entry := map[string]any{}
		if json.Unmarshal(raw, &entry) != nil {
			APIError(w, 503, "not_ready", "invalid metering report")
			return
		}
		entry["id"], entry["app_id"], entry["actor"], entry["reason"], entry["passed"], entry["at"] = id, app, actor, why, passed, at
		reports = append(reports, entry)
	}
	if rows.Err() != nil {
		APIError(w, 503, "not_ready", "metering reports unavailable")
		return
	}
	var undelivered int64
	if err := s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_outbox WHERE delivered=0`).Scan(&undelivered); err != nil {
		APIError(w, 503, "not_ready", "metering backlog unavailable")
		return
	}
	permitValid := false
	if a.Engine.Metering != nil {
		_, _, err := a.Engine.Metering.readPermit(c)
		permitValid = err == nil
	}
	JSON(w, map[string]any{"state": state, "generation": gen, "reason": reason, "reports": reports, "applications": apps, "next": next, "undelivered_events": undelivered, "permit_valid": permitValid})
}

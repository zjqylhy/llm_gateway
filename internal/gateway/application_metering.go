package gateway

import (
	"context"
	"database/sql"
	"net/http"
	"sort"
	"strings"
	"time"
)

type appMeteringControl struct {
	State        string
	CheckedAt    *time.Time
	ReportPassed *bool
	ReportAt     *time.Time
}

type appMeteringReader interface {
	applicationMeteringControls(context.Context, []string) (string, map[string]appMeteringControl, error)
}

func (s *SQLStore) applicationMeteringControls(c context.Context, ids []string) (string, map[string]appMeteringControl, error) {
	var global string
	if err := s.DB.QueryRowContext(c, `SELECT state FROM gw_metering_control WHERE id=1`).Scan(&global); err != nil {
		return "", nil, err
	}
	out := map[string]appMeteringControl{}
	if len(ids) == 0 {
		return global, out, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	// 每个应用只返回其当前控制记录关联的核验结果，避免全局最近报告挤掉低流量应用。
	rows, err := s.DB.QueryContext(c, `SELECT a.app_id,a.state,a.checked_at,r.passed,r.at FROM gw_metering_apps a LEFT JOIN gw_metering_reports r ON r.id=a.report_id WHERE a.app_id IN (`+strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")+`)`, args...)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var v appMeteringControl
		var checked, at sql.NullTime
		var passed sql.NullBool
		if err := rows.Scan(&id, &v.State, &checked, &passed, &at); err != nil {
			return "", nil, err
		}
		if checked.Valid {
			v.CheckedAt = &checked.Time
		}
		if at.Valid {
			v.ReportAt = &at.Time
		}
		if passed.Valid {
			v.ReportPassed = &passed.Bool
		}
		out[id] = v
	}
	return global, out, rows.Err()
}

type appMeteringView struct {
	AppID        string     `json:"app_id"`
	Name         string     `json:"name"`
	State        string     `json:"state"`
	Impact       string     `json:"impact"`
	CheckedAt    *time.Time `json:"checked_at,omitempty"`
	ReportPassed *bool      `json:"report_passed,omitempty"`
	ReportAt     *time.Time `json:"report_at,omitempty"`
}

func businessMeteringState(global, app string) (string, string) {
	if global == "offline" {
		return "offline", "离线开发模式，未执行生产计量核验"
	}
	if global == "frozen" {
		return "frozen", "平台计量异常，新增付费调用暂停"
	}
	if global == "unavailable" {
		return "unavailable", "计量许可不可用，新增付费调用暂停"
	}
	if global != "healthy" {
		return "unknown", "平台计量状态未确认"
	}
	switch app {
	case "healthy":
		return "healthy", "计量正常"
	case "quiescing":
		return "quiescing", "核验中，暂缓新调用"
	case "frozen":
		return "frozen", "应用计量异常，新增付费调用暂停"
	case "initializing":
		return "initializing", "应用计量初始化中"
	default:
		return "unknown", "应用计量状态未确认"
	}
}

func (a *Admin) applicationMetering(w http.ResponseWriter, r *http.Request) {
	u, cfg, _, _ := a.identity(r)
	selected, after := r.URL.Query().Get("app_id"), r.URL.Query().Get("after")
	if selected != "" {
		app, ok := cfg.Apps[selected]
		if !ok || u.Role == "developer" && app.Owner != u.Name {
			APIError(w, 403, "forbidden", "application access denied")
			return
		}
	}
	ids := []string{}
	for id, app := range cfg.Apps {
		if (selected == "" || id == selected) && id > after && (u.Role != "developer" || app.Owner == u.Name) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	next := ""
	if len(ids) > 100 {
		ids = ids[:100]
		next = ids[99]
	}
	global, controls := "offline", map[string]appMeteringControl{}
	if source, ok := a.Engine.Store.(appMeteringReader); ok {
		c, done := context.WithTimeout(r.Context(), 3*time.Second)
		defer done()
		var err error
		global, controls, err = source.applicationMeteringControls(c, ids)
		if err != nil {
			APIError(w, 503, "metering_unavailable", "application metering status unavailable")
			return
		}
		// SQL 控制正常并不保证短期许可仍有效；业务摘要必须反映 worker 停止的影响。
		if global == "healthy" && a.Engine.Metering != nil {
			if _, err := a.Engine.Metering.readPermit(c); err != nil {
				global = "unavailable"
			}
		}
	}
	out := []appMeteringView{}
	for _, id := range ids {
		v := controls[id]
		state, impact := businessMeteringState(global, v.State)
		out = append(out, appMeteringView{id, cfg.Apps[id].Name, state, impact, v.CheckedAt, v.ReportPassed, v.ReportAt})
	}
	JSON(w, map[string]any{"applications": out, "next": next})
}

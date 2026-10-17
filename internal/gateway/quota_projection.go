package gateway

import (
	"context"
	"errors"
	"strings"
)

// 核验和重建共享与 Lua 相同的财务投影，估计报表不能成为已确认扣费。
func expectedQuota(c context.Context, s *SQLStore) (map[string]Counters, int, error) {
	return expectedQuotaFor(c, s, "")
}
func expectedQuotaFor(c context.Context, s *SQLStore, app string) (map[string]Counters, int, error) {
	groups := map[string]Counters{}
	pending, count := 0, 0
	cursor := ""
	for {
		query := `SELECT attempt_id,payload,execution_phase,compensation_state FROM gw_attempts WHERE purpose<>'request' AND attempt_id>?`
		args := []any{cursor}
		if app != "" {
			query += ` AND app_id=?`
			args = append(args, app)
		}
		query += ` ORDER BY attempt_id LIMIT 500`
		rows, err := s.DB.QueryContext(c, query, args...)
		if err != nil {
			return nil, 0, err
		}
		read := 0
		for rows.Next() {
			var raw []byte
			var phase, compensation string
			if err = rows.Scan(&cursor, &raw, &phase, &compensation); err != nil {
				break
			}
			if phase == "prepared" || compensation == "pending" {
				err = errors.New("unresolved preparation or compensation requires recovery")
				break
			}
			e, decodeErr := decodeAttempt(raw)
			if decodeErr != nil {
				err = decodeErr
				break
			}
			_, v := billingExposure(e)
			key := quotaKeys(e.AppID, e.Month, "")[0]
			old := groups[key]
			old.Tokens += v.Tokens
			old.Micro += v.Micro
			old.ReservedTokens += v.ReservedTokens
			old.ReservedMicro += v.ReservedMicro
			groups[key] = old
			if e.Kind == "started" {
				pending++
			}
			count++
			read++
			if count > 100000 || len(groups) > 10000 {
				err = errors.New("quota audit exceeds bounded ledger size")
				break
			}
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return nil, 0, err
		}
		if read == 0 {
			break
		}
	}
	return groups, pending, nil
}

func decodeQuotaGroup(key string) (string, string, error) {
	app, month, ok := strings.Cut(strings.TrimPrefix(key, "gw:{app:"), "}:")
	if !ok || app == "" || month == "" {
		return "", "", errors.New("invalid quota group")
	}
	return app, month, nil
}

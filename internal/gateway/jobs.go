package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"time"
)

type Alert struct {
	ID      string    `json:"id"`
	AppID   string    `json:"app_id"`
	Kind    string    `json:"kind"`
	Active  bool      `json:"active"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}
type AlertStore interface {
	SaveAlert(context.Context, Alert) error
	Alerts(context.Context, string) ([]Alert, error)
}

func (s *SQLStore) SaveAlert(c context.Context, a Alert) error {
	b, e := json.Marshal(a)
	if e != nil {
		return e
	}
	_, e = s.DB.ExecContext(c, `INSERT INTO gw_alerts(id,app_id,active,at,payload) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE active=?,at=?,payload=?`, a.ID, a.AppID, a.Active, a.At, b, a.Active, a.At, b)
	return e
}
func (s *SQLStore) Alerts(c context.Context, app string) ([]Alert, error) {
	q := `SELECT payload FROM gw_alerts`
	args := []any{}
	if app != "" {
		q += ` WHERE app_id=?`
		args = append(args, app)
	}
	q += ` ORDER BY at DESC LIMIT 100`
	rows, e := s.DB.QueryContext(c, q, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Alert{}
	for rows.Next() {
		var b []byte
		var a Alert
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &a); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (s *MemoryStore) SaveAlert(_ context.Context, a Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.alerts == nil {
		s.alerts = map[string]Alert{}
	}
	s.alerts[a.ID] = a
	return nil
}
func (s *MemoryStore) Alerts(_ context.Context, app string) ([]Alert, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Alert{}
	for _, a := range s.alerts {
		if app == "" || a.AppID == app {
			out = append(out, a)
		}
	}
	return out, nil
}

type GraySample struct {
	RunID   string
	Alias   string
	ModelID string
	Count   int
	Errors  int
	MaxMS   int64
}
type GraySampler interface {
	GraySamples(context.Context, time.Time) ([]GraySample, error)
}

func (s *SQLStore) GraySamples(c context.Context, since time.Time) ([]GraySample, error) {
	rows, e := s.DB.QueryContext(c, `SELECT JSON_UNQUOTE(JSON_EXTRACT(payload,'$.alias')),model_id,COALESCE(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.gray_run')),''),COUNT(*),SUM(status NOT IN ('success','reconciled')),MAX(CAST(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.duration_ms')) AS UNSIGNED)) FROM gw_attempts WHERE at>=? AND purpose='inference' AND status<>'started' AND JSON_EXTRACT(payload,'$.gray')=true GROUP BY JSON_UNQUOTE(JSON_EXTRACT(payload,'$.alias')),model_id,COALESCE(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.gray_run')),'')`, since)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []GraySample{}
	for rows.Next() {
		var x GraySample
		if e = rows.Scan(&x.Alias, &x.ModelID, &x.RunID, &x.Count, &x.Errors, &x.MaxMS); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *MemoryStore) GraySamples(c context.Context, since time.Time) ([]GraySample, error) {
	rows, e := s.Records(c, Filter{From: since})
	if e != nil {
		return nil, e
	}
	out := map[string]GraySample{}
	for _, x := range rows {
		if !x.Gray || x.Purpose != "inference" || x.Kind != "completed" {
			continue
		}
		key := x.Alias + ":" + x.Model.ID + ":" + x.GrayRun
		v := out[key]
		v.Alias = x.Alias
		v.ModelID = x.Model.ID
		v.RunID = x.GrayRun
		v.Count++
		if x.Status != "success" && x.Status != "reconciled" {
			v.Errors++
		}
		if x.DurationMS > v.MaxMS {
			v.MaxMS = x.DurationMS
		}
		out[key] = v
	}
	res := []GraySample{}
	for _, v := range out {
		res = append(res, v)
	}
	return res, nil
}
func RunMaintenance(c context.Context, e *Engine) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	last := map[string]int64{}
	average := map[string]float64{}
	nextCacheCleanup := time.Now().Add(5 * time.Minute)
	for {
		select {
		case <-c.Done():
			return
		case <-tick.C:
			if cache, ok := e.Cache.(MilvusCache); ok && !time.Now().Before(nextCacheCleanup) {
				cleanup, done := context.WithTimeout(c, 3*time.Second)
				if err := cache.Prune(cleanup); err != nil {
					slog.Warn("expired cache vector cleanup unavailable")
				}
				done()
				nextCacheCleanup = time.Now().Add(5 * time.Minute)
			}
			ctx, cancel := context.WithTimeout(c, 5*time.Second)
			cfg, err := e.Store.Load(ctx)
			if err == nil {
				for _, app := range cfg.Apps {
					counts, ge := e.Guard.Counters(ctx, app.ID, month(time.Now()))
					if ge != nil {
						continue
					}
					if alerts, ok := e.Store.(AlertStore); ok {
						if app.BudgetMicro > 0 {
							active := counts.Micro >= app.BudgetMicro*8/10
							_ = alerts.SaveAlert(ctx, Alert{ID: app.ID + ":" + month(time.Now()) + ":budget", AppID: app.ID, Kind: "budget", Active: active, Message: "预算达到 80% 阈值", At: time.Now().UTC()})
						}
						delta := counts.Micro - last[app.ID]
						if avg := average[app.ID]; avg > 0 && float64(delta) > avg*5 && delta > 1000 {
							_ = alerts.SaveAlert(ctx, Alert{ID: app.ID + ":anomaly", AppID: app.ID, Kind: "cost_spike", Active: true, Message: "10 秒成本增量超过近期均值 5 倍", At: time.Now().UTC()})
						}
						if delta > 0 {
							average[app.ID] = average[app.ID]*.8 + float64(delta)*.2
						}
						last[app.ID] = counts.Micro
					}
				}
				_ = RollbackGray(ctx, e, cfg)
			}
			cancel()
			if err != nil {
				slog.Warn("maintenance unavailable")
			}
		}
	}
}
func RollbackGray(c context.Context, e *Engine, cfg Config) error {
	sampler, ok := e.Store.(GraySampler)
	if !ok {
		return nil
	}
	samples, err := sampler.GraySamples(c, time.Now().Add(-5*time.Minute))
	if err != nil {
		return err
	}
	for _, x := range samples {
		route, ok := cfg.Routes[x.Alias]
		if !ok || route.Gray.State != "running" || route.Gray.Candidate != x.ModelID || route.Gray.RunID != x.RunID || x.Count < route.Gray.MinSamples {
			continue
		}
		if float64(x.Errors)/float64(x.Count) <= route.Gray.MaxError && x.MaxMS <= route.Gray.MaxLatencyMS {
			continue
		}
		changed, er := e.Store.Update(c, "system", "gray.auto_rollback", func(current *Config) error {
			r := current.Routes[x.Alias]
			if r.Gray.State != "running" || r.Gray.Candidate != x.ModelID || r.Gray.RunID != x.RunID {
				return errors.New("gray configuration changed")
			}
			r.Gray.State = "rolled_back"
			r.Gray.Percent = 0
			current.Routes[x.Alias] = r
			return nil
		})
		if er != nil {
			return er
		}
		if er = e.Publisher.PublishConfig(c, changed); er != nil {
			return er
		}
	}
	return nil
}

type SessionStore interface {
	Get(context.Context, string) (session, error)
	Put(context.Context, string, session) error
	Delete(context.Context, string) error
}
type RedisSessions struct{ Client *redis.Client }

func (s RedisSessions) Get(c context.Context, id string) (session, error) {
	b, e := s.Client.Get(c, "gw:session:"+id).Bytes()
	if e != nil {
		return session{}, e
	}
	var out session
	e = json.Unmarshal(b, &out)
	return out, e
}
func (s RedisSessions) Put(c context.Context, id string, x session) error {
	b, e := json.Marshal(x)
	if e != nil {
		return e
	}
	return s.Client.Set(c, "gw:session:"+id, b, time.Until(x.Expires)).Err()
}
func (s RedisSessions) Delete(c context.Context, id string) error {
	return s.Client.Del(c, "gw:session:"+id).Err()
}

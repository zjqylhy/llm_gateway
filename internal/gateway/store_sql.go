package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"time"
)

type SQLStore struct{ DB *sql.DB }

func NewSQLStore(c context.Context, dsn string) (*SQLStore, error) {
	db, e := sql.Open("mysql", dsn)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(30)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	if e = db.PingContext(c); e != nil {
		db.Close()
		return nil, e
	}
	s := &SQLStore{db}
	return s, nil
}

// 迁移由显式 migrate 命令执行，运行服务不持有 DDL 权限。
func (s *SQLStore) Migrate(c context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS gw_config (id INT PRIMARY KEY, version BIGINT NOT NULL, payload JSON NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_audit (id VARCHAR(64) PRIMARY KEY, actor VARCHAR(128) NOT NULL, action VARCHAR(128) NOT NULL, version BIGINT NOT NULL, at DATETIME(6) NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_events (id VARCHAR(128) PRIMARY KEY, attempt_id VARCHAR(64) NOT NULL, revision BIGINT NOT NULL, payload JSON NOT NULL, at DATETIME(6) NOT NULL, INDEX(attempt_id))`,
		`CREATE TABLE IF NOT EXISTS gw_attempts (attempt_id VARCHAR(64) PRIMARY KEY, request_id VARCHAR(64) NOT NULL, app_id VARCHAR(64) NOT NULL, department VARCHAR(128) NOT NULL, model_id VARCHAR(64) NOT NULL, status VARCHAR(32) NOT NULL, purpose VARCHAR(32) NOT NULL, revision BIGINT NOT NULL, tokens BIGINT NOT NULL, cost_micro BIGINT NOT NULL, at DATETIME(6) NOT NULL, payload JSON NOT NULL, INDEX(app_id,at), INDEX(request_id), INDEX(model_id,at), INDEX(status,at))`,
		`CREATE TABLE IF NOT EXISTS gw_instances (id VARCHAR(128) PRIMARY KEY, version BIGINT NOT NULL, seen DATETIME(6) NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_alerts (id VARCHAR(128) PRIMARY KEY, app_id VARCHAR(64) NOT NULL, active BOOLEAN NOT NULL, at DATETIME(6) NOT NULL, payload JSON NOT NULL, INDEX(app_id,at))`,
	}
	for _, q := range statements {
		if _, e := s.DB.ExecContext(c, q); e != nil {
			return e
		}
	}
	b, _ := json.Marshal(EmptyConfig())
	_, e := s.DB.ExecContext(c, `INSERT IGNORE INTO gw_config(id,version,payload) VALUES(1,0,?)`, b)
	return e
}
func (s *SQLStore) Load(c context.Context) (Config, error) {
	var b []byte
	e := s.DB.QueryRowContext(c, `SELECT payload FROM gw_config WHERE id=1`).Scan(&b)
	if e != nil {
		return Config{}, e
	}
	var out Config
	e = json.Unmarshal(b, &out)
	return out, e
}
func (s *SQLStore) Update(c context.Context, actor, action string, fn func(*Config) error) (Config, error) {
	tx, e := s.DB.BeginTx(c, nil)
	if e != nil {
		return Config{}, e
	}
	defer tx.Rollback()
	var b []byte
	if e = tx.QueryRowContext(c, `SELECT payload FROM gw_config WHERE id=1 FOR UPDATE`).Scan(&b); e != nil {
		return Config{}, e
	}
	var cfg Config
	if e = json.Unmarshal(b, &cfg); e != nil {
		return cfg, e
	}
	if e = fn(&cfg); e != nil {
		return cfg, e
	}
	cfg.Version++
	b, e = json.Marshal(cfg)
	if e != nil {
		return cfg, e
	}
	if _, e = tx.ExecContext(c, `UPDATE gw_config SET payload=?,version=? WHERE id=1`, b, cfg.Version); e != nil {
		return cfg, e
	}
	if _, e = tx.ExecContext(c, `INSERT INTO gw_audit(id,actor,action,version,at) VALUES(?,?,?,?,?)`, ID("aud_"), actor, action, cfg.Version, time.Now().UTC()); e != nil {
		return cfg, e
	}
	return cfg, tx.Commit()
}
func (s *SQLStore) Apply(c context.Context, e Event) error {
	tx, err := s.DB.BeginTx(c, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(c, `INSERT IGNORE INTO gw_events(id,attempt_id,revision,payload,at) VALUES(?,?,?,?,?)`, e.ID, e.AttemptID, e.Revision, b, e.At); err != nil {
		return err
	}
	_, err = tx.ExecContext(c, `INSERT IGNORE INTO gw_attempts(attempt_id,request_id,app_id,department,model_id,status,purpose,revision,tokens,cost_micro,at,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, e.AttemptID, e.RequestID, e.AppID, e.Department, e.Model.ID, e.Status, e.Purpose, e.Revision, e.Usage.Total, e.CostMicro, e.At, b)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(c, `UPDATE gw_attempts SET status=?,revision=?,tokens=?,cost_micro=?,payload=? WHERE attempt_id=? AND revision<?`, e.Status, e.Revision, e.Usage.Total, e.CostMicro, b, e.AttemptID, e.Revision)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func where(f Filter) (string, []any) {
	parts := []string{"1=1"}
	args := []any{}
	add := func(q string, x any) { parts = append(parts, q); args = append(args, x) }
	if f.AppID != "" {
		add("app_id=?", f.AppID)
	}
	if f.ModelID != "" {
		add("model_id=?", f.ModelID)
	}
	if f.RequestID != "" {
		add("request_id=?", f.RequestID)
	}
	if f.Status != "" {
		add("status=?", f.Status)
	}
	if !f.From.IsZero() {
		add("at>=?", f.From.UTC())
	}
	if !f.To.IsZero() {
		add("at<=?", f.To.UTC())
	}
	return strings.Join(parts, " AND "), args
}
func (s *SQLStore) Records(c context.Context, f Filter) ([]Event, error) {
	w, args := where(f)
	n := f.Limit
	if n <= 0 || n > 1000 {
		n = 1000
	}
	args = append(args, n, f.Offset)
	rows, e := s.DB.QueryContext(c, `SELECT payload FROM gw_attempts WHERE `+w+` ORDER BY at DESC LIMIT ? OFFSET ?`, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		var x Event
		if e = json.Unmarshal(b, &x); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *SQLStore) Audits(c context.Context, n int) ([]Audit, error) {
	rows, e := s.DB.QueryContext(c, `SELECT id,actor,action,version,at FROM gw_audit ORDER BY at DESC LIMIT ?`, n)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Audit{}
	for rows.Next() {
		var a Audit
		if e = rows.Scan(&a.ID, &a.Actor, &a.Action, &a.Version, &a.At); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (s *SQLStore) Heartbeat(c context.Context, id string, v int64) error {
	_, e := s.DB.ExecContext(c, `INSERT INTO gw_instances(id,version,seen) VALUES(?,?,?) ON DUPLICATE KEY UPDATE version=?,seen=?`, id, v, time.Now().UTC(), v, time.Now().UTC())
	return e
}
func (s *SQLStore) Instances(c context.Context) (map[string]Instance, error) {
	rows, e := s.DB.QueryContext(c, `SELECT id,version,seen FROM gw_instances`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[string]Instance{}
	for rows.Next() {
		var id string
		var x Instance
		if e = rows.Scan(&id, &x.Version, &x.Seen); e != nil {
			return nil, e
		}
		out[id] = x
	}
	return out, rows.Err()
}

type Report struct {
	AppID      string `json:"app_id"`
	Department string `json:"department"`
	ModelID    string `json:"model_id"`
	Count      int64  `json:"count"`
	Tokens     int64  `json:"tokens"`
	Micro      int64  `json:"micro"`
	Pending    int64  `json:"pending"`
}
type Aggregator interface {
	Report(context.Context, Filter) ([]Report, error)
}

func (s *SQLStore) Report(c context.Context, f Filter) ([]Report, error) {
	w, args := where(f)
	rows, e := s.DB.QueryContext(c, `SELECT app_id,department,model_id,COUNT(*),COALESCE(SUM(tokens),0),COALESCE(SUM(cost_micro),0),SUM(status='started') FROM gw_attempts WHERE purpose<>'request' AND `+w+` GROUP BY app_id,department,model_id`, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Report{}
	for rows.Next() {
		var r Report
		if e = rows.Scan(&r.AppID, &r.Department, &r.ModelID, &r.Count, &r.Tokens, &r.Micro, &r.Pending); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *MemoryStore) Report(c context.Context, f Filter) ([]Report, error) {
	f.Limit = 0
	es, e := s.Records(c, f)
	if e != nil {
		return nil, e
	}
	m := map[string]Report{}
	for _, x := range es {
		if x.Purpose == "request" {
			continue
		}
		k := x.AppID + ":" + x.Model.ID
		r := m[k]
		r.AppID = x.AppID
		r.Department = x.Department
		r.ModelID = x.Model.ID
		r.Count++
		r.Tokens += x.Usage.Total
		r.Micro += x.CostMicro
		if x.Status == "started" {
			r.Pending++
		}
		m[k] = r
	}
	out := []Report{}
	for _, x := range m {
		out = append(out, x)
	}
	return out, nil
}
func CheckEvent(e Event) error {
	if e.ID == "" || e.AttemptID == "" || e.RequestID == "" || e.AppID == "" || e.Revision < 1 || e.Usage.Total < 0 || e.CostMicro < 0 {
		return errors.New("invalid event")
	}
	if e.Kind != "started" && e.Kind != "completed" {
		return fmt.Errorf("invalid event kind")
	}
	return nil
}

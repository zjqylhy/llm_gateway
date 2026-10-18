package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"net/url"
	"strings"
	"time"
)

type SQLStore struct {
	DB      *sql.DB
	Metrics *Metrics
}

func gatewayMySQLConfig(dsn string) (*mysql.Config, error) {
	cfg, e := mysql.ParseDSN(dsn)
	if e != nil {
		return nil, errors.New("invalid GW_MYSQL_DSN")
	}
	// 明确使用 utf8mb4 时由驱动安全转义参数，省去每条查询的临时 prepare/close 往返。
	// 其他字符集、非 utf8mb4 排序规则及显式 false 保留原有预处理行为。
	canonical := cfg.FormatDSN()
	_, query, _ := strings.Cut(canonical[strings.LastIndexByte(canonical, '/')+1:], "?")
	params, _ := url.ParseQuery(query)
	if !strings.Contains(dsn, "interpolateParams=") && params.Get("charset") == "utf8mb4" && (cfg.Collation == "" || strings.HasPrefix(cfg.Collation, "utf8mb4_")) {
		cfg.InterpolateParams = true
	}
	// Context 取消之外保留 socket 超时，覆盖网络半开及无期限后台操作。
	if cfg.Timeout == 0 {
		cfg.Timeout = 3 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	return cfg, nil
}
func NewSQLStore(c context.Context, dsn string) (*SQLStore, error) {
	open, idle, e := mysqlPoolOptions()
	if e != nil {
		return nil, e
	}
	cfg, e := gatewayMySQLConfig(dsn)
	if e != nil {
		return nil, e
	}
	db, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(open)
	db.SetMaxIdleConns(idle)
	db.SetConnMaxLifetime(5 * time.Minute)
	if e = db.PingContext(c); e != nil {
		db.Close()
		return nil, e
	}
	s := &SQLStore{DB: db}
	return s, nil
}

// 迁移由显式 migrate 命令执行，运行服务不持有 DDL 权限。
func (s *SQLStore) Migrate(c context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS gw_metering_apps (app_id VARCHAR(64) PRIMARY KEY, state VARCHAR(16) NOT NULL, generation VARCHAR(64) NOT NULL, reason VARCHAR(64) NOT NULL, report_id VARCHAR(64) NOT NULL, checked_at DATETIME(6) NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_metering_reports (id VARCHAR(64) PRIMARY KEY, app_id VARCHAR(64) NOT NULL, actor VARCHAR(128) NOT NULL, reason VARCHAR(128) NOT NULL, passed BOOLEAN NOT NULL, at DATETIME(6) NOT NULL, payload MEDIUMBLOB NOT NULL, INDEX(app_id,at))`,
		`CREATE TABLE IF NOT EXISTS gw_config (id INT PRIMARY KEY, version BIGINT NOT NULL, payload JSON NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_audit (id VARCHAR(64) PRIMARY KEY, actor VARCHAR(128) NOT NULL, action VARCHAR(128) NOT NULL, version BIGINT NOT NULL, at DATETIME(6) NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_events (id VARCHAR(128) PRIMARY KEY, attempt_id VARCHAR(64) NOT NULL, revision BIGINT NOT NULL, payload JSON NOT NULL, at DATETIME(6) NOT NULL, INDEX(attempt_id))`,
		`CREATE TABLE IF NOT EXISTS gw_attempts (attempt_id VARCHAR(64) PRIMARY KEY, request_id VARCHAR(64) NOT NULL, app_id VARCHAR(64) NOT NULL, department VARCHAR(128) NOT NULL, model_id VARCHAR(64) NOT NULL, status VARCHAR(32) NOT NULL, purpose VARCHAR(32) NOT NULL, revision BIGINT NOT NULL, tokens BIGINT NOT NULL, cost_micro BIGINT NOT NULL, at DATETIME(6) NOT NULL, payload JSON NOT NULL, INDEX(app_id,at), INDEX(request_id), INDEX(model_id,at), INDEX(status,at))`,
		`CREATE TABLE IF NOT EXISTS gw_instances (id VARCHAR(128) PRIMARY KEY, version BIGINT NOT NULL, seen DATETIME(6) NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_alerts (id VARCHAR(128) PRIMARY KEY, app_id VARCHAR(64) NOT NULL, active BOOLEAN NOT NULL, at DATETIME(6) NOT NULL, payload JSON NOT NULL, INDEX(app_id,at))`,
		`CREATE TABLE IF NOT EXISTS gw_metering_control (id INT PRIMARY KEY, state VARCHAR(16) NOT NULL, reason VARCHAR(64) NOT NULL, generation VARCHAR(64) NOT NULL, updated_at DATETIME(6) NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_metering_stream (id INT PRIMARY KEY, cluster_id VARCHAR(128) NOT NULL, topic VARCHAR(249) NOT NULL, group_id VARCHAR(249) NOT NULL, stream_id VARCHAR(64) NOT NULL, partitions INT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_metering_checkpoint (partition_id INT PRIMARY KEY, base_offset BIGINT NOT NULL, next_offset BIGINT NOT NULL, anchor_offset BIGINT NOT NULL, anchor_hash CHAR(64) NOT NULL, owner_epoch BIGINT NOT NULL, kafka_generation INT NOT NULL, owner_id VARCHAR(256) NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_metering_recovery (id CHAR(64) PRIMARY KEY, manifest MEDIUMBLOB NOT NULL, phase VARCHAR(32) NOT NULL, updated_at DATETIME(6) NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_usage_corrections (operation_id CHAR(64) PRIMARY KEY, input_hash CHAR(64) NOT NULL, payload MEDIUMBLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gw_metering_outbox (event_id VARCHAR(128) PRIMARY KEY, attempt_id VARCHAR(64) NOT NULL, app_id VARCHAR(64) NOT NULL, revision BIGINT NOT NULL, payload MEDIUMBLOB NULL, payload_hash CHAR(64) NOT NULL, charged_bytes BIGINT NOT NULL, published BOOLEAN NOT NULL DEFAULT 0, delivered BOOLEAN NOT NULL DEFAULT 0, claim_epoch BIGINT NOT NULL DEFAULT 0, retry_count INT NOT NULL DEFAULT 0, lease_until DATETIME(6) NULL, next_retry_at DATETIME(6) NOT NULL, INDEX(published,delivered,next_retry_at), INDEX(attempt_id))`,
	}
	for _, q := range statements {
		if _, e := s.DB.ExecContext(c, q); e != nil {
			return e
		}
	}
	if _, err := s.DB.ExecContext(c, `INSERT IGNORE INTO gw_metering_control(id,state,reason,generation,updated_at) VALUES(1,'healthy','',?,UTC_TIMESTAMP(6))`, ID("gen_")); err != nil {
		return err
	}
	if err := s.migrateAttemptLifecycle(c); err != nil {
		return err
	}
	if err := s.migrateCapacityProtocol(c); err != nil {
		return err
	}
	b, _ := json.Marshal(EmptyConfig())
	_, e := s.DB.ExecContext(c, `INSERT IGNORE INTO gw_config(id,version,payload) VALUES(1,0,?)`, string(b))
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
	if len(b) > maxConfigBytes {
		return cfg, errors.New("configuration exceeds 1 MiB")
	}
	if _, e = tx.ExecContext(c, `UPDATE gw_config SET payload=?,version=? WHERE id=1`, string(b), cfg.Version); e != nil {
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
	if err = applyEventTx(c, tx, e); err != nil {
		return err
	}
	return tx.Commit()
}

// 账本更新可与 Kafka 检查点共享事务，不能让恢复水位先于账本提交。
func applyEventTx(c context.Context, tx *sql.Tx, e Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(c, `INSERT IGNORE INTO gw_events(id,attempt_id,revision,payload,at) VALUES(?,?,?,?,?)`, e.ID, e.AttemptID, e.Revision, string(b), e.At); err != nil {
		return err
	}
	// INSERT IGNORE 只允许相同内容重放，不能吞掉同 ID 的矛盾计量。
	var existing []byte
	if err = tx.QueryRowContext(c, `SELECT payload FROM gw_events WHERE id=?`, e.ID).Scan(&existing); err != nil {
		return err
	}
	var previous Event
	if err = json.Unmarshal(existing, &previous); err != nil {
		return err
	}
	oldRaw, err := eventBytes(previous)
	if err != nil {
		return err
	}
	newRaw, err := eventBytes(e)
	if err != nil {
		return err
	}
	if Hash(string(oldRaw)) != Hash(string(newRaw)) {
		return ErrEventConflict
	}
	billing, _ := billingExposure(e)
	phase := "inflight"
	if e.Kind == "completed" {
		phase = "completed"
	}
	_, err = tx.ExecContext(c, `INSERT INTO gw_attempts(attempt_id,request_id,app_id,department,model_id,status,purpose,revision,tokens,cost_micro,at,payload,execution_phase,billing_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE attempt_id=attempt_id`, e.AttemptID, e.RequestID, e.AppID, e.Department, e.Model.ID, e.Status, e.Purpose, e.Revision, e.Usage.Total, e.CostMicro, e.At, string(b), phase, billing)
	if err != nil {
		return err
	}
	var latestRaw []byte
	var latestRevision int64
	if err = tx.QueryRowContext(c, `SELECT payload,revision FROM gw_attempts WHERE attempt_id=? FOR UPDATE`, e.AttemptID).Scan(&latestRaw, &latestRevision); err != nil {
		return err
	}
	var latest Event
	if err = json.Unmarshal(latestRaw, &latest); err != nil {
		return err
	}
	if latest.AppID != e.AppID || latest.RequestID != e.RequestID || latest.Purpose != e.Purpose || latest.Month != e.Month {
		return ErrEventConflict
	}
	if latestRevision == e.Revision {
		latestBytes, encodeErr := eventBytes(latest)
		if encodeErr != nil {
			return encodeErr
		}
		if Hash(string(latestBytes)) != Hash(string(newRaw)) {
			return ErrEventConflict
		}
	}
	_, err = tx.ExecContext(c, `UPDATE gw_attempts SET status=?,revision=?,tokens=?,cost_micro=?,payload=?,billing_state=?,execution_phase=CASE WHEN execution_phase='cancelled' THEN 'cancelled' WHEN execution_phase='prepared' AND ?='started' THEN 'prepared' ELSE ? END WHERE attempt_id=? AND revision<?`, e.Status, e.Revision, e.Usage.Total, e.CostMicro, string(b), billing, e.Kind, phase, e.AttemptID, e.Revision)
	if err != nil {
		return err
	}
	return nil
}
func where(f Filter) (string, []any) {
	// Prepare 只是持久准入意图，不能把尚未 Arm 的快照当成已调用记录。
	parts := []string{"execution_phase<>'prepared'", "compensation_state<>'pending'"}
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
	bytesRead := 0
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		bytesRead += len(b)
		if bytesRead > 16<<20 {
			return nil, ErrRecordCapacity
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
	rows, e := s.DB.QueryContext(c, `SELECT id,version,seen FROM gw_instances WHERE seen>=? LIMIT 10001`, time.Now().Add(-24*time.Hour))
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
		if len(out) > 10000 {
			return nil, errors.New("too many live instances")
		}
	}
	return out, rows.Err()
}

type Report struct {
	FeeParts
	AppID                string `json:"app_id"`
	Department           string `json:"department"`
	ModelID              string `json:"model_id"`
	Count                int64  `json:"count"`
	Tokens               int64  `json:"tokens"`
	Micro                int64  `json:"micro"`
	Pending              int64  `json:"pending"`
	InputTokens          int64  `json:"input_tokens"`
	UncachedInputTokens  int64  `json:"uncached_input_tokens"`
	OutputTokens         int64  `json:"output_tokens"`
	CachedTokens         int64  `json:"cached_tokens"`
	CacheUnreported      int64  `json:"cache_unreported"`
	UsageUnreported      int64  `json:"usage_unreported"`
	UnallocatedCount     int64  `json:"unallocated_count"`
	CacheEmbeddingCount  int64  `json:"cache_embedding_count"`
	CacheEmbeddingTokens int64  `json:"cache_embedding_tokens"`
	CacheEmbeddingMicro  int64  `json:"cache_embedding_micro"`
	CacheHits            int64  `json:"cache_hits"`
}
type Aggregator interface {
	Report(context.Context, Filter) ([]Report, error)
}

func (s *SQLStore) Report(c context.Context, f Filter) ([]Report, error) {
	w, args := where(f)
	rows, e := s.DB.QueryContext(c, costReportSQL(w), args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Report{}
	for rows.Next() {
		var r Report
		if e = rows.Scan(&r.AppID, &r.Department, &r.ModelID, &r.Count, &r.Tokens, &r.Micro, &r.Pending,
			&r.InputTokens, &r.UncachedInputTokens, &r.OutputTokens, &r.CachedTokens, &r.CacheUnreported, &r.UsageUnreported,
			&r.InputMicro, &r.CachedInputMicro, &r.OutputMicro, &r.UnallocatedMicro, &r.UnallocatedCount,
			&r.CacheEmbeddingCount, &r.CacheEmbeddingTokens, &r.CacheEmbeddingMicro, &r.CacheHits); e != nil {
			return nil, e
		}
		out = append(out, r)
		if len(out) > 10000 {
			return nil, ErrReportCapacity
		}
	}
	return out, rows.Err()
}
func (s *MemoryStore) Report(c context.Context, f Filter) ([]Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hits := map[string]bool{}
	for _, e := range s.events {
		if e.Purpose == "request" && e.Cached {
			hits[e.AppID+":"+e.RequestID] = true
		}
	}
	m := map[string]Report{}
	for _, x := range s.events {
		if x.Purpose == "request" || !Match(x, f) {
			continue
		}
		k := x.AppID + "\x00" + x.Department + "\x00" + x.Model.ID
		r := m[k]
		r.AppID = x.AppID
		r.Department = x.Department
		r.ModelID = x.Model.ID
		r.Count++
		r.Tokens += x.Usage.Total
		r.Micro += x.CostMicro
		r.InputTokens += x.Usage.Input
		r.UncachedInputTokens += x.Usage.Input - x.Usage.Cached
		r.OutputTokens += x.Usage.Output
		r.CachedTokens += x.Usage.Cached
		if !cacheUsageKnown(x) {
			r.CacheUnreported++
		}
		if x.Usage.Input+x.Usage.Output != x.Usage.Total || x.Kind == "started" || x.Status == "started" {
			r.UsageUnreported++
		}
		parts := breakdown(x)
		r.InputMicro += parts.InputMicro
		r.CachedInputMicro += parts.CachedInputMicro
		r.OutputMicro += parts.OutputMicro
		r.UnallocatedMicro += parts.UnallocatedMicro
		if !parts.Known {
			r.UnallocatedCount++
		}
		if x.Purpose == "cache_embedding" {
			r.CacheEmbeddingCount++
			r.CacheEmbeddingTokens += x.Usage.Total
			r.CacheEmbeddingMicro += x.CostMicro
			if hits[x.AppID+":"+x.RequestID] {
				r.CacheHits++
			}
		}
		if x.Status == "started" || x.Quality == "estimated" {
			r.Pending++
		}
		m[k] = r
		if len(m) > 10000 {
			return nil, ErrReportCapacity
		}
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

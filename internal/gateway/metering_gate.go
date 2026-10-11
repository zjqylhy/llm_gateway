package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const meteringPermitTTL = 3 * time.Second

var errMeteringBarrier = errors.New("application metering barrier")

type meteringPermit struct {
	Generation string `json:"generation"`
	Expires    int64  `json:"expires"`
	Token      string `json:"token"`
}
type admissionProof struct {
	Permit        meteringPermit
	AppGeneration string
	Raw           string
}
type admissionProofKey struct{}

func appControlKey(app string) string { return "gw:metering:app:" + app }
func (r *Runtime) permitKey() string {
	return r.Engine.Publisher.(EtcdPublisher).Prefix + "/metering/permit"
}

// 许可不缓存：etcd 不可访问时停止新费用；Redis 脚本再次校验绝对到期时间。
func (r *Runtime) readPermit(c context.Context) (meteringPermit, string, error) {
	ctx, done := context.WithTimeout(c, time.Second)
	defer done()
	et := r.Engine.Publisher.(EtcdPublisher).Client
	res, err := et.Txn(ctx).Then(clientv3.OpGet(r.permitKey()), clientv3.OpGet(r.permitStateKey())).Commit()
	if err != nil || len(res.Responses) != 2 || len(res.Responses[0].GetResponseRange().Kvs) != 1 || len(res.Responses[1].GetResponseRange().Kvs) != 1 {
		return meteringPermit{}, "", ErrUnavailable
	}
	var p meteringPermit
	raw := string(res.Responses[0].GetResponseRange().Kvs[0].Value)
	if json.Unmarshal([]byte(raw), &p) != nil || p.Generation == "" || p.Token == "" || p.Expires <= time.Now().UnixMilli() || p.Expires > time.Now().Add(meteringPermitTTL).UnixMilli() {
		return p, "", ErrUnavailable
	}
	if string(res.Responses[1].GetResponseRange().Kvs[0].Value) != "healthy:"+p.Generation {
		return p, "", ErrUnavailable
	}
	return p, raw, nil
}

// 登记早于 Redis 预占，应用屏障与登记共用同一行锁，涵盖尚未写 started 的准备阶段。
func (r *Runtime) beginAdmission(c context.Context, app, attempt string, required, appLimit int64) (context.Context, func(), error) {
	slot, endSlot := context.WithTimeout(c, time.Second)
	defer endSlot()
	select {
	case r.admissionSlots <- struct{}{}:
	case <-slot.Done():
		return c, nil, ErrUnavailable
	}
	registered := false
	defer func() {
		if !registered {
			<-r.admissionSlots
		}
	}()
	p, raw, err := r.readPermit(c)
	if err != nil {
		return c, nil, err
	}
	s := r.Engine.Store.(*SQLStore)
	ctx, done := context.WithTimeout(c, time.Second)
	defer done()
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return c, nil, err
	}
	defer tx.Rollback()
	var state, gen, appGen string
	if err = tx.QueryRowContext(ctx, `SELECT state,generation FROM gw_metering_control WHERE id=1 FOR SHARE`).Scan(&state, &gen); err != nil {
		return c, nil, err
	}
	if state != "healthy" || gen != p.Generation {
		return c, nil, ErrUnavailable
	}
	if err = tx.QueryRowContext(ctx, `SELECT state,generation FROM gw_metering_apps WHERE app_id=? FOR SHARE`, app).Scan(&state, &appGen); err != nil {
		return c, nil, ErrUnavailable
	}
	if state != "healthy" {
		return c, nil, ErrUnavailable
	}
	// 只做只读预检来减少已满容量下的预占/取消洪峰，最终额度仍由 outbox 原子事务决定。
	var globalUsed, appUsed int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT used_bytes FROM gw_metering_capacity WHERE id='global'),0),COALESCE((SELECT used_bytes FROM gw_metering_capacity WHERE id=?),0)`, "app:"+app).Scan(&globalUsed, &appUsed); err != nil {
		return c, nil, err
	}
	if required > appLimit-appUsed || required > r.Engine.Bus.(*DurableBus).GlobalBytes-globalUsed {
		return c, nil, ErrUnavailable
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO gw_metering_admissions(attempt_id,app_id,at) VALUES(?,?,UTC_TIMESTAMP(6))`, attempt, app); err != nil {
		return c, nil, err
	}
	if err = tx.Commit(); err != nil {
		return c, nil, err
	}
	proof := admissionProof{p, appGen, raw}
	c = context.WithValue(c, admissionProofKey{}, proof)
	registered = true
	end := func() {
		// 槽位涵盖上游与完成事件收尾，避免廉价接单把 completed 持久化压垮。
		defer func() { <-r.admissionSlots }()
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(c), time.Second)
		defer cancel()
		// 删除失败保留登记；对账超时会冻结，不能把未知调用当成已经退出。
		_, _ = s.DB.ExecContext(cleanup, `DELETE FROM gw_metering_admissions WHERE attempt_id=?`, attempt)
	}
	return c, end, nil
}

func checkAppBarrier(c context.Context, tx *sql.Tx, app string, correction bool) error {
	var state string
	err := tx.QueryRowContext(c, `SELECT state FROM gw_metering_apps WHERE app_id=? FOR SHARE`, app).Scan(&state)
	// 离线修复/旧测试来源可以没有应用控制行，真实入口必须先 beginAdmission。
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if state == "quiescing" || (!correction && state != "healthy") {
		return errMeteringBarrier
	}
	return nil
}

// 水位只在 Redis 已结算后推进。旧快照不能以新消息覆盖丢失的已提交历史。
const advanceWatermarkLua = `if redis.call('GET','gw:metering:generation')~=ARGV[1] then return -1 end
local old=tonumber(redis.call('HGET',KEYS[1],ARGV[2]));if not old or old<tonumber(ARGV[3]) then return -1 end
if old<tonumber(ARGV[4]) then redis.call('HSET',KEYS[1],ARGV[2],ARGV[4]) end;return 1`

func (r *Runtime) advanceWatermark(c context.Context, gen string, partition int, cursor, next int64) error {
	n, err := r.Redis.Eval(c, advanceWatermarkLua, []string{"gw:metering:watermarks"}, gen, partition, cursor, next).Int()
	if err != nil || n != 1 {
		return ErrCheckpoint
	}
	return nil
}

// 仅空部署或受控离线重建调用；运行探测绝不自动修复 generation/水位。
func initializeMeteringEvidence(c context.Context, s *SQLStore, g RedisGuard) error {
	var gen string
	if err := s.DB.QueryRowContext(c, `SELECT generation FROM gw_metering_control WHERE id=1`).Scan(&gen); err != nil {
		return err
	}
	_, checkpoints, err := s.Checkpoints(c)
	if err != nil {
		return err
	}
	p := g.Client.TxPipeline()
	p.Del(c, "gw:metering:permit", "gw:metering:watermarks")
	p.Set(c, "gw:metering:generation", gen, 0)
	for _, cp := range checkpoints {
		p.HSet(c, "gw:metering:watermarks", strconv.Itoa(cp.Partition), cp.Next)
	}
	_, err = p.Exec(c)
	return err
}

// 顺序取 Kafka -> SQL -> Redis，允许 SQL 领先 Redis；动态推进时只重新读取领先的一侧。
func (r *Runtime) permitEvidence(c context.Context) (string, int64, error) {
	s := r.Engine.Store.(*SQLStore)
	actual, ranges, err := r.KafkaRanges(c)
	if err != nil {
		return "", 0, err
	}
	source, cp, err := s.Checkpoints(c)
	if err != nil {
		return "", 0, err
	}
	if !sameSource(actual, source) {
		return "", 0, ErrCheckpoint
	}
	var state, gen string
	if err = s.DB.QueryRowContext(c, `SELECT state,generation FROM gw_metering_control WHERE id=1`).Scan(&state, &gen); err != nil {
		return "", 0, err
	}
	if state != "healthy" {
		return "", 0, ErrUnavailable
	}
	values, err := r.Redis.MGet(c, "gw:quota:state", "gw:metering:generation", "gw:metering:frozen").Result()
	if err != nil {
		return "", 0, err
	}
	if values[0] != "ready" || values[1] != gen || values[2] != nil {
		return "", 0, ErrCheckpoint
	}
	watermarks, err := r.Redis.HGetAll(c, "gw:metering:watermarks").Result()
	if err != nil {
		return "", 0, err
	}
	if len(watermarks) != len(cp) {
		return "", 0, ErrCheckpoint
	}
	var lag int64
	for i, p := range cp {
		w, parse := strconv.ParseInt(watermarks[strconv.Itoa(p.Partition)], 10, 64)
		committed := ranges[i].Committed
		if committed < 0 && p.Next == 0 {
			committed = 0
		}
		if parse != nil || w < committed || committed < 0 || w < ranges[i].First {
			return "", 0, ErrCheckpoint
		}
		if w > p.Next {
			if err = s.DB.QueryRowContext(c, `SELECT next_offset FROM gw_metering_checkpoint WHERE partition_id=?`, p.Partition).Scan(&p.Next); err != nil {
				return "", 0, err
			}
			if w > p.Next {
				return "", 0, ErrCheckpoint
			}
		}
		if committed > p.Next {
			return "", 0, ErrCheckpoint
		}
		if ranges[i].End > committed {
			lag += ranges[i].End - committed
		}
	}
	// 积压数量有界；年龄更能识别停滞，正常异步结算不能按一个消息的差额误冻结。
	var stale int
	if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_outbox WHERE delivered=0 AND next_retry_at<UTC_TIMESTAMP(6)-INTERVAL 30 SECOND`).Scan(&stale); err != nil {
		return "", lag, err
	}
	if stale > 0 || lag > 10000 {
		return "", lag, ErrUnavailable
	}
	return gen, lag, nil
}

func (r *Runtime) renewPermit(c context.Context) error {
	gen, lag, err := r.permitEvidence(c)
	r.Engine.Metrics.MeteringLag.Set(float64(lag))
	if err != nil {
		return err
	}
	p := meteringPermit{gen, time.Now().Add(meteringPermitTTL).UnixMilli(), ID("permit_")}
	raw, _ := json.Marshal(p)
	et := r.Engine.Publisher.(EtcdPublisher).Client
	lease, err := et.Grant(c, 3)
	if err != nil {
		return err
	}
	// Redis 先保存，etcd 后发布。部分失败的许可不会在入口通过两个门禁。
	if err = r.Redis.Set(c, "gw:metering:permit", raw, meteringPermitTTL).Err(); err != nil {
		return err
	}
	_, err = et.Txn(c).Then(clientv3.OpPut(r.permitKey(), string(raw), clientv3.WithLease(lease.ID)), clientv3.OpPut(r.permitStateKey(), "healthy:"+gen)).Commit()
	if err == nil {
		r.Engine.Metrics.MeteringPermit.Set(float64(p.Expires) / 1000)
	}
	return err
}

// 同一数据库只有持有独占连接锁的 worker 发布许可/轮转核验。锁连接丢失立即退出本轮。
func (r *Runtime) RunMeteringMonitor(c context.Context) {
	s := r.Engine.Store.(*SQLStore)
	for c.Err() == nil {
		ctx, done := context.WithTimeout(c, 2*time.Second)
		conn, err := s.DB.Conn(ctx)
		if err == nil {
			var locked int
			err = conn.QueryRowContext(ctx, `SELECT GET_LOCK(?,0)`, "gw-monitor-"+Hash(r.permitKey())[:32]).Scan(&locked)
			if err == nil && locked == 1 {
				done()
				r.runMonitorOwner(c, conn)
				_ = conn.Close()
				continue
			}
			_ = conn.Close()
		}
		done()
		select {
		case <-c.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (r *Runtime) runMonitorOwner(c context.Context, conn *sql.Conn) {
	name := "gw-monitor-" + Hash(r.permitKey())[:32]
	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_, _ = conn.ExecContext(ctx, `SELECT RELEASE_LOCK(?)`, name)
	}()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var cursor string
	rotationStart := time.Now()
	var auditing bool
	results := make(chan struct{}, 1)
	owner, cancel := context.WithCancel(c)
	defer func() {
		cancel()
		if auditing {
			select {
			case <-results:
			case <-time.After(4 * time.Second):
			}
		}
	}()
	for {
		ctx, done := context.WithTimeout(owner, 900*time.Millisecond)
		var owns bool
		err := conn.QueryRowContext(ctx, `SELECT IS_USED_LOCK(?)=CONNECTION_ID()`, name).Scan(&owns)
		if err != nil || !owns {
			done()
			return
		}
		if err == nil {
			err = r.renewPermit(ctx)
		}
		done()
		if err != nil {
			r.Engine.Metrics.MeteringPermit.Set(0)
		}
		if errors.Is(err, ErrCheckpoint) {
			r.freezeConsumer(owner)
		}
		if !auditing && err == nil {
			probe, end := context.WithTimeout(owner, 900*time.Millisecond)
			app, next, findErr := r.nextAuditApp(probe, cursor)
			end()
			previousCursor := cursor
			cursor = next
			if findErr == nil && next == "" && previousCursor != "" {
				r.Engine.Metrics.MeteringRotation.Set(time.Since(rotationStart).Seconds())
				rotationStart = time.Now()
			}
			if findErr == nil && app != "" {
				auditing = true
				go func() { _, _ = r.AuditApplication(owner, app, "system", "periodic"); results <- struct{}{} }()
			}
		}
		select {
		case <-owner.Done():
			return
		case <-results:
			auditing = false
		case <-tick.C:
		}
	}
}

func (r *Runtime) nextAuditApp(c context.Context, cursor string) (string, string, error) {
	s := r.Engine.Store.(*SQLStore)
	// 新应用仅在没有任何账本、补偿或在途调用时创建初始控制行。
	cfg, err := s.Load(c)
	if err != nil {
		return "", cursor, err
	}
	apps := make([]string, 0, len(cfg.Apps))
	for app := range cfg.Apps {
		apps = append(apps, app)
	}
	sort.Strings(apps)
	rows, err := s.DB.QueryContext(c, `SELECT app_id,state,generation FROM gw_metering_apps ORDER BY app_id LIMIT 10001`)
	if err != nil {
		return "", cursor, err
	}
	existing := map[string][2]string{}
	for rows.Next() {
		var app, state, gen string
		if err = rows.Scan(&app, &state, &gen); err != nil {
			break
		}
		existing[app] = [2]string{state, gen}
		if len(existing) > 10000 {
			err = errors.New("application controls exceed monitor limit")
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return "", cursor, err
	}
	created := 0
	for _, app := range apps {
		previous, present := existing[app]
		if present && previous[0] != "initializing" {
			continue
		}
		if created >= 32 {
			break
		}
		created++
		var scan uint64
		for {
			keys, next, e := r.Redis.Scan(c, scan, "gw:{app:"+app+"}:*", 500).Result()
			if e != nil {
				return "", cursor, e
			}
			if len(keys) > 0 {
				return "", cursor, ErrCheckpoint
			}
			scan = next
			if scan == 0 {
				break
			}
		}
		gen := previous[1]
		if !present {
			gen = ID("appgen_")
			res, e := s.DB.ExecContext(c, `INSERT IGNORE INTO gw_metering_apps(app_id,state,generation,reason,report_id) SELECT ?,'initializing',?,'empty_verification','' WHERE NOT EXISTS(SELECT 1 FROM gw_attempts WHERE app_id=?) AND NOT EXISTS(SELECT 1 FROM gw_metering_outbox WHERE app_id=?) AND NOT EXISTS(SELECT 1 FROM gw_metering_admissions WHERE app_id=?)`, app, gen, app, app, app)
			if e != nil {
				return "", cursor, e
			}
			changed, _ := res.RowsAffected()
			if changed != 1 {
				continue
			}
		}
		// 两阶段初始化可重试。Redis 已发布而 SQL 更新失败时仍不能接单。
		if err = r.Redis.HSet(c, appControlKey(app), "generation", gen, "state", "healthy").Err(); err != nil {
			return "", cursor, err
		}
		if _, err = s.DB.ExecContext(c, `UPDATE gw_metering_apps SET state='healthy',reason='',checked_at=UTC_TIMESTAMP(6) WHERE app_id=? AND state='initializing' AND generation=?`, app, gen); err != nil {
			return "", cursor, err
		}
	}
	var remaining int
	var used int64
	if err = s.DB.QueryRowContext(c, `SELECT COALESCE((SELECT used_bytes FROM gw_metering_capacity WHERE id='global'),0)`).Scan(&used); err != nil {
		return "", cursor, err
	}
	r.Engine.Metrics.MeteringOutboxBytes.Set(float64(used))
	if err = s.DB.QueryRowContext(c, `SELECT COUNT(*) FROM gw_metering_apps WHERE checked_at IS NULL OR checked_at<UTC_TIMESTAMP(6)-INTERVAL 60 SECOND`).Scan(&remaining); err != nil {
		return "", cursor, err
	}
	r.Engine.Metrics.MeteringUnchecked.Set(float64(remaining))
	var app string
	err = s.DB.QueryRowContext(c, `SELECT app_id FROM gw_metering_apps WHERE state='healthy' AND app_id>? AND (checked_at IS NULL OR checked_at<UTC_TIMESTAMP(6)-INTERVAL 60 SECOND) ORDER BY app_id LIMIT 1`, cursor).Scan(&app)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	return app, app, err
}

func eventDigest(e Event) string { raw, _ := eventBytes(e); return Hash(string(raw)) }

func syncApplicationEvidence(c context.Context, r *Runtime) error {
	rows, err := r.Engine.Store.(*SQLStore).DB.QueryContext(c, `SELECT app_id,state,generation FROM gw_metering_apps ORDER BY app_id LIMIT 10001`)
	if err != nil {
		return err
	}
	defer rows.Close()
	p := r.Redis.TxPipeline()
	count := 0
	for rows.Next() {
		var app, state, gen string
		if err = rows.Scan(&app, &state, &gen); err != nil {
			return err
		}
		count++
		if count > 10000 {
			return errors.New("application controls exceed recovery limit")
		}
		p.HSet(c, appControlKey(app), "state", state, "generation", gen)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	_, err = p.Exec(c)
	return err
}

func stampReservation(c context.Context, g RedisGuard, e Event) error {
	const script = `local rev=tonumber(redis.call('HGET',KEYS[1],'revision')) or 0;if rev==0 then local old=redis.call('HGET',KEYS[1],'hash');if old and old~=ARGV[1] then return -1 end;redis.call('HSET',KEYS[1],'hash',ARGV[1]) end;return 1`
	n, err := g.Client.Eval(c, script, []string{quotaKeys(e.AppID, e.Month, e.AttemptID)[1]}, eventDigest(e)).Int()
	if err != nil || n != 1 {
		return ErrEventConflict
	}
	return nil
}

func (r *Runtime) generation(c context.Context) (string, error) {
	var gen string
	err := r.Engine.Store.(*SQLStore).DB.QueryRowContext(c, `SELECT generation FROM gw_metering_control WHERE id=1`).Scan(&gen)
	return gen, err
}

func (r *Runtime) permitStateKey() string {
	return r.Engine.Publisher.(EtcdPublisher).Prefix + "/metering/state"
}
func (r *Runtime) freezeEtcd(c context.Context) {
	_, _ = r.Engine.Publisher.(EtcdPublisher).Client.Put(c, r.permitStateKey(), "frozen")
}

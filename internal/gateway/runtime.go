package gateway

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Runtime struct {
	admissionSlots  chan struct{}
	capacity        *capacityGate
	commitBatchSize int
	commitInterval  time.Duration
	Engine          *Engine
	Admin           *Admin
	readerConfig    kafka.ReaderConfig
	KafkaClient     *kafka.Client
	Redis           *redis.Client
	MaxConnections  int
	closers         []func()
	TraceShutdown   func(context.Context) error
}

func env(k, fallback string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return fallback
}
func OpenRuntime(ctx context.Context, dev bool) (out *Runtime, err error) {
	out = &Runtime{}
	cacheEnabled, err := semanticCacheEnabled()
	if err != nil {
		return nil, err
	}
	if _, _, err = mysqlPoolOptions(); err != nil {
		return nil, err
	}
	if _, err = poolEnv("GW_REDIS_POOL_SIZE", 32, 1); err != nil {
		return nil, err
	}
	out.commitBatchSize, out.commitInterval, err = meteringCommitOptions()
	if err != nil {
		return nil, err
	}
	meteringConcurrency, err := concurrencyEnv("GW_MAX_METERING_INFLIGHT", 8)
	if err != nil {
		return nil, err
	}
	out.admissionSlots = make(chan struct{}, meteringConcurrency)
	opened := out
	defer func() {
		if err != nil {
			opened.Close()
		}
	}()
	key := []byte{}
	if raw := os.Getenv("GW_MASTER_KEY"); raw != "" {
		key, err = base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, errors.New("GW_MASTER_KEY must be base64")
		}
	} else if dev {
		key = make([]byte, 32)
		_, err = rand.Read(key)
	} else {
		return nil, errors.New("GW_MASTER_KEY is required")
	}
	vault, err := NewVault(key)
	if err != nil {
		return nil, err
	}
	e := &Engine{Vault: vault, Client: NewHTTPClient(dev), Dev: dev, InstanceID: env("HOSTNAME", ID("node_")), Metrics: NewMetrics()}
	e.CacheDisabled = !cacheEnabled
	e.MaxConcurrent, err = concurrencyEnv("GW_MAX_CONCURRENT_REQUESTS", defaultGatewayConcurrency)
	if err != nil {
		return nil, err
	}
	adminConcurrency, err := concurrencyEnv("GW_MAX_ADMIN_REQUESTS", defaultAdminConcurrency)
	if err != nil {
		return nil, err
	}
	out.MaxConnections, err = concurrencyEnv("GW_MAX_CONNECTIONS", defaultConnectionLimit)
	if err != nil {
		return nil, err
	}
	out.Engine = e
	if dev {
		s := NewMemoryStore()
		g := NewMemoryGuard()
		e.Store = s
		e.Guard = g
		e.Bus = LocalBus{s, g}
		if cacheEnabled {
			e.Cache = NewMemoryCache()
		}
		e.Publisher = LocalPublisher{s}
		mock := httptest.NewServer(http.HandlerFunc(MockProvider))
		out.closers = append(out.closers, mock.Close)
		password := os.Getenv("GW_ADMIN_PASSWORD")
		if password == "" {
			password = ID("local_")
			fmt.Println("本地管理员一次性密码：", password)
		}
		if len(password) < 12 || len(password) > 72 {
			return nil, errors.New("admin password requires 12..72 bytes")
		}
		h, he := Password(password)
		if he != nil {
			return nil, he
		}
		token := ID("sk_gw_")
		_, err = s.Update(ctx, "bootstrap", "development.seed", func(c *Config) error {
			c.Users["admin"] = User{"admin", h, "admin"}
			for i, id := range []string{"primary", "backup", "third"} {
				p := Provider{id, "mock", mock.URL + "/v1", vault.Encrypt(id, "local-test")}
				c.Providers[id] = p
				m := Model{ID: id, ProviderID: id, Upstream: "mock-" + id, Version: "offline-v1", Capabilities: []string{"chat", "stream", "tools", "json_object", "json_schema", "seed"}, Context: 16000, MaxOutput: 4000, RPM: 100000, TPM: 10000000, InputMicro: 1000000, CachedInputMicro: 100000, OutputMicro: 2000000, PriceVersion: "offline-v1", Currency: "CNY", FXPPM: 1000000}
				c.Models[id] = m
				_ = i
			}
			m := Model{ID: "embedding", ProviderID: "primary", Upstream: "mock-embedding", Version: "offline-v1", Capabilities: []string{"embedding"}, Context: 16000, Dimensions: 32, InputMicro: 1000000, PriceVersion: "offline-v1", Currency: "CNY", FXPPM: 1000000}
			c.Models[m.ID] = m
			c.Routes["chat"] = Route{Alias: "chat", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{"primary", 60}, {"backup", 30}, {"third", 10}}}
			c.Routes["embedding"] = Route{Alias: "embedding", Kind: "embedding", Strategy: "priority", Candidates: []Candidate{{"embedding", 100}}}
			app := App{ID: "demo", Name: "本地测试应用", Department: "研发", Owner: "admin", Active: true, Models: []string{"chat", "embedding"}, QPS: 1000, MonthlyTokens: 10000000, BudgetMicro: 100000000, BudgetAction: "warn", CacheTTL: 300, Similarity: .97, EmbeddingModel: "embedding"}
			c.Apps[app.ID] = app
			c.Keys["demo-key"] = Key{ID: "demo-key", AppID: app.ID, Hash: Hash(token), State: "active"}
			return nil
		})
		if err != nil {
			return nil, err
		}
		fmt.Println("本地测试应用一次性 API Key：", token)
	} else {
		required := []string{"GW_MYSQL_DSN", "GW_REDIS_URL", "GW_KAFKA_BROKERS", "GW_ETCD_ENDPOINTS", "GW_ADMIN_ORIGIN"}
		if cacheEnabled {
			required = append(required, "GW_CACHE_REDIS_URL", "GW_MILVUS_URL")
		}
		for _, k := range required {
			if os.Getenv(k) == "" {
				return nil, fmt.Errorf("%s is required in production", k)
			}
		}
		sqlStore, se := NewSQLStore(ctx, os.Getenv("GW_MYSQL_DSN"))
		if se != nil {
			return nil, se
		}
		e.Store = sqlStore
		out.closers = append(out.closers, func() { _ = sqlStore.DB.Close() })
		opts, re := quotaRedisOptions(os.Getenv("GW_REDIS_URL"))
		var cacheRedis *redis.Client
		if re == nil && cacheEnabled {
			var cacheOpts *redis.Options
			opts, cacheOpts, re = isolatedRedisOptions(os.Getenv("GW_REDIS_URL"), os.Getenv("GW_CACHE_REDIS_URL"))
			if re == nil {
				cacheRedis = redis.NewClient(cacheOpts)
				out.closers = append(out.closers, func() { _ = cacheRedis.Close() })
			}
		}
		if re != nil {
			return nil, re
		}
		rc := redis.NewClient(opts)
		if re = rc.Ping(ctx).Err(); re != nil {
			_ = rc.Close()
			return nil, re
		}
		out.Redis = rc
		e.Metering = out
		out.closers = append(out.closers, func() { _ = rc.Close() })
		e.Guard = RedisGuard{Client: rc, RequireReady: true, EnforceMemory: true}
		brokers := strings.Split(os.Getenv("GW_KAFKA_BROKERS"), ",")
		transport := &kafka.Transport{DialTimeout: 3 * time.Second}
		dialer := &kafka.Dialer{Timeout: 3 * time.Second}
		if os.Getenv("GW_KAFKA_TLS") == "true" {
			transport.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
			dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		if user := os.Getenv("GW_KAFKA_USER"); user != "" {
			mech := plain.Mechanism{Username: user, Password: os.Getenv("GW_KAFKA_PASSWORD")}
			transport.SASL = mech
			dialer.SASLMechanism = mech
		}
		writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: env("GW_KAFKA_TOPIC", "gateway-events"), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false, WriteTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second, BatchTimeout: time.Millisecond, Transport: transport}
		out.KafkaClient = &kafka.Client{Addr: kafka.TCP(brokers...), Transport: transport, Timeout: 3 * time.Second}
		globalBytes, be := outboxBytesEnv("GW_OUTBOX_GLOBAL_BYTES", 512<<20)
		if be != nil {
			return nil, be
		}
		appBytes, be := outboxBytesEnv("GW_OUTBOX_APP_BYTES", 64<<20)
		if be != nil {
			return nil, be
		}
		sqlStore.Metrics = e.Metrics
		e.Metrics.registerSQLPool(sqlStore.DB)
		e.Metrics.MeteringOutboxLimit.Set(float64(globalBytes))
		e.Bus = &DurableBus{Store: sqlStore, Transport: &KafkaBus{Writer: writer, Store: sqlStore}, Metrics: e.Metrics, OnFreeze: func(c context.Context) {
			_ = rc.Set(c, "gw:metering:frozen", "delivery_failure", 0).Err()
			out.freezeEtcd(c)
		}}
		out.capacity = newCapacityGate(globalBytes, e.Metrics, sqlStore.sampleCapacity)
		out.capacity.limits = func(app string) (int64, bool) {
			state := e.state.Load()
			if state == nil {
				return 0, false
			}
			a, ok := state.cfg.Apps[app]
			limit := appBytes
			if a.OutboxBytes > 0 {
				limit = a.OutboxBytes
			}
			return limit, ok && a.Active
		}
		e.Bus.(*DurableBus).CheckFrozen = func(c context.Context) error {
			n, err := rc.Exists(c, "gw:metering:frozen").Result()
			if err != nil {
				return err
			}
			if n > 0 {
				return ErrUnavailable
			}
			_, err = out.readPermit(c)
			return err
		}
		out.closers = append(out.closers, func() { _ = writer.Close() })
		out.readerConfig = kafka.ReaderConfig{Brokers: brokers, Topic: env("GW_KAFKA_TOPIC", "gateway-events"), GroupID: env("GW_KAFKA_GROUP", "gateway-metering"), MinBytes: 1, MaxBytes: 1 << 20, QueueCapacity: 8, CommitInterval: 0, Dialer: dialer}
		etConfig := clientv3.Config{Endpoints: strings.Split(os.Getenv("GW_ETCD_ENDPOINTS"), ","), DialTimeout: 3 * time.Second, Username: os.Getenv("GW_ETCD_USER"), Password: os.Getenv("GW_ETCD_PASSWORD")}
		if strings.HasPrefix(etConfig.Endpoints[0], "https://") {
			etConfig.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		et, ee := clientv3.New(etConfig)
		if ee != nil {
			return nil, ee
		}
		out.closers = append(out.closers, func() { _ = et.Close() })
		e.Publisher = EtcdPublisher{et, env("GW_ETCD_PREFIX", "/llm-gateway")}
		if cacheEnabled {
			e.Cache = MilvusCache{URL: strings.TrimRight(os.Getenv("GW_MILVUS_URL"), "/"), Token: os.Getenv("GW_MILVUS_TOKEN"), Collection: env("GW_MILVUS_COLLECTION", "gateway_cache"), Client: &http.Client{Timeout: 3 * time.Second}, Redis: cacheRedis}
		}
	}
	out.Admin = NewAdmin(e, !dev, os.Getenv("GW_ADMIN_ORIGIN"))
	out.Admin.Grafana, err = newGrafanaProxy(os.Getenv("GW_GRAFANA_URL"), os.Getenv("GW_GRAFANA_VIEWER_USER"), os.Getenv("GW_GRAFANA_VIEWER_PASSWORD"))
	if err != nil {
		return nil, err
	}
	if out.Admin.Grafana != nil {
		out.closers = append(out.closers, out.Admin.Grafana.Client.CloseIdleConnections)
	}
	out.Admin.MaxConcurrent = adminConcurrency
	if out.Redis != nil {
		out.Admin.Sessions = RedisSessions{out.Redis}
	}
	assets := env("GW_WEB_DIR", "web/dist")
	out.Admin.Assets = http.FileServer(http.Dir(filepath.Clean(assets)))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		exporter, te := otlptracehttp.New(ctx)
		if te != nil {
			return nil, te
		}
		resource := resource.NewSchemaless(attribute.String("service.name", env("GW_SERVICE_NAME", "llm-gateway")))
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(resource), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(.1))))
		otel.SetTracerProvider(tp)
		out.TraceShutdown = tp.Shutdown
	}
	return out, nil
}

func (r *Runtime) Close() {
	if r == nil {
		return
	}
	if r.capacity != nil {
		r.capacity.close()
	}
	for i := len(r.closers) - 1; i >= 0; i-- {
		r.closers[i]()
	}
	if r.TraceShutdown != nil {
		ctx, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		_ = r.TraceShutdown(ctx)
	}
}
func Main(mode string) {
	if mode == "cache-init" {
		enabled, err := semanticCacheEnabled()
		if err != nil || !enabled {
			slog.Error("cache-init unavailable: semantic cache is disabled or GW_CACHE_ENABLED is invalid")
			os.Exit(1)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	dev := mode == "dev"
	runtime, e := OpenRuntime(startup, dev)
	if e != nil {
		slog.Error("startup failed", "error", e)
		os.Exit(1)
	}
	defer runtime.Close()
	if mode == "migrate" {
		if s, ok := runtime.Engine.Store.(*SQLStore); ok {
			e = s.Migrate(startup)
		}
		if e != nil {
			slog.Error("migration failed", "error", e)
			os.Exit(1)
		}
		return
	}
	if mode == "bootstrap" {
		if e = InitializeCheckpoints(startup, runtime, false); e == nil {
			e = Bootstrap(startup, runtime.Engine)
		}
		if e != nil {
			slog.Error("bootstrap failed", "error", e)
			os.Exit(1)
		}
		return
	}
	if mode == "metering-checkpoint-init" || mode == "metering-recover" {
		maintenance, finish := context.WithTimeout(ctx, 10*time.Minute)
		defer finish()
		if mode == "metering-checkpoint-init" {
			e = InitializeCheckpoints(maintenance, runtime, true)
		} else {
			e = RunRecoveryCommand(maintenance, runtime, os.Args[1:])
		}
		if e != nil {
			slog.Error("metering recovery failed", "error", e)
			os.Exit(1)
		}
		return
	}
	if mode == "cache-init" {
		n, err := strconv.Atoi(os.Getenv("GW_CACHE_DIMENSIONS"))
		if err != nil || n < 1 || n > 32768 {
			slog.Error("GW_CACHE_DIMENSIONS must be 1..32768")
			os.Exit(1)
		}
		if cache, ok := runtime.Engine.Cache.(MilvusCache); ok {
			e = cache.Init(startup, n)
		} else {
			e = errors.New("cache-init requires production mode")
		}
		if e != nil {
			slog.Error("cache initialization failed", "error", e)
			os.Exit(1)
		}
		return
	}
	if mode == "quota-rebuild" {
		if e = RebuildQuota(ctx, runtime); e != nil {
			slog.Error("quota rebuild failed", "error", e)
			os.Exit(1)
		}
		return
	}
	if mode == "metering-audit" || mode == "metering-resume" {
		auditCtx, finish := context.WithTimeout(ctx, 5*time.Minute)
		defer finish()
		var result MeteringAudit
		if mode == "metering-resume" {
			result, e = ResumeMetering(auditCtx, runtime)
		} else {
			result, e = AuditMetering(auditCtx, runtime)
		}
		if e != nil {
			slog.Error("metering verification failed", "error", e)
			os.Exit(1)
		}
		slog.Info("metering verification complete", "groups", result.Groups, "attempts", result.Attempts, "pending", result.Pending)
		return
	}
	if !dev {
		if state, err := runtime.Redis.Get(startup, "gw:quota:state").Result(); err != nil || state != "ready" {
			slog.Error("quota state unavailable; bootstrap or offline rebuild required")
			os.Exit(1)
		}
	}
	// 角色随心跳保存，运维页可识别同一服务器上的不同服务。
	runtime.Engine.InstanceID = mode + ":" + runtime.Engine.InstanceID
	if e = runtime.Engine.Refresh(startup); e != nil {
		slog.Error("configuration unavailable", "error", e)
		os.Exit(1)
	}
	if bus, ok := runtime.Engine.Bus.(*DurableBus); ok {
		if transport, ok := bus.Transport.(*KafkaBus); ok {
			if e = transport.PrepareSource(startup); e != nil {
				slog.Error("metering source unavailable", "error", e)
				os.Exit(1)
			}
		}
	}
	go runtime.Engine.RunRefresh(ctx)
	if mode == "worker" {
		go runtime.RunMeteringMonitor(ctx)
		go runtime.RunAttemptMaintenance(ctx)
		health := http.NewServeMux()
		health.Handle("GET /healthz", runtime.Engine.Handler())
		health.Handle("GET /readyz", runtime.Engine.Handler())
		health.Handle("GET /metrics", runtime.Engine.Metrics.Handler())
		workerHealth := &http.Server{Addr: env("GW_WORKER_ADDR", ":8082"), Handler: health, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768}
		go func() {
			if err := serveBounded(workerHealth, runtime.MaxConnections); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("worker health server failed", "error", err)
				stop()
			}
		}()
		defer workerHealth.Close()
		go RunMaintenance(ctx, runtime.Engine)
		if bus, ok := runtime.Engine.Bus.(*DurableBus); ok {
			go bus.RunRelay(ctx)
		}
		if e = runtime.ConsumeMetering(ctx); e != nil && !errors.Is(e, context.Canceled) {
			slog.Error("worker stopped", "error", e)
			os.Exit(1)
		}
		return
	}
	if dev {
		go RunMaintenance(ctx, runtime.Engine)
	}
	servers := []*http.Server{}
	if dev || mode == "gateway" {
		if runtime.capacity != nil {
			runtime.capacity.start()
		}
		handler := http.NewServeMux()
		handler.Handle("GET /metrics", runtime.Engine.Metrics.Handler())
		handler.Handle("/", runtime.Engine.Handler())
		servers = append(servers, &http.Server{Addr: env("GW_GATEWAY_ADDR", ":8080"), Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768})
	}
	if dev || mode == "admin" {
		servers = append(servers, &http.Server{Addr: env("GW_ADMIN_ADDR", ":8081"), Handler: runtime.Admin.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768})
	}
	errCh := make(chan error, len(servers))
	for _, server := range servers {
		go func(s *http.Server) {
			slog.Info("server listening", "mode", mode, "address", s.Addr)
			errCh <- serveBounded(s, runtime.MaxConnections)
		}(server)
	}
	failed := false
	select {
	case <-ctx.Done():
	case e = <-errCh:
		if !errors.Is(e, http.ErrServerClosed) {
			failed = true
			slog.Error("server stopped", "error", e)
		}
		stop()
	}
	shutdown, done := context.WithTimeout(context.Background(), 150*time.Second)
	defer done()
	for _, s := range servers {
		_ = s.Shutdown(shutdown)
	}
	if failed {
		runtime.Close()
		os.Exit(1)
	}
}
func Bootstrap(ctx context.Context, e *Engine) error {
	if existing, err := e.Store.Load(ctx); err == nil && len(existing.Users) > 0 {
		if err = e.Publisher.PublishConfig(ctx, existing); err != nil {
			return err
		}
		return InitializeQuota(ctx, e)
	}
	username := env("GW_ADMIN_USER", "admin")
	password := os.Getenv("GW_ADMIN_PASSWORD")
	if len(password) < 12 || len(password) > 72 {
		return errors.New("set GW_ADMIN_PASSWORD to 12..72 bytes")
	}
	hash, err := Password(password)
	if err != nil {
		return err
	}
	cfg, err := e.Store.Update(ctx, "bootstrap", "admin.bootstrap", func(c *Config) error {
		if len(c.Users) > 0 {
			return errors.New("already bootstrapped")
		}
		c.Users[username] = User{username, hash, "admin"}
		return nil
	})
	if err != nil {
		return err
	}
	if err = e.Publisher.PublishConfig(ctx, cfg); err != nil {
		return err
	}
	return InitializeQuota(ctx, e)
}

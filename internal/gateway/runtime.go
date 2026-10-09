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
	Engine        *Engine
	Admin         *Admin
	Reader        *kafka.Reader
	KafkaClient   *kafka.Client
	Redis         *redis.Client
	closers       []func()
	TraceShutdown func(context.Context) error
}

func env(k, fallback string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return fallback
}
func OpenRuntime(ctx context.Context, dev bool) (out *Runtime, err error) {
	out = &Runtime{}
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
	out.Engine = e
	if dev {
		s := NewMemoryStore()
		g := NewMemoryGuard()
		e.Store = s
		e.Guard = g
		e.Bus = LocalBus{s, g}
		e.Cache = NewMemoryCache()
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
		for _, k := range []string{"GW_MYSQL_DSN", "GW_REDIS_URL", "GW_KAFKA_BROKERS", "GW_ETCD_ENDPOINTS", "GW_MILVUS_URL", "GW_ADMIN_ORIGIN"} {
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
		opts, re := redis.ParseURL(os.Getenv("GW_REDIS_URL"))
		if re != nil {
			return nil, re
		}
		opts.ContextTimeoutEnabled = true
		opts.DialTimeout = 2 * time.Second
		opts.ReadTimeout = 250 * time.Millisecond
		opts.WriteTimeout = 250 * time.Millisecond
		rc := redis.NewClient(opts)
		if re = rc.Ping(ctx).Err(); re != nil {
			_ = rc.Close()
			return nil, re
		}
		out.Redis = rc
		out.closers = append(out.closers, func() { _ = rc.Close() })
		e.Guard = RedisGuard{Client: rc, RequireReady: true}
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
		e.Bus = KafkaBus{writer}
		out.closers = append(out.closers, func() { _ = writer.Close() })
		out.Reader = kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: env("GW_KAFKA_TOPIC", "gateway-events"), GroupID: "gateway-metering", MinBytes: 1, MaxBytes: 10 << 20, CommitInterval: 0, Dialer: dialer})
		out.closers = append(out.closers, func() { _ = out.Reader.Close() })
		etConfig := clientv3.Config{Endpoints: strings.Split(os.Getenv("GW_ETCD_ENDPOINTS"), ","), DialTimeout: 3 * time.Second, Username: os.Getenv("GW_ETCD_USER"), Password: os.Getenv("GW_ETCD_PASSWORD")}
		if strings.HasPrefix(etConfig.Endpoints[0], "https://") {
			etConfig.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		et, ee := clientv3.New(etConfig)
		if ee != nil {
			return nil, ee
		}
		out.closers = append(out.closers, func() { _ = et.Close() })
		e.Publisher = EtcdPublisher{et, "/llm-gateway"}
		e.Cache = MilvusCache{URL: strings.TrimRight(os.Getenv("GW_MILVUS_URL"), "/"), Token: os.Getenv("GW_MILVUS_TOKEN"), Collection: env("GW_MILVUS_COLLECTION", "gateway_cache"), Client: &http.Client{Timeout: 3 * time.Second}, Redis: rc}
	}
	out.Admin = NewAdmin(e, !dev, os.Getenv("GW_ADMIN_ORIGIN"))
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
		if e = Bootstrap(startup, runtime.Engine); e != nil {
			slog.Error("bootstrap failed", "error", e)
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
	if !dev {
		if state, err := runtime.Redis.Get(startup, "gw:quota:state").Result(); err != nil || state != "ready" {
			slog.Error("quota state unavailable; bootstrap or offline rebuild required")
			os.Exit(1)
		}
	}
	if e = runtime.Engine.Refresh(startup); e != nil {
		slog.Error("configuration unavailable", "error", e)
		os.Exit(1)
	}
	go runtime.Engine.RunRefresh(ctx)
	if mode == "worker" {
		health := http.NewServeMux()
		health.Handle("GET /healthz", runtime.Engine.Handler())
		health.Handle("GET /readyz", runtime.Engine.Handler())
		health.Handle("GET /metrics", runtime.Engine.Metrics.Handler())
		workerHealth := &http.Server{Addr: env("GW_WORKER_ADDR", ":8082"), Handler: health, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := workerHealth.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("worker health server failed", "error", err)
				stop()
			}
		}()
		defer workerHealth.Close()
		go RunMaintenance(ctx, runtime.Engine)
		if e = Consume(ctx, runtime.Reader, runtime.Engine.Store, runtime.Engine.Guard); e != nil && !errors.Is(e, context.Canceled) {
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
			errCh <- s.ListenAndServe()
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
	shutdown, done := context.WithTimeout(context.Background(), 125*time.Second)
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

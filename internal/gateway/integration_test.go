package gateway

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// 必须显式开启，并限定本机专用环境；离线测试不冒充真实基础设施联调。
func TestInfrastructureIntegration(t *testing.T) {
	if os.Getenv("GW_RUN_INFRA_TESTS") != "1" {
		t.Skip("requires dedicated local MySQL/Redis/Kafka/etcd/Milvus stack")
	}
	for _, name := range []string{"GW_MYSQL_DSN", "GW_REDIS_URL", "GW_KAFKA_BROKERS", "GW_ETCD_ENDPOINTS", "GW_MILVUS_URL"} {
		if !strings.Contains(os.Getenv(name), "127.0.0.1") {
			t.Fatalf("%s must point to dedicated loopback services", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := OpenRuntime(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.Engine.Store.(*SQLStore)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = Bootstrap(ctx, r.Engine); err != nil {
		t.Fatal(err)
	}
	cfg, err := s.Update(ctx, "integration", "configuration.test", func(c *Config) error {
		c.Apps["integration"] = App{ID: "integration", Name: "Integration", BudgetAction: "warn"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Engine.Publisher.PublishConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	old := cfg
	old.Version--
	if err = r.Engine.Publisher.PublishConfig(ctx, old); err != nil {
		t.Fatal(err)
	}
	actual, err := r.Engine.Publisher.Current(ctx)
	if err != nil || actual.Version != cfg.Version {
		t.Fatal("configuration regressed", actual.Version, err)
	}
	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- Consume(consumeCtx, r.Reader, s, r.Engine.Guard) }()
	event := Event{ID: ID("event_"), RequestID: ID("request_"), AttemptID: ID("attempt_"), Revision: 1, Kind: "started", Purpose: "inference", AppID: "integration", At: time.Now().UTC(), Month: month(time.Now()), Status: "started", ReserveTokens: 100, ReserveMicro: 100, Model: Model{ID: "integration"}}
	if err = r.Engine.Guard.Reserve(ctx, App{ID: "integration"}, Reservation{ID: event.AttemptID, AppID: event.AppID, Month: event.Month, Tokens: 100, Micro: 100}); err != nil {
		t.Fatal(err)
	}
	if err = r.Engine.Bus.Publish(ctx, event); err != nil {
		t.Fatal(err)
	}
	event.ID = ID("event_")
	event.Revision = 2
	event.Kind = "completed"
	event.Status = "success"
	event.Quality = "reported"
	event.Usage = Usage{Input: 10, Output: 20, Total: 30}
	event.CostMicro = 40
	for i := 0; i < 3; i++ {
		if err = r.Engine.Bus.Publish(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	for {
		rows, err := s.Records(ctx, Filter{RequestID: event.RequestID})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 1 && rows[0].Revision == 2 {
			break
		}
		select {
		case err := <-done:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("metering timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}
	for {
		counts, err := r.Engine.Guard.Counters(ctx, event.AppID, event.Month)
		if err != nil {
			t.Fatal(err)
		}
		if counts.ReservedTokens == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("reservation not settled")
		case <-time.After(100 * time.Millisecond):
		}
	}
	reports, err := s.Report(ctx, Filter{RequestID: event.RequestID})
	if err != nil || len(reports) != 1 || reports[0].Tokens != 30 || reports[0].Micro != 40 {
		t.Fatal("duplicate ledger posting", reports, err)
	}
	cache := r.Engine.Cache.(MilvusCache)
	cache.Collection = ID("integration_")
	if err = cache.Init(ctx, 32); err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, 32)
	vector[0] = 1
	item := CacheItem{ID: ID("cache_"), Scope: ID("scope_"), Vector: vector, Response: []byte(`{"choices":[]}`), Expire: time.Now().Add(time.Minute).Unix()}
	if err = cache.Put(ctx, item); err != nil {
		t.Fatal(err)
	}
	for {
		hit, err := cache.Find(ctx, item.Scope, vector, .97)
		if err != nil {
			t.Fatal(err)
		}
		if hit != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("vector index not searchable")
		case <-time.After(100 * time.Millisecond):
		}
	}
	stop()
	if err := <-done; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
}

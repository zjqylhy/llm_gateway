package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
)

// 只断开测试网关的连接，共享服务不被停机；真实存储协议及持久化参与验收。
type meteringFaultProxy struct {
	listener    net.Listener
	target      string
	mu          sync.Mutex
	down        bool
	connections map[net.Conn]bool
}

func newMeteringFaultProxy(t *testing.T, target string) *meteringFaultProxy {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &meteringFaultProxy{listener: listener, target: target, connections: map[net.Conn]bool{}}
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go p.bridge(client)
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); p.fail() })
	return p
}
func (p *meteringFaultProxy) bridge(client net.Conn) {
	server, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if p.down {
		p.mu.Unlock()
		_ = client.Close()
		_ = server.Close()
		return
	}
	p.connections[client] = true
	p.connections[server] = true
	p.mu.Unlock()
	defer func() {
		_ = client.Close()
		_ = server.Close()
		p.mu.Lock()
		delete(p.connections, client)
		delete(p.connections, server)
		p.mu.Unlock()
	}()
	go func() { _, _ = io.Copy(server, client); _ = server.Close() }()
	_, _ = io.Copy(client, server)
}
func (p *meteringFaultProxy) fail() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	for c := range p.connections {
		_ = c.Close()
	}
}

func testCompletionNetworkFailure(t *testing.T, r *Runtime, parent context.Context, dependency string, stopWorker, startWorker func()) {
	t.Helper()
	s := r.Engine.Store.(*SQLStore)
	var proxy *meteringFaultProxy
	if dependency == "sql" {
		dsn, err := mysql.ParseDSN(os.Getenv("GW_MYSQL_DSN"))
		if err != nil {
			t.Fatal(err)
		}
		proxy = newMeteringFaultProxy(t, dsn.Addr)
		dsn.Addr = proxy.listener.Addr().String()
		t.Setenv("GW_MYSQL_DSN", dsn.FormatDSN())
	} else {
		proxy = newMeteringFaultProxy(t, strings.Split(os.Getenv("GW_KAFKA_BROKERS"), ",")[0])
	}
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { calls.Add(1); proxy.fail(); MockProvider(w, req) }))
	defer upstream.Close()
	app := "network-" + dependency
	cfg, err := s.Update(parent, "acceptance", "metering.network.seed", func(c *Config) error {
		c.Providers[app] = Provider{app, "mock", upstream.URL + "/v1", r.Engine.Vault.Encrypt(app, "test")}
		c.Models[app] = Model{ID: app, ProviderID: app, Upstream: "mock-network", Capabilities: []string{"chat", "stream"}, Context: 16000, MaxOutput: 128, InputMicro: 1000000, OutputMicro: 2000000, PriceVersion: "test", Currency: "CNY", FXPPM: 1000000}
		c.Routes[app] = Route{Alias: app, Kind: "chat", Strategy: "priority", Candidates: []Candidate{{app, 100}}}
		c.Apps[app] = App{ID: app, Active: true, Owner: "admin", Models: []string{app}, BudgetAction: "warn"}
		c.Keys[app] = Key{ID: app, AppID: app, Hash: Hash(app + "-key"), State: "active"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Engine.Publisher.PublishConfig(parent, cfg); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.nextAuditApp(parent, ""); err != nil {
		t.Fatal(err)
	}
	if err = r.renewPermit(parent); err != nil {
		t.Fatal(err)
	}
	gateway, err := OpenRuntime(parent, false)
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	gateway.Engine.Client = NewHTTPClient(true)
	if dependency == "kafka" {
		transport := gateway.Engine.Bus.(*DurableBus).Transport.(*KafkaBus).Writer.Transport.(*kafka.Transport)
		transport.Dial = func(c context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: time.Second}).DialContext(c, "tcp", proxy.listener.Addr().String())
		}
	}
	if err = gateway.Engine.Refresh(parent); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{"model": app, "messages": []any{map[string]string{"role": "user", "content": "network failure after started ACK"}}, "max_tokens": 64, "stream": true})
	req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(body)).WithContext(parent)
	req.Header.Set("Authorization", "Bearer "+app+"-key")
	w := httptest.NewRecorder()
	gateway.Engine.Handler().ServeHTTP(w, req)
	if calls.Load() != 1 {
		t.Fatal("failure retried paid upstream", calls.Load())
	}
	if dependency == "sql" {
		if !strings.HasSuffix(w.Body.String(), "data: [DONE]\n\n") {
			t.Fatal("healthy Kafka path lost completion", w.Code, w.Body.String())
		}
	} else {
		if strings.Contains(w.Body.String(), "[DONE]") || !strings.Contains(w.Body.String(), "metering_unavailable") {
			t.Fatal("Kafka outage completed normally", w.Code, w.Body.String())
		}
	}
	if !gateway.Engine.Bus.(*DurableBus).stopped.Load() {
		t.Fatal("failure did not latch gateway")
	}
	if dependency == "kafka" {
		if err = r.Engine.Bus.(*DurableBus).RelayOnce(parent); err != nil {
			t.Fatal("SQL outbox failed to compensate", err)
		}
	}
	if err = waitMetering(parent, func() (bool, error) {
		var n int
		err := s.DB.QueryRowContext(parent, `SELECT COUNT(*) FROM gw_attempts WHERE app_id=? AND revision>=2 AND purpose<>'request'`, app).Scan(&n)
		return n == 1, err
	}); err != nil {
		t.Fatal(err)
	}
	if err = waitMetering(parent, func() (bool, error) {
		v, e := r.Engine.Guard.Counters(parent, app, month(time.Now()))
		return v.Tokens == 32 && v.Micro == 52 && v.ReservedTokens == 0, e
	}); err != nil {
		t.Fatal("network recovery ledger mismatch", err)
	}
	stopWorker()
	t.Setenv("GW_QUOTA_REBUILD_ACK", "writers-stopped")
	t.Setenv("GW_METERING_RECOVERY_ACK", "writers-stopped")
	if err = RebuildQuota(parent, r); err != nil {
		t.Fatal(err)
	}
	if _, err = ResumeMetering(parent, r); err != nil {
		t.Fatal(err)
	}
	r.Engine.Bus.(*DurableBus).stopped.Store(false)
	startWorker()
}

package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdmissionRejectsBeforeBodyAndRecovers(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		MockProvider(w, r)
	})
	e.MaxConcurrent = 1
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- call(t, e, "held", false) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("first request did not enter upstream")
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var wg sync.WaitGroup
	var rejected atomic.Int64
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest("POST", "/v1/chat/completions", &unreadBody{t})
			w := httptest.NewRecorder()
			e.Handler().ServeHTTP(w, r)
			if w.Code == 503 && w.Header().Get("Retry-After") == "1" {
				rejected.Add(1)
			}
		}()
	}
	wg.Wait()
	if rejected.Load() != 32 || calls.Load() != 1 {
		t.Fatal("overload reached body or upstream", rejected.Load(), calls.Load())
	}
	w := httptest.NewRecorder()
	e.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != 200 {
		t.Fatal("overload blocked liveness", w.Code)
	}
	close(release)
	if w := <-firstDone; w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(t, e, "after release", false); w.Code != 200 {
		t.Fatal("slot leaked", w.Code, w.Body.String())
	}
}

type unreadBody struct{ t *testing.T }

func (b *unreadBody) Read([]byte) (int, error) {
	b.t.Error("rejected request body was read")
	return 0, errors.New("must reject before read")
}

func TestSSEEventAggregateSizeBound(t *testing.T) {
	// 所有行都在 Scanner 上限之内，但累计事件已超过上限。
	r := NewSSEReader(strings.NewReader(strings.Repeat("data: "+strings.Repeat("x", 1024)+"\n", 2200) + "\n"))
	if _, err := r.Next(); err == nil || !strings.Contains(err.Error(), "2 MiB") {
		t.Fatal("oversized multiline event accepted", err)
	}
	r = NewSSEReader(strings.NewReader("data: one\ndata: two\n\ndata: [DONE]\n\n"))
	if b, err := r.Next(); err != nil || string(b) != "one\ntwo" {
		t.Fatal(string(b), err)
	}
	if b, err := r.Next(); err != nil || string(b) != "[DONE]" {
		t.Fatal(string(b), err)
	}
}

type pipeWriter struct{ net.Conn }

func (*pipeWriter) Header() http.Header { return http.Header{} }
func (*pipeWriter) WriteHeader(int)     {}
func (w *pipeWriter) FlushError() error { _, err := w.Write([]byte("flush")); return err }

func TestSlowClientWriteAndFlushAreBounded(t *testing.T) {
	for _, flush := range []bool{false, true} {
		t.Run(fmt.Sprint(flush), func(t *testing.T) {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			w := &deadlineWriter{&pipeWriter{server}, 50 * time.Millisecond}
			start := time.Now()
			var err error
			if flush {
				err = http.NewResponseController(w).Flush()
			} else {
				_, err = w.Write([]byte("blocked"))
			}
			var ne net.Error
			if !errors.As(err, &ne) || !ne.Timeout() || time.Since(start) > time.Second {
				t.Fatal("slow client was not bounded", err, time.Since(start))
			}
		})
	}
}

func TestCacheMemoryIsBoundedAndOversizedItemsBypass(t *testing.T) {
	c := NewMemoryCache()
	x := CacheItem{ID: "oversized", Scope: "scope", Response: make([]byte, maxCacheItemBytes), Vector: []float32{1}, Expire: time.Now().Add(time.Hour).Unix()}
	if err := c.Put(context.Background(), x); err != nil || len(c.items) != 0 {
		t.Fatal("oversized item retained", err)
	}
	x.Response = make([]byte, 500<<10)
	for i := 0; i < 140; i++ {
		x.ID = fmt.Sprint(i)
		if err := c.Put(context.Background(), x); err != nil {
			t.Fatal(err)
		}
	}
	if c.bytes > maxMemoryCacheBytes || len(c.items) >= 140 {
		t.Fatal("byte capacity was not enforced", c.bytes, len(c.items))
	}
	x.ID = "owned"
	x.Response = []byte("original")
	_ = c.Put(context.Background(), x)
	x.Response[0] = 'X'
	x.Vector[0] = 0
	if item := c.items["owned"]; string(item.Response) != "original" || item.Vector[0] != 1 {
		t.Fatal("cache retains caller buffers")
	}
	x.Response = []byte("new")
	x.Vector = []float32{1}
	_ = c.Put(context.Background(), x)
	sum := 0
	for _, item := range c.items {
		sum += cacheItemSize(item)
	}
	if sum != c.bytes {
		t.Fatal("replacement byte accounting", sum, c.bytes)
	}
}

func TestOfflineLedgerAndDedupeFailClosedAtCapacity(t *testing.T) {
	g := NewMemoryGuard()
	for i := 0; i < maxMemoryAttempts; i++ {
		g.settled[fmt.Sprint(i)] = settlement{2, 1, 1}
	}
	if err := g.Reserve(context.Background(), App{ID: "app"}, Reservation{ID: "overflow", AppID: "app", Month: "2026-10", Tokens: 1, Micro: 1}); !errors.Is(err, ErrUnavailable) {
		t.Fatal("dedupe exceeded capacity", err)
	}
	if len(g.reservations) != 0 {
		t.Fatal("failed reservation mutated state")
	}
	if err := g.Settle(context.Background(), Event{AttemptID: "0", AppID: "app", Month: "2026-10", Revision: 2, Usage: Usage{Total: 1}, CostMicro: 1}); err != nil {
		t.Fatal("duplicate cannot be replayed at capacity", err)
	}
	s := NewMemoryStore()
	old := Event{AttemptID: "old", Revision: 1}
	_ = s.Apply(context.Background(), old)
	s.eventBytes = 64 << 20
	if err := s.Apply(context.Background(), Event{AttemptID: "new", Revision: 1}); !errors.Is(err, ErrUnavailable) || len(s.events) != 1 {
		t.Fatal("ledger exceeded capacity", err)
	}
	if err := s.Apply(context.Background(), old); err != nil {
		t.Fatal("duplicate failed at capacity", err)
	}
}

func TestRedisDBNumbersDoNotProvideIsolation(t *testing.T) {
	for _, cache := range []string{"redis://quota:6379/1", "rediss://QUOTA:6379/2"} {
		if _, _, err := isolatedRedisOptions("redis://quota:6379/0", cache); err == nil {
			t.Fatal("shared server accepted", cache)
		}
	}
	q, c, err := isolatedRedisOptions("redis://quota:6379/0", "redis://cache:6379/0")
	if err != nil || q.MaxActiveConns != 32 || c.MaxActiveConns != 32 {
		t.Fatal("bounded isolated pools not configured", err)
	}
}

func TestConfigurationCapacityRejectsWithoutMutation(t *testing.T) {
	s := NewMemoryStore()
	_, err := s.Update(context.Background(), "test", "oversized", func(c *Config) error {
		c.Apps["large"] = App{ID: "large", Name: strings.Repeat("x", maxConfigBytes)}
		return nil
	})
	if err == nil || s.config.Version != 0 || len(s.config.Apps) != 0 || len(s.audits) != 0 {
		t.Fatal("oversized configuration was committed", err)
	}
}

func TestConnectionLimitAndRelease(t *testing.T) {
	l, err := listenBounded("127.0.0.1:0", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	client1, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()
	server1, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server1.Close()
	client2, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()
	accepted := make(chan net.Conn, 1)
	failed := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			failed <- err
			return
		}
		accepted <- conn
	}()
	select {
	case conn := <-accepted:
		conn.Close()
		t.Fatal("listener admitted beyond limit")
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(50 * time.Millisecond):
	}
	_ = server1.Close()
	select {
	case conn := <-accepted:
		conn.Close()
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("closed connection did not release capacity")
	}
}

package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/netutil"
)

const (
	defaultConnectionLimit    = 256
	defaultGatewayConcurrency = 16
	defaultAdminConcurrency   = 8
	maxCacheItemBytes         = 512 << 10
	maxMemoryCacheBytes       = 64 << 20
	maxMemoryAttempts         = 10000
	maxConfigBytes            = 1 << 20
)

// 请求槽位只限制已读完请求头的调用；监听器另限制慢请求头和空闲连接。
func listenBounded(address string, limit int) (net.Listener, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return netutil.LimitListener(l, limit), nil
}
func serveBounded(s *http.Server, limit int) error {
	l, err := listenBounded(s.Addr, limit)
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// 容量不足时立即拒绝，不把待处理请求堆积到进程内存。
type admission struct {
	once  sync.Once
	slots chan struct{}
}

func (a *admission) enter(limit, fallback int) bool {
	a.once.Do(func() {
		if limit <= 0 {
			limit = fallback
		}
		a.slots = make(chan struct{}, limit)
	})
	select {
	case a.slots <- struct{}{}:
		return true
	default:
		return false
	}
}
func (a *admission) leave() { <-a.slots }

func concurrencyEnv(name string, fallback int) (int, error) {
	n, err := strconv.Atoi(env(name, strconv.Itoa(fallback)))
	if err != nil || n < 1 || n > 4096 {
		return 0, fmt.Errorf("%s must be 1..4096", name)
	}
	return n, nil
}

// 写入期限按块更新，慢客户端不能无限占用 SSE 或普通响应的处理槽。
// Unwrap 保留 ResponseController 的底层连接能力。
type deadlineWriter struct {
	http.ResponseWriter
	timeout time.Duration
}

func (w *deadlineWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *deadlineWriter) Write(p []byte) (int, error) {
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.timeout))
	return w.ResponseWriter.Write(p)
}
func (w *deadlineWriter) FlushError() error {
	c := http.NewResponseController(w.ResponseWriter)
	_ = c.SetWriteDeadline(time.Now().Add(w.timeout))
	return c.Flush()
}
func (w *deadlineWriter) Flush() { _ = w.FlushError() }

func protectedHandler(next http.Handler, a *admission, limit, fallback int, prefix, role string, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = &deadlineWriter{w, 5 * time.Second}
		if strings.HasPrefix(r.URL.Path, prefix) {
			if !a.enter(limit, fallback) {
				metrics.Overload.WithLabelValues(role).Inc()
				w.Header().Set("Retry-After", "1")
				APIError(w, http.StatusServiceUnavailable, "gateway_overloaded", "instance concurrency capacity exceeded")
				return
			}
			defer a.leave()
			if role == "admin" && r.URL.Path != "/admin/playground" {
				ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
				defer cancel()
				r = r.WithContext(ctx)
			}
		}
		next.ServeHTTP(w, r)
	})
}

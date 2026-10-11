package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestQuotaOperationBoundsUnresponsiveConnection(t *testing.T) {
	// TCP 已连接但服务不响应，不能仅靠建连超时限制治理操作。
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			_, _ = io.Copy(io.Discard, conn)
		}
	}()
	client := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), ContextTimeoutEnabled: true, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, DialerRetries: 1, MaxRetries: -1})
	defer client.Close()
	guard := RedisGuard{Client: client, RequireReady: true}
	start := time.Now()
	err = guard.Reserve(context.Background(), App{ID: "app"}, Reservation{ID: "attempt", AppID: "app", Month: "2026-10", Tokens: 10})
	if !errors.Is(err, ErrUnavailable) || time.Since(start) > 2*time.Second {
		t.Fatalf("quota operation did not respect total deadline: elapsed=%s error=%v", time.Since(start), err)
	}
}

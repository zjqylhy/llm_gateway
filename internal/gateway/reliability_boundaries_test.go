package gateway

import (
	"context"
	"net/http"
	"sync/atomic"
)

// 只编译进验收二进制，暂停真实依赖调用以准确注入进程崩溃。
type boundaryGuard struct {
	Guard
	Stage  string
	Paused *atomic.Bool
}

func (g boundaryGuard) Reserve(c context.Context, a App, r Reservation) error {
	if g.Stage == "before-reserve" {
		g.Paused.Store(true)
		<-c.Done()
		return c.Err()
	}
	if err := g.Guard.Reserve(c, a, r); err != nil {
		return err
	}
	g.Paused.Store(true)
	<-c.Done()
	return c.Err()
}

func (g boundaryGuard) Cancel(c context.Context, e Event) error {
	return cancelReservation(c, g.Guard, e)
}

type boundaryTransport struct {
	http.RoundTripper
	Paused *atomic.Bool
}

func (tr boundaryTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	tr.Paused.Store(true)
	<-r.Context().Done()
	return nil, r.Context().Err()
}

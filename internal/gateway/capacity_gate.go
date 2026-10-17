package gateway

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrCapacityTimeout = errors.New("outbox capacity wait timed out")
var errPrepareCommitUnknown = errors.New("prepare commit outcome unknown")

const capacityInterval = 200 * time.Millisecond
const capacityMaxAge = time.Second

type capacityWaiter struct {
	event   Event
	ticket  capacityTicket
	limit   int64
	ready   chan struct{}
	err     error
	granted bool
}

type capacityGate struct {
	mu          sync.Mutex
	once        sync.Once
	stop        context.CancelFunc
	done        chan struct{}
	sample      func(context.Context, map[string]capacityTicket) (capacitySnapshot, error)
	limits      func(string) (int64, bool)
	metrics     *Metrics
	globalLimit int64
	snapshot    capacitySnapshot
	tickets     map[string]capacityTicket
	localApps   map[string]int64
	queues      map[string][]*capacityWaiter
	order       []string
	cursor      int
	closed      bool
}

func newCapacityGate(global int64, metrics *Metrics, sample func(context.Context, map[string]capacityTicket) (capacitySnapshot, error)) *capacityGate {
	return &capacityGate{globalLimit: global, metrics: metrics, sample: sample, tickets: map[string]capacityTicket{}, localApps: map[string]int64{}, queues: map[string][]*capacityWaiter{}}
}

func (g *capacityGate) start() {
	g.once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			cancel()
			return
		}
		g.stop, g.done = cancel, make(chan struct{})
		g.mu.Unlock()
		go func() {
			defer close(g.done)
			for {
				g.mu.Lock()
				tickets := make(map[string]capacityTicket, len(g.tickets))
				for id, t := range g.tickets {
					tickets[id] = t
				}
				g.mu.Unlock()
				c, done := context.WithTimeout(ctx, 500*time.Millisecond)
				at := time.Now()
				snap, err := g.sample(c, tickets)
				done()
				g.metrics.meteringStage("capacity_sample", "other", at, err)
				if err == nil {
					g.publish(snap)
				}
				timer := time.NewTimer(capacityInterval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	})
}

func (g *capacityGate) close() {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		if g.stop != nil {
			g.stop()
		}
		for _, q := range g.queues {
			for _, w := range q {
				w.err = ErrUnavailable
				close(w.ready)
			}
		}
		g.queues, g.order = map[string][]*capacityWaiter{}, nil
	}
	done := g.done
	g.mu.Unlock()
	if done != nil {
		<-done
	}
}

func (g *capacityGate) publish(s capacitySnapshot) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.snapshot = s
	for _, id := range s.Covered {
		g.dropLocked(id)
	}
	if g.metrics != nil {
		g.metrics.MeteringOutboxBytes.Set(float64(s.Global))
		g.metrics.CapacitySampleAt.Set(float64(s.At.UnixNano()) / 1e9)
		g.metrics.CapacityOverBytes.Set(float64(max(0, s.Global-g.globalLimit)))
		g.metrics.CapacitySampleRows.Set(float64(s.Rows))
		g.metrics.CapacitySampleVersion.Inc()
		var appOver int64
		if g.limits != nil {
			for app, used := range s.Apps {
				if limit, ok := g.limits(app); ok {
					appOver = max(appOver, used-limit)
				}
			}
		}
		g.metrics.CapacityAppOverBytes.Set(float64(appOver))
	}
	g.dispatchLocked()
}

func (g *capacityGate) dropLocked(id string) {
	if t, ok := g.tickets[id]; ok {
		delete(g.tickets, id)
		g.localApps[t.App] -= maxEventBytes
		if g.localApps[t.App] == 0 {
			delete(g.localApps, t.App)
		}
	}
}

func (g *capacityGate) release(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.dropLocked(id)
	g.dispatchLocked()
}

// 仅覆盖内存检查与登记；同应用 FIFO，跨应用轮转，跳过满额应用。
func (g *capacityGate) dispatchLocked() {
	defer func() {
		if g.metrics != nil {
			g.metrics.CapacityLocalBytes.Set(float64(len(g.tickets) * maxEventBytes))
		}
	}()
	if g.closed || g.snapshot.At.IsZero() || time.Since(g.snapshot.At) > capacityMaxAge {
		return
	}
	for scanned := 0; len(g.order) > 0 && scanned < len(g.order); {
		g.cursor %= len(g.order)
		app := g.order[g.cursor]
		q := g.queues[app]
		w := q[0]
		limit, active := w.limit, true
		if g.limits != nil {
			limit, active = g.limits(app)
		}
		fits := maxEventBytes <= g.globalLimit-g.snapshot.Global-int64(len(g.tickets))*maxEventBytes && maxEventBytes <= limit-g.snapshot.Apps[app]-g.localApps[app]
		if !active || fits {
			if !active {
				w.err = ErrUnavailable
			} else {
				g.tickets[w.event.AttemptID] = w.ticket
				g.localApps[app] += maxEventBytes
				w.granted = true
			}
			g.queues[app] = q[1:]
			if len(q) == 1 {
				delete(g.queues, app)
				g.order = append(g.order[:g.cursor], g.order[g.cursor+1:]...)
			} else {
				g.cursor++
			}
			close(w.ready)
			scanned = 0
		} else {
			g.cursor++
			scanned++
		}
	}
}

func (g *capacityGate) acquire(c context.Context, e Event, limit int64) (err error) {
	at := time.Now()
	defer func() { g.metrics.meteringStage("capacity_wait", "started", at, err) }()
	if c.Err() != nil {
		return ErrCapacityTimeout
	}
	w := &capacityWaiter{event: e, ticket: capacityTicket{e.AppID, eventDigest(e)}, limit: limit, ready: make(chan struct{})}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrUnavailable
	}
	if _, ok := g.tickets[e.AttemptID]; ok {
		g.mu.Unlock()
		return ErrEventConflict
	}
	for _, queue := range g.queues {
		for _, pending := range queue {
			if pending.event.AttemptID == e.AttemptID {
				g.mu.Unlock()
				return ErrEventConflict
			}
		}
	}
	if len(g.queues[e.AppID]) == 0 {
		g.order = append(g.order, e.AppID)
	}
	g.queues[e.AppID] = append(g.queues[e.AppID], w)
	if g.metrics != nil {
		g.metrics.CapacityWaiters.Inc()
		defer g.metrics.CapacityWaiters.Dec()
	}
	g.dispatchLocked()
	g.mu.Unlock()
	select {
	case <-w.ready:
		if c.Err() == nil {
			return w.err
		}
	case <-c.Done():
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if w.granted {
		g.dropLocked(e.AttemptID)
	} else {
		q := g.queues[e.AppID]
		for i, v := range q {
			if v == w {
				g.queues[e.AppID] = append(q[:i], q[i+1:]...)
				break
			}
		}
		if len(g.queues[e.AppID]) == 0 {
			delete(g.queues, e.AppID)
			for i, app := range g.order {
				if app == e.AppID {
					g.order = append(g.order[:i], g.order[i+1:]...)
					break
				}
			}
		}
	}
	g.dispatchLocked()
	return ErrCapacityTimeout
}

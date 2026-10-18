package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const maxEventBytes = 1 << 20

var ErrEventConflict = errors.New("event identity has conflicting content")
var errOutboxBusy = errors.New("outbox SQL admission busy")

func outboxBytesEnv(name string, fallback int64) (int64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 2*maxEventBytes || n > 1<<40 {
		return 0, errors.New(name + " requires 2 MiB..1 TiB in bytes")
	}
	return n, nil
}

type OutboxDelivery struct {
	Event Event
	Epoch int64
}

type OutboxStore interface {
	SaveOutbox(context.Context, Event) error
	PublishedOutbox(context.Context, Event, int64, bool) error
	ClaimOutbox(context.Context) ([]OutboxDelivery, error)
	FreezeMetering(context.Context, string) error
}

// 发送结果不明时保留同一事件；补偿只补计量，不再次调用上游。
type DurableBus struct {
	Store          OutboxStore
	Transport      Bus
	Metrics        *Metrics
	stopped        atomic.Bool
	OnFreeze       func(context.Context)
	CheckFrozen    func(context.Context) error
	slotsOnce      sync.Once
	completedSlots chan struct{}
}

func (b *DurableBus) Ready(c context.Context) error {
	if b.stopped.Load() {
		return ErrUnavailable
	}
	if checker, ok := b.Store.(interface{ MeteringHealthy(context.Context) error }); ok {
		if err := checker.MeteringHealthy(c); err != nil {
			return err
		}
	}
	if b.CheckFrozen != nil {
		return b.CheckFrozen(c)
	}
	return nil
}

func (b *DurableBus) save(c context.Context, e Event) error {
	b.slotsOnce.Do(func() { b.completedSlots = make(chan struct{}, 8) })
	slots := b.completedSlots
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-c.Done():
		return errOutboxBusy
	}
	return b.Store.SaveOutbox(c, e)
}

func eventBytes(e Event) ([]byte, error) {
	if err := CheckEvent(e); err != nil {
		return nil, err
	}
	e.At = e.At.UTC()
	raw, err := json.Marshal(e)
	if err == nil && len(raw) > maxEventBytes {
		err = errors.New("metering event exceeds 1 MiB")
	}
	return raw, err
}

func (b *DurableBus) freeze(c context.Context, reason string) {
	b.stopped.Store(true)
	ctx, done := context.WithTimeout(context.WithoutCancel(c), time.Second)
	_ = b.Store.FreezeMetering(ctx, reason)
	done()
	if b.OnFreeze != nil {
		ctx, finish := context.WithTimeout(context.WithoutCancel(c), time.Second)
		b.OnFreeze(ctx)
		finish()
	}
}

func (b *DurableBus) Publish(c context.Context, e Event) error {
	if e.Kind == "started" {
		return ErrUnavailable
	}
	if c.Err() != nil {
		return c.Err()
	}
	if _, err := eventBytes(e); err != nil && e.PayloadCipher != "" {
		e.PayloadCipher = ""
	}
	if _, err := eventBytes(e); err != nil {
		return err
	}
	save, done := context.WithTimeout(c, 2*time.Second)
	start := time.Now()
	err := b.save(save, e)
	b.Metrics.meteringStage("outbox_save", meteringEventKind(e), start, err)
	done()
	if errors.Is(err, ErrEventConflict) {
		b.freeze(c, "event_conflict")
		return ErrUnavailable
	}
	// completed 的备用发布有独立预算，SQL 失败不能短路健康的 Kafka。
	send, cancel := context.WithTimeout(c, 3*time.Second)
	start = time.Now()
	pubErr := b.Transport.Publish(send, e)
	b.Metrics.meteringStage("kafka_ack", meteringEventKind(e), start, pubErr)
	cancel()
	if c.Err() == nil && e.Purpose != "request" && (pubErr != nil || err != nil && !errors.Is(err, errOutboxBusy)) {
		slog.Warn("metering delivery failed", "kind", e.Kind, "save_error", err, "publish_error", pubErr)
		b.freeze(c, "metering_delivery_failure")
	}
	if pubErr != nil {
		return ErrUnavailable
	}
	mark, finish := context.WithTimeout(context.WithoutCancel(c), time.Second)
	defer finish()
	// 状态更新失败不否定已得到的 Kafka ACK，消费者仍能证明事件已交付。
	start = time.Now()
	markErr := b.Store.PublishedOutbox(mark, e, 0, true)
	b.Metrics.meteringStage("outbox_mark", meteringEventKind(e), start, markErr)
	return nil
}

func (b *DurableBus) Close() error { return b.Transport.Close() }

func (b *DurableBus) RelayOnce(c context.Context) error {
	claim, finish := context.WithTimeout(c, 2*time.Second)
	items, err := b.Store.ClaimOutbox(claim)
	finish()
	if err != nil {
		return err
	}
	for _, item := range items {
		if c.Err() != nil {
			return c.Err()
		}
		send, done := context.WithTimeout(c, 3*time.Second)
		err = b.Transport.Publish(send, item.Event)
		done()
		mark, cancel := context.WithTimeout(c, time.Second)
		markErr := b.Store.PublishedOutbox(mark, item.Event, item.Epoch, err == nil)
		cancel()
		if markErr != nil {
			return markErr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *DurableBus) RunRelay(c context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	lastWarning := time.Time{}
	for {
		select {
		case <-c.Done():
			return
		case <-tick.C:
			if err := b.RelayOnce(c); err != nil && c.Err() == nil && time.Since(lastWarning) >= 10*time.Second {
				slog.Warn("metering outbox retry deferred")
				lastWarning = time.Now()
			}
		}
	}
}

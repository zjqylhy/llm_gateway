package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
)

type MemoryStore struct {
	mu             sync.Mutex
	config         Config
	events         map[string]Event
	audits         []Audit
	instances      map[string]Instance
	alerts         map[string]Alert
	eventBytes     int
	attempts       map[string]memoryAttempt
	reservedGlobal int64
	reservedApps   map[string]int64
	eventHashes    map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{config: EmptyConfig(), events: map[string]Event{}, instances: map[string]Instance{}}
}
func (s *MemoryStore) Load(context.Context) (Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Clone(s.config), nil
}
func (s *MemoryStore) Update(_ context.Context, actor, action string, fn func(*Config) error) (Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := Clone(s.config)
	if e := fn(&c); e != nil {
		return Config{}, e
	}
	c.Version++
	if b, err := json.Marshal(c); err != nil || len(b) > maxConfigBytes {
		return Config{}, errors.New("configuration exceeds 1 MiB")
	}
	s.config = c
	s.audits = append(s.audits, Audit{ID: ID("aud_"), Actor: actor, Action: action, At: time.Now().UTC(), Version: c.Version})
	if len(s.audits) > 10000 {
		s.audits = append([]Audit(nil), s.audits[len(s.audits)-10000:]...)
	}
	return Clone(c), nil
}
func (s *MemoryStore) Apply(_ context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyLocked(e)
}
func (s *MemoryStore) applyLocked(e Event) error {
	e.At = e.At.UTC()
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	hash := Hash(string(raw))
	if e.ID != "" {
		if previous, exists := s.eventHashes[e.ID]; exists {
			if previous != hash {
				return ErrEventConflict
			}
			return nil
		}
		if len(s.eventHashes) >= 2*maxMemoryAttempts {
			return ErrUnavailable
		}
	}
	old, ok := s.events[e.AttemptID]
	if ok && old.Revision == e.Revision {
		previous, _ := json.Marshal(old)
		if Hash(string(previous)) != hash {
			return ErrEventConflict
		}
	}
	if !ok || old.Revision < e.Revision {
		oldBytes := 0
		if ok {
			b, _ := json.Marshal(old)
			oldBytes = len(b)
		}
		// 离线账本不能删除旧记录换取新额度；达到上限后停止接收新事件。
		if (!ok && len(s.events) >= 2*maxMemoryAttempts) || s.eventBytes-oldBytes+len(raw) > 64<<20 {
			return ErrUnavailable
		}
		s.eventBytes += len(raw) - oldBytes
		s.events[e.AttemptID] = e
		if attempt, ok := s.attempts[e.AttemptID]; ok && e.Kind == "completed" && attempt.Phase != "cancelled" {
			s.reservedGlobal -= attempt.Remaining
			s.reservedApps[e.AppID] -= attempt.Remaining
			attempt.Phase = "completed"
			attempt.Remaining = 0
			s.attempts[e.AttemptID] = attempt
		}
	}
	if e.ID != "" {
		if s.eventHashes == nil {
			s.eventHashes = map[string]string{}
		}
		s.eventHashes[e.ID] = hash
	}
	return nil
}
func (s *MemoryStore) Records(_ context.Context, f Filter) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Event{}
	for _, e := range s.events {
		if Match(e, f) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	return Page(out, f), nil
}
func Match(e Event, f Filter) bool {
	return (f.AppID == "" || e.AppID == f.AppID) && (f.ModelID == "" || e.Model.ID == f.ModelID) && (f.RequestID == "" || e.RequestID == f.RequestID) && (f.Status == "" || e.Status == f.Status) && (f.From.IsZero() || !e.At.Before(f.From)) && (f.To.IsZero() || !e.At.After(f.To))
}
func Page(es []Event, f Filter) []Event {
	if f.Offset >= len(es) {
		return []Event{}
	}
	if f.Offset > 0 {
		es = es[f.Offset:]
	}
	if f.Limit > 0 && len(es) > f.Limit {
		es = es[:f.Limit]
	}
	return es
}
func (s *MemoryStore) Audits(_ context.Context, n int) ([]Audit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := 0
	if len(s.audits) > n {
		start = len(s.audits) - n
	}
	return append([]Audit{}, s.audits[start:]...), nil
}
func (s *MemoryStore) Heartbeat(_ context.Context, id string, v int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[id] = Instance{v, time.Now().UTC()}
	return nil
}
func (s *MemoryStore) Instances(context.Context) (map[string]Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]Instance{}
	for k, v := range s.instances {
		out[k] = v
	}
	return out, nil
}

type LocalPublisher struct{ Store Store }

func (p LocalPublisher) PublishConfig(context.Context, Config) error { return nil }
func (p LocalPublisher) Current(c context.Context) (Config, error)   { return p.Store.Load(c) }

type LocalBus struct {
	Store Store
	Guard Guard
}

func (b LocalBus) Publish(c context.Context, e Event) error {
	if err := b.Store.Apply(c, e); err != nil {
		return err
	}
	if e.Kind == "completed" && e.Purpose != "request" {
		return b.Guard.Settle(c, e)
	}
	return nil
}
func (LocalBus) Close() error { return nil }

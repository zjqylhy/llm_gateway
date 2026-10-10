package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type KafkaBus struct{ Writer *kafka.Writer }

func (b KafkaBus) Publish(c context.Context, e Event) error {
	raw, err := eventBytes(e)
	if err != nil {
		return err
	}
	return b.Writer.WriteMessages(c, kafka.Message{Key: []byte(e.AppID), Value: raw})
}
func (b KafkaBus) Close() error { return b.Writer.Close() }
func Consume(c context.Context, r *kafka.Reader, s Store, g Guard) error {
	if redisGuard, ok := g.(RedisGuard); ok {
		redisGuard.Replay = true
		g = redisGuard
	}
	for {
		m, e := r.FetchMessage(c)
		if e != nil {
			return e
		}
		var event Event
		if e = json.Unmarshal(m.Value, &event); e != nil {
			return fmt.Errorf("invalid event at offset %d", m.Offset)
		}
		if e = CheckEvent(event); e != nil {
			return e
		}
		if e = s.Apply(c, event); e != nil {
			if errors.Is(e, ErrEventConflict) {
				freeze, done := context.WithTimeout(context.WithoutCancel(c), time.Second)
				if store, ok := s.(*SQLStore); ok {
					_ = store.FreezeMetering(freeze, "event_conflict")
				}
				done()
				if guard, ok := g.(RedisGuard); ok {
					freeze, done = context.WithTimeout(context.WithoutCancel(c), time.Second)
					_ = guard.Client.Set(freeze, "gw:metering:frozen", "event_conflict", 0).Err()
					done()
				}
			}
			return e
		}
		if event.Kind == "started" {
			if e = g.Reserve(c, App{ID: event.AppID}, Reservation{ID: event.AttemptID, AppID: event.AppID, Month: event.Month, Tokens: event.ReserveTokens, Micro: event.ReserveMicro}); e != nil {
				return e
			}
		}
		if event.Kind == "completed" && event.Purpose != "request" {
			if e = g.Settle(c, event); e != nil {
				return e
			}
		}
		if outbox, ok := s.(interface {
			DeliveredOutbox(context.Context, Event) error
		}); ok {
			if e = outbox.DeliveredOutbox(c, event); e != nil {
				return e
			}
		}
		if e = r.CommitMessages(c, m); e != nil {
			return e
		}
	}
}

type EtcdPublisher struct {
	Client *clientv3.Client
	Prefix string
}

func (p EtcdPublisher) PublishConfig(c context.Context, cfg Config) error {
	b, e := json.Marshal(cfg)
	if e != nil {
		return e
	}
	key := p.Prefix + "/config"
	for i := 0; i < 5; i++ {
		current, err := p.Client.Get(c, key)
		if err != nil {
			return err
		}
		revision := int64(0)
		if len(current.Kvs) > 0 {
			revision = current.Kvs[0].ModRevision
			var old Config
			if err = json.Unmarshal(current.Kvs[0].Value, &old); err != nil {
				return err
			}
			if old.Version >= cfg.Version {
				return nil
			}
		}
		result, err := p.Client.Txn(c).If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).Then(clientv3.OpPut(key, string(b))).Commit()
		if err != nil {
			return err
		}
		if result.Succeeded {
			return nil
		}
	}
	return errors.New("concurrent config publication; retry required")
}
func (p EtcdPublisher) Current(c context.Context) (Config, error) {
	resp, e := p.Client.Get(c, p.Prefix+"/config")
	if e != nil {
		return Config{}, e
	}
	if len(resp.Kvs) == 0 {
		return Config{}, errors.New("configuration not published")
	}
	var cfg Config
	e = json.Unmarshal(resp.Kvs[0].Value, &cfg)
	return cfg, e
}

type MemoryCache struct {
	mu    sync.Mutex
	items map[string]CacheItem
	bytes int
}

func NewMemoryCache() *MemoryCache { return &MemoryCache{items: map[string]CacheItem{}} }
func (c *MemoryCache) Find(_ context.Context, scope string, v []float32, threshold float64) (*CacheItem, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var best *CacheItem
	score := threshold
	for id, x := range c.items {
		if x.Expire <= time.Now().Unix() {
			c.bytes -= cacheItemSize(x)
			delete(c.items, id)
			continue
		}
		if x.Scope != scope {
			continue
		}
		s := cosine(v, x.Vector)
		if s >= score {
			copy := x
			best = &copy
			score = s
		}
	}
	return best, nil
}
func (c *MemoryCache) Put(_ context.Context, x CacheItem) error {
	size := cacheItemSize(x)
	if size > maxCacheItemBytes || x.Expire <= time.Now().Unix() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.items[x.ID]; ok {
		c.bytes -= cacheItemSize(old)
		delete(c.items, x.ID)
	}
	for len(c.items) >= 10000 || c.bytes+size > maxMemoryCacheBytes {
		for k := range c.items {
			c.bytes -= cacheItemSize(c.items[k])
			delete(c.items, k)
			break
		}
	}
	// 拷贝实际数据，避免短切片长期持有远大于记录大小的底层缓冲区。
	x.Response = bytes.Clone(x.Response)
	x.Vector = append([]float32(nil), x.Vector...)
	c.items[x.ID] = x
	c.bytes += size
	return nil
}
func cacheItemSize(x CacheItem) int {
	return len(x.ID) + len(x.Scope) + len(x.Response) + len(x.Vector)*4 + 256
}
func cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return -1
	}
	dot, aa, bb := 0.0, 0.0, 0.0
	for i, x := range a {
		y := b[i]
		dot += float64(x * y)
		aa += float64(x * x)
		bb += float64(y * y)
	}
	if aa == 0 || bb == 0 {
		return -1
	}
	return dot / math.Sqrt(aa*bb)
}

type MilvusCache struct {
	URL        string
	Token      string
	Collection string
	Client     *http.Client
	Redis      *redis.Client
}

func (c MilvusCache) call(ctx context.Context, path string, p any, out any) error {
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	req, e := http.NewRequestWithContext(ctx, "POST", c.URL+"/v2/vectordb/"+path, bytes.NewReader(b))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, e := c.Client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if e != nil {
		return e
	}
	if resp.StatusCode != 200 {
		return ErrUnavailable
	}
	var envelope struct {
		Code int `json:"code"`
	}
	if e = json.Unmarshal(raw, &envelope); e != nil || envelope.Code != 0 {
		return ErrUnavailable
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}
func (c MilvusCache) Init(ctx context.Context, dimension int) error {
	return c.call(ctx, "collections/create", map[string]any{"collectionName": c.Collection, "dimension": dimension, "metricType": "COSINE", "primaryFieldName": "id", "vectorFieldName": "vector", "idType": "VarChar", "autoId": false, "params": map[string]any{"max_length": 128, "enableDynamicField": true}}, nil)
}
func (c MilvusCache) Find(ctx context.Context, scope string, v []float32, threshold float64) (*CacheItem, error) {
	var out struct {
		Data []struct {
			ID       string  `json:"id"`
			Distance float64 `json:"distance"`
		} `json:"data"`
	}
	e := c.call(ctx, "entities/search", map[string]any{"collectionName": c.Collection, "data": [][]float32{v}, "annsField": "vector", "filter": "scope == " + strconv.Quote(scope) + " and expire > " + strconv.FormatInt(time.Now().Unix(), 10), "limit": 1, "outputFields": []string{"id"}}, &out)
	if e != nil {
		return nil, e
	}
	if len(out.Data) == 0 || out.Data[0].Distance < threshold {
		return nil, nil
	}
	b, e := c.Redis.Get(ctx, "gw:cache:"+out.Data[0].ID).Bytes()
	if e == redis.Nil {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var item CacheItem
	if e = json.Unmarshal(b, &item); e != nil {
		return nil, e
	}
	if item.Scope != scope || item.Expire <= time.Now().Unix() {
		return nil, nil
	}
	return &item, nil
}
func (c MilvusCache) Put(ctx context.Context, x CacheItem) error {
	if cacheItemSize(x) > maxCacheItemBytes {
		return nil
	}
	b, e := json.Marshal(x)
	if e != nil {
		return e
	}
	ttl := time.Until(time.Unix(x.Expire, 0))
	if ttl <= 0 {
		return nil
	}
	if e = c.Redis.Set(ctx, "gw:cache:"+x.ID, b, ttl).Err(); e != nil {
		return e
	}
	return c.call(ctx, "entities/upsert", map[string]any{"collectionName": c.Collection, "data": []map[string]any{{"id": x.ID, "vector": x.Vector, "scope": x.Scope, "expire": x.Expire}}}, nil)
}

// Redis 淘汰响应不会同步删除向量，定期删除过期实体供 Milvus 后台压缩回收。
func (c MilvusCache) Prune(ctx context.Context) error {
	return c.call(ctx, "entities/delete", map[string]any{"collectionName": c.Collection, "filter": "expire <= " + strconv.FormatInt(time.Now().Unix(), 10)}, nil)
}

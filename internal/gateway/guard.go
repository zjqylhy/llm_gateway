package gateway

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"math/big"
	"strconv"
	"sync"
	"time"
)

type bucket struct {
	tokens  float64
	at      time.Time
	expires time.Time
}
type settlement struct{ Revision, Tokens, Micro int64 }
type MemoryGuard struct {
	mu           sync.Mutex
	buckets      map[string]bucket
	counts       map[string]Counters
	reservations map[string]Reservation
	settled      map[string]settlement
}

func NewMemoryGuard() *MemoryGuard {
	return &MemoryGuard{buckets: map[string]bucket{}, counts: map[string]Counters{}, reservations: map[string]Reservation{}, settled: map[string]settlement{}}
}
func (g *MemoryGuard) Rate(c context.Context, key string, n int64, period time.Duration) error {
	return g.Take(c, key, n, 1, period)
}
func (g *MemoryGuard) Take(_ context.Context, key string, n, units int64, period time.Duration) error {
	if n <= 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	b, ok := g.buckets[key]
	if !ok {
		if len(g.buckets) >= 10000 {
			for id, old := range g.buckets {
				if now.After(old.expires) {
					delete(g.buckets, id)
				}
			}
			if len(g.buckets) >= 10000 {
				return ErrUnavailable
			}
		}
		b = bucket{tokens: float64(n), at: now}
	}
	b.tokens += now.Sub(b.at).Seconds() * float64(n) / period.Seconds()
	if b.tokens > float64(n) {
		b.tokens = float64(n)
	}
	b.at = now
	b.expires = now.Add(2 * period)
	if b.tokens < float64(units) {
		g.buckets[key] = b
		return ErrLimit
	}
	b.tokens -= float64(units)
	g.buckets[key] = b
	return nil
}
func (g *MemoryGuard) Reserve(_ context.Context, a App, r Reservation) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.reservations[r.ID]; ok {
		return nil
	}
	if _, ok := g.settled[r.ID]; ok {
		return nil
	}
	if len(g.reservations)+len(g.settled) >= maxMemoryAttempts || len(g.counts) >= maxMemoryAttempts {
		return ErrUnavailable
	}
	k := a.ID + ":" + r.Month
	c := g.counts[k]
	if a.MonthlyTokens > 0 && c.Tokens+c.ReservedTokens+r.Tokens > a.MonthlyTokens {
		return ErrLimit
	}
	if a.BudgetMicro > 0 && a.BudgetAction == "reject" && c.Micro+c.ReservedMicro+r.Micro > a.BudgetMicro {
		return ErrLimit
	}
	c.ReservedTokens += r.Tokens
	c.ReservedMicro += r.Micro
	g.counts[k] = c
	g.reservations[r.ID] = r
	return nil
}
func (g *MemoryGuard) Settle(_ context.Context, e Event) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	old, ok := g.settled[e.AttemptID]
	if ok && old.Revision >= e.Revision {
		return nil
	}
	if _, reserved := g.reservations[e.AttemptID]; !ok && !reserved && len(g.reservations)+len(g.settled) >= maxMemoryAttempts {
		return ErrUnavailable
	}
	k := e.AppID + ":" + e.Month
	c := g.counts[k]
	if !ok {
		if r, exists := g.reservations[e.AttemptID]; exists {
			c.ReservedTokens -= r.Tokens
			c.ReservedMicro -= r.Micro
		}
	}
	c.Tokens += e.Usage.Total - old.Tokens
	c.Micro += e.CostMicro - old.Micro
	g.counts[k] = c
	delete(g.reservations, e.AttemptID)
	// 去重只需版本及已入账金额，避免重复持有完整模型配置和采样正文。
	g.settled[e.AttemptID] = settlement{e.Revision, e.Usage.Total, e.CostMicro}
	return nil
}
func (g *MemoryGuard) Counters(_ context.Context, a, m string) (Counters, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.counts[a+":"+m], nil
}

type RedisGuard struct {
	Client        *redis.Client
	RequireReady  bool
	EnforceMemory bool
	Replay        bool
}

const rateLua = `local tm=redis.call('TIME');local now=tm[1]*1000+math.floor(tm[2]/1000)
local old=redis.call('HMGET',KEYS[1],'tokens','at');local n=tonumber(ARGV[1]);local tokens=tonumber(old[1]) or n;local at=tonumber(old[2]) or now
tokens=math.min(n,tokens+(now-at)*n/tonumber(ARGV[2]));local ok=0;local units=tonumber(ARGV[3]);if tokens>=units then tokens=tokens-units;ok=1 end
redis.call('HSET',KEYS[1],'tokens',tokens,'at',now);redis.call('PEXPIRE',KEYS[1],math.max(1000,tonumber(ARGV[2])*2));return ok`

func (g RedisGuard) Rate(c context.Context, k string, n int64, p time.Duration) error {
	return g.Take(c, k, n, 1, p)
}
func (g RedisGuard) Take(c context.Context, k string, n, units int64, p time.Duration) error {
	if n <= 0 {
		return nil
	}
	c, cancel := context.WithTimeout(c, time.Second)
	defer cancel()
	v, e := g.Client.Eval(c, rateLua, []string{"gw:rate:" + k}, n, p.Milliseconds(), units).Int()
	if e != nil {
		return ErrUnavailable
	}
	if v == 0 {
		return ErrLimit
	}
	return nil
}
func quotaKeys(a, m, id string) []string {
	tag := "{app:" + a + "}"
	return []string{"gw:" + tag + ":" + m, "gw:" + tag + ":attempt:" + id}
}

const reserveLua = `if ARGV[6]=='true' and redis.call('GET','gw:quota:state')~='ready' then return -1 end
if ARGV[8]=='true' then
 local tm=redis.call('TIME');local now=tm[1]*1000+math.floor(tm[2]/1000)
 if ARGV[9]=='' or tonumber(ARGV[10])<=now or redis.call('GET','gw:metering:generation')~=ARGV[9] then return -1 end
 local raw=redis.call('GET','gw:metering:permit');if not raw then return -1 end
 local ok,p=pcall(cjson.decode,raw);if not ok or p.generation~=ARGV[9] or p.expires<tonumber(ARGV[10]) or p.expires<=now then return -1 end
 local app=redis.call('HMGET',ARGV[11],'state','generation');if app[1]~='healthy' or app[2]~=ARGV[12] then return -1 end
end
if redis.call('HEXISTS',KEYS[2],'reserved')==1 or redis.call('HEXISTS',KEYS[2],'revision')==1 then return 1 end
if ARGV[8]=='true' and redis.call('EXISTS','gw:metering:frozen')==1 then return -1 end
if ARGV[7]=='true' then
 local info=redis.call('INFO','memory');local max=tonumber(string.match(info,'\r\nmaxmemory:(%d+)'));local used=tonumber(string.match(info,'\r\nused_memory:(%d+)'));local excluded=tonumber(string.match(info,'\r\nmem_not_counted_for_evict:(%d+)')) or 0
 local policy=string.match(info,'\r\nmaxmemory_policy:([^\r\n]+)')
 if not max or max<=0 or not used or policy~='noeviction' or math.max(0,used-excluded)>=max*0.9 then return -1 end
end
local v=redis.call('HMGET',KEYS[1],'tokens','micro','rt','rm');local t=tonumber(v[1]) or 0;local m=tonumber(v[2]) or 0;local rt=tonumber(v[3]) or 0;local rm=tonumber(v[4]) or 0
local limit=tonumber(ARGV[3]);local budget=tonumber(ARGV[4]);if limit>0 and t+rt+tonumber(ARGV[1])>limit then return 0 end
if budget>0 and ARGV[5]=='reject' and m+rm+tonumber(ARGV[2])>budget then return 0 end
redis.call('HINCRBY',KEYS[1],'rt',ARGV[1]);redis.call('HINCRBY',KEYS[1],'rm',ARGV[2]);redis.call('HSET',KEYS[2],'reserved',1,'rt',ARGV[1],'rm',ARGV[2]);return 1`

func (g RedisGuard) Reserve(c context.Context, a App, r Reservation) error {
	// 连接池、DNS 和网络读写共享一次操作的预算，依赖中断不能拖满整个推理请求。
	c, cancel := context.WithTimeout(c, time.Second)
	defer cancel()
	// 90% 水位拒绝新增预占，余量留给已发生费用的结算；INFO/策略无法核验时失败关闭。
	proof, _ := c.Value(admissionProofKey{}).(admissionProof)
	v, e := g.Client.Eval(c, reserveLua, quotaKeys(a.ID, r.Month, r.ID), r.Tokens, r.Micro, a.MonthlyTokens, a.BudgetMicro, a.BudgetAction, strconv.FormatBool(g.RequireReady), strconv.FormatBool(g.EnforceMemory), strconv.FormatBool(g.RequireReady && !g.Replay), proof.Permit.Generation, proof.Permit.Expires, appControlKey(a.ID), proof.AppGeneration).Int()
	if e != nil {
		return ErrUnavailable
	}
	if v == -1 {
		return ErrUnavailable
	}
	if v == 0 {
		return ErrLimit
	}
	return nil
}

const settleLua = `if ARGV[4]=='true' and redis.call('GET','gw:quota:state')~='ready' then return -1 end
local old=redis.call('HMGET',KEYS[2],'revision','tokens','micro','rt','rm','hash');local rev=tonumber(old[1]) or 0;if rev==tonumber(ARGV[1]) and old[6] and old[6]~=ARGV[5] then return -1 end;if rev>=tonumber(ARGV[1]) then return 1 end
if rev==0 then redis.call('HINCRBY',KEYS[1],'rt',-(tonumber(old[4]) or 0));redis.call('HINCRBY',KEYS[1],'rm',-(tonumber(old[5]) or 0)) end
redis.call('HINCRBY',KEYS[1],'tokens',tonumber(ARGV[2])-(tonumber(old[2]) or 0));redis.call('HINCRBY',KEYS[1],'micro',tonumber(ARGV[3])-(tonumber(old[3]) or 0));redis.call('HSET',KEYS[2],'revision',ARGV[1],'tokens',ARGV[2],'micro',ARGV[3],'hash',ARGV[5]);return 1`

func (g RedisGuard) Settle(c context.Context, e Event) error {
	c, cancel := context.WithTimeout(c, time.Second)
	defer cancel()
	if e.Revision < 2 {
		return errors.New("completed event requires revision >= 2")
	}
	v, err := g.Client.Eval(c, settleLua, quotaKeys(e.AppID, e.Month, e.AttemptID), e.Revision, e.Usage.Total, e.CostMicro, strconv.FormatBool(g.RequireReady), eventDigest(e)).Int()
	if err != nil || v == -1 {
		return ErrUnavailable
	}
	return nil
}
func (g RedisGuard) Counters(c context.Context, a, m string) (Counters, error) {
	c, cancel := context.WithTimeout(c, time.Second)
	defer cancel()
	v, e := g.Client.HMGet(c, quotaKeys(a, m, "")[0], "tokens", "micro", "rt", "rm").Result()
	if e != nil {
		return Counters{}, e
	}
	out := make([]int64, 4)
	for i, x := range v {
		if x != nil {
			z, ok := new(big.Int).SetString(x.(string), 10)
			if !ok || !z.IsInt64() {
				return Counters{}, ErrUnavailable
			}
			if ok {
				out[i] = z.Int64()
			}
		}
	}
	return Counters{out[0], out[1], out[2], out[3]}, nil
}
func Cost(m Model, u Usage) int64 {
	cached := u.Cached
	if cached > u.Input {
		cached = u.Input
	}
	if cached < 0 {
		cached = 0
	}
	sum := new(big.Int)
	add := func(n, p int64) { sum.Add(sum, new(big.Int).Mul(big.NewInt(n), big.NewInt(p))) }
	add(u.Input-cached, m.InputMicro)
	add(cached, m.CachedInputMicro)
	add(u.Output, m.OutputMicro)
	fx := m.FXPPM
	if fx == 0 {
		fx = 1000000
	}
	sum.Mul(sum, big.NewInt(fx))
	div := big.NewInt(1000000000000)
	sum.Add(sum, new(big.Int).Sub(div, big.NewInt(1)))
	sum.Div(sum, div)
	if !sum.IsInt64() {
		return 9223372036854775807
	}
	return sum.Int64()
}

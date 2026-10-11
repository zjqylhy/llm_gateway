package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type snapshot struct {
	cfg  Config
	keys map[string]Key
}

type requestMeteringKey struct{}
type requestMetering struct {
	Paid     bool
	Rejected bool
}
type Engine struct {
	Metering      *Runtime
	Store         Store
	Bus           Bus
	Guard         Guard
	Cache         Cache
	Publisher     Publisher
	Vault         *Vault
	Client        *http.Client
	Dev           bool
	InstanceID    string
	state         atomic.Pointer[snapshot]
	refreshed     atomic.Int64
	breakerMu     sync.Mutex
	breakers      map[string]breaker
	latencies     map[string]float64
	Metrics       *Metrics
	MaxConcurrent int
	admission     admission
}
type breaker struct {
	Failures int
	Until    time.Time
	Probe    bool
}

func (e *Engine) Refresh(c context.Context) error {
	cfg, err := e.Publisher.Current(c)
	if err != nil {
		return err
	}
	keys := map[string]Key{}
	for _, k := range cfg.Keys {
		keys[k.Hash] = k
	}
	e.state.Store(&snapshot{cfg, keys})
	e.refreshed.Store(time.Now().UnixMilli())
	e.Metrics.Config.Set(float64(cfg.Version))
	return e.Store.Heartbeat(c, e.InstanceID, cfg.Version)
}
func (e *Engine) RunRefresh(c context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-c.Done():
			return
		case <-tick.C:
			ctx, cancel := context.WithTimeout(c, 900*time.Millisecond)
			_ = e.Refresh(ctx)
			cancel()
		}
	}
}
func (e *Engine) authenticate(r *http.Request) (App, Config, bool) {
	s := e.state.Load()
	if s == nil || time.Now().UnixMilli()-e.refreshed.Load() > 3000 {
		return App{}, Config{}, false
	}
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return App{}, Config{}, false
	}
	k, ok := s.keys[Hash(strings.TrimPrefix(h, "Bearer "))]
	if !ok || k.State == "revoked" || (!k.Expires.IsZero() && time.Now().After(k.Expires)) {
		return App{}, Config{}, false
	}
	a, ok := s.cfg.Apps[k.AppID]
	return a, s.cfg, ok && a.Active
}
func APIError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	if status == 429 {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": message, "type": code, "param": nil, "code": code}})
}
func JSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func (e *Engine) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { JSON(w, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if e.state.Load() == nil || time.Now().UnixMilli()-e.refreshed.Load() > 3000 {
			APIError(w, 503, "not_ready", "configuration unavailable")
			return
		}
		if bus, ok := e.Bus.(*DurableBus); ok {
			ctx, done := context.WithTimeout(r.Context(), time.Second)
			err := bus.Ready(ctx)
			done()
			if err != nil {
				APIError(w, 503, "not_ready", "metering unavailable")
				return
			}
		}
		JSON(w, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, r *http.Request) {
		a, c, ok := e.authenticate(r)
		if !ok {
			APIError(w, 401, "invalid_api_key", "authentication failed")
			return
		}
		models := []map[string]any{}
		for alias := range c.Routes {
			if Contains(a.Models, alias) {
				models = append(models, map[string]any{"id": alias, "object": "model", "owned_by": "gateway"})
			}
		}
		JSON(w, map[string]any{"object": "list", "data": models})
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) { e.handle(w, r, "chat") })
	mux.HandleFunc("POST /v1/embeddings", func(w http.ResponseWriter, r *http.Request) { e.handle(w, r, "embedding") })
	return protectedHandler(mux, &e.admission, e.MaxConcurrent, defaultGatewayConcurrency, "/v1/", "gateway", e.Metrics)
}
func (e *Engine) handle(w http.ResponseWriter, r *http.Request, kind string) {
	start := time.Now()
	id := ID("req_")
	w.Header().Set("X-Request-Id", id)
	a, cfg, ok := e.authenticate(r)
	if !ok {
		APIError(w, 401, "invalid_api_key", "authentication failed")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		APIError(w, 413, "request_too_large", "request exceeds 1 MiB")
		return
	}
	req, err := DecodeRequest(raw, kind)
	if err != nil {
		APIError(w, 400, "invalid_request_error", err.Error())
		return
	}
	if !Contains(a.Models, req.Alias) {
		APIError(w, 403, "model_not_allowed", "model is not allowed for this application")
		return
	}
	route, ok := cfg.Routes[req.Alias]
	if !ok || route.Kind != kind {
		APIError(w, 404, "model_not_found", "model alias unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	if req.Stream {
		cancel()
		ctx, cancel = context.WithTimeout(r.Context(), 120*time.Second)
	}
	defer cancel()
	e.Metrics.Active.Inc()
	defer e.Metrics.Active.Dec()
	ctx, span := otel.Tracer("gateway").Start(ctx, "gateway.request")
	financial := &requestMetering{}
	ctx = context.WithValue(ctx, requestMeteringKey{}, financial)
	defer span.End()
	span.SetAttributes(attribute.String("request.id", id), attribute.String("app.id", a.ID))
	finalStatus := "failed"
	cached := false
	defer func() {
		e.Metrics.Request(a.ID, req.Alias, finalStatus, cached, time.Since(start))
		// 门禁拒绝尚未产生费用，避免重试洪峰继续挤压已经滞后的计量队列。
		if e.Metering != nil && financial.Rejected && !financial.Paid {
			return
		}
		event := Event{ID: id + ":result", RequestID: id, AttemptID: id, Revision: 2, Kind: "completed", Purpose: "request", AppID: a.ID, Department: a.Department, Alias: req.Alias, At: start.UTC(), Month: month(start), Status: finalStatus, Cached: cached, DurationMS: time.Since(start).Milliseconds()}
		pc, done := context.WithTimeout(context.WithoutCancel(ctx), 9*time.Second)
		defer done()
		if err := e.Bus.Publish(pc, event); err != nil {
			e.Metrics.EventError()
		}
	}()
	if err = e.Guard.Rate(ctx, "app:"+a.ID, a.QPS, time.Second); err != nil {
		finalStatus = rejectGuard(w, err)
		return
	}
	sensitive, blocked := Safety(cfg.Rules, req.SafetyText)
	if blocked {
		finalStatus = "blocked"
		APIError(w, 400, "safety_blocked", "request matched an application safety rule")
		return
	}
	if sensitive || r.Header.Get("X-Gateway-Realtime") == "true" || r.Header.Get("X-Gateway-Cache") == "bypass" {
		req.Cacheable = false
	}
	candidates := e.choose(a, cfg, route, req, id)
	if len(candidates) == 0 {
		APIError(w, 400, "unsupported_capability", "no configured model supports this request")
		return
	}
	var scope string
	var vector []float32
	if a.Cache && req.Cacheable && a.EmbeddingModel != "" {
		m, exists := cfg.Models[a.EmbeddingModel]
		if exists && Contains(m.Capabilities, "embedding") {
			eraw, _ := json.Marshal(map[string]any{"model": "internal", "input": req.Text})
			er, _ := DecodeRequest(eraw, "embedding")
			emb, _, erErr := e.attempt(ctx, cfg, a, m, er, id, "cache_embedding", false, nil)
			if erErr == nil {
				var parsed struct {
					Data []struct {
						Embedding []float32 `json:"embedding"`
					} `json:"data"`
				}
				if json.Unmarshal(emb, &parsed) == nil && len(parsed.Data) == 1 {
					vector = parsed.Data[0].Embedding
				}
			} else if errors.Is(erErr, ErrLimit) || errors.Is(erErr, ErrUnavailable) {
				finalStatus = rejectGuard(w, erErr)
				return
			}
			scope = CacheScope(a, cfg, req, candidates[0].model)
			if len(vector) > 0 {
				cc, done := context.WithTimeout(ctx, 150*time.Millisecond)
				hit, ce := e.Cache.Find(cc, scope, vector, a.Similarity)
				done()
				e.Metrics.Cache(ce, hit != nil)
				if ce == nil && hit != nil {
					cached = true
					finalStatus = "success"
					w.Header().Set("X-Gateway-Cache", "hit")
					e.replyCached(w, req, hit.Response, id)
					return
				}
			}
		}
	}
	var result []byte
	var last error
	attempts := 0
	for _, candidate := range candidates {
		if attempts >= 3 || ctx.Err() != nil {
			break
		}
		if !e.allow(candidate.model.ID) {
			continue
		}
		attempts++
		w.Header().Set("X-Gateway-Model", candidate.model.ID)
		result, committed, ae := e.attempt(ctx, cfg, a, candidate.model, req, id, "inference", candidate.gray, w)
		e.result(candidate.model.ID, ae)
		if ae == nil {
			finalStatus = "success"
			if attempts > 1 {
				finalStatus = "fallback_success"
			}
			if !req.Stream {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(result)
			}
			if a.Cache && req.Cacheable && len(result) > 0 && len(vector) > 0 && scope == CacheScope(a, cfg, req, candidate.model) {
				ttl := a.CacheTTL
				if ttl <= 0 {
					ttl = 300
				}
				cc, done := context.WithTimeout(context.WithoutCancel(ctx), 150*time.Millisecond)
				err = e.Cache.Put(cc, CacheItem{ID: ID("cache_"), Scope: scope, Vector: vector, Response: result, Expire: time.Now().Add(time.Duration(ttl) * time.Second).Unix()})
				done()
				if err != nil {
					e.Metrics.Cache(err, false)
				}
			}
			return
		}
		last = ae
		if committed {
			finalStatus = "stream_error"
			return
		}
		if ae == ErrProviderLimit {
			continue
		}
		if errors.Is(ae, ErrLimit) || errors.Is(ae, ErrUnavailable) {
			finalStatus = rejectGuard(w, ae)
			return
		}
		if !Retriable(ae) {
			break
		}
	}
	_ = result
	if last == ErrProviderLimit {
		finalStatus = rejectGuard(w, last)
		return
	}
	var up *UpstreamError
	if errors.As(last, &up) && up.Status == 400 {
		APIError(w, 400, "upstream_invalid_request", "upstream rejected parameters")
		return
	}
	APIError(w, 503, "upstream_unavailable", "all eligible upstream attempts failed")
}
func rejectGuard(w http.ResponseWriter, e error) string {
	if errors.Is(e, ErrMeteringCompletion) {
		APIError(w, 503, "metering_unavailable", "model call may have incurred usage; verify request_id before retrying")
		return "failed"
	}
	if errors.Is(e, ErrLimit) {
		APIError(w, 429, "insufficient_quota", "rate, token quota or budget exceeded")
		return "rate_limited"
	}
	APIError(w, 503, "governance_unavailable", "quota or metering service unavailable")
	return "failed"
}

type selection struct {
	model Model
	gray  bool
}

func (e *Engine) choose(a App, c Config, route Route, r Request, id string) []selection {
	out := []selection{}
	for _, x := range route.Candidates {
		m, ok := c.Models[x.ModelID]
		if !ok || !supports(m, r) {
			continue
		}
		out = append(out, selection{m, false})
	}
	if route.Strategy == "cost" {
		sort.SliceStable(out, func(i, j int) bool {
			return Cost(out[i].model, Usage{Input: r.InputEstimate, Output: r.MaxOutput}) < Cost(out[j].model, Usage{Input: r.InputEstimate, Output: r.MaxOutput})
		})
	}
	if route.Strategy == "weight" && len(out) > 1 {
		total := 0
		weights := map[string]int{}
		for _, x := range route.Candidates {
			weights[x.ModelID] = x.Weight
		}
		for _, x := range out {
			total += weights[x.model.ID]
		}
		if total > 0 {
			n := int(hashNumber(id) % uint64(total))
			chosen := 0
			for i, x := range out {
				n -= weights[x.model.ID]
				if n < 0 {
					chosen = i
					break
				}
			}
			out[0], out[chosen] = out[chosen], out[0]
		}
	}
	if route.Strategy == "latency" {
		e.breakerMu.Lock()
		sort.SliceStable(out, func(i, j int) bool {
			a, b := e.latencies[out[i].model.ID], e.latencies[out[j].model.ID]
			if a == 0 || b == 0 {
				return false
			}
			return a < b
		})
		e.breakerMu.Unlock()
	}
	if route.Gray.State == "running" && int(hashNumber(a.ID+":"+r.User())%100) < route.Gray.Percent {
		m, ok := c.Models[route.Gray.Candidate]
		if ok && supports(m, r) {
			next := []selection{{m, true}}
			for _, x := range out {
				if x.model.ID != m.ID {
					next = append(next, x)
				}
			}
			out = next
		}
	}
	return out
}
func (r Request) User() string { var s string; _ = json.Unmarshal(r.Raw["user"], &s); return s }
func supports(m Model, r Request) bool {
	if !Contains(m.Capabilities, r.Kind) || r.InputEstimate+r.MaxOutput > m.Context || r.MaxOutput > m.MaxOutput {
		return false
	}
	for _, x := range r.Capabilities {
		if !Contains(m.Capabilities, x) {
			return false
		}
	}
	if r.Kind == "embedding" {
		if b := r.Raw["dimensions"]; len(b) > 0 {
			var n int
			if json.Unmarshal(b, &n) != nil || n != m.Dimensions {
				return false
			}
		}
	}
	return true
}
func hashNumber(s string) uint64 {
	var n uint64
	for _, b := range []byte(Hash(s)[:16]) {
		n = n*31 + uint64(b)
	}
	return n
}
func month(t time.Time) string {
	return t.In(time.FixedZone("Asia/Shanghai", 8*3600)).Format("2006-01")
}
func (e *Engine) allow(id string) bool {
	e.breakerMu.Lock()
	defer e.breakerMu.Unlock()
	if e.breakers == nil {
		e.breakers = map[string]breaker{}
	}
	b := e.breakers[id]
	if time.Now().Before(b.Until) {
		return false
	}
	if b.Failures >= 3 {
		if b.Probe {
			return false
		}
		b.Probe = true
		e.breakers[id] = b
	}
	return true
}
func (e *Engine) result(id string, err error) {
	e.breakerMu.Lock()
	defer e.breakerMu.Unlock()
	b := e.breakers[id]
	b.Probe = false
	if err == nil {
		b = breaker{}
	} else if Retriable(err) {
		b.Failures++
		if b.Failures >= 3 {
			b.Until = time.Now().Add(15 * time.Second)
		}
	}
	e.breakers[id] = b
}
func (e *Engine) attempt(ctx context.Context, cfg Config, a App, m Model, r Request, requestID, purpose string, gray bool, w http.ResponseWriter) (body []byte, committed bool, err error) {
	financial, _ := ctx.Value(requestMeteringKey{}).(*requestMetering)
	defer func() {
		if financial != nil && errors.Is(err, ErrUnavailable) {
			financial.Rejected = true
		}
	}()
	ctx, span := otel.Tracer("gateway").Start(ctx, "provider.attempt")
	defer span.End()
	span.SetAttributes(attribute.String("gen_ai.provider.name", cfg.Providers[m.ProviderID].Kind), attribute.String("gen_ai.request.model", m.Upstream))
	started := time.Now()
	id := ID("att_")
	event := Event{ID: id + ":1", RequestID: requestID, AttemptID: id, Revision: 1, Kind: "started", Purpose: purpose, AppID: a.ID, Department: a.Department, Alias: r.Alias, Model: m, At: started.UTC(), Month: month(started), Status: "started", Quality: "pending", Gray: gray, ReserveTokens: r.InputEstimate + r.MaxOutput, ReserveMicro: Cost(m, Usage{Input: r.InputEstimate, Output: r.MaxOutput})}
	if gray {
		event.GrayRun = cfg.Routes[r.Alias].Gray.RunID
	}
	if _, ok := e.Store.(*SQLStore); ok {
		if e.Metering == nil {
			err = ErrUnavailable
			return
		}
		var finish func()
		raw, encodeErr := eventBytes(event)
		if encodeErr != nil {
			err = ErrUnavailable
			return
		}
		limit := e.Bus.(*DurableBus).AppBytes
		if a.OutboxBytes > 0 {
			limit = a.OutboxBytes
		}
		ctx, finish, err = e.Metering.beginAdmission(ctx, a.ID, id, int64(len(raw))+maxEventBytes, limit)
		if err != nil {
			err = ErrUnavailable
			return
		}
		defer finish()
	}
	limitID := Hash(m.ProviderID + ":" + m.Upstream)
	if err = e.Guard.Rate(ctx, "model:"+limitID, m.RPM, time.Minute); err != nil {
		if errors.Is(err, ErrLimit) {
			err = ErrProviderLimit
		}
		return
	}
	if err = e.Guard.Take(ctx, "tpm:"+limitID, m.TPM, event.ReserveTokens, time.Minute); err != nil {
		if errors.Is(err, ErrLimit) {
			err = ErrProviderLimit
		}
		return
	}
	res := Reservation{id, a.ID, event.Month, event.ReserveTokens, event.ReserveMicro}
	if err = e.Guard.Reserve(ctx, a, res); err != nil {
		return
	}
	if err = e.Bus.Publish(ctx, event); err != nil {
		release := event
		release.ID = id + ":2"
		release.Revision = 2
		release.Kind = "completed"
		release.Status = "cancelled"
		release.Quality = "reported"
		release.Usage = Usage{}
		release.CostMicro = 0
		// 此路径确定没有调用厂商；持久化取消可防止迟到 started 重建预占。
		cleanup, done := context.WithTimeout(context.WithoutCancel(ctx), 9*time.Second)
		_ = e.Bus.Publish(cleanup, release)
		done()
		settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		_ = e.Guard.Settle(settle, release)
		cancel()
		err = ErrUnavailable
		return
	}
	upstreamStart := time.Now()
	if financial != nil {
		financial.Paid = true
	}
	upstreamDuration := time.Duration(0)
	defer func() {
		e.Metrics.Overhead.WithLabelValues(purpose).Observe((time.Since(started) - upstreamDuration).Seconds())
	}()
	event.Usage = Usage{Input: r.InputEstimate, Total: r.InputEstimate}
	event.Quality = "estimated"
	event.Status = "failed"
	completed := false
	complete := func() {
		if completed {
			return
		}
		completed = true
		upstreamDuration = time.Since(upstreamStart)
		event.ID = id + ":2"
		event.Revision = 2
		event.Kind = "completed"
		event.DurationMS = time.Since(started).Milliseconds()
		event.GatewayMS = upstreamStart.Sub(started).Milliseconds()
		event.CostMicro = Cost(m, event.Usage)
		// 正文默认不留存；开启采样后先脱敏，再按 attempt 加密并限制大小。
		if purpose == "inference" && a.LogSample > 0 && float64(hashNumber(id)%1000000)/1000000 < a.LogSample {
			input, _ := json.Marshal(r.Raw)
			payload, _ := json.Marshal(map[string]string{"input": sampledText(string(input)), "output": sampledText(string(body))})
			event.PayloadCipher = e.Vault.Encrypt(id, string(payload))
		}
		if err == nil {
			event.Status = "success"
		} else {
			event.ErrorCode = "upstream_error"
		}
		pc, done := context.WithTimeout(context.WithoutCancel(ctx), 9*time.Second)
		defer done()
		if pe := e.Bus.Publish(pc, event); pe != nil {
			e.Metrics.EventError()
			err = ErrMeteringCompletion
		}
		e.Metrics.Attempt(event)
		e.breakerMu.Lock()
		if e.latencies == nil {
			e.latencies = map[string]float64{}
		}
		old := e.latencies[m.ID]
		if old == 0 {
			old = float64(event.DurationMS)
		}
		e.latencies[m.ID] = old*.8 + float64(event.DurationMS)*.2
		e.breakerMu.Unlock()
	}
	defer complete()
	p, ok := cfg.Providers[m.ProviderID]
	if !ok {
		err = errors.New("provider missing")
		return
	}
	token, de := e.Vault.Decrypt(p.ID, p.Secret)
	if de != nil {
		err = de
		return
	}
	resp, openErr := OpenUpstream(ctx, e.Client, p, m, r, token)
	if openErr != nil {
		err = openErr
		var ue *UpstreamError
		if errors.As(err, &ue) && ue.Status < 500 {
			event.Usage = Usage{}
			event.Quality = "reported"
		}
		return
	}
	defer resp.Body.Close()
	if !r.Stream {
		body, err = io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
		if err != nil {
			return
		}
		if len(body) > 8<<20 {
			err = errors.New("upstream response too large")
			return
		}
		if !json.Valid(body) {
			err = errors.New("invalid upstream JSON")
			return
		}
		var response struct {
			Error   json.RawMessage   `json:"error"`
			Choices []json.RawMessage `json:"choices"`
			Data    []json.RawMessage `json:"data"`
		}
		if json.Unmarshal(body, &response) != nil || (len(response.Error) > 0 && string(response.Error) != "null") || (r.Kind == "chat" && len(response.Choices) == 0) || (r.Kind == "embedding" && len(response.Data) == 0) {
			err = errors.New("invalid upstream response envelope")
			return
		}
		if u, exists := ExtractUsage(body); exists {
			event.Usage = u
			event.Quality = "reported"
		} else {
			var output struct {
				Choices []struct {
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				} `json:"choices"`
			}
			_ = json.Unmarshal(body, &output)
			for _, x := range output.Choices {
				event.Usage.Output += int64(utf8.RuneCountInString(x.Message.Content))
			}
			event.Usage.Total = event.Usage.Input + event.Usage.Output
		}
		return
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		err = errors.New("invalid SSE content type")
		return
	}
	reader := NewSSEReader(resp.Body)
	var text strings.Builder
	finish := ""
	doneSeen := false
	for {
		chunk, re := reader.Next()
		if re != nil {
			if re == io.EOF && finish != "" {
				break
			}
			err = re
			break
		}
		if string(chunk) == "[DONE]" {
			doneSeen = true
			break
		}
		var parsed struct {
			Choices []struct {
				Delta struct {
					Content string          `json:"content"`
					Tools   json.RawMessage `json:"tool_calls"`
				} `json:"delta"`
				Finish string `json:"finish_reason"`
			} `json:"choices"`
			Error json.RawMessage `json:"error"`
		}
		if json.Unmarshal(chunk, &parsed) != nil || (len(parsed.Error) > 0 && string(parsed.Error) != "null") {
			err = errors.New("invalid stream event")
			break
		}
		if u, ok := ExtractUsage(chunk); ok {
			event.Usage = u
			event.Quality = "reported"
			if !r.IncludeUsage && len(parsed.Choices) == 0 {
				continue
			}
		}
		for _, x := range parsed.Choices {
			if text.Len() < 65536 {
				text.WriteString(x.Delta.Content)
			}
			if x.Finish != "" {
				finish = x.Finish
			}
		}
		if !committed {
			event.FirstMS = time.Since(started).Milliseconds()
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			committed = true
		}
		if _, we := fmt.Fprintf(w, "data: %s\n\n", chunk); we != nil {
			err = we
			break
		}
		if fe := http.NewResponseController(w).Flush(); fe != nil {
			err = fe
			break
		}
	}
	if !doneSeen && finish == "" && err == nil {
		err = errors.New("incomplete stream")
	}
	if finish == "stop" && text.Len() < 65536 {
		body, _ = json.Marshal(map[string]any{"id": ID("chatcmpl_"), "object": "chat.completion", "created": time.Now().Unix(), "model": r.Alias, "choices": []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": text.String()}, "finish_reason": "stop"}}, "usage": event.Usage})
	}
	if event.Quality != "reported" {
		event.Usage.Output = int64(utf8.RuneCountInString(text.String()))
		event.Usage.Total = event.Usage.Input + event.Usage.Output
	}
	if err != nil {
		if committed {
			_, _ = fmt.Fprintf(w, "data: {\"error\":{\"code\":\"stream_interrupted\",\"message\":\"upstream stream interrupted\"}}\n\n")
			_ = http.NewResponseController(w).Flush()
		}
		return
	}
	if !committed {
		err = errors.New("empty stream")
		return
	}
	// 成功终止标记之前必须确认计量事件已持久化。
	complete()
	if err != nil {
		_, _ = fmt.Fprint(w, "data: {\"error\":{\"code\":\"metering_unavailable\",\"message\":\"completion publication not acknowledged; model usage may have occurred\"}}\n\n")
		_ = http.NewResponseController(w).Flush()
		return
	}
	_, err = fmt.Fprint(w, "data: [DONE]\n\n")
	_ = http.NewResponseController(w).Flush()
	return
}
func CacheScope(a App, cfg Config, r Request, m Model) string {
	params := map[string]json.RawMessage{}
	for k, v := range r.Raw {
		if k != "messages" && k != "stream" && k != "stream_options" && k != "user" {
			params[k] = v
		}
	}
	b, _ := json.Marshal(params)
	return Hash(fmt.Sprintf("%s:%d:%d:%s:%s:%s:%s:%s", a.ID, a.CacheEpoch, cfg.Version, m.ID, m.Version, a.EmbeddingModel, r.System, b))
}
func (e *Engine) replyCached(w http.ResponseWriter, r Request, b []byte, id string) {
	if !r.Stream {
		var cached map[string]any
		if json.Unmarshal(b, &cached) != nil {
			APIError(w, 503, "cache_invalid", "cached response unavailable")
			return
		}
		cached["id"], cached["created"], cached["model"], cached["usage"] = id, time.Now().Unix(), r.Alias, Usage{}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cached)
		return
	}
	var response struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	_ = json.Unmarshal(b, &response)
	w.Header().Set("Content-Type", "text/event-stream")
	content := ""
	if len(response.Choices) > 0 {
		content = response.Choices[0].Message.Content
	}
	chunk := map[string]any{"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": r.Alias, "choices": []any{map[string]any{"index": 0, "delta": map[string]string{"role": "assistant", "content": content}, "finish_reason": nil}}}
	raw, _ := json.Marshal(chunk)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", raw)
	chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}
	raw, _ = json.Marshal(chunk)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", raw)
	if r.IncludeUsage {
		chunk["choices"] = []any{}
		chunk["usage"] = Usage{}
		raw, _ = json.Marshal(chunk)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", raw)
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	_ = http.NewResponseController(w).Flush()
}

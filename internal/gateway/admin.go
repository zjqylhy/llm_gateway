package gateway

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type session struct {
	User    string
	Hash    string
	CSRF    string
	Expires time.Time
}
type Admin struct {
	Engine        *Engine
	Secure        bool
	Origin        string
	mu            sync.Mutex
	sessions      map[string]session
	Assets        http.Handler
	Sessions      SessionStore
	MaxConcurrent int
	admission     admission
	Grafana       *GrafanaProxy
}

func NewAdmin(e *Engine, secure bool, origin string) *Admin {
	return &Admin{Engine: e, Secure: secure, Origin: origin, sessions: map[string]session{}}
}
func (a *Admin) originOK(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	if a.Origin != "" {
		return origin == a.Origin
	}
	u, e := url.Parse(origin)
	return e == nil && u.Host == r.Host && (u.Scheme == "http" || u.Scheme == "https")
}
func (a *Admin) identity(r *http.Request) (User, Config, session, bool) {
	cookie, e := r.Cookie("gw_session")
	if e != nil {
		return User{}, Config{}, session{}, false
	}
	a.mu.Lock()
	s, ok := a.sessions[Hash(cookie.Value)]
	if ok && time.Now().After(s.Expires) {
		delete(a.sessions, Hash(cookie.Value))
		ok = false
	}
	a.mu.Unlock()
	if a.Sessions != nil {
		var err error
		s, err = a.Sessions.Get(r.Context(), Hash(cookie.Value))
		ok = err == nil && time.Now().Before(s.Expires)
	}
	if !ok {
		return User{}, Config{}, session{}, false
	}
	cfg, e := a.Engine.Store.Load(r.Context())
	if e != nil {
		return User{}, Config{}, session{}, false
	}
	u, ok := cfg.Users[s.User]
	return u, cfg, s, ok && Equal(s.Hash, u.PasswordHash)
}
func (a *Admin) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/login", a.login)
	mux.HandleFunc("GET /admin/session", a.sessionInfo)
	mux.HandleFunc("POST /admin/logout", a.logout)
	mux.HandleFunc("GET /admin/config", a.config)
	mux.HandleFunc("GET /admin/apps", a.apps)
	mux.HandleFunc("POST /admin/apps", a.saveApp)
	mux.HandleFunc("PUT /admin/apps/{id}", a.saveApp)
	mux.HandleFunc("POST /admin/apps/{id}/keys", a.issueKey)
	mux.HandleFunc("POST /admin/keys/{id}/rotate", a.rotateKey)
	mux.HandleFunc("POST /admin/keys/{id}/revoke", a.revokeKey)
	mux.HandleFunc("POST /admin/apps/{id}/cache/clear", a.clearCache)
	mux.HandleFunc("POST /admin/providers", a.saveProvider)
	mux.HandleFunc("PUT /admin/providers/{id}", a.saveProvider)
	mux.HandleFunc("POST /admin/models", a.saveModel)
	mux.HandleFunc("PUT /admin/models/{id}", a.saveModel)
	mux.HandleFunc("DELETE /admin/models/{id}", a.deleteModel)
	mux.HandleFunc("PUT /admin/routes", a.saveRoute)
	mux.HandleFunc("GET /admin/apps/{id}/routes", a.applicationRoutes)
	mux.HandleFunc("PUT /admin/apps/{id}/routes/{alias}", a.saveApplicationRoute)
	mux.HandleFunc("DELETE /admin/apps/{id}/routes/{alias}", a.saveApplicationRoute)
	mux.HandleFunc("GET /admin/app-metering", a.applicationMetering)
	mux.HandleFunc("POST /admin/gray-release", a.gray)
	mux.HandleFunc("PUT /admin/rules", a.rules)
	mux.HandleFunc("POST /admin/users", a.user)
	mux.HandleFunc("POST /admin/config/publish", a.publish)
	mux.HandleFunc("GET /admin/call-records", a.records)
	mux.HandleFunc("GET /admin/trace/{id}", a.records)
	mux.HandleFunc("GET /admin/payload/{id}", a.payload)
	mux.HandleFunc("GET /admin/cost-report", a.cost)
	mux.HandleFunc("GET /admin/metrics", a.metrics)
	mux.HandleFunc("GET /admin/monitoring", a.grafanaStatus)
	mux.HandleFunc("GET /admin/audit", a.audit)
	mux.HandleFunc("GET /admin/metering", a.meteringStatus)
	mux.HandleFunc("GET /admin/instances", a.instances)
	mux.HandleFunc("GET /admin/alerts", a.alerts)
	mux.HandleFunc("POST /admin/playground", a.playground)
	mux.HandleFunc("POST /admin/usage-correction", a.correct)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { JSON(w, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if _, e := a.Engine.Store.Load(r.Context()); e != nil {
			APIError(w, 503, "not_ready", "storage unavailable")
			return
		}
		JSON(w, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", a.Engine.Metrics.Handler())
	if a.Assets != nil {
		mux.Handle("GET /", a.Assets)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		if strings.HasPrefix(r.URL.Path, "/admin/") && r.URL.Path != "/admin/login" {
			u, _, s, ok := a.identity(r)
			if !ok {
				APIError(w, 401, "unauthorized", "login required")
				return
			}
			if r.Method != "GET" && (!a.originOK(r) || !Equal(s.CSRF, r.Header.Get("X-CSRF-Token"))) {
				APIError(w, 403, "csrf_failed", "CSRF or Origin validation failed")
				return
			}
			if r.Method != "GET" && u.Role != "admin" && r.URL.Path != "/admin/playground" && r.URL.Path != "/admin/logout" {
				APIError(w, 403, "forbidden", "administrator role required")
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
	ordinary := protectedHandler(handler, &a.admission, a.MaxConcurrent, defaultAdminConcurrency, "/admin/", "admin", a.Engine.Metrics)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Grafana 只读查询采用独立同源策略及并发池，不占普通管理槽位。
		if strings.HasPrefix(r.URL.Path, grafanaPrefix) {
			a.grafana(w, r)
			return
		}
		ordinary.ServeHTTP(w, r)
	})
}
func readJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(out); e != nil {
		APIError(w, 400, "invalid_request", e.Error())
		return false
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		APIError(w, 400, "invalid_request", "only one JSON value allowed")
		return false
	}
	return true
}
func (a *Admin) login(w http.ResponseWriter, r *http.Request) {
	if !a.originOK(r) {
		APIError(w, 403, "origin_required", "same-origin login required")
		return
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	if e := a.Engine.Guard.Rate(r.Context(), "login:"+host, 10, time.Minute); e != nil {
		rejectGuard(w, e)
		return
	}
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !readJSON(w, r, &input) {
		return
	}
	cfg, e := a.Engine.Store.Load(r.Context())
	u, ok := cfg.Users[input.Username]
	if e != nil || !ok || !CheckPassword(u.PasswordHash, input.Password) {
		APIError(w, 401, "invalid_credentials", "invalid credentials")
		return
	}
	token := ID("ses_")
	s := session{u.Name, u.PasswordHash, ID("csrf_"), time.Now().Add(8 * time.Hour)}
	a.mu.Lock()
	for k, x := range a.sessions {
		if time.Now().After(x.Expires) {
			delete(a.sessions, k)
		}
	}
	if len(a.sessions) >= 10000 {
		a.mu.Unlock()
		APIError(w, 503, "session_capacity", "session capacity exceeded")
		return
	}
	a.sessions[Hash(token)] = s
	a.mu.Unlock()
	if a.Sessions != nil {
		if err := a.Sessions.Put(r.Context(), Hash(token), s); err != nil {
			APIError(w, 503, "session_unavailable", "session could not be saved")
			return
		}
	}
	http.SetCookie(w, &http.Cookie{Name: "gw_session", Value: token, Path: "/admin", HttpOnly: true, Secure: a.Secure, SameSite: http.SameSiteStrictMode, MaxAge: 8 * 3600})
	JSON(w, map[string]any{"username": u.Name, "role": u.Role, "csrf": s.CSRF})
}
func (a *Admin) sessionInfo(w http.ResponseWriter, r *http.Request) {
	u, _, s, _ := a.identity(r)
	JSON(w, map[string]any{"username": u.Name, "role": u.Role, "csrf": s.CSRF})
}
func (a *Admin) logout(w http.ResponseWriter, r *http.Request) {
	cookie, _ := r.Cookie("gw_session")
	a.mu.Lock()
	delete(a.sessions, Hash(cookie.Value))
	a.mu.Unlock()
	if a.Sessions != nil {
		if err := a.Sessions.Delete(r.Context(), Hash(cookie.Value)); err != nil {
			APIError(w, 503, "session_unavailable", "logout could not be confirmed; retry required")
			return
		}
	}
	http.SetCookie(w, &http.Cookie{Name: "gw_session", Value: "", Path: "/admin", MaxAge: -1, HttpOnly: true, Secure: a.Secure, SameSite: http.SameSiteStrictMode})
	JSON(w, map[string]bool{"ok": true})
}
func (a *Admin) mutate(r *http.Request, action string, fn func(*Config) error) (Config, error) {
	u, _, _, _ := a.identity(r)
	cfg, e := a.Engine.Store.Update(r.Context(), u.Name, action, fn)
	if e != nil {
		return cfg, e
	}
	e = a.Engine.Publisher.PublishConfig(r.Context(), cfg)
	if e == nil {
		e = a.Engine.Refresh(r.Context())
	}
	if e != nil {
		return cfg, errPublishPending
	}
	return cfg, nil
}

var errPublishPending = errors.New("configuration saved; publication pending")

func mutationResult(w http.ResponseWriter, c Config, e error) {
	if errors.Is(e, errPublishPending) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		JSON(w, map[string]any{"ok": true, "published": false, "version": c.Version})
		return
	}
	if e != nil {
		APIError(w, 400, "configuration_error", e.Error())
		return
	}
	JSON(w, map[string]any{"ok": true, "version": c.Version})
}
func (a *Admin) config(w http.ResponseWriter, r *http.Request) {
	u, c, _, _ := a.identity(r)
	if u.Role != "admin" {
		APIError(w, 403, "forbidden", "administrator role required")
		return
	}
	for k, p := range c.Providers {
		p.Secret = ""
		c.Providers[k] = p
	}
	for k, key := range c.Keys {
		key.Hash = ""
		c.Keys[k] = key
	}
	for k, x := range c.Users {
		x.PasswordHash = ""
		c.Users[k] = x
	}
	JSON(w, struct {
		Config
		RuntimeCapabilities map[string]bool     `json:"runtime_capabilities"`
		ModelReferences     map[string][]string `json:"model_references"`
	}{c, map[string]bool{"cache_enabled": !a.Engine.CacheDisabled}, modelReferences(c)})
}
func (a *Admin) apps(w http.ResponseWriter, r *http.Request) {
	u, c, _, _ := a.identity(r)
	out := []App{}
	for _, x := range c.Apps {
		if u.Role != "developer" || x.Owner == u.Name {
			out = append(out, x)
		}
	}
	JSON(w, out)
}
func (a *Admin) saveApp(w http.ResponseWriter, r *http.Request) {
	var app App
	if !readJSON(w, r, &app) {
		return
	}
	if id := r.PathValue("id"); id != "" {
		app.ID = id
	} else {
		app.ID = ID("app_")
	}
	if app.QPS < 0 || app.QPS > 1000000 || app.MonthlyTokens < 0 || app.BudgetMicro < 0 || app.BudgetMicro > 1000000000000 || app.LogSample < 0 || app.LogSample > 1 || app.OutboxBytes != 0 && (app.OutboxBytes < 2*maxEventBytes || app.OutboxBytes > 1<<40) {
		APIError(w, 400, "invalid_app", "invalid limits")
		return
	}
	if app.Name == "" || len(app.Name) > 128 || len(app.Department) > 128 {
		APIError(w, 400, "invalid_app", "name is required")
		return
	}
	if app.CacheTTL <= 0 {
		app.CacheTTL = 300
	}
	if app.Similarity == 0 {
		app.Similarity = .97
	}
	if app.Similarity < .8 || app.Similarity > 1 || app.CacheTTL > 86400 {
		APIError(w, 400, "invalid_cache", "invalid cache policy")
		return
	}
	if app.BudgetAction != "reject" && app.BudgetAction != "warn" {
		APIError(w, 400, "invalid_budget_action", "use reject or warn")
		return
	}
	cfg, e := a.mutate(r, "app.save", func(c *Config) error {
		if app.Cache && a.Engine.CacheDisabled {
			// 旧应用仍可编辑并保留原策略，但不能在低配部署中新增启用。
			old, ok := c.Apps[app.ID]
			if !ok || !old.Cache {
				return errors.New("semantic cache is disabled for this deployment")
			}
		}
		if app.Owner != "" {
			if _, ok := c.Users[app.Owner]; !ok {
				return errors.New("owner user does not exist")
			}
		}
		for _, alias := range app.Models {
			if _, ok := c.Routes[alias]; !ok {
				return errors.New("route does not exist: " + alias)
			}
		}
		if app.Cache {
			m, ok := c.Models[app.EmbeddingModel]
			if !ok || !Contains(m.Capabilities, "embedding") {
				return errors.New("cache requires registered embedding model")
			}
		}
		if old, ok := c.Apps[app.ID]; ok {
			app.CacheEpoch = old.CacheEpoch + 1
		} else if r.PathValue("id") != "" {
			return errors.New("application does not exist")
		}
		c.Apps[app.ID] = app
		return validateAppRoutes(*c)
	})
	if e != nil && !errors.Is(e, errPublishPending) {
		mutationResult(w, cfg, e)
		return
	}
	JSON(w, map[string]any{"app": app, "published": e == nil, "version": cfg.Version})
}
func (a *Admin) issueKey(w http.ResponseWriter, r *http.Request) {
	token := ID("sk_gw_")
	k := Key{ID: ID("key_"), AppID: r.PathValue("id"), Hash: Hash(token), State: "active"}
	cfg, e := a.mutate(r, "key.issue", func(c *Config) error {
		if _, ok := c.Apps[k.AppID]; !ok {
			return errors.New("app does not exist")
		}
		c.Keys[k.ID] = k
		return nil
	})
	if e != nil && !errors.Is(e, errPublishPending) {
		mutationResult(w, cfg, e)
		return
	}
	JSON(w, map[string]any{"id": k.ID, "api_key": token, "version": cfg.Version, "published": e == nil})
}
func (a *Admin) rotateKey(w http.ResponseWriter, r *http.Request) {
	var input struct {
		GraceSeconds int `json:"grace_seconds"`
	}
	if !readJSON(w, r, &input) {
		return
	}
	if input.GraceSeconds < 0 || input.GraceSeconds > 86400 {
		APIError(w, 400, "invalid_grace", "grace must be 0..86400 seconds")
		return
	}
	token := ID("sk_gw_")
	var nk Key
	cfg, e := a.mutate(r, "key.rotate", func(c *Config) error {
		k, ok := c.Keys[r.PathValue("id")]
		if !ok || k.State == "revoked" {
			return errors.New("key unavailable")
		}
		k.State = "grace"
		k.Expires = time.Now().Add(time.Duration(input.GraceSeconds) * time.Second).UTC()
		c.Keys[k.ID] = k
		nk = Key{ID: ID("key_"), AppID: k.AppID, Hash: Hash(token), State: "active"}
		c.Keys[nk.ID] = nk
		return nil
	})
	if e != nil && !errors.Is(e, errPublishPending) {
		mutationResult(w, cfg, e)
		return
	}
	JSON(w, map[string]any{"id": nk.ID, "api_key": token, "version": cfg.Version, "published": e == nil})
}
func (a *Admin) revokeKey(w http.ResponseWriter, r *http.Request) {
	cfg, e := a.mutate(r, "key.revoke", func(c *Config) error {
		k, ok := c.Keys[r.PathValue("id")]
		if !ok {
			return errors.New("key does not exist")
		}
		k.State = "revoked"
		c.Keys[k.ID] = k
		return nil
	})
	mutationResult(w, cfg, e)
}
func (a *Admin) clearCache(w http.ResponseWriter, r *http.Request) {
	cfg, e := a.mutate(r, "cache.clear", func(c *Config) error {
		app, ok := c.Apps[r.PathValue("id")]
		if !ok {
			return errors.New("app missing")
		}
		app.CacheEpoch++
		c.Apps[app.ID] = app
		return validateAppRoutes(*c)
	})
	mutationResult(w, cfg, e)
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,64}$`)

func (a *Admin) saveProvider(w http.ResponseWriter, r *http.Request) {
	var p struct {
		ID     string `json:"id"`
		Kind   string `json:"kind"`
		URL    string `json:"url"`
		APIKey string `json:"api_key"`
	}
	if !readJSON(w, r, &p) {
		return
	}
	if r.Method == http.MethodPut && r.PathValue("id") != p.ID {
		APIError(w, 400, "provider_id_mismatch", "编辑时不能修改厂商编号")
		return
	}
	if !identifier.MatchString(p.ID) || !Contains([]string{"deepseek", "qwen", "openai", "mock"}, p.Kind) || (!a.Engine.Dev && p.Kind == "mock") {
		APIError(w, 400, "invalid_provider", "请检查厂商类型和编号，编号仅支持 1–64 个字母、数字、点、下划线或连字符")
		return
	}
	if e := ValidateURL(p.URL, a.Engine.Dev); e != nil {
		APIError(w, 400, "invalid_url", e.Error())
		return
	}
	cfg, e := a.mutate(r, "provider.save", func(c *Config) error {
		_, exists := c.Providers[p.ID]
		if r.Method == http.MethodPost && exists {
			return errProviderExists
		}
		if r.Method == http.MethodPut && !exists {
			return errProviderMissing
		}
		secret := c.Providers[p.ID].Secret
		if p.APIKey != "" {
			if strings.TrimSpace(p.APIKey) == "" {
				return errors.New("API Key 不能仅包含空格")
			}
			secret = a.Engine.Vault.Encrypt(p.ID, p.APIKey)
		}
		if secret == "" {
			return errors.New("新增厂商必须填写 API Key")
		}
		c.Providers[p.ID] = Provider{p.ID, p.Kind, p.URL, secret}
		return nil
	})
	switch {
	case errors.Is(e, errProviderExists):
		APIError(w, http.StatusConflict, "provider_exists", e.Error())
	case errors.Is(e, errProviderMissing):
		APIError(w, http.StatusNotFound, "provider_not_found", e.Error())
	default:
		mutationResult(w, cfg, e)
	}
}
func (a *Admin) saveModel(w http.ResponseWriter, r *http.Request) {
	var m Model
	if !readJSON(w, r, &m) {
		return
	}
	if r.Method == http.MethodPut && r.PathValue("id") != m.ID {
		APIError(w, 400, "model_id_mismatch", "编辑时不能修改模型编号")
		return
	}
	if !identifier.MatchString(m.ID) || m.Upstream == "" || m.Version == "" || m.Context <= 0 || m.Context > 1000000 || m.MaxOutput < 0 || m.MaxOutput > m.Context || m.RPM < 0 || m.TPM < 0 || m.InputMicro < 0 || m.OutputMicro < 0 || m.CachedInputMicro < 0 || m.InputMicro > 1000000000000 || m.OutputMicro > 1000000000000 || m.CachedInputMicro > 1000000000000 || m.Dimensions < 0 || m.Dimensions > 65536 || m.PriceVersion == "" || !Contains([]string{"CNY", "USD"}, m.Currency) || m.FXPPM <= 0 || m.FXPPM > 100000000 {
		APIError(w, 400, "invalid_model", "请检查模型名称、版本、上下文及输出上限、限流和价格配置")
		return
	}
	for _, cap := range m.Capabilities {
		if !Contains([]string{"chat", "embedding", "rerank", "rerank_instruct", "stream", "tools", "json_object", "json_schema", "seed"}, cap) {
			APIError(w, 400, "invalid_capability", "不支持的模型能力："+cap)
			return
		}
	}
	if Contains(m.Capabilities, "embedding") && m.Dimensions <= 0 {
		APIError(w, 400, "invalid_dimensions", "embedding 模型的向量维度必须大于 0")
		return
	}
	if !validRerankModel(m) {
		APIError(w, 400, "invalid_rerank_model", "请检查重排接口、文档数、输入上限、能力及价格配置")
		return
	}
	cfg, e := a.mutate(r, "model.save", func(c *Config) error {
		_, exists := c.Models[m.ID]
		if r.Method == http.MethodPost && exists {
			return errModelExists
		}
		if r.Method == http.MethodPut && !exists {
			return errModelMissing
		}
		if _, ok := c.Providers[m.ProviderID]; !ok {
			return errors.New("所属厂商不存在，请先配置厂商凭据")
		}
		c.Models[m.ID] = m
		for _, app := range c.Apps {
			if app.EmbeddingModel == m.ID && !Contains(m.Capabilities, "embedding") {
				return errors.New("应用 " + app.ID + " 的缓存向量模型必须支持 embedding")
			}
		}
		for _, route := range c.Routes {
			if err := ValidateRoute(*c, route); err != nil {
				return err
			}
		}
		return validateAppRoutes(*c)
	})
	modelMutationResult(w, cfg, e)
}
func ValidateRoute(c Config, r Route) error {
	if !identifier.MatchString(r.Alias) || !Contains([]string{"chat", "embedding", "rerank"}, r.Kind) || !Contains([]string{"priority", "weight", "cost", "latency"}, r.Strategy) || len(r.Candidates) == 0 || len(r.Candidates) > 20 {
		return errors.New("invalid route")
	}
	sum := 0
	seen := map[string]bool{}
	var embedding Model
	for i, x := range r.Candidates {
		m, ok := c.Models[x.ModelID]
		if !ok || !Contains(m.Capabilities, r.Kind) || seen[m.ID] || x.Weight < 0 {
			return errors.New("invalid candidate")
		}
		if r.Kind == "rerank" && !validRerankModel(m) {
			return errors.New("invalid rerank candidate configuration")
		}
		seen[m.ID] = true
		sum += x.Weight
		if r.Kind == "embedding" {
			if i == 0 {
				embedding = m
			} else if m.Upstream != embedding.Upstream || m.Version != embedding.Version || m.Dimensions != embedding.Dimensions {
				return errors.New("embedding candidates must use identical model, version and dimensions")
			}
		}
	}
	if r.Strategy == "weight" && sum != 100 {
		return errors.New("weights must sum to 100")
	}
	if r.Gray.State == "running" {
		m, ok := c.Models[r.Gray.Candidate]
		if r.Kind != "chat" || !ok || !Contains(m.Capabilities, "chat") || !Contains([]string{"10", "50", "100"}, strconv.Itoa(r.Gray.Percent)) || r.Gray.MinSamples < 10 || r.Gray.MaxError <= 0 || r.Gray.MaxError >= 1 || r.Gray.MaxLatencyMS <= 0 {
			return errors.New("invalid gray release policy")
		}
	}
	return nil
}
func (a *Admin) saveRoute(w http.ResponseWriter, r *http.Request) {
	var route Route
	if !readJSON(w, r, &route) {
		return
	}
	cfg, e := a.mutate(r, "route.save", func(c *Config) error {
		if e := ValidateRoute(*c, route); e != nil {
			return e
		}
		if route.Gray.State == "running" {
			route.Gray.RunID = ID("gray_")
		}
		c.Routes[route.Alias] = route
		return validateAppRoutes(*c)
	})
	mutationResult(w, cfg, e)
}
func (a *Admin) gray(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Alias  string `json:"alias"`
		AppID  string `json:"app_id,omitempty"`
		Policy Gray   `json:"policy"`
	}
	if !readJSON(w, r, &in) {
		return
	}
	cfg, e := a.mutate(r, "gray.release", func(c *Config) error {
		scope := "global"
		route, ok := c.Routes[in.Alias]
		if in.AppID != "" {
			app, exists := c.Apps[in.AppID]
			if !exists || !Contains(app.Models, in.Alias) {
				return errors.New("application alias not allowed")
			}
			route, scope, ok = effectiveRoute(*c, in.AppID, in.Alias)
			scope = "application:" + in.AppID
		}
		if !ok {
			return errors.New("route missing")
		}
		if !Contains([]string{"running", "rolled_back", "finished"}, in.Policy.State) {
			return errors.New("invalid gray state")
		}
		in.Policy.RunID = ID("gray_")
		route.Gray = in.Policy
		if e := ValidateRoute(*c, route); e != nil {
			return e
		}
		putScopedRoute(c, scope, route)
		return validateAppRoutes(*c)
	})
	mutationResult(w, cfg, e)
}
func (a *Admin) rules(w http.ResponseWriter, r *http.Request) {
	var rules []Rule
	if !readJSON(w, r, &rules) {
		return
	}
	if len(rules) > 100 {
		APIError(w, 400, "too_many_rules", "at most 100 rules")
		return
	}
	for _, rule := range rules {
		if len(rule.Pattern) > 512 || !Contains([]string{"block", "nocache"}, rule.Action) {
			APIError(w, 400, "invalid_rule", "invalid action or pattern")
			return
		}
		if _, e := regexp.Compile(rule.Pattern); e != nil {
			APIError(w, 400, "invalid_pattern", e.Error())
			return
		}
	}
	cfg, e := a.mutate(r, "safety.rules", func(c *Config) error { c.Rules = rules; return nil })
	mutationResult(w, cfg, e)
}
func (a *Admin) user(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name     string `json:"name"`
		Role     string `json:"role"`
		Password string `json:"password"`
	}
	if !readJSON(w, r, &in) {
		return
	}
	if !identifier.MatchString(in.Name) || !Contains([]string{"admin", "developer", "cost"}, in.Role) || len(in.Password) < 12 || len(in.Password) > 72 {
		APIError(w, 400, "invalid_user", "password requires 12..72 bytes and a valid role")
		return
	}
	h, e := Password(in.Password)
	if e != nil {
		APIError(w, 400, "invalid_password", e.Error())
		return
	}
	cfg, e := a.mutate(r, "user.save", func(c *Config) error {
		c.Users[in.Name] = User{in.Name, h, in.Role}
		for _, user := range c.Users {
			if user.Role == "admin" {
				return nil
			}
		}
		return errors.New("at least one administrator is required")
	})
	mutationResult(w, cfg, e)
}
func (a *Admin) publish(w http.ResponseWriter, r *http.Request) {
	_, cfg, _, _ := a.identity(r)
	e := a.Engine.Publisher.PublishConfig(r.Context(), cfg)
	if e == nil {
		e = a.Engine.Refresh(r.Context())
	}
	if e != nil {
		e = errPublishPending
	}
	mutationResult(w, cfg, e)
}
func (a *Admin) filter(r *http.Request) (Filter, error) {
	u, c, _, _ := a.identity(r)
	q := r.URL.Query()
	f := Filter{AppID: q.Get("app_id"), ModelID: q.Get("model_id"), RequestID: q.Get("request_id"), Status: q.Get("status"), Limit: 100}
	if id := r.PathValue("id"); id != "" {
		f.RequestID = id
	}
	if u.Role == "developer" {
		app, ok := c.Apps[f.AppID]
		if !ok || app.Owner != u.Name {
			return f, errors.New("select an owned app")
		}
	}
	for _, v := range []struct {
		k   string
		dst *time.Time
	}{{"from", &f.From}, {"to", &f.To}} {
		if q.Get(v.k) != "" {
			x, e := time.Parse(time.RFC3339, q.Get(v.k))
			if e != nil {
				return f, e
			}
			*v.dst = x
		}
	}
	if q.Get("limit") != "" {
		n, e := strconv.Atoi(q.Get("limit"))
		if e != nil || n < 1 || n > 1000 {
			return f, errors.New("limit must be 1..1000")
		}
		f.Limit = n
	}
	if q.Get("offset") != "" {
		n, e := strconv.Atoi(q.Get("offset"))
		if e != nil || n < 0 {
			return f, errors.New("invalid offset")
		}
		f.Offset = n
	}
	return f, nil
}
func (a *Admin) records(w http.ResponseWriter, r *http.Request) {
	f, e := a.filter(r)
	if e != nil {
		APIError(w, 403, "forbidden", e.Error())
		return
	}
	rows, e := a.Engine.Store.Records(r.Context(), f)
	if e != nil {
		if errors.Is(e, ErrRecordCapacity) {
			APIError(w, 413, "records_too_large", e.Error())
			return
		}
		APIError(w, 503, "storage_unavailable", "cannot query records")
		return
	}
	JSON(w, recordViews(rows))
}
func (a *Admin) cost(w http.ResponseWriter, r *http.Request) {
	f, e := a.filter(r)
	if e != nil {
		APIError(w, 403, "forbidden", e.Error())
		return
	}
	agg, ok := a.Engine.Store.(Aggregator)
	if !ok {
		APIError(w, 503, "storage_unavailable", "aggregation unavailable")
		return
	}
	rows, e := agg.Report(r.Context(), f)
	if e != nil {
		if errors.Is(e, ErrReportCapacity) {
			APIError(w, 413, "report_too_large", e.Error())
			return
		}
		APIError(w, 503, "storage_unavailable", "report unavailable")
		return
	}
	if r.URL.Query().Get("format") == "csv" {
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", "attachment; filename=cost-report.csv")
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"app", "department", "model", "attempts", "tokens", "cost_cny", "pending", "input_tokens", "uncached_input_tokens", "cached_tokens", "output_tokens", "input_cost_cny", "cached_input_cost_cny", "output_cost_cny", "unallocated_cost_cny", "unallocated_count", "cache_unreported", "cache_embedding_attempts", "cache_embedding_tokens", "cache_embedding_cost_cny", "semantic_cache_hits", "usage_unreported"})
		for _, x := range rows {
			integer := func(n int64) string { return strconv.FormatInt(n, 10) }
			_ = cw.Write([]string{x.AppID, csvSafe(x.Department), csvSafe(x.ModelID), integer(x.Count), integer(x.Tokens), microCNY(x.Micro), integer(x.Pending),
				integer(x.InputTokens), integer(x.UncachedInputTokens), integer(x.CachedTokens), integer(x.OutputTokens), microCNY(x.InputMicro), microCNY(x.CachedInputMicro), microCNY(x.OutputMicro), microCNY(x.UnallocatedMicro), integer(x.UnallocatedCount), integer(x.CacheUnreported), integer(x.CacheEmbeddingCount), integer(x.CacheEmbeddingTokens), microCNY(x.CacheEmbeddingMicro), integer(x.CacheHits), integer(x.UsageUnreported)})
		}
		cw.Flush()
		return
	}
	JSON(w, rows)
}
func csvSafe(s string) string {
	if len(s) > 0 && strings.ContainsRune("=+-@\t\r", rune(s[0])) {
		return "'" + s
	}
	return s
}
func (a *Admin) metrics(w http.ResponseWriter, r *http.Request) {
	f, e := a.filter(r)
	if e != nil {
		APIError(w, 403, "forbidden", e.Error())
		return
	}
	f.Limit = 1000
	rows, e := a.Engine.Store.Records(r.Context(), f)
	if e != nil {
		APIError(w, 503, "storage_unavailable", "metrics unavailable")
		return
	}
	requests, failed, hits := 0, 0, 0
	for _, x := range rows {
		if x.Purpose != "request" {
			continue
		}
		requests++
		if x.Status == "failed" || x.Status == "stream_error" {
			failed++
		}
		if x.Cached {
			hits++
		}
	}
	JSON(w, map[string]any{"requests": requests, "failed": failed, "cache_hits": hits, "sample_limit": 1000, "note": "最近调用样本；容量与延迟分位数以 Prometheus 为准"})
}
func (a *Admin) audit(w http.ResponseWriter, r *http.Request) {
	u, _, _, _ := a.identity(r)
	if u.Role != "admin" {
		APIError(w, 403, "forbidden", "administrator required")
		return
	}
	rows, e := a.Engine.Store.Audits(r.Context(), 100)
	if e != nil {
		APIError(w, 503, "storage_unavailable", "audit unavailable")
		return
	}
	JSON(w, rows)
}
func (a *Admin) instances(w http.ResponseWriter, r *http.Request) {
	u, _, _, _ := a.identity(r)
	if u.Role != "admin" {
		APIError(w, 403, "forbidden", "administrator required")
		return
	}
	rows, e := a.Engine.Store.Instances(r.Context())
	if e != nil {
		APIError(w, 503, "storage_unavailable", "instances unavailable")
		return
	}
	type view struct {
		Instance
		Service    string `json:"service"`
		Status     string `json:"status"`
		AgeSeconds int64  `json:"age_seconds"`
	}
	_, cfg, _, _ := a.identity(r)
	out := map[string]view{}
	now := time.Now()
	for id, node := range rows {
		service := strings.SplitN(id, ":", 2)[0]
		if !Contains([]string{"gateway", "admin", "worker", "dev"}, service) {
			service = "unknown"
		}
		status := "ready"
		if now.Sub(node.Seen) >= 10*time.Second {
			status = "stale"
		} else if node.Version != cfg.Version {
			status = "config_pending"
		}
		out[id] = view{node, service, status, int64(now.Sub(node.Seen).Seconds())}
	}
	JSON(w, out)
}
func (a *Admin) playground(w http.ResponseWriter, r *http.Request) {
	u, cfg, _, _ := a.identity(r)
	app, _, ok := a.Engine.authenticate(r)
	if !ok || (u.Role == "developer" && cfg.Apps[app.ID].Owner != u.Name) {
		APIError(w, 403, "forbidden", "provide an API key of an accessible app")
		return
	}
	clone := r.Clone(r.Context())
	clone.URL.Path = "/v1/chat/completions"
	a.Engine.Handler().ServeHTTP(w, clone)
}
func (a *Admin) correct(w http.ResponseWriter, r *http.Request) {
	var event Event
	if !readJSON(w, r, &event) {
		return
	}
	rows, e := a.Engine.Store.Records(r.Context(), Filter{RequestID: event.RequestID, Limit: 1000})
	if e != nil {
		APIError(w, 503, "storage_unavailable", "cannot find attempt")
		return
	}
	var old *Event
	for _, x := range rows {
		if x.AttemptID == event.AttemptID {
			copy := x
			old = &copy
			break
		}
	}
	if old == nil || old.Purpose == "request" || event.Usage.Input < 0 || event.Usage.Output < 0 || event.Usage.Cached < 0 || event.Usage.Cached > event.Usage.Input || event.Usage.Input+event.Usage.Output > 2000000 {
		APIError(w, 400, "invalid_correction", "attempt missing or usage invalid")
		return
	}
	fixed := *old
	fixed.Usage = event.Usage
	fixed.Usage.CachedReported = true
	fixed.Usage.Total = fixed.Usage.Input + fixed.Usage.Output
	fixed.Revision++
	if fixed.Revision < 3 {
		fixed.Revision = 3
	}
	fixed.ID = fixed.AttemptID + ":" + strconv.FormatInt(fixed.Revision, 10)
	fixed.Kind = "completed"
	fixed.Quality = "reconciled"
	fixed.Status = "reconciled"
	fixed.CostMicro = Cost(fixed.Model, fixed.Usage)
	if bus, ok := a.Engine.Bus.(*DurableBus); ok {
		key := r.Header.Get("Idempotency-Key")
		if len(key) > 128 {
			APIError(w, 400, "invalid_idempotency_key", "key exceeds 128 bytes")
			return
		}
		if key == "" {
			key = ID("correct_")
		}
		w.Header().Set("Idempotency-Key", key)
		s, ok := a.Engine.Store.(*SQLStore)
		if !ok {
			APIError(w, 503, "storage_unavailable", "durable correction requires SQL")
			return
		}
		fixed, e = s.CorrectOutbox(r.Context(), uName(a, r), key, fixed, bus.GlobalBytes, bus.AppBytes)
		if e != nil {
			if errors.Is(e, ErrEventConflict) {
				APIError(w, 409, "idempotency_conflict", "key was used for another correction")
				return
			}
			APIError(w, 503, "audit_unavailable", "correction not persisted")
			return
		}
		if e = bus.Publish(r.Context(), fixed); e != nil {
			APIError(w, 503, "metering_unavailable", "correction queued; publication not acknowledged")
			return
		}
		JSON(w, fixed)
		return
	}
	// 管理实例共享事务版本分配，避免异步入账期间并发修正使用相同 revision。
	_, e = a.Engine.Store.Update(r.Context(), uName(a, r), "usage.correct.reserve", func(c *Config) error {
		if c.CorrectionRevisions == nil {
			c.CorrectionRevisions = map[string]int64{}
		}
		if last := c.CorrectionRevisions[fixed.AttemptID]; last >= fixed.Revision {
			fixed.Revision = last + 1
		}
		c.CorrectionRevisions[fixed.AttemptID] = fixed.Revision
		fixed.ID = fixed.AttemptID + ":" + strconv.FormatInt(fixed.Revision, 10)
		return nil
	})
	if e != nil {
		APIError(w, 503, "audit_unavailable", "cannot allocate correction revision")
		return
	}
	if e = a.Engine.Bus.Publish(r.Context(), fixed); e != nil {
		APIError(w, 503, "metering_unavailable", "correction not persisted")
		return
	}
	JSON(w, fixed)
}
func uName(a *Admin, r *http.Request) string { u, _, _, _ := a.identity(r); return u.Name }
func WithTimeout(c context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c, 5*time.Second)
}
func (a *Admin) alerts(w http.ResponseWriter, r *http.Request) {
	f, e := a.filter(r)
	if e != nil {
		APIError(w, 403, "forbidden", e.Error())
		return
	}
	s, ok := a.Engine.Store.(AlertStore)
	if !ok {
		APIError(w, 503, "storage_unavailable", "alerts unavailable")
		return
	}
	rows, e := s.Alerts(r.Context(), f.AppID)
	if e != nil {
		APIError(w, 503, "storage_unavailable", "alerts unavailable")
		return
	}
	JSON(w, rows)
}
func (a *Admin) payload(w http.ResponseWriter, r *http.Request) {
	u, _, _, _ := a.identity(r)
	if u.Role == "cost" {
		APIError(w, 403, "forbidden", "body access is not part of the cost role")
		return
	}
	f, err := a.filter(r)
	if err != nil {
		APIError(w, 403, "forbidden", err.Error())
		return
	}
	f.RequestID = r.URL.Query().Get("request_id")
	if f.RequestID == "" {
		APIError(w, 400, "request_id_required", "request_id is required")
		return
	}
	rows, err := a.Engine.Store.Records(r.Context(), f)
	if err != nil {
		APIError(w, 503, "storage_unavailable", "payload unavailable")
		return
	}
	for _, event := range rows {
		if event.AttemptID != r.PathValue("id") || event.PayloadCipher == "" || time.Since(event.At) > 7*24*time.Hour {
			continue
		}
		b, err := a.Engine.Vault.Decrypt(event.AttemptID, event.PayloadCipher)
		if err != nil {
			APIError(w, 503, "payload_unavailable", "payload could not be decoded")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, b)
		return
	}
	APIError(w, 404, "payload_unavailable", "not sampled or outside the 7 day viewing period")
}

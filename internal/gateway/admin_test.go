package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func adminLogin(t *testing.T, a *Admin, name string) (*http.Cookie, string) {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "http://console/admin/login", strings.NewReader(`{"username":"`+name+`","password":"test-password-123"}`))
	r.Header.Set("Origin", "http://console")
	a.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var x struct {
		CSRF string `json:"csrf"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &x)
	return w.Result().Cookies()[0], x.CSRF
}
func adminCall(a *Admin, c *http.Cookie, csrf, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://console"+path, strings.NewReader(body))
	r.AddCookie(c)
	r.Header.Set("Origin", "http://console")
	r.Header.Set("X-CSRF-Token", csrf)
	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, r)
	return w
}
func TestAdminRBACCSRFAndPasswordInvalidation(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	c, csrf := adminLogin(t, a, "dev")
	for _, path := range []string{"/admin/config", "/admin/call-records?app_id=other", "/admin/trace/req_other?app_id=other"} {
		if w := adminCall(a, c, csrf, "GET", path, ""); w.Code != 403 {
			t.Fatal(path, w.Code)
		}
	}
	if w := adminCall(a, c, csrf, "GET", "/admin/call-records?app_id=app", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	c, csrf = adminLogin(t, a, "admin")
	if w := adminCall(a, c, "", "POST", "/admin/apps/app/cache/clear", ""); w.Code != 403 {
		t.Fatal(w.Code)
	}
	if w := adminCall(a, c, csrf, "POST", "/admin/users", `{"name":"admin","role":"developer","password":"test-password-123"}`); w.Code != 400 {
		t.Fatal("last administrator removed", w.Code)
	}
	h, _ := Password("new-password-123")
	_, _ = e.Store.Update(context.Background(), "test", "password", func(c *Config) error { u := c.Users["admin"]; u.PasswordHash = h; c.Users[u.Name] = u; return nil })
	if w := adminCall(a, c, csrf, "GET", "/admin/session", ""); w.Code != 401 {
		t.Fatal("old session survived password reset")
	}
}

type unavailablePublisher struct{ Publisher }

func (unavailablePublisher) PublishConfig(context.Context, Config) error {
	return errors.New("injected publication outage")
}
func TestKeyDisplayedOnceWhenPublicationPending(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	c, csrf := adminLogin(t, a, "admin")
	e.Publisher = unavailablePublisher{e.Publisher}
	w := adminCall(a, c, csrf, "POST", "/admin/apps/app/keys", "")
	var x struct {
		APIKey    string `json:"api_key"`
		Published bool   `json:"published"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &x)
	if w.Code != 200 || x.APIKey == "" || x.Published {
		t.Fatal(w.Code, w.Body.String())
	}
	cfg, _ := e.Store.Load(context.Background())
	for _, k := range cfg.Keys {
		if k.Hash == x.APIKey {
			t.Fatal("plaintext key stored")
		}
	}
	w = adminCall(a, c, csrf, "GET", "/admin/config", "")
	if strings.Contains(w.Body.String(), x.APIKey) || strings.Contains(w.Body.String(), Hash(x.APIKey)) {
		t.Fatal("key leaked in config")
	}
}
func TestKeyRotationGraceAndRevocation(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	c, csrf := adminLogin(t, a, "admin")
	w := adminCall(a, c, csrf, "POST", "/admin/keys/key/rotate", `{"grace_seconds":60}`)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, e, "hello", false); w.Code != 200 {
		t.Fatal("grace key rejected")
	}
	w = adminCall(a, c, csrf, "POST", "/admin/keys/key/revoke", "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, e, "hello", false); w.Code != 401 {
		t.Fatal("revoked key accepted")
	}
}
func TestRedisQuotaAtomicSettlementAndSharedSessions(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	g := RedisGuard{Client: client, RequireReady: true}
	if err := g.Reserve(context.Background(), App{ID: "unready"}, Reservation{ID: "unready", AppID: "unready"}); !errors.Is(err, ErrUnavailable) {
		t.Fatal("lost quota state accepted", err)
	}
	_ = client.Set(context.Background(), "gw:quota:state", "ready", 0).Err()
	ctx := context.Background()
	app := App{ID: "app", MonthlyTokens: 100, BudgetAction: "reject", BudgetMicro: 100}
	ctx = testPermitContext(t, client, ctx, "app", "b")
	var wg sync.WaitGroup
	var allowed atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g.Reserve(ctx, app, Reservation{ID: ID("attempt_"), AppID: app.ID, Month: "2026-10", Tokens: 10, Micro: 10}) == nil {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 10 {
		t.Fatal(allowed.Load())
	}
	// 使用另一应用验证重复及乱序修正不会重复入账。
	r := Reservation{"attempt", "b", "2026-10", 10, 10}
	if err := g.Reserve(ctx, App{ID: "b"}, r); err != nil {
		t.Fatal(err)
	}
	event := Event{AttemptID: r.ID, AppID: r.AppID, Month: r.Month, Revision: 2, Usage: Usage{Total: 5}, CostMicro: 5}
	for i := 0; i < 5; i++ {
		if err := g.Settle(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	event.Revision = 3
	event.Usage.Total = 8
	event.CostMicro = 8
	_ = g.Settle(ctx, event)
	event.Revision = 2
	_ = g.Settle(ctx, event)
	counts, _ := g.Counters(ctx, r.AppID, r.Month)
	if counts.Tokens != 8 || counts.Micro != 8 || counts.ReservedTokens != 0 {
		t.Fatal(counts)
	}
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	b := NewAdmin(e, false, "")
	a.Sessions = RedisSessions{client}
	b.Sessions = RedisSessions{client}
	cookie, csrf := adminLogin(t, a, "admin")
	if w := adminCall(b, cookie, csrf, "GET", "/admin/session", ""); w.Code != 200 {
		t.Fatal("session not shared", w.Body.String())
	}
	_ = adminCall(b, cookie, csrf, "POST", "/admin/logout", "")
	if w := adminCall(a, cookie, csrf, "GET", "/admin/session", ""); w.Code != 401 {
		t.Fatal("logout not shared")
	}
	if err := g.Take(ctx, "model", 100, 101, time.Minute); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}
func TestSampledBodiesEncryptedAndScoped(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, _ = e.Store.Update(context.Background(), "test", "sample", func(c *Config) error { app := c.Apps["app"]; app.LogSample = 1; c.Apps[app.ID] = app; return nil })
	_ = e.Refresh(context.Background())
	w := call(t, e, "contact demo@example.com", false)
	rows, _ := e.Store.Records(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
	var event Event
	for _, x := range rows {
		if x.Purpose == "inference" {
			event = x
		}
	}
	plain, err := e.Vault.Decrypt(event.AttemptID, event.PayloadCipher)
	if err != nil || strings.Contains(plain, "demo@example.com") || !strings.Contains(plain, "REDACTED") {
		t.Fatal("sampling privacy boundary failed")
	}
	a := NewAdmin(e, false, "")
	c, csrf := adminLogin(t, a, "dev")
	p := "/admin/payload/" + event.AttemptID + "?request_id=" + event.RequestID + "&app_id=other"
	if w := adminCall(a, c, csrf, "GET", p, ""); w.Code != 403 {
		t.Fatal("cross-app body access")
	}
}

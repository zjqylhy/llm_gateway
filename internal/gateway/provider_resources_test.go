package gateway

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func providerBody(id, key string) string {
	body, _ := json.Marshal(map[string]string{"id": id, "kind": "openai", "url": "https://api.example.com/v1", "api_key": key})
	return string(body)
}

func TestProviderCreateEditAndSecretPreservation(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	admin, csrf := adminLogin(t, a, "admin")
	firstKey, nextKey := "provider-first-test-key", "provider-next-test-key"
	if w := adminCall(a, admin, csrf, "POST", "/admin/providers", providerBody("vendor.new", firstKey)); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	created, _ := e.Store.Load(context.Background())
	p := created.Providers["vendor.new"]
	key, err := e.Vault.Decrypt(p.ID, p.Secret)
	if err != nil || key != firstKey || p.Secret == firstKey {
		t.Fatal("new secret not encrypted or incorrect")
	}
	for _, scenario := range []struct {
		method, path, body string
		code               int
	}{
		{"POST", "/admin/providers", providerBody(p.ID, nextKey), 409},
		{"POST", "/admin/providers", providerBody("missing-key", ""), 400},
		{"POST", "/admin/providers", providerBody("space-key", "   "), 400},
		{"PUT", "/admin/providers/vendor.new", providerBody("different", nextKey), 400},
		{"PUT", "/admin/providers/missing", providerBody("missing", nextKey), 404},
	} {
		w := adminCall(a, admin, csrf, scenario.method, scenario.path, scenario.body)
		if w.Code != scenario.code {
			t.Fatal(scenario, w.Code, w.Body.String())
		}
		after, _ := e.Store.Load(context.Background())
		if !reflect.DeepEqual(created, after) {
			t.Fatal("rejected mutation changed config")
		}
	}
	if w := adminCall(a, admin, csrf, "PUT", "/admin/providers/vendor.new", providerBody(p.ID, "")); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	updated, _ := e.Store.Load(context.Background())
	if updated.Providers[p.ID].Secret != p.Secret || updated.Version != created.Version+1 {
		t.Fatal("empty edit lost existing secret")
	}
	if w := adminCall(a, admin, csrf, "PUT", "/admin/providers/vendor.new", providerBody(p.ID, nextKey)); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	updated, _ = e.Store.Load(context.Background())
	key, err = e.Vault.Decrypt(p.ID, updated.Providers[p.ID].Secret)
	if err != nil || key != nextKey {
		t.Fatal("edit did not replace secret")
	}
	view := adminCall(a, admin, csrf, "GET", "/admin/config", "")
	if view.Code != 200 || strings.Contains(view.Body.String(), firstKey) || strings.Contains(view.Body.String(), nextKey) || strings.Contains(view.Body.String(), updated.Providers[p.ID].Secret) {
		t.Fatal("config exposed provider credential")
	}
	store := e.Store.(*MemoryStore)
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.audits) != 4 {
		t.Fatal("rejected operation wrote audit")
	}
	if got := store.audits[len(store.audits)-1]; got.Action != "provider.save" || got.Actor != "admin" || got.Version != updated.Version {
		t.Fatal("provider audit missing", got)
	}
}

func TestProviderPermissionsPublicationAndValidation(t *testing.T) {
	var publisher *unavailablePublisher
	e := testEngine(t, MockProvider, func(e *Engine) {
		publisher = &unavailablePublisher{Publisher: e.Publisher}
		e.Publisher = publisher
	})
	a := NewAdmin(e, false, "")
	admin, csrf := adminLogin(t, a, "admin")
	dev, devCSRF := adminLogin(t, a, "dev")
	for _, req := range []struct{ method, path string }{{"POST", "/admin/providers"}, {"PUT", "/admin/providers/new"}} {
		if w := adminCall(a, dev, devCSRF, req.method, req.path, providerBody("new", "test-key")); w.Code != 403 {
			t.Fatal(w.Code)
		}
		if w := adminCall(a, admin, "", req.method, req.path, providerBody("new", "test-key")); w.Code != 403 {
			t.Fatal(w.Code)
		}
	}
	for _, body := range []string{
		`{"id":"bad/id","kind":"openai","url":"https://api.example.com/v1","api_key":"test-key"}`,
		`{"id":"new","kind":"unknown","url":"https://api.example.com/v1","api_key":"test-key"}`,
		`{"id":"new","kind":"openai","url":"not-a-url","api_key":"test-key"}`,
	} {
		if w := adminCall(a, admin, csrf, "POST", "/admin/providers", body); w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	publisher.unavailable.Store(true)
	for _, req := range []struct{ method, path string }{{"POST", "/admin/providers"}, {"PUT", "/admin/providers/new"}} {
		w := adminCall(a, admin, csrf, req.method, req.path, providerBody("new", "test-key"))
		if w.Code != 202 || !strings.Contains(w.Body.String(), `"published":false`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	cfg, _ := e.Store.Load(context.Background())
	if cfg.Providers["new"].ID != "new" {
		t.Fatal("pending publication lost provider")
	}
}

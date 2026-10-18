package gateway

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func modelBody(t *testing.T, model Model) string {
	t.Helper()
	body, err := json.Marshal(model)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestModelCreateEditDeleteAndAudit(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	cookie, csrf := adminLogin(t, a, "admin")
	cfg, _ := e.Store.Load(context.Background())
	m := cfg.Models["primary"]
	m.ID, m.Upstream = "new.chat", "vendor-new"
	if w := adminCall(a, cookie, csrf, "POST", "/admin/models", modelBody(t, m)); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	before, _ := e.Store.Load(context.Background())
	m.Upstream = "vendor-changed"
	for _, req := range []struct {
		method, path string
		code         int
	}{
		{"POST", "/admin/models", 409},
		{"PUT", "/admin/models/missing", 400},
	} {
		if w := adminCall(a, cookie, csrf, req.method, req.path, modelBody(t, m)); w.Code != req.code {
			t.Fatal(w.Code, w.Body.String())
		}
		after, _ := e.Store.Load(context.Background())
		if !reflect.DeepEqual(before, after) {
			t.Fatal("rejected save changed config")
		}
	}
	missing := m
	missing.ID = "missing"
	if w := adminCall(a, cookie, csrf, "PUT", "/admin/models/missing", modelBody(t, missing)); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := adminCall(a, cookie, csrf, "PUT", "/admin/models/new.chat", modelBody(t, m)); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	updated, _ := e.Store.Load(context.Background())
	if updated.Models[m.ID].Upstream != m.Upstream || len(updated.Models) != len(before.Models) || updated.Version != before.Version+1 {
		t.Fatal("edit did not update exactly one resource")
	}
	// 删除配置不得删除历史计量依据。
	event := Event{AttemptID: "historical-model", RequestID: "historical-request", AppID: "app", Model: m, Revision: 1, Status: "success", Kind: "finished", CostMicro: 52}
	if err := e.Store.Apply(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if w := adminCall(a, cookie, csrf, "DELETE", "/admin/models/new.chat", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	deleted, _ := e.Store.Load(context.Background())
	if _, exists := deleted.Models[m.ID]; exists {
		t.Fatal("model survived deletion")
	}
	if !reflect.DeepEqual(updated.Providers, deleted.Providers) || deleted.Version != updated.Version+1 {
		t.Fatal("delete changed provider or failed to increment version")
	}
	rows, err := e.Store.Records(context.Background(), Filter{RequestID: event.RequestID})
	if err != nil || len(rows) != 1 || rows[0].Model.ID != m.ID || rows[0].CostMicro != 52 {
		t.Fatal("historical usage changed", rows, err)
	}
	if w := adminCall(a, cookie, csrf, "DELETE", "/admin/models/new.chat", ""); w.Code != 404 {
		t.Fatal(w.Code)
	}
	after, _ := e.Store.Load(context.Background())
	if !reflect.DeepEqual(deleted, after) {
		t.Fatal("missing delete changed config")
	}
	store := e.Store.(*MemoryStore)
	store.mu.Lock()
	defer store.mu.Unlock()
	if got := store.audits[len(store.audits)-1]; got.Action != "model.delete" || got.Actor != "admin" || got.Version != deleted.Version {
		t.Fatal("delete audit missing", got)
	}
	if len(store.audits) != 4 {
		t.Fatal("rejected operations wrote audit", store.audits)
	}
}

func TestModelDeleteProtectsAllReferences(t *testing.T) {
	for _, scenario := range []struct {
		name, location string
		configure      func(*Config, Model)
	}{
		{"default candidate", "默认路由 chat · 候选模型", func(c *Config, m Model) {
			r := c.Routes["chat"]
			r.Candidates = append(r.Candidates, Candidate{m.ID, 0})
			c.Routes["chat"] = r
		}},
		{"stopped default gray", "默认路由 chat · 灰度候选", func(c *Config, m Model) {
			r := c.Routes["chat"]
			r.Gray = Gray{Candidate: m.ID, State: "stopped"}
			c.Routes["chat"] = r
		}},
		{"application candidate", "应用 app 的路由 chat · 候选模型", func(c *Config, m Model) {
			c.AppRoutes = map[string]map[string]Route{"app": {"chat": {Alias: "chat", Kind: "chat", Strategy: "priority", Candidates: []Candidate{{m.ID, 100}}}}}
		}},
		{"stopped application gray", "应用 app 的路由 chat · 灰度候选", func(c *Config, m Model) {
			r := c.Routes["chat"]
			r.Gray = Gray{Candidate: m.ID, State: "stopped"}
			c.AppRoutes = map[string]map[string]Route{"app": {"chat": r}}
		}},
		{"disabled application cache", "应用 app 的缓存向量模型", func(c *Config, m Model) {
			app := c.Apps["app"]
			app.Active, app.Cache, app.EmbeddingModel = false, false, m.ID
			c.Apps[app.ID] = app
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			e := testEngine(t, MockProvider)
			a := NewAdmin(e, false, "")
			cookie, csrf := adminLogin(t, a, "admin")
			cfg, _ := e.Store.Load(context.Background())
			m := cfg.Models["primary"]
			m.ID = "isolated"
			// 控制台先看到未引用的模型，再发生引用变化。
			if w := adminCall(a, cookie, csrf, "POST", "/admin/models", modelBody(t, m)); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			view := adminCall(a, cookie, csrf, "GET", "/admin/config", "")
			var result struct {
				References map[string][]string `json:"model_references"`
			}
			if err := json.Unmarshal(view.Body.Bytes(), &result); err != nil || len(result.References[m.ID]) != 0 {
				t.Fatal(view.Body.String(), err)
			}
			before, err := e.Store.Update(context.Background(), "test", "add-reference", func(c *Config) error { scenario.configure(c, m); return nil })
			if err != nil {
				t.Fatal(err)
			}
			store := e.Store.(*MemoryStore)
			store.mu.Lock()
			audits := len(store.audits)
			store.mu.Unlock()
			w := adminCall(a, cookie, csrf, "DELETE", "/admin/models/isolated", "")
			if w.Code != 409 || !strings.Contains(w.Body.String(), scenario.location) {
				t.Fatal(w.Code, w.Body.String())
			}
			after, _ := e.Store.Load(context.Background())
			if !reflect.DeepEqual(before, after) {
				t.Fatal("in-use delete changed config")
			}
			store.mu.Lock()
			if len(store.audits) != audits {
				t.Error("blocked delete wrote audit")
			}
			store.mu.Unlock()
			view = adminCall(a, cookie, csrf, "GET", "/admin/config", "")
			if err := json.Unmarshal(view.Body.Bytes(), &result); err != nil || !reflect.DeepEqual(result.References[m.ID], []string{scenario.location}) {
				t.Fatal(view.Body.String(), err)
			}
		})
	}
}

func TestModelMutationPermissionsAndPublicationPending(t *testing.T) {
	var publisher *unavailablePublisher
	e := testEngine(t, MockProvider, func(e *Engine) {
		publisher = &unavailablePublisher{Publisher: e.Publisher}
		e.Publisher = publisher
	})
	a := NewAdmin(e, false, "")
	admin, csrf := adminLogin(t, a, "admin")
	dev, devCSRF := adminLogin(t, a, "dev")
	cfg, _ := e.Store.Load(context.Background())
	m := cfg.Models["primary"]
	m.ID = "unused"
	body := modelBody(t, m)
	for _, req := range []struct{ method, path string }{
		{"POST", "/admin/models"}, {"PUT", "/admin/models/unused"}, {"DELETE", "/admin/models/unused"},
	} {
		if w := adminCall(a, dev, devCSRF, req.method, req.path, body); w.Code != 403 {
			t.Fatal(w.Code)
		}
		if w := adminCall(a, admin, "", req.method, req.path, body); w.Code != 403 {
			t.Fatal(w.Code)
		}
	}
	if w := adminCall(a, admin, csrf, "POST", "/admin/models", body); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	publisher.unavailable.Store(true)
	for _, req := range []struct {
		method string
		body   string
	}{{"PUT", body}, {"DELETE", ""}} {
		w := adminCall(a, admin, csrf, req.method, "/admin/models/unused", req.body)
		if w.Code != 202 || !strings.Contains(w.Body.String(), `"published":false`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	after, _ := e.Store.Load(context.Background())
	if _, exists := after.Models[m.ID]; exists {
		t.Fatal("pending publication lost saved deletion")
	}
}

func TestModelEditPreservesCacheEmbeddingCapability(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	admin, csrf := adminLogin(t, a, "admin")
	before, err := e.Store.Update(context.Background(), "test", "cache-model", func(c *Config) error {
		m := c.Models["primary"]
		m.ID, m.Capabilities, m.Dimensions = "cache-only", []string{"embedding"}, 32
		c.Models[m.ID] = m
		app := c.Apps["app"]
		app.EmbeddingModel = m.ID
		c.Apps[app.ID] = app
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	m := before.Models["cache-only"]
	m.Capabilities = []string{"chat"}
	w := adminCall(a, admin, csrf, "PUT", "/admin/models/cache-only", modelBody(t, m))
	if w.Code != 400 || !strings.Contains(w.Body.String(), "缓存向量模型必须支持 embedding") {
		t.Fatal(w.Code, w.Body.String())
	}
	after, _ := e.Store.Load(context.Background())
	if !reflect.DeepEqual(before, after) {
		t.Fatal("invalid edit changed cache model")
	}
}

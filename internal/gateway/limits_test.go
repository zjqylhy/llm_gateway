package gateway

import (
	"context"
	"testing"
)

func TestProviderLimitAllowsEligibleFallback(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, _ = e.Store.Update(context.Background(), "test", "limit", func(c *Config) error { m := c.Models["primary"]; m.RPM = 1; c.Models[m.ID] = m; return nil })
	_ = e.Refresh(context.Background())
	if w := call(t, e, "one", false); w.Code != 200 || w.Header().Get("X-Gateway-Model") != "primary" {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(t, e, "two", false); w.Code != 200 || w.Header().Get("X-Gateway-Model") != "backup" {
		t.Fatal("eligible fallback ignored", w.Code, w.Body.String())
	}
}

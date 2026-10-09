package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProtocolToolsStructuredAndEmbedding(t *testing.T) {
	e := testEngine(t, MockProvider)
	cases := []struct{ path, body, want string }{
		{"/v1/chat/completions", `{"model":"chat","messages":[{"role":"user","content":"lookup"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]}`, `"tool_calls"`},
		{"/v1/chat/completions", `{"model":"chat","messages":[{"role":"user","content":"json"}],"response_format":{"type":"json_object"}}`, `\"ok\":true`},
		{"/v1/embeddings", `{"model":"embedding","input":["one","two"],"dimensions":32}`, `"embedding"`},
	}
	for _, tc := range cases {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		e.Handler().ServeHTTP(w, r)
		if w.Code != 200 || !strings.Contains(w.Body.String(), tc.want) {
			t.Fatal(tc.path, w.Code, w.Body.String())
		}
	}
}
func TestMalformedParametersRejected(t *testing.T) {
	for _, body := range []string{
		`{"model":"m","messages":[{"role":"user","content":"hello","audio":{"id":"hidden"}}]}`,
		`{"model":"m","messages":[{"role":"user","content":"hello"}],"stream":"yes"}`,
		`{"model":"m","messages":[{"role":"user","content":"hello"}],"temperature":3}`,
		`{"model":"m","messages":[{"role":"user","content":"hello"}],"tools":{}}`,
		`{"model":"m","messages":[{"role":"user","content":"hello"}],"response_format":{"type":"unknown"}}`,
	} {
		if _, err := DecodeRequest([]byte(body), "chat"); err == nil {
			t.Fatal("invalid request accepted", body)
		}
	}
	if _, err := DecodeRequest([]byte(`{"model":"m","input":"x","stream":true}`), "embedding"); err == nil {
		t.Fatal("embedding stream accepted")
	}
}
func TestDisconnectCancelsProvider(t *testing.T) {
	cancelled := make(chan struct{}, 1)
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"},\"finish_reason\":null}]}\n\n")
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
		cancelled <- struct{}{}
	})
	server := httptest.NewServer(e.Handler())
	defer server.Close()
	r, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"chat","messages":[{"role":"user","content":"x"}],"stream":true}`))
	r.Header.Set("Authorization", "Bearer test-key")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	_, _ = resp.Body.Read(buf)
	_ = resp.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not receive cancellation")
	}
}
func TestCacheUsageZeroAndEstimatedReportPending(t *testing.T) {
	e := testEngine(t, MockProvider)
	_, _ = e.Store.Update(context.Background(), "test", "cache", func(c *Config) error { a := c.Apps["app"]; a.Cache = true; c.Apps[a.ID] = a; return nil })
	_ = e.Refresh(context.Background())
	_ = call(t, e, "same", false)
	w := call(t, e, "same", false)
	var x struct {
		Usage Usage `json:"usage"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &x)
	if w.Header().Get("X-Gateway-Cache") != "hit" || x.Usage.Total != 0 {
		t.Fatal("cache recharged prior usage", w.Body.String())
	}
	w = call(t, e, "[fail-primary]", false)
	rows, err := e.Store.(Aggregator).Report(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
	if err != nil {
		t.Fatal(err)
	}
	pending := int64(0)
	for _, r := range rows {
		pending += r.Pending
	}
	if pending != 1 {
		t.Fatal("estimated failed attempt not marked pending", rows)
	}
}

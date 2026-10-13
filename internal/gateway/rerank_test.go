package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func rerankTestModel(id, protocol string) Model {
	return Model{ID: id, ProviderID: "primary", Upstream: "vendor-" + id, Version: "v1", Capabilities: []string{"rerank", "rerank_instruct"}, Context: 120000, RerankProtocol: protocol, MaxDocuments: 500, MaxItemTokens: 4000, InputMicro: 1000000, PriceVersion: "v1", Currency: "CNY", FXPPM: 1000000}
}

func configureRerank(t *testing.T, e *Engine, models ...Model) {
	t.Helper()
	_, err := e.Store.Update(context.Background(), "test", "rerank", func(c *Config) error {
		candidates := []Candidate{}
		for _, m := range models {
			c.Models[m.ID] = m
			candidates = append(candidates, Candidate{m.ID, 100})
		}
		c.Routes["rerank"] = Route{Alias: "rerank", Kind: "rerank", Strategy: "priority", Candidates: candidates}
		app := c.Apps["app"]
		app.Models = append(app.Models, "rerank")
		// 开启缓存后重排仍不得产生内部向量化调用。
		app.Cache = true
		c.Apps["app"] = app
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func rerankCall(e *Engine, path, key, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+key)
	w := httptest.NewRecorder()
	e.Handler().ServeHTTP(w, r)
	return w
}

const rerankTestBody = `{"model":"rerank","query":"gateway","documents":["first","second"],"top_n":2,"return_documents":true,"instruct":"sort by relevance"}`

func TestRerankProtocolsAndSettlement(t *testing.T) {
	for _, protocol := range []string{"compatible", "dashscope"} {
		t.Run(protocol, func(t *testing.T) {
			var calls atomic.Int32
			e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var payload map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				expectedPath := "/v1/reranks"
				input, params := payload, payload
				if protocol == "dashscope" {
					expectedPath = "/v1/services/rerank/text-rerank/text-rerank"
					_ = json.Unmarshal(payload["input"], &input)
					_ = json.Unmarshal(payload["parameters"], &params)
				}
				if r.URL.Path != expectedPath || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer test" || string(payload["model"]) != `"vendor-rank"` || string(input["query"]) != `"gateway"` || string(input["documents"]) != `["first","second"]` || string(params["top_n"]) != "2" || string(params["instruct"]) != `"sort by relevance"` || params["return_documents"] != nil {
					t.Errorf("unexpected upstream request: %s %s %+v", r.Method, r.URL.Path, payload)
					w.WriteHeader(400)
					return
				}
				results := []any{map[string]any{"index": 0, "relevance_score": .1}, map[string]any{"index": 1, "relevance_score": .9, "document": map[string]string{"text": "untrusted upstream document"}}}
				response := map[string]any{"results": results, "id": "vendor-id", "usage": map[string]int{"total_tokens": 79}}
				if protocol == "dashscope" {
					response = map[string]any{"output": map[string]any{"results": results}, "request_id": "vendor-id", "usage": map[string]int{"prompt_tokens": 79, "total_tokens": 79}}
				}
				JSON(w, response)
			})
			configureRerank(t, e, rerankTestModel("rank", protocol))
			for _, path := range []string{"/v1/rerank", "/v1/reranks"} {
				w := rerankCall(e, path, "test-key", rerankTestBody)
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				var response struct {
					ID, Object, Model string
					Results           []rerankResult
					Usage             Usage
					Quality           string `json:"usage_quality"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if response.ID != "vendor-id" || response.Object != "list" || response.Model != "rerank" || response.Quality != "reported" || response.Usage != (Usage{Input: 79, Total: 79}) || len(response.Results) != 2 || *response.Results[0].Index != 1 || response.Results[0].Document.Text != "second" {
					t.Fatal(w.Body.String())
				}
				events, err := e.Store.Records(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
				if err != nil {
					t.Fatal(err)
				}
				attempts := 0
				for _, event := range events {
					if event.Purpose == "cache_embedding" {
						t.Fatal("rerank used semantic cache")
					}
					if event.Purpose == "inference" {
						attempts++
						if event.Status != "success" || event.Quality != "reported" || event.CostMicro != 79 || event.Usage.Input != 79 || event.Usage.Output != 0 {
							t.Fatal(event)
						}
					}
				}
				if attempts != 1 {
					t.Fatal(events)
				}
			}
			counts, _ := e.Guard.Counters(context.Background(), "app", month(time.Now()))
			if calls.Load() != 2 || counts.Tokens != 158 || counts.Micro != 158 || counts.ReservedTokens != 0 || counts.ReservedMicro != 0 {
				t.Fatal(calls.Load(), counts)
			}
		})
	}
}

func TestRerankParameterAndCapabilityValidation(t *testing.T) {
	var calls atomic.Int32
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) })
	model := rerankTestModel("rank", "compatible")
	model.Capabilities = []string{"rerank"}
	model.MaxDocuments, model.MaxItemTokens, model.Context = 2, 20, 100
	configureRerank(t, e, model)
	for _, fields := range []string{
		`"query":"","documents":["x"]`, `"query":{},"documents":["x"]`,
		`"query":"x","documents":[]`, `"query":"x","documents":[null]`,
		`"query":"x","documents":[{"text":"x"}]`, `"query":"x","documents":["x"],"stream":false`,
		`"query":"x","documents":["x"],"return_documents":null`, `"query":"x","documents":["x"],"return_documents":"yes"`,
		`"query":"x","documents":["x"],"top_n":0`, `"query":"x","documents":["x"],"top_n":1.5`,
		`"query":"x","documents":["x"],"top_n":null`, `"query":"x","documents":["x"],"instruct":"order"`,
		`"query":"x","documents":["x","y","z"]`, `"query":"` + strings.Repeat("q", 21) + `","documents":["x"]`,
		`"query":"x","documents":["` + strings.Repeat("d", 21) + `"]`,
	} {
		w := rerankCall(e, "/v1/rerank", "test-key", `{"model":"rerank",`+fields+`}`)
		if w.Code != 400 {
			t.Fatal(fields, w.Code, w.Body.String())
		}
	}
	for _, key := range []string{"other-key", "invalid"} {
		w := rerankCall(e, "/v1/rerank", key, rerankTestBody)
		want := 403
		if key == "invalid" {
			want = 401
		}
		if w.Code != want {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid requests reached upstream", calls.Load())
	}
	// 未提供 top_n 返回全部，超出文档数也按全部处理。
	for _, fields := range []string{"", `,"top_n":999`} {
		r, err := DecodeRequest([]byte(`{"model":"rerank","query":"x","documents":["a","b"]`+fields+`}`), "rerank")
		if err != nil || r.Rerank.TopN != 2 || r.Cacheable || r.MaxOutput != 0 {
			t.Fatal(r, err)
		}
	}
}

func TestRerankRepeatedQueryReservationAndBudget(t *testing.T) {
	r, err := DecodeRequest([]byte(`{"model":"rerank","query":"`+strings.Repeat("q", 30)+`","documents":["x","y","z"]}`), "rerank")
	if err != nil || r.InputEstimate < 93 {
		t.Fatal("query was not repeated in reservation", r, err)
	}
	var calls atomic.Int32
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	configureRerank(t, e, rerankTestModel("rank", "compatible"))
	_, err = e.Store.Update(context.Background(), "test", "budget", func(c *Config) error {
		app := c.Apps["app"]
		app.BudgetAction, app.BudgetMicro = "reject", 1
		c.Apps["app"] = app
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	w := rerankCall(e, "/v1/rerank", "test-key", rerankTestBody)
	if w.Code != 429 || calls.Load() != 0 {
		t.Fatal(w.Code, w.Body.String(), calls.Load())
	}
	model := rerankTestModel("rank", "compatible")
	model.Context = r.InputEstimate - 1
	if supports(model, r) {
		t.Fatal("total input limit ignored")
	}
}

func TestRerankInvalidResultsStillRecordReportedUsage(t *testing.T) {
	for _, fragment := range []string{
		`"results":[{"index":3,"relevance_score":0.9}]`,
		`"results":[{"index":0,"relevance_score":1.1}]`,
		`"results":[{"index":0,"relevance_score":null}]`,
		`"results":[{"relevance_score":0.9}]`,
		`"results":[]`, `"results":null`,
		`"code":"InvalidParameter","results":[{"index":0,"relevance_score":0.9}]`,
		`"results":[{"index":0,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]`,
	} {
		t.Run(fragment, func(t *testing.T) {
			e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{`+fragment+`,"usage":{"total_tokens":79}}`)
			})
			configureRerank(t, e, rerankTestModel("rank", "compatible"))
			w := rerankCall(e, "/v1/rerank", "test-key", `{"model":"rerank","query":"x","documents":["a","b"],"top_n":1}`)
			if w.Code != 503 {
				t.Fatal(w.Code, w.Body.String())
			}
			counts, _ := e.Guard.Counters(context.Background(), "app", month(time.Now()))
			if counts.Tokens != 79 || counts.Micro != 79 || counts.ReservedTokens != 0 {
				t.Fatal(counts)
			}
		})
	}
	// 重复索引数量正确时也必须拒绝。
	r, _ := DecodeRequest([]byte(`{"model":"rerank","query":"x","documents":["a","b"]}`), "rerank")
	if _, err := normalizeRerank([]byte(`{"results":[{"index":0,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`), rerankTestModel("rank", "compatible"), r, "req", Usage{}, "estimated"); err == nil {
		t.Fatal("duplicate indices accepted")
	}
}

func TestRerankMissingAndInvalidUsageEstimated(t *testing.T) {
	for _, usage := range []string{"", `,"usage":{}`, `,"usage":{"total_tokens":null}`, `,"usage":{"total_tokens":-1}`, `,"usage":{"total_tokens":79,"prompt_tokens":5}`, `,"usage":{"total_tokens":79,"completion_tokens":1}`} {
		t.Run(usage, func(t *testing.T) {
			e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, `{"results":[{"index":0,"relevance_score":0.8}]`+usage+`}`)
			})
			configureRerank(t, e, rerankTestModel("rank", "compatible"))
			w := rerankCall(e, "/v1/rerank", "test-key", `{"model":"rerank","query":"x","documents":["a"]}`)
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"usage_quality":"estimated"`) || strings.Contains(w.Body.String(), `"document"`) {
				t.Fatal(w.Code, w.Body.String())
			}
			counts, _ := e.Guard.Counters(context.Background(), "app", month(time.Now()))
			if counts.Tokens != 0 || counts.Micro != 0 || counts.ReservedTokens <= 0 || counts.ReservedMicro <= 0 {
				t.Fatal(counts)
			}
		})
	}
}

func TestRerankFallbackLimitAndSeparateBilling(t *testing.T) {
	var calls atomic.Int32
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = fmt.Fprint(w, `{"results":[],"usage":{"total_tokens":7}}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"output":{"results":[{"index":0,"relevance_score":0.9}]},"usage":{"total_tokens":11}}`)
	})
	configureRerank(t, e, rerankTestModel("a", "compatible"), rerankTestModel("b", "dashscope"))
	w := rerankCall(e, "/v1/rerank", "test-key", `{"model":"rerank","query":"x","documents":["a"]}`)
	if w.Code != 200 || calls.Load() != 2 {
		t.Fatal(w.Code, w.Body.String(), calls.Load())
	}
	events, _ := e.Store.Records(context.Background(), Filter{RequestID: w.Header().Get("X-Request-Id")})
	ids := map[string]bool{}
	for _, event := range events {
		if event.Purpose == "inference" {
			ids[event.AttemptID] = true
		}
	}
	counts, _ := e.Guard.Counters(context.Background(), "app", month(time.Now()))
	if len(ids) != 2 || counts.Tokens != 18 || counts.Micro != 18 || counts.ReservedTokens != 0 {
		t.Fatal(ids, counts)
	}
	var unavailable atomic.Int32
	e2 := testEngine(t, func(w http.ResponseWriter, r *http.Request) { unavailable.Add(1); w.WriteHeader(503) })
	configureRerank(t, e2, rerankTestModel("a", "compatible"), rerankTestModel("b", "compatible"), rerankTestModel("c", "compatible"), rerankTestModel("d", "compatible"))
	w = rerankCall(e2, "/v1/rerank", "test-key", `{"model":"rerank","query":"x","documents":["a"]}`)
	if w.Code != 503 || unavailable.Load() != 3 {
		t.Fatal(w.Code, unavailable.Load())
	}
}

func TestRerankHTTPErrorAndBreaker(t *testing.T) {
	for _, status := range []int{400, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				_, _ = fmt.Fprint(w, `{"code":"upstream-error","message":"private-vendor-details"}`)
			})
			configureRerank(t, e, rerankTestModel("a", "compatible"), rerankTestModel("b", "compatible"))
			w := rerankCall(e, "/v1/rerank", "test-key", `{"model":"rerank","query":"x","documents":["a"]}`)
			want, attempts := 503, int32(2)
			if status == 400 {
				want, attempts = 400, 1
			}
			if w.Code != want || calls.Load() != attempts || strings.Contains(w.Body.String(), "private-vendor-details") {
				t.Fatal(w.Code, w.Body.String(), calls.Load())
			}
		})
	}
	var calls atomic.Int32
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) })
	configureRerank(t, e, rerankTestModel("a", "compatible"))
	for i := 0; i < 4; i++ {
		rerankCall(e, "/v1/rerank", "test-key", `{"model":"rerank","query":"x","documents":["a"]}`)
	}
	if calls.Load() != 3 {
		t.Fatal("open breaker did not skip rerank provider", calls.Load())
	}
}

func TestRerankCancellation(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
		// 读完请求体后，服务端才能及时观测客户端断开。
		var input any
		_ = json.NewDecoder(r.Body).Decode(&input)
		close(started)
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-time.After(3 * time.Second):
		}
	})
	configureRerank(t, e, rerankTestModel("rank", "compatible"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("POST", "/v1/rerank", strings.NewReader(`{"model":"rerank","query":"x","documents":["a"]}`)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer test-key")
	done := make(chan struct{})
	go func() { e.Handler().ServeHTTP(httptest.NewRecorder(), r); close(done) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream not started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not cancel")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream not cancelled")
	}
}

func TestAdminRerankModelAndRoutes(t *testing.T) {
	e := testEngine(t, MockProvider)
	a := NewAdmin(e, false, "")
	cookie, csrf := adminLogin(t, a, "admin")
	model := rerankTestModel("rank", "compatible")
	invalid := []Model{model, model, model, model, model, model}
	invalid[0].RerankProtocol = "unknown"
	invalid[1].MaxDocuments = 501
	invalid[2].MaxItemTokens = 0
	invalid[3].OutputMicro = 1
	invalid[4].MaxOutput = 1
	invalid[5].Capabilities = []string{"chat", "rerank"}
	for _, bad := range invalid {
		body, _ := json.Marshal(bad)
		if w := adminCall(a, cookie, csrf, "POST", "/admin/models", string(body)); w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	body, _ := json.Marshal(model)
	if w := adminCall(a, cookie, csrf, "POST", "/admin/models", string(body)); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	route := `{"alias":"rerank","kind":"rerank","strategy":"priority","candidates":[{"model_id":"rank","weight":100}]}`
	if w := adminCall(a, cookie, csrf, "PUT", "/admin/routes", route); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	model.RerankProtocol = "dashscope"
	body, _ = json.Marshal(model)
	if w := adminCall(a, cookie, csrf, "PUT", "/admin/models/rank", string(body)); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := adminCall(a, cookie, csrf, "PUT", "/admin/routes", strings.ReplaceAll(route, `"rank"`, `"primary"`)); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	developer, token := adminLogin(t, a, "dev")
	if w := adminCall(a, developer, token, "POST", "/admin/models", string(body)); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
}

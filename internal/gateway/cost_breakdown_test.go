package gateway

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCostBreakdownRoundingFXAndSaturation(t *testing.T) {
	models := []Model{
		{InputMicro: 1000000, CachedInputMicro: 100000, OutputMicro: 2000000, FXPPM: 7000000},
		{InputMicro: 1, CachedInputMicro: 1, OutputMicro: 1},
		{InputMicro: math.MaxInt64, CachedInputMicro: math.MaxInt64, OutputMicro: math.MaxInt64, FXPPM: math.MaxInt64},
		{},
	}
	for _, m := range models {
		for input := int64(0); input <= 30; input++ {
			for cached := int64(0); cached <= input; cached++ {
				u := Usage{Input: input, Output: 11, Total: input + 11, Cached: cached}
				e := Event{Kind: "completed", Model: m, Usage: u, CostMicro: Cost(m, u)}
				parts := breakdown(e)
				if !parts.Known || parts.InputMicro < 0 || parts.CachedInputMicro < 0 || parts.OutputMicro < 0 || parts.InputMicro+parts.CachedInputMicro+parts.OutputMicro != e.CostMicro {
					t.Fatalf("invalid breakdown: model=%+v usage=%+v parts=%+v", m, u, parts)
				}
			}
		}
	}
	m := models[0]
	u := Usage{Input: 100, Output: 10, Total: 110, Cached: 50}
	got := breakdown(Event{Kind: "completed", Model: m, Usage: u, CostMicro: Cost(m, u)})
	if got.InputMicro != 350 || got.CachedInputMicro != 35 || got.OutputMicro != 140 {
		t.Fatal(got)
	}
	// 三项各自取整会得到 3；现有账本只收 1 微单位。
	m = models[1]
	u = Usage{Input: 2, Cached: 1, Output: 1, Total: 3}
	got = breakdown(Event{Kind: "completed", Model: m, Usage: u, CostMicro: Cost(m, u)})
	if got.InputMicro != 1 || got.CachedInputMicro != 0 || got.OutputMicro != 0 {
		t.Fatal(got)
	}
}

func TestCostBreakdownUnknownPreservesLedgerAndPayload(t *testing.T) {
	for _, e := range []Event{
		{Kind: "started", Usage: Usage{}, CostMicro: 0},
		{Kind: "completed", Usage: Usage{Total: 10}, CostMicro: 23},
		{Kind: "completed", Usage: Usage{Input: 10, Total: 10}, CostMicro: 23},
	} {
		e.PayloadCipher = "private"
		before, _ := json.Marshal(e)
		view := recordViews([]Event{e})[0]
		after, _ := json.Marshal(e)
		if view.CostBreakdown.Known || view.CostBreakdown.UnallocatedMicro != e.CostMicro || view.PayloadCipher != "" || string(before) != string(after) {
			t.Fatal(view)
		}
	}
}

func TestExtractUsageMissingCacheAndInput(t *testing.T) {
	for _, tc := range []struct {
		raw                   string
		ok, cacheKnown        bool
		input, output, cached int64
	}{
		{`{"usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_tokens_details":{"cached_tokens":40}}}`, true, true, 100, 10, 40},
		{`{"usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_tokens_details":{"cached_tokens":0}}}`, true, true, 100, 10, 0},
		{`{"usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_cache_hit_tokens":40}}`, true, true, 100, 10, 40},
		{`{"usage":{"prompt_tokens":100,"completion_tokens":10}}`, true, false, 100, 10, 0},
		{`{"usage":{"prompt_tokens":100,"total_tokens":100}}`, true, false, 100, 0, 0},
		{`{"usage":{"total_tokens":100}}`, false, false, 0, 0, 0},
		{`{"usage":{}}`, false, false, 0, 0, 0},
		{`{"usage":{"prompt_tokens":100,"total_tokens":110}}`, false, false, 0, 0, 0},
		{`{"usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_cache_hit_tokens":-1}}`, false, false, 0, 0, 0},
	} {
		u, ok := ExtractUsage([]byte(tc.raw))
		if ok != tc.ok || u.CachedReported != tc.cacheKnown || u.Input != tc.input || u.Output != tc.output || u.Cached != tc.cached {
			t.Fatalf("%s: %+v %v", tc.raw, u, ok)
		}
	}
}

func breakdownFixtures() []Event {
	m := Model{ID: "detail", InputMicro: 1000000, CachedInputMicro: 100000, OutputMicro: 2000000, PriceVersion: "old-price", Currency: "USD", FXPPM: 7000000}
	e := Event{ID: "detail:2", AttemptID: "detail", RequestID: "detail-request", AppID: "app", Department: "engineering", Purpose: "inference", Kind: "completed", Status: "success", Quality: "reported", Revision: 2, At: time.Now().UTC(), Model: m, Usage: Usage{Input: 100, Output: 10, Cached: 50, Total: 110, CachedReported: true}, CostMicro: 525}
	emb := e
	emb.ID, emb.AttemptID, emb.Purpose, emb.Model.ID = "emb:2", "emb", "cache_embedding", "embedding"
	emb.Usage = Usage{Input: 10, Total: 10}
	emb.CostMicro = Cost(emb.Model, emb.Usage)
	request := e
	request.ID, request.AttemptID, request.Purpose, request.Model = "detail-request:result", "detail-request", "request", Model{}
	request.Cached, request.Usage, request.CostMicro = true, Usage{}, 0
	legacy := e
	legacy.ID, legacy.AttemptID, legacy.Usage, legacy.CostMicro = "legacy:2", "legacy", Usage{Total: 20}, 13
	pending := e
	pending.ID, pending.AttemptID, pending.Kind, pending.Status, pending.Quality, pending.Usage, pending.CostMicro = "pending:1", "pending", "started", "started", "pending", Usage{}, 0
	return []Event{e, emb, request, legacy, pending}
}

func TestCostReportBreakdownCorrectionAndFilters(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	fixtures := breakdownFixtures()
	for _, e := range fixtures {
		if err := s.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	// 重复事件及旧 revision 不改变分项；修正保留历史价格快照。
	fixed := fixtures[0]
	fixed.Revision = 3
	fixed.ID = fixed.AttemptID + ":3"
	fixed.Usage = Usage{Input: 200, Cached: 100, Output: 20, Total: 220}
	fixed.Quality = "reconciled"
	fixed.CostMicro = Cost(fixed.Model, fixed.Usage)
	for _, e := range []Event{fixed, fixed, fixtures[0]} {
		if err := s.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.Report(ctx, Filter{AppID: "app", ModelID: "detail", Offset: 99})
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	r := rows[0]
	if r.Tokens != 240 || r.InputTokens != 200 || r.CachedTokens != 100 || r.UncachedInputTokens != 100 || r.OutputTokens != 20 || r.Micro != 1063 || r.InputMicro != 700 || r.CachedInputMicro != 70 || r.OutputMicro != 280 || r.UnallocatedMicro != 13 || r.UnallocatedCount != 2 || r.Pending != 1 {
		t.Fatal(r)
	}
	rows, err = s.Report(ctx, Filter{ModelID: "embedding"})
	if err != nil || len(rows) != 1 || rows[0].CacheHits != 1 || rows[0].CacheEmbeddingMicro != rows[0].Micro || rows[0].CacheEmbeddingTokens != 10 || rows[0].CacheEmbeddingCount != 1 {
		t.Fatal(rows, err)
	}
	rows, err = s.Report(ctx, Filter{AppID: "other"})
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
}

func TestAdminBreakdownRecordsTraceAndCSV(t *testing.T) {
	e := testEngine(t, MockProvider)
	for _, event := range breakdownFixtures() {
		if err := e.Store.Apply(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	a := NewAdmin(e, false, "")
	cookie, csrf := adminLogin(t, a, "dev")
	for _, path := range []string{"/admin/call-records?app_id=app", "/admin/trace/detail-request?app_id=app"} {
		w := adminCall(a, cookie, csrf, "GET", path, "")
		var rows []RecordView
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &rows) != nil || len(rows) != 5 {
			t.Fatal(w.Code, w.Body.String())
		}
		for _, r := range rows {
			if r.AttemptID == "detail" && (r.CostBreakdown.InputMicro != 350 || r.Model.PriceVersion != "old-price" || !r.CacheReported) {
				t.Fatal(r)
			}
		}
	}
	w := adminCall(a, cookie, csrf, "GET", "/admin/cost-report?app_id=app&model_id=detail&format=csv", "")
	rows, err := csv.NewReader(strings.NewReader(w.Body.String())).ReadAll()
	if w.Code != 200 || err != nil || len(rows) != 2 {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	if !reflect.DeepEqual(rows[0][:7], []string{"app", "department", "model", "attempts", "tokens", "cost_cny", "pending"}) || len(rows[0]) != len(rows[1]) || rows[1][5] != "0.000538" || rows[1][11] != "0.000350" || rows[1][14] != "0.000013" {
		t.Fatal(rows)
	}
	if got := microCNY(math.MaxInt64); got != "9223372036854.775807" {
		t.Fatal(got)
	}
}

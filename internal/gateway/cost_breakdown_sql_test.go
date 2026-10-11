package gateway

import (
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestCostBreakdownMySQLParity(t *testing.T) {
	if os.Getenv("GW_RUN_COST_SQL_TESTS") != "1" {
		t.Skip("requires a dedicated local MySQL database via GW_COST_SQL_DSN")
	}
	dsn := os.Getenv("GW_COST_SQL_DSN")
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.Net != "tcp" || !regexp.MustCompile(`^127\.0\.0\.1:\d+$`).MatchString(cfg.Addr) || !regexp.MustCompile(`^gw_cost_test_[a-f0-9]{12}$`).MatchString(cfg.DBName) {
		t.Fatal("requires a dedicated gw_cost_test_<12 hex> database on localhost")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, err := NewSQLStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.DB.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM gw_attempts").Scan(&count); err != nil || count != 0 {
		t.Fatal("test requires an empty dedicated ledger", err)
	}
	memory := NewMemoryStore()
	fixtures := breakdownFixtures()
	base := fixtures[0]
	for i, m := range []Model{
		{ID: "tiny", InputMicro: 1, CachedInputMicro: 1, OutputMicro: 1},
		{ID: "fx-fraction", InputMicro: 19, CachedInputMicro: 7, OutputMicro: 31, FXPPM: 1234567},
		{ID: "free"},
		{ID: "saturated", InputMicro: math.MaxInt64, CachedInputMicro: math.MaxInt64, OutputMicro: math.MaxInt64, FXPPM: math.MaxInt64},
	} {
		e := base
		e.AttemptID, e.ID, e.Model = fmt.Sprintf("extra-%d", i), fmt.Sprintf("extra-%d:2", i), m
		e.Usage = Usage{Input: 2, Cached: 1, Output: 1, Total: 3, CachedReported: true}
		e.CostMicro = Cost(m, e.Usage)
		fixtures = append(fixtures, e)
	}
	// 非零与未返回之外，明确返回零也须在 MySQL 中保持可辨识。
	zero := base
	zero.AttemptID, zero.ID = "zero-cache", "zero-cache:2"
	zero.Usage = Usage{Input: 1, Total: 1, CachedReported: true}
	zero.CostMicro = Cost(zero.Model, zero.Usage)
	fixtures = append(fixtures, zero)
	for _, e := range fixtures {
		if err := s.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
		if err := memory.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	assertParity := func() {
		t.Helper()
		for _, filter := range []Filter{{}, {ModelID: "detail"}, {AppID: "app", ModelID: "embedding"}, {AppID: "other"}, {RequestID: base.RequestID}, {From: base.At.Add(-time.Second), To: base.At.Add(time.Second)}} {
			actual, err := s.Report(ctx, filter)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := memory.Report(ctx, filter)
			if err != nil {
				t.Fatal(err)
			}
			less := func(rows []Report) {
				sort.Slice(rows, func(i, j int) bool { return rows[i].ModelID < rows[j].ModelID })
			}
			less(actual)
			less(expected)
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("filter=%+v\nSQL=%+v\nmemory=%+v", filter, actual, expected)
			}
		}
	}
	assertParity()
	fixed := base
	fixed.Revision, fixed.ID, fixed.Quality = 3, "detail:3", "reconciled"
	fixed.Usage = Usage{Input: 200, Cached: 100, Output: 20, Total: 220}
	fixed.CostMicro = Cost(fixed.Model, fixed.Usage)
	for _, e := range []Event{fixed, fixed, base} {
		if err := s.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
		if err := memory.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	assertParity()
}

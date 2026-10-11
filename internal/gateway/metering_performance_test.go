package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestMySQLInterpolationPreservesExplicitAndNonUTF8Settings(t *testing.T) {
	for _, item := range []struct {
		query string
		want  bool
	}{
		{"charset=utf8mb4", true},
		{"charset=utf8mb4&interpolateParams=false", false},
		{"charset=utf8mb4&interpolateParams=true", true},
		{"charset=utf8mb4&collation=utf8mb4_bin", true},
		{"charset=utf8mb4&collation=latin1_bin", false},
		{"charset=gbk", false}, {"charset=utf8mb4,gbk", false}, {"parseTime=true", false},
	} {
		t.Run(item.query, func(t *testing.T) {
			cfg, err := gatewayMySQLConfig("user:password@tcp(localhost:3306)/gateway?" + item.query)
			if err != nil || cfg.InterpolateParams != item.want {
				t.Fatal("driver encoding or explicit setting changed", cfg, err)
			}
		})
	}
}

func TestMeteringDiagnosticLabelsAreBounded(t *testing.T) {
	m := NewMetrics()
	for i := 0; i < 100; i++ {
		m.meteringStage("outbox_save", fmt.Sprintf("invalid-kind-%d", i), time.Now(), nil)
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "gateway_metering_stage_seconds" {
			continue
		}
		if len(family.Metric) != 1 || family.Metric[0].Histogram.GetSampleCount() != 100 {
			t.Fatal("untrusted event kinds created diagnostic series", family)
		}
		for _, label := range family.Metric[0].Label {
			if label.GetName() == "app" || label.GetName() == "attempt_id" {
				t.Fatal("unbounded identifier in diagnostic metric")
			}
		}
		return
	}
	t.Fatal("metering diagnostics missing")
}

type fixedArrivalSample struct {
	Rate             int     `json:"offered_rps"`
	OfferedSeconds   float64 `json:"offered_seconds"`
	Stream           bool    `json:"stream"`
	SlowUpstream     bool    `json:"slow_upstream"`
	Applications     int     `json:"applications"`
	Requests         int     `json:"requests"`
	Accepted         int     `json:"accepted"`
	Rejected         int     `json:"protected_rejections"`
	SchedulerDropped int     `json:"scheduler_dropped"`
	Failed           int     `json:"protocol_failures"`
	AcceptedP95MS    float64 `json:"accepted_p95_ms"`
	AcceptedP99MS    float64 `json:"accepted_p99_ms"`
	FirstP95MS       float64 `json:"accepted_first_event_p95_ms,omitempty"`
	WallSeconds      float64 `json:"wall_seconds"`
	DrainSeconds     float64 `json:"ledger_drain_seconds"`
	MaxDispatchLagMS float64 `json:"max_dispatch_lag_ms"`
}

// 固定到达率不等待上一请求完成；最多 32 个客户端在途，调度丢弃单独统计。
func runFixedArrival(t *testing.T, ctx context.Context, rate int, duration time.Duration, stream, slow bool,
	call func(int) reliabilityResult,
) fixedArrivalSample {
	t.Helper()
	sample := fixedArrivalSample{Rate: rate, OfferedSeconds: duration.Seconds(), Stream: stream, SlowUpstream: slow, Applications: 2, Requests: int(duration.Seconds()) * rate}
	results := make(chan reliabilityResult, sample.Requests)
	slots := make(chan struct{}, 32)
	interval := time.Second / time.Duration(rate)
	start := time.Now()
	for i := 0; i < sample.Requests; i++ {
		due := start.Add(time.Duration(i) * interval)
		if wait := time.Until(due); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				t.Fatal(ctx.Err())
			}
		}
		lag := float64(time.Since(due).Microseconds()) / 1000
		if lag > sample.MaxDispatchLagMS {
			sample.MaxDispatchLagMS = lag
		}
		select {
		case slots <- struct{}{}:
			go func(index int) {
				defer func() { <-slots }()
				results <- call(index)
			}(i)
		default:
			sample.SchedulerDropped++
		}
	}
	latencies, firsts := []float64{}, []float64{}
	for i := 0; i < sample.Requests-sample.SchedulerDropped; i++ {
		var result reliabilityResult
		select {
		case result = <-results:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if result.Error == "" && result.Status == 503 && (strings.Contains(result.Body, `"governance_unavailable"`) || strings.Contains(result.Body, `"overloaded"`)) {
			sample.Rejected++
		} else if result.Status != 200 || result.Error != "" || stream && !strings.HasSuffix(result.Body, "data: [DONE]\n\n") || !stream && !strings.Contains(result.Body, `"choices"`) {
			sample.Failed++
			if sample.Failed <= 3 {
				t.Logf("fixed-arrival protocol failure: %+v", result)
			}
		} else {
			sample.Accepted++
			latencies = append(latencies, result.ElapsedMS)
			if stream {
				firsts = append(firsts, result.FirstMS)
			}
		}
	}
	sample.WallSeconds = time.Since(start).Seconds()
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		sample.AcceptedP95MS = latencies[(len(latencies)-1)*95/100]
		sample.AcceptedP99MS = latencies[(len(latencies)-1)*99/100]
	}
	if len(firsts) > 0 {
		sort.Float64s(firsts)
		sample.FirstP95MS = firsts[(len(firsts)-1)*95/100]
	}
	return sample
}

func writeFixedArrivalSamples(t *testing.T, dir string, samples []fixedArrivalSample) {
	t.Helper()
	raw, err := json.MarshalIndent(samples, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "sustained.json"), raw, 0600)
	}
	if err != nil {
		t.Fatal(err)
	}
}

package gateway

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestAPIMetricsCountRequestWithoutRetryDuplication(t *testing.T) {
	e := testEngine(t, MockProvider)
	w := call(t, e, "[fail-primary] metrics", false)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var metric dto.Metric
	if err := e.Metrics.APICalls.WithLabelValues("app", "/v1/chat/completions", "false", "success", "200", "").Write(&metric); err != nil {
		t.Fatal(err)
	}
	count := metric.GetCounter().GetValue()
	if count != 1 {
		t.Fatalf("fallback counted as multiple requests: %v", count)
	}
	families, err := e.Metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "gateway_tokens_total" {
			for _, metric := range family.Metric {
				quality := ""
				for _, label := range metric.Label {
					if label.GetName() == "quality" {
						quality = label.GetValue()
					}
				}
				if quality != "estimated" && quality != "reported" {
					t.Fatal("usage quality omitted", quality)
				}
			}
		}
	}
}

func TestTimeoutAndPartialStreamUsageRetainBudget(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			e := testEngine(t, func(w http.ResponseWriter, r *http.Request) {
				if !stream {
					w.WriteHeader(http.StatusRequestTimeout)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n")
			})
			_ = call(t, e, "timeout", stream)
			rows, _ := e.Store.Records(context.Background(), Filter{})
			for _, event := range rows {
				if event.Purpose == "inference" && event.Quality != "estimated" {
					t.Fatal("unconfirmed vendor cost became reported", event)
				}
			}
			counts, _ := e.Guard.Counters(context.Background(), "app", month(time.Now()))
			if counts.ReservedMicro <= 0 {
				t.Fatal("uncertain timeout/partial usage refunded", counts)
			}
		})
	}
}

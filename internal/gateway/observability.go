package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type Metrics struct {
	MeteringLag           prometheus.Gauge
	MeteringPermit        prometheus.Gauge
	MeteringUnchecked     prometheus.Gauge
	MeteringAuditLast     prometheus.Gauge
	MeteringAuditSeconds  prometheus.Histogram
	MeteringAuditFailures prometheus.Counter
	MeteringRotation      prometheus.Gauge
	MeteringOutboxBytes   prometheus.Gauge
	Registry              *prometheus.Registry
	Requests              *prometheus.CounterVec
	Attempts              *prometheus.CounterVec
	Tokens                *prometheus.CounterVec
	Cost                  *prometheus.CounterVec
	Latency               *prometheus.HistogramVec
	Upstream              *prometheus.HistogramVec
	First                 *prometheus.HistogramVec
	CacheOps              *prometheus.CounterVec
	Events                prometheus.Counter
	Active                prometheus.Gauge
	Config                prometheus.Gauge
	Overhead              *prometheus.HistogramVec
	Overload              *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry()}
	m.MeteringLag = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_lag_records", Help: "Captured Kafka end minus committed positions"})
	m.MeteringPermit = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_permit_expires_seconds", Help: "Last successful permit expiry, zero on unavailable evidence"})
	m.MeteringUnchecked = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_unchecked_apps", Help: "Applications without a recent barrier audit"})
	m.MeteringAuditLast = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_last_audit_seconds", Help: "Last successful application audit timestamp"})
	m.MeteringAuditSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "gateway_metering_audit_seconds", Help: "Application barrier and comparison duration", Buckets: []float64{.1, 1, 5, 15, 60, 120, 150}})
	m.MeteringAuditFailures = prometheus.NewCounter(prometheus.CounterOpts{Name: "gateway_metering_audit_failures_total", Help: "Failed application audits; application stays frozen"})
	m.MeteringRotation = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_rotation_seconds", Help: "Last completed application cursor rotation duration"})
	m.MeteringOutboxBytes = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_outbox_bytes", Help: "Global logical outbox payload plus completion reservations"})
	m.Registry.MustRegister(m.MeteringRotation, m.MeteringOutboxBytes)
	m.Registry.MustRegister(m.MeteringLag, m.MeteringPermit, m.MeteringUnchecked, m.MeteringAuditLast, m.MeteringAuditSeconds, m.MeteringAuditFailures)
	m.Requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_requests_total", Help: "Completed gateway requests"}, []string{"app", "alias", "status", "cached"})
	m.Attempts = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_attempts_total", Help: "Upstream attempts"}, []string{"app", "model", "purpose", "status", "quality"})
	m.Tokens = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_tokens_total", Help: "Observed upstream token consumption"}, []string{"app", "model", "kind"})
	m.Cost = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_cost_micro_total", Help: "Observed cost in CNY micro units"}, []string{"app", "model"})
	b := []float64{.001, .005, .01, .03, .05, .15, .5, 1, 2, 5, 15, 60, 120}
	m.Latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_request_seconds", Help: "End to end gateway request latency", Buckets: b}, []string{"app", "alias"})
	m.Upstream = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_attempt_seconds", Help: "Upstream attempt duration", Buckets: b}, []string{"model"})
	m.First = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_first_token_seconds", Help: "Attempt to first forwarded event", Buckets: b}, []string{"model"})
	m.CacheOps = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_cache_operations_total", Help: "Cache lookup outcomes"}, []string{"result"})
	m.Events = prometheus.NewCounter(prometheus.CounterOpts{Name: "gateway_event_publish_errors_total", Help: "Durable event publication failures"})
	m.Active = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_active_requests", Help: "Active authenticated requests"})
	m.Config = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_config_version", Help: "Applied config version"})
	m.Overhead = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_overhead_seconds", Help: "Attempt wall time minus upstream read and forwarding time; includes metering acknowledgements", Buckets: b}, []string{"purpose"})
	m.Overload = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_overload_rejections_total", Help: "Requests rejected before processing because instance concurrency is full"}, []string{"role"})
	m.Registry.MustRegister(m.Requests, m.Attempts, m.Tokens, m.Cost, m.Latency, m.Upstream, m.First, m.CacheOps, m.Events, m.Active, m.Config, m.Overhead, m.Overload, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}
func (m *Metrics) Handler() http.Handler {
	h := promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2, Timeout: 5 * time.Second})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(&deadlineWriter{w, 5 * time.Second}, r) })
}
func (m *Metrics) Request(app, alias, status string, cached bool, t time.Duration) {
	v := "false"
	if cached {
		v = "true"
	}
	m.Requests.WithLabelValues(app, alias, status, v).Inc()
	m.Latency.WithLabelValues(app, alias).Observe(t.Seconds())
}
func (m *Metrics) Attempt(e Event) {
	m.Attempts.WithLabelValues(e.AppID, e.Model.ID, e.Purpose, e.Status, e.Quality).Inc()
	m.Tokens.WithLabelValues(e.AppID, e.Model.ID, "input").Add(float64(e.Usage.Input))
	m.Tokens.WithLabelValues(e.AppID, e.Model.ID, "output").Add(float64(e.Usage.Output))
	m.Tokens.WithLabelValues(e.AppID, e.Model.ID, "cached_input").Add(float64(e.Usage.Cached))
	m.Cost.WithLabelValues(e.AppID, e.Model.ID).Add(float64(e.CostMicro))
	m.Upstream.WithLabelValues(e.Model.ID).Observe(float64(e.DurationMS) / 1000)
	if e.FirstMS > 0 {
		m.First.WithLabelValues(e.Model.ID).Observe(float64(e.FirstMS) / 1000)
	}
}
func (m *Metrics) Cache(err error, hit bool) {
	x := "miss"
	if hit {
		x = "hit"
	}
	if err != nil {
		x = "error"
	}
	m.CacheOps.WithLabelValues(x).Inc()
}
func (m *Metrics) EventError() { m.Events.Inc() }

var sensitiveRE = regexp.MustCompile(`(?i)(sk[-_][a-z0-9_-]{12,}|[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}|\b1[3-9][0-9]{9}\b|\b[0-9]{17}[0-9x]\b)`)

func Safety(rules []Rule, text string) (sensitive, blocked bool) {
	sensitive = sensitiveRE.MatchString(text)
	for _, r := range rules {
		re, e := regexp.Compile(r.Pattern)
		if e == nil && re.MatchString(text) {
			if r.Action == "block" {
				blocked = true
			} else {
				sensitive = true
			}
		}
	}
	return
}
func Scrub(text string) string {
	return sensitiveRE.ReplaceAllString(strings.ReplaceAll(text, "\x00", ""), "[REDACTED]")
}
func sampledText(text string) string {
	text = Scrub(text)
	if len(text) > 32768 {
		text = text[:32768]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	return text
}

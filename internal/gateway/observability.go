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
	MeteringLag             prometheus.Gauge
	MeteringPermit          prometheus.Gauge
	MeteringUnchecked       prometheus.Gauge
	MeteringAuditLast       prometheus.Gauge
	MeteringAuditSeconds    prometheus.Histogram
	MeteringAuditFailures   prometheus.Counter
	MeteringRotation        prometheus.Gauge
	MeteringOutboxBytes     prometheus.Gauge
	MeteringStages          *prometheus.HistogramVec
	MeteringRejections      *prometheus.CounterVec
	MeteringCommitBatch     prometheus.Histogram
	Registry                *prometheus.Registry
	Requests                *prometheus.CounterVec
	Attempts                *prometheus.CounterVec
	Tokens                  *prometheus.CounterVec
	Cost                    *prometheus.CounterVec
	Latency                 *prometheus.HistogramVec
	Upstream                *prometheus.HistogramVec
	First                   *prometheus.HistogramVec
	CacheOps                *prometheus.CounterVec
	Events                  prometheus.Counter
	Active                  prometheus.Gauge
	Config                  prometheus.Gauge
	Overhead                *prometheus.HistogramVec
	Overload                *prometheus.CounterVec
	Calls                   *prometheus.CounterVec
	QPSLimits               *prometheus.GaugeVec
	APICalls                *prometheus.CounterVec
	APILatency              *prometheus.HistogramVec
	MeteringStates          *prometheus.GaugeVec
	MeteringCompensationAge prometheus.Gauge
	MeteringHeldMicro       prometheus.Gauge
	MeteringSnapshot        prometheus.Gauge
	MeteringOutboxLimit     prometheus.Gauge
	MeteringGC              prometheus.Counter
	MeteringGCFailures      prometheus.Counter
	appState                appMetricState
}

func NewMetrics() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry()}
	m.Calls = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_calls_total", Help: "Client model calls including early and streamed failures, without upstream retry duplication", ConstLabels: prometheus.Labels{"source": ID("metrics_")}}, []string{"app", "outcome", "http_status", "error_code"})
	m.QPSLimits = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gateway_app_qps_limit", Help: "Applied application token-bucket QPS limit; zero means unlimited"}, []string{"app"})
	m.Registry.MustRegister(m.Calls, m.QPSLimits)
	m.initializeCalls("_unknown")
	m.APICalls = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_api_calls_total", Help: "One client call per API; stream unknown before body validation, protective rejects remain errors", ConstLabels: prometheus.Labels{"source": ID("api_metrics_")}}, []string{"app", "api", "stream", "outcome", "http_status", "error_code"})
	m.APILatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_api_seconds", Help: "Client request wall time including early refusals and cache hits", Buckets: []float64{.001, .005, .01, .03, .1, .3, 1, 3, 10, 30, 60, 120}}, []string{"app", "api", "stream"})
	m.MeteringStates = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gateway_metering_attempt_records", Help: "SQL lifecycle snapshot; use max across worker replicas"}, []string{"state"})
	m.MeteringCompensationAge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_compensation_oldest_seconds", Help: "Age of oldest pending cancellation"})
	m.MeteringHeldMicro = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_held_micro", Help: "SQL expected held exposure in CNY micro, not confirmed charges"})
	m.MeteringSnapshot = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_snapshot_seconds", Help: "Last successful lifecycle snapshot time; stale snapshots are not healthy evidence"})
	m.MeteringOutboxLimit = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_metering_outbox_limit_bytes", Help: "Configured global logical outbox limit; excludes metadata/indexes"})
	m.MeteringGC = prometheus.NewCounter(prometheus.CounterOpts{Name: "gateway_metering_gc_removed_total", Help: "Delivered outbox metadata rows removed with persistent ledger proof"})
	m.MeteringGCFailures = prometheus.NewCounter(prometheus.CounterOpts{Name: "gateway_metering_gc_errors_total", Help: "Failed bounded outbox cleanup operations"})
	m.Registry.MustRegister(m.APICalls, m.APILatency, m.MeteringStates, m.MeteringCompensationAge, m.MeteringHeldMicro, m.MeteringSnapshot, m.MeteringOutboxLimit, m.MeteringGC, m.MeteringGCFailures)
	m.MeteringStages = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_metering_stage_seconds", Help: "Metering operation duration including failed operations", Buckets: []float64{.001, .005, .01, .03, .05, .1, .25, .5, 1, 2, 5}}, []string{"stage", "kind", "result"})
	m.MeteringRejections = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_metering_rejections_total", Help: "Metering lifecycle admission rejection reasons"}, []string{"reason"})
	m.MeteringCommitBatch = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "gateway_metering_commit_batch_records", Help: "Contiguously settled records included in successful offset commits", Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256}})
	m.Registry.MustRegister(m.MeteringStages, m.MeteringRejections, m.MeteringCommitBatch)
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
	m.Tokens = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_tokens_total", Help: "Observed token consumption by usage quality; estimates are not confirmed"}, []string{"app", "model", "kind", "quality"})
	m.Cost = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_cost_micro_total", Help: "Observed cost estimates by quality, not a budget settlement ledger"}, []string{"app", "model", "quality"})
	b := []float64{.001, .005, .01, .03, .05, .15, .5, 1, 2, 5, 15, 60, 120}
	m.Latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_request_seconds", Help: "End to end gateway request latency", Buckets: b}, []string{"app", "alias"})
	m.Upstream = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_attempt_seconds", Help: "Attempt duration including admission before provider", Buckets: b}, []string{"app", "model"})
	m.First = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_first_token_seconds", Help: "Attempt to first forwarded event", Buckets: b}, []string{"app", "model"})
	m.CacheOps = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_cache_operations_total", Help: "Cache lookup outcomes"}, []string{"result"})
	m.Events = prometheus.NewCounter(prometheus.CounterOpts{Name: "gateway_event_publish_errors_total", Help: "Durable event publication failures"})
	m.Active = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_active_requests", Help: "Active authenticated requests"})
	m.Config = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_config_version", Help: "Applied config version"})
	m.Overhead = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_overhead_seconds", Help: "Attempt wall time minus upstream read and forwarding time; includes metering acknowledgements", Buckets: b}, []string{"purpose"})
	m.Overload = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_overload_rejections_total", Help: "Requests rejected before processing because instance concurrency is full"}, []string{"role"})
	m.Registry.MustRegister(m.Requests, m.Attempts, m.Tokens, m.Cost, m.Latency, m.Upstream, m.First, m.CacheOps, m.Events, m.Active, m.Config, m.Overhead, m.Overload, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}

// 诊断标签只使用固定阶段、事件种类和结果，避免应用或事件数量扩大指标基数。
func (m *Metrics) meteringStage(stage, kind string, start time.Time, err error) {
	if m == nil {
		return
	}
	if kind != "started" && kind != "completed" && kind != "request" {
		kind = "other"
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.MeteringStages.WithLabelValues(stage, kind, result).Observe(time.Since(start).Seconds())
}

func meteringEventKind(e Event) string {
	if e.Purpose == "request" {
		return "request"
	}
	return e.Kind
}

func (m *Metrics) meteringRejected(reason string) {
	if m != nil {
		m.MeteringRejections.WithLabelValues(reason).Inc()
	}
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
	m.Tokens.WithLabelValues(e.AppID, e.Model.ID, "input", e.Quality).Add(float64(e.Usage.Input))
	m.Tokens.WithLabelValues(e.AppID, e.Model.ID, "output", e.Quality).Add(float64(e.Usage.Output))
	m.Tokens.WithLabelValues(e.AppID, e.Model.ID, "cached_input", e.Quality).Add(float64(e.Usage.Cached))
	m.Cost.WithLabelValues(e.AppID, e.Model.ID, e.Quality).Add(float64(e.CostMicro))
	m.Upstream.WithLabelValues(e.AppID, e.Model.ID).Observe(float64(e.DurationMS) / 1000)
	if e.FirstMS > 0 {
		m.First.WithLabelValues(e.AppID, e.Model.ID).Observe(float64(e.FirstMS) / 1000)
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

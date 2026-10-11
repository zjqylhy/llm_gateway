package gateway

import (
	"net/http"
	"strconv"
	"sync"
)

// 错误标签由网关定义，避免任意上游错误文本造成高基数。
var callErrorStatuses = map[string]int{
	"invalid_api_key": 401, "request_too_large": 413, "invalid_request_error": 400,
	"model_not_allowed": 403, "model_not_found": 404, "safety_blocked": 400,
	"unsupported_capability": 400, "upstream_invalid_request": 400,
	"upstream_unavailable": 503, "metering_unavailable": 503, "governance_unavailable": 503,
	"insufficient_quota": 429, "app_qps_exceeded": 429, "gateway_overloaded": 503,
	"cache_invalid": 503, "stream_interrupted": 200,
	"client_write_failed": 200, "http_error": 500,
}

type appMetricState struct {
	sync.Mutex
	apps map[string]bool
}

func (m *Metrics) initializeCalls(app string) {
	m.Calls.WithLabelValues(app, "success", "200", "").Add(0)
	// SSE 已发送响应头时，计量错误仍沿用客户端收到的业务错误码。
	m.Calls.WithLabelValues(app, "error", "200", "metering_unavailable").Add(0)
	for code, status := range callErrorStatuses {
		m.Calls.WithLabelValues(app, "error", strconv.Itoa(status), code).Add(0)
	}
}

func (m *Metrics) refreshApps(cfg Config) {
	m.appState.Lock()
	defer m.appState.Unlock()
	if m.appState.apps == nil {
		m.appState.apps = map[string]bool{}
	}
	for id, app := range cfg.Apps {
		if !m.appState.apps[id] {
			m.initializeCalls(id)
		}
		m.QPSLimits.WithLabelValues(id).Set(float64(app.QPS))
	}
	for id := range m.appState.apps {
		if _, exists := cfg.Apps[id]; !exists {
			m.QPSLimits.DeleteLabelValues(id)
		}
	}
	m.appState.apps = make(map[string]bool, len(cfg.Apps))
	for id := range cfg.Apps {
		m.appState.apps[id] = true
	}
}

type callWriter struct {
	http.ResponseWriter
	status  int
	written bool
	code    string
}

func (w *callWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *callWriter) WriteHeader(status int) {
	if !w.written {
		w.status, w.written = status, true
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *callWriter) Write(body []byte) (int, error) {
	if !w.written {
		w.WriteHeader(200)
	}
	n, err := w.ResponseWriter.Write(body)
	if err != nil {
		w.code = "client_write_failed"
	}
	return n, err
}
func (w *callWriter) FlushError() error {
	if !w.written {
		w.WriteHeader(200)
	}
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if err != nil {
		w.code = "client_write_failed"
	}
	return err
}
func (w *callWriter) Flush() { _ = w.FlushError() }

func observeCallError(w http.ResponseWriter, code string) {
	for i := 0; i < 16; i++ {
		if observed, ok := w.(*callWriter); ok {
			observed.code = code
			return
		}
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return
		}
		w = unwrapper.Unwrap()
	}
}

func (e *Engine) observeCalls(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || !Contains([]string{"/v1/chat/completions", "/v1/embeddings", "/v1/rerank", "/v1/reranks"}, r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		// 仅读取本机配置快照，不读取正文或申请付费槽，过载也能归属应用。
		appID := "_unknown"
		if app, _, valid := e.authenticate(r); valid {
			appID = app.ID
		}
		observed := &callWriter{ResponseWriter: w, status: 200}
		defer func() {
			outcome := "success"
			code := observed.code
			if code != "" || observed.status >= 400 || r.Context().Err() != nil {
				outcome = "error"
				if code == "" {
					code = "http_error"
				}
				if _, known := callErrorStatuses[code]; !known {
					code = "http_error"
				}
			}
			e.Metrics.Calls.WithLabelValues(appID, outcome, strconv.Itoa(observed.status), code).Inc()
		}()
		next.ServeHTTP(observed, r)
	})
}

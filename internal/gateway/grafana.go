package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const grafanaPrefix = "/admin/grafana/"
const applicationDashboard = "gateway-applications"

type GrafanaProxy struct {
	URL                *url.URL
	Username, Password string
	Client             *http.Client
	requests           chan struct{}
	queries            chan struct{}
}

func newGrafanaProxy(raw, username, password string) (*GrafanaProxy, error) {
	if raw == "" {
		return nil, nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || username == "" || password == "" {
		return nil, errors.New("Grafana requires a root upstream URL and dedicated Viewer credentials")
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: 32, MaxIdleConnsPerHost: 16, MaxConnsPerHost: 24,
		IdleConnTimeout: 60 * time.Second, ResponseHeaderTimeout: 8 * time.Second, DisableCompression: true}
	return &GrafanaProxy{URL: u, Username: username, Password: password, requests: make(chan struct{}, 24), queries: make(chan struct{}, 4),
		Client: &http.Client{Transport: transport, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (a *Admin) grafanaStatus(w http.ResponseWriter, r *http.Request) {
	u, cfg, _, ok := a.identity(r)
	if !ok {
		grafanaError(w, 401, "unauthorized", "login required")
		return
	}
	ids := grafanaApps(u, cfg)
	dashboard := applicationDashboard
	if u.Role == "admin" {
		dashboard = "llm-gateway"
	}
	JSON(w, map[string]any{"enabled": a.Grafana != nil, "available": a.Grafana != nil && a.Grafana.available(r.Context()), "dashboard": dashboard, "has_apps": len(ids) > 0, "max_hours": 24})
}

func (a *Admin) grafana(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
	w.Header().Set("Content-Security-Policy", "frame-ancestors 'self'")
	w.Header().Set("Referrer-Policy", "no-referrer")
	g := a.Grafana
	// 先准入再读取会话/配置，静态资源的鉴权也不能无界占用数据库连接。
	if g != nil {
		select {
		case g.requests <- struct{}{}:
			defer func() { <-g.requests }()
		default:
			grafanaError(w, 503, "monitoring_busy", "monitoring concurrency exceeded")
			return
		}
	}
	u, cfg, _, ok := a.identity(r)
	if !ok {
		grafanaError(w, 401, "unauthorized", "login required")
		return
	}
	if u.Role != "admin" && u.Role != "developer" && u.Role != "cost" {
		grafanaError(w, 403, "forbidden", "unsupported role")
		return
	}
	if g == nil {
		grafanaError(w, 503, "monitoring_unavailable", "Grafana is not configured")
		return
	}
	relative := strings.TrimPrefix(r.URL.Path, grafanaPrefix)
	// 不依赖上游清理路径，避免编码分隔符或 dot segment 跳出白名单。
	if r.URL.RawPath != "" || strings.ContainsAny(relative, "\\%") || strings.Contains(relative, "//") || path.Clean("/"+relative) != "/"+strings.TrimSuffix(relative, "/") {
		grafanaError(w, 403, "forbidden", "invalid monitoring path")
		return
	}
	if strings.HasPrefix(relative, "avatar/") && regexp.MustCompile(`^avatar/[a-f0-9]{64}$`).MatchString(relative) && r.Method == "GET" {
		w.Header().Set("Content-Type", "image/svg+xml")
		_, _ = io.WriteString(w, `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"><rect width="32" height="32" rx="16" fill="#5068b2"/><text x="16" y="22" text-anchor="middle" fill="white" font-family="sans-serif" font-size="18">V</text></svg>`)
		return
	}
	static := strings.HasPrefix(relative, "public/build/") || strings.HasPrefix(relative, "public/img/") || strings.HasPrefix(relative, "public/fonts/") || strings.HasPrefix(relative, "public/plugins/prometheus/")
	ids := grafanaApps(u, cfg)
	admin := u.Role == "admin"
	query := r.URL.Query()
	// 固定且有界的 bootstrap；不能访问其它 dashboard 或 Grafana 写接口。
	allowed := static || relative == "api/frontend/settings" || relative == "api/user" || relative == "api/user/preferences" || relative == "api/org/preferences" || relative == "api/access-control/user/permissions" || relative == "api/health" || relative == "api/plugins/prometheus/settings"
	dashboardUID := ""
	if relative == "apis/dashboard.grafana.app/" || relative == "apis/preferences.grafana.app/v1/namespaces/default/preferences/merged" {
		allowed = true
	}
	for _, version := range []string{"v1", "v1beta1", "v2", "v2beta1"} {
		prefix := "apis/dashboard.grafana.app/" + version + "/namespaces/default/dashboards/"
		if strings.HasPrefix(relative, prefix) {
			id := strings.TrimSuffix(strings.TrimPrefix(relative, prefix), "/dto")
			if strings.HasSuffix(relative, "/dto") && (id == applicationDashboard || (admin && id == "llm-gateway")) {
				dashboardUID = id
				allowed = true
			}
		}
	}
	if strings.HasPrefix(relative, "api/dashboards/uid/") {
		dashboardUID = strings.TrimPrefix(relative, "api/dashboards/uid/")
		allowed = dashboardUID == applicationDashboard || (admin && dashboardUID == "llm-gateway")
	}
	if strings.HasPrefix(relative, "d/") {
		parts := strings.Split(relative, "/")
		allowed = len(parts) == 3 && (parts[1] == applicationDashboard || (admin && parts[1] == "llm-gateway"))
	}
	if relative == "api/prometheus/grafana/api/v1/rules" && r.Method == "GET" {
		JSON(w, map[string]any{"status": "success", "data": map[string]any{"groups": []any{}}})
		return
	}
	if relative == "api/user/stars" && r.Method == "GET" {
		JSON(w, []any{})
		return
	}
	if relative == "api/annotations" && r.Method == "GET" {
		JSON(w, []any{})
		return
	}
	if relative == "api/search" && r.Method == "GET" {
		entries := []map[string]any{{"uid": applicationDashboard, "title": "应用与错误码监控", "url": grafanaPrefix + "d/" + applicationDashboard + "/applications", "type": "dash-db"}}
		if admin {
			entries = append(entries, map[string]any{"uid": "llm-gateway", "title": "网关可靠性", "url": grafanaPrefix + "d/llm-gateway/gateway", "type": "dash-db"})
		}
		JSON(w, entries)
		return
	}
	var body []byte
	isQuery := relative == "api/ds/query"
	if isQuery {
		if r.Method != "POST" || !a.originOK(r) {
			grafanaError(w, 403, "forbidden", "same-origin read query required")
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			grafanaError(w, 413, "invalid_query", "query body too large")
			return
		}
		body, err = grafanaQueryBody(raw, ids, admin, time.Now())
		if err != nil {
			grafanaError(w, 403, "invalid_query", err.Error())
			return
		}
		query = url.Values{}
		allowed = true
	} else if resource, found := grafanaPrometheusResource(relative); found {
		if r.Method != "GET" {
			grafanaError(w, 403, "forbidden", "read-only resource required")
			return
		}
		var err error
		query, err = scopedPrometheusParams(resource, query, ids, admin, time.Now())
		if err != nil {
			grafanaError(w, 403, "invalid_query", err.Error())
			return
		}
		allowed = true
		isQuery = true
	} else if r.Method != "GET" {
		allowed = false
	}
	if !allowed {
		grafanaError(w, 403, "forbidden", "monitoring endpoint denied")
		return
	}
	if isQuery {
		queue, stop := context.WithTimeout(r.Context(), 2*time.Second)
		defer stop()
		select {
		case g.queries <- struct{}{}:
			defer func() { <-g.queries }()
		case <-queue.Done():
			grafanaError(w, 503, "monitoring_busy", "query concurrency exceeded")
			return
		}
	}
	timeout := 10 * time.Second
	if static {
		timeout = 60 * time.Second
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(timeout))
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	target := *g.URL
	target.Path = grafanaPrefix + relative
	target.RawPath = ""
	target.RawQuery = query.Encode()
	upstream, err := http.NewRequestWithContext(ctx, r.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		grafanaError(w, 502, "monitoring_unavailable", "invalid upstream request")
		return
	}
	// 白名单复制：浏览器会话、Authorization 和伪造身份不进入 Grafana。
	upstream.Header.Set("Accept", r.Header.Get("Accept"))
	upstream.Header.Set("Accept-Encoding", "identity")
	if static && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		upstream.Header.Set("Accept-Encoding", "gzip")
	}
	if r.Method == "POST" {
		upstream.Header.Set("Content-Type", "application/json")
	}
	upstream.SetBasicAuth(g.Username, g.Password)
	response, err := g.Client.Do(upstream)
	if err != nil {
		grafanaError(w, 502, "monitoring_unavailable", "Grafana query or connection failed")
		return
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		grafanaError(w, 502, "monitoring_unavailable", "unexpected Grafana redirect")
		return
	}
	if static {
		if response.ContentLength > 32<<20 {
			grafanaError(w, 502, "monitoring_unavailable", "static response exceeds limit")
			return
		}
		w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
		w.Header().Set("Cache-Control", "private, max-age=3600")
		w.Header().Set("Vary", "Accept-Encoding")
		if encoding := response.Header.Get("Content-Encoding"); encoding == "gzip" {
			w.Header().Set("Content-Encoding", encoding)
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, io.LimitReader(response.Body, 32<<20))
		return
	}
	limit := int64(8 << 20)
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || int64(len(data)) > limit {
		grafanaError(w, 502, "monitoring_unavailable", "Grafana response exceeds limit")
		return
	}
	if dashboardUID != "" && response.StatusCode == 200 {
		data, err = scopedDashboard(data, u, cfg)
		if err != nil {
			grafanaError(w, 502, "monitoring_unavailable", "invalid dashboard response")
			return
		}
	}
	w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
	// 不复制 Set-Cookie、Location 或上游允许任意嵌入的安全头。
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(data)
}

func grafanaQueryBody(raw []byte, ids []string, admin bool, now time.Time) ([]byte, error) {
	var body map[string]json.RawMessage
	if json.Unmarshal(raw, &body) != nil {
		return nil, errors.New("invalid query JSON")
	}
	var from, to string
	if json.Unmarshal(body["from"], &from) != nil || json.Unmarshal(body["to"], &to) != nil {
		return nil, errors.New("query requires absolute timestamps")
	}
	start, e1 := strconv.ParseInt(from, 10, 64)
	end, e2 := strconv.ParseInt(to, 10, 64)
	if e1 != nil || e2 != nil || start >= end || end-start > int64((24*time.Hour)/time.Millisecond) || start < now.Add(-24*time.Hour-time.Minute).UnixMilli() || end > now.Add(time.Minute).UnixMilli() {
		return nil, errors.New("query window must be within last 24 hours")
	}
	var queries []map[string]json.RawMessage
	if json.Unmarshal(body["queries"], &queries) != nil || len(queries) == 0 || len(queries) > 16 {
		return nil, errors.New("invalid query count")
	}
	for i, q := range queries {
		var ds struct {
			UID  string `json:"uid"`
			Type string `json:"type"`
		}
		var expr string
		if json.Unmarshal(q["datasource"], &ds) != nil || ds.UID != "prometheus" || (ds.Type != "" && ds.Type != "prometheus") || json.Unmarshal(q["expr"], &expr) != nil {
			return nil, errors.New("only Prometheus expression queries are allowed")
		}
		var interval int64
		_ = json.Unmarshal(q["intervalMs"], &interval)
		if interval < 10000 {
			interval = 10000
		}
		if interval > 3600000 {
			return nil, errors.New("query interval exceeds one hour")
		}
		// Grafana 的宏由 Prometheus backend 才展开；隔离必须在展开后解析。
		rate := interval + 10000
		if rate < 40000 {
			rate = 40000
		}
		duration := func(ms int64) string { return strconv.FormatInt(ms/1000, 10) + "s" }
		expr = strings.NewReplacer("$__rate_interval", duration(rate), "$__interval_ms", strconv.FormatInt(interval, 10), "$__interval", duration(interval), "$__range_ms", strconv.FormatInt(end-start, 10), "$__range_s", strconv.FormatInt((end-start)/1000, 10), "$__range", duration(end-start)).Replace(expr)
		expr, err := scopedPromQL(expr, ids, admin)
		if err != nil {
			return nil, err
		}
		q["expr"], _ = json.Marshal(expr)
		var points int64
		_ = json.Unmarshal(q["maxDataPoints"], &points)
		if points <= 0 || points > 1500 {
			points = 1500
		}
		q["maxDataPoints"], _ = json.Marshal(points)
		delete(q, "datasourceId")
		delete(q, "datasourceUid")
		q["exemplar"] = json.RawMessage("false")
		q["intervalMs"], _ = json.Marshal(interval)
		// 不透传插件可用的任意字段；避免后端二次改写 app 条件或启用其它查询通道。
		clean := map[string]json.RawMessage{}
		for _, key := range []string{"refId", "expr", "legendFormat", "format", "instant", "range", "intervalMs", "maxDataPoints", "requestId", "utcOffsetSec"} {
			if value, ok := q[key]; ok {
				clean[key] = value
			}
		}
		clean["datasource"] = json.RawMessage(`{"uid":"prometheus","type":"prometheus"}`)
		clean["exemplar"] = json.RawMessage("false")
		queries[i] = clean
	}
	// 仅转发校验后的批次，避免根级别字段启用其它数据源或表达式。
	return json.Marshal(map[string]any{"from": from, "to": to, "queries": queries})
}

func grafanaPrometheusResource(relative string) (string, bool) {
	for _, prefix := range []string{"api/datasources/uid/prometheus/resources/", "api/datasources/proxy/uid/prometheus/"} {
		if strings.HasPrefix(relative, prefix) {
			return strings.TrimPrefix(relative, prefix), true
		}
	}
	return "", false
}

var prometheusLabelPath = regexp.MustCompile(`^api/v1/label/[a-zA-Z_][a-zA-Z0-9_]*/values$`)

func scopedPrometheusParams(resource string, input url.Values, ids []string, admin bool, now time.Time) (url.Values, error) {
	q := url.Values{}
	for key, values := range input {
		if key != "match[]" && len(values) != 1 {
			return nil, errors.New("duplicate query parameter")
		}
		switch key {
		case "query", "match[]", "start", "end", "time", "step", "limit":
			q[key] = values
		default:
			return nil, errors.New("unsupported query parameter")
		}
	}
	if resource == "api/v1/query" || resource == "api/v1/query_range" {
		expr, err := scopedPromQL(q.Get("query"), ids, admin)
		if err != nil {
			return nil, err
		}
		q.Set("query", expr)
	} else if resource == "api/v1/series" || resource == "api/v1/labels" || prometheusLabelPath.MatchString(resource) {
		selectors := q["match[]"]
		if len(selectors) > 16 {
			return nil, errors.New("too many selectors")
		}
		if len(selectors) == 0 {
			if admin {
				return nil, errors.New("label query requires metric selector")
			}
			matcher, err := applicationMatcher(ids)
			if err != nil {
				return nil, err
			}
			names := []string{}
			for name := range applicationMetrics {
				names = append(names, name)
			}
			q.Add("match[]", fmt.Sprintf(`{__name__=~%q,%s}`, strings.Join(names, "|"), matcher.String()))
		} else {
			q.Del("match[]")
			for _, selector := range selectors {
				expr, err := scopedPromQL(selector, ids, admin)
				if err != nil {
					return nil, err
				}
				parsed, err := parserSelector(expr)
				if err != nil {
					return nil, err
				}
				q.Add("match[]", parsed)
			}
		}
	} else {
		return nil, errors.New("Prometheus resource denied")
	}
	start, end := now.Add(-24*time.Hour), now
	for key, target := range map[string]*time.Time{"start": &start, "end": &end, "time": &end} {
		if raw := q.Get(key); raw != "" {
			n, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return nil, errors.New("invalid query timestamp")
			}
			t := time.UnixMilli(int64(n * 1000))
			if t.Before(now.Add(-24*time.Hour-time.Minute)) || t.After(now.Add(time.Minute)) {
				return nil, errors.New("query timestamp outside monitoring window")
			}
			*target = t
		}
	}
	if start.After(end) {
		return nil, errors.New("invalid query window")
	}
	if resource == "api/v1/query" {
		q.Set("time", strconv.FormatFloat(float64(end.UnixMilli())/1000, 'f', 3, 64))
	} else {
		q.Set("start", strconv.FormatFloat(float64(start.UnixMilli())/1000, 'f', 3, 64))
		q.Set("end", strconv.FormatFloat(float64(end.UnixMilli())/1000, 'f', 3, 64))
	}
	if resource == "api/v1/query_range" {
		step, err := strconv.ParseFloat(q.Get("step"), 64)
		if err != nil || step < 10 || end.Sub(start).Seconds()/step > 1500 {
			return nil, errors.New("query step outside monitoring limits")
		}
	}
	q.Set("limit", "1500")
	return q, nil
}

func scopedDashboard(data []byte, u User, cfg Config) ([]byte, error) {
	var response map[string]any
	if json.Unmarshal(data, &response) != nil {
		return nil, errors.New("invalid dashboard")
	}
	dashboard, ok := response["dashboard"].(map[string]any)
	if !ok {
		dashboard, ok = response["spec"].(map[string]any)
	}
	if !ok {
		return nil, errors.New("missing dashboard")
	}
	dashboard["editable"] = false
	if templating, ok := dashboard["templating"].(map[string]any); ok {
		if list, ok := templating["list"].([]any); ok {
			for _, entry := range list {
				variable, ok := entry.(map[string]any)
				if !ok || variable["name"] != "app" {
					continue
				}
				query, options := grafanaAppOptions(u, cfg)
				variable["type"] = "custom"
				variable["query"] = query
				variable["options"] = options
				variable["refresh"] = 0
			}
		}
	}
	// Grafana 13 DTO v2 使用 variables[].spec，旧 templating 裁剪不能覆盖它。
	if variables, ok := dashboard["variables"].([]any); ok {
		for _, entry := range variables {
			variable, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			spec, ok := variable["spec"].(map[string]any)
			if !ok || spec["name"] != "app" {
				continue
			}
			query, options := grafanaAppOptions(u, cfg)
			variable["kind"] = "CustomVariable"
			spec["query"] = query
			spec["options"] = options
			delete(spec, "refresh")
			delete(spec, "regex")
			delete(spec, "sort")
		}
	}
	return json.Marshal(response)
}

func grafanaAppOptions(u User, cfg Config) (string, []any) {
	options := []any{}
	pairs := []string{}
	// 名称只取当前允许范围。变量显示值不是数据权限的来源。
	for _, id := range grafanaApps(u, cfg) {
		label := cfg.Apps[id].Name + " (" + id + ")"
		options = append(options, map[string]any{"text": label, "value": id, "selected": false})
	}
	if u.Role == "admin" {
		options = append(options, map[string]any{"text": "未知应用", "value": "_unknown", "selected": false})
	}
	for _, option := range options {
		item := option.(map[string]any)
		label := strings.NewReplacer(",", "，", ":", "：", "\n", " ", "\r", " ").Replace(item["text"].(string))
		pairs = append(pairs, label+" : "+item["value"].(string))
	}
	return strings.Join(pairs, ","), options
}

// Grafana 前端读取 message，保留稳定 code 便于代理错误诊断。
func grafanaError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": message, "code": code})
}

// iframe 的 HTTP 错误页未必可读，重连前用短期限健康探测给出明确状态。
func (g *GrafanaProxy) available(parent context.Context) bool {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	target := *g.URL
	target.Path = grafanaPrefix + "api/health"
	target.RawQuery = ""
	request, err := http.NewRequestWithContext(ctx, "GET", target.String(), nil)
	if err != nil {
		return false
	}
	request.Header.Set("Accept-Encoding", "identity")
	request.SetBasicAuth(g.Username, g.Password)
	response, err := g.Client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return err == nil && response.StatusCode == http.StatusOK
}

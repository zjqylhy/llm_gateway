package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TrafficPrometheus struct {
	URL    string
	Client *http.Client
}

func newTrafficPrometheus(raw string) (*TrafficPrometheus, error) {
	if raw == "" {
		return nil, nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || !Contains([]string{"http", "https"}, u.Scheme) || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("GW_PROMETHEUS_URL must be an HTTP(S) Base URL without credentials, query or fragment")
	}
	// 地址来自部署配置，允许私有监控网络；不接受客户端传入地址或重定向。
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost = 4, 4
	return &TrafficPrometheus{URL: strings.TrimRight(raw, "/"), Client: &http.Client{Timeout: 5 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

type trafficPoint struct {
	At    time.Time `json:"at"`
	Value float64   `json:"value"`
}
type trafficSeries struct {
	Kind       string         `json:"kind"`
	HTTPStatus string         `json:"http_status,omitempty"`
	ErrorCode  string         `json:"error_code,omitempty"`
	Points     []trafficPoint `json:"points"`
}
type trafficFailure struct {
	HTTPStatus string  `json:"http_status"`
	ErrorCode  string  `json:"error_code"`
	QPS        float64 `json:"qps"`
}
type trafficApp struct {
	ID            string           `json:"id"`
	Name          string           `json:"name"`
	ConfiguredQPS int64            `json:"configured_qps"`
	SuccessQPS    *float64         `json:"success_qps"`
	FailureQPS    *float64         `json:"failure_qps"`
	LimitAtEnd    *float64         `json:"limit_at_end"`
	Failures      []trafficFailure `json:"failures"`
	Series        []trafficSeries  `json:"series"`
}
type trafficResponse struct {
	Available   bool         `json:"available"`
	Reason      string       `json:"reason,omitempty"`
	From        time.Time    `json:"from"`
	To          time.Time    `json:"to"`
	StepSeconds int          `json:"step_seconds"`
	Apps        []trafficApp `json:"apps"`
}
type prometheusSeries struct {
	Metric map[string]string   `json:"metric"`
	Value  []json.RawMessage   `json:"value"`
	Values [][]json.RawMessage `json:"values"`
}

func trafficWindow(q url.Values, now time.Time) (time.Time, time.Time, error) {
	to := now.UTC()
	var err error
	if raw := q.Get("to"); raw != "" {
		to, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	from := to.Add(-time.Hour)
	if raw := q.Get("from"); raw != "" {
		from, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	from, to = from.Truncate(time.Millisecond), to.Truncate(time.Millisecond)
	// 容忍浏览器与服务端的少量时钟误差及网络延迟；区间长度仍严格限于一天。
	if to.Sub(from) < time.Millisecond || to.Sub(from) > 24*time.Hour || from.Before(now.Add(-24*time.Hour-30*time.Second)) || to.After(now.Add(30*time.Second)) {
		return from, to, errors.New("select a positive time range within the last 24 hours")
	}
	return from.UTC(), to.UTC(), nil
}

func trafficSelector(ids []string) string {
	if len(ids) == 1 {
		return "app=" + strconv.Quote(ids[0])
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = regexp.QuoteMeta(id)
	}
	return "app=~" + strconv.Quote("("+strings.Join(parts, "|")+")")
}

func trafficQueries(ids []string, from, to time.Time) (string, string) {
	selector := trafficSelector(ids) + `,job=~"gateway|admin"`
	kind := func(expr, label string) string {
		return `label_replace(` + expr + `,"kind","` + label + `","app",".*")`
	}
	limit := kind("max by(app) (gateway_app_qps_limit{"+selector+"})", "limit")
	success := "gateway_calls_total{" + selector + `,outcome="success"}`
	failure := "gateway_calls_total{" + selector + `,outcome="error"}`
	window := fmt.Sprintf("%dms", to.Sub(from).Milliseconds())
	seconds := strconv.FormatFloat(to.Sub(from).Seconds(), 'f', 3, 64)
	failureSum := func(expr string) string {
		return "sum by(app,http_status,error_code) (max by(app,source,http_status,error_code) (" + expr + "))"
	}
	// dev 模式共享注册表，先按 source 去重；失败趋势保留错误码维度。
	// 固定区间结束时间筛选发生过的错误，保留这些曲线的零值和采样缺口。
	activeFailures := failureSum("increase("+failure+"["+window+"] @ end())") + " > 0"
	failureRange := failureSum("rate("+failure+"[1m])") + " and on(app,http_status,error_code) (" + activeFailures + ")"
	rangeQuery := kind("sum by(app) (max by(app,source) (rate("+success+"[1m])))", "success") + " or " + kind(failureRange, "failure") + " or " + limit
	summaryQuery := kind("sum by(app) (max by(app,source) (increase("+success+"["+window+"]))) / "+seconds, "success") + " or " + kind("("+failureSum("increase("+failure+"["+window+"])")+" / "+seconds+") > 0", "failure") + " or " + limit
	return rangeQuery, summaryQuery
}

func (p *TrafficPrometheus) query(ctx context.Context, query string, from, to time.Time, rangeQuery bool) ([]prometheusSeries, error) {
	form := url.Values{"query": {query}, "timeout": {"4s"}}
	path, expectedType := "/api/v1/query", "vector"
	if rangeQuery {
		path, expectedType = "/api/v1/query_range", "matrix"
		form.Set("start", from.Format(time.RFC3339Nano))
		form.Set("end", to.Format(time.RFC3339Nano))
		form.Set("step", "60s")
	} else {
		form.Set("time", to.Format(time.RFC3339Nano))
	}
	req, err := http.NewRequestWithContext(ctx, "POST", p.URL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, errors.New("Prometheus query failed")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
	if err != nil || len(data) > 8<<20 {
		return nil, errors.New("Prometheus response unavailable or too large")
	}
	var envelope struct {
		Status   string   `json:"status"`
		Warnings []string `json:"warnings"`
		Data     struct {
			ResultType string             `json:"resultType"`
			Result     []prometheusSeries `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &envelope) != nil || envelope.Status != "success" || envelope.Data.ResultType != expectedType || len(envelope.Warnings) > 0 || len(envelope.Data.Result) > 256 {
		return nil, errors.New("Prometheus returned incomplete or invalid data")
	}
	return envelope.Data.Result, nil
}

func parseTrafficPoint(raw []json.RawMessage) (trafficPoint, error) {
	var timestamp float64
	var encoded string
	if len(raw) != 2 || json.Unmarshal(raw[0], &timestamp) != nil || json.Unmarshal(raw[1], &encoded) != nil {
		return trafficPoint{}, errors.New("invalid Prometheus point")
	}
	value, err := strconv.ParseFloat(encoded, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || math.IsNaN(timestamp) || math.IsInf(timestamp, 0) || timestamp < 0 || timestamp > 1e11 {
		return trafficPoint{}, errors.New("invalid Prometheus sample")
	}
	// Prometheus 以毫秒存储时间；浮点小数的误差不能把首个点误判为越界。
	return trafficPoint{At: time.UnixMilli(int64(math.Round(timestamp * 1000))).UTC(), Value: value}, nil
}

func mergeTraffic(result *trafficResponse, series []prometheusSeries, summary bool) error {
	apps := make(map[string]*trafficApp, len(result.Apps))
	for i := range result.Apps {
		apps[result.Apps[i].ID] = &result.Apps[i]
	}
	seen := map[string]bool{}
	for _, row := range series {
		app := apps[row.Metric["app"]]
		kind := row.Metric["kind"]
		if app == nil || !Contains([]string{"success", "failure", "limit"}, kind) {
			return errors.New("unexpected Prometheus application or series")
		}
		key := app.ID + ":" + kind
		if kind == "failure" {
			code, status := row.Metric["error_code"], row.Metric["http_status"]
			expectedStatus, known := callErrorStatuses[code]
			if !known || (status != strconv.Itoa(expectedStatus) && !(code == "metering_unavailable" && status == "200")) {
				return errors.New("unexpected error labels")
			}
			key += ":" + status + ":" + code
		}
		if seen[key] {
			return errors.New("duplicate aggregated Prometheus series")
		}
		seen[key] = true
		if summary {
			point, err := parseTrafficPoint(row.Value)
			if err != nil {
				return err
			}
			if math.Abs(point.At.Sub(result.To).Seconds()) > 1 {
				return errors.New("unexpected summary sample timestamp")
			}
			switch kind {
			case "success":
				app.SuccessQPS = &point.Value
			case "limit":
				app.LimitAtEnd = &point.Value
			case "failure":
				code, status := row.Metric["error_code"], row.Metric["http_status"]
				app.Failures = append(app.Failures, trafficFailure{HTTPStatus: status, ErrorCode: code, QPS: point.Value})
			}
		} else {
			if len(row.Values) > 1441 {
				return errors.New("too many Prometheus points")
			}
			line := trafficSeries{Kind: kind, HTTPStatus: row.Metric["http_status"], ErrorCode: row.Metric["error_code"], Points: []trafficPoint{}}
			for _, raw := range row.Values {
				point, err := parseTrafficPoint(raw)
				if err != nil {
					return err
				}
				if point.At.Before(result.From) || point.At.After(result.To) || (len(line.Points) > 0 && !point.At.After(line.Points[len(line.Points)-1].At)) {
					return errors.New("unexpected time-series sample timestamp")
				}
				line.Points = append(line.Points, point)
			}
			app.Series = append(app.Series, line)
		}
	}
	if summary {
		for i := range result.Apps {
			app := &result.Apps[i]
			if app.SuccessQPS != nil || len(app.Failures) > 0 {
				total := 0.0
				for _, failure := range app.Failures {
					total += failure.QPS
				}
				app.FailureQPS = &total
			}
			sort.Slice(app.Failures, func(i, j int) bool { return app.Failures[i].QPS > app.Failures[j].QPS })
		}
	}
	return nil
}

func (a *Admin) appTraffic(w http.ResponseWriter, r *http.Request) {
	u, cfg, _, authenticated := a.identity(r)
	if !authenticated {
		APIError(w, 401, "unauthorized", "login required")
		return
	}
	id := r.URL.Query().Get("app_id")
	if u.Role == "developer" && (id == "" || cfg.Apps[id].Owner != u.Name) {
		APIError(w, 403, "forbidden", "select an owned app")
		return
	}
	if id != "" {
		if _, exists := cfg.Apps[id]; !exists {
			APIError(w, 404, "app_not_found", "application unavailable")
			return
		}
	}
	from, to, err := trafficWindow(r.URL.Query(), time.Now())
	if err != nil {
		APIError(w, 400, "invalid_time_range", "select a time range within the last 24 hours")
		return
	}
	ids := []string{}
	for appID := range cfg.Apps {
		if id == "" || id == appID {
			ids = append(ids, appID)
		}
	}
	sort.Strings(ids)
	if len(ids) > 64 {
		APIError(w, 400, "too_many_apps", "select one application for traffic metrics")
		return
	}
	result := trafficResponse{From: from, To: to, StepSeconds: 60, Apps: []trafficApp{}}
	for _, appID := range ids {
		app := cfg.Apps[appID]
		result.Apps = append(result.Apps, trafficApp{ID: appID, Name: app.Name, ConfiguredQPS: app.QPS, Failures: []trafficFailure{}, Series: []trafficSeries{}})
	}
	if a.Prometheus == nil {
		result.Reason = "未配置 Prometheus，暂无监控数据"
		JSON(w, result)
		return
	}
	if len(ids) == 0 {
		result.Reason = "暂无可查看的应用"
		JSON(w, result)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rangeQuery, summaryQuery := trafficQueries(ids, from, to)
	// 两个独立查询共享总期限；任何一个失败时都不返回误导性的部分监控。
	type queryResult struct {
		summary bool
		series  []prometheusSeries
		err     error
	}
	completed := make(chan queryResult, 2)
	go func() {
		rows, err := a.Prometheus.query(ctx, rangeQuery, from, to, true)
		completed <- queryResult{series: rows, err: err}
	}()
	go func() {
		rows, err := a.Prometheus.query(ctx, summaryQuery, from, to, false)
		completed <- queryResult{summary: true, series: rows, err: err}
	}()
	queryResults := []queryResult{<-completed, <-completed}
	for _, response := range queryResults {
		if response.err != nil || mergeTraffic(&result, response.series, response.summary) != nil {
			for i := range result.Apps {
				result.Apps[i].SuccessQPS, result.Apps[i].FailureQPS, result.Apps[i].LimitAtEnd = nil, nil, nil
				result.Apps[i].Failures = []trafficFailure{}
				result.Apps[i].Series = []trafficSeries{}
			}
			result.Reason = "监控查询不可用或结果不完整，请稍后重试或选择单个应用"
			JSON(w, result)
			return
		}
	}
	for _, app := range result.Apps {
		if app.SuccessQPS != nil || len(app.Series) > 0 {
			result.Available = true
		}
	}
	if !result.Available {
		result.Reason = "所选时段暂无采样数据，请确认 Prometheus 已抓取网关指标"
	}
	JSON(w, result)
}

package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Request struct {
	Raw           map[string]json.RawMessage
	Alias         string
	Stream        bool
	IncludeUsage  bool
	Kind          string
	MaxOutput     int64
	InputEstimate int64
	Text          string
	System        string
	SafetyText    string
	Capabilities  []string
	Cacheable     bool
	Rerank        *rerankInput
}

func DecodeRequest(raw []byte, kind string) (Request, error) {
	r := Request{Kind: kind, MaxOutput: 512, Cacheable: kind == "chat"}
	if e := json.Unmarshal(raw, &r.Raw); e != nil {
		return r, errors.New("invalid JSON")
	}
	if e := json.Unmarshal(r.Raw["model"], &r.Alias); e != nil || r.Alias == "" {
		return r, errors.New("model is required")
	}
	if kind == "rerank" {
		return decodeRerank(r)
	}
	if b, ok := r.Raw["stream"]; ok && json.Unmarshal(b, &r.Stream) != nil {
		return r, errors.New("stream must be boolean")
	}
	var opts struct {
		Include bool `json:"include_usage"`
	}
	if b, ok := r.Raw["stream_options"]; ok && json.Unmarshal(b, &opts) != nil {
		return r, errors.New("invalid stream_options")
	}
	r.IncludeUsage = opts.Include
	allowed := map[string]bool{"model": true, "messages": true, "input": true, "encoding_format": true, "dimensions": true, "stream": true, "stream_options": true, "temperature": true, "top_p": true, "max_tokens": true, "max_completion_tokens": true, "stop": true, "presence_penalty": true, "frequency_penalty": true, "tools": true, "tool_choice": true, "response_format": true, "seed": true, "n": true, "user": true}
	for k := range r.Raw {
		if !allowed[k] {
			return r, errors.New("unsupported parameter: " + k)
		}
	}
	for _, limit := range []struct {
		name     string
		min, max float64
	}{{"temperature", 0, 2}, {"top_p", 0, 1}, {"presence_penalty", -2, 2}, {"frequency_penalty", -2, 2}} {
		if b, ok := r.Raw[limit.name]; ok {
			var v float64
			if json.Unmarshal(b, &v) != nil || v < limit.min || v > limit.max {
				return r, errors.New("invalid " + limit.name)
			}
		}
	}
	if kind == "embedding" {
		for k := range r.Raw {
			if !Contains([]string{"model", "input", "encoding_format", "dimensions", "user"}, k) {
				return r, errors.New("unsupported embedding parameter: " + k)
			}
		}
		if b, ok := r.Raw["encoding_format"]; ok {
			var f string
			if json.Unmarshal(b, &f) != nil || !Contains([]string{"float", "base64"}, f) {
				return r, errors.New("invalid encoding_format")
			}
		}
	}
	if _, a := r.Raw["max_tokens"]; a {
		if _, b := r.Raw["max_completion_tokens"]; b {
			return r, errors.New("use one output limit parameter")
		}
	}
	for _, k := range []string{"max_tokens", "max_completion_tokens"} {
		if b, ok := r.Raw[k]; ok {
			if e := json.Unmarshal(b, &r.MaxOutput); e != nil || r.MaxOutput <= 0 || r.MaxOutput > 1000000 {
				return r, errors.New("invalid output limit")
			}
		}
	}
	if r.Stream {
		r.Capabilities = append(r.Capabilities, "stream")
	}
	if len(r.Raw["tools"]) > 0 && string(r.Raw["tools"]) != "[]" {
		var tools []struct {
			Type     string `json:"type"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		}
		if json.Unmarshal(r.Raw["tools"], &tools) != nil || len(tools) == 0 || len(tools) > 128 {
			return r, errors.New("invalid tools")
		}
		for _, tool := range tools {
			if tool.Type != "function" || tool.Function.Name == "" {
				return r, errors.New("only named function tools supported")
			}
		}
		r.Capabilities = append(r.Capabilities, "tools")
		r.Cacheable = false
	}
	var format struct {
		Type string `json:"type"`
	}
	if b := r.Raw["response_format"]; len(b) > 0 {
		if e := json.Unmarshal(b, &format); e != nil {
			return r, errors.New("invalid response_format")
		}
		if !Contains([]string{"text", "json_object", "json_schema"}, format.Type) {
			return r, errors.New("unsupported response_format")
		}
		if format.Type != "text" {
			r.Capabilities = append(r.Capabilities, format.Type)
			r.Cacheable = false
		}
	}
	for _, k := range []string{"seed", "n"} {
		if b := r.Raw[k]; len(b) > 0 {
			var v int
			if json.Unmarshal(b, &v) != nil {
				return r, errors.New("invalid " + k)
			}
			if k == "n" && v != 1 {
				return r, errors.New("only n=1 is supported")
			}
			if k == "seed" {
				r.Capabilities = append(r.Capabilities, "seed")
			}
		}
	}
	if kind == "chat" {
		var rawMessages []map[string]json.RawMessage
		if json.Unmarshal(r.Raw["messages"], &rawMessages) != nil {
			return r, errors.New("invalid messages")
		}
		for _, message := range rawMessages {
			for key := range message {
				if !Contains([]string{"role", "content", "name", "tool_calls", "tool_call_id", "refusal"}, key) {
					return r, errors.New("unsupported message field: " + key)
				}
			}
			if calls := message["tool_calls"]; len(calls) > 0 && string(calls) != "null" {
				var tools []json.RawMessage
				if json.Unmarshal(calls, &tools) != nil || len(tools) == 0 {
					return r, errors.New("invalid message tool_calls")
				}
				r.Capabilities = append(r.Capabilities, "tools")
				r.Cacheable = false
			}
		}
		var messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		if e := json.Unmarshal(r.Raw["messages"], &messages); e != nil || len(messages) == 0 {
			return r, errors.New("messages must be nonempty")
		}
		users := 0
		for _, m := range messages {
			if m.Role == "tool" {
				r.Capabilities = append(r.Capabilities, "tools")
			}
			if !Contains([]string{"system", "developer", "user", "assistant", "tool"}, m.Role) {
				return r, errors.New("invalid message role")
			}
			var text string
			if json.Unmarshal(m.Content, &text) != nil {
				if string(m.Content) == "null" && m.Role == "assistant" {
					r.Cacheable = false
					continue
				}
				return r, errors.New("only text messages supported")
			}
			r.InputEstimate += int64(utf8.RuneCountInString(text)) + 8
			r.SafetyText += "\n" + text
			if m.Role == "user" {
				users++
				r.Text = text
			} else if m.Role == "system" {
				r.System += text
			} else {
				r.Cacheable = false
			}
		}
		if users != 1 {
			r.Cacheable = false
		}
	} else {
		r.Cacheable = false
		r.MaxOutput = 0
		var s string
		var ss []string
		if json.Unmarshal(r.Raw["input"], &s) == nil && s != "" {
			r.InputEstimate = int64(utf8.RuneCountInString(s))
			r.Text = s
		} else if json.Unmarshal(r.Raw["input"], &ss) == nil && len(ss) > 0 {
			for _, s := range ss {
				r.InputEstimate += int64(utf8.RuneCountInString(s))
			}
			r.Text = strings.Join(ss, "\n")
		} else {
			return r, errors.New("input must be text or text array")
		}
		r.SafetyText = r.Text
	}
	if r.InputEstimate <= 0 || r.InputEstimate > 1000000 {
		return r, errors.New("input size out of range")
	}
	// 无厂商分词器时使用 UTF-8 字节数作为保守预占，真实 usage 在结算时修正。
	if int64(len(raw)) > r.InputEstimate {
		r.InputEstimate = int64(len(raw))
	}
	return r, nil
}
func (r Request) Payload(m Model) ([]byte, error) {
	if r.Kind == "rerank" {
		return r.rerankPayload(m)
	}
	p := map[string]json.RawMessage{}
	for k, v := range r.Raw {
		p[k] = v
	}
	p["model"], _ = json.Marshal(m.Upstream)
	if r.Kind == "chat" {
		if _, ok := p["max_tokens"]; !ok {
			if _, ok = p["max_completion_tokens"]; !ok {
				p["max_tokens"], _ = json.Marshal(r.MaxOutput)
			}
		}
	}
	if r.Stream {
		p["stream_options"] = json.RawMessage(`{"include_usage":true}`)
	}
	return json.Marshal(p)
}

type UpstreamError struct {
	Status     int
	RetryAfter int
}

func (e *UpstreamError) Error() string { return "upstream HTTP " + strconv.Itoa(e.Status) }
func Retriable(e error) bool {
	if errors.Is(e, ErrLimit) || errors.Is(e, ErrUnavailable) {
		return false
	}
	var u *UpstreamError
	if errors.As(e, &u) {
		return u.Status == 429 || u.Status >= 500
	}
	return !errors.Is(e, context.Canceled)
}
func NewHTTPClient(dev bool) *http.Client {
	d := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	t := &http.Transport{MaxIdleConns: 1000, MaxIdleConnsPerHost: 100, MaxConnsPerHost: 500, IdleConnTimeout: 90 * time.Second, ResponseHeaderTimeout: 15 * time.Second, TLSHandshakeTimeout: 5 * time.Second}
	t.DialContext = func(c context.Context, network, address string) (net.Conn, error) {
		if dev {
			return d.DialContext(c, network, address)
		}
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		ips, e := net.DefaultResolver.LookupIPAddr(c, host)
		if e != nil {
			return nil, e
		}
		for _, ip := range ips {
			if UnsafeIP(ip.IP) {
				return nil, errors.New("unsafe upstream address")
			}
		}
		if len(ips) == 0 {
			return nil, ErrUnavailable
		}
		var last error
		for _, ip := range ips {
			conn, err := d.DialContext(c, network, net.JoinHostPort(ip.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
			if c.Err() != nil {
				break
			}
		}
		return nil, last
	}
	return &http.Client{Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func OpenUpstream(c context.Context, client *http.Client, p Provider, m Model, r Request, token string) (*http.Response, error) {
	payload, e := r.Payload(m)
	if e != nil {
		return nil, e
	}
	path := "/chat/completions"
	if r.Kind == "embedding" {
		path = "/embeddings"
	}
	if r.Kind == "rerank" {
		path = "/reranks"
		if m.RerankProtocol == "dashscope" {
			path = "/services/rerank/text-rerank/text-rerank"
		}
	}
	u, e := url.Parse(strings.TrimRight(p.URL, "/") + path)
	if e != nil {
		return nil, e
	}
	req, e := http.NewRequestWithContext(c, "POST", u.String(), bytes.NewReader(payload))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, e := client.Do(req)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		n, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		return nil, &UpstreamError{resp.StatusCode, n}
	}
	return resp, nil
}
func ExtractUsage(raw []byte) (Usage, bool) {
	var x struct {
		Usage *struct {
			Input   int64 `json:"prompt_tokens"`
			Output  int64 `json:"completion_tokens"`
			Total   int64 `json:"total_tokens"`
			Hit     int64 `json:"prompt_cache_hit_tokens"`
			Details struct {
				Cached int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(raw, &x) != nil || x.Usage == nil {
		return Usage{}, false
	}
	u := x.Usage
	cached := u.Details.Cached
	if u.Hit > cached {
		cached = u.Hit
	}
	if u.Input < 0 || u.Output < 0 || u.Total < 0 || cached < 0 || cached > u.Input || u.Input > 100000000 || u.Output > 100000000 {
		return Usage{}, false
	}
	total := u.Input + u.Output
	return Usage{u.Input, u.Output, total, cached}, true
}

// SSE 按事件边界读取，支持多行 data；从不等待整段模型输出。
type SSEReader struct{ scanner *bufio.Scanner }

func NewSSEReader(r io.Reader) *SSEReader {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 4096), 2<<20)
	return &SSEReader{s}
}
func (s *SSEReader) Next() ([]byte, error) {
	var data []string
	eventBytes := 0
	for s.scanner.Scan() {
		line := s.scanner.Text()
		// 单行 Scanner 上限不能约束无限多行的同一个 SSE 事件。
		eventBytes += len(line) + 1
		if eventBytes > 2<<20 {
			return nil, errors.New("SSE event exceeds 2 MiB")
		}
		if line == "" && len(data) > 0 {
			return []byte(strings.Join(data, "\n")), nil
		}
		if line == "" {
			eventBytes = 0
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if e := s.scanner.Err(); e != nil {
		return nil, e
	}
	if len(data) > 0 {
		return []byte(strings.Join(data, "\n")), nil
	}
	return nil, io.EOF
}

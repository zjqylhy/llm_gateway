package gateway

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// MockProvider 仅用于离线协议回归；生产启动明确禁止注册 mock。
func MockProvider(w http.ResponseWriter, r *http.Request) {
	var x struct {
		Model    string `json:"model"`
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
		Input  json.RawMessage `json:"input"`
		Stream bool            `json:"stream"`
		Tools  json.RawMessage `json:"tools"`
		Format json.RawMessage `json:"response_format"`
	}
	if !readJSONLoose(w, r, &x) {
		return
	}
	text := ""
	for _, m := range x.Messages {
		text += m.Content
	}
	if strings.Contains(text, "[fail-primary]") && strings.Contains(x.Model, "primary") {
		APIError(w, 503, "overloaded", "injected fault")
		return
	}
	usage := Usage{Input: 12, Output: 20, Total: 32}
	if strings.HasSuffix(r.URL.Path, "/embeddings") {
		var s string
		var ss []string
		if json.Unmarshal(x.Input, &s) == nil {
			ss = []string{s}
		} else {
			_ = json.Unmarshal(x.Input, &ss)
		}
		data := []any{}
		for i, text := range ss {
			h := sha256.Sum256([]byte(text))
			v := []float32{}
			for _, b := range h {
				v = append(v, (float32(b)-127)/128)
			}
			data = append(data, map[string]any{"index": i, "object": "embedding", "embedding": v})
		}
		JSON(w, map[string]any{"object": "list", "model": x.Model, "data": data, "usage": Usage{Input: 8, Total: 8}})
		return
	}
	answer := "本地协议验证响应：" + text
	message := map[string]any{"role": "assistant", "content": answer}
	finish := "stop"
	if len(x.Tools) > 0 && string(x.Tools) != "[]" {
		message["content"] = nil
		message["tool_calls"] = []any{map[string]any{"id": "call_local", "type": "function", "function": map[string]string{"name": "lookup", "arguments": "{}"}}}
		finish = "tool_calls"
	}
	if len(x.Format) > 0 && string(x.Format) != "{\"type\":\"text\"}" {
		message["content"] = `{"ok":true,"source":"offline"}`
	}
	if !x.Stream {
		JSON(w, map[string]any{"id": ID("chatcmpl_"), "object": "chat.completion", "created": time.Now().Unix(), "model": x.Model, "choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}}, "usage": usage})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	id := ID("chatcmpl_")
	send := func(choices any, u any) {
		data := map[string]any{"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": x.Model, "choices": choices}
		if u != nil {
			data["usage"] = u
		}
		b, _ := json.Marshal(data)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
		_ = http.NewResponseController(w).Flush()
	}
	send([]any{map[string]any{"index": 0, "delta": message, "finish_reason": nil}}, nil)
	if strings.Contains(text, "[break-stream]") {
		return
	}
	send([]any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finish}}, nil)
	send([]any{}, usage)
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	_ = http.NewResponseController(w).Flush()
}
func readJSONLoose(w http.ResponseWriter, r *http.Request, x any) bool {
	if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(x); e != nil {
		APIError(w, 400, "invalid_json", "invalid JSON")
		return false
	}
	return true
}

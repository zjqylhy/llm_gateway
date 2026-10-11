package gateway

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
)

type rerankInput struct {
	Query           string   `json:"query"`
	Documents       []string `json:"documents"`
	TopN            int      `json:"top_n"`
	ReturnDocuments bool     `json:"return_documents"`
	Instruct        string   `json:"instruct"`
	MaxItemEstimate int64    `json:"-"`
}

func decodeRerank(r Request) (Request, error) {
	for key, value := range r.Raw {
		if !Contains([]string{"model", "query", "documents", "top_n", "return_documents", "instruct"}, key) || string(value) == "null" {
			return r, errors.New("unsupported or null rerank parameter: " + key)
		}
	}
	r.Rerank = &rerankInput{}
	input := r.Rerank
	if json.Unmarshal(r.Raw["query"], &input.Query) != nil || strings.TrimSpace(input.Query) == "" {
		return r, errors.New("query must be nonempty text")
	}
	if json.Unmarshal(r.Raw["documents"], &input.Documents) != nil || len(input.Documents) == 0 || len(input.Documents) > 500 {
		return r, errors.New("documents must contain 1 to 500 text items")
	}
	for _, document := range input.Documents {
		if strings.TrimSpace(document) == "" {
			return r, errors.New("documents must contain nonempty text")
		}
	}
	if raw, ok := r.Raw["top_n"]; ok {
		if json.Unmarshal(raw, &input.TopN) != nil || input.TopN <= 0 || input.TopN > 1000000 {
			return r, errors.New("top_n must be a positive integer up to 1000000")
		}
	}
	if input.TopN == 0 || input.TopN > len(input.Documents) {
		input.TopN = len(input.Documents)
	}
	if raw, ok := r.Raw["return_documents"]; ok && json.Unmarshal(raw, &input.ReturnDocuments) != nil {
		return r, errors.New("return_documents must be boolean")
	}
	if raw, ok := r.Raw["instruct"]; ok {
		if json.Unmarshal(raw, &input.Instruct) != nil || strings.TrimSpace(input.Instruct) == "" {
			return r, errors.New("instruct must be nonempty text")
		}
		r.Capabilities = append(r.Capabilities, "rerank_instruct")
	}
	r.MaxOutput = 0
	// 查询会参与每一对文档计算，不能按普通请求仅预占一次查询用量。
	queryBytes := int64(len(input.Query) + len(input.Instruct))
	r.InputEstimate = (queryBytes + 16) * int64(len(input.Documents))
	input.MaxItemEstimate = queryBytes
	for _, document := range input.Documents {
		size := int64(len(document))
		r.InputEstimate += size
		if size > input.MaxItemEstimate {
			input.MaxItemEstimate = size
		}
	}
	if r.InputEstimate <= 0 || r.InputEstimate > 1000000 {
		return r, errors.New("rerank input size out of range")
	}
	r.Text = input.Query
	r.SafetyText = input.Query + "\n" + input.Instruct + "\n" + strings.Join(input.Documents, "\n")
	return r, nil
}

func validRerankModel(m Model) bool {
	if !Contains(m.Capabilities, "rerank") {
		return m.RerankProtocol == "" && m.MaxDocuments == 0 && m.MaxItemTokens == 0 && !Contains(m.Capabilities, "rerank_instruct")
	}
	return Contains([]string{"compatible", "dashscope"}, m.RerankProtocol) &&
		m.MaxDocuments > 0 && m.MaxDocuments <= 500 && m.MaxItemTokens > 0 && m.MaxItemTokens <= m.Context &&
		m.MaxOutput == 0 && m.OutputMicro == 0 && m.CachedInputMicro == 0 &&
		!Contains(m.Capabilities, "chat") && !Contains(m.Capabilities, "embedding") && !Contains(m.Capabilities, "stream")
}

func (r Request) rerankPayload(m Model) ([]byte, error) {
	if r.Rerank == nil || !validRerankModel(m) || !Contains(m.Capabilities, "rerank") {
		return nil, errors.New("invalid rerank model or request")
	}
	input := r.Rerank
	parameters := map[string]any{"top_n": input.TopN}
	if input.Instruct != "" {
		parameters["instruct"] = input.Instruct
	}
	if m.RerankProtocol == "dashscope" {
		return json.Marshal(map[string]any{"model": m.Upstream, "input": map[string]any{"query": input.Query, "documents": input.Documents}, "parameters": parameters})
	}
	parameters["model"], parameters["query"], parameters["documents"] = m.Upstream, input.Query, input.Documents
	// 原文按索引在网关补齐，兼容不接受 return_documents 的上游模型。
	return json.Marshal(parameters)
}

type rerankResult struct {
	Index    *int     `json:"index"`
	Score    *float64 `json:"relevance_score"`
	Document *struct {
		Text string `json:"text"`
	} `json:"document,omitempty"`
}

func normalizeRerank(raw []byte, m Model, r Request, requestID string, usage Usage, quality string) ([]byte, error) {
	var response struct {
		ID        string          `json:"id"`
		RequestID string          `json:"request_id"`
		Error     json.RawMessage `json:"error"`
		Code      string          `json:"code"`
		Results   []rerankResult  `json:"results"`
		Output    struct {
			Results []rerankResult `json:"results"`
		} `json:"output"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Code != "" || (len(response.Error) > 0 && string(response.Error) != "null") {
		return nil, errors.New("invalid rerank response envelope")
	}
	results := response.Results
	if m.RerankProtocol == "dashscope" {
		results = response.Output.Results
	}
	if r.Rerank == nil || len(results) != r.Rerank.TopN {
		return nil, errors.New("incomplete rerank results")
	}
	seen := make(map[int]bool, len(results))
	for i := range results {
		item := &results[i]
		if item.Index == nil || *item.Index < 0 || *item.Index >= len(r.Rerank.Documents) || seen[*item.Index] || item.Score == nil || math.IsNaN(*item.Score) || math.IsInf(*item.Score, 0) || *item.Score < 0 || *item.Score > 1 {
			return nil, errors.New("invalid rerank result index or score")
		}
		seen[*item.Index] = true
		item.Document = nil
		if r.Rerank.ReturnDocuments {
			item.Document = &struct {
				Text string `json:"text"`
			}{r.Rerank.Documents[*item.Index]}
		}
	}
	sort.SliceStable(results, func(i, j int) bool { return *results[i].Score > *results[j].Score })
	id := response.ID
	if id == "" {
		id = response.RequestID
	}
	if id == "" {
		id = requestID
	}
	return json.Marshal(map[string]any{"id": id, "object": "list", "model": r.Alias, "results": results, "usage": usage, "usage_quality": quality})
}

// 重排只有输入费用，total_tokens 是账本的输入用量，缺失时保留估算。
func extractRerankUsage(raw []byte) (Usage, bool) {
	var response struct {
		Usage *struct {
			Input  *int64 `json:"prompt_tokens"`
			Total  *int64 `json:"total_tokens"`
			Output *int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Usage == nil || response.Usage.Total == nil {
		return Usage{}, false
	}
	u := response.Usage
	if *u.Total < 0 || *u.Total > 100000000 || (u.Input != nil && *u.Input != *u.Total) || (u.Output != nil && *u.Output != 0) {
		return Usage{}, false
	}
	return Usage{Input: *u.Total, Total: *u.Total}, true
}

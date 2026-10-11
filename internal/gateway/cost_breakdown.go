package gateway

import "fmt"

// 金额用整数格式化，避免 CSV 导出的大额账本因 float64 丢失微单位。
func microCNY(n int64) string {
	return fmt.Sprintf("%d.%06d", n/1000000, n%1000000)
}

// FeeParts 的分项使用累计取整，避免每项单独取整抬高账本金额。
type FeeParts struct {
	InputMicro       int64 `json:"input_micro"`
	CachedInputMicro int64 `json:"cached_input_micro"`
	OutputMicro      int64 `json:"output_micro"`
	UnallocatedMicro int64 `json:"unallocated_micro"`
}

type CostBreakdown struct {
	FeeParts
	Known bool `json:"known"`
}

func breakdown(e Event) CostBreakdown {
	u := e.Usage
	if e.Kind == "started" || e.Status == "started" || u.Input < 0 || u.Output < 0 || u.Cached < 0 || u.Cached > u.Input || u.Input+u.Output != u.Total || e.Model.InputMicro < 0 || e.Model.CachedInputMicro < 0 || e.Model.OutputMicro < 0 || e.Model.FXPPM < 0 || Cost(e.Model, u) != e.CostMicro {
		return CostBreakdown{FeeParts: FeeParts{UnallocatedMicro: e.CostMicro}}
	}
	input := Cost(e.Model, Usage{Input: u.Input - u.Cached})
	withCache := Cost(e.Model, Usage{Input: u.Input, Cached: u.Cached})
	return CostBreakdown{FeeParts: FeeParts{InputMicro: input, CachedInputMicro: withCache - input, OutputMicro: e.CostMicro - withCache}, Known: true}
}

func cacheUsageKnown(e Event) bool {
	return e.Usage.CachedReported || e.Usage.Cached > 0 || e.Quality == "reconciled"
}

// 明细只在管理端派生，历史事件载荷和用于幂等校验的哈希保持原样。
type RecordView struct {
	Event
	CostBreakdown       CostBreakdown `json:"cost_breakdown"`
	CacheReported       bool          `json:"cache_reported"`
	UsageBreakdownKnown bool          `json:"usage_breakdown_known"`
}

func recordViews(events []Event) []RecordView {
	out := make([]RecordView, 0, len(events))
	for _, e := range events {
		e.PayloadCipher = ""
		out = append(out, RecordView{Event: e, CostBreakdown: breakdown(e), CacheReported: cacheUsageKnown(e), UsageBreakdownKnown: e.Kind != "started" && e.Status != "started" && e.Usage.Input+e.Usage.Output == e.Usage.Total})
	}
	return out
}

package gateway

// 执行收尾和财务确认不同：估计只形成暴露额度，不恢复可用预算。
func billingExposure(e Event) (state string, counters Counters) {
	if e.Kind == "started" {
		return "pending", Counters{ReservedTokens: e.ReserveTokens, ReservedMicro: e.ReserveMicro}
	}
	if e.Quality == "estimated" || e.Quality == "pending" {
		return "held", Counters{ReservedTokens: max(e.ReserveTokens, e.Usage.Total), ReservedMicro: max(e.ReserveMicro, e.CostMicro)}
	}
	state = "settled"
	if e.Quality == "reconciled" {
		state = "reconciled"
	}
	return state, Counters{Tokens: e.Usage.Total, Micro: e.CostMicro}
}

func cancelEvent(e Event) Event {
	e.ID, e.Revision, e.Kind = e.AttemptID+":2", 2, "completed"
	e.Status, e.Quality, e.Usage, e.CostMicro = "cancelled", "reported", Usage{}, 0
	return e
}

type reservationHashKey struct{}

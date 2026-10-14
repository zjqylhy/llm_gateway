package gateway

import (
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// 固定业务指标白名单与逐选择器约束共同构成开发者的数据边界。
var applicationMetrics = map[string]bool{
	"gateway_api_calls_total": true, "gateway_calls_total": true,
	"gateway_api_seconds_bucket": true, "gateway_api_seconds_sum": true, "gateway_api_seconds_count": true,
	"gateway_app_qps_limit": true, "gateway_app_info": true,
	"gateway_tokens_total": true, "gateway_cost_micro_total": true,
	"gateway_attempts_total": true, "gateway_attempt_seconds_bucket": true,
	"gateway_attempt_seconds_sum": true, "gateway_attempt_seconds_count": true,
	"gateway_first_token_seconds_bucket": true, "gateway_first_token_seconds_sum": true,
	"gateway_first_token_seconds_count": true,
}

func grafanaApps(u User, cfg Config) []string {
	ids := []string{}
	for id, app := range cfg.Apps {
		if u.Role != "developer" || app.Owner == u.Name {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func applicationMatcher(ids []string) (*labels.Matcher, error) {
	if len(ids) == 0 {
		return nil, errors.New("no authorized applications")
	}
	escaped := make([]string, len(ids))
	for i, id := range ids {
		escaped[i] = regexp.QuoteMeta(id)
	}
	return labels.NewMatcher(labels.MatchRegexp, "app", strings.Join(escaped, "|"))
}

func parserSelector(query string) (string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", errors.New("invalid selector")
	}
	if _, ok := expr.(*parser.VectorSelector); !ok {
		return "", errors.New("metadata requires vector selector")
	}
	return expr.String(), nil
}

func scopedPromQL(query string, ids []string, admin bool) (string, error) {
	if len(query) == 0 || len(query) > 16<<10 {
		return "", errors.New("invalid expression size")
	}
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", errors.New("invalid PromQL")
	}
	var scope *labels.Matcher
	if !admin {
		scope, err = applicationMatcher(ids)
		if err != nil {
			return "", err
		}
	}
	// 官方 parser 的遍历包括二元表达式两侧、函数、矩阵及子查询。
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if err != nil {
			return err
		}
		switch n := node.(type) {
		case *parser.Call:
			// info 会隐式读取额外信息序列，无法由显式选择器约束。
			if !admin && n.Func.Name == "info" {
				err = errors.New("implicit information query denied")
			}
		case *parser.VectorSelector:
			if n.OriginalOffset < 0 || n.OriginalOffset > 24*time.Hour || n.Timestamp != nil || n.StartOrEnd != 0 {
				err = errors.New("historical selector outside monitoring window")
				return err
			}
			if !admin {
				if !applicationMetrics[n.Name] {
					err = errors.New("metric outside application dashboard")
					return err
				}
				// 保留客户端条件，追加权限条件取交集，不能覆盖为更宽的选择器。
				n.LabelMatchers = append(n.LabelMatchers, scope)
			}
		case *parser.MatrixSelector:
			if n.Range > 24*time.Hour {
				err = errors.New("range exceeds monitoring window")
			}
		case *parser.SubqueryExpr:
			if n.Range > 24*time.Hour || n.OriginalOffset < 0 || n.OriginalOffset > 24*time.Hour || n.Timestamp != nil || n.StartOrEnd != 0 || (n.Step > 0 && n.Step < 10*time.Second) {
				err = errors.New("subquery exceeds monitoring limits")
			}
		}
		return err
	})
	if err != nil {
		return "", err
	}
	return expr.String(), nil
}

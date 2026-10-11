package gateway

import "fmt"

// MySQL 在数据库内按快照拆分后聚合，避免报表传输整份事件及采样正文。
func costReportSQL(where string) string {
	number := func(path string) string {
		return fmt.Sprintf("COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(a.payload,'$.%s')) AS DECIMAL(65,0)),0)", path)
	}
	text := func(path string) string {
		return fmt.Sprintf("COALESCE(JSON_UNQUOTE(JSON_EXTRACT(a.payload,'$.%s')),'')", path)
	}
	base := fmt.Sprintf(`SELECT app_id,department,model_id,tokens,cost_micro,status,purpose,
%s AS quality,%s AS kind,%s AS i,%s AS o,%s AS c,%s AS ip,%s AS cp,%s AS op,
IF(%s=0,1000000,%s) AS fx,%s='true' AS cache_reported,
IF(purpose='cache_embedding',COALESCE((SELECT JSON_UNQUOTE(JSON_EXTRACT(r.payload,'$.cached'))='true'
 FROM gw_attempts r WHERE r.attempt_id=a.request_id AND r.app_id=a.app_id AND r.purpose='request'),0),0) AS cache_hit
FROM gw_attempts a WHERE purpose<>'request' AND %s`,
		text("quality"), text("kind"), number("usage.prompt_tokens"), number("usage.completion_tokens"), number("usage.cached_tokens"),
		number("model.input_micro"), number("model.cached_input_micro"), number("model.output_micro"),
		number("model.fx_ppm"), number("model.fx_ppm"), text("usage.cached_tokens_reported"), where)
	// 与 Cost 一样，微额向上取整并饱和到 int64；累计取差不会多收舍入费用。
	round := func(n string) string {
		product := "(" + n + ")*fx"
		// DIV 避免 DECIMAL 除法的默认小数位截断微额；先判饱和防止 DIV 溢出。
		return "IF(" + product + ">=9223372036854775807*CAST(1000000000000 AS DECIMAL(65,0)),9223372036854775807,(" + product + "+999999999999) DIV 1000000000000)"
	}
	prefix := fmt.Sprintf(`SELECT b.*,%s AS p0,%s AS p1,%s AS p2 FROM (%s) b`,
		round("(i-LEAST(i,GREATEST(0,c)))*ip"),
		round("(i-LEAST(i,GREATEST(0,c)))*ip+LEAST(i,GREATEST(0,c))*cp"),
		round("(i-LEAST(i,GREATEST(0,c)))*ip+LEAST(i,GREATEST(0,c))*cp+o*op"), base)
	valid := `SELECT p.*,status<>'started' AND kind<>'started' AND i>=0 AND o>=0 AND c>=0 AND c<=i AND ip>=0 AND cp>=0 AND op>=0 AND fx>=0 AND i+o=tokens AND p2=cost_micro AS known FROM (` + prefix + `) p`
	return `SELECT app_id,department,model_id,COUNT(*),COALESCE(SUM(tokens),0),COALESCE(SUM(cost_micro),0),SUM(status='started' OR quality='estimated'),
SUM(i),SUM(i-c),SUM(o),SUM(c),SUM(NOT (cache_reported OR c>0 OR quality='reconciled')),SUM(i+o<>tokens OR kind='started' OR status='started'),
SUM(IF(known,p0,0)),SUM(IF(known,p1-p0,0)),SUM(IF(known,cost_micro-p1,0)),SUM(IF(known,0,cost_micro)),SUM(NOT known),
SUM(purpose='cache_embedding'),SUM(IF(purpose='cache_embedding',tokens,0)),SUM(IF(purpose='cache_embedding',cost_micro,0)),SUM(cache_hit)
FROM (` + valid + `) d GROUP BY app_id,department,model_id LIMIT 10001`
}

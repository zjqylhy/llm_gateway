import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// 压测只能针对可控上游，避免把容量实验变成真实 API 消费。
if (__ENV.GW_LOADTEST_ACK !== 'controlled-upstream') throw new Error('set GW_LOADTEST_ACK=controlled-upstream after verifying target provider');
if (!__ENV.GW_API_KEY) throw new Error('GW_API_KEY is required');
const failed = new Counter('gateway_protocol_failures');
const stream = __ENV.GW_STREAM === 'true';
export const options = {
  scenarios: { traffic: { executor: 'constant-vus', vus: Number(__ENV.GW_VUS || 50), duration: __ENV.GW_DURATION || '1m' } },
  thresholds: { gateway_protocol_failures: ['count==0'], http_req_failed: ['rate<0.001'] },
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
};
export default function () {
  const r = http.post((__ENV.GW_URL || 'http://127.0.0.1:8080') + '/v1/chat/completions', JSON.stringify({model:__ENV.GW_ALIAS || 'chat',messages:[{role:'user',content:'controlled throughput probe'}],stream,max_tokens:64, ...(stream?{stream_options:{include_usage:true}}:{})}), {headers:{Authorization:'Bearer '+__ENV.GW_API_KEY,'Content-Type':'application/json','X-Gateway-Cache':'bypass'},timeout:'125s'});
  const ok = check(r, { 'successful protocol': r => r.status===200 && (stream?r.body.includes('data: [DONE]'):!!r.json('choices')) });
  if (!ok) failed.add(1);
}
// k6 HTTP 会缓冲 SSE；首事件延迟必须使用 Prometheus 指标或专门的流式客户端测量。

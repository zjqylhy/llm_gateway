export type TrafficPoint = { at: string; value: number };
export type TrafficSeries = {
  kind: "success" | "failure" | "limit";
  points: TrafficPoint[];
};
export type TrafficApp = {
  id: string;
  name: string;
  configured_qps: number;
  success_qps: number | null;
  failure_qps: number | null;
  limit_at_end: number | null;
  failures: { http_status: string; error_code: string; qps: number }[];
  series: TrafficSeries[];
};
export type TrafficData = {
  available: boolean;
  reason?: string;
  from: string;
  to: string;
  step_seconds: number;
  apps: TrafficApp[];
};

export function trafficWindowError(from: string, to: string, now = Date.now()) {
  const start = new Date(from).getTime();
  const end = new Date(to).getTime();
  if (
    !Number.isFinite(start) ||
    !Number.isFinite(end) ||
    end <= start ||
    end - start > 86400000 ||
    start < now - 86400000 - 30000 ||
    end > now + 30000
  ) {
    return "请选择最近 24 小时内的起止时间，结束时间须晚于开始时间。";
  }
  return "";
}

// 数据缺口保留断线；限流值为零表示不限制，不绘制成零 QPS 的限流线。
export function trafficSegments(series: TrafficSeries, stepSeconds: number) {
  const segments: TrafficPoint[][] = [];
  let current: TrafficPoint[] = [];
  for (const point of series.points) {
    const previous = current.at(-1);
    const gap =
      previous &&
      new Date(point.at).getTime() - new Date(previous.at).getTime() >
        stepSeconds * 1500;
    if (gap || (series.kind === "limit" && point.value === 0)) {
      if (current.length) segments.push(current);
      current = [];
    }
    if (series.kind !== "limit" || point.value !== 0) current.push(point);
  }
  if (current.length) segments.push(current);
  return segments;
}

export function qpsText(value: number | null) {
  return value === null
    ? "暂无数据"
    : value.toLocaleString("zh-CN", { maximumFractionDigits: 3 });
}

export async function fetchTraffic(
  appID: string,
  from: string,
  to: string,
  signal: AbortSignal,
  request: typeof fetch = fetch,
): Promise<TrafficData> {
  const query = new URLSearchParams({ from, to });
  if (appID) query.set("app_id", appID);
  const response = await request("/admin/app-traffic?" + query, { signal });
  const data = await response.json();
  if (!response.ok) throw Error(data.error?.message ?? "监控查询失败");
  return data as TrafficData;
}

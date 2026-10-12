export type InstanceStatus = "ready" | "stale" | "config_pending";

export const instanceLabel = (state: string) =>
  ({ ready: "在线", stale: "心跳过期", config_pending: "配置未同步" })[state] ??
  "状态未知";

export const routeSource = (scope?: string) =>
  scope === "global"
    ? "继承全局"
    : scope?.startsWith("application:")
      ? "应用专属"
      : scope === "internal"
        ? "应用缓存配置"
        : "历史记录未保存";

export const meteringLabel = (state: string) =>
  ({
    healthy: "计量正常",
    quiescing: "核验中",
    frozen: "计量暂停",
    unavailable: "计量暂停",
    initializing: "初始化中",
    offline: "离线模式",
  })[state] ?? "状态未知";

export function tracePath(requestID: string, appID: string) {
  return `trace/${encodeURIComponent(requestID)}?app_id=${encodeURIComponent(appID)}`;
}

export type ConsoleAPI = <T>(
  path: string,
  method?: string,
  body?: unknown,
) => Promise<T>;

export type ApplicationRoute = {
  alias: string;
  kind: string;
  strategy: string;
  candidates: { model_id: string; weight: number }[];
  gray: {
    run_id?: string;
    percent: number;
    candidate: string;
    state: string;
    min_samples: number;
    max_error: number;
    max_latency_ms: number;
  };
  source: string;
  config_version: number;
};

// 展示来源和版本只供查看，保存接口只接受路由配置。
export function routePayload(route: ApplicationRoute) {
  const { alias, kind, strategy, candidates, gray } = route;
  return { alias, kind, strategy, candidates, gray };
}

import { instanceLabel, meteringLabel } from "./application";

export type InstanceView = {
  version: number;
  seen: string;
  service: string;
  status: string;
  age_seconds: number;
};
export type MeteringOperations = {
  state: string;
  reason?: string;
  undelivered_events?: number;
  permit_valid?: boolean;
  applications?: { app_id: string; state: string; reason: string }[];
  reports?: {
    id?: string;
    app_id?: string;
    passed?: boolean;
    error?: string;
    seconds?: number;
    at?: string;
  }[];
  next?: string;
};
export function OperationsPanel({
  instances,
  metering,
  version,
  apps,
}: {
  instances: Record<string, InstanceView>;
  metering: MeteringOperations;
  version?: number;
  apps: { id: string; name: string }[];
}) {
  const nodes = Object.entries(instances).sort((a, b) =>
    b[1].seen.localeCompare(a[1].seen),
  );
  const live = nodes.filter(([, node]) => node.status !== "stale");
  const history = nodes.filter(([, node]) => node.status === "stale");
  const name = (id?: string) =>
    apps.find((app) => app.id === id)?.name || id || "全局离线核验";
  function table(rows: typeof nodes) {
    return (
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>服务</th>
              <th>实例 ID</th>
              <th>心跳状态</th>
              <th>配置版本</th>
              <th>最近心跳</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(([id, node]) => (
              <tr key={id}>
                <td>
                  {{
                    gateway: "请求网关",
                    admin: "管理服务",
                    worker: "计量消费者",
                    dev: "开发服务",
                  }[node.service] || "历史实例（角色未知）"}
                </td>
                <td className="mono">{id}</td>
                <td>
                  <span
                    className={`badge ${node.status === "ready" ? "ok" : ""}`}
                  >
                    {instanceLabel(node.status)}
                  </span>
                </td>
                <td>
                  v{node.version} / 期望 v{version ?? "—"}
                </td>
                <td>
                  {new Date(node.seen).toLocaleString("zh-CN")}
                  <small>{node.age_seconds} 秒前</small>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }
  return (
    <>
      <section>
        <h2>网关计量与配置</h2>
        <div className="kpis">
          <div className="kpi">
            <span>全局计量控制</span>
            <strong>{meteringLabel(metering.state)}</strong>
            <small>{metering.reason || "无冻结原因"}</small>
          </div>
          <div className="kpi">
            <span>未交付计量事件</span>
            <strong>{metering.undelivered_events ?? "—"}</strong>
          </div>
          <div className="kpi">
            <span>期望配置版本</span>
            <strong>v{version ?? "—"}</strong>
          </div>
          <div className="kpi">
            <span>新增付费调用许可</span>
            <strong>
              {metering.permit_valid == null
                ? "未提供"
                : metering.permit_valid
                  ? "有效"
                  : "不可用"}
            </strong>
            <small>本次查询结果</small>
          </div>
        </div>
        <p className="muted">
          这里展示网关控制与结算情况；服务心跳表示进程存活及配置同步，不等同于业务调用就绪。
        </p>
        {metering.applications
          ?.filter((app) => app.state !== "healthy")
          .map((app) => (
            <div className="warning" key={app.app_id}>
              {name(app.app_id)} · {meteringLabel(app.state)} · {app.reason}
            </div>
          ))}
        {metering.next && (
          <p className="muted">
            应用控制列表仅展示前 100 项，单应用状态请通过应用概览查询。
          </p>
        )}
      </section>
      <section>
        <h2>服务实例 · 当前 {live.length} 个</h2>
        {live.length ? (
          table(live)
        ) : (
          <p className="warning">未发现心跳新鲜的实例</p>
        )}
        <details className="instance-history">
          <summary>历史实例 / 心跳过期 · {history.length} 个</summary>
          <p className="muted">
            保留最近 24 小时记录，容器重建会产生新的实例 ID。
          </p>
          {history.length > 0 && table(history)}
        </details>
      </section>
      <section>
        <h2>计量核验历史</h2>
        <p className="muted">
          最近 100 条平台报告；应用概览单独展示各应用的最新核验结果。
        </p>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>应用</th>
                <th>核验结果</th>
                <th>时间</th>
                <th>耗时 / 详情</th>
              </tr>
            </thead>
            <tbody>
              {metering.reports?.map((report, i) => (
                <tr key={report.id ?? i}>
                  <td>
                    {name(report.app_id)}
                    <small className="mono">{report.app_id}</small>
                  </td>
                  <td>{report.passed ? "通过" : "未通过"}</td>
                  <td>
                    {report.at
                      ? new Date(report.at).toLocaleString("zh-CN")
                      : "—"}
                  </td>
                  <td>
                    {report.seconds?.toFixed(2) ?? "—"} 秒
                    {report.error && <small>{report.error}</small>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

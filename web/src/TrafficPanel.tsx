import React, { useEffect, useState } from "react";
import {
  fetchTraffic,
  qpsText,
  trafficSegments,
  trafficWindowError,
  type TrafficApp,
  type TrafficData,
} from "./traffic";

const legend = {
  success: { title: "成功 QPS", color: "#2363db" },
  failure: { title: "失败 QPS", color: "#d13c54" },
  limit: { title: "历史上限", color: "#ca8c20" },
};

function localTime(date: Date) {
  const offset = date.getTimezoneOffset() * 60000;
  return new Date(date.getTime() - offset).toISOString().slice(0, 19);
}

function TrafficChart({ app, data }: { app: TrafficApp; data: TrafficData }) {
  const from = new Date(data.from).getTime();
  const to = new Date(data.to).getTime();
  const finitePoints = app.series.flatMap((series) =>
    series.points.filter((point) => series.kind !== "limit" || point.value > 0),
  );
  if (!finitePoints.length)
    return <p className="muted">所选区间暂无趋势采样。</p>;
  const max = Math.max(1, ...finitePoints.map((point) => point.value)) * 1.1;
  const x = (timestamp: string) =>
    54 + ((new Date(timestamp).getTime() - from) / (to - from)) * 850;
  const y = (value: number) => 190 - (value / max) * 165;
  return (
    <>
      <svg
        className="traffic-chart"
        viewBox="0 0 960 230"
        role="img"
        aria-label={`${app.name} 的成功、失败 QPS 和历史限流上限趋势`}
      >
        {[0, 0.5, 1].map((fraction) => (
          <g key={fraction}>
            <line
              x1={54}
              x2={904}
              y1={y(max * fraction)}
              y2={y(max * fraction)}
              stroke="#e4eaf3"
            />
            <text x={46} y={y(max * fraction) + 4} textAnchor="end">
              {qpsText(max * fraction)}
            </text>
          </g>
        ))}
        {app.series.map((series) =>
          trafficSegments(series, data.step_seconds).map((points, index) => (
            <g key={`${series.kind}-${index}`}>
              <polyline
                fill="none"
                stroke={legend[series.kind].color}
                strokeWidth={2}
                strokeDasharray={series.kind === "limit" ? "6 4" : undefined}
                points={points
                  .map((point) => `${x(point.at)},${y(point.value)}`)
                  .join(" ")}
              />
              {points
                .filter(
                  (_, pointIndex) =>
                    pointIndex % Math.ceil(points.length / 120) === 0 ||
                    pointIndex === points.length - 1,
                )
                .map((point) => (
                  <circle
                    key={point.at}
                    cx={x(point.at)}
                    cy={y(point.value)}
                    r={points.length === 1 ? 3 : 5}
                    fill={legend[series.kind].color}
                    opacity={points.length === 1 ? 1 : 0}
                    className="traffic-point"
                  >
                    <title>
                      {new Date(point.at).toLocaleString("zh-CN")} ·
                      {legend[series.kind].title} {qpsText(point.value)}
                    </title>
                  </circle>
                ))}
            </g>
          )),
        )}
        <text x={54} y={215}>
          {new Date(data.from).toLocaleString("zh-CN")}
        </text>
        <text x={904} y={215} textAnchor="end">
          {new Date(data.to).toLocaleString("zh-CN")}
        </text>
      </svg>
      <div className="traffic-legend">
        {Object.entries(legend).map(([kind, item]) => (
          <span key={kind}>
            <i style={{ background: item.color }} /> {item.title}
          </span>
        ))}
      </div>
    </>
  );
}

export function TrafficPanel({
  appID,
  revision,
  requiresApp,
}: {
  appID: string;
  revision: number;
  requiresApp: boolean;
}) {
  const [preset, setPreset] = useState("3600");
  const [draftFrom, setDraftFrom] = useState(() =>
    localTime(new Date(Date.now() - 3600000)),
  );
  const [draftTo, setDraftTo] = useState(() => localTime(new Date()));
  const [custom, setCustom] = useState<{ from: string; to: string } | null>(
    null,
  );
  const [data, setData] = useState<TrafficData | null>(null);
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    setPending(false);
    if (requiresApp && !appID) return;
    if (preset === "custom" && !custom) {
      setData(null);
      setError("");
      return;
    }
    const abort = new AbortController();
    const to = custom?.to ?? new Date().toISOString();
    const from =
      custom?.from ??
      new Date(new Date(to).getTime() - Number(preset) * 1000).toISOString();
    const invalid = trafficWindowError(from, to);
    setData(null);
    setError(invalid);
    if (invalid) return;
    setPending(true);
    void fetchTraffic(appID, from, to, abort.signal)
      .then((next) => {
        if (!abort.signal.aborted) setData(next);
      })
      .catch((reason: unknown) => {
        if (!abort.signal.aborted)
          setError(reason instanceof Error ? reason.message : "监控查询失败");
      })
      .finally(() => {
        if (!abort.signal.aborted) setPending(false);
      });
    return () => abort.abort();
  }, [appID, revision, requiresApp, preset, custom, refresh]);
  return (
    <section className="panel traffic-panel">
      <div className="section-head">
        <div>
          <h2>应用调用监控</h2>
          <p className="muted">
            最近 24 小时内可自由选择区间 · 汇总所有网关实例
          </p>
        </div>
        <button
          className="secondary"
          disabled={pending}
          onClick={() => setRefresh((value) => value + 1)}
        >
          刷新监控
        </button>
      </div>
      <div className="filter-bar traffic-filter">
        <label className="field">
          监控时间范围
          <select
            value={preset}
            onChange={(event) => {
              setPreset(event.target.value);
              setCustom(null);
            }}
          >
            <option value="300">最近 5 分钟</option>
            <option value="3600">最近 1 小时</option>
            <option value="21600">最近 6 小时</option>
            <option value="86400">最近 24 小时</option>
            <option value="custom">自定义起止时间</option>
          </select>
        </label>
        {preset === "custom" && (
          <>
            <label className="field">
              开始时间
              <input
                type="datetime-local"
                step={1}
                value={draftFrom}
                onChange={(event) => setDraftFrom(event.target.value)}
              />
            </label>
            <label className="field">
              结束时间
              <input
                type="datetime-local"
                step={1}
                value={draftTo}
                onChange={(event) => setDraftTo(event.target.value)}
              />
            </label>
            <button
              onClick={() => {
                const invalid = trafficWindowError(draftFrom, draftTo);
                setError(invalid);
                if (!invalid)
                  setCustom({
                    from: new Date(draftFrom).toISOString(),
                    to: new Date(draftTo).toISOString(),
                  });
              }}
            >
              查询区间
            </button>
          </>
        )}
      </div>
      {error && (
        <div className="error" role="alert">
          {error}
        </div>
      )}
      {pending && <p className="muted">正在读取监控数据…</p>}
      {requiresApp && !appID && <p className="muted">请先选择自己的应用。</p>}
      {data && !data.available && <p className="muted">{data.reason}</p>}
      {data?.apps.map((app) => (
        <article className="traffic-app" key={app.id}>
          <h3>
            {app.name || app.id} <small>{app.id}</small>
          </h3>
          <div className="traffic-stats">
            <div>
              <span>区间平均成功 QPS</span>
              <strong>{qpsText(app.success_qps)}</strong>
            </div>
            <div>
              <span>区间平均失败 QPS</span>
              <strong>{qpsText(app.failure_qps)}</strong>
            </div>
            <div>
              <span>当前配置 QPS 上限</span>
              <strong>
                {app.configured_qps === 0
                  ? "不限制"
                  : qpsText(app.configured_qps)}
              </strong>
            </div>
            <div>
              <span>区间结束时生效上限</span>
              <strong>
                {app.limit_at_end === 0 ? "不限制" : qpsText(app.limit_at_end)}
              </strong>
            </div>
          </div>
          <TrafficChart app={app} data={data} />
          {app.failures.length > 0 ? (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>HTTP 状态码</th>
                    <th>业务错误码</th>
                    <th>区间平均失败 QPS</th>
                  </tr>
                </thead>
                <tbody>
                  {app.failures.map((failure) => (
                    <tr key={`${failure.http_status}:${failure.error_code}`}>
                      <td>{failure.http_status}</td>
                      <td>
                        <code>{failure.error_code}</code>
                      </td>
                      <td>{qpsText(failure.qps)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : app.failure_qps !== null ? (
            <p className="muted">所选区间未观测到失败调用。</p>
          ) : null}
        </article>
      ))}
      <p className="muted traffic-note">
        趋势按 1
        分钟滑窗计算；汇总为所选区间的平均速率。历史上限取实例生效值的最大值，0
        表示不限制。 应用限流使用令牌桶，额度耗尽返回 429；缺失采样保留空缺。
      </p>
    </section>
  );
}

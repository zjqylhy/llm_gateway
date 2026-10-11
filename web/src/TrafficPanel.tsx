import React, { useEffect, useState } from "react";
import {
  fetchTraffic,
  failureTrafficSeries,
  combinedTrafficSeries,
  trafficSeriesKey,
  trafficSeriesTitle,
  type TrafficSeries,
  qpsText,
  trafficSegments,
  trafficWindowError,
  type TrafficApp,
  type TrafficData,
} from "./traffic";

const failureColors = [
  "#d13c54",
  "#7c3aed",
  "#0891b2",
  "#c05621",
  "#2563eb",
  "#0f766e",
  "#a21caf",
  "#4d7c0f",
  "#b45309",
  "#be185d",
  "#4338ca",
  "#047857",
  "#0369a1",
  "#9f1239",
  "#6d28d9",
  "#15803d",
  "#b91c1c",
  "#0e7490",
  "#854d0e",
];

function seriesColor(series: TrafficSeries, index: number) {
  return series.kind === "failure"
    ? failureColors[index % failureColors.length]
    : series.kind === "success"
      ? "#2363db"
      : "#ca8c20";
}

function localTime(date: Date) {
  const offset = date.getTimezoneOffset() * 60000;
  return new Date(date.getTime() - offset).toISOString().slice(0, 19);
}

function TrafficChart({
  app,
  data,
  series,
  title,
  colors,
}: {
  app: TrafficApp;
  data: TrafficData;
  series: TrafficSeries[];
  title: string;
  colors?: Map<string, string>;
}) {
  const color = (line: TrafficSeries, index: number) =>
    colors?.get(trafficSeriesKey(line)) ?? seriesColor(line, index);
  const from = new Date(data.from).getTime();
  const to = new Date(data.to).getTime();
  const finitePoints = series.flatMap((series) =>
    series.points.filter((point) => series.kind !== "limit" || point.value > 0),
  );
  if (!finitePoints.length)
    return <p className="muted">所选区间暂无趋势采样。</p>;
  // 调用速率共用左轴，上限单独使用右轴，避免高上限压平低流量曲线。
  const callPeak = series
    .filter((line) => line.kind !== "limit")
    .flatMap((line) => line.points)
    .reduce((peak, point) => Math.max(peak, point.value), 0);
  const limitPeak = series
    .filter((line) => line.kind === "limit")
    .flatMap((line) => line.points)
    .reduce((peak, point) => Math.max(peak, point.value), 0);
  const max = (callPeak || 1) * 1.1;
  const limitMax = (limitPeak || 1) * 1.1;
  const x = (timestamp: string) =>
    60 + ((new Date(timestamp).getTime() - from) / (to - from)) * 820;
  const y = (value: number, limit = false) =>
    190 - (value / (limit ? limitMax : max)) * 155;
  return (
    <>
      <svg
        className="traffic-chart"
        viewBox="0 0 960 230"
        role="img"
        aria-label={`${app.name} 的${title}`}
      >
        <text x={60} y={16}>
          调用 QPS（左轴）
        </text>
        {limitPeak > 0 && (
          <text x={880} y={16} textAnchor="end" className="traffic-limit-axis">
            历史上限 QPS（右轴）
          </text>
        )}
        {[0, 0.5, 1].map((fraction) => (
          <g key={fraction}>
            <line
              x1={60}
              x2={880}
              y1={y(max * fraction)}
              y2={y(max * fraction)}
              stroke="#e4eaf3"
            />
            <text x={52} y={y(max * fraction) + 4} textAnchor="end">
              {qpsText(max * fraction)}
            </text>
            {limitPeak > 0 && (
              <text
                x={888}
                y={y(max * fraction) + 4}
                className="traffic-limit-axis"
              >
                {qpsText(limitMax * fraction)}
              </text>
            )}
          </g>
        ))}
        {series.map((series, seriesIndex) =>
          trafficSegments(series, data.step_seconds).map((points, index) => (
            <g
              key={`${trafficSeriesKey(series)}-${index}`}
              data-series={trafficSeriesKey(series)}
              data-axis={series.kind === "limit" ? "right" : "left"}
            >
              <polyline
                fill="none"
                stroke={color(series, seriesIndex)}
                strokeWidth={2}
                strokeDasharray={series.kind === "limit" ? "6 4" : undefined}
                points={points
                  .map(
                    (point) =>
                      `${x(point.at)},${y(point.value, series.kind === "limit")}`,
                  )
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
                    cy={y(point.value, series.kind === "limit")}
                    r={points.length === 1 ? 3 : 5}
                    fill={color(series, seriesIndex)}
                    opacity={points.length === 1 ? 1 : 0}
                    className="traffic-point"
                  >
                    <title>
                      {new Date(point.at).toLocaleString("zh-CN")} ·
                      {trafficSeriesTitle(series)}
                      {series.kind === "limit" ? "（右轴）" : ""}{" "}
                      {qpsText(point.value)}
                    </title>
                  </circle>
                ))}
            </g>
          )),
        )}
        <text x={60} y={215}>
          {new Date(data.from).toLocaleString("zh-CN")}
        </text>
        <text x={880} y={215} textAnchor="end">
          {new Date(data.to).toLocaleString("zh-CN")}
        </text>
      </svg>
      <div className="traffic-legend">
        {series.map((line, index) => (
          <span key={trafficSeriesKey(line)}>
            <i style={{ background: color(line, index) }} />{" "}
            {trafficSeriesTitle(line)}
            {line.kind === "limit" ? "（右轴）" : ""}
          </span>
        ))}
      </div>
    </>
  );
}

function TrafficTrends({
  app,
  data,
  selected,
  onSelect,
}: {
  app: TrafficApp;
  data: TrafficData;
  selected: string;
  onSelect: (value: string) => void;
}) {
  const failures = failureTrafficSeries(app.series);
  const colors = new Map(
    failures.map((line, index) => [
      trafficSeriesKey(line),
      seriesColor(line, index),
    ]),
  );
  const selection = failures.some((line) => trafficSeriesKey(line) === selected)
    ? selected
    : "";
  return (
    <>
      <div className="section-head traffic-failure-head">
        <h4>调用 QPS · 成功与分错误码失败</h4>
        {failures.length > 0 && (
          <label className="field">
            失败错误码
            <select
              aria-label={`${app.name} 的失败错误码`}
              value={selection}
              onChange={(event) => onSelect(event.target.value)}
            >
              <option value="">全部错误码</option>
              {failures.map((line) => (
                <option
                  key={trafficSeriesKey(line)}
                  value={trafficSeriesKey(line)}
                >
                  {trafficSeriesTitle(line)}
                </option>
              ))}
            </select>
          </label>
        )}
      </div>
      <TrafficChart
        app={app}
        data={data}
        series={combinedTrafficSeries(app.series, selection)}
        title="成功、分错误码失败 QPS 与历史上限趋势"
        colors={colors}
      />
      {app.failure_qps === 0 && (
        <p className="muted">所选区间未观测到失败调用。</p>
      )}
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
  // 选择保存在面板层，自动刷新暂时卸载曲线时仍保留每个应用的下钻条件。
  const [selectedFailures, setSelectedFailures] = useState<
    Record<string, string>
  >({});
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
          <TrafficTrends
            app={app}
            data={data}
            selected={selectedFailures[app.id] ?? ""}
            onSelect={(value) =>
              setSelectedFailures((current) => ({
                ...current,
                [app.id]: value,
              }))
            }
          />
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

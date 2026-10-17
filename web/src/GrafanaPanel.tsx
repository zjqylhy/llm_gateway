import { useEffect, useState } from "react";

type Monitoring = {
  enabled: boolean;
  available: boolean;
  dashboard: string;
  has_apps: boolean;
};

export function GrafanaPanel({
  appID,
  range,
  refresh,
  requiresApp,
}: {
  appID: string;
  range: string;
  refresh: boolean;
  requiresApp: boolean;
}) {
  const [status, setStatus] = useState<Monitoring | null>(null);
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setError("");
    fetch("/admin/monitoring", { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error("监控授权或连接不可用");
        setStatus(await response.json());
      })
      .catch((err) => {
        if (!controller.signal.aborted) setError(err.message);
      });
    return () => controller.abort();
  }, [retry, appID]);
  const hours = Math.min(Number(range) || 24, 24);
  const params = new URLSearchParams({
    "var-app": appID || "$__all",
    from: `now-${hours}h`,
    to: "now",
    refresh: refresh ? "10s" : "",
    kiosk: "tv",
    theme: "light",
  });
  const src = status
    ? `/admin/grafana/d/${status.dashboard}/monitoring?${params}`
    : "";
  return (
    <section className="panel grafana-panel">
      <div className="section-head">
        <div>
          <h2>应用与错误码监控</h2>
          <p className="muted">
            最近 {hours} 小时 · 错误码筛选仅影响错误面板；计量与成本以账本为准。
          </p>
        </div>
        <div className="actions">
          {src && (
            <a href={src} target="_blank" rel="noreferrer">
              独立打开
            </a>
          )}
          <button onClick={() => setRetry((value) => value + 1)}>
            重新连接
          </button>
        </div>
      </div>
      {error ? (
        <p role="alert">{error}</p>
      ) : !status ? (
        <p className="muted">正在连接监控…</p>
      ) : !status.enabled ? (
        <p className="muted">Grafana 尚未配置，请联系管理员。</p>
      ) : !status.available ? (
        <p role="alert">监控暂不可用，请重新连接或联系管理员。</p>
      ) : requiresApp && (!status.has_apps || !appID) ? (
        <p className="muted">请选择自己拥有的应用。</p>
      ) : (
        <iframe
          key={`${src}-${retry}`}
          className="grafana-frame"
          title="Grafana 应用监控"
          src={src}
          onLoad={(event) => {
            // 同源错误响应可以识别；内嵌查询错误由 Grafana 面板原样显示。
            const body =
              event.currentTarget.contentDocument?.body?.textContent || "";
            if (/^\s*\{\s*"(?:code|error|message)"/.test(body))
              setError("监控暂不可用，请重新连接或联系管理员。");
          }}
        />
      )}
    </section>
  );
}

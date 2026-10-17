import { useEffect, useState } from "react";
import { meteringLabel, type ConsoleAPI } from "./application";

type Summary = {
  app_id: string;
  name: string;
  state: string;
  impact: string;
  checked_at?: string;
  report_passed?: boolean;
  report_at?: string;
};
type Response = { applications: Summary[]; next: string };

export function ApplicationMetering({
  appID,
  revision,
  api,
}: {
  appID: string;
  revision: number;
  api: ConsoleAPI;
}) {
  const [page, setPage] = useState<Response>({ applications: [], next: "" });
  const [after, setAfter] = useState("");
  const [scope, setScope] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    setAfter("");
  }, [appID]);
  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");
    const q = new URLSearchParams({ app_id: appID, after });
    void api<Response>("app-metering?" + q)
      .then((value) => {
        if (active) {
          setPage(value);
          setScope(appID);
        }
      })
      .catch((e) => {
        if (active) setError(String(e));
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [appID, after, revision]);
  return (
    <>
      <h2>应用计量</h2>
      <p className="muted">关注当前应用的计量影响与最近核验结果。</p>
      {error ? (
        <p className="error">计量状态未知：{error}</p>
      ) : scope === appID && page.applications.length ? (
        page.applications.map((item) => (
          <div className="application-metering-row" key={item.app_id}>
            <div>
              <strong>{item.name || item.app_id}</strong>
              <small className="mono">{item.app_id}</small>
            </div>
            <span className={`badge ${item.state === "healthy" ? "ok" : ""}`}>
              {meteringLabel(item.state)}
            </span>
            <p>{item.impact}</p>
            <small>
              最近核验：
              {item.checked_at
                ? new Date(item.checked_at).toLocaleString("zh-CN")
                : "尚未核验"}
            </small>
            <small>
              最近报告：
              {item.report_passed == null
                ? "暂无报告"
                : item.report_passed
                  ? "通过"
                  : "未通过"}
              {item.report_at &&
                ` · ${new Date(item.report_at).toLocaleString("zh-CN")}`}
            </small>
          </div>
        ))
      ) : (
        <p className="muted">
          {loading ? "正在读取应用计量状态…" : "暂无可见应用"}
        </p>
      )}
      {(after || page.next) && (
        <div className="actions">
          {after && (
            <button className="secondary" onClick={() => setAfter("")}>
              返回首页
            </button>
          )}
          {page.next && (
            <button className="secondary" onClick={() => setAfter(page.next)}>
              下一页
            </button>
          )}
        </div>
      )}
    </>
  );
}

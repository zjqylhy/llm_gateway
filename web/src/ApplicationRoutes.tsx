import React, { useEffect, useState } from "react";
import {
  routeSource,
  routePayload,
  type ApplicationRoute,
  type ConsoleAPI,
} from "./application";

type Props = {
  appID: string;
  admin: boolean;
  models: { id: string; capabilities: string[] }[];
  api: ConsoleAPI;
  onSaved: () => Promise<void>;
};
export function ApplicationRoutes({
  appID,
  admin,
  models,
  api,
  onSaved,
}: Props) {
  const [routes, setRoutes] = useState<ApplicationRoute[]>([]);
  const [draft, setDraft] = useState<ApplicationRoute | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let active = true;
    setRoutes([]);
    setDraft(null);
    setError("");
    if (appID)
      void api<ApplicationRoute[]>(`apps/${encodeURIComponent(appID)}/routes`)
        .then((value) => {
          if (active) {
            const routes = value.map((r) => ({
              ...r,
              gray: {
                ...r.gray,
                min_samples: r.gray.min_samples || 20,
                max_error: r.gray.max_error || 0.05,
                max_latency_ms: r.gray.max_latency_ms || 5000,
              },
            }));
            setRoutes(routes);
            setDraft(routes[0] ?? null);
          }
        })
        .catch((e) => {
          if (active) setError(String(e));
        });
    return () => {
      active = false;
    };
  }, [appID, revision]);
  async function mutate(path: string, method: string, body?: unknown) {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const result = await api<{ published?: boolean }>(path, method, body);
      await onSaved();
      setRevision((v) => v + 1);
      setNotice(
        result.published === false
          ? "已保存，尚未发布生效"
          : "配置已保存并发布",
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }
  if (!appID)
    return (
      <section>
        <h2>应用路由</h2>
        <p className="muted">请选择一个应用查看其有效策略。</p>
      </section>
    );
  const path = `apps/${encodeURIComponent(appID)}/routes/${encodeURIComponent(draft?.alias ?? "")}`;
  const eligible = models.filter((m) =>
    m.capabilities.includes(draft?.kind ?? ""),
  );
  function move(index: number, delta: number) {
    if (!draft) return;
    const candidates = [...draft.candidates];
    [candidates[index], candidates[index + delta]] = [
      candidates[index + delta],
      candidates[index],
    ];
    setDraft({ ...draft, candidates });
  }
  return (
    <section>
      <div className="section-head">
        <h2>应用路由</h2>
        <button
          className="secondary"
          disabled={busy}
          onClick={() => setRevision((v) => v + 1)}
        >
          刷新策略
        </button>
      </div>
      <p className="muted">
        {admin
          ? "未配置专属策略时继承全局；保存后仅影响当前应用，恢复继承将删除覆盖配置。"
          : "这里展示应用的有效策略；专属配置、灰度和回滚由管理员管理。"}
      </p>
      {error && <p className="error">{error}</p>}
      {notice && <p className="notice">{notice}</p>}
      {routes.map((r) => (
        <button
          className="chip"
          key={r.alias}
          onClick={() => {
            setDraft(r);
            setNotice("");
          }}
        >
          {r.alias} · {routeSource(r.source)}
        </button>
      ))}
      {draft ? (
        <>
          <div className="decision">
            {draft.alias} · {draft.kind} · {routeSource(draft.source)} · 配置 v
            {draft.config_version}
          </div>
          <label className="field">
            <span>选择策略</span>
            <select
              disabled={!admin || busy}
              value={draft.strategy}
              onChange={(e) => setDraft({ ...draft, strategy: e.target.value })}
            >
              <option value="priority">按候选顺序</option>
              <option value="weight">按权重</option>
              <option value="cost">优先成本</option>
              <option value="latency">优先近期延迟</option>
            </select>
          </label>
          <h3>候选模型与降级顺序</h3>
          {draft.candidates.map((c, i) => (
            <div className="candidate" key={i}>
              {admin ? (
                <select
                  disabled={busy}
                  aria-label={`候选模型 ${i + 1}`}
                  value={c.model_id}
                  onChange={(e) =>
                    setDraft({
                      ...draft,
                      candidates: draft.candidates.map((x, n) =>
                        n === i ? { ...x, model_id: e.target.value } : x,
                      ),
                    })
                  }
                >
                  {!eligible.some((m) => m.id === c.model_id) && (
                    <option>{c.model_id}</option>
                  )}
                  {eligible.map((m) => (
                    <option key={m.id}>{m.id}</option>
                  ))}
                </select>
              ) : (
                <span>
                  {i + 1}. {c.model_id}
                </span>
              )}
              <input
                type="number"
                min={0}
                max={100}
                aria-label={`候选权重 ${i + 1}`}
                disabled={!admin || busy}
                value={c.weight}
                onChange={(e) =>
                  setDraft({
                    ...draft,
                    candidates: draft.candidates.map((x, n) =>
                      n === i ? { ...x, weight: Number(e.target.value) } : x,
                    ),
                  })
                }
              />
              %
              {admin && (
                <>
                  <button
                    className="quiet"
                    disabled={busy || i === 0}
                    onClick={() => move(i, -1)}
                  >
                    上移
                  </button>
                  <button
                    className="quiet"
                    disabled={busy || i === draft.candidates.length - 1}
                    onClick={() => move(i, 1)}
                  >
                    下移
                  </button>
                  <button
                    className="quiet danger"
                    disabled={busy}
                    onClick={() =>
                      setDraft({
                        ...draft,
                        candidates: draft.candidates.filter((_, n) => n !== i),
                      })
                    }
                  >
                    移除
                  </button>
                </>
              )}
            </div>
          ))}
          {admin && (
            <div className="actions">
              <button
                className="secondary"
                disabled={busy || !eligible.length}
                onClick={() =>
                  setDraft({
                    ...draft,
                    candidates: [
                      ...draft.candidates,
                      { model_id: eligible[0]?.id ?? "", weight: 0 },
                    ],
                  })
                }
              >
                添加候选
              </button>
              <button
                disabled={busy}
                onClick={() => void mutate(path, "PUT", routePayload(draft))}
              >
                保存应用策略
              </button>
              <button
                className="secondary"
                disabled={busy || draft.source === "global"}
                onClick={() => void mutate(path, "DELETE")}
              >
                恢复继承全局
              </button>
            </div>
          )}
          <h3>
            应用灰度 ·{" "}
            {draft.gray.state === "running"
              ? `运行中 ${draft.gray.percent}%`
              : "未运行"}
          </h3>
          {admin && draft.kind === "chat" ? (
            <>
              <div className="form-grid">
                <label className="field">
                  <span>灰度目标</span>
                  <select
                    disabled={busy}
                    value={draft.gray.candidate}
                    onChange={(e) =>
                      setDraft({
                        ...draft,
                        gray: { ...draft.gray, candidate: e.target.value },
                      })
                    }
                  >
                    <option value="">请选择</option>
                    {models
                      .filter((m) => m.capabilities.includes("chat"))
                      .map((m) => (
                        <option key={m.id}>{m.id}</option>
                      ))}
                  </select>
                </label>
                {(["min_samples", "max_error", "max_latency_ms"] as const).map(
                  (key) => (
                    <label className="field" key={key}>
                      <span>
                        {
                          {
                            min_samples: "最小样本量",
                            max_error: "最大错误率",
                            max_latency_ms: "最大延迟 / 毫秒",
                          }[key]
                        }
                      </span>
                      <input
                        type="number"
                        disabled={busy}
                        step={key === "max_error" ? ".01" : "1"}
                        value={draft.gray[key]}
                        onChange={(e) =>
                          setDraft({
                            ...draft,
                            gray: {
                              ...draft.gray,
                              [key]: Number(e.target.value),
                            },
                          })
                        }
                      />
                    </label>
                  ),
                )}
              </div>
              <div className="actions">
                {[10, 50, 100].map((percent) => (
                  <button
                    className="secondary"
                    key={percent}
                    disabled={busy}
                    onClick={() =>
                      void mutate("gray-release", "POST", {
                        app_id: appID,
                        alias: draft.alias,
                        policy: { ...draft.gray, percent, state: "running" },
                      })
                    }
                  >
                    应用放量 {percent}%
                  </button>
                ))}
                <button
                  className="secondary danger"
                  disabled={busy || draft.source === "global"}
                  onClick={() =>
                    void mutate("gray-release", "POST", {
                      app_id: appID,
                      alias: draft.alias,
                      policy: {
                        ...draft.gray,
                        percent: 0,
                        state: "rolled_back",
                      },
                    })
                  }
                >
                  回滚应用灰度
                </button>
              </div>
              <p className="muted">
                灰度操作使用已保存的候选策略；候选变更请先保存。
              </p>
            </>
          ) : (
            <p className="muted">
              目标 {draft.gray.candidate || "—"} · 最小样本{" "}
              {draft.gray.min_samples} · 灰度与回滚由管理员管理
            </p>
          )}
        </>
      ) : (
        <p className="muted">暂无有效路由</p>
      )}
    </section>
  );
}

import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { createRefreshQueue, waitForAll } from "./refresh";
import { TrafficPanel } from "./TrafficPanel";
import {
  cachedTokens,
  uncachedTokens,
  sourceLabel,
  type FeeParts,
  type Usage,
} from "./metering";
import { ApplicationMetering } from "./ApplicationMetering";
import { ApplicationRoutes } from "./ApplicationRoutes";
import {
  OperationsPanel,
  type InstanceView,
  type MeteringOperations,
} from "./OperationsPanel";
import { routeSource, tracePath } from "./application";
import { ModelResources, type Model } from "./ModelResources";
import "./style.css";

type App = {
  id: string;
  name: string;
  department: string;
  owner: string;
  active: boolean;
  models: string[];
  qps: number;
  monthly_tokens: number;
  budget_micro: number;
  budget_action: string;
  cache: boolean;
  cache_ttl: number;
  similarity: number;
  cache_epoch: number;
  embedding_model: string;
  log_sample: number;
};
type Session = { username: string; role: string; csrf: string };
type RecordItem = {
  app_name?: string;
  route_scope?: string;
  route_strategy?: string;
  config_version?: number;
  request_id: string;
  attempt_id: string;
  purpose: string;
  app_id: string;
  alias: string;
  status: string;
  quality: string;
  at: string;
  duration_ms: number;
  cached: boolean;
  cost_micro: number;
  kind: string;
  usage: Usage;
  cache_reported?: boolean;
  usage_breakdown_known?: boolean;
  cost_breakdown?: FeeParts & { known: boolean };
  model: Partial<Model>;
};
type Cost = FeeParts & {
  app_id: string;
  department: string;
  model_id: string;
  count: number;
  tokens: number;
  micro: number;
  pending: number;
  input_tokens: number;
  uncached_input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
  cache_unreported: number;
  usage_unreported: number;
  unallocated_count: number;
  cache_embedding_count: number;
  cache_embedding_tokens: number;
  cache_embedding_micro: number;
  cache_hits: number;
};
type Config = {
  runtime_capabilities?: { cache_enabled: boolean };
  version: number;
  apps: Record<string, App>;
  keys: Record<
    string,
    { id: string; app_id: string; state: string; expires: string }
  >;
  providers: Record<string, { id: string; kind: string; url: string }>;
  models: Record<string, Model>;
  model_references?: Record<string, string[]>;
  routes: Record<string, Route>;
  users: Record<string, { name: string; role: string }>;
  rules: Rule[];
};
type Route = {
  alias: string;
  kind: string;
  strategy: string;
  candidates: { model_id: string; weight: number }[];
  gray: {
    percent: number;
    candidate: string;
    state: string;
    min_samples: number;
    max_error: number;
    max_latency_ms: number;
  };
};
type Rule = { id: string; pattern: string; action: string };
const tabs = [
  ["dashboard", "应用概览"],
  ["playground", "Playground"],
  ["apps", "应用与密钥"],
  ["app-routes", "应用路由"],
  ["routes", "模型资源与默认路由"],
  ["operations", "网关运维"],
  ["cost", "成本治理"],
  ["logs", "调用审计"],
  ["security", "权限与安全"],
];
const money = (n: number) =>
  (n / 1e6).toLocaleString("zh-CN", {
    minimumFractionDigits: 4,
    maximumFractionDigits: 6,
  });
const number = (n: number) => n.toLocaleString("zh-CN");
const initialApp: App = {
  id: "",
  name: "",
  department: "",
  owner: "admin",
  active: true,
  models: ["chat"],
  qps: 100,
  monthly_tokens: 1000000,
  budget_micro: 10000000,
  budget_action: "reject",
  cache: false,
  cache_ttl: 300,
  similarity: 0.97,
  cache_epoch: 0,
  embedding_model: "embedding",
  log_sample: 0,
};
function Login({ onLogin }: { onLogin: (s: Session) => void }) {
  const [name, setName] = useState("admin");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const r = await fetch("/admin/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: name, password }),
      });
      const x = await r.json();
      if (!r.ok) throw Error(x.error?.message ?? "登录失败");
      onLogin(x);
      setPassword("");
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="login-wrap">
      <div className="login-intro">
        <span className="eyebrow">LLM GATEWAY</span>
        <h1>
          统一接入，
          <br />
          看清每一次调用。
        </h1>
        <p>
          模型路由、流量保护与成本治理，
          <br />
          在一个控制台中完成。
        </p>
        <div className="login-tags">
          <span>多厂商接入</span>
          <span>按应用计量</span>
          <span>全链路审计</span>
        </div>
      </div>
      <form className="login-card" onSubmit={submit}>
        <h2>登录控制台</h2>
        <p className="muted">使用平台管理员分配的账号</p>
        <Field label="账号">
          <input
            autoComplete="username"
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
          />
        </Field>
        <Field label="密码">
          <input
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
        </Field>
        {error && (
          <div role="alert" className="error">
            {error}
          </div>
        )}
        <button disabled={busy}>{busy ? "登录中…" : "进入控制台"}</button>
        <p className="small muted">本地模式的初始密码在启动终端显示。</p>
      </form>
    </div>
  );
}
function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
    </label>
  );
}
function Badge({ value }: { value: string }) {
  return (
    <span
      className={
        "badge " +
        ([
          "success",
          "fallback_success",
          "active",
          "ready",
          "finished",
          "reported",
          "reconciled",
        ].includes(value)
          ? "ok"
          : ["failed", "revoked", "stream_error", "blocked"].includes(value)
            ? "bad"
            : "")
      }
    >
      {value}
    </span>
  );
}
function Empty({ children }: { children: React.ReactNode }) {
  return <div className="empty">{children}</div>;
}
function Console({
  session,
  onLogout,
}: {
  session: Session;
  onLogout: () => void;
}) {
  const [tab, setTab] = useState("dashboard");
  const [trafficRevision, setTrafficRevision] = useState(0);
  const [apps, setApps] = useState<App[]>([]);
  const [cfg, setCfg] = useState<Config | null>(null);
  const [appID, setAppID] = useState("");
  const [records, setRecords] = useState<RecordItem[]>([]);
  const [costs, setCosts] = useState<Cost[]>([]);
  const [metrics, setMetrics] = useState({
    requests: 0,
    failed: 0,
    cache_hits: 0,
  });
  const [alerts, setAlerts] = useState<
    { id: string; kind: string; active: boolean; message: string }[]
  >([]);
  const [instances, setInstances] = useState<Record<string, InstanceView>>({});
  const [metering, setMetering] = useState<MeteringOperations>({
    state: "unknown",
  });
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(true);
  const [formApp, setFormApp] = useState<App | null>(null);
  const [key, setKey] = useState("");
  const [apiKey, setAPIKey] = useState("");
  const [prompt, setPrompt] = useState("");
  const [alias, setAlias] = useState("chat");
  const [stream, setStream] = useState(true);
  const [answer, setAnswer] = useState("");
  const [decision, setDecision] = useState("");
  const [trace, setTrace] = useState<{
    requestID: string;
    appID: string;
  } | null>(null);
  const [range, setRange] = useState("24");
  const [status, setStatus] = useState("");
  const [requestSearch, setRequestSearch] = useState("");
  const [provider, setProvider] = useState({
    id: "",
    kind: "openai",
    url: "https://api.openai.com/v1",
    api_key: "",
  });
  const [route, setRoute] = useState<Route>({
    alias: "chat",
    kind: "chat",
    strategy: "priority",
    candidates: [],
    gray: {
      percent: 0,
      candidate: "",
      state: "finished",
      min_samples: 20,
      max_error: 0.05,
      max_latency_ms: 5000,
    },
  });
  const [ruleText, setRuleText] = useState("[]");
  const [user, setUser] = useState({
    name: "",
    role: "developer",
    password: "",
  });
  const controller = React.useRef<AbortController | null>(null);
  const refreshQueue = React.useRef(createRefreshQueue());
  const latestLoad = React.useRef(loadData);
  latestLoad.current = loadData;
  const viewKey = JSON.stringify([appID, range, status, tab, requestSearch]);
  const activeView = React.useRef(viewKey);
  activeView.current = viewKey;
  async function api<T>(
    path: string,
    method = "GET",
    body?: unknown,
  ): Promise<T> {
    const r = await fetch("/admin/" + path, {
      method,
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": session.csrf,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const x = await r.json();
    if (r.status === 401) {
      onLogout();
      throw Error("登录已失效");
    }
    if (!r.ok) throw Error(x.error?.message ?? "操作失败");
    if (x.published === false) setNotice("已保存，尚未发布生效；请重试发布");
    return x as T;
  }
  function query() {
    const q = new URLSearchParams();
    if (appID) q.set("app_id", appID);
    q.set("from", new Date(Date.now() - Number(range) * 3600000).toISOString());
    if (status) q.set("status", status);
    if (requestSearch) q.set("request_id", requestSearch);
    return q.toString();
  }
  async function load() {
    setTrafficRevision((value) => value + 1);
    return refreshQueue.current(latestLoad.current);
  }
  async function loadData() {
    setError("");
    try {
      const appList = await api<App[]>("apps");
      if (activeView.current !== viewKey) return;
      setApps(appList);
      if (tab === "operations" && session.role === "admin") {
        const [c, nodes, meter] = await waitForAll([
          api<Config>("config"),
          api<typeof instances>("instances"),
          api<typeof metering>("metering"),
        ]);
        if (activeView.current !== viewKey) return;
        setCfg(c);
        setInstances(nodes);
        setMetering(meter);
        return;
      }
      if (session.role === "developer" && !appID) {
        setAppID(appList[0]?.id ?? "");
        return;
      }
      const q = query();
      const results = await waitForAll([
        api<RecordItem[]>("call-records?" + q),
        api<Cost[]>("cost-report?" + q),
        api<typeof metrics>("metrics?" + q),
        api<typeof alerts>("alerts?" + q),
      ]);
      if (activeView.current !== viewKey) return;
      setRecords(results[0]);
      setCosts(results[1]);
      setMetrics(results[2]);
      setAlerts(results[3]);
      if (session.role === "admin") {
        const c = await api<Config>("config");
        if (activeView.current !== viewKey) return;
        setCfg(c);
        setRuleText(JSON.stringify(c.rules, null, 2));
      }
    } catch (e) {
      if (activeView.current === viewKey) setError(String(e));
    }
  }
  useEffect(() => {
    void load();
  }, [appID, range, status, tab]);
  useEffect(() => {
    if (!refresh) return;
    const id = setInterval(() => void load(), 10000);
    return () => clearInterval(id);
  }, [refresh, appID, range, status, tab]);
  async function action(
    path: string,
    method: string,
    body?: unknown,
    onError?: (message: string) => void,
  ) {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const x = await api<{ api_key?: string; published?: boolean }>(
        path,
        method,
        body,
      );
      if (x.api_key) setKey(x.api_key);
      setNotice(
        x.published === false
          ? "已保存，尚未发布生效；请重试发布"
          : "操作已完成",
      );
      await load();
      return true;
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setError(message);
      onError?.(message);
      return false;
    } finally {
      setBusy(false);
    }
  }
  async function send() {
    setBusy(true);
    setError("");
    setAnswer("");
    setDecision("等待路由决策…");
    controller.current = new AbortController();
    try {
      const r = await fetch("/admin/playground", {
        signal: controller.current.signal,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": session.csrf,
          Authorization: "Bearer " + apiKey,
        },
        body: JSON.stringify({
          model: alias,
          messages: [{ role: "user", content: prompt }],
          stream,
          stream_options: { include_usage: true },
        }),
      });
      setDecision(
        `请求 ${r.headers.get("X-Request-Id") ?? "—"} · 模型 ${r.headers.get("X-Gateway-Model") ?? "—"} · 缓存 ${r.headers.get("X-Gateway-Cache") ?? "未命中"}`,
      );
      if (!r.ok) {
        const x = await r.json();
        throw Error(x.error?.message ?? "调用失败");
      }
      if (!stream) {
        setAnswer(JSON.stringify(await r.json(), null, 2));
        return;
      }
      const reader = r.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = "",
        text = "";
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let index;
        while ((index = buffer.indexOf("\n\n")) >= 0) {
          const line = buffer.slice(0, index);
          buffer = buffer.slice(index + 2);
          const data = line
            .split("\n")
            .filter((s) => s.startsWith("data:"))
            .map((s) => s.slice(5).trim())
            .join("\n");
          if (!data || data === "[DONE]") continue;
          const x = JSON.parse(data);
          if (x.error) throw Error(x.error.message);
          for (const choice of x.choices ?? []) {
            text += choice.delta?.content ?? "";
            if (choice.delta?.tool_calls)
              text += "\n" + JSON.stringify(choice.delta.tool_calls);
          }
          if (x.usage) text += "\n\n用量：" + JSON.stringify(x.usage);
          setAnswer(text);
        }
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
      void load();
    }
  }
  const admin = session.role === "admin";
  const visibleTabs = tabs.filter(
    ([id]) => admin || !["routes", "operations", "security"].includes(id),
  );
  const activeAlerts = alerts.filter((a) => a.active);
  const modelList = Object.values(cfg?.models ?? {});
  const routeList = Object.values(cfg?.routes ?? {});
  return (
    <div className="layout">
      <aside>
        <div className="brand">
          <span className="brand-icon">G</span>
          <div>
            大模型网关<small>LLM GATEWAY</small>
          </div>
        </div>
        <nav>
          {[
            { label: "应用业务", operational: false },
            { label: "网关管理", operational: true },
          ].map((group) => {
            const items = visibleTabs.filter(
              ([id]) =>
                ["routes", "operations", "security"].includes(id) ===
                group.operational,
            );
            return (
              items.length > 0 && (
                <React.Fragment key={group.label}>
                  <div className="nav-group">{group.label}</div>
                  {items.map(([id, label]) => (
                    <button
                      key={id}
                      className={tab === id ? "selected" : ""}
                      onClick={() => setTab(id)}
                    >
                      {label}
                    </button>
                  ))}
                </React.Fragment>
              )
            );
          })}
        </nav>
        <div className="user">
          <span>{session.username}</span>
          <small>{session.role}</small>
          <button
            className="quiet"
            onClick={() =>
              void api("logout", "POST")
                .then(onLogout)
                .catch((e) => setError(String(e)))
            }
          >
            退出登录
          </button>
        </div>
      </aside>
      <main>
        <header>
          <div>
            <span className="eyebrow">WORKSPACE / 应用与网关治理</span>
            <h1>{tabs.find(([id]) => id === tab)?.[1]}</h1>
          </div>
          <div className="toolbar">
            <span className="muted">
              {["routes", "operations", "security"].includes(tab)
                ? "网关管理"
                : "应用业务"}
            </span>
            <button className="secondary" onClick={() => void load()}>
              刷新
            </button>
          </div>
        </header>
        {error && (
          <div className="error" role="alert">
            {error}
          </div>
        )}
        {notice && (
          <div className="notice" role="status">
            {notice}
          </div>
        )}
        {key && (
          <div className="secret-panel">
            <strong>密钥仅展示一次，请保存</strong>
            <code>{key}</code>
            <button
              className="secondary"
              onClick={() => {
                setKey("");
                setNotice("密钥已隐藏");
              }}
            >
              已保存，隐藏
            </button>
          </div>
        )}
        {["dashboard", "cost", "logs", "app-routes"].includes(tab) && (
          <div className="filter-bar">
            <Field label="应用">
              <select
                value={appID}
                onChange={(e) => {
                  setAppID(e.target.value);
                  setTrace(null);
                  setRecords([]);
                  setCosts([]);
                  setAlerts([]);
                  setMetrics({ requests: 0, failed: 0, cache_hits: 0 });
                }}
              >
                {session.role !== "developer" && (
                  <option value="">全部应用</option>
                )}
                {apps.map((a) => (
                  <option key={a.id} value={a.id}>
                    {a.name}
                  </option>
                ))}
              </select>
            </Field>
            {tab !== "app-routes" && (
              <>
                <Field label="时间范围">
                  <select
                    value={range}
                    onChange={(e) => setRange(e.target.value)}
                  >
                    <option value="1">最近 1 小时</option>
                    <option value="24">最近 24 小时</option>
                    <option value="720">最近 30 天</option>
                  </select>
                </Field>
                <label className="check">
                  <input
                    type="checkbox"
                    checked={refresh}
                    onChange={(e) => setRefresh(e.target.checked)}
                  />
                  每 10 秒刷新
                </label>
              </>
            )}
          </div>
        )}
        {tab === "dashboard" && (
          <>
            <TrafficPanel
              appID={appID}
              revision={trafficRevision}
              requiresApp={session.role === "developer"}
            />
            <div className="kpis">
              {[
                ["请求样本", number(metrics.requests)],
                ["失败请求", number(metrics.failed)],
                ["缓存命中", number(metrics.cache_hits)],
                [
                  "上游成本 / 元",
                  money(costs.reduce((n, x) => n + x.micro, 0)),
                ],
              ].map(([label, value]) => (
                <div className="kpi" key={label}>
                  <span>{label}</span>
                  <strong>{value}</strong>
                  <small>当前时间范围</small>
                </div>
              ))}
            </div>
            <div className="grid">
              <section>
                <h2>近期调用</h2>
                <p className="muted">点击请求编号查看尝试与降级路径</p>
                <RecordTable
                  rows={records
                    .filter((x) => x.purpose === "request")
                    .slice(0, 8)}
                  apps={apps}
                  onTrace={(record) => {
                    setTrace({
                      requestID: record.request_id,
                      appID: record.app_id,
                    });
                    setTab("logs");
                  }}
                />
              </section>
              <section>
                <ApplicationMetering
                  key={appID}
                  appID={appID}
                  revision={trafficRevision}
                  api={api}
                />
                <h3>预算与异常告警</h3>
                {activeAlerts.length ? (
                  activeAlerts.map((a) => (
                    <div className="warning" key={a.id}>
                      {a.message}
                    </div>
                  ))
                ) : (
                  <Empty>暂无预算或异常调用告警</Empty>
                )}
                <p className="small muted">
                  概览统计最近 1000 条记录；容量、趋势和延迟分位数请查看部署的
                  Grafana。
                </p>
              </section>
            </div>
          </>
        )}
        {tab === "operations" && admin && (
          <OperationsPanel
            instances={instances}
            metering={metering}
            version={cfg?.version}
            apps={apps}
          />
        )}
        {tab === "app-routes" && (
          <ApplicationRoutes
            key={appID}
            appID={appID}
            admin={admin}
            models={modelList}
            api={api}
            onSaved={load}
          />
        )}
        {tab === "playground" && (
          <div className="grid playground">
            <section>
              <h2>发送请求</h2>
              <Field label="应用 API Key">
                <input
                  type="password"
                  autoComplete="off"
                  value={apiKey}
                  onChange={(e) => setAPIKey(e.target.value)}
                  placeholder="只在当前页面使用，不保存"
                />
              </Field>
              <Field label="模型别名">
                <input
                  value={alias}
                  onChange={(e) => setAlias(e.target.value)}
                  list="aliases"
                />
                <datalist id="aliases">
                  {(routeList.length
                    ? routeList.map((r) => r.alias)
                    : apps.flatMap((a) => a.models)
                  ).map((x) => (
                    <option key={x}>{x}</option>
                  ))}
                </datalist>
              </Field>
              <Field label="Prompt">
                <textarea
                  rows={9}
                  value={prompt}
                  onChange={(e) => setPrompt(e.target.value)}
                  placeholder="输入测试问题；本地可用 [fail-primary] 验证降级"
                />
              </Field>
              <label className="check">
                <input
                  type="checkbox"
                  checked={stream}
                  onChange={(e) => setStream(e.target.checked)}
                />
                流式返回
              </label>
              <button
                disabled={busy || !prompt || !apiKey}
                onClick={() => void send()}
              >
                {busy ? "生成中…" : "发送请求"}
              </button>
              {busy && (
                <button
                  className="secondary"
                  onClick={() => controller.current?.abort()}
                >
                  停止生成
                </button>
              )}
            </section>
            <section>
              <h2>回答与调用结果</h2>
              <p className="decision">
                {decision || "发送后展示实际路由和请求编号"}
              </p>
              <pre className="answer">{answer || "模型回答将在这里出现。"}</pre>
            </section>
          </div>
        )}
        {tab === "apps" && (
          <section>
            <div className="section-head">
              <h2>接入应用</h2>
              {admin && (
                <button
                  onClick={() =>
                    setFormApp({ ...initialApp, owner: session.username })
                  }
                >
                  新建应用
                </button>
              )}
            </div>
            {apps.length ? (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>应用</th>
                      <th>部门</th>
                      <th>状态</th>
                      <th>QPS / 月度 Token</th>
                      <th>预算</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {apps.map((a) => (
                      <tr key={a.id}>
                        <td>
                          <strong>{a.name}</strong>
                          <small>{a.id}</small>
                        </td>
                        <td>{a.department || "—"}</td>
                        <td>
                          <Badge value={a.active ? "active" : "disabled"} />
                        </td>
                        <td>
                          {a.qps} / {number(a.monthly_tokens)}
                        </td>
                        <td>¥{money(a.budget_micro)}</td>
                        <td>
                          {admin && (
                            <>
                              <button
                                className="quiet"
                                onClick={() => setFormApp(a)}
                              >
                                编辑
                              </button>
                              <button
                                className="quiet"
                                onClick={() =>
                                  void action(`apps/${a.id}/keys`, "POST")
                                }
                              >
                                签发密钥
                              </button>
                              {a.cache &&
                                cfg?.runtime_capabilities?.cache_enabled !==
                                  false && (
                                  <button
                                    className="quiet"
                                    onClick={() =>
                                      void action(
                                        `apps/${a.id}/cache/clear`,
                                        "POST",
                                      )
                                    }
                                  >
                                    清除缓存
                                  </button>
                                )}
                            </>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <Empty>创建应用后即可签发密钥并接入。</Empty>
            )}
            {admin && (
              <>
                <h3>密钥状态</h3>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>编号</th>
                        <th>应用</th>
                        <th>状态</th>
                        <th>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.values(cfg?.keys ?? {}).map((k) => (
                        <tr key={k.id}>
                          <td>{k.id}</td>
                          <td>{cfg?.apps[k.app_id]?.name ?? k.app_id}</td>
                          <td>
                            <Badge value={k.state} />
                          </td>
                          <td>
                            <button
                              className="quiet"
                              disabled={busy || k.state === "revoked"}
                              onClick={() =>
                                void action(`keys/${k.id}/rotate`, "POST", {
                                  grace_seconds: 3600,
                                })
                              }
                            >
                              轮换（宽限 1 小时）
                            </button>
                            <button
                              className="quiet danger"
                              disabled={busy || k.state === "revoked"}
                              onClick={() => {
                                if (
                                  confirm(
                                    "吊销后该密钥将无法继续调用，确定吊销？",
                                  )
                                )
                                  void action(`keys/${k.id}/revoke`, "POST");
                              }}
                            >
                              吊销
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </section>
        )}
        {formApp && (
          <div className="modal-backdrop">
            <form
              className="modal"
              onSubmit={async (ev) => {
                ev.preventDefault();
                if (
                  await action(
                    formApp.id ? `apps/${formApp.id}` : "apps",
                    formApp.id ? "PUT" : "POST",
                    formApp,
                  )
                )
                  setFormApp(null);
              }}
            >
              <h2>{formApp.id ? "编辑应用" : "新建应用"}</h2>
              <div className="form-grid">
                {(["name", "department", "owner"] as const).map((k) => (
                  <Field
                    label={
                      { name: "名称", department: "部门", owner: "负责人账号" }[
                        k
                      ]
                    }
                    key={k}
                  >
                    <input
                      required={k === "name"}
                      value={formApp[k]}
                      onChange={(e) =>
                        setFormApp({ ...formApp, [k]: e.target.value })
                      }
                    />
                  </Field>
                ))}
                <Field label="允许的模型别名（逗号分隔）">
                  <input
                    value={formApp.models.join(",")}
                    onChange={(e) =>
                      setFormApp({
                        ...formApp,
                        models: e.target.value
                          .split(",")
                          .map((s) => s.trim())
                          .filter(Boolean),
                      })
                    }
                  />
                </Field>
                {(
                  [
                    "qps",
                    "monthly_tokens",
                    "cache_ttl",
                    "similarity",
                    "log_sample",
                  ] as const
                ).map((k) => (
                  <Field
                    label={
                      {
                        qps: "QPS 上限（0 不限制）",
                        monthly_tokens: "月度 Token 配额",
                        cache_ttl: "缓存有效期 / 秒",
                        similarity: "相似度阈值",
                        log_sample: "正文采样率（0 关闭，1 全量）",
                      }[k]
                    }
                    key={k}
                  >
                    <input
                      type="number"
                      min="0"
                      step={
                        ["similarity", "log_sample"].includes(k) ? ".01" : "1"
                      }
                      value={formApp[k]}
                      onChange={(e) =>
                        setFormApp({ ...formApp, [k]: Number(e.target.value) })
                      }
                    />
                  </Field>
                ))}
                <Field label="月度预算 / 元">
                  <input
                    type="number"
                    min="0"
                    step=".01"
                    value={formApp.budget_micro / 1e6}
                    onChange={(e) =>
                      setFormApp({
                        ...formApp,
                        budget_micro: Math.round(Number(e.target.value) * 1e6),
                      })
                    }
                  />
                </Field>
                <Field label="预算耗尽策略">
                  <select
                    value={formApp.budget_action}
                    onChange={(e) =>
                      setFormApp({ ...formApp, budget_action: e.target.value })
                    }
                  >
                    <option value="reject">拒绝新调用</option>
                    <option value="warn">仅告警</option>
                  </select>
                </Field>
                <Field label="缓存向量模型">
                  <select
                    disabled={
                      cfg?.runtime_capabilities?.cache_enabled === false
                    }
                    value={formApp.embedding_model}
                    onChange={(e) =>
                      setFormApp({
                        ...formApp,
                        embedding_model: e.target.value,
                      })
                    }
                  >
                    {modelList
                      .filter((m) => m.capabilities.includes("embedding"))
                      .map((m) => (
                        <option key={m.id}>{m.id}</option>
                      ))}
                  </select>
                </Field>
                <label className="check">
                  <input
                    type="checkbox"
                    checked={formApp.active}
                    onChange={(e) =>
                      setFormApp({ ...formApp, active: e.target.checked })
                    }
                  />
                  应用启用
                </label>
                <label className="check">
                  <input
                    type="checkbox"
                    checked={formApp.cache}
                    disabled={
                      cfg?.runtime_capabilities?.cache_enabled === false &&
                      !formApp.cache
                    }
                    onChange={(e) =>
                      setFormApp({ ...formApp, cache: e.target.checked })
                    }
                  />
                  开启应用内语义缓存
                </label>
              </div>
              <p className="small muted">
                {cfg?.runtime_capabilities?.cache_enabled === false
                  ? "当前部署已关闭语义缓存。已有策略会保留，所有请求均绕过缓存；取消勾选后无法在此部署重新开启。"
                  : "缓存只用于无工具、单轮、非实时问答；敏感请求自动绕过。"}
              </p>
              <div className="actions">
                <button disabled={busy}>保存并发布</button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => setFormApp(null)}
                >
                  取消
                </button>
              </div>
            </form>
          </div>
        )}
        {tab === "routes" && admin && (
          <>
            <section>
              <div className="section-head">
                <h2>配置发布</h2>
                <button
                  className="secondary"
                  disabled={busy}
                  onClick={() => void action("config/publish", "POST")}
                >
                  重试发布 v{cfg?.version}
                </button>
              </div>
              <p className="muted">
                保存后在网关运维查看实例配置版本；应用路由可单独覆盖默认策略。
              </p>
            </section>
            <section>
              <h2>厂商凭据</h2>
              <p className="muted">
                配置厂商连接和密钥，再新增该厂商的模型资源。下方编号是已配置厂商，点击可编辑。
              </p>
              {Object.values(cfg?.providers ?? {}).map((p) => (
                <button
                  className="chip"
                  key={p.id}
                  onClick={() => setProvider({ ...p, api_key: "" })}
                >
                  {p.id}
                </button>
              ))}
              <div className="form-grid">
                <Field label="厂商编号">
                  <input
                    value={provider.id}
                    onChange={(e) =>
                      setProvider({ ...provider, id: e.target.value })
                    }
                  />
                </Field>
                <Field label="厂商类型">
                  <select
                    value={provider.kind}
                    onChange={(e) =>
                      setProvider({ ...provider, kind: e.target.value })
                    }
                  >
                    {["deepseek", "qwen", "openai"].map((x) => (
                      <option key={x}>{x}</option>
                    ))}
                  </select>
                </Field>
                <Field label="API Base URL">
                  <input
                    value={provider.url}
                    onChange={(e) =>
                      setProvider({ ...provider, url: e.target.value })
                    }
                  />
                </Field>
                <Field label="厂商 API Key">
                  <input
                    type="password"
                    autoComplete="off"
                    value={provider.api_key}
                    onChange={(e) =>
                      setProvider({ ...provider, api_key: e.target.value })
                    }
                    placeholder="编辑时留空可保留原密钥"
                  />
                </Field>
              </div>
              <button
                disabled={busy}
                onClick={async () => {
                  if (await action("providers", "POST", provider))
                    setProvider({ ...provider, api_key: "" });
                }}
              >
                保存凭据
              </button>
            </section>
            <ModelResources
              models={modelList}
              providers={cfg?.providers ?? {}}
              references={cfg?.model_references}
              busy={busy}
              loaded={!!cfg}
              onMutation={action}
            />
            <section>
              <h2>默认路由与灰度</h2>
              <p className="muted">
                应用通过模型别名（如
                chat）调用；默认路由从已保存模型中选择实际模型。应用路由可覆盖此处的默认策略。
              </p>
              <p className="muted small">已保存默认路由（点击别名编辑）</p>
              {routeList.map((r) => (
                <button
                  className="chip"
                  key={r.alias}
                  onClick={() => setRoute(r)}
                >
                  {r.alias}
                </button>
              ))}
              <div className="form-grid">
                <Field label="模型别名">
                  <input
                    value={route.alias}
                    onChange={(e) =>
                      setRoute({ ...route, alias: e.target.value })
                    }
                  />
                </Field>
                <Field label="类型">
                  <select
                    value={route.kind}
                    onChange={(e) =>
                      setRoute({
                        ...route,
                        kind: e.target.value,
                        candidates: route.candidates.filter((candidate) =>
                          cfg?.models[
                            candidate.model_id
                          ]?.capabilities.includes(e.target.value),
                        ),
                        gray: {
                          ...route.gray,
                          state: "stopped",
                          percent: 0,
                          candidate: "",
                        },
                      })
                    }
                  >
                    <option value="chat">Chat</option>
                    <option value="embedding">Embedding</option>
                    <option value="rerank">Rerank 重排</option>
                  </select>
                </Field>
                <Field label="选择策略">
                  <select
                    value={route.strategy}
                    onChange={(e) =>
                      setRoute({ ...route, strategy: e.target.value })
                    }
                  >
                    <option value="priority">按候选顺序</option>
                    <option value="weight">按权重</option>
                    <option value="cost">优先成本</option>
                    <option value="latency">优先近期延迟</option>
                  </select>
                </Field>
              </div>
              <h3>候选模型（顺序同时作为降级顺序）</h3>
              {route.candidates.map((c, i) => (
                <div className="candidate" key={i}>
                  <select
                    value={c.model_id}
                    onChange={(e) =>
                      setRoute({
                        ...route,
                        candidates: route.candidates.map((x, n) =>
                          n === i ? { ...x, model_id: e.target.value } : x,
                        ),
                      })
                    }
                  >
                    {modelList
                      .filter((m) => m.capabilities.includes(route.kind))
                      .map((m) => (
                        <option key={m.id}>{m.id}</option>
                      ))}
                  </select>
                  <input
                    aria-label="权重"
                    type="number"
                    value={c.weight}
                    onChange={(e) =>
                      setRoute({
                        ...route,
                        candidates: route.candidates.map((x, n) =>
                          n === i
                            ? { ...x, weight: Number(e.target.value) }
                            : x,
                        ),
                      })
                    }
                  />
                  <span>%</span>
                  <button
                    className="quiet danger"
                    onClick={() =>
                      setRoute({
                        ...route,
                        candidates: route.candidates.filter((_, n) => n !== i),
                      })
                    }
                  >
                    移除
                  </button>
                </div>
              ))}
              <button
                className="secondary"
                onClick={() =>
                  setRoute({
                    ...route,
                    candidates: [
                      ...route.candidates,
                      {
                        model_id:
                          modelList.find((m) =>
                            m.capabilities.includes(route.kind),
                          )?.id ?? "",
                        weight: 0,
                      },
                    ],
                  })
                }
              >
                添加候选
              </button>
              <div className="actions">
                <button
                  disabled={busy}
                  onClick={() => void action("routes", "PUT", route)}
                >
                  保存路由
                </button>
              </div>
              <h3>灰度切换</h3>
              <div className="form-grid">
                <Field label="灰度目标模型">
                  <select
                    value={route.gray.candidate}
                    onChange={(e) =>
                      setRoute({
                        ...route,
                        gray: { ...route.gray, candidate: e.target.value },
                      })
                    }
                  >
                    <option value="">请选择</option>
                    {modelList
                      .filter((m) => m.capabilities.includes("chat"))
                      .map((m) => (
                        <option key={m.id}>{m.id}</option>
                      ))}
                  </select>
                </Field>
                {(["min_samples", "max_error", "max_latency_ms"] as const).map(
                  (k) => (
                    <Field
                      label={
                        {
                          min_samples: "最小样本量",
                          max_error: "最大错误率",
                          max_latency_ms: "最大延迟 / 毫秒",
                        }[k]
                      }
                      key={k}
                    >
                      <input
                        type="number"
                        step={k === "max_error" ? ".01" : "1"}
                        value={route.gray[k]}
                        onChange={(e) =>
                          setRoute({
                            ...route,
                            gray: {
                              ...route.gray,
                              [k]: Number(e.target.value),
                            },
                          })
                        }
                      />
                    </Field>
                  ),
                )}
              </div>
              <div className="actions">
                {[10, 50, 100].map((percent) => (
                  <button
                    disabled={busy || route.kind !== "chat"}
                    className="secondary"
                    key={percent}
                    onClick={() =>
                      void action("gray-release", "POST", {
                        alias: route.alias,
                        policy: { ...route.gray, percent, state: "running" },
                      })
                    }
                  >
                    放量 {percent}%
                  </button>
                ))}
                <button
                  className="secondary danger"
                  disabled={busy}
                  onClick={() => {
                    if (confirm("确定回滚灰度流量到原路由？"))
                      void action("gray-release", "POST", {
                        alias: route.alias,
                        policy: {
                          ...route.gray,
                          percent: 0,
                          state: "rolled_back",
                        },
                      });
                  }}
                >
                  回滚
                </button>
              </div>
            </section>
          </>
        )}
        {tab === "cost" && (
          <section>
            <div className="section-head">
              <h2>成本分摊</h2>
              <a
                className="button secondary"
                href={"/admin/cost-report?" + query() + "&format=csv"}
              >
                导出 CSV
              </a>
            </div>
            <p className="muted">
              输入总量包含模型缓存命中；总 Token = 输入总量 +
              输出。费用按调用时价格和汇率折算人民币，包含失败重试及缓存向量化。向量化费用属于总费用中的子项。
            </p>
            <p className="muted">
              缓存写入与推理 Token
              明细：未提供。缓存字段缺失及待核实记录不视为零消耗确认；无法还原的历史费用显示为未分摊。
            </p>
            {costs.length ? (
              <div className="table-wrap">
                <table className="cost-table">
                  <thead>
                    <tr>
                      <th>应用</th>
                      <th>部门</th>
                      <th>模型</th>
                      <th>调用次数</th>
                      <th>总 Token</th>
                      <th>普通输入 / 费用</th>
                      <th>模型缓存读取 / 费用</th>
                      <th>输出 / 费用</th>
                      <th>总费用 / 元</th>
                      <th>语义缓存 / 向量化子项</th>
                      <th>待核实</th>
                    </tr>
                  </thead>
                  <tbody>
                    {costs.map((c) => (
                      <tr
                        key={c.app_id + ":" + c.department + ":" + c.model_id}
                      >
                        <td>
                          {apps.find((a) => a.id === c.app_id)?.name ??
                            c.app_id}
                        </td>
                        <td>{c.department || "—"}</td>
                        <td>{c.model_id}</td>
                        <td>{number(c.count)}</td>
                        <td>
                          {number(c.tokens)}
                          <small>输入总量 {number(c.input_tokens)}</small>
                          {c.usage_unreported > 0 && (
                            <small>
                              {number(c.usage_unreported)} 次无完整用量明细
                            </small>
                          )}
                        </td>
                        <td>
                          {number(c.uncached_input_tokens)}
                          <small>¥{money(c.input_micro)}</small>
                        </td>
                        <td>
                          {c.cache_unreported === c.count
                            ? "未提供"
                            : number(c.cached_tokens)}
                          <small>
                            {c.cache_unreported === c.count
                              ? "费用未提供"
                              : `¥${money(c.cached_input_micro)}`}
                          </small>
                          {c.cache_unreported > 0 && (
                            <small>
                              {number(c.cache_unreported)} 次未提供缓存用量
                            </small>
                          )}
                        </td>
                        <td>
                          {number(c.output_tokens)}
                          <small>¥{money(c.output_micro)}</small>
                        </td>
                        <td>
                          ¥{money(c.micro)}
                          {c.unallocated_count > 0 && (
                            <small>
                              未分摊 ¥{money(c.unallocated_micro)} ·{" "}
                              {number(c.unallocated_count)} 次
                            </small>
                          )}
                        </td>
                        <td>
                          {c.cache_embedding_count > 0 ? (
                            <>
                              <span>命中 {number(c.cache_hits)} 次</span>
                              <small>
                                向量化 {number(c.cache_embedding_count)} 次 ·{" "}
                                {number(c.cache_embedding_tokens)} Token
                              </small>
                              <small>
                                其中 ¥{money(c.cache_embedding_micro)}
                              </small>
                            </>
                          ) : (
                            "—"
                          )}
                        </td>
                        <td>
                          {c.pending}
                          {c.pending > 0 && <small>含估算或进行中</small>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <Empty>当前时间范围暂无用量，先在 Playground 发送请求。</Empty>
            )}
            <h3>预算与异常告警</h3>
            {activeAlerts.length ? (
              activeAlerts.map((a) => (
                <div className="warning" key={a.id}>
                  {a.kind} · {a.message}
                </div>
              ))
            ) : (
              <Empty>暂无活跃告警</Empty>
            )}
          </section>
        )}
        {tab === "logs" && (
          <section>
            <div className="section-head">
              <h2>调用记录</h2>
            </div>
            <div className="filter-bar">
              <input
                aria-label="请求编号"
                value={requestSearch}
                onChange={(e) => setRequestSearch(e.target.value)}
                placeholder="按 request_id 精确检索"
              />
              <select
                aria-label="调用状态"
                value={status}
                onChange={(e) => setStatus(e.target.value)}
              >
                <option value="">全部状态</option>
                {[
                  "success",
                  "fallback_success",
                  "failed",
                  "stream_error",
                  "started",
                  "rate_limited",
                  "blocked",
                  "reconciled",
                ].map((x) => (
                  <option key={x}>{x}</option>
                ))}
              </select>
              <button className="secondary" onClick={() => void load()}>
                检索
              </button>
            </div>
            <RecordTable
              rows={records}
              apps={apps}
              onTrace={(record) =>
                setTrace({ requestID: record.request_id, appID: record.app_id })
              }
            />
            {trace && (
              <Trace
                key={trace.appID + ":" + trace.requestID}
                id={trace.requestID}
                appID={trace.appID}
                apps={apps}
                api={api}
                close={() => setTrace(null)}
              />
            )}
          </section>
        )}
        {tab === "security" && admin && (
          <div className="grid">
            <section>
              <h2>账号与权限</h2>
              {Object.values(cfg?.users ?? {}).map((u) => (
                <div className="state-row" key={u.name}>
                  <span>{u.name}</span>
                  <Badge value={u.role} />
                </div>
              ))}
              <Field label="账号">
                <input
                  value={user.name}
                  onChange={(e) => setUser({ ...user, name: e.target.value })}
                />
              </Field>
              <Field label="角色">
                <select
                  value={user.role}
                  onChange={(e) => setUser({ ...user, role: e.target.value })}
                >
                  <option value="developer">开发者（本人应用）</option>
                  <option value="cost">成本负责人</option>
                  <option value="admin">管理员</option>
                </select>
              </Field>
              <Field label="密码（12—72 字节）">
                <input
                  type="password"
                  autoComplete="new-password"
                  value={user.password}
                  onChange={(e) =>
                    setUser({ ...user, password: e.target.value })
                  }
                />
              </Field>
              <button
                disabled={busy}
                onClick={async () => {
                  if (await action("users", "POST", user))
                    setUser({ ...user, password: "" });
                }}
              >
                保存账号
              </button>
            </section>
            <section>
              <h2>Prompt 安全规则</h2>
              <p className="muted">
                正则规则支持 block 拦截和 nocache
                禁止缓存。检测不替代应用权限及业务安全控制。
              </p>
              <Field label="规则配置">
                <textarea
                  className="code-editor"
                  rows={14}
                  value={ruleText}
                  onChange={(e) => setRuleText(e.target.value)}
                />
              </Field>
              <button
                disabled={busy}
                onClick={() => {
                  try {
                    void action("rules", "PUT", JSON.parse(ruleText) as Rule[]);
                  } catch {
                    setError("规则必须是有效 JSON 数组");
                  }
                }}
              >
                保存规则
              </button>
            </section>
          </div>
        )}
        <footer>
          应用流量与管理操作分别授权 · 人民币计量账本 ·
          用量来源可追溯，支持对账修正
        </footer>
      </main>
    </div>
  );
}
function RecordMetering({ item: x }: { item: RecordItem }) {
  const quantity = (n: number | undefined) =>
    n == null ? "未提供" : number(n);
  if (x.purpose === "request") {
    return (
      <span>
        {x.cached ? "语义缓存命中" : "请求汇总"}
        <small>费用见各次实际调用</small>
      </span>
    );
  }
  if (x.kind === "started" || x.status === "started") {
    return (
      <span>
        进行中<small>用量与费用待核实</small>
      </span>
    );
  }
  const parts = x.cost_breakdown;
  const price = x.model;
  const usage = x.usage_breakdown_known === false ? undefined : x.usage;
  const cache = cachedTokens(usage, x.cache_reported);
  const fee = (n: number | undefined) =>
    parts?.known && n != null ? `¥${money(n)}` : "未分摊";
  return (
    <div className="metering-detail">
      <span>
        总计 {quantity(x.usage?.total_tokens)} Token · ¥{money(x.cost_micro)}
      </span>
      <small>
        输入总量 {quantity(usage?.prompt_tokens)} · 输出{" "}
        {quantity(usage?.completion_tokens)}
      </small>
      <small>
        普通输入 {quantity(uncachedTokens(usage))} · {fee(parts?.input_micro)}
      </small>
      <small>
        模型缓存读取 {quantity(cache)} ·{" "}
        {cache == null ? "费用未提供" : fee(parts?.cached_input_micro)}
      </small>
      <small>输出费用 {fee(parts?.output_micro)}</small>
      {!parts?.known && (
        <small>未分摊 ¥{money(parts?.unallocated_micro ?? x.cost_micro)}</small>
      )}
      {x.purpose === "cache_embedding" && (
        <small>语义缓存向量化子项，已计入总费用</small>
      )}
      <details>
        <summary>计价快照</summary>
        <small>
          价格版本 {price?.price_version || "未提供"} ·{" "}
          {price?.currency || "CNY"}
        </small>
        <small>
          单价 / 百万 Token：输入{" "}
          {price?.input_micro == null ? "未提供" : money(price.input_micro)} ·
          缓存{" "}
          {price?.cached_input_micro == null
            ? "未提供"
            : money(price.cached_input_micro)}{" "}
          · 输出{" "}
          {price?.output_micro == null ? "未提供" : money(price.output_micro)}
        </small>
        <small>
          折算系数{" "}
          {((price?.fx_ppm || 1000000) / 1000000).toLocaleString("zh-CN", {
            maximumFractionDigits: 6,
          })}
        </small>
        <small>缓存写入、推理 Token 明细：未提供</small>
      </details>
    </div>
  );
}
function RecordTable({
  rows,
  onTrace,
  apps,
}: {
  rows: RecordItem[];
  apps: { id: string; name: string }[];
  onTrace: (record: RecordItem) => void;
}) {
  return rows.length ? (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>应用</th>
            <th>请求 / 尝试</th>
            <th>模型与用途</th>
            <th>状态</th>
            <th>用量来源</th>
            <th>Token / 费用</th>
            <th>耗时</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((x) => (
            <tr key={x.attempt_id}>
              <td>
                {x.app_name ||
                  apps.find((app) => app.id === x.app_id)?.name ||
                  x.app_id}
                <small className="mono">{x.app_id}</small>
                {!x.app_name &&
                  apps.some((app) => app.id === x.app_id && app.name) && (
                    <small>名称取当前应用配置</small>
                  )}
              </td>
              <td>
                <button
                  className="quiet mono"
                  title={x.request_id}
                  onClick={() => onTrace(x)}
                >
                  {x.request_id.slice(0, 16)}…
                </button>
                <small className="mono">{x.attempt_id.slice(0, 16)}…</small>
              </td>
              <td>
                {x.model?.id || x.alias}
                <small>{x.purpose}</small>
                <small>
                  {routeSource(x.route_scope)}
                  {x.route_strategy && ` · ${x.route_strategy}`}
                  {x.config_version != null && ` · v${x.config_version}`}
                </small>
              </td>
              <td>
                <Badge value={x.status} />
              </td>
              <td>
                {x.purpose === "request" ? "请求汇总" : sourceLabel(x.quality)}
              </td>
              <td>
                <RecordMetering item={x} />
              </td>
              <td>{x.duration_ms} ms</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ) : (
    <Empty>暂无匹配记录，请调整筛选或发送测试请求。</Empty>
  );
}
function Trace({
  id,
  appID,
  apps,
  api,
  close,
}: {
  id: string;
  appID: string;
  apps: { id: string; name: string }[];
  api: <T>(path: string) => Promise<T>;
  close: () => void;
}) {
  const [rows, setRows] = useState<RecordItem[]>([]);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    void api<RecordItem[]>(tracePath(id, appID))
      .then((rows) => {
        if (active) setRows(rows);
      })
      .catch((e) => {
        if (active) setError(String(e));
      });
    return () => {
      active = false;
    };
  }, [id, appID]);
  return (
    <div className="trace">
      <div className="section-head">
        <h3>
          {apps.find((app) => app.id === appID)?.name || appID} · 请求路径 {id}
        </h3>
        <button className="quiet" onClick={close}>
          关闭
        </button>
      </div>
      {error && <p className="error">{error}</p>}
      <RecordTable rows={rows} apps={apps} onTrace={() => {}} />
    </div>
  );
}
function Root() {
  const [session, setSession] = useState<Session | null>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    fetch("/admin/session")
      .then((r) => (r.ok ? r.json() : null))
      .then(setSession)
      .finally(() => setLoading(false));
  }, []);
  if (loading) return <div className="loading">正在连接控制台…</div>;
  return session ? (
    <Console session={session} onLogout={() => setSession(null)} />
  ) : (
    <Login onLogin={setSession} />
  );
}
createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Root />
    {import.meta.env.VITE_ICP_NUMBER && (
      <footer className="filing-footer">
        <a href="https://beian.miit.gov.cn/" target="_blank" rel="noreferrer">
          {import.meta.env.VITE_ICP_NUMBER}
        </a>
      </footer>
    )}
  </React.StrictMode>,
);

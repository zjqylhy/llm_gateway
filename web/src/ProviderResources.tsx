import React, { useState } from "react";
import { ResourceDialog, type ResourceMutation } from "./ResourceDialog";

type Provider = { id: string; kind: string; url: string };
type ProviderDraft = Provider & { api_key: string };
const providerKinds: Record<string, string> = {
  deepseek: "DeepSeek",
  qwen: "Qwen / 百炼",
  openai: "OpenAI 兼容",
  mock: "本地模拟",
};

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

export function ProviderResources({
  providers,
  loaded,
  busy,
  onMutation,
}: {
  providers: Record<string, Provider>;
  loaded: boolean;
  busy: boolean;
  onMutation: ResourceMutation;
}) {
  const [draft, setDraft] = useState<ProviderDraft | null>(null);
  const [editingID, setEditingID] = useState<string | null>(null);
  const [error, setError] = useState("");
  const list = Object.values(providers).sort((a, b) =>
    a.id.localeCompare(b.id),
  );
  function open(saved?: Provider) {
    setError("");
    setEditingID(saved?.id ?? null);
    // 配置包含只读 secret；表单只复制允许提交的字段。
    setDraft(
      saved
        ? { id: saved.id, kind: saved.kind, url: saved.url, api_key: "" }
        : { id: "", kind: "openai", url: "", api_key: "" },
    );
  }
  async function save(event: React.FormEvent) {
    event.preventDefault();
    if (!draft || busy) return;
    setError("");
    if (!editingID && providers[draft.id]) {
      setError("厂商编号已存在，请取消新增并使用列表中的编辑操作。");
      return;
    }
    if (!editingID && !draft.api_key.trim()) {
      setError("新增厂商必须填写 API Key。");
      return;
    }
    if (
      await onMutation(
        editingID ? `providers/${encodeURIComponent(editingID)}` : "providers",
        editingID ? "PUT" : "POST",
        draft,
        setError,
      )
    )
      setDraft(null);
  }
  return (
    <section>
      <div className="section-head">
        <h2>
          已保存厂商{" "}
          <span className="muted small">
            {loaded ? `共 ${list.length} 个` : "加载中…"}
          </span>
        </h2>
        <button disabled={busy || !loaded} onClick={() => open()}>
          新增厂商
        </button>
      </div>
      <p className="muted">
        先保存厂商连接和密钥，再在下方新增该厂商的模型资源。密钥加密保存，编辑时不会回填。
      </p>
      {list.length ? (
        <div className="table-wrap">
          <table className="provider-table">
            <thead>
              <tr>
                <th>厂商编号</th>
                <th>厂商类型</th>
                <th>API Base URL</th>
                <th>密钥状态</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {list.map((provider) => (
                <tr key={provider.id}>
                  <td>
                    <strong>{provider.id}</strong>
                  </td>
                  <td>{providerKinds[provider.kind] ?? provider.kind}</td>
                  <td className="provider-url">{provider.url}</td>
                  <td>
                    <span className="muted">已保存</span>
                  </td>
                  <td>
                    <button
                      className="quiet"
                      disabled={busy}
                      onClick={() => open(provider)}
                      aria-label={`编辑厂商 ${provider.id}`}
                    >
                      编辑
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="muted">
          {loaded
            ? "尚未保存厂商，点击“新增厂商”开始配置。"
            : "正在加载已保存厂商…"}
        </p>
      )}
      {draft && (
        <ResourceDialog
          title={editingID ? `编辑厂商：${editingID}` : "新增厂商"}
          busy={busy}
          onClose={() => setDraft(null)}
        >
          <form onSubmit={(event) => void save(event)}>
            <p className="muted">
              {editingID
                ? "厂商编号保持不变，API Key 留空保留原密钥。"
                : "填写新的厂商编号、连接地址和 API Key；保存后出现在厂商列表中。"}
            </p>
            {error && (
              <p className="error" role="alert">
                {error}
              </p>
            )}
            <div className="form-grid">
              <Field label="厂商编号">
                <input
                  required
                  disabled={busy}
                  readOnly={!!editingID}
                  maxLength={64}
                  pattern="[a-zA-Z0-9._\-]{1,64}"
                  value={draft.id}
                  placeholder="如 qwen-beijing"
                  onChange={(event) =>
                    setDraft({ ...draft, id: event.target.value })
                  }
                />
              </Field>
              <Field label="厂商类型">
                <select
                  disabled={busy}
                  value={draft.kind}
                  onChange={(event) =>
                    setDraft({ ...draft, kind: event.target.value })
                  }
                >
                  {[
                    "deepseek",
                    "qwen",
                    "openai",
                    ...(draft.kind === "mock" ? ["mock"] : []),
                  ].map((kind) => (
                    <option value={kind} key={kind}>
                      {providerKinds[kind]}
                    </option>
                  ))}
                </select>
              </Field>
              <Field label="API Base URL">
                <input
                  required
                  disabled={busy}
                  type="url"
                  value={draft.url}
                  placeholder="厂商提供的 API Base URL"
                  onChange={(event) =>
                    setDraft({ ...draft, url: event.target.value })
                  }
                />
              </Field>
              <Field
                label={
                  editingID
                    ? "厂商 API Key（留空保留）"
                    : "厂商 API Key（必填）"
                }
              >
                <input
                  required={!editingID}
                  disabled={busy}
                  type="password"
                  autoComplete="new-password"
                  value={draft.api_key}
                  placeholder={
                    editingID ? "留空保留原密钥" : "填写新厂商的 API Key"
                  }
                  onChange={(event) =>
                    setDraft({ ...draft, api_key: event.target.value })
                  }
                />
              </Field>
            </div>
            <div className="actions">
              <button type="submit" disabled={busy}>
                {busy ? "正在保存…" : editingID ? "保存修改" : "新增并保存"}
              </button>
              <button
                type="button"
                className="secondary"
                disabled={busy}
                onClick={() => setDraft(null)}
              >
                取消
              </button>
            </div>
          </form>
        </ResourceDialog>
      )}
    </section>
  );
}

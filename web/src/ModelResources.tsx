import React, { useEffect, useRef, useState } from "react";

export type Model = {
  id: string;
  provider_id: string;
  upstream: string;
  version: string;
  capabilities: string[];
  context: number;
  max_output: number;
  rpm: number;
  tpm: number;
  dimensions: number;
  rerank_protocol?: string;
  max_documents?: number;
  max_item_tokens?: number;
  input_micro: number;
  cached_input_micro: number;
  output_micro: number;
  price_version: string;
  currency: string;
  fx_ppm: number;
};

type Mutation = (
  path: string,
  method: string,
  body?: unknown,
  onError?: (message: string) => void,
) => Promise<boolean>;

function newModel(): Model {
  return {
    id: "",
    provider_id: "",
    upstream: "",
    version: "",
    capabilities: ["chat", "stream"],
    context: 16000,
    max_output: 2048,
    rpm: 100,
    tpm: 100000,
    dimensions: 0,
    input_micro: 0,
    cached_input_micro: 0,
    output_micro: 0,
    price_version: "",
    currency: "CNY",
    fx_ppm: 1000000,
  };
}

const capabilities = {
  chat: "对话",
  embedding: "向量",
  rerank: "重排",
  rerank_instruct: "重排指令",
  stream: "流式输出",
  tools: "工具调用",
  json_object: "JSON 输出",
  json_schema: "结构化输出",
  seed: "随机种子",
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

// 原生对话框约束键盘焦点；取消和关闭后焦点返回触发按钮。
function Dialog({
  title,
  busy,
  onClose,
  children,
}: {
  title: string;
  busy: boolean;
  onClose: () => void;
  children: React.ReactNode;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = ref.current!;
    dialog.showModal();
    return () => dialog.close();
  }, []);
  return (
    <dialog
      ref={ref}
      className="modal model-dialog"
      aria-labelledby="model-dialog-title"
      onCancel={(event) => {
        event.preventDefault();
        if (!busy) onClose();
      }}
    >
      <div className="section-head">
        <h2 id="model-dialog-title">{title}</h2>
        <button
          type="button"
          className="quiet"
          disabled={busy}
          onClick={onClose}
          aria-label="关闭"
        >
          关闭
        </button>
      </div>
      {children}
    </dialog>
  );
}

export function ModelResources({
  models,
  providers,
  references,
  busy,
  loaded,
  onMutation,
}: {
  models: Model[];
  providers: Record<string, { id: string }>;
  references?: Record<string, string[]>;
  busy: boolean;
  loaded: boolean;
  onMutation: Mutation;
}) {
  const [model, setModel] = useState<Model | null>(null);
  const [editingID, setEditingID] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<Model | null>(null);
  const [error, setError] = useState("");
  const deleteRefs = deleting ? references?.[deleting.id] : undefined;
  function openEditor(saved?: Model) {
    setError("");
    setEditingID(saved?.id ?? null);
    setModel(
      saved ? { ...saved, capabilities: [...saved.capabilities] } : newModel(),
    );
  }
  async function save(event: React.FormEvent) {
    event.preventDefault();
    if (!model || busy) return;
    setError("");
    if (!editingID && models.some((saved) => saved.id === model.id)) {
      setError("模型编号已存在，请取消新增并使用列表中的编辑操作。");
      return;
    }
    if (
      await onMutation(
        editingID ? `models/${encodeURIComponent(editingID)}` : "models",
        editingID ? "PUT" : "POST",
        model,
        setError,
      )
    )
      setModel(null);
  }
  async function remove() {
    if (!deleting || busy || !deleteRefs || deleteRefs.length) return;
    setError("");
    if (
      await onMutation(
        `models/${encodeURIComponent(deleting.id)}`,
        "DELETE",
        undefined,
        setError,
      )
    )
      setDeleting(null);
  }
  const numericField = (
    key:
      | "context"
      | "max_output"
      | "rpm"
      | "tpm"
      | "dimensions"
      | "input_micro"
      | "cached_input_micro"
      | "output_micro"
      | "fx_ppm",
    label: string,
  ) => (
    <Field key={key} label={label}>
      <input
        type="number"
        min={key === "context" || key === "fx_ppm" ? 1 : 0}
        required
        disabled={busy}
        value={model![key]}
        onChange={(event) =>
          setModel({ ...model!, [key]: Number(event.target.value) })
        }
      />
    </Field>
  );
  return (
    <section>
      <div className="section-head">
        <h2>
          已保存模型{" "}
          <span className="muted small">
            {loaded ? `共 ${models.length} 个` : "加载中…"}
          </span>
        </h2>
        <button disabled={busy || !loaded} onClick={() => openEditor()}>
          新增模型
        </button>
      </div>
      <p className="muted">
        这里列出已保存的真实模型资源。新增或编辑资源后，在下方默认路由中选择模型，应用通过路由别名调用。
      </p>
      {loaded && !Object.keys(providers).length && (
        <p className="notice">请先在上方配置厂商凭据，再新增模型。</p>
      )}
      {models.length ? (
        <div className="table-wrap">
          <table className="model-table">
            <thead>
              <tr>
                <th>模型编号 / 版本</th>
                <th>所属厂商</th>
                <th>厂商模型名</th>
                <th>能力</th>
                <th>引用情况</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {[...models]
                .sort((a, b) => a.id.localeCompare(b.id))
                .map((saved) => {
                  const refs = references?.[saved.id];
                  return (
                    <tr key={saved.id}>
                      <td>
                        <strong>{saved.id}</strong>
                        <small>{saved.version}</small>
                      </td>
                      <td>{saved.provider_id}</td>
                      <td>{saved.upstream}</td>
                      <td>
                        {saved.capabilities.map((cap) => (
                          <span className="model-capability" key={cap}>
                            {capabilities[cap as keyof typeof capabilities] ??
                              cap}
                          </span>
                        ))}
                      </td>
                      <td>
                        {refs ? (
                          refs.length ? (
                            <details className="model-references">
                              <summary>使用中 · {refs.length} 处</summary>
                              <ul>
                                {refs.map((location) => (
                                  <li key={location}>{location}</li>
                                ))}
                              </ul>
                            </details>
                          ) : (
                            <span className="muted">未被引用</span>
                          )
                        ) : (
                          <span className="muted">引用信息暂不可用</span>
                        )}
                      </td>
                      <td>
                        <div className="model-row-actions">
                          <button
                            className="quiet"
                            disabled={busy}
                            onClick={() => openEditor(saved)}
                            aria-label={`编辑模型 ${saved.id}`}
                          >
                            编辑
                          </button>
                          <button
                            className="quiet danger"
                            disabled={busy || !refs}
                            onClick={() => {
                              setError("");
                              setDeleting(saved);
                            }}
                            aria-label={`删除模型 ${saved.id}`}
                          >
                            删除
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="muted">
          {loaded
            ? "尚未保存模型，点击“新增模型”开始配置。"
            : "正在加载已保存模型…"}
        </p>
      )}
      {model && (
        <Dialog
          title={editingID ? `编辑模型：${editingID}` : "新增模型"}
          busy={busy}
          onClose={() => setModel(null)}
        >
          <form onSubmit={(event) => void save(event)}>
            <p className="muted">
              {editingID
                ? "模型编号保持不变，保存后更新现有配置。"
                : "填写新的模型编号；保存后会出现在模型列表中。"}
            </p>
            {error && (
              <p className="error" role="alert">
                {error}
              </p>
            )}
            <h3>基本配置</h3>
            <div className="form-grid">
              <Field label="模型编号">
                <input
                  autoFocus
                  required
                  disabled={busy}
                  readOnly={!!editingID}
                  value={model.id}
                  placeholder="如 qwen-chat"
                  pattern="[a-zA-Z0-9._\-]{1,64}"
                  maxLength={64}
                  onChange={(event) =>
                    setModel({ ...model, id: event.target.value })
                  }
                />
              </Field>
              <Field label="所属厂商">
                <select
                  required
                  disabled={busy}
                  value={model.provider_id}
                  onChange={(event) =>
                    setModel({ ...model, provider_id: event.target.value })
                  }
                >
                  <option value="">请选择已配置厂商</option>
                  {Object.keys(providers).map((id) => (
                    <option key={id}>{id}</option>
                  ))}
                </select>
              </Field>
              <Field label="厂商模型名">
                <input
                  required
                  disabled={busy}
                  value={model.upstream}
                  placeholder="厂商 API 接受的模型名称"
                  onChange={(event) =>
                    setModel({ ...model, upstream: event.target.value })
                  }
                />
              </Field>
              <Field label="模型版本">
                <input
                  required
                  disabled={busy}
                  value={model.version}
                  onChange={(event) =>
                    setModel({ ...model, version: event.target.value })
                  }
                />
              </Field>
            </div>
            <h3>能力与限制</h3>
            <Field label="能力（逗号分隔）">
              <input
                required
                disabled={busy}
                value={model.capabilities.join(",")}
                onChange={(event) => {
                  const caps = event.target.value
                    .split(",")
                    .map((value) => value.trim());
                  const rerank = caps.includes("rerank");
                  setModel({
                    ...model,
                    capabilities: caps,
                    rerank_protocol: rerank
                      ? model.rerank_protocol || "compatible"
                      : undefined,
                    max_documents: rerank
                      ? model.max_documents || 500
                      : undefined,
                    max_item_tokens: rerank
                      ? model.max_item_tokens || 4000
                      : undefined,
                    ...(rerank
                      ? {
                          max_output: 0,
                          output_micro: 0,
                          cached_input_micro: 0,
                        }
                      : {}),
                  });
                }}
              />
              <small>
                基础能力：chat、embedding、rerank；附加能力：stream、tools、json_object、json_schema、seed、rerank_instruct。
              </small>
            </Field>
            <div className="form-grid">
              {numericField("context", "上下文上限（Token）")}
              {numericField("max_output", "输出上限（Token）")}
              {numericField("rpm", "每分钟请求上限（RPM，0 表示不限）")}
              {numericField("tpm", "每分钟 Token 上限（TPM，0 表示不限）")}
              {numericField("dimensions", "向量维度（仅 embedding 需要）")}
              {model.capabilities.includes("rerank") && (
                <>
                  <Field label="重排厂商接口">
                    <select
                      required
                      disabled={busy}
                      value={model.rerank_protocol ?? ""}
                      onChange={(event) =>
                        setModel({
                          ...model,
                          rerank_protocol: event.target.value,
                        })
                      }
                    >
                      <option value="">请选择</option>
                      <option value="compatible">
                        兼容接口（qwen3-rerank）
                      </option>
                      <option value="dashscope">DashScope 文本排序接口</option>
                    </select>
                  </Field>
                  <Field label="最大文档数（1–500）">
                    <input
                      type="number"
                      required
                      disabled={busy}
                      min={1}
                      max={500}
                      value={model.max_documents ?? 0}
                      onChange={(event) =>
                        setModel({
                          ...model,
                          max_documents: Number(event.target.value),
                        })
                      }
                    />
                  </Field>
                  <Field label="单条输入上限（Token）">
                    <input
                      type="number"
                      required
                      disabled={busy}
                      min={1}
                      value={model.max_item_tokens ?? 0}
                      onChange={(event) =>
                        setModel({
                          ...model,
                          max_item_tokens: Number(event.target.value),
                        })
                      }
                    />
                  </Field>
                </>
              )}
            </div>
            <h3>价格配置</h3>
            <p className="muted small">
              单价单位为微单位 / 百万 Token；1 元或 1 美元 = 1,000,000 微单位。
            </p>
            <div className="form-grid">
              <Field label="价格版本">
                <input
                  required
                  disabled={busy}
                  value={model.price_version}
                  onChange={(event) =>
                    setModel({ ...model, price_version: event.target.value })
                  }
                />
              </Field>
              <Field label="单价币种">
                <select
                  disabled={busy}
                  value={model.currency}
                  onChange={(event) =>
                    setModel({ ...model, currency: event.target.value })
                  }
                >
                  <option>CNY</option>
                  <option>USD</option>
                </select>
              </Field>
              {numericField("input_micro", "输入单价")}
              {numericField("cached_input_micro", "缓存输入单价")}
              {numericField("output_micro", "输出单价")}
              {numericField("fx_ppm", "汇率百万分比（CNY 为 1000000）")}
            </div>
            <div className="actions">
              <button
                type="submit"
                disabled={busy || !Object.keys(providers).length}
              >
                {busy ? "正在保存…" : editingID ? "保存修改" : "新增并保存"}
              </button>
              <button
                type="button"
                className="secondary"
                disabled={busy}
                onClick={() => setModel(null)}
              >
                取消
              </button>
            </div>
          </form>
        </Dialog>
      )}
      {deleting && (
        <Dialog
          title={`删除模型：${deleting.id}`}
          busy={busy}
          onClose={() => setDeleting(null)}
        >
          {error && (
            <p className="error" role="alert">
              {error}
            </p>
          )}
          {deleteRefs?.length ? (
            <>
              <p>此模型仍被以下配置引用，需先调整引用才能删除：</p>
              <ul className="model-reference-list">
                {deleteRefs.map((location) => (
                  <li key={location}>{location}</li>
                ))}
              </ul>
            </>
          ) : (
            <p>确认删除模型“{deleting.id}”？历史调用与成本记录会保留。</p>
          )}
          <div className="actions">
            <button
              className="model-delete-button"
              disabled={busy || !deleteRefs || deleteRefs.length > 0}
              onClick={() => void remove()}
            >
              {busy ? "正在删除…" : "确认删除"}
            </button>
            <button
              className="secondary"
              disabled={busy}
              onClick={() => setDeleting(null)}
            >
              取消
            </button>
          </div>
        </Dialog>
      )}
    </section>
  );
}

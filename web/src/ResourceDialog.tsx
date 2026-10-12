import React, { useEffect, useId, useRef } from "react";

export type ResourceMutation = (
  path: string,
  method: string,
  body?: unknown,
  onError?: (message: string) => void,
) => Promise<boolean>;

// 原生对话框限制键盘焦点，关闭后将焦点交还触发按钮。
export function ResourceDialog({
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
  const titleID = useId();
  useEffect(() => {
    const dialog = ref.current!;
    dialog.showModal();
    return () => dialog.close();
  }, []);
  return (
    <dialog
      ref={ref}
      className="modal model-dialog"
      aria-labelledby={titleID}
      onCancel={(event) => {
        event.preventDefault();
        if (!busy) onClose();
      }}
    >
      <div className="section-head">
        <h2 id={titleID}>{title}</h2>
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

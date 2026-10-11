// 刷新期间只保留最新筛选条件，避免自动刷新和手动操作叠加请求。
export function createRefreshQueue() {
  let pending: (() => Promise<void>) | undefined;
  let running: Promise<void> | undefined;
  return (task: () => Promise<void>): Promise<void> => {
    pending = task;
    if (!running) {
      running = (async () => {
        let failed = false;
        let failure: unknown;
        while (pending) {
          const next = pending;
          pending = undefined;
          try {
            await next();
          } catch (error) {
            if (!failed) failure = error;
            failed = true;
          }
        }
        if (failed) throw failure;
      })().finally(() => {
        running = undefined;
      });
    }
    return running;
  };
}

// 一个接口失败时仍等待同组请求结束，再允许下一轮刷新占用管理槽位。
export async function waitForAll<T extends readonly unknown[] | []>(tasks: T) {
  try {
    return await Promise.all(tasks);
  } finally {
    await Promise.allSettled(tasks);
  }
}

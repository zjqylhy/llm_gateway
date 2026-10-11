export type FeeParts = {
  input_micro: number;
  cached_input_micro: number;
  output_micro: number;
  unallocated_micro: number;
};

export type Usage = {
  prompt_tokens?: number;
  completion_tokens?: number;
  total_tokens: number;
  cached_tokens?: number;
};

export const sourceLabel = (quality: string) =>
  ({
    reported: "上游报告",
    estimated: "估算 · 待核实",
    reconciled: "对账修正",
    pending: "待核实",
  })[quality] || "未提供";

export function uncachedTokens(usage?: Usage): number | undefined {
  if (usage?.prompt_tokens == null) return undefined;
  // 缓存输入已包含在输入总量内，不再计入一次。
  return Math.max(0, usage.prompt_tokens - (usage.cached_tokens ?? 0));
}

export function cachedTokens(
  usage: Usage | undefined,
  reported: boolean | undefined,
): number | undefined {
  if (!reported && !usage?.cached_tokens) return undefined;
  return usage?.cached_tokens ?? 0;
}

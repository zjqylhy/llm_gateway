import test from "node:test";
import assert from "node:assert/strict";
import { cachedTokens, uncachedTokens, sourceLabel } from "../src/metering.ts";

test("cached input is a subset of input and is not counted twice", () => {
  const usage = {
    prompt_tokens: 100,
    completion_tokens: 10,
    cached_tokens: 40,
    total_tokens: 110,
  };
  assert.equal(uncachedTokens(usage), 60);
  assert.equal(cachedTokens(usage, true), 40);
  assert.equal(
    uncachedTokens(usage) + cachedTokens(usage, true) + usage.completion_tokens,
    usage.total_tokens,
  );
});

test("missing usage differs from explicitly reported zero and old nonzero cache remains visible", () => {
  assert.equal(cachedTokens({ total_tokens: 10 }, false), undefined);
  assert.equal(cachedTokens({ total_tokens: 10 }, true), 0);
  assert.equal(cachedTokens({ total_tokens: 10, cached_tokens: 5 }, false), 5);
  assert.equal(uncachedTokens({ total_tokens: 10 }), undefined);
  assert.equal(uncachedTokens({ prompt_tokens: 0, total_tokens: 0 }), 0);
  assert.equal(sourceLabel("estimated"), "估算 · 待核实");
  assert.equal(sourceLabel("pending"), "待核实");
});

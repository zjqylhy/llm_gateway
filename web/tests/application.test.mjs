import test from "node:test";
import assert from "node:assert/strict";
import {
  routePayload,
  instanceLabel,
  meteringLabel,
  routeSource,
  tracePath,
  callTime,
} from "../src/application.ts";

test("call timestamps use Beijing time including date rollover and midnight", () => {
  assert.equal(
    callTime("2026-10-12T21:00:13.700760037Z"),
    "2026-10-13 05:00:13",
  );
  assert.equal(callTime("2026-10-12T16:00:00Z"), "2026-10-13 00:00:00");
  assert.equal(callTime("2026-10-13T05:00:13+08:00"), "2026-10-13 05:00:13");
});

test("missing, invalid and Go zero timestamps are not fabricated", () => {
  for (const at of [undefined, "", "invalid", "0001-01-01T00:00:00Z"])
    assert.equal(callTime(at), "未提供");
});

test("saving an effective route excludes view-only fields rejected by the backend", () => {
  const route = {
    alias: "chat",
    kind: "chat",
    strategy: "priority",
    candidates: [{ model_id: "backup", weight: 100 }],
    gray: { state: "finished" },
    source: "global",
    config_version: 12,
  };
  assert.deepEqual(Object.keys(routePayload(route)).sort(), [
    "alias",
    "candidates",
    "gray",
    "kind",
    "strategy",
  ]);
  assert.equal(routePayload(route).candidates[0].model_id, "backup");
});

test("request trace uses the record's application and safely encodes both identifiers", () => {
  const path = tracePath("req/a?x=1", "app&other=1");
  const url = new URL(path, "https://console/admin/");
  assert.equal(url.pathname, "/admin/trace/req%2Fa%3Fx%3D1");
  assert.equal(url.searchParams.get("app_id"), "app&other=1");
  assert.equal(url.searchParams.has("other"), false);
});

test("service heartbeat, configuration lag and business metering have distinct labels", () => {
  assert.equal(instanceLabel("stale"), "心跳过期");
  assert.equal(instanceLabel("config_pending"), "配置未同步");
  assert.equal(meteringLabel("quiescing"), "核验中");
  assert.equal(meteringLabel("frozen"), "计量暂停");
  assert.equal(meteringLabel("unrecognized"), "状态未知");
});

test("legacy routing attribution remains unknown rather than claiming global inheritance", () => {
  assert.equal(routeSource(undefined), "历史记录未保存");
  assert.equal(routeSource("global"), "继承全局");
  assert.equal(routeSource("application:app"), "应用专属");
  assert.equal(routeSource("internal"), "应用缓存配置");
});

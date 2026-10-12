import test from "node:test";
import assert from "node:assert/strict";
import {
  routePayload,
  instanceLabel,
  meteringLabel,
  routeSource,
  tracePath,
} from "../src/application.ts";

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

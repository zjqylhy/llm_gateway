import test from "node:test";
import assert from "node:assert/strict";
import {
  fetchTraffic,
  failureTrafficSeries,
  trafficSeriesKey,
  trafficSeriesTitle,
  qpsText,
  trafficSegments,
  trafficWindowError,
} from "../src/traffic.ts";

test("traffic permits arbitrary historical intervals within the last day", () => {
  const now = Date.parse("2026-10-11T12:00:00Z");
  assert.equal(
    trafficWindowError("2026-10-10T12:00:00Z", "2026-10-11T12:00:00Z", now),
    "",
  );
  assert.equal(
    trafficWindowError("2026-10-10T14:23:00Z", "2026-10-10T17:12:00Z", now),
    "",
  );
  for (const [from, to] of [
    ["2026-10-10T11:00:00Z", "2026-10-11T12:00:00Z"],
    ["2026-10-09T12:00:00Z", "2026-10-09T13:00:00Z"],
    ["2026-10-11T12:00:00Z", "2026-10-11T12:00:00Z"],
    ["2026-10-11T12:00:00Z", "2026-10-11T13:00:00Z"],
    ["invalid", "2026-10-11T12:00:00Z"],
  ])
    assert.notEqual(trafficWindowError(from, to, now), "");
});

test("missing samples remain gaps and unlimited is not a zero limit line", () => {
  const points = [
    { at: "2026-10-11T10:00:00Z", value: 2 },
    { at: "2026-10-11T10:01:00Z", value: 0 },
    { at: "2026-10-11T10:05:00Z", value: 3 },
  ];
  assert.deepEqual(trafficSegments({ kind: "success", points }, 60), [
    points.slice(0, 2),
    points.slice(2),
  ]);
  assert.deepEqual(trafficSegments({ kind: "limit", points }, 60), [
    [points[0]],
    [points[2]],
  ]);
  assert.equal(qpsText(null), "暂无数据");
  assert.equal(qpsText(0), "0");
});

test("traffic requests propagate cancellation and encode the selected app", async () => {
  const controller = new AbortController();
  const data = { available: false, apps: [] };
  const result = await fetchTraffic(
    "app&other",
    "2026-10-11T10:00:00Z",
    "2026-10-11T11:00:00Z",
    controller.signal,
    async (path, options) => {
      const url = new URL(path, "http://console");
      assert.equal(url.searchParams.get("app_id"), "app&other");
      assert.equal(url.searchParams.get("from"), "2026-10-11T10:00:00Z");
      assert.equal(options.signal, controller.signal);
      return { ok: true, json: async () => data };
    },
  );
  assert.deepEqual(result, data);
  await assert.rejects(
    fetchTraffic("app", "from", "to", controller.signal, async () => ({
      ok: false,
      json: async () => ({ error: { message: "forbidden" } }),
    })),
    /forbidden/,
  );
});

test("one call per day remains visible as a nonzero QPS", () => {
  assert.notEqual(qpsText(1 / 86400), "0");
  assert.equal(qpsText(1 / 86400), "0.0000116");
});

test("failure drilldown isolates HTTP and business code pairs and preserves gaps", () => {
  const points = [
    { at: "2026-10-11T10:00:00Z", value: 0.1 },
    { at: "2026-10-11T10:01:00Z", value: 0 },
    { at: "2026-10-11T10:05:00Z", value: 0.2 },
  ];
  const stream = {
    kind: "failure",
    http_status: "200",
    error_code: "metering_unavailable",
    points,
  };
  const unavailable = { ...stream, http_status: "503" };
  const invalid = {
    ...stream,
    http_status: "400",
    error_code: "invalid_request_error",
  };
  const all = [
    unavailable,
    { kind: "success", points },
    invalid,
    stream,
    { kind: "limit", points },
  ];
  assert.deepEqual(failureTrafficSeries(all), [stream, invalid, unavailable]);
  assert.deepEqual(failureTrafficSeries(all, "200:metering_unavailable"), [
    stream,
  ]);
  assert.deepEqual(failureTrafficSeries(all, "400:invalid_request_error"), [
    invalid,
  ]);
  assert.deepEqual(failureTrafficSeries(all, "missing"), []);
  assert.equal(trafficSeriesTitle(stream), "HTTP 200 · metering_unavailable");
  assert.notEqual(trafficSeriesKey(stream), trafficSeriesKey(unavailable));
  assert.deepEqual(trafficSegments(stream, 60), [
    points.slice(0, 2),
    points.slice(2),
  ]);
  assert.equal(
    all[0],
    unavailable,
    "display sorting must not reorder API data",
  );
});

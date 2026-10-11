import assert from "node:assert/strict";
import test from "node:test";
import { createRefreshQueue, waitForAll } from "../src/refresh.ts";

function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test("overlapping refreshes run serially and keep the latest filters", async () => {
  const refresh = createRefreshQueue();
  const gate = deferred();
  const seen = [];
  const first = refresh(async () => {
    seen.push("first");
    await gate.promise;
  });
  const old = refresh(async () => {
    seen.push("old filters");
  });
  const latest = refresh(async () => {
    seen.push("latest filters");
  });
  assert.deepEqual(seen, ["first"]);
  gate.resolve();
  await Promise.all([first, old, latest]);
  assert.deepEqual(seen, ["first", "latest filters"]);
});

test("failed request groups finish before the next refresh starts", async () => {
  const refresh = createRefreshQueue();
  const gate = deferred();
  const seen = [];
  const failure = new Error("503");
  const first = refresh(async () => {
    await assert.rejects(
      waitForAll([
        Promise.reject(failure),
        gate.promise.then(() => seen.push("slow request finished")),
      ]),
      (error) => error === failure,
    );
  });
  const next = refresh(async () => {
    seen.push("next refresh");
  });
  await new Promise((done) => setImmediate(done));
  assert.deepEqual(seen, []);
  gate.resolve();
  await Promise.all([first, next]);
  assert.deepEqual(seen, ["slow request finished", "next refresh"]);
});

test("refresh queue recovers after failure", async () => {
  const refresh = createRefreshQueue();
  await assert.rejects(
    refresh(async () => {
      throw new Error("failed");
    }),
    /failed/,
  );
  let completed = false;
  await refresh(async () => {
    completed = true;
  });
  assert.equal(completed, true);
});

test("successful request groups preserve response order", async () => {
  const gate = deferred();
  const result = waitForAll([gate.promise, Promise.resolve("second")]);
  gate.resolve("first");
  assert.deepEqual(await result, ["first", "second"]);
});

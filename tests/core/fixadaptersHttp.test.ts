import assert from "node:assert/strict";
import { test } from "node:test";
import { createRateLimitedFetch } from "../../src/core/gaps/adapters/http.js";
import type { FetchLike } from "../../src/core/gaps/adapters/types.js";

test("rate limited fetch retries 429 and honors Retry-After", async () => {
  let nowMs = 0;
  const sleeps: number[] = [];
  let calls = 0;

  const fetchImpl: FetchLike = async () => {
    calls += 1;
    if (calls === 1) {
      return new Response("busy", { status: 429, headers: { "retry-after": "0.2" } });
    }
    return new Response("ok", { status: 200 });
  };

  const wrapped = createRateLimitedFetch(fetchImpl, {
    defaultPolicy: { minIntervalMs: 50, maxAttempts: 4, baseBackoffMs: 10, maxBackoffMs: 200 },
    hostOverrides: {
      "api-pub.bitfinex.com": { minIntervalMs: 50, maxAttempts: 4, baseBackoffMs: 10, maxBackoffMs: 200 },
    },
    now: () => nowMs,
    sleep: async (ms: number) => {
      sleeps.push(ms);
      nowMs += ms;
    },
  });

  const response = await wrapped("https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist?start=1&end=2");
  assert.strictEqual(response.status, 200);
  assert.strictEqual(calls, 2);
  assert.deepStrictEqual(sleeps, [200]);
});

test("rate limited fetch honors long Retry-After values", async () => {
  let nowMs = 0;
  const sleeps: number[] = [];
  let calls = 0;

  const fetchImpl: FetchLike = async () => {
    calls += 1;
    if (calls === 1) {
      return new Response("busy", { status: 429, headers: { "retry-after": "60" } });
    }
    return new Response("ok", { status: 200 });
  };

  const wrapped = createRateLimitedFetch(fetchImpl, {
    defaultPolicy: { minIntervalMs: 50, maxAttempts: 4, baseBackoffMs: 10, maxBackoffMs: 200 },
    hostOverrides: {
      "api-pub.bitfinex.com": { minIntervalMs: 50, maxAttempts: 4, baseBackoffMs: 10, maxBackoffMs: 200 },
    },
    now: () => nowMs,
    sleep: async (ms: number) => {
      sleeps.push(ms);
      nowMs += ms;
    },
  });

  const response = await wrapped("https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist?start=1&end=2");
  assert.strictEqual(response.status, 200);
  assert.strictEqual(calls, 2);
  assert.deepStrictEqual(sleeps, [60_000]);
});

test("rate limited fetch caps extreme Retry-After values", async () => {
  let nowMs = 0;
  const sleeps: number[] = [];
  let calls = 0;

  const fetchImpl: FetchLike = async () => {
    calls += 1;
    if (calls === 1) {
      return new Response("busy", { status: 429, headers: { "retry-after": "9999" } });
    }
    return new Response("ok", { status: 200 });
  };

  const wrapped = createRateLimitedFetch(fetchImpl, {
    defaultPolicy: { minIntervalMs: 50, maxAttempts: 4, baseBackoffMs: 10, maxBackoffMs: 200 },
    hostOverrides: {
      "api-pub.bitfinex.com": { minIntervalMs: 50, maxAttempts: 4, baseBackoffMs: 10, maxBackoffMs: 200 },
    },
    now: () => nowMs,
    sleep: async (ms: number) => {
      sleeps.push(ms);
      nowMs += ms;
    },
  });

  const response = await wrapped("https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist?start=1&end=2");
  assert.strictEqual(response.status, 200);
  assert.strictEqual(calls, 2);
  assert.deepStrictEqual(sleeps, [300_000]);
});

test("rate limited fetch enforces host pacing only per host", async () => {
  let nowMs = 0;
  const sleeps: number[] = [];
  const callTimes: number[] = [];

  const fetchImpl: FetchLike = async () => {
    callTimes.push(nowMs);
    return new Response("ok", { status: 200 });
  };

  const wrapped = createRateLimitedFetch(fetchImpl, {
    defaultPolicy: { minIntervalMs: 50, maxAttempts: 1, baseBackoffMs: 10, maxBackoffMs: 100 },
    now: () => nowMs,
    sleep: async (ms: number) => {
      sleeps.push(ms);
      nowMs += ms;
    },
  });

  await wrapped("https://a.test/path-1");
  await wrapped("https://a.test/path-2");
  await wrapped("https://b.test/path-1");

  assert.deepStrictEqual(callTimes, [0, 50, 50]);
  assert.deepStrictEqual(sleeps, [50]);
});

test("rate limited fetch stops retries at max attempts", async () => {
  let nowMs = 0;
  const sleeps: number[] = [];
  let calls = 0;

  const fetchImpl: FetchLike = async () => {
    calls += 1;
    return new Response("busy", { status: 429 });
  };

  const wrapped = createRateLimitedFetch(fetchImpl, {
    defaultPolicy: { minIntervalMs: 10, maxAttempts: 3, baseBackoffMs: 20, maxBackoffMs: 100 },
    hostOverrides: {
      "api-pub.bitfinex.com": { minIntervalMs: 10, maxAttempts: 3, baseBackoffMs: 20, maxBackoffMs: 100 },
    },
    now: () => nowMs,
    sleep: async (ms: number) => {
      sleeps.push(ms);
      nowMs += ms;
    },
  });

  const response = await wrapped("https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist?start=1&end=2");
  assert.strictEqual(response.status, 429);
  assert.strictEqual(calls, 3);
  assert.deepStrictEqual(sleeps, [20, 40]);
});

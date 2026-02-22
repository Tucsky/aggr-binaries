import assert from "node:assert/strict";
import { test } from "node:test";
import { createKrakenAdapter } from "../../src/core/gaps/adapters/kraken/index.js";
import type { FetchLike } from "../../src/core/gaps/adapters/types.js";
import type { RecoveredTrade } from "../../src/core/gaps/adapters/types.js";
import { appendRecoveredTrades, inferKrakenTickSide } from "../../src/core/gaps/adapters/kraken/direct.js";
import { isAllowedKrakenTermination, shouldRestartKrakenCursor } from "../../src/core/gaps/adapters/kraken/directZip.js";

function createNoopDirectSource() {
  return {
    async recover() {
      return { trades: [], coverageEndTs: undefined };
    },
  };
}

test("kraken adapter accepts fractional window timestamps", async () => {
  let calls = 0;
  const fetchImpl: FetchLike = async () => {
    calls += 1;
    const payload = {
      error: [],
      result: {
        XXBTZUSD: [
          ["20000.1", "0.1", "1660708799.904240", "b", "l", ""],
          ["20001.0", "0.2", "1660708806.000000", "s", "l", ""],
        ],
        last: "1660708806000000000",
      },
    };
    return new Response(JSON.stringify(payload), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  const adapter = createKrakenAdapter(fetchImpl, {
    directSource: createNoopDirectSource(),
  });
  const trades = await adapter.recover({
    exchange: "KRAKEN",
    symbol: "XBT-USD",
    windows: [
      {
        eventId: 1,
        fromTs: 1_660_708_799_042.407,
        toTs: 1_660_708_805_000.12,
      },
    ],
  });

  assert.strictEqual(calls, 1);
  assert.strictEqual(trades.length, 1);
  assert.strictEqual(Number.isInteger(trades[0]?.ts), true);
  assert.strictEqual(trades[0]?.ts, 1_660_708_799_904);
});

test("kraken adapter surfaces kraken errors when result is omitted", async () => {
  const fetchImpl: FetchLike = async () => {
    const payload = {
      error: ["EGeneral:Temporary lockout"],
    };
    return new Response(JSON.stringify(payload), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  const adapter = createKrakenAdapter(fetchImpl, {
    directSource: createNoopDirectSource(),
  });
  await assert.rejects(
    () =>
      adapter.recover({
        exchange: "KRAKEN",
        symbol: "XBT-USD",
        windows: [{ eventId: 1, fromTs: 1_660_000_000_000, toTs: 1_660_000_060_000 }],
      }),
    /Kraken error: EGeneral:Temporary lockout/,
  );
});

test("kraken adapter retries kraken rate-limit payloads", async () => {
  let calls = 0;
  const sleeps: number[] = [];
  const fetchImpl: FetchLike = async () => {
    calls += 1;
    if (calls < 3) {
      return new Response(JSON.stringify({ error: ["EGeneral:Too many requests"] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (calls > 3) {
      const emptyPayload = {
        error: [],
        result: {
          XXBTZUSD: [],
          last: "1660708800000000000",
        },
      };
      return new Response(JSON.stringify(emptyPayload), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    const payload = {
      error: [],
      result: {
        XXBTZUSD: [["20000.1", "0.1", "1660708799.904240", "b", "l", ""]],
        last: "1660708800000000000",
      },
    };
    return new Response(JSON.stringify(payload), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  const adapter = createKrakenAdapter(fetchImpl, {
    sleep: async (ms: number) => {
      sleeps.push(ms);
    },
    directSource: createNoopDirectSource(),
  });
  const trades = await adapter.recover({
    exchange: "KRAKEN",
    symbol: "XBT-USD",
    windows: [{ eventId: 1, fromTs: 1_660_708_799_000, toTs: 1_660_708_800_000 }],
  });

  assert.ok(calls >= 3);
  assert.deepStrictEqual(sleeps, [1000, 2000]);
  assert.strictEqual(trades.length, 1);
});

test("kraken adapter falls back to api when direct source throws", async () => {
  const fetchImpl: FetchLike = async () => {
    const payload = {
      error: [],
      result: {
        XXBTZUSD: [["20000.1", "0.1", "1660708799.904240", "b", "l", ""]],
        last: "1660708800000000000",
      },
    };
    return new Response(JSON.stringify(payload), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  const adapter = createKrakenAdapter(fetchImpl, {
    directSource: {
      async recover() {
        throw new Error("simulated direct source failure");
      },
    },
  });
  const trades = await adapter.recover({
    exchange: "KRAKEN",
    symbol: "XBT-USD",
    windows: [{ eventId: 1, fromTs: 1_660_708_799_000, toTs: 1_660_708_800_000 }],
  });
  assert.strictEqual(trades.length, 1);
});

test("kraken adapter merges direct trades and only calls API for uncovered tail", async () => {
  const urls: string[] = [];
  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    urls.push(url);
    const since = new URL(url).searchParams.get("since") ?? "0";
    const payload = {
      error: [],
      result: {
        XXBTZUSD: [
          ["20100", "0.05", "1700000100.000000", "b", "l", ""],
        ],
        last: since,
      },
    };
    return new Response(JSON.stringify(payload), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  const adapter = createKrakenAdapter(fetchImpl, {
    directSource: {
      async recover() {
        return {
          trades: [
            {
              ts: 1_699_999_500_000,
              price: 20_000,
              size: 0.1,
              side: "buy",
              priceText: "20000",
              sizeText: "0.1",
            },
          ],
          coverageEndTs: 1_700_000_000_000,
        };
      },
    },
  });

  const trades = await adapter.recover({
    exchange: "KRAKEN",
    symbol: "XBT-USD",
    windows: [
      { eventId: 1, fromTs: 1_699_999_000_000, toTs: 1_699_999_900_000 },
      { eventId: 2, fromTs: 1_700_000_050_000, toTs: 1_700_000_120_000 },
    ],
  });

  assert.ok(urls.length >= 1);
  assert.match(urls[0] ?? "", /since=1699999000000000000/);
  assert.strictEqual(trades.length, 2);
});

test("inferKrakenTickSide uses previous side when price is unchanged", () => {
  const state = { lastSide: "buy" as const, lastPrice: 100 };
  const first = inferKrakenTickSide(101, state);
  const second = inferKrakenTickSide(101, state);
  const third = inferKrakenTickSide(99, state);
  assert.strictEqual(first, "buy");
  assert.strictEqual(second, "buy");
  assert.strictEqual(third, "sell");
});

test("appendRecoveredTrades handles large batches without stack overflow", () => {
  const target: RecoveredTrade[] = [];
  const source: RecoveredTrade[] = [];
  for (let i = 0; i < 250_000; i += 1) {
    source.push({
      ts: 1_700_000_000_000 + i,
      price: 20_000 + i * 0.01,
      size: 0.001,
      side: "buy",
      priceText: String(20_000 + i * 0.01),
      sizeText: "0.001",
    });
  }

  appendRecoveredTrades(target, source);
  assert.strictEqual(target.length, source.length);
  assert.strictEqual(target[0]?.ts, source[0]?.ts);
  assert.strictEqual(target[target.length - 1]?.ts, source[source.length - 1]?.ts);
});

test("kraken cursor restart tolerance ignores sub-ms rewind noise", () => {
  assert.strictEqual(shouldRestartKrakenCursor(1000, 1000), false);
  assert.strictEqual(shouldRestartKrakenCursor(999.4, 1000), false);
  assert.strictEqual(shouldRestartKrakenCursor(998.9, 1000), true);
});

test("kraken cursor close treats unzip termination codes as benign", () => {
  assert.strictEqual(isAllowedKrakenTermination({ code: 80, signal: null }, true), true);
  assert.strictEqual(isAllowedKrakenTermination({ code: 141, signal: null }, true), true);
  assert.strictEqual(isAllowedKrakenTermination({ code: 143, signal: null }, true), true);
  assert.strictEqual(isAllowedKrakenTermination({ code: 2, signal: null }, true), false);
  assert.strictEqual(isAllowedKrakenTermination({ code: 2, signal: null }, false), false);
});

import assert from "node:assert/strict";
import { test } from "node:test";
import { createKrakenAdapter } from "../../src/core/gaps/adapters/kraken.js";
import type { FetchLike } from "../../src/core/gaps/adapters/types.js";

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

  const adapter = createKrakenAdapter(fetchImpl);
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

  const adapter = createKrakenAdapter(fetchImpl);
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

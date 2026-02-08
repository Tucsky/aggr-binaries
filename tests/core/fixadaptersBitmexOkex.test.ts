import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import { test } from "node:test";
import { createBitmexAdapter } from "../../src/core/gaps/adapters/bitmex.js";
import { createOkexAdapter } from "../../src/core/gaps/adapters/okex.js";
import type { FetchLike } from "../../src/core/gaps/adapters/types.js";

test("bitmex adapter reads public day files and filters by symbol/window", async () => {
  const targetTs = Date.parse("2024-01-01T00:00:01.000Z");
  const csv = [
    "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,trdType",
    "2024-01-01D00:00:01.000000000,XBTUSD,Buy,10,100,PlusTick,id-1,1000,0.1,10,Regular",
    "2024-01-01D00:00:02.000000000,ETHUSD,Sell,20,200,MinusTick,id-2,4000,0.2,40,Regular",
  ].join("\n");
  const body = gzipSync(csv);
  const urls: string[] = [];

  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    urls.push(url);
    return new Response(body, { status: 200 });
  };

  const adapter = createBitmexAdapter(fetchImpl);
  const trades = await adapter.recover({
    exchange: "BITMEX",
    symbol: "XBTUSD",
    windows: [{ eventId: 1, fromTs: targetTs - 500, toTs: targetTs + 500 }],
  });

  assert.strictEqual(urls.length, 1);
  assert.match(urls[0] ?? "", /\/20240101\.csv\.gz$/);
  assert.deepStrictEqual(
    trades.map((trade) => ({
      ts: trade.ts,
      price: trade.price,
      size: trade.size,
      side: trade.side,
    })),
    [{ ts: targetTs, price: 100, size: 0.1, side: "buy" }],
  );
});

test("okex adapter recovers direct daily trades for derivatives", async () => {
  const calls: string[] = [];
  const fromTs = Date.parse("2024-01-01T00:00:01.000Z");
  const toTs = fromTs + 2_000;
  const directZip = createStoredZip(
    "BTC-USDT-SWAP-trades-2024-01-01.csv",
    [
      "instrument_name,trade_id,side,price,size,created_time",
      `BTC-USDT-SWAP,1,buy,100,2,${fromTs + 1_500}`,
      `BTC-USDT-SWAP,2,sell,110,4,${fromTs + 500}`,
    ].join("\n"),
  );

  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    calls.push(url);
    if (url.startsWith("https://www.okx.com/api/v5/public/instruments")) {
      return new Response(
        JSON.stringify({
          code: "0",
          msg: "",
          data: [
            {
              instId: "BTC-USDT-SWAP",
              instType: "SWAP",
              uly: "BTC-USDT",
              ctType: "linear",
              ctVal: "0.01",
            },
          ],
        }),
        { status: 200 },
      );
    }
    if (url.startsWith("https://static.okx.com/cdn/okex/traderecords/trades/daily/")) {
      return binaryResponse(directZip, 200);
    }
    throw new Error(`Unexpected URL ${url}`);
  };

  const adapter = createOkexAdapter(fetchImpl);
  const trades = await adapter.recover({
    exchange: "OKEX",
    symbol: "BTC-USDT-SWAP",
    windows: [{ eventId: 1, fromTs, toTs }],
  });

  assert.deepStrictEqual(
    trades.map((trade) => ({
      ts: trade.ts,
      price: trade.price,
      size: trade.size,
      side: trade.side,
    })),
    [
      { ts: fromTs + 500, price: 110, size: 0.04, side: "sell" },
      { ts: fromTs + 1_500, price: 100, size: 0.02, side: "buy" },
    ],
  );

  assert.ok(calls.some((url) => url.startsWith("https://static.okx.com/cdn/okex/traderecords/trades/daily/")));
  assert.ok(!calls.some((url) => url.startsWith("https://www.okx.com/api/v5/market/history-trades")));
  assert.ok(!calls.some((url) => url.startsWith("https://www.okx.com/api/v5/public/liquidation-orders")));
});

test("okex adapter returns no trades when direct daily file is unavailable", async () => {
  const calls: string[] = [];
  const fromTs = Date.parse("2024-01-03T00:00:01.000Z");
  const toTs = fromTs + 2_000;

  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    calls.push(url);
    if (url.startsWith("https://www.okx.com/api/v5/public/instruments")) {
      return new Response(
        JSON.stringify({
          code: "0",
          msg: "",
          data: [
            {
              instId: "BTC-USDT",
              instType: "SPOT",
              ctType: "linear",
              ctVal: "1",
            },
          ],
        }),
        { status: 200 },
      );
    }
    if (url.startsWith("https://static.okx.com/cdn/okex/traderecords/trades/daily/")) {
      return new Response("missing", { status: 404 });
    }
    throw new Error(`Unexpected URL ${url}`);
  };

  const adapter = createOkexAdapter(fetchImpl);
  const trades = await adapter.recover({
    exchange: "OKEX",
    symbol: "BTC-USDT",
    windows: [{ eventId: 1, fromTs, toTs }],
  });

  assert.deepStrictEqual(trades, []);

  assert.ok(calls.some((url) => url.startsWith("https://static.okx.com/cdn/okex/traderecords/trades/daily/")));
  assert.ok(!calls.some((url) => url.startsWith("https://www.okx.com/api/v5/market/history-trades")));
});

test("okex adapter skips direct daily files before 2021-09-02 without trades api fallback", async () => {
  const calls: string[] = [];
  const fromTs = Date.parse("2021-08-01T00:00:00.000Z");
  const toTs = fromTs + 2_000;

  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    calls.push(url);
    if (url.startsWith("https://www.okx.com/api/v5/public/instruments")) {
      return new Response(
        JSON.stringify({
          code: "0",
          msg: "",
          data: [
            {
              instId: "BTC-USDT",
              instType: "SPOT",
              ctType: "linear",
              ctVal: "1",
            },
          ],
        }),
        { status: 200 },
      );
    }
    throw new Error(`Unexpected URL ${url}`);
  };

  const adapter = createOkexAdapter(fetchImpl);
  const trades = await adapter.recover({
    exchange: "OKEX",
    symbol: "BTC-USDT",
    windows: [{ eventId: 1, fromTs, toTs }],
  });

  assert.deepStrictEqual(trades, []);

  assert.ok(!calls.some((url) => url.startsWith("https://static.okx.com/cdn/okex/traderecords/trades/daily/")));
  assert.ok(!calls.some((url) => url.startsWith("https://www.okx.com/api/v5/market/history-trades")));
});

function createStoredZip(fileName: string, content: string): Buffer {
  const name = Buffer.from(fileName, "utf8");
  const data = Buffer.from(content, "utf8");
  const header = Buffer.alloc(30);
  header.writeUInt32LE(0x04034b50, 0); // Local file header signature
  header.writeUInt16LE(20, 4); // Version needed
  header.writeUInt16LE(0, 6); // General purpose bit flag
  header.writeUInt16LE(0, 8); // Compression method (store)
  header.writeUInt16LE(0, 10); // Last mod file time
  header.writeUInt16LE(0, 12); // Last mod file date
  header.writeUInt32LE(0, 14); // CRC-32 (unused by parser)
  header.writeUInt32LE(data.length, 18); // Compressed size
  header.writeUInt32LE(data.length, 22); // Uncompressed size
  header.writeUInt16LE(name.length, 26); // File name length
  header.writeUInt16LE(0, 28); // Extra field length
  return Buffer.concat([header, name, data]);
}

function binaryResponse(data: Buffer, status: number): Response {
  const bodySlice = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: new Headers(),
    body: null,
    arrayBuffer: async () => bodySlice,
    text: async () => Buffer.from(bodySlice).toString("utf8"),
    json: async () => JSON.parse(Buffer.from(bodySlice).toString("utf8")),
  } as unknown as Response;
}

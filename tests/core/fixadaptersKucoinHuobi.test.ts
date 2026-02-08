import assert from "node:assert/strict";
import { test } from "node:test";
import { createHuobiAdapter } from "../../src/core/gaps/adapters/huobi.js";
import { createKucoinAdapter } from "../../src/core/gaps/adapters/kucoin.js";
import type { FetchLike } from "../../src/core/gaps/adapters/types.js";

test("kucoin adapter recovers spot daily trades from direct zip", async () => {
  const zip = createStoredZip(
    "BTCUSDT-trades-2022-12-31.csv",
    [
      "trade_id,trade_time,price,size,side",
      "1,1672444801000,16603.9,0.1,BUY",
      "2,1672444802000,16604.1,0.2,SELL",
    ].join("\n"),
  );

  const calls: string[] = [];
  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    calls.push(url);
    if (url.endsWith("/BTCUSDT/BTCUSDT-trades-2022-12-31.zip")) {
      return binaryResponse(zip, 200);
    }
    return new Response("missing", { status: 404 });
  };

  const adapter = createKucoinAdapter(fetchImpl);
  const trades = await adapter.recover({
    exchange: "KUCOIN",
    symbol: "BTCUSDT",
    windows: [{ eventId: 1, fromTs: 1672444800000, toTs: 1672444802500 }],
  });

  assert.deepStrictEqual(
    trades.map((trade) => ({
      ts: trade.ts,
      price: trade.price,
      size: trade.size,
      side: trade.side,
    })),
    [
      { ts: 1672444801000, price: 16603.9, size: 0.1, side: "buy" },
      { ts: 1672444802000, price: 16604.1, size: 0.2, side: "sell" },
    ],
  );
  assert.strictEqual(calls.length, 1);
});

test("huobi adapter prefers spot dataset for compact symbols", async () => {
  const zip = createStoredZip(
    "BTCUSDT-trades-2021-10-14.csv",
    [
      "102543685911,1634140800375,56161.55,0.000101,sell",
      "102543685912,1634140800674,56161.56,0.001000,buy",
    ].join("\n"),
  );

  const calls: string[] = [];
  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    calls.push(url);
    if (url.includes("/trades/spot/daily/BTCUSDT/BTCUSDT-trades-2021-10-14.zip")) {
      return binaryResponse(zip, 200);
    }
    return new Response("missing", { status: 404 });
  };

  const adapter = createHuobiAdapter(fetchImpl);
  const trades = await adapter.recover({
    exchange: "HUOBI",
    symbol: "BTCUSDT",
    windows: [{ eventId: 1, fromTs: 1634140800000, toTs: 1634140801000 }],
  });

  assert.deepStrictEqual(
    trades.map((trade) => ({
      ts: trade.ts,
      price: trade.price,
      size: trade.size,
      side: trade.side,
    })),
    [
      { ts: 1634140800375, price: 56161.55, size: 0.000101, side: "sell" },
      { ts: 1634140800674, price: 56161.56, size: 0.001, side: "buy" },
    ],
  );
  assert.strictEqual(calls.length, 1);
  assert.match(calls[0] ?? "", /\/trades\/spot\/daily\//);
});

test("huobi adapter supports linear-swap daily dataset and uses base size column", async () => {
  const zip = createStoredZip(
    "BTC-USDT-trades-2022-03-04.csv",
    [
      "98383949353,1646323200290,42524.7,1.0,0.001,42.5247,buy",
      "98383949466,1646323200592,42524.6,2.0,0.002,85.0492,sell",
    ].join("\n"),
  );

  const calls: string[] = [];
  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    calls.push(url);
    if (url.includes("/trades/linear-swap/daily/BTC-USDT/BTC-USDT-trades-2022-03-04.zip")) {
      return binaryResponse(zip, 200);
    }
    return new Response("missing", { status: 404 });
  };

  const adapter = createHuobiAdapter(fetchImpl);
  const trades = await adapter.recover({
    exchange: "HUOBI",
    symbol: "BTC-USDT-SWAP",
    windows: [{ eventId: 1, fromTs: 1646323200000, toTs: 1646323201000 }],
  });

  assert.deepStrictEqual(
    trades.map((trade) => ({
      ts: trade.ts,
      price: trade.price,
      size: trade.size,
      side: trade.side,
    })),
    [
      { ts: 1646323200290, price: 42524.7, size: 0.001, side: "buy" },
      { ts: 1646323200592, price: 42524.6, size: 0.002, side: "sell" },
    ],
  );
  assert.strictEqual(calls.length, 1);
  assert.match(calls[0] ?? "", /\/trades\/linear-swap\/daily\//);
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

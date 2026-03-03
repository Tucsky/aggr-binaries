import assert from "node:assert/strict";
import { test } from "node:test";
import { VOL_SCALE, accumulate, parseTradeLine, type Candle, type ParseReject } from "../../src/core/trades.js";

test("parseTradeLine handles whitespace without regex", () => {
  const reject: ParseReject = {};
  const trade = parseTradeLine("\t1704067200100   123.45\t0.5\t1", reject);

  assert.ok(trade);
  assert.strictEqual(trade?.ts, 1704067200100);
  assert.strictEqual(trade?.price, 123.45);
  assert.strictEqual(trade?.size, 0.5);
  assert.strictEqual(trade?.side, "buy");
  assert.strictEqual(trade?.liquidation, false);
  assert.strictEqual(reject.reason, undefined);
});

test("parseTradeLine keeps validation semantics", () => {
  const reject: ParseReject = {};
  const badNotional = parseTradeLine("1704067200200 60000 20000 0 1", reject);
  assert.strictEqual(badNotional, null);
  assert.strictEqual(reject.reason, "notional_too_large");

  reject.reason = undefined;
  const missingField = parseTradeLine("1704067200300 1 1", reject);
  assert.strictEqual(missingField, null);
  assert.strictEqual(reject.reason, "parts_short");

  reject.reason = undefined;
  const nonFinite = parseTradeLine("bad_ts 1 1 0", reject);
  assert.strictEqual(nonFinite, null);
  assert.strictEqual(reject.reason, "non_finite");
});

test("accumulate excludes liquidation rows from OHLC, trade vol, and trade counts", () => {
  const timeframeMs = 60_000;
  const slot = 1_704_067_200_000;
  const acc = {
    buckets: new Map<number, Candle>(),
    minMinute: Number.POSITIVE_INFINITY,
    maxMinute: Number.NEGATIVE_INFINITY,
  };

  accumulate(acc, { ts: slot + 1_000, price: 100, size: 1, side: "buy", liquidation: true }, timeframeMs);
  accumulate(acc, { ts: slot + 2_000, price: 102, size: 0.5, side: "sell", liquidation: false }, timeframeMs);
  accumulate(acc, { ts: slot + 3_000, price: 90, size: 2, side: "sell", liquidation: true }, timeframeMs);
  accumulate(acc, { ts: slot + 4_000, price: 105, size: 1.25, side: "buy", liquidation: false }, timeframeMs);

  const candle = acc.buckets.get(slot);
  assert.ok(candle);
  assert.deepStrictEqual(candle, {
    open: 1_020_000,
    high: 1_050_000,
    low: 1_020_000,
    close: 1_050_000,
    buyVol: BigInt(Math.round(105 * 1.25 * VOL_SCALE)),
    sellVol: BigInt(Math.round(102 * 0.5 * VOL_SCALE)),
    buyCount: 1,
    sellCount: 1,
    liqBuy: BigInt(Math.round(100 * 1 * VOL_SCALE)),
    liqSell: BigInt(Math.round(90 * 2 * VOL_SCALE)),
    openTs: slot + 2_000,
    closeTs: slot + 4_000,
  });
});

test("accumulate keeps OHLC deterministic when trades for a bucket arrive out of order", () => {
  const timeframeMs = 60_000;
  const slot = 1_704_067_200_000;
  const acc = {
    buckets: new Map<number, Candle>(),
    minMinute: Number.POSITIVE_INFINITY,
    maxMinute: Number.NEGATIVE_INFINITY,
  };

  accumulate(acc, { ts: slot + 40_000, price: 100, size: 1, side: "buy", liquidation: false }, timeframeMs);
  accumulate(acc, { ts: slot + 50_000, price: 101, size: 1, side: "sell", liquidation: false }, timeframeMs);
  accumulate(acc, { ts: slot + 10_000, price: 99, size: 1, side: "buy", liquidation: false }, timeframeMs);
  accumulate(acc, { ts: slot + 25_000, price: 130, size: 0.5, side: "sell", liquidation: false }, timeframeMs);

  const candle = acc.buckets.get(slot);
  assert.ok(candle);
  assert.deepStrictEqual(candle, {
    open: 990_000,
    high: 1_300_000,
    low: 990_000,
    close: 1_010_000,
    buyVol: BigInt(Math.round((100 + 99) * VOL_SCALE)),
    sellVol: BigInt(Math.round((101 + 65) * VOL_SCALE)),
    buyCount: 2,
    sellCount: 2,
    liqBuy: 0n,
    liqSell: 0n,
    openTs: slot + 10_000,
    closeTs: slot + 50_000,
  });
});

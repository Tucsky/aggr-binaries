import assert from "node:assert/strict";
import { test } from "node:test";
import { parseTradeLine, type ParseReject } from "../../src/core/trades.js";

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

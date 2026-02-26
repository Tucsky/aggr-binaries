import assert from "node:assert/strict";
import { test } from "node:test";
import {
  buildViewportEventsQueryKey,
  clampRangeToBounds,
  expandRangeWithinBounds,
  filterMarketsWithRange,
  isRangeCoveredBy,
  shouldKeepFilterSelection,
} from "../../client/src/lib/features/timeline/timelinePageHelpers.js";
import type { TimelineMarket } from "../../client/src/lib/features/timeline/timelineTypes.js";

test("shouldKeepFilterSelection keeps saved filters until options are loaded", () => {
  assert.strictEqual(shouldKeepFilterSelection("PI", []), true);
  assert.strictEqual(shouldKeepFilterSelection("PI", ["PI", "RAM"]), true);
  assert.strictEqual(shouldKeepFilterSelection("PI", ["RAM"]), false);
  assert.strictEqual(shouldKeepFilterSelection("", ["RAM"]), true);
});

test("filterMarketsWithRange returns filtered list and global range in one pass", () => {
  const markets: TimelineMarket[] = [
    { collector: "PI", exchange: "OKEX", symbol: "BTC-USDT", timeframe: "1m", startTs: 100, endTs: 400 },
    { collector: "PI", exchange: "BINANCE", symbol: "BTCUSDT", timeframe: "1m", startTs: 700, endTs: 1200 },
    { collector: "PI", exchange: "BINANCE", symbol: "ETHUSDT", timeframe: "1m", startTs: 800, endTs: 900 },
  ];
  const filtered = filterMarketsWithRange(markets, "PI", "BINANCE", "btc");
  assert.deepStrictEqual(filtered.markets, [markets[1]]);
  assert.deepStrictEqual(filtered.range, { startTs: 700, endTs: 1200 });
  const empty = filterMarketsWithRange(markets, "PI", "KRAKEN", "");
  assert.deepStrictEqual(empty.markets, []);
  assert.strictEqual(empty.range, null);
});

test("timeline event viewport helpers clamp, expand, and coverage-check deterministically", () => {
  const bounds = { startTs: 100, endTs: 400 };
  const view = { startTs: 80, endTs: 250 };
  const clamped = clampRangeToBounds(view, bounds);
  assert.deepStrictEqual(clamped, { startTs: 100, endTs: 250 });

  const expanded = expandRangeWithinBounds(clamped, bounds, 0.5);
  assert.deepStrictEqual(expanded, { startTs: 100, endTs: 325 });
  assert.strictEqual(isRangeCoveredBy(expanded, clamped), true);
  assert.strictEqual(isRangeCoveredBy(clamped, expanded), false);
});

test("buildViewportEventsQueryKey includes scope, range, and row ordering", () => {
  const key = buildViewportEventsQueryKey("1m|PI|BYBIT|btc|100|200", { startTs: 110, endTs: 190 }, [
    "PI:BYBIT:BTCUSDT",
    "RAM:BITMEX:ETHUSD",
  ]);
  assert.strictEqual(
    key,
    "1m|PI|BYBIT|btc|100|200|110|190|PI:BYBIT:BTCUSDT,RAM:BITMEX:ETHUSD",
  );
});

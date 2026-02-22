import assert from "node:assert/strict";
import { test } from "node:test";
import { filterMarketsWithRange, shouldKeepFilterSelection } from "../../client/src/lib/features/timeline/timelinePageHelpers.js";
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

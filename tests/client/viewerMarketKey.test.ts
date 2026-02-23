import assert from "node:assert/strict";
import { test } from "node:test";
import {
  buildViewerMarketKey,
  parseViewerMarketKey,
} from "../../client/src/lib/features/viewer/viewerMarketKey.js";

test("buildViewerMarketKey normalizes collector/exchange and preserves symbol content", () => {
  assert.strictEqual(
    buildViewerMarketKey({
      collector: " pi ",
      exchange: " kraken ",
      symbol: " XBT:USD ",
    }),
    "PI:KRAKEN:XBT:USD",
  );
});

test("parseViewerMarketKey accepts COLLECTOR:EXCHANGE:SYMBOL and keeps symbols with colons", () => {
  assert.deepStrictEqual(
    parseViewerMarketKey("ram:binance:btc:usdt"),
    { collector: "RAM", exchange: "BINANCE", symbol: "btc:usdt" },
  );
});

test("parseViewerMarketKey rejects incomplete keys", () => {
  assert.strictEqual(parseViewerMarketKey("KRAKEN:XBTUSD"), null);
  assert.strictEqual(parseViewerMarketKey("::"), null);
  assert.strictEqual(parseViewerMarketKey(""), null);
});

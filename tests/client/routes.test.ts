import assert from "node:assert/strict";
import { test } from "node:test";
import {
  applyChartRouteToPrefs,
  buildAppRouteUrl,
  parseAppRoute,
  resolveRouteMarket,
  type ChartRoute,
} from "../../client/src/lib/routes.js";
import type { Prefs } from "../../client/src/lib/types.js";

test("parseAppRoute supports timeline and chart path/query combinations", () => {
  assert.deepStrictEqual(parseAppRoute("/", ""), { kind: "timeline" });
  assert.deepStrictEqual(parseAppRoute("/timeline", ""), { kind: "timeline" });
  assert.deepStrictEqual(parseAppRoute("/unknown", ""), { kind: "timeline" });

  assert.deepStrictEqual(parseAppRoute("/chart", ""), { kind: "chart", timeframe: undefined, startTs: undefined });
  assert.deepStrictEqual(parseAppRoute("/chart/ram/binance/btcusdt", ""), {
    kind: "chart",
    market: { collector: "RAM", exchange: "BINANCE", symbol: "btcusdt" },
    timeframe: undefined,
    startTs: undefined,
  });
  assert.deepStrictEqual(parseAppRoute("/chart/RAM/BINANCE/BTCUSDT", "?timeframe=5m&startTs=1700000000000"), {
    kind: "chart",
    market: { collector: "RAM", exchange: "BINANCE", symbol: "BTCUSDT" },
    timeframe: "5m",
    startTs: 1700000000000,
  });
});

test("buildAppRouteUrl generates canonical chart URLs with optional query params", () => {
  assert.strictEqual(buildAppRouteUrl({ kind: "timeline" }), "/timeline");
  assert.strictEqual(buildAppRouteUrl({ kind: "chart" }), "/chart");
  assert.strictEqual(
    buildAppRouteUrl({
      kind: "chart",
      market: { collector: "RAM", exchange: "BINANCE", symbol: "BTCUSDT" },
    }),
    "/chart/RAM/BINANCE/BTCUSDT",
  );
  assert.strictEqual(
    buildAppRouteUrl({
      kind: "chart",
      market: { collector: "RAM", exchange: "BINANCE", symbol: "BTCUSDT" },
      timeframe: "1m",
      startTs: 1700000000123,
    }),
    "/chart/RAM/BINANCE/BTCUSDT?timeframe=1m&startTs=1700000000123",
  );
});

test("applyChartRouteToPrefs lets URL route values override stored prefs", () => {
  const base: Prefs = {
    collector: "PI",
    exchange: "BYBIT",
    symbol: "ETHUSDT",
    timeframe: "1h",
    timeframes: ["1m", "5m", "1h"],
    start: "",
  };
  const route: ChartRoute = {
    kind: "chart",
    market: { collector: "RAM", exchange: "BINANCE", symbol: "BTCUSDT" },
    timeframe: "5m",
    startTs: 1700000000000,
  };
  const merged = applyChartRouteToPrefs(base, route);
  assert.strictEqual(merged.collector, "RAM");
  assert.strictEqual(merged.exchange, "BINANCE");
  assert.strictEqual(merged.symbol, "BTCUSDT");
  assert.strictEqual(merged.timeframe, "5m");
  assert.ok(merged.start.length > 0);
});

test("resolveRouteMarket matches symbol case-insensitively", () => {
  const found = resolveRouteMarket(
    [
      { collector: "RAM", exchange: "BINANCE", symbol: "BTCUSDT" },
      { collector: "PI", exchange: "BYBIT", symbol: "ETHUSDT" },
    ],
    { collector: "ram", exchange: "binance", symbol: "btcusdt" },
  );
  assert.deepStrictEqual(found, { collector: "RAM", exchange: "BINANCE", symbol: "BTCUSDT" });
});

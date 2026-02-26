import assert from "node:assert/strict";
import { test } from "node:test";
import {
  buildTimelineEventsRowsPayload,
  TIMELINE_SYMBOL_MODE,
  buildTimelineEventsPath,
  buildTimelineMarketsPath,
} from "../../client/src/lib/features/timeline/timelineApi.js";

test("buildTimelineMarketsPath only serializes provided query params", () => {
  assert.strictEqual(buildTimelineMarketsPath(), "/api/timeline/markets");
  assert.strictEqual(
    buildTimelineMarketsPath({
      timeframe: "1m",
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
    }),
    "/api/timeline/markets?timeframe=1m&collector=PI&exchange=BYBIT&symbol=BTCUSDT",
  );
});

test("buildTimelineEventsPath serializes symbol mode and floors range bounds", () => {
  assert.strictEqual(
    buildTimelineEventsPath({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      symbolMode: TIMELINE_SYMBOL_MODE.Exact,
      startTs: 100.9,
      endTs: 200.1,
    }),
    "/api/timeline/events?collector=PI&exchange=BYBIT&symbol=BTCUSDT&symbolMode=exact&startTs=100&endTs=200",
  );
});

test("buildTimelineEventsRowsPayload normalizes rows and floors range bounds", () => {
  assert.deepStrictEqual(
    buildTimelineEventsRowsPayload({
      rows: [
        { collector: "pi", exchange: "bybit", symbol: "BTCUSDT" },
        { collector: "RAM", exchange: "bitmex", symbol: "ETHUSD" },
        { collector: " ", exchange: "bitmex", symbol: "XRPUSD" },
      ],
      startTs: 10.4,
      endTs: 20.9,
    }),
    {
      startTs: 10,
      endTs: 20,
      rows: [
        { collector: "PI", exchange: "BYBIT", symbol: "BTCUSDT" },
        { collector: "RAM", exchange: "BITMEX", symbol: "ETHUSD" },
      ],
    },
  );
});

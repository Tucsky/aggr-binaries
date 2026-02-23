import assert from "node:assert/strict";
import { test } from "node:test";
import {
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

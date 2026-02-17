import assert from "node:assert/strict";
import { test } from "node:test";
import {
  clampTs,
  groupEventsByMarket,
  toTimelineTs,
  toTimelineX,
  type TimelineRange,
} from "../../client/src/lib/timelineUtils.js";
import type { TimelineEvent } from "../../client/src/lib/timelineTypes.js";

test("toTimelineX and toTimelineTs map deterministically and clamp to bounds", () => {
  const range: TimelineRange = { startTs: 1_000, endTs: 11_000 };
  const width = 1000;
  assert.strictEqual(toTimelineX(1_000, range, width), 0);
  assert.strictEqual(toTimelineX(11_000, range, width), 1000);
  assert.strictEqual(toTimelineX(6_000, range, width), 500);

  assert.strictEqual(toTimelineTs(0, range, width), 1_000);
  assert.strictEqual(toTimelineTs(1000, range, width), 11_000);
  assert.strictEqual(toTimelineTs(500, range, width), 6_000);
  assert.strictEqual(toTimelineTs(-10, range, width), 1_000);
  assert.strictEqual(toTimelineTs(1200, range, width), 11_000);
});

test("toTimelineX and toTimelineTs honor edge padding", () => {
  const range: TimelineRange = { startTs: 1_000, endTs: 11_000 };
  const width = 1000;
  const padding = 8;
  assert.strictEqual(toTimelineX(1_000, range, width, padding), 8);
  assert.strictEqual(toTimelineX(11_000, range, width, padding), 992);
  assert.strictEqual(toTimelineTs(8, range, width, padding), 1_000);
  assert.strictEqual(toTimelineTs(992, range, width, padding), 11_000);
});

test("clampTs enforces min/max range", () => {
  assert.strictEqual(clampTs(5, 10, 20), 10);
  assert.strictEqual(clampTs(25, 10, 20), 20);
  assert.strictEqual(clampTs(15, 10, 20), 15);
});

test("groupEventsByMarket groups rows and preserves deterministic order by ts/id", () => {
  const events: TimelineEvent[] = [
    {
      id: 3,
      collector: "RAM",
      exchange: "BINANCE",
      symbol: "BTCUSDT",
      eventType: "gap",
      gapFixStatus: null,
      ts: 300,
      startLine: 1,
      endLine: 1,
      gapMs: 1,
      gapMiss: 1,
    },
    {
      id: 1,
      collector: "RAM",
      exchange: "BINANCE",
      symbol: "BTCUSDT",
      eventType: "gap",
      gapFixStatus: null,
      ts: 200,
      startLine: 1,
      endLine: 1,
      gapMs: 1,
      gapMiss: 1,
    },
    {
      id: 2,
      collector: "RAM",
      exchange: "BINANCE",
      symbol: "BTCUSDT",
      eventType: "gap",
      gapFixStatus: null,
      ts: 200,
      startLine: 1,
      endLine: 1,
      gapMs: 1,
      gapMiss: 1,
    },
  ];

  const grouped = groupEventsByMarket(events);
  const bucket = grouped.get("RAM:BINANCE:BTCUSDT");
  assert.ok(bucket);
  assert.deepStrictEqual(bucket?.map((event) => event.id), [1, 2, 3]);
});

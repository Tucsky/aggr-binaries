import assert from "node:assert/strict";
import { test } from "node:test";
import {
  clampTs,
  findTimelineEventWindow,
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

test("toTimelineX and toTimelineTs clamp deterministically for non-positive widths", () => {
  const range: TimelineRange = { startTs: 1_000, endTs: 11_000 };
  assert.strictEqual(toTimelineX(1_000, range, 0), 0);
  assert.strictEqual(toTimelineX(11_000, range, 0), 1);
  assert.strictEqual(toTimelineTs(0, range, 0), 1_000);
  assert.strictEqual(toTimelineTs(1, range, 0), 11_000);
  assert.strictEqual(toTimelineTs(50, range, 0), 11_000);
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
      relativePath: "RAM/BINANCE/BTCUSDT/2024-01-01.gz",
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
      relativePath: "RAM/BINANCE/BTCUSDT/2024-01-01.gz",
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
      relativePath: "RAM/BINANCE/BTCUSDT/2024-01-01.gz",
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

test("findTimelineEventWindow returns visible inclusive ts window via binary search", () => {
  const events: TimelineEvent[] = [
    {
      id: 1,
      collector: "RAM",
      exchange: "BINANCE",
      symbol: "BTCUSDT",
      relativePath: "RAM/BINANCE/BTCUSDT/2024-01-01.gz",
      eventType: "gap",
      gapFixStatus: null,
      ts: 100,
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
      relativePath: "RAM/BINANCE/BTCUSDT/2024-01-01.gz",
      eventType: "gap",
      gapFixStatus: null,
      ts: 200,
      startLine: 1,
      endLine: 1,
      gapMs: 1,
      gapMiss: 1,
    },
    {
      id: 3,
      collector: "RAM",
      exchange: "BINANCE",
      symbol: "BTCUSDT",
      relativePath: "RAM/BINANCE/BTCUSDT/2024-01-01.gz",
      eventType: "gap",
      gapFixStatus: null,
      ts: 300,
      startLine: 1,
      endLine: 1,
      gapMs: 1,
      gapMiss: 1,
    },
    {
      id: 4,
      collector: "RAM",
      exchange: "BINANCE",
      symbol: "BTCUSDT",
      relativePath: "RAM/BINANCE/BTCUSDT/2024-01-01.gz",
      eventType: "gap",
      gapFixStatus: null,
      ts: 300,
      startLine: 1,
      endLine: 1,
      gapMs: 1,
      gapMiss: 1,
    },
    {
      id: 5,
      collector: "RAM",
      exchange: "BINANCE",
      symbol: "BTCUSDT",
      relativePath: "RAM/BINANCE/BTCUSDT/2024-01-01.gz",
      eventType: "gap",
      gapFixStatus: null,
      ts: 400,
      startLine: 1,
      endLine: 1,
      gapMs: 1,
      gapMiss: 1,
    },
  ];

  const win = findTimelineEventWindow(events, 200, 300);
  assert.deepStrictEqual(win, { startIndex: 1, endIndex: 4 });
  assert.deepStrictEqual(events.slice(win.startIndex, win.endIndex).map((event) => event.id), [2, 3, 4]);
});

test("findTimelineEventWindow handles empty or invalid ranges deterministically", () => {
  const oneEvent: TimelineEvent[] = [
    {
      id: 1,
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      relativePath: "PI/BYBIT/BTCUSDT/2024-01-01.gz",
      eventType: "gap",
      gapFixStatus: null,
      ts: 250,
      startLine: 1,
      endLine: 1,
      gapMs: 1,
      gapMiss: 1,
    },
  ];

  assert.deepStrictEqual(findTimelineEventWindow([], 0, 1), { startIndex: 0, endIndex: 0 });
  assert.deepStrictEqual(findTimelineEventWindow(oneEvent, 300, 200), { startIndex: 0, endIndex: 0 });
  assert.deepStrictEqual(findTimelineEventWindow(oneEvent, 0, 200), { startIndex: 0, endIndex: 0 });
  assert.deepStrictEqual(findTimelineEventWindow(oneEvent, 200, 250), { startIndex: 0, endIndex: 1 });
});

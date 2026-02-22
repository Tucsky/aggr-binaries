import assert from "node:assert/strict";
import { test } from "node:test";
import { resolveOpenTsFromClick } from "../../client/src/lib/features/timeline/timelineRowClick.js";
import type { TimelineEvent } from "../../client/src/lib/features/timeline/timelineTypes.js";
import type { TimelineRange } from "../../client/src/lib/features/timeline/timelineUtils.js";

const marketRange: TimelineRange = { startTs: 1_000, endTs: 10_000 };

function makeGapEvent(ts: number, gapMs: number | null): TimelineEvent {
  return {
    id: 1,
    collector: "PI",
    exchange: "KRAKEN",
    symbol: "XBT-USD",
    relativePath: "PI/KRAKEN/XBT-USD/2022-11-10.gz",
    eventType: "gap",
    gapFixStatus: null,
    ts,
    startLine: 1,
    endLine: 1,
    gapMs,
    gapMiss: null,
  };
}

test("resolveOpenTsFromClick returns clamped click when no marker event", () => {
  assert.strictEqual(resolveOpenTsFromClick(900, marketRange, null), 1_000);
  assert.strictEqual(resolveOpenTsFromClick(5_000, marketRange, null), 5_000);
  assert.strictEqual(resolveOpenTsFromClick(12_000, marketRange, null), 10_000);
});

test("resolveOpenTsFromClick clamps marker clicks to event interval", () => {
  const event = makeGapEvent(8_000, 3_000); // marker interval [5000, 8000]
  assert.strictEqual(resolveOpenTsFromClick(4_500, marketRange, event), 5_000);
  assert.strictEqual(resolveOpenTsFromClick(6_200, marketRange, event), 6_200);
  assert.strictEqual(resolveOpenTsFromClick(8_900, marketRange, event), 8_000);
});

test("resolveOpenTsFromClick keeps point events pinned to event ts", () => {
  const point = makeGapEvent(7_250, null);
  assert.strictEqual(resolveOpenTsFromClick(7_100, marketRange, point), 7_250);
  assert.strictEqual(resolveOpenTsFromClick(8_000, marketRange, point), 7_250);
});

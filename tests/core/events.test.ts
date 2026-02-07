import assert from "node:assert/strict";
import { test } from "node:test";
import {
  EventAccumulator,
  EventType,
  createGapTracker,
  recordGap,
  type GapSnapshot,
} from "../../src/core/events.js";

test("EventAccumulator groups successive events and keeps max gap", () => {
  const ctx = { rootId: 1, relativePath: "a/b", collector: "RAM", exchange: "X", symbol: "Y" };
  const acc = new EventAccumulator(ctx);

  acc.record(EventType.PartsShort, 1);
  acc.record(EventType.PartsShort, 2);
  acc.record(EventType.NonFinite, 4);
  acc.record(EventType.Gap, 5, 1000, 12, 111);
  acc.record(EventType.Gap, 6, 500, 8, 222);

  const events = acc.finish();
  assert.strictEqual(events.length, 3);
  assert.deepStrictEqual(events[0], {
    ...ctx,
    type: EventType.PartsShort,
    startLine: 1,
    endLine: 2,
    gapMs: undefined,
    gapMiss: undefined,
    gapEndTs: undefined,
  });
  assert.deepStrictEqual(events[1], {
    ...ctx,
    type: EventType.NonFinite,
    startLine: 4,
    endLine: 4,
    gapMs: undefined,
    gapMiss: undefined,
    gapEndTs: undefined,
  });
  assert.deepStrictEqual(events[2], {
    ...ctx,
    type: EventType.Gap,
    startLine: 5,
    endLine: 6,
    gapMs: 1000,
    gapMiss: 12,
    gapEndTs: 111,
  });
});

test("gap tracker adapts threshold to observed gaps", () => {
  const tracker = createGapTracker();
  const baseTs = 1_000_000;
  const windowMs = 60_000;

  recordGap(tracker, baseTs, windowMs);
  let ts = baseTs;
  for (let i = 0; i < 30; i += 1) {
    ts += 1;
    const gap = recordGap(tracker, ts, windowMs);
    assert.strictEqual(gap, undefined);
  }

  ts += 3;
  assert.strictEqual(recordGap(tracker, ts, windowMs), undefined);

  ts += 20;
  const gap = recordGap(tracker, ts, windowMs);
  assert.strictEqual(gap, undefined);

  ts += 500;
  const bigGap = recordGap(tracker, ts, windowMs);
  assert.ok(bigGap && bigGap.gapMs >= 500);
  assert.ok(bigGap && bigGap.gapMiss >= 400);

  const slowBase = 2_000_000_000_000;
  const slowSnapshot: GapSnapshot = { gapAvgMs: 3_600_000, gapSamples: 50, lastTradeTs: slowBase };
  const slowTracker = createGapTracker(slowSnapshot);
  const skippedGap = recordGap(slowTracker, slowBase + 3 * 3_600_000, windowMs);
  assert.strictEqual(skippedGap, undefined);

  const slowerTracker = createGapTracker(slowSnapshot);
  const slowGap = recordGap(slowerTracker, slowBase + 72 * 3_600_000, windowMs);
  assert.ok(slowGap && slowGap.gapMs >= 72 * 3_600_000);
  assert.ok(slowGap && slowGap.gapMiss >= 71);
});

test("gap tracker uses time-weighted smoothing window", () => {
  const baseTs = 1_000;
  const snapshot: GapSnapshot = { gapAvgMs: 1_000, gapSamples: 10, lastTradeTs: baseTs };
  const fastWindow = 1_000;
  const slowWindow = 10_000;

  const fastTracker = createGapTracker(snapshot);
  const slowTracker = createGapTracker(snapshot);

  recordGap(fastTracker, baseTs + 2_000, fastWindow);
  recordGap(slowTracker, baseTs + 2_000, slowWindow);

  assert.ok(fastTracker.avgGapMs > slowTracker.avgGapMs);
  assert.ok(Math.abs(fastTracker.avgGapMs - 1666.6667) < 0.3);
});

test("gap miss uses current average", () => {
  const baseTs = 10_000;
  const snapshot: GapSnapshot = {
    gapAvgMs: 1_000,
    gapSamples: 10,
    lastTradeTs: baseTs,
  };
  const tracker = createGapTracker(snapshot);
  const windowMs = 60_000;

  const gap = recordGap(tracker, baseTs + 30_000, windowMs);
  assert.ok(gap);
  assert.strictEqual(gap.gapMiss, 29);
});

test("same-timestamp trades reduce effective delta", () => {
  const baseTs = 1_000;
  const snapshot: GapSnapshot = { gapAvgMs: 1_000, gapSamples: 10, lastTradeTs: baseTs };
  const tracker = createGapTracker(snapshot);
  const windowMs = 60_000;

  assert.strictEqual(recordGap(tracker, baseTs, windowMs), undefined);
  const before = tracker.avgGapMs;
  assert.strictEqual(recordGap(tracker, baseTs + 10, windowMs), undefined);
  assert.ok(tracker.avgGapMs < before);
});

test("gap tracker updates avg on detected gaps with capped delta", () => {
  const baseTs = 1_000_000;
  const snapshot: GapSnapshot = { gapAvgMs: 100, gapSamples: 1_000, lastTradeTs: baseTs };
  const tracker = createGapTracker(snapshot);
  const windowMs = 60_000;
  const beforeAvg = tracker.avgGapMs;

  const gap = recordGap(tracker, baseTs + 200_000, windowMs);
  assert.ok(gap);
  assert.ok(tracker.avgGapMs > beforeAvg);
});

test("gap tracker tolerates out-of-order timestamps without resetting samples", () => {
  const tracker = createGapTracker();
  const windowMs = 60_000;
  recordGap(tracker, 1_000, windowMs);
  recordGap(tracker, 1_010, windowMs);

  const beforeSamples = tracker.samples;
  assert.strictEqual(recordGap(tracker, 1_005, windowMs), undefined);
  assert.strictEqual(tracker.lastTradeTs, 1_005);
  assert.strictEqual(tracker.samples, beforeSamples);

  const gap = recordGap(tracker, 1_020, windowMs);
  assert.ok(!gap || gap.gapMs >= 10);
});

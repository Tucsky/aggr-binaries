import assert from "node:assert/strict";
import { test } from "node:test";
import { createGapTracker, recordGap } from "../../src/core/gapTracker.js";
import type { CompanionMetadata } from "../../src/core/model.js";

test("gap tracker initializes from companion snapshot fields", () => {
  const companion: CompanionMetadata = {
    exchange: "BITFINEX",
    symbol: "BTCUSD",
    timeframe: "1m",
    timeframeMs: 60_000,
    startTs: 1_700_000_000_000,
    endTs: 1_700_000_060_000,
    priceScale: 100_000,
    volumeScale: 100_000_000,
    records: 100,
    gapTracker: {
      detectedGapCount: 4,
      detectedGapAvgMs: 1_250,
      startTs: 1_700_000_000_000,
      samples: 2_500,
      lastTradeTs: 1_700_000_060_000,
      sameTsCount: 3,
      emaFastLog: 2,
      emaSlowLog: 3,
      devFastLog: 0.2,
      devSlowLog: 0.3,
      avgGapMs: 900,
    },
  };

  const tracker = createGapTracker(companion);
  assert.strictEqual(tracker.samples, 2_500);
  assert.strictEqual(tracker.lastTradeTs, 1_700_000_060_000);
  assert.strictEqual(tracker.avgGapMs, 900);
  assert.strictEqual(tracker.emaFastLog, 2);
  assert.strictEqual(tracker.emaSlowLog, 3);
});

test("recordGap returns undefined on first sample and initializes tracker start", () => {
  const tracker = createGapTracker();
  assert.strictEqual(recordGap(tracker, 1_000), undefined);
  assert.strictEqual(tracker.startTs, 1_000);
  assert.strictEqual(tracker.lastTradeTs, 1_000);
});

test("recordGap emits a gap after warm-up on a large spike", () => {
  const tracker = createGapTracker();
  let ts = 1_000_000;
  recordGap(tracker, ts);

  // Warm-up with stable cadence so detection has enough samples and elapsed buffer.
  for (let i = 0; i < 2_400; i += 1) {
    ts += 300;
    const gap = recordGap(tracker, ts);
    assert.strictEqual(gap, undefined);
  }

  ts += 60_000;
  const detected = recordGap(tracker, ts);
  assert.ok(detected);
  assert.strictEqual(detected?.gapMs, 60_000);
  assert.strictEqual(detected?.gapEndTs, ts);
  assert.ok((detected?.gapMiss ?? 0) >= 1);
  assert.ok((detected?.gapScore ?? 0) > 1);
});

test("recordGap ignores non-increasing timestamps and keeps sample count stable", () => {
  const tracker = createGapTracker();
  recordGap(tracker, 10_000);
  recordGap(tracker, 10_010);
  const before = tracker.samples;
  const beforeLastTradeTs = tracker.lastTradeTs;

  assert.strictEqual(recordGap(tracker, 10_010), undefined);
  assert.strictEqual(recordGap(tracker, 10_009), undefined);
  assert.strictEqual(tracker.samples, before);
  assert.strictEqual(tracker.lastTradeTs, beforeLastTradeTs);
});

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  resolveChartCrosshairPrice,
  resolveChartCrosshairTarget,
} from "../../client/src/lib/features/viewer/candleChartCrosshair.js";
import type { Candle } from "../../client/src/lib/features/viewer/types.js";

function candle(time: number, close: number, open = close, high = close, low = close): Candle {
  return {
    time,
    open,
    high,
    low,
    close,
    buyVol: 0,
    sellVol: 0,
    buyCount: 0,
    sellCount: 0,
    liqBuy: 0,
    liqSell: 0,
    index: 0,
  };
}

test("resolveChartCrosshairTarget snaps to candle at-or-before requested timestamp", () => {
  const points = [candle(1_000, 10), candle(2_000, 20), candle(3_000, 30)];
  assert.deepStrictEqual(resolveChartCrosshairTarget(points, 2_500), {
    snappedTs: 2_000,
    timeSec: 2,
    price: 20,
  });
  assert.deepStrictEqual(resolveChartCrosshairTarget(points, 3_000), {
    snappedTs: 3_000,
    timeSec: 3,
    price: 30,
  });
  assert.strictEqual(resolveChartCrosshairTarget(points, 500), null);
});

test("resolveChartCrosshairPrice falls back from close to other finite positive prices", () => {
  const withFallback = candle(1_000, 0, 0, 42, 0);
  assert.strictEqual(resolveChartCrosshairPrice(withFallback), 42);
  const missing = candle(1_000, 0, 0, 0, 0);
  assert.strictEqual(resolveChartCrosshairPrice(missing), null);
});

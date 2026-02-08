import assert from "node:assert/strict";
import { test } from "node:test";
import { mapPreviewSeries, toPreviewSeriesPoint } from "../../src/shared/previewSeries.js";

test("maps deterministic volume, delta, and liquidation values", () => {
  const point = toPreviewSeriesPoint({
    time: 1_700_000_000_000,
    open: 100,
    high: 120,
    low: 90,
    close: 110,
    buyVol: 10,
    sellVol: 4,
    liqBuy: 2.5,
    liqSell: 7.25,
  });

  assert.deepStrictEqual(point.price, {
    time: 1_700_000_000,
    open: 100,
    high: 120,
    low: 90,
    close: 110,
  });
  assert.deepStrictEqual(point.totalVolume, {
    time: 1_700_000_000,
    value: 14,
    color: "rgba(59, 202, 109, 0.45)",
  });
  assert.strictEqual(point.volumeDelta.value, 6);
  assert.strictEqual(point.volumeDelta.color, "#3bca6d");
  assert.strictEqual(point.longLiquidation.value, -7.25);
  assert.strictEqual(point.shortLiquidation.value, 2.5);
});

test("volume delta is absolute and colors switch on sell dominance", () => {
  const point = toPreviewSeriesPoint({
    time: 1_700_000_000_000,
    open: 100,
    high: 100,
    low: 100,
    close: 100,
    buyVol: 3,
    sellVol: 10,
    liqBuy: 0,
    liqSell: 0,
  });

  assert.strictEqual(point.volumeDelta.value, 7);
  assert.strictEqual(point.volumeDelta.color, "#d62828");
  assert.strictEqual(point.totalVolume.color, "rgba(214, 40, 40, 0.45)");
});

test("maps gap candles to whitespace while preserving aligned histogram time", () => {
  const point = toPreviewSeriesPoint({
    time: 1_700_000_060_000,
    open: 0,
    high: 0,
    low: 0,
    close: 0,
    buyVol: 0,
    sellVol: 0,
    liqBuy: 0,
    liqSell: 0,
  });

  assert.deepStrictEqual(point.price, { time: 1_700_000_060 });
  assert.strictEqual(point.totalVolume.time, 1_700_000_060);
  assert.strictEqual(point.volumeDelta.time, 1_700_000_060);
  assert.strictEqual(point.longLiquidation.time, 1_700_000_060);
  assert.strictEqual(point.shortLiquidation.time, 1_700_000_060);
});

test("mapPreviewSeries is deterministic for identical inputs", () => {
  const candles = [
    {
      time: 1_700_000_000_000,
      open: 100,
      high: 105,
      low: 95,
      close: 102,
      buyVol: 9,
      sellVol: 8,
      liqBuy: 1,
      liqSell: 2,
    },
    {
      time: 1_700_000_060_000,
      open: 102,
      high: 108,
      low: 101,
      close: 106,
      buyVol: 12,
      sellVol: 3,
      liqBuy: 0.25,
      liqSell: 0.5,
    },
  ];
  const first = mapPreviewSeries(candles);
  const second = mapPreviewSeries(candles);
  assert.deepStrictEqual(second, first);
});

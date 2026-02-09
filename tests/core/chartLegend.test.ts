import assert from "node:assert/strict";
import test from "node:test";
import {
  findCandleAtOrBefore,
  formatLiquidationLegend,
  formatPriceLegend,
  formatVolumeLegend,
  toNonZero,
  type ChartLegendCandle,
} from "../../src/shared/chartLegend.js";

const sample: ChartLegendCandle = {
  time: 1_700_000_000_000,
  open: 10,
  high: 12,
  low: 9,
  close: 11,
  buyVol: 1_200_000,
  sellVol: 300_000,
  liqBuy: 20_000,
  liqSell: 0,
};

test("legend formatters are deterministic", () => {
  assert.equal(formatPriceLegend(sample), "O:10.0 H:12.0 L:9.0 C:11.0");
  assert.equal(formatVolumeLegend(sample), "1.5 M | 900 K");
  assert.equal(formatLiquidationLegend(sample), "na | 20 K");
});

test("findCandleAtOrBefore returns nearest prior candle", () => {
  const points: ChartLegendCandle[] = [
    { ...sample, time: 1000, close: 1 },
    { ...sample, time: 2000, close: 2 },
    { ...sample, time: 3000, close: 3 },
  ];
  assert.equal(findCandleAtOrBefore(points, 2000)?.close, 2);
  assert.equal(findCandleAtOrBefore(points, 2500)?.close, 2);
  assert.equal(findCandleAtOrBefore(points, 999), null);
});

test("toNonZero converts zero values to gaps", () => {
  assert.equal(toNonZero(0), null);
  assert.equal(toNonZero(1), 1);
  assert.equal(toNonZero(-2), -2);
});

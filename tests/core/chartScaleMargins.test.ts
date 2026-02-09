import assert from "node:assert/strict";
import test from "node:test";
import { computeChartScaleMargins } from "../../src/shared/chartScaleMargins.js";

test("all series visible uses requested base margins", () => {
  const margins = computeChartScaleMargins({ price: true, liq: true, volume: true });
  assert.deepEqual(margins.right, { top: 0.04, bottom: 0.26 });
  assert.deepEqual(margins.liq, { top: 0.76, bottom: 0.17 });
  assert.deepEqual(margins.volume, { top: 0.84, bottom: 0 });
});

test("price only takes all available height", () => {
  const margins = computeChartScaleMargins({ price: true, liq: false, volume: false });
  assert.equal(margins.right.top, 0.04);
  assert.equal(margins.right.bottom, 0);
});

test("price + liq redistributes without forcing liq bottom to zero", () => {
  const margins = computeChartScaleMargins({ price: true, liq: true, volume: false });
  assert.equal(margins.liq.bottom, 0.03);
  assert.ok(margins.right.bottom < 0.26);
});

test("price + volume keeps volume pinned to bottom zero", () => {
  const margins = computeChartScaleMargins({ price: true, liq: false, volume: true });
  assert.equal(margins.volume.bottom, 0);
  assert.equal(margins.right.top, 0.04);
});

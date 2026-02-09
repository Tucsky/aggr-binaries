import assert from "node:assert/strict";
import test from "node:test";
import {
  computeAnchoredVisibleRange,
  computeChartInitialSlice,
} from "../../src/shared/chartInitialSlice.js";

test("initial slice keeps latest-right behavior when anchor is last record", () => {
  assert.deepEqual(
    computeChartInitialSlice(999, 1000),
    { fromIndex: 499, toIndex: 999 },
  );
});

test("initial slice anchors to the left when anchor is before last record", () => {
  assert.deepEqual(
    computeChartInitialSlice(200, 1000),
    { fromIndex: 200, toIndex: 700 },
  );
});

test("initial slice clamps anchor and bounds", () => {
  assert.deepEqual(
    computeChartInitialSlice(980, 1000),
    { fromIndex: 980, toIndex: 999 },
  );
  assert.deepEqual(
    computeChartInitialSlice(-20, 1000),
    { fromIndex: 0, toIndex: 500 },
  );
});

test("initial slice returns null for empty datasets", () => {
  assert.equal(computeChartInitialSlice(0, 0), null);
});

test("anchored visible range starts from first loaded point", () => {
  assert.deepEqual(
    computeAnchoredVisibleRange(501, 120.2),
    { from: 0, to: 120 },
  );
  assert.deepEqual(
    computeAnchoredVisibleRange(40, 120.2),
    { from: 0, to: 39 },
  );
});

test("anchored visible range falls back to default span", () => {
  assert.deepEqual(
    computeAnchoredVisibleRange(501, null),
    { from: 0, to: 120 },
  );
  assert.equal(computeAnchoredVisibleRange(0, 120), null);
});

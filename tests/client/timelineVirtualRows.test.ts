import assert from "node:assert/strict";
import { test } from "node:test";
import { computeTimelineVirtualWindow } from "../../client/src/lib/features/timeline/timelineVirtualRows.js";

test("computeTimelineVirtualWindow returns visible window with overscan", () => {
  const window = computeTimelineVirtualWindow(3_500, 33 * 100, 660, 33, 8);
  assert.deepStrictEqual(window, {
    startIndex: 92,
    endIndex: 128,
    topPadding: 3_036,
    bottomPadding: 111_276,
  });
});

test("computeTimelineVirtualWindow clamps indices at boundaries", () => {
  const startWindow = computeTimelineVirtualWindow(3_500, 0, 660, 33, 8);
  assert.deepStrictEqual(startWindow, {
    startIndex: 0,
    endIndex: 28,
    topPadding: 0,
    bottomPadding: 114_576,
  });

  const endWindow = computeTimelineVirtualWindow(3_500, 33 * 3_490, 660, 33, 8);
  assert.deepStrictEqual(endWindow, {
    startIndex: 3_482,
    endIndex: 3_500,
    topPadding: 114_906,
    bottomPadding: 0,
  });
});

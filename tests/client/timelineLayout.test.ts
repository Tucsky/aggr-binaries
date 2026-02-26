import assert from "node:assert/strict";
import { test } from "node:test";
import {
  clampTimelineTitleWidth,
  computeTimelineViewportWidth,
  MIN_TIMELINE_TITLE_WIDTH,
  MIN_TIMELINE_VIEWPORT_WIDTH,
} from "../../client/src/lib/features/timeline/timelineLayout.js";

test("clampTimelineTitleWidth enforces lower and upper bounds", () => {
  const containerWidth = 1_100;
  assert.strictEqual(clampTimelineTitleWidth(40, containerWidth), MIN_TIMELINE_TITLE_WIDTH);
  assert.strictEqual(clampTimelineTitleWidth(1_000, containerWidth), 780);
});

test("computeTimelineViewportWidth keeps minimum viewport width", () => {
  assert.strictEqual(
    computeTimelineViewportWidth(360, MIN_TIMELINE_TITLE_WIDTH),
    MIN_TIMELINE_VIEWPORT_WIDTH,
  );
  assert.strictEqual(computeTimelineViewportWidth(1_300, 300), 1_000);
});

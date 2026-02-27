import assert from "node:assert/strict";
import { test } from "node:test";
import {
  computeTimelinePanDeltaMsFromPointer,
  computeTimelinePanDeltaMsFromWheel,
  resolveTimelineSurfaceCoordinates,
  resolveTimelineSurfaceX,
} from "../../client/src/lib/features/timeline/timelineSurfaceInteraction.js";
import type { TimelineRange } from "../../client/src/lib/features/timeline/timelineUtils.js";

test("resolveTimelineSurfaceX only accepts x values inside the timeline viewport", () => {
  assert.strictEqual(resolveTimelineSurfaceX(500, 100, 150, 500), 250);
  assert.strictEqual(resolveTimelineSurfaceX(249, 100, 150, 500), null);
  assert.strictEqual(resolveTimelineSurfaceX(751, 100, 150, 500), null);
});

test("resolveTimelineSurfaceCoordinates maps pointer x to timestamp deterministically", () => {
  const viewRange: TimelineRange = { startTs: 1_000, endTs: 2_000 };
  assert.deepStrictEqual(
    resolveTimelineSurfaceCoordinates(500, 100, 150, 500, viewRange),
    { x: 250, ts: 1_500 },
  );
});

test("timeline surface pan delta math keeps pointer and wheel signs stable", () => {
  const viewRange: TimelineRange = { startTs: 1_000, endTs: 2_000 };
  assert.strictEqual(computeTimelinePanDeltaMsFromPointer(25, viewRange, 500), -50);
  assert.strictEqual(computeTimelinePanDeltaMsFromWheel(25, viewRange, 500), 50);
});

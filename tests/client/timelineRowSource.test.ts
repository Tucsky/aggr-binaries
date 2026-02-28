import assert from "node:assert/strict";
import { test } from "node:test";
import { resolveTimelineSourceRect } from "../../client/src/lib/features/timeline/timelineRowSource.js";
import type { TimelineRange } from "../../client/src/lib/features/timeline/timelineUtils.js";

const viewRange: TimelineRange = { startTs: 100, endTs: 200 };

test("resolveTimelineSourceRect clamps visible bounds and returns deterministic geometry", () => {
  const rect = resolveTimelineSourceRect(50, 150, viewRange, 1000, 40);
  assert.ok(rect);
  assert.deepStrictEqual(rect, {
    x1: 0,
    x2: 500,
    y: 10,
    height: 20,
    roundLeft: false,
    roundRight: true,
  });
});

test("resolveTimelineSourceRect returns null when source is not visible", () => {
  assert.strictEqual(resolveTimelineSourceRect(10, 90, viewRange, 1000, 40), null);
  assert.strictEqual(resolveTimelineSourceRect(210, 260, viewRange, 1000, 40), null);
});

test("resolveTimelineSourceRect rounds ends based on viewport clipping", () => {
  const rect = resolveTimelineSourceRect(120, 220, viewRange, 1000, 40);
  assert.ok(rect);
  assert.strictEqual(rect?.x1, 200);
  assert.strictEqual(rect?.x2, 1000);
  assert.strictEqual(rect?.roundLeft, true);
  assert.strictEqual(rect?.roundRight, false);
});

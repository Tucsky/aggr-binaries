import assert from "node:assert/strict";
import { test } from "node:test";
import { resolveTimelineHighlightWindow } from "../../client/src/lib/features/timeline/timelineRowCanvas.js";
import type { TimelineRange } from "../../client/src/lib/features/timeline/timelineUtils.js";

test("resolveTimelineHighlightWindow maps clamped highlight range into view coordinates", () => {
  const highlight: TimelineRange = { startTs: 900, endTs: 2_100 };
  const view: TimelineRange = { startTs: 1_000, endTs: 2_000 };
  const window = resolveTimelineHighlightWindow(highlight, view, 1_000);
  assert.deepStrictEqual(window, { x1: 0, x2: 1_000 });
});

test("resolveTimelineHighlightWindow returns null when highlight is disjoint", () => {
  const highlight: TimelineRange = { startTs: 2_100, endTs: 2_300 };
  const view: TimelineRange = { startTs: 1_000, endTs: 2_000 };
  assert.strictEqual(resolveTimelineHighlightWindow(highlight, view, 1_000), null);
});

test("resolveTimelineHighlightWindow respects partial overlap and non-positive width clamps", () => {
  const highlight: TimelineRange = { startTs: 1_500, endTs: 2_500 };
  const view: TimelineRange = { startTs: 1_000, endTs: 2_000 };
  const window = resolveTimelineHighlightWindow(highlight, view, 0);
  assert.deepStrictEqual(window, { x1: 0.5, x2: 1 });
});

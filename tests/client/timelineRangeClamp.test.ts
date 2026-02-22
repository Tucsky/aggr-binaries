import assert from "node:assert/strict";
import { test } from "node:test";
import { shiftViewRangeIntoRangeIfDisjoint } from "../../client/src/lib/features/timeline/timelineViewport.js";
import type { TimelineRange } from "../../client/src/lib/features/timeline/timelineUtils.js";

test("shiftViewRangeIntoRangeIfDisjoint keeps overlapping views unchanged", () => {
  const range: TimelineRange = { startTs: 1000, endTs: 2000 };
  const view: TimelineRange = { startTs: 900, endTs: 1200 };
  assert.deepStrictEqual(shiftViewRangeIntoRangeIfDisjoint(range, view), view);
});

test("shiftViewRangeIntoRangeIfDisjoint moves disjoint-left views to range start", () => {
  const range: TimelineRange = { startTs: 1000, endTs: 2000 };
  const view: TimelineRange = { startTs: 0, endTs: 600 };
  assert.deepStrictEqual(shiftViewRangeIntoRangeIfDisjoint(range, view), {
    startTs: 1000,
    endTs: 1600,
  });
});

test("shiftViewRangeIntoRangeIfDisjoint moves disjoint-right views to range end", () => {
  const range: TimelineRange = { startTs: 1000, endTs: 2000 };
  const view: TimelineRange = { startTs: 2600, endTs: 2900 };
  assert.deepStrictEqual(shiftViewRangeIntoRangeIfDisjoint(range, view), {
    startTs: 1700,
    endTs: 2000,
  });
});

test("shiftViewRangeIntoRangeIfDisjoint clamps to full range when view span is wider", () => {
  const range: TimelineRange = { startTs: 1000, endTs: 2000 };
  const view: TimelineRange = { startTs: -9000, endTs: -1000 };
  assert.deepStrictEqual(shiftViewRangeIntoRangeIfDisjoint(range, view), range);
});

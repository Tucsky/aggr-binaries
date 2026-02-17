import assert from "node:assert/strict";
import { test } from "node:test";
import {
  buildInitialViewRange,
  panTimelineRange,
  zoomTimelineRange,
} from "../../client/src/lib/timelineViewport.js";
import type { TimelineRange } from "../../client/src/lib/timelineUtils.js";

test("buildInitialViewRange anchors to latest end when full span exceeds default window", () => {
  const range: TimelineRange = { startTs: 0, endTs: 2_000 };
  assert.deepStrictEqual(buildInitialViewRange(range, 500), {
    startTs: 1_500,
    endTs: 2_000,
  });
});

test("zoomTimelineRange expands and clamps to selected range", () => {
  const selected: TimelineRange = { startTs: 0, endTs: 10_000 };
  const current: TimelineRange = { startTs: 4_000, endTs: 6_000 };
  const zoomedOut = zoomTimelineRange(selected, current, 5_000, 2_000);
  assert.ok(zoomedOut.startTs <= current.startTs);
  assert.ok(zoomedOut.endTs >= current.endTs);
  assert.ok(zoomedOut.startTs >= selected.startTs);
  assert.ok(zoomedOut.endTs <= selected.endTs);
});

test("zoomTimelineRange responds to sensitivity", () => {
  const selected: TimelineRange = { startTs: 0, endTs: 10_000 };
  const current: TimelineRange = { startTs: 4_000, endTs: 6_000 };
  const lowSensitivity = zoomTimelineRange(selected, current, 5_000, 100, 1, 0.001);
  const highSensitivity = zoomTimelineRange(selected, current, 5_000, 100, 1, 0.008);
  const lowSpan = lowSensitivity.endTs - lowSensitivity.startTs;
  const highSpan = highSensitivity.endTs - highSensitivity.startTs;
  assert.ok(highSpan > lowSpan);
});

test("panTimelineRange clamps at left and right bounds", () => {
  const selected: TimelineRange = { startTs: 1_000, endTs: 5_000 };
  const current: TimelineRange = { startTs: 2_000, endTs: 4_000 };
  assert.deepStrictEqual(panTimelineRange(selected, current, -5_000), {
    startTs: 1_000,
    endTs: 3_000,
  });
  assert.deepStrictEqual(panTimelineRange(selected, current, 5_000), {
    startTs: 3_000,
    endTs: 5_000,
  });
});

test("panTimelineRange allows bounded overscroll when ratio is set", () => {
  const selected: TimelineRange = { startTs: 1_000, endTs: 5_000 };
  const current: TimelineRange = { startTs: 2_000, endTs: 4_000 };
  assert.deepStrictEqual(panTimelineRange(selected, current, -5_000, 0.1), {
    startTs: 800,
    endTs: 2_800,
  });
  assert.deepStrictEqual(panTimelineRange(selected, current, 5_000, 0.1), {
    startTs: 3_200,
    endTs: 5_200,
  });
});

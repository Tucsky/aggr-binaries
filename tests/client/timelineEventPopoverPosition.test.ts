import assert from "node:assert/strict";
import { test } from "node:test";
import { computeTimelinePopoverPlacement } from "../../client/src/lib/timelineEventPopoverPosition.js";

test("computeTimelinePopoverPlacement follows pointer movement on the same marker", () => {
  const base = {
    markerLeftClientX: 400,
    markerRightClientX: 440,
    markerTopClientY: 200,
    markerBottomClientY: 216,
    popoverWidth: 240,
    popoverHeight: 120,
    viewportWidth: 1280,
    viewportHeight: 720,
  };

  const first = computeTimelinePopoverPlacement({
    ...base,
    pointerClientX: 390,
    pointerClientY: 210,
  });
  const second = computeTimelinePopoverPlacement({
    ...base,
    pointerClientX: 410,
    pointerClientY: 210,
  });
  assert.ok(second.left > first.left);
});

test("computeTimelinePopoverPlacement prefers right and below when viewport allows", () => {
  const placement = computeTimelinePopoverPlacement({
    pointerClientX: 400,
    pointerClientY: 200,
    markerLeftClientX: 390,
    markerRightClientX: 410,
    markerTopClientY: 192,
    markerBottomClientY: 208,
    popoverWidth: 220,
    popoverHeight: 120,
    viewportWidth: 1200,
    viewportHeight: 800,
  });

  assert.equal(placement.placeLeft, false);
  assert.equal(placement.placeAbove, false);
  assert.ok(placement.left > 400);
  assert.ok(placement.top > 200);
});

test("computeTimelinePopoverPlacement flips to the left when space on the right is insufficient", () => {
  const placement = computeTimelinePopoverPlacement({
    pointerClientX: 980,
    pointerClientY: 260,
    markerLeftClientX: 940,
    markerRightClientX: 980,
    markerTopClientY: 240,
    markerBottomClientY: 256,
    popoverWidth: 240,
    popoverHeight: 120,
    viewportWidth: 1024,
    viewportHeight: 768,
  });

  assert.ok(placement.placeLeft);
  assert.ok(placement.left < 980);
});

test("computeTimelinePopoverPlacement flips above when space below is insufficient", () => {
  const placement = computeTimelinePopoverPlacement({
    pointerClientX: 320,
    pointerClientY: 740,
    markerLeftClientX: 300,
    markerRightClientX: 340,
    markerTopClientY: 700,
    markerBottomClientY: 716,
    popoverWidth: 260,
    popoverHeight: 140,
    viewportWidth: 1200,
    viewportHeight: 768,
  });

  assert.ok(placement.placeAbove);
  assert.ok(placement.top < 740);
});

test("computeTimelinePopoverPlacement always clamps inside viewport margins", () => {
  const placement = computeTimelinePopoverPlacement({
    pointerClientX: 2,
    pointerClientY: 4,
    markerLeftClientX: 0,
    markerRightClientX: 5,
    markerTopClientY: 0,
    markerBottomClientY: 8,
    popoverWidth: 460,
    popoverHeight: 250,
    viewportWidth: 480,
    viewportHeight: 260,
  });

  assert.ok(placement.left >= 10);
  assert.ok(placement.top >= 10);
  assert.ok(placement.left <= 10);
  assert.ok(placement.top <= 10);
});

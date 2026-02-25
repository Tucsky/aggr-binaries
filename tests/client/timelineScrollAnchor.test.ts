import assert from "node:assert/strict";
import { test } from "node:test";
import {
  captureTimelineScrollAnchor,
  resolveTimelineRestoredScrollTop,
} from "../../client/src/lib/features/timeline/timelineScrollAnchor.js";

test("restores to the same anchored row after rows are inserted above", () => {
  const rowHeight = 33;
  const before = ["A", "B", "C", "D"];
  const after = ["X", "A", "B", "C", "D"];
  const anchor = captureTimelineScrollAnchor(before, rowHeight * 2 + 5, rowHeight, (row) => row);
  const restored = resolveTimelineRestoredScrollTop(after, anchor, rowHeight, 10_000, (row) => row);
  assert.strictEqual(restored, rowHeight * 3 + 5);
});

test("falls back to previous scrollTop when anchored row is gone", () => {
  const rowHeight = 33;
  const before = ["A", "B", "C", "D"];
  const after = ["A", "B", "D"];
  const anchor = captureTimelineScrollAnchor(before, rowHeight * 2 + 4, rowHeight, (row) => row);
  const restored = resolveTimelineRestoredScrollTop(after, anchor, rowHeight, 10_000, (row) => row);
  assert.strictEqual(restored, rowHeight * 2 + 4);
});

test("clamps restored scrollTop to the container max", () => {
  const rowHeight = 33;
  const rows = ["A", "B", "C", "D"];
  const anchor = captureTimelineScrollAnchor(rows, rowHeight * 3 + 20, rowHeight, (row) => row);
  const restored = resolveTimelineRestoredScrollTop(rows, anchor, rowHeight, 80, (row) => row);
  assert.strictEqual(restored, 80);
});

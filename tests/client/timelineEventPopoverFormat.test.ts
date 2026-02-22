import assert from "node:assert/strict";
import { test } from "node:test";
import { formatElapsedDhms, formatEstimatedMiss } from "../../client/src/lib/timelineEventPopoverFormat.js";

test("formatElapsedDhms includes days and hh:mm:ss", () => {
  assert.equal(formatElapsedDhms(null), "0s");
  assert.equal(formatElapsedDhms(999), "0s");
  assert.equal(formatElapsedDhms(1_000), "1s");
  assert.equal(formatElapsedDhms(3_661_000), "1h 1m 1s");
  assert.equal(formatElapsedDhms(86_415_000), "1d 15s");
  assert.equal(formatElapsedDhms(90_061_000), "1d 1h 1m 1s");
});

test("formatEstimatedMiss emits compact k/M (2x)", () => {
  assert.equal(formatEstimatedMiss(null), "n/a");
  assert.equal(formatEstimatedMiss(0), "~0");
  assert.equal(formatEstimatedMiss(980), "~2k");
  assert.equal(formatEstimatedMiss(1_500), "~3k");
  assert.equal(formatEstimatedMiss(2_500_000), "~5m");
});

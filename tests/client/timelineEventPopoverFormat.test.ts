import assert from "node:assert/strict";
import { test } from "node:test";
import { formatElapsedDhms, formatEstimatedMissRange } from "../../client/src/lib/timelineEventPopoverFormat.js";

test("formatElapsedDhms includes days and hh:mm:ss", () => {
  assert.equal(formatElapsedDhms(null), "0s");
  assert.equal(formatElapsedDhms(999), "0s");
  assert.equal(formatElapsedDhms(1_000), "1s");
  assert.equal(formatElapsedDhms(3_661_000), "1h 1m 1s");
  assert.equal(formatElapsedDhms(86_415_000), "1d 15s");
  assert.equal(formatElapsedDhms(90_061_000), "1d 1h 1m 1s");
});

test("formatEstimatedMissRange emits compact k/M range from base to 2x", () => {
  assert.equal(formatEstimatedMissRange(null), "n/a");
  assert.equal(formatEstimatedMissRange(0), "0-0");
  assert.equal(formatEstimatedMissRange(980), "980-2k");
  assert.equal(formatEstimatedMissRange(1_500), "1.5k-3k");
  assert.equal(formatEstimatedMissRange(2_500_000), "2.5M-5M");
});

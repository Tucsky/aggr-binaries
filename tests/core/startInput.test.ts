import assert from "node:assert/strict";
import test from "node:test";
import {
  formatStartInputUtc,
  normalizeStartInput,
  parseStartInputUtcMs,
} from "../../src/shared/startInput.js";

test("start input parser accepts DMY values and defaults missing time to midnight UTC", () => {
  assert.equal(
    parseStartInputUtcMs("12/12/2022, --:--"),
    Date.UTC(2022, 11, 12, 0, 0, 0, 0),
  );
  assert.equal(
    parseStartInputUtcMs("12/12/2022, --:30"),
    Date.UTC(2022, 11, 12, 0, 30, 0, 0),
  );
  assert.equal(
    parseStartInputUtcMs("11/09/2022, 10:00"),
    Date.UTC(2022, 8, 11, 10, 0, 0, 0),
  );
});

test("start input parser treats ISO values without zone as UTC", () => {
  assert.equal(
    parseStartInputUtcMs("2022-09-11T10:00"),
    Date.UTC(2022, 8, 11, 10, 0, 0, 0),
  );
  assert.equal(
    parseStartInputUtcMs("2022-09-11 10:00"),
    Date.UTC(2022, 8, 11, 10, 0, 0, 0),
  );
});

test("start input parser supports explicit ISO timezone offsets", () => {
  assert.equal(
    parseStartInputUtcMs("2022-09-11T10:00:30+02:00"),
    Date.UTC(2022, 8, 11, 8, 0, 30, 0),
  );
  assert.equal(
    parseStartInputUtcMs("2022-09-11T10:00:30-0130"),
    Date.UTC(2022, 8, 11, 11, 30, 30, 0),
  );
});

test("start input parser accepts unix timestamps in seconds and milliseconds", () => {
  const ts = Date.UTC(2022, 8, 11, 10, 0, 0, 0);
  assert.equal(parseStartInputUtcMs("1662890400"), ts);
  assert.equal(parseStartInputUtcMs("1662890400000"), ts);
});

test("start input formatter and normalizer are deterministic", () => {
  const ts = Date.UTC(2022, 8, 11, 10, 0, 0, 0);
  assert.equal(formatStartInputUtc(ts), "11/09/2022, 10:00");
  assert.equal(normalizeStartInput("2022-09-11T10:00:00.000Z"), "11/09/2022, 10:00");
  assert.equal(normalizeStartInput("1662890400"), "11/09/2022, 10:00");
});

test("start input parser rejects invalid dates", () => {
  assert.equal(parseStartInputUtcMs("31/02/2022, 10:00"), null);
  assert.equal(parseStartInputUtcMs("2022-13-11T10:00"), null);
  assert.equal(parseStartInputUtcMs("not-a-date"), null);
});

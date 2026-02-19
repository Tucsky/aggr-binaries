import assert from "node:assert/strict";
import { test } from "node:test";
import {
  filterTimeframesByQuery,
  type TimeframeEntry,
} from "../../client/src/lib/timeframeDropdownUtils.js";

test("filterTimeframesByQuery returns original list for empty query", () => {
  const list: TimeframeEntry[] = [
    { value: "15s", ms: 15_000 },
    { value: "1m", ms: 60_000 },
  ];
  const filtered = filterTimeframesByQuery(list, "  ");
  assert.strictEqual(filtered, list);
});

test("filterTimeframesByQuery matches case-insensitive substring and preserves order", () => {
  const list: TimeframeEntry[] = [
    { value: "15s", ms: 15_000 },
    { value: "1m", ms: 60_000 },
    { value: "15m", ms: 900_000 },
    { value: "1h", ms: 3_600_000 },
  ];
  const filtered = filterTimeframesByQuery(list, "M");
  assert.deepStrictEqual(filtered, [list[1], list[2]]);
});

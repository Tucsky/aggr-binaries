import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { extractGapWindows } from "../../src/core/gaps/extract.js";
import type { GapFixEventRow } from "../../src/core/gaps/queue.js";

const TS0 = 1_704_067_200_000;
const TS2 = TS0 + 120_000;

test("extractGapWindows ignores liquidation rows when resolving line-based windows", async () => {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-fixextract-"));
  const filePath = path.join(baseDir, "trades.txt");
  await fs.writeFile(
    filePath,
    [
      `${TS0} 100 1 1 0`,
      `${TS0 + 30_000} 99 0.5 0 1`,
      `${TS0 + 40_000} 98 0.25 1 1`,
      `${TS2} 102 1 0 0`,
    ].join("\n"),
  );

  const rows: GapFixEventRow[] = [
    {
      id: 1,
      root_id: 1,
      root_path: baseDir,
      relative_path: "trades.txt",
      collector: "RAM",
      exchange: "BITFINEX",
      symbol: "BTCUSD",
      start_line: 4,
      end_line: 4,
      gap_ms: TS2 - TS0,
      gap_miss: 1,
      gap_end_ts: TS2,
      gap_fix_status: null,
    },
  ];

  const extracted = await extractGapWindows(filePath, rows);
  assert.deepStrictEqual(extracted.unresolvedEventIds, []);
  assert.deepStrictEqual(extracted.windows, [{ eventId: 1, fromTs: TS0, toTs: TS2 }]);
});

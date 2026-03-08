import assert from "node:assert/strict";
import { test } from "node:test";
import type { GapFixQueueRow } from "../../src/core/db.js";
import { resolveFlushTargetFile } from "../../src/core/gaps/flushBatchTarget.js";

const DAY_MS = 86_400_000;
const ROOT_PATH = "/tmp/aggr-root";

function buildGapRow(overrides: Partial<GapFixQueueRow>): GapFixQueueRow {
  return {
    id: 1,
    start_relative_path: "PI/2025/KRAKEN/XBT-USD/2025-03-12-12.gz",
    end_relative_path: "PI/2025/KRAKEN/XBT-USD/2025-04-02-20.gz",
    collector: "PI",
    exchange: "KRAKEN",
    symbol: "XBT-USD",
    gap_ms: null,
    gap_miss: null,
    start_ts: Date.UTC(2025, 2, 12, 13, 14, 27, 846),
    end_ts: Date.UTC(2025, 3, 2, 20, 12, 48, 142),
    gap_fix_status: null,
    gap_score: null,
    ...overrides,
  };
}

test("resolveFlushTargetFile routes wide early chunk to start file when chunk begins near gap start", () => {
  const row = buildGapRow({});
  const target = resolveFlushTargetFile(
    ROOT_PATH,
    row,
    row.start_ts + 60_000,
    row.start_ts + (4 * DAY_MS),
  );

  assert.strictEqual(target.relativePath, row.start_relative_path);
});

test("resolveFlushTargetFile routes middle chunk to deterministic intermediate 4h file", () => {
  const row = buildGapRow({});
  const firstTradeTs = Date.UTC(2025, 2, 20, 14, 37, 0, 0);
  const target = resolveFlushTargetFile(
    ROOT_PATH,
    row,
    firstTradeTs,
    firstTradeTs + (2 * 60_000),
  );

  assert.strictEqual(target.relativePath, "PI/2025/KRAKEN/XBT-USD/2025-03-20-12.gz");
});

test("resolveFlushTargetFile keeps end file routing for near-end chunks", () => {
  const row = buildGapRow({});
  const firstTradeTs = row.end_ts - DAY_MS + 1;
  const target = resolveFlushTargetFile(
    ROOT_PATH,
    row,
    firstTradeTs,
    row.end_ts - 1,
  );

  assert.strictEqual(target.relativePath, row.end_relative_path);
});

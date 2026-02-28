import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Db } from "../../src/core/db.js";
import { openDatabase } from "../../src/core/db.js";
import { EventType, GapFixStatus } from "../../src/core/events.js";

function insertGapEvent(
  db: Db,
  payload: {
    rootId: number;
    relativePath: string;
    collector: string;
    exchange: string;
    symbol: string;
    startLine: number;
    gapEndTs: number;
  },
): void {
  db.insertEvents([
    {
      rootId: payload.rootId,
      relativePath: payload.relativePath,
      collector: payload.collector,
      exchange: payload.exchange,
      symbol: payload.symbol,
      type: EventType.Gap,
      startLine: payload.startLine,
      endLine: payload.startLine,
      gapMs: 60_000,
      gapMiss: 1,
      gapEndTs: payload.gapEndTs,
    },
  ]);
}

test("fixgaps queue stays symbol-local across page boundaries while statuses update", async () => {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-fixgaps-queue-order-"));
  const dbPath = path.join(baseDir, "index.sqlite");
  const db = openDatabase(dbPath);

  try {
    const collector = "RAM";
    const exchange = "BITMEX";
    const rootId = db.ensureRoot(path.join(baseDir, "input"));
    const dogeUsdBucket2021 = "RAM/2021-2022/BITMEX/DOGEUSD/2022-08-13-00.gz";
    const dogeUsdBucket2022 = "RAM/2022/BITMEX/DOGEUSD/2022-08-15-00.gz";
    const dogeUsdtBucket2021 = "RAM/2021-2022/BITMEX/DOGEUSDT/2021-07-14-00.gz";

    // 1024 rows force a second keyset page; the remaining two rows assert cross-bucket symbol ordering.
    for (let i = 1; i <= 1024; i += 1) {
      insertGapEvent(db, {
        rootId,
        relativePath: dogeUsdBucket2021,
        collector,
        exchange,
        symbol: "DOGEUSD",
        startLine: i,
        gapEndTs: 1_700_000_000_000 + i,
      });
    }
    insertGapEvent(db, {
      rootId,
      relativePath: dogeUsdBucket2022,
      collector,
      exchange,
      symbol: "DOGEUSD",
      startLine: 1,
      gapEndTs: 1_700_100_000_000,
    });
    insertGapEvent(db, {
      rootId,
      relativePath: dogeUsdtBucket2021,
      collector,
      exchange,
      symbol: "DOGEUSDT",
      startLine: 1,
      gapEndTs: 1_700_200_000_000,
    });

    const seen: Array<{ id: number; symbol: string; relativePath: string; startLine: number }> = [];
    for (const row of db.iterateGapEventsForFix({ collector, exchange })) {
      seen.push({
        id: row.id,
        symbol: row.symbol,
        relativePath: row.relative_path,
        startLine: row.start_line,
      });
      db.updateGapFixStatus([{ id: row.id, status: GapFixStatus.Fixed, error: null, recovered: 0 }]);
    }

    assert.strictEqual(seen.length, 1026);
    assert.deepStrictEqual(
      seen.slice(1023).map((row) => `${row.symbol}|${row.relativePath}|${row.startLine}`),
      [
        `DOGEUSD|${dogeUsdBucket2021}|1024`,
        `DOGEUSD|${dogeUsdBucket2022}|1`,
        `DOGEUSDT|${dogeUsdtBucket2021}|1`,
      ],
    );

    const pending = db.db
      .prepare(
        `SELECT COUNT(*) AS cnt
           FROM events
          WHERE event_type = 'gap'
            AND collector = :collector
            AND exchange = :exchange
            AND gap_fix_status IS NULL;`,
      )
      .get({ collector, exchange }) as { cnt?: number };
    assert.strictEqual(Number(pending.cnt ?? 0), 0);
  } finally {
    db.close();
  }
});

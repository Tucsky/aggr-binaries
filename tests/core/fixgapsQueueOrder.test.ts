import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Db } from "../../src/core/db.js";
import { openDatabase } from "../../src/core/db.js";
import { GapFixStatus } from "../../src/core/model.js";

function insertGapEvent(
  db: Db,
  payload: {
    rootId: number;
    relativePath: string;
    collector: string;
    exchange: string;
    symbol: string;
    gapEndTs: number;
  },
): void {
  db.insertGaps(
    {
      rootId: payload.rootId,
      relativePath: payload.relativePath,
      collector: payload.collector,
      exchange: payload.exchange,
      symbol: payload.symbol,
    },
    [{
      gapMs: 60_000,
      gapMiss: 1,
      gapEndTs: payload.gapEndTs,
    }],
  );
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
        gapEndTs: 1_700_000_000_000 + i,
      });
    }
    insertGapEvent(db, {
      rootId,
      relativePath: dogeUsdBucket2022,
      collector,
      exchange,
      symbol: "DOGEUSD",
      gapEndTs: 1_700_100_000_000,
    });
    insertGapEvent(db, {
      rootId,
      relativePath: dogeUsdtBucket2021,
      collector,
      exchange,
      symbol: "DOGEUSDT",
      gapEndTs: 1_700_200_000_000,
    });

    const seen: Array<{ id: number; symbol: string; relativePath: string; gapEndTs: number | null }> = [];
    for (const row of db.iterateGapsForFix({ collector, exchange })) {
      seen.push({
        id: row.id,
        symbol: row.symbol,
        relativePath: row.relative_path,
        gapEndTs: row.gap_end_ts,
      });
      db.updateGapFixStatus([{ id: row.id, status: GapFixStatus.Fixed, error: null, recovered: 0 }]);
    }

    assert.strictEqual(seen.length, 1026);
    assert.deepStrictEqual(
      seen.slice(1023).map((row) => `${row.symbol}|${row.relativePath}|${row.gapEndTs}`),
      [
        `DOGEUSD|${dogeUsdBucket2021}|1700000001024`,
        `DOGEUSD|${dogeUsdBucket2022}|1700100000000`,
        `DOGEUSDT|${dogeUsdtBucket2021}|1700200000000`,
      ],
    );

    const pending = db.db
      .prepare(
        `SELECT COUNT(*) AS cnt
           FROM gaps
          WHERE collector = :collector
            AND exchange = :exchange
            AND gap_fix_status IS NULL;`,
      )
      .get({ collector, exchange }) as { cnt?: number };
    assert.strictEqual(Number(pending.cnt ?? 0), 0);
  } finally {
    db.close();
  }
});

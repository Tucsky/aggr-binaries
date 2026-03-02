import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Config } from "../../src/core/config.js";
import { parseTimeframeMs } from "../../src/core/config.js";
import type { Db } from "../../src/core/db.js";
import { openDatabase } from "../../src/core/db.js";
import { GapFixStatus } from "../../src/core/model.js";
import { createAdapterRegistry } from "../../src/core/gaps/adapters/index.js";
import { runFixGaps } from "../../src/core/gaps/index.js";
import { classifyPath } from "../../src/core/normalize.js";

const MARKET = {
  collector: "RAM",
  bucket: "bucketA",
  exchange: "BITMEX",
  symbol: "AXSUSD",
};

const TS0 = 1_704_067_200_000;
const TS2 = TS0 + 120_000;
const TS4 = TS0 + 240_000;

interface Fixture {
  root: string;
  outDir: string;
  dbPath: string;
  fileDir: string;
}

async function createFixture(): Promise<Fixture> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-fixgaps-queue-lock-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const dbPath = path.join(baseDir, "index.sqlite");
  const fileDir = path.join(root, MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol);
  await fs.mkdir(fileDir, { recursive: true });
  return { root, outDir, dbPath, fileDir };
}

function buildConfig(root: string, outDir: string, dbPath: string, timeframe: string): Config {
  const timeframeMs = parseTimeframeMs(timeframe);
  if (!timeframeMs) throw new Error(`Invalid timeframe ${timeframe}`);
  return {
    root,
    dbPath,
    outDir,
    batchSize: 100,
    flushIntervalSeconds: 1,
    collector: MARKET.collector,
    exchange: MARKET.exchange,
    symbol: MARKET.symbol,
    timeframe,
    timeframeMs,
  };
}

async function createInputFile(fixture: Fixture, fileName: string): Promise<string> {
  const fullPath = path.join(fixture.fileDir, fileName);
  await fs.writeFile(fullPath, `${TS0} 100 1 1 0\n${TS2} 102 1 0 0\n`);
  return path.posix.join(MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol, fileName);
}

function insertIndexedFile(db: Db, root: string, relativePath: string): number {
  const rootId = db.ensureRoot(root);
  const row = classifyPath(rootId, relativePath);
  if (!row) {
    throw new Error(`Failed to classify ${relativePath}`);
  }
  db.insertFiles([row]);
  return rootId;
}

function insertGapEvent(
  db: Db,
  payload: {
    rootId: number;
    relativePath: string;
    gapMs: number;
    gapEndTs: number;
  },
): void {
  db.db
    .prepare(
      `INSERT INTO gaps
        (root_id, relative_path, collector, exchange, symbol, gap_ms, gap_miss, gap_end_ts, gap_fix_status, gap_score)
       VALUES
        (:rootId, :relativePath, :collector, :exchange, :symbol, :gapMs, :gapMiss, :gapEndTs, NULL, NULL);`,
    )
    .run({
      rootId: payload.rootId,
      relativePath: payload.relativePath,
      collector: MARKET.collector,
      exchange: MARKET.exchange,
      symbol: MARKET.symbol,
      gapMs: payload.gapMs,
      gapMiss: 1,
      gapEndTs: payload.gapEndTs,
    });
}

function readGapStatuses(db: Db): string[] {
  const rows = db.db
    .prepare("SELECT gap_fix_status AS status FROM gaps ORDER BY id;")
    .all() as Array<{ status: string | null }>;
  return rows.map((row) => row.status ?? "");
}

test("fixgaps continues when another connection writes during queue processing", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);

  try {
    const firstRelativePath = await createInputFile(fixture, "2024-01-01-00");
    const secondRelativePath = await createInputFile(fixture, "2024-01-01-01");
    const rootId = insertIndexedFile(db, fixture.root, firstRelativePath);
    const secondRootId = insertIndexedFile(db, fixture.root, secondRelativePath);
    assert.strictEqual(secondRootId, rootId);

    insertGapEvent(db, {
      rootId,
      relativePath: firstRelativePath,
      gapMs: TS2 - TS0,
      gapEndTs: TS2,
    });
    insertGapEvent(db, {
      rootId,
      relativePath: secondRelativePath,
      gapMs: TS4 - TS2,
      gapEndTs: TS4,
    });

    let concurrentWrites = 0;
    const adapterRegistry = createAdapterRegistry({
      BITMEX: {
        name: "contention-writer",
        async recover() {
          if (concurrentWrites === 0) {
            concurrentWrites += 1;
            const writerDb = openDatabase(fixture.dbPath);
            try {
              writerDb.upsertRegistry({
                collector: MARKET.collector,
                exchange: MARKET.exchange,
                symbol: MARKET.symbol,
                timeframe: "1m",
                startTs: TS0,
                endTs: TS4,
              });
            } finally {
              writerDb.close();
            }
          }
          return [];
        },
      },
    });

    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      adapterRegistry,
    });

    assert.strictEqual(concurrentWrites, 1);
    assert.strictEqual(stats.selectedEvents, 2);
    assert.strictEqual(stats.fixedEvents, 2);
    assert.deepStrictEqual(readGapStatuses(db), [GapFixStatus.Fixed, GapFixStatus.Fixed]);
  } finally {
    db.close();
  }
});

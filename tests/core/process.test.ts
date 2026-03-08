import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { gzipSync } from "node:zlib";
import { test } from "node:test";
import type { Config } from "../../src/core/config.js";
import type { Db } from "../../src/core/db.js";
import type { CompanionMetadata } from "../../src/core/model.js";
import { openDatabase } from "../../src/core/db.js";
import { classifyPath } from "../../src/core/normalize.js";
import { runProcess } from "../../src/core/process.js";
import { parseTimeframeMs } from "../../src/shared/timeframes.js";

const TIMEFRAME = "1m";
const TIMEFRAME_MS: number = (() => {
  const ms = parseTimeframeMs(TIMEFRAME);
  if (ms === undefined) {
    throw new Error("Failed to parse timeframe");
  }
  return ms;
})();
const MARKET = { collector: "RAM", bucket: "bucketA", exchange: "BITFINEX", symbol: "BTCUSD" };
const LIQ_GAP_BASE_TS = 1_710_000_000_000;

interface FixturePaths {
  baseDir: string;
  root: string;
  outDir: string;
  fileRelatives: string[];
}

async function createFixture(): Promise<FixturePaths> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-process-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const marketDir = path.join(root, MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol);

  await fs.mkdir(marketDir, { recursive: true });

  const gzTrades = [
    "1704067200000 50000 1 1 0",
    "1704067260000 50010 2 0 1",
    "corrupted line",
    "1704067320000 50020 1.5 1 0",
  ].join("\n");
  const gzPath = path.join(marketDir, "2024-01-01-00.gz");
  await fs.writeFile(gzPath, gzipSync(gzTrades));

  const plainTrades = [
    "1704070800000 50030 0.5 1 0",
    "1704070860000 50040 0.25 0 0",
  ].join("\n");
  const plainPath = path.join(marketDir, "2024-01-01-01");
  await fs.writeFile(plainPath, plainTrades);

  const relGz = path.posix.join(
    MARKET.collector,
    MARKET.bucket,
    MARKET.exchange,
    MARKET.symbol,
    "2024-01-01-00.gz",
  );
  const relPlain = path.posix.join(
    MARKET.collector,
    MARKET.bucket,
    MARKET.exchange,
    MARKET.symbol,
    "2024-01-01-01",
  );

  return { baseDir, root, outDir, fileRelatives: [relGz, relPlain] };
}

async function createEventFixture(): Promise<FixturePaths> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-process-events-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const marketDir = path.join(root, MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol);

  await fs.mkdir(marketDir, { recursive: true });

  let ts = 1_700_000_000_000;
  const lines: string[] = [];
  for (let i = 0; i < 32; i += 1) {
    if (i > 0) ts += 10;
    lines.push(`${ts} 50000 1 1 0`);
  }

  lines.push("bad");
  lines.push("still bad");

  ts += 50_000;
  lines.push(`${ts} 50010 0.5 1 0`);
  ts += 10;
  lines.push(`${ts} 50020 0.75 0 0`);
  lines.push("bad_ts 1 1 1 0");

  const plainPath = path.join(marketDir, "2024-02-02-00");
  await fs.writeFile(plainPath, lines.join("\n"));

  const relPlain = path.posix.join(
    MARKET.collector,
    MARKET.bucket,
    MARKET.exchange,
    MARKET.symbol,
    "2024-02-02-00",
  );

  return { baseDir, root, outDir, fileRelatives: [relPlain] };
}

async function createLiquidationGapFixture(): Promise<FixturePaths> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-process-liq-gap-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const marketDir = path.join(root, MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol);

  await fs.mkdir(marketDir, { recursive: true });

  let ts = LIQ_GAP_BASE_TS;
  const lines: string[] = [];
  for (let i = 0; i < 32; i += 1) {
    if (i > 0) ts += 10;
    lines.push(`${ts} 50000 1 1 0`);
  }

  ts += 20_000;
  lines.push(`${ts} 50005 0.5 1 1`);
  ts += 20_000;
  lines.push(`${ts} 50010 0.25 0 1`);
  ts += 10_000;
  lines.push(`${ts} 50020 1 1 0`);
  ts += 10;
  lines.push(`${ts} 50030 1 0 0`);

  const plainPath = path.join(marketDir, "2024-03-03-00");
  await fs.writeFile(plainPath, lines.join("\n"));

  const relPlain = path.posix.join(
    MARKET.collector,
    MARKET.bucket,
    MARKET.exchange,
    MARKET.symbol,
    "2024-03-03-00",
  );

  return { baseDir, root, outDir, fileRelatives: [relPlain] };
}

async function createPagedFilesFixture(fileCount: number): Promise<FixturePaths> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-process-paged-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const marketDir = path.join(root, MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol);

  await fs.mkdir(marketDir, { recursive: true });

  const fileRelatives: string[] = new Array(fileCount);
  const baseHourTs = 1_704_067_200_000;

  for (let i = 0; i < fileCount; i += 1) {
    const hourTs = baseHourTs + i * 3_600_000;
    const fileName = formatUtcHourFileName(hourTs);
    const filePath = path.join(marketDir, fileName);
    await fs.writeFile(filePath, `${hourTs + 1_000} 50000 1 1 0\n`);
    fileRelatives[i] = path.posix.join(MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol, fileName);
  }

  return { baseDir, root, outDir, fileRelatives };
}

function formatUtcHourFileName(ts: number): string {
  const d = new Date(ts);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}-${pad2(d.getUTCHours())}`;
}

function pad2(value: number): string {
  return value.toString().padStart(2, "0");
}

function buildConfig(root: string, outDir: string, dbPath: string): Config {
  return {
    root,
    dbPath,
    batchSize: 10,
    flushIntervalSeconds: 1,
    outDir,
    collector: MARKET.collector,
    exchange: MARKET.exchange,
    symbol: MARKET.symbol,
    timeframe: TIMEFRAME,
    timeframeMs: TIMEFRAME_MS,
  };
}

function insertFixtureFiles(db: Db, _root: string, fileRelatives: string[], opts?: { start?: number; count?: number }) {
  const start = opts?.start ?? 0;
  const end = Math.min(fileRelatives.length, start + (opts?.count ?? fileRelatives.length - start));
  const rows = [];

  for (let i = start; i < end; i += 1) {
    const rel = fileRelatives[i];
    const row = classifyPath(rel);
    if (!row) throw new Error(`Failed to classify fixture path ${rel}`);
    rows.push(row);
  }

  if (rows.length) {
    db.insertFiles(rows);
  }
}

async function readOutputs(outDir: string): Promise<{ bin: Buffer; companion: CompanionMetadata }> {
  const base = path.join(outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, TIMEFRAME);
  const bin = await fs.readFile(`${base}.bin`);
  const companionRaw = await fs.readFile(`${base}.json`, "utf8");
  return { bin, companion: JSON.parse(companionRaw) as CompanionMetadata };
}

function stripCompanionStableFields(companion: CompanionMetadata): Omit<CompanionMetadata, "gapTracker"> {
  const { gapTracker: _ignored, ...stable } = companion;
  return stable;
}

test("process is idempotent with stable inputs", async () => {
  const fixture = await createFixture();
  const dbPath = path.join(fixture.baseDir, "idempotent.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(fixture.root, fixture.outDir, dbPath);

  try {
    insertFixtureFiles(db, fixture.root, fixture.fileRelatives);

    await runProcess(config, db);
    const first = await readOutputs(fixture.outDir);

    await runProcess(config, db);
    const second = await readOutputs(fixture.outDir);

    assert.strictEqual(Buffer.compare(first.bin, second.bin), 0, "binaries should match");
    assert.deepStrictEqual(stripCompanionStableFields(second.companion), stripCompanionStableFields(first.companion));
    assert.ok((second.companion.gapTracker?.samples ?? 0) >= (first.companion.gapTracker?.samples ?? 0));
  } finally {
    db.close();
  }
});

test("resume produces the same binary as a clean rebuild", async () => {
  const fixture = await createFixture();

  const fullDbPath = path.join(fixture.baseDir, "full.sqlite");
  const fullOut = path.join(fixture.baseDir, "full-out");
  const fullDb = openDatabase(fullDbPath);
  const fullConfig = buildConfig(fixture.root, fullOut, fullDbPath);

  const clean = await (async () => {
    try {
      insertFixtureFiles(fullDb, fixture.root, fixture.fileRelatives);
      await runProcess(fullConfig, fullDb);
      return await readOutputs(fullOut);
    } finally {
      fullDb.close();
    }
  })();

  const resumeDbPath = path.join(fixture.baseDir, "resume.sqlite");
  const resumeOut = path.join(fixture.baseDir, "resume-out");
  const resumeDb = openDatabase(resumeDbPath);
  const resumeConfig = buildConfig(fixture.root, resumeOut, resumeDbPath);

  try {
    insertFixtureFiles(resumeDb, fixture.root, fixture.fileRelatives, { count: 1 });
    await runProcess(resumeConfig, resumeDb);

    insertFixtureFiles(resumeDb, fixture.root, fixture.fileRelatives, { start: 1, count: 1 });
    await runProcess(resumeConfig, resumeDb);

    const resumed = await readOutputs(resumeOut);
    assert.strictEqual(Buffer.compare(resumed.bin, clean.bin), 0, "resumed binary should match clean rebuild");
    assert.deepStrictEqual(stripCompanionStableFields(resumed.companion), stripCompanionStableFields(clean.companion));
    assert.ok((resumed.companion.gapTracker?.samples ?? 0) >= (clean.companion.gapTracker?.samples ?? 0));
  } finally {
    resumeDb.close();
  }
});

test("process persists no parse-reject rows and keeps gap rows deterministic for this fixture", async () => {
  const fixture = await createEventFixture();
  const dbPath = path.join(fixture.baseDir, "events.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(fixture.root, fixture.outDir, dbPath);

  const readGaps = () =>
    (db.db
      .prepare(
        "SELECT gap_ms, gap_miss, start_ts, end_ts, gap_score FROM gaps ORDER BY id;",
      )
      .all() as Array<{
      gap_ms: number | null;
      gap_miss: number | null;
      start_ts: number;
      end_ts: number;
      gap_score: number | null;
    }>).map((row) => ({
      gap_ms: row.gap_ms,
      gap_miss: row.gap_miss,
      start_ts: row.start_ts,
      end_ts: row.end_ts,
      gap_score: row.gap_score,
    }));

  try {
    insertFixtureFiles(db, fixture.root, fixture.fileRelatives);

    const strip = (
      rows: Array<{
        gap_ms: number | null;
        start_ts: number;
        end_ts: number;
      }>,
    ) =>
      rows.map((row) => ({
        gap_ms: row.gap_ms,
        start_ts: row.start_ts,
        end_ts: row.end_ts,
      }));

    const expected: Array<{ gap_ms: number | null; start_ts: number; end_ts: number }> = [];

    await runProcess(config, db);
    const first = readGaps();
    assert.deepStrictEqual(strip(first), expected);
    assert.strictEqual(first.length, 0);

    await runProcess(config, db);
    const second = readGaps();
    assert.deepStrictEqual(strip(second), expected);
    assert.strictEqual(second.length, 0);
  } finally {
    db.close();
  }
});

test("process gap detection keeps this liquidation-heavy fixture gap-free", async () => {
  const fixture = await createLiquidationGapFixture();
  const dbPath = path.join(fixture.baseDir, "liq-gap.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(fixture.root, fixture.outDir, dbPath);

  try {
    insertFixtureFiles(db, fixture.root, fixture.fileRelatives);
    await runProcess(config, db);

    const gapRows = db.db
      .prepare(
        "SELECT gap_ms, gap_miss, start_ts, end_ts FROM gaps ORDER BY id;",
      )
      .all() as Array<{
      gap_ms: number | null;
      gap_miss: number | null;
      start_ts: number;
      end_ts: number;
    }>;

    assert.strictEqual(gapRows.length, 0);
  } finally {
    db.close();
  }
});

test("process fails fast on indexed input missing on disk without mutating file gaps", async () => {
  const fixture = await createFixture();
  const dbPath = path.join(fixture.baseDir, "missing-input.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(fixture.root, fixture.outDir, dbPath);

  try {
    insertFixtureFiles(db, fixture.root, fixture.fileRelatives);
    const missingRelative = fixture.fileRelatives[0];
    db.insertGaps(
      {
        collector: MARKET.collector,
        exchange: MARKET.exchange,
        symbol: MARKET.symbol,
      },
      [{
        gapMs: 60_000,
        gapMiss: 1,
        startTs: 1_704_067_140_000,
        endTs: 1_704_067_200_000,
        startRelativePath: missingRelative,
        endRelativePath: missingRelative,
      }],
    );
    await fs.unlink(path.join(fixture.root, missingRelative));

    await assert.rejects(runProcess(config, db), (err: unknown) => {
      const message = err instanceof Error ? err.message : String(err);
      return message.includes("indexed input file missing on disk") && message.includes(missingRelative);
    });
    const eventRow = db.db
      .prepare(
        "SELECT COUNT(*) AS cnt FROM gaps WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol AND end_relative_path = :relativePath;",
      )
      .get({
        collector: MARKET.collector,
        exchange: MARKET.exchange,
        symbol: MARKET.symbol,
        relativePath: missingRelative,
      }) as { cnt: number };
    assert.strictEqual(eventRow.cnt, 1);

    const outputBin = path.join(fixture.outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, `${TIMEFRAME}.bin`);
    await assert.rejects(fs.stat(outputBin), (err: unknown) => (err as { code?: string } | null)?.code === "ENOENT");
  } finally {
    db.close();
  }
});

test("process iterates files across sqlite keyset pages without skipping final ranges", async () => {
  const fixture = await createPagedFilesFixture(1030);
  const dbPath = path.join(fixture.baseDir, "paged.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(fixture.root, fixture.outDir, dbPath);

  try {
    insertFixtureFiles(db, fixture.root, fixture.fileRelatives);
    await runProcess(config, db);

    const maxStart = db.db
      .prepare(
        `SELECT MAX(start_ts) AS max_start
         FROM files
         WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol;`,
      )
      .get({
        collector: MARKET.collector,
        exchange: MARKET.exchange,
        symbol: MARKET.symbol,
      }) as { max_start?: number };
    assert.ok(Number.isFinite(maxStart.max_start), "fixture should include indexed files");

    const companionPath = path.join(fixture.outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, `${TIMEFRAME}.json`);
    const companionRaw = await fs.readFile(companionPath, "utf8");
    const companion = JSON.parse(companionRaw) as CompanionMetadata;
    assert.strictEqual(companion.lastInputStartTs, maxStart.max_start);

    const registryRow = db.getRegistryEntry({
      collector: MARKET.collector,
      exchange: MARKET.exchange,
      symbol: MARKET.symbol,
      timeframe: TIMEFRAME,
    });
    assert.ok(registryRow !== null);
    assert.strictEqual(registryRow?.endTs, companion.endTs);
  } finally {
    db.close();
  }
});

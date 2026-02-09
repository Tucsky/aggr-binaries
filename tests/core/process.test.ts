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

function insertFixtureFiles(db: Db, root: string, fileRelatives: string[], opts?: { start?: number; count?: number }) {
  const rootId = db.ensureRoot(root);
  const start = opts?.start ?? 0;
  const end = Math.min(fileRelatives.length, start + (opts?.count ?? fileRelatives.length - start));
  const rows = [];

  for (let i = start; i < end; i += 1) {
    const rel = fileRelatives[i];
    const row = classifyPath(rootId, rel);
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
    assert.deepStrictEqual(second.companion, first.companion);
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
    assert.deepStrictEqual(resumed.companion, clean.companion);
  } finally {
    resumeDb.close();
  }
});

test("process logs grouped parse rejects and gaps into events table", async () => {
  const fixture = await createEventFixture();
  const dbPath = path.join(fixture.baseDir, "events.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(fixture.root, fixture.outDir, dbPath);

  const readEvents = () =>
    (db.db
      .prepare(
        "SELECT event_type, start_line, end_line, gap_ms, gap_miss, gap_end_ts FROM events ORDER BY start_line;",
      )
      .all() as Array<{
      event_type: string;
      start_line: number;
      end_line: number;
      gap_ms: number | null;
      gap_miss: number | null;
      gap_end_ts: number | null;
    }>).map((row) => ({
      event_type: row.event_type,
      start_line: row.start_line,
      end_line: row.end_line,
      gap_ms: row.gap_ms,
      gap_miss: row.gap_miss,
      gap_end_ts: row.gap_end_ts,
    }));

  try {
    insertFixtureFiles(db, fixture.root, fixture.fileRelatives);

    const strip = (
      rows: Array<{
        event_type: string;
        start_line: number;
        end_line: number;
        gap_ms: number | null;
        gap_end_ts: number | null;
      }>,
    ) =>
      rows.map((row) => ({
        event_type: row.event_type,
        start_line: row.start_line,
        end_line: row.end_line,
        gap_ms: row.gap_ms,
        gap_end_ts: row.gap_end_ts,
      }));

    const expected = [
      { event_type: "parts_short", start_line: 33, end_line: 34, gap_ms: null, gap_end_ts: null },
      { event_type: "gap", start_line: 35, end_line: 35, gap_ms: 50_000, gap_end_ts: 1_700_000_050_310 },
      { event_type: "non_finite", start_line: 37, end_line: 37, gap_ms: null, gap_end_ts: null },
    ];

    await runProcess(config, db);
    const first = readEvents();
    assert.deepStrictEqual(strip(first), expected);
    assert.ok(first[1]?.gap_miss !== null && first[1]?.gap_miss > 0);

    await runProcess(config, db);
    const second = readEvents();
    assert.deepStrictEqual(strip(second), expected);
    assert.ok(second[1]?.gap_miss !== null && second[1]?.gap_miss > 0);
  } finally {
    db.close();
  }
});

test("process gap detection ignores liquidation rows", async () => {
  const fixture = await createLiquidationGapFixture();
  const dbPath = path.join(fixture.baseDir, "liq-gap.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(fixture.root, fixture.outDir, dbPath);

  try {
    insertFixtureFiles(db, fixture.root, fixture.fileRelatives);
    await runProcess(config, db);

    const gapRows = db.db
      .prepare(
        "SELECT event_type, start_line, end_line, gap_ms, gap_end_ts FROM events WHERE event_type = 'gap' ORDER BY id;",
      )
      .all() as Array<{
      event_type: string;
      start_line: number;
      end_line: number;
      gap_ms: number | null;
      gap_end_ts: number | null;
    }>;

    assert.strictEqual(gapRows.length, 1);
    assert.deepStrictEqual({ ...gapRows[0] }, {
      event_type: "gap",
      start_line: 35,
      end_line: 35,
      gap_ms: 50_000,
      gap_end_ts: LIQ_GAP_BASE_TS + 50_310,
    });
  } finally {
    db.close();
  }
});

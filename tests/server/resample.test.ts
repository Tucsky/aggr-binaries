import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Db } from "../../src/core/db.js";
import { openDatabase } from "../../src/core/db.js";
import { ensurePreviewTimeframe } from "../../src/server/resample.js";

test("ensurePreviewTimeframe skips registry upsert for up-to-date target timeframe", async () => {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-resample-fresh-"));
  const dbPath = path.join(baseDir, "index.sqlite");
  const outputRoot = path.join(baseDir, "out");

  const collector = "RAM";
  const exchange = "BINANCE";
  const symbol = "btcusdt";
  const timeframe = "1m";
  const startTs = 1_704_067_200_000;
  const endTs = startTs + 60_000;

  const db = openDatabase(dbPath);
  let upsertCalls = 0;
  const wrappedDb: Db = {
    ...db,
    upsertRegistry: (entry) => {
      upsertCalls += 1;
      db.upsertRegistry(entry);
    },
  };

  const marketDir = path.join(outputRoot, collector, exchange, symbol);
  const binPath = path.join(marketDir, `${timeframe}.bin`);
  const companionPath = path.join(marketDir, `${timeframe}.json`);

  try {
    await fs.mkdir(marketDir, { recursive: true });
    await fs.writeFile(binPath, Buffer.alloc(56));
    await fs.writeFile(
      companionPath,
      JSON.stringify({
        exchange,
        symbol,
        timeframe,
        timeframeMs: 60_000,
        startTs,
        endTs,
        priceScale: 100,
        volumeScale: 1000,
      }),
    );

    db.upsertRegistry({ collector, exchange, symbol, timeframe, startTs, endTs });
    upsertCalls = 0;

    const companion = await ensurePreviewTimeframe(
      { db: wrappedDb, outputRoot },
      collector,
      exchange,
      symbol,
      timeframe,
    );
    assert.strictEqual(companion.startTs, startTs);
    assert.strictEqual(companion.endTs, endTs);
    assert.strictEqual(upsertCalls, 0);
  } finally {
    db.close();
  }
});

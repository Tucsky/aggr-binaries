import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { openDatabase, type Db } from "../../src/core/db.js";
import { EventType } from "../../src/core/events.js";
import { Collector } from "../../src/core/model.js";
import { listTimelineEvents, listTimelineMarkets } from "../../src/server/timelineApi.js";

test("timeline markets aggregate min/max ranges across timeframes", async () => {
  const db = await withDb();
  try {
    db.upsertRegistry({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      timeframe: "5m",
      startTs: 90,
      endTs: 210,
    });
    db.upsertRegistry({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      timeframe: "1m",
      startTs: 100,
      endTs: 300,
    });
    db.upsertRegistry({
      collector: "RAM",
      exchange: "BINANCE",
      symbol: "ETHUSDT",
      timeframe: "1m",
      startTs: 10,
      endTs: 20,
    });

    const result = listTimelineMarkets(db);
    assert.deepStrictEqual(result.timeframes, ["1m", "5m"]);
    assert.deepStrictEqual(result.markets, [
      {
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        timeframe: "ALL",
        startTs: 90,
        endTs: 300,
        indexedStartTs: null,
        indexedEndTs: null,
        processedStartTs: 90,
        processedEndTs: 300,
      },
      {
        collector: "RAM",
        exchange: "BINANCE",
        symbol: "ETHUSDT",
        timeframe: "ALL",
        startTs: 10,
        endTs: 20,
        indexedStartTs: null,
        indexedEndTs: null,
        processedStartTs: 10,
        processedEndTs: 20,
      },
    ]);
  } finally {
    db.close();
  }
});

test("timeline markets can be filtered by timeframe", async () => {
  const db = await withDb();
  try {
    db.upsertRegistry({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      timeframe: "1m",
      startTs: 100,
      endTs: 300,
    });
    db.upsertRegistry({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      timeframe: "5m",
      startTs: 90,
      endTs: 210,
    });

    const result = listTimelineMarkets(db, "1m");
    assert.deepStrictEqual(result.markets, [
      {
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        timeframe: "1m",
        startTs: 100,
        endTs: 300,
        indexedStartTs: null,
        indexedEndTs: null,
        processedStartTs: 100,
        processedEndTs: 300,
      },
    ]);
  } finally {
    db.close();
  }
});

test("timeline markets include indexed rows and prefer registry ranges when available", async () => {
  const db = await withDb();
  try {
    const rootId = db.ensureRoot("/tmp/source");
    db.insertFiles([
      {
        rootId,
        relativePath: "PI/BYBIT/BTCUSDT/2024-01-01.gz",
        collector: Collector.PI,
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        startTs: 100,
      },
      {
        rootId,
        relativePath: "PI/BYBIT/BTCUSDT/2024-01-02.gz",
        collector: Collector.PI,
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        startTs: 200,
      },
      {
        rootId,
        relativePath: "RAM/BINANCE/ETHUSDT/2024-01-01.gz",
        collector: Collector.RAM,
        exchange: "BINANCE",
        symbol: "ETHUSDT",
        startTs: 50,
      },
    ]);

    db.upsertRegistry({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      timeframe: "1m",
      startTs: 120,
      endTs: 180,
    });
    db.upsertRegistry({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      timeframe: "5m",
      startTs: 90,
      endTs: 300,
    });

    const all = listTimelineMarkets(db);
    assert.deepStrictEqual(all.markets, [
      {
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        timeframe: "ALL",
        startTs: 90,
        endTs: 300,
        indexedStartTs: 100,
        indexedEndTs: 200,
        processedStartTs: 90,
        processedEndTs: 300,
      },
      {
        collector: "RAM",
        exchange: "BINANCE",
        symbol: "ETHUSDT",
        timeframe: "ALL",
        startTs: 50,
        endTs: 50,
        indexedStartTs: 50,
        indexedEndTs: 50,
        processedStartTs: null,
        processedEndTs: null,
      },
    ]);

    const oneMinute = listTimelineMarkets(db, "1m");
    assert.deepStrictEqual(oneMinute.markets, [
      {
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        timeframe: "1m",
        startTs: 100,
        endTs: 200,
        indexedStartTs: 100,
        indexedEndTs: 200,
        processedStartTs: 120,
        processedEndTs: 180,
      },
      {
        collector: "RAM",
        exchange: "BINANCE",
        symbol: "ETHUSDT",
        timeframe: "1m",
        startTs: 50,
        endTs: 50,
        indexedStartTs: 50,
        indexedEndTs: 50,
        processedStartTs: null,
        processedEndTs: null,
      },
    ]);
  } finally {
    db.close();
  }
});

test("timeline events use gap_end_ts first and fallback to file start_ts for non-gap events", async () => {
  const db = await withDb();
  try {
    const rootId = db.ensureRoot("/tmp/source");
    db.insertFiles([
      {
        rootId,
        relativePath: "PI/BYBIT/BTCUSDT/2024-01-01.gz",
        collector: Collector.PI,
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        startTs: 200,
      },
    ]);
    db.insertEvents([
      {
        rootId,
        relativePath: "PI/BYBIT/BTCUSDT/2024-01-01.gz",
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        type: EventType.PartsShort,
        startLine: 2,
        endLine: 2,
      },
      {
        rootId,
        relativePath: "PI/BYBIT/BTCUSDT/2024-01-01.gz",
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        type: EventType.Gap,
        startLine: 5,
        endLine: 5,
        gapMs: 10,
        gapMiss: 2,
        gapEndTs: 600,
      },
    ]);
    db.db
      .prepare("UPDATE events SET gap_fix_status = :status WHERE event_type = :eventType")
      .run({ status: "adapter_error", eventType: EventType.Gap });

    const events = listTimelineEvents(db, {
      collector: "PI",
      exchange: "BYBIT",
      symbol: "btc",
      startTs: 100,
      endTs: 1000,
    });

    assert.strictEqual(events.length, 2);
    assert.deepStrictEqual(events.map((event) => event.ts), [200, 600]);
    assert.deepStrictEqual(events.map((event) => event.relativePath), [
      "PI/BYBIT/BTCUSDT/2024-01-01.gz",
      "PI/BYBIT/BTCUSDT/2024-01-01.gz",
    ]);
    assert.strictEqual(events[0].eventType, EventType.PartsShort);
    assert.strictEqual(events[0].gapFixStatus, null);
    assert.strictEqual(events[1].eventType, EventType.Gap);
    assert.strictEqual(events[1].gapFixStatus, "adapter_error");
  } finally {
    db.close();
  }
});

test("timeline events are deterministically ordered by market, timestamp, and id", async () => {
  const db = await withDb();
  try {
    const rootId = db.ensureRoot("/tmp/source");
    db.insertFiles([
      {
        rootId,
        relativePath: "PI/BYBIT/BTCUSDT/2024-01-01.gz",
        collector: Collector.PI,
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        startTs: 200,
      },
      {
        rootId,
        relativePath: "RAM/BINANCE/ETHUSDT/2024-01-01.gz",
        collector: Collector.RAM,
        exchange: "BINANCE",
        symbol: "ETHUSDT",
        startTs: 200,
      },
    ]);
    db.insertEvents([
      {
        rootId,
        relativePath: "RAM/BINANCE/ETHUSDT/2024-01-01.gz",
        collector: "RAM",
        exchange: "BINANCE",
        symbol: "ETHUSDT",
        type: EventType.Gap,
        startLine: 5,
        endLine: 5,
        gapEndTs: 300,
      },
      {
        rootId,
        relativePath: "PI/BYBIT/BTCUSDT/2024-01-01.gz",
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        type: EventType.Gap,
        startLine: 5,
        endLine: 5,
        gapEndTs: 500,
      },
      {
        rootId,
        relativePath: "PI/BYBIT/BTCUSDT/2024-01-01.gz",
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        type: EventType.Gap,
        startLine: 6,
        endLine: 6,
        gapEndTs: 500,
      },
    ]);

    const events = listTimelineEvents(db, { startTs: 0, endTs: 1_000 });
    assert.strictEqual(events.length, 3);
    assert.deepStrictEqual(
      events.map((event) => `${event.collector}:${event.exchange}:${event.symbol}:${event.ts}:${event.id}`),
      [
        `PI:BYBIT:BTCUSDT:500:${events[0].id}`,
        `PI:BYBIT:BTCUSDT:500:${events[1].id}`,
        `RAM:BINANCE:ETHUSDT:300:${events[2].id}`,
      ],
    );
  } finally {
    db.close();
  }
});

test("timeline events enforce valid range inputs", async () => {
  const db = await withDb();
  try {
    assert.throws(
      () =>
        listTimelineEvents(db, {
          startTs: Number.NaN,
          endTs: 100,
        }),
      /required/,
    );
    assert.throws(
      () =>
        listTimelineEvents(db, {
          startTs: 101,
          endTs: 100,
        }),
      />=/,
    );
  } finally {
    db.close();
  }
});

async function withDb(): Promise<Db> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-timeline-api-"));
  return openDatabase(path.join(root, "index.sqlite"));
}

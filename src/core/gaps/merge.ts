import fs from "node:fs";
import fsp from "node:fs/promises";
import { once } from "node:events";
import { finished } from "node:stream/promises";
import zlib from "node:zlib";
import type { RecoveredTrade } from "./adapters/index.js";
import { openTradeReadStream } from "./io.js";
import { parseTradeLine } from "../trades.js";

export interface MergeRecoveredTradesResult {
  inserted: number;
  insertedTrades: RecoveredTrade[];
  insertedMinTs?: number;
  insertedMaxTs?: number;
}

export async function mergeRecoveredTradesIntoFile(
  filePath: string,
  recoveredTrades: RecoveredTrade[],
): Promise<MergeRecoveredTradesResult> {
  if (!recoveredTrades.length) {
    return { inserted: 0, insertedTrades: [] };
  }

  const existingKeys = await scanExistingTradeKeys(filePath);
  const sortedTrades = [...recoveredTrades].sort((a, b) => (a.ts - b.ts) || (buildRecoveredKey(a).localeCompare(buildRecoveredKey(b))));
  const uniqueRecovered: RecoveredTrade[] = [];
  const seenRecovered = new Set<string>();
  for (const trade of sortedTrades) {
    const key = buildRecoveredKey(trade);
    if (existingKeys.has(key) || seenRecovered.has(key)) continue;
    seenRecovered.add(key);
    uniqueRecovered.push(trade);
  }

  if (!uniqueRecovered.length) {
    return { inserted: 0, insertedTrades: [] };
  }

  const tempPath = `${filePath}.fixgaps.tmp-${process.pid}-${Date.now()}`;
  const writer = openLineWriter(tempPath, filePath.endsWith(".gz"));

  let inserted = 0;
  const insertedTrades: RecoveredTrade[] = [];
  let insertedMinTs = Number.POSITIVE_INFINITY;
  let insertedMaxTs = Number.NEGATIVE_INFINITY;

  try {
    let cursor = 0;
    await forEachLine(filePath, async (line) => {
      const parsed = parseExistingTradeLine(line);
      if (parsed) {
        while (cursor < uniqueRecovered.length && uniqueRecovered[cursor].ts < parsed.ts) {
          await writer.writeLine(formatRecoveredLine(uniqueRecovered[cursor]));
          insertedTrades.push(uniqueRecovered[cursor]);
          inserted += 1;
          insertedMinTs = Math.min(insertedMinTs, uniqueRecovered[cursor].ts);
          insertedMaxTs = Math.max(insertedMaxTs, uniqueRecovered[cursor].ts);
          cursor += 1;
        }
      }
      await writer.writeLine(line);
    });

    while (cursor < uniqueRecovered.length) {
      await writer.writeLine(formatRecoveredLine(uniqueRecovered[cursor]));
      insertedTrades.push(uniqueRecovered[cursor]);
      inserted += 1;
      insertedMinTs = Math.min(insertedMinTs, uniqueRecovered[cursor].ts);
      insertedMaxTs = Math.max(insertedMaxTs, uniqueRecovered[cursor].ts);
      cursor += 1;
    }

    await writer.close();
    await fsp.rename(tempPath, filePath);
  } catch (err) {
    await writer.abort();
    await fsp.rm(tempPath, { force: true }).catch(() => {});
    throw err;
  }

  return {
    inserted,
    insertedTrades,
    insertedMinTs: inserted ? insertedMinTs : undefined,
    insertedMaxTs: inserted ? insertedMaxTs : undefined,
  };
}

async function scanExistingTradeKeys(filePath: string): Promise<Set<string>> {
  const keys = new Set<string>();
  let prevTs: number | undefined;

  await forEachLine(filePath, (line) => {
    const parsed = parseExistingTradeLine(line);
    if (!parsed) return;
    if (prevTs !== undefined && parsed.ts < prevTs) {
      throw new Error(`Input file is not timestamp-monotonic: ${filePath}`);
    }
    prevTs = parsed.ts;
    keys.add(parsed.key);
  });
  return keys;
}

interface ParsedExistingTrade {
  ts: number;
  key: string;
}

function parseExistingTradeLine(line: string): ParsedExistingTrade | undefined {
  const trade = parseTradeLine(line);
  if (!trade) return undefined;
  const fields = readFields(line, 5);
  if (fields.length < 4) return undefined;
  const sideBit = fields[3] === "1" ? "1" : "0";
  const liqBit = fields.length >= 5 && fields[4] === "1" ? "1" : "0";
  const priceKey = trade.price.toString();
  const sizeKey = trade.size.toString();
  return {
    ts: trade.ts,
    key: `${trade.ts}|${priceKey}|${sizeKey}|${sideBit}|${liqBit}`,
  };
}

function readFields(line: string, maxFields: number): string[] {
  const fields: string[] = [];
  const len = line.length;
  let i = 0;
  while (i < len && fields.length < maxFields) {
    while (i < len && line.charCodeAt(i) <= 32) i += 1;
    if (i >= len) break;
    const start = i;
    while (i < len && line.charCodeAt(i) > 32) i += 1;
    fields.push(line.slice(start, i));
  }
  return fields;
}

function buildRecoveredKey(trade: RecoveredTrade): string {
  const sideBit = trade.side === "buy" ? "1" : "0";
  return `${trade.ts}|${trade.price.toString()}|${trade.size.toString()}|${sideBit}|0`;
}

function formatRecoveredLine(trade: RecoveredTrade): string {
  const sideBit = trade.side === "buy" ? "1" : "0";
  return `${trade.ts} ${trade.priceText} ${trade.sizeText} ${sideBit} 0`;
}

interface LineWriter {
  writeLine(line: string): Promise<void>;
  close(): Promise<void>;
  abort(): Promise<void>;
}

function openLineWriter(filePath: string, gzip: boolean): LineWriter {
  const fileStream = fs.createWriteStream(filePath, { encoding: "utf8" });
  const gzipStream = gzip ? zlib.createGzip() : undefined;
  if (gzipStream) {
    gzipStream.pipe(fileStream);
    gzipStream.on("error", () => {
      fileStream.destroy();
    });
  }
  const target: NodeJS.WritableStream = gzipStream ?? fileStream;

  return {
    async writeLine(line: string): Promise<void> {
      if (!target.write(`${line}\n`)) {
        await once(target, "drain");
      }
    },
    async close(): Promise<void> {
      target.end();
      await finished(fileStream);
    },
    async abort(): Promise<void> {
      destroyWritable(target);
      destroyWritable(fileStream);
      await finished(fileStream).catch(() => {});
    },
  };
}

function destroyWritable(stream: NodeJS.WritableStream): void {
  const maybeDestroy = (stream as unknown as { destroy?: (err?: Error) => void }).destroy;
  if (typeof maybeDestroy === "function") {
    maybeDestroy.call(stream);
  }
}

async function forEachLine(filePath: string, onLine: (line: string) => Promise<void> | void): Promise<void> {
  const stream = await openTradeReadStream(filePath);
  const maybeSetEncoding = (stream as unknown as { setEncoding?: (encoding: BufferEncoding) => void }).setEncoding;
  if (typeof maybeSetEncoding === "function") {
    maybeSetEncoding.call(stream, "utf8");
  }

  let pending = "";
  for await (const chunk of stream as AsyncIterable<string | Buffer>) {
    pending += typeof chunk === "string" ? chunk : chunk.toString("utf8");
    let nl = pending.indexOf("\n");
    while (nl !== -1) {
      let line = pending.slice(0, nl);
      if (line.endsWith("\r")) {
        line = line.slice(0, -1);
      }
      pending = pending.slice(nl + 1);
      await onLine(line);
      nl = pending.indexOf("\n");
    }
  }

  if (pending.length) {
    let line = pending;
    if (line.endsWith("\r")) {
      line = line.slice(0, -1);
    }
    await onLine(line);
  }
}

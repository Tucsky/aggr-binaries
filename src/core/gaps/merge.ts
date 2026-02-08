import fs from "node:fs";
import fsp from "node:fs/promises";
import { once } from "node:events";
import { spawn } from "node:child_process";
import { finished } from "node:stream/promises";
import path from "node:path";
import zlib from "node:zlib";
import type { RecoveredTrade } from "./adapters/index.js";
import { setFixgapsProgress } from "./progress.js";
import { openTradeReadStream } from "./io.js";
import { parseTradeLine } from "../trades.js";

export interface MergeRecoveredTradesResult {
  inserted: number;
  insertedTrades: RecoveredTrade[];
  insertedMinTs?: number;
  insertedMaxTs?: number;
}

interface ExistingTradeScan {
  keys: Set<string>;
}

interface SortableRow {
  key: string;
  source: 0 | 1;
  line: string;
}

export async function mergeRecoveredTradesIntoFile(
  filePath: string,
  recoveredTrades: RecoveredTrade[],
): Promise<MergeRecoveredTradesResult> {
  if (!recoveredTrades.length) {
    return { inserted: 0, insertedTrades: [] };
  }

  const existing = await scanExistingTrades(filePath);
  const uniqueRecovered = dedupeRecoveredTrades(recoveredTrades, existing.keys);
  if (!uniqueRecovered.length) {
    return { inserted: 0, insertedTrades: [] };
  }

  return rewriteWithSortedTrades(filePath, uniqueRecovered);
}

function dedupeRecoveredTrades(recoveredTrades: RecoveredTrade[], existingKeys: Set<string>): RecoveredTrade[] {
  const sortedTrades = [...recoveredTrades].sort(
    (a, b) => (a.ts - b.ts) || buildRecoveredKey(a).localeCompare(buildRecoveredKey(b)),
  );
  const uniqueRecovered: RecoveredTrade[] = [];
  const seenRecovered = new Set<string>();

  for (const trade of sortedTrades) {
    const key = buildRecoveredKey(trade);
    if (existingKeys.has(key) || seenRecovered.has(key)) continue;
    seenRecovered.add(key);
    uniqueRecovered.push(trade);
  }

  return uniqueRecovered;
}

async function rewriteWithSortedTrades(
  filePath: string,
  recoveredTrades: RecoveredTrade[],
): Promise<MergeRecoveredTradesResult> {
  const baseName = path.basename(filePath);
  const sortDir = `${filePath}.fixgaps.sort-${process.pid}-${Date.now()}`;
  const sortablePath = path.join(sortDir, "sortable.txt");
  const sortedPath = path.join(sortDir, "sorted.txt");
  const invalidPath = path.join(sortDir, "invalid.txt");
  const tempPath = `${filePath}.fixgaps.tmp-${process.pid}-${Date.now()}`;

  await fsp.mkdir(sortDir, { recursive: true });

  const sortableWriter = fs.createWriteStream(sortablePath, { encoding: "utf8" });
  const invalidWriter = fs.createWriteStream(invalidPath, { encoding: "utf8" });
  let hasInvalid = false;
  let seq = 0;

  try {
    setFixgapsProgress(`[fixgaps] sorting ${baseName} ...`);

    await forEachLine(filePath, async (line) => {
      const parsed = parseExistingTradeLine(line);
      if (!parsed) {
        hasInvalid = true;
        await writeRawLine(invalidWriter, line);
        return;
      }
      await writeSortableRow(sortableWriter, parsed.ts, parsed.key, 0, seq, line);
      seq += 1;
    });

    for (const trade of recoveredTrades) {
      await writeSortableRow(sortableWriter, trade.ts, buildRecoveredKey(trade), 1, seq, formatRecoveredLine(trade));
      seq += 1;
    }

    sortableWriter.end();
    invalidWriter.end();
    await Promise.all([finished(sortableWriter), finished(invalidWriter)]);

    setFixgapsProgress(`[fixgaps] sorting ${baseName} external ...`);
    await runExternalSort(sortablePath, sortedPath);

    const outputWriter = openLineWriter(tempPath, filePath.endsWith(".gz"));
    const recoveredByKey = new Map<string, RecoveredTrade>();
    for (const trade of recoveredTrades) {
      const key = buildRecoveredKey(trade);
      if (!recoveredByKey.has(key)) {
        recoveredByKey.set(key, trade);
      }
    }

    let inserted = 0;
    const insertedTrades: RecoveredTrade[] = [];
    let insertedMinTs = Number.POSITIVE_INFINITY;
    let insertedMaxTs = Number.NEGATIVE_INFINITY;

    let currentKey = "";
    let keyHasExisting = false;
    let keyHasRecovered = false;

    try {
      setFixgapsProgress(`[fixgaps] writing sorted ${baseName} ...`);
      await forEachLine(sortedPath, async (line) => {
        const row = parseSortableRow(line);
        if (!row) return;

        if (row.key !== currentKey) {
          currentKey = row.key;
          keyHasExisting = false;
          keyHasRecovered = false;
        }

        if (row.source === 0) {
          await outputWriter.writeLine(row.line);
          keyHasExisting = true;
          return;
        }

        if (keyHasExisting || keyHasRecovered) {
          return;
        }

        await outputWriter.writeLine(row.line);
        keyHasRecovered = true;
        const trade = recoveredByKey.get(row.key);
        if (!trade) return;

        inserted += 1;
        insertedTrades.push(trade);
        if (trade.ts < insertedMinTs) insertedMinTs = trade.ts;
        if (trade.ts > insertedMaxTs) insertedMaxTs = trade.ts;
      });

      if (hasInvalid) {
        setFixgapsProgress(`[fixgaps] appending non-trade lines ${baseName} ...`);
        await forEachLine(invalidPath, async (line) => {
          await outputWriter.writeLine(line);
        });
      }

      await outputWriter.close();
      await fsp.rename(tempPath, filePath);

      return {
        inserted,
        insertedTrades,
        insertedMinTs: inserted ? insertedMinTs : undefined,
        insertedMaxTs: inserted ? insertedMaxTs : undefined,
      };
    } catch (err) {
      await outputWriter.abort();
      await fsp.rm(tempPath, { force: true }).catch(() => {});
      throw err;
    }
  } finally {
    sortableWriter.destroy();
    invalidWriter.destroy();
    await fsp.rm(sortDir, { recursive: true, force: true }).catch(() => {});
  }
}

async function runExternalSort(inputPath: string, outputPath: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const args = ["-t", "\t", "-k1,1n", "-k2,2", "-k3,3n", "-k4,4n", "-o", outputPath, inputPath];
    const proc = spawn("sort", args, {
      stdio: ["ignore", "ignore", "pipe"],
      env: { ...process.env, LC_ALL: "C" },
    });

    let stderr = "";
    if (proc.stderr) {
      proc.stderr.setEncoding("utf8");
      proc.stderr.on("data", (chunk: string) => {
        stderr += chunk;
      });
    }

    proc.once("error", (err) => {
      reject(new Error(`Failed to run sort: ${err.message}`));
    });

    proc.once("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      const details = stderr.trim();
      reject(new Error(`sort exited with code ${String(code)}${details ? `: ${details}` : ""}`));
    });
  });
}

async function scanExistingTrades(filePath: string): Promise<ExistingTradeScan> {
  const keys = new Set<string>();

  await forEachLine(filePath, (line) => {
    const parsed = parseExistingTradeLine(line);
    if (!parsed) return;
    keys.add(parsed.key);
  });

  return { keys };
}

interface ParsedExistingTrade {
  ts: number;
  key: string;
}

function parseExistingTradeLine(line: string): ParsedExistingTrade | undefined {
  const trade = parseTradeLine(line);
  if (!trade) return undefined;
  const fields = readFields(line, 4);
  if (fields.length < 4) return undefined;
  const sideBit = fields[3] === "1" ? "1" : "0";
  const priceKey = trade.price.toString();
  const sizeKey = trade.size.toString();

  return {
    ts: trade.ts,
    key: `${trade.ts}|${priceKey}|${sizeKey}|${sideBit}`,
  };
}

function parseSortableRow(line: string): SortableRow | undefined {
  const first = line.indexOf("\t");
  if (first <= 0) return undefined;
  const second = line.indexOf("\t", first + 1);
  if (second <= first) return undefined;
  const third = line.indexOf("\t", second + 1);
  if (third <= second) return undefined;
  const fourth = line.indexOf("\t", third + 1);
  if (fourth <= third) return undefined;

  const sourceRaw = line.slice(second + 1, third);
  const source: 0 | 1 = sourceRaw === "0" ? 0 : 1;

  return {
    key: line.slice(first + 1, second),
    source,
    line: line.slice(fourth + 1),
  };
}

async function writeSortableRow(
  stream: fs.WriteStream,
  ts: number,
  key: string,
  source: 0 | 1,
  seq: number,
  line: string,
): Promise<void> {
  await writeRawLine(stream, `${ts}\t${key}\t${source}\t${seq}\t${line}`);
}

async function writeRawLine(stream: fs.WriteStream, line: string): Promise<void> {
  if (!stream.write(`${line}\n`)) {
    await once(stream, "drain");
  }
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
  return `${trade.ts}|${trade.price.toString()}|${trade.size.toString()}|${sideBit}`;
}

function formatRecoveredLine(trade: RecoveredTrade): string {
  const sideBit = trade.side === "buy" ? "1" : "0";
  return `${trade.ts} ${trade.priceText} ${trade.sizeText} ${sideBit}`;
}

export interface LineWriter {
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

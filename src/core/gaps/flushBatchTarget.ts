import fs from "node:fs/promises";
import path from "node:path";
import zlib from "node:zlib";
import type { Db } from "../db.js";
import { classifyPath } from "../normalize.js";
import type { GapFixEventRow } from "./queue.js";
import type { DirtyMarketRange } from "./rollup.js";

const DAY_MS = 86_400_000;
const FOUR_HOUR_MS = 3_600_000 * 4;

interface FilePathTemplate {
  dir: string;
  ext: string;
  includeHour: boolean;
}

export interface FlushTargetFile {
  relativePath: string;
  absolutePath: string;
  label: string;
}

/**
 * Resolve the merge target file for a recovered flush batch from chunk timestamps + persisted gap boundaries.
 * Rules:
 * 1) default -> gap end file
 * 2) starts close to gap start (<= 1 day) -> gap start file
 * 3) far from both sides (>= 1 day from start and end) -> deterministic intermediate file inferred from boundary filenames
 */
export function resolveFlushTargetFile(
  rootPath: string,
  row: GapFixEventRow,
  firstTradeTs: number,
  lastTradeTs: number,
): FlushTargetFile {
  const endTarget = toFlushTarget(rootPath, row.end_relative_path);
  if (!Number.isFinite(firstTradeTs) || !Number.isFinite(lastTradeTs)) return endTarget;

  // Use the first trade in the batch so wide chunks that begin at the gap start
  // do not get routed to later files and break chronological replay ordering.
  if (firstTradeTs <= row.start_ts + DAY_MS) {
    return toFlushTarget(rootPath, row.start_relative_path);
  }

  if (firstTradeTs >= row.start_ts + DAY_MS && lastTradeTs <= row.end_ts - DAY_MS) {
    const intermediateRelativePath = resolveIntermediateRelativePath(row, firstTradeTs);
    if (intermediateRelativePath) {
      return toFlushTarget(rootPath, intermediateRelativePath);
    }
  }

  return endTarget;
}

/**
 * Ensure the batch target file exists and is present in the files index table.
 */
export async function ensureFlushTargetFile(
  row: GapFixEventRow,
  db: Db,
  relativePath: string,
  absolutePath: string,
): Promise<void> {
  await fs.mkdir(path.dirname(absolutePath), { recursive: true });
  const exists = await fs
    .stat(absolutePath)
    .then(() => true)
    .catch(() => false);
  if (!exists) {
    if (absolutePath.endsWith(".gz")) {
      await fs.writeFile(absolutePath, zlib.gzipSync(""));
    } else {
      await fs.writeFile(absolutePath, "");
    }
  }

  const indexedRelativePath = relativePath.startsWith(`${row.collector}/`)
    ? relativePath
    : `${row.collector}/${relativePath}`;
  const indexed = classifyPath(indexedRelativePath);
  if (indexed) db.insertFiles([indexed]);
}

/**
 * Merge per-batch dirty ranges into one market range used by higher-timeframe rollups.
 */
export function mergeDirtyRange(
  current: DirtyMarketRange | undefined,
  next: DirtyMarketRange,
): DirtyMarketRange {
  if (!current) return next;
  return {
    collector: current.collector,
    exchange: current.exchange,
    symbol: current.symbol,
    minTs: Math.min(current.minTs, next.minTs),
    maxTs: Math.max(current.maxTs, next.maxTs),
  };
}

function parseFilePathTemplate(referenceRelativePath: string): FilePathTemplate | undefined {
  const base = path.posix.basename(referenceRelativePath);
  const ext = base.endsWith(".gz") ? ".gz" : "";
  const token = ext ? base.slice(0, -3) : base;
  if (/^\d{4}-\d{2}-\d{2}-\d{2}$/.test(token)) {
    return {
      dir: path.posix.dirname(referenceRelativePath),
      ext,
      includeHour: true,
    };
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(token)) {
    return {
      dir: path.posix.dirname(referenceRelativePath),
      ext,
      includeHour: false,
    };
  }
  return undefined;
}

function resolveIntermediateRelativePath(row: GapFixEventRow, firstTradeTs: number): string | undefined {
  const startTemplate = parseFilePathTemplate(row.start_relative_path);
  const endTemplate = parseFilePathTemplate(row.end_relative_path);
  if (!startTemplate && !endTemplate) return undefined;

  const includeHour = Boolean(startTemplate?.includeHour || endTemplate?.includeHour);
  const dir = endTemplate?.dir ?? startTemplate?.dir;
  if (!dir) return undefined;

  const ext = endTemplate?.ext ?? startTemplate?.ext ?? "";
  const slotMs = includeHour ? FOUR_HOUR_MS : DAY_MS;
  const slotStartTs = Math.floor(firstTradeTs / slotMs) * slotMs;
  const token = formatPathTokenUtc(slotStartTs, includeHour);
  return path.posix.join(dir, `${token}${ext}`);
}

function toFlushTarget(rootPath: string, relativePath: string): FlushTargetFile {
  return {
    relativePath,
    absolutePath: path.join(rootPath, relativePath),
    label: path.posix.basename(relativePath),
  };
}

function formatPathTokenUtc(ts: number, includeHour: boolean): string {
  const d = new Date(ts);
  const year = d.getUTCFullYear();
  const month = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  if (!includeHour) return `${year}-${month}-${day}`;
  const hour = String(d.getUTCHours()).padStart(2, "0");
  return `${year}-${month}-${day}-${hour}`;
}

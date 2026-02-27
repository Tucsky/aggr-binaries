import fs from "node:fs/promises";
import path from "node:path";
import zlib from "node:zlib";
import type { Db } from "../db.js";
import type { Collector } from "../model.js";
import { classifyPath } from "../normalize.js";
import type { GapFixEventRow } from "./queue.js";
import type { DirtyMarketRange } from "./rollup.js";

const DAY_MS = 86_400_000;
const HOUR_MS = 3_600_000;

interface FilePathTemplate {
  dir: string;
  ext: string;
  slotMs: number;
  includeHour: boolean;
}

export interface FlushTargetFile {
  relativePath: string;
  absolutePath: string;
  label: string;
}

/**
 * Resolve the merge target file for a recovered flush batch from its last trade timestamp.
 */
export function resolveFlushTargetFile(row: GapFixEventRow, lastTradeTs: number): FlushTargetFile {
  const fallback = {
    relativePath: row.relative_path,
    absolutePath: path.join(row.root_path, row.relative_path),
    label: path.posix.basename(row.relative_path),
  };
  const template = parseFilePathTemplate(row.relative_path);
  if (!template || !Number.isFinite(lastTradeTs)) return fallback;

  const slotStart = Math.floor(lastTradeTs / template.slotMs) * template.slotMs;
  const token = formatPathTokenUtc(slotStart, template.includeHour);
  const relativePath = path.posix.join(template.dir, `${token}${template.ext}`);
  return {
    relativePath,
    absolutePath: path.join(row.root_path, relativePath),
    label: path.posix.basename(relativePath),
  };
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

  const indexed = classifyPath(row.root_id, relativePath, row.collector as Collector);
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
      slotMs: HOUR_MS,
      includeHour: true,
    };
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(token)) {
    return {
      dir: path.posix.dirname(referenceRelativePath),
      ext,
      slotMs: DAY_MS,
      includeHour: false,
    };
  }
  return undefined;
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

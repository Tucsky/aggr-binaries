import path from "node:path";
import { mergeWindows, sortRecoveredTrades, summarizeBounds } from "../common.js";
import { defaultKrakenCacheDir, loadKrakenManifest, toKrakenLocalZipPath } from "./directStore.js";
import { KrakenZipCursor, resolveKrakenZipEntryName } from "./directZip.js";
import type {
  KrakenDirectRequest,
  KrakenDirectResult,
  KrakenDirectSource,
  KrakenManifest,
  KrakenManifestFile,
  KrakenSelectedFile,
} from "./directTypes.js";
import type { FetchLike, GapWindow, RecoveredTrade } from "../types.js";

const EMPTY_DIRECT_RESULT: KrakenDirectResult = { trades: [] };
const CURSOR_IDLE_CLOSE_MS = 2_000;
const DEBUG_ADAPTERS = process.env.AGGR_FIXGAPS_DEBUG_ADAPTERS === "1" || process.env.AGGR_FIXGAPS_DEBUG === "1";

interface KrakenDirectSourceOptions {
  cacheDir?: string;
  now?: () => number;
}

interface KrakenDirectSession {
  manifestDay?: string;
  manifest?: KrakenManifest;
  entryNameByKey: Map<string, string | undefined>;
  activeCursor?: ActiveCursorState;
}

interface ActiveCursorState {
  key: string;
  cursor: KrakenZipCursor;
  idleTimer?: NodeJS.Timeout;
  idleToken: number;
}

export type { KrakenDirectRequest, KrakenDirectResult, KrakenDirectSource } from "./directTypes.js";
export { inferKrakenTickSide } from "./directSide.js";

export function createKrakenDirectSource(fetchImpl: FetchLike, options: KrakenDirectSourceOptions = {}): KrakenDirectSource {
  const cacheDir = path.resolve(options.cacheDir ?? defaultKrakenCacheDir());
  const now = options.now ?? Date.now;
  const session: KrakenDirectSession = {
    entryNameByKey: new Map<string, string | undefined>(),
  };
  return {
    async recover(req: KrakenDirectRequest): Promise<KrakenDirectResult> {
      return recoverFromKrakenDrive(req, fetchImpl, cacheDir, now, session);
    },
  };
}

async function recoverFromKrakenDrive(
  req: KrakenDirectRequest,
  fetchImpl: FetchLike,
  cacheDir: string,
  now: () => number,
  session: KrakenDirectSession,
): Promise<KrakenDirectResult> {
  const started = Date.now();
  const windows = mergeWindows(req.windows);
  const bounds = summarizeBounds(windows);
  if (!bounds) return EMPTY_DIRECT_RESULT;

  const manifest = await loadCachedManifest(fetchImpl, cacheDir, now, session);
  const selected = selectFilesForWindows(manifest.files, windows);
  if (!selected.length) {
    return { trades: [], coverageEndTs: computeCoverageEndTs(manifest.files) };
  }

  const collected: RecoveredTrade[] = [];
  let usedCursor = false;
  for (const file of selected) {
    const fileStarted = Date.now();
    const fileWindows = clipWindowsToRange(windows, file.rangeStartTs, file.rangeEndTs);
    if (!fileWindows.length) continue;
    const zipPath = toKrakenLocalZipPath(cacheDir, file.id);

    const entryName = await resolveEntryName(zipPath, req.symbol, session);
    if (!entryName) continue;

    const cursorKey = `${zipPath}\u0000${entryName}`;
    const cursorState = await ensureActiveCursor(session, cursorKey, zipPath, entryName);
    usedCursor = true;
    const trades = await cursorState.cursor.recoverWindows(fileWindows);
    if (trades.length) {
      appendRecoveredTrades(collected, trades);
    }
    if (DEBUG_ADAPTERS) {
      const source = file.source === "full" ? "full" : file.name;
      console.log(
        `[fixgaps/kraken] direct_file symbol=${req.symbol} source=${source} windows=${fileWindows.length} recovered=${trades.length} elapsed_ms=${Date.now() - fileStarted}`,
      );
    }
  }

  if (usedCursor) {
    scheduleCursorIdleClose(session);
  }

  if (DEBUG_ADAPTERS) {
    console.log(
      `[fixgaps/kraken] direct_done symbol=${req.symbol} files=${selected.length} recovered=${collected.length} elapsed_ms=${Date.now() - started}`,
    );
  }

  return {
    trades: dedupeSortedTrades(sortRecoveredTrades(collected)),
    coverageEndTs: computeCoverageEndTs(manifest.files),
  };
}

async function loadCachedManifest(
  fetchImpl: FetchLike,
  cacheDir: string,
  now: () => number,
  session: KrakenDirectSession,
): Promise<KrakenManifest> {
  const day = formatUtcDay(now());
  if (session.manifest && session.manifestDay === day) {
    return session.manifest;
  }
  const manifest = await loadKrakenManifest(fetchImpl, cacheDir, now);
  session.manifest = manifest;
  session.manifestDay = day;
  return manifest;
}

async function resolveEntryName(
  zipPath: string,
  symbol: string,
  session: KrakenDirectSession,
): Promise<string | undefined> {
  const key = `${zipPath}\u0000${symbol.toUpperCase()}`;
  if (session.entryNameByKey.has(key)) {
    return session.entryNameByKey.get(key);
  }
  const entry = await resolveKrakenZipEntryName(zipPath, symbol);
  session.entryNameByKey.set(key, entry);
  return entry;
}

async function ensureActiveCursor(
  session: KrakenDirectSession,
  key: string,
  zipPath: string,
  entryName: string,
): Promise<ActiveCursorState> {
  const active = session.activeCursor;
  if (active && active.key === key) {
    if (active.idleTimer) {
      clearTimeout(active.idleTimer);
      active.idleTimer = undefined;
    }
    active.idleToken += 1;
    return active;
  }

  if (active) {
    if (active.idleTimer) {
      clearTimeout(active.idleTimer);
      active.idleTimer = undefined;
    }
    await active.cursor.close();
  }

  const next: ActiveCursorState = {
    key,
    cursor: new KrakenZipCursor(zipPath, entryName),
    idleToken: 1,
  };
  session.activeCursor = next;
  return next;
}

function scheduleCursorIdleClose(session: KrakenDirectSession): void {
  const active = session.activeCursor;
  if (!active) return;
  if (active.idleTimer) {
    clearTimeout(active.idleTimer);
  }
  const idleToken = active.idleToken + 1;
  active.idleToken = idleToken;
  active.idleTimer = setTimeout(() => {
    const current = session.activeCursor;
    if (!current || current !== active) return;
    if (current.idleToken !== idleToken) return;
    void current.cursor.close().catch(() => {});
    session.activeCursor = undefined;
  }, CURSOR_IDLE_CLOSE_MS);
  active.idleTimer.unref?.();
}

function selectFilesForWindows(files: KrakenManifestFile[], windows: GapWindow[]): KrakenSelectedFile[] {
  const full = files.find((file) => file.source === "full");
  const quarterlies = files
    .filter((file): file is KrakenManifestFile & { quarterStartTs: number; quarterEndTs: number } => {
      return file.source === "quarterly" && file.quarterStartTs !== undefined && file.quarterEndTs !== undefined;
    })
    .sort((a, b) => a.quarterStartTs - b.quarterStartTs);

  const selected: KrakenSelectedFile[] = [];
  const fullEnd = quarterlies.length ? quarterlies[0].quarterStartTs : Number.POSITIVE_INFINITY;
  if (full) {
    selected.push({ ...full, rangeStartTs: Number.NEGATIVE_INFINITY, rangeEndTs: fullEnd });
  }
  for (const quarterly of quarterlies) {
    selected.push({ ...quarterly, rangeStartTs: quarterly.quarterStartTs, rangeEndTs: quarterly.quarterEndTs });
  }

  return selected.filter((file) => intersectsAnyWindow(file.rangeStartTs, file.rangeEndTs, windows));
}

function intersectsAnyWindow(startTs: number, endTs: number, windows: GapWindow[]): boolean {
  for (const window of windows) {
    if (window.fromTs < endTs && window.toTs > startTs) {
      return true;
    }
  }
  return false;
}

function clipWindowsToRange(windows: GapWindow[], startTs: number, endTs: number): GapWindow[] {
  const clipped: GapWindow[] = [];
  for (const window of windows) {
    const fromTs = window.fromTs > startTs ? window.fromTs : startTs;
    const toTs = window.toTs < endTs ? window.toTs : endTs;
    if (toTs <= fromTs) continue;
    clipped.push({ eventId: window.eventId, fromTs, toTs });
  }
  return clipped;
}

function computeCoverageEndTs(files: KrakenManifestFile[]): number | undefined {
  let maxEnd = Number.NEGATIVE_INFINITY;
  for (const file of files) {
    if (file.source !== "quarterly" || file.quarterEndTs === undefined) continue;
    if (file.quarterEndTs > maxEnd) {
      maxEnd = file.quarterEndTs;
    }
  }
  return Number.isFinite(maxEnd) ? maxEnd : undefined;
}

function dedupeSortedTrades(trades: RecoveredTrade[]): RecoveredTrade[] {
  if (trades.length < 2) return trades;
  const out: RecoveredTrade[] = [trades[0]];
  for (let i = 1; i < trades.length; i += 1) {
    const prev = out[out.length - 1];
    const next = trades[i];
    if (!prev || !next) continue;
    if (prev.ts !== next.ts || prev.price !== next.price || prev.size !== next.size || prev.side !== next.side) {
      out.push(next);
    }
  }
  return out;
}

export function appendRecoveredTrades(target: RecoveredTrade[], source: RecoveredTrade[]): void {
  for (let i = 0; i < source.length; i += 1) {
    const trade = source[i];
    if (trade) target.push(trade);
  }
}

function formatUtcDay(ts: number): string {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

import { createReadStream, createWriteStream } from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import zlib from "node:zlib";
import type { Config } from "./config.js";
import { parseLegacyStartTs } from "./dates.js";
import type { Db } from "./db.js";
import { Era, type FileRow } from "./model.js";
import { applyCorrections, LEGACY_MAP, type Trade } from "./trades.js";

type ManifestPath = string;

interface ManifestFile {
  version: 1;
  inProgress: string[];
}

interface ManifestState {
  file: ManifestFile;
  path: ManifestPath;
  inProgress: Set<string>;
}

interface LegacyMeta {
  relativePath: string;
  collector: string;
  collectorInPath: boolean;
  bucket: string;
  dateToken: string;
  utcStart: number;
  hasHour: boolean;
}

interface KillerCounts {
  cols: number;
  unknownExchange: number;
  nonFinite: number;
  invalidSide: number;
}

const LOG_PATH = path.resolve("convert.log");
let logStream: import("node:fs").WriteStream | null = null;
let logStreamErrored = false;

function ensureLogStream(): import("node:fs").WriteStream | null {
  if (logStream || logStreamErrored) return logStream;
  try {
    logStream = createWriteStream(LOG_PATH, { flags: "a" });
    logStream.on("error", (err) => {
      logStreamErrored = true;
      console.warn(`[convert] failed to write log ${LOG_PATH}: ${String(err)}`);
    });
  } catch (err) {
    logStreamErrored = true;
    console.warn(`[convert] failed to open log ${LOG_PATH}: ${String(err)}`);
    return null;
  }
  return logStream;
}

function mirrorLog(line: string): void {
  const stream = ensureLogStream();
  if (!stream) return;
  if (!stream.write(`${line}\n`)) {
    stream.once("drain", () => {});
  }
}

function convertLog(line: string): void {
  console.log(line);
  mirrorLog(line);
}

function convertWarn(line: string): void {
  console.warn(line);
  mirrorLog(line);
}

function sanitizeLineForLog(line: string): string {
  if (!line) return line;
  // allow tab and printable ASCII
  if (/[^\t\x20-\x7e]/.test(line)) return "???";
  return line.length > 160 ? `${line.slice(0, 160)}…` : line;
}

export async function runConvert(config: Config, db: Db): Promise<void> {
  const manifestPath = path.resolve("convert-progress.json");
  const useManifest = !config.exchange && !config.symbol && !config.force;
  const workers = resolveWorkers(config);
  const allowExchange = config.exchange ? config.exchange.toUpperCase() : null;
  const allowSymbol = config.symbol ? config.symbol.toUpperCase() : null;
  const runStarted = Date.now();

  const manifest = await loadManifest(manifestPath);
  const candidates = loadLegacyCandidates(db, config.collector, config.includePaths);
  const metaByPath = new Map(candidates.map((m) => [m.relativePath, m]));

  if (useManifest && manifest.inProgress.size) {
    convertLog(`[convert] cleaning ${manifest.inProgress.size} in-progress files from previous run`);
    for (const rel of manifest.inProgress) {
      const meta = metaByPath.get(rel);
      if (meta) await removeOutputsForMeta(meta, config.root);
    }
    manifest.inProgress.clear();
    manifest.file.inProgress = [];
    await saveManifest(manifest);
  }

  convertLog(
    `[convert] files=${candidates.length} workers=${workers} filters=${config.collector ?? "ALL"}/${allowExchange ?? "ALL"}/${allowSymbol ?? "ALL"} manifest=${useManifest ? "on" : "off"}`,
  );

  let processed = 0;
  let totalLines = 0;
  let totalTrades = 0;

  let idx = 0;
  const next = (): LegacyMeta | null => {
    if (idx >= candidates.length) return null;
    const m = candidates[idx];
    idx += 1;
    return m;
  };

  const workerFns = Array.from({ length: workers }, () =>
    (async () => {
      for (;;) {
        const meta = next();
        if (!meta) return;
        if (useManifest) {
          manifest.inProgress.add(meta.relativePath);
          manifest.file.inProgress = Array.from(manifest.inProgress);
          await saveManifest(manifest);
        }
        const res = await convertSingleFile(meta, config);
        if (useManifest) {
          manifest.inProgress.delete(meta.relativePath);
          manifest.file.inProgress = Array.from(manifest.inProgress);
          await saveManifest(manifest);
        }
        processed += 1;
        totalLines += res.linesRead;
        totalTrades += res.tradesWritten;
        if (processed % 50 === 0 || processed === candidates.length) {
          convertLog(
            `[convert] progress ${processed}/${candidates.length} lines=${formatCount(totalLines)} trades=${formatCount(totalTrades)}`,
          );
        }
      }
    })(),
  );

  await Promise.all(workerFns);

  const totalElapsed = ((Date.now() - runStarted) / 1000).toFixed(2);
  convertLog(
    `[convert] done files=${processed}/${candidates.length} lines=${formatCount(totalLines)} trades=${formatCount(totalTrades)} elapsed=${totalElapsed}s`,
  );
}

function resolveWorkers(config: Config): number {
  const fromConfig = config.workers;
  if (fromConfig && Number.isFinite(fromConfig) && fromConfig > 0) return Math.floor(fromConfig);
  const cpu = Math.max(1, os.cpus().length - 1);
  return Math.min(16, cpu);
}

function loadLegacyCandidates(db: Db, collector?: string, includePaths?: string[]): LegacyMeta[] {
  const include = buildIncludeFilter(includePaths);
  const params: Record<string, string | null> = {
    legacy: Era.Legacy,
    collector: collector ? collector.toUpperCase() : null,
    ...include.params,
  };

  const rows = db.db
    .prepare(
      `SELECT collector, relative_path, start_ts
       FROM files
       WHERE era = :legacy
         AND (:collector IS NULL OR collector = :collector)
         ${include.sql}
       ORDER BY collector, COALESCE(start_ts, 0), relative_path;`,
    )
    .all(params) as unknown as FileRow[];

  const metas: LegacyMeta[] = [];
  for (const row of rows) {
    const meta = deriveMeta(row);
    if (meta) metas.push(meta);
  }
  return metas;
}

function deriveMeta(row: FileRow): LegacyMeta | null {
  if (!row.relative_path) return null;
  const parts = row.relative_path.split("/");
  if (parts.length < 2) return null;
  const collectorDir = parts[0];
  const collector = row.collector ?? collectorDir;
  const collectorUpper = collector.toUpperCase();
  const collectorInPath = collectorDir.toUpperCase() === collector.toUpperCase();
  const bucketParts = collectorInPath ? parts.slice(1, -1) : parts.slice(0, -1);
  const bucket = bucketParts.join("/");
  const fileName = parts[parts.length - 1];
  const dateToken = extractDateToken(stripCompression(fileName));
  const hasHour = dateToken.length > "YYYY-MM-DD".length;
  let utcStart: number =
    collectorUpper === "PI"
      ? parseLegacyStartTsPi(dateToken) ?? 0
      : parseLegacyStartTs(dateToken) ?? 0;
  if (utcStart === 0) throw new Error(`Cannot parse date token "${dateToken}" in ${row.relative_path}`);
  return {
    relativePath: row.relative_path,
    collector,
    collectorInPath,
    bucket,
    dateToken,
    utcStart,
    hasHour,
  };
}

const HOUR_MS = 60 * 60 * 1000;
const PI_LABEL_PARIS_END = 2019121917; // inclusive
const PI_LABEL_UTC_START = 2020022916; // inclusive
const PI_LABEL_LAG_START = 2020032905; // inclusive
const PI_LABEL_LAG_END = 2020100709; // inclusive

function parseLegacyStartTsPi(token: string): number | undefined {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:-(\d{2}))?$/.exec(token);
  if (!m) return undefined;
  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  const hour = m[4] ? Number(m[4]) : 0;
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day) || !Number.isFinite(hour)) return undefined;
  const label = ((year * 100 + month) * 100 + day) * 100 + hour;
  const baseUtc = Date.UTC(year, month - 1, day, hour);
  if (label <= PI_LABEL_PARIS_END) return parseLegacyStartTs(token); // Paris (DST aware)
  if (label < PI_LABEL_UTC_START) return parseLegacyStartTs(token); // gap but keep Paris rule
  if (label < PI_LABEL_LAG_START) return baseUtc; // filenames in UTC
  if (label <= PI_LABEL_LAG_END) return baseUtc - HOUR_MS; // filenames are UTC+1 (lagging DST)
  return baseUtc; // remainder effectively UTC
}

function extractDateToken(baseName: string): string {
  const idx = baseName.lastIndexOf("_");
  if (idx === -1 || idx === baseName.length - 1) {
    throw new Error(`Cannot extract date token from ${baseName}`);
  }
  return baseName.slice(idx + 1);
}

function stripCompression(name: string): string {
  return name.replace(/\.gz$/i, "");
}

function formatUtcToken(ts: number, includeHour: boolean): string {
  const d = new Date(ts);
  const pad = (n: number) => String(n).padStart(2, "0");
  const y = d.getUTCFullYear();
  const mo = pad(d.getUTCMonth() + 1);
  const da = pad(d.getUTCDate());
  if (!includeHour) return `${y}-${mo}-${da}`;
  const h = pad(d.getUTCHours());
  return `${y}-${mo}-${da}-${h}`;
}

async function convertSingleFile(
  meta: LegacyMeta,
  config: Config
): Promise<{ linesRead: number; tradesWritten: number }> {
  const fullPath = path.join(config.root, meta.relativePath);
  const outToken = formatUtcToken(meta.utcStart, meta.hasHour);
  console.log(`Converting ${meta.relativePath} → ${outToken}.gz`);
  const outDirBase = path.join(
    config.root,
    meta.collectorInPath ? meta.collector : "",
    meta.bucket,
  );

  const started = Date.now();
  convertLog(`[convert] start ${meta.relativePath}`);

  const streams = new Map<string, { gzip: zlib.Gzip; file: import("node:fs").WriteStream }>();
  let linesRead = 0;
  let tradesWritten = 0;
  const killerCounts: KillerCounts = {
    cols: 0,
    unknownExchange: 0,
    nonFinite: 0,
    invalidSide: 0,
  };

  const input = createReadStream(fullPath, { highWaterMark: 1 << 20 });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });

  try {
    for await (const rawLine of rl) {
      const line = rawLine.trim();
      if (!line) continue;
      linesRead += 1;
      const trade = parseLegacyTrade(line, meta.relativePath, linesRead, killerCounts);
      if (!trade) continue;
      const corrected = applyCorrections(trade);
      if (!corrected) continue;

      const key = `${corrected.exchange}::${corrected.symbol}`;
      let stream = streams.get(key);
      if (!stream) {
        const outDir = path.join(outDirBase, corrected.exchange, corrected.symbol);
        await fs.mkdir(outDir, { recursive: true });
        const outPath = path.join(outDir, `${outToken}.gz`);
        const writeStream = createWriteStream(outPath, { highWaterMark: 1 << 20 });
        const gzip = zlib.createGzip();
        gzip.pipe(writeStream);
        stream = { gzip, file: writeStream };
        streams.set(key, stream);
      }
      const side = corrected.side === "buy" ? "1" : "0";
      const outLine = corrected.liquidation
        ? `${corrected.ts} ${corrected.price} ${corrected.size} ${side} 1\n`
        : `${corrected.ts} ${corrected.price} ${corrected.size} ${side}\n`;
      if (!stream.gzip.write(outLine)) {
        await onceDrain(stream.gzip);
      }
      tradesWritten += 1;
    }
  } finally {
    await Promise.all(
      Array.from(streams.values()).map(
        (s) =>
          new Promise<void>((resolve, reject) => {
            s.gzip.once("error", reject);
            s.file.once("error", reject);
            s.file.once("close", resolve);
            s.gzip.end();
          }),
      ),
    );
  }

  const elapsed = ((Date.now() - started) / 1000).toFixed(2);
  const offset = tradesWritten - linesRead;
  const parts = [
    `[convert] done ${meta.relativePath}`,
    `total=${formatCount(tradesWritten)}`,
    `offset=${offset}`,
  ];
  const killerTuples: Array<[number, string]> = [
    [killerCounts.unknownExchange, "unx"],
    [killerCounts.nonFinite, "nan"],
    [killerCounts.cols, "inv"],
    [killerCounts.invalidSide, "sd"],
  ];
  for (const [count, label] of killerTuples) {
    if (count > 0) parts.push(`${label}=${count}`);
  }
  parts.push(`elapsed=${elapsed}s`);
  convertLog(parts.join(" "));

  return { linesRead, tradesWritten };
}

function parseLegacyTrade(
  line: string,
  file: string,
  lineNo: number,
  killers?: KillerCounts,
): Trade | null {
  const parts = line.split(/\s+/);
  if (parts.length < 5) {
    if (killers) killers.cols += 1;
    convertLog(`Invalid legacy line (cols<5) ${file}:${lineNo} : ${sanitizeLineForLog(line)}`);
    return null;
  }
  const rawExchange = parts[0];
  const lower = rawExchange.toLowerCase();
  const mapped = LEGACY_MAP[lower];
  if (!mapped) {
    if (killers) killers.unknownExchange += 1;
    // console.log(`Unknown legacy exchange "${rawExchange}" at ${file}:${lineNo} : ${line}`);
    return null;
  }
  const ts = Number(parts[1]);
  const price = Number(parts[2]);
  const size = Number(parts[3]);
  const sideToken = parts[4];
  const liquidationToken = parts[5];
  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) {
    if (killers) killers.nonFinite += 1;
    convertLog(`Non-finite field at ${file}:${lineNo} : ${sanitizeLineForLog(line)}`);
    return null;
  }
  if (sideToken !== "1" && sideToken !== "0") {
    if (killers) killers.invalidSide += 1;
    convertLog(`Invalid side token "${sideToken}" at ${file}:${lineNo} : ${sanitizeLineForLog(line)}`);
    return null;
  }
  return {
    ts,
    price,
    size,
    side: sideToken === "1" ? "buy" : "sell",
    liquidation: liquidationToken === "1",
    exchange: mapped[0],
    symbol: mapped[1],
  };
}

function formatCount(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${trimTrailingZero((value / 1_000_000_000).toFixed(1))}B`;
  if (abs >= 1_000_000) return `${trimTrailingZero((value / 1_000_000).toFixed(1))}M`;
  if (abs >= 1_000) return `${trimTrailingZero((value / 1_000).toFixed(1))}k`;
  return String(value);
}

function trimTrailingZero(num: string): string {
  return num.replace(/\.0$/, "");
}

function onceDrain(stream: zlib.Gzip): Promise<void> {
  return new Promise((resolve) => stream.once("drain", resolve));
}

async function removeOutputsForMeta(meta: LegacyMeta, root: string): Promise<void> {
  const token = formatUtcToken(meta.utcStart, meta.hasHour);
  const base = path.join(root, meta.collectorInPath ? meta.collector : "", meta.bucket);
  let exchanges: import("node:fs").Dirent[];
  try {
    exchanges = await fs.readdir(base, { withFileTypes: true });
  } catch {
    return;
  }
  for (const ex of exchanges) {
    if (!ex.isDirectory()) continue;
    const exDir = path.join(base, ex.name);
    let symbols: import("node:fs").Dirent[];
    try {
      symbols = await fs.readdir(exDir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const sym of symbols) {
      if (!sym.isDirectory()) continue;
      const filePath = path.join(exDir, sym.name, `${token}.gz`);
      await fs.rm(filePath, { force: true });
    }
  }
}

async function loadManifest(manifestPath: ManifestPath): Promise<ManifestState> {
  let file: ManifestFile;
  try {
    const raw = await fs.readFile(manifestPath, "utf8");
    const parsed = JSON.parse(raw) as Partial<ManifestFile>;
    file = {
      version: 1,
      inProgress: Array.isArray(parsed.inProgress) ? parsed.inProgress : [],
    };
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === "ENOENT") {
      file = { version: 1, inProgress: [] };
    } else {
      throw new Error(`Failed to read manifest ${manifestPath}: ${String(err)}`);
    }
  }
  return {
    file,
    path: manifestPath,
    inProgress: new Set(file.inProgress),
  };
}

async function saveManifest(manifest: ManifestState): Promise<void> {
  const payload: ManifestFile = {
    version: 1,
    inProgress: Array.from(manifest.inProgress),
  };
  const body = JSON.stringify(payload, null, 2);
  try {
    await fs.writeFile(manifest.path, body);
  } catch (err) {
    convertWarn(`[convert] failed to write manifest ${manifest.path}: ${String(err)}`);
  }
  manifest.file = payload;
}

function buildIncludeFilter(includePaths?: string[]): { sql: string; params: Record<string, string> } {
  const clauses: string[] = [];
  const params: Record<string, string> = {};
  includePaths?.forEach((raw, idx) => {
    const inc = normalizeInclude(raw);
    if (!inc) return;
    const key = `inc${idx}`;
    clauses.push(`(relative_path = :${key} OR relative_path LIKE :${key}Pref)`);
    params[key] = inc;
    params[`${key}Pref`] = `${inc}/%`;
  });
  const sql = clauses.length ? `AND (${clauses.join(" OR ")})` : "";
  return { sql, params };
}

function normalizeInclude(p: string): string {
  return p.replace(/\\/g, "/").replace(/^\.?\//, "").replace(/\/+$/, "");
}

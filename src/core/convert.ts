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

export async function runConvert(config: Config, db: Db): Promise<void> {
  const manifestPath = path.resolve("convert-progress.json");
  const useManifest = !config.exchange && !config.symbol && !config.force;
  const workers = resolveWorkers(config);

  const manifest = await loadManifest(manifestPath);
  const candidates = loadLegacyCandidates(db, config.collector, config.includePaths);
  const metaByPath = new Map(candidates.map((m) => [m.relativePath, m]));

  if (useManifest && manifest.inProgress.size) {
    console.log(
      `[convert] cleaning ${manifest.inProgress.size} in-progress files from previous run`,
    );
    for (const rel of manifest.inProgress) {
      const meta = metaByPath.get(rel);
      if (meta) await removeOutputsForMeta(meta, config.root);
    }
    manifest.inProgress.clear();
    manifest.file.inProgress = [];
    await saveManifest(manifest);
  }

  const allowExchange = config.exchange ? config.exchange.toUpperCase() : undefined;
  const allowSymbol = config.symbol;

  console.log(
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
        const res = await convertSingleFile(meta, config, allowExchange, allowSymbol);
        if (useManifest) {
          manifest.inProgress.delete(meta.relativePath);
          manifest.file.inProgress = Array.from(manifest.inProgress);
          await saveManifest(manifest);
        }
        processed += 1;
        totalLines += res.linesRead;
        totalTrades += res.tradesWritten;
        if (processed % 50 === 0 || processed === candidates.length) {
          console.log(
            `[convert] progress ${processed}/${candidates.length} lines=${totalLines} trades=${totalTrades}`,
          );
        }
      }
    })(),
  );

  await Promise.all(workerFns);

  console.log(
    `[convert] done files=${processed}/${candidates.length} lines=${totalLines} trades=${totalTrades}`,
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
  const collectorInPath = collectorDir.toUpperCase() === collector.toUpperCase();
  const bucketParts = collectorInPath ? parts.slice(1, -1) : parts.slice(0, -1);
  const bucket = bucketParts.join("/");
  const fileName = parts[parts.length - 1];
  const dateToken = extractDateToken(stripCompression(fileName));
  const hasHour = dateToken.length > "YYYY-MM-DD".length;
  const utcStart = row.start_ts ?? parseLegacyStartTs(dateToken) ?? 0;
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
  config: Config,
  allowExchange?: string,
  allowSymbol?: string,
): Promise<{ linesRead: number; tradesWritten: number }> {
  const fullPath = path.join(config.root, meta.relativePath);
  const outToken = formatUtcToken(meta.utcStart, meta.hasHour);
  const outDirBase = path.join(
    config.root,
    meta.collectorInPath ? meta.collector : "",
    meta.bucket,
  );

  const started = Date.now();
  console.log(`[convert] start ${meta.relativePath}`);

  const streams = new Map<string, { gzip: zlib.Gzip; file: import("node:fs").WriteStream }>();
  let linesRead = 0;
  let tradesWritten = 0;

  const input = createReadStream(fullPath);
  const rl = readline.createInterface({ input, crlfDelay: Infinity });

  try {
    for await (const rawLine of rl) {
      const line = rawLine.trim();
      if (!line) continue;
      linesRead += 1;
      const trade = parseLegacyTrade(line, meta.relativePath, linesRead);
      if (!trade) continue;
      if (allowExchange && trade.exchange !== allowExchange) continue;
      if (allowSymbol && trade.symbol !== allowSymbol) continue;
      const corrected = applyCorrections(trade);
      if (!corrected) continue;

      const key = `${corrected.exchange}::${corrected.symbol}`;
      let stream = streams.get(key);
      if (!stream) {
        const outDir = path.join(outDirBase, corrected.exchange, corrected.symbol);
        await fs.mkdir(outDir, { recursive: true });
        const outPath = path.join(outDir, `${outToken}.gz`);
        const writeStream = createWriteStream(outPath);
        const gzip = zlib.createGzip();
        gzip.pipe(writeStream);
        stream = { gzip, file: writeStream };
        streams.set(key, stream);
      }
      const side = corrected.side === "buy" ? "1" : "0";
      const liq = corrected.liquidation ? "1" : "0";
      const outLine = `${corrected.ts} ${corrected.price} ${corrected.size} ${side} ${liq}\n`;
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
  console.log(
    `[convert] done ${meta.relativePath} lines=${linesRead} trades=${tradesWritten} elapsed=${elapsed}s`,
  );

  return { linesRead, tradesWritten };
}

function parseLegacyTrade(line: string, file: string, lineNo: number): Trade | null {
  const parts = line.split(/\s+/);
  if (parts.length < 5) {
    console.log(`Invalid legacy line (cols<5) ${file}:${lineNo} : ${line}`);
    return null;
  }
  const rawExchange = parts[0];
  const lower = rawExchange.toLowerCase();
  const mapped = LEGACY_MAP[lower];
  if (!mapped) {
    // console.log(`Unknown legacy exchange "${rawExchange}" at ${file}:${lineNo} : ${line}`);
    return null;
  }
  const ts = Number(parts[1]);
  const price = Number(parts[2]);
  const size = Number(parts[3]);
  const sideToken = parts[4];
  const liquidationToken = parts[5];
  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) {
    console.log(`Non-finite field at ${file}:${lineNo} : ${line}`);
    return null;
  }
  if (sideToken !== "1" && sideToken !== "0") {
    console.log(`Invalid side token "${sideToken}" at ${file}:${lineNo} : ${line}`);
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
    console.warn(`[convert] failed to write manifest ${manifest.path}: ${String(err)}`);
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

import fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { fetchText } from "../common.js";
import type { KrakenManifest, KrakenManifestFile } from "./directTypes.js";
import type { FetchLike } from "../types.js";
import { formatProgressUrl, setFixgapsProgress } from "../../progress.js";

const KRAKEN_HISTORY_ARTICLE_URL =
  "https://support.kraken.com/articles/360047543791-downloadable-historical-market-data-time-and-sales-";
const KRAKEN_DEFAULT_FULL_FILE_ID = "10zh3tDpqANYvVtYVgczwVz3UZFRUb1el";
const KRAKEN_DEFAULT_QUARTERLY_FOLDER_ID = "188O9xQjZTythjyLNes_5zfMEFaMbTT22";
const KRAKEN_DRIVE_DOWNLOAD_BASE = "https://drive.usercontent.google.com/download";
const KRAKEN_DRIVE_EMBEDDED_FOLDER_BASE = "https://drive.google.com/embeddedfolderview";
const DOWNLOAD_DIR_NAME = "downloads";
const MANIFEST_VERSION = 1;
const DEBUG_ADAPTERS = process.env.AGGR_FIXGAPS_DEBUG_ADAPTERS === "1" || process.env.AGGR_FIXGAPS_DEBUG === "1";

interface KrakenDriveRefs {
  fullFileId: string;
  quarterlyFolderId: string;
}

interface QuarterRange {
  startTs: number;
  endTs: number;
}

export function defaultKrakenCacheDir(): string {
  return path.resolve(".aggr-cache/fixgaps/kraken");
}

export async function loadKrakenManifest(
  fetchImpl: FetchLike,
  cacheDir: string,
  now: () => number,
): Promise<KrakenManifest> {
  const dayToken = utcDayToken(now());
  const existing = await readManifest(cacheDir);

  if (existing && existing.version === MANIFEST_VERSION && existing.refreshedDay === dayToken) {
    await ensureCachedFiles(existing.files, cacheDir, fetchImpl);
    return existing;
  }

  try {
    const refs = await resolveDriveRefs(fetchImpl);
    const quarterlyFiles = await listQuarterlyFiles(refs.quarterlyFolderId, fetchImpl);
    const files = await buildManifestFiles(refs.fullFileId, quarterlyFiles, fetchImpl);
    const manifest: KrakenManifest = {
      version: MANIFEST_VERSION,
      refreshedDay: dayToken,
      fullFileId: refs.fullFileId,
      quarterlyFolderId: refs.quarterlyFolderId,
      files,
    };
    await ensureCachedFiles(manifest.files, cacheDir, fetchImpl);
    await writeManifest(cacheDir, manifest);
    return manifest;
  } catch (err) {
    if (!existing) throw err;
    if (DEBUG_ADAPTERS) {
      const message = err instanceof Error ? err.message : String(err);
      console.log(`[fixgaps/kraken] manifest_refresh_error error=${message}`);
    }
    await ensureCachedFiles(existing.files, cacheDir, fetchImpl);
    return existing;
  }
}

export function toKrakenLocalZipPath(cacheDir: string, fileId: string): string {
  return path.join(cacheDir, DOWNLOAD_DIR_NAME, `${fileId}.zip`);
}

async function resolveDriveRefs(fetchImpl: FetchLike): Promise<KrakenDriveRefs> {
  try {
    const html = await fetchText(KRAKEN_HISTORY_ARTICLE_URL, fetchImpl);
    const fileId = extractFirstMatch(html, /https:\/\/drive\.google\.com\/file\/d\/([A-Za-z0-9_-]{10,})/);
    const folderId = extractFirstMatch(html, /https:\/\/drive\.google\.com\/drive\/folders\/([A-Za-z0-9_-]{10,})/);
    if (fileId && folderId) {
      return {
        fullFileId: fileId,
        quarterlyFolderId: folderId,
      };
    }
  } catch {
    // Fallback uses known public IDs if article fetch is blocked or reshaped.
  }
  return {
    fullFileId: KRAKEN_DEFAULT_FULL_FILE_ID,
    quarterlyFolderId: KRAKEN_DEFAULT_QUARTERLY_FOLDER_ID,
  };
}

function extractFirstMatch(text: string, pattern: RegExp): string | undefined {
  const match = pattern.exec(text);
  return match && match[1] ? match[1] : undefined;
}

async function listQuarterlyFiles(folderId: string, fetchImpl: FetchLike): Promise<KrakenManifestFile[]> {
  const url = `${KRAKEN_DRIVE_EMBEDDED_FOLDER_BASE}?id=${encodeURIComponent(folderId)}#list`;
  const html = await fetchText(url, fetchImpl);
  const files: KrakenManifestFile[] = [];
  const re =
    /<div class="flip-entry" id="entry-([A-Za-z0-9_-]+)"[\s\S]*?<div class="flip-entry-title">([^<]+)<\/div>[\s\S]*?<div class="flip-entry-last-modified"><div>([^<]+)<\/div>/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(html)) !== null) {
    const id = match[1] ?? "";
    const name = (match[2] ?? "").trim();
    if (!id || !name.toLowerCase().endsWith(".zip")) continue;
    const quarter = parseQuarterRange(name);
    if (!quarter) continue;
    files.push({
      id,
      name,
      source: "quarterly",
      sizeBytes: 0,
      lastModifiedTs: 0,
      quarterStartTs: quarter.startTs,
      quarterEndTs: quarter.endTs,
    });
  }
  files.sort((a, b) => (a.quarterStartTs ?? 0) - (b.quarterStartTs ?? 0));
  return files;
}

function parseQuarterRange(name: string): QuarterRange | undefined {
  const match = /_Q([1-4])_(\d{4})\.zip$/i.exec(name);
  if (!match) return undefined;
  const quarter = Number(match[1]);
  const year = Number(match[2]);
  if (!Number.isFinite(quarter) || !Number.isFinite(year)) return undefined;
  const startMonth = (quarter - 1) * 3;
  const startTs = Date.UTC(year, startMonth, 1, 0, 0, 0, 0);
  const endTs = Date.UTC(year, startMonth + 3, 1, 0, 0, 0, 0);
  return { startTs, endTs };
}

async function buildManifestFiles(
  fullFileId: string,
  quarterlyFiles: KrakenManifestFile[],
  fetchImpl: FetchLike,
): Promise<KrakenManifestFile[]> {
  const refs: KrakenManifestFile[] = [
    {
      id: fullFileId,
      name: "Kraken_Trading_History_Full.zip",
      source: "full",
      sizeBytes: 0,
      lastModifiedTs: 0,
    },
    ...quarterlyFiles,
  ];
  const out: KrakenManifestFile[] = [];
  for (const file of refs) {
    const metadata = await fetchDriveMetadata(file.id, fetchImpl);
    out.push({
      ...file,
      name: metadata.name || file.name,
      sizeBytes: metadata.sizeBytes,
      lastModifiedTs: metadata.lastModifiedTs,
    });
  }
  return out;
}

async function fetchDriveMetadata(
  fileId: string,
  fetchImpl: FetchLike,
): Promise<{ name: string; sizeBytes: number; lastModifiedTs: number }> {
  const url = buildDriveDownloadUrl(fileId);
  setFixgapsProgress(`[fixgaps] fetching ${formatProgressUrl(url)} ...`);
  const res = await fetchImpl(url, {
    method: "GET",
    headers: { Range: "bytes=0-0" },
  });
  if (!(res.status === 200 || res.status === 206)) {
    const text = await safeText(res);
    throw new Error(`HTTP ${res.status} for ${url}: ${text.slice(0, 200)}`);
  }
  const contentRange = res.headers.get("content-range");
  const sizeHeader = res.headers.get("content-length");
  const lastModified = Date.parse(res.headers.get("last-modified") ?? "");
  const disposition = res.headers.get("content-disposition") ?? "";
  await disposeBody(res);
  return {
    name: parseContentDispositionFileName(disposition),
    sizeBytes: parseTotalSize(contentRange, sizeHeader),
    lastModifiedTs: Number.isFinite(lastModified) ? lastModified : 0,
  };
}

function parseContentDispositionFileName(contentDisposition: string): string {
  const utfMatch = /filename\*=UTF-8''([^;]+)/i.exec(contentDisposition);
  if (utfMatch && utfMatch[1]) {
    try {
      return decodeURIComponent(utfMatch[1]);
    } catch {
      return utfMatch[1];
    }
  }
  const quoted = /filename="([^"]+)"/i.exec(contentDisposition);
  if (quoted && quoted[1]) return quoted[1];
  const plain = /filename=([^;]+)/i.exec(contentDisposition);
  return plain && plain[1] ? plain[1].trim() : "";
}

function parseTotalSize(contentRange: string | null, contentLength: string | null): number {
  if (contentRange) {
    const slash = contentRange.lastIndexOf("/");
    if (slash >= 0 && slash + 1 < contentRange.length) {
      const total = Number(contentRange.slice(slash + 1));
      if (Number.isFinite(total) && total > 0) return total;
    }
  }
  const size = Number(contentLength);
  return Number.isFinite(size) && size > 0 ? size : 0;
}

async function ensureCachedFiles(files: KrakenManifestFile[], cacheDir: string, fetchImpl: FetchLike): Promise<void> {
  const downloadDir = path.join(cacheDir, DOWNLOAD_DIR_NAME);
  await fs.mkdir(downloadDir, { recursive: true });
  for (const file of files) {
    const localPath = toKrakenLocalZipPath(cacheDir, file.id);
    if (!(await needsDownload(localPath, file.sizeBytes))) {
      continue;
    }
    await downloadDriveFile(file.id, localPath, file.sizeBytes, fetchImpl);
  }
}

async function needsDownload(localPath: string, expectedSize: number): Promise<boolean> {
  try {
    const stat = await fs.stat(localPath);
    if (!stat.isFile()) return true;
    if (expectedSize > 0 && stat.size !== expectedSize) return true;
    return false;
  } catch {
    return true;
  }
}

async function downloadDriveFile(fileId: string, localPath: string, expectedSize: number, fetchImpl: FetchLike): Promise<void> {
  const url = buildDriveDownloadUrl(fileId);
  setFixgapsProgress(`[fixgaps] downloading ${formatProgressUrl(url)} ...`);
  const res = await fetchImpl(url, { method: "GET" });
  const contentType = (res.headers.get("content-type") ?? "").toLowerCase();
  if (!res.ok || !res.body || contentType.includes("text/html")) {
    const text = await safeText(res);
    throw new Error(`Failed to download Kraken drive file ${fileId}: HTTP ${res.status} ${text.slice(0, 200)}`);
  }

  const tmpPath = `${localPath}.tmp-${process.pid}-${Date.now()}`;
  try {
    const input = Readable.fromWeb(res.body as unknown as NodeReadableStream<Uint8Array>);
    await pipeline(input, createWriteStream(tmpPath));
    const stat = await fs.stat(tmpPath);
    if (expectedSize > 0 && stat.size !== expectedSize) {
      throw new Error(`Size mismatch for ${fileId}: expected=${expectedSize} got=${stat.size}`);
    }
    await fs.rename(tmpPath, localPath);
  } catch (err) {
    await fs.rm(tmpPath, { force: true }).catch(() => {});
    throw err;
  }
}

function buildDriveDownloadUrl(fileId: string): string {
  return `${KRAKEN_DRIVE_DOWNLOAD_BASE}?id=${encodeURIComponent(fileId)}&export=download&confirm=t`;
}

async function readManifest(cacheDir: string): Promise<KrakenManifest | undefined> {
  try {
    const raw = await fs.readFile(path.join(cacheDir, "manifest.json"), "utf8");
    return JSON.parse(raw) as KrakenManifest;
  } catch {
    return undefined;
  }
}

async function writeManifest(cacheDir: string, manifest: KrakenManifest): Promise<void> {
  await fs.mkdir(cacheDir, { recursive: true });
  const outPath = path.join(cacheDir, "manifest.json");
  const tmpPath = `${outPath}.tmp-${process.pid}-${Date.now()}`;
  await fs.writeFile(tmpPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  await fs.rename(tmpPath, outPath);
}

function utcDayToken(ts: number): string {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

async function safeText(res: Response): Promise<string> {
  try {
    return await res.text();
  } catch {
    return "";
  }
}

async function disposeBody(res: Response): Promise<void> {
  const body = res.body;
  if (!body) return;
  await body.cancel().catch(() => {});
}

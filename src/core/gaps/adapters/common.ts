import zlib from "node:zlib";
import type { FetchLike, GapWindow, RecoveredTrade, TradeSide } from "./types.js";

interface LocalZipHeader {
  method: number;
  compressedSize: number;
  dataOffset: number;
}

interface TimeBounds {
  minTs: number;
  maxTs: number;
}

const QUOTES = ["USDT", "USDC", "USD", "EUR", "BTC", "ETH"] as const;

export function normalizeSymbolToken(symbol: string): string {
  let out = "";
  for (let i = 0; i < symbol.length; i += 1) {
    const code = symbol.charCodeAt(i);
    if ((code >= 48 && code <= 57) || (code >= 65 && code <= 90) || (code >= 97 && code <= 122)) {
      out += symbol[i];
    }
  }
  return out.toUpperCase();
}

export function toCoinbasePair(symbol: string): string {
  const upper = symbol.toUpperCase();
  if (upper.includes("-")) return upper;
  for (const quote of QUOTES) {
    if (upper.endsWith(quote) && upper.length > quote.length) {
      const base = upper.slice(0, upper.length - quote.length);
      return `${base}-${quote}`;
    }
  }
  return upper;
}

export function toKrakenPair(symbol: string): string {
  const upper = normalizeSymbolToken(symbol);
  if (upper.startsWith("BTC")) {
    return `XBT${upper.slice(3)}`;
  }
  return upper;
}

export function toBitfinexPair(symbol: string): string {
  return `t${normalizeSymbolToken(symbol)}`;
}

export function mergeWindows(windows: GapWindow[]): GapWindow[] {
  if (!windows.length) return [];
  const sorted = [...windows].sort((a, b) => (a.fromTs - b.fromTs) || (a.toTs - b.toTs) || (a.eventId - b.eventId));
  const merged: GapWindow[] = [];
  for (const w of sorted) {
    if (!Number.isFinite(w.fromTs) || !Number.isFinite(w.toTs) || w.toTs <= w.fromTs) continue;
    const prev = merged[merged.length - 1];
    if (!prev || w.fromTs > prev.toTs) {
      merged.push({ ...w });
      continue;
    }
    if (w.toTs > prev.toTs) prev.toTs = w.toTs;
  }
  return merged;
}

export function filterTradesByWindows(trades: RecoveredTrade[], windows: GapWindow[]): RecoveredTrade[] {
  if (!trades.length || !windows.length) return [];
  const merged = mergeWindows(windows);
  const result: RecoveredTrade[] = [];
  let cursor = 0;
  const sorted = [...trades].sort((a, b) => a.ts - b.ts);
  for (const trade of sorted) {
    while (cursor < merged.length && merged[cursor].toTs <= trade.ts) {
      cursor += 1;
    }
    if (cursor >= merged.length) break;
    const w = merged[cursor];
    if (trade.ts > w.fromTs && trade.ts < w.toTs) {
      result.push(trade);
    }
  }
  return result;
}

export function summarizeBounds(windows: GapWindow[]): TimeBounds | undefined {
  if (!windows.length) return undefined;
  let minTs = Number.POSITIVE_INFINITY;
  let maxTs = Number.NEGATIVE_INFINITY;
  for (const w of windows) {
    if (w.fromTs < minTs) minTs = w.fromTs;
    if (w.toTs > maxTs) maxTs = w.toTs;
  }
  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs) || maxTs <= minTs) return undefined;
  return { minTs, maxTs };
}

export function collectUtcDays(bounds: TimeBounds): string[] {
  const startDay = Math.floor(bounds.minTs / 86_400_000);
  const endDay = Math.floor(bounds.maxTs / 86_400_000);
  const days: string[] = [];
  for (let day = startDay; day <= endDay; day += 1) {
    const ts = day * 86_400_000;
    days.push(formatUtcDay(ts));
  }
  return days;
}

export function formatUtcDay(ts: number): string {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = (d.getUTCMonth() + 1).toString().padStart(2, "0");
  const day = d.getUTCDate().toString().padStart(2, "0");
  return `${y}-${m}-${day}`;
}

export function parseCsvLines(raw: string): string[] {
  const lines = raw.split("\n");
  if (lines.length && lines[lines.length - 1].trim() === "") {
    lines.pop();
  }
  return lines;
}

export function buildRecoveredTrade(
  ts: number,
  priceText: string,
  sizeText: string,
  side: TradeSide,
): RecoveredTrade | undefined {
  const price = Number(priceText);
  const size = Number(sizeText);
  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) {
    return undefined;
  }
  if (ts <= 0 || price <= 0 || size <= 0) {
    return undefined;
  }
  return { ts, price, size, side, priceText, sizeText };
}

export async function fetchText(url: string, fetchImpl: FetchLike): Promise<string> {
  const res = await fetchImpl(url, { method: "GET" });
  if (!res.ok) {
    const body = await safeText(res);
    throw new Error(`HTTP ${res.status} for ${url}: ${body.slice(0, 300)}`);
  }
  return res.text();
}

export async function fetchBuffer(url: string, fetchImpl: FetchLike): Promise<Buffer> {
  const res = await fetchImpl(url, { method: "GET" });
  if (!res.ok) {
    const body = await safeText(res);
    throw new Error(`HTTP ${res.status} for ${url}: ${body.slice(0, 300)}`);
  }
  const ab = await res.arrayBuffer();
  return Buffer.from(ab);
}

async function safeText(res: Response): Promise<string> {
  try {
    return await res.text();
  } catch {
    return "";
  }
}

export function extractFirstZipEntry(zipData: Buffer): string {
  const local = parseLocalHeader(zipData, 0);
  if (local.compressedSize > 0) {
    return decodeZipData(zipData, local);
  }

  const centralOffset = findCentralDirectory(zipData);
  if (centralOffset < 0) {
    throw new Error("Unable to parse zip central directory");
  }

  const compSize = zipData.readUInt32LE(centralOffset + 20);
  const localOffset = zipData.readUInt32LE(centralOffset + 42);
  const localFromCentral = parseLocalHeader(zipData, localOffset);
  const fixedLocal: LocalZipHeader = {
    method: localFromCentral.method,
    compressedSize: compSize,
    dataOffset: localFromCentral.dataOffset,
  };
  return decodeZipData(zipData, fixedLocal);
}

function parseLocalHeader(data: Buffer, offset: number): LocalZipHeader {
  if (offset + 30 > data.length) {
    throw new Error("Invalid zip local header (out of bounds)");
  }
  const sig = data.readUInt32LE(offset);
  if (sig !== 0x04034b50) {
    throw new Error("Invalid zip local header signature");
  }
  const method = data.readUInt16LE(offset + 8);
  const compressedSize = data.readUInt32LE(offset + 18);
  const fileNameLen = data.readUInt16LE(offset + 26);
  const extraLen = data.readUInt16LE(offset + 28);
  const dataOffset = offset + 30 + fileNameLen + extraLen;
  if (dataOffset > data.length) {
    throw new Error("Invalid zip data offset");
  }
  return { method, compressedSize, dataOffset };
}

function decodeZipData(data: Buffer, header: LocalZipHeader): string {
  const end = header.dataOffset + header.compressedSize;
  if (end > data.length) {
    throw new Error("Invalid zip compressed size");
  }
  const payload = data.subarray(header.dataOffset, end);
  if (header.method === 0) {
    return payload.toString("utf8");
  }
  if (header.method === 8) {
    return zlib.inflateRawSync(payload).toString("utf8");
  }
  throw new Error(`Unsupported zip compression method: ${header.method}`);
}

function findCentralDirectory(data: Buffer): number {
  for (let i = 0; i <= data.length - 4; i += 1) {
    if (data.readUInt32LE(i) === 0x02014b50) {
      return i;
    }
  }
  return -1;
}

export function gunzipToString(gzipData: Buffer): string {
  return zlib.gunzipSync(gzipData).toString("utf8");
}

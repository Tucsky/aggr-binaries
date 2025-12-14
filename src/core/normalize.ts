import path from "node:path";
import { parseLogicalStartTs } from "./dates.js";
import type { IndexedFile } from "./model.js";
import { Collector, QuoteCurrency } from "./model.js";

const BITGET_SYMBOL_CHANGE_TS = Date.UTC(2025, 10, 28, 0, 0, 0); // 2025-11-28
const POLONIEX_QUOTES = new Set<string>(Object.values(QuoteCurrency));

function normalizePoloniexSymbol(raw: string): string {
  if (!raw.includes("_")) return raw;
  const [first, second] = raw.split("_");
  if (!first || !second) return raw;

  if (POLONIEX_QUOTES.has(first) && !POLONIEX_QUOTES.has(second)) {
    return `${second}_${first}`;
  }

  return `${first}_${second}`;
}

function normalizeBitgetSymbol(raw: string, startTs?: number): string {
  const suffixMatch = raw.match(/_(UMCBL|DMCBL|CMCBL)$/);
  let symbol = suffixMatch ? raw.replace(/_(UMCBL|DMCBL|CMCBL)$/, "") : raw;
  const hasSpotSuffix = symbol.endsWith("-SPOT");

  if (hasSpotSuffix) return symbol;

  if (!suffixMatch && startTs !== undefined && startTs < BITGET_SYMBOL_CHANGE_TS) {
    return `${symbol}-SPOT`;
  }

  return symbol;
}

export function normalizeSymbol(exchange?: string, rawSymbol?: string, startTs?: number): string | undefined {
  if (!rawSymbol) return undefined;
  if (!exchange) return rawSymbol;
  if (exchange === "POLONIEX") return normalizePoloniexSymbol(rawSymbol.toUpperCase());
  if (exchange === "BITGET") return normalizeBitgetSymbol(rawSymbol.toUpperCase(), startTs);
  return rawSymbol;
}

function stripCompression(name: string): string {
  return name.replace(/\.gz$/i, "");
}

export function classifyPath(
  rootId: number,
  relativePath: string,
  collectorHint?: Collector,
): IndexedFile | null {
  const normalizedPath = path.sep === "/" ? relativePath : relativePath.split(path.sep).join("/");
  const segments = normalizedPath.split("/");
  if (segments.length < 1) return null;

  let collector: Collector | null = null;
  let offset = 0;

  const first = segments[0].toUpperCase();
  if (first === Collector.RAM || first === Collector.PI) {
    collector = first as Collector;
    offset = 1; // bucket starts at index 1
  } else if (collectorHint) {
    collector = collectorHint;
    offset = 0; // bucket starts at index 0 when root is inside collector
  } else {
    return null;
  }

  const fileName = segments[segments.length - 1];
  const ext = path.extname(fileName) || undefined;

  // logical structure: {collector?}/{bucket}/{exchange}/{symbol}/{file}
  const exchangeDir = segments[offset + 1];
  const symbolDir = segments[offset + 2];

  if (!exchangeDir || !symbolDir) return null;

  const baseName = stripCompression(fileName);
  const startTs = parseLogicalStartTs(baseName);
  if (startTs === undefined) return null;
  const exchange = exchangeDir.toUpperCase();
  const symbol = normalizeSymbol(exchange, symbolDir, startTs);
  if (!symbol) return null;

  return {
    rootId,
    relativePath: normalizedPath,
    collector,
    exchange,
    symbol,
    startTs,
    ext,
  };
}

import path from "node:path";
import { parseLegacyStartTs, parseLogicalStartTs } from "./dates.js";
import type { IndexedFile } from "./model.js";
import { Collector, Era, QuoteCurrency } from "./model.js";

const EXCHANGE_MAP: Record<string, string> = {
  bitfinex: "BITFINEX",
  binance: "BINANCE",
  okex: "OKEX",
  kraken: "KRAKEN",
  gdax: "COINBASE",
  poloniex: "POLONIEX",
  huobi: "HUOBI",
  bitstamp: "BITSTAMP",
  bitmex: "BITMEX",
  binance_futures: "BINANCE_FUTURES",
  deribit: "DERIBIT",
  ftx: "FTX",
  bybit: "BYBIT",
  hitbtc: "HITBTC",
};

const BITGET_SYMBOL_CHANGE_TS = Date.UTC(2025, 10, 28, 0, 0, 0); // 2025-11-28
const POLONIEX_QUOTES = new Set<string>(Object.values(QuoteCurrency));

export function normalizeExchange(raw?: string): string | undefined {
  if (!raw) return undefined;
  const mapped = EXCHANGE_MAP[raw.toLowerCase()];
  return mapped ?? raw.toUpperCase();
}

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
  const segsAfterCollector = segments.length - offset;

  // logical: {collector?}/{bucket}/{exchange}/{symbol}/{file}
  if (segsAfterCollector >= 4) {
    const exchangeDir = segments[offset + 1];
    const symbolDir = segments[offset + 2];
    const baseName = stripCompression(fileName);
    const startTs = parseLogicalStartTs(baseName);
    const exchange = normalizeExchange(exchangeDir);
    const symbol = normalizeSymbol(exchange, symbolDir, startTs);

    return {
      rootId,
      relativePath: normalizedPath,
      collector,
      era: Era.Logical,
      exchange,
      symbol,
      startTs,
      ext,
    };
  }

  // legacy: {collector?}/{bucket}/{file}
  if (segsAfterCollector >= 2) {
    const baseName = stripCompression(fileName);
    const underscoreIdx = baseName.indexOf("_");
    if (underscoreIdx === -1) return null;
    const symbolToken = baseName.slice(0, underscoreIdx);
    const dateToken = baseName.slice(underscoreIdx + 1);
    const startTs = parseLegacyStartTs(dateToken);

    return {
      rootId,
      relativePath: normalizedPath,
      collector,
      era: Era.Legacy,
      exchange: undefined,
      symbol: symbolToken || undefined,
      startTs,
      ext,
    };
  }

  return null;
}

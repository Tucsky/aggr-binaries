import {
  buildRecoveredTrade,
  collectUtcDays,
  extractFirstZipEntry,
  fetchBufferIfFound,
  forEachCsvLine,
  isTsWithinAnyWindow,
  mergeWindows,
  sortRecoveredTrades,
  summarizeBounds,
  toCoinbasePair,
} from "./common.js";
import type { AdapterRequest, FetchLike, GapWindow, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

const OKX_PUBLIC_INSTRUMENTS = "https://www.okx.com/api/v5/public/instruments";
const OKX_DIRECT_DAILY_TRADES_BASE = "https://static.okx.com/cdn/okex/traderecords/trades/daily";
const OKX_DIRECT_TRADES_START_TS = Date.UTC(2021, 8, 2, 0, 0, 0); // 2021-09-02
const DEBUG_ADAPTERS = process.env.AGGR_FIXGAPS_DEBUG_ADAPTERS === "1" || process.env.AGGR_FIXGAPS_DEBUG === "1";

const OKX_INSTRUMENT_TYPES = ["SPOT", "FUTURES", "SWAP"] as const;
type OkxInstrumentType = (typeof OKX_INSTRUMENT_TYPES)[number];

interface OkxInstrumentInfo {
  instId: string;
  instType: OkxInstrumentType;
  ctVal?: number;
  inverse: boolean;
}

export function createOkexAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: "okx-direct-trades",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      const bounds = summarizeBounds(windows);
      if (!bounds) return [];

      const instrument = await resolveInstrument(req.symbol, fetchImpl);
      if (!instrument) return [];

      const recovered = await recoverDirectTrades(instrument, windows, fetchImpl);
      return sortRecoveredTrades(recovered);
    },
  };
}

async function recoverDirectTrades(
  instrument: OkxInstrumentInfo,
  windows: GapWindow[],
  fetchImpl: FetchLike,
): Promise<RecoveredTrade[]> {
  const bounds = summarizeBounds(windows);
  if (!bounds) return [];

  const days = collectUtcDays(bounds);
  const trades: RecoveredTrade[] = [];
  let preCutoffDays = 0;
  let directAttemptDays = 0;
  let directMissDays = 0;

  for (const day of days) {
    const dayStart = Date.parse(`${day}T00:00:00.000Z`);
    if (!Number.isFinite(dayStart) || dayStart < OKX_DIRECT_TRADES_START_TS) {
      preCutoffDays += 1;
      continue;
    }

    const dayToken = day.replaceAll("-", "");
    const url = `${OKX_DIRECT_DAILY_TRADES_BASE}/${dayToken}/${instrument.instId}-trades-${day}.zip`;
    directAttemptDays += 1;

    let zipData: Buffer | undefined;
    try {
      zipData = await fetchBufferIfFound(url, fetchImpl);
    } catch {
      zipData = undefined;
    }
    if (!zipData) {
      directMissDays += 1;
      continue;
    }

    let csv = "";
    try {
      csv = extractFirstZipEntry(zipData);
    } catch {
      continue;
    }
    forEachCsvLine(csv, (line) => {
      const cols = line.split(",");
      const trade = parseDirectTradeRow(cols, instrument);
      if (!trade) return;
      if (!isTsWithinAnyWindow(trade.ts, windows)) return;
      trades.push(trade);
    });
  }

  if (DEBUG_ADAPTERS) {
    console.log(
      `[fixgaps/okex] symbol=${instrument.instId} days=${days.length} pre_cutoff=${preCutoffDays} direct_attempts=${directAttemptDays} direct_miss=${directMissDays} direct_trades=${trades.length}`,
    );
  }

  return trades;
}

function parseDirectTradeRow(cols: string[], instrument: OkxInstrumentInfo): RecoveredTrade | undefined {
  if (cols.length < 6) return undefined;
  const ts = Number(cols[5]);
  const side = parseSide(cols[2]);
  const priceText = cols[3] ?? "";
  const rawSizeText = cols[4] ?? "";
  const sizeText = normalizeSize(rawSizeText, priceText, instrument);
  return buildRecoveredTrade(ts, priceText, sizeText, side);
}

function normalizeSize(sizeText: string, priceText: string, instrument: OkxInstrumentInfo): string {
  if (instrument.instType === "SPOT" || instrument.ctVal === undefined || instrument.ctVal <= 0) {
    return sizeText;
  }

  const contracts = Number(sizeText);
  if (!Number.isFinite(contracts) || contracts <= 0) {
    return sizeText;
  }

  let normalized = contracts * instrument.ctVal;
  if (instrument.inverse) {
    const price = Number(priceText);
    if (!Number.isFinite(price) || price <= 0) {
      return sizeText;
    }
    normalized /= price;
  }

  if (!Number.isFinite(normalized) || normalized <= 0) {
    return sizeText;
  }
  return normalized.toString();
}

async function resolveInstrument(symbol: string, fetchImpl: FetchLike): Promise<OkxInstrumentInfo | undefined> {
  const candidates = buildSymbolCandidates(symbol);
  for (const candidate of candidates) {
    const types = inferInstrumentTypes(candidate);
    for (const instType of types) {
      const url = `${OKX_PUBLIC_INSTRUMENTS}?instType=${instType}&instId=${encodeURIComponent(candidate)}`;
      const rows = parseOkxData(await fetchJson(url, fetchImpl));
      for (const row of rows) {
        const parsed = parseInstrument(row);
        if (!parsed) continue;
        if (parsed.instId.toUpperCase() === candidate) {
          return parsed;
        }
      }
    }
  }
  return undefined;
}

function parseInstrument(row: unknown): OkxInstrumentInfo | undefined {
  if (!isRecord(row)) return undefined;
  const instIdRaw = row["instId"];
  const instTypeRaw = row["instType"];
  if (typeof instIdRaw !== "string" || typeof instTypeRaw !== "string") return undefined;

  const instType = asInstrumentType(instTypeRaw.toUpperCase());
  if (!instType) return undefined;

  const ctValRaw = Number(row["ctVal"]);
  const ctVal = Number.isFinite(ctValRaw) && ctValRaw > 0 ? ctValRaw : undefined;
  const ctTypeRaw = typeof row["ctType"] === "string" ? row["ctType"].toLowerCase() : "";

  return {
    instId: instIdRaw,
    instType,
    ctVal,
    inverse: ctTypeRaw === "inverse",
  };
}

function buildSymbolCandidates(symbol: string): string[] {
  const upper = symbol.trim().toUpperCase();
  if (!upper) return [];

  const noSwapSuffix = upper.endsWith("-SWAP") ? upper.slice(0, -5) : upper;
  const pair = toCoinbasePair(noSwapSuffix);
  const out = new Set<string>([
    upper,
    noSwapSuffix,
    pair,
    `${pair}-SWAP`,
  ]);

  return [...out].filter((candidate) => candidate.length > 0);
}

function inferInstrumentTypes(instId: string): OkxInstrumentType[] {
  if (instId.endsWith("-SWAP")) return ["SWAP"];
  if (/-\d{6}$/.test(instId) || /-\d{8}$/.test(instId)) return ["FUTURES"];
  return ["SPOT", "SWAP", "FUTURES"];
}

function asInstrumentType(raw: string): OkxInstrumentType | undefined {
  for (const instType of OKX_INSTRUMENT_TYPES) {
    if (instType === raw) return instType;
  }
  return undefined;
}

function parseSide(value: string): TradeSide {
  return value.trim().toLowerCase() === "buy" ? "buy" : "sell";
}

async function fetchJson(url: string, fetchImpl: FetchLike): Promise<unknown> {
  const res = await fetchImpl(url, { method: "GET" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return res.json();
}

function parseOkxData(payload: unknown): unknown[] {
  if (!isRecord(payload)) {
    throw new Error("Unexpected OKX payload shape");
  }

  const code = payload["code"];
  const msg = payload["msg"];
  if (typeof code === "string" && code !== "0") {
    const reason = typeof msg === "string" && msg ? msg : "unknown";
    throw new Error(`OKX error ${code}: ${reason}`);
  }

  const data = payload["data"];
  if (!Array.isArray(data)) return [];
  return data;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

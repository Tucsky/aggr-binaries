import { setTimeout as delay } from "node:timers/promises";
import { filterTradesByWindows, mergeWindows, sortRecoveredTrades, toKrakenPair } from "../common.js";
import { createKrakenDirectSource, type KrakenDirectSource } from "./direct.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "../types.js";
import { setFixgapsProgress } from "../../progress.js";

const MAX_PAGES = 4000;
const MAX_RATE_LIMIT_RETRIES = 6;
const RATE_LIMIT_BASE_DELAY_MS = 1000;
const RATE_LIMIT_MAX_DELAY_MS = 30000;
const API_TAIL_OVERLAP_MS = 86_400_000;
const DEBUG_ADAPTERS = process.env.AGGR_FIXGAPS_DEBUG_ADAPTERS === "1" || process.env.AGGR_FIXGAPS_DEBUG === "1";

interface CreateKrakenAdapterOptions {
  sleep?: (ms: number) => Promise<void>;
  directSource?: KrakenDirectSource;
}

export function createKrakenAdapter(
  fetchImpl: FetchLike,
  options: CreateKrakenAdapterOptions = {},
): TradeRecoveryAdapter {
  const sleep = options.sleep ?? delay;
  const directSource = options.directSource ?? createKrakenDirectSource(fetchImpl);

  return {
    name: "kraken-direct-plus-api",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      if (!windows.length) return [];

      let directTrades: RecoveredTrade[] = [];
      let directCoverageEndTs: number | undefined;
      try {
        const direct = await directSource.recover({
          symbol: req.symbol,
          windows,
        });
        directTrades = direct.trades;
        directCoverageEndTs = direct.coverageEndTs;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        const sanitized = sanitizeLogValue(message);
        console.log(`[fixgaps/kraken] direct_source_error symbol=${req.symbol} fallback=api error=${sanitized}`);
        directCoverageEndTs = undefined;
        if (DEBUG_ADAPTERS) {
          console.log(`[fixgaps/kraken] direct_source_error_debug symbol=${req.symbol} error=${sanitized}`);
        }
      }

      const apiWindows = selectApiWindows(windows, directCoverageEndTs);
      const apiTrades = apiWindows.length ? await recoverFromKrakenApi(req.symbol, apiWindows, fetchImpl, sleep) : [];

      const merged = sortRecoveredTrades([...directTrades, ...apiTrades]);
      return dedupeSortedTrades(filterTradesByWindows(merged, windows));
    },
  };
}

function selectApiWindows(windows: AdapterRequest["windows"], directCoverageEndTs: number | undefined): AdapterRequest["windows"] {
  if (directCoverageEndTs === undefined) {
    return windows;
  }

  const tailStart = directCoverageEndTs - API_TAIL_OVERLAP_MS;
  const selected: AdapterRequest["windows"] = [];
  for (const window of windows) {
    if (window.toTs <= tailStart) {
      continue;
    }
    const fromTs = window.fromTs > tailStart ? window.fromTs : tailStart;
    if (window.toTs <= fromTs) continue;
    selected.push({
      eventId: window.eventId,
      fromTs,
      toTs: window.toTs,
    });
  }
  return selected;
}

async function recoverFromKrakenApi(
  symbol: string,
  windows: AdapterRequest["windows"],
  fetchImpl: FetchLike,
  sleep: (ms: number) => Promise<void>,
): Promise<RecoveredTrade[]> {
  const pair = toKrakenPair(symbol);
  const sinceMs = Math.floor(Math.max(0, windows[0].fromTs));
  let since = BigInt(sinceMs) * 1_000_000n;
  const collected: RecoveredTrade[] = [];

  for (let page = 0; page < MAX_PAGES; page += 1) {
    const url = `https://api.kraken.com/0/public/Trades?pair=${encodeURIComponent(pair)}&count=1000&since=${since.toString()}`;
    const parsed = await fetchKrakenPage(url, fetchImpl, sleep);
    if (parsed.error.length) {
      throw new Error(`Kraken error: ${parsed.error.join(", ")}`);
    }

    const rows = parsed.rows;
    if (!rows.length) break;

    let maxTs = Number.NEGATIVE_INFINITY;
    for (const row of rows) {
      const trade = parseKrakenApiRow(row);
      if (!trade) continue;
      collected.push(trade);
      if (trade.ts > maxTs) maxTs = trade.ts;
    }

    const nextSince = BigInt(parsed.last);
    if (nextSince <= since) break;
    since = nextSince;

    if (maxTs >= windows[windows.length - 1].toTs) {
      break;
    }
  }

  return filterTradesByWindows(collected, windows);
}

async function fetchKrakenPage(
  url: string,
  fetchImpl: FetchLike,
  sleep: (ms: number) => Promise<void>,
): Promise<ParsedKrakenPayload> {
  for (let attempt = 1; attempt <= MAX_RATE_LIMIT_RETRIES; attempt += 1) {
    const payload = await fetchJson(url, fetchImpl);
    const parsed = parseKrakenPayload(payload);
    if (!isRateLimitedError(parsed.error)) {
      return parsed;
    }

    if (attempt >= MAX_RATE_LIMIT_RETRIES) {
      return parsed;
    }

    const delayMs = computeRateLimitDelay(attempt);
    setFixgapsProgress(
      `[fixgaps] waiting retry api.kraken.com attempt=${attempt + 1}/${MAX_RATE_LIMIT_RETRIES} delay=${delayMs}ms ...`,
    );
    await sleep(delayMs);
  }

  throw new Error("Unexpected Kraken retry loop exit");
}

async function fetchJson(url: string, fetchImpl: FetchLike): Promise<unknown> {
  const res = await fetchImpl(url, { method: "GET" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return res.json();
}

interface ParsedKrakenPayload {
  error: string[];
  rows: unknown[];
  last: string;
}

function parseKrakenPayload(payload: unknown): ParsedKrakenPayload {
  if (!isRecord(payload)) {
    throw new Error("Unexpected Kraken payload shape");
  }

  const errorRaw = payload["error"];
  const resultRaw = payload["result"];
  if (!Array.isArray(errorRaw)) {
    throw new Error("Malformed Kraken payload");
  }
  const error = errorRaw.filter((v): v is string => typeof v === "string");
  if (error.length) {
    return { error, rows: [], last: "0" };
  }
  if (!isRecord(resultRaw)) {
    throw new Error("Malformed Kraken payload");
  }

  let rows: unknown[] = [];
  for (const [key, value] of Object.entries(resultRaw)) {
    if (key === "last") continue;
    if (Array.isArray(value)) {
      rows = value;
      break;
    }
  }
  const lastRaw = resultRaw["last"];
  const last = typeof lastRaw === "string" ? lastRaw : String(lastRaw ?? "0");

  return { error, rows, last };
}

function parseKrakenApiRow(row: unknown): RecoveredTrade | undefined {
  if (!Array.isArray(row) || row.length < 4) return undefined;
  const priceText = String(row[0]);
  const sizeText = String(row[1]);
  const ts = Math.round(Number(row[2]) * 1000);
  const side = parseKrakenSide(String(row[3]));
  const price = Number(priceText);
  const size = Number(sizeText);
  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) return undefined;
  if (ts <= 0 || price <= 0 || size <= 0) return undefined;
  return { ts, price, size, side, priceText, sizeText };
}

function isRateLimitedError(errors: string[]): boolean {
  if (!errors.length) return false;
  for (const error of errors) {
    const lower = error.toLowerCase();
    if (lower.includes("too many requests") || lower.includes("rate limit")) {
      return true;
    }
  }
  return false;
}

function computeRateLimitDelay(attempt: number): number {
  const raw = RATE_LIMIT_BASE_DELAY_MS * (2 ** (attempt - 1));
  return raw > RATE_LIMIT_MAX_DELAY_MS ? RATE_LIMIT_MAX_DELAY_MS : raw;
}

function parseKrakenSide(value: string): TradeSide {
  return value === "b" ? "buy" : "sell";
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

function sanitizeLogValue(text: string): string {
  return text.replaceAll("\n", " ").replaceAll("\r", " ").trim().slice(0, 300);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

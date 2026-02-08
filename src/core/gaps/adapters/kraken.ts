import { setTimeout as delay } from "node:timers/promises";
import { buildRecoveredTrade, filterTradesByWindows, mergeWindows, toKrakenPair } from "./common.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";
import { setFixgapsProgress } from "../progress.js";

const MAX_PAGES = 4000;
const MAX_RATE_LIMIT_RETRIES = 6;
const RATE_LIMIT_BASE_DELAY_MS = 1000;
const RATE_LIMIT_MAX_DELAY_MS = 30000;

interface CreateKrakenAdapterOptions {
  sleep?: (ms: number) => Promise<void>;
}

export function createKrakenAdapter(
  fetchImpl: FetchLike,
  options: CreateKrakenAdapterOptions = {},
): TradeRecoveryAdapter {
  const sleep = options.sleep ?? delay;
  return {
    name: "kraken-public-trades",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      if (!windows.length) return [];

      const pair = toKrakenPair(req.symbol);
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
          const trade = parseKrakenRow(row);
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
    },
  };
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

function parseKrakenRow(row: unknown): RecoveredTrade | undefined {
  if (!Array.isArray(row) || row.length < 4) return undefined;
  const priceText = String(row[0]);
  const sizeText = String(row[1]);
  const ts = Math.round(Number(row[2]) * 1000);
  const side = parseKrakenSide(String(row[3]));
  return buildRecoveredTrade(ts, priceText, sizeText, side);
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

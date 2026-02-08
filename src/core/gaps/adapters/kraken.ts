import { buildRecoveredTrade, filterTradesByWindows, mergeWindows, toKrakenPair } from "./common.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

const MAX_PAGES = 4000;

export function createKrakenAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: "kraken-public-trades",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      if (!windows.length) return [];

      const pair = toKrakenPair(req.symbol);
      let since = BigInt(Math.max(0, windows[0].fromTs)) * 1_000_000n;
      const collected: RecoveredTrade[] = [];

      for (let page = 0; page < MAX_PAGES; page += 1) {
        const url = `https://api.kraken.com/0/public/Trades?pair=${encodeURIComponent(pair)}&count=1000&since=${since.toString()}`;
        const payload = await fetchJson(url, fetchImpl);
        const parsed = parseKrakenPayload(payload);
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
  if (!Array.isArray(errorRaw) || !isRecord(resultRaw)) {
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

  return { error: errorRaw.filter((v): v is string => typeof v === "string"), rows, last };
}

function parseKrakenRow(row: unknown): RecoveredTrade | undefined {
  if (!Array.isArray(row) || row.length < 4) return undefined;
  const priceText = String(row[0]);
  const sizeText = String(row[1]);
  const ts = Math.round(Number(row[2]) * 1000);
  const side = parseKrakenSide(String(row[3]));
  return buildRecoveredTrade(ts, priceText, sizeText, side);
}

function parseKrakenSide(value: string): TradeSide {
  return value === "b" ? "buy" : "sell";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

import { buildRecoveredTrade, filterTradesByWindows, mergeWindows, summarizeBounds, toCoinbasePair } from "./common.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

const PAGE_LIMIT = 1000;
const MAX_PAGES = 4000;

interface CoinbaseTickerTrade {
  trade_id: string;
  price: string;
  size: string;
  side: string;
  time: string;
}

export function createCoinbaseAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: "coinbase-trades-api",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      const bounds = summarizeBounds(windows);
      if (!bounds) return [];

      const pair = toCoinbasePair(req.symbol);
      const startSec = Math.floor(bounds.minTs / 1000);
      const endSec = Math.ceil(bounds.maxTs / 1000);

      const tickerUrl =
        `https://api.coinbase.com/api/v3/brokerage/market/products/${encodeURIComponent(pair)}/ticker` +
        `?limit=100&end=${endSec}&start=${startSec}`;
      const tickerPayload = await fetchJson(tickerUrl, fetchImpl);
      const tickerTrades = parseTickerTrades(tickerPayload);

      const collected: RecoveredTrade[] = [];
      let earliestTradeId: number | undefined;
      for (const trade of tickerTrades) {
        const parsed = parseCoinbaseTrade(trade.trade_id, trade.price, trade.size, trade.side, trade.time);
        if (!parsed) continue;
        collected.push(parsed);
        const id = Number(trade.trade_id);
        if (Number.isFinite(id)) {
          earliestTradeId = earliestTradeId === undefined ? id : Math.min(earliestTradeId, id);
        }
      }

      if (earliestTradeId === undefined) {
        return filterTradesByWindows(collected, windows);
      }

      let cursor = earliestTradeId;
      for (let page = 0; page < MAX_PAGES; page += 1) {
        const url =
          `https://api.exchange.coinbase.com/products/${encodeURIComponent(pair)}/trades` +
          `?limit=${PAGE_LIMIT}&after=${cursor}`;
        const payload = await fetchJson(url, fetchImpl);
        if (!Array.isArray(payload) || payload.length === 0) {
          break;
        }

        let minId = cursor;
        let oldestTs = Number.POSITIVE_INFINITY;
        for (const row of payload) {
          const parsed = parseExchangeTrade(row);
          if (!parsed) continue;
          collected.push(parsed.trade);
          if (parsed.tradeId < minId) minId = parsed.tradeId;
          if (parsed.trade.ts < oldestTs) oldestTs = parsed.trade.ts;
        }

        if (minId >= cursor) {
          break;
        }
        cursor = minId;
        if (oldestTs <= bounds.minTs) {
          break;
        }
        if (payload.length < PAGE_LIMIT) {
          break;
        }
      }

      return filterTradesByWindows(collected, windows);
    },
  };
}

function parseTickerTrades(payload: unknown): CoinbaseTickerTrade[] {
  if (!isRecord(payload)) return [];
  const trades = payload["trades"];
  if (!Array.isArray(trades)) return [];
  const out: CoinbaseTickerTrade[] = [];
  for (const row of trades) {
    if (!isRecord(row)) continue;
    const trade_id = row["trade_id"];
    const price = row["price"];
    const size = row["size"];
    const side = row["side"];
    const time = row["time"];
    if (
      typeof trade_id === "string" &&
      typeof price === "string" &&
      typeof size === "string" &&
      typeof side === "string" &&
      typeof time === "string"
    ) {
      out.push({ trade_id, price, size, side, time });
    }
  }
  return out;
}

function parseExchangeTrade(row: unknown): { trade: RecoveredTrade; tradeId: number } | undefined {
  if (!isRecord(row)) return undefined;
  const tradeIdRaw = row["trade_id"];
  const sideRaw = row["side"];
  const sizeRaw = row["size"];
  const priceRaw = row["price"];
  const timeRaw = row["time"];
  if (
    !Number.isFinite(Number(tradeIdRaw)) ||
    typeof sideRaw !== "string" ||
    typeof sizeRaw !== "string" ||
    typeof priceRaw !== "string" ||
    typeof timeRaw !== "string"
  ) {
    return undefined;
  }
  const trade = parseCoinbaseTrade(String(tradeIdRaw), priceRaw, sizeRaw, sideRaw, timeRaw);
  if (!trade) return undefined;
  return { trade, tradeId: Number(tradeIdRaw) };
}

function parseCoinbaseTrade(
  tradeId: string,
  priceText: string,
  sizeText: string,
  sideText: string,
  isoTime: string,
): RecoveredTrade | undefined {
  if (!tradeId) return undefined;
  const ts = Date.parse(isoTime);
  const side = parseSide(sideText);
  return buildRecoveredTrade(ts, priceText, sizeText, side);
}

function parseSide(sideText: string): TradeSide {
  return sideText.trim().toLowerCase() === "buy" ? "buy" : "sell";
}

async function fetchJson(url: string, fetchImpl: FetchLike): Promise<unknown> {
  const res = await fetchImpl(url, { method: "GET" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return res.json();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

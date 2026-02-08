import { createBinanceFuturesAdapter, createBinanceSpotAdapter } from "./binance.js";
import { createBitfinexAdapter } from "./bitfinex.js";
import { createBitmexAdapter } from "./bitmex.js";
import { createBybitAdapter } from "./bybit.js";
import { createCoinbaseAdapter } from "./coinbase.js";
import { createHuobiAdapter } from "./huobi.js";
import { createRateLimitedFetch } from "./http.js";
import { createKrakenAdapter } from "./kraken.js";
import { createKucoinAdapter } from "./kucoin.js";
import { createOkexAdapter } from "./okex.js";
import type { FetchLike, TradeRecoveryAdapter } from "./types.js";

export type { AdapterRequest, GapWindow, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

export interface AdapterRegistry {
  getAdapter(exchange: string): TradeRecoveryAdapter | undefined;
}

export function createDefaultAdapterRegistry(fetchImpl?: FetchLike): AdapterRegistry {
  const fetchFn = createRateLimitedFetch(fetchImpl ?? fetch);
  const okexAdapter = createOkexAdapter(fetchFn);
  const huobiAdapter = createHuobiAdapter(fetchFn);
  const byExchange = new Map<string, TradeRecoveryAdapter>([
    ["BINANCE", createBinanceSpotAdapter(fetchFn)],
    ["BINANCE_FUTURES", createBinanceFuturesAdapter(fetchFn)],
    ["BYBIT", createBybitAdapter(fetchFn)],
    ["KRAKEN", createKrakenAdapter(fetchFn)],
    ["BITFINEX", createBitfinexAdapter(fetchFn)],
    ["BITMEX", createBitmexAdapter(fetchFn)],
    ["OKEX", okexAdapter],
    ["OKX", okexAdapter],
    ["KUCOIN", createKucoinAdapter(fetchFn)],
    ["HUOBI", huobiAdapter],
    ["HTX", huobiAdapter],
    ["COINBASE", createCoinbaseAdapter(fetchFn)],
  ]);

  return {
    getAdapter(exchange: string): TradeRecoveryAdapter | undefined {
      return byExchange.get(exchange.toUpperCase());
    },
  };
}

export function createAdapterRegistry(map: Record<string, TradeRecoveryAdapter>): AdapterRegistry {
  const byExchange = new Map<string, TradeRecoveryAdapter>();
  for (const [exchange, adapter] of Object.entries(map)) {
    byExchange.set(exchange.toUpperCase(), adapter);
  }
  return {
    getAdapter(exchange: string): TradeRecoveryAdapter | undefined {
      return byExchange.get(exchange.toUpperCase());
    },
  };
}

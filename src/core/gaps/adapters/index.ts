import { createBinanceFuturesAdapter, createBinanceSpotAdapter } from "./binance.js";
import { createBitfinexAdapter } from "./bitfinex.js";
import { createBybitAdapter } from "./bybit.js";
import { createCoinbaseAdapter } from "./coinbase.js";
import { createRateLimitedFetch } from "./http.js";
import { createKrakenAdapter } from "./kraken.js";
import type { FetchLike, TradeRecoveryAdapter } from "./types.js";

export type { AdapterRequest, GapWindow, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

export interface AdapterRegistry {
  getAdapter(exchange: string): TradeRecoveryAdapter | undefined;
}

export function createDefaultAdapterRegistry(fetchImpl?: FetchLike): AdapterRegistry {
  const fetchFn = createRateLimitedFetch(fetchImpl ?? fetch);
  const byExchange = new Map<string, TradeRecoveryAdapter>([
    ["BINANCE", createBinanceSpotAdapter(fetchFn)],
    ["BINANCE_FUTURES", createBinanceFuturesAdapter(fetchFn)],
    ["BYBIT", createBybitAdapter(fetchFn)],
    ["KRAKEN", createKrakenAdapter(fetchFn)],
    ["BITFINEX", createBitfinexAdapter(fetchFn)],
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

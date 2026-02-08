import {
  buildRecoveredTrade,
  collectUtcDays,
  extractFirstZipEntry,
  fetchBuffer,
  filterTradesByWindows,
  parseCsvLines,
  summarizeBounds,
  normalizeSymbolToken,
} from "./common.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

interface BinanceAdapterConfig {
  name: string;
  datasetPath: string;
}

const SPOT_CONFIG: BinanceAdapterConfig = {
  name: "binance-spot",
  datasetPath: "spot/daily/trades",
};

const FUTURES_CONFIG: BinanceAdapterConfig = {
  name: "binance-futures-um",
  datasetPath: "futures/um/daily/trades",
};

export function createBinanceSpotAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return createBinanceAdapter(SPOT_CONFIG, fetchImpl);
}

export function createBinanceFuturesAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return createBinanceAdapter(FUTURES_CONFIG, fetchImpl);
}

function createBinanceAdapter(config: BinanceAdapterConfig, fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: config.name,
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const bounds = summarizeBounds(req.windows);
      if (!bounds) return [];

      const symbol = normalizeSymbolToken(req.symbol);
      const days = collectUtcDays(bounds);
      const collected: RecoveredTrade[] = [];

      for (const day of days) {
        const url = `https://data.binance.vision/data/${config.datasetPath}/${symbol}/${symbol}-trades-${day}.zip`;
        const zipData = await fetchBuffer(url, fetchImpl);
        const csv = extractFirstZipEntry(zipData);
        const lines = parseCsvLines(csv);
        for (const line of lines) {
          if (!line) continue;
          if (isHeader(line)) continue;
          const cols = line.split(",");
          if (cols.length < 6) continue;
          const ts = Number(cols[4]);
          const priceText = cols[1];
          const sizeText = cols[2];
          const side = parseBuyerMaker(cols[5]);
          const trade = buildRecoveredTrade(ts, priceText, sizeText, side);
          if (trade) {
            collected.push(trade);
          }
        }
      }

      return filterTradesByWindows(collected, req.windows);
    },
  };
}

function isHeader(line: string): boolean {
  const c0 = line.charCodeAt(0);
  return c0 < 48 || c0 > 57;
}

function parseBuyerMaker(value: string): TradeSide {
  return value.trim().toLowerCase() === "true" ? "sell" : "buy";
}

import {
  buildRecoveredTrade,
  collectUtcDays,
  fetchBuffer,
  filterTradesByWindows,
  gunzipToString,
  normalizeSymbolToken,
  parseCsvLines,
  summarizeBounds,
} from "./common.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

export function createBybitAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: "bybit-public-trading",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const bounds = summarizeBounds(req.windows);
      if (!bounds) return [];

      const symbol = normalizeSymbolToken(req.symbol);
      const days = collectUtcDays(bounds);
      const trades: RecoveredTrade[] = [];

      for (const day of days) {
        const url = `https://public.bybit.com/trading/${symbol}/${symbol}${day}.csv.gz`;
        const gzipData = await fetchBuffer(url, fetchImpl);
        const csv = gunzipToString(gzipData);
        const lines = parseCsvLines(csv);
        for (const line of lines) {
          if (!line || line.startsWith("timestamp,")) continue;
          const cols = line.split(",");
          if (cols.length < 5) continue;
          const ts = Math.round(Number(cols[0]) * 1000);
          const side = parseSide(cols[2]);
          const sizeText = cols[3];
          const priceText = cols[4];
          const trade = buildRecoveredTrade(ts, priceText, sizeText, side);
          if (trade) {
            trades.push(trade);
          }
        }
      }

      return filterTradesByWindows(trades, req.windows);
    },
  };
}

function parseSide(value: string): TradeSide {
  return value.trim().toLowerCase() === "buy" ? "buy" : "sell";
}

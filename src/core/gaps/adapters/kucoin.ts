import {
  buildRecoveredTrade,
  collectUtcDays,
  extractFirstZipEntry,
  fetchBufferIfFound,
  forEachCsvLine,
  isTsWithinAnyWindow,
  mergeWindows,
  normalizeSymbolToken,
  sortRecoveredTrades,
  summarizeBounds,
} from "./common.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

const KUCOIN_SPOT_DAILY_BASE = "https://historical-data.kucoin.com/data/spot/daily/trades";

export function createKucoinAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: "kucoin-spot-daily-trades",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      const bounds = summarizeBounds(windows);
      if (!bounds) return [];

      const symbol = normalizeSymbolToken(req.symbol);
      const days = collectUtcDays(bounds);
      const trades: RecoveredTrade[] = [];

      for (const day of days) {
        const url = `${KUCOIN_SPOT_DAILY_BASE}/${symbol}/${symbol}-trades-${day}.zip`;
        const zipData = await fetchBufferIfFound(url, fetchImpl);
        if (!zipData) continue;

        const csv = extractKucoinZip(zipData);
        forEachCsvLine(csv, (line) => {
          if (isHeader(line)) return;
          const cols = line.split(",");
          if (cols.length < 5) return;

          const ts = Number(cols[1]);
          if (!isTsWithinAnyWindow(ts, windows)) return;
          const side = parseSide(cols[4]);
          const trade = buildRecoveredTrade(ts, cols[2] ?? "", cols[3] ?? "", side);
          if (trade) {
            trades.push(trade);
          }
        });
      }

      return sortRecoveredTrades(trades);
    },
  };
}

function extractKucoinZip(zipData: Buffer): string {
  return extractFirstZipEntry(zipData);
}

function isHeader(line: string): boolean {
  const c0 = line.charCodeAt(0);
  return c0 < 48 || c0 > 57;
}

function parseSide(value: string | undefined): TradeSide {
  return (value ?? "").trim().toLowerCase() === "buy" ? "buy" : "sell";
}

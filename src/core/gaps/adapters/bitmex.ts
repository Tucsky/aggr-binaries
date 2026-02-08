import {
  buildRecoveredTrade,
  collectUtcDays,
  fetchBufferIfFound,
  forEachCsvLine,
  gunzipToString,
  isTsWithinAnyWindow,
  mergeWindows,
  normalizeSymbolToken,
  sortRecoveredTrades,
  summarizeBounds,
} from "./common.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

const BITMEX_DATASET_BASE = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade";

export function createBitmexAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: "bitmex-public-trade",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      const bounds = summarizeBounds(windows);
      if (!bounds) return [];

      const symbol = normalizeSymbolToken(req.symbol);
      const days = collectUtcDays(bounds);
      const trades: RecoveredTrade[] = [];

      for (const day of days) {
        const compactDay = day.replaceAll("-", "");
        const url = `${BITMEX_DATASET_BASE}/${compactDay}.csv.gz`;
        const gzipData = await fetchBufferIfFound(url, fetchImpl);
        if (!gzipData) continue;
        const csv = gunzipToString(gzipData);
        forEachCsvLine(csv, (line) => {
          if (!line || isHeader(line)) return;
          const cols = line.split(",");
          if (cols.length < 9) return;
          if (normalizeSymbolToken(cols[1] ?? "") !== symbol) return;

          const ts = parseBitmexTimestamp(cols[0]);
          if (!isTsWithinAnyWindow(ts, windows)) return;
          const side = parseBitmexSide(cols[2]);
          const priceText = cols[4] ?? "";
          const sizeText = parseBitmexSize(cols);
          const trade = buildRecoveredTrade(ts, priceText, sizeText, side);
          if (trade) {
            trades.push(trade);
          }
        });
      }

      return sortRecoveredTrades(trades);
    },
  };
}

function isHeader(line: string): boolean {
  const c0 = line.charCodeAt(0);
  return c0 < 48 || c0 > 57;
}

function parseBitmexTimestamp(raw: string | undefined): number {
  if (!raw) return NaN;
  const trimmed = raw.trim();
  if (!trimmed) return NaN;
  const dPos = trimmed.indexOf("D");
  const iso = dPos === -1 ? trimmed : `${trimmed.slice(0, dPos)}T${trimmed.slice(dPos + 1)}`;
  return Date.parse(iso.endsWith("Z") ? iso : `${iso}Z`);
}

function parseBitmexSide(value: string | undefined): TradeSide {
  return (value ?? "").trim().toLowerCase() === "buy" ? "buy" : "sell";
}

function parseBitmexSize(cols: string[]): string {
  if (cols[8] && cols[8].trim() !== "") {
    return cols[8];
  }
  return cols[3] ?? "";
}

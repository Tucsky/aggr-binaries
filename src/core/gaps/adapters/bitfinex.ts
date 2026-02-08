import { buildRecoveredTrade, filterTradesByWindows, mergeWindows, toBitfinexPair } from "./common.js";
import type { AdapterRequest, FetchLike, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

const PAGE_LIMIT = 1000;
const MAX_PAGES_PER_WINDOW = 2000;
const DEBUG_ADAPTERS = process.env.AGGR_FIXGAPS_DEBUG_ADAPTERS === "1" || process.env.AGGR_FIXGAPS_DEBUG === "1";

export function createBitfinexAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: "bitfinex-hist-trades",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      if (!windows.length) return [];

      const symbol = toBitfinexPair(req.symbol);
      const trades: RecoveredTrade[] = [];

      for (const window of windows) {
        let start = window.fromTs + 1;
        let windowPages = 0;
        let windowTrades = 0;
        if (DEBUG_ADAPTERS) {
          console.log(
            `[fixgaps/bitfinex] window_start symbol=${symbol} from=${window.fromTs} to=${window.toTs} start=${start}`,
          );
        }
        for (let page = 0; page < MAX_PAGES_PER_WINDOW; page += 1) {
          const url =
            `https://api-pub.bitfinex.com/v2/trades/${encodeURIComponent(symbol)}/hist` +
            `?start=${start}&end=${window.toTs}&limit=${PAGE_LIMIT}&sort=1`;
          const payload = await fetchJson(url, fetchImpl);
          if (!Array.isArray(payload) || payload.length === 0) {
            if (DEBUG_ADAPTERS) {
              console.log(
                `[fixgaps/bitfinex] window_stop symbol=${symbol} reason=empty_page pages=${windowPages} trades=${windowTrades}`,
              );
            }
            break;
          }
          windowPages += 1;

          let lastTs = start;
          let parsedTrades = 0;
          for (const row of payload) {
            if (!Array.isArray(row) || row.length < 4) continue;
            const ts = Number(row[1]);
            const amountText = String(row[2]);
            const priceText = String(row[3]);
            const sizeText = amountText.startsWith("-") ? amountText.slice(1) : amountText;
            const side = parseAmountSide(amountText);
            const trade = buildRecoveredTrade(ts, priceText, sizeText, side);
            if (!trade) continue;
            trades.push(trade);
            parsedTrades += 1;
            if (ts > lastTs) lastTs = ts;
          }
          windowTrades += parsedTrades;
          if (DEBUG_ADAPTERS && (windowPages === 1 || windowPages % 10 === 0 || payload.length < PAGE_LIMIT)) {
            console.log(
              `[fixgaps/bitfinex] page symbol=${symbol} page=${windowPages} rows=${payload.length} parsed=${parsedTrades} lastTs=${lastTs}`,
            );
          }

          if (lastTs < start) {
            if (DEBUG_ADAPTERS) {
              console.log(
                `[fixgaps/bitfinex] window_stop symbol=${symbol} reason=non_advancing_cursor pages=${windowPages} trades=${windowTrades}`,
              );
            }
            break;
          }
          start = lastTs + 1;
          if (start >= window.toTs) {
            if (DEBUG_ADAPTERS) {
              console.log(
                `[fixgaps/bitfinex] window_stop symbol=${symbol} reason=reached_window_end pages=${windowPages} trades=${windowTrades}`,
              );
            }
            break;
          }
          if (payload.length < PAGE_LIMIT) {
            if (DEBUG_ADAPTERS) {
              console.log(
                `[fixgaps/bitfinex] window_stop symbol=${symbol} reason=short_page pages=${windowPages} trades=${windowTrades}`,
              );
            }
            break;
          }
        }
        if (DEBUG_ADAPTERS) {
          console.log(`[fixgaps/bitfinex] window_done symbol=${symbol} pages=${windowPages} trades=${windowTrades}`);
        }
      }

      return filterTradesByWindows(trades, windows);
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

function parseAmountSide(amountText: string): TradeSide {
  return amountText.startsWith("-") ? "sell" : "buy";
}

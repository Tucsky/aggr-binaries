import {
  buildRecoveredTrade,
  extractFirstZipEntry,
  formatUtcDay,
  fetchBufferIfFound,
  forEachCsvLine,
  isTsWithinAnyWindow,
  mergeWindows,
  normalizeSymbolToken,
  sortRecoveredTrades,
  summarizeBounds,
  toCoinbasePair,
} from "./common.js";
import type { AdapterRequest, FetchLike, GapWindow, RecoveredTrade, TradeRecoveryAdapter, TradeSide } from "./types.js";

const HTX_TRADES_BASE = "https://www.htx.com/data/data/trades";
const HTX_DAY_SHIFT_MS = 8 * 60 * 60 * 1000;

type HuobiDatasetKind = "spot" | "linear-swap";

interface HuobiDatasetCandidate {
  kind: HuobiDatasetKind;
  symbol: string;
}

interface HuobiRecoveryResult {
  hadFile: boolean;
  trades: RecoveredTrade[];
}

export function createHuobiAdapter(fetchImpl: FetchLike): TradeRecoveryAdapter {
  return {
    name: "huobi-direct-daily-trades",
    async recover(req: AdapterRequest): Promise<RecoveredTrade[]> {
      const windows = mergeWindows(req.windows);
      const bounds = summarizeBounds(windows);
      if (!bounds) return [];

      const days = collectShiftedDays(bounds.minTs, bounds.maxTs, HTX_DAY_SHIFT_MS);
      const candidates = buildCandidates(req.symbol);
      for (const candidate of candidates) {
        const recovered = await recoverCandidate(candidate, days, windows, fetchImpl);
        if (recovered.hadFile) {
          return sortRecoveredTrades(recovered.trades);
        }
      }

      return [];
    },
  };
}

function buildCandidates(symbol: string): HuobiDatasetCandidate[] {
  const raw = symbol.trim().toUpperCase();
  const noSwapSuffix = raw.endsWith("-SWAP") ? raw.slice(0, -5) : raw;
  const compact = normalizeSymbolToken(noSwapSuffix);
  const linear = noSwapSuffix.includes("-") ? noSwapSuffix : toCoinbasePair(compact);
  if (noSwapSuffix.includes("-")) {
    return [{ kind: "linear-swap", symbol: linear }];
  }

  const out: HuobiDatasetCandidate[] = [{ kind: "spot", symbol: compact }];
  if (linear.includes("-")) {
    out.push({ kind: "linear-swap", symbol: linear });
  }
  return out;
}

async function recoverCandidate(
  candidate: HuobiDatasetCandidate,
  days: string[],
  windows: GapWindow[],
  fetchImpl: FetchLike,
): Promise<HuobiRecoveryResult> {
  const trades: RecoveredTrade[] = [];
  let hadFile = false;

  for (const day of days) {
    const url = buildDailyUrl(candidate, day);
    const zipData = await fetchBufferIfFound(url, fetchImpl);
    if (!zipData) continue;
    hadFile = true;

    const csv = extractFirstZipEntry(zipData);
    forEachCsvLine(csv, (line) => {
      if (isHeader(line)) return;
      const cols = line.split(",");
      if (candidate.kind === "spot") {
        const parsed = parseSpotTrade(cols);
        if (!parsed || !isTsWithinAnyWindow(parsed.ts, windows)) return;
        trades.push(parsed);
        return;
      }
      const parsed = parseLinearTrade(cols);
      if (!parsed || !isTsWithinAnyWindow(parsed.ts, windows)) return;
      trades.push(parsed);
    });
  }

  return { hadFile, trades };
}

function buildDailyUrl(candidate: HuobiDatasetCandidate, day: string): string {
  const folder = candidate.kind === "spot" ? "spot/daily" : "linear-swap/daily";
  return `${HTX_TRADES_BASE}/${folder}/${candidate.symbol}/${candidate.symbol}-trades-${day}.zip`;
}

function parseSpotTrade(cols: string[]): RecoveredTrade | undefined {
  if (cols.length < 5) return undefined;
  const ts = Number(cols[1]);
  const priceText = cols[2] ?? "";
  const sizeText = cols[3] ?? "";
  const side = parseSide(cols[4]);
  return buildRecoveredTrade(ts, priceText, sizeText, side);
}

function parseLinearTrade(cols: string[]): RecoveredTrade | undefined {
  if (cols.length < 5) return undefined;
  const ts = Number(cols[1]);
  const priceText = cols[2] ?? "";
  const contractSizeText = cols[3] ?? "";
  const baseSizeText = cols[4] ?? "";
  const sizeText = Number.isFinite(Number(baseSizeText)) && Number(baseSizeText) > 0 ? baseSizeText : contractSizeText;
  const side = parseSide(cols[cols.length - 1]);
  return buildRecoveredTrade(ts, priceText, sizeText, side);
}

function collectShiftedDays(minTs: number, maxTs: number, shiftMs: number): string[] {
  const dayMs = 86_400_000;
  const startDay = Math.floor((minTs + shiftMs) / dayMs);
  const endDay = Math.floor((maxTs + shiftMs) / dayMs);
  const out: string[] = [];
  for (let day = startDay; day <= endDay; day += 1) {
    out.push(formatUtcDay(day * dayMs));
  }
  return out;
}

function parseSide(value: string | undefined): TradeSide {
  return (value ?? "").trim().toLowerCase() === "buy" ? "buy" : "sell";
}

function isHeader(line: string): boolean {
  const c0 = line.charCodeAt(0);
  return c0 < 48 || c0 > 57;
}

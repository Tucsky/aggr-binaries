export interface ChartLegendCandle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  buyVol: number;
  sellVol: number;
  liqBuy: number;
  liqSell: number;
}

export function toNonZero(value: number): number | null {
  return value === 0 ? null : value;
}

export function isGapCandle(candle: ChartLegendCandle): boolean {
  return candle.open === 0 || candle.high === 0 || candle.low === 0 || candle.close === 0;
}

export function findCandleAtOrBefore(
  points: readonly ChartLegendCandle[],
  timeMs: number,
): ChartLegendCandle | null {
  let lo = 0;
  let hi = points.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const midTime = points[mid].time;
    if (midTime === timeMs) return points[mid];
    if (midTime < timeMs) lo = mid + 1;
    else hi = mid - 1;
  }
  return hi >= 0 ? points[hi] : null;
}

export function formatPriceLegend(candle: ChartLegendCandle | null): string {
  if (!candle || isGapCandle(candle)) return "na";
  return `O:${candle.open.toFixed(1)} H:${candle.high.toFixed(1)} L:${candle.low.toFixed(1)} C:${candle.close.toFixed(1)}`;
}

export function formatVolumeLegend(candle: ChartLegendCandle | null): string {
  if (!candle) return "na";
  const total = candle.buyVol + candle.sellVol;
  const delta = Math.abs(candle.buyVol - candle.sellVol);
  return `${formatCompact(total)} | ${formatCompact(delta)}`;
}

export function formatLiquidationLegend(candle: ChartLegendCandle | null): string {
  if (!candle) return "na";
  const longText = candle.liqSell > 0 ? formatCompact(candle.liqSell) : "na";
  const shortText = candle.liqBuy > 0 ? formatCompact(candle.liqBuy) : "na";
  return `${longText} | ${shortText}`;
}

function formatCompact(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${trimFixed(value / 1_000_000_000)} B`;
  if (abs >= 1_000_000) return `${trimFixed(value / 1_000_000)} M`;
  if (abs >= 1_000) return `${trimFixed(value / 1_000)} K`;
  return trimFixed(value);
}

function trimFixed(value: number): string {
  const text = value.toFixed(1);
  return text.endsWith(".0") ? text.slice(0, -2) : text;
}

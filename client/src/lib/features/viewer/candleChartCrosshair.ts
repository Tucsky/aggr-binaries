import { findCandleAtOrBefore } from "../../../../../src/shared/chartLegend.js";
import type { Candle } from "./types.js";

export interface ChartCrosshairTarget {
  snappedTs: number;
  timeSec: number;
  price: number;
}

/**
 * Resolves a stable chart crosshair target from a requested UTC timestamp.
 * Returns null when no candle is loaded at/before the timestamp or when no finite price can be derived.
 */
export function resolveChartCrosshairTarget(
  points: readonly Candle[],
  requestedTs: number | null,
): ChartCrosshairTarget | null {
  if (requestedTs === null || !Number.isFinite(requestedTs)) return null;
  const candle = findCandleAtOrBefore(points, requestedTs);
  if (!candle) return null;
  const price = resolveChartCrosshairPrice(candle);
  if (price === null) return null;
  return {
    snappedTs: candle.time,
    timeSec: Math.floor(candle.time / 1000),
    price,
  };
}

/**
 * Chooses the first finite positive price from close/open/high/low for crosshair placement.
 */
export function resolveChartCrosshairPrice(
  candle: Pick<Candle, "close" | "open" | "high" | "low">,
): number | null {
  const candidates = [candle.close, candle.open, candle.high, candle.low];
  for (let i = 0; i < candidates.length; i += 1) {
    const value = candidates[i];
    if (Number.isFinite(value) && value > 0) return value;
  }
  return null;
}

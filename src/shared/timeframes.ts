export const COMMON_TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"] as const;

export type CommonTimeframe = (typeof COMMON_TIMEFRAMES)[number];

export function parseTimeframeMs(tf: string): number | undefined {
  const m = tf.trim().toLowerCase().match(/^(\d+)([smhd])$/);
  if (!m) return undefined;
  const n = Number(m[1]);
  switch (m[2]) {
    case "s":
      return n * 1000;
    case "m":
      return n * 60_000;
    case "h":
      return n * 3_600_000;
    case "d":
      return n * 86_400_000;
    default:
      return undefined;
  }
}

export function sortTimeframes(values: string[]): string[] {
  return Array.from(new Set(values))
    .map((v) => ({ v, ms: parseTimeframeMs(v) ?? Number.POSITIVE_INFINITY }))
    .sort((a, b) => a.ms - b.ms || a.v.localeCompare(b.v))
    .map((entry) => entry.v);
}

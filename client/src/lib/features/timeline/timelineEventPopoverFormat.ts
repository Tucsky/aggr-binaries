export { formatElapsedDhms } from "../../../../../src/shared/elapsed.js";

export function formatEstimatedMiss(gapMiss: number | null): string {
  if (gapMiss === null || !Number.isFinite(gapMiss)) return "n/a";
  const miss = Math.max(0, Math.round(gapMiss)) * 2 // we multiply by 2 because right now estimations are ALWAYS too low
  return `~${formatCompactCount(miss)}`;
}

export function formatRecoveredCount(recoveredCount: number | null | undefined): string {
  if (recoveredCount === null || recoveredCount === undefined || !Number.isFinite(recoveredCount)) return "n/a";
  return `${formatCompactCount(Math.max(0, Math.round(recoveredCount)))}`;
}

function formatCompactCount(value: number): string {
  if (value >= 1_000_000) return formatCompactWithSuffix(value, 1_000_000, "m");
  if (value >= 1_000) return formatCompactWithSuffix(value, 1_000, "k");
  return String(value);
}

function formatCompactWithSuffix(value: number, unit: number, suffix: "k" | "m"): string {
  const whole = Math.floor(value / unit);
  if (whole >= 10) return `${whole}${suffix}`;
  const scaledTenths = Math.round((value * 10) / unit);
  const integer = Math.floor(scaledTenths / 10);
  const decimal = scaledTenths % 10;
  if (integer >= 10) return `${integer}${suffix}`;
  if (decimal === 0) return `${integer}${suffix}`;
  return `${integer}.${decimal}${suffix}`;
}

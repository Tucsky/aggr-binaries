export function formatElapsedDhms(ms: number | null): string {
  if (ms === null || !Number.isFinite(ms) || ms <= 0) return "0s";
  const totalSeconds = Math.floor(ms / 1000);
  const days = Math.floor(totalSeconds / 86_400);
  const daySeconds = totalSeconds % 86_400;
  const hours = Math.floor(daySeconds / 3_600);
  const minutes = Math.floor((daySeconds % 3_600) / 60);
  const seconds = daySeconds % 60;
  const parts: string[] = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  if (seconds > 0 || parts.length === 0) parts.push(`${seconds}s`);
  return parts.join(" ");
}

export function formatEstimatedMissRange(gapMiss: number | null): string {
  if (gapMiss === null || !Number.isFinite(gapMiss)) return "n/a";
  const base = Math.max(0, Math.round(gapMiss));
  const top = base * 2;
  return `${formatCompactCount(base)} - ${formatCompactCount(top)}`;
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

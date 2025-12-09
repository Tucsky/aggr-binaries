const lastSundayCache = new Map<number, number>();

// month = 1..12
function lastSundayOfMonth(year: number, month: number): number {
  const key = year * 100 + month;
  const cached = lastSundayCache.get(key);
  if (cached !== undefined) return cached;

  // Date.UTC: month is 0-based; using `month` as "next month" and day 0 gives last day of the desired month.
  // e.g. month=3 -> Date.UTC(year, 3, 0) = 2019-03-31T00:00Z
  const lastDay = new Date(Date.UTC(year, month, 0));
  const dow = lastDay.getUTCDay();           // 0=Sun..6=Sat
  const lastSunday = lastDay.getUTCDate() - dow;

  lastSundayCache.set(key, lastSunday);
  return lastSunday;
}

// Return offset in minutes for Europe/Paris at given *local* date/time.
function getParisOffsetMinutes(
  year: number,
  month: number, // 1..12
  day: number,
  hour: number   // 0..23
): number {
  const lastSundayMarch = lastSundayOfMonth(year, 3);  // March
  const lastSundayOct   = lastSundayOfMonth(year, 10); // October

  // Winter time: UTC+1
  if (month < 3 || month > 10) return 60;

  // Summer time: UTC+2
  if (month > 3 && month < 10) return 120;

  // March transition day (DST starts)
  if (month === 3) {
    if (day < lastSundayMarch) return 60;
    if (day > lastSundayMarch) return 120;
    // day === lastSundayMarch:
    // DST starts at 02:00 local → from 02:00 inclusive we are UTC+2
    return hour < 2 ? 60 : 120;
  }

  // October transition day (DST ends)
  if (month === 10) {
    if (day < lastSundayOct) return 120;
    if (day > lastSundayOct) return 60;
    // day === lastSundayOct:
    // DST ends at 03:00 local → from 03:00 inclusive we are back to UTC+1
    return hour < 3 ? 120 : 60;
  }

  // Fallback, shouldn't be reached
  return 60;
}

function parisLocalToUtcMs(
  year: number,
  month: number, // 1..12
  day: number,
  hour: number   // 0..23
): number {
  const offsetMinutes = getParisOffsetMinutes(year, month, day, hour);
  // Date.UTC treats the inputs as UTC; our *local* time = UTC + offset.
  // So to get the real UTC ms, subtract the offset.
  const pseudoUtcMs = Date.UTC(year, month - 1, day, hour);
  return pseudoUtcMs - offsetMinutes * 60_000;
}

export function parseLegacyStartTs(dateToken: string): number | undefined {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:-(\d{2}))?$/.exec(dateToken);
  if (!m) return;
  const [, yStr, moStr, dStr, hStr] = m;
  const year  = Number(yStr);
  const month = Number(moStr);
  const day   = Number(dStr);
  const hour  = hStr ? Number(hStr) : 0;
  if (!hStr) {
    return Date.UTC(year, month - 1, day, 0, 0, 0)
  }

  if (
    !Number.isFinite(year)  ||
    !Number.isFinite(month) ||
    !Number.isFinite(day)   ||
    !Number.isFinite(hour)
  ) {
    return undefined;
  }

  return parisLocalToUtcMs(year, month, day, hour);
}

export function parseLogicalStartTs(fileNameOrToken: string): number | undefined {
  const token = fileNameOrToken.replace(/\.[^.]+$/, '');

  const m = /^(\d{4})-(\d{2})-(\d{2})(?:-(\d{2}))?$/.exec(token);
  if (!m) return;
  const [, yStr, moStr, dStr, hStr] = m;

  const year  = Number(yStr);
  const month = Number(moStr);
  const day   = Number(dStr);
  const hour  = hStr ? Number(hStr) : 0;

  if (
    !Number.isFinite(year)  ||
    !Number.isFinite(month) ||
    !Number.isFinite(day)   ||
    !Number.isFinite(hour)
  ) {
    return undefined;
  }

  return Date.UTC(year, month - 1, day, hour);
}

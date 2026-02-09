const MAX_DATE_MS = 8.64e15;

const DMY_RE =
  /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s*,?\s*(\d{1,2}|--)(?::(\d{1,2}|--))?(?::(\d{1,2}|--))?)?$/;
const ISO_RE =
  /^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{1,2})(?::(\d{1,2}))?(?::(\d{1,2}))?(?:\.(\d{1,3}))?)?(?:\s*(Z|[+\-]\d{2}(?::?\d{2})?))?$/i;
const NUMERIC_RE = /^-?\d+(?:\.\d+)?$/;

export function parseStartInputUtcMs(raw: string): number | null {
  const input = raw.trim();
  if (!input) return null;

  const ts = parseNumericTimestamp(input);
  if (ts !== null) return ts;

  const dmy = parseDmy(input);
  if (dmy !== null) return dmy;

  return parseIsoLike(input);
}

export function normalizeStartInput(raw: string): string | null {
  const ms = parseStartInputUtcMs(raw);
  if (ms === null) return null;
  return formatStartInputUtc(ms);
}

export function formatStartInputUtc(ms: number): string {
  if (!Number.isFinite(ms) || Math.abs(ms) > MAX_DATE_MS) return "";
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return "";
  return `${pad2(d.getUTCDate())}/${pad2(d.getUTCMonth() + 1)}/${d.getUTCFullYear()}, ${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}`;
}

function parseNumericTimestamp(input: string): number | null {
  if (!NUMERIC_RE.test(input)) return null;
  const numeric = Number(input);
  if (!Number.isFinite(numeric)) return null;
  const abs = Math.abs(numeric);
  const ms = abs < 1e12 ? Math.trunc(numeric * 1000) : Math.trunc(numeric);
  if (!Number.isFinite(ms) || Math.abs(ms) > MAX_DATE_MS) return null;
  return ms;
}

function parseDmy(input: string): number | null {
  const match = DMY_RE.exec(input);
  if (!match) return null;
  const [, dayText, monthText, yearText, hourText, minuteText, secondText] = match;
  const day = Number(dayText);
  const month = Number(monthText);
  const year = Number(yearText);
  const hour = parseTimePiece(hourText);
  const minute = parseTimePiece(minuteText);
  const second = parseTimePiece(secondText);
  if (hour === null || minute === null || second === null) return null;
  return fromUtcParts(year, month, day, hour, minute, second, 0, 0);
}

function parseIsoLike(input: string): number | null {
  const match = ISO_RE.exec(input);
  if (!match) return null;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, millisecondText, zoneText] =
    match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = hourText ? Number(hourText) : 0;
  const minute = minuteText ? Number(minuteText) : 0;
  const second = secondText ? Number(secondText) : 0;
  const millisecond = millisecondText ? Number(millisecondText.padEnd(3, "0")) : 0;
  const zoneMinutes = parseZoneMinutes(zoneText);
  if (zoneMinutes === null) return null;
  return fromUtcParts(year, month, day, hour, minute, second, millisecond, zoneMinutes);
}

function parseZoneMinutes(text?: string): number | null {
  if (!text) return 0;
  if (text === "Z" || text === "z") return 0;
  const match = /^([+\-])(\d{2})(?::?(\d{2}))?$/.exec(text);
  if (!match) return null;
  const sign = match[1] === "-" ? -1 : 1;
  const hh = Number(match[2]);
  const mm = Number(match[3] ?? "0");
  if (!Number.isInteger(hh) || !Number.isInteger(mm)) return null;
  if (hh > 23 || mm > 59) return null;
  return sign * (hh * 60 + mm);
}

function fromUtcParts(
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
  second: number,
  millisecond: number,
  zoneMinutes: number,
): number | null {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
  if (!Number.isInteger(hour) || !Number.isInteger(minute) || !Number.isInteger(second)) return null;
  if (!Number.isInteger(millisecond) || !Number.isInteger(zoneMinutes)) return null;
  if (month < 1 || month > 12) return null;
  if (day < 1 || day > 31) return null;
  if (hour < 0 || hour > 23) return null;
  if (minute < 0 || minute > 59) return null;
  if (second < 0 || second > 59) return null;
  if (millisecond < 0 || millisecond > 999) return null;

  const pseudoUtcMs = Date.UTC(year, month - 1, day, hour, minute, second, millisecond);
  if (!Number.isFinite(pseudoUtcMs)) return null;
  const check = new Date(pseudoUtcMs);
  if (check.getUTCFullYear() !== year) return null;
  if (check.getUTCMonth() + 1 !== month) return null;
  if (check.getUTCDate() !== day) return null;
  if (check.getUTCHours() !== hour) return null;
  if (check.getUTCMinutes() !== minute) return null;
  if (check.getUTCSeconds() !== second) return null;
  if (check.getUTCMilliseconds() !== millisecond) return null;

  const utcMs = pseudoUtcMs - zoneMinutes * 60_000;
  if (!Number.isFinite(utcMs) || Math.abs(utcMs) > MAX_DATE_MS) return null;
  return utcMs;
}

function pad2(value: number): string {
  return value < 10 ? `0${value}` : `${value}`;
}

function parseTimePiece(text?: string): number | null {
  if (text === undefined || text === "--") return 0;
  const value = Number(text);
  return Number.isInteger(value) ? value : null;
}

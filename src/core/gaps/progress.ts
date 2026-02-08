const ANSI_CLEAR_LINE = "\r\x1b[2K";
const MAX_STATUS_LEN = 180;
const MIN_UPDATE_INTERVAL_MS = 150;

let hasTransientLine = false;
let lastRendered = "";
let lastRenderedAt = 0;

function canRenderTransientLine(): boolean {
  return process.stdout.isTTY === true && process.env.AGGR_FIXGAPS_PROGRESS !== "0";
}

export function setFixgapsProgress(message: string): void {
  if (!canRenderTransientLine()) return;
  const sanitized = sanitizeMessage(message);
  const truncated = truncateMessage(sanitized, MAX_STATUS_LEN);
  const now = Date.now();
  if (truncated === lastRendered && now - lastRenderedAt < MIN_UPDATE_INTERVAL_MS) {
    return;
  }
  process.stdout.write(`${ANSI_CLEAR_LINE}${truncated}`);
  hasTransientLine = true;
  lastRendered = truncated;
  lastRenderedAt = now;
}

export function clearFixgapsProgress(): void {
  if (!canRenderTransientLine() || !hasTransientLine) return;
  process.stdout.write(ANSI_CLEAR_LINE);
  hasTransientLine = false;
  lastRendered = "";
  lastRenderedAt = 0;
}

export function logFixgapsLine(line: string): void {
  clearFixgapsProgress();
  console.log(line);
}

export function formatProgressUrl(url: string): string {
  try {
    const parsed = new URL(url);
    const path = `${parsed.host}${parsed.pathname}`;
    if (!parsed.search) return path;
    return truncateMessage(`${path}${parsed.search}`, 150);
  } catch {
    return truncateMessage(url, 150);
  }
}

function sanitizeMessage(message: string): string {
  return message.replaceAll("\n", " ").replaceAll("\r", " ");
}

function truncateMessage(message: string, maxLen: number): string {
  if (message.length <= maxLen) return message;
  return `${message.slice(0, maxLen - 3)}...`;
}

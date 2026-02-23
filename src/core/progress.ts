const ANSI_CLEAR_LINE = "\r\x1b[2K";

export type ProgressMode = "off" | "transient" | "line";

export interface ProgressReporter {
  setContext(context?: string): void;
  update(message: string): void;
  clear(): void;
  log(line: string): void;
}

export interface CreateProgressReporterOptions {
  envVar: string;
  prefix?: string;
  maxLen?: number;
  minUpdateIntervalMs?: number;
}

export function createProgressReporter(options: CreateProgressReporterOptions): ProgressReporter {
  const maxLen = options.maxLen ?? 180;
  const minUpdateIntervalMs = options.minUpdateIntervalMs ?? 150;
  const prefix = options.prefix ? sanitizeMessage(options.prefix) : "";
  let hasTransientLine = false;
  let lastRendered = "";
  let lastRenderedAt = 0;
  let context = "";

  const resolveMode = (): ProgressMode => {
    const raw = (process.env[options.envVar] ?? "").trim().toLowerCase();
    if (raw === "0" || raw === "off" || raw === "false") return "off";
    if (raw === "line" || raw === "lines" || raw === "print") return "line";
    return process.stdout.isTTY === true ? "transient" : "off";
  };

  const clear = (): void => {
    if (resolveMode() !== "transient" || !hasTransientLine) return;
    process.stdout.write(ANSI_CLEAR_LINE);
    hasTransientLine = false;
    lastRendered = "";
    lastRenderedAt = 0;
  };

  const log = (line: string): void => {
    clear();
    console.log(line);
  };

  const setContext = (next?: string): void => {
    context = next ? sanitizeMessage(next) : "";
  };

  const update = (message: string): void => {
    const mode = resolveMode();
    if (mode === "off") return;
    const formatted = truncateMessage(formatMessage(sanitizeMessage(message), prefix, context), maxLen);
    const now = Date.now();
    if (formatted === lastRendered && now - lastRenderedAt < minUpdateIntervalMs) return;

    if (mode === "transient") {
      process.stdout.write(`${ANSI_CLEAR_LINE}${formatted}`);
      hasTransientLine = true;
    } else {
      console.log(formatted);
    }
    lastRendered = formatted;
    lastRenderedAt = now;
  };

  return { setContext, update, clear, log };
}

export function formatProgressUrl(url: string, maxLen = 150): string {
  try {
    const parsed = new URL(url);
    const path = `${parsed.host}${parsed.pathname}`;
    if (!parsed.search) return path;
    return truncateMessage(`${path}${parsed.search}`, maxLen);
  } catch {
    return truncateMessage(url, maxLen);
  }
}

function sanitizeMessage(message: string): string {
  return message.replaceAll("\n", " ").replaceAll("\r", " ");
}

function stripPrefix(message: string, prefix: string): string {
  if (!prefix) return message;
  const fullPrefix = `${prefix} `;
  if (message.startsWith(fullPrefix)) return message.slice(fullPrefix.length);
  return message;
}

function formatMessage(message: string, prefix: string, context: string): string {
  const body = stripPrefix(message, prefix);
  if (!prefix) return context ? `${context} : ${body}` : body;
  if (!context) return message;
  return `${prefix} ${context} : ${body}`;
}

function truncateMessage(message: string, maxLen: number): string {
  if (message.length <= maxLen) return message;
  return `${message.slice(0, maxLen - 3)}...`;
}

import readline from "node:readline";
import { spawn } from "node:child_process";
import { normalizeSymbolToken, toKrakenPair } from "../common.js";
import { inferKrakenTickSide, type SymbolState } from "./directSide.js";
import type { GapWindow, RecoveredTrade } from "../types.js";

interface ParsedKrakenTrade {
  ts: number;
  price: number;
  size: number;
  priceText: string;
  sizeText: string;
}

interface SpawnOutcome {
  code: number;
  signal: NodeJS.Signals | null;
}

const CURSOR_REWIND_TOLERANCE_MS = 1;
const BENIGN_TERMINATION_CODES = new Set<number>([80, 141, 143]);

export class KrakenZipCursor {
  private proc?: ReturnType<typeof spawn>;
  private rl?: readline.Interface;
  private iter?: AsyncIterableIterator<string>;
  private exitPromise?: Promise<SpawnOutcome>;
  private stderr = "";
  private cursorTs = Number.NEGATIVE_INFINITY;
  private sideState: SymbolState = { lastSide: "buy" };

  constructor(
    private readonly zipPath: string,
    private readonly entryName: string,
  ) {}

  async recoverWindows(windows: GapWindow[]): Promise<RecoveredTrade[]> {
    const sorted = normalizeWindows(windows);
    if (!sorted.length) return [];

    const fromTs = sorted[0]?.fromTs ?? Number.POSITIVE_INFINITY;
    if (shouldRestartKrakenCursor(fromTs, this.cursorTs)) {
      await this.restart();
    }

    const recovered: RecoveredTrade[] = [];
    const lastWindowEnd = sorted[sorted.length - 1]?.toTs ?? Number.NEGATIVE_INFINITY;
    let windowIndex = 0;
    while (windowIndex < sorted.length) {
      const trade = await this.nextTrade();
      if (!trade) break;

      const side = inferKrakenTickSide(trade.price, this.sideState);
      while (windowIndex < sorted.length && trade.ts >= sorted[windowIndex].toTs) {
        windowIndex += 1;
      }
      if (windowIndex >= sorted.length) {
        break;
      }

      const window = sorted[windowIndex];
      if (window && trade.ts > window.fromTs && trade.ts < window.toTs) {
        recovered.push({
          ts: trade.ts,
          price: trade.price,
          size: trade.size,
          side,
          priceText: trade.priceText,
          sizeText: trade.sizeText,
        });
      }
      if (trade.ts >= lastWindowEnd) {
        break;
      }
    }
    return recovered;
  }

  async close(): Promise<void> {
    const proc = this.proc;
    if (!proc) return;

    proc.kill("SIGTERM");
    try {
      await this.awaitExit(true);
    } finally {
      this.clearStreamState();
    }
  }

  private async restart(): Promise<void> {
    await this.close();
    this.cursorTs = Number.NEGATIVE_INFINITY;
    this.sideState = { lastSide: "buy" };
  }

  private async nextTrade(): Promise<ParsedKrakenTrade | undefined> {
    while (true) {
      const line = await this.nextLine();
      if (line === undefined) return undefined;
      const parsed = parseKrakenCsvLine(line);
      if (!parsed) continue;
      this.cursorTs = parsed.ts;
      return parsed;
    }
  }

  private async nextLine(): Promise<string | undefined> {
    await this.ensureStarted();
    const iter = this.iter;
    if (!iter) return undefined;

    const item = await iter.next();
    if (!item.done) {
      return item.value;
    }

    await this.awaitExit(false);
    this.clearStreamState();
    return undefined;
  }

  private async ensureStarted(): Promise<void> {
    if (this.proc) return;

    const proc = spawn("unzip", ["-p", this.zipPath, this.entryName], { stdio: ["ignore", "pipe", "pipe"] });
    this.proc = proc;
    this.stderr = "";
    proc.stderr.on("data", (chunk: Buffer) => {
      if (this.stderr.length < 800) {
        this.stderr += chunk.toString("utf8");
      }
    });

    this.rl = readline.createInterface({ input: proc.stdout, crlfDelay: Infinity });
    this.iter = this.rl[Symbol.asyncIterator]();
    this.exitPromise = new Promise<SpawnOutcome>((resolve, reject) => {
      proc.on("error", reject);
      proc.on("close", (code, signal) => resolve({ code: code ?? 0, signal }));
    });
  }

  private async awaitExit(allowTerminated: boolean): Promise<void> {
    const exit = this.exitPromise;
    if (!exit) return;
    const outcome = await exit;
    if (outcome.code !== 0 && !isAllowedKrakenTermination(outcome, allowTerminated)) {
      throw new Error(
        `unzip failed code=${outcome.code} signal=${outcome.signal ?? "none"} path=${this.zipPath} entry=${this.entryName} stderr=${this.stderr.trim().slice(0, 300)}`,
      );
    }
  }

  private clearStreamState(): void {
    this.rl?.close();
    this.rl = undefined;
    this.iter = undefined;
    this.proc = undefined;
    this.exitPromise = undefined;
  }
}

export async function resolveKrakenZipEntryName(zipPath: string, symbol: string): Promise<string | undefined> {
  const pair = toKrakenPair(symbol);
  const normalized = normalizeSymbolToken(symbol);
  const targetNames = new Set<string>([`${pair}.CSV`, `${normalized}.CSV`]);
  const entries = await listZipEntries(zipPath);
  for (const entry of entries) {
    const upper = entry.toUpperCase();
    const slash = upper.lastIndexOf("/");
    const base = slash >= 0 ? upper.slice(slash + 1) : upper;
    if (targetNames.has(base)) {
      return entry;
    }
  }
  return undefined;
}

function normalizeWindows(windows: GapWindow[]): GapWindow[] {
  return [...windows]
    .filter((window) => Number.isFinite(window.fromTs) && Number.isFinite(window.toTs) && window.toTs > window.fromTs)
    .sort((a, b) => (a.fromTs - b.fromTs) || (a.toTs - b.toTs) || (a.eventId - b.eventId));
}

function parseKrakenCsvLine(line: string): ParsedKrakenTrade | undefined {
  const firstComma = line.indexOf(",");
  if (firstComma <= 0) return undefined;
  const secondComma = line.indexOf(",", firstComma + 1);
  if (secondComma <= firstComma + 1) return undefined;

  const ts = Math.round(Number(line.slice(0, firstComma)) * 1000);
  const priceText = line.slice(firstComma + 1, secondComma);
  const sizeText = line.slice(secondComma + 1);
  const price = Number(priceText);
  const size = Number(sizeText);
  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) return undefined;
  if (ts <= 0 || price <= 0 || size <= 0) return undefined;
  return { ts, price, size, priceText, sizeText };
}

async function listZipEntries(zipPath: string): Promise<string[]> {
  const proc = spawn("unzip", ["-Z1", zipPath], { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  proc.stderr.on("data", (chunk: Buffer) => {
    if (stderr.length < 800) {
      stderr += chunk.toString("utf8");
    }
  });

  const rl = readline.createInterface({ input: proc.stdout, crlfDelay: Infinity });
  const entries: string[] = [];
  rl.on("line", (line) => {
    if (line.length) entries.push(line);
  });

  const outcome = await new Promise<SpawnOutcome>((resolve, reject) => {
    proc.on("error", reject);
    proc.on("close", (code, signal) => resolve({ code: code ?? 0, signal }));
  });
  rl.close();

  if (outcome.code !== 0) {
    throw new Error(`unzip -Z1 failed code=${outcome.code} path=${zipPath} stderr=${stderr.trim().slice(0, 300)}`);
  }
  return entries;
}

export function shouldRestartKrakenCursor(fromTs: number, cursorTs: number): boolean {
  return fromTs + CURSOR_REWIND_TOLERANCE_MS < cursorTs;
}

export function isAllowedKrakenTermination(outcome: SpawnOutcome, allowTerminated: boolean): boolean {
  if (!allowTerminated) return false;
  if (outcome.signal !== null) return true;
  return BENIGN_TERMINATION_CODES.has(outcome.code);
}

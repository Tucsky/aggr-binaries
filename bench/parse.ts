import fs from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { parseTradeLine as parseWithHelper } from "../src/core/trades.js";

type Side = "buy" | "sell";

interface Trade {
  ts: number;
  price: number;
  size: number;
  side: Side;
  liquidation: boolean;
}

type ParseRejectReason = "parts_short" | "non_finite" | "invalid_ts_range" | "notional_too_large";

interface ParseReject {
  reason?: ParseRejectReason;
}

const MIN_TS_MS = 1e11;
const MAX_TS_MS = 1e13;
const MAX_NOTIONAL = 1e9;
const GENERATED_LINES = 2_000_000;

type ParseImpl = { name: string; fn: (line: string, reject?: ParseReject) => Trade | null };

const implementations: ParseImpl[] = [
  { name: "split_regex", fn: parseWithSplit },
  { name: "manual_single_pass", fn: parseManual },
  { name: "helper_single_pass", fn: parseWithHelper },
];

function parseWithSplit(line: string, reject?: ParseReject): Trade | null {
  const parts = line.trim().split(/\s+/);
  if (parts.length < 4) {
    if (reject) reject.reason = "parts_short";
    return null;
  }

  const ts = Number(parts[0]);
  const price = Number(parts[1]);
  const size = Number(parts[2]);

  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) {
    if (reject) reject.reason = "non_finite";
    return null;
  }

  if (ts <= MIN_TS_MS || ts >= MAX_TS_MS) {
    if (reject) reject.reason = "invalid_ts_range";
    return null;
  }

  const notional = price * size;
  if (!Number.isFinite(notional) || notional > MAX_NOTIONAL) {
    if (reject) reject.reason = "notional_too_large";
    return null;
  }

  const side = parts[3] === "1" ? "buy" : "sell";
  const liquidation = parts[4] === "1";

  return { ts, price, size, side, liquidation };
}

function parseManual(line: string, reject?: ParseReject): Trade | null {
  let i = 0;
  const len = line.length;

  while (i < len && line.charCodeAt(i) <= 32) i += 1;
  const tsStart = i;
  while (i < len && line.charCodeAt(i) > 32) i += 1;
  if (tsStart === i) {
    if (reject) reject.reason = "parts_short";
    return null;
  }
  const ts = Number(line.slice(tsStart, i));

  while (i < len && line.charCodeAt(i) <= 32) i += 1;
  const priceStart = i;
  while (i < len && line.charCodeAt(i) > 32) i += 1;
  if (priceStart === i) {
    if (reject) reject.reason = "parts_short";
    return null;
  }
  const price = Number(line.slice(priceStart, i));

  while (i < len && line.charCodeAt(i) <= 32) i += 1;
  const sizeStart = i;
  while (i < len && line.charCodeAt(i) > 32) i += 1;
  if (sizeStart === i) {
    if (reject) reject.reason = "parts_short";
    return null;
  }
  const size = Number(line.slice(sizeStart, i));

  while (i < len && line.charCodeAt(i) <= 32) i += 1;
  const sideStart = i;
  while (i < len && line.charCodeAt(i) > 32) i += 1;
  if (sideStart === i) {
    if (reject) reject.reason = "parts_short";
    return null;
  }
  const side: Side = i - sideStart === 1 && line.charCodeAt(sideStart) === 49 ? "buy" : "sell";

  while (i < len && line.charCodeAt(i) <= 32) i += 1;
  let liquidation = false;
  if (i < len) {
    const liqStart = i;
    while (i < len && line.charCodeAt(i) > 32) i += 1;
    liquidation = i - liqStart === 1 && line.charCodeAt(liqStart) === 49;
  }

  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) {
    if (reject) reject.reason = "non_finite";
    return null;
  }

  if (ts <= MIN_TS_MS || ts >= MAX_TS_MS) {
    if (reject) reject.reason = "invalid_ts_range";
    return null;
  }

  const notional = price * size;
  if (!Number.isFinite(notional) || notional > MAX_NOTIONAL) {
    if (reject) reject.reason = "notional_too_large";
    return null;
  }

  return { ts, price, size, side, liquidation };
}

function buildGeneratedLines(count: number): string[] {
  const lines = new Array<string>(count);
  for (let i = 0; i < count; i += 1) {
    const mod = i % 24;
    if (mod === 0) {
      lines[i] = "";
    } else if (mod === 1) {
      lines[i] = "100 1";
    } else if (mod === 2) {
      lines[i] = `${MIN_TS_MS - 1} 1 1 1 0`;
    } else if (mod === 3) {
      lines[i] = `${MIN_TS_MS + 1000} NaN 1 1 0`;
    } else if (mod === 4) {
      lines[i] = `${MIN_TS_MS + 2000} 1000000 2000 1 0`;
    } else {
      const ts = MIN_TS_MS + 10_000 + (i % 10_000);
      const price = 10_000 + (i % 500);
      const size = (i % 5) + 0.5;
      const side = (i & 1) === 0 ? "1" : "0";
      const liquidation = i % 7 === 0 ? "1" : "0";
      lines[i] = `${ts} ${price} ${size} ${side} ${liquidation}`;
    }
  }
  return lines;
}

async function loadFixtureLines(): Promise<string[] | undefined> {
  const fixturePath = path.resolve(process.cwd(), "bench/fixtures/parse-sample.txt");
  try {
    const raw = await fs.readFile(fixturePath, "utf8");
    const lines = raw
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l.length > 0);
    return lines.length ? lines : undefined;
  } catch (err) {
    if ((err as { code?: string }).code === "ENOENT") return undefined;
    throw err;
  }
}

function runDataset(label: string, lines: string[]): void {
  console.log(`[bench] dataset=${label} lines=${lines.length}`);
  for (const impl of implementations) {
    const reject: ParseReject = {};
    let rejects = 0;
    let kept = 0;
    const start = performance.now();
    for (const line of lines) {
      reject.reason = undefined;
      const t = impl.fn(line, reject);
      if (!t) {
        if (reject.reason) rejects += 1;
        continue;
      }
      kept += 1;
    }
    const elapsedMs = performance.now() - start;
    const opsPerSec = (lines.length / (elapsedMs / 1000)).toFixed(1);
    console.log(
      `[bench:${label}] impl=${impl.name} ops/sec=${opsPerSec} rejects=${rejects} kept=${kept} elapsedMs=${elapsedMs.toFixed(
        1,
      )}`,
    );
  }
}

async function main() {
  const generated = buildGeneratedLines(GENERATED_LINES);
  runDataset("generated", generated);

  const fixtureLines = await loadFixtureLines();
  if (fixtureLines) {
    runDataset("fixture", fixtureLines);
  } else {
    console.log("[bench] fixture not found; skipping fixture dataset");
  }
}

void main();

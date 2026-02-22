import fs from "node:fs/promises";
import path from "node:path";
import type http from "node:http";
import { loadConfig, type CliOverrides } from "../core/config.js";
import type { Db } from "../core/db.js";
import { runFixGaps } from "../core/gaps/index.js";
import { runIndex } from "../core/indexer.js";
import { runProcess } from "../core/process.js";
import { runRegistry } from "../core/registry.js";

const MAX_REQUEST_BYTES = 16_384;

export enum TimelineMarketAction {
  Index = "index",
  Process = "process",
  FixGaps = "fixgaps",
  Registry = "registry",
}

export interface TimelineActionsApiOptions {
  dbPath: string;
  outDir: string;
  configPath?: string;
}

export interface TimelineActionsRequestPayload {
  action: TimelineMarketAction;
  collector: string;
  exchange: string;
  symbol: string;
  timeframe?: string;
}

interface TimelineActionMarket {
  collector: string;
  exchange: string;
  symbol: string;
  timeframe?: string;
}

export interface TimelineActionResult {
  action: TimelineMarketAction;
  market: TimelineActionMarket;
  durationMs: number;
  details: Record<string, number>;
}

export interface TimelineActionDependencies {
  loadConfig: (overrides: CliOverrides) => Promise<Awaited<ReturnType<typeof loadConfig>>>;
  runIndex: typeof runIndex;
  runProcess: typeof runProcess;
  runFixGaps: typeof runFixGaps;
  runRegistry: typeof runRegistry;
}

const DEFAULT_DEPS: TimelineActionDependencies = {
  loadConfig,
  runIndex,
  runProcess,
  runFixGaps,
  runRegistry,
};

export function createTimelineActionsApiHandler(
  db: Db,
  options: TimelineActionsApiOptions,
  deps: TimelineActionDependencies = DEFAULT_DEPS,
): (req: http.IncomingMessage, res: http.ServerResponse, url: URL) => Promise<boolean> {
  let inFlightLabel: string | null = null;

  return async (req, res, url) => {
    if (url.pathname !== "/api/timeline/actions") {
      return false;
    }
    if (req.method !== "POST") {
      writeJson(res, 405, { error: "Method not allowed" });
      return true;
    }

    let payload: TimelineActionsRequestPayload;
    try {
      payload = parseTimelineActionPayload(await readJsonBody(req));
    } catch (err) {
      writeJson(res, 400, { error: err instanceof Error ? err.message : "Invalid request payload" });
      return true;
    }

    const marketLabel = `${payload.collector}/${payload.exchange}/${payload.symbol}`;
    if (inFlightLabel) {
      writeJson(res, 409, { error: `Action already running for ${inFlightLabel}` });
      return true;
    }

    const startedAt = Date.now();
    inFlightLabel = marketLabel;
    try {
      const result = await executeTimelineAction(db, payload, options, deps);
      writeJson(res, 200, {
        ...result,
        durationMs: Date.now() - startedAt,
      });
    } catch (err) {
      writeJson(res, 500, { error: err instanceof Error ? err.message : "Action failed" });
    } finally {
      inFlightLabel = null;
    }
    return true;
  };
}

export async function executeTimelineAction(
  db: Db,
  payload: TimelineActionsRequestPayload,
  options: TimelineActionsApiOptions,
  deps: TimelineActionDependencies,
): Promise<TimelineActionResult> {
  const overrides = buildBaseOverrides(payload, options);
  const config = await deps.loadConfig(overrides);

  if (payload.action === TimelineMarketAction.Index) {
    const includePaths = await resolveIndexIncludePaths(config.root, payload.collector, payload.exchange, payload.symbol);
    const stats = await deps.runIndex(
      {
        ...config,
        includePaths,
      },
      db,
    );
    return {
      action: payload.action,
      market: buildResultMarket(payload),
      durationMs: 0,
      details: {
        seen: stats.seen,
        inserted: stats.inserted,
        existing: stats.existing,
        conflicts: stats.conflicts,
        skipped: stats.skipped,
      },
    };
  }

  if (payload.action === TimelineMarketAction.Process) {
    await deps.runProcess(config, db);
    return {
      action: payload.action,
      market: buildResultMarket(payload),
      durationMs: 0,
      details: {},
    };
  }

  if (payload.action === TimelineMarketAction.FixGaps) {
    const stats = await deps.runFixGaps(config, db);
    return {
      action: payload.action,
      market: buildResultMarket(payload),
      durationMs: 0,
      details: {
        selectedEvents: stats.selectedEvents,
        processedFiles: stats.processedFiles,
        recoveredTrades: stats.recoveredTrades,
        fixedEvents: stats.fixedEvents,
        deletedEvents: stats.deletedEvents,
        missingAdapter: stats.missingAdapter,
        adapterError: stats.adapterError,
        binariesPatched: stats.binariesPatched,
      },
    };
  }

  if (payload.action === TimelineMarketAction.Registry) {
    const stats = await deps.runRegistry(config, db);
    return {
      action: payload.action,
      market: buildResultMarket(payload),
      durationMs: 0,
      details: {
        scanned: stats.scanned,
        upserted: stats.upserted,
        deleted: stats.deleted,
      },
    };
  }

  throw new Error(`Unsupported action: ${payload.action}`);
}

function buildBaseOverrides(
  payload: TimelineActionsRequestPayload,
  options: TimelineActionsApiOptions,
): CliOverrides {
  const timeframe = normalizeTimeframe(payload.timeframe);
  return {
    dbPath: options.dbPath,
    outDir: options.outDir,
    configPath: options.configPath,
    collector: payload.collector,
    exchange: payload.exchange,
    symbol: payload.symbol,
    ...(timeframe ? { timeframe } : {}),
  };
}

function normalizeTimeframe(raw?: string): string | undefined {
  if (!raw) return undefined;
  const timeframe = raw.trim();
  if (!timeframe) return undefined;
  if (timeframe.toUpperCase() === "ALL") return undefined;
  return timeframe;
}

function parseTimelineActionPayload(raw: unknown): TimelineActionsRequestPayload {
  if (!raw || typeof raw !== "object") {
    throw new Error("Expected JSON object payload");
  }
  const value = raw as Record<string, unknown>;
  const action = parseAction(value.action);
  const collector = parseRequiredField(value.collector, "collector").toUpperCase();
  const exchange = parseRequiredField(value.exchange, "exchange").toUpperCase();
  const symbol = parseRequiredField(value.symbol, "symbol");
  const timeframe = parseOptionalField(value.timeframe);

  return { action, collector, exchange, symbol, timeframe };
}

function parseAction(raw: unknown): TimelineMarketAction {
  const action = typeof raw === "string" ? raw.trim().toLowerCase() : "";
  if (action === TimelineMarketAction.Index) return TimelineMarketAction.Index;
  if (action === TimelineMarketAction.Process) return TimelineMarketAction.Process;
  if (action === TimelineMarketAction.FixGaps) return TimelineMarketAction.FixGaps;
  if (action === TimelineMarketAction.Registry) return TimelineMarketAction.Registry;
  throw new Error("action must be one of: index, process, fixgaps, registry");
}

function parseRequiredField(raw: unknown, name: string): string {
  if (typeof raw !== "string") {
    throw new Error(`${name} is required`);
  }
  const trimmed = raw.trim();
  if (!trimmed) {
    throw new Error(`${name} is required`);
  }
  return trimmed;
}

function parseOptionalField(raw: unknown): string | undefined {
  if (typeof raw !== "string") return undefined;
  const trimmed = raw.trim();
  return trimmed ? trimmed : undefined;
}

async function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let totalBytes = 0;

  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    totalBytes += buf.length;
    if (totalBytes > MAX_REQUEST_BYTES) {
      throw new Error(`Payload too large (max ${MAX_REQUEST_BYTES} bytes)`);
    }
    chunks.push(buf);
  }

  if (!chunks.length) {
    throw new Error("Request body is required");
  }
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) {
    throw new Error("Request body is required");
  }

  try {
    return JSON.parse(raw) as unknown;
  } catch {
    throw new Error("Request body must be valid JSON");
  }
}

function buildResultMarket(payload: TimelineActionsRequestPayload): TimelineActionMarket {
  return {
    collector: payload.collector,
    exchange: payload.exchange,
    symbol: payload.symbol,
    timeframe: payload.timeframe,
  };
}

async function resolveIndexIncludePaths(
  root: string,
  collector: string,
  exchange: string,
  symbol: string,
): Promise<string[]> {
  const out = new Set<string>();

  const collectorRoot = path.join(root, collector);
  const baseRoot = (await isDirectory(collectorRoot)) ? collectorRoot : root;
  const directPath = path.join(baseRoot, exchange, symbol);
  if (await isDirectory(directPath)) {
    out.add(path.relative(root, directPath));
  }

  const bucketDirs = await readSubdirs(baseRoot);
  for (const bucket of bucketDirs) {
    const candidate = path.join(baseRoot, bucket, exchange, symbol);
    if (await isDirectory(candidate)) {
      out.add(path.relative(root, candidate));
    }
  }

  if (!out.size) {
    if (baseRoot === collectorRoot) {
      out.add(path.join(collector, exchange, symbol));
    }
    out.add(path.join(exchange, symbol));
  }

  return [...out].sort();
}

async function readSubdirs(root: string): Promise<string[]> {
  try {
    const entries = await fs.readdir(root, { withFileTypes: true });
    return entries.filter((entry) => entry.isDirectory()).map((entry) => entry.name);
  } catch {
    return [];
  }
}

async function isDirectory(absPath: string): Promise<boolean> {
  try {
    const stat = await fs.stat(absPath);
    return stat.isDirectory();
  } catch {
    return false;
  }
}

function writeJson(res: http.ServerResponse, status: number, payload: unknown): void {
  res.writeHead(status, {
    "Content-Type": "application/json",
  });
  res.end(JSON.stringify(payload));
}

import assert from "node:assert/strict";
import fs from "node:fs/promises";
import type http from "node:http";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { test } from "node:test";
import type { Config } from "../../src/core/config.js";
import type { Db } from "../../src/core/db.js";
import {
  createTimelineActionsApiHandler,
  executeTimelineAction,
  TimelineMarketAction,
  type TimelineActionDependencies,
} from "../../src/server/timelineActionsApi.js";

test("timeline action index resolves bucketed include paths for the selected market", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-timeline-actions-index-"));
  await fs.mkdir(path.join(root, "PI", "2024", "BYBIT", "BTCUSDT"), { recursive: true });
  await fs.mkdir(path.join(root, "PI", "2025", "BYBIT", "BTCUSDT"), { recursive: true });
  await fs.mkdir(path.join(root, "PI", "2025", "BYBIT", "ETHUSDT"), { recursive: true });

  let includePathsSeen: string[] | undefined;
  const deps = buildDeps(root, {
    runIndex: async (config) => {
      includePathsSeen = config.includePaths;
      return {
        seen: 12,
        inserted: 8,
        existing: 4,
        conflicts: 0,
        skipped: 0,
      };
    },
  });

  const result = await executeTimelineAction(
    {} as Db,
    {
      action: TimelineMarketAction.Index,
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
    },
    { dbPath: path.join(root, "index.sqlite"), outDir: path.join(root, "out") },
    deps,
  );

  assert.deepStrictEqual(includePathsSeen, ["PI/2024/BYBIT/BTCUSDT", "PI/2025/BYBIT/BTCUSDT"]);
  assert.deepStrictEqual(result.details, {
    seen: 12,
    inserted: 8,
    existing: 4,
    conflicts: 0,
    skipped: 0,
  });
});

test("timeline action process ignores ALL timeframe override and preserves market filters", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-timeline-actions-process-"));
  const seenOverrides: Array<Partial<Config>> = [];

  const deps = buildDeps(root, {
    loadConfig: async (overrides) => {
      seenOverrides.push(overrides);
      return makeConfig(root, overrides);
    },
    runProcess: async () => undefined,
  });

  await executeTimelineAction(
    {} as Db,
    {
      action: TimelineMarketAction.Process,
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      timeframe: "ALL",
    },
    { dbPath: path.join(root, "index.sqlite"), outDir: path.join(root, "out") },
    deps,
  );

  assert.strictEqual(seenOverrides.length, 1);
  assert.strictEqual(seenOverrides[0].collector, "PI");
  assert.strictEqual(seenOverrides[0].exchange, "BYBIT");
  assert.strictEqual(seenOverrides[0].symbol, "BTCUSDT");
  assert.strictEqual(seenOverrides[0].timeframe, undefined);
});

test("timeline actions endpoint enforces a single in-flight action", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-timeline-actions-lock-"));
  const gate = defer<void>();

  const deps = buildDeps(root, {
    runProcess: async () => {
      await gate.promise;
    },
  });

  const handler = createTimelineActionsApiHandler(
    {} as Db,
    { dbPath: path.join(root, "index.sqlite"), outDir: path.join(root, "out") },
    deps,
  );

  const firstRes = createMockResponse();
  const firstReq = createJsonRequest({
    action: "process",
    collector: "pi",
    exchange: "bybit",
    symbol: "btcusdt",
  });
  const firstPromise = handler(firstReq as http.IncomingMessage, firstRes.asServerResponse(), new URL("http://localhost/api/timeline/actions"));
  await new Promise<void>((resolve) => setImmediate(resolve));

  const secondRes = createMockResponse();
  const secondReq = createJsonRequest({
    action: "process",
    collector: "PI",
    exchange: "BYBIT",
    symbol: "BTCUSDT",
  });
  const secondHandled = await handler(
    secondReq as http.IncomingMessage,
    secondRes.asServerResponse(),
    new URL("http://localhost/api/timeline/actions"),
  );

  assert.strictEqual(secondHandled, true);
  assert.strictEqual(secondRes.statusCode, 409);
  const secondPayload = asRecord(secondRes.parsedJson());
  assert.match(secondPayload.error, /Action already running/);

  gate.resolve();
  const firstHandled = await firstPromise;
  assert.strictEqual(firstHandled, true);
  assert.strictEqual(firstRes.statusCode, 200);
  const firstPayload = firstRes.parsedJson();
  const market = asRecord(firstPayload.market);
  assert.strictEqual(market.collector, "PI");
  assert.strictEqual(market.exchange, "BYBIT");
  assert.strictEqual(market.symbol, "btcusdt");
});

function buildDeps(
  root: string,
  overrides: Partial<TimelineActionDependencies> = {},
): TimelineActionDependencies {
  return {
    loadConfig: overrides.loadConfig ?? (async (partial) => makeConfig(root, partial)),
    runIndex:
      overrides.runIndex ??
      (async () => {
        throw new Error("runIndex should not be called in this test");
      }),
    runProcess:
      overrides.runProcess ??
      (async () => {
        throw new Error("runProcess should not be called in this test");
      }),
    runFixGaps:
      overrides.runFixGaps ??
      (async () => {
        throw new Error("runFixGaps should not be called in this test");
      }),
    runRegistry:
      overrides.runRegistry ??
      (async () => {
        throw new Error("runRegistry should not be called in this test");
      }),
  };
}

function makeConfig(root: string, overrides: Partial<Config>): Config {
  return {
    root,
    dbPath: path.join(root, "index.sqlite"),
    batchSize: 1000,
    flushIntervalSeconds: 10,
    outDir: path.join(root, "out"),
    timeframe: "1m",
    timeframeMs: 60_000,
    collector: typeof overrides.collector === "string" ? overrides.collector : undefined,
    exchange: typeof overrides.exchange === "string" ? overrides.exchange : undefined,
    symbol: typeof overrides.symbol === "string" ? overrides.symbol : undefined,
    includePaths: Array.isArray(overrides.includePaths) ? overrides.includePaths : undefined,
  };
}

function createJsonRequest(payload: unknown): Readable {
  const req = Readable.from([Buffer.from(JSON.stringify(payload), "utf8")]);
  (req as { method?: string }).method = "POST";
  return req;
}

function defer<T>() {
  let resolve: (value: T | PromiseLike<T>) => void = () => undefined;
  let reject: (reason?: unknown) => void = () => undefined;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function createMockResponse() {
  const state = {
    statusCode: 0,
    headers: {} as Record<string, string>,
    body: "",
  };
  return {
    get statusCode() {
      return state.statusCode;
    },
    asServerResponse(): http.ServerResponse {
      return {
        writeHead(statusCode: number, headers?: Record<string, string>) {
          state.statusCode = statusCode;
          state.headers = headers ?? {};
          return this;
        },
        end(chunk?: string | Buffer) {
          if (typeof chunk === "string") {
            state.body += chunk;
          } else if (Buffer.isBuffer(chunk)) {
            state.body += chunk.toString("utf8");
          }
          return this;
        },
      } as unknown as http.ServerResponse;
    },
    parsedJson(): Record<string, unknown> {
      return JSON.parse(state.body) as Record<string, unknown>;
    },
  };
}

function asRecord(value: unknown): Record<string, string> {
  if (!value || typeof value !== "object") {
    throw new Error("Expected object payload");
  }
  return value as Record<string, string>;
}

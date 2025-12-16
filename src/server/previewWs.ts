import crypto from "node:crypto";
import type http from "node:http";
import { computeAnchorIndex, loadCompanion, readCandles, type Companion, type PreviewContext } from "./previewData.js";
import { listMarkets, listTimeframes } from "./registryApi.js";
import { decodeFrames, encodeFrame } from "./wsFrames.js";

type IncomingMessage =
  | {
      type: "setTarget";
      collector?: string;
      exchange?: string;
      symbol?: string;
      timeframe?: string;
      startTs?: number | null;
    }
  | { type: "setTimeframe"; timeframe?: string }
  | { type: "setStart"; startTs?: number }
  | { type: "slice"; fromIndex?: number; toIndex?: number }
  | { type: "listMarkets" }
  | { type: "listTimeframes"; collector?: string; exchange?: string; symbol?: string };

interface ConnectionState {
  collector: string | null;
  exchange: string | null;
  symbol: string | null;
  timeframe: string;
  startMs: number | null;
  companion: Companion | null;
  anchorIndex: number | null;
}

export function attachPreviewWs(server: http.Server, ctx: PreviewContext): void {
  server.on("upgrade", async (req, socket) => {
    const url = new URL(req.url ?? "", "http://localhost");
    if (url.pathname !== "/ws") {
      socket.destroy();
      return;
    }

    const state: ConnectionState = {
      collector: null,
      exchange: null,
      symbol: null,
      timeframe: "1m",
      startMs: null,
      companion: null,
      anchorIndex: null,
    };

    const key = req.headers["sec-websocket-key"];
    if (!key || Array.isArray(key)) {
      socket.destroy();
      return;
    }
    const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
    const headers = [
      "HTTP/1.1 101 Switching Protocols",
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Accept: ${accept}`,
      "\r\n",
    ];
    socket.write(headers.join("\r\n"));

    let inbox: Buffer = Buffer.alloc(0);
    socket.on("data", async (buf) => {
      inbox = Buffer.concat([inbox, buf]);
      const { frames, rest } = decodeFrames(inbox);
      inbox = rest;
      for (const frame of frames) {
        if (frame.opcode !== 1) continue;
        try {
          const payload = JSON.parse(frame.data.toString()) as IncomingMessage;
          await handleMessage(socket, ctx, state, payload);
        } catch (err) {
          console.error("[ws] failed to handle message", err);
          sendError(socket, err instanceof Error ? err.message : "Failed to handle message");
        }
      }
    });
  });
}

async function handleMessage(socket: any, ctx: PreviewContext, state: ConnectionState, payload: IncomingMessage) {
  switch (payload.type) {
    case "setTarget":
      await handleSetTarget(socket, ctx, state, payload);
      return;
    case "setTimeframe":
      await handleSetTimeframe(socket, ctx, state, payload);
      return;
    case "setStart":
      await handleSetStart(socket, ctx, state, payload);
      return;
    case "slice":
      if (typeof payload.fromIndex === "number" && typeof payload.toIndex === "number" && state.companion) {
        const resp = await readCandles(
          ctx,
          state.collector!,
          state.exchange!,
          state.symbol!,
          state.timeframe,
          payload.fromIndex,
          payload.toIndex,
          state.companion,
        );
        send(socket, resp);
      }
      return;
    case "listMarkets":
      send(socket, { type: "markets", markets: listMarkets(ctx.db) });
      return;
    case "listTimeframes":
      await handleListTimeframes(socket, ctx, payload, state);
      return;
    default:
      sendError(socket, "Unknown message type");
      return;
  }
}

function send(socket: any, data: any) {
  const json = Buffer.from(JSON.stringify(data));
  const frame = encodeFrame(json);
  socket.write(frame);
}

function sendError(socket: any, message: string): void {
  send(socket, { type: "error", message });
}

async function handleSetTarget(socket: any, ctx: PreviewContext, state: ConnectionState, payload: IncomingMessage & { type: "setTarget" }) {
  const collector = (payload.collector ?? "").trim().toUpperCase();
  const exchange = (payload.exchange ?? "").trim().toUpperCase();
  const symbol = (payload.symbol ?? "").trim();
  const timeframe = (payload.timeframe ?? state.timeframe ?? "").trim();
  const startTs = payload.startTs;
  if (!collector || !exchange || !symbol) {
    sendError(socket, "Missing collector/exchange/symbol");
    return;
  }
  state.collector = collector;
  state.exchange = exchange;
  state.symbol = symbol;
  if (timeframe) {
    state.timeframe = timeframe;
  }
  if (startTs === null || startTs === undefined) {
    state.startMs = null;
  } else if (typeof startTs === "number" && Number.isFinite(startTs)) {
    state.startMs = startTs;
  }
  await refreshCompanion(socket, ctx, state);
}

async function handleSetTimeframe(
  socket: any,
  ctx: PreviewContext,
  state: ConnectionState,
  payload: IncomingMessage & { type: "setTimeframe" },
) {
  const timeframe = (payload.timeframe ?? "").trim();
  if (!timeframe) {
    sendError(socket, "Missing timeframe");
    return;
  }
  state.timeframe = timeframe;
  if (!state.collector || !state.exchange || !state.symbol) {
    sendError(socket, "Target not set");
    return;
  }
  await refreshCompanion(socket, ctx, state);
}

async function handleSetStart(
  socket: any,
  ctx: PreviewContext,
  state: ConnectionState,
  payload: IncomingMessage & { type: "setStart" },
) {
  const startTs = payload.startTs;
  if (startTs === null || startTs === undefined) {
    state.startMs = null;
  } else if (typeof startTs === "number" && Number.isFinite(startTs)) {
    state.startMs = startTs;
  } else {
    sendError(socket, "Invalid startTs");
    return;
  }
  if (!state.companion || !state.collector || !state.exchange || !state.symbol) {
    return;
  }
  state.anchorIndex = await computeAnchorIndex(
    ctx,
    state.collector,
    state.exchange,
    state.symbol,
    state.timeframe,
    state.companion,
    state.startMs,
  );
  sendMeta(socket, state);
}

async function handleListTimeframes(
  socket: any,
  ctx: PreviewContext,
  payload: IncomingMessage & { type: "listTimeframes" },
  state: ConnectionState,
) {
  const collector = (payload.collector ?? state.collector ?? "").toString().trim().toUpperCase();
  const exchange = (payload.exchange ?? state.exchange ?? "").toString().trim().toUpperCase();
  const symbol = (payload.symbol ?? state.symbol ?? "").toString().trim();
  if (!collector || !exchange || !symbol) {
    sendError(socket, "Missing collector/exchange/symbol for timeframes");
    return;
  }
  const timeframes = listTimeframes(ctx.db, collector, exchange, symbol);
  send(socket, { type: "timeframes", collector, exchange, symbol, timeframes });
}

async function refreshCompanion(socket: any, ctx: PreviewContext, state: ConnectionState): Promise<void> {
  if (!state.collector || !state.exchange || !state.symbol) {
    sendError(socket, "Target not set");
    return;
  }
  try {
    const companion = await loadCompanion(ctx, state.collector, state.exchange, state.symbol, state.timeframe);
    state.companion = companion;
    state.anchorIndex = await computeAnchorIndex(
      ctx,
      state.collector,
      state.exchange,
      state.symbol,
      state.timeframe,
      companion,
      state.startMs,
    );
    console.log(
      "[ws] meta",
      state.collector,
      state.exchange,
      state.symbol,
      state.timeframe,
      new Date(companion.startTs).toISOString(),
      new Date(companion.endTs).toISOString(),
    );
    sendMeta(socket, state);
  } catch (err) {
    console.error("[ws] failed to load companion", err);
    sendError(socket, err instanceof Error ? err.message : "Failed to load companion");
  }
}

function sendMeta(socket: any, state: ConnectionState): void {
  const companion = state.companion;
  if (!companion) return;
  const anchor = state.anchorIndex ?? Math.max(0, companion.records - 1);
  send(socket, {
    type: "meta",
    startTs: companion.startTs,
    endTs: companion.endTs,
    priceScale: companion.priceScale,
    volumeScale: companion.volumeScale,
    timeframeMs: companion.timeframeMs ?? 60_000,
    timeframe: companion.timeframe ?? state.timeframe,
    sparse: companion.sparse ?? false,
    records: companion.records,
    anchorIndex: anchor,
  });
}

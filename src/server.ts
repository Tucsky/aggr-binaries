import crypto from "node:crypto";
import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { openDatabase } from "./core/db.js";
import type { CompanionMetadata } from "./core/model.js";

const PORT = Number(process.env.PORT || 3000);
const OUTPUT_ROOT = path.resolve(process.env.OUTPUT_ROOT || "output");
const DB_PATH = path.resolve(process.env.DB_PATH || "index.sqlite");
const PUBLIC_DIR = path.resolve(process.env.PUBLIC_DIR || "client/dist");
const db = openDatabase(DB_PATH);

process.on("exit", () => {
  try {
    db.close();
  } catch {
    // ignore
  }
});
process.on("SIGINT", () => {
  try {
    db.close();
  } finally {
    process.exit(0);
  }
});

type Companion = CompanionMetadata;

interface CandleMsg {
  type: "candles";
  fromIndex: number;
  toIndex: number;
  candles: Array<{
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
    buyVol: number;
    sellVol: number;
    buyCount: number;
    sellCount: number;
    liqBuy: number;
    liqSell: number;
  }>;
}

async function loadCompanion(collector: string, exchange: string, symbol: string, timeframe: string): Promise<Companion> {
  const entry = db.getRegistryEntry({ collector, exchange, symbol, timeframe });
  if (!entry) {
    throw new Error(`Registry entry not found for ${collector}/${exchange}/${symbol}/${timeframe}`);
  }
  const companionPath = path.join(OUTPUT_ROOT, collector, exchange, symbol, `${timeframe}.json`);
  const raw = await fs.readFile(companionPath, "utf8");
  const parsed = JSON.parse(raw) as Companion;
  return {
    ...parsed,
    timeframe: parsed.timeframe ?? timeframe,
    sparse: parsed.sparse ?? entry.sparse,
    startTs: parsed.startTs ?? entry.startTs,
    endTs: parsed.endTs ?? entry.endTs,
  };
}

async function readCandles(
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
  fromIndex: number,
  toIndex: number,
  companion: Companion,
): Promise<CandleMsg> {
  if (companion.sparse) {
    return readCandlesSparse(collector, exchange, symbol, timeframe, fromIndex, toIndex, companion);
  }
  const binPath = path.join(OUTPUT_ROOT, collector, exchange, symbol, `${timeframe}.bin`);
  const fh = await fs.open(binPath, "r");
  const tf = companion.timeframeMs ?? 60_000;
  const firstIdx = Math.max(0, Math.min(companion.records - 1, fromIndex));
  const lastIdx = Math.max(firstIdx, Math.min(companion.records - 1, toIndex));
  const count = Math.max(0, lastIdx - firstIdx + 1);
  const buf = Buffer.allocUnsafe(count * 56);
  await fh.read(buf, 0, buf.length, firstIdx * 56);
  await fh.close();

  const candles: CandleMsg["candles"] = [];
  for (let i = 0; i < count; i++) {
    const base = i * 56;
    const open = buf.readInt32LE(base) / companion.priceScale;
    const high = buf.readInt32LE(base + 4) / companion.priceScale;
    const low = buf.readInt32LE(base + 8) / companion.priceScale;
    const close = buf.readInt32LE(base + 12) / companion.priceScale;
    const buyVol = Number(buf.readBigInt64LE(base + 16)) / companion.volumeScale;
    const sellVol = Number(buf.readBigInt64LE(base + 24)) / companion.volumeScale;
    const buyCount = buf.readUInt32LE(base + 32);
    const sellCount = buf.readUInt32LE(base + 36);
    const liqBuy = Number(buf.readBigInt64LE(base + 40)) / companion.volumeScale;
    const liqSell = Number(buf.readBigInt64LE(base + 48)) / companion.volumeScale;
    candles.push({
      time: companion.startTs + (firstIdx + i) * tf,
      open,
      high,
      low,
      close,
      buyVol,
      sellVol,
      buyCount,
      sellCount,
      liqBuy,
      liqSell,
    });
  }

  return { type: "candles", fromIndex: firstIdx, toIndex: lastIdx, candles };
}

async function readCandlesSparse(
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
  fromIndex: number,
  toIndex: number,
  companion: Companion,
): Promise<CandleMsg> {
  const binPath = path.join(OUTPUT_ROOT, collector, exchange, symbol, `${timeframe}.bin`);
  const buf = await fs.readFile(binPath);
  const recordSize = 8 + 56;
  const total = Math.floor(buf.length / recordSize);
  const firstIdx = Math.max(0, Math.min(total - 1, fromIndex));
  const lastIdx = Math.max(firstIdx, Math.min(total - 1, toIndex));
  const candles: CandleMsg["candles"] = [];
  for (let i = firstIdx; i <= lastIdx; i++) {
    const base = i * recordSize;
    const ts = Number(buf.readBigInt64LE(base));
    const open = buf.readInt32LE(base + 8) / companion.priceScale;
    const high = buf.readInt32LE(base + 12) / companion.priceScale;
    const low = buf.readInt32LE(base + 16) / companion.priceScale;
    const close = buf.readInt32LE(base + 20) / companion.priceScale;
    const buyVol = Number(buf.readBigInt64LE(base + 24)) / companion.volumeScale;
    const sellVol = Number(buf.readBigInt64LE(base + 32)) / companion.volumeScale;
    const buyCount = buf.readUInt32LE(base + 40);
    const sellCount = buf.readUInt32LE(base + 44);
    const liqBuy = Number(buf.readBigInt64LE(base + 48)) / companion.volumeScale;
    const liqSell = Number(buf.readBigInt64LE(base + 56)) / companion.volumeScale;
    candles.push({
      time: ts,
      open,
      high,
      low,
      close,
      buyVol,
      sellVol,
      buyCount,
      sellCount,
      liqBuy,
      liqSell,
    });
  }
  return { type: "candles", fromIndex: firstIdx, toIndex: firstIdx + candles.length - 1, candles };
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? "/", "http://localhost");
    let filePath = url.pathname === "/" ? "/index.html" : url.pathname;
    const abs = path.join(PUBLIC_DIR, filePath);
    const data = await fs.readFile(abs);
    const ext = path.extname(abs);
    const mime =
      ext === ".html"
        ? "text/html"
        : ext === ".js"
          ? "application/javascript"
          : ext === ".css"
            ? "text/css"
            : "application/octet-stream";
    res.writeHead(200, { "Content-Type": mime });
    res.end(data);
  } catch {
    res.writeHead(404);
    res.end("Not found");
  }
});

server.on("upgrade", async (req, socket) => {
  const url = new URL(req.url ?? "", "http://localhost");
  if (url.pathname !== "/ws") {
    socket.destroy();
    return;
  }

  const collector = (url.searchParams.get("collector") ?? "").toUpperCase();
  const exchange = (url.searchParams.get("exchange") ?? "").toUpperCase();
  const symbol = url.searchParams.get("symbol") ?? "";
  const timeframe = url.searchParams.get("timeframe") ?? "1m";
  const startOverride = url.searchParams.get("start");
  const startMs = startOverride ? Number(startOverride) : null;
  if (!collector || !exchange || !symbol) {
    socket.destroy();
    return;
  }

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

  let companion: Companion | null = null;
  let anchorIndex: number | null = null;
  try {
    companion = await loadCompanion(collector, exchange, symbol, timeframe);
    anchorIndex = await computeAnchorIndex(collector, exchange, symbol, timeframe, companion, startMs);
    console.log(
      "[ws] meta",
      collector,
      exchange,
      symbol,
      timeframe,
      new Date(companion.startTs).toISOString(),
      new Date(companion.endTs).toISOString(),
    );
    send(socket, {
      type: "meta",
      startTs: companion.startTs,
      endTs: companion.endTs,
      priceScale: companion.priceScale,
      volumeScale: companion.volumeScale,
      timeframeMs: companion.timeframeMs ?? 60_000,
      timeframe: companion.timeframe ?? timeframe,
      sparse: companion.sparse ?? false,
      records: companion.records,
      anchorIndex: anchorIndex ?? companion.records - 1,
    });
  } catch (err) {
    console.error("[ws] failed to load companion", err);
    socket.destroy();
    return;
  }

  socket.on("data", async (buf) => {
    const msg = decodeFrame(buf);
    if (!msg || msg.opcode !== 1 || !companion) return;
    try {
      const payload = JSON.parse(msg.data.toString()) as { type: string; fromIndex?: number; toIndex?: number };
      if (payload.type === "slice" && typeof payload.fromIndex === "number" && typeof payload.toIndex === "number") {
        const resp = await readCandles(collector, exchange, symbol, timeframe, payload.fromIndex, payload.toIndex, companion);
        send(socket, resp);
      }
    } catch {
      // ignore
    }
  });
});

function send(socket: any, data: any) {
  const json = Buffer.from(JSON.stringify(data));
  const frame = encodeFrame(json);
  socket.write(frame);
}

// Minimal WebSocket frame encoder/decoder (text frames only)
function encodeFrame(payload: Buffer): Buffer {
  const len = payload.length;
  let header: Buffer;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

function decodeFrame(buf: Buffer): { opcode: number; data: Buffer } | null {
  if (buf.length < 2) return null;
  const opcode = buf[0] & 0x0f;
  let length = buf[1] & 0x7f;
  let offset = 2;
  if (length === 126) {
    length = buf.readUInt16BE(2);
    offset += 2;
  } else if (length === 127) {
    const l = buf.readBigUInt64BE(2);
    length = Number(l);
    offset += 8;
  }
  const masked = (buf[1] & 0x80) !== 0;
  let maskingKey: Buffer | undefined;
  if (masked) {
    maskingKey = buf.slice(offset, offset + 4);
    offset += 4;
  }
  const data = buf.slice(offset, offset + length);
  if (masked && maskingKey) {
    for (let i = 0; i < data.length; i++) {
      data[i] ^= maskingKey[i % 4];
    }
  }
  return { opcode, data };
}

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});

async function computeAnchorIndex(
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
  companion: Companion,
  startMs: number | null,
): Promise<number> {
  const records = companion.records;
  if (!records || records <= 0) return 0;
  if (!startMs || Number.isNaN(startMs)) {
    return records - 1;
  }

  if (!companion.sparse) {
    const tf = companion.timeframeMs ?? 60_000;
    const idx = Math.floor((startMs - companion.startTs) / tf);
    return Math.max(0, Math.min(records - 1, idx));
  }

  // sparse: binary search by timestamp
  const binPath = path.join(OUTPUT_ROOT, collector, exchange, symbol, `${timeframe}.bin`);
  const recordSize = 8 + 56;
  let lo = 0;
  let hi = records - 1;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    const ts = await readSparseTimestamp(binPath, mid, recordSize);
    if (ts >= startMs) {
      hi = mid;
    } else {
      lo = mid + 1;
    }
  }
  return lo;
}

async function readSparseTimestamp(binPath: string, index: number, recordSize: number): Promise<number> {
  const fh = await fs.open(binPath, "r");
  const buf = Buffer.allocUnsafe(8);
  await fh.read(buf, 0, 8, index * recordSize);
  await fh.close();
  return Number(buf.readBigInt64LE(0));
}

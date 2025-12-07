import crypto from "node:crypto";
import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";

const PORT = Number(process.env.PORT || 3000);
const OUTPUT_ROOT = path.resolve(process.env.OUTPUT_ROOT || "output");
const PUBLIC_DIR = path.resolve(process.env.PUBLIC_DIR || "public");

interface Companion {
  exchange: string;
  symbol: string;
  timeframe: string;
  startTs: number;
  endTs: number;
  priceScale: number;
  volumeScale: number;
  records: number;
}

interface CandleMsg {
  type: "candles";
  from: number;
  to: number;
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

async function readCompanion(collector: string, exchange: string, symbol: string): Promise<Companion> {
  const companionPath = path.join(OUTPUT_ROOT, collector, exchange, `${symbol}.json`);
  const raw = await fs.readFile(companionPath, "utf8");
  return JSON.parse(raw) as Companion;
}

async function readCandles(
  collector: string,
  exchange: string,
  symbol: string,
  fromTs: number,
  toTs: number,
  companion: Companion,
): Promise<CandleMsg> {
  const binPath = path.join(OUTPUT_ROOT, collector, exchange, `${symbol}.bin`);
  const fh = await fs.open(binPath, "r");
  const firstIdx = Math.max(0, Math.floor((fromTs - companion.startTs) / 60000));
  const lastIdx = Math.min(companion.records - 1, Math.floor((toTs - companion.startTs) / 60000));
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
      time: companion.startTs + (firstIdx + i) * 60000,
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

  return { type: "candles", from: fromTs, to: toTs, candles };
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
  try {
    companion = await readCompanion(collector, exchange, symbol);
    console.log(
      "[ws] meta",
      collector,
      exchange,
      symbol,
      new Date(companion.startTs).toISOString(),
      new Date(companion.endTs).toISOString(),
    );
    send(socket, {
      type: "meta",
      startTs: companion.startTs,
      endTs: companion.endTs,
      priceScale: companion.priceScale,
      volumeScale: companion.volumeScale,
    });
  } catch {
    socket.destroy();
    return;
  }

  socket.on("data", async (buf) => {
    const msg = decodeFrame(buf);
    if (!msg || msg.opcode !== 1 || !companion) return;
    try {
      const payload = JSON.parse(msg.data.toString()) as { type: string; from?: number; to?: number };
      if (payload.type === "range" && typeof payload.from === "number" && typeof payload.to === "number") {
        console.log(
          "[ws] range request",
          new Date(payload.from).toISOString(),
          "->",
          new Date(payload.to).toISOString(),
        );
        const resp = await readCandles(collector, exchange, symbol, payload.from, payload.to, companion);
        if (resp.candles.length) {
          send(socket, resp);
        } else {
          console.log("[ws] range request - no data in range");
        }
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

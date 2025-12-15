export interface DecodedFrame {
  opcode: number;
  data: Buffer;
}

export function encodeFrame(payload: Buffer): Buffer {
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

export function decodeFrames(buf: Buffer): { frames: DecodedFrame[]; rest: Buffer } {
  const frames: DecodedFrame[] = [];
  let offset = 0;
  while (offset + 2 <= buf.length) {
    const opcode = buf[offset] & 0x0f;
    let length = buf[offset + 1] & 0x7f;
    let headerSize = 2;
    if (length === 126) {
      if (offset + 4 > buf.length) break;
      length = buf.readUInt16BE(offset + 2);
      headerSize = 4;
    } else if (length === 127) {
      if (offset + 10 > buf.length) break;
      const l = buf.readBigUInt64BE(offset + 2);
      length = Number(l);
      headerSize = 10;
    }
    const masked = (buf[offset + 1] & 0x80) !== 0;
    const maskOffset = offset + headerSize;
    const payloadOffset = maskOffset + (masked ? 4 : 0);
    const frameEnd = payloadOffset + length;
    if (frameEnd > buf.length) break;

    const data = Buffer.from(buf.slice(payloadOffset, frameEnd));
    if (masked) {
      const maskingKey = buf.slice(maskOffset, maskOffset + 4);
      for (let i = 0; i < data.length; i++) {
        data[i] ^= maskingKey[i % 4];
      }
    }

    frames.push({ opcode, data });
    offset = frameEnd;
  }
  const rest = Buffer.from(buf.slice(offset));
  return { frames, rest };
}

import fs from "node:fs";
import zlib from "node:zlib";

export async function openTradeReadStream(filePath: string): Promise<NodeJS.ReadableStream> {
  const raw = fs.createReadStream(filePath);
  if (filePath.endsWith(".gz")) {
    return raw.pipe(zlib.createGunzip());
  }
  return raw;
}

import path from "node:path";
import { openDatabase } from "./core/db.js";
import { createStaticServer } from "./server/staticServer.js";
import { attachPreviewWs } from "./server/previewWs.js";
import type { PreviewContext } from "./server/previewData.js";
import { createTimelineApiHandler } from "./server/timelineApi.js";

const PORT = Number(process.env.PORT || 3000);
const OUTPUT_ROOT = path.resolve(process.env.OUTPUT_ROOT || "output");
const DB_PATH = path.resolve(process.env.DB_PATH || "index.sqlite");
const PUBLIC_DIR = path.resolve(process.env.PUBLIC_DIR || "client/dist");
const CONFIG_PATH = process.env.CONFIG_PATH;

const db = openDatabase(DB_PATH);
const ctx: PreviewContext = { db, outputRoot: OUTPUT_ROOT };
const server = createStaticServer(
  PUBLIC_DIR,
  createTimelineApiHandler(db, {
    dbPath: DB_PATH,
    outDir: OUTPUT_ROOT,
    configPath: CONFIG_PATH,
  }),
);
attachPreviewWs(server, ctx);

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

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});

import path from "node:path";
import { loadConfig, type CliOverrides } from "./core/config.js";
import { openDatabase } from "./core/db.js";
import { createStaticServer } from "./server/staticServer.js";
import { attachPreviewWs } from "./server/previewWs.js";
import type { PreviewContext } from "./server/previewData.js";
import { createTimelineApiHandler } from "./server/timelineApi.js";

function closeQuietly(close: () => void): void {
  try {
    close();
  } catch {
    // ignore
  }
}

async function main(): Promise<void> {
  const configPath = trimToUndefined(process.env.CONFIG_PATH);
  const dbPathOverride = trimToUndefined(process.env.DB_PATH);
  const outDirOverride = trimToUndefined(process.env.OUTPUT_ROOT);
  const configOverrides: CliOverrides = {
    ...(configPath ? { configPath } : {}),
    ...(dbPathOverride ? { dbPath: dbPathOverride } : {}),
    ...(outDirOverride ? { outDir: outDirOverride } : {}),
  };
  const coreConfig = await loadConfig(configOverrides);
  const port = parsePortOrDefault(trimToUndefined(process.env.PORT), 3000);
  const publicDir = path.resolve(trimToUndefined(process.env.PUBLIC_DIR) ?? "client/dist");

  const db = openDatabase(coreConfig.dbPath);
  const ctx: PreviewContext = { db, outputRoot: coreConfig.outDir };
  const server = createStaticServer(
    publicDir,
    createTimelineApiHandler(db, {
      dbPath: coreConfig.dbPath,
      outDir: coreConfig.outDir,
      configPath,
    }),
  );
  attachPreviewWs(server, ctx);

  process.on("exit", () => {
    closeQuietly(() => db.close());
  });
  process.on("SIGINT", () => {
    try {
      closeQuietly(() => db.close());
    } finally {
      process.exit(0);
    }
  });

  server.listen(port, () => {
    console.log(`Server listening on http://localhost:${port}`);
  });
}

main().catch((err) => {
  console.error("Server startup failed:", err);
  process.exit(1);
});

function trimToUndefined(value?: string): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function parsePortOrDefault(raw: string | undefined, fallback: number): number {
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return fallback;
  const port = Math.floor(parsed);
  if (port < 1 || port > 65_535) return fallback;
  return port;
}

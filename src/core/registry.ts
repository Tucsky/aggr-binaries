import fs from "node:fs/promises";
import path from "node:path";
import type { Config } from "./config.js";
import type { Db } from "./db.js";
import type { CompanionMetadata, RegistryEntry, RegistryFilter } from "./model.js";
import { normalizeCompanionRange } from "./model.js";

export interface RegistryStats {
  scanned: number;
  upserted: number;
  deleted: number;
}

export async function runRegistry(config: Config, db: Db): Promise<RegistryStats> {
  const outRoot = config.outDir;
  const filter: RegistryFilter = {
    collector: config.collector?.toUpperCase(),
    exchange: config.exchange?.toUpperCase(),
    symbol: config.symbol,
  };

  const rootStat = await fs
    .stat(outRoot)
    .catch((err: NodeJS.ErrnoException) => {
      if (err.code === "ENOENT") {
        throw new Error(`Output directory does not exist: ${outRoot}`);
      }
      throw err;
    });
  if (!rootStat.isDirectory()) {
    throw new Error(`Output path is not a directory: ${outRoot}`);
  }

  const entries: RegistryEntry[] = [];
  for await (const entry of walkCompanions(outRoot, filter)) {
    entries.push(entry);
  }

  const res = db.replaceRegistry(entries, filter);
  return { scanned: entries.length, upserted: res.upserted, deleted: res.deleted };
}

async function* walkCompanions(outRoot: string, filter: RegistryFilter): AsyncGenerator<RegistryEntry> {
  const collectors = await fs.readdir(outRoot, { withFileTypes: true });
  console.log(outRoot)
  for (const collectorDir of collectors) {
    if (!collectorDir.isDirectory()) continue;
    const collector = collectorDir.name.toUpperCase();
    if (filter.collector && collector !== filter.collector) continue;
    const collectorPath = path.join(outRoot, collectorDir.name);

    const exchanges = await fs.readdir(collectorPath, { withFileTypes: true });
    for (const exchangeDir of exchanges) {
      if (!exchangeDir.isDirectory()) continue;
      const exchange = exchangeDir.name.toUpperCase();
      if (filter.exchange && exchange !== filter.exchange) continue;
      const exchangePath = path.join(collectorPath, exchangeDir.name);

      const symbols = await fs.readdir(exchangePath, { withFileTypes: true });
      for (const symbolDir of symbols) {
        if (!symbolDir.isDirectory()) continue;
        const symbol = symbolDir.name;
        if (filter.symbol && symbol !== filter.symbol) continue;
        const symbolPath = path.join(exchangePath, symbolDir.name);

        const files = await fs.readdir(symbolPath, { withFileTypes: true });
        for (const file of files) {
          if (!file.isFile() || !file.name.endsWith(".json")) continue;
          const timeframe = file.name.replace(/\.json$/, "");
          if (filter.timeframe && timeframe !== filter.timeframe) continue;
          const companionPath = path.join(symbolPath, file.name);
          const metadata = await readCompanion(companionPath);
          
          // Normalize to handle both monolithic and segmented formats
          const normalized = normalizeCompanionRange({
            ...metadata,
            exchange,
            symbol,
            timeframe: metadata.timeframe ?? timeframe,
          });

          yield {
            collector,
            exchange,
            symbol,
            timeframe: normalized.timeframe,
            startTs: normalized.startTs,
            endTs: normalized.endTs,
            sparse: Boolean(normalized.sparse),
          };
        }
      }
    }
  }
}

async function readCompanion(filePath: string): Promise<CompanionMetadata> {
  const raw = await fs.readFile(filePath, "utf8");
  try {
    return JSON.parse(raw) as CompanionMetadata;
  } catch (err) {
    throw new Error(`Failed to parse companion ${filePath}: ${String(err)}`);
  }
}

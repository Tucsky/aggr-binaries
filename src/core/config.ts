import fs from "node:fs/promises";
import path from "node:path";
import { parseTimeframeMs } from "../shared/timeframes.js";

export interface Config {
  root: string;
  dbPath: string;
  batchSize: number;
  includePaths?: string[]; // relative to root
  // processing options (optional overrides from CLI)
  collector?: string;
  exchange?: string;
  symbol?: string;
  outDir: string;
  force?: boolean;
  timeframe: string; // e.g., "1m", "5m", "1h"
  timeframeMs: number;
}

export interface CliOverrides extends Partial<Config> {
  configPath?: string;
  useConfig?: boolean;
}

const DEFAULTS: Config = {
  root: "/Volumes/AGGR/input",
  dbPath: "index.sqlite",
  batchSize: 1000,
  outDir: "output",
  timeframe: "1m",
  timeframeMs: 60_000,
};

export async function loadConfig(overrides: CliOverrides = {}): Promise<Config> {
  const useConfig = overrides.useConfig !== false;
  const configPath = overrides.configPath ?? "config.json";
  let fileConfig: Partial<Config> = {};

  if (useConfig) {
    const resolved = path.resolve(configPath);
    try {
      const raw = await fs.readFile(resolved, "utf8");
      fileConfig = JSON.parse(raw) as Partial<Config>;
    } catch (err) {
      const code = (err as { code?: string }).code;
      if (code !== "ENOENT" || overrides.configPath) {
        throw new Error(`Failed to read config ${resolved}: ${String(err)}`);
      }
    }
  }

  const merged: Config = {
    ...DEFAULTS,
    ...fileConfig,
    ...overrides,
  };

  merged.root = path.resolve(merged.root);
  merged.dbPath = path.resolve(merged.dbPath);
  merged.outDir = path.resolve(merged.outDir || DEFAULTS.outDir);
  merged.timeframe = (merged.timeframe || DEFAULTS.timeframe).trim();
  merged.timeframeMs = parseTimeframeMs(merged.timeframe) ?? DEFAULTS.timeframeMs;

  if (!Number.isFinite(merged.batchSize) || merged.batchSize <= 0) {
    merged.batchSize = DEFAULTS.batchSize;
  }

  if (merged.includePaths && !Array.isArray(merged.includePaths)) {
    merged.includePaths = undefined;
  }

  return merged;
}

export function printHelp(): void {
  const help = `
Usage: npm start -- <command> [options]

Commands:
  index      Index filesystem into SQLite
  process    Build/update binaries from indexed files (resume supported)
  registry   Rebuild registry table from existing companions

Options (override config file):
  -r, --root <path>         Root input directory (default: ${DEFAULTS.root})
  -d, --db <path>           SQLite index path (default: ${DEFAULTS.dbPath})
  -b, --batch <n>           Inserts per transaction (default: ${DEFAULTS.batchSize})
  --include <path>          Relative path (under root) to scan; repeatable (default: whole root)
  --collector <RAM|PI>      Processing: target collector
  --exchange <name>         Processing: target exchange (normalized, e.g., BITFINEX)
  --symbol <name>           Processing: target symbol (normalized, e.g., BTCUSD)
  --outdir <path>           Processing: output directory (default: ${DEFAULTS.outDir})
  --force                   Processing: ignore processed-files cache
  --timeframe <tf>          Processing: timeframe string (e.g., 1m, 5m, 1h) (default: ${DEFAULTS.timeframe})
  --config <path>           Path to JSON config (default: ./config.json if present)
  --no-config               Skip loading any config file
  -h, --help                Show this help
`;
  console.log(help.trim());
}

export { parseTimeframeMs };

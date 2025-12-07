import fs from "node:fs/promises";
import path from "node:path";

export interface Config {
  root: string;
  dbPath: string;
  concurrency: number;
  batchSize: number;
  verifyExisting: boolean;
  includePaths?: string[]; // relative to root
  // processing options (optional overrides from CLI)
  collector?: string;
  exchange?: string;
  symbol?: string;
  outDir?: string;
  force?: boolean;
}

export interface CliOverrides extends Partial<Config> {
  configPath?: string;
  useConfig?: boolean;
}

const DEFAULTS: Config = {
  root: "/Volumes/AGGR/input",
  dbPath: "index.sqlite",
  concurrency: 32,
  batchSize: 1000,
  verifyExisting: false,
};

export async function loadConfig(overrides: CliOverrides = {}): Promise<Config> {
  const useConfig = overrides.useConfig !== false;
  const configPath = overrides.configPath ?? "indexer.config.json";
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

  if (!Number.isFinite(merged.concurrency) || merged.concurrency <= 0) {
    merged.concurrency = DEFAULTS.concurrency;
  }
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
  process    (stub) Processing step, not implemented yet

Options (override config file):
  -r, --root <path>         Root input directory (default: ${DEFAULTS.root})
  -d, --db <path>           SQLite index path (default: ${DEFAULTS.dbPath})
  -c, --concurrency <n>     Max concurrent stat calls (default: ${DEFAULTS.concurrency})
  -b, --batch <n>           Inserts per transaction (default: ${DEFAULTS.batchSize})
  --verify                  Check size/mtime for already indexed files
  --include <path>          Relative path (under root) to scan; repeatable (default: whole root)
  --collector <RAM|PI>      Processing: target collector
  --exchange <name>         Processing: target exchange (normalized, e.g., BITFINEX)
  --symbol <name>           Processing: target symbol (normalized, e.g., BTCUSD)
  --outdir <path>           Processing: output directory (default: ./output)
  --force                   Processing: ignore processed-files cache
  --config <path>           Path to JSON config (default: ./indexer.config.json if present)
  --no-config               Skip loading any config file
  -h, --help                Show this help
`;
  console.log(help.trim());
}

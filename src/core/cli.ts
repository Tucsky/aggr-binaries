import { loadConfig, printHelp } from "./config.js";
import { openDatabase } from "./db.js";
import { runFixGaps } from "./gaps/index.js";
import { runIndex } from "./indexer.js";
import { runProcess } from "./process.js";
import { runRegistry } from "./registry.js";

interface ParsedArgs {
  command: "index" | "process" | "registry" | "fixgaps";
  overrides: {
    root?: string;
    dbPath?: string;
    batchSize?: number;
    includePaths?: string[];
    configPath?: string;
    useConfig?: boolean;
    collector?: string;
    exchange?: string;
    symbol?: string;
    outDir?: string;
    force?: boolean;
    timeframe?: string;
    flushIntervalSeconds?: number;
  };
  fixgaps: {
    limit?: number;
    retryStatuses?: string[];
    dryRun?: boolean;
    id?: number;
  };
  showHelp: boolean;
}

function parseArgs(argv: string[]): ParsedArgs {
  const parsed: ParsedArgs = {
    command: "index",
    overrides: {},
    fixgaps: {},
    showHelp: false,
  };

  // command is first non-flag argument
  const args = argv.slice(2);
  if (args[0] && !args[0].startsWith("-")) {
    if (args[0] === "index" || args[0] === "process" || args[0] === "registry" || args[0] === "fixgaps") {
      parsed.command = args[0];
      args.shift();
    } else {
      throw new Error(`Unknown command: ${args[0]}`);
    }
  }

  const normalized: string[] = [];
  for (const arg of args) {
    if (arg.startsWith("--") && arg.includes("=")) {
      const [k, v] = arg.split("=", 2);
      normalized.push(k, v);
    } else {
      normalized.push(arg);
    }
  }

  for (let i = 0; i < normalized.length; i += 1) {
    const arg = normalized[i];
    switch (arg) {
      case "-r":
      case "--root":
        parsed.overrides.root = normalized[++i];
        break;
      case "-d":
      case "--db":
        parsed.overrides.dbPath = normalized[++i];
        break;
      case "-c":
      case "-b":
      case "--batch":
        parsed.overrides.batchSize = Number(normalized[++i]);
        break;
      case "--include": {
        const val = normalized[++i];
        if (!val) throw new Error("Missing value for --include");
        if (!parsed.overrides.includePaths) parsed.overrides.includePaths = [];
        parsed.overrides.includePaths.push(val);
        break;
      }
      case "--config":
        parsed.overrides.configPath = normalized[++i];
        parsed.overrides.useConfig = true;
        break;
      case "--no-config":
        parsed.overrides.useConfig = false;
        break;
      case "--collector":
        parsed.overrides.collector = normalized[++i];
        break;
      case "--exchange":
        parsed.overrides.exchange = normalized[++i];
        break;
      case "--symbol":
        parsed.overrides.symbol = normalized[++i];
        break;
      case "--outdir":
        parsed.overrides.outDir = normalized[++i];
        break;
      case "--force":
        parsed.overrides.force = true;
        break;
      case "--timeframe":
        parsed.overrides.timeframe = normalized[++i];
        break;
      case "--flush-interval":
        parsed.overrides.flushIntervalSeconds = Number(normalized[++i]);
        break;
      case "--limit":
        parsed.fixgaps.limit = Number(normalized[++i]);
        break;
      case "--retry-status": {
        const raw = normalized[++i] ?? "";
        const statuses = raw
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
        parsed.fixgaps.retryStatuses = statuses.length ? statuses : undefined;
        break;
      }
      case "--dry-run":
        parsed.fixgaps.dryRun = true;
        break;
      case "--id":
        parsed.fixgaps.id = Number(normalized[++i]);
        break;
      case "-h":
      case "--help":
        parsed.showHelp = true;
        break;
      default:
        throw new Error(`Unknown flag: ${arg}`);
    }
  }

  return parsed;
}

async function main() {
  let parsed: ParsedArgs;
  try {
    parsed = parseArgs(process.argv);
  } catch (err) {
    console.error(String(err));
    printHelp();
    process.exit(1);
    return;
  }

  if (parsed.showHelp) {
    printHelp();
    process.exit(0);
  }

  let config;
  try {
    config = await loadConfig(parsed.overrides);
  } catch (err) {
    console.error(String(err));
    process.exit(1);
    return;
  }

  const db = openDatabase(config.dbPath);
  const start = Date.now();

  try {
    if (parsed.command === "index") {
      console.log(
        `Indexing from ${config.root} -> ${config.dbPath} (batch=${config.batchSize})`,
      );
      const stats = await runIndex(config, db);
      const duration = ((Date.now() - start) / 1000).toFixed(1);
      console.log(
        `Done in ${duration}s. inserted=${stats.inserted} existing=${stats.existing} conflicts=${stats.conflicts} skipped=${stats.skipped}`,
      );
    }
    if (parsed.command === "process") {
      await runProcess(config, db);
    }
    if (parsed.command === "registry") {
      console.log(
        `Rebuilding registry from ${config.outDir ?? "output"}${config.collector ? ` collector=${config.collector}` : ""}${config.exchange ? ` exchange=${config.exchange}` : ""}${config.symbol ? ` symbol=${config.symbol}` : ""}`,
      );
      const stats = await runRegistry(config, db);
      const duration = ((Date.now() - start) / 1000).toFixed(1);
      console.log(
        `Registry synced in ${duration}s. scanned=${stats.scanned} upserted=${stats.upserted} deleted=${stats.deleted}`,
      );
    }
    if (parsed.command === "fixgaps") {
      await runFixGaps(config, db, {
        limit: parsed.fixgaps.limit,
        retryStatuses: parsed.fixgaps.retryStatuses,
        dryRun: parsed.fixgaps.dryRun,
        id: parsed.fixgaps.id,
      });
    }
  } catch (err) {
    console.error("Operation failed:", err);
    process.exit(1);
  } finally {
    db.close();
  }
}

main();

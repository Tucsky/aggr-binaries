import { loadConfig, printHelp } from "./config.js";
import { openDatabase } from "./db.js";
import { runIndex } from "./indexer.js";
import { runProcess } from "./process.js";

interface ParsedArgs {
  command: "index" | "process";
  overrides: {
    root?: string;
    dbPath?: string;
    concurrency?: number;
    batchSize?: number;
    verifyExisting?: boolean;
    includePaths?: string[];
    configPath?: string;
    useConfig?: boolean;
    collector?: string;
    exchange?: string;
    symbol?: string;
    outDir?: string;
    force?: boolean;
  };
  showHelp: boolean;
}

function parseArgs(argv: string[]): ParsedArgs {
  const parsed: ParsedArgs = {
    command: "index",
    overrides: {},
    showHelp: false,
  };

  // command is first non-flag argument
  const args = argv.slice(2);
  if (args[0] && !args[0].startsWith("-")) {
    if (args[0] === "index" || args[0] === "process") {
      parsed.command = args[0];
      args.shift();
    } else {
      throw new Error(`Unknown command: ${args[0]}`);
    }
  }

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    switch (arg) {
      case "-r":
      case "--root":
        parsed.overrides.root = args[++i];
        break;
      case "-d":
      case "--db":
        parsed.overrides.dbPath = args[++i];
        break;
      case "-c":
      case "--concurrency":
        parsed.overrides.concurrency = Number(args[++i]);
        break;
      case "-b":
      case "--batch":
        parsed.overrides.batchSize = Number(args[++i]);
        break;
      case "--verify":
        parsed.overrides.verifyExisting = true;
        break;
      case "--include": {
        const val = args[++i];
        if (!val) throw new Error("Missing value for --include");
        if (!parsed.overrides.includePaths) parsed.overrides.includePaths = [];
        parsed.overrides.includePaths.push(val);
        break;
      }
      case "--config":
        parsed.overrides.configPath = args[++i];
        parsed.overrides.useConfig = true;
        break;
      case "--no-config":
        parsed.overrides.useConfig = false;
        break;
      case "--collector":
        parsed.overrides.collector = args[++i];
        break;
      case "--exchange":
        parsed.overrides.exchange = args[++i];
        break;
      case "--symbol":
        parsed.overrides.symbol = args[++i];
        break;
      case "--outdir":
        parsed.overrides.outDir = args[++i];
        break;
      case "--force":
        parsed.overrides.force = true;
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
        `Indexing from ${config.root} -> ${config.dbPath} (concurrency=${config.concurrency}, batch=${config.batchSize}, verify=${config.verifyExisting})`,
      );
      const stats = await runIndex(config, db);
      const duration = ((Date.now() - start) / 1000).toFixed(1);
      console.log(
        `Done in ${duration}s. inserted=${stats.inserted} existing=${stats.existing} conflicts=${stats.conflicts} skipped=${stats.skipped}`,
      );
    } else {
      await runProcess(config, db);
    }
  } catch (err) {
    console.error("Operation failed:", err);
    process.exit(1);
  } finally {
    db.close();
  }
}

main();

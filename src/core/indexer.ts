import path from "node:path";
import type { Db } from "./db.js";
import type { Config } from "./config.js";
import { classifyPath } from "./normalize.js";
import type { IndexStats, IndexedFile } from "./model.js";
import { walkFiles } from "./walker.js";

export async function runIndex(config: Config, db: Db): Promise<IndexStats> {
  const rootId = db.ensureRoot(config.root);
  const stats: IndexStats = { seen: 0, inserted: 0, existing: 0, conflicts: 0, skipped: 0 };

  let batch: IndexedFile[] = [];
  const collectorHint = deriveCollectorHint(config.root);
  let skipLogged = 0;

  for await (const entry of walkFiles(config.root, rootId, {
    includePaths: config.includePaths,
  })) {
    stats.seen += 1;

    const row = classifyPath(entry.rootId, entry.relativePath, collectorHint);
    if (!row) {
      stats.skipped += 1;
      if (skipLogged < 50) {
        console.log(`[skip] ${entry.relativePath}`);
        skipLogged++;
      }
      continue;
    }

    batch.push(row);
    if (batch.length >= config.batchSize) {
      const res = db.insertFiles(batch);
      stats.inserted += res.inserted;
      stats.existing += res.existing;
      batch = [];
    }

    if (stats.seen % 10_000 === 0) {
      logProgress(stats, batch.length);
    }
  }

  if (batch.length) {
    const res = db.insertFiles(batch);
    stats.inserted += res.inserted;
    stats.existing += res.existing;
  }

  return stats;
}

function logProgress(stats: IndexStats, pendingBatch: number): void {
  console.log(
    `[progress] seen=${stats.seen} inserted=${stats.inserted} existing=${stats.existing} conflicts=${stats.conflicts} skipped=${stats.skipped} pendingBatch=${pendingBatch}`,
  );
}

function deriveCollectorHint(rootPath: string) {
  const base = rootPath.split(path.sep).pop();
  if (!base) return undefined;
  const upper = base.toUpperCase();
  if (upper === "RAM" || upper === "PI") return upper as any;
  return undefined;
}

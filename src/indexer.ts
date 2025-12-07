import type { Db } from "./db.js";
import type { Config } from "./config.js";
import { classifyPath } from "./normalize.js";
import type { IndexStats, IndexedFile } from "./model.js";
import { walkFiles } from "./walker.js";

export async function runIndex(config: Config, db: Db): Promise<IndexStats> {
  const rootId = db.ensureRoot(config.root);
  const stats: IndexStats = { seen: 0, inserted: 0, existing: 0, conflicts: 0, skipped: 0 };

  let batch: IndexedFile[] = [];

  for await (const entry of walkFiles(config.root, rootId, {
    statConcurrency: config.concurrency,
    includePaths: config.includePaths,
  })) {
    stats.seen += 1;

    const row = classifyPath(entry.rootId, entry.relativePath, entry.size, entry.mtimeMs);
    if (!row) {
      stats.skipped += 1;
      continue;
    }

    if (config.verifyExisting) {
      const existing = db.getExistingMeta(row.rootId, row.relativePath);
      if (existing) {
        stats.existing += 1;
        if (existing.size !== row.size || existing.mtime_ms !== row.mtimeMs) {
          stats.conflicts += 1;
        }
        continue;
      }
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

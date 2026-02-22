import fs from "node:fs/promises";
import path from "node:path";
import type { Db } from "../core/db.js";

interface TimelineClearMarket {
  collector: string;
  exchange: string;
  symbol: string;
}

export interface TimelineClearStats {
  outputsDeleted: number;
  eventsDeleted: number;
  filesDeleted: number;
  registryDeleted: number;
}

export async function clearTimelineMarket(
  db: Db,
  outDir: string,
  market: TimelineClearMarket,
): Promise<TimelineClearStats> {
  const outputsDeleted = await deleteMarketOutputs(outDir, market);
  const dbStats = deleteMarketRows(db, market);
  return {
    outputsDeleted,
    eventsDeleted: dbStats.eventsDeleted,
    filesDeleted: dbStats.filesDeleted,
    registryDeleted: dbStats.registryDeleted,
  };
}

async function deleteMarketOutputs(
  outDir: string,
  market: TimelineClearMarket,
): Promise<number> {
  const marketOutDir = path.join(outDir, market.collector, market.exchange, market.symbol);
  try {
    await fs.rm(marketOutDir, { recursive: true });
    return 1;
  } catch (err) {
    if ((err as { code?: string }).code === "ENOENT") {
      return 0;
    }
    throw err;
  }
}

function deleteMarketRows(
  db: Db,
  market: TimelineClearMarket,
): { eventsDeleted: number; filesDeleted: number; registryDeleted: number } {
  const params = {
    collector: market.collector,
    exchange: market.exchange,
    symbol: market.symbol,
  };
  const deleteEventsStmt = db.db.prepare(
    `DELETE FROM events
     WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol;`,
  );
  const deleteFilesStmt = db.db.prepare(
    `DELETE FROM files
     WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol;`,
  );
  const deleteRegistryStmt = db.db.prepare(
    `DELETE FROM registry
     WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol;`,
  );

  db.db.exec("BEGIN");
  try {
    const eventsDeleted = Number(deleteEventsStmt.run(params).changes ?? 0);
    const filesDeleted = Number(deleteFilesStmt.run(params).changes ?? 0);
    const registryDeleted = Number(deleteRegistryStmt.run(params).changes ?? 0);
    db.db.exec("COMMIT");
    return { eventsDeleted, filesDeleted, registryDeleted };
  } catch (err) {
    db.db.exec("ROLLBACK");
    throw err;
  }
}

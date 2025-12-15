import type { Db } from "../core/db.js";

export interface MarketEntry {
  collector: string;
  exchange: string;
  symbol: string;
}

export function listMarkets(db: Db): MarketEntry[] {
  const rows =
    (db.db
      .prepare("SELECT DISTINCT collector, exchange, symbol FROM registry ORDER BY collector, exchange, symbol;")
      .all() as Array<{ collector: string; exchange: string; symbol: string }>) ?? [];
  return rows.map((r) => ({
    collector: r.collector.toUpperCase(),
    exchange: r.exchange.toUpperCase(),
    symbol: r.symbol,
  }));
}

export function listTimeframes(db: Db, collector: string, exchange: string, symbol: string): string[] {
  const rows =
    (db.db
      .prepare(
        "SELECT timeframe FROM registry WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol ORDER BY timeframe;",
      )
      .all({ collector, exchange, symbol }) as Array<{ timeframe: string }>) ?? [];
  return rows.map((r) => r.timeframe);
}

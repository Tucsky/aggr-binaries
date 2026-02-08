import type { Db, GapFixQueueRow } from "../db.js";

export interface FixGapsQueryOptions {
  collector?: string;
  exchange?: string;
  symbol?: string;
  retryStatuses?: string[];
  limit?: number;
  id?: number;
}

export type GapFixEventRow = GapFixQueueRow;

export function iterateGapFixEvents(db: Db, opts: FixGapsQueryOptions): Iterable<GapFixEventRow> {
  return db.iterateGapEventsForFix({
    collector: opts.collector,
    exchange: opts.exchange,
    symbol: opts.symbol,
    retryStatuses: opts.retryStatuses,
    limit: opts.limit,
    id: opts.id,
  });
}

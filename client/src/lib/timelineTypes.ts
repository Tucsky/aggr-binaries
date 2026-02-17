export interface TimelineMarket {
  collector: string;
  exchange: string;
  symbol: string;
  timeframe: string;
  startTs: number;
  endTs: number;
}

export interface TimelineEvent {
  id: number;
  collector: string;
  exchange: string;
  symbol: string;
  eventType: string;
  gapFixStatus: string | null;
  ts: number;
  startLine: number;
  endLine: number;
  gapMs: number | null;
  gapMiss: number | null;
}

export type TimelineEventKind =
  | "gap"
  | "parse_error"
  | "adapter_error"
  | "missing_adapter"
  | "unknown";

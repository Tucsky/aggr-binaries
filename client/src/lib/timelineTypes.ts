export interface TimelineMarket {
  collector: string;
  exchange: string;
  symbol: string;
  timeframe: string;
  startTs: number;
  endTs: number;
  indexedStartTs?: number | null;
  indexedEndTs?: number | null;
  processedStartTs?: number | null;
  processedEndTs?: number | null;
}

export interface TimelineEvent {
  id: number;
  collector: string;
  exchange: string;
  symbol: string;
  relativePath: string;
  eventType: string;
  gapFixStatus: string | null;
  ts: number;
  startLine: number;
  endLine: number;
  gapMs: number | null;
  gapMiss: number | null;
}

export interface TimelineHoverEvent {
  event: TimelineEvent;
  market: TimelineMarket;
  pointerClientX: number;
  pointerClientY: number;
  markerLeftClientX: number;
  markerRightClientX: number;
  markerTopClientY: number;
  markerBottomClientY: number;
}

export enum TimelineMarketAction {
  Index = "index",
  Process = "process",
  FixGaps = "fixgaps",
  Registry = "registry",
}

export type TimelineEventKind =
  | "gap"
  | "parse_error"
  | "adapter_error"
  | "missing_adapter"
  | "unknown";

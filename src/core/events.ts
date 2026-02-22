import type { CompanionMetadata } from "./model.js";

export enum EventType {
  PartsShort = "parts_short",
  NonFinite = "non_finite",
  InvalidTsRange = "invalid_ts_range",
  NotionalTooLarge = "notional_too_large",
  Gap = "gap",
}

export enum GapFixStatus {
  MissingAdapter = "missing_adapter",
  AdapterError = "adapter_error",
  Fixed = "fixed",
}

export type ParseRejectReason =
  | EventType.PartsShort
  | EventType.NonFinite
  | EventType.InvalidTsRange
  | EventType.NotionalTooLarge;

export interface EventContext {
  rootId: number;
  relativePath: string;
  collector: string;
  exchange: string;
  symbol: string;
}

export interface EventRange extends EventContext {
  type: EventType;
  startLine: number;
  endLine: number;
  gapMs?: number;
  gapMiss?: number;
  gapEndTs?: number;
}

interface PendingRange {
  type: EventType;
  startLine: number;
  endLine: number;
  gapMs?: number;
  gapMiss?: number;
  gapEndTs?: number;
}

export class EventAccumulator {
  private readonly ranges: EventRange[] = [];
  private current?: PendingRange;

  constructor(private readonly ctx: EventContext) {}

  record(type: EventType, line: number, gapMs?: number, gapMiss?: number, gapEndTs?: number): void {
    const current = this.current;
    if (current && current.type === type && line === current.endLine + 1) {
      current.endLine = line;
      if (type === EventType.Gap && gapMs !== undefined) {
        const prev = current.gapMs ?? 0;
        if (gapMs > prev) {
          current.gapMs = gapMs;
          current.gapEndTs = gapEndTs ?? current.gapEndTs;
        }
        if (gapMiss !== undefined) {
          const prevMiss = current.gapMiss ?? 0;
          current.gapMiss = gapMiss > prevMiss ? gapMiss : prevMiss;
        }
      }
      return;
    }

    this.flushCurrent();
    this.current = {
      type,
      startLine: line,
      endLine: line,
      gapMs: type === EventType.Gap ? gapMs : undefined,
      gapMiss: type === EventType.Gap ? gapMiss : undefined,
      gapEndTs: type === EventType.Gap ? gapEndTs : undefined,
    };
  }

  finish(): EventRange[] {
    this.flushCurrent();
    return this.ranges;
  }

  private flushCurrent(): void {
    if (!this.current) return;
    this.ranges.push({
      ...this.ctx,
      type: this.current.type,
      startLine: this.current.startLine,
      endLine: this.current.endLine,
      gapMs: this.current.gapMs,
      gapMiss: this.current.gapMiss,
      gapEndTs: this.current.gapEndTs,
    });
    this.current = undefined;
  }
}

// Gap detection parameters tuned for high-frequency markets and to avoid false positives on sparse markets.
// Threshold is adaptive: assume gaps are exponentially distributed with mean avgGapMs.
// We log when the gap is larger than the expected maximum over a time window:
// avgGapMs * ln(windowMs / avgGapMs), clamped to at least avgGapMs.
// avgGapMs is time-weighted using the configured timeframe as the smoothing window.
const GAP_UPDATE_CAP_MULT = 8;
const GAP_MAX_SAMPLES = 1_000_000;

export interface GapTrackerState {
  lastTradeTs?: number;
  avgGapMs: number;
  samples: number;
  sameTsCount: number;
}

export interface GapRecord {
  gapMs: number;
  gapMiss: number;
}

export type GapSnapshot = Pick<CompanionMetadata, "gapAvgMs" | "gapSamples" | "lastTradeTs" | "gapSameTsCount">;

export function createGapTracker(snapshot?: GapSnapshot): GapTrackerState {
  return {
    lastTradeTs: snapshot?.lastTradeTs,
    avgGapMs: snapshot?.gapAvgMs ?? 0,
    samples: snapshot?.gapSamples ?? 0,
    sameTsCount: snapshot?.gapSameTsCount ?? 0,
  };
}

export function snapshotGapTracker(tracker: GapTrackerState): GapSnapshot {
  return {
    gapAvgMs: tracker.samples > 0 ? tracker.avgGapMs : undefined,
    gapSamples: tracker.samples > 0 ? tracker.samples : undefined,
    lastTradeTs: tracker.lastTradeTs,
    gapSameTsCount: tracker.sameTsCount > 0 ? tracker.sameTsCount : undefined,
  };
}

export function recordGap(tracker: GapTrackerState, ts: number, windowMs: number): GapRecord | undefined {
  const prevTs = tracker.lastTradeTs;
  tracker.lastTradeTs = ts;
  if (prevTs === undefined) return undefined;

  const span = ts - prevTs;
  if (span < 0) {
    tracker.sameTsCount = 0;
    return undefined;
  }
  if (span === 0) {
    tracker.sameTsCount += 1;
    return undefined;
  }

  const effectiveDelta = span / (tracker.sameTsCount + 1);
  tracker.sameTsCount = 0;

  const baseline = tracker.avgGapMs;
  const sampleCount = tracker.samples;
  let gapDetected = false;
  let gapMiss = 0;
  if (baseline > 0 && sampleCount > 1) {
    const baseWindow = Number.isFinite(windowMs) && windowMs > 0 ? windowMs : baseline;
    const window = Math.max(baseWindow, baseline * GAP_UPDATE_CAP_MULT * GAP_UPDATE_CAP_MULT);
    const expectedCount = window / baseline;
    let logN = Math.log(expectedCount);
    if (!Number.isFinite(logN) || logN < 1) logN = 1;
    const expectedMax = baseline * logN * logN;
    gapDetected = span > expectedMax;
    if (gapDetected) {
      const miss = Math.floor(span / baseline) - 1;
      gapMiss = miss > 0 ? miss : 0;
    }
  }

  const cappedDelta = baseline > 0 ? Math.min(effectiveDelta, baseline * GAP_UPDATE_CAP_MULT) : effectiveDelta;
  if (sampleCount === 0) {
    tracker.avgGapMs = cappedDelta;
    tracker.samples = 1;
    return gapDetected ? { gapMs: span, gapMiss } : undefined;
  }

  const window = Number.isFinite(windowMs) && windowMs > 0 ? windowMs : cappedDelta;
  const alpha = cappedDelta / (window + cappedDelta);
  tracker.avgGapMs += (cappedDelta - baseline) * alpha;
  if (tracker.samples < GAP_MAX_SAMPLES) {
    tracker.samples += 1;
  }

  return gapDetected ? { gapMs: span, gapMiss } : undefined;
}

import type { CompanionMetadata } from "./model.js";

export interface GapTrackerState {
  detectedGapCount: number;
  detectedGapAvgMs: number;
  startTs: number | undefined;
  samples: number;
  lastTradeTs: number | undefined;
  sameTsCount: number;

  // --- adaptive model in log-gap space ---
  emaFastLog: number;  // fast mean of log1p(gap)
  emaSlowLog: number;  // slow mean of log1p(gap)
  devFastLog: number;  // fast mean abs deviation in log space
  devSlowLog: number;  // slow mean abs deviation in log space

  // Optional: keep a human-friendly baseline for logs / metrics
  avgGapMs: number; // derived-ish (slow)
}

export interface Gap {
  gapMs: number;
  gapMiss: number;
  gapEndTs: number;
  gapScore?: number;
}

export function createGapTracker(companion?: CompanionMetadata): GapTrackerState {
  const gt = (companion?.gapTracker as Partial<GapTrackerState> | undefined) ?? {};

  return {
    lastTradeTs: gt?.lastTradeTs,
    avgGapMs: gt?.avgGapMs ?? 0,
    samples: gt?.samples ?? 0,
    sameTsCount: gt?.sameTsCount ?? 0,
    startTs: companion?.startTs,
    detectedGapAvgMs: gt?.detectedGapAvgMs ?? 0,
    detectedGapCount: gt?.detectedGapCount ?? 0,

    emaFastLog: gt?.emaFastLog ?? 0,
    emaSlowLog: gt?.emaSlowLog ?? 0,
    devFastLog: gt?.devFastLog ?? 0,
    devSlowLog: gt?.devSlowLog ?? 0,
  };
}

/**
 * Tuning notes:
 * - TAU_FAST_MS: reacts to local speed changes
 * - TAU_SLOW_MS: "normal" regime
 *
 * For ultra-hot markets, TAU_FAST helps track bursts; for dead markets, time-aware alpha prevents inertia.
 */
const TAU_FAST_MS = 2_000;                    // ~2s memory
const TAU_SLOW_MS = 120_000;                  // ~2min memory

// Warm-up and safety
const BUFFER_MS = 600_000;                    // ignore detection early (same as yours)
const MIN_SAMPLES_FOR_DETECT = 2_000;

// Robust thresholding in log space
const K_FAST = 3.0;
const K_SLOW = 6.0;
const HOT_REGIME_BIAS = 1.25;

const MIN_DEV_LOG = 0.03;
const EXTRA_FLOOR_SLOW_LOG = 0.08;            // keep only on slow side

const RATIO_MIN = 8;                          // gap must be >= 8x expected (tune 6..12)
const LOG_RATIO_MIN = Math.log(RATIO_MIN);
const MEDIUM_DISABLE_FAST_MS = 1_000;         // if avg gap > 1s => disable fast path
const ILLIQUID_DISABLE_ALL_MS = 30_000;       // if avg gap > 30s => near-disable detection

const MEDIUM_MIN_GAP_MS = 120_000;            // >= 2 minutes (tune 60_000..300_000)
const MEDIUM_RATIO = 30;                      // >= 30x expected slow gap

const ILLIQUID_MIN_GAP_MS = 6 * 60 * 60_000;  // >= 6 hours
const ILLIQUID_RATIO = 200;                   // >= 200x expected slow gap

const FAST_DEV_FLOOR_REL = 0.35;              // prevent fastDev collapse relative to slowDev
const Z_CLAMP = 50;                           // cap z to avoid insane scores                  // cap z to avoid insane scores

// For miss estimation: use expected gap from fast mean (more representative in hot markets)
const MIN_EXPECTED_MS = 1;      // avoid 0

function alphaFromDelta(deltaMs: number, tauMs: number): number {
  // alpha = 1 - exp(-dt/tau)
  // dt is the inter-trade gap, so the model is self-clocked and adapts fast to speed changes.
  // Clamp to keep numeric stability in weird inputs.
  if (deltaMs <= 0) return 0;
  const a = 1 - Math.exp(-deltaMs / tauMs);
  return a < 0 ? 0 : a > 1 ? 1 : a;
}

// Smooth scoring: ~1 at threshold, grows >1 as it becomes more extreme
function gapScoreFromLog(z: number): number {
  // z ~ (# of devs above mean). Turn it into a smooth score:
  // - below 0 => <1, above 0 => >1
  // - keep cheap math: exp is ok, called only per trade
  return Math.exp(z * 0.25); // gentler than exp(z)
}

export function recordGap(tracker: GapTrackerState, ts: number): Gap | undefined {
  const prevTs = tracker.lastTradeTs;
  const startTs = tracker.startTs;

  if (prevTs === undefined || startTs === undefined) {
    tracker.lastTradeTs = ts;
    tracker.startTs = ts;
    return;
  }

  const span = ts - prevTs;

  if (span <= 0) {
    if (span === 0) {
      tracker.sameTsCount++;
    } else {
      tracker.sameTsCount = 0;
    }
    return;
  }

  tracker.lastTradeTs = ts;
  tracker.sameTsCount = 0;

  // --- snapshot BEFORE update (for detection) ---
  const samples = tracker.samples;
  const slowMean = tracker.emaSlowLog;
  const slowDev = tracker.devSlowLog;
  const fastMean = tracker.emaFastLog;
  const fastDev = tracker.devFastLog;

  const elapsed = ts - startTs;
  const x = Math.log1p(span); // log-gap sample

  let gap: Gap | undefined;

  // Only detect once we have enough history; also keep your buffer behavior.
  if (samples >= MIN_SAMPLES_FOR_DETECT && elapsed > BUFFER_MS && slowMean > 0 && fastMean > 0) {
    const sDev0 = slowDev > MIN_DEV_LOG ? slowDev : MIN_DEV_LOG;
    const fDev0 = fastDev > MIN_DEV_LOG ? fastDev : MIN_DEV_LOG;

    const sDev = sDev0;
    const fDev = Math.max(fDev0, sDev0 * FAST_DEV_FLOOR_REL);

    const expectedMsFast = Math.expm1(fastMean);
    const expectedMsSlow = Math.expm1(slowMean);

    const expectedSlowMs =
      Number.isFinite(expectedMsSlow) && expectedMsSlow > 0 ? expectedMsSlow : MIN_EXPECTED_MS;

    const xDetect = Math.log1p(span);

    // ----- DEAD / VERY ILLIQUID: effectively disable detection -----
    if (expectedSlowMs > ILLIQUID_DISABLE_ALL_MS) {
      const insane = span >= Math.max(ILLIQUID_MIN_GAP_MS, expectedSlowMs * ILLIQUID_RATIO);

      if (insane) {
        const zSlow0 = (xDetect - slowMean) / sDev;
        const zSlow = !Number.isFinite(zSlow0) ? 0 : (zSlow0 > Z_CLAMP ? Z_CLAMP : zSlow0);

        const miss = Math.floor(span / Math.max(expectedSlowMs, MIN_EXPECTED_MS)) - 1;

        gap = {
          gapMs: span,
          gapMiss: miss > 0 ? miss : 0,
          gapEndTs: ts,
          gapScore: gapScoreFromLog(zSlow),
        };
      }
    }

    // ----- MEDIUM / LOW-VOL: disable fast path; require multi-minute gaps -----
    else if (expectedSlowMs > MEDIUM_DISABLE_FAST_MS) {
      // Require it to be clearly huge vs slow baseline (ratio) AND also not "small absolute"
      const ratioTrigger = span >= expectedSlowMs * MEDIUM_RATIO;
      const absTrigger = span >= MEDIUM_MIN_GAP_MS;

      // Strong slow statistical confirmation (a bit stricter than K_SLOW alone)
      const statTrigger = xDetect > (slowMean + (K_SLOW + 2.0) * sDev + EXTRA_FLOOR_SLOW_LOG);

      if ((ratioTrigger || absTrigger) && statTrigger) {
        const miss = Math.floor(span / Math.max(expectedSlowMs, MIN_EXPECTED_MS)) - 1;

        const zSlow0 = (xDetect - slowMean) / sDev;
        const zSlow = !Number.isFinite(zSlow0) ? 0 : (zSlow0 > Z_CLAMP ? Z_CLAMP : zSlow0);

        gap = {
          gapMs: span,
          gapMiss: miss > 0 ? miss : 0,
          gapEndTs: ts,
          gapScore: gapScoreFromLog(zSlow),
        };
      }
    }

    // ----- ACTIVE: keep your current high-vol behavior -----
    else {
      const zFast0 = (xDetect - fastMean) / fDev;
      const zFast = !Number.isFinite(zFast0) ? 0 : (zFast0 > Z_CLAMP ? Z_CLAMP : zFast0);

      const hotRegime = (slowMean - fastMean) > (HOT_REGIME_BIAS * (sDev + fDev));

      const fastTrigger =
        (zFast >= K_FAST) &&
        ((xDetect - fastMean) >= LOG_RATIO_MIN);

      const slowConfirm = xDetect > (slowMean + K_SLOW * sDev + EXTRA_FLOOR_SLOW_LOG);

      if (fastTrigger && (slowConfirm || hotRegime)) {
        const expectedMs =
          Number.isFinite(expectedMsFast) && expectedMsFast > 0
            ? expectedMsFast
            : expectedSlowMs;

        const miss = Math.floor(span / Math.max(expectedMs, MIN_EXPECTED_MS)) - 1;

        gap = {
          gapMs: span,
          gapMiss: miss > 0 ? miss : 0,
          gapEndTs: ts,
          gapScore: gapScoreFromLog(zFast),
        };
      }
    }
  }
  
  /*if (ts === 1694019918360 || ts === 1693657786340) {
    console.log(`
      --- GAP DEBUG ---
      ts: ${ts}
      prevTs: ${prevTs}
      span: ${span}
      x(log1p): ${x}
      slowMeanLog: ${slowMean}
      slowDevLog: ${slowDev}
      fastMeanLog: ${fastMean}
      fastDevLog: ${fastDev}
      samples: ${samples}
      gap: ${JSON.stringify(gap)}
      tracker: ${JSON.stringify(tracker)}
      `);
  }*/

  /*if (gap) {
    tracker.detectedGapCount++;
    tracker.detectedGapAvgMs += (gap.gapMs - tracker.detectedGapAvgMs) / tracker.detectedGapCount;

    console.log(`Detected gap: ${gap.gapMs}ms, score: ${gap.gapScore}, span: ${span}, x(log1p): ${x}, slowMeanLog: ${slowMean}, slowDevLog: ${slowDev}, fastMeanLog: ${fastMean}, fastDevLog: ${fastDev}`);
  }*/

  // --- update model AFTER detection ---
  const aFast = alphaFromDelta(span, TAU_FAST_MS);
  const aSlow = alphaFromDelta(span, TAU_SLOW_MS);

  if (samples === 0) {
    // initialize means to first observation
    tracker.emaFastLog = x;
    tracker.emaSlowLog = x;
    tracker.devFastLog = MIN_DEV_LOG;
    tracker.devSlowLog = MIN_DEV_LOG;
    tracker.samples = 1;
    tracker.avgGapMs = span;
    return gap;
  }

  // Update means
  const newFastMean = tracker.emaFastLog + (x - tracker.emaFastLog) * aFast;
  const newSlowMean = tracker.emaSlowLog + (x - tracker.emaSlowLog) * aSlow;

  // Update deviations (EMA of absolute deviation in log space)
  const fastAbsDev = Math.abs(x - newFastMean);
  const slowAbsDev = Math.abs(x - newSlowMean);

  tracker.devFastLog = tracker.devFastLog + (fastAbsDev - tracker.devFastLog) * aFast;
  tracker.devSlowLog = tracker.devSlowLog + (slowAbsDev - tracker.devSlowLog) * aSlow;

  tracker.emaFastLog = newFastMean;
  tracker.emaSlowLog = newSlowMean;

  // keep a human-scale baseline (slow mean)
  const baselineMs = Math.expm1(tracker.emaSlowLog);
  tracker.avgGapMs = Number.isFinite(baselineMs) && baselineMs > 0 ? baselineMs : tracker.avgGapMs;

  if (tracker.samples < 1_000_000) {
    tracker.samples++;
  }

  return gap;
}

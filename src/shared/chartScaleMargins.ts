export interface ScaleMargins {
  top: number;
  bottom: number;
}

export interface ScaleVisibility {
  price: boolean;
  liq: boolean;
  volume: boolean;
}

type ScaleId = "right" | "liq" | "volume";

const TOP_MARGIN = 0.04;
const GAP_PRICE_LIQ = 0.02;
const GAP_LIQ_VOL = 0.01;
const GAP_PRICE_VOL = GAP_PRICE_LIQ + GAP_LIQ_VOL;
const LIQ_BOTTOM_WHEN_LAST = 0.03;

const WEIGHT_BY_SCALE: Record<ScaleId, number> = {
  right: 70,
  liq: 7,
  volume: 16,
};

const DEFAULT_MARGINS: Record<ScaleId, ScaleMargins> = {
  right: { top: 0.04, bottom: 0.26 },
  liq: { top: 0.76, bottom: 0.17 },
  volume: { top: 0.84, bottom: 0 },
};

export function computeChartScaleMargins(
  visibility: ScaleVisibility,
): Record<ScaleId, ScaleMargins> {
  const active: ScaleId[] = [];
  if (visibility.price) active.push("right");
  if (visibility.liq) active.push("liq");
  if (visibility.volume) active.push("volume");

  const margins: Record<ScaleId, ScaleMargins> = {
    right: { ...DEFAULT_MARGINS.right },
    liq: { ...DEFAULT_MARGINS.liq },
    volume: { ...DEFAULT_MARGINS.volume },
  };
  if (active.length === 0) return margins;

  const last = active[active.length - 1];
  const bottomReserve = last === "liq" ? LIQ_BOTTOM_WHEN_LAST : 0;

  let gapTotal = 0;
  for (let i = 0; i < active.length - 1; i++) {
    gapTotal += gapBetween(active[i], active[i + 1]);
  }

  let totalWeight = 0;
  for (let i = 0; i < active.length; i++) {
    totalWeight += WEIGHT_BY_SCALE[active[i]];
  }

  const usable = Math.max(0, 1 - TOP_MARGIN - bottomReserve - gapTotal);
  const unit = totalWeight > 0 ? usable / totalWeight : 0;

  let cursor = TOP_MARGIN;
  for (let i = 0; i < active.length; i++) {
    const id = active[i];
    cursor += WEIGHT_BY_SCALE[id] * unit;
    margins[id] = {
      top: round6(cursor - WEIGHT_BY_SCALE[id] * unit),
      bottom: round6(1 - cursor),
    };
    if (i < active.length - 1) {
      cursor += gapBetween(id, active[i + 1]);
    }
  }

  return margins;
}

function gapBetween(a: ScaleId, b: ScaleId): number {
  if (a === "right" && b === "liq") return GAP_PRICE_LIQ;
  if (a === "liq" && b === "volume") return GAP_LIQ_VOL;
  if (a === "right" && b === "volume") return GAP_PRICE_VOL;
  return 0;
}

function round6(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}

export interface EventPaintStyle {
  fill: string;
  stroke: string;
}

export interface RectCorners {
  topLeft: number;
  topRight: number;
  bottomLeft: number;
  bottomRight: number;
}

export function eventPaintStyle(kind: string): EventPaintStyle {
  if (kind === "adapter_error") return { fill: "rgba(153,27,27,0.72)", stroke: "rgba(252,165,165,0.95)" };
  if (kind === "missing_adapter") return { fill: "rgba(88,28,135,0.64)", stroke: "rgba(216,180,254,0.94)" };
  if (kind === "parse_error") return { fill: "rgba(127,29,29,0.58)", stroke: "rgba(254,202,202,0.90)" };
  return { fill: "rgba(185,28,28,0.64)", stroke: "rgba(252,165,165,0.95)" };
}

export function roundedRectPath(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  w: number,
  h: number,
  radius: RectCorners,
): void {
  const max = Math.min(w / 2, h / 2);
  const tl = Math.min(radius.topLeft, max);
  const tr = Math.min(radius.topRight, max);
  const br = Math.min(radius.bottomRight, max);
  const bl = Math.min(radius.bottomLeft, max);

  ctx.beginPath();
  ctx.moveTo(x + tl, y);
  ctx.lineTo(x + w - tr, y);
  if (tr > 0) ctx.arcTo(x + w, y, x + w, y + tr, tr);
  else ctx.lineTo(x + w, y);
  ctx.lineTo(x + w, y + h - br);
  if (br > 0) ctx.arcTo(x + w, y + h, x + w - br, y + h, br);
  else ctx.lineTo(x + w, y + h);
  ctx.lineTo(x + bl, y + h);
  if (bl > 0) ctx.arcTo(x, y + h, x, y + h - bl, bl);
  else ctx.lineTo(x, y + h);
  ctx.lineTo(x, y + tl);
  if (tl > 0) ctx.arcTo(x, y, x + tl, y, tl);
  else ctx.lineTo(x, y);
  ctx.closePath();
}

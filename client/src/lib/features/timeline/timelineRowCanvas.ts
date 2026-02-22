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
  if (kind === "gap_fixed") return { fill: "rgba(46, 204, 113, 0.25)", stroke: "rgba(67, 238, 143, 0.25)" };
  if (kind === "gap") return { fill: "#d62828", stroke: "#ff5253" };
  return { fill: "#e65100", stroke: "#f57c00" };
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

import type { TradeSide } from "../types.js";

export interface SymbolState {
  lastPrice?: number;
  lastSide: TradeSide;
}

export function inferKrakenTickSide(price: number, state: SymbolState): TradeSide {
  let side = state.lastSide;
  if (state.lastPrice !== undefined) {
    if (price > state.lastPrice) {
      side = "buy";
    } else if (price < state.lastPrice) {
      side = "sell";
    }
  }
  state.lastPrice = price;
  state.lastSide = side;
  return side;
}

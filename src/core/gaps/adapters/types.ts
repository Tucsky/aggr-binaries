export type TradeSide = "buy" | "sell";

export interface GapWindow {
  eventId: number;
  fromTs: number;
  toTs: number;
}

export interface RecoveredTrade {
  ts: number;
  price: number;
  size: number;
  side: TradeSide;
  priceText: string;
  sizeText: string;
}

export interface AdapterRequest {
  exchange: string;
  symbol: string;
  windows: GapWindow[];
}

export interface TradeRecoveryAdapter {
  readonly name: string;
  recover(req: AdapterRequest): Promise<RecoveredTrade[]>;
}

export type FetchLike = (input: string | URL, init?: RequestInit) => Promise<Response>;

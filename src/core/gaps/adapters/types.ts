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

export type RecoveredBatchHandler = (batch: RecoveredTrade[]) => Promise<void> | void;

export interface AdapterRequest {
  exchange: string;
  symbol: string;
  windows: GapWindow[];
  onRecoveredBatch?: RecoveredBatchHandler;
}

export interface TradeRecoveryAdapter {
  readonly name: string;
  readonly apiOnly?: boolean;
  recover(req: AdapterRequest): Promise<RecoveredTrade[]>;
}

export type FetchLike = (input: string | URL, init?: RequestInit) => Promise<Response>;

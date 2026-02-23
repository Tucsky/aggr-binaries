export interface ViewerMarketKey {
  collector: string;
  exchange: string;
  symbol: string;
}

export function buildViewerMarketKey(
  input: Pick<ViewerMarketKey, "collector" | "exchange" | "symbol">,
): string {
  const collector = input.collector.trim().toUpperCase();
  const exchange = input.exchange.trim().toUpperCase();
  const symbol = input.symbol.trim();
  if (!collector || !exchange || !symbol) return "";
  return `${collector}:${exchange}:${symbol}`;
}

export function parseViewerMarketKey(value: string): ViewerMarketKey | null {
  const [rawCollector, rawExchange, ...rawSymbolParts] = value.split(":");
  if (!rawCollector || !rawExchange || rawSymbolParts.length === 0) return null;
  const collector = rawCollector.trim().toUpperCase();
  const exchange = rawExchange.trim().toUpperCase();
  const symbol = rawSymbolParts.join(":").trim();
  if (!collector || !exchange || !symbol) return null;
  return { collector, exchange, symbol };
}

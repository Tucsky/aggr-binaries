# Data rules and quirks

## Eras
- **Legacy (pre-2021)**: BTCUSD only, exchange in first column. Mostly uncompressed. Filenames date in Paris time (UTC+1/UTC+2 DST). Example line: `{exchange} {ts_ms} {price} {size} {side(1=buy,0=sell)} {liquidation?}`.
- **Logical (2021+)**: tree `{exchange}/{pair}/{YYYY-MM-DD-HH}` (UTC). Mostly `.gz`. No exchange column; pair is exchange-native symbol (slashes replaced with `-`).

## Timeline (key points)
- 2018-04-14 RAM starts (daily files), 2018-12-02 → 4h files (Paris TZ).
- 2019-03-01 PI starts; 2019-04-01 DST UTC+2; 2019-10-28 back to UTC+1.
- 2019-12-19 17:00 PI gap; resumes 2020-02-29 16:00.
- 2020-10-07 09:00 last legacy file for PI.
- 2021-05-24 12:40 PI switches to logical structure + .gz (4h UTC).
- 2021-07-01 RAM switches to logical (first days hourly UTC, then 4h).
- 2021-08-08 20:00 RAM settles on 4h (00/04/08/12/16/20).

## Exchange/pair remaps
### Legacy exchange column → exchange/symbol
```
bitfinex         -> BITFINEX / BTCUSD
binance          -> BINANCE / btcusdt
okex             -> OKEX / BTC-USDT
kraken           -> KRAKEN / XBT-USD
gdax             -> COINBASE / BTC-USD
poloniex         -> POLONIEX / BTC_USDT
huobi            -> HUOBI / btcusdt
bitstamp         -> BITSTAMP / btcusd
bitmex           -> BITMEX / XBTUSD
binance_futures  -> BINANCE_FUTURES / btcusdt
deribit          -> DERIBIT / BTC-PERPETUAL
ftx              -> FTX / BTC-PERP
bybit            -> BYBIT / BTCUSD
hitbtc           -> HITBTC / BTCUSD
```

### Logical remaps
- **POLONIEX** 2021-08-18-16 onward: `USDT_BTC` → `BTC_USDT` (apply to all pairs; prefer new base/quote order in outputs).
- **BITGET** 2025-11-28:  
  - Spot pairs add `-SPOT` suffix (`BTCUSDT` → `BTCUSDT-SPOT`, etc).  
  - Perp pairs drop suffix (`BTCUSDT_UMCBL` → `BTCUSDT`, `BTCUSD_DMCBL` → `BTCUSD`, `BTCPERP_CMCBL` → `BTCPERP`).

## Corrections
- BITFINEX liquidations (legacy): flip side.
- OKEX liquidations `1572940388059 <= ts < 1572964319495`: divide size by 500.
- Non-liquidations `1574193600000 <= ts <= 1575489600000`: randomize side per trade.
- Ignore 10% wicks: if next trade price moves ≥10% vs previous trade for the same stream, drop it.

## Data quality
- Timestamps may contain decimals (`1565827212058.1234`); use integer ms for candle bucket.
- Possible corruption: partial rows or concatenated rows without newline. Heuristic: timestamp length and last two columns are single-char fields; split/repair before parse.

## Output format
- Per candle (1m): 4×int32 OHLC (16B) + 2×int64 vBuy/vSell (16B) + 2×uint32 cBuy/cSell (8B) + 2×int64 lBuy/lSell (16B) ≈ 56B.
- Companion JSON example:
```
{
  "exchange": "BINANCE",
  "symbol": "BTCUSDT",
  "timeframe": "1m",
  "startTs": 1514764800000,
  "endTs": 1735603200000,
  "priceScale": 100,
  "volumeScale": 1000,
  "records": 10519200
}
```
- Gap handling: write zero-volume candles with O=H=L=C=prev close, or a sentinel with bitmask/flag; must keep full indexable timeline.

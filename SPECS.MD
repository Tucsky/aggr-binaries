## I. Context

Target: index ~12M tick-trade files (~3–4 TB) and convert them into **gap-aware 1m OHLC+ binaries**, one file per market per collector (RAM, PI). Performance, correctness, and resumability are mandatory.

- Two eras: **legacy** (pre-2021) and **logical** (post-2021).
- Two collectors: **RAM** (primary) and **PI** (secondary/backup).
- Processing must normalize symbols, fix known data issues, and be rerunnable without reprocessing old input.

---

## II. Collection timeline (key milestones)

- **2018-04-14** RAM starts legacy collection. Daily files like `BTCUSD_2018-11-29`. Rows: `{exchange} {ts_ms} {price} {size} {side(1=buy)} {liquidation?}`.
- **2018-12-02 10:37** RAM switches to 4h files (filenames in Paris time, UTC+1).
- **2019-03-01** PI starts legacy collection.
- **2019-04-01** DST to UTC+2 (filenames reflect Paris summer time).
- **2019-10-28** DST back to UTC+1.
- **2019-12-19 17:00** PI stops.
- **2020-02-29 16:00** PI resumes.
- **2020-10-07 09:00** Last PI legacy file.
- **2021-05-24 12:40** PI switches to logical structure (UTC filenames, gzip, 4h).
- **2021-07-01** RAM switches to logical structure (UTC filenames; briefly hourly, then 4h).
- **2021-08-08 20:00** RAM settles on 4h (00,04,08,12,16,20).

---

## III. Structure & normalization

### Legacy era (path)
- Path: `{collector}/{bucket}/BTCUSD_YYYY-...`
- Exchange is **not** in the path; inferred per row using a fixed map.
- `start_ts` is parsed from filename in **Europe/Paris** local time (DST aware), then stored as UTC ms.

### Logical era (path)
- Path: `{collector}/{bucket}/{exchange}/{symbol}/{YYYY-MM-DD[-HH][.gz]}`
- Exchange/symbol come from the path.
- `start_ts` parsed from filename as **UTC**.

### Symbol/exchange normalization
- Legacy row exchange map:
  `bitfinex→BITFINEX/BTCUSD`, `binance→BINANCE/btcusdt`, `okex→OKEX/BTC-USDT`, `kraken→KRAKEN/XBT-USD`, `gdax→COINBASE/BTC-USD`, `poloniex→POLONIEX/BTC_USDT`, `huobi→HUOBI/btcusdt`, `bitstamp→BITSTAMP/btcusd`, `bitmex→BITMEX/XBTUSD`, `binance_futures→BINANCE_FUTURES/btcusdt`, `deribit→DERIBIT/BTC-PERPETUAL`, `ftx→FTX/BTC-PERP`, `bybit→BYBIT/BTCUSD`, `hitbtc→HITBTC/BTCUSD`.
- Poloniex flip (2021-08-18 16:00): `USDT_BTC` → `BTC_USDT` (apply to all pairs; enforce quote-second form always).
- Bitget rename (2025-11-28): spot gains `-SPOT`; `_UMCBL/_DMCBL/_CMCBL` dropped from perps; suffix-less before the cut → spot (`-SPOT`), after the cut → derivative.

---

## IV. Data correction rules (per trade)

1) Bitfinex liquidations: flip side.  
2) OKEX liquidation bug window `1572940388059 ≤ ts < 1572964319495`: size ÷ 500.  
3) Bad non-liq sides window `1574193600000 ≤ ts ≤ 1575489600000`: deterministic random side.  
4) Corrupted/concatenated rows: parse defensively; drop if invalid.
5) (Wick filtering is noted historically but not currently applied in processing code.)

---

## V. Output format

Gap-aware 1m candles; every minute between the combined previous companion range and new data is represented.

Per-candle layout (~56 B):
```
OHLC:          4 × int32  = 16 B
vBuy/vSell:    2 × int64  = 16 B   // quote volume
cBuy/cSell:    2 × uint32 =  8 B   // trade counts
lBuy/lSell:    2 × int64  = 16 B   // liquidation quote volume
--------------------------------------
Total ≈ 56 B
```

Companion JSON example:
```json
{
  "exchange": "BINANCE",
  "symbol": "BTCUSDT",
  "timeframe": "1m",
  "startTs": 1514764800000,
  "endTs": 1735603200000,
  "priceScale": 10000,
  "volumeScale": 1000000,
  "records": 10519200,
  "lastInputStartTs": 1714608000000
}
```
`lastInputStartTs` is used to skip already-processed files on resume; trades older than `endTs` are ignored unless `--force`.

---

## VI. Indexing (Step 1)

Persisted in SQLite (`files` table keyed by `(root_id, relative_path)`), columns: collector, era, exchange, symbol, start_ts, size, mtime_ms, ext. Runs are append-only (`INSERT OR IGNORE`); `--verify` optionally flags mutated size/mtime. `--include` allows partial subtree walks. Legacy `start_ts` is parsed in Paris local time; logical in UTC.

---

## VII. Processing (Step 2)

- Load candidate files from SQLite ordered by collector, `start_ts`, path. Filters: collector/exchange/symbol. Logical files are pre-filtered by exchange/symbol; legacy files are always read (may contain multiple exchanges).
- Stream each file once (gzip if needed); parse by era; apply normalization + corrections; dispatch to per-market accumulators created on demand.
- Resume: if companion `lastInputStartTs` exists, skip files with `start_ts` older than that; skip trades earlier than companion `endTs` unless `--force`.
- Accumulators hold 1m buckets; outputs written to `output/{collector}/{exchange}/{symbol}.bin` + companion JSON. Binary writes are chunked to reduce syscall overhead; gap-aware across previous + current range.
- No DB “processed” flags; resume relies solely on companions.

---

## VIII. Controls & performance

- Filters (`--collector`, `--exchange`, `--symbol`) mainly for scoped/debug runs; production runs process all.
- Chunked binary writes (4096-candle blocks) to avoid millions of small writes.
- No per-trade DB I/O during processing; all routing is in-memory maps keyed by collector/exchange/symbol.
- TypeScript-driven CLI (`src/cli.ts`) with subcommands: `index`, `process`.

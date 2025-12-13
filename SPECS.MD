## I. Context

Target: index ~12M tick-trade files (~3–4 TB) and convert them into **gap-aware 1m OHLC+ binaries**, one file per market per collector (RAM, PI). Performance, correctness, and resumability are mandatory.

- Single logical layout for all collectors (there use to be two structures, but the legacy input has been converted away into logical which is the one we only know now and will reference throughout the repo).
- Two collectors: **RAM** (primary) and **PI** (secondary/backup).
- Processing must normalize symbols, fix known data issues, and be rerunnable without reprocessing old input.

---

## II. Collection timeline (key milestones)

Historical reference (⚠️ all legacy inputs have already been normalized into the logical layout):

- **2018-04-14** RAM starts legacy collection. Daily files like `BTCUSD_2018-11-29`. Rows: `{exchange} {ts_ms} {price} {size} {side(1=buy)} {liquidation?}`.
- **2018-12-02 10:37** RAM switches to 4h files (filenames in Paris time, UTC+1).
- **2019-03-01** PI starts legacy collection (filenames in Paris time; in sync with RAM).
- **2019-04-01** DST to UTC+2 (filenames reflect Paris summer time).
- **2019-10-28** DST back to UTC+1.
- **2019-12-19 17:00** PI stops mid-file.
- **2020-02-29 16:00** PI resumes; filenames appear to be UTC until **2020-03-29 05**.
- **2020-03-29 05 → 2020-10-07 09** PI filenames lag DST by +1h (UTC+1) while France is UTC+2.
- **2020-10-07 09:00** Last PI legacy file (PI shuts down before DST back).
- **2021-05-24 12:40** PI switches to logical structure (UTC filenames, gzip, 4h).
- **2021-07-01** RAM switches to logical structure (UTC filenames; briefly hourly, then 4h).
- **2021-08-08 20:00** RAM settles on 4h (00,04,08,12,16,20).
- **2021-08-08 - 2023** Starting to collect more markets (alt coins)
- **2025-06-03 13:49** Remove most of alt coins, focus on top 14 coins (~700-800 markets)
- **2025-12-06** Started working on aggr-binaries (this repo) to index/normalize/combine/compact the collected data
- **2025-12-09** Normalized legacy collection into logical structure, removed legacy aspect from this repo
---

## III. Structure & normalization

### Logical layout
- Path: `{collector}/{bucket}/{exchange}/{symbol}/{YYYY-MM-DD[-HH][.gz]}`
- Exchange/symbol come from the path; filenames are parsed as UTC.
- Content: `{ts_ms} {price} {size} {side(1=buy)} {liquidation?}` (1 trade = 1 line)
- Files may be plain text or gzip-compressed (probably 99.9% are gzip tho).

### Symbol/exchange normalization
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

Persisted in SQLite (`files` table keyed by `(root_id, relative_path)`), columns: collector, exchange, symbol, start_ts, ext. Runs are append-only (`INSERT OR IGNORE`); `--include` allows partial subtree walks. `start_ts` is parsed from the filename in UTC.

---

## VII. Processing (Step 2)

- Load candidate files from SQLite ordered by collector, `start_ts`, path. Filters: collector/exchange/symbol.
- Stream each file once (gzip if needed); parse rows, apply normalization + corrections; dispatch to per-market accumulators created on demand.
- Resume: if companion `lastInputStartTs` exists, skip files with `start_ts` older than that; skip trades earlier than companion `endTs` unless `--force`.
- Accumulators hold 1m buckets; outputs written to `output/{collector}/{exchange}/{symbol}.bin` + companion JSON. Binary writes are chunked to reduce syscall overhead; gap-aware across previous + current range.
- No DB “processed” flags; resume relies solely on companions.

---

## VIII. Controls & performance

- Filters (`--collector`, `--exchange`, `--symbol`) mainly for scoped/debug runs; production runs process all.
- Chunked binary writes (4096-candle blocks) to avoid millions of small writes.
- No per-trade DB I/O during processing; all routing is in-memory maps keyed by collector/exchange/symbol.
- TypeScript-driven CLI (`src/cli.ts`) with subcommands: `index`, `process`.

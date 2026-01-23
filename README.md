
# AGGR index & processor — specification

Tooling to index ~11–12M tick-trade files (~3–4 TB) and convert them into **gap-aware OHLC+ binaries** (default 1m). Everything is local: Node.js + SQLite, no server dependency for the core pipeline. Performance, correctness, and resumability are mandatory design constraints.

---

## I. Context & goals

**Target**  
- Index ~12M tick-trade files collected since 2018.
- Convert them into **gap-aware OHLC+ binaries**, one binary per `(collector, exchange, symbol)`.
- Output must be rerunnable, resumable, and deterministic.

**Key constraints**
- Single logical input layout for all collectors (legacy inputs already converted).
- Two collectors:
  - **RAM** (primary)
  - **PI** (secondary / backup)
- No per-trade database I/O during processing.
- Resume must rely on output state, not DB flags.

---

## II. Collection timeline (historical reference)

⚠️ All legacy inputs mentioned below have already been normalized into the logical layout. The timeline is kept verbatim for reasoning, validation, and audits.

- **2018-04-14** RAM starts legacy collection.  
  Daily files like `BTCUSD_2018-11-29`.  
  Rows: `{exchange} {ts_ms} {price} {size} {side(1=buy)} {liquidation?}`.
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
- **2021-08-08 – 2023** Start collecting many more markets (altcoins).
- **2025-06-03 13:49** Remove most altcoins, focus on top 14 coins (~700–800 markets).
- **2025-12-06** Start work on `aggr-binaries` (this repo).
- **2025-12-09** Legacy collections normalized into logical structure; legacy code paths removed.

---

## III. Logical input structure

### Path layout

```

{collector}/{bucket}/{exchange}/{symbol}/{YYYY-MM-DD[-HH][.gz]}

```

- Exchange and symbol are derived from the path.
- Filenames are parsed as **UTC**, regardless of historical origin.
- Files may be plain text or gzip (≈99.9% gzip).

### File content (1 trade per line)

```

{ts_ms} {price} {size} {side(1=buy)} {liquidation?}

```

---

## IV. Symbol & exchange normalization

Normalization is path-based and deterministic.

### Poloniex
- **2021-08-18 16:00**: `USDT_BTC` → `BTC_USDT`
- Enforce **quote-second form** for all pairs, globally.

### Bitget (2025-11-28 rename)
- Spot markets gain `-SPOT`.
- `_UMCBL`, `_DMCBL`, `_CMCBL` suffixes dropped from perps.
- Before cut:
  - Suffix-less → **spot** (`-SPOT`)
- After cut:
  - Suffix-less → **derivative**

---

## V. Per-trade data correction rules

Applied during streaming, before accumulation.

1. **Bitfinex liquidations**: flip side.
2. **OKEX liquidation bug**  
   Window: `1572940388059 ≤ ts < 1572964319495`  
   Action: `size /= 500`
3. **Bad non-liquidation sides**  
   Window: `1574193600000 ≤ ts ≤ 1575489600000`  
   Action: deterministic random side.
4. **Corrupted / concatenated rows**  
   Defensive parsing; drop invalid rows.
5. Wick filtering existed historically but is **not applied** in current processing.

---

## VI. Output format

### Candles

- Gap-aware **1m candles** by default (configurable).
- Every minute between previous companion range and new data is represented.

**Per-candle binary layout (~56 B)**

```
OHLC:          4 × int32  = 16 B
vBuy/vSell:    2 × int64  = 16 B   // quote volume
cBuy/cSell:    2 × uint32 =  8 B   // trade counts
lBuy/lSell:    2 × int64  = 16 B   // liquidation quote volume
--------------------------------------------------------------

Total ≈ 56 B
```

### Companion JSON

Example:

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

* `lastInputStartTs`: used to skip already-processed files.
* Trades `< endTs` are ignored on resume unless `--force`.

---

## VII. Indexing (step 1)

### SQLite schema

```sql
CREATE TABLE roots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE
);

CREATE TABLE files (
  root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
  relative_path TEXT NOT NULL,
  collector TEXT NOT NULL,
  exchange TEXT NOT NULL,
  symbol TEXT NOT NULL,
  start_ts INTEGER NOT NULL,
  ext TEXT,
  created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
  PRIMARY KEY (root_id, relative_path)
);

CREATE INDEX idx_files_exchange_symbol ON files(exchange, symbol);
CREATE INDEX idx_files_start_ts ON files(start_ts);
CREATE INDEX idx_files_collector ON files(collector);
```

### Behavior

* Walk input tree once.
* Parse filename → `start_ts` (UTC).
* Append-only via `INSERT OR IGNORE`.
* `--include` allows subtree-restricted walks.
* No mutation or “processed” flags.

---

## VIII. Processing (step 2)

### Core logic

* Load **markets** from SQLite ordered by `collector, exchange, symbol` (filters: `--collector`, `--exchange`, `--symbol`).
* For each market:

  * Read companion once (resume bounds, metadata).
  * Query that market’s files ordered by `start_ts, path`, applying resume guard in SQL (`start_ts >= lastInputStartTs` when present).
  * Stream each file **once** (gzip if needed), apply trade-level resume guard (`ts < endTs - timeframeMs`), and accumulate. The last candle is always rebuilt.
* Single in-memory accumulator per market; flush outputs before moving to the next market.
* Write:

  ```
  output/{collector}/{exchange}/{symbol}/{timeframe}.bin
  output/{collector}/{exchange}/{symbol}/{timeframe}.json
  ```

* On successful write, registry is upserted (collector/exchange/symbol/timeframe + startTs/endTs).

### Resume semantics

* If companion exists:

  * Skip files with `start_ts < lastInputStartTs`
  * Skip trades `< endTs - timeframeMs` (resumeSlot). Only the last candle is rebuilt; new candles are appended.
  * Binary resume: truncate the existing `.bin` to the resumeSlot offset and append dense candles onward. If the companion exists but the binary is missing, fall back to a full rebuild for that market.
* `--force` bypasses both guards and rebuilds from scratch.
* No database state is used for resume.

### Performance characteristics

* Low-memory directory walk.
* Batched SQLite writes during indexing.
* No per-trade DB I/O.
* Binary writes chunked in **4096-candle blocks** to avoid millions of syscalls.
* One accumulator at a time (per market) → predictable memory use even across many markets.

---

## IX. Configuration

Requires **Node ≥ 22**
(uses built-in `node:sqlite` and `--experimental-strip-types`).

Install:

```bash
npm install
```

Config file: `config.json`
(copy from `config.example.json`)

```json
{
  "root": "/Volumes/AGGR/input",
  "dbPath": "./index.sqlite",
  "batchSize": 1000,
  "includePaths": [],
  "outDir": "output",
  "timeframe": "1m"
}
```

All values can be overridden via CLI flags.

---

## X. CLI

Entry point:

```bash
npm start -- <subcommand> [flags]
```

### Subcommands

* `index` — build / append SQLite inventory.
* `process` — generate binaries.
* `registry` — rebuild registry table by scanning output companions.

### Shared flags

* `-r, --root <path>` input root
* `-d, --db <path>` SQLite path
* `--config <path>` config file
* `--no-config` ignore config file
* `--include <path>` repeatable subtree restriction

### Indexer flags

* `-b, --batch <n>` inserts per transaction (default 1000)

### Processor flags

* `--collector <name>` (RAM / PI)
* `--exchange <EXCHANGE>`
* `--symbol <SYMBOL>`
* `--timeframe <tf>` (1m, 5m, 1h…)
* `--force` ignore resume guards

### Examples

```bash
npm start -- index
npm start -- index --include "PI/2018-2019-2020"

npm start -- process --collector RAM --exchange BITMEX --symbol XBTUSD
npm start -- process
```

---

## XI. Preview module (frontend + websocket server)

### Structure

* Core pipeline: `src/core`
* WebSocket server: `src/server.ts` (split into `server/` helpers)
* Frontend: `client/` (Svelte + Vite + Tailwind)

### Commands

```bash
npm run dev:client
npm run build:client
npm run serve
```

* `serve` builds everything and starts the WS + static server from `dist/server.js`.

### WebSocket API (message-driven, single connection)

* `GET /` → serves built frontend
* `WS /ws` → no query params; all control uses JSON messages
* Per-connection state: collector, exchange, symbol, timeframe, startMs, companion, anchorIndex
* Client → server messages:
  * `setTarget {collector,exchange,symbol}` → loads companion + anchor and replies with `meta`
  * `setTimeframe {timeframe}` → reloads companion for same market, replies with `meta`
  * `setStart {startTs}` → recomputes anchor, replies with `meta`
  * `slice {fromIndex,toIndex}` → replies with `candles` slice
  * `listMarkets {}` → replies with `markets` (registry-backed)
  * `listTimeframes {collector,exchange,symbol}` → replies with `timeframes` (registry-backed)
* Server → client messages:
* `meta` (timeframe/timeframeMs, records, anchorIndex, startTs/endTs, priceScale/volumeScale)
  * `candles` (slice response)
  * `markets`, `timeframes`, `error`
* Works with dense (gap-filled) binaries; no reconnects required for target/timeframe/start changes.

### Preview resampling (on-demand, registry + binaries)

* When the client requests a timeframe, the server ensures the binary exists and is fresh before serving slices.
* Freshness: for each timeframe `tfMs`, `maxEnd = floor(rootEnd / tfMs) * tfMs` using the smallest timeframe (`root`) in the registry; a timeframe is fresh if its `endTs === maxEnd`.
* Source selection: among timeframes where `dst % src === 0`, pick the largest fresh candidate (fallback to root); skip missing binaries and purge their registry rows.
* Updates are append-only: compute missing `[from = dst.endTs, to = maxEnd]`; aggregate source candles into dst buckets and append to `dst.bin` and companion, then upsert registry.
* Gaps (zero OHLC) are ignored for price aggregation so resampled OHLCs are not flattened by holes.

### Preview UI (registry-backed controls)

* Single WS connection; no URL params.
* Collector select, market autocomplete (`EXCHANGE:SYMBOL` from registry), timeframe dropdown (user-managed list seeded by common TFs, highlights server-available TFs), start datetime.
* Registry-driven discovery populates controls; changing selections sends messages instead of reconnecting.

---

## XII. Summary

* Append-only indexing, resumable processing, gap-aware outputs.
* Outputs are timeframe-scoped (`collector/exchange/symbol/<tf>.bin/.json`) and registered in SQLite (upsert during processing; `npm start -- registry` to rescan companions).
* No legacy assumptions in code; historical complexity is captured here for correctness.
* Designed to safely process millions of files with minimal memory and syscall overhead.

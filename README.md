
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
* On resume, trades `< endTs - timeframeMs` are ignored (`endTs - timeframeMs` is rebuilt); `--force` disables this guard.

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

  * Read companion once (resume bounds, metadata) and set resume slot if present.
  * Query that market’s files ordered by `start_ts, path`, applying resume guard in SQL (`start_ts >= lastInputStartTs` when present).
  * Stream each file **once** (gzip if needed), apply trade-level resume guard (`ts < endTs - timeframeMs`), and accumulate. The last candle is always rebuilt.
  * Checkpoint outputs + registry during processing (every `--flush-interval` seconds, default 10s) and on completion.
* Single in-memory accumulator per market; buckets older than the latest slot are pruned after each flush to keep memory bounded.
* Write:

  ```
  output/{collector}/{exchange}/{symbol}/{timeframe}.bin
  output/{collector}/{exchange}/{symbol}/{timeframe}.json
  ```

* On successful write, registry is upserted (collector/exchange/symbol/timeframe + startTs/endTs).

### Resume semantics

* If companion exists:

  * Skip files with `start_ts < lastInputStartTs`.
  * Skip trades `< endTs - timeframeMs` (resumeSlot). The first flush after a resume truncates the binary to the resumeSlot and rewrites that candle before appending new ones.
  * Checkpoints update both companion and registry on every flush (interval + final) with the latest `endTs` and `lastInputStartTs` (most recent file that yielded trades).
  * If the companion exists but the binary is missing, fall back to a full rebuild for that market.
* `--force` bypasses both guards and rebuilds from scratch (fresh companion + binary).
* No database state is used for resume.

### Performance characteristics

* Low-memory directory walk.
* Batched SQLite writes during indexing.
* No per-trade DB I/O.
* Periodic checkpoint flushes keep outputs resumable mid-market and prune old buckets to cap memory.
* Binary writes chunked in **4096-candle blocks** to avoid millions of syscalls.
* One accumulator at a time (per market) → predictable memory use even across many markets.

---

## IX. Events & gap detection

Processing also logs **events** into SQLite for later inspection and dashboarding.

### What gets logged
* **Parse rejects** (per-line):
  * `parts_short` — malformed or truncated line
  * `non_finite` — NaN/Inf in numeric fields
  * `invalid_ts_range` — timestamp out of sane bounds
  * `notional_too_large` — price × size exceeds guardrail
* **Gaps** — significant time discontinuities in the trade stream

Events are grouped into **contiguous line ranges** per file for efficiency (no per-line logs).

### Schema (events table)
```
events(
  id INTEGER PRIMARY KEY,
  root_id INTEGER,
  relative_path TEXT,
  collector TEXT,
  exchange TEXT,
  symbol TEXT,
  event_type TEXT,
  start_line INTEGER,
  end_line INTEGER,
  gap_ms INTEGER,        -- max gap size within the grouped range (if gap)
  gap_miss INTEGER,      -- estimated missing trades for the max gap (if gap)
  gap_end_ts INTEGER,    -- timestamp of the trade after the max gap (if gap)
  gap_fix_status TEXT,   -- NULL | missing_adapter | adapter_error
  gap_fix_error TEXT,    -- last error message for fix attempt
  gap_fix_updated_at INTEGER,
  created_at INTEGER
)
```

Fix queue index (SQL-first filtering):
* `idx_events_fix_queue(event_type, gap_fix_status, collector, exchange, symbol, root_id, relative_path, id)`

### Gap detection (adaptive, single state)
The detector maintains a **single** adaptive average gap (`avgGapMs`) per market, updated in a time‑weighted way.
* **Liquidation rows are excluded** from gap tracking and fixgaps window extraction.
* **Same‑timestamp trades** are handled by spreading the next observed time span across them.
* **Threshold**: treat gaps as exponential tail events and log when:
  ```
  span_ms > avgGapMs * ln(window/avgGapMs)^2
  ```
  where `window = max(timeframeMs, avgGapMs * 64)` and `ln(·)` is clamped to ≥ 1.
* **gap_miss** is estimated as:
  ```
  floor(span_ms / avgGapMs) - 1
  ```
* Each detected gap is logged with `gap_end_ts` (the timestamp after the gap) so it can be validated later.

### Validation scripts
Located in `scripts/`:
* `verify_binance_gaps.py` — compare logged gaps to Binance Vision raw trades in the gap window.
* `compare_binance_day.py` — compare **per‑day** local trade counts vs Binance Vision raw trades.

---

## X. Configuration

Requires:
* **Node ≥ 22** (uses built-in `node:sqlite` and `--experimental-strip-types`)
* System `sort` utility (`/usr/bin/sort` on macOS, GNU/BSD sort accepted) for `fixgaps` file rewrites

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
  "flushIntervalSeconds": 10,
  "includePaths": [],
  "outDir": "output",
  "timeframe": "1m"
}
```

`flushIntervalSeconds` controls checkpoint cadence during `process`.

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
* `fixgaps` — recover missing trades from `events(event_type='gap')`, rewrite raw files, and patch binaries.

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
* `--flush-interval <s>` checkpoint every _s_ seconds (default 10)
* `--force` ignore resume guards

### Fixgaps flags

* `--collector <name>` limit queue to a collector
* `--exchange <EXCHANGE>` limit queue to an exchange
* `--symbol <SYMBOL>` limit queue to a symbol
* `--id <n>` process only one specific `events.id` gap row
* `--limit <n>` process at most _n_ gap events
* `--retry-status <csv>` include statused rows in addition to null status rows (example: `adapter_error,missing_adapter`)
* `--dry-run` run adapter recovery without mutating raw files, binaries, or events table

`--id` selection is applied directly on `events.id`; when present, it takes precedence over `--retry-status` queue gating.

### Fixgaps behavior

`fixgaps` is event-driven recovery over `events.event_type='gap'`.

For each grouped `(root_id, relative_path)` file:
1. Resolve one or more gap windows from event line ranges (with `gap_end_ts/gap_ms` fallback when needed).
2. Route by exchange adapter and fetch trades for those windows.
   * Adapters recover **trades only** in this iteration (no liquidation backfill via `fixgaps`).
3. Deterministically rewrite recovered files with a full timestamp sort-normalization pass:
   * Build a temporary sortable stream of existing trades + recovered trades.
   * Run external `sort` by timestamp and stable key.
   * Write sorted output atomically; existing rows win duplicates, recovered rows are inserted only when missing.
   * Preserve non-trade lines by appending them unchanged after sorted trade rows.
   * Recovered inserts are emitted as canonical trade rows (`ts price size side`) without liquidation marker.
4. Patch all existing timeframe binaries for that market over the affected timestamp range.
5. Update event lifecycle:
   * Adapter succeeded (including 0 recovered trades): delete event row(s).
   * Missing adapter: keep row, set `gap_fix_status='missing_adapter'`.
   * Adapter/merge/patch failure: keep row, set `gap_fix_status='adapter_error'` + `gap_fix_error`.

When `--dry-run` is enabled:
* `fixgaps` still scans gaps and calls adapters, and logs `recovered X / Y`.
* No raw file rewrites, no binary patches, and no `events` row updates/deletes are performed.

Supported adapters in this iteration:
* `BINANCE` / `BINANCE_FUTURES` → `data.binance.vision`
* `BYBIT` → `public.bybit.com/trading`
* `KRAKEN` → `api.kraken.com/0/public/Trades`
* `BITFINEX` → `api-pub.bitfinex.com/v2/trades/.../hist`
* `BITMEX` → `public.bitmex.com/data/trade` daily gzip dataset
* `OKEX` / `OKX` → direct daily trades only (`static.okx.com`, available from 2021-09-02)
* `KUCOIN` → direct spot daily trades (`historical-data.kucoin.com`)
* `HUOBI` / `HTX` → direct daily trades (`www.htx.com`) for spot + linear swap
* `COINBASE` → brokerage ticker + exchange trades pagination
* Adapter-level rate-limit handling includes host pacing/backoff. Bitfinex is clamped to conservative pacing (`4s` min interval, `14 req/min`). Kraken also retries `EGeneral:Too many requests` JSON responses.
* Internal exchange ids remain `OKEX` and `HUOBI`; `OKX` / `HTX` are supported as compatibility aliases for adapter routing.

### Fixgaps output

Definitive per-gap lines:
* Success: `[fixgaps] [EXCHANGE/SYMBOL/YYYY-MM-DD] {gap_ms}ms gap @ HH:mm : recovered {recovered} / {gap_miss}`
* Failure: `[fixgaps] [EXCHANGE/SYMBOL/YYYY-MM-DD] {gap_ms}ms gap @ HH:mm : error (reason)`

Transient status line (single-line TTY progress) shows fetch/wait/sort/patch phases while work is in flight.

### Fixgaps debug (optional)

Set env vars to inspect long-running retries and per-file progress:
* `AGGR_FIXGAPS_DEBUG=1` — high-level file/window + adapter + HTTP retry logs
* `AGGR_FIXGAPS_DEBUG_ADAPTERS=1` — adapter pagination logs only
* `AGGR_FIXGAPS_DEBUG_HTTP=1` — HTTP retry/backoff logs only
* `AGGR_FIXGAPS_PROGRESS=0` — disable transient one-line progress rendering

### Examples

```bash
npm start -- index
npm start -- index --include "PI/2018-2019-2020"

npm start -- process --collector RAM --exchange BITMEX --symbol XBTUSD
npm start -- process

npm start -- fixgaps --collector PI --exchange BITFINEX --symbol BTCUSD
npm start -- fixgaps --retry-status adapter_error
npm start -- fixgaps --retry-status adapter_error --limit 1
npm start -- fixgaps --dry-run --id 85
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
* Per-connection state: collector, exchange, symbol, timeframe, startMs, companion, anchorIndex, hasLiquidations
* Client → server messages:
  * `setTarget {collector,exchange,symbol}` → loads companion + anchor and replies with `meta`
  * `setTimeframe {timeframe}` → reloads companion for same market, replies with `meta`
  * `setStart {startTs}` → recomputes anchor, replies with `meta`
  * `slice {fromIndex,toIndex}` → replies with `candles` slice
  * `listMarkets {}` → replies with `markets` (registry-backed)
  * `listTimeframes {collector,exchange,symbol}` → replies with `timeframes` (registry-backed)
* Server → client messages:
* `meta` (timeframe/timeframeMs, records, anchorIndex, startTs/endTs, priceScale/volumeScale, hasLiquidations)
  * `candles` (slice response)
  * `markets`, `timeframes`, `error`
* Works with dense (gap-filled) binaries; no reconnects required for target/timeframe/start changes.
* `hasLiquidations` is persisted in companion metadata during processing/resampling and allows O(1) pane visibility decisions in preview (no runtime binary scan).

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

### Preview chart panes

* Built on `lightweight-charts` v5 with three panes (price / volume / liquidations).
* Volume pane overlays two histogram series on one scale:
  * Total volume: `buyVol + sellVol` (dimmed, sign-colored by buy-vs-sell dominance).
  * Volume delta overlay: `abs(buyVol - sellVol)` (brighter foreground bar, same sign-color logic).
* Liquidation pane uses centered signed histograms:
  * Long liquidations: `-liqSell`.
  * Short liquidations: `+liqBuy`.
* If `meta.hasLiquidations` is false, the liquidation pane is collapsed/hidden in the UI.

---

## XII. Summary

* Append-only indexing, gap-aware outputs, checkpointed/resumable processing driven solely by companions/binaries.
* Outputs are timeframe-scoped (`collector/exchange/symbol/<tf>.bin/.json`) and registered in SQLite (upsert during processing; `npm start -- registry` to rescan companions).
* Processing flushes at intervals (`--flush-interval`) to persist checkpoints mid-market, truncate/rewrite resume slots, and prune old buckets for bounded memory.
* No legacy assumptions in code; historical complexity is captured here for correctness.
* Designed to safely process millions of files with minimal memory and syscall overhead.

## XIII. Development

- `npm test` builds `dist-tests` via `tsconfig.tests.json` and runs `node --test dist-tests/tests/core/**/*.test.js`.

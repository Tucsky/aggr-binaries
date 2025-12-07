# AGGR index & processor

Tooling to index ~11M tick-trade files and turn them into gap-aware 1m OHLCV binaries. Everything is local: Node.js + SQLite (no server).

## Features

- **Index**: walk the input tree once, classify `legacy` vs `logical`, record collector/exchange/symbol/start_ts/ext into SQLite. Append-only: reruns insert only new paths (`INSERT OR IGNORE`). Legacy `start_ts` parsed as Europe/Paris (DST aware); logical parsed as UTC.
- **Normalize**: path-based exchange/symbol normalization (Poloniex quote-second rule; Bitget 2025-11-28 rename; legacy exchange map when reading file contents).
- **Process**: stream each input file once, apply corrections, route trades into per-market accumulators on demand, then write `output/{collector}/{exchange}/{symbol}.bin` + companion JSON (`lastInputStartTs` for resume). Resume by skipping files with `start_ts < lastInputStartTs` and trades `< endTs` unless `--force`. Optional sparse output (only populated candles) and configurable timeframe (e.g., 1m, 5m, 1h).
- **Filters**: optional `--collector/--exchange/--symbol` to narrow both file selection (logical only) and per-trade routing (legacy can still contain multiple exchanges).
- **Performance**: low-memory walk, batched SQLite writes, chunked binary writes (4096-candle blocks) to avoid millions of tiny syscalls, no per-trade DB I/O during processing.

## Installation

Requires Node ≥ 22 (uses built-in `node:sqlite` and `--experimental-strip-types`).

```bash
npm install
```

Copy and edit `indexer.config.example.json` → `indexer.config.json` (overridable via `--config`):

```json
{
  "root": "/Volumes/AGGR/input",
  "dbPath": "./index.sqlite",
  "batchSize": 1000,
  "includePaths": [],        // optional: restrict walk to subtrees
  "outDir": "output",
  "timeframe": "1m",
  "sparseOutput": false
}
```

## CLI (core)

Entry: `npm start -- <subcommand> [flags]`

Subcommands:

- `index` — build/append the SQLite inventory.
- `process` — read indexed files, generate binaries.

Shared flags (override config):

- `-r, --root <path>` input root
- `-d, --db <path>` SQLite path
- `--config <path>` config file (default `indexer.config.json` if present), `--no-config` to ignore
- `--include <path>` repeatable, restrict index walk to relative subtrees

Indexer flags:

- `-b, --batch <n>` inserts per transaction (default 1000)

Processor flags:

- `--collector <name>` (RAM/PI)
- `--exchange <EXCHANGE>`
- `--symbol <SYMBOL>`
- `--timeframe <tf>` (e.g., 1m, 5m, 1h; default 1m)
- `--sparse` write only populated candles (no gap fill; for testing)
- `--force` ignore resume guards (`lastInputStartTs` / `endTs`)

Examples:

```bash
# index everything from config
npm start -- index

# index only a subtree
npm start -- index --include "PI/2018-2019-2020"

# process one market
npm start -- process --collector RAM --exchange BITMEX --symbol XBTUSD

# process all collectors/markets (uses resume via companions)
npm start -- process
```

## Schema (SQLite)

```sql
CREATE TABLE roots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE
);

CREATE TABLE files (
  root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
  relative_path TEXT NOT NULL,
  collector TEXT NOT NULL,
  era TEXT NOT NULL,
  exchange TEXT,
  symbol TEXT,
  start_ts INTEGER,
  ext TEXT,
  created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
  PRIMARY KEY (root_id, relative_path)
);
CREATE INDEX idx_files_exchange_symbol ON files(exchange, symbol);
CREATE INDEX idx_files_start_ts ON files(start_ts);
CREATE INDEX idx_files_collector ON files(collector);
```

## Processing notes

- Legacy lines use a fixed exchange→(exchange,symbol) map; logical uses path exchange/symbol.
- Corrections applied per trade (Bitfinex liquidation side flip; OKEX liquidation size/500 window; randomized side window).
- Accumulators are created on demand per (collector, exchange, symbol); each input file is streamed once, dispatching trades to the right accumulator.
- Output companion JSON includes `lastInputStartTs` to skip already-processed files; trades older than `endTs` are ignored unless `--force`.
- Binary writer emits gap-aware 1m candles across the observed/previous range; writes are chunked to keep syscalls low.

## Preview module (frontend + websocket server)

Structure is split:
- Core (indexer/processor) lives under `src/core`.
- Preview websocket server under `src/server.ts`.
- Frontend lives in `client/` (Svelte + Vite + Tailwind).

Commands:
- `npm run dev:client` — run Vite dev server for the frontend.
- `npm run build:client` — build static assets into `client/dist`.
- `npm run serve` — build everything (core + client) and start the websocket/static server from `dist/server.js` (serves `client/dist`).

Preview WS endpoints:
- `GET /` serves the built frontend.
- `WS /ws?collector=...&exchange=...&symbol=...&start=ms` returns `meta` with timeframe/sparse/records/anchorIndex, and supports `slice` requests by candle index. Works with dense (gap-filled) and sparse binaries.

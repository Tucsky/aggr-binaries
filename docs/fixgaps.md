# Fixgaps task

## Purpose
Recover missing trades from gap events, rewrite affected raw files deterministically, and patch binaries.

## Command
```bash
npm start -- fixgaps [flags]
```

Key flags:
- `--collector <RAM|PI>`
- `--exchange <EXCHANGE>`
- `--symbol <SYMBOL>`
- `--id <event_id>`
- `--limit <n>`
- `--retry-status <csv>`
- `--dry-run`

Queue source:
- `events` rows where `event_type='gap'`
- `--id` takes precedence over retry-status queue selection

## Recovery batching
- Fixgaps does not skip events by gap duration.
- Recovered trades are merged in deterministic flush batches of `1,000,000` trades.
- Each flush batch is written into a raw file path derived from the batch's last trade timestamp using the same file naming pattern as the source event file.
- If a target file path does not exist, fixgaps creates it and indexes it before merge/patch.

## Pipeline
For each grouped `(root_id, relative_path)`:
1. Resolve one recovery window per event from persisted event payload timestamps (`gap_end_ts - gap_ms` to `gap_end_ts`).
2. Fetch trades from the exchange adapter.
3. Split recovered trades into deterministic flush batches (`1,000,000` max per batch).
4. For each flush batch, resolve target file from the batch last trade timestamp and deterministically rewrite by timestamp sort-normalization:
   - existing + recovered trades merged
   - recovered row multiplicity is preserved (no trade-level dedupe by key)
   - non-trade lines preserved
5. Patch base timeframe binary over the recovered range.
6. Roll up affected higher timeframes from patched base.
7. Update event lifecycle fields.

## Event status lifecycle
- `fixed`: adapter/rewrite/patch pipeline succeeded (including 0 recovered)
- `missing_adapter`: adapter unavailable
- `adapter_error`: adapter or patch pipeline failure

## Dry-run behavior
With `--dry-run`:
- adapters are called
- recovery counts are reported
- no file rewrites
- no binary patches
- no `events` status updates

## Adapter coverage
Current adapters include:
- BINANCE / BINANCE_FUTURES
- BYBIT
- KRAKEN (archive + API tail fallback)
- BITFINEX
- BITMEX
- OKEX / OKX
- KUCOIN
- HUOBI / HTX
- COINBASE

## Runtime notes
- Requires `sort` and `unzip` binaries in `PATH`.
- Progress renderer env vars:
  - `AGGR_FIXGAPS_PROGRESS`
  - `AGGR_FIXGAPS_DEBUG`
  - `AGGR_FIXGAPS_DEBUG_ADAPTERS`
  - `AGGR_FIXGAPS_DEBUG_HTTP`

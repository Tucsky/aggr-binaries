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

## Pipeline
For each grouped `(root_id, relative_path)`:
1. Resolve one or more gap windows from event line ranges (with gap timestamp fallback when needed).
2. Fetch trades from the exchange adapter.
3. Deterministically rewrite file by timestamp sort-normalization:
   - existing + recovered trades merged
   - existing rows win exact duplicates
   - recovered rows inserted only when missing
   - non-trade lines preserved
4. Patch base timeframe binary over the recovered range.
5. Roll up affected higher timeframes from patched base.
6. Update event lifecycle fields.

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

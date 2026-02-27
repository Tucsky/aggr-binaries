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
- Fixgaps always provides an adapter batch callback; adapters can emit recovered batches through it, and any returned tail array is ingested through the same accumulator path.

## Pipeline
For each grouped `(root_id, relative_path)`:
1. Resolve one recovery window per event from persisted event payload timestamps (`gap_end_ts - gap_ms` to `gap_end_ts`).
2. Call the exchange adapter with `onRecoveredBatch` callback support.
3. Ingest callback batches and returned adapter tail through the same accumulator.
4. Flush deterministic chunks (`1,000,000` max per chunk), resolve target file from the chunk last trade timestamp, then deterministically rewrite by timestamp sort-normalization:
   - existing + recovered trades merged
   - recovered row multiplicity is preserved (no trade-level dedupe by key)
   - non-trade lines preserved
5. Patch base timeframe binary over the recovered range.
6. Roll up affected higher timeframes from patched base.
7. Update event lifecycle fields.

### Mermaid flow
```mermaid
flowchart TD
  A[CLI fixgaps] --> B[Build queue from events gap rows]
  B --> C[Group rows by root_id + relative_path]
  C --> D[processFileGapBatch for each file group]

  D --> E[Resolve adapter for exchange]
  E -->|missing| E1[Mark rows missing_adapter]
  E -->|found| F[Extract windows from event payload gap_end_ts-gap_ms -> gap_end_ts]
  F --> G[Mark unresolved windows adapter_error]
  G --> H{Any resolvable windows?}
  H -->|no| HX[Return]
  H -->|yes| I[Call adapter.recover with onRecoveredBatch callback]

  I --> J[Accumulator ingests callback batches]
  J --> K[Adapter returns recovered tail array]
  K --> L[Accumulator ingests returned tail]
  L --> M{dry-run?}
  M -->|yes| N[Keep counters only]
  M -->|no| O[Flush chunks <= 1,000,000]
  O --> P[Resolve target file by last-trade timestamp]
  P --> Q[Ensure file exists + index file row]
  Q --> R[Merge recovered trades into raw file]
  R --> S[Patch base timeframe binary]
  S --> T[Merge dirty market ts range]

  N --> Z[Mark resolvable rows fixed with recovered counts]
  T --> Z
  Z --> AA[Return dirty range for market]

  D --> AB[Next file group]
  AB --> AC{Any dirty ranges?}
  AC -->|yes| AD[Roll up higher timeframes once per market]
  AC -->|no| AE[Skip rollup]
  AD --> AF[Print final stats]
  AE --> AF
```

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

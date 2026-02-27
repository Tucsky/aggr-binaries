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
- Fixgaps skips recovery for events where `gap_ms > 60d`; those events are marked `skipped_large_gap` with `recovered=0` and no adapter call.
- Recovered trades are merged in deterministic flush batches of `1,000,000` trades.
- Each flush batch is written into a raw file path derived from the batch's last trade timestamp using the same file naming pattern as the source event file.
- If a target file path does not exist, fixgaps creates it and indexes it before merge/patch.
- Fixgaps always provides an adapter batch callback; adapters can emit recovered batches through it, and any returned tail array is ingested through the same accumulator path.

## Pipeline
For each grouped `(root_id, relative_path)`:
1. Resolve one recovery window per event from persisted event payload timestamps (`gap_end_ts - gap_ms` to `gap_end_ts`), and skip windows with `gap_ms > 60d`.
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
  A["CLI fixgaps<br/>fn: runFixGaps"] --> B["Build queue from events gap rows<br/>fn: iterateGapFixEvents"]
  B --> C["Group rows by root_id + relative_path<br/>fn: runFixGaps grouping loop"]
  C --> D["processFileGapBatch for each file group<br/>fn: processFileGapBatch"]

  D --> E["Resolve adapter for exchange<br/>fn: adapterRegistry.getAdapter"]
  E -->|missing| E1[Mark rows missing_adapter]
  E -->|found| F["Extract windows from event payload gap_end_ts-gap_ms -> gap_end_ts<br/>fn: extractResolvableWindows"]
  F --> G["Mark unresolved windows adapter_error<br/>fn: markUnresolvedWindowEvents"]
  G --> H{Any resolvable windows?}
  H -->|no| HX[Return]
  H -->|yes| I["Call adapter.recover with onRecoveredBatch callback<br/>fn: recoverTradesForWindows / adapter.recover"]

  I --> J["Accumulator ingests callback batches<br/>fn: ingestStreamingRecoveredBatch"]
  J --> K[Adapter returns recovered tail array]
  K --> L["Accumulator ingests returned tail<br/>fn: finalizeStreamingRecoveredBatches"]
  L --> M{dry-run?}
  M -->|yes| N[Keep counters only]
  M -->|no| O["Flush chunks <= 1,000,000<br/>fn: mergeAndPatchRecoveredTradesFlushBatch"]
  O --> P["Resolve target file by last-trade timestamp<br/>fn: resolveFlushTargetFile"]
  P --> Q["Ensure file exists + index file row<br/>fn: ensureFlushTargetFile"]
  Q --> R["Merge recovered trades into raw file<br/>fn: mergeRecoveredTradesIntoFile"]
  R --> S["Patch base timeframe binary<br/>fn: patchBinariesForRecoveredTrades"]
  S --> T["Merge dirty market ts range<br/>fn: mergeDirtyRange / mergeDirtyMarketRange"]

  N --> Z["Mark resolvable rows fixed with recovered counts<br/>fn: markResolvedWindowEvents"]
  T --> Z
  Z --> AA[Return dirty range for market]

  D --> AB[Next file group]
  AB --> AC{Any dirty ranges?}
  AC -->|yes| AD["Roll up higher timeframes once per market<br/>fn: rollupHigherTimeframesFromBase"]
  AC -->|no| AE[Skip rollup]
  AD --> AF[Print final stats]
  AE --> AF
```

## Event status lifecycle
- `fixed`: adapter/rewrite/patch pipeline succeeded (including 0 recovered)
- `skipped_large_gap`: event was intentionally skipped by the `>60d` long-gap guard
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

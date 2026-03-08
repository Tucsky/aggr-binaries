# Fixgaps task

## Purpose
Recover missing trades from persisted gap rows, rewrite affected raw files deterministically, and patch binaries.

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
- `gaps` rows
- `--id` takes precedence over retry-status queue selection
- Default traversal is market-first: `collector/exchange/symbol`, then deterministic file order (`end_relative_path/id`).
- Queue reads are keyset-paged (`1024` rows/page) so fixgaps can keep writing gap statuses without holding a long-lived read cursor.

## Recovery batching
- Fixgaps skips recovery for rows where `gap_ms > 60d`; those rows are marked `skipped_large_gap` with `recovered=0` and no adapter call.
- Recovered trades are merged in deterministic flush batches of `1,000,000` trades.
- Each flush batch target path is resolved from persisted gap boundary fields and chunk timestamps:
  - default: write to `end_relative_path`
  - if `firstTs <= start_ts + 1 day`: write to `start_relative_path`
  - if `firstTs >= start_ts + 1 day` and `lastTs <= end_ts - 1 day`: write to an intermediate file inferred from boundary filename format (`YYYY-MM-DD-HH(.gz)` => 4h slots, otherwise daily).
- If a target file path does not exist, fixgaps creates it and indexes it before merge/patch.
- Fixgaps always provides an adapter batch callback; adapters can emit recovered batches through it, and any returned tail array is ingested through the same accumulator path.

## Pipeline
For each grouped `(collector, exchange, symbol, end_relative_path)`:
1. Resolve one recovery window per gap row from persisted payload timestamps (`start_ts` to `end_ts`), and skip windows with `gap_ms > 60d`.
2. Call the exchange adapter with `onRecoveredBatch` callback support.
3. Ingest callback batches and returned adapter tail through the same accumulator.
4. Flush deterministic chunks (`1,000,000` max per chunk), resolve target file with the start/end boundary rules above, then deterministically rewrite by timestamp sort-normalization:
   - existing + recovered trades merged
   - recovered row multiplicity is preserved (no trade-level dedupe by key)
   - non-trade lines preserved
5. Patch base timeframe binary over the recovered range.
6. Roll up affected higher timeframes from patched base.
7. Update gap lifecycle fields.

### Mermaid flow
```mermaid
flowchart TD
  A["CLI fixgaps<br/>fn: runFixGaps"] --> B["Build keyset-paged queue from gaps rows (market-first order)<br/>fn: iterateGapFixEvents"]
  B --> C["Group rows by market + end_relative_path<br/>fn: runFixGaps grouping loop"]
  C --> D["processFileGapBatch for each file group<br/>fn: processFileGapBatch"]

  D --> E["Resolve adapter for exchange<br/>fn: adapterRegistry.getAdapter"]
  E -->|missing| E1[Mark rows missing_adapter]
  E -->|found| F["Extract windows from row payload start_ts -> end_ts<br/>fn: extractResolvableWindows"]
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
  O --> P["Resolve target file using chunk bounds + gap boundary fields<br/>fn: resolveFlushTargetFile"]
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

## Gap status lifecycle
- `fixed`: adapter/rewrite/patch pipeline succeeded (including 0 recovered)
- `skipped_large_gap`: row was intentionally skipped by the `>60d` long-gap guard
- `missing_adapter`: adapter unavailable
- `adapter_error`: adapter or patch pipeline failure

## Timeline events viewport contract
Timeline page event rendering (`client/src/pages/TimelinePage.svelte` + `client/src/lib/features/timeline/*timelineEventViewport*`) treats timeline events as `gaps` rows.

Server query contract:
- `GET /api/timeline/events`: range + optional collector/exchange/symbol filters.
- `POST /api/timeline/events/query`: range + explicit row tuples for viewport fetches.
- Row tuple cap is `200` and request body cap is `256 KiB`.
- Returned order is deterministic: `collector, exchange, symbol, ts, id`.

Viewport loading/caching invariants:
- Event row selection uses the current virtual viewport window directly (`startIndex..endIndex`) with no extra row overscan and no fixed client-side row cap.
- Row window math is shared between rendering and event fetch to avoid drift.
- Requested time range is the visible viewport clamped to selected bounds, then expanded by `50%` overscan.
- Scope identity is timeframe + selected range + collector/exchange/symbol filters; scope changes clear loaded coverage.
- Cache is row-partitioned and segmented (`<=3` segments per row, `<=20,000` events per segment, global caps `128` rows / `60,000` events).
- Cache eviction is deterministic: farthest from active range first, then least-recently-accessed.
- Cache merge order is deterministic by `(ts, id)`; duplicate event IDs are collapsed to one entry and prefer the most recently fetched payload.

UI status mapping (`eventKind`) from gap lifecycle:
- `fixed` -> `gap_fixed`
- `skipped_large_gap` -> `skipped_large_gap`
- `adapter_error` -> `adapter_error`
- `missing_adapter` -> `missing_adapter`
- any other value (including `null`) -> `gap`

### Mermaid flow (`timelineEventViewport` load cycle)
```mermaid
flowchart TD
  A["Trigger: scroll/pan/zoom/filter/timeframe/resize -> scheduleEventsReload(delay)"] --> B["loadEvents(forceReload?)"]
  B --> C{"selectedRange + viewRange + filteredMarkets available?"}
  C -->|no| C1["Abort in-flight + clear loaded coverage"] --> Z
  C -->|yes| D["scopeKey = buildTimelineEventsScopeKey(timeframe, selectedRange, filters)"]
  D --> E{"scope changed?"}
  E -->|yes| E1["resetLoadedEventsState(clearCache=true, clearScope=false)"]
  E -->|no| F
  E1 --> F["selection = selectTimelineViewportEventRows(startIndex..endIndex, overscan=0, maxRows=visibleRowCount)"]
  F --> G{"selection empty?"}
  G -->|yes| G1["Clear loaded coverage and stop"] --> Z
  G -->|no| H["request = resolveTimelineViewportEventRequest(clamped view + 50% range overscan)"]
  H --> I{"request is null (already covered)?"}
  I -->|yes| Z
  I -->|no| J{"loadingEvents in progress?"}
  J -->|yes and same query| Z
  J -->|yes and different query| J1["queuePendingEventsReload(force?)"] --> Z
  J -->|no| K["cached = readTimelineViewportEventCache(scopeKey, requestRange, selection)"]
  K --> L{"all selected rows covered by cache and not forceReload?"}
  L -->|yes| L1["Serve cached events and mark coverage"] --> Z
  L -->|no| M["rowsToFetch = force ? selection : missingRows(selection, coveredRows)"]
  M --> N{"rowsToFetch empty?"}
  N -->|yes| N1["Serve cached events and mark coverage"] --> Z
  N -->|no| O["Optional immediate paint from cached subset"]
  O --> P["POST /api/timeline/events/query(requestRange, rowsToFetch)"]
  P --> Q["writeTimelineViewportEventCache(scopeKey, requestRange, rowKeys, fetchedEvents)"]
  Q --> R["refreshed = read cache for full selection/requestRange"]
  R --> S["Set allEvents + loadedRange + loadedRowKeys + lastQueryKey"]
  S --> T["loadingEvents=false; flushPendingEventsReload()"]
  T --> Z["groupEventsByMarket -> TimelineRow render"]
```

### Mermaid flow (`timelineEventViewport` query + cache internals)
```mermaid
flowchart TD
  A["POST /api/timeline/events/query"] --> B["Parse JSON body (<=256 KiB), validate startTs/endTs"]
  B --> C["Normalize row tuples (collector, exchange, symbol)"]
  C --> D{"rows count 1..200?"}
  D -->|no| D1["400 invalid timeline query"]
  D -->|yes| E["listTimelineEvents SQL with row filter CTE/join"]
  E --> F["Deterministic order: collector, exchange, symbol, ts, id"]
  F --> G["events[] response"]

  G --> H["writeTimelineViewportEventCache"]
  H --> I["Group fetched events by rowKey"]
  I --> J["Upsert requestRange segment per row"]
  J --> K{"Range overlaps/touches existing segment?"}
  K -->|yes and merged <=20k events| L["Merge by (ts,id) and dedupe event id"]
  K -->|no| M["Keep separate segment"]
  L --> N
  M --> N["Enforce <=3 segments/row (evict farthest, then LRU tie-break)"]
  N --> O["Global prune: max 128 rows and 60,000 events"]
  O --> P["readTimelineViewportEventCache finds covering segment and ts window via binary search"]
```

## Dry-run behavior
With `--dry-run`:
- adapters are called
- recovery counts are reported
- no file rewrites
- no binary patches
- no `gaps` status updates

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

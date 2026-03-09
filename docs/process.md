# Process task

## Purpose
Stream indexed trade files and produce gap-aware candle binaries + companions.

## Command
```bash
npm start -- process [flags]
```

Key flags:
- `--collector <RAM|PI>`
- `--exchange <EXCHANGE>`
- `--symbol <SYMBOL>`
- `--timeframe <tf>`
- `--flush-interval <seconds>` checkpoint cadence
- `--force` full rebuild (disable resume guards)

## Input assumptions
- Files are discovered from `files` table, ordered by `start_ts, relative_path`.
- Per-trade line format:
  ```text
  {ts_ms} {price} {size} {side(1=buy)} {liquidation?}
  ```

## Per-trade correction rules
Applied during streaming before accumulation:
- Corrupted rows: rejected by parser and summarized in warning logs.
- Non-increasing trade timestamps: ignored by gap tracking state (no tracker rewind, no synthetic gap row).
- Out-of-order trades within a candle: OHLC uses per-bucket timestamp anchors so late backfill rows update wick/high-low and volume, but not chronological close unless they are the latest timestamp in that bucket.

## Output files
For each `(collector, exchange, symbol, timeframe)`:
- `{outDir}/{collector}/{exchange}/{symbol}/{timeframe}.bin`
- `{outDir}/{collector}/{exchange}/{symbol}/{timeframe}.json`

Registry is upserted from flushed companion range.

## Binary format
Per candle (`56` bytes):
```text
OHLC:          4 x int32  = 16 B
vBuy/vSell:    2 x int64  = 16 B   (quote volume)
cBuy/cSell:    2 x uint32 =  8 B   (trade counts)
lBuy/lSell:    2 x int64  = 16 B   (liquidation quote volume)
----------------------------------------------
Total                      56 B
```

## Companion contract
Companion JSON includes at least:
- market identity (`exchange`, `symbol`)
- timeframe info (`timeframe`, `timeframeMs`)
- range (`startTs`, `endTs`)
- scales (`priceScale`, `volumeScale`)
- record count (`records`)
- resume anchor (`lastInputStartTs`)
- adaptive gap tracker snapshot fields

## Viewer websocket contract (`/ws`)
The chart viewer transport (`client/src/lib/features/viewer/viewerWs.ts`) reads process outputs through the preview websocket endpoint.

Client -> server messages:
- `setTarget` (`collector`, `exchange`, `symbol`, optional `timeframe`, optional `startTs`)
- `setTimeframe` (`timeframe`)
- `setStart` (`startTs` or `null`)
- `slice` (`fromIndex`, `toIndex`)
- `listMarkets`
- `listTimeframes` (`collector`, `exchange`, `symbol`)

Server -> client messages:
- `meta` (`startTs`, `endTs`, `timeframe`, `timeframeMs`, `priceScale`, `volumeScale`, `records`, `anchorIndex`)
- `candles` (`fromIndex`, `toIndex`, `candles[]`)
- `markets`
- `timeframes`
- `error`

Deterministic semantics:
- `setTarget` reloads companion state for one `(collector, exchange, symbol, timeframe)` market and emits `meta`.
- `setStart` updates `anchorIndex = clamp(floor((startTs - companion.startTs) / timeframeMs), 0..records-1)`; `null` start anchors to latest.
- `slice` reads fixed-width candle rows (`56` bytes each) from `{outDir}/{collector}/{exchange}/{symbol}/{timeframe}.bin`; requested indices are clamped to `[0, records-1]`.
- The client queues outbound messages while the socket is not open, suppresses duplicate in-flight or last-requested slices, and clears slice state whenever `meta` changes.

### Mermaid flow (`viewerWs`)
```mermaid
flowchart TD
  subgraph Client["Client (`viewerWs.ts` + controls/chart)"]
    A["ViewerControls onMount -> connect()"]
    B{"Socket already OPEN/CONNECTING?"}
    C["Open WebSocket(buildViewerWsUrl())"]
    D["onopen: status=connected, flushQueue(), requestMarkets()"]
    E["setTarget/setTimeframe/setStart update local state and send message"]
    F["requestSlice(from,to) guards: meta set, clamped range, not pending, not lastRequested"]
    G["sendMessage(payload) or queue until OPEN"]
    H["onmessage dispatcher"]
    I["meta -> handleMeta (dedupe unchanged, resetSlices(false), set meta store)"]
    J["candles -> clear pending key, notify subscribers"]
    K["markets/timeframes -> update stores (target key guarded)"]
    L["CandleChart on meta: reset and request initial slice around anchorIndex"]
    M["CandleChart near range edge: request additional slices"]
  end

  subgraph Server["Server (`previewWs.ts`, `/ws`)"]
    S0["Upgrade HTTP -> websocket and decode frames"]
    S1{"Incoming type"}
    S2["setTarget/setTimeframe -> refreshCompanion"]
    S3["refreshCompanion: loadCompanion + computeAnchorIndex"]
    S4["setStart -> recompute anchorIndex only"]
    S5["slice -> readCandles(bin, 56B records, clamped indices)"]
    S6["listMarkets -> registryApi.listMarkets"]
    S7["listTimeframes -> registryApi.listTimeframes"]
    S8["send meta/candles/markets/timeframes or error"]
  end

  A --> B
  B -->|yes| E
  B -->|no| C --> D
  D --> G
  E --> G
  F --> G
  H --> I --> L --> F
  H --> J
  H --> K
  M --> F

  G --> S0 --> S1
  S1 -->|setTarget or setTimeframe| S2 --> S3 --> S8
  S1 -->|setStart| S4 --> S8
  S1 -->|slice| S5 --> S8
  S1 -->|listMarkets| S6 --> S8
  S1 -->|listTimeframes| S7 --> S8
  S1 -->|unknown/invalid| S8
  S8 --> H
```

## Resume semantics
Without `--force`, when companion exists:
- Skip files with `start_ts < lastInputStartTs`.
- Skip trades `< endTs - timeframeMs` (resume slot).
- First resume flush truncates binary to resume slot, rewrites that slot, then appends.
- Checkpoint flushes update companion + registry.
- Companion exists but binary missing => full rebuild for that market.

With `--force`:
- Ignore resume guards and rebuild from scratch.

## Gap persistence and detection
Process writes gap rows into `gaps` (one row per detected gap):
- `gap_ms`, `gap_miss`, `gap_score`
- `start_ts`, `end_ts`
- `start_relative_path`, `end_relative_path`
- parse rejects are not persisted; they are summarized in logs (`[parse-skip] ...`)

Gap detection is adaptive per market:
- liquidation rows excluded from gap tracking
- same-timestamp handling avoids false gap inflation
- thresholds use adaptive average gap and timeframe window

## Performance model
- One market accumulator at a time.
- Old buckets pruned after flushes.
- Chunked binary writes.
- No per-trade DB writes.
- Periodic checkpoint flushes keep runs resumable.

## Mermaid flow
```mermaid
flowchart TD
  A["CLI process<br/>fn: runProcess"] --> B["Resolve collectors from flags or files table<br/>fn: resolveCollectors"]
  B --> C["Count candidate files and markets for scope<br/>fn: countCandidateFiles / countMarkets"]
  C --> D{Any candidate files?}
  D -->|no| E[Log no files and exit]
  D -->|yes| F["Iterate markets collector exchange symbol sorted<br/>fn: iterateMarkets"]

  F --> G["Initialize market accumulator<br/>fn: startAccumulatorForMarket"]
  G --> H["Read companion unless force and init gap tracker<br/>fn: readCompanion / createGapTracker"]
  H --> I{Companion present but binary missing?}
  I -->|yes| J[Disable resume and rebuild full market]
  I -->|no| K[Compute minStartTs and resume slot]
  J --> K
  K --> L["Iterate market files ordered by start_ts and relative_path<br/>fn: iterateFilesForMarket"]

  L --> M["Open file stream plain or gzip<br/>fn: streamFile / makeStream"]
  M --> N{Stream open failed ENOENT?}
  N -->|yes| O[Fail fast stale-index error for market file]
  N -->|no| P["Read lines and parse trade rows<br/>fn: parseTradeLine"]
  P --> Q{Trade parsed?}
  Q -->|no| R[Increment parse-reject counters]
  Q -->|yes| S["Run adaptive gap detection on non-liquidation rows<br/>fn: recordGap"]
  S --> T{Trade before resume slot?}
  T -->|yes| P
  T -->|no| U["Accumulate candle bucket<br/>fn: accumulate"]
  U --> P
  R --> P
  P --> V["Persist file gaps in DB (replace by gap end file key)<br/>fn: db.deleteGapsForEndFile / db.insertGaps"]
  V --> W[Log parse-reject summary if non-zero]
  W --> X[Update in-memory stats]
  X --> Y{Flush interval elapsed?}
  Y -->|no| L
  Y -->|yes| Z["flushMarketOutput checkpoint<br/>fn: flushMarketOutput"]

  Z --> AA{Closed range available to flush?}
  AA -->|no| L
  AA -->|yes| AB["Write or rewrite binary range in chunks<br/>fn: writeBinaryRange"]
  AB --> AC["Write companion json and upsert registry row<br/>fn: writeCompanion / db.upsertRegistry"]
  AC --> AD["Prune old buckets and advance flush state<br/>fn: pruneBucketsBefore"]
  AD --> L

  L --> AE[Final flush for market]
  AE --> AF[Next market]
  AF --> AG{More markets?}
  AG -->|yes| F
  AG -->|no| AH["Log final totals and clear progress<br/>fn: runProcess return"]
```

## Failure handling
- SQLite write contention (`SQLITE_BUSY`/`SQLITE_LOCKED`) retries with bounded backoff.
- If an indexed `files` row points to a missing input path on disk, process fails fast with an explicit stale-index error for that market/file.
- On that missing-input failure, no further files are processed and existing gap rows for the failing file are left unchanged.
- If a gzip input is corrupt (`Z_DATA_ERROR`/`Z_BUF_ERROR`), process logs the file context, skips that file, and continues the run; final totals include `skipped=<count>`.
- If binary/companion persisted but registry missed update (interruption), run `registry` for affected scope.

# Registry task

## Purpose
Rebuild registry rows from output companion files.

Registry is derived state; use this command to reconcile DB with existing binaries/companions.

## Command
```bash
npm start -- registry [flags]
```

Key flags:
- `--collector <RAM|PI>`
- `--exchange <EXCHANGE>`
- `--symbol <SYMBOL>`
- `--timeframe <tf>`

## Behavior
- Scan companions under `{outDir}/{collector}/{exchange}/{symbol}`.
- Build registry entries from companion time ranges.
- Replace matching DB scope atomically.

## Mermaid flow
```mermaid
flowchart TD
  A["CLI registry<br/>fn: runRegistry"] --> B[Open DB and build optional filters]
  B --> C[Validate outDir exists and is a directory]
  C --> D["Walk collector exchange symbol companion json files<br/>fn: walkCompanions"]
  D --> E[Apply collector exchange symbol timeframe filters]
  E --> F["Read companion JSON<br/>fn: readCompanion"]
  F --> G{Companion parse succeeds?}
  G -->|no| H[Fail command with parse error]
  G -->|yes| I{legacy sparse companion?}
  I -->|yes| J[Warn and skip]
  I -->|no| K["Normalize range startTs endTs from monolithic or segmented fields<br/>fn: normalizeCompanionRange"]
  K --> L[Append registry entry collector exchange symbol timeframe range]
  J --> M{scanned mod 1000 == 0?}
  L --> M
  M -->|yes| N[Emit scan progress]
  M -->|no| D
  N --> D
  D --> O{scan complete?}
  O -->|no| D
  O -->|yes| P["replaceRegistry transaction<br/>fn: db.replaceRegistry"]
  P --> Q[Delete registry rows in selected scope]
  Q --> R[Upsert scanned entries]
  R --> S[Return scanned upserted deleted and clear progress]
```

## When to run
- After process interruption where companion/binary likely persisted but DB upsert failed.
- After manual output restoration/copy.
- As periodic DB consistency repair.

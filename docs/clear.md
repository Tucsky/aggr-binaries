# Clear task

## Purpose
Delete one market's output + DB state, then reindex only that market subtree.

## Command
```bash
npm start -- clear --collector <RAM|PI> --exchange <EXCHANGE> --symbol <SYMBOL>
```

Required flags:
- `--collector <RAM|PI>`
- `--exchange <EXCHANGE>`
- `--symbol <SYMBOL>`

## Behavior
- delete `{outDir}/{collector}/{exchange}/{symbol}` if present
- delete matching market rows from `gaps` and `registry`
- keep `files` rows intact (clear does not currently delete indexed file inventory)
- resolve include paths for only the target market under `root`
- run `index` with those scoped include paths

## Include-path resolution
- If `{root}/{collector}` exists, clear treats that as the collector base.
- It includes direct `{baseRoot}/{exchange}/{symbol}` when present.
- It also scans buckets under `baseRoot` and includes `{baseRoot}/{bucket}/{exchange}/{symbol}` matches.
- If no physical matches are found, deterministic fallback include paths are used.

## Mermaid flow
```mermaid
flowchart TD
  A["CLI clear<br/>fn: runClear"] --> B["Validate required market flags<br/>fn: resolveMarketFromConfig"]
  B --> C["Delete output market directory<br/>fn: clearMarket / deleteMarketOutputs"]
  C --> D{Directory exists?}
  D -->|yes| E[outputsDeleted = 1]
  D -->|no ENOENT| F[outputsDeleted = 0]
  E --> G["Delete market DB rows in one transaction<br/>fn: deleteMarketRows / runSqliteWriteTransaction"]
  F --> G
  G --> H[DELETE gaps registry for market keep files]
  H --> I["Resolve include paths for target market<br/>fn: resolveIndexIncludePaths"]
  I --> J{collector root exists?}
  J -->|yes| K[baseRoot = root/collector]
  J -->|no| L[baseRoot = root]
  K --> M[Collect direct and bucketed exchange/symbol matches]
  L --> M
  M --> N{No matches found?}
  N -->|yes| O[Use deterministic fallback include paths]
  N -->|no| P[Use discovered include paths]
  O --> Q["Run scoped reindex<br/>fn: runIndex"]
  P --> Q
  Q --> R[Return clear delete stats plus index stats]
```

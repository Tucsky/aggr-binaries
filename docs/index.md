# Index task

## Purpose
Build and maintain the SQLite inventory used by `process`, `fixgaps`, and timeline APIs.

## Command
```bash
npm start -- index [flags]
```

Key flags:
- `--root <path>` input root
- `--db <path>` SQLite path
- `--batch <n>` insert batch size
- `--include <path>` repeatable subtree filter
- `--collector <RAM|PI>` optional scope
- `--exchange <EXCHANGE>` optional scope
- `--symbol <SYMBOL>` optional scope

## Clear workflow
`clear` uses the same market include-path resolution logic as `index`, then runs scoped reindex.

Command:
```bash
npm start -- clear --collector <RAM|PI> --exchange <EXCHANGE> --symbol <SYMBOL>
```

Behavior:
- delete `{outDir}/{collector}/{exchange}/{symbol}` if present
- delete matching market rows from `events`, `files`, and `registry`
- reindex only the resolved market subtree paths under `root`

## Input contract
Path layout:
```text
{collector}/{bucket}/{exchange}/{symbol}/{YYYY-MM-DD[-HH][.gz]}
```

- Exchange/symbol are derived from path.
- Filename timestamp is parsed as UTC.
- Plain text and gzip are accepted.

## Normalization rules
- Path-based deterministic normalization.
- Poloniex: `USDT_BTC -> BTC_USDT` (quote-second form).
- Bitget rename handling:
  - spot markets are `-SPOT`
  - `_UMCBL/_DMCBL/_CMCBL` suffixes dropped for derivatives

## Stored inventory model
`index` maintains:
- `roots`: indexed root paths
- `files`: one row per file (`collector/exchange/symbol/start_ts/...`)
- `indexed_market_ranges`: cached per-market min/max `start_ts`

## Behavior
- Walk matching files once.
- Insert rows with append-only `INSERT OR IGNORE` semantics.
- Update `indexed_market_ranges` incrementally during inserted-file batches.
- No processed flags are written.

## Determinism and schema policy
- Same input tree + config => same indexed set.
- Duplicate paths are ignored by primary key.
- Startup validates expected schema shape and required constraints.
- No in-place DB migrations are performed.
- Incompatible schema => fail fast with rebuild instruction.

## Troubleshooting
- `Incompatible schema ...`: delete DB and rerun `index`.
- `indexed_market_ranges is empty while files has data`: delete DB and rerun `index`.

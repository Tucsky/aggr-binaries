# aggr-binaries

`aggr-binaries` indexes large trade archives and builds deterministic, resumable candle binaries.

Core commands:
- `index`: inventory raw files into SQLite
- `process`: generate gap-aware binaries + companions
- `registry`: rebuild derived registry rows from companions
- `fixgaps`: recover missing trades from gap events and patch outputs
- `clear`: delete one market's outputs/state and reindex only that market

Core design constraints:
- determinism (`same input => same output`)
- resumability (resume from output state)
- scalability (millions of files)

## Documentation map
Task docs hold detailed behavior and contracts:
- [Index task](docs/index.md)
- [Clear workflow](docs/index.md#clear-workflow)
- [Process task](docs/process.md)
- [Registry task](docs/registry.md)
- [Fixgaps task](docs/fixgaps.md)
- [Merge task (planned)](docs/merge.md)

## Requirements
- Node.js `>= 22`
- `sort` in `PATH` (fixgaps rewrite flow)
- `unzip` in `PATH` (Kraken direct archive extraction)

Install:
```bash
npm install
```

## Configuration
Create `config.json` (or copy `config.example.json`) and set:
- `root`
- `dbPath`
- `batchSize`
- `flushIntervalSeconds`
- `includePaths`
- `outDir`
- `timeframe`

All values can be overridden via CLI flags.

## Quickstart
```bash
npm start -- index
npm start -- process
npm start -- registry
```

Selective run example:
```bash
npm start -- process --collector RAM --exchange BITMEX --symbol SOLUSD --timeframe 1m
```

## CLI map
- `index`: [docs/index.md](docs/index.md)
- `process`: [docs/process.md](docs/process.md)
- `registry`: [docs/registry.md](docs/registry.md)
- `fixgaps`: [docs/fixgaps.md](docs/fixgaps.md)
- `clear`: [docs/index.md](docs/index.md) (`Clear workflow`)

Shared flags:
- `-r, --root <path>`
- `-d, --db <path>`
- `--config <path>`
- `--no-config`
- `--include <path>` (repeatable)

## Input/output at a glance
Input path layout:
```text
{collector}/{bucket}/{exchange}/{symbol}/{YYYY-MM-DD[-HH][.gz]}
```

Trade line format:
```text
{ts_ms} {price} {size} {side(1=buy)} {liquidation?}
```

Output per market/timeframe:
```text
{outDir}/{collector}/{exchange}/{symbol}/{timeframe}.bin
{outDir}/{collector}/{exchange}/{symbol}/{timeframe}.json
```

Binary layout and companion contract are documented in [docs/process.md](docs/process.md).

## Timeline server/client
- Full app: `npm run serve`
- Client dev: `npm run dev:client`

Main UI routes:
- `/timeline`
- `/viewer`

## Operational notes
- DB schema policy is fresh-schema-only; incompatible schemas fail fast with rebuild guidance.
- SQLite write contention (`SQLITE_BUSY`/`SQLITE_LOCKED`) is retried with bounded backoff.
- If registry diverges from existing outputs after interruption, run `registry` for the affected scope.

## Development
```bash
npm run build:core
npm run build:client
npm run build:tests
npm test
```

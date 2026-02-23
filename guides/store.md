# Store Guide

`Slither.Store` provides ETS-backed shared state owned by a GenServer.

## Why Store Exists

- Fast, lock-free reads from ETS.
- Serialized writes for consistency.
- Python-accessible views for cross-runtime lookups.

## Behaviour

A store module implements:

- `tables/0`
- `views/0`
- `load/1`

See `lib/slither/store.ex`.

## Read/Write Model

From `Slither.Store.Server`:

- Reads: direct ETS lookup (`get`, `match`, `select`, `fold`, `size`).
- Writes: GenServer calls (`put`, `update`, `delete`, `bulk_insert`).

This gives high read concurrency with controlled write ordering.

## Hot Reload

`Server.reload/1` loads new tables then atomically swaps pointers.
Readers see old or new table, never partial loads.

## View Exposure to Python

Views declared in `views/0` are registered for each session by `Pipe.Runner`.
Use:

- `scope: :session` for run-scoped tools.
- `scope: :global` for always-on tools.

## Example

See:

- `lib/slither/examples/data_etl/schema_store.ex`
- `lib/slither/examples/text_analysis/stopword_store.ex`

# Architecture

Slither is a thin BEAM/Python coordination layer built on SnakeBridge and Snakepit.

## Core Primitives

- `Store`: ETS-backed shared state with lock-free reads and serialized writes.
- `Dispatch`: bounded batched fan-out with backpressure.
- `Pipe`: stage composition over BEAM and Python work.

## Runtime Model

- Python runs in worker processes managed by Snakepit.
- BEAM owns shared state and orchestration.
- Python workers call BEAM-exposed views through SnakeBridge callbacks.

## Supervision

The Slither application supervisor starts:

- `Task.Supervisor` for loaders
- `Slither.Bridge.ViewRegistry`
- `Slither.Store.Supervisor`
- `Slither.Pipe.Supervisor`

See `lib/slither/application.ex`.

## Dataflow

1. Input payloads are wrapped as `Slither.Item`.
2. `Pipe.Runner` creates context/session and executes stages.
3. Python stages route through `Slither.Dispatch` and executors.
4. Router stages split outputs into named buckets.

## Isolation Model

- Shared mutable state in Python is worker-local by process.
- Cross-worker merges happen on BEAM side in a deterministic single-process path.
- Backpressure is enforced with bounded `max_in_flight` concurrency.

## Where to Go Next

- Store details: `guides/store.md`
- Dispatch details: `guides/dispatch.md`
- Pipe DSL: `guides/pipe.md`

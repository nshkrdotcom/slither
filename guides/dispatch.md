# Dispatch Guide

`Slither.Dispatch` executes Python work in bounded concurrent batches.

## API

- `run/2`: process full input and return list.
- `stream/2`: lazy result stream.
- `call_batch/2`: execute one pre-assembled batch.

See `lib/slither/dispatch.ex`.

## Backpressure

`Slither.Dispatch.Runner` uses `Task.async_stream` with `max_concurrency`.
That caps in-flight batches and prevents unbounded queue growth.

## Strategies

- `FixedBatch`: fixed-size chunks.
- `WeightedBatch`: cumulative weight threshold (`max_weight`, `weight_fn`).
- `KeyPartition`: group items by key.

See `lib/slither/dispatch/strategies/*.ex`.

## Executors

- `Slither.Dispatch.Executors.SnakeBridge`: calls Python module/function via `SnakeBridge.call`.
- `Slither.Dispatch.Executors.Snakepit`: calls adapter commands via `Snakepit.execute`.

## Error Policies

- `:halt`
- `:skip`
- `{:route, fun}`
- `{:retry, opts}` (currently halts at dispatch level)

## Example

See `lib/slither/examples/batch_stats/stats_demo.ex` for all strategies.

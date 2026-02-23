# Pipe Guide

`Slither.Pipe` is a DSL for composing BEAM and Python stages.

## Define a Pipe

```elixir
defmodule MyPipe do
  use Slither.Pipe

  pipe :demo do
    stage :prepare, :beam, handler: &__MODULE__.prepare/2

    stage :score, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "my_model",
      function: "predict_batch",
      batch_size: 16

    stage :route, :router,
      routes: [
        {fn item -> item.payload["score"] > 0.8 end, :high}
      ]

    output :high
    on_error :score, :skip
    on_error :*, :halt
  end
end
```

## Execute

```elixir
{:ok, outputs} = Slither.Pipe.Runner.run(MyPipe, input)
```

`Pipe.Runner` responsibilities:

1. build run context/session
2. register store views
3. execute stages in order
4. collect outputs
5. unregister session views

See `lib/slither/pipe/runner.ex`.

## Error Policies

Configure per-stage with `on_error` in the pipe DSL.

Typical patterns:

- Skip Python failures in enrichment/scoring stages.
- Halt for topology or routing stage failures.

## Session Affinity

Pass `session_id` in `Runner.run/3` options for sticky Python state.
Useful for model training/prediction continuity.

## Example

See `lib/slither/examples/ml_scoring/scoring_pipe.ex`.

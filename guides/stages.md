# Stage Guide

A stage is one pipeline processing step implementing `Slither.Stage`.

## Built-in Stage Types

- `:beam` -> `Slither.Stage.Beam`
- `:python` -> `Slither.Stage.Python`
- `:router` -> `Slither.Stage.Router`

Mapping is defined in `lib/slither/pipe/definition.ex`.

## Beam Stage

Use for Elixir transformations.

- Per-item handler through `:handler`.
- Optional batch handler through `:batch_handler`.
- Supports `:many` cardinality expansion.

## Python Stage

Always dispatches through `Slither.Dispatch`.

Key options:

- `executor`
- `module` / `function` (SnakeBridge)
- batching and timeout options

## Router Stage

Routes by predicate list in order.

- First match wins.
- Unmatched items go to `:default`.
- Can return batch-routed maps.

## Return Semantics

Per-item stage results include:

- `{:ok, [items]}`
- `{:route, destination, [items]}`
- `{:skip, reason}`
- `{:error, reason}`

Batch stage results include:

- `{:ok, [items]}`
- `{:routed, %{dest => [items]}}`
- `{:error, reason}`

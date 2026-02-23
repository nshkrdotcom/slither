# Getting Started

This guide gets Slither running locally and introduces the first end-to-end pipeline run.

## Prerequisites

- Elixir `~> 1.15`
- Erlang/OTP compatible with your Elixir version
- Python 3 (Snakepit/SnakeBridge runtime)

## Install

Add Slither to `mix.exs`:

```elixir
defp deps do
  [
    {:slither, "~> 0.1.0"}
  ]
end
```

Fetch and compile:

```bash
mix deps.get
mix compile
```

## Configure Runtime

Slither examples use SnakeBridge + Snakepit worker pools. A minimal runtime config:

```elixir
# config/runtime.exs
import Config

SnakeBridge.ConfigHelper.configure_snakepit!(pool_size: 2)
```

## Run Examples

List available examples:

```bash
mix slither.example
```

Run one example:

```bash
mix slither.example text_analysis
```

Run all stdlib examples:

```bash
mix slither.example --all
```

Skip pure-Python baseline comparison:

```bash
mix slither.example --no-baseline
```

## Minimal Pipe

```elixir
defmodule MyPipe do
  use Slither.Pipe

  pipe :demo do
    stage :transform, :beam, handler: fn item, _ctx -> item.payload * 2 end
    output :default
  end
end

{:ok, outputs} = Slither.Pipe.Runner.run(MyPipe, [1, 2, 3])
```

## Next Guides

- `guides/architecture.md`
- `guides/store.md`
- `guides/dispatch.md`
- `guides/pipe.md`
- `guides/examples.md`

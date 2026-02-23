<p align="center">
  <img src="assets/slither.svg" alt="Slither" width="200"/>
</p>

<p align="center">
  <strong>Bounded, observable BEAM<->Python pipelines with shared state on ETS</strong>
</p>

<p align="center">
  <a href="https://hex.pm/packages/slither"><img src="https://img.shields.io/hexpm/v/slither.svg?style=flat-square&color=8b1e3f" alt="Hex.pm"/></a>
  <a href="https://hexdocs.pm/slither"><img src="https://img.shields.io/badge/docs-hexdocs-4cb8a4.svg?style=flat-square" alt="HexDocs"/></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-c9a227.svg?style=flat-square" alt="License"/></a>
</p>

Slither is a low-level concurrency substrate for BEAM + Python systems, built on top of SnakeBridge and Snakepit.

It gives you three primitives:

- `Store`: ETS-backed shared state (lock-free reads, serialized writes), exposed to Python via views.
- `Dispatch`: batched fan-out to Python workers with bounded in-flight work (`max_in_flight`).
- `Pipe`: a DSL to compose BEAM stages, Python stages, and routing in one supervised flow.

## Why use Slither

Use Slither when you want Python to keep doing compute, while BEAM owns concurrency, state, and coordination.

Common wins:

- no unbounded queue growth (backpressure)
- no shared-thread state races in Python workers
- run-scoped session affinity for Python state
- consistent telemetry across runs, stages, and batches

## Installation

```elixir
defp deps do
  [
    {:slither, "~> 0.1.0"}
  ]
end
```

Then:

```bash
mix deps.get
mix compile
```

## Quick Start

### 1) Configure a Python worker pool

```elixir
# config/runtime.exs
import Config

SnakeBridge.ConfigHelper.configure_snakepit!(pool_size: 2)
```

### 2) Define a pipeline

```elixir
defmodule MyApp.ScorePipe do
  use Slither.Pipe

  pipe :score do
    stage :prepare, :beam,
      handler: fn item, _ctx ->
        %{"text" => item.payload}
      end

    stage :predict, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "my_model",
      function: "predict_batch",
      pool: :default,
      batch_size: 32,
      max_in_flight: 4

    stage :route, :router,
      routes: [
        {fn item -> item.payload["score"] >= 0.8 end, :accept},
        {fn item -> item.payload["score"] >= 0.4 end, :review}
      ]

    output :accept
    output :review
    output :default

    on_error :predict, :skip
    on_error :*, :halt
  end
end
```

### 3) Run it

```elixir
{:ok, outputs} = Slither.run_pipe(MyApp.ScorePipe, ["first", "second", "third"])

accepted_payloads = Enum.map(outputs.accept, & &1.payload)
review_payloads = Enum.map(outputs.review, & &1.payload)
```

## Run the built-in demos

```bash
mix slither.example
mix slither.example text_analysis
mix slither.example batch_stats
mix slither.example data_etl
mix slither.example ml_scoring
mix slither.example image_pipeline
mix slither.example --all
mix slither.example --no-baseline
```

`ml_scoring` and `image_pipeline` auto-install Python deps via Snakepit/uv.

## Core APIs

### Pipe (orchestration)

- `Slither.Pipe.Runner.run/3`: run a pipe and return output buckets.
- `Slither.Pipe.Runner.stream/3`: stream outputs lazily.
- `Slither.run_pipe/3`: convenience wrapper.

### Dispatch (batched Python calls)

```elixir
items = Slither.Item.wrap_many([1, 2, 3, 4])

{:ok, results} =
  Slither.dispatch(items,
    executor: Slither.Dispatch.Executors.SnakeBridge,
    module: "my_model",
    function: "predict_batch",
    pool: :default,
    batch_size: 64,
    max_in_flight: 8,
    ordering: :preserve,
    on_error: :halt
  )
```

For large workloads, use `Slither.Dispatch.stream/2`.

### Store (shared state on ETS)

```elixir
defmodule MyApp.FeatureStore do
  @behaviour Slither.Store

  @impl true
  def tables do
    [
      %{name: :features, type: :set, read_concurrency: true}
    ]
  end

  @impl true
  def views do
    [
      %{
        name: :lookup_feature,
        mode: :scalar,
        scope: :session,
        handler: fn %{"key" => key}, _ctx ->
          case Slither.Store.Server.get(__MODULE__, :features, key) do
            nil -> %{"error" => "not_found"}
            value -> %{"value" => value}
          end
        end,
        timeout_ms: 5_000
      }
    ]
  end

  @impl true
  def load(tables) do
    :ets.insert(tables[:features], {"user:42", %{tier: "gold"}})
    :ok
  end
end
```

Start store processes by listing modules in config:

```elixir
config :slither,
  stores: [MyApp.FeatureStore]
```

Read/write API:

```elixir
Slither.Store.Server.get(MyApp.FeatureStore, :features, "user:42")
Slither.Store.Server.put(MyApp.FeatureStore, :features, "user:99", %{tier: "silver"})
```

## Configuration knobs

```elixir
config :slither,
  stores: [],
  dispatch: [
    default_batch_size: 64,
    default_max_in_flight: 8,
    default_ordering: :preserve,
    default_on_error: :halt
  ],
  bridge: [
    default_scope: :session
  ]
```

## Telemetry events

Slither emits under `[:slither, ...]`.

- dispatch: `[:slither, :dispatch, :batch, :start|:stop|:exception]`
- pipe run: `[:slither, :pipe, :run, :start|:stop|:exception]`
- pipe stage: `[:slither, :pipe, :stage, :start|:stop|:exception]`
- store: `[:slither, :store, :write]`, `[:slither, :store, :reload, :start|:stop]`
- bridge view callbacks: `[:slither, :bridge, :view, :start|:stop]`

## Troubleshooting

- Python module not found:
  run examples through `mix slither.example ...` so `PYTHONPATH` is set.
- SnakeBridge/Snakepit calls failing:
  verify runtime pool config in `config/runtime.exs`.
- Optional package import errors:
  run `mix slither.example ml_scoring` or `mix slither.example image_pipeline` to auto-install deps.

## Guides

- `guides/getting-started.md`
- `guides/store.md`
- `guides/dispatch.md`
- `guides/pipe.md`
- `guides/examples.md`
- `guides/operations.md`

## License

MIT

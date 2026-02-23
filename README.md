<p align="center">
  <img src="assets/slither.svg" alt="Slither" width="200"/>
</p>

<p align="center">
  <strong>Low-level BEAM&#x2194;Python concurrency substrate</strong>
</p>

<p align="center">
  <a href="https://hex.pm/packages/slither"><img src="https://img.shields.io/hexpm/v/slither.svg?style=flat-square&color=8b1e3f" alt="Hex.pm"/></a>
  <a href="https://hexdocs.pm/slither"><img src="https://img.shields.io/badge/docs-hexdocs-4cb8a4.svg?style=flat-square" alt="HexDocs"/></a>
  <a href="https://github.com/nshkrdotcom/slither/actions"><img src="https://img.shields.io/github/actions/workflow/status/nshkrdotcom/slither/ci.yml?style=flat-square&label=CI" alt="CI"/></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-c9a227.svg?style=flat-square" alt="License"/></a>
</p>

---

Slither is a thin, low-level framework on top of [SnakeBridge](https://github.com/nshkrdotcom/snakebridge) and [Snakepit](https://github.com/nshkrdotcom/snakepit) that provides three reusable primitives for hybrid BEAM/Python systems:

- **Store** -- ETS-backed shared state with lock-free concurrent reads and serialized writes, exposed to Python via bidirectional tool calls
- **Dispatch** -- Bounded batched fan-out to Python pools with real backpressure, ordering control, and policy-driven error handling
- **Pipe** -- Stage composition over BEAM + Python steps with routing, telemetry, and supervised execution

Everything is OTP. Everything has telemetry. Nothing is magic.

## Installation

```elixir
def deps do
  [
    {:slither, "~> 0.1.0"}
  ]
end
```

## Design Goals

- **Keep Python code in Python** (PyTorch, HF, spaCy, etc.) while moving concurrency and shared state to the BEAM.
- **Generic primitives** usable across fraud feature stores, game worlds, IoT fleets, crawlers, and more.
- Make the bridge membrane explicit: **Python computes; BEAM coordinates and owns state**.
- **Batching to amortize bridge overhead** and **true backpressure** to prevent OOM / queue collapse.

## Quick Start

```elixir
defmodule MyPipe do
  use Slither.Pipe

  pipe :scoring do
    store :features, MyApp.FeatureStore

    stage :enrich, :beam,
      handler: &MyApp.enrich/2

    stage :score, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "my_model",
      function: "predict_batch",
      pool: :gpu_pool,
      batch_size: 32

    stage :route, :router,
      routes: [
        {&MyApp.high_confidence?/1, :accept},
        {&MyApp.low_confidence?/1, :review},
        {fn _ -> true end, :reject}
      ]

    output :accept
    output :review
    output :reject

    on_error :score, :skip
    on_error :*, :halt
  end
end

{:ok, results} = Slither.Pipe.Runner.run(MyPipe, input_data)
# => %{accept: [...], review: [...], reject: [...], default: [...]}
```

## Architecture

```
slither/
├── lib/slither/
│   ├── item.ex                          # Work envelope (id, payload, meta, route)
│   ├── context.ex                       # Run context (session, stores, metadata)
│   ├── errors.ex                        # Error types
│   ├── telemetry.ex                     # Event definitions + span helpers
│   ├── application.ex                   # OTP supervisor tree
│   │
│   ├── bridge.ex                        # Tool registration (Python↔BEAM views)
│   ├── bridge/view_registry.ex          # ETS-backed session→callback mapping
│   │
│   ├── store.ex                         # Behaviour: tables, views, load
│   ├── store/server.ex                  # GenServer: owns ETS, serialized writes
│   ├── store/pointer.ex                 # persistent_term for hot-swap reload
│   ├── store/supervisor.ex              # Supervises store processes
│   │
│   ├── dispatch.ex                      # API: run/stream/call_batch
│   ├── dispatch/executor.ex             # Behaviour: execute_batch/3
│   ├── dispatch/executors/snakepit.ex   # Execute via Snakepit commands
│   ├── dispatch/executors/snakebridge.ex# Execute via SnakeBridge FFI
│   ├── dispatch/strategy.ex             # Behaviour: batching strategy
│   ├── dispatch/strategies/
│   │   ├── fixed_batch.ex               # Fixed-size chunks
│   │   ├── weighted_batch.ex            # Batch by weight function
│   │   └── key_partition.ex             # Group by key function
│   ├── dispatch/runner.ex               # Bounded concurrency engine
│   │
│   ├── stage.ex                         # Behaviour: handle_item/handle_batch
│   ├── stage/beam.ex                    # Elixir function stage
│   ├── stage/python.ex                  # Python dispatch stage
│   ├── stage/router.ex                  # Predicate-based routing
│   │
│   ├── pipe.ex                          # DSL entry (use Slither.Pipe)
│   ├── pipe/builder.ex                  # Macro DSL
│   ├── pipe/definition.ex               # Compiled pipe struct
│   ├── pipe/runner.ex                   # Executes pipe with session mgmt
│   └── pipe/supervisor.ex               # DynamicSupervisor for long-lived pipes
```

### Supervision Tree

```
Slither.Supervisor (one_for_one)
├── Slither.LoaderSupervisor          Task.Supervisor for store loaders
├── Slither.Bridge.ViewRegistry       ETS registry for view callbacks
├── Slither.Store.Supervisor          Supervises configured store GenServers
│   └── Slither.Store.Server          One per store module, owns ETS tables
└── Slither.Pipe.Supervisor           DynamicSupervisor for long-lived pipes
```

---

## Core Concepts

### Item Envelope

All work flows through Slither as an `Item` -- a wrapper with a stable ID for ordering and error attribution, stage-local metadata for tracing, and a route tag for dispatch without coupling payload types.

```elixir
%Slither.Item{
  id: 42,                  # auto-generated monotonic integer
  payload: %{text: "..."},  # your data
  meta: %{},                # stage-local metadata
  route: :default           # routing destination
}
```

| Function | Description |
|----------|------------|
| `Item.wrap(payload)` | Wrap a raw payload with an auto-generated ID |
| `Item.wrap_many(payloads)` | Wrap a list of payloads with sequential IDs |
| `Item.update_payload(item, new)` | Update payload, preserving ID and meta |
| `Item.put_meta(item, map)` | Merge metadata into the item |

### Run Context

Every stage receives a `Context` containing session affinity for Snakepit/SnakeBridge, store handles, and run metadata.

```elixir
ctx = Slither.Context.new(
  pipe: :my_pipe,
  session_id: "session_123",
  stores: %{features: MyApp.FeatureStore},
  metadata: %{user_id: 42}
)
```

Each `Pipe.Runner.run/3` creates a unique session by default. The runner registers store views as session-scoped tools and unregisters them on completion.

---

## Store

### The Problem

Python workers need to read large reference data (drug databases, feature stores, lookup tables). Python threads cause refcount contention. Redis turns the hot path into network serialization. Both are wrong.

### The Solution

ETS tables owned by a GenServer on the BEAM side. Tables use `:protected` access -- any BEAM process reads directly (lock-free with `:read_concurrency`), but only the owning GenServer writes. Python accesses state through SnakeBridge callbacks -- one bridge round-trip per lookup, no locks, no middlebox.

### Defining a Store

Implement the `Slither.Store` behaviour with three callbacks:

```elixir
defmodule MyApp.FeatureStore do
  @behaviour Slither.Store

  alias Slither.Store.Server

  @impl true
  def tables do
    [
      %{
        name: :features,
        type: :set,
        keypos: 1,
        read_concurrency: true,
        write_concurrency: false,
        compressed: false
      },
      %{
        name: :user_index,
        type: :ordered_set,
        keypos: 1,
        read_concurrency: true,
        write_concurrency: false,
        compressed: false
      }
    ]
  end

  @impl true
  def views do
    [
      # Scalar view: Python calls with a single key
      %{
        name: :lookup_feature,
        mode: :scalar,
        scope: :session,
        handler: fn %{"key" => key}, _ctx ->
          case Server.get(__MODULE__, :features, key) do
            nil -> %{"error" => "not_found"}
            value -> %{"value" => value}
          end
        end,
        timeout_ms: 5_000
      },
      # Batch view: Python sends N keys, gets N results in one round-trip
      %{
        name: :lookup_features_batch,
        mode: :batch,
        scope: :session,
        handler: fn %{"keys" => keys}, _ctx ->
          results = Enum.map(keys, &Server.get(__MODULE__, :features, &1))
          %{"results" => results}
        end,
        timeout_ms: 10_000
      }
    ]
  end

  @impl true
  def load(tables) do
    # Called in the GenServer process that owns the tables.
    # Populate from your data source.
    for {key, value} <- load_from_database() do
      :ets.insert(tables[:features], {key, value})
    end
    :ok
  end
end
```

Register stores in your application config:

```elixir
config :slither,
  stores: [MyApp.FeatureStore]
```

### Reading and Writing

Reads bypass the GenServer entirely and go straight to ETS. Writes serialize through the owning process for consistency.

```elixir
alias Slither.Store.Server

# Reads -- lock-free, from any process
Server.get(MyApp.FeatureStore, :features, "key123")
Server.get!(MyApp.FeatureStore, :features, "key123")  # raises on miss
Server.match(MyApp.FeatureStore, :features, {"prefix_" <> _, :_})
Server.select(MyApp.FeatureStore, :features, match_spec)
Server.fold(MyApp.FeatureStore, :features, 0, fn {_k, v}, acc -> acc + v end)
Server.size(MyApp.FeatureStore, :features)

# Writes -- serialized through GenServer
Server.put(MyApp.FeatureStore, :features, "new_key", "value")
Server.update(MyApp.FeatureStore, :features, "counter", &(&1 + 1))
Server.delete(MyApp.FeatureStore, :features, "old_key")
Server.bulk_insert(MyApp.FeatureStore, :features, records)
```

### Hot Reload

`Server.reload/1` creates new ETS tables, fully loads them, then atomically swaps the pointer via `:persistent_term`. Readers see either the old or the new table, never a half-loaded state. Old tables are retired after a grace period so in-flight reads complete safely.

```elixir
Server.reload(MyApp.FeatureStore)
```

### View Scoping

Views can be `:session` (default) or `:global`:

- **Session-scoped** views are registered when a pipe run starts and unregistered when it completes. They're tied to a `session_id` for Snakepit affinity.
- **Global** views persist for the application lifetime.

---

## Dispatch

### The Problem

You have 10,000 items that each need Python processing. One at a time wastes throughput (bridge overhead). All at once causes OOM. Naive `Task.async` spam collapses queues and makes tail latencies unpredictable.

### The Solution

Bounded batched fan-out. Items are split by a pluggable strategy, executed through an Executor with `max_in_flight` concurrency, and returned in order or as-completed. Upstream enumeration only advances when a worker slot is available -- real backpressure.

### Basic Usage

```elixir
items = Slither.Item.wrap_many(data)

# Run with SnakeBridge executor
{:ok, results} = Slither.Dispatch.run(items,
  executor: Slither.Dispatch.Executors.SnakeBridge,
  module: "my_model",
  function: "predict_batch",
  pool: :gpu_pool,
  batch_size: 32,
  max_in_flight: 4,
  on_error: :skip
)

# Or with Snakepit executor
{:ok, results} = Slither.Dispatch.run(items,
  executor: Slither.Dispatch.Executors.Snakepit,
  pool: :compute,
  command: "process_batch",
  batch_size: 64,
  max_in_flight: 8
)

# Stream results lazily
stream = Slither.Dispatch.stream(items,
  executor: Slither.Dispatch.Executors.SnakeBridge,
  module: "processor",
  function: "transform",
  batch_size: 100
)

Enum.each(stream, &handle_result/1)

# Send a pre-assembled batch directly (no strategy applied)
{:ok, results} = Slither.Dispatch.call_batch(batch, executor: executor, ...)
```

### Dispatch Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:executor` | `module()` | (required) | Executor module |
| `:strategy` | `module()` | `FixedBatch` | Batching strategy |
| `:batch_size` | `pos_integer()` | `64` | Items per batch |
| `:max_in_flight` | `pos_integer()` | `8` | Max concurrent batches |
| `:ordering` | `:preserve \| :as_completed` | `:preserve` | Result ordering |
| `:on_error` | error policy | `:halt` | Error handling |
| `:timeout` | `pos_integer()` | `30_000` | Per-batch timeout (ms) |
| `:context` | `Context.t()` | auto-created | Run context |

### Batching Strategies

Strategies implement the `Slither.Dispatch.Strategy` behaviour:

```elixir
@callback batch([Item.t()], keyword()) :: [{ref :: pos_integer(), [Item.t()]}]
```

**Built-in strategies:**

| Strategy | Use Case | Key Option |
|----------|----------|-----------|
| `Strategies.FixedBatch` | Default. Split into N-sized chunks | `batch_size: 64` |
| `Strategies.WeightedBatch` | Batch by token count, byte size, etc. | `max_weight: 1000`, `weight_fn: &weight/1` |
| `Strategies.KeyPartition` | Items with same key must be processed together | `key_fn: &partition_key/1` |

```elixir
# Weighted batching by token count
Slither.Dispatch.run(items,
  executor: executor,
  strategy: Slither.Dispatch.Strategies.WeightedBatch,
  max_weight: 4096,
  weight_fn: fn item -> String.length(item.payload.text) end
)

# Key-partitioned batching
Slither.Dispatch.run(items,
  executor: executor,
  strategy: Slither.Dispatch.Strategies.KeyPartition,
  key_fn: fn item -> item.payload.user_id end
)
```

### Executors

Executors implement the `Slither.Dispatch.Executor` behaviour:

```elixir
@callback execute_batch([Item.t()], Context.t(), keyword()) ::
            {:ok, [Item.t()]} | {:error, term()}
```

**`Slither.Dispatch.Executors.SnakeBridge`** -- Universal FFI calls via `SnakeBridge.call/4`. Use when calling a Python module/function pair directly. Requires `:module` and `:function`.

**`Slither.Dispatch.Executors.Snakepit`** -- Tool/adapter commands via `Snakepit.execute/3`. Use when Python work is exposed as a Snakepit tool. Requires `:pool` and `:command`.

Both executors send the batch as `%{"items" => [payloads]}` and expect `%{"items" => [results]}` back.

### Custom Executors

```elixir
defmodule MyExecutor do
  @behaviour Slither.Dispatch.Executor

  @impl true
  def execute_batch(batch, ctx, opts) do
    payloads = Enum.map(batch, & &1.payload)
    results = MyExternalService.process(payloads)

    updated = Enum.zip_with(batch, results, fn item, result ->
      Slither.Item.update_payload(item, result)
    end)

    {:ok, updated}
  end
end
```

---

## Stage

Every processing step in a pipe is a Stage. Stages implement the `Slither.Stage` behaviour:

```elixir
@callback init(keyword()) :: {:ok, state} | {:error, reason}
@callback handle_item(Item.t(), Context.t(), state) :: item_result()
@callback handle_batch([Item.t()], Context.t(), state) :: batch_result()  # optional
@callback shutdown(state) :: :ok                                          # optional
```

### Result Types

From `handle_item/3`:

| Return | Meaning |
|--------|---------|
| `{:ok, [items]}` | Emit items downstream (1:1 or 1:many) |
| `{:route, dest, [items]}` | Route items to a named output |
| `{:skip, reason}` | Drop this item |
| `{:error, reason}` | Error -- handled by pipe's error policy |

From `handle_batch/3`:

| Return | Meaning |
|--------|---------|
| `{:ok, [items]}` | Emit items downstream |
| `{:routed, %{dest => [items]}}` | Route to multiple outputs |
| `{:error, reason}` | Error |
| `:per_item` | Fall back to per-item processing with pipe error policy |

### Built-In Stages

#### Beam Stage

Wraps an Elixir function. The handler receives an `Item` and `Context`, and its return value becomes the new payload.

```elixir
stage :transform, :beam,
  handler: fn item, _ctx ->
    String.upcase(item.payload)
  end
```

The handler can return bare values (used as payload) or tagged tuples:

```elixir
# Bare value -- becomes new payload
fn item, _ctx -> item.payload * 2 end

# Tagged ok
fn item, _ctx -> {:ok, item.payload * 2} end

# Skip
fn item, _ctx -> {:skip, :too_small} end

# Error (handled by pipe error policy)
fn item, _ctx -> {:error, :invalid_input} end

# Route to a named output
fn item, _ctx -> {:route, :special, item.payload} end
```

**1:many expansion** -- when `:cardinality` is `:many`, a list return value is expanded into separate downstream items:

```elixir
stage :extract_claims, :beam,
  handler: fn item, _ctx ->
    extract_claims(item.payload.summary)
    # Returns ["claim1", "claim2", "claim3"]
    # Each becomes a separate item downstream
  end,
  cardinality: :many
```

**Custom batch handler** -- bypass per-item processing for vectorized BEAM work:

```elixir
stage :vectorized, :beam,
  handler: fn item, _ctx -> item.payload end,
  batch_handler: fn items, _ctx ->
    results = MyModule.process_all(Enum.map(items, & &1.payload))
    {:ok, Enum.zip_with(items, results, &Slither.Item.update_payload/2)}
  end
```

#### Python Stage

Dispatches batches to Python through `Slither.Dispatch`. Always operates in batch mode.

```elixir
stage :predict, :python,
  executor: Slither.Dispatch.Executors.SnakeBridge,
  module: "transformers_model",
  function: "predict_batch",
  pool: :gpu_pool,
  batch_size: 32,
  max_in_flight: 4,
  timeout: 60_000
```

All Dispatch options are forwarded. The Python function receives a list of payloads and must return a list of results in the same order.

#### Router Stage

Routes items to named outputs based on predicate functions. Predicates are evaluated in order; the first match wins. Unmatched items go to `:default`.

```elixir
stage :classify, :router,
  routes: [
    {fn item -> item.payload.score > 0.9 end, :accept},
    {fn item -> item.payload.score > 0.5 end, :review},
    {fn _ -> true end, :reject}
  ]
```

---

## Pipe

### The Problem

You need to compose BEAM and Python work into a sequence. Each step might be 1:1, 1:many, or filter. Some route. You need backpressure, error handling, session-scoped tools, and ordered output. And you need to declare this statically so it's inspectable and testable.

### The Solution

A Pipe is a list of stages with wiring, declared via a compile-time DSL and executed by `Slither.Pipe.Runner`.

### DSL

```elixir
defmodule MyApp.ScoringPipe do
  use Slither.Pipe

  pipe :scoring do
    # Register stores -- views are auto-registered as session tools
    store :features, MyApp.FeatureStore

    # BEAM stage: enrich items with feature data
    stage :enrich, :beam,
      handler: &MyApp.Enricher.enrich/2

    # Python stage: score via ML model
    stage :score, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "scoring_model",
      function: "predict_batch",
      pool: :gpu_pool,
      batch_size: 32,
      max_in_flight: 4

    # BEAM stage: post-process scores
    stage :normalize, :beam,
      handler: fn item, _ctx -> Map.update!(item.payload, :score, &Float.round(&1, 4)) end

    # Router stage: split by confidence
    stage :classify, :router,
      routes: [
        {fn item -> item.payload.score >= 0.9 end, :accept},
        {fn item -> item.payload.score >= 0.5 end, :review},
        {fn _ -> true end, :reject}
      ]

    # Declare named outputs
    output :accept
    output :review
    output :reject

    # Error policies per stage
    on_error :score, :skip       # skip items that fail Python scoring
    on_error :*, :halt           # halt on any other stage error
  end
end
```

### DSL Reference

| Macro | Arguments | Description |
|-------|-----------|-------------|
| `pipe(name, do: block)` | `atom, block` | Define a named pipe |
| `stage(name, type, opts)` | `atom, :beam \| :python \| :router, keyword` | Add a processing stage |
| `store(name, module)` | `atom, module` | Register a store for session-scoped views |
| `output(name)` | `atom` | Declare a named output bucket |
| `on_error(stage, policy)` | `atom \| :*, policy` | Set error policy for a stage |

### Running Pipes

```elixir
# Basic run -- input can be raw payloads or pre-wrapped Items
{:ok, outputs} = Slither.Pipe.Runner.run(MyApp.ScoringPipe, input_data)
# => %{accept: [%Item{}, ...], review: [...], reject: [...], default: [...]}

# With explicit session
{:ok, outputs} = Slither.Pipe.Runner.run(MyApp.ScoringPipe, input_data,
  session_id: "my_session_123",
  metadata: %{batch_id: "batch_42"}
)

# Stream mode (v0.1: delegates to run internally)
streams = Slither.Pipe.Runner.stream(MyApp.ScoringPipe, input_data)
# => %{accept: #Stream<...>, review: #Stream<...>, ...}
```

### Session Model

Each `run/3` call creates or uses a `session_id`:

1. Ensures store processes are running
2. Registers store views as session-scoped tools for Python
3. Executes stages sequentially
4. Unregisters session tools on completion (even on error)

This means Python code within a pipe run can call back into BEAM to read store data, and those callbacks are automatically scoped and cleaned up.

### Execution Flow

```
Input (Enumerable)
  │
  │ wrap into Items
  ▼
Stage 1 (:beam)     ──→ handle_item per item (or handle_batch)
  │
  ▼
Stage 2 (:python)   ──→ Dispatch.run with configured executor + strategy
  │
  ▼
Stage 3 (:beam)     ──→ handle_item per item
  │
  ▼
Stage 4 (:router)   ──→ handle_batch, split into named outputs
  │
  ├──→ :accept  ──→ output collector
  ├──→ :review  ──→ output collector
  └──→ :reject  ──→ output collector
```

---

## Error Handling

Errors are handled at two levels with explicit policies.

### Error Policies

| Policy | Behavior |
|--------|----------|
| `:skip` | Drop failed items, continue processing |
| `:halt` | Stop the run, return partial results with error |
| `{:retry, opts}` | Retry failed batches (planned; falls back to error in v0.1) |
| `{:route, fun}` | Route failed items through a compensation function |

### Dispatch-Level Errors

When an executor returns `{:error, reason}`, the Dispatch runner applies its `on_error` policy per batch. With `:skip`, failed items get error metadata attached. With `:halt`, the run stops and returns `{:error, reason, partial_results}`.

### Pipe-Level Errors

The pipe runner applies per-stage error policies declared via `on_error` in the DSL. When using BEAM stages, errors from individual items are handled according to the policy -- `:skip` drops the item, `:halt` stops the pipe.

```elixir
# Skip failures in one stage, halt on everything else
on_error :risky_stage, :skip
on_error :*, :halt
```

### Error Types

| Exception | When |
|-----------|------|
| `Slither.Error` | General operation failure |
| `Slither.DispatchError` | Batch dispatch failed with `:halt` policy |
| `Slither.PipeError` | Pipe run hit an unrecoverable failure |

---

## Telemetry

Every boundary emits telemetry events under the `[:slither, ...]` prefix.

### Dispatch Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:slither, :dispatch, :batch, :start]` | `system_time` | `batch_ref`, `batch_size`, `executor`, `pipe`, `stage` |
| `[:slither, :dispatch, :batch, :stop]` | `duration` | same |
| `[:slither, :dispatch, :batch, :exception]` | `duration` | same + `kind`, `reason` |

### Store Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:slither, :store, :write]` | `count` | `store`, `table` |
| `[:slither, :store, :reload, :start]` | `system_time` | `store` |
| `[:slither, :store, :reload, :stop]` | `duration` | `store` |

### Pipe Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:slither, :pipe, :run, :start]` | `system_time` | `pipe`, `input_count` |
| `[:slither, :pipe, :run, :stop]` | `duration` | `pipe`, `input_count` |
| `[:slither, :pipe, :run, :exception]` | `duration` | `pipe`, `input_count`, `kind`, `reason` |
| `[:slither, :pipe, :stage, :start]` | `system_time` | `pipe`, `stage`, `type` |
| `[:slither, :pipe, :stage, :stop]` | `duration` | `pipe`, `stage`, `type` |

### Attaching Handlers

```elixir
:telemetry.attach_many("slither-logger", [
  [:slither, :dispatch, :batch, :stop],
  [:slither, :pipe, :run, :stop],
  [:slither, :store, :write]
], &MyApp.TelemetryHandler.handle_event/4, nil)
```

---

## Configuration

```elixir
# config/config.exs
config :slither,
  # Store modules to start at application boot
  stores: [MyApp.FeatureStore, MyApp.UserStore],

  # Dispatch defaults
  dispatch: [
    default_batch_size: 64,       # items per batch
    default_max_in_flight: 8,     # concurrent batches
    default_ordering: :preserve,  # :preserve | :as_completed
    default_on_error: :halt       # :skip | :halt
  ],

  # Bridge defaults
  bridge: [
    default_scope: :session       # :session | :global
  ]
```

Recommended Snakepit configuration for hybrid runs:

```elixir
config :snakepit,
  affinity: :strict_queue    # session/tool calls remain coherent under load
```

---

## What Slither Does and Does Not Do

### Slither adds:

| Primitive | What It Does |
|-----------|-------------|
| **Store** | Shared state on BEAM with Python-visible views |
| **Dispatch** | Bounded batching + backpressure for cross-bridge calls |
| **Pipe** | Stage composition + routing with session-scoped tools |

### Slither delegates:

| Concern | Handled By |
|---------|-----------|
| Python env setup, venvs, wheels | SnakeBridge (`mix snakebridge.setup`) |
| Pool management, worker lifecycle | Snakepit |
| Wire protocol, type encoding | SnakeBridge (JSON-over-gRPC) |
| Retries, circuit breakers | Snakepit |
| Python process supervision | Snakepit (crash barriers) |
| Durable jobs | Oban |
| Distributed process registry | Horde/pg |

---

## Dependencies

```elixir
{:snakebridge, "~> 0.16.0"},   # Python bindings + codegen
{:snakepit, "~> 0.13.0"},      # Process pool (transitive via snakebridge)
{:gen_stage, "~> 1.2"},        # Backpressure primitives
{:telemetry, "~> 1.2"},        # Observability
{:nimble_options, "~> 1.1"},   # Option validation
```

## License

MIT

# Slither Examples and Pure-Python Baselines

These examples are designed to compare two approaches under concurrency pressure:

1. Slither pipelines with process-per-worker isolation, routing, and backpressure.
2. A pure-Python baseline harness in `priv/python/examples/pure_python_baselines.py` that runs:
   - `unsafe_threaded`: naive shared-state threading
   - `safe_python`: synchronized pure-Python implementation

The comparison goal is fairness and reproducibility, not "Python can never do this" claims.
Safe Python can be made correct, but it needs explicit locking, careful ownership rules,
and bespoke control flow that Slither gives as first-class pipeline primitives.

## Quick Start

```bash
# List examples
mix slither.example

# Run one example + baseline comparison
mix slither.example text_analysis
mix slither.example batch_stats
mix slither.example data_etl
mix slither.example ml_scoring
mix slither.example image_pipeline

# Run stdlib examples only (1-3)
mix slither.example --all

# Skip baseline comparison
mix slither.example --no-baseline
```

## Baseline Runtime Rules

Baseline runs are executed with free-threaded CPython and strict mode checks:

- interpreter target: CPython 3.14 free-threaded (`python3.14t` or `uv run --python cpython-3.14+freethreaded`)
- required flag: `--require-free-threaded` (hard-fails on GIL runtime)
- mode: `--mode both` (`unsafe_threaded` + `safe_python`)
- threads: matches configured Snakepit pool size

Direct baseline run:

```bash
uv run --python cpython-3.14+freethreaded -- \
  priv/python/examples/pure_python_baselines.py --all --mode both --threads 48 --require-free-threaded
```

## Workload Matrix

| Example | Slither Workload | Baseline Workload Shape |
|---|---|---|
| `text_analysis` | 5,000 docs through pipe stages | 5,000 docs, shared index + doc counter |
| `batch_stats` | 2,000 datasets (incl. 50 poison) | same dataset mix, Welford aggregation |
| `data_etl` | 15,000 rows over 3 schema phases | same 3 phases with schema swaps |
| `ml_scoring` | 2 sessions, 2,000 train + 2,000 predict each | same dual-session train/predict flow |
| `image_pipeline` | 200 images with weighted batching | same image mix, pure-Python resize simulation |

## What Each Example Demonstrates

### 1) Text Analysis (`text_analysis`)

- Unsafe threaded hazard: lock-free shared `dict` + shared counter updates lose writes.
- Safe Python fix: thread-local `Counter`s merged deterministically.
- Slither value: process-local state by default; merge happens explicitly in Elixir.

### 2) Batch Stats (`batch_stats`)

- Unsafe threaded hazard: lock-free multi-step Welford updates corrupt running stats.
- Safe Python fix: thread-local `RunningStats` with deterministic merge.
- Slither value: isolate failures with `on_error: :skip`, keep workers independent.

### 3) Data ETL (`data_etl`)

- Unsafe threaded hazard: unsynchronized counters and schema handoff create inconsistent totals.
- Safe Python fix: locked schema snapshots + synchronized counters.
- Slither value: ETS-backed schema handoff + per-worker audit state without cross-thread mutation.

### 4) ML Scoring (`ml_scoring`)

- Unsafe threaded hazard: shared model slot/counter design collapses session isolation.
- Safe Python fix: session-keyed model store + synchronized prediction counting.
- Slither value: session affinity and per-process model ownership are part of dispatch semantics.

### 5) Image Pipeline (`image_pipeline`)

- Unsafe threaded hazard: unbounded fan-out and lock-free memory counters hide true pressure.
- Safe Python fix: weighted batches + bounded in-flight processing + synchronized memory tracking.
- Slither value: built-in weighted batching and `max_in_flight` backpressure at pipeline level.

## Interpreting Results Correctly

- `unsafe_threaded` is expected to expose race-prone design choices.
- `safe_python` should pass invariants when designed correctly.
- If `safe_python` fails, treat it as a real bug in the baseline harness (not proof of Slither superiority).
- Slither's core advantage is reducing custom coordination code while preserving isolation/backpressure guarantees.

## File Layout

```text
lib/slither/examples/
  text_analysis/
    text_pipe.ex
    stopword_store.ex
  batch_stats/
    stats_demo.ex
  data_etl/
    etl_pipe.ex
    schema_store.ex
  ml_scoring/
    scoring_pipe.ex
    feature_store.ex
  image_pipeline/
    thumbnail_demo.ex

priv/python/examples/
  text_analyzer.py
  batch_stats.py
  csv_transformer.py
  ml_scorer.py
  image_processor.py
  pure_python_baselines.py

lib/mix/tasks/
  slither.example.ex
```

## Troubleshooting

- `ERROR: free-threaded Python is required for this baseline run`
  - Install/resolve CPython 3.14 free-threaded and rerun via `uv`.
- `scikit-learn unavailable` / `Pillow unavailable` in the Slither examples
  - Run through `mix slither.example <name>` so dependency setup runs first.
- Python module import errors in examples
  - Use `mix slither.example ...` so `PYTHONPATH` is configured automatically.

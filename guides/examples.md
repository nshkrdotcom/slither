# Slither Examples: Why Process Isolation Beats Free-Threaded Python

Python 3.13+ ships with `--disable-gil` (PEP 703). The pitch: "just use threads,
no GIL serialization, true parallelism." But removing the GIL doesn't make concurrent
code safe -- it removes the safety net and hands you the foot-gun.

Each example below demonstrates a **real concurrency scenario that breaks under naive
free-threaded Python** but works correctly under Slither's process-per-worker architecture.

## Quick Start

```bash
# List all examples
mix slither.example

# Run a specific example
mix slither.example text_analysis
mix slither.example batch_stats
mix slither.example data_etl
mix slither.example ml_scoring      # auto-installs scikit-learn via Snakepit/uv
mix slither.example image_pipeline  # auto-installs Pillow via Snakepit/uv

# Run all stdlib examples (no extra packages needed)
mix slither.example --all

# Skip pure-Python threaded baseline comparison
mix slither.example --no-baseline
```

---

## What Free-Threaded Python Gets Wrong

| Problem | What Happens | GIL Hid This? |
|---------|-------------|---------------|
| Shared mutable state | `dict[key] += count` from N threads = lost updates | Yes |
| C extension safety | numpy BLAS, Pillow libImaging not thread-safe | Yes |
| Fault isolation | One thread segfaults = entire process dies | N/A |
| Backpressure | No limit on concurrent work = OOM | N/A |
| Hot-reload contention | Read config while another thread writes it = torn reads | Yes |
| Session-scoped state | Trained model in shared dict = cross-contamination | Yes |

## What Slither Gets Right

- **Process isolation**: each Python worker is its own OS process. Crash one, others continue.
- **Session affinity**: train a model on worker 3, predict on worker 3. No shared dict.
- **ETS-backed stores**: Elixir manages shared state with lock-free reads, serialized writes.
- **Batched dispatch with backpressure**: `max_in_flight` + `WeightedBatch` = bounded memory.
- **Fault tolerance**: `on_error: :skip` drops failed batches, pipeline continues.

---

## Examples

### 1. Shared Accumulator Races \[stdlib\]

**Free-threaded Python failure:** `_global_index[word] += count` from N threads = lost
updates. `_doc_count += 1` is a torn counter. Classic read-modify-write races that the
GIL accidentally prevented.

**Slither solution:** Each worker accumulates into its own `_global_index` in its own
process. Zero contention. The Elixir side merges per-worker results in a single process.

**Modules:**
- `Slither.Examples.TextAnalysis.TextPipe` -- 3-stage Pipe + post-run worker stats merge
- `Slither.Examples.TextAnalysis.StopwordStore` -- ETS-backed stopword list
- `text_analyzer.py` -- word frequency, readability, sentiment with per-worker accumulation

**Pipeline:**
```
40+ documents
  |
  v
[prepare] :beam -- read stopwords from ETS
  |
  v
[analyze] :python -- analyze + accumulate per-worker stats
  (each worker has its own _global_index, no contention)
  |
  v
[classify] :router -- route by sentiment
  |
  v
[merge] Elixir-side -- collect per-worker stats, merge safely
  Prints per-worker PIDs, doc counts, vocab sizes
  Prints merged global top words
```

**Key output:** Per-worker PIDs prove different processes. Per-worker doc counts sum to
total. Merged vocabulary is race-free because merging happens in a single Elixir process.

---

### 2. Fault-Isolated Parallel Compute \[stdlib\]

**Free-threaded Python failure:** Poison pill dataset triggers C extension bug. Under
threading: entire process segfaults, all threads die, all in-flight work lost. Shared
running statistics accumulator (`_running_stats["mean"] = new_mean`) produces incorrect
Welford's algorithm results from concurrent multi-step updates.

**Slither solution:** Worker processing poison pill crashes and gets restarted. Other
workers continue. `on_error: :skip` drops the failed batch. Each worker has its own
running stats accumulator -- Welford's algorithm is correct per-worker.

**Modules:**
- `Slither.Examples.BatchStats.StatsDemo` -- all 3 batching strategies + streaming + fault stats
- `batch_stats.py` -- descriptive statistics with Welford's online accumulator

**Flow:**
```
50 datasets + 5 poison pills (nil, NaN, empty)
  |
  +---> FixedBatch(10)     [on_error: :skip]
  +---> WeightedBatch(500) [on_error: :skip]
  +---> KeyPartition       [on_error: :skip]
  +---> Streaming          [as_completed]
  |
  v
Worker stats: per-worker Welford accumulator results
Fault report: N poison pills, N batches skipped, N results completed
```

**Key output:** "Under free-threaded Python, one bad dataset crashes all threads."
Shows N batches failed, N succeeded. Per-worker running stats are internally consistent.

---

### 3. Hot-Reload Under Contention \[stdlib\]

**Free-threaded Python failure:** Thread A reads validation schema. Thread B swaps the
schema (hot-reload). Thread A validates against half-old, half-new schema (torn read).
`_audit_log.append(...)` from concurrent threads = corrupted list (list.append is only
atomic under the GIL). `_validation_count += 1` is a textbook race.

**Slither solution:** Schema lives in ETS. `Server.put()` is atomic per-key. Python
workers receive the schema as part of their batch payload (read from ETS in the prepare
stage). Each worker has its own `_audit_log` -- no list corruption.

**Modules:**
- `Slither.Examples.DataEtl.EtlPipe` -- 4-stage Pipe with hot-reload between batches
- `Slither.Examples.DataEtl.SchemaStore` -- hot-reloadable validation schemas
- `csv_transformer.py` -- validation with per-worker audit log

**Pipeline:**
```
Batch 1 (16 rows, v1 schema -- lenient)
  |
  v
[prepare] :beam -- read schema from ETS (lock-free)
[validate] :python -- validate + append to per-worker _audit_log
[transform] :beam -- apply rename/cast/default rules
[route] :router -- valid -> :default, invalid -> :invalid

--- hot-reload: Server.put(SchemaStore, :schemas, :current, v2) ---
    (atomic ETS write, zero Python-side coordination)

Batch 2 (16 rows, v2 schema -- stricter)
  same pipeline, stricter rules, rows that passed v1 now fail v2
  |
  v
Audit stats: per-worker validation counts, schema versions seen
  Proves: batch 1 saw v1, batch 2 saw v2, no torn reads
```

**Key output:** Per-worker audit logs are internally consistent. Schema versions
seen per worker match expectations (v1 for batch 1, v2 for batch 2). No torn reads.

---

### 4. Session-Scoped State Isolation \[scikit-learn\]

**Requires:** scikit-learn, numpy (auto-installed via Snakepit/uv)

**Free-threaded Python failure:** Shared `_models` dict mutated from multiple threads =
corruption (dict resize during concurrent insert is undefined). `_prediction_count +=
len(items)` is a read-modify-write race. numpy BLAS operations from multiple threads
share internal C buffers not designed for concurrent access.

**Slither solution:** Session affinity routes train+predict to the same OS process.
Each process has its own `_models` dict, its own BLAS context, its own counters.

**Modules:**
- `Slither.Examples.MlScoring.ScoringPipe` -- 4-stage Pipe with concurrent dual-session demo
- `Slither.Examples.MlScoring.FeatureStore` -- write-through ETS feature cache
- `ml_scorer.py` -- train, featurize, predict with session-scoped model storage

**Pipeline:**
```
Phase 1: Train two models on two sessions CONCURRENTLY
  Session A: logistic regression on distribution A (centers [1,2,1,2] / [3,4,3,4])
  Session B: logistic regression on distribution B (centers [0,0,0,0] / [5,5,5,5])
  Both via Task.async -- parallel training

Phase 2: Score 20 test records through BOTH sessions
  Session A predictions use model A
  Session B predictions use model B
  Prediction divergence count proves isolation

Phase 3: Per-session worker stats
  Worker PIDs, model IDs stored, prediction counts

Phase 4: Isolation summary
  "Session A trained model X on worker PID P1"
  "Session B trained model Y on worker PID P2"
  "Under free-threaded Python: shared _models dict = one session overwrites another"
```

**Key output:** Prediction divergence between sessions proves models are different.
Per-worker model lists prove no cross-contamination. Worker PIDs prove process isolation.

---

### 5. Backpressure + Memory Safety \[Pillow\]

**Requires:** Pillow (auto-installed via Snakepit/uv)

**Free-threaded Python failure:** 16 threads each loading a 4K image = 500MB+ memory
spike. No built-in backpressure. Pillow's C internals (libImaging) are not thread-safe --
concurrent `Image.resize(LANCZOS)` can produce corrupted output or segfault. One thread's
segfault kills all threads.

**Slither solution:** WeightedBatch caps concurrent pixel processing. `max_in_flight: 2`
limits simultaneous batches. Each worker is a separate OS process -- Pillow is safe.

**Modules:**
- `Slither.Examples.ImagePipeline.ThumbnailDemo` -- WeightedBatch dispatch with memory stats
- `image_processor.py` -- image generation, thumbnail creation with per-worker memory tracking

**Flow:**
```
30 images (100x100 to 3840x2160, including 5x 4K)
  |
  v
Phase 1: Generate test images [FixedBatch(10)]
  |
  v
Phase 2: Create thumbnails [WeightedBatch(max_weight: 2MP)]
  weight_fn = pixel_count
  max_in_flight = 2  (backpressure)
  on_error = :skip   (crash containment)
  |
  v
Per-worker memory stats: peak MB, images processed
Backpressure analysis: theoretical worst case vs actual peak
```

**Key output:** "Without backpressure: 30 images * avg X = Y MB simultaneous. With
Slither: peak per-worker was Z MB." Per-worker memory watermarks prove bounded usage.

---

## Summary: What Each Example Proves

| # | Example | Free-Threaded Failure | Slither Solution |
|---|---------|----------------------|------------------|
| 1 | Text Analysis | `dict[k] += v` lost updates | Per-process accumulators, beam-side merge |
| 2 | Batch Stats | Worker crash kills all threads | Process isolation, `on_error: :skip` |
| 3 | Data ETL | Torn reads on shared config | ETS atomic reads, schema-in-payload |
| 4 | ML Scoring | Shared `_models` dict corruption | Session affinity, per-process storage |
| 5 | Image Pipeline | No backpressure, Pillow C safety | WeightedBatch, max_in_flight, process isolation |

---

## Architecture

### File Layout

```
lib/slither/examples/
  text_analysis/
    text_pipe.ex          Pipe + per-worker stats merge
    stopword_store.ex     ETS stopword list (Store behaviour)
  batch_stats/
    stats_demo.ex         Dispatch with fault isolation demo
  data_etl/
    etl_pipe.ex           Pipe with hot-reload + audit log demo
    schema_store.ex       Versioned schemas + transform rules
  ml_scoring/
    scoring_pipe.ex       Dual-session concurrent training + scoring
    feature_store.ex      Write-through feature cache
  image_pipeline/
    thumbnail_demo.ex     WeightedBatch with memory backpressure

priv/python/examples/
  text_analyzer.py        Per-worker word index accumulation (stdlib)
  batch_stats.py          Welford's accumulator + poison pill handling (stdlib)
  csv_transformer.py      Validation with per-worker audit log (stdlib)
  ml_scorer.py            Session-scoped model storage + stats (scikit-learn)
  image_processor.py      Memory-tracked image processing (Pillow)
  pure_python_baselines.py  Threaded shared-state equivalents (intentional choke)

lib/mix/tasks/
  slither.example.ex      Mix task runner with Snakepit/uv auto-install
```

### How It Works

1. `mix slither.example <name>` sets `PYTHONPATH`, auto-installs deps via
   `Snakepit.PythonPackages.ensure!`, and starts the OTP application.
2. Each example's `run_demo/0` starts any required Store processes, generates test
   data, runs the pipeline, then collects per-worker statistics.
3. After each run, the mix task executes `pure_python_baselines.py <example>` to
   run a threaded shared-state equivalent and report whether it choked.
4. Python modules accumulate shared mutable state that would race under threading
   but is safe under process isolation.
5. The Elixir side merges per-worker results, demonstrating race-free aggregation.

---

## Troubleshooting

**"Module text_analyzer not found"** -- `PYTHONPATH` was not set. Use `mix slither.example`
(handles this automatically) rather than calling `run_demo/0` from IEx.

**"scikit-learn is not installed"** / **"Pillow not installed"** -- Running via
`mix slither.example <name>` auto-installs through Snakepit's uv-based package manager.
If running `run_demo/0` directly from IEx, call
`Snakepit.PythonPackages.ensure!({:list, ["scikit-learn~=1.3"]})` first.

**"SnakeBridge is not available"** -- Ensure SnakeBridge and Snakepit are configured.
Check `config/config.exs` and run `mix snakebridge.setup` if needed.

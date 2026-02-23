# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-02-22

### Added

- `Slither.Store` — ETS-backed shared state with Python-readable views
- `Slither.Dispatch` — batched fan-out to Python worker pools with backpressure
- `Slither.Pipe` — stage composition over BEAM + Python steps
- Process-isolated Python workers via Snakepit pool management
- `mix slither.example` task for running bundled examples
- Five example modules demonstrating concurrency-safety advantages over free-threaded Python:
  - `text_analysis` — shared accumulator race detection
  - `batch_stats` — fault isolation with poison-pill recovery
  - `data_etl` — hot-reload torn-read prevention via ETS atomic writes
  - `ml_scoring` — session-scoped state isolation
  - `image_pipeline` — backpressure and memory safety
- Getting Started, Architecture, and per-module documentation guides
- Telemetry integration for dispatch and store operations

[0.1.0]: https://github.com/nshkrdotcom/slither/releases/tag/v0.1.0

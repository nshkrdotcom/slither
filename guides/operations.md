# Operations And Troubleshooting

## Runtime Checklist

- Snakepit pool configured in `config/runtime.exs`
- Python runtime available (`python3` or configured executable)
- `PYTHONPATH` includes `priv/python/examples` for demo modules

## Common Commands

```bash
mix compile
mix test
mix docs
mix slither.example --all
```

## Common Problems

### Python module not found

Symptom:

- `Module text_analyzer not found`

Fix:

- Run through `mix slither.example ...` so `PYTHONPATH` is set.

### Missing optional Python deps

Symptoms:

- scikit-learn/Pillow import errors in optional demos.

Fix:

- Run `mix slither.example ml_scoring` or `mix slither.example image_pipeline`.
- The task auto-installs required packages via Snakepit.

### SnakeBridge unavailable

Symptoms:

- runtime call failures during Python stage execution.

Fix:

- Verify `SnakeBridge.ConfigHelper.configure_snakepit!/1` is called in runtime config.
- Ensure Python executable and venv are valid.

## Observability

Slither emits telemetry spans for:

- pipe run and per-stage execution
- dispatch batch execution
- store writes/reloads

Use telemetry handlers in your app to export metrics/traces.

## Production Notes

- Tune `batch_size` and `max_in_flight` by workload profile.
- Use `WeightedBatch` for memory-sensitive payloads.
- Prefer `on_error: :skip` only when partial success is acceptable.
- Keep shared mutable state on BEAM side when possible.

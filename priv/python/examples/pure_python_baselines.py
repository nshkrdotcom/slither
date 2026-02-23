#!/usr/bin/env python3
"""
Pure-Python threaded baselines for Slither examples.

Each baseline shares mutable state across threads WITHOUT locks to demonstrate
corruption under free-threaded Python (PEP 703). No artificial sleeps or yields —
races come from real concurrent access on real threads.

Designed for an i9-12900KS (24 threads) with 48 threads to match Slither's
worker count (2x oversubscription).

Usage:
    python3.14t pure_python_baselines.py text_analysis
    python3.14t pure_python_baselines.py --all
    python3.14t pure_python_baselines.py text_analysis --threads 48 --rounds 5
"""

from __future__ import annotations

import argparse
import concurrent.futures
import math
import random
import sysconfig
import threading
import time
from collections import Counter


# ---------------------------------------------------------------------------
# Free-threaded Python detection
# ---------------------------------------------------------------------------

def _is_free_threaded() -> bool:
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def _print_gil_status() -> None:
    if _is_free_threaded():
        print("  Python: free-threaded (GIL disabled) -- races will manifest naturally")
    else:
        print("  WARNING: Running under GIL -- races may not manifest")
        print("  Install free-threaded Python: uv python install cpython-3.14.0+freethreaded")


# ---------------------------------------------------------------------------
# text_analysis baseline — 5,000 docs, 48 threads, 5 rounds
#
# Race: dict[key] += count from 48 threads = lost updates on shared_index
#       shared_doc_count read-modify-write = lost increments
# ---------------------------------------------------------------------------

TEXT_VOCAB = [
    "good", "bad", "excellent", "terrible", "service", "quality",
    "today", "result", "happy", "sad", "amazing", "awful",
    "fast", "slow", "reliable", "broken", "love", "hate",
    "recommend", "avoid",
]


def _text_analysis_baseline(threads: int = 48, rounds: int = 5) -> tuple[bool, list[str]]:
    random.seed(42)
    num_docs = 5000

    docs = [
        [random.choice(TEXT_VOCAB) for _ in range(random.randint(20, 60))]
        for _ in range(num_docs)
    ]

    shared_index: dict[str, int] = {}
    shared_doc_count = 0

    def worker(chunk: list[list[str]]) -> None:
        nonlocal shared_doc_count
        for _ in range(rounds):
            for words in chunk:
                local = Counter(words)
                for word, count in local.items():
                    # Race: read-modify-write on shared dict
                    current = shared_index.get(word, 0)
                    shared_index[word] = current + count
                # Race: read-modify-write on shared counter
                current = shared_doc_count
                shared_doc_count = current + 1

    chunks = [docs[i::threads] for i in range(threads)]

    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, chunk) for chunk in chunks]
        for future in futures:
            future.result()
    elapsed = time.monotonic() - t0

    # Ground truth
    expected_counter: Counter[str] = Counter()
    for _ in range(rounds):
        for words in docs:
            expected_counter.update(words)

    expected_docs = num_docs * rounds
    observed_docs = shared_doc_count
    expected_total = sum(expected_counter.values())
    observed_total = sum(shared_index.values())
    lost_docs = expected_docs - observed_docs
    lost_words = expected_total - observed_total

    corrupted = observed_docs != expected_docs or observed_total != expected_total

    details = [
        f"docs: expected={expected_docs} observed={observed_docs} lost={lost_docs} ({_pct(lost_docs, expected_docs)}%)",
        f"words: expected={expected_total} observed={observed_total} lost={lost_words} ({_pct(lost_words, expected_total)}%)",
        f"vocab: expected={len(expected_counter)} observed={len(shared_index)}",
        f"threads={threads} rounds={rounds} elapsed={elapsed:.2f}s",
    ]

    return corrupted, details


# ---------------------------------------------------------------------------
# batch_stats baseline — 2,000 datasets, 48 threads, 3 rounds
#
# Race: Welford online accumulator (n/mean/m2) has multi-step
#       read-modify-write — interleaving corrupts all three.
# ---------------------------------------------------------------------------

def _batch_stats_baseline(threads: int = 48, rounds: int = 3) -> tuple[bool, list[str]]:
    random.seed(137)
    num_datasets = 2000

    datasets = [
        [random.uniform(1.0, 1000.0) for _ in range(random.randint(5, 100))]
        for _ in range(num_datasets)
    ]

    # Shared Welford accumulator — textbook race target
    running = {"n": 0, "mean": 0.0, "m2": 0.0}

    def worker(chunk: list[list[float]]) -> None:
        for _ in range(rounds):
            for values in chunk:
                for x in values:
                    # Multi-step read-modify-write: n, mean, m2
                    n0 = running["n"]
                    n1 = n0 + 1
                    running["n"] = n1

                    mean0 = running["mean"]
                    delta = x - mean0
                    mean1 = mean0 + delta / n1
                    running["mean"] = mean1

                    m20 = running["m2"]
                    delta2 = x - mean1
                    running["m2"] = m20 + delta * delta2

    chunks = [datasets[i::threads] for i in range(threads)]

    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, chunk) for chunk in chunks]
        for future in futures:
            future.result()
    elapsed = time.monotonic() - t0

    # Ground truth
    all_values: list[float] = []
    for _ in range(rounds):
        for values in datasets:
            all_values.extend(values)

    expected_n = len(all_values)
    expected_mean = sum(all_values) / expected_n
    expected_var = sum((x - expected_mean) ** 2 for x in all_values) / expected_n

    observed_n = running["n"]
    observed_mean = running["mean"]
    observed_var = running["m2"] / max(observed_n, 1)

    n_diff = expected_n - observed_n
    mean_diff = abs(expected_mean - observed_mean)
    var_diff = abs(expected_var - observed_var)

    corrupted = observed_n != expected_n or mean_diff > 1e-6 or var_diff > 1e-6

    details = [
        f"n: expected={expected_n} observed={observed_n} lost={n_diff} ({_pct(n_diff, expected_n)}%)",
        f"mean: expected={expected_mean:.4f} observed={observed_mean:.4f} diff={mean_diff:.4f}",
        f"variance: expected={expected_var:.4f} observed={observed_var:.4f} diff={var_diff:.4f}",
        f"threads={threads} rounds={rounds} elapsed={elapsed:.2f}s",
    ]

    return corrupted, details


# ---------------------------------------------------------------------------
# data_etl baseline — 10,000 rows, 48 threads, hot-reload thread
#
# Race: list.append() is NOT atomic without GIL. Torn reads on shared schema
#       dict while hot-reload thread swaps fields. validation_count lost increments.
# ---------------------------------------------------------------------------

def _data_etl_baseline(threads: int = 48, rounds: int = 1) -> tuple[bool, list[str]]:
    schema = {
        "version": 1, "age_min": 0, "age_max": 150,
        "name_min": 1, "email_pattern": None,
    }
    schema_v1 = dict(schema)
    schema_v2 = {
        "version": 2, "age_min": 18, "age_max": 120,
        "name_min": 2, "email_pattern": "@",
    }

    stop_event = threading.Event()
    num_rows = 10000

    rows = [
        {"name": f"User{i}", "age": 25 + (i % 50), "email": f"u{i}@example.com"}
        for i in range(num_rows)
    ]

    torn_reads = 0
    audit_log: list[dict] = []
    validation_count = 0

    def hot_reload_loop() -> None:
        toggle = False
        while not stop_event.is_set():
            target = schema_v2 if toggle else schema_v1
            # Non-atomic multi-field update — readers see partial state
            for key in ("version", "age_min", "age_max", "name_min", "email_pattern"):
                schema[key] = target[key]
            toggle = not toggle

    def validate_worker(chunk: list[dict]) -> None:
        nonlocal torn_reads, validation_count
        for _ in range(rounds):
            for row in chunk:
                # Read schema fields — may get mix of v1 and v2
                version = schema["version"]
                age_min = schema["age_min"]
                age_max = schema["age_max"]
                name_min = schema["name_min"]
                email_pattern = schema["email_pattern"]

                torn = False
                if version == 1:
                    torn = (age_min, age_max, name_min, email_pattern) != (0, 150, 1, None)
                elif version == 2:
                    torn = (age_min, age_max, name_min, email_pattern) != (18, 120, 2, "@")

                if torn:
                    # Race: read-modify-write on torn_reads counter
                    current = torn_reads
                    torn_reads = current + 1

                # Race: list.append not atomic without GIL
                audit_log.append({"version": version, "ok": not torn, "row_id": row.get("name")})
                # Race: read-modify-write on counter
                current = validation_count
                validation_count = current + 1

    reloader = threading.Thread(target=hot_reload_loop, daemon=True)
    reloader.start()

    chunks = [rows[i::threads] for i in range(threads)]

    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(validate_worker, chunk) for chunk in chunks]
        for future in futures:
            future.result()
    elapsed = time.monotonic() - t0

    stop_event.set()
    reloader.join(timeout=2.0)

    expected_validations = num_rows * rounds
    audit_size = len(audit_log)
    count_diff = expected_validations - validation_count
    log_diff = expected_validations - audit_size

    corrupted = torn_reads > 0 or validation_count != expected_validations or audit_size != expected_validations

    details = [
        f"torn_reads={torn_reads}",
        f"validations: expected={expected_validations} observed={validation_count} lost={count_diff} ({_pct(count_diff, expected_validations)}%)",
        f"audit_log: expected={expected_validations} observed={audit_size} lost={log_diff} ({_pct(log_diff, expected_validations)}%)",
        f"threads={threads} rounds={rounds} elapsed={elapsed:.2f}s",
    ]

    return corrupted, details


# ---------------------------------------------------------------------------
# ml_scoring baseline — 2 sessions, shared models dict, 48 predict threads
#
# Race: Two sessions train into shared models["active"] — last writer wins.
#       48 predict threads read from corrupted shared state.
#       _prediction_count read-modify-write race.
# ---------------------------------------------------------------------------

def _ml_scoring_baseline(threads: int = 48, rounds: int = 1) -> tuple[bool, list[str]]:
    random.seed(42)

    # Shared model storage — no session isolation
    models: dict[str, dict] = {}
    prediction_count = 0

    num_train = 2000
    num_test = 2000

    # Generate training data for two sessions with different distributions
    train_a = [(random.gauss(2.0, 1.0), random.gauss(2.0, 1.0)) for _ in range(num_train)]
    train_b = [(random.gauss(7.0, 1.0), random.gauss(7.0, 1.0)) for _ in range(num_train)]

    def train_model(session_id: str, data: list[tuple[float, float]], center: float) -> None:
        # Simulate training by computing a simple threshold model
        model = {
            "session": session_id,
            "center": center,
            "n_samples": len(data),
            "mean_x": sum(d[0] for d in data) / len(data),
            "mean_y": sum(d[1] for d in data) / len(data),
        }
        # Race: both sessions write to same key
        models["active"] = model

    def predict_worker(test_data: list[tuple[float, float]]) -> list[int]:
        nonlocal prediction_count
        results = []
        for x, y in test_data:
            model = models.get("active", {})
            center = model.get("center", 5.0)
            pred = 1 if (x + y) / 2 >= center else 0
            results.append(pred)
            # Race: read-modify-write
            current = prediction_count
            prediction_count = current + 1
        return results

    test_data = [(random.gauss(5.0, 3.0), random.gauss(5.0, 3.0)) for _ in range(num_test)]

    # Train concurrently — race on models["active"]
    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fa = pool.submit(train_model, "session_a", train_a, 2.0)
        fb = pool.submit(train_model, "session_b", train_b, 7.0)
        fa.result()
        fb.result()

    # What would isolated predictions look like?
    expected_a = [1 if (x + y) / 2 >= 2.0 else 0 for x, y in test_data]
    expected_b = [1 if (x + y) / 2 >= 7.0 else 0 for x, y in test_data]
    expected_divergence = sum(1 for a, b in zip(expected_a, expected_b) if a != b)

    # Predict from shared (corrupted) state — all threads see same model
    chunks = [test_data[i::threads] for i in range(threads)]
    all_preds: list[int] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(predict_worker, chunk) for chunk in chunks]
        for future in futures:
            all_preds.extend(future.result())
    elapsed = time.monotonic() - t0

    # Since both sessions wrote to same key, predictions are identical
    observed_divergence = 0  # all preds use same model
    shared_model = models.get("active", {})
    expected_pred_count = num_test
    lost_preds = expected_pred_count - prediction_count

    corrupted = observed_divergence < expected_divergence or prediction_count != expected_pred_count

    details = [
        f"session_isolation: expected_divergence={expected_divergence} observed_divergence={observed_divergence}",
        f"shared_model_owner={shared_model.get('session', '?')} center={shared_model.get('center', '?')}",
        f"predictions: expected={expected_pred_count} observed={prediction_count} lost={lost_preds} ({_pct(lost_preds, expected_pred_count)}%)",
        f"train_samples={num_train}x2 test_records={num_test} threads={threads} elapsed={elapsed:.2f}s",
    ]

    return corrupted, details


# ---------------------------------------------------------------------------
# image_pipeline baseline — 200 images, 48 threads, no backpressure
#
# Race: in_flight / peak tracking via read-modify-write.
#       No backpressure = all 200 images in-flight simultaneously.
# ---------------------------------------------------------------------------

def _image_pipeline_baseline(threads: int = 48, rounds: int = 1) -> tuple[bool, list[str]]:
    # 200 images: 50 small, 80 medium, 50 large (1080p), 20 XL (4K)
    image_specs: list[int] = []
    image_specs.extend([200 * 150] * 50)     # small
    image_specs.extend([800 * 600] * 80)     # medium
    image_specs.extend([1920 * 1080] * 50)   # large (1080p)
    image_specs.extend([3840 * 2160] * 20)   # XL (4K)

    in_flight = 0
    peak_in_flight = 0
    current_bytes = 0
    peak_bytes = 0

    def process_image(pixel_count: int) -> int:
        nonlocal in_flight, peak_in_flight, current_bytes, peak_bytes

        # Allocate memory proportional to image size (3 bytes/pixel for RGB)
        mem_size = pixel_count * 3
        buf = bytearray(mem_size)

        # Race: read-modify-write on all counters
        current_in = in_flight
        in_flight = current_in + 1
        current_b = current_bytes
        current_bytes = current_b + mem_size
        peak_in = peak_in_flight
        peak_in_flight = max(peak_in, in_flight)
        peak_b = peak_bytes
        peak_bytes = max(peak_b, current_bytes)

        # Simulate resize work — iterate over pixels to create a real
        # contention window (threads stay in-flight longer)
        thumb_size = max(1, pixel_count // 64)
        thumb = bytearray(thumb_size * 3)
        # Downsample: strided copy from source buffer
        stride = max(1, len(buf) // len(thumb))
        for i in range(len(thumb)):
            thumb[i] = buf[i * stride % len(buf)]

        # Release
        current_in = in_flight
        in_flight = current_in - 1
        current_b = current_bytes
        current_bytes = current_b - mem_size

        del buf
        return thumb_size

    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(process_image, px) for px in image_specs]
        for future in futures:
            future.result()
    elapsed = time.monotonic() - t0

    total_pixels = sum(image_specs)
    megapixels = total_pixels / 1_000_000
    bounded_in_flight_target = 4
    bounded_memory_mb = 50.0
    peak_mb = peak_bytes / (1024 * 1024)

    corrupted = peak_in_flight > bounded_in_flight_target or peak_mb > bounded_memory_mb

    details = [
        f"images={len(image_specs)} total_megapixels={megapixels:.1f}",
        f"peak_in_flight={peak_in_flight} (bounded_target={bounded_in_flight_target})",
        f"peak_memory={peak_mb:.1f}MB (bounded_target={bounded_memory_mb:.1f}MB)",
        f"threads={threads} elapsed={elapsed:.2f}s",
    ]

    return corrupted, details


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(diff: int, total: int) -> str:
    if total == 0:
        return "0.0"
    return f"{abs(diff) / total * 100:.1f}"


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

RUNNERS = {
    "text_analysis": _text_analysis_baseline,
    "batch_stats": _batch_stats_baseline,
    "data_etl": _data_etl_baseline,
    "ml_scoring": _ml_scoring_baseline,
    "image_pipeline": _image_pipeline_baseline,
}


def _run_one(name: str, threads: int, rounds: int | None) -> int:
    if name not in RUNNERS:
        print(f"Unknown baseline target: {name}")
        return 2

    _print_gil_status()

    fn = RUNNERS[name]
    kwargs: dict = {"threads": threads}
    if rounds is not None:
        kwargs["rounds"] = rounds

    corrupted, details = fn(**kwargs)

    print(f"\n=== Pure Python Baseline: {name} ===")
    for line in details:
        print(f"  {line}")

    if corrupted:
        print("  outcome=CORRUPTED (expected under free-threaded Python)")
    else:
        print("  outcome=CLEAN (races did not manifest this run)")

    return corrupted


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run pure-Python threaded baselines (no locks, no sleeps)"
    )
    parser.add_argument(
        "example",
        nargs="?",
        choices=sorted(RUNNERS.keys()),
        help="Which baseline to run",
    )
    parser.add_argument("--all", action="store_true", help="Run all baselines")
    parser.add_argument("--threads", type=int, default=48, help="Thread count (default: 48)")
    parser.add_argument("--rounds", type=int, default=None, help="Override round count")

    args = parser.parse_args()

    if args.all:
        results = []
        for name in sorted(RUNNERS.keys()):
            was_corrupted = _run_one(name, args.threads, args.rounds)
            results.append((name, was_corrupted))
            print()

        print("=== Summary ===")
        for name, was_corrupted in results:
            status = "CORRUPTED" if was_corrupted else "CLEAN"
            print(f"  {name}: {status}")
        return 0

    if args.example is None:
        parser.print_help()
        return 2

    _run_one(args.example, args.threads, args.rounds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

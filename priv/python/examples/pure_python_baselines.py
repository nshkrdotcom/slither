#!/usr/bin/env python3
"""
Pure-Python threaded baselines for Slither examples.

Each baseline intentionally shares mutable state across threads without locks,
so it can demonstrate corruption or cross-contamination under concurrency.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import random
import threading
import time
from collections import Counter


def _yield_thread():
    # Encourage context switches at racy read-modify-write points.
    time.sleep(0)


# ---------------------------------------------------------------------------
# text_analysis baseline
# ---------------------------------------------------------------------------

TEXT_VOCAB = [
    "good",
    "bad",
    "excellent",
    "terrible",
    "service",
    "quality",
    "today",
    "result",
]


def _text_analysis_baseline() -> tuple[bool, list[str]]:
    random.seed(42)

    docs = [
        [random.choice(TEXT_VOCAB) for _ in range(32)]
        for _ in range(80)
    ]
    rounds = 20

    shared_index = {}
    shared_doc_count = 0

    def racy_inc_doc_count() -> None:
        nonlocal shared_doc_count
        current = shared_doc_count
        _yield_thread()
        shared_doc_count = current + 1

    def racy_add_word(word: str, delta: int) -> None:
        current = shared_index.get(word, 0)
        _yield_thread()
        shared_index[word] = current + delta

    def worker(chunk: list[list[str]]) -> None:
        for _ in range(rounds):
            for words in chunk:
                local = Counter(words)
                for word, count in local.items():
                    racy_add_word(word, count)
                racy_inc_doc_count()

    chunks = [docs[i::8] for i in range(8)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker, chunk) for chunk in chunks]
        for future in futures:
            future.result()

    expected_counter = Counter()
    for _ in range(rounds):
        for words in docs:
            expected_counter.update(words)

    expected_docs = len(docs) * rounds
    observed_docs = shared_doc_count
    expected_total = sum(expected_counter.values())
    observed_total = sum(shared_index.values())

    choked = observed_docs != expected_docs or observed_total != expected_total

    details = [
        f"expected_docs={expected_docs} observed_docs={observed_docs}",
        f"expected_total_words={expected_total} observed_total_words={observed_total}",
        f"distinct_words_expected={len(expected_counter)} observed={len(shared_index)}",
    ]

    return choked, details


# ---------------------------------------------------------------------------
# batch_stats baseline
# ---------------------------------------------------------------------------


def _batch_stats_baseline() -> tuple[bool, list[str]]:
    random.seed(137)

    datasets = [
        [random.uniform(1.0, 1000.0) for _ in range(20)]
        for _ in range(24)
    ]
    rounds = 16

    running = {"n": 0, "mean": 0.0, "m2": 0.0}

    def racy_welford(x: float) -> None:
        n0 = running["n"]
        _yield_thread()
        n1 = n0 + 1
        running["n"] = n1

        mean0 = running["mean"]
        delta = x - mean0
        _yield_thread()
        mean1 = mean0 + delta / n1
        running["mean"] = mean1

        m20 = running["m2"]
        delta2 = x - mean1
        _yield_thread()
        running["m2"] = m20 + delta * delta2

    def worker(chunk: list[list[float]]) -> None:
        for _ in range(rounds):
            for values in chunk:
                for x in values:
                    racy_welford(x)

    chunks = [datasets[i::6] for i in range(6)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(worker, chunk) for chunk in chunks]
        for future in futures:
            future.result()

    # Ground truth (single-threaded)
    all_values = []
    for _ in range(rounds):
        for values in datasets:
            all_values.extend(values)

    expected_n = len(all_values)
    expected_mean = sum(all_values) / expected_n

    observed_n = running["n"]
    observed_mean = running["mean"]

    mean_diff = abs(expected_mean - observed_mean)

    choked = observed_n != expected_n or mean_diff > 1e-6

    details = [
        f"expected_n={expected_n} observed_n={observed_n}",
        f"expected_mean={expected_mean:.6f} observed_mean={observed_mean:.6f}",
        f"mean_abs_diff={mean_diff:.6f}",
    ]

    return choked, details


# ---------------------------------------------------------------------------
# data_etl baseline
# ---------------------------------------------------------------------------


def _data_etl_baseline() -> tuple[bool, list[str]]:
    schema = {
        "version": 1,
        "age_min": 0,
        "age_max": 150,
        "name_min": 1,
        "email_pattern": None,
    }

    schema_v1 = {
        "version": 1,
        "age_min": 0,
        "age_max": 150,
        "name_min": 1,
        "email_pattern": None,
    }

    schema_v2 = {
        "version": 2,
        "age_min": 18,
        "age_max": 120,
        "name_min": 2,
        "email_pattern": "@",
    }

    stop_event = threading.Event()

    torn_reads = 0
    audit_log = []
    validation_count = 0

    def hot_reload_loop() -> None:
        toggle = False
        while not stop_event.is_set():
            target = schema_v2 if toggle else schema_v1
            schema["version"] = target["version"]
            _yield_thread()
            schema["age_min"] = target["age_min"]
            _yield_thread()
            schema["age_max"] = target["age_max"]
            _yield_thread()
            schema["name_min"] = target["name_min"]
            _yield_thread()
            schema["email_pattern"] = target["email_pattern"]
            toggle = not toggle

    rows = [
        {"name": "User", "age": 25, "email": "u@example.com"}
        for _ in range(600)
    ]

    def validate_worker(chunk: list[dict]) -> None:
        nonlocal torn_reads, validation_count

        for row in chunk:
            version = schema["version"]
            _yield_thread()
            age_min = schema["age_min"]
            _yield_thread()
            age_max = schema["age_max"]
            _yield_thread()
            name_min = schema["name_min"]
            _yield_thread()
            email_pattern = schema["email_pattern"]

            torn = False
            if version == 1:
                torn = (age_min, age_max, name_min, email_pattern) != (0, 150, 1, None)
            elif version == 2:
                torn = (age_min, age_max, name_min, email_pattern) != (18, 120, 2, "@")

            if torn:
                current = torn_reads
                _yield_thread()
                torn_reads = current + 1

            # Racy audit accounting.
            audit_log.append({"version": version, "ok": not torn, "row": row})
            current = validation_count
            _yield_thread()
            validation_count = current + 1

    reloader = threading.Thread(target=hot_reload_loop, daemon=True)
    reloader.start()

    chunks = [rows[i::8] for i in range(8)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(validate_worker, chunk) for chunk in chunks]
        for future in futures:
            future.result()

    stop_event.set()
    reloader.join(timeout=1.0)

    audit_size = len(audit_log)
    choked = torn_reads > 0 or validation_count != audit_size

    details = [
        f"torn_reads_detected={torn_reads}",
        f"validation_count={validation_count} audit_log_size={audit_size}",
    ]

    return choked, details


# ---------------------------------------------------------------------------
# ml_scoring baseline
# ---------------------------------------------------------------------------


def _ml_scoring_baseline() -> tuple[bool, list[str]]:
    # Naive global model storage with no session isolation.
    models = {}

    def train_model(session_id: str, center: float) -> None:
        model = {"session": session_id, "center": center}
        # Intentional bug: both sessions overwrite the same key.
        _yield_thread()
        models["active_model"] = model

    def predict(center: float, values: list[float]) -> list[int]:
        return [1 if v >= center else 0 for v in values]

    records = [i / 10.0 for i in range(0, 100)]

    # Isolated expectation: two different models should diverge.
    expected_a = predict(2.0, records)
    expected_b = predict(7.0, records)
    expected_divergence = sum(1 for a, b in zip(expected_a, expected_b) if a != b)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(train_model, "session_a", 2.0)
        future_b = pool.submit(train_model, "session_b", 7.0)
        future_a.result()
        future_b.result()

    shared = models.get("active_model")
    observed_a = predict(shared["center"], records)
    observed_b = predict(shared["center"], records)
    observed_divergence = sum(1 for a, b in zip(observed_a, observed_b) if a != b)

    choked = observed_divergence < expected_divergence

    details = [
        f"expected_divergence={expected_divergence}",
        f"observed_divergence={observed_divergence}",
        f"shared_model_owner={shared['session']} center={shared['center']}",
    ]

    return choked, details


# ---------------------------------------------------------------------------
# image_pipeline baseline
# ---------------------------------------------------------------------------


def _image_pipeline_baseline() -> tuple[bool, list[str]]:
    # Approximate pixel payload sizes (1 byte per pixel to keep memory bounded).
    pixel_counts = [
        100 * 100,
        320 * 240,
        640 * 480,
        800 * 600,
        1024 * 768,
        1280 * 720,
        1920 * 1080,
        2560 * 1440,
        3840 * 2160,
    ]

    # Duplicate larger images to create contention like the Slither demo.
    pixel_counts = pixel_counts + pixel_counts[-5:] + pixel_counts[-5:] + pixel_counts[-3:]

    stats_lock = threading.Lock()
    in_flight = 0
    peak_in_flight = 0
    current_bytes = 0
    peak_bytes = 0

    def process_image(pixel_count: int) -> int:
        nonlocal in_flight, peak_in_flight, current_bytes, peak_bytes

        buf = bytearray(pixel_count)

        with stats_lock:
            in_flight += 1
            current_bytes += len(buf)
            peak_in_flight = max(peak_in_flight, in_flight)
            peak_bytes = max(peak_bytes, current_bytes)

        # Simulate expensive resize work while memory is retained.
        time.sleep(0.03)
        thumb_size = max(1, pixel_count // 64)
        _ = bytes(thumb_size)
        time.sleep(0.005)

        with stats_lock:
            in_flight -= 1
            current_bytes -= len(buf)

        return thumb_size

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(process_image, px) for px in pixel_counts]
        for future in futures:
            future.result()

    bounded_in_flight_target = 2
    bounded_memory_target = 20 * 1024 * 1024

    choked = peak_in_flight > bounded_in_flight_target and peak_bytes > bounded_memory_target

    details = [
        f"peak_in_flight={peak_in_flight} (bounded_target={bounded_in_flight_target})",
        f"peak_memory_mb={peak_bytes / (1024 * 1024):.2f} (bounded_target_mb={bounded_memory_target / (1024 * 1024):.2f})",
    ]

    return choked, details


RUNNERS = {
    "text_analysis": _text_analysis_baseline,
    "batch_stats": _batch_stats_baseline,
    "data_etl": _data_etl_baseline,
    "ml_scoring": _ml_scoring_baseline,
    "image_pipeline": _image_pipeline_baseline,
}


def _run_one(name: str) -> int:
    if name not in RUNNERS:
        print(f"Unknown baseline target: {name}")
        return 2

    choked, details = RUNNERS[name]()

    print(f"=== Pure Python Baseline: {name} ===")
    for line in details:
        print(f"  {line}")

    if choked:
        print("  outcome=CHOKED (expected)")
        return 0

    print("  outcome=NOT_CHOKED (unexpected)")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pure-Python threaded baseline equivalents")
    parser.add_argument("example", choices=sorted(RUNNERS.keys()))
    args = parser.parse_args()
    return _run_one(args.example)


if __name__ == "__main__":
    raise SystemExit(main())

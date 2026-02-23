#!/usr/bin/env python3
"""
Pure-Python comparison harness for Slither examples.

Each example is implemented in two modes:

1) unsafe_threaded
   Intentionally naive threaded/shared-state approach.

2) safe_python
   Pure-Python solution with explicit synchronization and/or bounded
   concurrency so correctness invariants hold under free-threaded Python.

The goal is long-term, reproducible, apples-to-apples comparisons:
- same core workload shape as each Slither demo
- explicit invariant checks
- no hardcoded outcomes
- explicit interpreter mode reporting
"""

from __future__ import annotations

import argparse
import concurrent.futures
import math
import random
import sys
import sysconfig
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable


# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


def _is_free_threaded() -> bool:
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def _python_mode_label() -> str:
    return "free-threaded" if _is_free_threaded() else "gil"


def _pct(diff: int, total: int) -> str:
    if total == 0:
        return "0.0"
    return f"{abs(diff) / total * 100:.1f}"


def _run_timed(fn: Callable[[], tuple[bool, list[str]]]) -> tuple[bool, list[str], float]:
    t0 = time.monotonic()
    ok, details = fn()
    return ok, details, time.monotonic() - t0


def _chunk_strided(items: list, workers: int) -> list[list]:
    if workers <= 0:
        raise ValueError("workers must be positive")
    return [items[i::workers] for i in range(workers)]


# ---------------------------------------------------------------------------
# Shared result model
# ---------------------------------------------------------------------------


@dataclass
class ModeResult:
    mode: str
    ok: bool
    details: list[str]
    elapsed_s: float


# ---------------------------------------------------------------------------
# Example 1: text_analysis (5,000 docs)
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
    "happy",
    "sad",
    "amazing",
    "awful",
    "fast",
    "slow",
    "reliable",
    "broken",
    "love",
    "hate",
    "recommend",
    "avoid",
    "product",
    "team",
    "experience",
    "delivery",
    "price",
    "support",
    "feedback",
    "performance",
    "design",
    "usability",
    "innovation",
    "problem",
    "solution",
    "customer",
    "value",
    "improve",
]


def _build_text_docs(seed: int = 42, num_docs: int = 5000) -> list[list[str]]:
    rng = random.Random(seed)
    docs: list[list[str]] = []

    for _ in range(num_docs):
        word_count = 15 + rng.randint(1, 50)  # 16..65, same shape as Elixir demo
        docs.append([rng.choice(TEXT_VOCAB) for _ in range(word_count)])

    return docs


def _text_analysis_unsafe(threads: int) -> tuple[bool, list[str]]:
    docs = _build_text_docs()

    expected_counter: Counter[str] = Counter()
    for words in docs:
        expected_counter.update(words)

    expected_docs = len(docs)
    expected_words = sum(expected_counter.values())

    shared_index: dict[str, int] = {}
    shared_doc_count = 0

    def worker(chunk: list[list[str]]) -> None:
        nonlocal shared_doc_count
        for words in chunk:
            local = Counter(words)
            for word, count in local.items():
                # Intentional read-modify-write race in unsafe mode
                current = shared_index.get(word, 0)
                shared_index[word] = current + count

            # Intentional read-modify-write race in unsafe mode
            current = shared_doc_count
            shared_doc_count = current + 1

    chunks = _chunk_strided(docs, threads)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, chunk) for chunk in chunks]
        for fut in futures:
            fut.result()

    observed_docs = shared_doc_count
    observed_words = sum(shared_index.values())
    lost_docs = expected_docs - observed_docs
    lost_words = expected_words - observed_words

    ok = (
        observed_docs == expected_docs
        and observed_words == expected_words
        and len(shared_index) == len(expected_counter)
    )

    details = [
        f"docs: expected={expected_docs} observed={observed_docs} lost={lost_docs} ({_pct(lost_docs, expected_docs)}%)",
        f"words: expected={expected_words} observed={observed_words} lost={lost_words} ({_pct(lost_words, expected_words)}%)",
        f"vocab: expected={len(expected_counter)} observed={len(shared_index)}",
        "hazard: lock-free shared dict/counter updates",
    ]
    return ok, details


def _text_analysis_safe(threads: int) -> tuple[bool, list[str]]:
    docs = _build_text_docs()

    expected_counter: Counter[str] = Counter()
    for words in docs:
        expected_counter.update(words)

    expected_docs = len(docs)
    expected_words = sum(expected_counter.values())

    def worker(chunk: list[list[str]]) -> tuple[Counter[str], int]:
        local_counter: Counter[str] = Counter()
        local_docs = 0

        for words in chunk:
            local_counter.update(words)
            local_docs += 1

        return local_counter, local_docs

    chunks = _chunk_strided(docs, threads)

    merged_counter: Counter[str] = Counter()
    merged_docs = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, chunk) for chunk in chunks]
        for fut in futures:
            local_counter, local_docs = fut.result()
            merged_counter.update(local_counter)
            merged_docs += local_docs

    observed_docs = merged_docs
    observed_words = sum(merged_counter.values())
    lost_docs = expected_docs - observed_docs
    lost_words = expected_words - observed_words

    ok = (
        observed_docs == expected_docs
        and observed_words == expected_words
        and merged_counter == expected_counter
    )

    details = [
        f"docs: expected={expected_docs} observed={observed_docs} lost={lost_docs} ({_pct(lost_docs, expected_docs)}%)",
        f"words: expected={expected_words} observed={observed_words} lost={lost_words} ({_pct(lost_words, expected_words)}%)",
        f"vocab: expected={len(expected_counter)} observed={len(merged_counter)}",
        "solution: thread-local Counters merged deterministically",
    ]
    return ok, details


# ---------------------------------------------------------------------------
# Example 2: batch_stats (2,000 datasets + 50 poison pills)
# ---------------------------------------------------------------------------


@dataclass
class RunningStats:
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, value: float) -> None:
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def merge(self, other: "RunningStats") -> None:
        if other.n == 0:
            return
        if self.n == 0:
            self.n = other.n
            self.mean = other.mean
            self.m2 = other.m2
            return

        delta = other.mean - self.mean
        total_n = self.n + other.n

        self.m2 = self.m2 + other.m2 + delta * delta * self.n * other.n / total_n
        self.mean = (self.mean * self.n + other.mean * other.n) / total_n
        self.n = total_n

    @property
    def variance(self) -> float:
        if self.n == 0:
            return 0.0
        return self.m2 / self.n


def _generate_batch_datasets(seed: int = 137) -> list[dict]:
    rng = random.Random(seed)

    def make_dataset(data_type: str, min_size: int, max_size: int) -> dict:
        size = rng.randint(min_size, max_size)
        values = [rng.uniform(1.0, 1000.0) for _ in range(size)]
        return {"values": values, "data_type": data_type}

    datasets: list[dict] = []
    datasets.extend(make_dataset("small", 5, 10) for _ in range(800))
    datasets.extend(make_dataset("medium", 50, 100) for _ in range(800))
    datasets.extend(make_dataset("large", 200, 500) for _ in range(350))

    # Same poison shape as Slither demo (20 nil, 20 NaN-string, 10 empty)
    datasets.extend({"values": [1.0, 2.0, None, 4.0, None, 6.0], "data_type": "poison"} for _ in range(20))
    datasets.extend({"values": [10.0, "NaN", 30.0, "NaN", 50.0], "data_type": "poison"} for _ in range(20))
    datasets.extend({"values": [], "data_type": "poison"} for _ in range(10))

    rng.shuffle(datasets)
    return datasets


def _numeric_values_or_none(values: list) -> list[float] | None:
    if not values:
        return None

    cleaned: list[float] = []

    for value in values:
        if value is None:
            return None
        if isinstance(value, str):
            return None
        if not isinstance(value, (int, float)):
            return None
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        cleaned.append(float(value))

    return cleaned


def _expected_batch_stats(datasets: list[dict]) -> tuple[RunningStats, int, int]:
    stats = RunningStats()
    skipped = 0
    processed = 0

    for dataset in datasets:
        values = _numeric_values_or_none(dataset.get("values", []))
        if values is None:
            skipped += 1
            continue

        processed += 1
        for x in values:
            stats.update(x)

    return stats, skipped, processed


def _batch_stats_unsafe(threads: int) -> tuple[bool, list[str]]:
    datasets = _generate_batch_datasets()
    expected, expected_skipped, expected_processed = _expected_batch_stats(datasets)

    running = {"n": 0, "mean": 0.0, "m2": 0.0}
    skipped = 0
    processed = 0

    def worker(chunk: list[dict]) -> None:
        nonlocal skipped, processed

        for dataset in chunk:
            values = _numeric_values_or_none(dataset.get("values", []))
            if values is None:
                # Intentional read-modify-write race in unsafe mode
                current = skipped
                skipped = current + 1
                continue

            current = processed
            processed = current + 1

            for x in values:
                # Intentional lock-free Welford updates in unsafe mode
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

    chunks = _chunk_strided(datasets, threads)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, chunk) for chunk in chunks]
        for fut in futures:
            fut.result()

    observed = RunningStats(n=running["n"], mean=running["mean"], m2=running["m2"])

    n_diff = expected.n - observed.n
    mean_diff = abs(expected.mean - observed.mean)
    var_diff = abs(expected.variance - observed.variance)

    ok = (
        observed.n == expected.n
        and mean_diff <= 1e-9
        and var_diff <= 1e-9
        and skipped == expected_skipped
        and processed == expected_processed
    )

    details = [
        f"n: expected={expected.n} observed={observed.n} lost={n_diff} ({_pct(n_diff, expected.n)}%)",
        f"mean: expected={expected.mean:.6f} observed={observed.mean:.6f} diff={mean_diff:.6f}",
        f"variance: expected={expected.variance:.6f} observed={observed.variance:.6f} diff={var_diff:.6f}",
        f"processed: expected={expected_processed} observed={processed}",
        f"skipped(poison): expected={expected_skipped} observed={skipped}",
        "hazard: lock-free shared Welford accumulator",
    ]
    return ok, details


def _batch_stats_safe(threads: int) -> tuple[bool, list[str]]:
    datasets = _generate_batch_datasets()
    expected, expected_skipped, expected_processed = _expected_batch_stats(datasets)

    def worker(chunk: list[dict]) -> tuple[RunningStats, int, int]:
        local_stats = RunningStats()
        local_skipped = 0
        local_processed = 0

        for dataset in chunk:
            values = _numeric_values_or_none(dataset.get("values", []))
            if values is None:
                local_skipped += 1
                continue

            local_processed += 1
            for x in values:
                local_stats.update(x)

        return local_stats, local_skipped, local_processed

    chunks = _chunk_strided(datasets, threads)

    observed = RunningStats()
    skipped = 0
    processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, chunk) for chunk in chunks]
        for fut in futures:
            local_stats, local_skipped, local_processed = fut.result()
            observed.merge(local_stats)
            skipped += local_skipped
            processed += local_processed

    n_diff = expected.n - observed.n
    mean_diff = abs(expected.mean - observed.mean)
    var_diff = abs(expected.variance - observed.variance)

    ok = (
        observed.n == expected.n
        and mean_diff <= 1e-9
        and var_diff <= 1e-9
        and skipped == expected_skipped
        and processed == expected_processed
    )

    details = [
        f"n: expected={expected.n} observed={observed.n} lost={n_diff} ({_pct(n_diff, expected.n)}%)",
        f"mean: expected={expected.mean:.6f} observed={observed.mean:.6f} diff={mean_diff:.6f}",
        f"variance: expected={expected.variance:.6f} observed={observed.variance:.6f} diff={var_diff:.6f}",
        f"processed: expected={expected_processed} observed={processed}",
        f"skipped(poison): expected={expected_skipped} observed={skipped}",
        "solution: thread-local Welford accumulators + deterministic merge",
    ]
    return ok, details


# ---------------------------------------------------------------------------
# Example 3: data_etl (15,000 rows over 3 phases, schema hot-reload)
# ---------------------------------------------------------------------------

NAMES = [
    "Alice",
    "Bob",
    "Charlie",
    "Diana",
    "Eve",
    "Frank",
    "Grace",
    "Hank",
    "Irene",
    "Jack",
    "Karen",
    "Leo",
    "Mona",
    "Nate",
    "Oscar",
    "Paula",
    "Quinn",
    "Rita",
    "Steve",
    "Tina",
    "Uma",
    "Victor",
    "Wendy",
    "Xavier",
    "Yara",
    "Zane",
    "Amber",
    "Blake",
    "Casey",
    "Drew",
]

SCHEMA_V1 = {
    "version": 1,
    "name_min": 1,
    "age_min": 0,
    "age_max": 150,
    "email_required": False,
    "email_pattern": None,
}

SCHEMA_V2 = {
    "version": 2,
    "name_min": 2,
    "age_min": 18,
    "age_max": 120,
    "email_required": True,
    "email_pattern": "@",
}


def _schema_signature(schema: dict) -> tuple:
    return (
        schema["version"],
        schema["name_min"],
        schema["age_min"],
        schema["age_max"],
        schema["email_required"],
        schema["email_pattern"],
    )


def _generate_valid_row(i: int) -> dict:
    name = NAMES[i % len(NAMES)]
    age = 20 + (i % 60)
    return {
        "name": name,
        "age": age,
        "email": f"{name.lower()}{i}@example.com",
    }


def _generate_invalid_row(i: int) -> dict:
    kind = i % 4
    if kind == 0:
        return {"name": "", "age": 25, "email": f"empty{i}@example.com"}
    if kind == 1:
        return {"name": f"User{i}", "age": -5, "email": f"neg{i}@example.com"}
    if kind == 2:
        return {"age": 35, "email": f"noname{i}@test.com"}
    return {"name": f"Bad{i}", "age": 200, "email": f"bad{i}@example.com"}


def _generate_rows(count: int, invalid_ratio: float, seed: int) -> list[dict]:
    rng = random.Random(seed)
    rows: list[dict] = []

    for i in range(1, count + 1):
        if rng.random() < invalid_ratio:
            rows.append(_generate_invalid_row(i))
        else:
            rows.append(_generate_valid_row(i))

    return rows


def _validate_row(row: dict, schema: dict) -> bool:
    name = row.get("name")
    if not isinstance(name, str) or len(name) < schema["name_min"]:
        return False

    age = row.get("age")
    if not isinstance(age, int):
        return False
    if age < schema["age_min"] or age > schema["age_max"]:
        return False

    email = row.get("email")
    if schema["email_required"]:
        if not isinstance(email, str):
            return False
        pattern = schema["email_pattern"]
        if pattern and pattern not in email:
            return False

    return True


class _SchemaHolder:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._schema = dict(SCHEMA_V1)

    def set_schema(self, schema: dict) -> None:
        with self._lock:
            self._schema = dict(schema)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._schema)


def _data_etl_unsafe(threads: int) -> tuple[bool, list[str]]:
    schema = dict(SCHEMA_V1)

    validation_count = 0
    valid_count = 0
    invalid_count = 0
    torn_reads = 0
    audit_entries: list[tuple[int, bool]] = []

    valid_sigs = {_schema_signature(SCHEMA_V1), _schema_signature(SCHEMA_V2)}

    def set_schema_non_atomic(target: dict) -> None:
        for key in ("version", "name_min", "age_min", "age_max", "email_required", "email_pattern"):
            schema[key] = target[key]

    def snapshot_non_atomic() -> dict:
        return {
            "version": schema["version"],
            "name_min": schema["name_min"],
            "age_min": schema["age_min"],
            "age_max": schema["age_max"],
            "email_required": schema["email_required"],
            "email_pattern": schema["email_pattern"],
        }

    def validate_worker(chunk: list[dict]) -> None:
        nonlocal validation_count, valid_count, invalid_count, torn_reads

        for row in chunk:
            snap = snapshot_non_atomic()
            if _schema_signature(snap) not in valid_sigs:
                current = torn_reads
                torn_reads = current + 1

            is_valid = _validate_row(row, snap)

            # Intentional read-modify-write races in unsafe mode
            current = validation_count
            validation_count = current + 1

            if is_valid:
                current = valid_count
                valid_count = current + 1
            else:
                current = invalid_count
                invalid_count = current + 1

            audit_entries.append((snap["version"], is_valid))

    def run_batch(rows: list[dict]) -> None:
        chunks = _chunk_strided(rows, threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [pool.submit(validate_worker, chunk) for chunk in chunks]
            for fut in futures:
                fut.result()

    # Batch 1: v1, 5000 rows, 10% invalid
    batch1 = _generate_rows(5000, 0.10, seed=101)
    run_batch(batch1)

    # Batch 2: switch to v2, 5000 rows, 20% invalid
    set_schema_non_atomic(SCHEMA_V2)
    batch2 = _generate_rows(5000, 0.20, seed=202)
    run_batch(batch2)

    # Batch 3: concurrent schema toggling, 5000 rows, 15% invalid
    batch3 = _generate_rows(5000, 0.15, seed=303)

    def toggler() -> None:
        toggle = False
        for _ in range(20):
            set_schema_non_atomic(SCHEMA_V2 if toggle else SCHEMA_V1)
            toggle = not toggle
            time.sleep(0.005)

    toggle_thread = threading.Thread(target=toggler, daemon=True)
    toggle_thread.start()
    run_batch(batch3)
    toggle_thread.join(timeout=5.0)

    expected_validations = 15_000
    observed_audit = len(audit_entries)
    consistency_diff = validation_count - (valid_count + invalid_count)

    ok = (
        torn_reads == 0
        and validation_count == expected_validations
        and observed_audit == expected_validations
        and consistency_diff == 0
    )

    details = [
        f"torn_reads={torn_reads}",
        f"validations: expected={expected_validations} observed={validation_count} lost={expected_validations - validation_count} ({_pct(expected_validations - validation_count, expected_validations)}%)",
        f"audit_entries: expected={expected_validations} observed={observed_audit} lost={expected_validations - observed_audit} ({_pct(expected_validations - observed_audit, expected_validations)}%)",
        f"valid+invalid consistency diff={consistency_diff}",
        "hazard: non-atomic schema swaps + lock-free shared counters",
    ]
    return ok, details


def _data_etl_safe(threads: int) -> tuple[bool, list[str]]:
    holder = _SchemaHolder()

    counter_lock = threading.Lock()
    validation_count = 0
    valid_count = 0
    invalid_count = 0
    torn_reads = 0
    audit_entries = 0

    valid_sigs = {_schema_signature(SCHEMA_V1), _schema_signature(SCHEMA_V2)}

    def validate_worker(chunk: list[dict]) -> None:
        nonlocal validation_count, valid_count, invalid_count, torn_reads, audit_entries

        for row in chunk:
            snap = holder.snapshot()

            # Should remain zero in safe mode
            if _schema_signature(snap) not in valid_sigs:
                with counter_lock:
                    torn_reads += 1

            is_valid = _validate_row(row, snap)

            with counter_lock:
                validation_count += 1
                audit_entries += 1
                if is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1

    def run_batch(rows: list[dict]) -> None:
        chunks = _chunk_strided(rows, threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [pool.submit(validate_worker, chunk) for chunk in chunks]
            for fut in futures:
                fut.result()

    # Batch 1: v1
    batch1 = _generate_rows(5000, 0.10, seed=101)
    run_batch(batch1)

    # Batch 2: v2
    holder.set_schema(SCHEMA_V2)
    batch2 = _generate_rows(5000, 0.20, seed=202)
    run_batch(batch2)

    # Batch 3: toggling during run
    batch3 = _generate_rows(5000, 0.15, seed=303)

    def toggler() -> None:
        toggle = False
        for _ in range(20):
            holder.set_schema(SCHEMA_V2 if toggle else SCHEMA_V1)
            toggle = not toggle
            time.sleep(0.005)

    toggle_thread = threading.Thread(target=toggler, daemon=True)
    toggle_thread.start()
    run_batch(batch3)
    toggle_thread.join(timeout=5.0)

    expected_validations = 15_000
    consistency_diff = validation_count - (valid_count + invalid_count)

    ok = (
        torn_reads == 0
        and validation_count == expected_validations
        and audit_entries == expected_validations
        and consistency_diff == 0
    )

    details = [
        f"torn_reads={torn_reads}",
        f"validations: expected={expected_validations} observed={validation_count} lost={expected_validations - validation_count} ({_pct(expected_validations - validation_count, expected_validations)}%)",
        f"audit_entries: expected={expected_validations} observed={audit_entries} lost={expected_validations - audit_entries} ({_pct(expected_validations - audit_entries, expected_validations)}%)",
        f"valid+invalid consistency diff={consistency_diff}",
        "solution: locked schema snapshots + synchronized counters",
    ]
    return ok, details


# ---------------------------------------------------------------------------
# Example 4: ml_scoring (2 sessions, concurrent train+predict)
# ---------------------------------------------------------------------------


def _generate_training_data(seed: int, n: int, center0: float, center1: float) -> list[tuple[float, float]]:
    rng = random.Random(seed)
    half = n // 2

    class0 = [(rng.gauss(center0, 1.0), rng.gauss(center0, 1.0)) for _ in range(half)]
    class1 = [(rng.gauss(center1, 1.0), rng.gauss(center1, 1.0)) for _ in range(n - half)]

    return class0 + class1


def _generate_test_points(seed: int, n: int) -> list[tuple[float, float]]:
    rng = random.Random(seed)
    return [(rng.gauss(5.0, 3.0), rng.gauss(5.0, 3.0)) for _ in range(n)]


def _predict_point(point: tuple[float, float], center: float) -> int:
    return 1 if (point[0] + point[1]) / 2.0 >= center else 0


def _ml_scoring_unsafe(threads: int) -> tuple[bool, list[str]]:
    num_train = 2000
    num_test = 2000

    train_a = _generate_training_data(seed=42, n=num_train, center0=2.0, center1=3.0)
    train_b = _generate_training_data(seed=43, n=num_train, center0=7.0, center1=8.0)
    test_points = _generate_test_points(seed=99, n=num_test)

    models: dict[str, dict] = {}
    prediction_count = 0

    def train_model(session_id: str, center: float, data: list[tuple[float, float]]) -> None:
        # Intentional architecture flaw: shared "active" slot overwrites by last writer.
        models["active"] = {
            "session": session_id,
            "center": center,
            "n_samples": len(data),
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fa = pool.submit(train_model, "session_a", 2.0, train_a)
        fb = pool.submit(train_model, "session_b", 7.0, train_b)
        fa.result()
        fb.result()

    def predict_session(_session_id: str) -> list[int]:
        nonlocal prediction_count

        chunks = _chunk_strided(test_points, threads)

        def predict_chunk(chunk: list[tuple[float, float]]) -> list[int]:
            nonlocal prediction_count

            out: list[int] = []
            for point in chunk:
                model = models.get("active", {"center": 5.0})
                out.append(_predict_point(point, model["center"]))

                # Intentional read-modify-write race in unsafe mode
                current = prediction_count
                prediction_count = current + 1

            return out

        preds: list[int] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [pool.submit(predict_chunk, chunk) for chunk in chunks]
            for fut in futures:
                preds.extend(fut.result())

        return preds

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fa = pool.submit(predict_session, "session_a")
        fb = pool.submit(predict_session, "session_b")
        preds_a = fa.result()
        preds_b = fb.result()

    expected_a = [_predict_point(point, 2.0) for point in test_points]
    expected_b = [_predict_point(point, 7.0) for point in test_points]
    expected_divergence = sum(1 for a, b in zip(expected_a, expected_b) if a != b)

    observed_divergence = sum(1 for a, b in zip(preds_a, preds_b) if a != b)

    expected_prediction_count = num_test * 2
    lost_predictions = expected_prediction_count - prediction_count

    ok = (
        observed_divergence == expected_divergence
        and prediction_count == expected_prediction_count
        and set(models.keys()) == {"session_a", "session_b"}
    )

    details = [
        f"session_isolation: expected_divergence={expected_divergence} observed_divergence={observed_divergence}",
        f"shared_model_owner={models.get('active', {}).get('session', '?')} center={models.get('active', {}).get('center', '?')}",
        f"predictions: expected={expected_prediction_count} observed={prediction_count} lost={lost_predictions} ({_pct(lost_predictions, expected_prediction_count)}%)",
        f"train_samples_per_session={num_train}",
        "hazard: single shared active model slot + lock-free counter",
    ]
    return ok, details


def _ml_scoring_safe(threads: int) -> tuple[bool, list[str]]:
    num_train = 2000
    num_test = 2000

    train_a = _generate_training_data(seed=42, n=num_train, center0=2.0, center1=3.0)
    train_b = _generate_training_data(seed=43, n=num_train, center0=7.0, center1=8.0)
    test_points = _generate_test_points(seed=99, n=num_test)

    models: dict[str, dict] = {}
    model_lock = threading.Lock()

    prediction_count = 0
    count_lock = threading.Lock()

    def train_model(session_id: str, center: float, data: list[tuple[float, float]]) -> None:
        with model_lock:
            models[session_id] = {
                "session": session_id,
                "center": center,
                "n_samples": len(data),
            }

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fa = pool.submit(train_model, "session_a", 2.0, train_a)
        fb = pool.submit(train_model, "session_b", 7.0, train_b)
        fa.result()
        fb.result()

    def predict_session(session_id: str) -> list[int]:
        nonlocal prediction_count

        with model_lock:
            model = dict(models[session_id])

        center = model["center"]
        chunks = _chunk_strided(test_points, threads)

        def predict_chunk(chunk: list[tuple[float, float]]) -> list[int]:
            nonlocal prediction_count

            out: list[int] = []
            for point in chunk:
                out.append(_predict_point(point, center))
                with count_lock:
                    prediction_count += 1
            return out

        preds: list[int] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [pool.submit(predict_chunk, chunk) for chunk in chunks]
            for fut in futures:
                preds.extend(fut.result())

        return preds

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fa = pool.submit(predict_session, "session_a")
        fb = pool.submit(predict_session, "session_b")
        preds_a = fa.result()
        preds_b = fb.result()

    expected_a = [_predict_point(point, 2.0) for point in test_points]
    expected_b = [_predict_point(point, 7.0) for point in test_points]
    expected_divergence = sum(1 for a, b in zip(expected_a, expected_b) if a != b)

    observed_divergence = sum(1 for a, b in zip(preds_a, preds_b) if a != b)

    expected_prediction_count = num_test * 2
    lost_predictions = expected_prediction_count - prediction_count

    ok = (
        observed_divergence == expected_divergence
        and prediction_count == expected_prediction_count
        and set(models.keys()) == {"session_a", "session_b"}
        and models["session_a"]["n_samples"] == num_train
        and models["session_b"]["n_samples"] == num_train
    )

    details = [
        f"session_isolation: expected_divergence={expected_divergence} observed_divergence={observed_divergence}",
        f"models_stored={sorted(models.keys())}",
        f"predictions: expected={expected_prediction_count} observed={prediction_count} lost={lost_predictions} ({_pct(lost_predictions, expected_prediction_count)}%)",
        f"train_samples_per_session={num_train}",
        "solution: session-keyed models + synchronized counters",
    ]
    return ok, details


# ---------------------------------------------------------------------------
# Example 5: image_pipeline (200 images, pure-Python resize simulation)
# ---------------------------------------------------------------------------


def _generate_image_specs(seed: int = 42) -> list[dict]:
    rng = random.Random(seed)

    specs: list[dict] = []

    for _ in range(50):
        specs.append(
            {
                "width": 100 + rng.randint(1, 300),
                "height": 100 + rng.randint(1, 300),
                "color": [rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255)],
                "label": "small",
            }
        )

    for _ in range(80):
        specs.append(
            {
                "width": 500 + rng.randint(1, 780),
                "height": 400 + rng.randint(1, 520),
                "color": [rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255)],
                "label": "medium",
            }
        )

    for _ in range(50):
        specs.append(
            {
                "width": 1920,
                "height": 1080,
                "color": [rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255)],
                "label": "large-1080p",
            }
        )

    for _ in range(20):
        specs.append(
            {
                "width": 3840,
                "height": 2160,
                "color": [rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255)],
                "label": "xl-4k",
            }
        )

    rng.shuffle(specs)
    return specs


def _build_image_payloads() -> list[dict]:
    payloads: list[dict] = []

    for spec in _generate_image_specs():
        width = spec["width"]
        height = spec["height"]

        payloads.append(
            {
                "width": width,
                "height": height,
                "pixel_count": width * height,
                "color": tuple(spec["color"]),
                "label": spec["label"],
            }
        )

    return payloads


def _make_source_buffer(width: int, height: int, color: tuple[int, int, int]) -> bytearray:
    pixel = bytes((color[0], color[1], color[2]))
    return bytearray(pixel * (width * height))


def _resize_nearest(src: bytearray, src_w: int, src_h: int, dst_w: int, dst_h: int) -> bytearray:
    dst = bytearray(dst_w * dst_h * 3)

    for y in range(dst_h):
        sy = min(src_h - 1, (y * src_h) // max(dst_h, 1))
        src_row = sy * src_w * 3
        dst_row = y * dst_w * 3

        for x in range(dst_w):
            sx = min(src_w - 1, (x * src_w) // max(dst_w, 1))
            si = src_row + sx * 3
            di = dst_row + x * 3
            dst[di] = src[si]
            dst[di + 1] = src[si + 1]
            dst[di + 2] = src[si + 2]

    return dst


def _weighted_batches(items: list[dict], max_weight: int) -> list[list[dict]]:
    batches: list[list[dict]] = []
    current: list[dict] = []
    weight = 0

    for item in items:
        item_weight = item["pixel_count"]

        if current and weight + item_weight > max_weight:
            batches.append(current)
            current = [item]
            weight = item_weight
        else:
            current.append(item)
            weight += item_weight

    if current:
        batches.append(current)

    return batches


def _image_pipeline_unsafe(threads: int) -> tuple[bool, list[str]]:
    payloads = _build_image_payloads()

    in_flight = 0
    peak_in_flight = 0
    current_bytes = 0
    peak_bytes = 0
    processed = 0

    def process_one(item: dict) -> None:
        nonlocal in_flight, peak_in_flight, current_bytes, peak_bytes, processed

        width = item["width"]
        height = item["height"]
        color = item["color"]

        target_w = 150
        target_h = max(1, int(height * (target_w / max(width, 1))))

        mem_est = width * height * 3 + target_w * target_h * 3

        # Intentional lock-free counters in unsafe mode
        current = in_flight
        in_flight = current + 1
        current = current_bytes
        current_bytes = current + mem_est

        peak_in_flight = max(peak_in_flight, in_flight)
        peak_bytes = max(peak_bytes, current_bytes)

        src = _make_source_buffer(width, height, color)
        thumb = _resize_nearest(src, width, height, target_w, target_h)
        _ = thumb[0] if thumb else 0

        current = in_flight
        in_flight = current - 1
        current = current_bytes
        current_bytes = current - mem_est
        processed += 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(process_one, item) for item in payloads]
        for fut in futures:
            fut.result()

    max_in_flight_target = 4
    ok = processed == len(payloads) and peak_in_flight <= max_in_flight_target

    details = [
        f"images={len(payloads)} processed={processed}",
        f"peak_in_flight={peak_in_flight} (bounded_target={max_in_flight_target})",
        f"peak_memory={peak_bytes / (1024 * 1024):.1f}MB",
        "hazard: no weighted batching/backpressure + lock-free memory counters",
    ]
    return ok, details


def _image_pipeline_safe(threads: int) -> tuple[bool, list[str]]:
    # Keep thread pool creation tied to requested thread count, but enforce
    # processing bounds via weighted batches + max_in_flight.
    _ = threads

    payloads = _build_image_payloads()

    max_weight = 2_000_000
    max_in_flight = 4
    batches = _weighted_batches(payloads, max_weight=max_weight)

    lock = threading.Lock()
    image_in_flight = 0
    peak_image_in_flight = 0
    batch_in_flight = 0
    peak_batch_in_flight = 0
    current_bytes = 0
    peak_bytes = 0
    processed = 0

    def process_batch(batch: list[dict]) -> None:
        nonlocal image_in_flight, peak_image_in_flight
        nonlocal batch_in_flight, peak_batch_in_flight
        nonlocal current_bytes, peak_bytes, processed

        with lock:
            batch_in_flight += 1
            peak_batch_in_flight = max(peak_batch_in_flight, batch_in_flight)

        try:
            for item in batch:
                width = item["width"]
                height = item["height"]
                color = item["color"]

                target_w = 150
                target_h = max(1, int(height * (target_w / max(width, 1))))
                mem_est = width * height * 3 + target_w * target_h * 3

                with lock:
                    image_in_flight += 1
                    peak_image_in_flight = max(peak_image_in_flight, image_in_flight)
                    current_bytes += mem_est
                    peak_bytes = max(peak_bytes, current_bytes)

                try:
                    src = _make_source_buffer(width, height, color)
                    thumb = _resize_nearest(src, width, height, target_w, target_h)
                    _ = thumb[0] if thumb else 0
                finally:
                    with lock:
                        image_in_flight -= 1
                        current_bytes -= mem_est
                        processed += 1
        finally:
            with lock:
                batch_in_flight -= 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_in_flight) as pool:
        futures = [pool.submit(process_batch, batch) for batch in batches]
        for fut in futures:
            fut.result()

    ok = (
        processed == len(payloads)
        and peak_batch_in_flight <= max_in_flight
        and peak_image_in_flight <= max_in_flight
    )

    details = [
        f"images={len(payloads)} processed={processed} batches={len(batches)}",
        f"peak_batch_in_flight={peak_batch_in_flight} (target={max_in_flight})",
        f"peak_image_in_flight={peak_image_in_flight} (target={max_in_flight})",
        f"peak_memory={peak_bytes / (1024 * 1024):.1f}MB",
        "solution: weighted batches + max_in_flight + synchronized counters (pure Python)",
    ]
    return ok, details


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------

SCENARIOS = {
    "text_analysis": {
        "unsafe": _text_analysis_unsafe,
        "safe": _text_analysis_safe,
        "description": "5K documents / shared accumulator",
    },
    "batch_stats": {
        "unsafe": _batch_stats_unsafe,
        "safe": _batch_stats_safe,
        "description": "2K datasets + poison pills / running stats",
    },
    "data_etl": {
        "unsafe": _data_etl_unsafe,
        "safe": _data_etl_safe,
        "description": "15K rows / schema hot-reload contention",
    },
    "ml_scoring": {
        "unsafe": _ml_scoring_unsafe,
        "safe": _ml_scoring_safe,
        "description": "2 sessions train+predict / model isolation",
    },
    "image_pipeline": {
        "unsafe": _image_pipeline_unsafe,
        "safe": _image_pipeline_safe,
        "description": "200 images / backpressure + memory bounds",
    },
}


# ---------------------------------------------------------------------------
# Runner + CLI
# ---------------------------------------------------------------------------


def _run_mode(example: str, mode: str, threads: int) -> ModeResult:
    fn = SCENARIOS[example][mode]
    ok, details, elapsed = _run_timed(lambda: fn(threads))

    mode_label = "unsafe_threaded" if mode == "unsafe" else "safe_python"
    return ModeResult(mode=mode_label, ok=ok, details=details, elapsed_s=elapsed)


def _print_mode_result(result: ModeResult) -> None:
    status = "PASS" if result.ok else "FAIL"
    print(f"  {result.mode}: {status} ({result.elapsed_s:.2f}s)")
    for line in result.details:
        print(f"    {line}")


def _run_one(example: str, mode: str, threads: int) -> tuple[bool, dict[str, str]]:
    print(f"\n=== Pure Python Comparison: {example} ===")
    print(f"  desc={SCENARIOS[example]['description']}")
    print(f"  python_mode={_python_mode_label()} threads={threads}")

    summary: dict[str, str] = {}
    safe_ok = True

    if mode in ("unsafe", "both"):
        unsafe = _run_mode(example, "unsafe", threads)
        _print_mode_result(unsafe)
        summary["unsafe"] = "PASS" if unsafe.ok else "FAIL"

    if mode in ("safe", "both"):
        safe = _run_mode(example, "safe", threads)
        _print_mode_result(safe)
        summary["safe"] = "PASS" if safe.ok else "FAIL"
        safe_ok = safe.ok

    return safe_ok, summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pure-Python comparison harness (unsafe threaded vs safe Python)"
    )

    parser.add_argument(
        "example",
        nargs="?",
        choices=sorted(SCENARIOS.keys()),
        help="Which example to run",
    )
    parser.add_argument("--all", action="store_true", help="Run all examples")
    parser.add_argument("--threads", type=int, default=48, help="Thread count (default: 48)")
    parser.add_argument(
        "--mode",
        choices=["unsafe", "safe", "both"],
        default="both",
        help="Which Python implementations to run (default: both)",
    )
    parser.add_argument(
        "--require-free-threaded",
        action="store_true",
        help="Exit non-zero unless running on free-threaded Python",
    )

    args = parser.parse_args()

    if args.require_free_threaded and not _is_free_threaded():
        print("ERROR: free-threaded Python is required for this baseline run")
        print("  current_mode=gil")
        print("  install: uv python install cpython-3.14.0+freethreaded")
        return 3

    targets: list[str]
    if args.all:
        targets = sorted(SCENARIOS.keys())
    else:
        if args.example is None:
            parser.print_help()
            return 2
        targets = [args.example]

    safe_failures = 0
    summary_rows: list[tuple[str, dict[str, str]]] = []

    for example in targets:
        safe_ok, row = _run_one(example, args.mode, args.threads)
        summary_rows.append((example, row))
        if args.mode in ("safe", "both") and not safe_ok:
            safe_failures += 1

    print("\n=== Summary ===")
    for example, row in summary_rows:
        unsafe = row.get("unsafe")
        safe = row.get("safe")

        if unsafe and safe:
            print(f"  {example}: unsafe={unsafe} safe={safe}")
        elif unsafe:
            print(f"  {example}: unsafe={unsafe}")
        elif safe:
            print(f"  {example}: safe={safe}")
        else:
            print(f"  {example}: (no mode run)")

    if safe_failures > 0:
        print(f"\nSAFE MODE FAILURES: {safe_failures}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

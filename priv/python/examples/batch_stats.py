"""
Batch statistics module for the Slither batch statistics example.

Computes descriptive statistics on numeric datasets using only Python stdlib.
Supports basic stats, weighted stats, percentile computation, and running
(accumulated) statistics per worker process. Each public function receives a
list of dataset dicts and returns a list of result dicts of the SAME LENGTH.

Under free-threaded Python, _running_stats mutations from concurrent threads
produce incorrect Welford's algorithm results (the mean/variance update is a
multi-step read-modify-write). A segfault in one thread's C extension call
kills the entire process and all in-flight work. Slither runs each worker in
a separate OS process -- accumulator updates are sequential per-worker, and a
crash in one worker is contained without affecting others.
"""

import math
import os
import statistics


# ---------------------------------------------------------------------------
# Shared mutable state -- safe only because Slither gives each worker its own
# OS process.  Under free-threaded Python (no GIL), concurrent threads would
# corrupt these via interleaved read-modify-write sequences.
# ---------------------------------------------------------------------------

_running_stats = {
    "n": 0,
    "mean": 0.0,
    "m2": 0.0,       # for Welford's online variance
    "min": float("inf"),
    "max": float("-inf"),
}
_batches_processed = 0
_worker_id = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _init_worker():
    """Set the worker ID on first call."""
    global _worker_id
    if _worker_id is None:
        _worker_id = os.getpid()


def _sorted_values(values):
    """Return a sorted copy of values, filtering out non-numeric entries."""
    return sorted(v for v in values if isinstance(v, (int, float)) and not math.isnan(v) and not math.isinf(v))


def _median(sorted_vals):
    """Compute median from a pre-sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0
    return float(sorted_vals[mid])


def _percentile(sorted_vals, p):
    """
    Compute the p-th percentile using linear interpolation.

    Uses the same method as Excel's PERCENTILE.INC / numpy's default
    interpolation='linear'.
    """
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n == 1:
        return float(sorted_vals[0])

    # Rank in [0, n-1] space
    rank = (p / 100.0) * (n - 1)
    lower = int(math.floor(rank))
    upper = min(lower + 1, n - 1)
    fraction = rank - lower

    return sorted_vals[lower] + fraction * (sorted_vals[upper] - sorted_vals[lower])


def _update_running_stats(sorted_vals):
    """
    Update the module-level running statistics using Welford's online algorithm.

    This is a multi-step read-modify-write operation.  Under free-threaded
    Python, concurrent threads interleaving these steps would produce incorrect
    running mean and variance values.  In Slither each worker is a separate OS
    process, so these updates are sequential and correct.
    """
    global _batches_processed

    for x in sorted_vals:
        _running_stats["n"] += 1
        n = _running_stats["n"]
        delta = x - _running_stats["mean"]
        _running_stats["mean"] += delta / n
        delta2 = x - _running_stats["mean"]
        _running_stats["m2"] += delta * delta2

        if x < _running_stats["min"]:
            _running_stats["min"] = x
        if x > _running_stats["max"]:
            _running_stats["max"] = x

    _batches_processed += 1


def _empty_stats():
    """Return a zeroed-out stats dict."""
    return {
        "mean": 0.0,
        "median": 0.0,
        "stdev": 0.0,
        "min": 0.0,
        "max": 0.0,
        "count": 0,
        "variance": 0.0,
        "range": 0.0,
    }


def _compute_single_stats(values):
    """
    Compute descriptive statistics for a single dataset's values list.

    Returns a result dict on success.  Raises on poison-pill data (None,
    NaN, Inf mixed into values) so the caller can catch and report the error.
    """
    if not values:
        raise ValueError("Empty dataset -- cannot compute statistics")

    # Detect poison data: None, NaN, Inf, or non-numeric types in the list.
    # Under the GIL these might silently corrupt shared state; here we fail
    # fast so on_error: :skip can demonstrate fault isolation.
    for v in values:
        if v is None:
            raise ValueError("Dataset contains None -- corrupt input data")
        if not isinstance(v, (int, float)):
            raise TypeError("Dataset contains non-numeric value: {!r}".format(v))
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            raise ValueError("Dataset contains NaN/Inf -- corrupt input data")

    sorted_vals = _sorted_values(values)
    n = len(sorted_vals)

    if n == 0:
        return _empty_stats()

    mean_val = statistics.mean(sorted_vals)
    median_val = _median(sorted_vals)
    min_val = sorted_vals[0]
    max_val = sorted_vals[-1]
    range_val = max_val - min_val

    if n >= 2:
        variance_val = statistics.variance(sorted_vals)
        stdev_val = math.sqrt(variance_val)
    else:
        variance_val = 0.0
        stdev_val = 0.0

    _update_running_stats(sorted_vals)

    return {
        "mean": round(mean_val, 6),
        "median": round(median_val, 6),
        "stdev": round(stdev_val, 6),
        "min": round(min_val, 6),
        "max": round(max_val, 6),
        "count": n,
        "variance": round(variance_val, 6),
        "range": round(range_val, 6),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_stats(datasets):
    """
    Compute descriptive statistics for each dataset.

    Args:
        datasets: list of dicts, each with key "values" (list of numbers)

    Returns:
        list of result dicts (SAME LENGTH as input) with keys:
            mean, median, stdev, min, max, count, variance, range

    Poison-pill datasets (containing None, NaN, empty, or non-numeric values)
    raise an exception that crashes the entire batch.  Slither's on_error: :skip
    drops the failed batch and continues processing other batches -- demonstrating
    fault isolation that free-threaded Python cannot provide (one thread's crash
    kills all threads).
    """
    _init_worker()
    results = []

    for dataset in datasets:
        values = dataset.get("values", [])
        results.append(_compute_single_stats(values))

    return results


def compute_weighted_stats(datasets):
    """
    Compute descriptive statistics plus a weighted mean for each dataset.

    Args:
        datasets: list of dicts, each with:
            - "values" (list of numbers)
            - "weights" (optional list of numbers, same length as values)

    Returns:
        list of result dicts (SAME LENGTH as input) with all keys from
        compute_stats plus: weighted_mean
    """
    base_results = compute_stats(datasets)

    for i, dataset in enumerate(datasets):
        if "error" in base_results[i]:
            continue

        values = dataset.get("values", [])
        weights = dataset.get("weights", None)

        if not values or not weights:
            base_results[i]["weighted_mean"] = base_results[i]["mean"]
            continue

        # Ensure weights and values have the same length
        if len(weights) != len(values):
            base_results[i]["weighted_mean"] = base_results[i]["mean"]
            continue

        total_weight = sum(weights)
        if total_weight == 0:
            base_results[i]["weighted_mean"] = 0.0
            continue

        weighted_sum = sum(v * w for v, w in zip(values, weights))
        base_results[i]["weighted_mean"] = round(weighted_sum / total_weight, 6)

    return base_results


def compute_percentiles(datasets):
    """
    Compute percentile values for each dataset.

    Args:
        datasets: list of dicts, each with key "values" (list of numbers)

    Returns:
        list of result dicts (SAME LENGTH as input) with keys:
            p25, p50, p75, p90, p99, count
    """
    _init_worker()
    results = []

    for dataset in datasets:
        values = dataset.get("values", [])

        try:
            if not values:
                results.append({
                    "p25": 0.0,
                    "p50": 0.0,
                    "p75": 0.0,
                    "p90": 0.0,
                    "p99": 0.0,
                    "count": 0,
                })
                continue

            sorted_vals = _sorted_values(values)
            n = len(sorted_vals)

            if n == 0:
                results.append({
                    "p25": 0.0,
                    "p50": 0.0,
                    "p75": 0.0,
                    "p90": 0.0,
                    "p99": 0.0,
                    "count": 0,
                })
                continue

            results.append({
                "p25": round(_percentile(sorted_vals, 25), 6),
                "p50": round(_percentile(sorted_vals, 50), 6),
                "p75": round(_percentile(sorted_vals, 75), 6),
                "p90": round(_percentile(sorted_vals, 90), 6),
                "p99": round(_percentile(sorted_vals, 99), 6),
                "count": n,
            })
        except Exception as exc:
            results.append({
                "error": "computation failed: {}".format(str(exc)),
                "values_count": len(values) if values else 0,
            })

    return results


def get_running_stats(items):
    """
    Return this worker's accumulated running statistics.

    Returns one result per input item (Slither contract: len(output) == len(input)).
    Each result contains the same worker stats snapshot.
    """
    _init_worker()

    n = _running_stats["n"]
    if n > 1:
        variance = _running_stats["m2"] / max(n - 1, 1)
    else:
        variance = 0.0

    stats = {
        "worker_id": _worker_id,
        "batches_processed": _batches_processed,
        "running_n": n,
        "running_mean": round(_running_stats["mean"], 6),
        "running_variance": round(variance, 6),
        "running_min": _running_stats["min"] if _running_stats["min"] != float("inf") else 0.0,
        "running_max": _running_stats["max"] if _running_stats["max"] != float("-inf") else 0.0,
    }
    return [stats for _ in items]

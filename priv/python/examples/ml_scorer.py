"""
ML scoring: train model, featurize records, batch predict, worker stats.

Provides session-scoped model storage so that train_model and predict_batch
can share a trained scikit-learn model within the same SnakeBridge session.
Requires scikit-learn and numpy.

Under free-threaded Python, concurrent dict mutations on _models from multiple
threads = corruption (dict resize during concurrent insert is undefined).
_prediction_count += len(items) is a read-modify-write race. numpy BLAS
operations called from multiple threads share internal C buffers not designed
for concurrent access. Slither's session affinity routes train+predict to the
same OS process -- each has its own _models dict, its own BLAS context, and
its own counters with zero contention.
"""

import os
import time
import uuid

_models = {}             # session-scoped model storage
_prediction_count = 0    # total predictions across all calls
_worker_id = os.getpid() # this worker's OS process ID
_training_history = []   # records of all models trained on this worker


def train_model(items):
    """
    Train a scikit-learn model and store it for later prediction.

    Args:
        items: list with ONE element: {
            "X": [[features...], ...],
            "y": [labels...],
            "model_type": "logistic" | "random_forest"
        }

    Returns:
        list with ONE element: {
            "model_id": str,
            "classes": list,
            "n_features": int,
            "accuracy": float,
            "worker_id": int
        }

    Session affinity ensures train and predict go to the same Python
    worker so the stored model is accessible for prediction.
    """
    global _prediction_count, _worker_id

    try:
        import numpy as np
    except ImportError:
        return [{"error": "numpy is not installed (run: mix slither.example ml_scoring)"}]

    try:
        from sklearn.linear_model import LogisticRegression
    except ImportError:
        return [{"error": "scikit-learn is not installed (run: mix slither.example ml_scoring)"}]

    spec = items[0]
    X = spec.get("X", [])
    y = spec.get("y", [])
    model_type = spec.get("model_type", "logistic")

    if not X or not y:
        return [{"error": "Training data X and y must be non-empty"}]

    try:
        if model_type == "random_forest":
            from sklearn.ensemble import RandomForestClassifier
            model = RandomForestClassifier(n_estimators=50)
        else:
            model = LogisticRegression(max_iter=1000)

        X_arr = np.array(X, dtype=float)
        y_arr = np.array(y)
        model.fit(X_arr, y_arr)

        model_id = str(uuid.uuid4())
        _models[model_id] = model

        accuracy = float(model.score(X_arr, y_arr))

        _training_history.append({
            "model_id": model_id,
            "n_samples": len(X),
            "accuracy": accuracy,
            "timestamp": time.time(),
            "worker_id": _worker_id,
        })

        return [{
            "model_id": model_id,
            "classes": [int(c) if isinstance(c, (int, float, np.integer)) else c
                        for c in model.classes_.tolist()],
            "n_features": int(X_arr.shape[1]),
            "n_samples": int(X_arr.shape[0]),
            "accuracy": round(accuracy, 4),
            "worker_id": _worker_id,
        }]
    except Exception as e:
        return [{"error": f"Training failed: {str(e)}"}]


def featurize_batch(items):
    """
    Extract and normalize numeric features from raw data dicts.

    Handles two cases:
      - Cache hit: item already has "features" key -> pass through unchanged
      - Cache miss: item has "raw_data" key -> extract numeric features

    Args:
        items: list of dicts, each with either:
            {"raw_data": {field: value, ...}, "model_id": str, "record_id": ...}
            or
            {"features": [float, ...], "model_id": str, "cache_hit": true}

    Returns:
        list of dicts with "features", "feature_names", and "model_id" keys
    """
    results = []
    for item in items:
        # Cache hit: already has features, pass through
        if "features" in item and item["features"] is not None:
            results.append({
                "features": item["features"],
                "feature_names": item.get("feature_names", []),
                "model_id": item.get("model_id"),
                "cache_hit": True,
            })
            continue

        # Cache miss: extract features from raw_data
        raw = item.get("raw_data", {})
        model_id = item.get("model_id")

        # Extract all numeric values, sorted by key for consistency
        sorted_keys = sorted(
            k for k, v in raw.items()
            if isinstance(v, (int, float))
        )
        features = [float(raw[k]) for k in sorted_keys]

        results.append({
            "features": features,
            "feature_names": sorted_keys,
            "model_id": model_id,
            "cache_hit": False,
        })

    return results


def predict_batch(items):
    """
    Predict using a previously trained model stored in the session.

    Args:
        items: list of {"model_id": str, "features": [float, ...]}

    Returns:
        list of {
            "prediction": int or str,
            "confidence": float,
            "probabilities": {class_label: probability, ...}
        }

    Session affinity ensures the model trained by train_model is available.
    """
    global _prediction_count

    if not items:
        return []

    try:
        import numpy as np
    except ImportError:
        return [{"prediction": None, "confidence": 0.0,
                 "error": "numpy is not installed"} for _ in items]

    model_id = items[0].get("model_id")
    if model_id is None:
        return [{"prediction": None, "confidence": 0.0,
                 "error": "No model_id provided"} for _ in items]

    model = _models.get(model_id)
    if model is None:
        return [{"prediction": None, "confidence": 0.0,
                 "error": f"Model {model_id} not found in session"} for _ in items]

    try:
        features = [item.get("features", []) for item in items]
        X = np.array(features, dtype=float)

        predictions = model.predict(X)
        probas = model.predict_proba(X)
        confidences = probas.max(axis=1)

        _prediction_count += len(items)

        results = []
        classes = model.classes_.tolist()

        for i in range(len(items)):
            pred = predictions[i]
            conf = confidences[i]
            proba_row = probas[i]

            # Convert numpy types to native Python types
            if isinstance(pred, (np.integer,)):
                pred = int(pred)
            elif isinstance(pred, (np.floating,)):
                pred = float(pred)

            prob_dict = {}
            for cls, p in zip(classes, proba_row.tolist()):
                cls_key = str(int(cls) if isinstance(cls, (int, float, np.integer)) else cls)
                prob_dict[cls_key] = round(float(p), 4)

            results.append({
                "prediction": pred,
                "confidence": round(float(conf), 4),
                "probabilities": prob_dict,
            })

        return results
    except Exception as e:
        return [{"prediction": None, "confidence": 0.0,
                 "error": f"Prediction failed: {str(e)}"} for _ in items]


def get_model_stats(items):
    """Return this worker's model and prediction statistics.

    Returns one result per input item (Slither contract: len(output) == len(input)).
    """
    stats = {
        "worker_id": _worker_id,
        "models_stored": len(_models),
        "model_ids": list(_models.keys()),
        "prediction_count": _prediction_count,
        "training_history": _training_history,
    }
    return [stats for _ in items]

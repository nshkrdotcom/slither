"""
CSV data transformation module for the Slither data ETL pipeline example.

Provides batch validation and transformation of row data using only Python
stdlib. Validates rows against a schema (type checks, required fields,
min/max constraints, patterns) and applies transformation rules (rename,
cast, computed fields, defaults).

CONCURRENCY NOTE:
Under free-threaded Python, _audit_log.append() from concurrent threads can
produce a corrupted list (list.append is only atomic under the GIL, not in
free-threaded builds). _validation_count += 1 is a textbook read-modify-write
race. The schema dict read concurrently with a hot-reload write produces torn
reads. Slither passes the schema as immutable payload (read atomically from
ETS) and each worker maintains its own audit log in its own process.
"""

import os
import re


# ---------------------------------------------------------------------------
# Module-level shared mutable state (per-worker audit log)
#
# Under free-threaded Python, concurrent threads mutating these structures
# would cause data corruption:
#   - _audit_log.append(...)           ->  corrupted list (lost entries, index errors)
#   - _validation_count += 1           ->  torn counter (read-modify-write race)
#   - reading schema while swapping it ->  torn reads on the dict
#
# Slither runs each worker in a separate OS process, so each has its own
# copy of these globals with zero contention.
# ---------------------------------------------------------------------------

_audit_log = []          # records every validation decision
_validation_count = 0    # total validations performed
_worker_id = None        # set via os.getpid() on first call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check_type(value, expected_type):
    """
    Check whether a value matches the expected type string.

    Returns True if the value is compatible with the type, False otherwise.
    """
    if expected_type == "string":
        return isinstance(value, str)
    elif expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    elif expected_type == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    elif expected_type == "boolean":
        return isinstance(value, bool)
    return True


def _validate_field(field_name, value, constraints):
    """
    Validate a single field value against its schema constraints.

    Returns a list of error strings (empty if valid).
    """
    errors = []
    field_type = constraints.get("type", "string")

    # Type validation
    if not _check_type(value, field_type):
        errors.append(
            f"{field_name}: expected type '{field_type}', got '{type(value).__name__}'"
        )
        # If type is wrong, skip further constraint checks
        return errors

    # Numeric constraints
    if field_type in ("integer", "float"):
        if "min" in constraints and value < constraints["min"]:
            errors.append(
                f"{field_name}: value {value} is below minimum {constraints['min']}"
            )
        if "max" in constraints and value > constraints["max"]:
            errors.append(
                f"{field_name}: value {value} exceeds maximum {constraints['max']}"
            )

    # String constraints
    if field_type == "string":
        if "min_length" in constraints and len(value) < constraints["min_length"]:
            errors.append(
                f"{field_name}: length {len(value)} is below minimum "
                f"{constraints['min_length']}"
            )
        if "max_length" in constraints and len(value) > constraints["max_length"]:
            errors.append(
                f"{field_name}: length {len(value)} exceeds maximum "
                f"{constraints['max_length']}"
            )
        if "pattern" in constraints:
            if not re.search(constraints["pattern"], value):
                errors.append(
                    f"{field_name}: value '{value}' does not match pattern "
                    f"'{constraints['pattern']}'"
                )

    return errors


def _validate_one(item):
    """
    Validate a single row against its schema.

    Args:
        item: dict with keys "row" (dict) and "schema" (dict with "fields" key)

    Returns:
        dict with keys "valid" (bool), "row" (dict), "errors" (list of str)
    """
    row = item.get("row", {})
    schema = item.get("schema", {})
    fields = schema.get("fields", {})
    errors = []

    for field_name, constraints in fields.items():
        required = constraints.get("required", False)

        if field_name not in row:
            if required:
                errors.append(f"{field_name}: required field is missing")
            continue

        value = row[field_name]
        field_errors = _validate_field(field_name, value, constraints)
        errors.extend(field_errors)

    return {
        "valid": len(errors) == 0,
        "row": row,
        "errors": errors,
    }


def _record_audit(item, result, index):
    """
    Record a validation decision in the per-worker audit log.

    Under free-threaded Python with multiple threads, this is a race condition:
    _audit_log.append(...) from thread A and thread B can interleave, producing
    a corrupted list (lost entries, index errors). _validation_count += 1 is a
    classic read-modify-write race where two threads reading the same value
    before either writes back = one increment is lost.

    In Slither, each worker is a separate OS process with its own _audit_log,
    so there is zero contention.
    """
    global _validation_count

    _audit_log.append({
        "row_id": item.get("row", {}).get("id", index),
        "valid": result["valid"],
        "error_count": len(result["errors"]),
        "schema_version": item.get("schema", {}).get("version", "unknown"),
        "worker_id": _worker_id,
    })

    # _validation_count += 1 is a textbook race: read, increment, write.
    # Two threads reading the same value = one increment is lost.
    _validation_count += 1


def validate_batch(items):
    """
    Validate a batch of rows against their schemas.

    Args:
        items: list of dicts, each with keys:
            - "row" (dict): the data row to validate
            - "schema" (dict): schema with "fields" mapping field names to
              constraint dicts supporting keys: type (string|integer|float|
              boolean), required (bool), min/max (numeric), min_length/
              max_length (string), pattern (regex string)

    Returns:
        list of result dicts (SAME LENGTH as input) with keys:
            valid (bool), row (dict), errors (list of str)

    Side-effect:
        Appends to _audit_log and increments _validation_count for each row.
        Under free-threaded Python, these side-effects corrupt when called
        from multiple threads concurrently.
    """
    global _worker_id

    # Set worker identity on first call
    if _worker_id is None:
        _worker_id = os.getpid()

    results = []
    for i, item in enumerate(items):
        try:
            result = _validate_one(item)
            results.append(result)
        except Exception as exc:
            result = {
                "valid": False,
                "row": item.get("row", {}),
                "errors": [f"validation error: {exc}"],
            }
            results.append(result)

        # Record audit trail -- this is the race condition under free-threaded Python.
        # _audit_log.append() from N threads = corrupted list.
        # _validation_count += 1 from N threads = lost increments.
        _record_audit(item, result, i)

    return results


def get_audit_stats(items):
    """
    Return this worker's audit log and validation stats.

    Returns one result per input item (Slither contract: len(output) == len(input)).
    Each result contains the same worker stats snapshot.
    """
    stats = {
        "worker_id": _worker_id,
        "validation_count": _validation_count,
        "audit_log_size": len(_audit_log),
        "valid_count": sum(1 for e in _audit_log if e["valid"]),
        "invalid_count": sum(1 for e in _audit_log if not e["valid"]),
        "schema_versions_seen": list(set(e["schema_version"] for e in _audit_log)),
        "recent_entries": _audit_log[-5:] if _audit_log else [],
    }
    return [stats for _ in items]


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------


def _cast_value(value, target_type):
    """
    Cast a value to the target type string. Returns the cast value
    or the original value if casting fails.
    """
    try:
        if target_type == "integer":
            return int(value)
        elif target_type == "float":
            return float(value)
        elif target_type == "string":
            return str(value)
        elif target_type == "boolean":
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes")
            return bool(value)
    except (ValueError, TypeError):
        return value
    return value


def _evaluate_computed(expression, row):
    """
    Evaluate a simple computed field expression.

    Supports concatenation via '+' between field references and string
    literals (quoted with single quotes). For example:
        "first_name + ' ' + last_name"

    Returns the computed string value, or None if a referenced field
    is missing.
    """
    parts = [p.strip() for p in expression.split("+")]
    result_parts = []

    for part in parts:
        # String literal (single-quoted)
        if part.startswith("'") and part.endswith("'"):
            result_parts.append(part[1:-1])
        # String literal (double-quoted)
        elif part.startswith('"') and part.endswith('"'):
            result_parts.append(part[1:-1])
        else:
            # Field reference
            if part in row:
                result_parts.append(str(row[part]))
            else:
                return None

    return "".join(result_parts)


def _transform_one(item):
    """
    Transform a single row according to transformation rules.

    Args:
        item: dict with keys "row" (dict) and "rules" (dict with optional
              keys: rename, cast, computed, defaults)

    Returns:
        transformed row dict
    """
    row = dict(item.get("row", {}))
    rules = item.get("rules", {})

    # Apply renames: {"old_name": "new_name"}
    renames = rules.get("rename", {})
    for old_name, new_name in renames.items():
        if old_name in row:
            row[new_name] = row.pop(old_name)

    # Apply type casts: {"field": "target_type"}
    casts = rules.get("cast", {})
    for field, target_type in casts.items():
        if field in row:
            row[field] = _cast_value(row[field], target_type)

    # Apply computed fields: {"new_field": "expression"}
    computed = rules.get("computed", {})
    for field, expression in computed.items():
        value = _evaluate_computed(expression, row)
        if value is not None:
            row[field] = value

    # Apply defaults: {"field": "default_value"}
    defaults = rules.get("defaults", {})
    for field, default_value in defaults.items():
        if field not in row:
            row[field] = default_value

    return row


def transform_batch(items):
    """
    Transform a batch of rows according to their rules.

    Args:
        items: list of dicts, each with keys:
            - "row" (dict): the data row to transform
            - "rules" (dict): transformation rules with optional keys:
              rename (dict mapping old->new field names),
              cast (dict mapping field->target type),
              computed (dict mapping new field->expression using + for concat),
              defaults (dict mapping field->default value)

    Returns:
        list of transformed row dicts (SAME LENGTH as input)
    """
    results = []
    for item in items:
        try:
            results.append(_transform_one(item))
        except Exception:
            # On error, return the original row unchanged
            results.append(item.get("row", {}))
    return results

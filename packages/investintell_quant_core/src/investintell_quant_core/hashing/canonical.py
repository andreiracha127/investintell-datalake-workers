"""Canonical in-memory hashing utilities.

This module is deliberately filesystem-free and network-free. It accepts already
materialized Python values and returns deterministic SHA-256 hashes.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from typing import Any


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def logical_payload_hash(payload: Any) -> str:
    return stable_hash(normalize_logical_value(payload))


def logical_records_hash(rows: list[dict[str, Any]]) -> str:
    columns = sorted({key for row in rows for key in row})
    normalized_rows = [
        {column: normalize_logical_value(row.get(column)) for column in columns}
        for row in rows
    ]
    normalized_rows.sort(key=lambda row: json.dumps(row, sort_keys=True, default=str))
    return stable_hash({
        "schema": columns,
        "rows": normalized_rows,
    })


# Reserved key of the tagged object that stands in for a non-finite float. A
# string sentinel would collide with the same text arriving as data (macro
# sources do emit the literal "NaN"), so the tag is structural, and an input
# dict that carries the reserved key is refused rather than left to collide.
NON_FINITE_TAG_KEY = "$float"


def normalize_logical_value(value: Any) -> Any:
    """Canonical logical form of ``value`` (``a3.metrics.METRICS_HASH_POLICY_VERSION``).

    Finite floats round to 12 decimals with ``-0.0`` folded into ``0.0``; NaN and
    the infinities become ``{"$float": "NaN" | "Infinity" | "-Infinity"}``, which
    no ordinary input value (number, string, ``None``, list, dict without the
    reserved key) can produce. ``src.calibration_harness.normalize_logical_value``
    mirrors this function line for line.
    """
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except (AttributeError, TypeError, ValueError):
            pass
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    elif hasattr(value, "to_pydatetime64"):
        value = str(value)
    if isinstance(value, dict):
        if NON_FINITE_TAG_KEY in value:
            raise ValueError(
                f"{NON_FINITE_TAG_KEY!r} is reserved for the non-finite float tag"
            )
        return {str(key): normalize_logical_value(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [normalize_logical_value(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_logical_value(item) for item in value]
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value):
            return {NON_FINITE_TAG_KEY: "NaN"}
        if math.isinf(value):
            return {NON_FINITE_TAG_KEY: "Infinity" if value > 0 else "-Infinity"}
        rounded = round(value, 12)
        # ``-0.0 == 0.0`` but serializes differently; same convention as
        # ``a3.metrics.canonical_metric_value``.
        return 0.0 if rounded == 0 else rounded
    return value


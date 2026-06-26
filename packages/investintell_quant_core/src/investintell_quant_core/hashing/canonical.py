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


def normalize_logical_value(value: Any) -> Any:
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
            return None
        if math.isinf(value):
            return str(value)
        return round(value, 12)
    return value


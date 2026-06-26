"""Canonical hashing helpers for certified input packs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

SHA256_HEX_LENGTH = 64


def sha256_bytes(data: bytes) -> str:
    """Return a lowercase SHA-256 hex digest."""
    return hashlib.sha256(data).hexdigest()


def canonical_json_bytes(payload: Any) -> bytes:
    """Serialize JSON data in the pack's canonical digest form.

    Dict keys are sorted, separators are compact, UTF-8 is used, and NaN/Inf are
    rejected. List order is preserved because row/order semantics can be
    material to a source manifest.
    """
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def load_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def canonical_json_sha256(payload: Any) -> str:
    return sha256_bytes(canonical_json_bytes(payload))


def file_sha256(path: str | Path, *, canonical_json: bool = True) -> str:
    """Hash a file using canonical JSON for ``*.json`` and bytes otherwise."""
    p = Path(path)
    if canonical_json and p.suffix.lower() == ".json":
        return canonical_json_sha256(load_json(p))
    return sha256_bytes(p.read_bytes())


def is_sha256_hex(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == SHA256_HEX_LENGTH
        and all(ch in "0123456789abcdef" for ch in value)
    )


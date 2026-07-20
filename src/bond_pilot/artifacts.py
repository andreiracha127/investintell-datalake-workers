"""Collision-safe artifact writing helpers for internal pilot runs."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any

from .contracts import PilotError
from ._secure_local_fs import lexical_local_path, secure_read_file


def validated_local_path(value: str | Path, *, error_code: str) -> Path:
    """Return only the lexical local label; callers must open it through a capability."""
    return lexical_local_path(value, error_code=error_code)


def _nominal_reparse_point(path: Path, status: os.stat_result) -> bool:
    attributes = getattr(status, "st_file_attributes", 0)
    if stat.S_ISLNK(status.st_mode) or attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0):
        return True
    isjunction = getattr(os.path, "isjunction", None)
    return bool(callable(isjunction) and isjunction(path))


def validated_output_path_nominal(value: str | Path, *, error_code: str) -> Path:
    """Temporarily reject existing unsafe output ancestors before legacy path writes.

    This is a static, nominal check only; Commit 3 will replace these output
    path operations with handle-anchored capabilities.
    """
    path = lexical_local_path(value, error_code=error_code)
    current = Path(path.anchor)
    try:
        status = os.lstat(current)
        if not stat.S_ISDIR(status.st_mode) or _nominal_reparse_point(current, status):
            raise OSError("unsafe output root")
        for part in path.parent.parts[1:]:
            current /= part
            try:
                status = os.lstat(current)
            except FileNotFoundError:
                break
            if not stat.S_ISDIR(status.st_mode) or _nominal_reparse_point(current, status):
                raise OSError("unsafe output ancestor")
    except OSError as exc:
        raise PilotError(error_code) from exc
    return path


def read_secure_local_file(value: str | Path, *, max_bytes: int, error_code: str, too_large_code: str | None = None) -> tuple[Path, bytes]:
    """Capture a bounded regular local file through the secure filesystem core."""
    return secure_read_file(value, max_bytes=max_bytes, error_code=error_code, too_large_code=too_large_code)


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize JSON deterministically for manifests and checksum inputs."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def sha256_file(path: Path, *, chunk_size: int = 1024**2) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()

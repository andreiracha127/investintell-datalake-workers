"""Collision-safe artifact writing helpers for internal pilot runs."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from .contracts import PilotError


_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")


def _reparse_point(path: Path, status: os.stat_result) -> bool:
    attributes = getattr(status, "st_file_attributes", 0)
    if attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0):
        return True
    isjunction = getattr(os.path, "isjunction", None)
    return bool(callable(isjunction) and isjunction(path))


def validated_local_path(value: str | Path, *, error_code: str) -> Path:
    """Return a lexical absolute local path after rejecting unsafe existing ancestors."""
    locator = str(value)
    normalized = locator.strip()
    if normalized.startswith(("\\\\", "//")) or (urlsplit(normalized).scheme and not _WINDOWS_DRIVE.match(normalized)):
        raise PilotError(error_code)
    path = Path(os.path.abspath(locator))
    current = Path(path.anchor)
    try:
        ancestors = path.parent.parts[1:]
        status = os.lstat(current)
        if not stat.S_ISDIR(status.st_mode) or _reparse_point(current, status):
            raise OSError("unsafe anchor")
        for part in ancestors:
            current /= part
            status = os.lstat(current)
            if not stat.S_ISDIR(status.st_mode) or _reparse_point(current, status):
                raise OSError("unsafe ancestor")
    except OSError as exc:
        raise PilotError(error_code) from exc
    return path


def read_secure_local_file(value: str | Path, *, max_bytes: int, error_code: str, too_large_code: str | None = None) -> tuple[Path, bytes]:
    """Capture a bounded regular local file through one lstat/open/fstat sequence."""
    path = validated_local_path(value, error_code=error_code)
    try:
        before = os.lstat(path)
        if not stat.S_ISREG(before.st_mode) or _reparse_point(path, before):
            raise OSError("unsafe control")
        if before.st_size > max_bytes:
            raise PilotError(too_large_code or error_code)
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise OSError("swapped control")
            raw = handle.read(max_bytes + 1)
            after = os.fstat(handle.fileno())
        if len(raw) > max_bytes:
            raise PilotError(too_large_code or error_code)
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) or len(raw) != before.st_size:
            raise OSError("changed control")
    except PilotError:
        raise
    except OSError as exc:
        raise PilotError(error_code) from exc
    return path, raw


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize JSON deterministically for manifests and checksum inputs."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def sha256_file(path: Path, *, chunk_size: int = 1024**2) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_bytes_once(path: Path, contents: bytes) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as output:
            output.write(contents)
    except FileExistsError as exc:
        raise PilotError("already_exists", {"path": str(path)}) from exc
    return path


def create_run_directory(root: Path, run_name: str) -> Path:
    root = Path(root)
    if not isinstance(run_name, str) or not run_name or Path(run_name).name != run_name:
        raise PilotError("invalid_run_name", {"run_name": run_name})
    root.mkdir(parents=True, exist_ok=True)
    run_directory = root / run_name
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise PilotError("already_exists", {"path": str(run_directory)}) from exc
    return run_directory


def write_json_once(path: Path, value: Any) -> Path:
    return _write_bytes_once(Path(path), canonical_json_bytes(value))


def write_text_once(path: Path, value: str) -> Path:
    if not isinstance(value, str):
        raise PilotError("invalid_text", {"path": str(path)})
    return _write_bytes_once(Path(path), value.encode("utf-8"))


def partial_path(final_path: Path) -> Path:
    final_path = Path(final_path)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    return final_path.parent / f".{final_path.name}.{uuid4().hex}.partial"


def commit_partial(partial: Path, final: Path) -> Path:
    partial = Path(partial)
    final = Path(final)
    if partial.parent.resolve() != final.parent.resolve():
        raise PilotError("partial_not_same_directory", {"partial": str(partial), "final": str(final)})
    if not partial.is_file():
        raise PilotError("missing_partial", {"path": str(partial)})
    try:
        os.link(partial, final)
    except FileExistsError as exc:
        raise PilotError("already_exists", {"path": str(final)}) from exc
    except OSError as exc:
        raise PilotError("partial_commit_failed", {"path": str(final), "reason": str(exc)}) from exc
    partial.unlink()
    return final


def replace_checkpoint(path: Path, contents: bytes | str) -> Path:
    path = Path(path)
    if path.name != "checkpoint.json":
        raise PilotError("checkpoint_path_invalid", {"path": str(path)})
    payload = contents.encode("utf-8") if isinstance(contents, str) else contents
    if not isinstance(payload, bytes):
        raise PilotError("invalid_checkpoint_contents", {"path": str(path)})
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(mode="xb", dir=path.parent, prefix=f".{path.name}.", suffix=".partial", delete=False) as temporary:
        temporary.write(payload)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)
    return path


def write_checksums(root: Path) -> Path:
    root = Path(root)
    files = sorted(
        (
            path
            for path in root.rglob("*")
            if path.is_file() and path.name != "checksums.sha256" and not path.name.endswith(".partial")
        ),
        key=lambda path: path.relative_to(root).as_posix(),
    )
    lines = "".join(f"{sha256_file(path)}  {path.relative_to(root).as_posix()}\n" for path in files)
    return write_text_once(root / "checksums.sha256", lines)

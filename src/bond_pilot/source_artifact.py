"""Qualification of a human-pinned, internal bond-pilot source artifact."""

from __future__ import annotations

from contextlib import contextmanager
import ctypes
from datetime import date, datetime
import errno
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import struct
import sys
from typing import BinaryIO, Iterator, Mapping, Protocol
from urllib.parse import urlsplit
from uuid import uuid4
from zipfile import BadZipFile, ZipFile, ZipInfo

import httpx
import pyarrow.parquet as pq

from .artifacts import commit_partial, partial_path, write_json_once
from .contracts import ArtifactLimits, PilotError, SourceApproval, SourceCandidate


REQUIRED_COLUMNS = ("cusip_id", "trd_exctn_dt", "pr")
OPTIONAL_COLUMNS = (
    "prfull",
    "acclast",
    "ytm",
    "mod_dur",
    "mac_dur",
    "convexity",
    "bond_maturity",
    "credit_spread",
    "qvolume",
    "dvolume",
    "db_type",
)
_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")
_ZIP_DRIVE = re.compile(r"^[A-Za-z]:")
_AT_FDCWD = -100
_RENAME_NOREPLACE = 1


class _HttpClient(Protocol):
    def stream(self, method: str, url: str): ...


def _path_lexists(path: Path) -> bool:
    return os.path.lexists(path)


def _create_attempt_directory(run_dir: Path) -> Path:
    if _path_lexists(run_dir):
        raise PilotError("already_exists", {"path": str(run_dir)})
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(3):
        attempt_dir = run_dir.parent / f".{run_dir.name}.qualification-{uuid4().hex}.partial-dir"
        try:
            attempt_dir.mkdir(mode=0o700)
        except FileExistsError:
            continue
        return attempt_dir
    raise PilotError("attempt_directory_collision", {"path": str(run_dir)})


def _renameat2(old: bytes, new: bytes, flags: int) -> None:
    try:
        renameat2 = ctypes.CDLL(None, use_errno=True).renameat2
    except (AttributeError, OSError) as exc:
        raise OSError(errno.ENOSYS, "renameat2 unavailable") from exc
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    if renameat2(_AT_FDCWD, old, _AT_FDCWD, new, flags) != 0:
        raise OSError(ctypes.get_errno(), "renameat2 failed")


def _publish_directory_no_replace(attempt_dir: Path, run_dir: Path) -> None:
    if _path_lexists(run_dir):
        raise PilotError("already_exists", {"path": str(run_dir)})
    if sys.platform == "win32":
        try:
            os.rename(attempt_dir, run_dir)
        except OSError as exc:
            if _path_lexists(run_dir) or getattr(exc, "winerror", None) in {80, 145, 183}:
                raise PilotError("already_exists", {"path": str(run_dir)}) from exc
            raise PilotError("qualification_publish_failed", {"path": str(run_dir), "reason": str(exc)}) from exc
        return
    if sys.platform != "linux":
        raise PilotError("atomic_no_replace_unavailable", {"path": str(run_dir)})
    try:
        _renameat2(os.fsencode(attempt_dir), os.fsencode(run_dir), _RENAME_NOREPLACE)
    except OSError as exc:
        if exc.errno in {errno.EEXIST, errno.ENOTEMPTY}:
            raise PilotError("already_exists", {"path": str(run_dir)}) from exc
        if exc.errno == errno.ENOSYS:
            raise PilotError("atomic_no_replace_unavailable", {"path": str(run_dir)}) from exc
        raise PilotError("qualification_publish_failed", {"path": str(run_dir), "reason": str(exc)}) from exc


def _raw_member_name(archive_path: Path, info: ZipInfo) -> str:
    with archive_path.open("rb") as archive:
        archive.seek(info.header_offset)
        header = archive.read(30)
        if len(header) != 30:
            raise PilotError("invalid_zip_archive")
        fields = struct.unpack("<IHHHHHIIIHH", header)
        signature = fields[0]
        filename_length = fields[-2]
        if signature != 0x04034B50:
            raise PilotError("invalid_zip_archive")
        encoded_name = archive.read(filename_length)
    try:
        return encoded_name.decode("utf-8" if info.flag_bits & 0x800 else "cp437")
    except UnicodeDecodeError as exc:
        raise PilotError("invalid_zip_member", {"member_name": info.filename}) from exc


def _safe_member(archive_path: Path, info: ZipInfo) -> None:
    name = _raw_member_name(archive_path, info)
    mode = info.external_attr >> 16
    file_type = stat.S_IFMT(mode)
    if (
        info.is_dir()
        or name.endswith("/")
        or "\\" in name
        or name.startswith("/")
        or _ZIP_DRIVE.match(name)
        or file_type not in (0, stat.S_IFREG)
        or not name.endswith(".parquet")
    ):
        raise PilotError("invalid_zip_member", {"member_name": name})
    path = PurePosixPath(name)
    if not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise PilotError("invalid_zip_member", {"member_name": name})


def _stream_to_partial(chunks: Iterator[bytes], final: Path, limit: int) -> tuple[Path, int, str]:
    partial = partial_path(final)
    size = 0
    digest = hashlib.sha256()
    try:
        with partial.open("xb") as output:
            for chunk in chunks:
                if not chunk:
                    continue
                size += len(chunk)
                if size > limit:
                    raise PilotError("archive_size_exceeded", {"limit": limit})
                digest.update(chunk)
                output.write(chunk)
    except Exception:
        partial.unlink(missing_ok=True)
        raise
    return partial, size, digest.hexdigest()


def _local_chunks(path: Path, chunk_size: int) -> Iterator[bytes]:
    with path.open("rb") as source:
        yield from iter(lambda: source.read(chunk_size), b"")


@contextmanager
def _remote_chunks(client: _HttpClient, locator: str, chunk_size: int) -> Iterator[Iterator[bytes]]:
    with client.stream("GET", locator) as response:
        response.raise_for_status()
        yield iter(response.iter_bytes(chunk_size))


def _source_chunks(locator: str, client: _HttpClient | None, chunk_size: int) -> tuple[Iterator[bytes] | None, object | None]:
    parsed = urlsplit(locator)
    if parsed.scheme == "https":
        return None, client or httpx.Client()
    if parsed.scheme and not _WINDOWS_DRIVE.match(locator):
        raise PilotError("unsupported_source_locator", {"locator": locator})
    path = Path(locator)
    if not path.is_file():
        raise PilotError("source_not_found", {"locator": locator})
    return _local_chunks(path, chunk_size), None


def _iso_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise PilotError("invalid_trade_dates")
    try:
        if len(value) == 10:
            return date.fromisoformat(value)
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise PilotError("invalid_trade_dates") from exc
    raise PilotError("invalid_trade_dates")


def _date_bounds(parquet_path: Path, chunk_size: int) -> tuple[str, str]:
    minimum: date | None = None
    maximum: date | None = None
    try:
        with pq.ParquetFile(parquet_path) as parquet:
            for batch in parquet.iter_batches(columns=["trd_exctn_dt"], batch_size=chunk_size):
                for value in batch.column(0).to_pylist():
                    if value is None:
                        continue
                    candidate = _iso_date(value)
                    minimum = candidate if minimum is None or candidate < minimum else minimum
                    maximum = candidate if maximum is None or candidate > maximum else maximum
    except PilotError:
        raise
    except Exception as exc:
        raise PilotError("invalid_trade_dates") from exc
    if minimum is None or maximum is None:
        raise PilotError("invalid_trade_dates")
    return minimum.isoformat(), maximum.isoformat()


def _read_object(path: Path) -> Mapping[str, object]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PilotError("invalid_json_object", {"path": str(path)}) from exc
    if not isinstance(value, dict):
        raise PilotError("invalid_json_object", {"path": str(path)})
    return value


def load_candidate(path: Path) -> SourceCandidate:
    """Load a canonical source-candidate manifest into its frozen contract."""
    return SourceCandidate.from_json_mapping(_read_object(path))


def load_source_approval(path: Path) -> SourceApproval:
    """Load a human approval pin without inferring any legal decision."""
    return SourceApproval.from_json_mapping(_read_object(path))


def verify_source_approval(candidate: SourceCandidate, approval: SourceApproval) -> None:
    """Require the exact pins enforced by the immutable approval contract."""
    approval.validate_for(candidate)


def verify_requalified_candidate(approved: SourceCandidate, requalified: SourceCandidate) -> None:
    """Require a local requalification to preserve every approved source invariant."""
    immutable_fields = (
        "schema_version",
        "artifact_bytes",
        "artifact_sha256",
        "member_name",
        "member_uncompressed_bytes",
        "schema_sha256",
        "schema_columns",
        "schema_optional_columns",
        "row_count",
        "row_group_count",
        "global_start",
        "global_cutoff",
        "duplicate_check_scope",
        "approval_state",
    )
    if any(getattr(approved, field) != getattr(requalified, field) for field in immutable_fields):
        raise PilotError("source_integrity_failed")


def _reparse_point(status: os.stat_result) -> bool:
    return bool(getattr(status, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _local_path(value: str | Path) -> Path:
    locator = str(value)
    normalized = locator.strip()
    if normalized.startswith(("\\\\", "//")) or (urlsplit(normalized).scheme and not _WINDOWS_DRIVE.match(normalized)):
        raise PilotError("source_integrity_failed")
    return Path(locator)


def _regular_file(status: os.stat_result, *, limit: int) -> None:
    if not stat.S_ISREG(status.st_mode) or _reparse_point(status) or status.st_size > limit:
        raise PilotError("source_integrity_failed")


def _fingerprint(status: os.stat_result) -> tuple[int, int, int, int]:
    return status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns


def _open_regular_file(value: str | Path, *, limit: int) -> tuple[Path, os.stat_result, BinaryIO]:
    path = _local_path(value)
    handle: BinaryIO | None = None
    try:
        before = os.lstat(path)
        _regular_file(before, limit=limit)
        handle = path.open("rb")
        opened = os.fstat(handle.fileno())
        _regular_file(opened, limit=limit)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise PilotError("source_integrity_failed")
        return path, before, handle
    except Exception as exc:
        if handle is not None:
            handle.close()
        if isinstance(exc, PilotError):
            raise
        if isinstance(exc, OSError):
            raise PilotError("source_integrity_failed") from exc
        raise


def _digest_open_file(handle: BinaryIO, *, limit: int, chunk_size: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    for chunk in iter(lambda: handle.read(chunk_size), b""):
        total += len(chunk)
        if total > limit:
            raise PilotError("source_integrity_failed")
        digest.update(chunk)
    return digest.hexdigest(), total


def _require_unchanged(handle: BinaryIO, before: os.stat_result, *, total: int) -> None:
    try:
        after = os.fstat(handle.fileno())
    except OSError as exc:
        raise PilotError("source_integrity_failed") from exc
    if _fingerprint(after) != _fingerprint(before) or total != before.st_size:
        raise PilotError("source_integrity_failed")


def _regular_file_digest(path: str | Path, *, limit: int, chunk_size: int) -> str:
    _, before, handle = _open_regular_file(path, limit=limit)
    try:
        digest, total = _digest_open_file(handle, limit=limit, chunk_size=chunk_size)
        _require_unchanged(handle, before, total=total)
        return digest
    finally:
        handle.close()


def verify_matching_extracted_files(original: str | Path, verified: str | Path, *, limit: int, chunk_size: int) -> None:
    """Require the approved extraction and staged re-extraction to be identical regular files."""
    if _regular_file_digest(original, limit=limit, chunk_size=chunk_size) != _regular_file_digest(verified, limit=limit, chunk_size=chunk_size):
        raise PilotError("source_integrity_failed")


def capture_local_archive(archive_path: str | Path, capture_path: Path, *, expected_sha256: str, limits: ArtifactLimits = ArtifactLimits()) -> None:
    """Copy a local archive once while binding its bytes to its approved SHA-256."""
    _, before, source = _open_regular_file(archive_path, limit=limits.archive_bytes)
    partial = partial_path(capture_path)
    try:
        try:
            with partial.open("xb") as captured:
                digest = hashlib.sha256()
                total = 0
                for chunk in iter(lambda: source.read(limits.streaming_chunk_bytes), b""):
                    total += len(chunk)
                    if total > limits.archive_bytes:
                        raise PilotError("source_integrity_failed")
                    digest.update(chunk)
                    captured.write(chunk)
            _require_unchanged(source, before, total=total)
            if digest.hexdigest() != expected_sha256:
                raise PilotError("source_integrity_failed")
            commit_partial(partial, capture_path)
        except Exception:
            partial.unlink(missing_ok=True)
            raise
    finally:
        source.close()


@contextmanager
def open_verified_extracted_file(original: str | Path, verified: str | Path, *, limit: int, chunk_size: int) -> Iterator[BinaryIO]:
    """Yield a duplicate descriptor for the staged Parquet while retaining its guarded original handle."""
    expected_digest = _regular_file_digest(original, limit=limit, chunk_size=chunk_size)
    _, before, source = _open_regular_file(verified, limit=limit)
    duplicate: BinaryIO | None = None
    try:
        digest, total = _digest_open_file(source, limit=limit, chunk_size=chunk_size)
        _require_unchanged(source, before, total=total)
        if digest != expected_digest:
            raise PilotError("source_integrity_failed")
        source.seek(0)
        try:
            duplicate = os.fdopen(os.dup(source.fileno()), "rb")
        except OSError as exc:
            raise PilotError("source_integrity_failed") from exc
        yield duplicate
    finally:
        try:
            if duplicate is not None:
                duplicate.close()
        finally:
            try:
                _require_unchanged(source, before, total=before.st_size)
            finally:
                source.close()


def qualify_local_source(archive_path: str | Path, run_dir: Path, *, capture_path: Path, expected_sha256: str, limits: ArtifactLimits = ArtifactLimits()) -> SourceCandidate:
    """Requalify an already-local archive without allowing a network locator."""
    capture_local_archive(archive_path, capture_path, expected_sha256=expected_sha256, limits=limits)
    try:
        return qualify_source(capture_path, run_dir, expected_sha256=expected_sha256, limits=limits)
    finally:
        capture_path.unlink(missing_ok=True)


def qualify_source(
    locator: str | Path,
    run_dir: Path,
    expected_sha256: str | None = None,
    limits: ArtifactLimits = ArtifactLimits(),
    client: _HttpClient | None = None,
) -> SourceCandidate:
    """Acquire and inspect one ZIP-wrapped Parquet source for internal review."""
    locator_text = str(locator)
    run_dir = Path(run_dir)
    attempt_dir = _create_attempt_directory(run_dir)
    archive_final = attempt_dir / "source.zip"
    extracted_final = attempt_dir / "source.parquet"
    published = False
    default_client: httpx.Client | None = None
    try:
        local_chunks, remote_client = _source_chunks(locator_text, client, limits.streaming_chunk_bytes)
        if remote_client is None:
            assert local_chunks is not None
            partial, archive_bytes, artifact_sha = _stream_to_partial(local_chunks, archive_final, limits.archive_bytes)
        else:
            if client is None:
                default_client = remote_client  # type: ignore[assignment]
            with _remote_chunks(remote_client, locator_text, limits.streaming_chunk_bytes) as chunks:
                partial, archive_bytes, artifact_sha = _stream_to_partial(chunks, archive_final, limits.archive_bytes)
        if expected_sha256 is not None and artifact_sha != expected_sha256:
            partial.unlink(missing_ok=True)
            raise PilotError("artifact_sha256_mismatch", {"expected": expected_sha256, "actual": artifact_sha})
        try:
            commit_partial(partial, archive_final)
        except Exception:
            partial.unlink(missing_ok=True)
            raise

        try:
            with ZipFile(archive_final) as archive:
                members = archive.infolist()
                if len(members) != 1:
                    raise PilotError("invalid_zip_member_count", {"count": len(members)})
                member = members[0]
                _safe_member(archive_final, member)
                if member.file_size > limits.member_uncompressed_bytes:
                    raise PilotError("member_size_exceeded", {"limit": limits.member_uncompressed_bytes})
                partial = partial_path(extracted_final)
                extracted_bytes = 0
                try:
                    with archive.open(member) as source, partial.open("xb") as output:
                        for chunk in iter(lambda: source.read(limits.streaming_chunk_bytes), b""):
                            extracted_bytes += len(chunk)
                            if extracted_bytes > limits.member_uncompressed_bytes:
                                raise PilotError("member_size_exceeded", {"limit": limits.member_uncompressed_bytes})
                            output.write(chunk)
                    commit_partial(partial, extracted_final)
                except Exception:
                    partial.unlink(missing_ok=True)
                    raise
        except PilotError:
            raise
        except BadZipFile as exc:
            raise PilotError("invalid_zip_archive") from exc

        with pq.ParquetFile(extracted_final) as parquet:
            columns = tuple(parquet.schema_arrow.names)
            missing = [column for column in REQUIRED_COLUMNS if column not in columns]
            if missing:
                raise PilotError("missing_required_columns", {"columns": missing})
            schema_sha256 = hashlib.sha256(parquet.schema_arrow.serialize().to_pybytes()).hexdigest()
            row_count = parquet.metadata.num_rows
            row_group_count = parquet.metadata.num_row_groups
        global_start, global_cutoff = _date_bounds(extracted_final, limits.streaming_chunk_bytes)
        candidate = SourceCandidate(
            schema_version="source-candidate-v1",
            source_locator=locator_text,
            local_archive_path=str(run_dir / "source.zip"),
            local_extracted_path=str(run_dir / "source.parquet"),
            artifact_bytes=archive_bytes,
            artifact_sha256=artifact_sha,
            member_name=member.filename,
            member_uncompressed_bytes=extracted_bytes,
            schema_sha256=schema_sha256,
            schema_columns=columns,
            schema_optional_columns=tuple(column for column in OPTIONAL_COLUMNS if column in columns),
            row_count=row_count,
            row_group_count=row_group_count,
            global_start=global_start,
            global_cutoff=global_cutoff,
            duplicate_check_scope="not_checked_during_qualification",
        )
        manifest = attempt_dir / "source-manifest.json"
        report = attempt_dir / "qualification-report.json"
        write_json_once(manifest, candidate.to_json_mapping())
        write_json_once(
            report,
            {
                "internal_only": True,
                "approval_state": "unapproved",
                "local_use_allowed": False,
                "redistribution_decision": "not_evaluated",
                "human_approval_required": True,
            },
        )
        if default_client is not None:
            default_client.close()
            default_client = None
        _publish_directory_no_replace(attempt_dir, run_dir)
        published = True
        return candidate
    finally:
        try:
            if default_client is not None:
                default_client.close()
        finally:
            if not published:
                shutil.rmtree(attempt_dir, ignore_errors=True)

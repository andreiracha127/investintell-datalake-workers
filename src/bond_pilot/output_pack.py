"""Completion-sealed internal output packs built only through retained handles.

If marker rollback itself cannot be durably completed, this process poisons the
pack. Filesystem-only validation cannot make that condition restart-safe; a
strict cross-restart recovery decision requires an external durability ledger.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import PurePosixPath
import stat
from typing import Iterator
from uuid import uuid4

from ._secure_local_fs import CreatedFile, SecureDirectory, SecureFile
from .artifacts import canonical_json_bytes
from .contracts import PilotError


_CHECKSUMS = "checksums.sha256"
_COMPLETION = "completion.json"
_MAX_CONTROL_BYTES = 16 * 1024 * 1024
_MAX_PAYLOAD_BYTES = 1024**4
_RESERVED = frozenset({_CHECKSUMS, _COMPLETION})
_COMPLETION_KEYS = frozenset({"schema_version", "pack_schema_version", "run_id", "files", "checksums_sha256", "producer_version", "completed_at"})
_POISONED_PACKS: set[tuple[tuple[object, ...], str]] = set()


def _incomplete() -> PilotError:
    return PilotError("incomplete_output")


def _relative_name(value: str) -> tuple[str, ...]:
    if not isinstance(value, str) or not value or "\\" in value:
        raise PilotError("invalid_output_name")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise PilotError("invalid_output_name")
    if path.parts[-1] in _RESERVED:
        raise PilotError("invalid_output_name")
    return path.parts


def _sha256(file: SecureFile) -> str:
    digest = hashlib.sha256()
    for chunk in file.iter_chunks(1024**2, max_bytes=_MAX_PAYLOAD_BYTES):
        digest.update(chunk)
    return digest.hexdigest()


def _read(file: SecureFile, *, maximum: int = _MAX_CONTROL_BYTES) -> bytes:
    try:
        return file.read_all(max_bytes=maximum, too_large_code="incomplete_output")
    except PilotError as exc:
        if exc.code == "incomplete_output":
            raise
        raise _incomplete() from exc


def _json(raw: bytes) -> dict[str, object]:
    def duplicate(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    def nonfinite(value: str) -> object:
        raise ValueError(value)

    try:
        value = json.loads(raw, object_pairs_hook=duplicate, parse_constant=nonfinite)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _incomplete() from exc
    if not isinstance(value, dict) or canonical_json_bytes(value) != raw:
        raise _incomplete()
    return value


def _check_parent(parent: SecureDirectory) -> None:
    if os.name == "nt":
        if parent._backend.api.filesystem_name(parent.native_handle).upper() != "NTFS":
            raise PilotError("durability_error")
        return
    try:
        status = os.fstat(parent.native_handle)
    except OSError as exc:
        raise PilotError("unsafe_parent") from exc
    if status.st_uid != os.geteuid() or status.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise PilotError("unsafe_parent")


def _pack_key(parent: SecureDirectory, run_id: str) -> tuple[tuple[object, ...], str]:
    return (parent.stable_identity(), run_id)


def _temporary_name() -> str:
    return f".pending-{uuid4().hex}"


@dataclass(frozen=True)
class CompletedPack:
    run_id: str
    files: tuple[str, ...]
    completion: dict[str, object]


class _PayloadWriter:
    def __init__(self, pack: OutputPack, directory: SecureDirectory, name: str, relative: str, created: CreatedFile) -> None:
        self._pack = pack
        self._directory = directory
        self._name = name
        self._relative = relative
        self._created = created
        self._closed = False
        pack._open_writers.add(self)

    @property
    def closed(self) -> bool:
        return self._closed

    def __getattr__(self, name: str) -> object:
        return getattr(self._created, name)

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._pack._publish_payload(self._directory, self._name, self._relative, self._created)
        except BaseException:
            self.abort()
            raise
        else:
            self._created.close()
            self._closed = True
            self._pack._open_writers.discard(self)
            if self._relative not in self._pack._payloads:
                self._pack._reserved_payloads.discard(self._relative)

    def abort(self) -> None:
        if self._closed:
            return
        failure: BaseException | None = None
        try:
            self._created.close()
        except BaseException as exc:
            failure = exc
        if self._created.name:
            try:
                self._directory.unlink_file(self._created.name, error_code="incomplete_output")
            except BaseException as exc:
                failure = failure or exc
        self._closed = True
        self._pack._open_writers.discard(self)
        self._pack._reserved_payloads.discard(self._relative)
        if failure is not None:
            raise PilotError("indeterminate_durability") from failure

    def __enter__(self) -> _PayloadWriter:
        return self

    def __exit__(self, exc_type: object, *_exc: object) -> None:
        if exc_type is None:
            self.close()
            return
        self.abort()


class OutputPack:
    """A private run directory which becomes readable only after completion publication."""

    def __init__(self, parent: SecureDirectory, directory: SecureDirectory, *, run_id: str, pack_schema_version: str, producer_version: str) -> None:
        self._parent = parent
        self._stable_key = _pack_key(parent, run_id)
        self.directory = directory
        self.run_id = run_id
        self.pack_schema_version = pack_schema_version
        self.producer_version = producer_version
        self.state = "BUILDING"
        self._payloads: set[str] = set()
        self._reserved_payloads: set[str] = set()
        self._open_writers: set[_PayloadWriter] = set()
        self._directories: dict[tuple[str, ...], SecureDirectory] = {(): directory}

    @classmethod
    def create(cls, parent: SecureDirectory, *, run_id: str, pack_schema_version: str, producer_version: str) -> OutputPack:
        if not all(isinstance(value, str) and value for value in (run_id, pack_schema_version, producer_version)):
            raise PilotError("invalid_output_pack")
        _relative_name(run_id)
        if _pack_key(parent, run_id) in _POISONED_PACKS:
            raise PilotError("indeterminate_durability")
        _check_parent(parent)
        directory = parent.create_private_directory_no_replace(run_id, error_code="unsafe_parent")
        return cls(parent, directory, run_id=run_id, pack_schema_version=pack_schema_version, producer_version=producer_version)

    def _require_building(self) -> None:
        if self.state != "BUILDING":
            raise PilotError("output_pack_frozen")

    def _directory_for(self, parts: tuple[str, ...]) -> SecureDirectory:
        directory = self.directory
        for depth, part in enumerate(parts):
            prefix = parts[: depth + 1]
            directory = self._directories.get(prefix) or directory.create_private_directory_no_replace(part, error_code="incomplete_output")
            self._directories[prefix] = directory
        return directory

    def create_payload(self, relative: str) -> _PayloadWriter:
        self._require_building()
        parts = _relative_name(relative)
        normalized = "/".join(parts)
        if normalized in self._reserved_payloads:
            raise PilotError("already_exists")
        self._reserved_payloads.add(normalized)
        directory = self._directory_for(parts[:-1])
        try:
            created = directory.create_file(_temporary_name(), error_code="incomplete_output")
            return _PayloadWriter(self, directory, parts[-1], normalized, created)
        except Exception:
            self._reserved_payloads.discard(normalized)
            raise

    def write_payload(self, relative: str, contents: bytes) -> None:
        if not isinstance(contents, bytes):
            raise PilotError("invalid_output_payload")
        with self.create_payload(relative) as output:
            output.write(contents)

    def _publish_payload(self, directory: SecureDirectory, name: str, relative: str, created: CreatedFile) -> None:
        self._require_building()
        if relative in self._payloads:
            raise PilotError("already_exists")
        directory.publish_no_replace(created, name, error_code="incomplete_output")
        self._payloads.add(relative)

    def _write_control(self, name: str, contents: bytes) -> None:
        created = self.directory.create_file(_temporary_name(), error_code="incomplete_output")
        try:
            created.write(contents)
            self.directory.publish_no_replace(created, name, error_code="incomplete_output")
        finally:
            created.close()

    def _publish_completion(self, contents: bytes) -> None:
        created = self.directory.create_file(_temporary_name(), error_code="durability_error")
        temporary_name = created.name
        try:
            created.write(contents)
            self.directory.publish_no_replace(created, _COMPLETION, error_code="durability_error")
        except Exception as publish_error:
            cleanup_failure: BaseException | None = None
            for cleanup in (
                created.close,
                lambda: self.directory.unlink_file(_COMPLETION, error_code="durability_error"),
                *((lambda: self.directory.unlink_file(temporary_name, error_code="durability_error"),) if temporary_name else ()),
                lambda: self.directory.flush(error_code="durability_error"),
            ):
                try:
                    cleanup()
                except BaseException as cleanup_error:
                    cleanup_failure = cleanup_failure or cleanup_error
            if cleanup_failure is not None:
                self._poison_and_close()
                raise PilotError("indeterminate_durability") from cleanup_failure
            raise PilotError("durability_error") from publish_error
        try:
            created.close()
        except BaseException as close_error:
            self._poison_and_close()
            raise PilotError("indeterminate_durability") from close_error

    def _poison_and_close(self) -> None:
        self.state = "POISONED"
        _POISONED_PACKS.add(self._stable_key)
        for writer in tuple(self._open_writers):
            try:
                writer.abort()
            except BaseException:
                pass
        for directory in sorted(self._directories.values(), key=lambda item: len(item.path.parts), reverse=True):
            try:
                directory.close()
            except BaseException:
                pass

    def finalize(self, *, completed_at: str | None = None) -> CompletedPack:
        self._require_building()
        if self._open_writers:
            raise PilotError("output_pack_open_writer")
        payloads = _collect_payloads(self.directory)
        if tuple(sorted(self._payloads)) != payloads:
            raise _incomplete()
        checksums = _checksums_for(self.directory, payloads)
        self._write_control(_CHECKSUMS, checksums)
        self.state = "CHECKSUMMED"
        timestamp = completed_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        files = tuple(sorted((*payloads, _CHECKSUMS, _COMPLETION)))
        completion = {
            "schema_version": "output-completion-v1",
            "pack_schema_version": self.pack_schema_version,
            "run_id": self.run_id,
            "files": list(files),
            "checksums_sha256": hashlib.sha256(checksums).hexdigest(),
            "producer_version": self.producer_version,
            "completed_at": timestamp,
        }
        _flush_directories(self._directories)
        self._parent.flush(error_code="durability_error")
        self.state = "DURABILITY_PENDING"
        self._publish_completion(canonical_json_bytes(completion))
        self.state = "COMMITTED"
        return CompletedPack(self.run_id, files, completion)

    def close(self) -> None:
        failure: BaseException | None = None
        for writer in tuple(self._open_writers):
            try:
                writer.abort()
            except BaseException as exc:
                failure = failure or exc
        if failure is not None:
            self.state = "POISONED"
            _POISONED_PACKS.add(self._stable_key)
        if self.state == "COMMITTED":
            self.state = "CLOSED"
        for directory in sorted(self._directories.values(), key=lambda item: len(item.path.parts), reverse=True):
            directory.close()
        if failure is not None:
            raise PilotError("indeterminate_durability") from failure


def _collect_payloads(directory: SecureDirectory, prefix: tuple[str, ...] = ()) -> tuple[str, ...]:
    found: list[str] = []
    try:
        directory.validate_private(error_code="incomplete_output")
        names = directory.enumerate()
    except PilotError as exc:
        raise _incomplete() from exc
    if len(names) != len(set(names)):
        raise _incomplete()
    for name in names:
        if not isinstance(name, str) or not name or "/" in name or "\\" in name:
            raise _incomplete()
        relative = "/".join((*prefix, name))
        try:
            with directory.open_file(name, error_code="incomplete_output") as file:
                directory.validate_private_file(file, error_code="incomplete_output")
                if os.name != "nt" and os.fstat(file.native_handle).st_nlink != 1:
                    raise _incomplete()
                if name in _RESERVED:
                    if prefix:
                        raise _incomplete()
                    continue
                found.append(relative)
            continue
        except PilotError:
            pass
        try:
            with directory.open_dir(name, error_code="incomplete_output") as child:
                nested = _collect_payloads(child, (*prefix, name))
                if not nested:
                    raise _incomplete()
                found.extend(nested)
        except PilotError as exc:
            raise _incomplete() from exc
    return tuple(sorted(found))


def _checksums_for(directory: SecureDirectory, payloads: tuple[str, ...]) -> bytes:
    lines: list[str] = []
    for relative in payloads:
        parts = relative.split("/")
        current = directory
        opened: list[SecureDirectory] = []
        try:
            for part in parts[:-1]:
                current = current.open_dir(part, error_code="incomplete_output")
                opened.append(current)
            with current.open_file(parts[-1], error_code="incomplete_output") as file:
                current.validate_private_file(file, error_code="incomplete_output")
                if os.name != "nt" and os.fstat(file.native_handle).st_nlink != 1:
                    raise _incomplete()
                lines.append(f"{_sha256(file)}  {relative}\\n")
        finally:
            for child in reversed(opened):
                child.close()
    return "".join(lines).encode("ascii")


def _flush_directories(directories: dict[tuple[str, ...], SecureDirectory]) -> None:
    try:
        for parts in sorted(directories, key=len, reverse=True):
            directories[parts].flush(error_code="durability_error")
    except PilotError as exc:
        raise PilotError("durability_error") from exc


def _validate_open_pack(directory: SecureDirectory, run_id: str, expected_pack_schema_version: str, expected_payloads: tuple[str, ...] | None) -> CompletedPack:
    payloads = _collect_payloads(directory)
    if expected_payloads is not None and payloads != tuple(sorted(expected_payloads)):
        raise _incomplete()
    names = set(directory.enumerate())
    if _CHECKSUMS not in names or _COMPLETION not in names:
        raise _incomplete()
    with directory.open_file(_CHECKSUMS, error_code="incomplete_output") as checksums_file:
        directory.validate_private_file(checksums_file, error_code="incomplete_output")
        checksums = _read(checksums_file)
    with directory.open_file(_COMPLETION, error_code="incomplete_output") as completion_file:
        directory.validate_private_file(completion_file, error_code="incomplete_output")
        completion = _json(_read(completion_file))
    expected_checksums = _checksums_for(directory, payloads)
    if checksums != expected_checksums:
        raise _incomplete()
    files = tuple(sorted((*payloads, _CHECKSUMS, _COMPLETION)))
    if set(completion) != _COMPLETION_KEYS:
        raise _incomplete()
    if completion["schema_version"] != "output-completion-v1" or completion["run_id"] != run_id or completion["pack_schema_version"] != expected_pack_schema_version:
        raise _incomplete()
    if completion["files"] != list(files) or completion["checksums_sha256"] != hashlib.sha256(checksums).hexdigest():
        raise _incomplete()
    if not all(isinstance(completion[key], str) and completion[key] for key in ("producer_version", "completed_at")):
        raise _incomplete()
    return CompletedPack(run_id, files, completion)


@contextmanager
def open_validated_output_pack(
    parent: SecureDirectory,
    run_id: str,
    *,
    expected_pack_schema_version: str,
    expected_payloads: tuple[str, ...] | None = None,
) -> Iterator[tuple[SecureDirectory, CompletedPack]]:
    """Validate and yield the same retained run-directory capability."""
    _relative_name(run_id)
    if not isinstance(expected_pack_schema_version, str) or not expected_pack_schema_version:
        raise PilotError("incomplete_output")
    if expected_payloads is not None:
        normalized = tuple(sorted("/".join(_relative_name(name)) for name in expected_payloads))
        if len(normalized) != len(set(normalized)):
            raise _incomplete()
        expected_payloads = normalized
    if _pack_key(parent, run_id) in _POISONED_PACKS:
        raise PilotError("indeterminate_durability")
    _check_parent(parent)
    directory: SecureDirectory | None = None
    try:
        directory = parent.open_dir(run_id, error_code="incomplete_output")
        completed = _validate_open_pack(directory, run_id, expected_pack_schema_version, expected_payloads)
    except PilotError as exc:
        if directory is not None:
            directory.close()
        if exc.code in {"incomplete_output", "indeterminate_durability"}:
            raise
        raise _incomplete() from exc
    try:
        yield directory, completed
    finally:
        directory.close()


def validate_output_pack(parent: SecureDirectory, run_id: str, *, expected_pack_schema_version: str, expected_payloads: tuple[str, ...] | None = None) -> CompletedPack:
    """Validate a completed pack and close its retained run handle."""
    with open_validated_output_pack(parent, run_id, expected_pack_schema_version=expected_pack_schema_version, expected_payloads=expected_payloads) as (_directory, completed):
        return completed

from __future__ import annotations

import errno
import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.bond_pilot.contracts import ArtifactLimits, PilotError, SourceApproval, SourceCandidate
from src.bond_pilot import source_artifact
from src.bond_pilot.source_artifact import (
    load_candidate,
    load_source_approval,
    qualify_source,
    verify_source_approval,
)

from conftest import FakeClient


def test_qualifies_local_nested_parquet_and_writes_unapproved_internal_manifests(make_source_zip, tmp_path: Path) -> None:
    archive = make_source_zip(
        columns={
            "cusip_id": ["123456789", "987654321"],
            "trd_exctn_dt": ["2024-01-03", "2024-02-05"],
            "pr": [101.25, 99.75],
            "ytm": [None, 0.052],
            "qvolume": [10, 20],
        }
    )
    run_dir = tmp_path / "run"

    candidate = qualify_source(str(archive), run_dir)

    assert candidate.source_locator == str(archive)
    assert candidate.member_name == "nested/source.parquet"
    assert candidate.schema_columns == ("cusip_id", "trd_exctn_dt", "pr", "ytm", "qvolume")
    assert candidate.schema_optional_columns == ("ytm", "qvolume")
    assert candidate.global_start == "2024-01-03"
    assert candidate.global_cutoff == "2024-02-05"
    assert candidate.approval_state == "unapproved"
    assert (run_dir / "source.zip").is_file()
    assert (run_dir / "source.parquet").is_file()
    assert load_candidate(run_dir / "source-manifest.json") == candidate
    report = json.loads((run_dir / "qualification-report.json").read_text(encoding="utf-8"))
    assert report == {
        "approval_state": "unapproved",
        "human_approval_required": True,
        "internal_only": True,
        "local_use_allowed": False,
        "redistribution_decision": "not_evaluated",
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("artifact_bytes", 999),
        ("artifact_sha256", "0" * 64),
        ("member_name", "other.parquet"),
        ("member_uncompressed_bytes", 999),
        ("schema_sha256", "0" * 64),
        ("schema_columns", ("cusip_id", "pr", "trd_exctn_dt")),
        ("schema_optional_columns", ("ytm",)),
        ("row_count", 999),
        ("row_group_count", 999),
        ("global_start", "2020-01-01"),
        ("global_cutoff", "2020-01-01"),
        ("duplicate_check_scope", "different_scope"),
    ],
)
def test_requalified_candidate_requires_every_immutable_approved_invariant(field: str, value: object, make_source_zip, tmp_path: Path) -> None:
    approved = qualify_source(make_source_zip(), tmp_path / "approved")
    forged = SourceCandidate(**{**approved.to_json_mapping(), field: value})

    compare = getattr(source_artifact, "verify_requalified_candidate", None)
    assert compare is not None
    with pytest.raises(PilotError, match="source_integrity_failed"):
        compare(approved, forged)


@pytest.mark.parametrize("unsafe", ["missing", "directory", "oversized"])
def test_matching_extracted_files_rejects_unsafe_original(unsafe: str, tmp_path: Path) -> None:
    original = tmp_path / "original.parquet"
    verified = tmp_path / "verified.parquet"
    verified.write_bytes(b"same")
    if unsafe == "directory":
        original.mkdir()
    elif unsafe == "oversized":
        original.write_bytes(b"oversized")

    with pytest.raises(PilotError, match="^source_integrity_failed$") as error:
        source_artifact.verify_matching_extracted_files(original, verified, limit=4, chunk_size=2)

    assert error.value.details == {}


def test_matching_extracted_files_detects_a_file_race(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = tmp_path / "original.parquet"
    verified = tmp_path / "verified.parquet"
    original.write_bytes(b"same")
    verified.write_bytes(b"same")
    real_fstat = source_artifact.os.fstat
    calls = 0

    def raced_fstat(fd: int):
        nonlocal calls
        calls += 1
        current = real_fstat(fd)
        if calls == 2:
            return SimpleNamespace(
                st_mode=current.st_mode,
                st_dev=current.st_dev,
                st_ino=current.st_ino,
                st_size=current.st_size,
                st_mtime_ns=current.st_mtime_ns + 1,
            )
        return current

    monkeypatch.setattr(source_artifact.os, "fstat", raced_fstat)
    with pytest.raises(PilotError, match="^source_integrity_failed$"):
        source_artifact.verify_matching_extracted_files(original, verified, limit=4, chunk_size=2)


def test_local_archive_capture_rejects_a_symlink_before_opening(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    capture = getattr(source_artifact, "capture_local_archive", None)
    assert capture is not None
    link = tmp_path / "archive-link.zip"
    destination = tmp_path / "capture.zip"
    monkeypatch.setattr(source_artifact.os, "lstat", lambda _path: SimpleNamespace(st_mode=stat.S_IFLNK, st_size=0))
    monkeypatch.setattr(source_artifact.Path, "open", lambda *_args, **_kwargs: pytest.fail("symlink must not be opened"))

    with pytest.raises(PilotError, match="^source_integrity_failed$"):
        capture(link, destination, expected_sha256="a" * 64)

    assert not destination.exists()


def test_local_archive_capture_rejects_a_reparse_point_before_opening(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = tmp_path / "archive.zip"
    destination = tmp_path / "capture.zip"
    regular = SimpleNamespace(st_mode=stat.S_IFREG, st_size=1)
    monkeypatch.setattr(source_artifact.os, "lstat", lambda _path: regular)
    monkeypatch.setattr(source_artifact, "_reparse_point", lambda _status: True)
    monkeypatch.setattr(source_artifact.Path, "open", lambda *_args, **_kwargs: pytest.fail("reparse point must not be opened"))

    with pytest.raises(PilotError, match="^source_integrity_failed$"):
        source_artifact.capture_local_archive(archive, destination, expected_sha256="a" * 64)

    assert not destination.exists()


def test_qualifies_https_only_through_injected_client(make_source_zip, tmp_path: Path) -> None:
    archive = make_source_zip()
    client = FakeClient(archive.read_bytes())

    candidate = qualify_source("https://example.test/source.zip", tmp_path / "run", client=client)

    assert candidate.source_locator == "https://example.test/source.zip"
    assert client.urls == ["https://example.test/source.zip"]
    with pytest.raises(PilotError, match="unsupported_source_locator"):
        qualify_source("http://example.test/source.zip", tmp_path / "http", client=client)


def test_rejects_wrong_sha_and_removes_outputs_from_failed_attempt(make_source_zip, tmp_path: Path) -> None:
    archive = make_source_zip()
    run_dir = tmp_path / "run"

    with pytest.raises(PilotError, match="artifact_sha256_mismatch"):
        qualify_source(str(archive), run_dir, expected_sha256="0" * 64)

    assert not run_dir.exists()


def test_collision_keeps_existing_output_and_cleans_attempt_partial(make_source_zip, tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "source.zip").write_bytes(b"pre-existing")

    with pytest.raises(PilotError, match="already_exists"):
        qualify_source(str(make_source_zip()), run_dir)

    assert (run_dir / "source.zip").read_bytes() == b"pre-existing"
    assert not list(run_dir.glob("*.partial"))


def test_rejects_existing_empty_final_directory_without_removing_it(make_source_zip, tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    with pytest.raises(PilotError, match="already_exists"):
        qualify_source(str(make_source_zip()), run_dir)

    assert run_dir.is_dir()
    assert not list(run_dir.iterdir())
    assert not list(tmp_path.glob(".run.qualification-*.partial-dir"))


def test_lexists_counts_a_dangling_final_symlink(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    try:
        run_dir.symlink_to(tmp_path / "missing-target", target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    assert source_artifact._path_lexists(run_dir)


def test_linux_no_replace_uses_renameat2_flag_and_translates_collision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    attempt_dir = tmp_path / ".run.qualification-test.partial-dir"
    run_dir = tmp_path / "run"
    calls: list[tuple[bytes, bytes, int]] = []

    def collision(old: bytes, new: bytes, flags: int) -> None:
        calls.append((old, new, flags))
        raise OSError(errno.EEXIST, "already exists")

    monkeypatch.setattr(os.sys, "platform", "linux")
    monkeypatch.setattr(source_artifact, "_renameat2", collision, raising=False)

    with pytest.raises(PilotError, match="already_exists"):
        source_artifact._publish_directory_no_replace(attempt_dir, run_dir)

    assert calls == [(os.fsencode(attempt_dir), os.fsencode(run_dir), 1)]


def test_late_write_failure_leaves_no_final_or_attempt_directory(make_source_zip, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = tmp_path / "run"
    original_write_json_once = source_artifact.write_json_once

    def fail_late(path: Path, value: object) -> Path:
        if path.name == "qualification-report.json":
            raise RuntimeError("injected later failure")
        return original_write_json_once(path, value)

    monkeypatch.setattr(source_artifact, "write_json_once", fail_late)

    with pytest.raises(RuntimeError, match="injected later failure"):
        qualify_source(str(make_source_zip()), run_dir)

    assert not run_dir.exists()
    assert not list(tmp_path.glob(".run.qualification-*.partial-dir"))


@pytest.mark.parametrize("platform", ["win32", "linux"])
def test_publication_collision_preserves_competing_run_directory(make_source_zip, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, platform: str) -> None:
    run_dir = tmp_path / "run"

    def create_competing_final(target: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> None:
        target_path = Path(target)
        target_path.mkdir()
        (target_path / "source.parquet").write_bytes(b"replacement")

    monkeypatch.setattr(source_artifact.sys, "platform", platform)
    if platform == "win32":
        def windows_collision(_source: str | bytes | os.PathLike[str] | os.PathLike[bytes], target: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> None:
            create_competing_final(target)
            raise FileExistsError(target)

        monkeypatch.setattr(os, "rename", windows_collision)
    else:
        def linux_collision(_old: bytes, target: bytes, _flags: int) -> None:
            create_competing_final(os.fsdecode(target))
            raise OSError(errno.EEXIST, "already exists")

        monkeypatch.setattr(source_artifact, "_renameat2", linux_collision)

    with pytest.raises(PilotError, match="already_exists"):
        qualify_source(str(make_source_zip()), run_dir)

    assert (run_dir / "source.parquet").read_bytes() == b"replacement"
    assert not list(tmp_path.glob(".run.qualification-*.partial-dir"))


def test_unsupported_platform_fails_closed_without_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    attempt_dir = tmp_path / ".run.qualification-test.partial-dir"
    attempt_dir.mkdir()
    run_dir = tmp_path / "run"
    monkeypatch.setattr(source_artifact.sys, "platform", "darwin")

    with pytest.raises(PilotError, match="atomic_no_replace_unavailable"):
        source_artifact._publish_directory_no_replace(attempt_dir, run_dir)

    assert attempt_dir.is_dir()
    assert not run_dir.exists()


def test_enforces_streaming_archive_and_member_caps(make_source_zip, tmp_path: Path) -> None:
    archive = make_source_zip()

    with pytest.raises(PilotError, match="archive_size_exceeded"):
        qualify_source(str(archive), tmp_path / "archive-cap", limits=ArtifactLimits(archive_bytes=1, member_uncompressed_bytes=10_000, streaming_chunk_bytes=3))
    with pytest.raises(PilotError, match="member_size_exceeded"):
        qualify_source(str(archive), tmp_path / "member-cap", limits=ArtifactLimits(archive_bytes=10_000, member_uncompressed_bytes=1, streaming_chunk_bytes=3))


@pytest.mark.parametrize(
    ("member_name", "attributes"),
    [
        ("../source.parquet", 0),
        ("nested\\source.parquet", 0),
        ("C:/source.parquet", 0),
        ("C:source.parquet", 0),
        ("C:nested/source.parquet", 0),
        ("//server/share/source.parquet", 0),
        ("/source.parquet", 0),
        ("nested/", 0),
        ("nested/source.txt", 0),
        ("nested/source.parquet", stat.S_IFLNK << 16),
        ("nested/source.parquet", stat.S_IFDIR << 16),
        ("nested/source.parquet", stat.S_IFIFO << 16),
    ],
)
def test_rejects_unsafe_or_non_parquet_zip_members(make_source_zip, tmp_path: Path, member_name: str, attributes: int) -> None:
    archive = make_source_zip(member_name=member_name, member_attributes=attributes)

    with pytest.raises(PilotError, match="invalid_zip_member"):
        qualify_source(str(archive), tmp_path / "run")


def test_rejects_multiple_or_extra_zip_members(make_source_zip, tmp_path: Path) -> None:
    archive = make_source_zip(extra_entries=[("extra.txt", b"nope", 0)])

    with pytest.raises(PilotError, match="invalid_zip_member_count"):
        qualify_source(str(archive), tmp_path / "run")


def test_requires_columns_and_uses_fixed_optional_order(make_source_zip, tmp_path: Path) -> None:
    missing = make_source_zip(columns={"cusip_id": ["123456789"], "trd_exctn_dt": ["2024-01-03"]})
    with pytest.raises(PilotError, match="missing_required_columns"):
        qualify_source(str(missing), tmp_path / "missing")
    present = make_source_zip(
        columns={
            "cusip_id": ["123456789"],
            "trd_exctn_dt": ["2024-01-03"],
            "pr": [1.0],
            "dvolume": [1],
            "prfull": [1.1],
            "credit_spread": [2.0],
        }
    )
    candidate = qualify_source(str(present), tmp_path / "present")
    assert candidate.schema_optional_columns == ("prfull", "credit_spread", "dvolume")


@pytest.mark.parametrize("dates", [[None, None], ["not-a-date"]])
def test_rejects_null_only_and_invalid_trade_dates(make_source_zip, tmp_path: Path, dates: list[object]) -> None:
    archive = make_source_zip(columns={"cusip_id": ["123456789"] * len(dates), "trd_exctn_dt": dates, "pr": [1.0] * len(dates)})

    with pytest.raises(PilotError, match="invalid_trade_dates"):
        qualify_source(str(archive), tmp_path / "run")


def test_load_and_verify_human_approval_exactly_pins_candidate(make_source_zip, tmp_path: Path) -> None:
    candidate = qualify_source(str(make_source_zip()), tmp_path / "run")
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(
        json.dumps(
            {
                "schema_version": "source-approval-v1",
                "source_locator": candidate.source_locator,
                "artifact_sha256": candidate.artifact_sha256,
                "schema_sha256": candidate.schema_sha256,
                "cutoff": candidate.global_cutoff,
                "terms_evidence": "internal record: license reviewed",
                "local_use_allowed": True,
                "redistribution_allowed": False,
                "approved_by": "reviewer",
                "approved_at": "2026-07-19T12:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    approval = load_source_approval(approval_path)

    assert isinstance(approval, SourceApproval)
    assert verify_source_approval(candidate, approval) is None
    with pytest.raises(PilotError, match="cutoff_mismatch"):
        verify_source_approval(candidate, SourceApproval.from_json_mapping({**approval.to_json_mapping(), "cutoff": "2024-01-01"}))


def test_load_candidate_requires_a_json_object(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("[]", encoding="utf-8")

    with pytest.raises(PilotError, match="invalid_json_object"):
        load_candidate(manifest)

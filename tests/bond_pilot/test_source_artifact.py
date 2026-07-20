from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pyarrow.parquet as pq
import pytest

from src.bond_pilot.contracts import ArtifactLimits, PilotError, SourceApproval, SourceCandidate
from src.bond_pilot import artifacts, source_artifact
from src.bond_pilot._secure_local_fs import secure_open_dir
from src.bond_pilot.output_pack import OutputPack, validate_output_pack
from src.bond_pilot.source_artifact import (
    load_candidate,
    load_source_approval,
    qualify_source,
    verify_source_approval,
)

from conftest import FakeClient


_qualify_into_pack = qualify_source


def qualify_source(locator, destination, **kwargs):
    """Test boundary helper: create/finalize a source pack through capabilities."""
    if isinstance(destination, OutputPack):
        return _qualify_into_pack(locator, destination, **kwargs)
    destination = Path(destination)
    with secure_open_dir(destination.parent, error_code="unsafe_parent") as parent:
        pack = OutputPack.create(parent, run_id=destination.name, pack_schema_version="bond-pilot-source-v1", producer_version="test")
        try:
            candidate = _qualify_into_pack(locator, pack, **kwargs)
            pack.finalize()
            return candidate
        finally:
            pack.close()


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


@pytest.mark.parametrize("document", ["candidate", "approval"])
@pytest.mark.parametrize("invalid", ["duplicate", "unknown", "missing", "nonfinite"])
def test_source_control_json_requires_strict_unique_exact_finite_top_level(document: str, invalid: str, make_source_zip, tmp_path: Path) -> None:
    candidate = qualify_source(make_source_zip(), tmp_path / "qualified")
    approval = SourceApproval(
        "source-approval-v1", candidate.source_locator, candidate.artifact_sha256, candidate.schema_sha256,
        candidate.global_cutoff, "internal terms", True, False, "reviewer", "2026-07-19T12:00:00Z",
    )
    value = candidate.to_json_mapping() if document == "candidate" else approval.to_json_mapping()
    required = "approval_state" if document == "candidate" else "approved_at"
    if invalid == "unknown":
        value["unexpected"] = True
        raw = json.dumps(value)
    elif invalid == "missing":
        value.pop(required)
        raw = json.dumps(value)
    elif invalid == "nonfinite":
        raw = json.dumps(value)[:-1] + ',"unexpected":NaN}'
    else:
        raw = json.dumps(value)[:-1] + f',"{required}":{json.dumps(value[required])}' + "}"
    path = tmp_path / f"{document}.json"
    path.write_text(raw, encoding="utf-8")

    loader = load_candidate if document == "candidate" else load_source_approval
    with pytest.raises(PilotError, match="^source_control_invalid$") as error:
        loader(path)

    assert error.value.details == {}


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


def test_matching_extracted_files_opens_both_inputs_through_capabilities(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = tmp_path / "original.parquet"
    verified = tmp_path / "verified.parquet"
    original.write_bytes(b"same")
    verified.write_bytes(b"same")
    real_open = source_artifact.secure_open_file
    opened: list[Path] = []
    monkeypatch.setattr(source_artifact, "secure_open_file", lambda path, **kwargs: opened.append(Path(path)) or real_open(path, **kwargs))
    source_artifact.verify_matching_extracted_files(original, verified, limit=4, chunk_size=2)
    assert opened == [original, verified]


def test_archive_and_parquet_consumers_leave_supplied_secure_capabilities_open(make_source_zip, tmp_path: Path) -> None:
    candidate = qualify_source(make_source_zip(), tmp_path / "qualified")
    archive = source_artifact._open_regular_file(candidate.local_archive_path, limit=10_000)
    try:
        with ZipFile(archive) as reader:
            assert reader.namelist() == [candidate.member_name]
        assert archive.closed is False
    finally:
        archive.close()

    with source_artifact.open_verified_extracted_file(candidate.local_extracted_path, candidate.local_extracted_path, limit=10_000, chunk_size=1024) as parquet_file:
        with pq.ParquetFile(parquet_file) as reader:
            assert reader.metadata.num_rows == candidate.row_count
        assert parquet_file.closed is False


def test_verified_extracted_file_rejects_post_consumer_integrity_failure(make_source_zip, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = qualify_source(make_source_zip(), tmp_path / "qualified")
    real_verify = source_artifact.SecureFile.verify_unchanged
    calls = 0

    def fail_after_consumer(self, *, expected_size: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise PilotError("source_integrity_failed")
        real_verify(self, expected_size=expected_size)

    monkeypatch.setattr(source_artifact.SecureFile, "verify_unchanged", fail_after_consumer)
    with pytest.raises(PilotError, match="^source_integrity_failed$"):
        with source_artifact.open_verified_extracted_file(candidate.local_extracted_path, candidate.local_extracted_path, limit=10_000, chunk_size=1024) as parquet_file:
            with pq.ParquetFile(parquet_file) as reader:
                assert reader.metadata.num_rows == candidate.row_count


def test_verified_extracted_file_fails_closed_when_consumer_closes_capability(make_source_zip, tmp_path: Path) -> None:
    candidate = qualify_source(make_source_zip(), tmp_path / "qualified")

    with pytest.raises(PilotError, match="^source_integrity_failed$"):
        with source_artifact.open_verified_extracted_file(candidate.local_extracted_path, candidate.local_extracted_path, limit=10_000, chunk_size=1024) as parquet_file:
            parquet_file.close()


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


@pytest.mark.parametrize("unsafe", [r"\\server\share\source.zip", "//server/share/source.zip", r"\\?\C:\source.zip", r"\\.\PhysicalDrive0"])
def test_initial_local_qualification_rejects_unc_and_device_before_stat_open_or_network(unsafe: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real_lstat = artifacts.os.lstat
    real_open = source_artifact.Path.open
    real_is_file = source_artifact.Path.is_file
    monkeypatch.setattr(artifacts.os, "lstat", lambda path: pytest.fail("unsafe source must fail before lstat") if str(path) == unsafe else real_lstat(path))
    monkeypatch.setattr(source_artifact.Path, "open", lambda path, *args, **kwargs: pytest.fail("unsafe source must fail before open") if str(path) == unsafe else real_open(path, *args, **kwargs))
    monkeypatch.setattr(source_artifact.Path, "is_file", lambda path: pytest.fail("unsafe source must fail before stat") if str(path) == unsafe else real_is_file(path))
    monkeypatch.setattr(source_artifact.httpx, "Client", lambda: pytest.fail("local qualification must not create an HTTP client"))

    with pytest.raises(PilotError):
        qualify_source(unsafe, tmp_path / "run")


def test_initial_local_qualification_never_resolves_user_input(make_source_zip, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = make_source_zip()
    real_resolve = source_artifact.Path.resolve
    monkeypatch.setattr(source_artifact.Path, "resolve", lambda path, *args, **kwargs: pytest.fail("input must not be resolved") if Path(path) == source else real_resolve(path, *args, **kwargs))
    monkeypatch.setattr(source_artifact.httpx, "Client", lambda: pytest.fail("local qualification must not create an HTTP client"))

    assert qualify_source(source, tmp_path / "run").artifact_bytes == source.stat().st_size


def test_initial_local_qualification_uses_a_capability_not_a_path_open(make_source_zip, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = make_source_zip()
    real_open = source_artifact.Path.open
    monkeypatch.setattr(
        source_artifact.Path,
        "open",
        lambda path, *args, **kwargs: pytest.fail("input archive must not be path-opened")
        if Path(path) == archive
        else real_open(path, *args, **kwargs),
    )

    candidate = qualify_source(archive, tmp_path / "run")

    assert candidate.artifact_bytes == archive.stat().st_size


def test_initial_local_qualification_rejects_relative_user_input(make_source_zip, tmp_path: Path) -> None:
    relative = "source-input.zip"

    with pytest.raises(PilotError, match="source_integrity_failed"):
        qualify_source(relative, tmp_path / "run")


def test_rejects_wrong_sha_leaves_an_incomplete_unconsumable_pack(make_source_zip, tmp_path: Path) -> None:
    archive = make_source_zip()
    run_dir = tmp_path / "run"

    with pytest.raises(PilotError, match="artifact_sha256_mismatch"):
        qualify_source(str(archive), run_dir, expected_sha256="0" * 64)

    assert run_dir.is_dir()
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        with pytest.raises(PilotError, match="incomplete_output"):
            validate_output_pack(parent, "run", expected_pack_schema_version="bond-pilot-source-v1", expected_payloads=("qualification-report.json", "source-manifest.json", "source.parquet", "source.zip"))


def test_qualify_source_rejects_path_output(make_source_zip, tmp_path: Path) -> None:
    with pytest.raises(PilotError, match="output_pack_required"):
        source_artifact.qualify_source(make_source_zip(), tmp_path / "run")


def test_qualification_rejects_in_place_source_zip_mutation_during_extraction(make_source_zip, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = make_source_zip()
    run_dir = tmp_path / "run"
    real_zip = source_artifact.ZipFile

    class MutatingZip(real_zip):
        def open(self, *args, **kwargs):
            stream = super().open(*args, **kwargs)
            target = run_dir / "source.zip"
            status = target.stat()
            os.utime(target, ns=(status.st_atime_ns, status.st_mtime_ns + 1_000_000_000))
            return stream

    monkeypatch.setattr(source_artifact, "ZipFile", MutatingZip)
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = OutputPack.create(parent, run_id="run", pack_schema_version="bond-pilot-source-v1", producer_version="test")
        try:
            with pytest.raises(PilotError, match="source_integrity_failed"):
                _qualify_into_pack(archive, pack)
        finally:
            pack.close()


def test_qualification_rejects_in_place_source_parquet_mutation_during_consumer(make_source_zip, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = make_source_zip()
    run_dir = tmp_path / "run"
    real_bounds = source_artifact._date_bounds

    def mutate_then_read(source, chunk_size):
        target = run_dir / "source.parquet"
        status = target.stat()
        os.utime(target, ns=(status.st_atime_ns, status.st_mtime_ns + 1_000_000_000))
        return real_bounds(source, chunk_size)

    monkeypatch.setattr(source_artifact, "_date_bounds", mutate_then_read)
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = OutputPack.create(parent, run_id="run", pack_schema_version="bond-pilot-source-v1", producer_version="test")
        try:
            with pytest.raises(PilotError, match="source_integrity_failed"):
                _qualify_into_pack(archive, pack)
        finally:
            pack.close()


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


def test_repeated_run_id_is_already_exists(make_source_zip, tmp_path: Path) -> None:
    qualify_source(str(make_source_zip()), tmp_path / "run")
    with pytest.raises(PilotError, match="already_exists"):
        qualify_source(str(make_source_zip()), tmp_path / "run")


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

    with pytest.raises(PilotError, match="source_control_invalid"):
        load_candidate(manifest)

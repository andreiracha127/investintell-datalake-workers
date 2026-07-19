from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from src.bond_pilot.contracts import ArtifactLimits, PilotError, SourceApproval
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

    assert not list(run_dir.glob("*.partial"))
    assert not (run_dir / "source.zip").exists()
    assert not (run_dir / "source.parquet").exists()


def test_collision_keeps_existing_output_and_cleans_attempt_partial(make_source_zip, tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "source.zip").write_bytes(b"pre-existing")

    with pytest.raises(PilotError, match="already_exists"):
        qualify_source(str(make_source_zip()), run_dir)

    assert (run_dir / "source.zip").read_bytes() == b"pre-existing"
    assert not list(run_dir.glob("*.partial"))


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
        ("//server/share/source.parquet", 0),
        ("/source.parquet", 0),
        ("nested/", 0),
        ("nested/source.txt", 0),
        ("nested/source.parquet", stat.S_IFLNK << 16),
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

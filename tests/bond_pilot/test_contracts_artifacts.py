from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.bond_pilot import artifacts
from src.bond_pilot.artifacts import canonical_json_bytes
from src.bond_pilot.contracts import (
    ArtifactLimits,
    DebtState,
    FieldState,
    IdentifierState,
    MatchState,
    PilotError,
    SourceApproval,
    SourceCandidate,
)


PIN = "a" * 64
SCHEMA_PIN = "b" * 64


def candidate() -> SourceCandidate:
    return SourceCandidate(
        schema_version="source-candidate-v1",
        source_locator="https://example.test/source.zip",
        local_archive_path="input/source.zip",
        local_extracted_path="input/source.parquet",
        artifact_bytes=12,
        artifact_sha256=PIN,
        member_name="source.parquet",
        member_uncompressed_bytes=24,
        schema_sha256=SCHEMA_PIN,
        schema_columns=("cusip", "reported_at"),
        schema_optional_columns=("issuer",),
        row_count=3,
        row_group_count=1,
        global_start="2024-01-01",
        global_cutoff="2024-01-31",
        duplicate_check_scope="full_source",
    )


def approval_mapping() -> dict[str, object]:
    return {
        "schema_version": "source-approval-v1",
        "source_locator": "https://example.test/source.zip",
        "artifact_sha256": PIN,
        "schema_sha256": SCHEMA_PIN,
        "cutoff": "2024-01-31",
        "terms_evidence": "https://example.test/terms",
        "local_use_allowed": True,
        "redistribution_allowed": False,
        "approved_by": "Ana Reviewer",
        "approved_at": "2024-02-01T12:00:00Z",
    }


def test_contract_enums_limits_and_error_are_typed() -> None:
    assert [state.value for state in FieldState] == ["present", "null", "invalid", "not_in_schema"]
    assert [state.value for state in IdentifierState] == [
        "valid_cusip9",
        "blank",
        "placeholder",
        "synthetic",
        "invalid_format",
    ]
    assert [state.value for state in DebtState] == [
        "debt_like_eligible",
        "ineligible_non_debt",
        "ambiguous_category",
        "missing_category",
    ]
    assert [state.value for state in MatchState] == [
        "ineligible_non_debt",
        "ambiguous_category",
        "missing_category",
        "invalid_identifier",
        "outside_window_before_source",
        "outside_window_after_cutoff",
        "unmatched_no_cusip",
        "unmatched_no_prior_observation",
        "stale",
        "unavailable_ambiguous",
        "matched",
    ]
    assert ArtifactLimits().archive_bytes == 5 * 1024**3
    assert ArtifactLimits().member_uncompressed_bytes == 10 * 1024**3
    assert ArtifactLimits().streaming_chunk_bytes == 1024**2
    error = PilotError("bad_pin", {"field": "artifact_sha256"})
    assert error.code == "bad_pin"
    assert error.details == {"field": "artifact_sha256"}
    assert "bad_pin" in str(error)


def test_source_candidate_roundtrips_as_frozen_json_mapping() -> None:
    source = candidate()
    assert source.approval_state == "unapproved"
    assert SourceCandidate.from_json_mapping(json.loads(json.dumps(source.to_json_mapping()))) == source
    with pytest.raises(PilotError, match="artifact_sha256"):
        SourceCandidate.from_json_mapping({**source.to_json_mapping(), "artifact_sha256": "A" * 64})


def test_source_candidate_normalizes_schema_lists_to_immutable_tuples() -> None:
    schema_columns = ["cusip", "reported_at"]
    optional_columns = ["issuer"]
    source = SourceCandidate(
        **{
            **candidate().to_json_mapping(),
            "schema_columns": schema_columns,
            "schema_optional_columns": optional_columns,
        }
    )

    schema_columns.append("mutated")
    optional_columns.append("mutated")

    assert source.schema_columns == ("cusip", "reported_at")
    assert source.schema_optional_columns == ("issuer",)
    assert isinstance(source.schema_columns, tuple)
    assert isinstance(source.schema_optional_columns, tuple)


def test_source_approval_requires_evidence_and_matches_candidate() -> None:
    approval = SourceApproval.from_json_mapping(approval_mapping())
    assert approval.validate_for(candidate()) is None
    with pytest.raises(PilotError, match="terms_evidence"):
        SourceApproval.from_json_mapping({**approval_mapping(), "terms_evidence": ""})
    with pytest.raises(PilotError, match="source_locator"):
        SourceApproval.from_json_mapping({**approval_mapping(), "source_locator": "https://other.test/source.zip"}).validate_for(candidate())


@pytest.mark.parametrize("approved_at", ["2024-02-01", "2024-02-01T12:00:00+00:00", "2024-02-30T12:00:00Z", "2024-02-01T24:00:00Z"])
def test_source_approval_rejects_noncanonical_or_impossible_utc_timestamps(approved_at: str) -> None:
    with pytest.raises(PilotError, match="approved_at"):
        SourceApproval.from_json_mapping({**approval_mapping(), "approved_at": approved_at})


def test_canonical_json_is_compact_sorted_utf8_lf_and_rejects_nonfinite() -> None:
    assert canonical_json_bytes({"z": "café", "a": [1, 2]}) == b'{"a":[1,2],"z":"caf\xc3\xa9"}\n'
    with pytest.raises(ValueError):
        canonical_json_bytes({"bad": float("nan")})


@pytest.mark.parametrize("unsafe", [r"\\server\share\control.json", "//server/share/control.json", r"\\?\C:\control.json", "file:///C:/control.json"])
def test_secure_local_reader_rejects_nonlocal_paths_before_filesystem_access(unsafe: str, monkeypatch: pytest.MonkeyPatch) -> None:
    reader = getattr(artifacts, "read_secure_local_file", None)
    assert reader is not None
    monkeypatch.setattr(artifacts.os, "lstat", lambda _path: pytest.fail("unsafe path must fail before lstat"))
    monkeypatch.setattr(artifacts.Path, "open", lambda *_args, **_kwargs: pytest.fail("unsafe path must fail before open"))

    with pytest.raises(PilotError, match="^control_invalid$"):
        reader(unsafe, max_bytes=1024, error_code="control_invalid")


def test_secure_local_reader_delegates_to_the_capability_core(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reader = getattr(artifacts, "read_secure_local_file", None)
    assert reader is not None
    child = tmp_path / "control.json"
    child.write_bytes(b"{}")
    calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(artifacts, "secure_read_file", lambda *args, **kwargs: calls.append((*args, kwargs)) or (_ for _ in ()).throw(PilotError("control_invalid")))

    with pytest.raises(PilotError, match="^control_invalid$"):
        reader(child, max_bytes=1024, error_code="control_invalid")
    assert calls

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from src.bond_pilot.artifacts import (
    canonical_json_bytes,
    commit_partial,
    create_run_directory,
    partial_path,
    replace_checkpoint,
    write_checksums,
    write_json_once,
    write_text_once,
)
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


def test_source_approval_requires_evidence_and_matches_candidate() -> None:
    approval = SourceApproval.from_json_mapping(approval_mapping())
    assert approval.validate_for(candidate()) is None
    with pytest.raises(PilotError, match="terms_evidence"):
        SourceApproval.from_json_mapping({**approval_mapping(), "terms_evidence": ""})
    with pytest.raises(PilotError, match="source_locator"):
        SourceApproval.from_json_mapping({**approval_mapping(), "source_locator": "https://other.test/source.zip"}).validate_for(candidate())


def test_canonical_json_is_compact_sorted_utf8_lf_and_rejects_nonfinite() -> None:
    assert canonical_json_bytes({"z": "café", "a": [1, 2]}) == b'{"a":[1,2],"z":"caf\xc3\xa9"}\n'
    with pytest.raises(ValueError):
        canonical_json_bytes({"bad": float("nan")})


def test_final_outputs_do_not_overwrite_and_run_directory_does_not_collide(tmp_path: Path) -> None:
    run = create_run_directory(tmp_path, "2024-01-01")
    assert run == tmp_path / "2024-01-01"
    with pytest.raises(PilotError, match="already_exists"):
        create_run_directory(tmp_path, "2024-01-01")
    output = tmp_path / "output.json"
    write_json_once(output, {"a": 1})
    with pytest.raises(PilotError, match="already_exists"):
        write_json_once(output, {"a": 2})
    text = tmp_path / "notes.txt"
    write_text_once(text, "first")
    with pytest.raises(PilotError, match="already_exists"):
        write_text_once(text, "second")


def test_partial_commit_is_unique_same_directory_and_never_replaces_final(tmp_path: Path) -> None:
    final = tmp_path / "output.parquet"
    first = partial_path(final)
    second = partial_path(final)
    assert first.parent == final.parent
    assert first != second
    first.write_bytes(b"first")
    commit_partial(first, final)
    assert final.read_bytes() == b"first"
    second.write_bytes(b"second")
    with pytest.raises(PilotError, match="already_exists"):
        commit_partial(second, final)
    assert final.read_bytes() == b"first"
    assert second.exists()


def test_checkpoint_replacement_is_atomic_overwrite_exception(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint.json"
    write_text_once(checkpoint, "old")
    replace_checkpoint(checkpoint, b"new")
    assert checkpoint.read_bytes() == b"new"


def test_checksums_are_sorted_relative_and_exclude_self_and_partials(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "z.txt").write_bytes(b"z")
    (tmp_path / "nested" / "a.txt").write_bytes(b"a")
    (tmp_path / "ignored.partial").write_bytes(b"ignore")
    (tmp_path / "checksums.sha256").write_text("old", encoding="utf-8")
    (tmp_path / "checksums.sha256").unlink()

    checksums = write_checksums(tmp_path)

    expected = "".join(
        [
            f"{hashlib.sha256(b'a').hexdigest()}  nested/a.txt\n",
            f"{hashlib.sha256(b'z').hexdigest()}  z.txt\n",
        ]
    )
    assert checksums == tmp_path / "checksums.sha256"
    assert checksums.read_text(encoding="utf-8") == expected

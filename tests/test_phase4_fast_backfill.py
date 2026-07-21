from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_prepare_cli_defaults_to_exactly_ten_process_workers() -> None:
    from scripts.phase4_fast_backfill import build_parser

    args = build_parser().parse_args(["prepare", "--root", "source", "--output", "prepared"])

    assert args.workers == 10


def test_prepared_package_artifacts_and_manifest_are_deterministic(tmp_path: Path) -> None:
    from scripts.phase4_fast_backfill import PreparedPackage, _write_prepared_package

    prepared = PreparedPackage(
        identity="nport:2024Q1:2024q1_nport",
        form="nport",
        quarter="2024Q1",
        relative_package_path="2024q1_nport",
        package_sha256="a" * 64,
        metadata_sha256="b" * 64,
        readme_sha256="c" * 64,
        parser_version="nport-v1",
        raw_table="nport_raw_rows",
        files=(
            {
                "source_table": "FUND_REPORTED_HOLDING.tsv",
                "source_sha256": "d" * 64,
                "byte_size": 12,
                "headers": ["ACCESSION_NUMBER", "HOLDING_ID"],
                "expected_count": 1,
                "data_count": 1,
                "lexical_count": 1,
                "typed_success_count": 1,
                "quarantine_count": 0,
                "reject_count": 0,
            },
        ),
        rows=(
            {
                "source_table": "FUND_REPORTED_HOLDING.tsv",
                "source_sha256": "d" * 64,
                "source_row_number": 2,
                "original_lexical_row": {"ACCESSION_NUMBER": "0001", "HOLDING_ID": "H1"},
                "typed_projection": {"ACCESSION_NUMBER": "0001", "HOLDING_ID": "H1"},
                "parse_status": "typed",
                "parse_errors": [],
                "candidate_key_evidence": {"columns": ["ACCESSION_NUMBER"], "complete": True, "values": ["0001"]},
                "accession_number": "0001",
                "holding_id": "H1",
            },
        ),
    )

    first = _write_prepared_package(prepared, tmp_path)
    second = _write_prepared_package(prepared, tmp_path)

    assert first == second
    assert json.loads((tmp_path / "prepared-manifest.json").read_text(encoding="utf-8"))["packages"][0]["identity"] == prepared.identity
    payload = tmp_path / first["payload_path"]
    assert payload.is_file()
    assert first["payload_sha256"]


def test_prepare_never_connects_to_database(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import scripts.phase4_fast_backfill as backfill

    monkeypatch.setattr(backfill, "_connect", lambda *_args, **_kwargs: pytest.fail("prepare opened a database connection"))
    monkeypatch.setattr(backfill, "_discover_tasks", lambda *_args, **_kwargs: [])

    result = backfill.prepare(root=tmp_path, output=tmp_path / "out", workers=10, forms=("nport",), identities=())

    assert result["state"] == "prepared"
    assert result["packages"] == 0


def test_parse_task_reuses_complete_package_without_reparsing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import scripts.phase4_fast_backfill as backfill

    prepared = backfill.PreparedPackage(
        identity="nport:2024Q1:2024q1_nport", form="nport", quarter="2024Q1", relative_package_path="2024q1_nport",
        package_sha256="a" * 64, metadata_sha256="b" * 64, readme_sha256="c" * 64, parser_version="nport-v1",
        raw_table="nport_raw_rows", files=(), rows=(),
    )
    expected = backfill._write_prepared_package(prepared, tmp_path)
    monkeypatch.setattr(backfill, "_adapter", lambda *_args: pytest.fail("completed package was reparsed"))

    actual = backfill._parse_task(backfill.ParseTask("nport", "unused", "unused", "2024q1_nport", str(tmp_path)))

    assert actual == expected
    assert not list(tmp_path.rglob("*.tmp"))

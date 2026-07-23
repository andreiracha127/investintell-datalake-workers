from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from src.nport import local_v2_generator as generator


SOURCE_FILES = (
    "SUBMISSION.tsv",
    "FUND_REPORTED_INFO.tsv",
    "FUND_REPORTED_HOLDING.tsv",
    "IDENTIFIERS.tsv",
    "DEBT_SECURITY.tsv",
)


def _write_tsv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.write_text(
        "\t".join(header) + "\n" + "".join("\t".join(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def _source_hashes(source_dir: Path) -> dict[str, str]:
    return {
        name: hashlib.sha256((source_dir / name).read_bytes()).hexdigest()
        for name in SOURCE_FILES
    }


def _fixture(source_dir: Path) -> None:
    old = "0000000000-26-000010"
    current = "0000000000-26-000011"
    _write_tsv(
        source_dir / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "FILING_DATE", "FILE_NUM", "SUB_TYPE", "REPORT_ENDING_PERIOD", "REPORT_DATE", "IS_LAST_FILING"],
        [
            [old, "01-MAR-2026", "811-1", "NPORT-P", "31-JAN-2026", "31-JAN-2026", "N"],
            [current, "02-MAR-2026", "811-1", "NPORT-P/A", "31-JAN-2026", "31-JAN-2026", "Y"],
        ],
    )
    _write_tsv(
        source_dir / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[old, "S000001234"], [current, "S000001234"]],
    )
    _write_tsv(
        source_dir / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER", "HOLDING_ID", "ISSUER_NAME", "ISSUER_CUSIP",
            "ISSUER_LEI", "ASSET_CAT", "PAYOFF_PROFILE", "PERCENTAGE",
            "CURRENCY_VALUE", "ISSUER_TYPE",
        ],
        [
            [old, "OLD", "Old issuer", "037833100", "", "EC", "Long", "10", "10", "corporate"],
            [current, "CUSIP", "Cusip issuer", "037833100", "529900T8BM49AURSDO25", "EC", "Long", "20", "200", "corporate"],
            [current, "ISIN", "Isin issuer", "", "", "DBT", "Long", "30", "300", "government"],
            [current, "AMBIG", "Ambiguous issuer", "BAD", "NOT-A-LEI", "EC", "Long", "40", "400", "corporate"],
        ],
    )
    _write_tsv(
        source_dir / "IDENTIFIERS.tsv",
        ["HOLDING_ID", "IDENTIFIER_ISIN"],
        [
            ["CUSIP", "US0378331005"],
            ["ISIN", "IE00B4L5Y983"],
            ["AMBIG", "US0378331005"],
            ["AMBIG", "IE00B4L5Y983"],
        ],
    )
    _write_tsv(
        source_dir / "DEBT_SECURITY.tsv",
        ["HOLDING_ID", "COUPON_TYPE", "ANNUALIZED_RATE", "MATURITY_DATE"],
        [["ISIN", "Fixed", "4.5", "25-FEB-2026"]],
    )


def _run(source_dir: Path, output_dir: Path, hashes: dict[str, str]) -> generator.GenerationResult:
    return generator.generate(
        source_dir=source_dir,
        source_run_id="11111111-1111-1111-1111-111111111111",
        package_id="22222222-2222-2222-2222-222222222222",
        package_sha256="a" * 64,
        parser_version="nport_parser_v2",
        publication_id="33333333-3333-3333-3333-333333333333",
        expected_hashes=hashes,
        output_dir=output_dir,
        generator_version="nport_local_v2",
        config_version="nport_config_v2",
    )


def _read_payload(path: Path) -> list[dict[str, str | None]]:
    import csv

    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        return [
            {key: (None if value == "\\N" else value) for key, value in row.items()}
            for row in csv.DictReader(stream, delimiter="\t")
        ]


def test_generator_selects_effective_filing_preserves_lots_and_projects_debt(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _fixture(source_dir)

    result = _run(source_dir, tmp_path / "output", _source_hashes(source_dir))

    holdings = _read_payload(result.holdings_path)
    bridge = _read_payload(result.bridge_path)
    assert [row["holding_id"] for row in holdings] == ["AMBIG", "CUSIP", "ISIN"]
    assert {row["accession_number"] for row in holdings} == {"0000000000-26-000011"}
    assert holdings[1]["cusip"] == "037833100"
    assert holdings[1]["isin"] == "US0378331005"
    assert holdings[2]["isin"] == "IE00B4L5Y983"
    projection = json.loads(holdings[2]["source_typed_projection"] or "{}")
    assert projection["ASSET_CAT"] == "DBT"
    assert projection["DEBT_SECURITY"] == {
        "ANNUALIZED_RATE": "4.5", "COUPON_TYPE": "Fixed", "MATURITY_DATE": "2026-02-25"
    }
    assert bridge[0]["class_id"] is None
    assert bridge[0]["instrument_id"] is None
    assert bridge[1]["instrument_id"] == "CUSIP:037833100"
    assert bridge[2]["instrument_id"] == "ISIN:IE00B4L5Y983"
    assert json.loads((tmp_path / "output" / "manifest.json").read_text(encoding="utf-8"))["counts"] == {
        "bridge_rows": 3,
        "duplicate_primary_keys": 0,
        "eligible_filing_candidates": 2,
        "excluded_ineligible_filings": 0,
        "filing_candidates": 2,
        "holdings_rows": 3,
        "identified_rows": 2,
        "report_dates": 1,
        "resolved_rows": 3,
        "selected_filings": 1,
        "series": 1,
        "unidentified_rows": 1,
    }
    manifest = json.loads((tmp_path / "output" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["report_dates"] == {"max": "2026-01-31", "min": "2026-01-31", "values": ["2026-01-31"]}
    assert len(manifest["fingerprints"]["generator_code_sha256"]) == 64


def test_generator_is_byte_deterministic_and_fails_closed_on_source_hash_mismatch(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _fixture(source_dir)
    hashes = _source_hashes(source_dir)

    first = _run(source_dir, tmp_path / "one", hashes)
    second = _run(source_dir, tmp_path / "two", hashes)

    assert first.bridge_path.read_bytes() == second.bridge_path.read_bytes()
    assert first.holdings_path.read_bytes() == second.holdings_path.read_bytes()
    bad_hashes = dict(hashes, **{"SUBMISSION.tsv": "0" * 64})
    with pytest.raises(generator.SourceHashMismatch, match="SUBMISSION.tsv"):
        _run(source_dir, tmp_path / "bad", bad_hashes)


def test_generator_rejects_duplicate_publication_primary_keys(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _fixture(source_dir)
    with (source_dir / "FUND_REPORTED_HOLDING.tsv").open("a", encoding="utf-8") as stream:
        stream.write("0000000000-26-000011\tCUSIP\tDuplicate\t037833100\t\tEC\tLong\t1\t1\tcorporate\n")

    with pytest.raises(generator.DuplicatePrimaryKeyError, match="CUSIP"):
        _run(source_dir, tmp_path / "output", _source_hashes(source_dir))
    assert not (tmp_path / "output").exists()


def test_generator_rejects_missing_series_without_publishing(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _fixture(source_dir)
    info = source_dir / "FUND_REPORTED_INFO.tsv"
    info.write_text(info.read_text(encoding="utf-8").replace("S000001234", ""), encoding="utf-8")

    with pytest.raises(ValueError, match="no eligible N-PORT holdings"):
        _run(source_dir, tmp_path / "output", _source_hashes(source_dir))

    assert not (tmp_path / "output").exists()


def test_generator_cleans_staging_when_compression_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _fixture(source_dir)
    real_gzip = generator._gzip_payload
    calls = 0

    def fail_second_compression(source: Path, destination: Path) -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected compression failure")
        return real_gzip(source, destination)

    monkeypatch.setattr(generator, "_gzip_payload", fail_second_compression)
    with pytest.raises(OSError, match="injected compression failure"):
        _run(source_dir, tmp_path / "output", _source_hashes(source_dir))

    assert not (tmp_path / "output").exists()
    assert not list(tmp_path.glob(".output.tmp-*"))


def test_generator_iterates_across_arrow_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _fixture(source_dir)
    monkeypatch.setattr(generator, "_BATCH_SIZE", 2)

    result = _run(source_dir, tmp_path / "output", _source_hashes(source_dir))

    assert [row["holding_id"] for row in _read_payload(result.holdings_path)] == ["AMBIG", "CUSIP", "ISIN"]


def test_normalize_cusip_rejects_nonnumeric_check_character() -> None:
    assert generator.normalize_cusip("03783310A") is None
    assert generator.normalize_cusip("03783310*") is None


def test_generator_uses_bounded_duckdb_batches_not_full_corpus_fetchall() -> None:
    source = Path(generator.__file__).read_text(encoding="utf-8")
    assert ".fetchall(" not in source
    assert "to_arrow_reader" in source
    assert "read_bytes" not in source

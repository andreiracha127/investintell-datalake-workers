from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from scripts import backfill_nport_holding_attributes as backfill


def _write_tsv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.write_text(
        "\t".join(header) + "\n" + "".join("\t".join(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_identifiers(tmp_path: Path, rows: list[list[str]] | None = None) -> None:
    """Every DERA bundle carries IDENTIFIERS.tsv; the loader now requires it."""
    _write_tsv(
        tmp_path / "IDENTIFIERS.tsv", ["HOLDING_ID", "IDENTIFIER_ISIN"], rows or []
    )


def test_build_equity_rollups_preserves_lots_collapsed_by_cloud_pk(tmp_path: Path) -> None:
    accession = "0000000000-26-000003"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[accession, "31-JAN-2026"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[accession, "S000001234"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER",
            "HOLDING_ID",
            "ISSUER_CUSIP",
            "PAYOFF_PROFILE",
            "INVESTMENT_COUNTRY",
            "ASSET_CAT",
            "PERCENTAGE",
        ],
        [
            [accession, "H1", "037833100", "Long", "GB", "EC", "80"],
            [accession, "H2", "037833100", "Short", "JP", "EC", "30"],
            [accession, "H3", "912810TM0", "Long", "US", "DBT", "40"],
        ],
    )

    _write_identifiers(tmp_path)

    summaries, countries, weights = backfill.build_equity_rollups(tmp_path)

    assert summaries == [
        backfill.EquitySummary(
            dt.date(2026, 1, 31), "S000001234", 110.0, 50.0, tmp_path.name
        )
    ]
    assert countries == [
        backfill.CountryExposure(
            dt.date(2026, 1, 31), "S000001234", "GB", 80.0, tmp_path.name
        ),
        backfill.CountryExposure(
            dt.date(2026, 1, 31), "S000001234", "JP", -30.0, tmp_path.name
        ),
    ]
    assert weights == [
        backfill.HoldingWeight(
            dt.date(2026, 1, 31), "S000001234", "037833100", 50.0,
            tmp_path.name, 110.0
        ),
    ]
    assert getattr(weights[0], "gross_pct_of_nav", None) == 110.0


def test_build_equity_rollups_uses_latest_filing_per_series_report(tmp_path: Path) -> None:
    old_accession = "0000000000-26-000010"
    new_accession = "0000000000-26-000011"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE", "FILING_DATE", "IS_LAST_FILING"],
        [
            [old_accession, "31-JAN-2026", "01-MAR-2026", "N"],
            [new_accession, "31-JAN-2026", "02-MAR-2026", "Y"],
        ],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[old_accession, "S000001234"], [new_accession, "S000001234"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER",
            "HOLDING_ID",
            "ISSUER_CUSIP",
            "PAYOFF_PROFILE",
            "INVESTMENT_COUNTRY",
            "ASSET_CAT",
            "PERCENTAGE",
        ],
        [
            [old_accession, "H1", "037833100", "Long", "US", "EC", "10"],
            [new_accession, "H2", "037833100", "Long", "US", "EC", "20"],
        ],
    )

    _write_identifiers(tmp_path)

    summaries, countries, weights = backfill.build_equity_rollups(tmp_path)

    assert summaries[0].gross_equity_pct == 20.0
    assert summaries[0].net_equity_pct == 20.0
    assert countries[0].direct_pct == 20.0
    assert weights[0].signed_pct_of_nav == 20.0


def test_build_equity_rollups_routes_non_iso_country_to_unknown(tmp_path: Path) -> None:
    accession = "0000000000-26-000019"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[accession, "31-JAN-2026"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[accession, "S000001234"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER",
            "HOLDING_ID",
            "ISSUER_CUSIP",
            "PAYOFF_PROFILE",
            "INVESTMENT_COUNTRY",
            "ASSET_CAT",
            "PERCENTAGE",
        ],
        [[accession, "H1", "037833100", "Long", "XX", "EC", "20"]],
    )

    _write_identifiers(tmp_path)

    _summaries, countries, _weights = backfill.build_equity_rollups(tmp_path)

    assert countries == [
        backfill.CountryExposure(
            dt.date(2026, 1, 31), "S000001234", "UNKNOWN", 20.0, tmp_path.name
        )
    ]


def test_build_equity_rollups_recovers_blank_country_from_valid_isin(
    tmp_path: Path,
) -> None:
    accession = "0000000000-26-000020"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[accession, "31-JAN-2026"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[accession, "S000001234"]],
    )
    _write_tsv(
        tmp_path / "IDENTIFIERS.tsv",
        ["HOLDING_ID", "IDENTIFIER_ISIN"],
        [["H1", "IE00B4L5Y983"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER",
            "HOLDING_ID",
            "ISSUER_CUSIP",
            "PAYOFF_PROFILE",
            "INVESTMENT_COUNTRY",
            "ASSET_CAT",
            "PERCENTAGE",
        ],
        [[accession, "H1", "", "Long", "N/A", "EC", "20"]],
    )

    _summaries, countries, _weights = backfill.build_equity_rollups(tmp_path)

    assert countries == [
        backfill.CountryExposure(
            dt.date(2026, 1, 31), "S000001234", "IE", 20.0, tmp_path.name
        )
    ]


def test_build_equity_rollups_uses_synthetic_isin_key_for_cusipless_short(
    tmp_path: Path,
) -> None:
    accession = "0000000000-26-000017"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[accession, "31-JAN-2026"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[accession, "S000001234"]],
    )
    _write_tsv(
        tmp_path / "IDENTIFIERS.tsv",
        ["HOLDING_ID", "IDENTIFIER_ISIN"],
        [["H1", "IE00B4L5Y983"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER",
            "HOLDING_ID",
            "ISSUER_CUSIP",
            "PAYOFF_PROFILE",
            "INVESTMENT_COUNTRY",
            "ASSET_CAT",
            "PERCENTAGE",
        ],
        [[accession, "H1", "N/A", "Short", "IE", "EC", "10"]],
    )

    _summaries, _countries, weights = backfill.build_equity_rollups(tmp_path)

    assert weights == [
        backfill.HoldingWeight(
            dt.date(2026, 1, 31),
            "S000001234",
            "IS:IE00B4L5Y983",
            -10.0,
            tmp_path.name,
            10.0,
        )
    ]


def test_build_equity_rollups_emits_zero_tombstone_when_equity_disappears(
    tmp_path: Path,
) -> None:
    accession = "0000000000-26-000018"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[accession, "31-JAN-2026"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[accession, "S000001234"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER",
            "HOLDING_ID",
            "ISSUER_CUSIP",
            "PAYOFF_PROFILE",
            "INVESTMENT_COUNTRY",
            "ASSET_CAT",
            "PERCENTAGE",
        ],
        [[accession, "H1", "912810TM0", "Long", "US", "DBT", "100"]],
    )

    _write_identifiers(tmp_path)

    summaries, countries, weights = backfill.build_equity_rollups(tmp_path)

    assert summaries == [
        backfill.EquitySummary(
            dt.date(2026, 1, 31), "S000001234", 0.0, 0.0, tmp_path.name
        )
    ]
    assert countries == []
    assert weights == []


def test_build_equity_rollups_skips_seriesless_rows(tmp_path: Path) -> None:
    accession = "0000000000-26-000020"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[accession, "31-JAN-2026"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[accession, ""]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        ["ACCESSION_NUMBER", "PAYOFF_PROFILE", "INVESTMENT_COUNTRY", "ASSET_CAT", "PERCENTAGE"],
        [[accession, "Long", "US", "EC", "100"]],
    )

    _write_identifiers(tmp_path)

    assert backfill.build_equity_rollups(tmp_path) == ([], [], [])


def test_backfill_schema_is_additive() -> None:
    schema = (
        Path(__file__).resolve().parents[1]
        / "schemas"
        / "nport_equity_classification_inputs.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS nport_equity_exposure_summary" in schema
    assert "CREATE TABLE IF NOT EXISTS nport_equity_country_exposures" in schema
    assert "CREATE TABLE IF NOT EXISTS nport_equity_holding_weights" in schema
    assert "gross_pct_of_nav" in schema
    assert "ALTER TABLE sec_nport_holdings" not in schema
    assert "DROP COLUMN" not in schema.upper()


class _Copy:
    def __init__(self) -> None:
        self.rows: list[tuple] = []

    def __enter__(self) -> _Copy:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def write_row(self, row: tuple) -> None:
        self.rows.append(row)


class _Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, _Copy]] = []

    def copy(self, sql: str) -> _Copy:
        stream = _Copy()
        self.calls.append((sql, stream))
        return stream


def test_copy_rollups_sends_only_compact_classification_inputs() -> None:
    cursor = _Cursor()
    summary = backfill.EquitySummary(
        dt.date(2026, 1, 31), "S000001234", 110.0, 50.0, "2026q1_nport"
    )
    country = backfill.CountryExposure(
        dt.date(2026, 1, 31), "S000001234", "GB", 80.0, "2026q1_nport"
    )
    weight = backfill.HoldingWeight(
        dt.date(2026, 1, 31), "S000001234", "037833100", 80.0,
        "2026q1_nport", 80.0
    )

    counts = backfill.copy_rollups(cursor, [summary], [country], [weight])

    assert counts == (1, 1, 1)
    assert cursor.calls[0][1].rows == [summary.as_tuple()]
    assert cursor.calls[1][1].rows == [country.as_tuple()]
    assert cursor.calls[2][1].rows == [weight.as_tuple()]
    assert "tmp_nport_equity_exposure_summary" in cursor.calls[0][0]
    assert "tmp_nport_equity_country_exposures" in cursor.calls[1][0]
    assert "tmp_nport_equity_holding_weights" in cursor.calls[2][0]


def test_upsert_rollups_replaces_holding_weight_sidecars() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.rowcount = 1

        def execute(self, query: str) -> None:
            self.queries.append(query)

    cursor = Cursor()

    assert backfill._upsert_rollups(cursor) == (1, 1, 1)
    sql = "\n".join(cursor.queries)
    assert "DELETE FROM nport_equity_holding_weights" in sql
    assert "INSERT INTO nport_equity_holding_weights" in sql
    assert "tmp_nport_equity_holding_weights" in sql


def test_build_equity_rollups_refuses_a_bundle_without_identifiers(
    tmp_path: Path,
) -> None:
    """The silent `{}` was how the sidecar rotted: no ISIN map means every
    CUSIP-less holding lands on the non-unique LE:<issuer_lei> key and lots
    collapse, with nothing in the output to say so."""
    accession = "0000000000-26-000021"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[accession, "31-JAN-2026"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[accession, "S000001234"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER",
            "HOLDING_ID",
            "ISSUER_CUSIP",
            "ISSUER_LEI",
            "PAYOFF_PROFILE",
            "INVESTMENT_COUNTRY",
            "ASSET_CAT",
            "PERCENTAGE",
        ],
        [
            [accession, "H1", "N/A", "LEI0000000000000001", "Long", "IE", "EC", "10"],
            [accession, "H2", "N/A", "LEI0000000000000001", "Long", "IE", "EC", "20"],
        ],
    )

    with pytest.raises(FileNotFoundError, match="IDENTIFIERS.tsv"):
        backfill.build_equity_rollups(tmp_path)


def test_build_equity_rollups_only_report_dates_scopes_the_bundle(
    tmp_path: Path,
) -> None:
    """A quarterly bundle carries several report_dates. Rebuilding one must not
    drag its neighbours along — they may be the dates held aside as controls."""
    target = "0000000000-26-000030"
    neighbour = "0000000000-26-000031"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[target, "31-JAN-2026"], [neighbour, "28-FEB-2026"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[target, "S000001234"], [neighbour, "S000005678"]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_HOLDING.tsv",
        [
            "ACCESSION_NUMBER",
            "HOLDING_ID",
            "ISSUER_CUSIP",
            "PAYOFF_PROFILE",
            "INVESTMENT_COUNTRY",
            "ASSET_CAT",
            "PERCENTAGE",
        ],
        [
            [target, "H1", "037833100", "Long", "US", "EC", "10"],
            [neighbour, "H2", "037833100", "Long", "US", "EC", "20"],
        ],
    )
    _write_identifiers(tmp_path)

    scoped = frozenset({dt.date(2026, 1, 31)})
    summaries, countries, weights = backfill.build_equity_rollups(tmp_path, scoped)

    assert [row.report_date for row in summaries] == [dt.date(2026, 1, 31)]
    assert [row.series_id for row in summaries] == ["S000001234"]
    assert {row.report_date for row in countries} == {dt.date(2026, 1, 31)}
    assert {row.report_date for row in weights} == {dt.date(2026, 1, 31)}

    # unscoped, the same bundle still yields both dates
    all_summaries, _c, _w = backfill.build_equity_rollups(tmp_path)
    assert {row.report_date for row in all_summaries} == {
        dt.date(2026, 1, 31),
        dt.date(2026, 2, 28),
    }


def test_parse_only_report_dates_rejects_junk_and_empty_scope() -> None:
    assert backfill._parse_only_report_dates(None) is None
    assert backfill._parse_only_report_dates("2026-01-31,2026-02-28") == frozenset(
        {dt.date(2026, 1, 31), dt.date(2026, 2, 28)}
    )
    with pytest.raises(SystemExit, match="not YYYY-MM-DD"):
        backfill._parse_only_report_dates("31-JAN-2026")
    # an empty scope would write nothing while exiting 0
    with pytest.raises(SystemExit, match="lists no date"):
        backfill._parse_only_report_dates(" , ")

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


def _isin_bundle(
    tmp_path: Path,
    holdings: list[list[str]],
    identifiers: list[list[str]],
    report_date: str = "31-JAN-2026",
) -> None:
    """One bundle, one series, whatever holdings and ISIN map the test needs."""
    accession = "0000000000-26-000040"
    _write_tsv(
        tmp_path / "SUBMISSION.tsv",
        ["ACCESSION_NUMBER", "REPORT_DATE"],
        [[accession, report_date]],
    )
    _write_tsv(
        tmp_path / "FUND_REPORTED_INFO.tsv",
        ["ACCESSION_NUMBER", "SERIES_ID"],
        [[accession, "S000001234"]],
    )
    _write_tsv(
        tmp_path / "IDENTIFIERS.tsv", ["HOLDING_ID", "IDENTIFIER_ISIN"], identifiers
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
        [[accession] + row for row in holdings],
    )


def test_isin_key_fill_is_measured_on_the_rollup_not_on_the_parsed_rows(
    tmp_path: Path,
) -> None:
    """The lesson the first ISIN floor taught, kept as a test.

    That floor measured the parsed input and refused the re-parse that FIXED the
    defect (2023-08-31 read 0.5858 as a file and 0.9912 as the table it
    produced). One cause: ``LE:<lei>`` is not unique per security, so many input
    rows land on one key and the ISIN-poor side is over-represented in the file.
    Here one holding carries an ISIN and forty lots of a single LEI-only issuer
    do not, so the input reads 1/41 = 0.024 -- under any sane floor -- while the
    rollup the database will hold is one ``IS:`` key against one ``LE:`` key.

    What is asserted is not which number is prettier: it is that the two
    readings disagree across a floor, and that the gate uses the second.
    """
    holdings = [["HISIN", "N/A", "", "Long", "IE", "EC", "1"]]
    holdings += [
        [f"H{i}", "N/A", "LEI0000000000000001", "Long", "IE", "EC", "1"]
        for i in range(40)
    ]
    _isin_bundle(tmp_path, holdings, [["HISIN", "IE00B4L5Y983"]])

    _s, _c, weights = backfill.build_equity_rollups(tmp_path)

    # the rollup: one IS: key, one LE: key with forty lots collapsed onto it
    assert sorted(w.cusip for w in weights) == [
        "IS:IE00B4L5Y983",
        "LE:LEI0000000000000001",
    ]

    reading = backfill.isin_key_fill_by_report_date(weights)[dt.date(2026, 1, 31)]
    assert reading["synthetic_keys"] == 2
    assert reading["isin_key_fill"] == 0.5

    # the reading the discarded design would have taken, over the parsed rows
    input_side = 1 / 41
    assert input_side < 0.25 < reading["isin_key_fill"]

    # and the gate takes the rollup one: a floor between them lets this through
    assert backfill.report_dates_below_isin_floor(weights, 0.25, min_keys=1) == []


def test_report_dates_below_isin_floor_catches_a_short_identifier_map(
    tmp_path: Path,
) -> None:
    """A present-but-short IDENTIFIERS.tsv is exactly what a presence check misses."""
    holdings = [
        [f"H{i}", "N/A", f"LEI000000000000{i:04d}", "Long", "IE", "EC", "1"]
        for i in range(10)
    ]
    # only two of the ten CUSIP-less holdings are in the map
    _isin_bundle(
        tmp_path, holdings, [["H0", "IE00B4L5Y983"], ["H1", "IE00B4L5Y984"]]
    )

    _s, _c, weights = backfill.build_equity_rollups(tmp_path)

    fills = backfill.isin_key_fill_by_report_date(weights)
    assert fills[dt.date(2026, 1, 31)]["isin_key_fill"] == 0.2
    assert backfill.report_dates_below_isin_floor(
        weights, backfill.DEFAULT_MIN_ISIN_FILL, min_keys=1
    ) == [(dt.date(2026, 1, 31), 0.2)]


def test_isin_floor_does_not_judge_a_thin_report_date(tmp_path: Path) -> None:
    """Off-cycle month-ends carry a handful of keys and one filer's junk is not a
    failed bundle. The same gate is what keeps a scoped run over a neighbouring
    bundle -- a few late or amended declarations -- from reading as one."""
    holdings = [
        [f"H{i}", "N/A", f"LEI000000000000{i:04d}", "Long", "IE", "EC", "1"]
        for i in range(10)
    ]
    _isin_bundle(tmp_path, holdings, [])

    _s, _c, weights = backfill.build_equity_rollups(tmp_path)

    fills = backfill.isin_key_fill_by_report_date(weights)
    assert fills[dt.date(2026, 1, 31)]["isin_key_fill"] == 0.0
    # ten keys is far under DEFAULT_MIN_ISIN_FILL_KEYS, so nothing is judged
    assert (
        backfill.report_dates_below_isin_floor(
            weights, backfill.DEFAULT_MIN_ISIN_FILL
        )
        == []
    )
    assert backfill.report_dates_below_isin_floor(
        weights, backfill.DEFAULT_MIN_ISIN_FILL, min_keys=10
    ) == [(dt.date(2026, 1, 31), 0.0)]


def test_isin_key_fill_ignores_holdings_that_never_needed_the_map(
    tmp_path: Path,
) -> None:
    """A real CUSIP-9 never consults the ISIN map, so it must not dilute the
    reading. Over the 102 production report_dates that choice is the difference
    between 12.81 pp and 6.09 pp of separation from the rotten dates."""
    holdings = [
        ["H1", "037833100", "", "Long", "US", "EC", "1"],
        ["H2", "594918104", "", "Long", "US", "EC", "1"],
        ["H3", "N/A", "LEI0000000000000001", "Long", "IE", "EC", "1"],
        ["H4", "N/A", "", "Long", "IE", "EC", "1"],
    ]
    _isin_bundle(tmp_path, holdings, [["H4", "IE00B4L5Y983"]])

    _s, _c, weights = backfill.build_equity_rollups(tmp_path)
    reading = backfill.isin_key_fill_by_report_date(weights)[dt.date(2026, 1, 31)]

    assert reading["keys"] == 4
    assert reading["real"] == 2
    assert reading["synthetic_keys"] == 2
    assert reading["isin_key_fill"] == 0.5
    assert reading["identifiable_share"] == 0.75


def test_load_directory_refuses_a_short_map_before_touching_the_database(
    tmp_path: Path,
) -> None:
    """Fail closed, and fail before the connection is used: a short map is a
    property of the bundle on disk and no database state changes the verdict."""

    class _Exploding:
        def cursor(self) -> object:
            raise AssertionError("the database must not be touched")

        def rollback(self) -> None:
            raise AssertionError("the database must not be touched")

        def commit(self) -> None:
            raise AssertionError("the database must not be touched")

    holdings = [
        [f"H{i}", "N/A", f"LEI000000000000{i:04d}", "Long", "IE", "EC", "1"]
        for i in range(1500)
    ]
    _isin_bundle(tmp_path, holdings, [])

    with pytest.raises(ValueError, match="ISIN map too short"):
        backfill.load_directory(
            _Exploding(), tmp_path, apply=False, minimum_isin_fill=0.75
        )


def test_min_isin_fill_zero_is_the_documented_opt_out(tmp_path: Path) -> None:
    """Zero disables the floor. Nothing else does, and it has to be typed."""

    class _Recording:
        def __init__(self) -> None:
            self.used = False

        def cursor(self) -> object:
            self.used = True
            raise RuntimeError("reached the database")

        def rollback(self) -> None:
            return None

    holdings = [
        [f"H{i}", "N/A", f"LEI000000000000{i:04d}", "Long", "IE", "EC", "1"]
        for i in range(1500)
    ]
    _isin_bundle(tmp_path, holdings, [])

    conn = _Recording()
    with pytest.raises(RuntimeError, match="reached the database"):
        backfill.load_directory(conn, tmp_path, apply=False, minimum_isin_fill=0.0)
    assert conn.used, "with the floor off, the run must get past the guard"


def test_min_isin_fill_default_sits_between_the_readings_it_was_derived_from() -> None:
    """The default is derived, so pin the two ends of the derivation.

    Worst healthy reading over the 102 report_dates of
    nport_equity_holding_weights after the 2026-08-06 repair waves: 0.8296
    (2023-04-28, which is also the worst clean date of the base table's own
    probe). Best of the eight the sidecar was rotten on: 0.7015 (2024-11-29).
    """
    worst_healthy = 0.8296
    best_degraded = 0.7015
    assert best_degraded < backfill.DEFAULT_MIN_ISIN_FILL < worst_healthy
    assert worst_healthy - backfill.DEFAULT_MIN_ISIN_FILL >= 0.05
    assert backfill.DEFAULT_MIN_ISIN_FILL - best_degraded >= 0.04

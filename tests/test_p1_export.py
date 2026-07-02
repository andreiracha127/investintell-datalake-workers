"""Tests for the P1 historical source export script (read-only SELECT exports).

No live DB: a fake connection/cursor is injected into the core export function.
"""

import datetime as dt
import hashlib
import json
import pathlib
from decimal import Decimal

import pytest

from scripts.p1_export import export_p1_sources as p1
from src.macro_sources import SEED_SOURCES

UTC = dt.timezone.utc

AS_OF = dt.date(2026, 6, 30)
NOW = dt.datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC)


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))
        self._last_sql = sql

    def fetchall(self):
        if "macro_observation_vintage" in self._last_sql:
            return list(self._conn.macro_rows)
        if "eod_prices" in self._last_sql:
            return list(self._conn.eod_rows)
        raise AssertionError(f"unexpected query: {self._last_sql!r}")


class FakeConn:
    def __init__(self, macro_rows=(), eod_rows=()):
        self.macro_rows = list(macro_rows)
        self.eod_rows = list(eod_rows)
        self.executed = []

    def cursor(self):
        return FakeCursor(self)


def _macro_rows():
    # Deliberately unsorted; Decimal values with trailing float noise.
    return [
        ("PAYEMS", dt.date(2026, 4, 1), dt.date(2026, 6, 5),
         Decimal("159876.000000"), dt.datetime(2026, 6, 5, tzinfo=UTC),
         1, "alfred", "macro_quadrant_us_v1.0"),
        ("INDPRO", dt.date(2026, 5, 1), dt.date(2026, 6, 17),
         Decimal("103.412500"), dt.datetime(2026, 6, 17, tzinfo=UTC),
         0, "alfred", "macro_quadrant_us_v1.0"),
        ("INDPRO", dt.date(2026, 4, 1), dt.date(2026, 5, 15),
         Decimal("103.200000"), dt.datetime(2026, 5, 15, tzinfo=UTC),
         0, "alfred", "macro_quadrant_us_v1.0"),
    ]


def _eod_rows():
    return [
        ("TLT", dt.date(1998, 1, 5), Decimal("101.500000"),
         Decimal("55.250000"), Decimal("2500000")),
        ("SPY", dt.date(2026, 6, 30), Decimal("601.000000"),
         Decimal("601.000000"), 71000000),
        ("SPY", dt.date(1998, 1, 2), Decimal("97.310000"),
         Decimal("48.700000"), 6300000),
    ]


def _run_export(tmp_path, name="out"):
    conn = FakeConn(macro_rows=_macro_rows(), eod_rows=_eod_rows())
    out_dir = tmp_path / name
    payload = p1.export_p1_sources(conn, out_dir, as_of=AS_OF, now=NOW)
    return conn, out_dir, payload


def _load(out_dir, filename):
    return json.loads((out_dir / filename).read_text(encoding="utf-8"))


# -- (1) canonical row formatting + sort order ------------------------------

def test_macro_vintage_file_canonical_rows_and_sort_order(tmp_path):
    _, out_dir, _ = _run_export(tmp_path)
    rows = _load(out_dir, "macro_observation_vintage.json")
    assert [(r["series_id"], r["observation_period"], r["vintage_date"]) for r in rows] == [
        ("INDPRO", "2026-04-01", "2026-05-15"),
        ("INDPRO", "2026-05-01", "2026-06-17"),
        ("PAYEMS", "2026-04-01", "2026-06-05"),
    ]
    first = rows[0]
    assert first == {
        "available_at": "2026-05-15T00:00:00+00:00",
        "observation_period": "2026-04-01",
        "revision_number": 0,
        "series_id": "INDPRO",
        "source": "alfred",
        "source_spec_version": "macro_quadrant_us_v1.0",
        "value": 103.2,
        "vintage_date": "2026-05-15",
    }
    # Decimal noise stripped: exact int where integral, plain float otherwise.
    assert rows[1]["value"] == 103.4125
    assert rows[2]["value"] == 159876
    assert isinstance(rows[2]["value"], int)


def test_eod_prices_file_canonical_rows_and_sort_order(tmp_path):
    _, out_dir, _ = _run_export(tmp_path)
    rows = _load(out_dir, "eod_prices.json")
    assert [(r["ticker"], r["date"]) for r in rows] == [
        ("SPY", "1998-01-02"),
        ("SPY", "2026-06-30"),
        ("TLT", "1998-01-05"),
    ]
    assert rows[0] == {
        "adjusted_close": 48.7,
        "close": 97.31,
        "date": "1998-01-02",
        "ticker": "SPY",
        "volume": 6300000,
    }
    assert rows[1]["close"] == 601
    assert isinstance(rows[1]["close"], int)
    assert isinstance(rows[2]["volume"], int)


# -- (2) SOURCE.json provenance completeness --------------------------------

def test_source_json_provenance_complete(tmp_path):
    conn, out_dir, _ = _run_export(tmp_path)
    source = _load(out_dir, "SOURCE.json")

    assert source["export_id"] == "open_macro_v03_p1_sources_001"
    assert source["exported_at"] == "2026-07-01T12:00:00+00:00"
    assert source["db_source"] == "tiger_t83f4np6x4"
    assert source["as_of"] == "2026-06-30"
    assert source["runtime_activation"] is False
    assert source["A5"] == "blocked"
    assert source["schema_version"] == 1

    tables = {entry["table"]: entry for entry in source["tables"]}
    assert set(tables) == {"macro_observation_vintage", "eod_prices"}

    executed = {sql: params for sql, params in conn.executed}
    for filename, table, key in (
        ("macro_observation_vintage.json", "macro_observation_vintage", "observation_period"),
        ("eod_prices.json", "eod_prices", "date"),
    ):
        entry = tables[table]
        file_bytes = (out_dir / filename).read_bytes()
        assert entry["sha256"] == hashlib.sha256(file_bytes).hexdigest()
        rows = json.loads(file_bytes.decode("utf-8"))
        assert entry["row_count"] == len(rows)
        dates = [r[key] for r in rows]
        assert entry["min_date"] == min(dates)
        assert entry["max_date"] == max(dates)
        # sql echoed exactly as issued, with the exact params.
        assert entry["sql"] in executed
        assert executed[entry["sql"]] == entry["params"]


# -- (3) read-only enforcement -----------------------------------------------

def test_all_issued_sql_is_read_only_select(tmp_path):
    conn, _, _ = _run_export(tmp_path)
    assert conn.executed, "export issued no SQL"
    for sql, _params in conn.executed:
        assert sql.lstrip().upper().startswith("SELECT"), f"non-SELECT issued: {sql!r}"


def test_execute_select_guard_rejects_non_select():
    conn = FakeConn()
    with pytest.raises(ValueError, match="read-only"):
        p1._execute_select(conn, "DELETE FROM eod_prices", {})
    assert conn.executed == []


# -- (4) determinism ----------------------------------------------------------

def test_determinism_two_runs_byte_identical(tmp_path):
    _, out_a, _ = _run_export(tmp_path, "a")
    _, out_b, _ = _run_export(tmp_path, "b")
    filenames = ["macro_observation_vintage.json", "eod_prices.json", "SOURCE.json"]
    assert sorted(p.name for p in out_a.iterdir()) == sorted(filenames)
    for filename in filenames:
        assert (out_a / filename).read_bytes() == (out_b / filename).read_bytes()


# -- (5) as_of filtering via query params ------------------------------------

def test_as_of_filter_params_passed(tmp_path):
    conn, _, _ = _run_export(tmp_path)
    by_table = {}
    for sql, params in conn.executed:
        if "macro_observation_vintage" in sql:
            by_table["macro"] = (sql, params)
        elif "eod_prices" in sql:
            by_table["eod"] = (sql, params)

    macro_sql, macro_params = by_table["macro"]
    assert "available_at <= %(as_of_end)s" in macro_sql
    assert macro_params["as_of_end"] == "2026-06-30T23:59:59.999999+00:00"

    eod_sql, eod_params = by_table["eod"]
    assert "date >= %(min_date)s" in eod_sql
    assert "date <= %(as_of)s" in eod_sql
    assert eod_params["min_date"] == "1998-01-01"
    assert eod_params["as_of"] == "2026-06-30"


# -- (6) SEED_SOURCES import + sleeve tickers pinned --------------------------

def test_seed_series_ids_come_from_seed_sources(tmp_path):
    expected = sorted(spec.series_id for spec in SEED_SOURCES)
    assert len(expected) == 8
    assert sorted(s.series_id for s in SEED_SOURCES if s.axis == "growth") == [
        "ACOGNO", "INDPRO", "PAYEMS", "PCEC96"]
    assert sorted(s.series_id for s in SEED_SOURCES if s.axis == "inflation") == [
        "AHETPI", "CPILFESL", "MICH", "PPIFIS"]
    assert list(p1.seed_series_ids()) == expected

    conn, _, _ = _run_export(tmp_path)
    macro_params = next(params for sql, params in conn.executed
                        if "macro_observation_vintage" in sql)
    assert macro_params["series_ids"] == expected


def test_sleeve_tickers_pinned(tmp_path):
    assert list(p1.SLEEVE_TICKERS) == ["SPY", "TLT", "TIP", "GLD", "DBC", "SHY"]
    conn, _, _ = _run_export(tmp_path)
    eod_params = next(params for sql, params in conn.executed if "eod_prices" in sql)
    assert eod_params["tickers"] == list(p1.SLEEVE_TICKERS)


# -- output style: p0_sources conventions -------------------------------------

def test_output_files_match_p0_style(tmp_path):
    _, out_dir, _ = _run_export(tmp_path)
    for filename in ("macro_observation_vintage.json", "eod_prices.json", "SOURCE.json"):
        raw = (out_dir / filename).read_bytes()
        text = raw.decode("utf-8")
        assert text.endswith("\n") and not text.endswith("\n\n")
        assert b"\r" not in raw, "output must use LF newlines (byte determinism)"
        payload = json.loads(text)
        assert text == json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"


def test_eod_sql_selects_only_real_schema_columns():
    """eod_prices in prod (Tiger t83f4np6x4) has adj_close, NOT adjusted_close.
    The canonical output keeps the field name adjusted_close via an SQL alias.
    Regression: the fake connection echoed the assumed schema, so a bare
    adjusted_close reference passed tests but would fail against the real DB."""
    from scripts.p1_export.export_p1_sources import EOD_PRICES_SQL

    real_columns = {
        "ticker", "date", "open", "high", "low", "close", "volume",
        "adj_open", "adj_high", "adj_low", "adj_close", "adj_volume",
        "div_cash", "split_factor",
    }
    select_clause = EOD_PRICES_SQL.split("FROM")[0]
    for token in select_clause.replace("SELECT", "").replace("\n", " ").split(","):
        column = token.strip().split(" AS ")[0].strip()
        assert column in real_columns, f"column {column!r} does not exist in eod_prices"
    assert "adj_close AS adjusted_close" in EOD_PRICES_SQL


def test_assert_pinned_db_source_accepts_tiger_service_dsn():
    """SOURCE.json provenance must only ever be stamped after verifying the DSN
    actually references the pinned Tiger service (regression: db_source was an
    unconditional constant, so a staging/local export would carry prod provenance)."""
    from scripts.p1_export.export_p1_sources import assert_pinned_db_source

    dsn = "postgresql://user:secret@t83f4np6x4.abc123.tsdb.cloud.timescale.com:32648/tsdb"
    assert assert_pinned_db_source(dsn) == "tiger_t83f4np6x4"


def test_assert_pinned_db_source_rejects_foreign_dsn():
    import pytest as _pytest

    from scripts.p1_export.export_p1_sources import assert_pinned_db_source

    with _pytest.raises(SystemExit, match="t83f4np6x4"):
        assert_pinned_db_source("postgresql://user:secret@localhost:5434/investintell_alloc")


def test_cli_refuses_to_connect_when_dsn_is_not_pinned(monkeypatch, tmp_path):
    import pytest as _pytest

    from scripts.p1_export import export_p1_sources as mod
    from src import db

    monkeypatch.setattr(db, "resolve_dsn", lambda: "postgresql://u:p@staging-host:5432/other")

    def _must_not_connect(dsn):  # pragma: no cover - reaching this is the failure
        raise AssertionError("connect() must not be called for a non-pinned DSN")

    monkeypatch.setattr(db, "connect", _must_not_connect)

    with _pytest.raises(SystemExit, match="t83f4np6x4"):
        mod.main(["--out", str(tmp_path)])

import pathlib

from src import db


def test_ddl_file_exists_and_declares_table() -> None:
    sql = pathlib.Path("schemas/macro_observation_vintage.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS macro_observation_vintage" in sql
    for col in ("series_id", "observation_period", "vintage_date", "value",
                "available_at", "revision_number", "source", "source_spec_version", "ingested_at"):
        assert col in sql, f"missing column {col}"
    assert "PRIMARY KEY (series_id, observation_period, vintage_date)" in sql
    assert "create_hypertable" in sql


def test_lock_id_registered_and_unique() -> None:
    assert db.LOCK_MACRO_VINTAGE == 900_321
    ids = [v for k, v in vars(db).items() if k.startswith("LOCK_") and isinstance(v, int)]
    assert ids.count(900_321) == 1


import datetime as _dt

from src.workers import macro_vintage as mv

# Real ALFRED output_type=2 for PAYEMS 2010-03 (trimmed to the transitions):
# 129750 (1st print 2010-04-02) -> 129871 -> 129849 (held) -> 129438 (benchmark 2011-02).
_ALFRED_PAYEMS = {
    "observations": [
        {
            "date": "2010-03-01",
            "PAYEMS_20100402": "129750",
            "PAYEMS_20100507": "129871",
            "PAYEMS_20100604": "129849",
            "PAYEMS_20100702": "129849",
            "PAYEMS_20110107": "129849",
            "PAYEMS_20110204": "129438",
            "PAYEMS_20111231": "129438",
        }
    ]
}


def test_parse_alfred_compresses_to_real_revisions() -> None:
    rows = mv.parse_alfred_vintages("PAYEMS", _ALFRED_PAYEMS)
    # 7 vintages collapse to 4 distinct-value revisions
    assert [(r["vintage_date"], r["value"], r["revision_number"]) for r in rows] == [
        (_dt.date(2010, 4, 2), 129750.0, 0),
        (_dt.date(2010, 5, 7), 129871.0, 1),
        (_dt.date(2010, 6, 4), 129849.0, 2),
        (_dt.date(2011, 2, 4), 129438.0, 3),
    ]
    assert all(r["observation_period"] == _dt.date(2010, 3, 1) for r in rows)
    assert all(r["series_id"] == "PAYEMS" for r in rows)


def test_parse_alfred_drops_missing_markers() -> None:
    payload = {"observations": [{"date": "2020-01-01", "X_20200115": ".", "X_20200215": "5.0"}]}
    rows = mv.parse_alfred_vintages("X", payload)
    assert [r["value"] for r in rows] == [5.0]
    assert rows[0]["revision_number"] == 0  # missing print does not consume a revision number


def test_parse_alfred_ignores_non_vintage_columns() -> None:
    payload = {"observations": [{"date": "2020-01-01", "realtime_start": "2020-01-01", "X_20200115": "3.0"}]}
    rows = mv.parse_alfred_vintages("X", payload)
    assert len(rows) == 1 and rows[0]["value"] == 3.0

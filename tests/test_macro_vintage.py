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

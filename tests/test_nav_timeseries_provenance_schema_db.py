from __future__ import annotations

import os
import time
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql


ROOT = Path(__file__).parents[1]
BOOTSTRAP = (ROOT / "schemas" / "instrument_ingestion.sql").read_text(encoding="utf-8")
UPGRADE = (ROOT / "schemas" / "nav_timeseries_provenance.sql").read_text(
    encoding="utf-8"
)
EXPECTED_COLUMNS = {
    "source_nav": "numeric(18,6)",
    "source_nav_kind": "character varying(16)",
    "nav_repair_kind": "character varying(48)",
    "return_start_date": "date",
    "return_source_boundary": "boolean",
    "return_uses_repaired_nav": "boolean",
    "return_semantics": "character varying(48)",
    "return_verification_status": "character varying(24)",
    "calendar_id": "character varying(128)",
    "calendar_version": "character varying(64)",
    "calendar_source": "text",
}
LEGACY_COLUMN_NAMES = {
    "instrument_id",
    "nav_date",
    "nav",
    "return_1d",
    "aum_usd",
    "currency",
    "source",
    "return_type",
}
LOCAL_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _test_dsn() -> str:
    dsn = os.getenv("SEC_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SEC_TEST_DATABASE_URL unavailable; local DB gate not evaluated")
    parsed = psycopg.conninfo.conninfo_to_dict(dsn)
    if parsed.get("host") not in LOCAL_HOSTS:
        raise RuntimeError("SEC_TEST_DATABASE_URL must target a loopback database")
    if not parsed.get("dbname", "").startswith("nav_schema_compat"):
        raise RuntimeError(
            "SEC_TEST_DATABASE_URL must target a disposable nav_schema_compat database"
        )
    return dsn


@pytest.fixture(scope="session")
def dsn() -> str:
    value = _test_dsn()
    with psycopg.connect(value) as conn:
        database, disposable, version = conn.execute(
            "SELECT current_database(), "
            "current_setting('nav_schema_compat.disposable', true), "
            "current_setting('server_version_num')"
        ).fetchone()
        assert database.startswith("nav_schema_compat")
        assert disposable == "on"
        assert int(version) >= 120000
    return value


@pytest.fixture
def isolated_schema(dsn: str):
    schema = f"nav_provenance_{uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield schema
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


def _set_search_path(
    conn: psycopg.Connection, schema: str, *, include_public: bool = False
) -> None:
    template = (
        "SET search_path TO {}, public" if include_public else "SET search_path TO {}"
    )
    conn.execute(sql.SQL(template).format(sql.Identifier(schema)))


def _create_legacy_table(
    conn: psycopg.Connection, schema: str, extra_definition: str | None = None
) -> None:
    extra = (
        sql.SQL(", {} ").format(sql.SQL(extra_definition))
        if extra_definition
        else sql.SQL("")
    )
    conn.execute(
        sql.SQL(
            """
            CREATE TABLE {}.nav_timeseries (
                instrument_id UUID NOT NULL,
                nav_date DATE NOT NULL,
                nav NUMERIC(18,6),
                return_1d NUMERIC(12,8),
                aum_usd NUMERIC(18,2),
                currency VARCHAR(3),
                source VARCHAR(30) DEFAULT 'tiingo',
                return_type VARCHAR(10) NOT NULL DEFAULT 'arithmetic'
                {},
                PRIMARY KEY (instrument_id, nav_date)
            )
            """
        ).format(sql.Identifier(schema), extra)
    )


def _apply_upgrade(conn: psycopg.Connection, schema: str) -> None:
    _set_search_path(conn, schema)
    conn.execute(UPGRADE)


def _catalog(conn: psycopg.Connection, schema: str) -> dict[str, tuple]:
    rows = conn.execute(
        """
        SELECT a.attname,
               pg_catalog.format_type(a.atttypid, a.atttypmod),
               a.attnotnull,
               a.attgenerated,
               a.attidentity,
               ad.oid IS NOT NULL
          FROM pg_catalog.pg_attribute AS a
          JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
          JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
          LEFT JOIN pg_catalog.pg_attrdef AS ad
            ON ad.adrelid = a.attrelid
           AND ad.adnum = a.attnum
         WHERE n.nspname = %s
           AND c.relname = 'nav_timeseries'
           AND a.attnum > 0
           AND NOT a.attisdropped
        """,
        (schema,),
    ).fetchall()
    return {row[0]: tuple(row[1:]) for row in rows}


def _assert_expected_catalog(catalog: dict[str, tuple]) -> None:
    for name, type_name in EXPECTED_COLUMNS.items():
        assert catalog[name] == (type_name, False, "", "", False)


def test_legacy_upgrade_rerun_preserves_rows_and_seven_column_upsert(
    dsn: str, isolated_schema: str
) -> None:
    instrument_id = uuid4()
    with psycopg.connect(dsn, autocommit=True) as conn:
        _create_legacy_table(conn, isolated_schema)
        conn.execute(
            sql.SQL(
                "INSERT INTO {}.nav_timeseries "
                "(instrument_id, nav_date, nav, return_1d, aum_usd, currency, source) "
                "VALUES (%s, DATE '2026-01-02', 10.25, 0.01, 1000, 'USD', 'legacy')"
            ).format(sql.Identifier(isolated_schema)),
            (instrument_id,),
        )
        before = conn.execute(
            sql.SQL(
                "SELECT instrument_id, nav_date, nav, return_1d, aum_usd, currency, source, return_type "
                "FROM {}.nav_timeseries"
            ).format(sql.Identifier(isolated_schema))
        ).fetchall()

        _apply_upgrade(conn, isolated_schema)
        first_catalog = _catalog(conn, isolated_schema)
        _assert_expected_catalog(first_catalog)
        _apply_upgrade(conn, isolated_schema)
        assert _catalog(conn, isolated_schema) == first_catalog

        after = conn.execute(
            sql.SQL(
                "SELECT instrument_id, nav_date, nav, return_1d, aum_usd, currency, source, return_type "
                "FROM {}.nav_timeseries"
            ).format(sql.Identifier(isolated_schema))
        ).fetchall()
        assert after == before
        nulls = conn.execute(
            sql.SQL("SELECT {} FROM {}.nav_timeseries").format(
                sql.SQL(", ").join(map(sql.Identifier, EXPECTED_COLUMNS)),
                sql.Identifier(isolated_schema),
            )
        ).fetchone()
        assert nulls == (None,) * len(EXPECTED_COLUMNS)

        conn.execute(
            sql.SQL(
                "INSERT INTO {}.nav_timeseries "
                "(instrument_id, nav_date, nav, return_1d, return_type, currency, source) "
                "VALUES (%s, DATE '2026-01-02', 11.50, 0.02, 'arithmetic', 'USD', 'producer') "
                "ON CONFLICT (instrument_id, nav_date) DO UPDATE SET "
                "nav=EXCLUDED.nav, return_1d=EXCLUDED.return_1d, "
                "return_type=EXCLUDED.return_type, currency=EXCLUDED.currency, source=EXCLUDED.source"
            ).format(sql.Identifier(isolated_schema)),
            (instrument_id,),
        )
        row = conn.execute(
            sql.SQL("SELECT nav, source, {} FROM {}.nav_timeseries").format(
                sql.SQL(", ").join(map(sql.Identifier, EXPECTED_COLUMNS)),
                sql.Identifier(isolated_schema),
            )
        ).fetchone()
        assert row[:2] == (11.500000, "producer")
        assert row[2:] == (None,) * len(EXPECTED_COLUMNS)


@pytest.mark.parametrize(
    "definition",
    [
        "source_nav NUMERIC(18,5)",
        "source_nav TEXT",
        "source_nav_kind VARCHAR(15)",
        "return_start_date TIMESTAMP",
        "return_source_boundary INTEGER",
        "calendar_source VARCHAR",
        "source_nav NUMERIC(18,6) NOT NULL",
        "source_nav NUMERIC(18,6) DEFAULT NULL",
        "source_nav NUMERIC(18,6) GENERATED ALWAYS AS (1::numeric) STORED",
        "source_nav BIGINT GENERATED BY DEFAULT AS IDENTITY",
    ],
)
def test_wrong_existing_contract_fails_without_partial_additions(
    dsn: str, isolated_schema: str, definition: str
) -> None:
    existing_name = definition.split()[0].lower()
    with psycopg.connect(dsn, autocommit=True) as conn:
        _create_legacy_table(conn, isolated_schema, definition)
        before = _catalog(conn, isolated_schema)
        with pytest.raises(psycopg.Error) as raised:
            _apply_upgrade(conn, isolated_schema)
        assert raised.value.sqlstate == "42804"
        conn.rollback()
        after = _catalog(conn, isolated_schema)
        assert after == before
        assert set(after) == LEGACY_COLUMN_NAMES | {existing_name}


def test_last_contract_field_mismatch_is_atomic(dsn: str, isolated_schema: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        _create_legacy_table(conn, isolated_schema, "calendar_source VARCHAR")
        with pytest.raises(psycopg.Error) as raised:
            _apply_upgrade(conn, isolated_schema)
        assert raised.value.sqlstate == "42804"
        conn.rollback()
        assert set(_catalog(conn, isolated_schema)) == LEGACY_COLUMN_NAMES | {
            "calendar_source"
        }


def test_missing_or_wrong_schema_never_falls_back(
    dsn: str, isolated_schema: str
) -> None:
    shadow = f"shadow_{uuid4().hex}"
    wrong = f"wrong_{uuid4().hex}"
    missing = f"missing_{uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(shadow)))
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(wrong)))
        try:
            _create_legacy_table(conn, shadow)
            _set_search_path(conn, wrong)
            with pytest.raises(psycopg.Error) as absent:
                conn.execute(UPGRADE)
            assert absent.value.sqlstate == "42P01"
            conn.rollback()
            assert set(_catalog(conn, shadow)) == LEGACY_COLUMN_NAMES

            _set_search_path(conn, missing)
            with pytest.raises(psycopg.Error) as absent_schema:
                conn.execute(UPGRADE)
            assert absent_schema.value.sqlstate == "3F000"
            conn.rollback()
            assert set(_catalog(conn, shadow)) == LEGACY_COLUMN_NAMES

            conn.execute(
                sql.SQL("SET search_path TO {}, {}").format(
                    sql.Identifier(missing), sql.Identifier(shadow)
                )
            )
            with pytest.raises(psycopg.Error) as fallback_schema:
                conn.execute(UPGRADE)
            assert fallback_schema.value.sqlstate == "3F000"
            conn.rollback()
            assert set(_catalog(conn, shadow)) == LEGACY_COLUMN_NAMES
        finally:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(wrong))
            )
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(shadow))
            )


def test_wrong_relation_kind_is_rejected(dsn: str, isolated_schema: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("CREATE VIEW {}.nav_timeseries AS SELECT 1 AS value").format(
                sql.Identifier(isolated_schema)
            )
        )
        with pytest.raises(psycopg.Error) as raised:
            _apply_upgrade(conn, isolated_schema)
        assert raised.value.sqlstate == "42809"
        conn.rollback()


def test_conflicting_lock_times_out_rolls_back_then_rerun_passes(
    dsn: str, isolated_schema: str
) -> None:
    with psycopg.connect(dsn, autocommit=True) as setup:
        _create_legacy_table(setup, isolated_schema)

    blocker = psycopg.connect(dsn)
    contender = psycopg.connect(dsn, autocommit=True)
    try:
        _set_search_path(blocker, isolated_schema)
        blocker.execute("LOCK TABLE nav_timeseries IN ACCESS SHARE MODE")
        _set_search_path(contender, isolated_schema)
        started = time.monotonic()
        with pytest.raises(psycopg.Error) as raised:
            contender.execute(UPGRADE)
        elapsed = time.monotonic() - started
        assert raised.value.sqlstate == "55P03"
        assert 1.5 <= elapsed < 8
        contender.rollback()
        assert set(_catalog(blocker, isolated_schema)) == LEGACY_COLUMN_NAMES
        blocker.rollback()

        _apply_upgrade(contender, isolated_schema)
        _assert_expected_catalog(_catalog(contender, isolated_schema))
    finally:
        blocker.close()
        contender.close()


def _timescale_version(conn: psycopg.Connection) -> str | None:
    row = conn.execute(
        "SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'timescaledb'"
    ).fetchone()
    return row[0] if row else None


def test_timescale_fresh_bootstrap_and_upgrade(dsn: str, isolated_schema: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        version = _timescale_version(conn)
        if version is None:
            pytest.skip(
                "TimescaleDB extension unavailable; fresh hypertable gate not evaluated"
            )
        _set_search_path(conn, isolated_schema, include_public=True)
        conn.execute(BOOTSTRAP)
        _apply_upgrade(conn, isolated_schema)
        _apply_upgrade(conn, isolated_schema)
        _assert_expected_catalog(_catalog(conn, isolated_schema))
        hypertable = conn.execute(
            """
            SELECT 1 FROM timescaledb_information.hypertables
             WHERE hypertable_schema = %s AND hypertable_name = 'nav_timeseries'
            """,
            (isolated_schema,),
        ).fetchone()
        assert hypertable == (1,)


def test_timescale_compressed_legacy_chunk_upgrade(
    dsn: str, isolated_schema: str
) -> None:
    first_id = uuid4()
    second_id = uuid4()
    with psycopg.connect(dsn, autocommit=True) as conn:
        version = _timescale_version(conn)
        if version is None:
            pytest.skip(
                "TimescaleDB extension unavailable; compressed hypertable gate not evaluated"
            )
        _create_legacy_table(conn, isolated_schema)
        _set_search_path(conn, isolated_schema, include_public=True)
        relation_name = f"{isolated_schema}.nav_timeseries"
        conn.execute(
            "SELECT create_hypertable(%s::regclass, 'nav_date', chunk_time_interval => INTERVAL '1 month')",
            (relation_name,),
        )
        conn.execute(
            sql.SQL(
                "ALTER TABLE {}.nav_timeseries SET "
                "(timescaledb.compress, timescaledb.compress_segmentby = 'instrument_id')"
            ).format(sql.Identifier(isolated_schema))
        )
        conn.execute(
            sql.SQL(
                "INSERT INTO {}.nav_timeseries "
                "(instrument_id, nav_date, nav, return_1d, return_type, currency, source) VALUES "
                "(%s, DATE '2025-01-02', 10, 0.01, 'arithmetic', 'USD', 'legacy'), "
                "(%s, DATE '2025-03-02', 12, 0.02, 'arithmetic', 'USD', 'legacy')"
            ).format(sql.Identifier(isolated_schema)),
            (first_id, second_id),
        )
        chunk = conn.execute(
            "SELECT show_chunks(%s::regclass) ORDER BY 1 LIMIT 1",
            (relation_name,),
        ).fetchone()[0]
        conn.execute(
            "SELECT compress_chunk(%s::regclass, if_not_compressed => true)", (chunk,)
        )

        compressed_before = conn.execute(
            """
            SELECT count(*) FROM timescaledb_information.chunks
             WHERE hypertable_schema = %s
               AND hypertable_name = 'nav_timeseries'
               AND is_compressed
            """,
            (isolated_schema,),
        ).fetchone()[0]
        assert compressed_before >= 1

        _apply_upgrade(conn, isolated_schema)
        _apply_upgrade(conn, isolated_schema)
        _assert_expected_catalog(_catalog(conn, isolated_schema))
        rows = conn.execute(
            sql.SQL(
                "SELECT instrument_id, nav, source_nav, calendar_source "
                "FROM {}.nav_timeseries ORDER BY nav_date"
            ).format(sql.Identifier(isolated_schema))
        ).fetchall()
        assert rows == [(first_id, 10, None, None), (second_id, 12, None, None)]
        compressed_after = conn.execute(
            """
            SELECT count(*) FROM timescaledb_information.chunks
             WHERE hypertable_schema = %s
               AND hypertable_name = 'nav_timeseries'
               AND is_compressed
            """,
            (isolated_schema,),
        ).fetchone()[0]
        assert compressed_after == compressed_before

        conn.execute(
            sql.SQL(
                "INSERT INTO {}.nav_timeseries "
                "(instrument_id, nav_date, nav, return_1d, return_type, currency, source) "
                "VALUES (%s, DATE '2025-03-02', 13, 0.03, 'arithmetic', 'USD', 'producer') "
                "ON CONFLICT (instrument_id, nav_date) DO UPDATE SET "
                "nav=EXCLUDED.nav, return_1d=EXCLUDED.return_1d, "
                "return_type=EXCLUDED.return_type, currency=EXCLUDED.currency, source=EXCLUDED.source"
            ).format(sql.Identifier(isolated_schema)),
            (second_id,),
        )
        assert conn.execute(
            sql.SQL(
                "SELECT nav, source_nav, calendar_source FROM {}.nav_timeseries "
                "WHERE instrument_id = %s"
            ).format(sql.Identifier(isolated_schema)),
            (second_id,),
        ).fetchone() == (13, None, None)

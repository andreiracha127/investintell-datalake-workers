"""Disposable Timescale integration gate for the N-PORT loader.

Run against ``timescale/timescaledb:2.27.2-pg18`` with a loopback DSN in
``NPORT_TEST_DATABASE_URL``. The database name must start with ``nport_proof``
and ``ALTER DATABASE ... SET nport_loader_proof.disposable = 'on'`` must have
marked it disposable. No production URL is read. Each test creates and drops
its own schema; a configured compression policy is deliberately left paused.
``NPORT_REAL_SEED_DIR`` optionally enables the 58,691-row real-provider seed
gate for 2026-05-29; larger dates are never loaded by these tests.
"""

from __future__ import annotations

import concurrent.futures
import csv
import datetime as dt
import json
import os
import threading
import time
from decimal import Decimal, localcontext
from pathlib import Path
from uuid import uuid4

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql  # noqa: E402

from tools.nport_dera import nport_parallel_load as load  # noqa: E402


@pytest.fixture(scope="module")
def local_dsn() -> str:
    value = os.getenv("NPORT_TEST_DATABASE_URL")
    if not value:
        pytest.skip(
            "NPORT_TEST_DATABASE_URL unavailable; disposable Timescale gate not evaluated"
        )
    parsed = psycopg.conninfo.conninfo_to_dict(value)
    if parsed.get("host") not in {"127.0.0.1", "::1", "localhost"}:
        raise RuntimeError("NPORT_TEST_DATABASE_URL must target a loopback database")
    if not parsed.get("dbname", "").startswith("nport_proof"):
        raise RuntimeError(
            "NPORT_TEST_DATABASE_URL must target a disposable nport_proof database"
        )
    with psycopg.connect(value, autocommit=True) as conn:
        assert (
            conn.execute(
                "SELECT current_setting('nport_loader_proof.disposable', true)"
            ).fetchone()[0]
            == "on"
        )
        conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        assert (
            conn.execute(
                "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
            ).fetchone()[0]
            == "2.27.2"
        )
        assert int(conn.execute("SHOW server_version_num").fetchone()[0]) // 10000 == 18
    return value


@pytest.fixture
def database(local_dsn: str):
    schema = f"nport_loader_{uuid4().hex}"
    # Explicit unlimited compressed-chunk DML setting mirrors the operational
    # loader environment, while search_path isolates table names used by it.
    dsn = psycopg.conninfo.make_conninfo(
        local_dsn,
        options=f"-c search_path={schema},public -c timescaledb.max_tuples_decompressed_per_dml_transaction=0",
    )
    with psycopg.connect(local_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                "CREATE TABLE sec_nport_holdings ("
                "report_date date NOT NULL, cik text NOT NULL, cusip text NOT NULL, "
                "isin text, issuer_name text, asset_class text, sector text, market_value bigint, "
                "quantity numeric, currency text, pct_of_nav numeric, is_restricted boolean, "
                "fair_value_level text, series_id text NOT NULL, cik_padded text, "
                "created_at timestamptz NOT NULL, UNIQUE (report_date,series_id,cusip))"
            )
            conn.execute(
                "SELECT create_hypertable('sec_nport_holdings', 'report_date', "
                "chunk_time_interval => INTERVAL '1 month')"
            )
            conn.execute(
                "ALTER TABLE sec_nport_holdings SET (timescaledb.compress, "
                "timescaledb.compress_segmentby='series_id', "
                "timescaledb.compress_orderby='report_date,cusip')"
            )
            policy = conn.execute(
                "SELECT add_compression_policy('sec_nport_holdings', INTERVAL '4 months', "
                "schedule_interval => INTERVAL '2 days')"
            ).fetchone()[0]
            conn.execute("SELECT alter_job(%s, scheduled => false)", (policy,))
            conn.execute(
                "CREATE MATERIALIZED VIEW cagg_nport_series_profile "
                "WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS "
                "SELECT time_bucket(INTERVAL '1 day', report_date) AS report_day, series_id, "
                "count(*) AS n_holdings, sum(market_value) AS total_market_value, "
                "sum(pct_of_nav) AS coverage_pct, "
                "count(*) FILTER (WHERE cusip LIKE 'IS:%' OR cusip LIKE 'LE:%' OR cusip LIKE 'H:%') AS n_synthetic "
                "FROM sec_nport_holdings GROUP BY 1,2 WITH NO DATA"
            )
        yield dsn
    finally:
        with psycopg.connect(local_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


def _seed(
    directory: Path,
    date: str,
    *,
    series: str = "S1",
    prefix: str = "A",
    filled: bool = True,
    count: int = 4,
    pct_total: int = 100,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{date}-{prefix}.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=load.CSV_COLS)
        writer.writeheader()
        for i in range(count):
            writer.writerow(
                {
                    "report_date": date,
                    "cik": "123",
                    "cusip": f"{prefix}{i:08d}",
                    "isin": "US0378331005" if filled else "",
                    "issuer_name": "Test issuer",
                    "asset_class": "EC",
                    "sector": "Technology",
                    "market_value": 100,
                    "quantity": 1,
                    "currency": "USD",
                    "pct_of_nav": pct_total / count,
                    "is_restricted": "false",
                    "fair_value_level": "1",
                    "series_id": series,
                }
            )
    return path


def _run(dsn: str, directory: Path, dates: str, *extra: str) -> int:
    return load.main(
        [
            "--seed-dir",
            str(directory),
            "--dsn",
            dsn,
            "--workers",
            "2",
            "--only-report-dates",
            dates,
            "--skip-matview",
            *extra,
        ]
    )


def _rows(dsn: str) -> list[tuple]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT report_date,series_id,cusip,isin FROM sec_nport_holdings ORDER BY 1,2,3"
        ).fetchall()


def _policy(dsn: str) -> tuple | None:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT job_id,schedule_interval,scheduled,config FROM timescaledb_information.jobs "
            "WHERE hypertable_schema=current_schema() AND hypertable_name='sec_nport_holdings' "
            "AND proc_name='policy_compression'"
        ).fetchone()


def _reject_isins(dsn: str, predicate: str) -> None:
    """Force the real post-insert verdict to fail after a clean preflight."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "CREATE FUNCTION reject_inserted_isin() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN IF "
            + predicate
            + " THEN NEW.isin := NULL; END IF; RETURN NEW; END $$"
        )
        conn.execute(
            "CREATE TRIGGER reject_isin BEFORE INSERT ON sec_nport_holdings "
            "FOR EACH ROW EXECUTE FUNCTION reject_inserted_isin()"
        )


def _secapi_manifest(
    directory: Path,
    date: str,
    *,
    series: str = "S1",
    count: int = 4,
    pct_sum: str = "100",
) -> None:
    (directory / "manifest.json").write_text(
        json.dumps(
            {
                "generated_by": "tools.nport_secapi.convert",
                "validation_contract": 1,
                "report_dates": {
                    date: {
                        "file": f"{date}-A.csv",
                        "rows": count,
                        "series": 1,
                        "partial": False,
                        "filing_quality": [
                            {
                                "accession": "0000000123-26-000001",
                                "series_id": series,
                                "source_holdings": count,
                                "malformed_holdings": 0,
                                "rows": count,
                                "conflict_key_dupes": 0,
                                "source_pct_sum": pct_sum,
                                "source_pct_present": count,
                            }
                        ],
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def test_clean_date_commits_and_cagg_can_publish(database, tmp_path):
    _seed(tmp_path, "2026-05-31")
    before = _policy(database)
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only", "--dry-run") == 0
    assert _rows(database) == []
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only") == 0
    assert len(_rows(database)) == 4
    assert _policy(database) == before
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute(
            "CALL refresh_continuous_aggregate('cagg_nport_series_profile', "
            "'2026-05-31'::date, '2026-06-01'::date)"
        )
        assert conn.execute(
            "SELECT series_id,n_holdings,total_market_value,coverage_pct FROM cagg_nport_series_profile"
        ).fetchall() == [("S1", 4, 400, 100)]


def test_rejected_date_has_no_committed_rows(database, tmp_path):
    _seed(tmp_path, "2026-05-31")
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute("CREATE SEQUENCE insert_attempts")
        conn.execute(
            "CREATE FUNCTION record_insert_attempt() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN PERFORM nextval('insert_attempts'); NEW.isin := NULL; RETURN NEW; END $$"
        )
        conn.execute(
            "CREATE TRIGGER observe_insert BEFORE INSERT ON sec_nport_holdings "
            "FOR EACH ROW EXECUTE FUNCTION record_insert_attempt()"
        )
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only") == 2
    assert _rows(database) == []
    with psycopg.connect(database, autocommit=True) as conn:
        # Sequence advances survive a rollback: rows really reached the target
        # table before the transactional verify refused the commit.
        assert conn.execute(
            "SELECT last_value,is_called FROM insert_attempts"
        ).fetchone() == (4, True)
        conn.execute(
            "CALL refresh_continuous_aggregate('cagg_nport_series_profile', "
            "'2026-05-31'::date, '2026-06-01'::date)"
        )
        assert (
            conn.execute("SELECT count(*) FROM cagg_nport_series_profile").fetchone()[0]
            == 0
        )


def test_revisited_date_skips_existing_series(database, tmp_path):
    old = tmp_path / "old"
    _seed(old, "2026-05-31")
    assert _run(database, old, "2026-05-31", "--new-series-only") == 0
    before = _rows(database)
    revised = tmp_path / "revised"
    _seed(revised, "2026-05-31", prefix="B", filled=False)
    _seed(revised, "2026-05-31", series="S2", prefix="C")
    assert _run(database, revised, "2026-05-31", "--new-series-only", "--dry-run") == 0
    assert _run(database, revised, "2026-05-31", "--new-series-only") == 0
    after = _rows(database)
    assert [row for row in after if row[1] == "S1"] == before
    assert len([row for row in after if row[1] == "S2"]) == 4


def test_two_concurrent_new_series_loads_keep_one_whole_filing(database, tmp_path):
    left, right = tmp_path / "left", tmp_path / "right"
    _seed(left, "2026-05-31", prefix="L", count=12)
    _seed(right, "2026-05-31", prefix="R", count=12)
    barrier = threading.Barrier(2)

    def run(directory):
        barrier.wait(timeout=10)
        return _run(database, directory, "2026-05-31", "--new-series-only")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = [pool.submit(run, directory) for directory in (left, right)]
        assert [result.result(timeout=30) for result in results] == [0, 0]
    rows = _rows(database)
    assert len(rows) == 12
    assert len({row[2][0] for row in rows}) == 1


def test_concurrent_transactions_serialize_new_series_snapshot(database, tmp_path):
    left = _seed(tmp_path / "left", "2026-05-31", prefix="L", count=12)
    right = _seed(tmp_path / "right", "2026-05-31", prefix="R", count=12)
    barrier = threading.Barrier(2)

    def run(path):
        barrier.wait(timeout=10)
        return load.load_one(
            database, str(path), dt.datetime.now(dt.UTC), ["2026-05-31"], True
        )

    # Bypass the main CLI's maintenance lock so two COPY transactions really
    # compete at NEW_SERIES_LOCK and must read a post-wait table snapshot.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, path) for path in (left, right)]
        inserted = [future.result(timeout=30)[1] for future in futures]
    assert sorted(inserted) == [0, 12]
    rows = _rows(database)
    assert len(rows) == 12
    assert len({row[2][0] for row in rows}) == 1


def test_compressed_chunk_load_restores_compression_and_policy(database, tmp_path):
    old = tmp_path / "old"
    _seed(old, "2026-05-31")
    assert _run(database, old, "2026-05-31", "--new-series-only") == 0
    with psycopg.connect(database, autocommit=True) as conn:
        chunk = conn.execute(
            "SELECT show_chunks('sec_nport_holdings')::text"
        ).fetchone()[0]
        conn.execute("SELECT compress_chunk(%s::regclass)", (chunk,))
        assert (
            conn.execute(
                "SELECT current_setting('timescaledb.max_tuples_decompressed_per_dml_transaction')"
            ).fetchone()[0]
            == "0"
        )
    before = _policy(database)
    added = tmp_path / "added"
    _seed(added, "2026-05-31", series="S2", prefix="B")
    assert _run(database, added, "2026-05-31", "--new-series-only") == 0
    assert len(_rows(database)) == 8
    assert _policy(database) == before
    with psycopg.connect(database) as conn:
        assert conn.execute(
            "SELECT is_compressed FROM timescaledb_information.chunks "
            "WHERE hypertable_schema=current_schema() AND hypertable_name='sec_nport_holdings'"
        ).fetchall() == [(True,)]


def test_missing_matview_does_not_remove_compression_policy(database, tmp_path):
    _seed(tmp_path, "2026-05-31")
    before = _policy(database)
    argv = [
        "--seed-dir",
        str(tmp_path),
        "--dsn",
        database,
        "--workers",
        "1",
        "--only-report-dates",
        "2026-05-31",
        "--new-series-only",
    ]
    result = load.main(argv)
    assert result != 0
    assert _policy(database) == before


def test_matview_failure_after_commit_restores_running_policy(database, tmp_path):
    _seed(tmp_path, "2026-05-31")
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute(
            "CREATE MATERIALIZED VIEW mv_nport_sector_attribution AS "
            "SELECT report_date,series_id,count(*) FROM sec_nport_holdings GROUP BY 1,2"
        )
        conn.execute(
            "CREATE UNIQUE INDEX sector_attribution_key ON mv_nport_sector_attribution (report_date,series_id)"
        )
        job = _policy(database)[0]
        conn.execute(
            "SELECT alter_job(%s, scheduled => true, next_start => now() + INTERVAL '1 year')",
            (job,),
        )
        # A real DB-side failure after the clean preflight and accepted commit:
        # finalize's REFRESH finds that the requested relation vanished.
        conn.execute(
            "CREATE FUNCTION remove_refresh_target() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN IF to_regclass('mv_nport_sector_attribution') IS NOT NULL THEN "
            "DROP MATERIALIZED VIEW mv_nport_sector_attribution; END IF; RETURN NEW; END $$"
        )
        conn.execute(
            "CREATE TRIGGER remove_refresh_target BEFORE INSERT ON sec_nport_holdings "
            "FOR EACH ROW EXECUTE FUNCTION remove_refresh_target()"
        )
    before = _policy(database)
    assert (
        load.main(
            [
                "--seed-dir",
                str(tmp_path),
                "--dsn",
                database,
                "--workers",
                "1",
                "--only-report-dates",
                "2026-05-31",
                "--new-series-only",
            ]
        )
        == 1
    )
    assert (
        len(_rows(database)) == 4
    )  # verified rows remain valid despite refresh failure
    assert _policy(database) == before


def test_same_date_across_csvs_rolls_back_as_one_transaction(database, tmp_path):
    _seed(tmp_path, "2026-05-31", series="S1", prefix="A")
    _seed(tmp_path, "2026-05-31", series="S2", prefix="B")
    _reject_isins(database, "NEW.series_id = 'S2'")
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only") == 2
    assert _rows(database) == []


def test_rejected_delete_first_restores_existing_date(database, tmp_path):
    original = tmp_path / "original"
    _seed(original, "2026-05-31")
    assert _run(database, original, "2026-05-31", "--new-series-only") == 0
    before = _rows(database)
    replacement = tmp_path / "replacement"
    _seed(replacement, "2026-05-31", prefix="B")
    _reject_isins(database, "NEW.cusip LIKE 'B%'")
    assert _run(database, replacement, "2026-05-31", "--delete-first") == 2
    assert _rows(database) == before


def test_loader_preserves_absence_of_compression_policy(database, tmp_path):
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute("SELECT remove_compression_policy('sec_nport_holdings')")
    _seed(tmp_path, "2026-05-31")
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only") == 0
    assert _policy(database) is None


def test_disjoint_dates_commit_independently(database, tmp_path):
    _seed(tmp_path, "2026-05-31")
    _seed(tmp_path, "2026-06-30", prefix="B")
    _reject_isins(database, "NEW.report_date = DATE '2026-06-30'")
    assert _run(database, tmp_path, "2026-05-31,2026-06-30", "--new-series-only") == 2
    rows = _rows(database)
    assert len(rows) == 4
    assert {str(row[0]) for row in rows} == {"2026-05-31"}


def test_policy_restore_error_is_reported_as_failure(database, tmp_path):
    _seed(tmp_path, "2026-05-31")
    job = _policy(database)[0]
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute(
            "SELECT public.alter_job(%s, scheduled => true, next_start => now() + INTERVAL '1 year')",
            (job,),
        )
        # The isolated schema shadows only the two-argument calls used by the
        # loader. Pause still executes the real Timescale function; restore
        # deliberately fails in PostgreSQL so a success exit cannot mask it.
        conn.execute(
            "CREATE FUNCTION alter_job(job_id integer, scheduled boolean) RETURNS boolean "
            "LANGUAGE plpgsql AS $$ BEGIN IF scheduled THEN "
            "RAISE EXCEPTION 'injected compression policy restoration failure'; END IF; "
            "PERFORM public.alter_job(job_id, scheduled => scheduled); RETURN true; END $$"
        )
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only") == 1
    assert len(_rows(database)) == 4
    policy = _policy(database)
    assert policy[0] == job
    assert (
        policy[2] is False
    )  # the failure was explicit; restoration did not silently succeed


@pytest.mark.parametrize(
    "column,value", [("market_value", "NULL"), ("pct_of_nav", "1")]
)
def test_secapi_quality_checks_actual_inserted_rows(database, tmp_path, column, value):
    _seed(tmp_path, "2026-05-31")
    _secapi_manifest(tmp_path, "2026-05-31")
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute(
            "CREATE FUNCTION corrupt_inserted_quality() RETURNS trigger LANGUAGE plpgsql AS "
            f"$$ BEGIN NEW.{column} := {value}; RETURN NEW; END $$"
        )
        conn.execute(
            "CREATE TRIGGER corrupt_quality BEFORE INSERT ON sec_nport_holdings "
            "FOR EACH ROW EXECUTE FUNCTION corrupt_inserted_quality()"
        )
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only", "--dry-run") == 0
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only") == 2
    assert _rows(database) == []


def test_authentic_leverage_matches_source_percentage_reference(database, tmp_path):
    _seed(tmp_path, "2026-05-31", pct_total=200)
    _secapi_manifest(tmp_path, "2026-05-31", pct_sum="200")
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only", "--dry-run") == 0
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only") == 0
    assert len(_rows(database)) == 4
    with psycopg.connect(database) as conn:
        assert (
            conn.execute("SELECT sum(pct_of_nav) FROM sec_nport_holdings").fetchone()[0]
            == 200
        )


def test_worker_detects_and_recovers_stale_cagg_after_committed_load(
    database, tmp_path
):
    from src.workers import nport_secapi_monthly as lane

    _seed(tmp_path, "2026-05-31")
    _secapi_manifest(tmp_path, "2026-05-31")
    assert _run(database, tmp_path, "2026-05-31", "--new-series-only") == 0
    counts = lane.report_date_counts(database, ["2026-05-31"])
    assert counts == {"2026-05-31": {"rows": 4, "series": 1}}
    assert lane.cagg_needs_refresh(
        database, "2026-05-31", counts["2026-05-31"]["series"]
    )
    ranges = lane.refresh_ranges(["2026-05-31"], set())
    assert ranges == [("2026-05-31", "2026-06-01")]
    for start, end in ranges:
        lane.refresh_cagg(database, start, end)
    assert not lane.cagg_needs_refresh(
        database, "2026-05-31", counts["2026-05-31"]["series"]
    )
    with psycopg.connect(database) as conn:
        assert conn.execute(
            "SELECT n_holdings,coverage_pct FROM cagg_nport_series_profile"
        ).fetchall() == [(4, 100)]


def test_real_secapi_date_load_values_cagg_and_idempotency(database):
    real_seed = os.getenv("NPORT_REAL_SEED_DIR")
    if not real_seed:
        pytest.skip(
            "NPORT_REAL_SEED_DIR unavailable; real provider-record gate not evaluated"
        )
    directory = Path(real_seed)
    date = "2026-05-29"
    path = directory / f"{date}.csv"
    assert path.is_file()
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    entry = manifest["report_dates"][date]
    assert entry["rows"] == 58_691
    assert entry["series"] == 197
    expected: dict[str, list] = {}
    isin = 0
    # Independently aggregate the real emitted records for comparison with the
    # target hypertable and cagg. No production connection or provider call.
    with localcontext() as context:
        context.prec = 100
        with path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                assert row["report_date"] == date
                aggregate = expected.setdefault(row["series_id"], [0, 0, Decimal(0)])
                aggregate[0] += 1
                aggregate[1] += int(row["market_value"]) if row["market_value"] else 0
                aggregate[2] += (
                    Decimal(row["pct_of_nav"]) if row["pct_of_nav"] else Decimal(0)
                )
                isin += bool(row["isin"])
    expected_series = sorted((series, *values) for series, values in expected.items())
    before_policy = _policy(database)
    started = time.monotonic()
    assert (
        _run(database, directory, date, "--new-series-only", "--only", f"{date}.csv")
        == 0
    )
    load_seconds = time.monotonic() - started
    with psycopg.connect(database) as conn:
        actual = conn.execute(
            "SELECT series_id,count(*),sum(market_value),sum(pct_of_nav) "
            "FROM sec_nport_holdings GROUP BY 1 ORDER BY 1"
        ).fetchall()
        assert actual == expected_series
        snapshot = conn.execute(
            "SELECT count(*),count(DISTINCT series_id),sum(market_value), "
            "count(*) FILTER (WHERE isin IS NOT NULL AND isin <> ''), "
            "min(created_at),max(created_at) FROM sec_nport_holdings"
        ).fetchone()
        assert snapshot[:2] == (58_691, 197)
        assert snapshot[3] == isin
    started = time.monotonic()
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute(
            "CALL refresh_continuous_aggregate('cagg_nport_series_profile', "
            "'2026-05-29'::date,'2026-05-30'::date)"
        )
        cagg = conn.execute(
            "SELECT series_id,n_holdings,total_market_value,coverage_pct "
            "FROM cagg_nport_series_profile ORDER BY 1"
        ).fetchall()
        assert cagg == expected_series
    refresh_seconds = time.monotonic() - started
    started = time.monotonic()
    assert (
        _run(database, directory, date, "--new-series-only", "--only", f"{date}.csv")
        == 0
    )
    rerun_seconds = time.monotonic() - started
    with psycopg.connect(database) as conn:
        assert (
            conn.execute(
                "SELECT count(*),count(DISTINCT series_id),sum(market_value), "
                "count(*) FILTER (WHERE isin IS NOT NULL AND isin <> ''), "
                "min(created_at),max(created_at) FROM sec_nport_holdings"
            ).fetchone()
            == snapshot
        )
        assert (
            conn.execute(
                "SELECT series_id,count(*),sum(market_value),sum(pct_of_nav) "
                "FROM sec_nport_holdings GROUP BY 1 ORDER BY 1"
            ).fetchall()
            == actual
        )
    assert _policy(database) == before_policy
    print(
        f"real sec-api {date}: rows={snapshot[0]:,} series={snapshot[1]} "
        f"market_value_usd={snapshot[2]} isin={snapshot[3]:,} "
        f"load_s={load_seconds:.3f} refresh_s={refresh_seconds:.3f} rerun_s={rerun_seconds:.3f}"
    )

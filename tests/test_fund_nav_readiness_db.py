"""Real, non-skipping PG18/Timescale W1 contract tests on a disposable database."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import psycopg
import pytest
from psycopg import sql

from scripts import fund_nav_readiness_schema as operator
from src.db import LOCK_FUND_NAV_READINESS, LOCK_INSTRUMENT_INGESTION, advisory_lock
from src.workers import fund_nav_readiness as readiness
from src.workers import instrument_ingestion as ingest
from src.workers import nav_current_daily_chain as chain
from src.workers import risk_metrics as risk
from src.workers._nav_policy import (
    FEATURE_DEFINITION_VERSION,
    calendar_digest,
    canonical_digest,
    level_evidence_digest,
    risk_universe_digest,
)
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS
from src.workers._tiingo import NavObservation
from src.workers._tiingo import NavFetchResult

ROOT = Path(__file__).parents[1]
SCHEMA_SQL = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_text(
    encoding="utf-8"
)
NAV_SQL = (ROOT / "schemas" / "instrument_ingestion.sql").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def test_dsn():
    dsn = os.environ["NAV_W1_TEST_DSN"]  # no skip: absence fails the DB gate
    parsed = psycopg.conninfo.conninfo_to_dict(dsn)
    assert parsed.get("host") in (
        "127.0.0.1",
        "localhost",
        "::1",
        "host.docker.internal",
    )
    if parsed.get("host") == "host.docker.internal":
        assert parsed.get("port") in (
            "55487",
            "55488",
        )  # owned disposable W1 containers
    assert parsed.get("dbname", "").startswith("nav_readiness_w1")
    with psycopg.connect(dsn) as conn:
        version, extension = conn.execute(
            """SELECT current_setting('server_version_num')::int,
               (SELECT extversion FROM pg_extension WHERE extname='timescaledb')"""
        ).fetchone()
        assert version >= 180000 and extension is not None
    return dsn


@pytest.fixture
def schema(test_dsn):
    name = "nav_w1_" + uuid.uuid4().hex
    with psycopg.connect(test_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
    try:
        yield name
    finally:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))


def _connect(dsn, schema, *, autocommit=False):
    return psycopg.connect(
        dsn, autocommit=autocommit, options=f"-csearch_path={schema},public"
    )


def _grid(end=None):
    end = end or dt.date.today() - dt.timedelta(days=1)
    while end.weekday() >= 5:
        end -= dt.timedelta(days=1)
    dates = []
    d = end
    while len(dates) < 401:
        if d.weekday() < 5 and d != dt.date(2026, 9, 7):
            dates.append(d)
        d -= dt.timedelta(days=1)
    return list(reversed(dates))


def _install(conn, schema):
    """Apply the readiness DDL exactly like the operator (<schema>, pg_temp)."""
    operator.apply_ddl(conn, schema, SCHEMA_SQL.encode("utf-8"))
    conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))


def _access_fixture(conn, schema):
    """Light runtime prerequisites that W1 never grants: schema USAGE, the
    external risk read model and SELECT on the external read dependencies."""
    ident = sql.Identifier(schema)
    conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO app_runtime").format(ident))
    if conn.execute("SELECT to_regclass('fund_risk_latest_mv') IS NULL").fetchone()[0]:
        conn.execute(RISK_READ_MODEL_SQL)
    conn.execute("GRANT SELECT ON nav_timeseries, fund_risk_latest_mv TO app_runtime")


def _bootstrap(dsn, schema):
    with _connect(dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        _install(conn, schema)
        _install(conn, schema)  # additive, rerunnable, no MV replacement
        _access_fixture(conn, schema)
        conn.execute("CREATE TABLE funds_profile_mv (instrument_id uuid PRIMARY KEY)")


def _plan(check_args, capsys):
    """Run the operator check and return its plan v4 digest (asserts exit 0)."""
    assert operator.main(check_args) == 0, capsys.readouterr().out
    out = json.loads(capsys.readouterr().out)
    assert out["plan"]["plan_version"] == "nav-schema-plan-v4"
    return out["plan_sha256"]


def _write(conn, rows):
    """Attributed write for mechanics tests on bare instruments (no policy)."""
    return _provider_write(conn, rows)


def _provider_write(conn, rows, *, start=None, end=None, provider=None,
                    complete=True, status="success_new"):
    """Governed provider write: running run, then attempt + NAV in ONE txn.

    The successful attempt and the revisions it attributes share the commit
    xid (checked by the deferred DB trigger); the run is completed afterwards
    unless ``complete=False``. Returns the run id.
    """
    run_id = uuid.uuid4()
    days = sorted({row["nav_date"] for row in rows})
    provider = provider or rows[0]["source"]
    conn.execute(
        "INSERT INTO nav_ingestion_runs (run_id,requested_end,status) "
        "VALUES (%s,%s,'running')",
        (run_id, end or days[-1]),
    )
    conn.commit()
    for iid in sorted({row["instrument_id"] for row in rows}, key=str):
        conn.execute(
            "INSERT INTO nav_ingestion_attempts (run_id,instrument_id,ticker,provider,"
            "requested_start,requested_end,attempted_at,finished_at,status,"
            "newest_observed_date,row_count) VALUES (%s,%s,'SYN',%s,%s,%s,"
            "clock_timestamp()-interval '1 minute',clock_timestamp(),%s,%s,%s)",
            (run_id, iid, provider, start or days[0], end or days[-1], status,
             days[-1], len(days)),
        )
    ingest.upsert_nav_timeseries(conn, rows, run_id=run_id, provider=provider)
    if complete:
        conn.execute(
            "UPDATE nav_ingestion_runs SET status='completed' WHERE run_id=%s", (run_id,)
        )
        conn.commit()
    return run_id


def _seed(
    dsn,
    schema,
    *,
    with_policy=True,
    extra_closed=False,
    stamp=True,
    valid_for=dt.timedelta(days=1),
):
    _bootstrap(dsn, schema)
    iid = uuid.uuid4()
    grid = _grid()
    if extra_closed:
        local_today = (
            dt.datetime.now(dt.timezone.utc)
            .astimezone(ZoneInfo("America/New_York"))
            .date()
        )
        if local_today <= grid[-1]:
            grid = _grid(local_today - dt.timedelta(days=1))
    policy_hash = "a" * 64
    source = "fixture:NYSE-valuation-due"
    sessions = [
        (
            date,
            dt.datetime.combine(date, dt.time(20), dt.timezone.utc),
            dt.datetime.combine(date, dt.time(23), dt.timezone.utc),
            source,
        )
        for date in grid
    ]
    if extra_closed:
        now = dt.datetime.now(dt.timezone.utc)
        closed_date = now.astimezone(ZoneInfo("America/New_York")).date()
        assert closed_date > grid[-1], (
            "synthetic extra closed session needs a later NY day"
        )
        sessions.append((closed_date, now, now + dt.timedelta(hours=1), source))
    digest = calendar_digest(sessions)
    valid_through = dt.datetime.now(dt.timezone.utc) + valid_for
    with _connect(dsn, schema) as conn:
        conn.execute("INSERT INTO funds_profile_mv VALUES (%s)", (iid,))
        if with_policy:
            conn.execute(
                """INSERT INTO nav_policy_versions
                   (policy_id,policy_version,policy_hash,readiness_profile,
                    valuation_frequency,calendar_id,calendar_version,calendar_source,
                      timezone,coverage_start,coverage_end,valid_through,
                      calendar_session_count,calendar_digest,
                      sample_intervals,annualization_sessions,
                     required_nav_kind,required_return_semantics,modeling_currency,
                     currency_treatment,source_reference,published_at)
                    VALUES ('synthetic','v1',%s,'current_daily_nav_v1','daily',
                             'NYSE-TEST','v1',%s,'America/New_York',%s,%s,%s,%s,%s,400,252,
                            'adjusted','observed_interval_log_ratio','USD','native_only',%s,
                            NULL)""",
                (
                    policy_hash,
                    source,
                    grid[0],
                    sessions[-1][0],
                    valid_through,
                    len(sessions),
                    digest,
                    source,
                ),
            )
            for date, close_at, due_at, _ in sessions:
                conn.execute(
                    """INSERT INTO nav_valuation_schedules
                       VALUES ('NYSE-TEST','v1',%s,%s,%s,%s,%s)""",
                    (
                        date,
                        close_at,
                        due_at,
                        source,
                        source,
                    ),
                )
            conn.execute(
                "UPDATE nav_policy_versions SET published_at=clock_timestamp() "
                "WHERE policy_id='synthetic' AND policy_version='v1'"
            )
            conn.execute(
                "INSERT INTO nav_policy_current (readiness_profile,policy_id,policy_version) "
                "VALUES ('current_daily_nav_v1','synthetic','v1')"
            )
            conn.execute(
                """INSERT INTO nav_instrument_policy_evidence
                   (instrument_id,policy_id,policy_version,known_at,effective_at,
                    fund_status,valuation_frequency,identity_verified,
                    return_basis_verified,currency_verified,evidence_reference)
                   VALUES (%s,'synthetic','v1',clock_timestamp()-interval '1 day',
                           clock_timestamp()-interval '1 day','ACTIVE','daily',
                           true,true,true,'synthetic_verified_basis')""",
                (iid,),
            )
        conn.commit()
        observations = tuple(
            NavObservation(d, round(100.0 + i * 0.01, 6), "adjusted")
            for i, d in enumerate(grid)
        )
        calendar = (
            {d: ("NYSE-TEST", "v1", source) for d in grid}
            if with_policy and stamp
            else None
        )
        rows = ingest.build_rows(observations, [(iid, "USD")], calendar=calendar)
        ing_run = _provider_write(conn, rows)
        nav_rows = conn.execute(
            "SELECT nav_date, nav FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date",
            (iid,),
        ).fetchall()
        _publish_risk_run(conn, grid[-1], {iid: nav_rows})
    return iid, grid, ing_run


def _register_risk_run(conn, run_id, calc_date, members, *, scope, reason=None):
    """Explicit P1 fixture: parent + frozen members, no W3 worker classification.

    ``scope`` and ``reason`` are asserted by the fixture itself (diagnostic by
    default in P1 persistence tests); ``current_full`` pins the current policy
    and requires ``calc_date`` to be its due session, as the DB CHECK demands.
    """
    policy = conn.execute(
        "SELECT p.policy_id,p.policy_version,p.policy_hash FROM nav_policy_current c "
        "JOIN nav_policy_versions p USING (policy_id,policy_version)"
    ).fetchone()
    pins = policy if policy is not None else (None, None, None)
    conn.execute(
        """INSERT INTO fund_nav_risk_runs
           (risk_run_id,calc_date,run_scope,nonpublishing_reason,policy_id,
            policy_version,policy_hash,due_session,requested_calc_date,
            requested_limit,universe_digest,feature_definition_version,status,
            expected_rows)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,%s,%s,'running',%s)""",
        (run_id, calc_date, scope, reason, *pins,
         calc_date if scope == "current_full" else None,
         risk_universe_digest(members), FEATURE_DEFINITION_VERSION, len(members)),
    )
    if members:
        conn.execute(
            "INSERT INTO fund_nav_risk_run_members "
            "SELECT %s, m FROM unnest(%s::uuid[]) AS m",
            (run_id, list(members)),
        )


def _publish_risk_run(conn, calc_date, features, exclusions=()):
    """Fixture DB state for a published run (not the W3 worker).

    With a current policy the run is ``current_full``; without one the schema
    only admits ``diagnostic``/POLICY_UNAVAILABLE, which the snapshot never uses.
    """
    run_id = uuid.uuid4()
    has_policy = conn.execute("SELECT count(*) FROM nav_policy_current").fetchone()[0]
    _register_risk_run(
        conn,
        run_id,
        calc_date,
        [*features, *exclusions],
        scope="current_full" if has_policy else "diagnostic",
        reason=None if has_policy else "POLICY_UNAVAILABLE",
    )
    for iid, nav_rows in features.items():
        risk._persist_feature_evidence(
            conn, iid, calc_date, nav_rows, 0.04, run_id, None, {}, {}, []
        )
    for iid in exclusions:
        risk._record_risk_exclusion(conn, run_id, iid, calc_date, "METRICS_UNAVAILABLE", [])
    conn.commit()
    risk._finish_risk_run(conn, run_id, len(features) + len(exclusions), len(features))
    conn.execute(
        """INSERT INTO fund_nav_risk_publication
           (readiness_profile,revision_id,state,published_risk_run_id)
           VALUES ('current_daily_nav_v1',1,'idle',%s)
           ON CONFLICT (readiness_profile) DO UPDATE SET
             revision_id=fund_nav_risk_publication.revision_id+1, state='idle',
             published_risk_run_id=EXCLUDED.published_risk_run_id,
             active_risk_run_id=NULL""",
        (run_id,),
    )
    conn.execute(
        "UPDATE fund_nav_risk_runs SET status='complete',completed_at=clock_timestamp() "
        "WHERE risk_run_id=%s",
        (run_id,),
    )
    conn.commit()
    return run_id


def test_real_db_run_replay_and_dynamic_due(test_dsn, schema, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    one = readiness.run(test_dsn)
    assert one["state"] == "complete" and one["ready_count"] == 1
    two = readiness.run(test_dsn)
    assert two["sample_id"] == one["sample_id"]
    with _connect(test_dsn, schema) as conn:
        row = conn.execute(
            """SELECT instrument_id,policy_version,window_end,observed_endpoint_count,
                      observed_return_count,ready,reason_code,snapshot_current,
                      input_fingerprint,run_fingerprint
               FROM fund_nav_readiness_current_v1 WHERE instrument_id=%s""",
            (iid,),
        ).fetchone()
        assert row[:8] == (iid, "v1", grid[-1], 401, 400, True, None, True)
        assert (
            conn.execute(
                "SELECT latest_closed_session FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            == grid[-1]
        )
        assert len(row[8]) == len(row[9]) == 64
        runs = conn.execute(
            "SELECT count(*) FROM fund_nav_readiness_runs WHERE state='complete'"
        ).fetchone()[0]
        assert runs == 2


@pytest.mark.parametrize(
    ("column", "value", "reason"),
    [
        ("source_nav_kind", "raw", "NAV_RETURN_SEMANTICS_UNSUPPORTED"),
        ("currency", "EUR", "NAV_DATA_UNAVAILABLE"),
        ("return_type", "arithmetic", "NAV_RETURN_SEMANTICS_UNSUPPORTED"),
    ],
)
def test_real_db_policy_never_admits_uncomparable_levels(
    test_dsn, schema, monkeypatch, column, value, reason
):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            sql.SQL(
                "UPDATE nav_timeseries SET {}=%s WHERE instrument_id=%s AND nav_date=%s"
            ).format(sql.Identifier(column)),
            (value, iid, grid[-1]),
        )
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT reason_code FROM fund_nav_readiness_current_v1 WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            == reason
        )


def test_ingestion_calendar_tags_only_governed_sessions(test_dsn, schema):
    _iid, grid, _run = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        assigned = ingest._published_calendar(conn, [grid[-1], dt.date(2026, 9, 7)])
        assert assigned[grid[-1]] == ("NYSE-TEST", "v1", "fixture:NYSE-valuation-due")
        assert dt.date(2026, 9, 7) not in assigned


def test_real_db_partial_failure_does_not_flip_pointer(test_dsn, schema, monkeypatch):
    _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    first = readiness.run(test_dsn)
    monkeypatch.setattr(
        readiness,
        "_input_evidence",
        lambda *_: (_ for _ in ()).throw(RuntimeError("injected partial build")),
    )
    with pytest.raises(RuntimeError, match="injected partial build"):
        readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        assert (
            str(
                conn.execute(
                    "SELECT run_id FROM fund_nav_readiness_current"
                ).fetchone()[0]
            )
            == first["run_id"]
        )
        assert (
            conn.execute("SELECT count(*) FROM fund_nav_readiness_runs").fetchone()[0]
            == 1
        )


def test_completed_snapshot_rows_and_run_are_immutable(test_dsn, schema, monkeypatch):
    _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    report = readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute(
                "UPDATE fund_nav_readiness_v1 SET fund_status='UNKNOWN' WHERE run_id=%s",
                (report["run_id"],),
            )
        conn.rollback()
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute(
                "UPDATE fund_nav_readiness_runs SET policy_hash=%s WHERE run_id=%s",
                ("0" * 64, report["run_id"]),
            )
        conn.rollback()
        assert (
            conn.execute("SELECT ready FROM fund_nav_readiness_current_v1").fetchone()[
                0
            ]
            is True
        )


def test_real_db_absent_policy_and_lock_busy_never_publish(
    test_dsn, schema, monkeypatch
):
    _seed(test_dsn, schema, with_policy=False)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with pytest.raises(RuntimeError, match="NAV_POLICY_UNAVAILABLE"):
        readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute("SELECT count(*) FROM fund_nav_readiness_current").fetchone()[
                0
            ]
            == 0
        )
    with _connect(test_dsn, schema) as first:
        with advisory_lock(first, LOCK_FUND_NAV_READINESS) as locked:
            assert locked
            assert readiness.run(test_dsn) == {
                "status": "lock_busy",
                "state": "lock_busy",
                "published": False,
                "retryable": True,
            }


def test_chain_preflight_blocks_provider_without_policy(test_dsn, schema, monkeypatch):
    _seed(test_dsn, schema, with_policy=False)
    monkeypatch.setattr(chain, "connect", lambda dsn: _connect(dsn, schema))
    monkeypatch.setattr(
        chain.fund_nav_readiness, "_policy_and_grid", readiness._policy_and_grid
    )
    called = []
    with pytest.raises(RuntimeError, match="NAV_POLICY_UNAVAILABLE"):
        chain.run(
            test_dsn,
            ingestion_runner=lambda *_a, **_kw: called.append("provider"),
            risk_runner=lambda *_a, **_kw: called.append("risk"),
        )
    assert called == []


def test_ingestion_refuses_to_write_while_readiness_has_snapshot_lock(
    test_dsn,
    schema,
    monkeypatch,
):
    _bootstrap(test_dsn, schema)
    monkeypatch.setattr(ingest, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as first:
        with advisory_lock(first, LOCK_FUND_NAV_READINESS) as locked:
            assert locked
            stats = ingest.run(test_dsn, calc_date=dt.date.today().isoformat(), limit=0)
            assert stats["skipped"] == "lock_busy"
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute("SELECT count(*) FROM nav_ingestion_runs").fetchone()[0] == 0
        )


def test_real_db_stale_feature_and_legacy_unknown(test_dsn, schema, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as conn:
        with pytest.raises(psycopg.Error, match="append-only"):
            conn.execute(
                "UPDATE fund_nav_feature_evidence SET feature_as_of=%s "
                "WHERE instrument_id=%s",
                (grid[-2], iid),
            )
        conn.rollback()
        stale_rows = conn.execute(
            "SELECT nav_date, nav FROM nav_timeseries WHERE instrument_id=%s "
            "AND nav_date < %s ORDER BY nav_date",
            (iid, grid[-1]),
        ).fetchall()
        _publish_risk_run(conn, grid[-1], {iid: stale_rows})  # feature_as_of = grid[-2]
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT reason_code FROM fund_nav_readiness_current_v1"
        ).fetchone()[0] == ("RETURN_SAMPLE_NOT_CURRENT")
        unknown_id = uuid.uuid4()
        conn.execute("INSERT INTO funds_profile_mv VALUES (%s)", (unknown_id,))
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT reason_code FROM fund_nav_readiness_current_v1 WHERE instrument_id=%s",
                (unknown_id,),
            ).fetchone()[0]
            == "UNKNOWN_FUND_STATUS"
        )


def test_operator_check_only_does_not_change_catalog(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        before = conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone()[0]
        report = operator._check(conn, schema)
        assert report["status"] == "ready" and report["tables_present"] == len(
            operator.TABLES
        )
        assert (
            conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone()[0]
            == before
        )


def test_operator_cli_dry_run_then_allowlisted_apply_on_disposable_db(
    test_dsn,
    schema,
    monkeypatch,
    capsys,
):
    _bootstrap(test_dsn, schema)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    sha = hashlib.sha256(ddl).hexdigest()
    base = ["--schema", schema, "--expected-sql-sha256", sha]
    fingerprint = _plan(base, capsys)
    assert operator.main([*base, "--mode", "apply"]) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "plan_hash_required"
    legacy = operator.plan_hash(ddl, schema, b"", [], None, None)
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", legacy]) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "plan_version_mismatch"
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", fingerprint]) == 0
    result = json.loads(capsys.readouterr().out)
    assert (result["status"], result["ddl"], result["dml_committed"]) == (
        "unchanged", "unchanged", False,
    )
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone()[0] == 0
        )


def test_operator_rejects_wrong_existing_schema_without_mutation(test_dsn, schema):
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        conn.execute("CREATE TABLE nav_policy_versions (policy_id integer)")
        report = operator._check(conn, schema)
        assert report["status"] == "upgrade_required"
        assert report["compatibility"] == "incompatible"
        assert (
            conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone()[0] == 0
        )


def test_operator_lock_timeout_rolls_back_then_reruns(
    test_dsn, schema, monkeypatch, capsys
):
    _bootstrap(test_dsn, schema)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    check = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute("DROP TRIGGER fund_nav_stamp_revision ON nav_timeseries")  # repairable
    base = [*check, "--mode", "apply", "--plan-sha256", _plan(check, capsys)]
    with _connect(test_dsn, schema) as blocker:
        blocker.execute("LOCK TABLE nav_policy_versions IN ACCESS EXCLUSIVE MODE")
        assert operator.main(base) == 2
        out = json.loads(capsys.readouterr().out)
        assert (out["status"], out["code"], out["sqlstate"]) == (
            "blocked", "database_error", "55P03",
        )
        blocker.rollback()
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone()[0] == 0
        )
    assert operator.main(base) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["status"], out["ddl"]) == ("applied", "applied")


def test_operator_rejects_precoverage_w1_shape_without_altering_policy(
    test_dsn, schema
):
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        conn.execute(
            "CREATE TABLE nav_policy_versions "
            "(policy_id text,policy_version text,policy_hash char(64),"
            "published_at timestamptz)"
        )
        conn.execute(
            "INSERT INTO nav_policy_versions VALUES "
            "('synthetic','old',%s,clock_timestamp())",
            ("a" * 64,),
        )
        assert operator._check(conn, schema)["compatibility"] == "incompatible"
        assert (
            conn.execute("SELECT policy_hash FROM nav_policy_versions").fetchone()[0]
            == "a" * 64
        )


def test_operator_detects_missing_nav_revision_trigger_and_repairs_idempotently(
    test_dsn,
    schema,
):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute("DROP TRIGGER fund_nav_stamp_revision ON nav_timeseries")
        report = operator._check(conn, schema)
        assert (report["status"], report["compatibility"]) == (
            "upgrade_required",
            "repairable",
        )
        _install(conn, schema)
        assert operator._check(conn, schema)["status"] == "ready"


# Round3 F4: the plan-v4 publication receipt ledger is additive. A W1 schema
# of the previous release (every receipt object absent) is repairable by the
# idempotent DDL with its data intact; partial or reshaped receipt objects
# stay incompatible and receive no DDL.
_RECEIPT_STATEMENTS = {
    "previous_release_without_ledger": [
        "DROP TABLE nav_policy_publication_receipts",
        "DROP FUNCTION nav_policy_publication_receipt_guard_v1()",
    ],
    "ledger_trigger_missing": [
        "DROP TRIGGER nav_policy_publication_receipt_guard ON nav_policy_publication_receipts",
    ],
    "ledger_without_guard_function": [
        "DROP FUNCTION nav_policy_publication_receipt_guard_v1() CASCADE",
    ],
    "ledger_reshaped": [
        "ALTER TABLE nav_policy_publication_receipts DROP COLUMN captured_at CASCADE",
    ],
    "ledger_function_without_table": [
        "DROP TABLE nav_policy_publication_receipts",
    ],
}


@pytest.mark.parametrize(
    "case,compatibility",
    [
        ("previous_release_without_ledger", "repairable"),
        ("ledger_trigger_missing", "repairable"),
        ("ledger_without_guard_function", "incompatible"),
        ("ledger_reshaped", "incompatible"),
        ("ledger_function_without_table", "incompatible"),
    ],
)
def test_publication_receipt_ledger_is_an_additive_upgrade(
    test_dsn, schema, monkeypatch, capsys, case, compatibility
):
    _seed(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        for statement in _RECEIPT_STATEMENTS[case]:
            conn.execute(statement)
        report = operator._check(conn, schema)
    assert (report["compatibility"], report["ready"]) == (compatibility, False)
    before = _w1_state(test_dsn, schema)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    base = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    if compatibility == "incompatible":
        assert operator.main([*base, "--mode", "apply", "--plan-sha256", "0" * 64]) == 3
        out = json.loads(capsys.readouterr().out)
        assert (out["code"], out["ddl"]) == ("incompatible_schema", "not_attempted")
        with _connect(test_dsn, schema, autocommit=True) as conn:
            assert operator._check(conn, schema)["catalog_sha256"] == report["catalog_sha256"]
        return
    assert report["code"] is None and report["access"] in ("exact", "repairable")
    plan = _plan_planned(base, capsys)
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["status"], out["ddl"], out["dml_committed"]) == ("applied", "applied", False)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        after = operator._check(conn, schema)
        assert (after["compatibility"], after["access"], after["ready"]) == (
            "exact", "exact", True)
        assert conn.execute(
            "SELECT count(*) FROM nav_policy_publication_receipts").fetchone()[0] == 0
        assert conn.execute(
            "SELECT has_table_privilege('app_runtime', 'nav_policy_publication_receipts', "
            "'SELECT')").fetchone()[0] is False
    assert _w1_state(test_dsn, schema) == before  # existing W1 data untouched


def _plan_planned(check_args, capsys):
    """Operator check on a repairable schema: exit 0, status ``planned``."""
    assert operator.main(check_args) == 0, capsys.readouterr().out
    out = json.loads(capsys.readouterr().out)
    assert (out["status"], out["code"]) == ("planned", None)
    return out["plan_sha256"]


def test_schema_apply_lock_timeout_rolls_back_all_new_relations(test_dsn, schema):
    with _connect(test_dsn, schema, autocommit=True) as setup:
        setup.execute(NAV_SQL)
    with (
        _connect(test_dsn, schema) as held,
        _connect(test_dsn, schema, autocommit=True) as installer,
    ):
        held.execute("LOCK TABLE nav_timeseries IN ACCESS EXCLUSIVE MODE")
        with pytest.raises(psycopg.Error) as exc:
            installer.execute(SCHEMA_SQL)
        assert exc.value.sqlstate == "55P03"
        installer.execute("ROLLBACK")
        assert (
            installer.execute("SELECT to_regclass('fund_nav_readiness_v1')").fetchone()[
                0
            ]
            is None
        )
        held.rollback()
        assert (
            installer.execute("SELECT to_regclass('nav_timeseries')").fetchone()[0]
            is not None
        )


def test_governed_policy_publication_idempotent_and_version_frozen(
    test_dsn, schema, tmp_path
):
    _bootstrap(test_dsn, schema)
    grid = _grid()
    source = "synthetic-calendar-fixture"
    iid = uuid.uuid4()
    policy = {
        "policy_id": "synthetic",
        "policy_version": "v1",
        "readiness_profile": "current_daily_nav_v1",
        "valuation_frequency": "daily",
        "timezone": "America/New_York",
        "calendar_id": "NYSE-FIXTURE",
        "calendar_version": "v1",
        "calendar_source": source,
        "source_reference": "fixture-only-not-operational",
        "sample_intervals": 400,
        "annualization_sessions": 252,
        "required_nav_kind": "adjusted",
        "required_return_semantics": "observed_interval_log_ratio",
        "modeling_currency": "USD",
        "currency_treatment": "native_only",
        "publication_state": "approved",
        "repaired_nav_kinds": sorted(REPAIRED_NAV_KINDS),
        "adjusted_overlap_absolute_tolerance": 0.0000005,
        "adjusted_overlap_relative_tolerance": 0.00000001,
        "coverage_start": grid[0].isoformat(),
        "coverage_end": grid[-1].isoformat(),
        "valid_through": (
            dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
        ).isoformat(),
        "calendar_session_count": len(grid),
        "calendar_digest": calendar_digest(
            [
                (
                    d,
                    dt.datetime.combine(d, dt.time(20), dt.timezone.utc),
                    dt.datetime.combine(d, dt.time(23), dt.timezone.utc),
                    "fixture-only-not-operational",
                )
                for d in grid
            ]
        ),
        "sessions": [
            {
                "session_date": d.isoformat(),
                "valuation_close_at": dt.datetime.combine(
                    d, dt.time(20), dt.timezone.utc
                ).isoformat(),
                "nav_due_at": dt.datetime.combine(
                    d, dt.time(23), dt.timezone.utc
                ).isoformat(),
            }
            for d in grid
        ],
        "instrument_evidence": [
            {
                "instrument_id": str(iid),
                "fund_status": "ACTIVE",
                "valuation_frequency": "daily",
                "identity_verified": True,
                "return_basis_verified": True,
                "currency_verified": True,
                "known_at": dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).isoformat(),
                "effective_at": dt.datetime(
                    2025, 1, 1, tzinfo=dt.timezone.utc
                ).isoformat(),
                "evidence_reference": "fixture-identity-verified",
            }
        ],
    }
    path = tmp_path / "synthetic-policy.json"
    path.write_text(json.dumps(policy), encoding="utf-8")
    parsed, raw = operator._policy(str(path))
    assert parsed == policy and raw
    with _connect(test_dsn, schema) as conn:
        fingerprint, changed = operator._publish_policy(conn, parsed)
        assert changed is True
        # Identical republish: same hash, nothing changed, no pointer re-stamp.
        stamp = conn.execute("SELECT published_at FROM nav_policy_current").fetchone()[0]
        assert operator._publish_policy(conn, parsed) == (fingerprint, False)
        assert conn.execute(
            "SELECT published_at FROM nav_policy_current"
        ).fetchone()[0] == stamp
        assert (
            conn.execute("SELECT count(*) FROM nav_valuation_schedules").fetchone()[0]
            == 401
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_instrument_policy_evidence"
            ).fetchone()[0]
            == 1
        )
        conn.commit()
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute("UPDATE nav_policy_versions SET policy_hash=%s", ("b" * 64,))
        conn.rollback()
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute(
                "UPDATE nav_valuation_schedules SET source_reference='changed'"
            )
        conn.rollback()
        with pytest.raises(psycopg.Error, match="append-only"):
            conn.execute("DELETE FROM nav_instrument_policy_evidence")
        conn.rollback()
    template = ROOT / "configs" / "fund_nav_policy_v1.json"
    with pytest.raises(ValueError, match="policy_evidence_incomplete"):
        operator._policy(str(template))
    policy["sessions"].append(policy["sessions"][-1])
    path.write_text(json.dumps(policy), encoding="utf-8")
    with pytest.raises(ValueError, match="calendar_not_ordered_or_too_short"):
        operator._policy(str(path))
    policy["sessions"].pop()
    policy["calendar_digest"] = "0" * 64
    path.write_text(json.dumps(policy), encoding="utf-8")
    with pytest.raises(ValueError, match="calendar_not_ordered_or_too_short"):
        operator._policy(str(path))
    policy["instrument_evidence"][0]["return_basis_verified"] = "false"
    policy["calendar_digest"] = calendar_digest(
        [
            (
                d,
                dt.datetime.combine(d, dt.time(20), dt.timezone.utc),
                dt.datetime.combine(d, dt.time(23), dt.timezone.utc),
                "fixture-only-not-operational",
            )
            for d in grid
        ]
    )
    path.write_text(json.dumps(policy), encoding="utf-8")
    with pytest.raises(ValueError, match="evidence_boolean_unverified"):
        operator._policy(str(path))


def test_new_lifecycle_evidence_invalidates_old_pointer(test_dsn, schema, monkeypatch):
    iid, _grid_dates, _run = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            """INSERT INTO nav_instrument_policy_evidence
               (instrument_id,policy_id,policy_version,known_at,effective_at,
                fund_status,valuation_frequency,identity_verified,
                return_basis_verified,currency_verified,evidence_reference)
               VALUES (%s,'synthetic','v1',clock_timestamp(),clock_timestamp(),
                       'INACTIVE','daily',true,true,true,'new-status-fact')""",
            (iid,),
        )
        assert (
            conn.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            is False
        )
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT reason_code FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            == "INACTIVE_FUND"
        )


def test_preknown_future_inactive_invalidates_at_effective_instant(
    test_dsn,
    schema,
    monkeypatch,
):
    iid, grid, _run = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    effective = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            """INSERT INTO nav_instrument_policy_evidence
               (instrument_id,policy_id,policy_version,known_at,effective_at,
                fund_status,valuation_frequency,identity_verified,
                return_basis_verified,currency_verified,evidence_reference)
               VALUES (%s,'synthetic','v1',clock_timestamp()-interval '1 minute',%s,
                       'INACTIVE','daily',true,true,true,'preknown-future-inactive')""",
            (iid, effective),
        )
    run = readiness.run(test_dsn)
    assert run["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        before = conn.execute(
            "SELECT fund_nav_snapshot_current_at_v1(%s,%s,%s)",
            (iid, run["run_id"], effective - dt.timedelta(microseconds=1)),
        ).fetchone()[0]
        after = conn.execute(
            "SELECT fund_nav_snapshot_current_at_v1(%s,%s,%s)",
            (iid, run["run_id"], effective),
        ).fetchone()[0]
        assert before is True and after is False
        assert (
            conn.execute(
                "SELECT latest_closed_session FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            == grid[-1]
        )


def test_calendar_expiry_fails_policy_and_invalidates_snapshot(
    test_dsn, schema, monkeypatch
):
    iid, _grid_dates, _run = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    run = readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        expires = conn.execute(
            "SELECT valid_through FROM nav_policy_versions"
        ).fetchone()[0]
        assert (
            conn.execute(
                "SELECT fund_nav_snapshot_current_at_v1(%s,%s,%s)",
                (iid, run["run_id"], expires),
            ).fetchone()[0]
            is True
        )
        after = expires + dt.timedelta(microseconds=1)
        assert (
            conn.execute(
                "SELECT fund_nav_snapshot_current_at_v1(%s,%s,%s)",
                (iid, run["run_id"], after),
            ).fetchone()[0]
            is False
        )
        with pytest.raises(RuntimeError, match="calendar coverage expired"):
            readiness._policy_and_grid(conn, after)


def test_backfill_does_not_promote_legacy_nulls(test_dsn, schema):
    iid, grid, _ = _seed(test_dsn, schema)
    old_id = uuid.uuid4()
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            "INSERT INTO nav_timeseries (instrument_id,nav_date,nav,source) "
            "VALUES (%s,%s,100,'tiingo')",
            (old_id, grid[-1]),
        )
        conn.execute(
            "INSERT INTO nav_instrument_policy_evidence "
            "(instrument_id,policy_id,policy_version,known_at,effective_at,"
            "fund_status,valuation_frequency,identity_verified,"
            "return_basis_verified,currency_verified,evidence_reference) "
            "VALUES (%s,'synthetic','v1',clock_timestamp()-interval '1 day',"
            "clock_timestamp()-interval '1 day','ACTIVE','daily',true,true,true,'synthetic')",
            (old_id,),
        )
        conn.commit()
    with _connect(test_dsn, schema, autocommit=True) as conn:
        plan = operator._maintenance_plan(conn, [str(old_id)], grid[-1], grid[-1])
        assert plan["eligible_rows"] == 0 and plan["scope_rows"] == 1
        applied = operator._apply_calendar_maintenance(
            conn, [str(old_id)], grid[-1], grid[-1], "c" * 64
        )
        assert applied["status"] == "no_op"
        assert (
            conn.execute("SELECT count(*) FROM nav_calendar_maintenance_runs").fetchone()[0]
            == 0
        )
        assert conn.execute(
            "SELECT source_nav,source_nav_kind,calendar_id FROM nav_timeseries "
            "WHERE instrument_id=%s",
            (old_id,),
        ).fetchone() == (None, None, None)


def test_db_versions_and_compression_capability(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        version = conn.execute(
            "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
        ).fetchone()[0]
        table = conn.execute(
            """SELECT 1 FROM timescaledb_information.hypertables
               WHERE hypertable_schema=%s AND hypertable_name='nav_timeseries'""",
            (schema,),
        ).fetchone()
        assert version.startswith("2.27") and table == (1,)


def test_revision_trigger_applies_to_precompressed_hypertable_without_rewriting_history(
    test_dsn,
    schema,
):
    iid = uuid.uuid4()
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        conn.execute(
            "ALTER TABLE nav_timeseries SET "
            "(timescaledb.compress,timescaledb.compress_segmentby='instrument_id')"
        )
        conn.execute(
            "INSERT INTO nav_timeseries (instrument_id,nav_date,nav,source) "
            "VALUES (%s,DATE '2025-01-02',100,'legacy')",
            (iid,),
        )
        chunk = conn.execute(
            "SELECT show_chunks('nav_timeseries'::regclass) LIMIT 1"
        ).fetchone()[0]
        conn.execute(
            "SELECT compress_chunk(%s::regclass,if_not_compressed=>true)", (chunk,)
        )
        _install(conn, schema)
        _install(conn, schema)
        assert (
            conn.execute(
                "SELECT count(*) FROM timescaledb_information.chunks "
                "WHERE hypertable_schema=%s AND is_compressed",
                (schema,),
            ).fetchone()[0]
            == 1
        )
        assert conn.execute(
            "SELECT nav,source_nav,return_1d FROM nav_timeseries "
            "WHERE instrument_id=%s",
            (iid,),
        ).fetchone() == (100, None, None)
        conn.execute(
            "UPDATE nav_timeseries SET nav=101 WHERE instrument_id=%s "
            "AND nav_date=DATE '2025-01-02'",
            (iid,),
        )
        assert (
            conn.execute(
                "SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            > 0
        )


def test_risk_metrics_and_feature_evidence_roll_back_together(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    date = dt.date.today() - dt.timedelta(days=1)
    with _connect(test_dsn, schema) as conn:
        # The bootstrap already installs the real risk read model (N6 dependency).
        risk._upsert(conn, iid, date, {"volatility_1y": 0.2})
        rows = [(date - dt.timedelta(days=n), 100 + n) for n in range(22, 0, -1)]
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            risk._persist_feature_evidence(
                conn, iid, date, rows, 0.04, uuid.uuid4(), None, {}, {}, []
            )
        conn.rollback()
        assert conn.execute("SELECT count(*) FROM fund_risk_metrics").fetchone()[0] == 0
        assert (
            conn.execute("SELECT count(*) FROM fund_nav_feature_evidence").fetchone()[0]
            == 0
        )


def test_excluded_risk_fund_is_accounted_but_never_uses_old_feature(
    test_dsn,
    schema,
    monkeypatch,
):
    iid, grid, _run = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    assert readiness.run(test_dsn)["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        plan, token = risk._begin_risk_generation(conn, grid[-1].isoformat(), None)
        assert plan.eligible and plan.members == (iid,)
        new_run = plan.risk_run_id
        risk._record_risk_exclusion(
            conn, new_run, iid, grid[-1], "METRICS_UNAVAILABLE", []
        )
        conn.commit()
        risk._finish_risk_run(conn, new_run, 1, 0)
        assert conn.execute(
            "SELECT status,persisted_rows FROM fund_nav_risk_runs WHERE risk_run_id=%s",
            (new_run,),
        ).fetchone() == ("metrics_complete", 1)
        risk._complete_risk_run(conn, new_run)
        assert risk._mark_risk_published(conn, plan, token)["published"] is True
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            is False
        )
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT reason_code FROM fund_nav_readiness_current_v1"
        ).fetchone()[0] == ("RETURN_SAMPLE_NOT_CURRENT")


def test_two_connections_risk_recalc_blocks_pointer_until_mv_verified_revision(
    test_dsn,
    schema,
    monkeypatch,
):
    iid, grid, _run = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    first = readiness.run(test_dsn)
    with _connect(test_dsn, schema) as publisher, _connect(test_dsn, schema) as reader:
        assert reader.execute(
            "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
        ).fetchone()[0]
        plan, token = risk._begin_risk_generation(publisher, grid[-1].isoformat(), None)
        replacement = plan.risk_run_id
        assert (
            reader.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            is False
        )
        with pytest.raises(RuntimeError, match="risk run not published"):
            readiness.run(test_dsn)
        nav = publisher.execute(
            "SELECT nav_date,nav FROM nav_timeseries WHERE instrument_id=%s "
            "ORDER BY nav_date",
            (iid,),
        ).fetchall()
        risk._upsert(
            publisher,
            iid,
            grid[-1],
            risk.compute_metrics(np.array([float(v) for _d, v in nav]), 0.04),
        )
        risk._persist_feature_evidence(
            publisher, iid, grid[-1], nav, 0.04, replacement, None, {}, {}, []
        )
        publisher.commit()
        risk._finish_risk_run(publisher, replacement, 1, 1)
        risk._refresh_fund_risk_latest_mv(_schema_dsn(test_dsn, schema))
        risk._complete_risk_run(publisher, replacement)
        assert (
            reader.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            is False
        )
        with _connect(test_dsn, schema) as blocker:
            with advisory_lock(blocker, LOCK_FUND_NAV_READINESS) as acquired:
                assert acquired
                busy = risk._mark_risk_published(publisher, plan, token)
                assert (busy["published"], busy["reason"], busy["retryable"]) == (
                    False,
                    "LOCK_BUSY",
                    True,
                )
        assert reader.execute(
            "SELECT state, active_risk_run_id FROM fund_nav_risk_publication"
        ).fetchone() == ("running", replacement)
        assert risk._mark_risk_published(publisher, plan, token)["published"] is True
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            is False
        )
        # fixture publication 1 -> invalidation 2 -> CAS publication 3
        assert (
            conn.execute(
                "SELECT revision_id FROM fund_nav_risk_publication"
            ).fetchone()[0]
            == 3
        )
    second = readiness.run(test_dsn)
    assert second["ready_count"] == 1 and second["run_id"] != first["run_id"]
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            is True
        )


def test_incomplete_risk_run_cannot_be_marked_complete(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    run_id = uuid.uuid4()
    with _connect(test_dsn, schema) as conn:
        _register_risk_run(
            conn, run_id, dt.date.today(), [uuid.uuid4()], scope="diagnostic",
            reason="POLICY_UNAVAILABLE",
        )
        conn.commit()
        with pytest.raises(RuntimeError, match="risk evidence incomplete"):
            risk._finish_risk_run(conn, run_id, 1, 0)
        assert conn.execute(
            "SELECT status,persisted_rows FROM fund_nav_risk_runs WHERE risk_run_id=%s",
            (run_id,),
        ).fetchone() == ("running", None)


def test_overlap_revision_reconciles_successor_without_changing_source_level(
    test_dsn, schema
):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, 2), dt.date(2026, 1, 5), dt.date(2026, 1, 6)]
    with _connect(test_dsn, schema) as conn:
        first = ingest.build_rows(
            tuple(
                NavObservation(d, float(100 + i), "adjusted")
                for i, d in enumerate(days)
            ),
            [(iid, "USD")],
        )
        _write(conn, first)
        revised = ingest.build_rows(
            (NavObservation(days[1], 101.5, "adjusted"),), [(iid, "USD")]
        )
        _write(conn, revised)
        prior, current, following = conn.execute(
            """SELECT nav,source_nav,return_start_date,return_1d
               FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date""",
            (iid,),
        ).fetchall()
        assert float(prior[0]) == 100
        assert float(current[0]) == float(current[1]) == 101.5
        assert current[2] == days[0]
        assert following[2] == days[1] and float(following[1]) == 102
        assert float(following[3]) == pytest.approx(
            __import__("math").log(102 / 101.5), abs=1e-8
        )


def test_gap_payload_reconciles_persisted_b_and_true_successors(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, day) for day in (2, 5, 6, 7)]
    with _connect(test_dsn, schema) as conn:
        initial = ingest.build_rows(
            tuple(NavObservation(d, 100.0 + i, "adjusted") for i, d in enumerate(days)),
            [(iid, "USD")],
        )
        _write(conn, initial)
        changed = ingest.build_rows(
            (
                NavObservation(days[0], 100.5, "adjusted"),
                NavObservation(days[2], 102.5, "adjusted"),
            ),
            [(iid, "USD")],
        )
        _write(conn, changed)
        data = conn.execute(
            """SELECT nav_date,nav,return_start_date,return_1d,source_nav
               FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date""",
            (iid,),
        ).fetchall()
        assert len(data) == 4 and float(data[1][1]) == float(data[1][4]) == 101.0
        assert data[1][2] == days[0] and float(data[1][3]) == pytest.approx(
            __import__("math").log(101 / 100.5), abs=1e-8
        )
        assert data[2][2] == days[1] and float(data[2][3]) == pytest.approx(
            __import__("math").log(102.5 / 101), abs=1e-8
        )
        assert data[3][2] == days[2] and float(data[3][3]) == pytest.approx(
            __import__("math").log(103 / 102.5), abs=1e-8
        )
        hold = conn.execute(
            "SELECT first_changed_date,last_changed_date FROM "
            "fund_nav_reexpression_holds WHERE instrument_id=%s",
            (iid,),
        ).fetchone()
        assert hold == (days[0], days[2])


def test_sanitizer_real_rows_repaired_flags_and_contract_old_kinds(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    config = json.loads(
        (ROOT / "configs" / "fund_nav_policy_v1.json").read_text(encoding="utf-8")
    )
    assert set(config["repaired_nav_kinds"]) == REPAIRED_NAV_KINDS
    days = [dt.date(2026, 1, day) for day in (2, 5, 6)]
    iid = uuid.uuid4()
    with _connect(test_dsn, schema) as conn:
        produced = ingest.build_rows(
            tuple(
                NavObservation(day, value, "adjusted")
                for day, value in zip(days, (100.0, 0.02, 101.0))
            ),
            [(iid, "USD")],
        )
        _write(conn, produced)
        original = conn.execute(
            "SELECT source_nav,nav,nav_repair_kind,return_uses_repaired_nav "
            "FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date",
            (iid,),
        ).fetchall()
        assert float(original[1][0]) == 0.02 and float(original[1][1]) > 100
        assert original[1][2] == "log_linear_interpolation_v1"
        assert original[1][3] is True and original[2][3] is True
        for legacy in ("centered_interpolation_v1", "local_median_v1"):
            conn.execute(
                "UPDATE nav_timeseries SET nav_repair_kind=%s WHERE instrument_id=%s "
                "AND nav_date=%s",
                (legacy, iid, days[1]),
            )
            revised = ingest.build_rows(
                (NavObservation(days[2], 101.5, "adjusted"),), [(iid, "USD")]
            )
            _write(conn, revised)
            assert (
                conn.execute(
                    "SELECT return_uses_repaired_nav FROM nav_timeseries "
                    "WHERE instrument_id=%s AND nav_date=%s",
                    (iid, days[2]),
                ).fetchone()[0]
                is True
            )
        carry_id = uuid.uuid4()
        carry = ingest.build_rows(
            tuple(
                NavObservation(day, value, "adjusted")
                for day, value in zip(days, (0.02, 100.0, 101.0))
            ),
            [(carry_id, "USD")],
        )
        _write(conn, carry)
        assert conn.execute(
            "SELECT nav_repair_kind FROM nav_timeseries "
            "WHERE instrument_id=%s AND nav_date=%s",
            (carry_id, days[0]),
        ).fetchone()[0] == ("one_sided_carry_v1")
        assert (
            conn.execute(
                "SELECT return_uses_repaired_nav FROM nav_timeseries "
                "WHERE instrument_id=%s AND nav_date=%s",
                (carry_id, days[1]),
            ).fetchone()[0]
            is True
        )


def test_source_switch_updates_true_successor_boundaries(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, day) for day in (2, 5, 6, 7)]
    with _connect(test_dsn, schema) as conn:
        _write(conn,
            ingest.build_rows(
                tuple(
                    NavObservation(d, 100.0 + i, "adjusted") for i, d in enumerate(days)
                ),
                [(iid, "USD")],
            ),
        )
        yahoo = ingest.build_rows(
            (NavObservation(days[1], 101.0, "adjusted"),),
            [(iid, "USD")],
            source="yahoo",
        )
        _write(conn, yahoo)
        rows = conn.execute(
            "SELECT nav_date,source,return_1d,return_source_boundary "
            "FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date",
            (iid,),
        ).fetchall()
        assert rows[1][1] == "yahoo" and rows[1][2] is None and rows[1][3] is True
        assert rows[2][1] == "tiingo" and rows[2][2] is None and rows[2][3] is True
        assert rows[3][2] is not None and rows[3][3] is False


@pytest.mark.parametrize("position", [0, 1, 3])
def test_first_middle_and_last_endpoint_revise_real_successor(
    test_dsn, schema, position
):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, day) for day in (2, 5, 6, 7)]
    with _connect(test_dsn, schema) as conn:
        _write(conn,
            ingest.build_rows(
                tuple(
                    NavObservation(d, 100.0 + i, "adjusted") for i, d in enumerate(days)
                ),
                [(iid, "USD")],
            ),
        )
        revised = ingest.build_rows(
            (NavObservation(days[position], 100.5 + position, "adjusted"),),
            [(iid, "USD")],
        )
        _write(conn, revised)
        data = conn.execute(
            "SELECT nav,return_start_date,return_1d FROM nav_timeseries "
            "WHERE instrument_id=%s ORDER BY nav_date",
            (iid,),
        ).fetchall()
        assert float(data[position][0]) == 100.5 + position
        if position > 0:
            assert data[position][1] == days[position - 1]
        if position < 3:
            assert data[position + 1][1] == days[position]
            assert float(data[position + 1][2]) == pytest.approx(
                __import__("math").log(
                    float(data[position + 1][0]) / float(data[position][0])
                ),
                abs=1e-8,
            )


def test_adjusted_proportional_reexpression_opens_hold_despite_small_daily_returns(
    test_dsn,
    schema,
):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, day) for day in (2, 5, 6, 7)]
    with _connect(test_dsn, schema) as conn:
        observations = tuple(
            NavObservation(d, 100.0 + i, "adjusted") for i, d in enumerate(days)
        )
        _write(conn, ingest.build_rows(observations, [(iid, "USD")])
        )
        revised = tuple(
            NavObservation(d, (100.0 + i) / 2, "adjusted") for i, d in enumerate(days)
        )
        run_id = _write(conn, ingest.build_rows(revised, [(iid, "USD")]))
        hold = conn.execute(
            "SELECT first_changed_date,last_changed_date FROM "
            "fund_nav_reexpression_holds WHERE instrument_id=%s",
            (iid,),
        ).fetchone()
        assert hold == (days[0], days[-1])
        # The hold is an active DETECTED event of the ledger, attributed to the
        # run/provider and to the revision head of its own write.
        event = conn.execute(
            "SELECT event_kind, source_run_id, source_provider, revision_head, "
            "resolves_event_id, rebase_receipt_id FROM fund_nav_reexpression_events "
            "WHERE instrument_id=%s",
            (iid,),
        ).fetchone()
        assert event[:3] == ("DETECTED", run_id, "tiingo") and event[4:] == (None, None)
        assert 0 < event[3] <= _head(conn, iid)
        rows = conn.execute(
            "SELECT return_1d FROM nav_timeseries WHERE instrument_id=%s "
            "AND nav_date>%s ORDER BY nav_date",
            (iid, days[0]),
        ).fetchall()
        assert all(abs(float(r[0])) < 0.02 for r in rows)


def test_nav_level_and_revision_roll_back_together_after_successor_failure(
    test_dsn, schema
):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, day) for day in (2, 5)]
    with _connect(test_dsn, schema) as conn:
        _write(conn,
            ingest.build_rows(
                (
                    NavObservation(days[0], 100, "adjusted"),
                    NavObservation(days[1], 101, "adjusted"),
                ),
                [(iid, "USD")],
            ),
        )
        head_before = conn.execute(
            "SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s", (iid,)
        ).fetchone()[0]
        conn.execute(
            "ALTER TABLE nav_timeseries ADD CONSTRAINT reject_successor "
            "CHECK (nav_date<>DATE '2026-01-05' OR return_1d<.1)"
        )
        conn.commit()
        with pytest.raises(psycopg.Error):
            _write(conn,
                ingest.build_rows(
                    (NavObservation(days[0], 50, "adjusted"),), [(iid, "USD")]
                ),
            )
        conn.rollback()
        assert (
            float(
                conn.execute(
                    "SELECT nav FROM nav_timeseries WHERE instrument_id=%s "
                    "AND nav_date=%s",
                    (iid, days[0]),
                ).fetchone()[0]
            )
            == 100
        )
        assert (
            conn.execute(
                "SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            == head_before
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM fund_nav_reexpression_holds "
                "WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            == 0
        )


def test_concurrent_reader_sees_nav_revision_only_after_same_commit(
    test_dsn,
    schema,
    monkeypatch,
):
    iid, grid, _attempt = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    readiness.run(test_dsn)
    with _connect(test_dsn, schema) as writer, _connect(test_dsn, schema) as reader:
        head_before = writer.execute(
            "SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s", (iid,)
        ).fetchone()[0]
        writer.execute(
            "UPDATE nav_timeseries SET nav=nav+.5 "
            "WHERE instrument_id=%s AND nav_date=%s",
            (iid, grid[-1]),
        )
        assert (
            reader.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1 "
                "WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            is True
        )
        writer.commit()
        assert (
            reader.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1 "
                "WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            is False
        )
        head_after = reader.execute(
            "SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s", (iid,)
        ).fetchone()[0]
        assert head_after > head_before
        with pytest.raises(psycopg.Error, match="append-only"):
            reader.execute(
                "DELETE FROM fund_nav_data_revisions WHERE revision_id=%s",
                (head_after,),
            )
        reader.rollback()


def test_unattributed_direct_nav_revision_cannot_be_readmitted(
    test_dsn, schema, monkeypatch
):
    iid, grid, _run = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    assert readiness.run(test_dsn)["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            "UPDATE nav_timeseries SET aum_usd=42 WHERE instrument_id=%s "
            "AND nav_date=%s",
            (iid, grid[-1]),
        )
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT reason_code FROM fund_nav_readiness_current_v1"
        ).fetchone()[0] == ("NAV_DATA_UNAVAILABLE")


def test_new_completed_head_does_not_hide_older_unverified_window_revision(
    test_dsn, schema, monkeypatch
):
    iid, grid, _run = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    assert readiness.run(test_dsn)["ready_count"] == 1
    repaired_run = uuid.uuid4()
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            "UPDATE nav_timeseries SET aum_usd=42 WHERE instrument_id=%s "
            "AND nav_date=%s",
            (iid, grid[-2]),
        )
        conn.execute(
            "UPDATE nav_timeseries SET source='temporary' "
            "WHERE instrument_id=%s AND nav_date=%s",
            (iid, grid[-1]),
        )
        price = float(
            conn.execute(
                "SELECT nav FROM nav_timeseries WHERE instrument_id=%s AND nav_date=%s",
                (iid, grid[-1]),
            ).fetchone()[0]
        )
        conn.commit()
        corrected = ingest.build_rows(
            (NavObservation(grid[-1], price, "adjusted"),),
            [(iid, "USD")],
            calendar={grid[-1]: ("NYSE-TEST", "v1", "fixture:NYSE-valuation-due")},
        )
        repaired_run = _provider_write(conn, corrected)
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        head = conn.execute(
            "SELECT r.source_run_id FROM fund_nav_data_heads h "
            "JOIN fund_nav_data_revisions r ON r.revision_id=h.revision_id "
            "WHERE h.instrument_id=%s",
            (iid,),
        ).fetchone()[0]
        assert head == repaired_run
        assert conn.execute(
            "SELECT reason_code FROM fund_nav_readiness_current_v1"
        ).fetchone()[0] == ("NAV_DATA_UNAVAILABLE")


def test_first_instrument_commit_then_crash_invalidates_without_http_attempt(
    test_dsn,
    schema,
    monkeypatch,
):
    iid, grid, attempt_run = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    readiness.run(test_dsn)
    other = uuid.uuid4()
    with _connect(test_dsn, schema) as writer, _connect(test_dsn, schema) as reader:
        first = ingest.build_rows(
            (NavObservation(grid[-1], 104.5, "adjusted"),),
            [(iid, "USD")],
            calendar={grid[-1]: ("NYSE-TEST", "v1", "fixture:NYSE-valuation-due")},
        )
        repeated = ingest.build_rows(
            (NavObservation(grid[-1], 105, "adjusted"),), [(other, "USD")]
        )
        # First instrument: its attempt and NAV commit together (same xid).
        attempt_id = _provider_write(writer, first, complete=False)
        # Second instrument crashes before commit: nothing of it survives.
        with pytest.raises(ValueError, match="duplicate NAV date"):
            ingest.upsert_nav_timeseries(
                writer, repeated + repeated, run_id=attempt_id, provider="tiingo"
            )
        assert (
            reader.execute(
                "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s", (other,)
            ).fetchone()[0]
            == 0
        )
        assert (
            reader.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1 "
                "WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            is False
        )
        assert reader.execute(
            "SELECT array_agg(run_id ORDER BY persisted_at) FROM nav_ingestion_attempts "
            "WHERE instrument_id=%s",
            (iid,),
        ).fetchone()[0] == [attempt_run, attempt_id]
        # The committed attempt stays valid although its run is still running.
        assert reader.execute(
            "SELECT status FROM nav_ingestion_runs WHERE run_id=%s", (attempt_id,)
        ).fetchone()[0] == "running"
        revisions = reader.execute(
            "SELECT revision_id,source_run_id FROM fund_nav_data_revisions "
            "WHERE instrument_id=%s ORDER BY revision_id",
            (iid,),
        ).fetchall()
        assert revisions[-1][1] == attempt_id
        assert revisions[-1][0] > revisions[0][0]


def test_ingestion_attempts_distinguish_no_data_error_no_attempt_and_not_due(
    test_dsn,
    schema,
    monkeypatch,
):
    _bootstrap(test_dsn, schema)
    ids = {ticker: uuid.uuid4() for ticker in "ABCDE"}
    today = dt.date.today()
    with _connect(test_dsn, schema) as conn:
        conn.execute("""CREATE TABLE instruments_universe (
             instrument_id uuid PRIMARY KEY,ticker text,currency text,
             is_active boolean,attributes jsonb)""")
        for rank, (ticker, iid) in enumerate(ids.items()):
            conn.execute(
                """INSERT INTO instruments_universe VALUES
                   (%s,%s,'USD',true,jsonb_build_object('aum_usd',%s::text))""",
                (iid, ticker, str(500 - rank * 100)),
            )
        conn.execute(
            "INSERT INTO nav_timeseries (instrument_id,nav_date,nav) "
            "VALUES (%s,%s,100)",
            (ids["D"], today),
        )

    class _Primary:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def fetch_daily_observations(self, ticker, _start, _end):
            timestamp = dt.datetime.now(dt.timezone.utc)
            if ticker == "A":
                bars = (
                    NavObservation(today - dt.timedelta(days=1), 100.0, "adjusted"),
                    NavObservation(today, 101.0, "adjusted"),
                )
                return NavFetchResult("success_new", bars, timestamp, timestamp)
            return NavFetchResult(
                "transient_error" if ticker == "C" else "success_no_new",
                attempted_at=timestamp,
                finished_at=timestamp,
            )

    class _Fallback:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def fetch_observations(self, ticker, _start, _end):
            timestamp = dt.datetime.now(dt.timezone.utc)
            status = "invalid_payload" if ticker == "C" else "not_found"
            return (
                NavFetchResult("success_no_new"),
                None,
                [
                    (
                        "yahoo",
                        NavFetchResult(
                            status, attempted_at=timestamp, finished_at=timestamp
                        ),
                    )
                ],
            )

    monkeypatch.setattr(ingest, "connect", lambda dsn: _connect(dsn, schema))
    monkeypatch.setattr(ingest, "TiingoClient", _Primary)
    from src.workers import _fallback_nav

    monkeypatch.setattr(_fallback_nav, "FallbackNav", _Fallback)
    stats = ingest.run(test_dsn, calc_date=today.isoformat(), limit=3)
    assert stats["tickers_loaded"] == 1 and stats["tickers_empty"] == 2
    with _connect(test_dsn, schema) as conn:
        records = conn.execute(
            "SELECT ticker,provider,status FROM nav_ingestion_attempts ORDER BY ticker,provider"
        ).fetchall()
        assert ("A", "tiingo", "success_new") in records
        # A success claim without persisted rows is not success evidence.
        assert ("B", "tiingo", "empty") in records
        assert ("B", "yahoo", "not_found") in records
        assert ("C", "tiingo", "transient_error") in records
        assert ("C", "yahoo", "invalid_payload") in records
        assert ("D", "tiingo", "not_due") in records
        assert ("E", "tiingo", "not_attempted_budget") in records
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s",
                (ids["A"],),
            ).fetchone()[0]
            == 2
        )
        assert (
            conn.execute("SELECT status FROM nav_ingestion_runs").fetchone()[0]
            == "completed"
        )


def test_shared_ticker_one_missing_instrument_does_not_hide_due_attempt(
    test_dsn, schema
):
    _bootstrap(test_dsn, schema)
    current, missing = uuid.uuid4(), uuid.uuid4()
    due = dt.date.today()
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            "CREATE TABLE instruments_universe (instrument_id uuid PRIMARY KEY,"
            "ticker text,currency text,is_active boolean,attributes jsonb)"
        )
        for iid in (current, missing):
            conn.execute(
                "INSERT INTO instruments_universe VALUES (%s,'SAME','USD',true,'{}')",
                (iid,),
            )
        conn.execute(
            "INSERT INTO nav_timeseries (instrument_id,nav_date,nav) "
            "VALUES (%s,%s,100)",
            (current, due),
        )
        universe = ingest._fetch_universe(conn)
        watermarks = ingest._fetch_watermarks(conn)
        assert "SAME" not in watermarks
        plans = ingest.select_stale_tickers(
            universe, watermarks, due, 1, target_session=due
        )
        assert len(plans) == 1 and {iid for iid, _ in plans[0].instruments} == {
            current,
            missing,
        }


# ──────────────────────────────────────────────────────────────────────────────
# Phase A: W1 maintenance lineage, W4 calendar tuple/rollover, W5 risk identity
# ──────────────────────────────────────────────────────────────────────────────
SOURCE = "fixture:NYSE-valuation-due"
STAMP = ("NYSE-TEST", "v1", SOURCE)


def _reason(conn, iid):
    return conn.execute(
        "SELECT reason_code FROM fund_nav_readiness_current_v1 WHERE instrument_id=%s",
        (iid,),
    ).fetchone()[0]


def _head(conn, iid):
    return conn.execute(
        "SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s", (iid,)
    ).fetchone()[0]


def _completed_run(conn, iid, start, end, *, status="completed"):
    """Register a run and a successful attempt without NAV writes (own txn).

    Such an attempt cannot attribute later writes (different xid); tests that
    write NAV use ``_provider_write``. Returns the run id.
    """
    run_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO nav_ingestion_runs (run_id,requested_end,status) "
        "VALUES (%s,%s,'running')",
        (run_id, end),
    )
    conn.commit()
    conn.execute(
        "INSERT INTO nav_ingestion_attempts (run_id,instrument_id,ticker,provider,"
        "requested_start,requested_end,attempted_at,finished_at,status,"
        "newest_observed_date,row_count) VALUES (%s,%s,'SYN','tiingo',%s,%s,"
        "clock_timestamp(),clock_timestamp(),'success_new',%s,1)",
        (run_id, iid, start, end, end),
    )
    if status != "running":
        conn.execute(
            "UPDATE nav_ingestion_runs SET status=%s WHERE run_id=%s", (status, run_id)
        )
    conn.commit()
    return run_id


def _begin_maintenance(conn, ids, start, end):
    """Register a running maintenance run inside the caller's transaction."""
    run_id = uuid.uuid4()
    pins = conn.execute(
        "SELECT p.policy_id,p.policy_version,p.policy_hash,p.calendar_id,"
        "p.calendar_version,p.calendar_source FROM nav_policy_current c "
        "JOIN nav_policy_versions p USING (policy_id,policy_version)"
    ).fetchone()
    conn.execute(
        "INSERT INTO nav_calendar_maintenance_runs (maintenance_run_id,operation,"
        "policy_id,policy_version,policy_hash,calendar_id,calendar_version,"
        "calendar_source,plan_sha256,instrument_ids,window_start,window_end,status,"
        "before_digest) VALUES (%s,'calendar_stamp',%s,%s,%s,%s,%s,%s,%s,%s::uuid[],"
        "%s,%s,'running',%s)",
        (run_id, *pins, "d" * 64, ids, start, end, "e" * 64),
    )
    conn.execute("SELECT set_config('nav.maintenance_run_id', %s, true)", (str(run_id),))
    return run_id


def test_maintenance_stamps_provider_data_and_preserves_economic_lineage(
    test_dsn, schema, monkeypatch
):
    iid, grid, ing_run = _seed(test_dsn, schema, stamp=False)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema, autocommit=True) as conn:
        assert _reason(conn, iid) == "RETURN_INTERVAL_INCOMPATIBLE"
        runs_before = conn.execute(
            "SELECT (SELECT count(*) FROM nav_ingestion_runs),"
            "(SELECT count(*) FROM nav_ingestion_attempts)"
        ).fetchone()
        plan = operator._maintenance_plan(conn, [str(iid)], grid[0], grid[-1])
        assert plan["eligible_rows"] == 401 and plan["scope_rows"] == 401
        result = operator._apply_calendar_maintenance(
            conn, [str(iid)], grid[0], grid[-1], "d" * 64
        )
        assert result["status"] == "completed" and result["changed_rows"] == 401
        assert result["after_digest"] == plan["expected_after_digest"]
        assert conn.execute(
            "SELECT count(*), bool_and(NOT data_changed AND calendar_changed "
            "AND source_run_id IS NULL) FROM fund_nav_data_revisions "
            "WHERE maintenance_run_id=%s",
            (result["maintenance_run_id"],),
        ).fetchone() == (401, True)
        assert conn.execute(
            "SELECT status,changed_rows FROM nav_calendar_maintenance_runs"
        ).fetchone() == ("completed", 401)
        # Maintenance never fabricates provider runs or attempts.
        assert (
            conn.execute(
                "SELECT (SELECT count(*) FROM nav_ingestion_runs),"
                "(SELECT count(*) FROM nav_ingestion_attempts)"
            ).fetchone()
            == runs_before
        )
        head = _head(conn, iid)
        replay = operator._apply_calendar_maintenance(
            conn, [str(iid)], grid[0], grid[-1], "d" * 64
        )
        assert replay["status"] == "no_op" and _head(conn, iid) == head
        assert (
            conn.execute("SELECT count(*) FROM nav_calendar_maintenance_runs").fetchone()[0]
            == 1
        )
    assert readiness.run(test_dsn)["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        row = conn.execute(
            "SELECT ingestion_run_id, calendar_equivalence_digest, snapshot_current "
            "FROM fund_nav_readiness_current_v1 WHERE instrument_id=%s",
            (iid,),
        ).fetchone()
        assert row[0] == ing_run and len(row[1]) == 64 and row[2] is True


def test_datum_without_revision_stays_unavailable_after_maintenance_stamp(
    test_dsn, schema, monkeypatch
):
    iid, grid, ing_run = _seed(test_dsn, schema, stamp=False)
    legacy = uuid.uuid4()
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as conn:
        conn.execute("INSERT INTO funds_profile_mv VALUES (%s)", (legacy,))
        conn.execute(
            """INSERT INTO nav_instrument_policy_evidence
               (instrument_id,policy_id,policy_version,known_at,effective_at,
                fund_status,valuation_frequency,identity_verified,
                return_basis_verified,currency_verified,evidence_reference)
               VALUES (%s,'synthetic','v1',clock_timestamp()-interval '1 day',
                       clock_timestamp()-interval '1 day','ACTIVE','daily',
                       true,true,true,'legacy-history')""",
            (legacy,),
        )
        conn.commit()
        # A successful attempt that justifies no revision (different xid).
        _completed_run(conn, legacy, grid[0], grid[-1])
        # Pre-DDL history: rows exist with full provenance but no revision.
        conn.execute("SET LOCAL session_replication_role = replica")
        columns = (
            "nav_date, nav, return_1d, currency, source, return_type, source_nav,"
            " source_nav_kind, nav_repair_kind, return_start_date,"
            " return_source_boundary, return_uses_repaired_nav, return_semantics,"
            " return_verification_status, calendar_id, calendar_version, calendar_source"
        )
        conn.execute(
            f"INSERT INTO nav_timeseries (instrument_id, {columns}) "
            f"SELECT %s, {columns} FROM nav_timeseries WHERE instrument_id=%s",
            (legacy, iid),
        )
        conn.commit()
    with _connect(test_dsn, schema, autocommit=True) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM fund_nav_data_revisions WHERE instrument_id=%s",
                (legacy,),
            ).fetchone()[0]
            == 0
        )
        applied = operator._apply_calendar_maintenance(
            conn, [str(iid), str(legacy)], grid[0], grid[-1], "d" * 64
        )
        assert applied["changed_rows"] == 802
    assert readiness.run(test_dsn)["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, legacy) == "NAV_DATA_UNAVAILABLE"
        assert _reason(conn, iid) is None


def test_metadata_revision_never_hides_unknown_data_revision(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema, stamp=False)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            "UPDATE nav_timeseries SET aum_usd=42 WHERE instrument_id=%s AND nav_date=%s",
            (iid, grid[-3]),
        )
        conn.commit()
    with _connect(test_dsn, schema, autocommit=True) as conn:
        operator._apply_calendar_maintenance(conn, [str(iid)], grid[0], grid[-1], "d" * 64)
        latest = conn.execute(
            "SELECT maintenance_run_id IS NOT NULL, data_changed FROM "
            "fund_nav_data_revisions WHERE instrument_id=%s AND nav_date=%s "
            "ORDER BY revision_id DESC LIMIT 1",
            (iid, grid[-3]),
        ).fetchone()
        assert latest == (True, False)
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "NAV_DATA_UNAVAILABLE"


def test_incomplete_or_uncovering_provider_run_fails_data_lineage(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as conn:
        price = float(
            conn.execute(
                "SELECT nav FROM nav_timeseries WHERE instrument_id=%s AND nav_date=%s",
                (iid, grid[-1]),
            ).fetchone()[0]
        )
        running = _provider_write(
            conn,
            ingest.build_rows(
                (NavObservation(grid[-1], price + 0.000001, "adjusted"),),
                [(iid, "USD")],
                calendar={grid[-1]: STAMP},
            ),
            start=grid[0],
            complete=False,
        )
    # N1-b: the committed same-xid attempt proves the level although its parent
    # is still running; the NAV change only makes the old risk input stale.
    readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "RETURN_SAMPLE_NOT_CURRENT"
        conn.execute(
            "UPDATE nav_ingestion_runs SET status='aborted', reason_code='INTERRUPTED' "
            "WHERE run_id=%s",
            (running,),
        )
        conn.commit()
    readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "RETURN_SAMPLE_NOT_CURRENT"
        # A write whose attempt window does not cover the level date is refused
        # by the DB at COMMIT (no revision can claim it).
        with pytest.raises(psycopg.errors.RaiseException, match="same-transaction"):
            _provider_write(
                conn,
                ingest.build_rows(
                    (NavObservation(grid[-2], 50.0, "adjusted"),),
                    [(iid, "USD")],
                    calendar={grid[-2]: STAMP},
                ),
                start=grid[-1],
                end=grid[-1],
            )
        conn.rollback()
        assert float(conn.execute(
            "SELECT nav FROM nav_timeseries WHERE instrument_id=%s AND nav_date=%s",
            (iid, grid[-2]),
        ).fetchone()[0]) != 50.0
        # An unattributed direct write still invalidates lineage.
        conn.execute(
            "UPDATE nav_timeseries SET nav=50.0 WHERE instrument_id=%s AND nav_date=%s",
            (iid, grid[-2]),
        )
        conn.commit()
    readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "NAV_DATA_UNAVAILABLE"


def test_maintenance_contexts_scope_and_tampering_fail_closed(test_dsn, schema):
    iid, grid, ing_run = _seed(test_dsn, schema, stamp=False)
    other = uuid.uuid4()
    stamp_sql = (
        "UPDATE nav_timeseries SET calendar_id=%s,calendar_version=%s,"
        "calendar_source=%s{extra} WHERE instrument_id=%s AND nav_date=%s"
    )
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            "SELECT set_config('nav.ingestion_run_id', %s, true)", (str(ing_run),)
        )
        _begin_maintenance(conn, [iid], grid[-5], grid[-1])
        with pytest.raises(psycopg.Error, match="mutually exclusive"):
            conn.execute(stamp_sql.format(extra=""), (*STAMP, iid, grid[-1]))
        conn.rollback()
        cases = [
            (stamp_sql.format(extra=",nav=nav+1"), (*STAMP, iid, grid[-1]), "metadata"),
            (stamp_sql.format(extra=""), (*STAMP, iid, grid[0]), "pinned scope"),
            (stamp_sql.format(extra=""), ("NYSE-TEST", "v9", SOURCE, iid, grid[-1]),
             "current published policy"),
            ("DELETE FROM nav_timeseries WHERE instrument_id=%s AND nav_date=%s",
             (iid, grid[-1]), "only update"),
        ]
        for statement, params, message in cases:
            _begin_maintenance(conn, [iid], grid[-5], grid[-1])
            with pytest.raises(psycopg.Error, match=message):
                conn.execute(statement, params)
            conn.rollback()
        with pytest.raises(psycopg.Error, match="not running"):
            conn.execute(
                "SELECT set_config('nav.maintenance_run_id', %s, true)",
                (str(uuid.uuid4()),),
            )
            conn.execute(stamp_sql.format(extra=""), (*STAMP, iid, grid[-1]))
        conn.rollback()
        # Completion must match the attributed revisions exactly.
        run_id = _begin_maintenance(conn, [iid], grid[-5], grid[-1])
        conn.execute(stamp_sql.format(extra=""), (*STAMP, iid, grid[-1]))
        with pytest.raises(psycopg.Error, match="does not match"):
            conn.execute(
                "UPDATE nav_calendar_maintenance_runs SET status='completed',"
                "completed_at=clock_timestamp(),changed_rows=2,after_digest=%s "
                "WHERE maintenance_run_id=%s",
                ("f" * 64, run_id),
            )
        conn.rollback()
        with pytest.raises(psycopg.Error, match="stamp trigger"):
            conn.execute(
                "INSERT INTO fund_nav_data_revisions (instrument_id,nav_date,"
                "mutation_kind,data_changed,calendar_changed) "
                "VALUES (%s,%s,'UPDATE',true,false)",
                (iid, grid[-1]),
            )
        conn.rollback()
        for ids, start in (([uuid.uuid4() for _ in range(21)], grid[-5]),
                           ([iid], grid[-1] - dt.timedelta(days=601)),
                           ([iid, iid], grid[-5])):
            with pytest.raises(psycopg.errors.CheckViolation):
                _begin_maintenance(conn, ids, start, grid[-1])
            conn.rollback()
        # A newer, unverified lifecycle fact wins over an older verified one.
        conn.execute(
            """INSERT INTO nav_instrument_policy_evidence
               (instrument_id,policy_id,policy_version,known_at,effective_at,
                fund_status,valuation_frequency,identity_verified,
                return_basis_verified,currency_verified,evidence_reference)
               VALUES (%s,'synthetic','v1',clock_timestamp(),clock_timestamp(),
                       'ACTIVE','daily',true,true,false,'currency-unverified')""",
            (iid,),
        )
        conn.commit()
        _begin_maintenance(conn, [iid], grid[-5], grid[-1])
        with pytest.raises(psycopg.Error, match="lifecycle"):
            conn.execute(stamp_sql.format(extra=""), (*STAMP, iid, grid[-1]))
        conn.rollback()
    with _connect(test_dsn, schema, autocommit=True) as conn:
        plan = operator._maintenance_plan(conn, [str(iid), str(other)], grid[-5], grid[-1])
        assert plan["eligible_rows"] == 0
        assert conn.execute(
            "SELECT count(*) FROM nav_calendar_maintenance_runs"
        ).fetchone()[0] == 0


def test_completed_maintenance_is_immutable(test_dsn, schema):
    iid, grid, _ = _seed(test_dsn, schema, stamp=False)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        done = operator._apply_calendar_maintenance(
            conn, [str(iid)], grid[-2], grid[-1], "d" * 64
        )
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute(
                "UPDATE nav_calendar_maintenance_runs SET plan_sha256=%s "
                "WHERE maintenance_run_id=%s",
                ("0" * 64, done["maintenance_run_id"]),
            )
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute(
                "DELETE FROM nav_calendar_maintenance_runs WHERE maintenance_run_id=%s",
                (done["maintenance_run_id"],),
            )


def test_maintenance_failure_rolls_back_levels_revisions_and_run(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema, stamp=False)
    monkeypatch.setattr(operator, "_stamped", lambda *_a: [])
    with _connect(test_dsn, schema, autocommit=True) as conn:
        head = _head(conn, iid)
        with pytest.raises(ValueError, match="maintenance_verification_failed"):
            operator._apply_calendar_maintenance(
                conn, [str(iid)], grid[0], grid[-1], "d" * 64
            )
        assert conn.execute(
            "SELECT (SELECT count(*) FROM nav_calendar_maintenance_runs),"
            "(SELECT count(*) FROM fund_nav_data_revisions WHERE maintenance_run_id "
            "IS NOT NULL),(SELECT count(*) FROM nav_timeseries WHERE calendar_id "
            "IS NOT NULL)"
        ).fetchone() == (0, 0, 0)
        assert _head(conn, iid) == head


def test_maintenance_and_ingestion_locks_exclude_each_other(
    test_dsn, schema, monkeypatch, capsys
):
    iid, grid, _ = _seed(test_dsn, schema, stamp=False)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    ids = [str(iid)]
    cli = ["--schema", schema, "--expected-sql-sha256",
           hashlib.sha256(ddl).hexdigest(), "--instrument-id", ids[0],
           "--start", grid[0].isoformat(), "--end", grid[-1].isoformat()]
    assert operator.main(cli) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["plan"]["before"]["maintenance"]["eligible_rows"] == 401
    assert dry["plan"]["operations"] == ["ddl", "maintenance"]
    plan = dry["plan_sha256"]
    with _connect(test_dsn, schema) as holder:
        with advisory_lock(holder, LOCK_INSTRUMENT_INGESTION) as got:
            assert got
            assert operator.main([*cli, "--mode", "apply", "--plan-sha256", plan]) == 4
            busy = json.loads(capsys.readouterr().out)
            assert busy["status"] == "lock_busy" and busy["retryable"] is True
            assert busy["dml_committed"] is False
    with _connect(test_dsn, schema) as holder:
        holder.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_INSTRUMENT_INGESTION,))
        monkeypatch.setattr(ingest, "connect", lambda dsn: _connect(dsn, schema))
        stats = ingest.run(test_dsn, calc_date=dt.date.today().isoformat(), limit=0)
        assert stats["skipped"] == "lock_busy"
        holder.rollback()
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT count(*) FROM nav_timeseries WHERE calendar_id IS NOT NULL"
        ).fetchone()[0] == 0
    assert operator.main([*cli, "--mode", "apply", "--plan-sha256", plan]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert (applied["maintenance"], applied["changed_rows"], applied["dml_committed"]) == (
        "committed", 401, True,
    )
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT plan_sha256 FROM nav_calendar_maintenance_runs"
        ).fetchone()[0] == plan
    # Retry of the same (now stale) plan after commit: truthful no-op.
    assert operator.main([*cli, "--mode", "apply", "--plan-sha256", plan]) == 0
    replay = json.loads(capsys.readouterr().out)
    assert (replay["status"], replay["maintenance"], replay["dml_committed"]) == (
        "unchanged", "unchanged", False,
    )
    too_many = [a for i in range(21) for a in ("--instrument-id", str(uuid.uuid4()))]
    assert operator.main(["--schema", schema, "--expected-sql-sha256",
                          hashlib.sha256(ddl).hexdigest(), *too_many,
                          "--start", grid[0].isoformat(),
                          "--end", grid[-1].isoformat()]) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "backfill_allowlist_invalid"


def test_overlap_without_mapping_preserves_tuple_and_head(test_dsn, schema, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as conn:
        head = _head(conn, iid)
        same = tuple(
            NavObservation(d, round(100.0 + (401 - 5 + i) * 0.01, 6), "adjusted")
            for i, d in enumerate(grid[-5:])
        )
        _provider_write(conn, ingest.build_rows(same, [(iid, "USD")]))
        assert _head(conn, iid) == head
        assert conn.execute(
            "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s AND "
            "(calendar_id,calendar_version,calendar_source)=(%s,%s,%s)",
            (iid, *STAMP),
        ).fetchone()[0] == 401
    assert readiness.run(test_dsn)["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        changed = (NavObservation(grid[-1], 150.0, "adjusted"),)
        overlap = _provider_write(conn, ingest.build_rows(changed, [(iid, "USD")]))
        assert _head(conn, iid) > head
        assert conn.execute(
            "SELECT calendar_id,calendar_version,calendar_source FROM nav_timeseries "
            "WHERE instrument_id=%s AND nav_date=%s",
            (iid, grid[-1]),
        ).fetchone() == STAMP
        assert conn.execute(
            "SELECT data_changed,calendar_changed,source_run_id FROM "
            "fund_nav_data_revisions WHERE instrument_id=%s AND nav_date=%s "
            "ORDER BY revision_id DESC LIMIT 1",
            (iid, grid[-1]),
        ).fetchone() == (True, False, overlap)


def test_db_rejects_partial_reset_unattributed_and_ungoverned_stamps(test_dsn, schema):
    iid, grid, ing_run = _seed(test_dsn, schema)
    fresh = uuid.uuid4()
    with _connect(test_dsn, schema) as conn:
        for statement, params, message in (
            ("UPDATE nav_timeseries SET calendar_version=NULL WHERE instrument_id=%s "
             "AND nav_date=%s", (iid, grid[-1]), "indivisible"),
            ("UPDATE nav_timeseries SET calendar_id=NULL,calendar_version=NULL,"
             "calendar_source=NULL WHERE instrument_id=%s AND nav_date=%s",
             (iid, grid[-1]), "reset"),
            ("INSERT INTO nav_timeseries (instrument_id,nav_date,nav,calendar_id,"
             "calendar_version,calendar_source) VALUES (%s,%s,1,%s,%s,%s)",
             (fresh, grid[-1], *STAMP), "attribution"),
        ):
            with pytest.raises(psycopg.Error, match=message):
                conn.execute(statement, params)
            conn.rollback()
        conn.execute("SELECT set_config('nav.ingestion_run_id', %s, true)", (str(ing_run),))
        with pytest.raises(psycopg.Error, match="both run and provider"):
            conn.execute(
                "INSERT INTO nav_timeseries (instrument_id,nav_date,nav) VALUES (%s,%s,1)",
                (fresh, grid[-1]),
            )
        conn.rollback()
        for day, stamp in ((grid[-1], ("NYSE-TEST", "v9", SOURCE)),
                           (dt.date(2026, 9, 7), STAMP)):
            conn.execute(
                "SELECT set_config('nav.ingestion_run_id', %s, true)", (str(ing_run),)
            )
            conn.execute("SELECT set_config('nav.ingestion_provider', 'tiingo', true)")
            with pytest.raises(psycopg.Error, match="current published policy"):
                conn.execute(
                    "INSERT INTO nav_timeseries (instrument_id,nav_date,nav,calendar_id,"
                    "calendar_version,calendar_source) VALUES (%s,%s,1,%s,%s,%s)",
                    (fresh, day, *stamp),
                )
            conn.rollback()
        partial = ingest.build_rows(
            (NavObservation(grid[-1], 1.0, "adjusted"),), [(fresh, "USD")]
        )
        partial[0]["calendar_version"] = "v1"
        with pytest.raises(ValueError, match="partial"):
            ingest.upsert_nav_timeseries(conn, partial, run_id=ing_run, provider="tiingo")
        assert conn.execute(
            "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s", (fresh,)
        ).fetchone()[0] == 0


def _publish_rollover(conn, iid, grid, *, version="v2", tweak_index=None):
    sessions = []
    for index, day in enumerate(grid):
        close = dt.datetime.combine(day, dt.time(20), dt.timezone.utc)
        due = dt.datetime.combine(day, dt.time(23), dt.timezone.utc)
        if index == tweak_index:
            due += dt.timedelta(minutes=30)
        sessions.append((day, close, due, SOURCE))
    conn.execute(
        """INSERT INTO nav_policy_versions
           (policy_id,policy_version,policy_hash,readiness_profile,
            valuation_frequency,calendar_id,calendar_version,calendar_source,
            timezone,coverage_start,coverage_end,valid_through,
            calendar_session_count,calendar_digest,sample_intervals,
            annualization_sessions,required_nav_kind,required_return_semantics,
            modeling_currency,currency_treatment,source_reference,published_at)
           VALUES ('synthetic',%s,%s,'current_daily_nav_v1','daily','NYSE-TEST',%s,
                   %s,'America/New_York',%s,%s,clock_timestamp()+interval '1 day',
                   %s,%s,400,252,'adjusted','observed_interval_log_ratio','USD',
                   'native_only',%s,NULL)""",
        (version, "b" * 64, version, SOURCE, grid[0], grid[-1], len(sessions),
         calendar_digest(sessions), SOURCE),
    )
    for day, close, due, _ in sessions:
        conn.execute(
            "INSERT INTO nav_valuation_schedules VALUES ('NYSE-TEST',%s,%s,%s,%s,%s,%s)",
            (version, day, close, due, SOURCE, SOURCE),
        )
    conn.execute(
        "UPDATE nav_policy_versions SET published_at=clock_timestamp() "
        "WHERE policy_version=%s",
        (version,),
    )
    conn.execute(
        "UPDATE nav_policy_current SET policy_version=%s,published_at=clock_timestamp()",
        (version,),
    )
    conn.execute(
        """INSERT INTO nav_instrument_policy_evidence
           (instrument_id,policy_id,policy_version,known_at,effective_at,
            fund_status,valuation_frequency,identity_verified,
            return_basis_verified,currency_verified,evidence_reference)
           VALUES (%s,'synthetic',%s,clock_timestamp()-interval '1 minute',
                   clock_timestamp()-interval '1 minute','ACTIVE','daily',
                   true,true,true,'rollover-fixture')""",
        (iid, version),
    )
    conn.commit()


@pytest.mark.parametrize("tweak_index", [None, 0, 200])
def test_rollover_accepts_equivalent_old_stamps_without_restamp(
    test_dsn, schema, monkeypatch, tweak_index
):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as conn:
        head = _head(conn, iid)
        _publish_rollover(conn, iid, grid, tweak_index=tweak_index)
    # N3: the v1-pinned risk run is not evidence for the v2 readiness policy.
    assert readiness.run(test_dsn)["ready_count"] == 0
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) != "NAV_POLICY_UNAVAILABLE"
        nav_rows = conn.execute(
            "SELECT nav_date, nav FROM nav_timeseries WHERE instrument_id=%s "
            "ORDER BY nav_date",
            (iid,),
        ).fetchall()
        _publish_risk_run(conn, grid[-1], {iid: nav_rows})  # pins v2
    report = readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        row = conn.execute(
            "SELECT calendar_version, reason_code, calendar_equivalence_digest, "
            "snapshot_current FROM fund_nav_readiness_current_v1 WHERE instrument_id=%s",
            (iid,),
        ).fetchone()
        assert row[0] == "v2" and len(row[2]) == 64
        if tweak_index is None:
            assert report["ready_count"] == 1 and row[1] is None and row[3] is True
        else:
            # One different due instant (first predecessor or middle) is not
            # equivalent; string equality of dates alone is never enough.
            assert report["ready_count"] == 0
            assert row[1] == "RETURN_INTERVAL_INCOMPATIBLE"
        assert _head(conn, iid) == head
        assert conn.execute(
            "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s "
            "AND calendar_version='v1'",
            (iid,),
        ).fetchone()[0] == 401


def test_two_risk_runs_same_date_keep_both_evidences(test_dsn, schema, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    first = readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        first_risk = conn.execute(
            "SELECT risk_run_id FROM fund_nav_readiness_v1 WHERE run_id=%s",
            (first["run_id"],),
        ).fetchone()[0]
        nav = conn.execute(
            "SELECT nav_date,nav FROM nav_timeseries WHERE instrument_id=%s "
            "ORDER BY nav_date",
            (iid,),
        ).fetchall()
        second_risk = _publish_risk_run(conn, grid[-1], {iid: nav})
        assert conn.execute(
            "SELECT count(*) FROM fund_nav_feature_evidence WHERE instrument_id=%s "
            "AND calc_date=%s",
            (iid, grid[-1]),
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
        ).fetchone()[0] is False
    second = readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        joined = conn.execute(
            "SELECT r.run_id::text, f.risk_run_id FROM fund_nav_readiness_v1 r "
            "JOIN fund_nav_readiness_runs run USING (run_id) "
            "JOIN fund_nav_feature_evidence f ON f.risk_run_id=r.risk_run_id "
            "AND f.instrument_id=r.instrument_id ORDER BY run.evaluated_at"
        ).fetchall()
        assert joined == [(first["run_id"], first_risk), (second["run_id"], second_risk)]
        assert first_risk != second_risk and second["ready_count"] == 1


def test_risk_evidence_append_only_and_exact_member_sets(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    day = dt.date.today() - dt.timedelta(days=1)
    rows = [(day - dt.timedelta(days=n), 100.0 + n) for n in range(30, -1, -1)]
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    run = uuid.uuid4()

    def feature(conn, iid, calc=day):
        risk._persist_feature_evidence(conn, iid, calc, rows, 0.04, run, None, {}, {}, [])

    with _connect(test_dsn, schema) as conn:
        _register_risk_run(
            conn, run, day, [a, b], scope="diagnostic", reason="POLICY_UNAVAILABLE"
        )
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                "INSERT INTO fund_nav_risk_run_members VALUES (%s,%s)", (run, a)
            )
        conn.rollback()
        _register_risk_run(
            conn, run, day, [a, b], scope="diagnostic", reason="POLICY_UNAVAILABLE"
        )
        conn.commit()
        feature(conn, a)
        conn.commit()
        for action, error in (
            (lambda: feature(conn, a), psycopg.errors.UniqueViolation),
            (lambda: feature(conn, c), psycopg.errors.ForeignKeyViolation),
        ):
            with pytest.raises(error):
                action()
            conn.rollback()
        for statement, message in (
            (f"INSERT INTO fund_nav_risk_run_members VALUES ('{run}','{c}')", "frozen"),
            ("UPDATE fund_nav_feature_evidence SET nav_count=99", "append-only"),
            ("DELETE FROM fund_nav_feature_evidence", "append-only"),
            ("DELETE FROM fund_nav_risk_run_members", "append-only"),
            ("DELETE FROM fund_nav_risk_runs", "append-only"),
            ("UPDATE fund_nav_risk_runs SET run_scope='current_full'", "immutable"),
        ):
            with pytest.raises(psycopg.Error, match=message):
                conn.execute(statement)
            conn.rollback()
        with pytest.raises(psycopg.Error, match="calc_date differs"):
            feature(conn, b, day - dt.timedelta(days=1))
        conn.rollback()
        # Count-equal but set-wrong: a has feature+exclusion, b uncovered.
        risk._record_risk_exclusion(conn, run, a, day, "METRICS_UNAVAILABLE", [])
        with pytest.raises(RuntimeError, match="incomplete"):
            risk._finish_risk_run(conn, run, 2, 1)
        conn.rollback()
        risk._record_risk_exclusion(conn, run, a, day, "METRICS_UNAVAILABLE", [])
        with pytest.raises(psycopg.Error, match="exact member set"):
            conn.execute(
                "UPDATE fund_nav_risk_runs SET status='metrics_complete',"
                "persisted_rows=2 WHERE risk_run_id=%s",
                (run,),
            )
        conn.rollback()
        risk._record_risk_exclusion(conn, run, b, day, "NAV_WINDOW_TOO_SHORT", [])
        conn.commit()
        risk._finish_risk_run(conn, run, 2, 1)
        with pytest.raises(psycopg.Error, match="no longer accepting"):
            risk._record_risk_exclusion(conn, run, b, day, "METRICS_UNAVAILABLE", [])
        conn.rollback()
        with pytest.raises(psycopg.Error, match="not permitted"):
            conn.execute(
                "UPDATE fund_nav_risk_runs SET status='running',persisted_rows=NULL "
                "WHERE risk_run_id=%s",
                (run,),
            )
        conn.rollback()


_RISK_RUN_INSERT = (
    "INSERT INTO fund_nav_risk_runs (risk_run_id,calc_date,run_scope,"
    "nonpublishing_reason,policy_id,policy_version,policy_hash,due_session,"
    "requested_calc_date,requested_limit,universe_digest,feature_definition_version,"
    "status,expected_rows) VALUES "
    "(%(id)s,%(calc)s,%(scope)s,%(reason)s,%(pid)s,%(pver)s,%(phash)s,%(due)s,"
    "%(req)s,%(limit)s,%(universe)s,%(definition)s,%(status)s,0)"
)


@pytest.mark.parametrize(
    ("override", "error"),
    [
        ({}, None),
        ({"scope": "diagnostic", "reason": "LIMITED_RUN", "limit": 5}, None),
        ({"scope": "diagnostic", "reason": "POLICY_UNAVAILABLE", "pid": None,
          "pver": None, "phash": None, "due": None}, None),
        ({"scope": "current_full", "pid": None, "pver": None, "phash": None},
         psycopg.errors.CheckViolation),
        ({"pver": None}, psycopg.errors.CheckViolation),
        ({"limit": 5}, psycopg.errors.CheckViolation),
        ({"reason": "LIMITED_RUN"}, psycopg.errors.CheckViolation),
        ({"due": "calc-1"}, psycopg.errors.CheckViolation),
        ({"due": None}, psycopg.errors.CheckViolation),
        ({"scope": "diagnostic"}, psycopg.errors.CheckViolation),
        ({"scope": "diagnostic", "reason": "BUSY"}, psycopg.errors.CheckViolation),
        ({"scope": "published"}, psycopg.errors.CheckViolation),
        ({"universe": "X" * 64}, psycopg.errors.CheckViolation),
        ({"definition": " "}, psycopg.errors.CheckViolation),
        ({"pver": "missing"}, psycopg.errors.ForeignKeyViolation),
        ({"status": "metrics_complete"}, psycopg.Error),
    ],
)
def test_risk_run_scope_and_pins_are_schema_enforced(test_dsn, schema, override, error):
    """W5 schema only: explicit rows, no worker classification (that is W3/B)."""
    _iid, grid, _ = _seed(test_dsn, schema)
    values = {
        "id": uuid.uuid4(), "calc": grid[-1], "scope": "current_full",
        "reason": None, "pid": "synthetic", "pver": "v1", "phash": None,
        "due": grid[-1], "req": None, "limit": None, "universe": "c" * 64,
        "definition": FEATURE_DEFINITION_VERSION, "status": "running",
    }
    with _connect(test_dsn, schema) as conn:
        values["phash"] = conn.execute(
            "SELECT policy_hash FROM nav_policy_versions WHERE policy_version='v1'"
        ).fetchone()[0]
        values.update(override)
        if values["due"] == "calc-1":
            values["due"] = grid[-2]
        if error is None:
            conn.execute(_RISK_RUN_INSERT, values)
            assert conn.execute(
                "SELECT status FROM fund_nav_risk_runs WHERE risk_run_id=%s",
                (values["id"],),
            ).fetchone() == ("running",)
        else:
            with pytest.raises(error):
                conn.execute(_RISK_RUN_INSERT, values)
        conn.rollback()


@pytest.mark.parametrize(
    ("column", "value"),
    [
        (None, None),
        ("ingestion_run_id", None),
        ("feature_evidence_id", " "),
        ("risk_input_max_date", None),
        ("feature_as_of", None),
        ("calendar_equivalence_digest", None),
        ("risk_input_fingerprint", None),
        ("return_semantics", None),
    ],
)
def test_admissible_row_check_never_passes_unknown(
    test_dsn, schema, monkeypatch, column, value
):
    _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    report = readiness.run(test_dsn)
    run_columns = (
        "readiness_profile,readiness_version,policy_id,policy_version,policy_hash,"
        "calendar_id,calendar_version,decision_at,as_of_session,latest_closed_session,"
        "window_start,window_end,sample_id,input_watermark,risk_publication_revision,"
        "published_risk_run_id,expected_rows"
    )
    with _connect(test_dsn, schema) as conn:
        building = uuid.uuid4()
        conn.execute(
            f"INSERT INTO fund_nav_readiness_runs (run_id,{run_columns},state) "
            f"SELECT %s,{run_columns},'building' FROM fund_nav_readiness_runs "
            "WHERE run_id=%s",
            (building, report["run_id"]),
        )
        selected = ",".join(
            "%(run)s" if name == "run_id" else name for name in readiness._ROW_FIELDS
        )
        conn.execute(
            f"INSERT INTO fund_nav_readiness_v1 ({','.join(readiness._ROW_FIELDS)}) "
            f"SELECT {selected} FROM fund_nav_readiness_v1 WHERE run_id=%(old)s "
            "AND admissible",
            {"run": building, "old": report["run_id"]},
        )
        if column is not None:
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute(
                    sql.SQL("UPDATE fund_nav_readiness_v1 SET {}=%s WHERE run_id=%s")
                    .format(sql.Identifier(column)),
                    (value, building),
                )
        conn.rollback()


def test_operator_old_contract_shape_is_upgrade_required_without_mutation(
    test_dsn, schema, monkeypatch, capsys
):
    _bootstrap(test_dsn, schema)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    with _connect(test_dsn, schema, autocommit=True) as conn:
        assert operator._check(conn, schema)["compatibility"] == "exact"
        # Deliberate pre-remediation shape: feature identity by instrument/date.
        conn.execute(
            "ALTER TABLE fund_nav_feature_evidence "
            "DROP CONSTRAINT fund_nav_feature_evidence_pkey, "
            "ADD PRIMARY KEY (instrument_id, calc_date, definition_version)"
        )
        conn.execute("ALTER TABLE fund_nav_risk_runs DROP COLUMN run_scope")
        report = operator._check(conn, schema)
        assert report["status"] == "upgrade_required"
        assert report["compatibility"] == "incompatible"
        assert {"columns", "constraints"} <= set(report["mismatches"])
    base = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    assert operator.main(base) == 3
    assert json.loads(capsys.readouterr().out)["compatibility"] == "incompatible"
    plan = operator.plan_hash(ddl, schema, b"", [], None, None)
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 3
    capsys.readouterr()
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conname='fund_nav_feature_evidence_pkey' "
            "AND connamespace=%s::regnamespace",
            (schema,),
        ).fetchone()[0] == "PRIMARY KEY (instrument_id, calc_date, definition_version)"
        assert conn.execute(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema=%s "
            "AND table_name='fund_nav_risk_runs' AND column_name='run_scope'",
            (schema,),
        ).fetchone()[0] == 0


def test_operator_absent_schema_installs_fresh(test_dsn, schema, monkeypatch, capsys):
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        report = operator._check(conn, schema)
        assert (report["status"], report["compatibility"]) == ("upgrade_required", "absent")
        assert report["code"] == "blocked_schema_usage_missing"
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    base = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    assert operator.main(base) == 3  # not ready: USAGE and read dependencies missing
    plan = json.loads(capsys.readouterr().out)["plan_sha256"]
    # DDL is a separate idempotent step: applied, but never reported ready.
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 3
    out = json.loads(capsys.readouterr().out)
    assert (out["ddl"], out["code"], out["dml_committed"]) == (
        "applied", "blocked_schema_usage_missing", False,
    )
    with _connect(test_dsn, schema, autocommit=True) as conn:
        assert operator._check(conn, schema)["compatibility"] == "exact"
        _access_fixture(conn, schema)
        assert operator._check(conn, schema)["status"] == "ready"
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["status"], out["ddl"]) == ("unchanged", "unchanged")


def test_catalog_manifest_is_pinned_to_the_ddl():
    manifest = json.loads(
        (ROOT / "schemas" / "fund_nav_readiness_v1.catalog.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["ddl_sha256"] == hashlib.sha256(
        (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    ).hexdigest()
    assert set(manifest["signature"]["relations"]) == {
        *operator.TABLES,
        *operator.VIEWS,
    }
    # The external NAV hypertable trigger and its owned function are contract.
    assert manifest["signature"]["triggers"]["nav_timeseries"] == [
        [
            "fund_nav_stamp_revision",
            "CREATE TRIGGER fund_nav_stamp_revision AFTER INSERT OR DELETE OR UPDATE "
            "ON nav_timeseries FOR EACH ROW EXECUTE FUNCTION fund_nav_stamp_revision_v1()",
            "O",
            "@schema@.fund_nav_stamp_revision_v1()",
        ]
    ]
    snapshot = manifest["signature"]["functions"][
        "fund_nav_snapshot_current_at_v1(subject_id uuid, selected_run_id uuid, "
        "evaluated_at timestamp with time zone)"
    ]
    assert snapshot["security_definer"] is True
    # N2: trusted target schema first, pg_temp explicitly last, nothing else.
    assert snapshot["config"] == ["search_path=@schema@, pg_temp"]
    assert snapshot["public_execute"] is False
    assert {
        json.dumps(fn["config"]) for fn in manifest["signature"]["functions"].values()
    } == {'["search_path=@schema@, pg_temp"]'}
    assert not any(
        fn["public_execute"] for fn in manifest["signature"]["functions"].values()
    )
    access = manifest["access_profile"]
    assert (access["name"], access["role"]) == ("light_app_runtime_v1", "app_runtime")
    assert access["read_relations"] == list(operator.READ_RELATIONS)
    readable = sorted(
        name for name, rel in access["signature"]["relations"].items() if rel["effective"]
    )
    assert readable == sorted(operator.READ_RELATIONS)
    assert all(
        rel["effective"] in ([], ["SELECT"]) and rel["grantable"] == []
        and rel["public"] == [] and rel["column_acl"] == []
        for rel in access["signature"]["relations"].values()
    )
    assert [
        name for name, fn in access["signature"]["functions"].items() if fn["execute"]
    ] == [
        "fund_nav_snapshot_current_at_v1(subject_id uuid, selected_run_id uuid, "
        "evaluated_at timestamp with time zone)"
    ]
    assert access["signature"]["unsafe_membership"] == []


def test_generator_reproduces_committed_manifest_byte_identical(test_dsn):
    from scripts import generate_fund_nav_readiness_catalog as generator

    committed = (ROOT / "schemas" / "fund_nav_readiness_v1.catalog.json").read_bytes()
    signature, access = generator.fresh_signatures(test_dsn)
    regenerated = generator.manifest_bytes(
        signature, access, (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    )
    assert regenerated == committed


def test_catalog_classifier_rejects_tampered_or_stale_manifest(tmp_path, monkeypatch):
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    manifest = json.loads(operator.CATALOG_MANIFEST.read_text(encoding="utf-8"))
    tampered = dict(manifest)
    tampered["signature"] = {**manifest["signature"], "views": {}}
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(tampered), encoding="utf-8")
    monkeypatch.setattr(operator, "CATALOG_MANIFEST", path)
    with pytest.raises(ValueError, match="catalog_manifest_tampered"):
        operator.load_manifest(ddl)
    with pytest.raises(ValueError, match="catalog_manifest_stale"):
        operator.load_manifest(ddl + b"\n-- edited")


def _catalog_state(conn):
    return conn.execute(
        """SELECT (SELECT count(*) FROM nav_policy_versions),
                  (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
                   JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname=current_schema() AND NOT t.tgisinternal),
                  (SELECT count(*) FROM pg_proc p JOIN pg_namespace n
                   ON n.oid=p.pronamespace WHERE n.nspname=current_schema())"""
    ).fetchone()


@pytest.mark.parametrize(
    "tamper",
    [
        "wrong_ext_trigger_events",
        "extra_ext_trigger",
        "disabled_ext_trigger",
        "foreign_namespace_trigger_function",
        "function_body",
        "function_search_path",
        "function_public_execute",
        "extra_owned_function_overload",
    ],
)
def test_catalog_divergence_is_upgrade_required_without_writes(
    test_dsn, schema, monkeypatch, capsys, tamper
):
    _bootstrap(test_dsn, schema)
    other = schema + "_x"
    statements = {
        "wrong_ext_trigger_events": [
            "DROP TRIGGER fund_nav_stamp_revision ON nav_timeseries",
            "CREATE TRIGGER fund_nav_stamp_revision AFTER INSERT OR UPDATE ON "
            "nav_timeseries FOR EACH ROW EXECUTE FUNCTION fund_nav_stamp_revision_v1()",
        ],
        "extra_ext_trigger": [
            "CREATE TRIGGER zz_extra AFTER DELETE ON nav_timeseries FOR EACH ROW "
            "EXECUTE FUNCTION fund_nav_stamp_revision_v1()",
        ],
        "disabled_ext_trigger": [
            "ALTER TABLE nav_timeseries DISABLE TRIGGER fund_nav_stamp_revision",
        ],
        "foreign_namespace_trigger_function": [
            f'CREATE SCHEMA "{other}"',
            f'CREATE FUNCTION "{other}".fund_nav_stamp_revision_v1() RETURNS trigger '
            "LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$",
            "DROP TRIGGER fund_nav_stamp_revision ON nav_timeseries",
            "CREATE TRIGGER fund_nav_stamp_revision AFTER INSERT OR DELETE OR UPDATE "
            f'ON nav_timeseries FOR EACH ROW EXECUTE FUNCTION "{other}".'
            "fund_nav_stamp_revision_v1()",
        ],
        "function_body": [
            "CREATE OR REPLACE FUNCTION fund_nav_revision_append_only_v1() "
            "RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
        ],
        "function_search_path": [
            "ALTER FUNCTION fund_nav_snapshot_current_at_v1(uuid,uuid,timestamptz) "
            f'SET search_path = "{schema}", public',
        ],
        "function_public_execute": [
            "GRANT EXECUTE ON FUNCTION "
            "fund_nav_snapshot_current_at_v1(uuid,uuid,timestamptz) TO PUBLIC",
        ],
        "extra_owned_function_overload": [
            "CREATE FUNCTION nav_uuid_array_unique_v1(ids text[]) RETURNS boolean "
            "LANGUAGE sql IMMUTABLE AS $$SELECT true$$",
        ],
    }[tamper]
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    base = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    plan = operator.plan_hash(ddl, schema, b"", [], None, None)
    try:
        with _connect(test_dsn, schema, autocommit=True) as conn:
            for statement in statements:
                conn.execute(statement)
            report = operator._check(conn, schema)
            assert (report["status"], report["compatibility"]) == (
                "upgrade_required",
                "incompatible",
            ), report
            before = _catalog_state(conn)
            signature_before = report["catalog_sha256"]
        assert operator.main(base) == operator.EXIT_INCOMPATIBLE
        capsys.readouterr()
        assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == (
            operator.EXIT_INCOMPATIBLE
        )
        assert json.loads(capsys.readouterr().out)["compatibility"] == "incompatible"
        with _connect(test_dsn, schema, autocommit=True) as conn:
            assert _catalog_state(conn) == before
            assert operator._check(conn, schema)["catalog_sha256"] == signature_before
    finally:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(other))
            )


def test_orphan_ext_trigger_without_owned_objects_is_not_absent(test_dsn, schema):
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        conn.execute(
            "CREATE FUNCTION stray() RETURNS trigger LANGUAGE plpgsql "
            "AS $$BEGIN RETURN NULL; END$$"
        )
        conn.execute(
            "CREATE TRIGGER fund_nav_stamp_revision AFTER INSERT ON nav_timeseries "
            "FOR EACH ROW EXECUTE FUNCTION stray()"
        )
        report = operator._check(conn, schema)
        assert (report["status"], report["compatibility"]) == (
            "upgrade_required",
            "incompatible",
        )


def test_homonymous_relations_in_other_schema_do_not_affect_check(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    other = schema + "_homonym"
    try:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(other)))
            conn.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(other))
            )
            conn.execute(NAV_SQL)
            conn.execute("CREATE TABLE fund_nav_risk_runs (x int)")
            conn.execute(
                "CREATE FUNCTION fund_nav_stamp_revision_v1() RETURNS trigger "
                "LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$"
            )
            conn.execute(
                "CREATE TRIGGER fund_nav_stamp_revision AFTER INSERT ON nav_timeseries "
                "FOR EACH ROW EXECUTE FUNCTION fund_nav_stamp_revision_v1()"
            )
            conn.execute(
                "CREATE TRIGGER zz_extra AFTER DELETE ON nav_timeseries "
                "FOR EACH ROW EXECUTE FUNCTION fund_nav_stamp_revision_v1()"
            )
        with _connect(test_dsn, schema, autocommit=True) as conn:
            report = operator._check(conn, schema)
            assert (report["status"], report["compatibility"]) == ("ready", "exact")
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            other_report = operator._check(conn, other)
            assert other_report["compatibility"] == "incompatible"
    finally:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(other))
            )


def test_concurrent_evidence_and_finalization_serialize_on_parent(test_dsn, schema):
    """Two connections: finalize waits for in-flight evidence, then late writes fail."""
    _bootstrap(test_dsn, schema)
    day = dt.date.today() - dt.timedelta(days=1)
    rows = [(day - dt.timedelta(days=n), 100.0 + n) for n in range(30, -1, -1)]
    a, b = uuid.uuid4(), uuid.uuid4()
    run = uuid.uuid4()
    with _connect(test_dsn, schema) as writer, _connect(test_dsn, schema) as finisher:
        _register_risk_run(
            writer, run, day, [a, b], scope="diagnostic", reason="LIMITED_RUN"
        )
        risk._persist_feature_evidence(writer, a, day, rows, 0.04, run, None, {}, {}, [])
        writer.commit()
        risk._record_risk_exclusion(writer, run, b, day, "METRICS_UNAVAILABLE", [])
        # writer holds FOR SHARE on the parent: finalization must wait.
        finisher.execute("SET lock_timeout = '300ms'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            finisher.execute(
                "UPDATE fund_nav_risk_runs SET status='metrics_complete',"
                "persisted_rows=2 WHERE risk_run_id=%s",
                (run,),
            )
        finisher.rollback()
        writer.commit()
        risk._finish_risk_run(finisher, run, 2, 1)
        assert finisher.execute(
            "SELECT status FROM fund_nav_risk_runs WHERE risk_run_id=%s", (run,)
        ).fetchone() == ("metrics_complete",)
        with pytest.raises(psycopg.Error, match="no longer accepting"):
            risk._record_risk_exclusion(writer, run, a, day, "METRICS_UNAVAILABLE", [])
        writer.rollback()
        # Rollback of a failed finalization leaves the parent running and intact.
        second = uuid.uuid4()
        _register_risk_run(
            writer, second, day, [a], scope="diagnostic", reason="LIMITED_RUN"
        )
        writer.commit()
        with pytest.raises(RuntimeError, match="incomplete"):
            risk._finish_risk_run(finisher, second, 1, 0)
        finisher.rollback()
        assert finisher.execute(
            "SELECT status,persisted_rows FROM fund_nav_risk_runs WHERE risk_run_id=%s",
            (second,),
        ).fetchone() == ("running", None)


def test_fresh_install_reapply_is_exact_and_catalog_stable(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        first = operator._check(conn, schema)
        _install(conn, schema)
        second = operator._check(conn, schema)
    assert first["compatibility"] == second["compatibility"] == "exact"
    assert first["catalog_sha256"] == second["catalog_sha256"] == (
        first["reference_catalog_sha256"]
    )


def test_identity_and_lineage_lookups_are_index_backed(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    run, iid = uuid.uuid4(), uuid.uuid4()
    with _connect(test_dsn, schema) as conn:
        conn.execute("SET LOCAL enable_seqscan = off")
        # Each lookup must be served by one of the contract indexes (the planner
        # may pick either covering index on an empty table); never a Seq Scan.
        for statement, indexes in (
            (sql.SQL(
                "SELECT * FROM fund_nav_feature_evidence WHERE risk_run_id={} "
                "AND instrument_id={}").format(sql.Literal(run), sql.Literal(iid)),
             ("fund_nav_feature_evidence_pkey", "fund_nav_feature_evidence_identity_idx")),
            (sql.SQL(
                "SELECT * FROM fund_nav_feature_evidence WHERE instrument_id={} "
                "AND calc_date<=DATE '2026-09-01' AND definition_version='x' "
                "ORDER BY calc_date DESC").format(sql.Literal(iid)),
             ("fund_nav_feature_evidence_identity_idx",)),
            (sql.SQL(
                "SELECT * FROM fund_nav_data_revisions WHERE instrument_id={} AND "
                "nav_date BETWEEN DATE '2025-01-01' AND DATE '2026-09-01'"
             ).format(sql.Literal(iid)),
             ("fund_nav_data_revisions_window_idx",)),
            (sql.SQL(
                "SELECT * FROM fund_nav_risk_run_members WHERE risk_run_id={}"
             ).format(sql.Literal(run)),
             ("fund_nav_risk_run_members_pkey",)),
        ):
            plan = "\n".join(
                row[0] for row in conn.execute(sql.SQL("EXPLAIN ") + statement)
            )
            assert "Seq Scan" not in plan, plan
            assert any(index in plan for index in indexes), plan


# ──────────────────────────────────────────────────────────────────────────────
# Phase B: W3 eligibility/invalidation/CAS through the real risk worker, W2
# busy/exit contract and W5 same-date evidence, on PostgreSQL 18 / Timescale.
# Benchmark/macro/peer/manager inputs need unrelated catalogue tables and are
# replaced by neutral values; NAV, metrics math, persistence, MV and
# publication are real.
# ──────────────────────────────────────────────────────────────────────────────
import dataclasses  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402

from src.db import (  # noqa: E402
    LOCK_FUND_NAV_CURRENT_CHAIN,
    LOCK_RISK_METRICS,
)
from src.workers import analytics_refresh_chain  # noqa: E402

RISK_SQL = (ROOT / "schemas" / "risk_metrics.sql").read_text(encoding="utf-8")
RISK_READ_MODEL_SQL = RISK_SQL[
    RISK_SQL.index("CREATE TABLE IF NOT EXISTS fund_risk_metrics") : RISK_SQL.index(
        "CREATE MATERIALIZED VIEW funds_list_mv"
    )
]


def _schema_dsn(dsn, schema):
    return psycopg.conninfo.make_conninfo(dsn, options=f"-csearch_path={schema},public")


def _offline_risk_inputs(monkeypatch):
    for name, value in {
        "_risk_free_rate": lambda _c, _d: 0.04,
        "_fetch_benchmark_returns": lambda _c, _d: {},
        "_fetch_fund_benchmarks": lambda _c: {},
        "_fetch_macro_changes": lambda _c, _d: {},
        "_fetch_fund_asset_classes": lambda _c: {},
        "_update_peer_percentiles": lambda _c, _d: 0,
        "_update_manager_scores": lambda _c, _d: 0,
    }.items():
        monkeypatch.setattr(risk, name, value)


def _add_fund(conn, _ing_run, dates, *, base, step):
    fid = uuid.uuid4()
    observations = tuple(
        NavObservation(d, round(base + i * step + (i % 5) * 0.07, 6), "adjusted")
        for i, d in enumerate(dates)
    )
    _provider_write(conn, ingest.build_rows(observations, [(fid, "USD")]))
    return fid


def _risk_env(test_dsn, schema, monkeypatch, *, extra=2, stale=1, **seed):
    """Seeded policy/NAV/fixture publication plus the real risk read model."""
    iid, grid, ing_run = _seed(test_dsn, schema, **seed)
    with _connect(test_dsn, schema) as conn:
        extras = [
            _add_fund(conn, ing_run, grid[-300:], base=50.0 + n, step=0.02 + 0.01 * n)
            for n in range(extra)
        ]
        old = [grid[0] - dt.timedelta(days=12 * 366 + k) for k in range(30, 0, -1)]
        stales = [_add_fund(conn, ing_run, old, base=10.0, step=0.01) for _ in range(stale)]
        conn.commit()
    _offline_risk_inputs(monkeypatch)
    return iid, grid, [iid, *extras], stales, _schema_dsn(test_dsn, schema)


def _singleton(conn):
    return conn.execute(
        "SELECT revision_id, state, active_risk_run_id, published_risk_run_id "
        "FROM fund_nav_risk_publication"
    ).fetchone()


def _run_row(conn, run_id):
    return conn.execute(
        "SELECT run_scope, nonpublishing_reason, status, due_session, requested_limit,"
        " expected_rows, universe_digest FROM fund_nav_risk_runs WHERE risk_run_id=%s",
        (uuid.UUID(str(run_id)),),
    ).fetchone()


def _publication(result):
    return result["risk_publication"]


_RESULT_KEYS = {
    "processed", "upserted", "calc_date", "workers", "risk_run_id",
    "mv_refreshed", "risk_publication",
}
_PUBLICATION_KEYS = {
    "eligible", "published", "reason", "risk_run_id", "as_of_session", "retryable",
}


def _assert_shape(result):
    assert _RESULT_KEYS <= set(result)
    assert set(_publication(result)) == _PUBLICATION_KEYS


def test_full_due_run_invalidates_before_first_write_then_publishes(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    assert readiness.run(dsn)["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        before = _singleton(conn)
    observed = []
    real_upsert = risk._upsert

    def _observing_upsert(conn, instrument_id, calc_date, metrics):
        if not observed:
            with _connect(test_dsn, schema) as reader:
                observed.append(
                    (
                        _singleton(reader),
                        reader.execute(
                            "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
                        ).fetchone()[0],
                        reader.execute(
                            "SELECT count(*) FROM fund_nav_risk_runs WHERE status='running'"
                        ).fetchone()[0],
                    )
                )
        return real_upsert(conn, instrument_id, calc_date, metrics)

    monkeypatch.setattr(risk, "_upsert", _observing_upsert)
    result = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    _assert_shape(result)
    run_id = uuid.UUID(result["risk_run_id"])
    # Before the first metric write another connection already saw the
    # committed invalidation: revision+1, running, active=this, pointer NULL.
    (revision, state, active, published), snapshot_current, running = observed[0]
    assert (revision, state, active, published) == (before[0] + 1, "running", run_id, None)
    assert snapshot_current is False and running == 1
    assert result["mv_refreshed"] is True
    assert _publication(result) == {
        "eligible": True,
        "published": True,
        "reason": None,
        "risk_run_id": str(run_id),
        "as_of_session": grid[-1].isoformat(),
        "retryable": False,
    }
    assert (result["upserted"], result["processed"]) == (len(funds), len(funds))
    with _connect(test_dsn, schema) as conn:
        assert _singleton(conn) == (before[0] + 2, "idle", None, run_id)
        scope, reason, status, due, limit, expected, digest = _run_row(conn, run_id)
        assert (scope, reason, status, due, limit) == (
            "current_full", None, "complete", grid[-1], None
        )
        assert expected == len(funds) + len(stales)
        assert digest == risk_universe_digest([*funds, *stales])
        assert conn.execute(
            "SELECT (SELECT count(*) FROM fund_nav_feature_evidence WHERE risk_run_id=%s),"
            "(SELECT array_agg(instrument_id) FROM fund_nav_risk_exclusions "
            " WHERE risk_run_id=%s AND reason_code='NAV_WINDOW_TOO_SHORT'),"
            "(SELECT count(*) FROM fund_risk_latest_mv WHERE calc_date=%s)",
            (run_id, run_id, grid[-1]),
        ).fetchone() == (len(funds), stales, len(funds))
    snapshot = readiness.run(dsn)
    assert snapshot["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT snapshot_current, risk_run_id FROM fund_nav_readiness_current_v1 "
            "WHERE instrument_id=%s",
            (iid,),
        ).fetchone() == (True, run_id)


@pytest.mark.parametrize("limit", [1, 50])
def test_limited_runs_are_diagnostic_invalidate_and_never_promote(
    test_dsn, schema, monkeypatch, limit
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    universe = [*funds, *stales]
    assert limit == 1 or limit >= len(universe)
    with _connect(test_dsn, schema) as conn:
        before = _singleton(conn)
    result = risk.run(dsn, calc_date=grid[-1].isoformat(), limit=limit, serial=True)
    _assert_shape(result)
    assert result["mv_refreshed"] is True
    assert _publication(result) == {
        "eligible": False,
        "published": False,
        "reason": "LIMITED_RUN",
        "risk_run_id": result["risk_run_id"],
        "as_of_session": grid[-1].isoformat(),
        "retryable": False,
    }
    with _connect(test_dsn, schema) as conn:
        scope, reason, status, due, requested, expected, digest = _run_row(
            conn, result["risk_run_id"]
        )
        assert (scope, reason, status, requested) == (
            "diagnostic", "LIMITED_RUN", "complete", limit
        )
        assert expected == min(limit, len(universe))
        # The frozen universe is the canonical pre-limit set, not the members.
        assert digest == risk_universe_digest(universe)
        # Same date as the served generation: invalidated before any write,
        # never replaced by the diagnostic run.
        assert _singleton(conn) == (before[0] + 1, "idle", None, None)
    with pytest.raises(RuntimeError, match="risk run not published"):
        readiness.run(dsn)


@pytest.mark.parametrize(
    ("case", "reason", "as_of"),
    [
        ("historical", "NON_CURRENT_SESSION", True),
        ("future", "NON_CURRENT_SESSION", True),
        ("policy_absent", "POLICY_UNAVAILABLE", False),
        ("policy_expired", "POLICY_EXPIRED", False),
    ],
)
def test_non_current_or_policy_less_runs_never_promote(
    test_dsn, schema, monkeypatch, case, reason, as_of
):
    seed = {
        "policy_absent": {"with_policy": False},
        # An expired policy cannot stamp new rows (A trigger), so NAV is unstamped.
        "policy_expired": {"valid_for": -dt.timedelta(minutes=1), "stamp": False},
    }.get(case, {})
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch, **seed)
    calc = {
        "historical": grid[-2],
        "future": grid[-1] + dt.timedelta(days=1),
    }.get(case, grid[-1])
    with _connect(test_dsn, schema) as conn:
        before = _singleton(conn)
    result = risk.run(dsn, calc_date=calc.isoformat(), serial=True)
    _assert_shape(result)
    publication = _publication(result)
    assert (publication["eligible"], publication["published"], publication["reason"]) == (
        False, False, reason
    )
    assert publication["retryable"] is False
    assert publication["as_of_session"] == (grid[-1].isoformat() if as_of else None)
    assert result["mv_refreshed"] is True
    with _connect(test_dsn, schema) as conn:
        scope, stored_reason, status, due, *_ = _run_row(conn, result["risk_run_id"])
        assert (scope, stored_reason, status) == ("diagnostic", reason, "complete")
        pins = conn.execute(
            "SELECT policy_version FROM fund_nav_risk_runs WHERE risk_run_id=%s",
            (uuid.UUID(result["risk_run_id"]),),
        ).fetchone()[0]
        assert pins == (None if case == "policy_absent" else "v1")
        after = _singleton(conn)
        assert after[3] != uuid.UUID(result["risk_run_id"])
        assert after[:4] == (before[0] + 1, "idle", None, None)


def test_strictly_historical_inert_diagnostic_preserves_pointer_else_invalidates(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch, stale=0)
    full = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert _publication(full)["published"] is True
    assert readiness.run(dsn)["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        served = _singleton(conn)
    historical = risk.run(dsn, calc_date=grid[-2].isoformat(), serial=True)
    assert _publication(historical)["reason"] == "NON_CURRENT_SESSION"
    with _connect(test_dsn, schema) as conn:
        # Every member has a newer row and no fund is served at grid[-2].
        assert _singleton(conn) == served
        assert conn.execute(
            "SELECT bool_and(snapshot_current) FROM fund_nav_readiness_current_v1"
        ).fetchone()[0] is True
        # A fund without any newer row makes the same historical write impactful.
        ing_run = conn.execute("SELECT run_id FROM nav_ingestion_runs LIMIT 1").fetchone()[0]
        _add_fund(conn, ing_run, grid[-40:-1], base=70.0, step=0.03)
        conn.commit()
    impactful = risk.run(dsn, calc_date=grid[-2].isoformat(), serial=True)
    assert _publication(impactful)["published"] is False
    with _connect(test_dsn, schema) as conn:
        assert _singleton(conn) == (served[0] + 1, "idle", None, None)
        assert conn.execute(
            "SELECT bool_or(snapshot_current) FROM fund_nav_readiness_current_v1"
        ).fetchone()[0] is False


def test_shard_failure_keeps_invalidation_and_next_full_run_replaces(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        before = _singleton(conn)
    calls = []
    real_upsert = risk._upsert

    def _failing_upsert(*args):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("shard crashed")
        return real_upsert(*args)

    monkeypatch.setattr(risk, "_upsert", _failing_upsert)
    with pytest.raises(RuntimeError, match="shard crashed"):
        risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    with _connect(test_dsn, schema) as conn:
        revision, state, active, published = _singleton(conn)
        failed = conn.execute(
            "SELECT risk_run_id, status FROM fund_nav_risk_runs "
            "WHERE risk_run_id=%s",
            (active,),
        ).fetchone()
        # No pointer restore: the old published run is not current again.
        assert (revision, state, published) == (before[0] + 1, "running", None)
        assert failed[1] == "running"
        assert conn.execute(
            "SELECT count(*) FROM fund_nav_feature_evidence WHERE risk_run_id=%s",
            (active,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT pg_try_advisory_lock(%s)", (LOCK_RISK_METRICS,)
        ).fetchone()[0] is True  # released after the failure
        conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_RISK_METRICS,))
    with pytest.raises(RuntimeError, match="risk run not published"):
        readiness.run(dsn)
    monkeypatch.setattr(risk, "_upsert", real_upsert)
    retry = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert _publication(retry)["published"] is True
    assert retry["risk_run_id"] != str(active)
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT status FROM fund_nav_risk_runs WHERE risk_run_id=%s", (active,)
        ).fetchone()[0] == "running"  # interrupted run stays explicitly incomplete


def test_mv_operational_failure_is_typed_and_programming_error_propagates(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        before = _singleton(conn)
        conn.execute("DROP INDEX fund_risk_latest_mv_pk")  # CONCURRENTLY now impossible
    result = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    _assert_shape(result)
    assert result["mv_refreshed"] is False
    assert result["mv_refresh_error"] == "ObjectNotInPrerequisiteState"
    assert _publication(result) == {
        "eligible": True,
        "published": False,
        "reason": "MV_REFRESH_FAILED",
        "risk_run_id": result["risk_run_id"],
        "as_of_session": grid[-1].isoformat(),
        "retryable": True,
    }
    run_id = uuid.UUID(result["risk_run_id"])
    with _connect(test_dsn, schema, autocommit=True) as conn:
        assert _run_row(conn, run_id)[2] == "metrics_complete"
        assert _singleton(conn) == (before[0] + 1, "running", run_id, None)
        conn.execute("DROP MATERIALIZED VIEW fund_risk_latest_mv")
    with pytest.raises(psycopg.errors.UndefinedTable):
        risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    with _connect(test_dsn, schema) as conn:
        revision, state, active, published = _singleton(conn)
        assert (state, published) == ("running", None) and active != run_id


def test_generation_lock_is_held_through_mv_refresh_and_blocks_second_generation(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    real_refresh = risk._refresh_fund_risk_latest_mv
    seen = {}

    def _barrier_refresh(refresh_dsn):
        with _connect(test_dsn, schema) as probe:
            seen["risk_lock_free"] = probe.execute(
                "SELECT pg_try_advisory_lock(%s)", (LOCK_RISK_METRICS,)
            ).fetchone()[0]
            seen["runs_before"] = probe.execute(
                "SELECT count(*) FROM fund_nav_risk_runs"
            ).fetchone()[0]
        seen["second"] = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
        with _connect(test_dsn, schema) as probe:
            seen["runs_after"] = probe.execute(
                "SELECT count(*) FROM fund_nav_risk_runs"
            ).fetchone()[0]
        with pytest.raises(RuntimeError, match="risk run not published"):
            readiness.run(dsn)
        real_refresh(refresh_dsn)

    monkeypatch.setattr(risk, "_refresh_fund_risk_latest_mv", _barrier_refresh)
    result = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert seen["risk_lock_free"] is False
    assert seen["second"]["skipped"] == "lock_busy"
    assert _publication(seen["second"])["reason"] == "LOCK_BUSY"
    assert seen["runs_after"] == seen["runs_before"]
    assert _publication(result)["published"] is True


def test_readiness_lock_busy_at_registration_writes_nothing(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        before = (
            _singleton(conn),
            conn.execute("SELECT count(*) FROM fund_nav_risk_runs").fetchone()[0],
            conn.execute("SELECT count(*) FROM fund_risk_metrics").fetchone()[0],
        )
    with _connect(test_dsn, schema) as holder:
        with advisory_lock(holder, LOCK_FUND_NAV_READINESS) as held:
            assert held
            result = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert result == risk._busy_result()
    with _connect(test_dsn, schema) as conn:
        assert (
            _singleton(conn),
            conn.execute("SELECT count(*) FROM fund_nav_risk_runs").fetchone()[0],
            conn.execute("SELECT count(*) FROM fund_risk_metrics").fetchone()[0],
        ) == before


def _shift_clock(monkeypatch, delta):
    real = risk.resolve_policy_and_grid
    monkeypatch.setattr(
        risk, "resolve_policy_and_grid", lambda conn, now: real(conn, now + delta)
    )


@pytest.mark.parametrize(
    "interference", ["busy", "superseded", "policy_rollover", "clock_due", "clock_expired"]
)
def test_publication_cas_revalidation_never_publishes_on_change(
    test_dsn, schema, monkeypatch, interference
):
    iid, grid, funds, stales, dsn = _risk_env(
        test_dsn, schema, monkeypatch, extra_closed=interference == "clock_due"
    )
    real_refresh = risk._refresh_fund_risk_latest_mv
    holder = _connect(test_dsn, schema)

    def _interfering_refresh(refresh_dsn):
        real_refresh(refresh_dsn)
        if interference == "busy":
            holder.execute("SELECT pg_advisory_lock(%s)", (LOCK_FUND_NAV_READINESS,))
        elif interference == "superseded":
            holder.execute(
                "UPDATE fund_nav_risk_publication SET revision_id=revision_id+1"
            )
            holder.commit()
        elif interference == "policy_rollover":
            _publish_rollover(holder, iid, grid)
        elif interference == "clock_due":
            _shift_clock(monkeypatch, dt.timedelta(hours=2))
        else:
            _shift_clock(monkeypatch, dt.timedelta(days=2))

    monkeypatch.setattr(risk, "_refresh_fund_risk_latest_mv", _interfering_refresh)
    try:
        result = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    finally:
        holder.close()  # also releases the busy session lock
    expected = {
        "busy": "LOCK_BUSY",
        "superseded": "SUPERSEDED",
        "policy_rollover": "POLICY_CHANGED",
        "clock_due": "DUE_SESSION_CHANGED",
        "clock_expired": "POLICY_CHANGED",
    }[interference]
    publication = _publication(result)
    assert result["mv_refreshed"] is True
    assert (publication["eligible"], publication["published"]) == (True, False)
    assert (publication["reason"], publication["retryable"]) == (expected, True)
    run_id = uuid.UUID(result["risk_run_id"])
    with _connect(test_dsn, schema) as conn:
        revision, state, active, published = _singleton(conn)
        assert published is None
        assert _run_row(conn, run_id)[2] == "complete"  # computed, never promoted
        if interference == "busy":
            assert (state, active) == ("running", run_id)  # left invalidated
        elif interference == "superseded":
            assert (state, active) == ("running", run_id)  # other owner untouched
        else:
            assert (state, active) == ("idle", None)  # own claim released


def test_same_date_worker_runs_keep_both_evidences_and_snapshots_join_their_run(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    first = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    snapshot_one = readiness.run(dsn)
    second = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    snapshot_two = readiness.run(dsn)
    assert _publication(first)["published"] and _publication(second)["published"]
    with _connect(test_dsn, schema) as conn:
        counts = conn.execute(
            "SELECT risk_run_id, count(*) FROM fund_nav_feature_evidence "
            "WHERE calc_date=%s AND risk_run_id = ANY(%s) GROUP BY 1",
            (grid[-1], [uuid.UUID(first["risk_run_id"]), uuid.UUID(second["risk_run_id"])]),
        ).fetchall()
        assert sorted(n for _run, n in counts) == [len(funds), len(funds)]
        joined = conn.execute(
            "SELECT r.run_id::text, f.risk_run_id::text FROM fund_nav_readiness_v1 r "
            "JOIN fund_nav_feature_evidence f ON f.risk_run_id=r.risk_run_id "
            "AND f.instrument_id=r.instrument_id WHERE r.instrument_id=%s "
            "AND r.run_id = ANY(%s)",
            (iid, [uuid.UUID(snapshot_one["run_id"]), uuid.UUID(snapshot_two["run_id"])]),
        ).fetchall()
        assert sorted(joined) == sorted(
            [
                (snapshot_one["run_id"], first["risk_run_id"]),
                (snapshot_two["run_id"], second["risk_run_id"]),
            ]
        )


def test_serial_and_sharded_runs_are_equivalent(test_dsn, schema, monkeypatch):
    iid, grid, funds, stales, dsn = _risk_env(
        test_dsn, schema, monkeypatch, extra=3, stale=1
    )
    monkeypatch.setattr(risk, "_resolve_max_workers", lambda: 2)

    def _snapshot(run_id):
        with _connect(test_dsn, schema) as conn:
            metrics = conn.execute(
                "SELECT to_jsonb(m) FROM fund_risk_metrics m WHERE calc_date=%s "
                "ORDER BY instrument_id",
                (grid[-1],),
            ).fetchall()
            features = conn.execute(
                "SELECT instrument_id, input_fingerprint, nav_input_fingerprint, nav_count "
                "FROM fund_nav_feature_evidence WHERE risk_run_id=%s ORDER BY 1",
                (uuid.UUID(run_id),),
            ).fetchall()
            exclusions = conn.execute(
                "SELECT instrument_id, reason_code, nav_count FROM fund_nav_risk_exclusions "
                "WHERE risk_run_id=%s ORDER BY 1",
                (uuid.UUID(run_id),),
            ).fetchall()
        return metrics, features, exclusions

    serial = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    serial_state = _snapshot(serial["risk_run_id"])
    sharded = risk.run(dsn, calc_date=grid[-1].isoformat())
    sharded_state = _snapshot(sharded["risk_run_id"])
    assert (serial["workers"], sharded["workers"]) == (1, 2)
    assert serial_state == sharded_state
    assert len(serial_state[1]) == len(funds) and len(serial_state[2]) == len(stales)
    for result in (serial, sharded):
        assert (result["processed"], result["upserted"]) == (len(funds), len(funds))
        assert _publication(result)["published"] is True


def test_mv_success_with_unpublished_nav_keeps_analytics_green_and_nav_chain_blocked(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    limited = risk.run(dsn, calc_date=grid[-1].isoformat(), limit=1, serial=True)
    assert limited["mv_refreshed"] is True
    assert _publication(limited)["published"] is False
    analytics = analytics_refresh_chain.run(
        dsn,
        risk_runner=lambda *_a, **_k: limited,
        momentum_runner=lambda *_a, **kw: {"calc_date": kw["calc_date"], "upserted": 1},
    )
    assert analytics["published"] is True
    monkeypatch.setattr(
        chain.matview_refresh, "_refresh_all", lambda *_a: ["fund_nav_coverage_mv"]
    )
    readiness_calls = []
    nav = chain.run(
        dsn,
        ingestion_runner=lambda *_a, **_k: {"ingestion_run_id": "stub"},
        risk_runner=lambda *_a, **_k: limited,
        readiness_runner=lambda *_a: readiness_calls.append(1),
    )
    assert nav == {
        "status": "blocked",
        "state": "blocked",
        "published": False,
        "retryable": False,
        "reason": "LIMITED_RUN",
        "blocked_stage": "risk_metrics",
    }
    assert readiness_calls == []


def test_finish_rejects_persisted_count_that_differs_from_features(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    day = dt.date.today() - dt.timedelta(days=1)
    rows = [(day - dt.timedelta(days=n), 100.0 + n) for n in range(30, -1, -1)]
    a, b = uuid.uuid4(), uuid.uuid4()
    run = uuid.uuid4()
    with _connect(test_dsn, schema) as conn:
        _register_risk_run(conn, run, day, [a, b], scope="diagnostic", reason="LIMITED_RUN")
        risk._persist_feature_evidence(conn, a, day, rows, 0.04, run, None, {}, {}, [])
        risk._record_risk_exclusion(conn, run, b, day, "METRICS_UNAVAILABLE", [])
        conn.commit()
        for persisted in (0, 2):
            with pytest.raises(RuntimeError, match="incomplete"):
                risk._finish_risk_run(conn, run, 2, persisted)
            conn.rollback()
        with pytest.raises(RuntimeError, match="incomplete"):
            risk._finish_risk_run(conn, run, 3, 1)
        conn.rollback()
        risk._finish_risk_run(conn, run, 2, 1)


def test_diagnostic_plan_never_calls_publication_lock(test_dsn, schema, monkeypatch):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        plan, token = risk._begin_risk_generation(conn, grid[-1].isoformat(), 1)
        assert (plan.eligible, token) == (False, None)
        with _connect(test_dsn, schema) as holder:
            with advisory_lock(holder, LOCK_FUND_NAV_READINESS):
                decision = risk._mark_risk_published(conn, plan, token)
        assert decision["reason"] == "LIMITED_RUN" and decision["published"] is False
        eligible, token = risk._begin_risk_generation(conn, grid[-1].isoformat(), None)
        forged = dataclasses.replace(eligible, run_scope="current_full", risk_run_id=plan.risk_run_id)
        risk._record_risk_exclusion(conn, plan.risk_run_id, plan.members[0], grid[-1],
                                    "METRICS_UNAVAILABLE", [])
        conn.commit()
        risk._finish_risk_run(conn, plan.risk_run_id, 1, 0)
        risk._complete_risk_run(conn, plan.risk_run_id)
        # A diagnostic run can never win the CAS even with a stolen token.
        assert risk._mark_risk_published(conn, forged, token)["reason"] == "SUPERSEDED"


def _runner(worker, dsn, secret):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DB_TLS_", "WORKER"))
    }
    env.update(WORKER=worker, DATABASE_URL=dsn, PYTHONDONTWRITEBYTECODE="1")
    flags = ["-s"] if sys.flags.no_user_site else []
    proc = subprocess.run(
        [sys.executable, *flags, "-m", "src.run_worker"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert secret not in proc.stdout + proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.startswith("{")]
    assert len(lines) == 1, (proc.stdout[-2000:], proc.stderr[-2000:])
    return proc.returncode, json.loads(lines[0])


def test_runner_end_to_end_for_readiness_and_chain_lanes(test_dsn, schema):
    _seed(test_dsn, schema)
    dsn = _schema_dsn(test_dsn, schema)
    secret = psycopg.conninfo.conninfo_to_dict(test_dsn).get("password") or "<none>"
    busy = {"status": "lock_busy", "state": "lock_busy", "published": False,
            "retryable": True}
    with _connect(test_dsn, schema) as holder:
        with advisory_lock(holder, LOCK_FUND_NAV_READINESS) as held:
            assert held
            code, payload = _runner("fund_nav_readiness", dsn, secret)
    assert code == 1 and payload == {"worker": "fund_nav_readiness", **busy}
    code, payload = _runner("fund_nav_readiness", dsn, secret)
    assert code == 0
    assert (payload["state"], payload["published"], payload["ready_count"]) == (
        "complete", True, 1
    )
    with _connect(test_dsn, schema) as holder:
        with advisory_lock(holder, LOCK_FUND_NAV_CURRENT_CHAIN) as held:
            assert held
            code, payload = _runner("nav_current_daily_chain", dsn, secret)
    assert code == 1
    assert payload == {
        "worker": "nav_current_daily_chain",
        **busy,
        "blocked_stage": "nav_current_daily_chain",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Finding 1 (final review): the read model must serve the publishing generation.
# ──────────────────────────────────────────────────────────────────────────────
# Light-equivalent read model: latest global row per fund with every base
# column (the Light full MV projects risk columns from the same latest row).
LIGHT_EQUIVALENT_MV_SQL = """
DROP MATERIALIZED VIEW fund_risk_latest_mv;
CREATE MATERIALIZED VIEW fund_risk_latest_mv AS
WITH latest_global AS (
    SELECT DISTINCT ON (instrument_id) *
    FROM fund_risk_metrics
    WHERE organization_id IS NULL
    ORDER BY instrument_id, calc_date DESC
)
SELECT * FROM latest_global;
CREATE UNIQUE INDEX fund_risk_latest_mv_pk ON fund_risk_latest_mv (instrument_id);
"""


def _capture_correspondence(monkeypatch):
    seen = []
    real = risk._mv_correspondence

    def _capturing(cur, plan):
        result = real(cur, plan)
        seen.append(result)
        return result

    monkeypatch.setattr(risk, "_mv_correspondence", _capturing)
    return seen


def _publish_policy_with_due(conn, version, sessions):
    """Publish ``version`` as current with explicit (date, close, due) sessions."""
    rows = [(day, close, due, SOURCE) for day, close, due in sessions]
    conn.execute(
        """INSERT INTO nav_policy_versions
           (policy_id,policy_version,policy_hash,readiness_profile,
            valuation_frequency,calendar_id,calendar_version,calendar_source,
            timezone,coverage_start,coverage_end,valid_through,
            calendar_session_count,calendar_digest,sample_intervals,
            annualization_sessions,required_nav_kind,required_return_semantics,
            modeling_currency,currency_treatment,source_reference,published_at)
           VALUES ('synthetic',%s,%s,'current_daily_nav_v1','daily','NYSE-TEST',%s,
                   %s,'America/New_York',%s,%s,clock_timestamp()+interval '1 day',
                   %s,%s,400,252,'adjusted','observed_interval_log_ratio','USD',
                   'native_only',%s,NULL)""",
        (version, "c" * 64, version, SOURCE, rows[0][0], rows[-1][0], len(rows),
         calendar_digest(rows), SOURCE),
    )
    for day, close, due, _ in rows:
        conn.execute(
            "INSERT INTO nav_valuation_schedules VALUES ('NYSE-TEST',%s,%s,%s,%s,%s,%s)",
            (version, day, close, due, SOURCE, SOURCE),
        )
    conn.execute(
        "UPDATE nav_policy_versions SET published_at=clock_timestamp() "
        "WHERE policy_version=%s",
        (version,),
    )
    conn.execute(
        "UPDATE nav_policy_current SET policy_version=%s,published_at=clock_timestamp()",
        (version,),
    )
    conn.commit()


def _served_by_published_run(conn):
    """Light-equivalent current read: MV rows backed by the published run's features."""
    return conn.execute(
        """SELECT count(*) FROM fund_risk_latest_mv mv
           JOIN fund_nav_risk_publication pub
             ON pub.readiness_profile = 'current_daily_nav_v1'
           JOIN fund_nav_feature_evidence f
             ON f.risk_run_id = pub.published_risk_run_id
            AND f.instrument_id = mv.instrument_id AND f.calc_date = mv.calc_date"""
    ).fetchone()[0]


@pytest.mark.parametrize("mv_shape", ["worker", "light_full"])
def test_future_diagnostic_served_by_mv_blocks_current_publication_until_recovery(
    test_dsn, schema, monkeypatch, mv_shape
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    if mv_shape == "light_full":
        with _connect(test_dsn, schema, autocommit=True) as conn:
            conn.execute(LIGHT_EQUIVALENT_MV_SQL)
    seen = _capture_correspondence(monkeypatch)
    future_day = grid[-1] + dt.timedelta(days=1)

    future = risk.run(dsn, calc_date=future_day.isoformat(), serial=True)
    assert _publication(future)["reason"] == "NON_CURRENT_SESSION"
    assert future["mv_refreshed"] is True and seen == []  # diagnostic: no CAS path
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT array_agg(DISTINCT calc_date) FROM fund_risk_latest_mv"
        ).fetchone()[0] == [future_day]

    current = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert current["mv_refreshed"] is True
    assert _publication(current) == {
        "eligible": True,
        "published": False,
        "reason": "MV_RUN_MISMATCH",
        "risk_run_id": current["risk_run_id"],
        "as_of_session": grid[-1].isoformat(),
        "retryable": True,
    }
    assert seen[-1]["features"] == len(funds)
    assert seen[-1]["date_mismatch"] == len(funds)  # MV still serves the future rows
    assert seen[-1]["corresponding"] == 0
    current_run = uuid.UUID(current["risk_run_id"])
    with _connect(test_dsn, schema) as conn:
        revision, state, active, published = _singleton(conn)
        assert (state, active, published) == ("idle", None, None)  # claim released
        assert _run_row(conn, current_run)[2] == "complete"  # computed, never promoted
        assert conn.execute(
            "SELECT count(*) FROM fund_risk_metrics WHERE calc_date=%s", (grid[-1],)
        ).fetchone()[0] == len(funds)
        assert _served_by_published_run(conn) == 0
    with pytest.raises(RuntimeError, match="risk run not published"):
        readiness.run(dsn)
    monkeypatch.setattr(
        chain.matview_refresh, "_refresh_all", lambda *_a: ["fund_nav_coverage_mv"]
    )
    assert chain.run(
        dsn,
        ingestion_runner=lambda *_a, **_k: {"ingestion_run_id": "stub"},
        risk_runner=lambda *_a, **_k: current,
        readiness_runner=lambda *_a: pytest.fail("readiness must not run"),
    ) == {
        "status": "blocked",
        "state": "blocked",
        "published": False,
        "retryable": True,
        "reason": "MV_RUN_MISMATCH",
        "blocked_stage": "risk_metrics",
    }
    assert analytics_refresh_chain.run(
        dsn,
        risk_runner=lambda *_a, **_k: current,
        momentum_runner=lambda *_a, **kw: {"calc_date": kw["calc_date"], "upserted": 1},
    )["published"] is True

    # Governed recovery: the calendar reaches the future date as its due
    # session. The next full run at that session supersedes the future
    # diagnostic rows through the normal upsert; nothing is deleted.
    now = dt.datetime.now(dt.timezone.utc)
    sessions = [
        (
            day,
            dt.datetime.combine(day, dt.time(20), dt.timezone.utc),
            dt.datetime.combine(day, dt.time(23), dt.timezone.utc),
        )
        for day in grid[1:]
    ] + [(future_day, now - dt.timedelta(hours=2), now - dt.timedelta(hours=1))]
    with _connect(test_dsn, schema) as conn:
        _publish_policy_with_due(conn, "v2", sessions)
    recovered = risk.run(dsn, calc_date=future_day.isoformat(), serial=True)
    assert _publication(recovered) == {
        "eligible": True,
        "published": True,
        "reason": None,
        "risk_run_id": recovered["risk_run_id"],
        "as_of_session": future_day.isoformat(),
        "retryable": False,
    }
    assert seen[-1]["corresponding"] == seen[-1]["features"] == len(funds)
    recovered_run = uuid.UUID(recovered["risk_run_id"])
    with _connect(test_dsn, schema) as conn:
        assert _singleton(conn)[1:] == ("idle", None, recovered_run)
        assert _served_by_published_run(conn) == len(funds)
        # Every generation keeps its append-only evidence.
        assert conn.execute(
            "SELECT risk_run_id, count(*) FROM fund_nav_feature_evidence "
            "WHERE risk_run_id = ANY(%s) GROUP BY 1 ORDER BY 1",
            ([uuid.UUID(future["risk_run_id"]), current_run, recovered_run],),
        ).fetchall() == sorted(
            [
                (uuid.UUID(future["risk_run_id"]), len(funds)),
                (current_run, len(funds)),
                (recovered_run, len(funds)),
            ]
        )
        assert conn.execute(
            "SELECT count(*) FROM fund_nav_risk_exclusions WHERE risk_run_id=%s",
            (recovered_run,),
        ).fetchone()[0] == len(stales)


def test_missing_or_stale_mv_content_is_mismatch_even_with_matching_date(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    seen = _capture_correspondence(monkeypatch)
    real_refresh = risk._refresh_fund_risk_latest_mv
    monkeypatch.setattr(risk, "_refresh_fund_risk_latest_mv", lambda _dsn: None)
    never_refreshed = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert _publication(never_refreshed)["reason"] == "MV_RUN_MISMATCH"
    assert (seen[-1]["corresponding"], seen[-1]["date_mismatch"]) == (0, len(funds))

    monkeypatch.setattr(risk, "_refresh_fund_risk_latest_mv", real_refresh)
    first = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert _publication(first)["published"] is True
    second = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)  # same date
    assert _publication(second)["published"] is True
    assert seen[-1]["corresponding"] == seen[-1]["features"] == len(funds)

    # Same calc_date in the MV, but its content predates this generation.
    changed = funds[1]
    with _connect(test_dsn, schema) as conn:
        _provider_write(
            conn,
            ingest.build_rows(
                (NavObservation(grid[-1], 175.0, "adjusted"),), [(changed, "USD")]
            ),
        )
    monkeypatch.setattr(risk, "_refresh_fund_risk_latest_mv", lambda _dsn: None)
    stale = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert _publication(stale)["reason"] == "MV_RUN_MISMATCH"
    assert seen[-1]["date_mismatch"] == 0
    assert seen[-1]["content_mismatch"] == 1
    assert seen[-1]["corresponding"] == len(funds) - 1
    with _connect(test_dsn, schema) as conn:
        assert _singleton(conn)[1:] == ("idle", None, None)

    monkeypatch.setattr(risk, "_refresh_fund_risk_latest_mv", real_refresh)
    fresh = risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    assert _publication(fresh)["published"] is True
    with _connect(test_dsn, schema) as conn:
        assert _served_by_published_run(conn) == len(funds)


def test_read_model_without_risk_contract_columns_fails_loud(
    test_dsn, schema, monkeypatch
):
    iid, grid, funds, stales, dsn = _risk_env(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute("DROP MATERIALIZED VIEW fund_risk_latest_mv")
        conn.execute(
            "CREATE MATERIALIZED VIEW fund_risk_latest_mv AS "
            "SELECT DISTINCT ON (instrument_id) instrument_id, calc_date, return_1y "
            "FROM fund_risk_metrics WHERE organization_id IS NULL "
            "ORDER BY instrument_id, calc_date DESC"
        )
        conn.execute(
            "CREATE UNIQUE INDEX fund_risk_latest_mv_pk ON fund_risk_latest_mv (instrument_id)"
        )
    with pytest.raises(RuntimeError, match="does not project the risk contract"):
        risk.run(dsn, calc_date=grid[-1].isoformat(), serial=True)
    with _connect(test_dsn, schema) as conn:
        revision, state, active, published = _singleton(conn)
        assert (state, published) == ("running", None)  # invalidated, never promoted


# ──────────────────────────────────────────────────────────────────────────────
# R2-A: N1 seams (attempt/row evidence/revision same xid), N1-a hold ledger,
# N3 server-controlled time, N4 atomic operator, N5 PR132 prerequisite.
# ──────────────────────────────────────────────────────────────────────────────
REBASE_CONTRACT = "w1-tiingo-adjusted-daily-v1"


def _replica(conn):
    """Controlled-catalog simulation: bypass W1 triggers in this txn only."""
    conn.execute("SET LOCAL session_replication_role = replica")


def _attempt(conn, run_id, iid, start, end, *, status="success_new", rows=1,
             finished="clock_timestamp()", provider="tiingo"):
    conn.execute(
        "INSERT INTO nav_ingestion_attempts (run_id,instrument_id,ticker,provider,"
        "requested_start,requested_end,attempted_at,finished_at,status,"
        f"newest_observed_date,row_count) VALUES (%s,%s,'SYN',%s,%s,%s,"
        f"clock_timestamp()-interval '1 minute',{finished},%s,%s,%s)",
        (run_id, iid, provider, start, end, status, end, rows),
    )


def _running_run(conn, end, *, operation="normal", plan=None):
    run_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO nav_ingestion_runs (run_id,requested_end,status,operation,"
        "contract_version,plan_sha256) VALUES (%s,%s,'running',%s,%s,%s)",
        (run_id, end, operation, REBASE_CONTRACT if operation == "rebase" else None,
         plan),
    )
    return run_id


def test_revision_attribution_requires_same_transaction_success_attempt(
    test_dsn, schema
):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, 2), dt.date(2026, 1, 5)]
    rows = ingest.build_rows(
        tuple(NavObservation(d, 100.0 + i, "adjusted") for i, d in enumerate(days)),
        [(iid, "USD")],
    )
    with _connect(test_dsn, schema) as conn:
        # Attempt committed in an earlier transaction: different xid.
        earlier = _completed_run(conn, iid, days[0], days[-1], status="running")
        with pytest.raises(psycopg.errors.RaiseException, match="same-transaction"):
            ingest.upsert_nav_timeseries(conn, rows, run_id=earlier, provider="tiingo")
        # Same transaction but a failed fetch.
        run_id = _running_run(conn, days[-1])
        conn.commit()
        _attempt(conn, run_id, iid, days[0], days[-1], status="transient_error", rows=0)
        with pytest.raises(psycopg.errors.RaiseException, match="same-transaction"):
            ingest.upsert_nav_timeseries(conn, rows, run_id=run_id, provider="tiingo")
        # A success cannot finish after it was persisted (so after the write).
        run_id = _running_run(conn, days[-1])
        conn.commit()
        with pytest.raises(psycopg.errors.CheckViolation):
            _attempt(conn, run_id, iid, days[0], days[-1], rows=2,
                     finished="clock_timestamp()+interval '1 hour'")
        conn.rollback()
        assert conn.execute(
            "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s", (iid,)
        ).fetchone()[0] == 0
        # Valid: success attempt + NAV in one txn; a later aborted parent does
        # not invalidate the committed proof (N1-b schema seam).
        run_id = _running_run(conn, days[-1])
        conn.commit()
        _attempt(conn, run_id, iid, days[0], days[-1], rows=2)
        ingest.upsert_nav_timeseries(conn, rows, run_id=run_id, provider="tiingo")
        conn.execute("UPDATE nav_ingestion_runs SET status='aborted', "
                     "reason_code='TIINGO_BUDGET' WHERE run_id=%s", (run_id,))
        conn.commit()
        proof = conn.execute(
            "SELECT bool_and(r.source_attempt_xid = a.commit_xid), count(*) "
            "FROM fund_nav_data_revisions r JOIN nav_ingestion_attempts a "
            "ON a.run_id=r.source_run_id AND a.instrument_id=r.instrument_id "
            "AND a.provider=r.source_provider WHERE r.instrument_id=%s",
            (iid,),
        ).fetchone()
        assert proof[0] is True and proof[1] >= 2


def test_runs_and_attempts_are_append_only_with_server_fields(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    day = dt.date(2026, 1, 2)
    with _connect(test_dsn, schema) as conn:
        for statement, error in (
            ("INSERT INTO nav_ingestion_runs (run_id,requested_end,status,completed_at) "
             "VALUES (gen_random_uuid(),CURRENT_DATE,'completed',clock_timestamp())",
             "registered running"),
            ("INSERT INTO nav_ingestion_runs (run_id,requested_end,status,operation) "
             "VALUES (gen_random_uuid(),CURRENT_DATE,'running','rebase')", None),
        ):
            if error:
                with pytest.raises(psycopg.Error, match=error):
                    conn.execute(statement)
            else:
                with pytest.raises(psycopg.errors.CheckViolation):
                    conn.execute(statement)
            conn.rollback()
        run_id = uuid.uuid4()
        conn.execute(
            "INSERT INTO nav_ingestion_runs (run_id,started_at,requested_end,status) "
            "VALUES (%s,'2000-01-01',%s,'running')",
            (run_id, day),
        )
        with pytest.raises(psycopg.Error, match="may only transition"):
            conn.execute("UPDATE nav_ingestion_runs SET requested_end=%s WHERE run_id=%s",
                         (day, run_id))
        conn.rollback()
        conn.execute(
            "INSERT INTO nav_ingestion_runs (run_id,started_at,requested_end,status) "
            "VALUES (%s,'2000-01-01',%s,'running')",
            (run_id, day),
        )
        conn.commit()
        _attempt(conn, run_id, iid, day, day)
        conn.commit()
        with pytest.raises(psycopg.errors.CheckViolation):
            _attempt(conn, run_id, uuid.uuid4(), day, day, rows=0)
        conn.rollback()
        with pytest.raises(psycopg.errors.UniqueViolation):
            _attempt(conn, run_id, iid, day, day)
        conn.rollback()
        for statement in ("UPDATE nav_ingestion_attempts SET status='empty'",
                          "DELETE FROM nav_ingestion_attempts"):
            with pytest.raises(psycopg.Error, match="append-only"):
                conn.execute(statement)
            conn.rollback()
        conn.execute("UPDATE nav_ingestion_runs SET status='completed', "
                     "completed_at='2000-01-01' WHERE run_id=%s", (run_id,))
        started, completed = conn.execute(
            "SELECT started_at, completed_at FROM nav_ingestion_runs WHERE run_id=%s",
            (run_id,),
        ).fetchone()
        assert started.year > 2000 and completed.year > 2000  # server stamped
        conn.commit()
        with pytest.raises(psycopg.Error, match="attempt requires a running parent"):
            _attempt(conn, run_id, uuid.uuid4(), day, day)
        conn.rollback()
        for statement in ("UPDATE nav_ingestion_runs SET status='aborted'",
                          "DELETE FROM nav_ingestion_runs"):
            with pytest.raises(psycopg.Error, match="immutable"):
                conn.execute(statement)
            conn.rollback()


def _row_evidence(conn, run_id, iid, start, end, *, provider="tiingo", digest=None):
    conn.execute(
        """INSERT INTO nav_ingestion_row_evidence
           (run_id,instrument_id,provider,nav_date,level_digest,observed_nav,
            source_nav_kind,revision_head,commit_xid,recorded_at)
           SELECT %s, n.instrument_id, %s, n.nav_date,
                  COALESCE(%s, nav_level_evidence_digest_v1(n.nav_date, n.nav,
                      n.source_nav, n.source, n.source_nav_kind, n.currency,
                      n.nav_repair_kind)),
                  n.source_nav, n.source_nav_kind,
                  COALESCE((SELECT revision_id FROM fund_nav_data_heads h
                            WHERE h.instrument_id = n.instrument_id), 0),
                  '0'::xid8, TIMESTAMPTZ '2000-01-01'
           FROM nav_timeseries n
           WHERE n.instrument_id=%s AND n.nav_date BETWEEN %s AND %s""",
        (run_id, provider, digest, iid, start, end),
    )


def test_unchanged_refetch_is_row_evidence_not_a_fake_revision(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, 2), dt.date(2026, 1, 5), dt.date(2026, 1, 6)]
    rows = ingest.build_rows(
        tuple(NavObservation(d, 100.0 + i, "adjusted") for i, d in enumerate(days)),
        [(iid, "USD")],
    )
    with _connect(test_dsn, schema) as conn:
        _write(conn, rows)
        head = _head(conn, iid)
        # Identical re-fetch: no DML, no revision; proof is explicit row evidence.
        run_id = _running_run(conn, days[-1])
        conn.commit()
        _attempt(conn, run_id, iid, days[0], days[-1], status="success_no_new", rows=3)
        result = ingest._write_instrument_nav_tx(conn, rows, run_id=run_id, provider="tiingo")
        assert (result.changed_level_rows, result.revision_count) == (0, 0)
        _row_evidence(conn, run_id, iid, days[0], days[-1])
        conn.commit()
        assert _head(conn, iid) == head
        evidence = conn.execute(
            "SELECT count(*), bool_and(e.commit_xid = a.commit_xid), "
            "bool_and(e.recorded_at > TIMESTAMPTZ '2001-01-01'), "
            "bool_and(e.revision_head = %s) FROM nav_ingestion_row_evidence e "
            "JOIN nav_ingestion_attempts a USING (run_id, instrument_id, provider)",
            (head,),
        ).fetchone()
        assert evidence == (3, True, True, True)
        # Python/SQL digest parity on persisted values.
        stored = conn.execute(
            "SELECT e.level_digest, n.nav_date, n.nav, n.source_nav, n.source, "
            "n.source_nav_kind, n.currency, n.nav_repair_kind "
            "FROM nav_ingestion_row_evidence e JOIN nav_timeseries n "
            "USING (instrument_id, nav_date) ORDER BY n.nav_date"
        ).fetchall()
        assert all(row[0] == level_evidence_digest(*row[1:]) for row in stored)
        # Wrong digest, stale head, other-xid attempt: rejected at COMMIT.
        for case in ("digest", "head", "xid"):
            run_id = _running_run(conn, days[-1])
            conn.commit()
            if case == "xid":
                _attempt(conn, run_id, iid, days[0], days[-1], rows=3)
                conn.commit()
                with pytest.raises(psycopg.Error, match="same-transaction successful attempt"):
                    _row_evidence(conn, run_id, iid, days[0], days[-1])
                conn.rollback()
                continue
            _attempt(conn, run_id, iid, days[0], days[-1], rows=3)
            _row_evidence(conn, run_id, iid, days[0], days[-1],
                          digest="0" * 64 if case == "digest" else None)
            if case == "head":
                conn.execute(
                    "UPDATE nav_timeseries SET aum_usd=1 WHERE instrument_id=%s "
                    "AND nav_date=%s", (iid, days[0]),
                )
            with pytest.raises(psycopg.Error, match="does not match the persisted level"):
                conn.commit()
            conn.rollback()
        for statement in ("UPDATE nav_ingestion_row_evidence SET observed_nav=1",
                          "DELETE FROM nav_ingestion_row_evidence"):
            with pytest.raises(psycopg.Error, match="append-only"):
                conn.execute(statement)
            conn.rollback()


def test_successor_return_revision_is_derived_not_a_claimed_fetch(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, day) for day in (2, 5, 6)]
    with _connect(test_dsn, schema) as conn:
        _write(conn, ingest.build_rows(
            tuple(NavObservation(d, 100.0 + i, "adjusted") for i, d in enumerate(days)),
            [(iid, "USD")],
        ))
        # Fetch covers only days[1]; its successor's return must be recomputed.
        run_id = _provider_write(
            conn,
            ingest.build_rows((NavObservation(days[1], 101.25, "adjusted"),), [(iid, "USD")]),
        )
        revisions = conn.execute(
            "SELECT nav_date, data_changed, derived_return_only, dependency_start_date "
            "FROM fund_nav_data_revisions WHERE source_run_id=%s ORDER BY revision_id",
            (run_id,),
        ).fetchall()
        # Level write, its own return recomputation, and the successor's return,
        # which is a derived revision depending on the fetched date (no fake fetch).
        assert revisions == [
            (days[1], True, False, None),
            (days[1], True, True, days[0]),
            (days[2], True, True, days[1]),
        ]


# ── N1-a: reexpression ledger ────────────────────────────────────────────────
def _reexpress(conn, iid, dates, factor=0.5):
    prices = dict(conn.execute(
        "SELECT nav_date, source_nav FROM nav_timeseries WHERE instrument_id=%s "
        "AND nav_date = ANY(%s)", (iid, list(dates)),
    ).fetchall())
    rows = ingest.build_rows(
        tuple(NavObservation(d, round(float(prices[d]) * factor, 6), "adjusted")
              for d in sorted(dates)),
        [(iid, "USD")],
        calendar={d: STAMP for d in dates} if dates[-1] >= dt.date(2025, 1, 1) else None,
    )
    return _provider_write(conn, rows)


def _receipt_tx(conn, iid, start, end, *, plan="e" * 64, drop_day=None):
    """Hand-built rebase receipt (DB contract seam; R2-B builds the real one)."""
    run_id = _running_run(conn, end, operation="rebase", plan=plan)
    _attempt(conn, run_id, iid, start, end, status="success_no_new", rows=401)
    _row_evidence(conn, run_id, iid, start, end)
    if drop_day is not None:
        _replica(conn)
        conn.execute("DELETE FROM nav_ingestion_row_evidence WHERE run_id=%s "
                     "AND nav_date=%s", (run_id, drop_day))
        conn.execute("SET LOCAL session_replication_role = origin")
    days = [row[0] for row in conn.execute(
        "SELECT session_date FROM nav_valuation_schedules WHERE calendar_id='NYSE-TEST' "
        "AND calendar_version='v1' AND session_date BETWEEN %s AND %s "
        "AND nav_due_at <= clock_timestamp() ORDER BY session_date", (start, end),
    ).fetchall()]
    evidence = conn.execute(
        "SELECT nav_date, level_digest FROM nav_ingestion_row_evidence WHERE run_id=%s "
        "ORDER BY nav_date", (run_id,),
    ).fetchall()
    grid_json = "[" + ",".join(f'"{d.isoformat()}"' for d in days) + "]"
    evidence_json = "[" + ",".join(f'["{d.isoformat()}","{g}"]' for d, g in evidence) + "]"
    head = _head(conn, iid)
    lifecycle = conn.execute(
        "SELECT evidence_id FROM nav_instrument_policy_evidence WHERE instrument_id=%s "
        "ORDER BY effective_at DESC, known_at DESC, recorded_at DESC, evidence_id DESC "
        "LIMIT 1", (iid,),
    ).fetchone()[0]
    receipt_id = uuid.uuid4()
    conn.execute(
        """INSERT INTO nav_rebase_receipts
           (receipt_id,run_id,instrument_id,provider,contract_version,plan_sha256,
            policy_id,policy_version,policy_hash,lifecycle_evidence_id,window_start,
            window_end,grid_digest,provider_snapshot_sha256,row_evidence_digest,
            before_head,after_head,observed_levels_count,changed_level_rows,
            changed_return_rows,committed_at,commit_xid)
           VALUES (%s,%s,%s,'tiingo',%s,%s,'synthetic','v1',%s,%s,%s,%s,%s,%s,%s,
                   %s,%s,401,0,0,TIMESTAMPTZ '2000-01-01','0'::xid8)""",
        (receipt_id, run_id, iid, REBASE_CONTRACT, plan, "a" * 64, lifecycle, start, end,
         hashlib.sha256(grid_json.encode()).hexdigest(), "f" * 64,
         hashlib.sha256(evidence_json.encode()).hexdigest(), head, head),
    )
    return run_id, receipt_id


def _resolve(conn, iid, event_id, run_id, receipt_id, first, last):
    conn.execute(
        """INSERT INTO fund_nav_reexpression_events
           (instrument_id,event_kind,first_changed_date,last_changed_date,
            source_run_id,source_provider,revision_head,recorded_at,reason_code,
            resolves_event_id,rebase_receipt_id)
           VALUES (%s,'RESOLVED',%s,%s,%s,'tiingo',0,TIMESTAMPTZ '2000-01-01',
                   'FULL_WINDOW_RECONCILED',%s,%s)""",
        (iid, first, last, run_id, event_id, receipt_id),
    )


def _holds(conn, iid):
    return conn.execute(
        "SELECT event_id, first_changed_date, last_changed_date FROM "
        "fund_nav_reexpression_holds WHERE instrument_id=%s ORDER BY event_id", (iid,),
    ).fetchall()


def test_hold_ledger_blocks_overlap_only_and_resolves_with_same_txn_receipt(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    with _connect(test_dsn, schema) as conn:
        # A detection wholly outside the due grid is ledger-only (never blocks).
        # Old history is fetched together with grid[0] (unchanged level), so the
        # recomputed grid[0] return is covered by that attempt; derived-return
        # lineage across attempt windows is an R2-B read (fail-closed today).
        old = [dt.date(2012, 3, day) for day in (5, 6, 7)]
        _provider_write(conn, ingest.build_rows(
            (*(NavObservation(d, 20.0 + i, "adjusted") for i, d in enumerate(old)),
             NavObservation(grid[0], 100.0, "adjusted")),
            [(iid, "USD")],
        ))
        _reexpress(conn, iid, old[:2])  # not grid[0]'s predecessor
        assert [h[1:] for h in _holds(conn, iid)] == [(old[0], old[1])]
    assert readiness.run(test_dsn)["ready_count"] == 1
    with _connect(test_dsn, schema) as conn:
        scope = grid[-3:]
        _reexpress(conn, iid, scope)
        event_id = _holds(conn, iid)[-1][0]
        assert _holds(conn, iid)[-1][1:] == (scope[0], scope[-1])
    readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "RETURN_INTERVAL_INCOMPATIBLE"
        # Tamper and ungoverned resolution fail.
        for statement in ("UPDATE fund_nav_reexpression_events SET last_changed_date=first_changed_date",
                          "DELETE FROM fund_nav_reexpression_events"):
            with pytest.raises(psycopg.Error, match="append-only"):
                conn.execute(statement)
            conn.rollback()
        # Receipt committed in an EARLIER transaction cannot resolve.
        early_run, early_receipt = _receipt_tx(conn, iid, grid[0], grid[-1], plan="1" * 64)
        conn.commit()
        with pytest.raises(psycopg.Error, match="same-transaction receipt"):
            _resolve(conn, iid, event_id, early_run, early_receipt, scope[0], scope[-1])
        conn.rollback()
        # Receipt missing one due session: the whole transaction rolls back.
        run_id, receipt_id = _receipt_tx(conn, iid, grid[0], grid[-1], plan="2" * 64,
                                         drop_day=grid[100])
        _resolve(conn, iid, event_id, run_id, receipt_id, scope[0], scope[-1])
        with pytest.raises(psycopg.Error, match="governed full-window evidence"):
            conn.commit()
        conn.rollback()
        assert [h[0] for h in _holds(conn, iid)][-1] == event_id  # still active
        # Narrower than the event, or another instrument: refused.
        run_id, receipt_id = _receipt_tx(conn, iid, grid[0], grid[-1], plan="3" * 64)
        with pytest.raises(psycopg.Error, match="must cover one DETECTED"):
            _resolve(conn, iid, event_id, run_id, receipt_id, scope[1], scope[-1])
        conn.rollback()
        run_id, receipt_id = _receipt_tx(conn, iid, grid[0], grid[-1], plan="4" * 64)
        with pytest.raises(psycopg.Error, match="must cover one DETECTED"):
            _resolve(conn, uuid.uuid4(), event_id, run_id, receipt_id, scope[0], scope[-1])
        conn.rollback()
        # Valid: same-transaction full-window receipt covering the event.
        run_id, receipt_id = _receipt_tx(conn, iid, grid[0], grid[-1], plan="5" * 64)
        _resolve(conn, iid, event_id, run_id, receipt_id, scope[0], scope[-1])
        conn.commit()
        assert [h[0] for h in _holds(conn, iid)] != [event_id]
        assert event_id not in [h[0] for h in _holds(conn, iid)]
        audit = conn.execute(
            "SELECT event_kind, resolves_event_id, rebase_receipt_id IS NOT NULL "
            "FROM fund_nav_reexpression_events WHERE instrument_id=%s ORDER BY event_id",
            (iid,),
        ).fetchall()
        assert audit[-2:] == [("DETECTED", None, False), ("RESOLVED", event_id, True)]
        # Repeated resolution of the same event fails.
        run_id, receipt_id = _receipt_tx(conn, iid, grid[0], grid[-1], plan="6" * 64)
        with pytest.raises(psycopg.errors.UniqueViolation):
            _resolve(conn, iid, event_id, run_id, receipt_id, scope[0], scope[-1])
        conn.rollback()
        # A new detection reopens the hold.
        _reexpress(conn, iid, scope, factor=2.0)
        assert any(h[1:] == (scope[0], scope[-1]) for h in _holds(conn, iid))
    readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "RETURN_INTERVAL_INCOMPATIBLE"


def test_unattributed_reexpression_is_refused_and_rolls_back(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    iid = uuid.uuid4()
    days = [dt.date(2026, 1, 2), dt.date(2026, 1, 5)]
    with _connect(test_dsn, schema) as conn:
        _write(conn, ingest.build_rows(
            tuple(NavObservation(d, 100.0 + i, "adjusted") for i, d in enumerate(days)),
            [(iid, "USD")],
        ))
        head = _head(conn, iid)
        with pytest.raises(ValueError, match="requires provider attribution"):
            ingest.upsert_nav_timeseries(conn, ingest.build_rows(
                tuple(NavObservation(d, 50.0, "adjusted") for d in days), [(iid, "USD")],
            ))
        assert _head(conn, iid) == head and _holds(conn, iid) == []


# ── N3: server-controlled instants and conservative current semantics ────────
def test_server_stamps_override_or_reject_client_timestamps(test_dsn, schema, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    readiness.run(test_dsn)
    ancient = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)
    with _connect(test_dsn, schema) as conn:
        with pytest.raises(psycopg.Error, match="inserted unpublished"):
            conn.execute(
                "INSERT INTO nav_policy_versions SELECT policy_id, 'v9', policy_hash, "
                "readiness_profile, valuation_frequency, calendar_id, calendar_version, "
                "calendar_source, timezone, coverage_start, coverage_end, valid_through, "
                "calendar_session_count, calendar_digest, sample_intervals, "
                "annualization_sessions, required_nav_kind, required_return_semantics, "
                "modeling_currency, currency_treatment, source_reference, %s "
                "FROM nav_policy_versions", (ancient,),
            )
        conn.rollback()
        for table in ("nav_policy_current", "fund_nav_readiness_current"):
            before = conn.execute(f"SELECT published_at FROM {table}").fetchone()[0]
            conn.execute(f"UPDATE {table} SET published_at=%s", (ancient,))
            after = conn.execute(f"SELECT published_at FROM {table}").fetchone()[0]
            assert after > before > ancient
        conn.execute(
            """INSERT INTO nav_instrument_policy_evidence
               (instrument_id,policy_id,policy_version,known_at,effective_at,fund_status,
                valuation_frequency,identity_verified,return_basis_verified,
                currency_verified,evidence_reference,recorded_at)
               VALUES (%s,'synthetic','v1',%s,%s,'ACTIVE','daily',true,true,true,'x',%s)""",
            (iid, ancient, ancient, ancient),
        )
        assert conn.execute(
            "SELECT max(recorded_at) FROM nav_instrument_policy_evidence"
        ).fetchone()[0] > ancient
        conn.rollback()
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute("UPDATE fund_nav_readiness_runs SET completed_at=%s", (ancient,))
        conn.rollback()
        with pytest.raises(psycopg.Error, match="not published"):
            conn.execute("UPDATE nav_policy_current SET policy_version='v9'")
        conn.rollback()
        # Risk completion and feature computation are stamped by the server.
        nav = conn.execute(
            "SELECT nav_date, nav FROM nav_timeseries WHERE instrument_id=%s "
            "ORDER BY nav_date", (iid,),
        ).fetchall()
        run_id = uuid.uuid4()
        _register_risk_run(conn, run_id, grid[-1], [iid], scope="current_full")
        risk._persist_feature_evidence(conn, iid, grid[-1], nav, 0.04, run_id, None, {}, {}, [])
        conn.commit()
        risk._finish_risk_run(conn, run_id, 1, 1)
        conn.execute("UPDATE fund_nav_risk_runs SET status='complete', completed_at=%s "
                     "WHERE risk_run_id=%s", (ancient, run_id))
        stamps = conn.execute(
            "SELECT r.completed_at, f.computed_at FROM fund_nav_risk_runs r "
            "JOIN fund_nav_feature_evidence f USING (risk_run_id) WHERE risk_run_id=%s",
            (run_id,),
        ).fetchone()
        assert all(stamp > ancient for stamp in stamps)
        conn.rollback()


def test_repointing_older_version_or_run_gets_a_new_instant(test_dsn, schema, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    first = uuid.UUID(readiness.run(test_dsn)["run_id"])
    second = uuid.UUID(readiness.run(test_dsn)["run_id"])
    with _connect(test_dsn, schema) as conn:
        t_second = conn.execute("SELECT published_at FROM fund_nav_readiness_current").fetchone()[0]
        conn.execute("UPDATE fund_nav_readiness_current SET run_id=%s", (first,))
        conn.commit()
        t_back = conn.execute("SELECT published_at FROM fund_nav_readiness_current").fetchone()[0]
        assert t_back > t_second
        # The old run is NOT current at an instant before its re-publication.
        assert conn.execute(
            "SELECT fund_nav_snapshot_current_at_v1(%s,%s,%s)", (iid, first, t_second)
        ).fetchone()[0] is False
        assert conn.execute(
            "SELECT fund_nav_snapshot_current_at_v1(%s,%s,clock_timestamp())", (iid, first)
        ).fetchone()[0] is True
        assert conn.execute(
            "SELECT fund_nav_snapshot_current_at_v1(%s,%s,clock_timestamp())", (iid, second)
        ).fetchone()[0] is False
        # Policy V1 -> V2 -> V1: every pointer write is a new, later instant.
        stamps = [conn.execute("SELECT published_at FROM nav_policy_current").fetchone()[0]]
        _publish_rollover(conn, iid, grid)
        stamps.append(conn.execute("SELECT published_at FROM nav_policy_current").fetchone()[0])
        conn.execute("UPDATE nav_policy_current SET policy_version='v1'")
        conn.commit()
        stamps.append(conn.execute("SELECT published_at FROM nav_policy_current").fetchone()[0])
        assert stamps == sorted(stamps) and len(set(stamps)) == 3


@pytest.mark.parametrize("table", ["nav_policy_current", "fund_nav_readiness_current"])
def test_publication_clock_regression_aborts(test_dsn, schema, monkeypatch, table):
    _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    readiness.run(test_dsn)
    with _connect(test_dsn, schema) as conn:
        _replica(conn)  # simulate a server clock that later reads earlier
        conn.execute(f"UPDATE {table} SET published_at=clock_timestamp()+interval '1 day'")
        conn.commit()
        with pytest.raises(psycopg.Error, match="publication_clock_regressed"):
            conn.execute(f"UPDATE {table} SET published_at=published_at")
        conn.rollback()


BOUNDARIES = {
    "readiness_pointer": "UPDATE fund_nav_readiness_current SET published_at=%(t)s",
    "readiness_run": "UPDATE fund_nav_readiness_runs SET completed_at=%(t)s",
    "policy_pointer": "UPDATE nav_policy_current SET published_at=%(t)s",
    "policy_version": "UPDATE nav_policy_versions SET published_at=%(t)s",
    "risk_completed": "UPDATE fund_nav_risk_runs SET completed_at=%(t)s "
                      "WHERE risk_run_id=%(risk)s",
    "feature_computed": "UPDATE fund_nav_feature_evidence SET computed_at=%(t)s "
                        "WHERE risk_run_id=%(risk)s",
    "lifecycle_recorded": "UPDATE nav_instrument_policy_evidence SET recorded_at=%(t)s",
}


@pytest.mark.parametrize("boundary", sorted(BOUNDARIES))
def test_snapshot_is_false_before_each_evidence_instant(
    test_dsn, schema, monkeypatch, boundary
):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    run_id = uuid.UUID(readiness.run(test_dsn)["run_id"])
    with _connect(test_dsn, schema) as conn:
        published_risk = conn.execute(
            "SELECT published_risk_run_id FROM fund_nav_risk_publication"
        ).fetchone()[0]
        now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        instants = conn.execute(
            """SELECT p.published_at, run.completed_at, cp.published_at, pv.published_at,
                      risk.completed_at, f.computed_at, e.recorded_at
               FROM fund_nav_readiness_current p
               JOIN fund_nav_readiness_runs run ON run.run_id = p.run_id
               JOIN nav_policy_current cp ON true
               JOIN nav_policy_versions pv ON pv.policy_id = cp.policy_id
                AND pv.policy_version = cp.policy_version
               JOIN fund_nav_risk_runs risk ON risk.risk_run_id=%s
               JOIN fund_nav_feature_evidence f ON f.risk_run_id=risk.risk_run_id
               JOIN nav_instrument_policy_evidence e ON e.instrument_id=%s""",
            (published_risk, iid),
        ).fetchone()
        assert max(instants) == instants[0] <= now  # pointer is the latest instant
        later = now + dt.timedelta(hours=1)
        _replica(conn)  # controlled catalog: move ONE instant after the others
        conn.execute(BOUNDARIES[boundary], {"t": later, "risk": published_risk})
        conn.commit()
        at = [
            conn.execute(
                "SELECT fund_nav_snapshot_current_at_v1(%s,%s,%s)", (iid, run_id, t)
            ).fetchone()[0]
            for t in (later - dt.timedelta(minutes=30), later + dt.timedelta(minutes=30))
        ]
        assert at == [False, True]


def test_backdated_lifecycle_fact_cannot_rewrite_a_past_instant(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    run_id = uuid.UUID(readiness.run(test_dsn)["run_id"])
    with _connect(test_dsn, schema) as conn:
        before = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        # Known/effective before `before`, newer than the ACTIVE fact, but only
        # recorded now: it must not change what was current at `before`.
        conn.execute(
            """INSERT INTO nav_instrument_policy_evidence
               (instrument_id,policy_id,policy_version,known_at,effective_at,fund_status,
                valuation_frequency,identity_verified,return_basis_verified,
                currency_verified,evidence_reference)
               VALUES (%s,'synthetic','v1',%s,%s,'INACTIVE','daily',true,true,true,
                       'backdated-closure')""",
            (iid, before - dt.timedelta(minutes=1), before - dt.timedelta(minutes=1)),
        )
        conn.commit()
        snapshot = [
            conn.execute("SELECT fund_nav_snapshot_current_at_v1(%s,%s,%s)",
                         (iid, run_id, t)).fetchone()[0]
            for t in (before, conn.execute("SELECT clock_timestamp()").fetchone()[0])
        ]
        assert snapshot == [True, False]


def test_risk_policy_pin_mismatch_is_not_current(test_dsn, schema, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    run_id = uuid.UUID(readiness.run(test_dsn)["run_id"])
    with _connect(test_dsn, schema) as conn:
        assert conn.execute("SELECT fund_nav_snapshot_current_at_v1(%s,%s,clock_timestamp())",
                            (iid, run_id)).fetchone()[0] is True
        _replica(conn)
        conn.execute("UPDATE fund_nav_risk_runs SET policy_hash=%s", ("f" * 64,))
        conn.commit()
        assert conn.execute("SELECT fund_nav_snapshot_current_at_v1(%s,%s,clock_timestamp())",
                            (iid, run_id)).fetchone()[0] is False
        # NULL pins cannot exist on a current_full run (CHECK, even without triggers).
        _replica(conn)
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE fund_nav_risk_runs SET policy_hash=NULL")
        conn.rollback()


# ── N4: operator atomicity ───────────────────────────────────────────────────
def _policy_document(grid, iid, *, calendar_id="NYSE-OPS", source="ops-fixture"):
    sessions = [
        (d, dt.datetime.combine(d, dt.time(20), dt.timezone.utc),
         dt.datetime.combine(d, dt.time(23), dt.timezone.utc), "ops-fixture-ref")
        for d in grid
    ]
    return {
        "policy_id": "ops", "policy_version": "v1",
        "readiness_profile": "current_daily_nav_v1", "valuation_frequency": "daily",
        "timezone": "America/New_York", "calendar_id": calendar_id,
        "calendar_version": "v1", "calendar_source": source,
        "source_reference": "ops-fixture-ref", "sample_intervals": 400,
        "annualization_sessions": 252, "required_nav_kind": "adjusted",
        "required_return_semantics": "observed_interval_log_ratio",
        "modeling_currency": "USD", "currency_treatment": "native_only",
        "publication_state": "approved",
        "repaired_nav_kinds": sorted(REPAIRED_NAV_KINDS),
        "adjusted_overlap_absolute_tolerance": 0.0000005,
        "adjusted_overlap_relative_tolerance": 0.00000001,
        "coverage_start": grid[0].isoformat(), "coverage_end": grid[-1].isoformat(),
        "valid_through": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).isoformat(),
        "calendar_session_count": len(grid),
        "calendar_digest": calendar_digest(sessions),
        "sessions": [
            {"session_date": d.isoformat(), "valuation_close_at": c.isoformat(),
             "nav_due_at": due.isoformat()}
            for d, c, due, _ref in sessions
        ],
        "instrument_evidence": [{
            "instrument_id": str(iid), "fund_status": "ACTIVE",
            "valuation_frequency": "daily", "identity_verified": True,
            "return_basis_verified": True, "currency_verified": True,
            "known_at": "2025-01-01T00:00:00+00:00",
            "effective_at": "2025-01-01T00:00:00+00:00",
            "evidence_reference": "ops-fixture-identity",
        }],
    }


W1_STATE_SQL = """
SELECT (SELECT count(*) FROM nav_policy_versions),
       (SELECT count(*) FROM nav_valuation_schedules),
       (SELECT count(*) FROM nav_instrument_policy_evidence),
       (SELECT to_jsonb(c) FROM nav_policy_current c),
       (SELECT count(*) FROM nav_calendar_maintenance_runs),
       (SELECT count(*) FROM fund_nav_data_revisions),
       (SELECT to_jsonb(p) FROM fund_nav_readiness_current p),
       (SELECT md5(string_agg(to_jsonb(n)::text, ',' ORDER BY instrument_id, nav_date))
        FROM nav_timeseries n)
"""


def _w1_state(test_dsn, schema):
    with _connect(test_dsn, schema) as conn:
        return conn.execute(W1_STATE_SQL).fetchone()


def _fixture_receipt(valid_until=None) -> dict:
    """Aggregate plan-v4 receipt for atomicity fixtures (bootstrap: no pointer).

    Only the receipt FILE validation is replaced here; locks, plan, freshness
    and pointer checks of the real operator still run. The governed receipt
    itself is covered end-to-end in test_generate_fund_nav_policy_v1_db.py.
    """
    now = dt.datetime.now(dt.timezone.utc)
    until = valid_until or now + dt.timedelta(days=7) - dt.timedelta(hours=1)
    # Internally consistent window: until == min matched + 7d, the capture
    # (== decision) lies in [min matched, until] and precedes now.
    minimum = until - dt.timedelta(days=7)
    captured = min(now - dt.timedelta(seconds=2), until)
    receipt = {key: "0" * 64 for key in operator.AUDIT_RECEIPT_KEYS}
    receipt.update(
        audit_contract_version=operator.AUDIT_CONTRACT_VERSION,
        audit_contract_sha256=operator.AUDIT_CONTRACT_SHA256,
        sec_source_contract=operator.SEC_SOURCE_CONTRACT,
        sec_query_contract_sha256=operator.SEC_QUERY_CONTRACT_SHA256,
        captured_at=captured.isoformat(),
        decision_at=captured.isoformat(),
        min_matched_synced_at=minimum.isoformat(),
        sec_valid_until=until.isoformat(),
        previous_policy_identity=None,
    )
    return receipt


def _install_receipt_seam(monkeypatch, receipt=None):
    fixed = receipt or _fixture_receipt()  # one receipt: the plan digest is stable

    def governed(args):
        if args.policy_file is None:
            return None, b"", None
        evidence, raw = operator._policy(args.policy_file)
        return evidence, raw, fixed

    monkeypatch.setattr(operator, "_governed_policy", governed)


def _ops_env(test_dsn, schema, tmp_path, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema, with_policy=False)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    _install_receipt_seam(monkeypatch)
    path = tmp_path / "ops-policy.json"
    path.write_text(json.dumps(_policy_document(grid, iid)), encoding="utf-8")
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    base = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    combined = [*base, "--policy-file", str(path), "--instrument-id", str(iid),
                "--start", grid[0].isoformat(), "--end", grid[-1].isoformat()]
    policy_only = [*base, "--policy-file", str(path)]
    return iid, grid, combined, policy_only


@pytest.mark.parametrize("mode", ["combined", "policy_only"])
@pytest.mark.parametrize("lock", ["ingestion", "readiness"])
def test_operator_lock_busy_is_exit4_with_zero_dml(
    test_dsn, schema, tmp_path, monkeypatch, capsys, mode, lock
):
    iid, grid, combined, policy_only = _ops_env(test_dsn, schema, tmp_path, monkeypatch)
    cli = combined if mode == "combined" else policy_only
    plan = _plan(cli, capsys)
    before = _w1_state(test_dsn, schema)
    key = LOCK_INSTRUMENT_INGESTION if lock == "ingestion" else LOCK_FUND_NAV_READINESS
    with _connect(test_dsn, schema) as holder:
        with advisory_lock(holder, key) as held:
            assert held
            assert operator.main([*cli, "--mode", "apply", "--plan-sha256", plan]) == 4
    out = json.loads(capsys.readouterr().out)
    assert (out["status"], out["retryable"], out["dml_committed"], out["published"]) == (
        "lock_busy", True, False, False,
    )
    assert out["ddl"] == "unchanged"
    assert _w1_state(test_dsn, schema) == before


def test_combined_failure_after_policy_rolls_back_everything_then_retries(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    iid, grid, combined, _ = _ops_env(test_dsn, schema, tmp_path, monkeypatch)
    plan_out_code = operator.main(combined)
    plan_out = json.loads(capsys.readouterr().out)
    assert plan_out_code == 0 and plan_out["plan"]["operations"] == [
        "ddl", "maintenance", "policy",
    ]
    assert plan_out["plan"]["before"]["maintenance"]["eligible_rows"] == 401
    plan = plan_out["plan_sha256"]
    before = _w1_state(test_dsn, schema)
    real_tx = operator._apply_calendar_maintenance_tx
    written = {}

    def _fail_before_commit(conn, *args, **kwargs):
        written.update(real_tx(conn, *args, **kwargs))  # policy + stamps written
        written["policy_in_txn"] = conn.execute(
            "SELECT policy_id FROM nav_policy_current").fetchone()[0]
        raise ValueError("maintenance_verification_failed")

    monkeypatch.setattr(operator, "_apply_calendar_maintenance_tx", _fail_before_commit)
    assert operator.main([*combined, "--mode", "apply", "--plan-sha256", plan]) == 2
    out = json.loads(capsys.readouterr().out)
    assert (out["code"], out["policy"], out["maintenance"], out["dml_committed"]) == (
        "maintenance_verification_failed", "rolled_back", "rolled_back", False,
    )
    assert (written["changed_rows"], written["policy_in_txn"]) == (401, "ops")
    assert _w1_state(test_dsn, schema) == before  # policy rolled back with it
    monkeypatch.setattr(operator, "_apply_calendar_maintenance_tx", real_tx)
    assert operator.main([*combined, "--mode", "apply", "--plan-sha256", plan]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["status"], out["policy"], out["maintenance"], out["changed_rows"],
            out["published"], out["dml_committed"]) == (
        "applied", "committed", "committed", 401, True, True,
    )
    after = _w1_state(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s "
            "AND calendar_id='NYSE-OPS'", (iid,),
        ).fetchone()[0] == 401
        assert conn.execute(
            "SELECT policy_id FROM nav_policy_current").fetchone()[0] == "ops"
    # Same hash again: truthful no-op, no re-publication instant.
    assert operator.main([*combined, "--mode", "apply", "--plan-sha256", plan]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["status"], out["policy"], out["maintenance"], out["dml_committed"]) == (
        "unchanged", "unchanged", "unchanged", False,
    )
    assert _w1_state(test_dsn, schema) == after


def test_stale_plan_is_blocked_without_dml(test_dsn, schema, tmp_path, monkeypatch, capsys):
    iid, grid, combined, policy_only = _ops_env(test_dsn, schema, tmp_path, monkeypatch)
    plan = _plan(combined, capsys)
    policy_plan = _plan(policy_only, capsys)
    with _connect(test_dsn, schema) as conn:  # scope changes after planning
        _provider_write(conn, ingest.build_rows(
            (NavObservation(grid[-1], 999.0, "adjusted"),), [(iid, "USD")]))
    before = _w1_state(test_dsn, schema)
    assert operator.main([*combined, "--mode", "apply", "--plan-sha256", plan]) == 2
    out = json.loads(capsys.readouterr().out)
    assert (out["code"], out["dml_committed"]) == ("PLAN_STALE", False)
    assert _w1_state(test_dsn, schema) == before
    # Policy-only plan is unaffected by the NAV change and still applies once.
    assert operator.main([*policy_only, "--mode", "apply", "--plan-sha256", policy_plan]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["policy"], out["maintenance"], out["dml_committed"]) == (
        "committed", "not_attempted", True,
    )


# ── R2-C F1: replay identity is the whole persisted operation ───────────────
def _check_out(cli, capsys):
    assert operator.main(cli) == 0, capsys.readouterr().out
    return json.loads(capsys.readouterr().out)


def _apply_out(cli, plan, capsys):
    code = operator.main([*cli, "--mode", "apply", "--plan-sha256", plan])
    return code, json.loads(capsys.readouterr().out)


def _lifecycle(conn, iid):
    return conn.execute(
        "SELECT fund_status, evidence_reference FROM nav_instrument_policy_evidence "
        "WHERE instrument_id=%s ORDER BY effective_at, known_at", (iid,),
    ).fetchall()


def _receipt_rows(conn):
    return conn.execute(
        "SELECT plan_sha256, policy_document_digest, audit_receipt_sha256 "
        "FROM nav_policy_publication_receipts ORDER BY published_at"
    ).fetchall()


def test_target_pointer_accepts_only_exact_replay_never_new_lifecycle(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    """Round3 F3: a pointer already at the target is an exact replay or nothing."""
    iid, grid, _combined, policy_only = _ops_env(test_dsn, schema, tmp_path, monkeypatch)
    first = _check_out(policy_only, capsys)
    assert first["plan"]["policy_document_digest"] == canonical_digest(
        json.loads((tmp_path / "ops-policy.json").read_text()))
    code, out = _apply_out(policy_only, first["plan_sha256"], capsys)
    assert (code, out["policy"], out["dml_committed"]) == (0, "committed", True)
    with _connect(test_dsn, schema) as conn:
        receipts = _receipt_rows(conn)
        pointer = conn.execute("SELECT published_at FROM nav_policy_current").fetchone()[0]
    # One receipt row, bound to the applied plan digest and document.
    assert [row[:2] for row in receipts] == [
        (first["plan_sha256"], first["plan"]["policy_document_digest"])]
    # Same version (same policy_hash, pointer already at it) + a NEW lifecycle row.
    original = (tmp_path / "ops-policy.json").read_text()
    document = json.loads(original)
    later = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
    document["instrument_evidence"].append({
        **document["instrument_evidence"][0], "fund_status": "INACTIVE",
        "known_at": later.isoformat(), "effective_at": later.isoformat(),
        "evidence_reference": "ops-fixture-closure",
    })
    (tmp_path / "ops-policy.json").write_text(json.dumps(document), encoding="utf-8")
    state = _w1_state(test_dsn, schema)
    # Rejected at check and at apply (old plan hash or none): nothing appended.
    assert operator.main(policy_only) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "target_policy_not_exact_replay"
    code, out = _apply_out(policy_only, first["plan_sha256"], capsys)
    assert (code, out["code"], out["dml_committed"]) == (
        2, "target_policy_not_exact_replay", False)
    assert _w1_state(test_dsn, schema) == state
    with _connect(test_dsn, schema) as conn:
        assert _lifecycle(conn, iid) == [("ACTIVE", "ops-fixture-identity")]
        assert _receipt_rows(conn) == receipts
    # The original document is an exact replay: its old plan and a fresh plan
    # both return "unchanged" without any write or pointer re-stamp.
    (tmp_path / "ops-policy.json").write_text(original, encoding="utf-8")
    code, out = _apply_out(policy_only, first["plan_sha256"], capsys)
    assert (code, out["status"], out["policy"], out["dml_committed"]) == (
        0, "unchanged", "unchanged", False)
    fresh = _check_out(policy_only, capsys)
    assert fresh["plan_sha256"] != first["plan_sha256"]  # before-state moved
    code, out = _apply_out(policy_only, fresh["plan_sha256"], capsys)
    assert (code, out["status"], out["policy"], out["dml_committed"]) == (
        0, "unchanged", "unchanged", False)
    assert _w1_state(test_dsn, schema) == state
    with _connect(test_dsn, schema) as conn:
        assert _receipt_rows(conn) == receipts
        assert conn.execute(
            "SELECT published_at FROM nav_policy_current").fetchone()[0] == pointer
    # A different audit receipt of the same policy bytes never replays.
    other = _fixture_receipt()
    other["audit_dossier_sha256"] = "1" * 64
    _install_receipt_seam(monkeypatch, other)
    assert operator.main(policy_only) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "target_policy_not_exact_replay"
    code, out = _apply_out(policy_only, first["plan_sha256"], capsys)
    assert (code, out["code"], out["dml_committed"]) == (
        2, "target_policy_not_exact_replay", False)
    assert _w1_state(test_dsn, schema) == state


def test_publication_receipt_ledger_is_private_append_only_and_windowed(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    """Round3 F4: one server-stamped receipt row per governed publication."""
    _iid, _grid, _combined, policy_only = _ops_env(test_dsn, schema, tmp_path, monkeypatch)
    check = _check_out(policy_only, capsys)
    code, out = _apply_out(policy_only, check["plan_sha256"], capsys)
    assert (code, out["policy"]) == (0, "committed")
    audit = check["audit"]
    with _connect(test_dsn, schema) as conn:
        row = conn.execute(
            "SELECT policy_id, policy_version, policy_hash, plan_version, plan_sha256, "
            "policy_artifact_sha256, policy_document_digest, audit_receipt_sha256, "
            "previous_policy_id IS NULL, captured_at <= published_at, "
            "published_at <= sec_valid_until, commit_xid IS NOT NULL "
            "FROM nav_policy_publication_receipts"
        ).fetchone()
        assert row == (
            "ops", "v1", check["plan"]["policy_hash"], "nav-schema-plan-v4",
            check["plan_sha256"], check["plan"]["policy_sha256"],
            check["plan"]["policy_document_digest"], canonical_digest(audit),
            True, True, True, True,
        )
        # No Light runtime access to the private ledger.
        assert conn.execute(
            "SELECT has_table_privilege('app_runtime', 'nav_policy_publication_receipts', "
            "'SELECT')").fetchone()[0] is False
        for statement in (
            "UPDATE nav_policy_publication_receipts SET plan_sha256 = %s",
            "DELETE FROM nav_policy_publication_receipts WHERE plan_sha256 <> %s",
        ):
            with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
                conn.execute(statement, ("f" * 64,))
            conn.rollback()
        # A row must describe the current published pointer, inside its window.
        columns = (
            "readiness_profile, policy_id, policy_version, policy_hash, plan_version, "
            "plan_sha256, policy_artifact_sha256, policy_document_digest, "
            "audit_receipt_sha256, audit_dossier_sha256, canary_manifest_sha256, "
            "capture_bundle_sha256, captured_at, sec_valid_until"
        )
        values = (
            "'current_daily_nav_v1', 'ops', 'v1', %s, 'nav-schema-plan-v4', %s, %s, %s, "
            "%s, %s, %s, %s, clock_timestamp() - interval '1 minute', %s"
        )
        base = [check["plan"]["policy_hash"], "a" * 64, *["b" * 64] * 6]
        with pytest.raises(psycopg.errors.RaiseException, match="current published"):
            conn.execute(
                f"INSERT INTO nav_policy_publication_receipts ({columns}) VALUES ({values})",
                ["0" * 64, *base[1:], "2099-01-01T00:00:00+00:00"],
            )
        conn.rollback()
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                f"INSERT INTO nav_policy_publication_receipts ({columns}) VALUES ({values})",
                [*base, "2000-01-01T00:00:00+00:00"],
            )
        conn.rollback()


def test_maintenance_receipt_of_scope_a_never_suppresses_scope_b(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    iid, grid, _combined, policy_only = _ops_env(test_dsn, schema, tmp_path, monkeypatch)
    code, out = _apply_out(policy_only, _check_out(policy_only, capsys)["plan_sha256"],
                           capsys)
    assert code == 0
    base = policy_only[:4]
    half = grid[len(grid) // 2]
    scope_a = [*base, "--instrument-id", str(iid), "--start", grid[0].isoformat(),
               "--end", half.isoformat()]
    scope_b = [*base, "--instrument-id", str(iid),
               "--start", (half + dt.timedelta(days=1)).isoformat(),
               "--end", grid[-1].isoformat()]
    plan_a = _check_out(scope_a, capsys)["plan_sha256"]
    plan_b = _check_out(scope_b, capsys)["plan_sha256"]
    code, out = _apply_out(scope_a, plan_a, capsys)
    assert (code, out["maintenance"]) == (0, "committed")
    stamped_a = out["changed_rows"]
    # B supplied with A's receipt hash: A's receipt describes another scope.
    code, out = _apply_out(scope_b, plan_a, capsys)
    assert (code, out["code"], out["dml_committed"]) == (2, "PLAN_STALE", False)
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s AND calendar_id "
            "IS NOT NULL", (iid,)).fetchone()[0] == stamped_a
    code, out = _apply_out(scope_b, plan_b, capsys)
    assert (code, out["maintenance"], out["changed_rows"]) == (0, "committed",
                                                              401 - stamped_a)
    # Exact replay of A: its receipt and post-state still hold -> no-op.
    code, out = _apply_out(scope_a, plan_a, capsys)
    assert (code, out["status"], out["maintenance"]) == (0, "unchanged", "unchanged")
    # Post-state moved inside A's scope: the receipt no longer proves the
    # operation, so the replay is stale (never a silent "unchanged").
    with _connect(test_dsn, schema) as conn:
        _provider_write(conn, ingest.build_rows(
            (NavObservation(grid[3], 777.0, "adjusted"),), [(iid, "USD")],
            calendar={grid[3]: ("NYSE-OPS", "v1", "ops-fixture")}))
    code, out = _apply_out(scope_a, plan_a, capsys)
    assert (code, out["code"], out["dml_committed"]) == (2, "PLAN_STALE", False)
    # A receipt of a different (fabricated) scope under the hash is ignored.
    with _connect(test_dsn, schema) as conn:
        assert operator._maintenance_receipt_exact(
            conn, plan_a, [str(uuid.uuid4())], grid[0], half,
            operator._pinned_policy(conn, lock=False)) is False
        conn.rollback()


def test_previous_plan_version_digest_is_rejected(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    _iid, _grid, combined, _ = _ops_env(test_dsn, schema, tmp_path, monkeypatch)
    out = _check_out(combined, capsys)
    assert out["plan"]["audit"] == out["audit"]
    assert out["plan"]["operation"]["audit"] == ["0" * 64] * 3
    for legacy in (operator.previous_version_digest(out["plan"]),
                   operator.v2_version_digest(out["plan"])):
        assert legacy != out["plan_sha256"]
        code, applied = _apply_out(combined, legacy, capsys)
        assert (code, applied["code"], applied["dml_committed"]) == (
            2, "plan_version_mismatch", False)


def test_hand_authored_policy_cli_publication_is_blocked(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    """No flag or fallback publishes a hand-authored document from the CLI."""
    iid, grid, _ = _seed(test_dsn, schema, with_policy=False)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    path = tmp_path / "hand.json"
    path.write_text(json.dumps(_policy_document(grid, iid)), encoding="utf-8")
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    base = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    before = _w1_state(test_dsn, schema)
    for extra in ([], ["--mode", "apply", "--plan-sha256", "0" * 64]):
        assert operator.main([*base, "--policy-file", str(path), *extra]) == 2
        out = json.loads(capsys.readouterr().out)
        assert (out["code"], out["dml_committed"], out["published"]) == (
            "audit_receipt_required", False, False)
    # Receipt arguments without a policy are refused too.
    assert operator.main([*base, "--audit-dossier-file", str(path)]) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "audit_receipt_without_policy"
    assert _w1_state(test_dsn, schema) == before
    # The parser itself still accepts the document programmatically.
    assert operator._policy(str(path))[0]["policy_id"] == "ops"


def test_expired_receipt_and_foreign_pointer_block_under_locks(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    _iid, _grid, _combined, policy_only = _ops_env(test_dsn, schema, tmp_path, monkeypatch)
    plan = _check_out(policy_only, capsys)["plan_sha256"]
    # SEC freshness expires between check and apply: re-decided at apply.
    expired = _fixture_receipt(dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1))
    _install_receipt_seam(monkeypatch, expired)
    before = _w1_state(test_dsn, schema)
    assert operator.main(policy_only) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "audit_sec_freshness_expired"
    code, out = _apply_out(policy_only, plan, capsys)
    assert (code, out["code"], out["dml_committed"]) == (2, "audit_sec_freshness_expired", False)
    assert _w1_state(test_dsn, schema) == before
    # The receipt expects "no pointer"; a pointer published meanwhile is foreign.
    _install_receipt_seam(monkeypatch)
    with _connect(test_dsn, schema) as conn:
        other = _policy_document(_grid, _iid)
        other["policy_id"], other["policy_version"] = "foreign", "f1"
        operator._publish_policy(conn, operator._policy(other)[0])
        conn.commit()
    code, out = _apply_out(policy_only, plan, capsys)
    assert (code, out["code"], out["dml_committed"]) == (2, "current_pointer_not_previous", False)


# ── N5: PR132 prerequisite (real catalog, representable mutations) ───────────
PR132_ALT_TYPES = {
    "source_nav": "numeric(18,4)", "source_nav_kind": "varchar(17)",
    "nav_repair_kind": "varchar(49)", "return_start_date": "timestamp",
    "return_source_boundary": "text", "return_uses_repaired_nav": "text",
    "return_semantics": "varchar(49)", "return_verification_status": "varchar(25)",
    "calendar_id": "varchar(129)", "calendar_version": "varchar(65)",
    "calendar_source": "varchar(100)",
}


def _pr132_cases():
    from scripts.nav_timeseries_provenance_schema import EXPECTED_COLUMNS

    defaults = {"numeric": "0", "date": "DATE '2000-01-01'", "boolean": "false"}
    for column, _type in EXPECTED_COLUMNS:
        default = defaults.get(_type.split("(")[0], "'x'")
        yield f"{column}:notnull", [f"ALTER TABLE nav_timeseries ALTER COLUMN {column} SET NOT NULL"]
        yield f"{column}:default", [
            f"ALTER TABLE nav_timeseries ALTER COLUMN {column} SET DEFAULT {default}"
        ]
        yield f"{column}:type", [
            f"ALTER TABLE nav_timeseries ALTER COLUMN {column} TYPE {PR132_ALT_TYPES[column]} "
            f"USING {column}::text::{PR132_ALT_TYPES[column]}"
        ]
        yield f"{column}:missing", [f"ALTER TABLE nav_timeseries DROP COLUMN {column}"]
    yield "calendar_source:generated", [
        "ALTER TABLE nav_timeseries DROP COLUMN calendar_source",
        "ALTER TABLE nav_timeseries ADD COLUMN calendar_source text "
        "GENERATED ALWAYS AS ('x') STORED",
    ]
    yield "relkind:view", [
        "ALTER TABLE nav_timeseries RENAME TO nav_timeseries_base",
        "CREATE VIEW nav_timeseries AS SELECT * FROM nav_timeseries_base",
    ]


@pytest.mark.parametrize(("case", "statements"), list(_pr132_cases()),
                         ids=[case for case, _ in _pr132_cases()])
def test_pr132_contract_blocks_before_any_ddl_or_dml(
    test_dsn, schema, monkeypatch, capsys, case, statements
):
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        try:
            for statement in statements:
                conn.execute(statement)
        except psycopg.errors.FeatureNotSupported:
            pytest.fail(f"mutation not representable on this server: {case}")
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    base = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    for argv in (base, [*base, "--mode", "apply", "--plan-sha256", "0" * 64]):
        assert operator.main(argv) == 3
        out = json.loads(capsys.readouterr().out)
        assert out["code"].startswith("nav_provenance_pr132_")
        assert out["ddl"] == "not_attempted"
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname = ANY(%s)", (schema, list(operator.TABLES)),
        ).fetchone()[0] == 0


# ── Old shapes are incompatible, never migrated ──────────────────────────────
@pytest.mark.parametrize(
    "statements",
    [
        ["DROP VIEW fund_nav_reexpression_holds",
         "CREATE TABLE fund_nav_reexpression_holds (instrument_id uuid PRIMARY KEY, "
         "first_changed_date date NOT NULL, last_changed_date date NOT NULL, "
         "source_run_id uuid, reason_code text NOT NULL, detected_at timestamptz)"],
        ["ALTER TABLE nav_ingestion_attempts DROP COLUMN commit_xid"],
        ["ALTER TABLE nav_ingestion_runs DROP COLUMN operation CASCADE"],
        ["DROP TABLE nav_rebase_receipts CASCADE"],
    ],
    ids=["holds_table", "attempt_without_xid", "run_without_operation", "no_receipts"],
)
def test_pre_r2_shapes_are_upgrade_required_without_mutation(
    test_dsn, schema, monkeypatch, capsys, statements
):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        for statement in statements:
            conn.execute(statement)
        before = operator._check(conn, schema)
    assert (before["compatibility"], before["ready"]) == ("incompatible", False)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    base = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", "0" * 64]) == 3
    assert json.loads(capsys.readouterr().out)["code"] == "incompatible_schema"
    with _connect(test_dsn, schema, autocommit=True) as conn:
        assert operator._check(conn, schema)["catalog_sha256"] == before["catalog_sha256"]

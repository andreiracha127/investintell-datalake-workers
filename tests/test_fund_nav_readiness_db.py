"""Real, non-skipping PG18/Timescale W1 contract tests on a disposable database."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
import pytest
from psycopg import sql

from scripts import fund_nav_readiness_schema as operator
from src.db import LOCK_FUND_NAV_READINESS, advisory_lock
from src.workers import fund_nav_readiness as readiness
from src.workers import instrument_ingestion as ingest
from src.workers import nav_current_daily_chain as chain
from src.workers import risk_metrics as risk
from src.workers._nav_policy import calendar_digest
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


def _bootstrap(dsn, schema):
    with _connect(dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        conn.execute(SCHEMA_SQL)
        conn.execute(SCHEMA_SQL)  # additive, rerunnable, no MV replacement
        conn.execute("CREATE TABLE funds_profile_mv (instrument_id uuid PRIMARY KEY)")


def _seed(dsn, schema, *, with_policy=True, extra_closed=False):
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
    valid_through = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
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
        ing_run = uuid.uuid4()
        conn.execute(
            """INSERT INTO nav_ingestion_runs
               VALUES (%s,clock_timestamp()-interval '1 hour',clock_timestamp(),%s,'completed')""",
            (ing_run, grid[-1]),
        )
        conn.execute(
            """INSERT INTO nav_ingestion_attempts
               (run_id,instrument_id,ticker,provider,requested_start,requested_end,
                attempted_at,finished_at,status,newest_observed_date,row_count)
               VALUES (%s,%s,'SYN','tiingo',%s,%s,
                       clock_timestamp()-interval '1 hour',clock_timestamp()-interval '1 minute',
                       'success_new',%s,401)""",
            (ing_run, iid, grid[0], grid[-1], grid[-1]),
        )
        conn.commit()
        observations = tuple(
            NavObservation(d, round(100.0 + i * 0.01, 6), "adjusted")
            for i, d in enumerate(grid)
        )
        calendar = (
            {d: ("NYSE-TEST", "v1", source) for d in grid} if with_policy else None
        )
        rows = ingest.build_rows(observations, [(iid, "USD")], calendar=calendar)
        ingest.upsert_nav_timeseries(conn, rows, run_id=ing_run)
        risk_run = uuid.uuid4()
        conn.execute(
            """INSERT INTO fund_nav_risk_runs
               (risk_run_id,calc_date,status,expected_rows,persisted_rows,completed_at)
               VALUES (%s,%s,'complete',1,1,clock_timestamp())""",
            (risk_run, grid[-1]),
        )
        nav_rows = conn.execute(
            "SELECT nav_date, nav FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date",
            (iid,),
        ).fetchall()
        risk._persist_feature_evidence(
            conn, iid, grid[-1], nav_rows, 0.04, risk_run, None, {}, {}, []
        )
        conn.execute(
            """INSERT INTO fund_nav_risk_publication
               (readiness_profile,revision_id,state,published_risk_run_id)
               VALUES ('current_daily_nav_v1',1,'idle',%s)""",
            (risk_run,),
        )
        conn.commit()
    return iid, grid, ing_run


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
            assert readiness.run(test_dsn)["state"] == "locked"


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
        conn.execute(
            """UPDATE fund_nav_feature_evidence SET feature_as_of=%s
               WHERE instrument_id=%s""",
            (grid[-2], iid),
        )
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
    assert operator.main(base) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    assert operator.main([*base, "--mode", "apply"]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "blocked"
    fingerprint = operator.plan_hash(ddl, schema, b"", [], None, None)
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", fingerprint]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "applied"
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone()[0] == 0
        )


def test_operator_rejects_wrong_existing_schema_without_mutation(test_dsn, schema):
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        conn.execute("CREATE TABLE nav_policy_versions (policy_id integer)")
        with pytest.raises(ValueError, match="readiness_schema_contract_mismatch"):
            operator._check(conn, schema)
        assert (
            conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone()[0] == 0
        )


def test_operator_lock_timeout_rolls_back_then_reruns(
    test_dsn, schema, monkeypatch, capsys
):
    _bootstrap(test_dsn, schema)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    base = [
        "--schema",
        schema,
        "--expected-sql-sha256",
        hashlib.sha256(ddl).hexdigest(),
        "--mode",
        "apply",
        "--plan-sha256",
        operator.plan_hash(ddl, schema, b"", [], None, None),
    ]
    with _connect(test_dsn, schema) as blocker:
        blocker.execute("LOCK TABLE nav_policy_versions IN ACCESS EXCLUSIVE MODE")
        assert operator.main(base) == 2
        assert json.loads(capsys.readouterr().out)["status"] == "blocked"
        blocker.rollback()
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone()[0] == 0
        )
    assert operator.main(base) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "applied"


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
        with pytest.raises(ValueError, match="readiness_schema_contract_mismatch"):
            operator._check(conn, schema)
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
        assert operator._check(conn, schema)["status"] == "upgrade_required"
        conn.execute(SCHEMA_SQL)
        assert operator._check(conn, schema)["status"] == "ready"


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
        fingerprint = operator._publish_policy(conn, parsed)
        assert fingerprint == operator._publish_policy(conn, parsed)
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
        assert operator._backfill(conn, [str(old_id)], grid[-1], grid[-1]) == 0
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
        conn.execute(SCHEMA_SQL)
        conn.execute(SCHEMA_SQL)
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
        conn.execute(
            """CREATE TABLE fund_risk_metrics (
                 instrument_id uuid,calc_date date,organization_id uuid,
                 volatility_1y numeric,
                 UNIQUE NULLS NOT DISTINCT (instrument_id,calc_date,organization_id))"""
        )
        conn.commit()
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
    monkeypatch.setattr(risk, "connect", lambda dsn: _connect(dsn, schema))
    assert readiness.run(test_dsn)["ready_count"] == 1
    new_run = uuid.uuid4()
    with _connect(test_dsn, schema) as conn:
        risk._start_risk_run(conn, new_run, grid[-1], 1)
        risk._record_risk_exclusion(
            conn, new_run, iid, grid[-1], "METRICS_UNAVAILABLE", []
        )
        conn.commit()
        risk._finish_risk_run(conn, new_run, 1, 0)
        assert conn.execute(
            "SELECT status,persisted_rows FROM fund_nav_risk_runs WHERE risk_run_id=%s",
            (new_run,),
        ).fetchone() == ("metrics_complete", 1)
    risk._mark_risk_published(test_dsn, new_run)
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
    monkeypatch.setattr(risk, "connect", lambda dsn: _connect(dsn, schema))
    first = readiness.run(test_dsn)
    replacement = uuid.uuid4()
    with _connect(test_dsn, schema) as publisher, _connect(test_dsn, schema) as reader:
        assert reader.execute(
            "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
        ).fetchone()[0]
        risk._start_risk_run(publisher, replacement, grid[-1], 1)
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
        risk._persist_feature_evidence(
            publisher, iid, grid[-1], nav, 0.04, replacement, None, {}, {}, []
        )
        publisher.commit()
        risk._finish_risk_run(publisher, replacement, 1, 1)
        assert (
            reader.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            is False
        )
        with _connect(test_dsn, schema) as blocker:
            with advisory_lock(blocker, LOCK_FUND_NAV_READINESS) as acquired:
                assert acquired
                with pytest.raises(RuntimeError, match="readiness lock busy"):
                    risk._mark_risk_published(test_dsn, replacement)
        assert (
            reader.execute("SELECT state FROM fund_nav_risk_publication").fetchone()[0]
            == "running"
        )
    risk._mark_risk_published(test_dsn, replacement)
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1"
            ).fetchone()[0]
            is False
        )
        assert (
            conn.execute(
                "SELECT revision_id FROM fund_nav_risk_publication"
            ).fetchone()[0]
            == 2
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
        risk._start_risk_run(conn, run_id, dt.date.today(), 1)
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
        assert ingest.upsert_nav_timeseries(conn, first) == 3
        revised = ingest.build_rows(
            (NavObservation(days[1], 101.5, "adjusted"),), [(iid, "USD")]
        )
        assert ingest.upsert_nav_timeseries(conn, revised) == 1
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
        ingest.upsert_nav_timeseries(conn, initial)
        changed = ingest.build_rows(
            (
                NavObservation(days[0], 100.5, "adjusted"),
                NavObservation(days[2], 102.5, "adjusted"),
            ),
            [(iid, "USD")],
        )
        ingest.upsert_nav_timeseries(conn, changed)
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
        ingest.upsert_nav_timeseries(conn, produced)
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
            ingest.upsert_nav_timeseries(conn, revised)
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
        ingest.upsert_nav_timeseries(conn, carry)
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
        ingest.upsert_nav_timeseries(
            conn,
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
        ingest.upsert_nav_timeseries(conn, yahoo)
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
        ingest.upsert_nav_timeseries(
            conn,
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
        ingest.upsert_nav_timeseries(conn, revised)
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
        ingest.upsert_nav_timeseries(
            conn, ingest.build_rows(observations, [(iid, "USD")])
        )
        revised = tuple(
            NavObservation(d, (100.0 + i) / 2, "adjusted") for i, d in enumerate(days)
        )
        ingest.upsert_nav_timeseries(conn, ingest.build_rows(revised, [(iid, "USD")]))
        hold = conn.execute(
            "SELECT first_changed_date,last_changed_date FROM "
            "fund_nav_reexpression_holds WHERE instrument_id=%s",
            (iid,),
        ).fetchone()
        assert hold == (days[0], days[-1])
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
        ingest.upsert_nav_timeseries(
            conn,
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
            ingest.upsert_nav_timeseries(
                conn,
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
            "UPDATE nav_timeseries SET calendar_source='temporary' "
            "WHERE instrument_id=%s AND nav_date=%s",
            (iid, grid[-1]),
        )
        price = float(
            conn.execute(
                "SELECT nav FROM nav_timeseries WHERE instrument_id=%s AND nav_date=%s",
                (iid, grid[-1]),
            ).fetchone()[0]
        )
        conn.execute(
            "INSERT INTO nav_ingestion_runs (run_id,started_at,completed_at,"
            "requested_end,status) VALUES (%s,clock_timestamp(),clock_timestamp(),"
            "%s,'completed')",
            (repaired_run, grid[-1]),
        )
        conn.execute(
            "INSERT INTO nav_ingestion_attempts "
            "(run_id,instrument_id,ticker,provider,requested_start,requested_end,"
            "attempted_at,finished_at,status,newest_observed_date,row_count) "
            "VALUES (%s,%s,'SYN','tiingo',%s,%s,clock_timestamp(),"
            "clock_timestamp(),'success_new',%s,1)",
            (repaired_run, iid, grid[-1], grid[-1], grid[-1]),
        )
        conn.commit()
        corrected = ingest.build_rows(
            (NavObservation(grid[-1], price, "adjusted"),),
            [(iid, "USD")],
            calendar={grid[-1]: ("NYSE-TEST", "v1", "fixture:NYSE-valuation-due")},
        )
        ingest.upsert_nav_timeseries(conn, corrected, run_id=repaired_run)
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
        attempt_id = uuid.uuid4()
        writer.execute(
            "INSERT INTO nav_ingestion_runs "
            "(run_id,started_at,requested_end,status) "
            "VALUES (%s,clock_timestamp(),%s,'running')",
            (attempt_id, grid[-1]),
        )
        writer.commit()
        with pytest.raises(ValueError, match="duplicate NAV date"):
            ingest.upsert_nav_timeseries(
                writer, first + repeated + repeated, run_id=attempt_id
            )
        assert (
            reader.execute(
                "SELECT snapshot_current FROM fund_nav_readiness_current_v1 "
                "WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            is False
        )
        assert (
            reader.execute(
                "SELECT count(*) FROM nav_ingestion_attempts WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            == 1
        )
        assert (
            reader.execute(
                "SELECT run_id FROM nav_ingestion_attempts WHERE instrument_id=%s",
                (iid,),
            ).fetchone()[0]
            == attempt_run
        )
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
        assert ("B", "tiingo", "success_no_new") in records
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

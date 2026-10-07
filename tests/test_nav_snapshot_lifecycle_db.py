"""Snapshot continuity and fail-closed proofs on the disposable W1 database."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid

import pytest
from psycopg import sql

from scripts import fund_nav_readiness_schema as operator
from src.workers import instrument_ingestion as ingest
from src.workers._tiingo import NavObservation
from tests import test_fund_nav_readiness_db as base

test_dsn = base.test_dsn
schema = base.schema


def _published(dsn, schema, monkeypatch):
    iid, grid, _ing, run_id, later = base._lag_publication(dsn, schema, monkeypatch)
    return iid, grid, run_id, later


def _current(conn, iid, run_id):
    return base._current_at(conn, iid, run_id, conn.execute("SELECT clock_timestamp()").fetchone()[0])


def _append(conn, iid, grid, later):
    day = later[0][0]
    rows = ingest.build_rows(
        (NavObservation(grid[-1], 104.0, "adjusted"),
         NavObservation(day, 104.01, "adjusted")),
        [(iid, "USD")],
        calendar={day: ("NYSE-TEST", "v1", "fixture:NYSE-valuation-due")},
    )
    # The previous observation computes the new return; it is not written.
    base._provider_write(conn, rows[-1:])
    return day


def _attributed_mutation(conn, iid, date):
    """Give an adversarial write valid provider attribution, isolating the pin."""
    run_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO nav_ingestion_runs (run_id,requested_end,status) VALUES (%s,%s,'running')",
        (run_id, date),
    )
    conn.commit()
    conn.execute(
        """INSERT INTO nav_ingestion_attempts
           (run_id,instrument_id,ticker,provider,requested_start,requested_end,
            attempted_at,finished_at,status,newest_observed_date,row_count)
           VALUES (%s,%s,'SYN','tiingo',%s,%s,clock_timestamp()-interval '1 minute',
                   clock_timestamp(),'success_new',%s,1)""",
        (run_id, iid, date, date, date),
    )
    conn.execute("SELECT set_config('nav.ingestion_run_id', %s, true)", (str(run_id),))
    conn.execute("SELECT set_config('nav.ingestion_provider', 'tiingo', true)")


def test_snapshot_policy_rollover_keeps_the_published_generation(
    test_dsn, schema, monkeypatch
):
    iid, grid, run_id, _later = _published(test_dsn, schema, monkeypatch)
    with base._connect(test_dsn, schema) as conn:
        before = conn.execute("SELECT run_id FROM fund_nav_readiness_current").fetchone()
        assert _current(conn, iid, run_id)
        base._publish_rollover(conn, iid, grid)
        assert _current(conn, iid, run_id) is True
        assert conn.execute("SELECT run_id FROM fund_nav_readiness_current").fetchone() == before


@pytest.mark.parametrize(
    "change",
    ["INACTIVE", "UNKNOWN", "missing", "identity", "return_basis", "currency", "frequency"],
)
def test_successor_policy_cannot_preserve_revoked_or_missing_lifecycle(
    test_dsn, schema, monkeypatch, change
):
    iid, grid, run_id, _later = _published(test_dsn, schema, monkeypatch)
    with base._connect(test_dsn, schema) as conn:
        base._publish_rollover(conn, iid, grid)
        if change == "missing":
            # A new policy with no evidence for this snapshot's instrument.
            other = uuid.uuid4()
            base._publish_rollover(conn, other, grid, version="v3")
        else:
            status = change if change in {"INACTIVE", "UNKNOWN"} else "ACTIVE"
            conn.execute(
                """INSERT INTO nav_instrument_policy_evidence
                   (instrument_id,policy_id,policy_version,known_at,effective_at,
                    fund_status,valuation_frequency,identity_verified,
                    return_basis_verified,currency_verified,evidence_reference)
                   VALUES (%s,'synthetic','v2',clock_timestamp(),clock_timestamp(),
                           %s,%s,%s,%s,%s,'successor-restriction')""",
                (iid, status, "monthly" if change == "frequency" else "daily",
                 change != "identity", change != "return_basis", change != "currency"),
            )
            conn.commit()
        assert _current(conn, iid, run_id) is False


def test_snapshot_survives_only_proven_future_inserts(test_dsn, schema, monkeypatch):
    iid, grid, run_id, later = _published(test_dsn, schema, monkeypatch)
    with base._connect(test_dsn, schema) as conn:
        prefix = conn.execute(
            "SELECT nav_date,nav,return_1d FROM nav_timeseries "
            "WHERE instrument_id=%s AND nav_date<=%s ORDER BY nav_date", (iid, grid[-1])
        ).fetchall()
        _append(conn, iid, grid, later)
        assert _current(conn, iid, run_id) is True
        assert conn.execute(
            "SELECT nav_date,nav,return_1d FROM nav_timeseries "
            "WHERE instrument_id=%s AND nav_date<=%s ORDER BY nav_date", (iid, grid[-1])
        ).fetchall() == prefix
        # Existing session allowance still ends at the second later close.
        assert base._current_at(conn, iid, run_id, later[1][1]) is False


@pytest.mark.parametrize(
    "mutation",
    ["prefix_update", "prefix_delete", "backfill", "future_update", "future_delete",
     "later_return_rewrite", "unattributed_insert", "invented_head", "rewound_head"],
)
def test_append_allowance_rejects_every_unproved_history_change(
    test_dsn, schema, monkeypatch, mutation
):
    iid, grid, run_id, later = _published(test_dsn, schema, monkeypatch)
    with base._connect(test_dsn, schema) as conn:
        day = _append(conn, iid, grid, later)
        if mutation in {"prefix_update", "future_update"}:
            date = grid[-1] if mutation.startswith("prefix") else day
            _attributed_mutation(conn, iid, date)
            conn.execute(
                "UPDATE nav_timeseries SET nav=nav+1 WHERE instrument_id=%s AND nav_date=%s",
                (iid, date),
            )
        elif mutation in {"prefix_delete", "future_delete"}:
            date = grid[-1] if mutation.startswith("prefix") else day
            _attributed_mutation(conn, iid, date)
            conn.execute("DELETE FROM nav_timeseries WHERE instrument_id=%s AND nav_date=%s", (iid, date))
        elif mutation == "later_return_rewrite":
            _attributed_mutation(conn, iid, day)
            conn.execute(
                "UPDATE nav_timeseries SET return_1d=return_1d+0.001 "
                "WHERE instrument_id=%s AND nav_date=%s", (iid, day),
            )
        elif mutation in {"backfill", "unattributed_insert"}:
            date = grid[0] - dt.timedelta(days=1) if mutation == "backfill" else later[1][0]
            if mutation == "backfill":
                _attributed_mutation(conn, iid, date)
            conn.execute(
                "INSERT INTO nav_timeseries (instrument_id,nav_date,nav) VALUES (%s,%s,100)",
                (iid, date),
            )
        elif mutation == "invented_head":
            conn.execute("UPDATE fund_nav_data_heads SET revision_id=revision_id+1 WHERE instrument_id=%s", (iid,))
        else:
            conn.execute("UPDATE fund_nav_data_heads SET revision_id=0 WHERE instrument_id=%s", (iid,))
        conn.commit()
        assert _current(conn, iid, run_id) is False


def test_session_lag_predecessor_upgrades_to_continuous_snapshot(
    test_dsn, schema, monkeypatch, capsys
):
    iid, grid, run_id, later = _published(test_dsn, schema, monkeypatch)
    predecessor = (base.ROOT / "tests/fixtures/nav_snapshot_current_at_session_lag.sql").read_text(encoding="utf-8")
    with base._connect(test_dsn, schema) as conn:
        conn.execute(sql.SQL("SET search_path TO {}, pg_temp").format(sql.Identifier(schema)))
        conn.execute(predecessor)
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        conn.commit()
        _append(conn, iid, grid, later)
        publication_before = conn.execute(
            "SELECT run_id,published_at FROM fund_nav_readiness_current"
        ).fetchall()
        data_before = conn.execute(
            "SELECT nav_date,nav,return_1d FROM nav_timeseries "
            "WHERE instrument_id=%s ORDER BY nav_date", (iid,)
        ).fetchall()
        assert _current(conn, iid, run_id) is False
        check = operator._check(conn, schema)
        assert (check["compatibility"], check["mismatches"]) == ("repairable", ["functions"])
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    ddl = operator.DDL.read_bytes()
    args = ["--schema", schema, "--expected-sql-sha256", hashlib.sha256(ddl).hexdigest()]
    plan = base._plan(args, capsys)
    assert operator.main([*args, "--mode", "apply", "--plan-sha256", plan]) == 0
    assert json.loads(capsys.readouterr().out)["compatibility"] == "exact"
    with base._connect(test_dsn, schema) as conn:
        assert _current(conn, iid, run_id) is True
        assert conn.execute(
            "SELECT run_id,published_at FROM fund_nav_readiness_current"
        ).fetchall() == publication_before
        assert conn.execute(
            "SELECT nav_date,nav,return_1d FROM nav_timeseries "
            "WHERE instrument_id=%s ORDER BY nav_date", (iid,)
        ).fetchall() == data_before
        assert conn.execute(
            "SELECT has_function_privilege('app_runtime', "
            "'fund_nav_snapshot_current_at_v1(uuid,uuid,timestamptz)', 'EXECUTE'), "
            "has_function_privilege('public', "
            "'fund_nav_snapshot_current_at_v1(uuid,uuid,timestamptz)', 'EXECUTE')"
        ).fetchone() == (True, False)


def test_session_lag_predecessor_tampering_is_not_repairable(test_dsn, schema):
    base._bootstrap(test_dsn, schema)
    predecessor = (base.ROOT / "tests/fixtures/nav_snapshot_current_at_session_lag.sql").read_text(encoding="utf-8")
    with base._connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(sql.SQL("SET search_path TO {}, pg_temp").format(sql.Identifier(schema)))
        conn.execute(predecessor.replace("due_lag.sessions <= 1", "due_lag.sessions <= 2"))
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        report = operator._check(conn, schema)
        assert (report["compatibility"], report["mismatches"]) == ("incompatible", ["functions"])

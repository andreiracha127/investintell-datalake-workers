"""Stored-row repair contract on an owned PostgreSQL 18 / Timescale database.

No real provider may be constructed and Python socket access is forbidden.
The replica fixture represents rows persisted before the writer correction;
all operations under test run with the real revision and attribution triggers.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import socket
import uuid

import psycopg
import pytest
from psycopg.rows import dict_row

from scripts import repair_nav_return_lineage_v1 as cli
from src.db import LOCK_FUND_NAV_READINESS, LOCK_INSTRUMENT_INGESTION
from src.workers import _fallback_nav
from src.workers import fund_nav_readiness as readiness
from src.workers import instrument_ingestion as ingest
from src.workers import nav_return_lineage_repair as repair
from src.workers._tiingo import NavFetchResult, NavObservation
from tests import test_fund_nav_readiness_db as base

test_dsn = base.test_dsn
schema = base.schema


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*_args, **_kwargs):
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)

    class Forbidden:
        def __init__(self, *_args, **_kwargs):
            refuse()

    monkeypatch.setattr(ingest, "TiingoClient", Forbidden)
    monkeypatch.setattr(_fallback_nav, "FallbackNav", Forbidden)


class FakeProvider:
    def __init__(self, *, price=101.0, kind="adjusted"):
        self.price = price
        self.kind = kind
        self.calls = []

    def fetch(self, provider, ticker, start, end, *, remaining):
        assert provider in ("tiingo", "yahoo")
        assert start == end, "repair must validate just the stored date"
        assert callable(remaining) and remaining() > 0
        self.calls.append((provider, ticker, start, end))
        now = dt.datetime.now(dt.timezone.utc)
        return NavFetchResult(
            "success_new", (NavObservation(start, self.price, self.kind),), now, now
        )


def _limits(*, requests=20):
    return repair.RepairLimits(
        batch_size=20, max_requests=requests, max_seconds=120,
        rate_per_second=2.5,
    )


def _seed(conn, *, ticker="SYN", provider="tiingo"):
    iid = uuid.uuid4()
    day = dt.date.today() - dt.timedelta(days=2)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS instruments_universe "
        "(instrument_id uuid PRIMARY KEY,ticker text,currency text,"
        "is_active boolean,attributes jsonb)"
    )
    conn.execute(
        "INSERT INTO instruments_universe VALUES (%s,%s,'USD',true,'{}')",
        (iid, ticker),
    )
    conn.execute("SET LOCAL session_replication_role = replica")
    conn.execute(
        """INSERT INTO nav_timeseries
           (instrument_id,nav_date,nav,source_nav,source,currency,return_type,
            source_nav_kind,nav_repair_kind,aum_usd)
           VALUES (%s,%s,100,100,%s,'USD','log',NULL,NULL,123456)""",
        (iid, day - dt.timedelta(days=1), provider),
    )
    conn.execute(
        """INSERT INTO nav_timeseries
           (instrument_id,nav_date,nav,source_nav,source,currency,return_type,
            source_nav_kind,nav_repair_kind,return_source_boundary,aum_usd)
           VALUES (%s,%s,101,101,%s,'USD','log','adjusted','none',true,123456)""",
        (iid, day, provider),
    )
    conn.execute(
        """INSERT INTO nav_timeseries
           (instrument_id,nav_date,nav,source_nav,source,currency,return_type,
            source_nav_kind,nav_repair_kind,return_1d,return_start_date,
            return_source_boundary,return_uses_repaired_nav,return_semantics,
            return_verification_status,aum_usd)
           VALUES (%s,%s,102,102,%s,'USD','log','adjusted','none',%s,%s,
                   false,false,'observed_interval_log_ratio','unverified',123456)""",
        (iid, day + dt.timedelta(days=1), provider,
         round(math.log(102 / 101), 8), day),
    )
    conn.commit()
    return iid, day


def _add_earlier_bad_date(conn, iid, day):
    conn.execute("SET LOCAL session_replication_role = replica")
    conn.execute(
        """INSERT INTO nav_timeseries
           (instrument_id,nav_date,nav,source_nav,source,currency,return_type,
            source_nav_kind,nav_repair_kind,return_source_boundary)
           VALUES (%s,%s,100,100,'tiingo','USD','log',NULL,NULL,NULL),
                  (%s,%s,101,101,'tiingo','USD','log','adjusted','none',true)""",
        (iid, day - dt.timedelta(days=3), iid, day - dt.timedelta(days=2)),
    )
    conn.commit()


def _rows(conn, iid):
    return [row[0] for row in conn.execute(
        "SELECT to_jsonb(n) FROM nav_timeseries n WHERE instrument_id=%s "
        "ORDER BY nav_date", (iid,),
    ).fetchall()]


def _ledger(conn):
    return tuple(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                 for table in ("nav_ingestion_runs", "nav_ingestion_attempts",
                               "nav_ingestion_row_evidence", "fund_nav_data_revisions",
                               "fund_nav_data_heads", "nav_rebase_receipts"))


def _plan(conn, schema, limits):
    conn.rollback()
    conn.execute("SET TRANSACTION READ ONLY")
    try:
        return repair.build_plan(conn, schema=schema, limits=limits)
    finally:
        conn.rollback()


def _run(conn, plan, ids, client, *, limits=None, **kwargs):
    conn.rollback()
    return repair.run_repair(
        conn, plan, instrument_ids=[str(iid) for iid in ids],
        supplied_sha256=repair.sha256(plan), limits=limits or _limits(),
        client=client, **kwargs,
    )


@pytest.mark.parametrize("provider", ["tiingo", "yahoo"])
def test_stored_row_only_attributed_repair_and_idempotency(test_dsn, schema, provider):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, day = _seed(conn, provider=provider)
        before = _rows(conn, iid)
        plan = _plan(conn, schema, _limits())
        assert len(plan["items"]) == 1
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)
        fake = FakeProvider()
        result = _run(conn, plan, [iid], fake)
        assert result["exit_code"] == 0, result
        assert result["changed_rows"] == 1
        assert result["requests"] == 1
        assert fake.calls == [(provider, "SYN", day, day)]
        expected = [dict(row) for row in before]
        expected[1]["return_source_boundary"] = None
        assert _rows(conn, iid) == expected
        assert _ledger(conn) == (1, 1, 1, 1, 1, 0)
        revision = conn.execute(
            """SELECT r.nav_date,r.derived_return_only,r.data_changed,
                      r.calendar_changed,r.source_provider,
                      r.source_attempt_xid=a.commit_xid,
                      a.requested_start<=r.nav_date AND a.requested_end>=r.nav_date,
                      h.revision_id=r.revision_id,r.dependency_start_date
               FROM fund_nav_data_revisions r
               JOIN nav_ingestion_attempts a ON a.run_id=r.source_run_id
                 AND a.instrument_id=r.instrument_id AND a.provider=r.source_provider
               JOIN fund_nav_data_heads h ON h.instrument_id=r.instrument_id
               WHERE r.instrument_id=%s""", (iid,),
        ).fetchone()
        assert revision == (day, True, True, False, provider, True, True, True, None)
        now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        with conn.cursor(row_factory=dict_row) as cur:
            verified, lineage = readiness._per_date_lineage(cur, iid, [day], day, now)
        assert verified, lineage
        assert lineage[0][1][0] == "evidence"
        assert _plan(conn, schema, _limits())["items"] == []
        ledger = _ledger(conn)
        replay = _run(conn, plan, [iid], fake)
        assert replay["exit_code"] == 0, replay
        assert replay["changed_rows"] == 0
        assert replay["requests"] == 0
        assert _rows(conn, iid) == expected
        assert _ledger(conn) == ledger


@pytest.mark.parametrize("price,kind", [(150.0, "adjusted"), (101.0, "raw")])
def test_provider_mismatch_has_zero_writes(test_dsn, schema, price, kind):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, _ = _seed(conn)
        before = _rows(conn, iid)
        plan = _plan(conn, schema, _limits())
        result = _run(conn, plan, [iid], FakeProvider(price=price, kind=kind))
        assert result["exit_code"] == 0, result
        assert result["changed_rows"] == 0
        assert result["requests"] == 1
        assert result["instruments"], "residual must be reported"
        assert _rows(conn, iid) == before
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)


def test_open_detected_event_is_a_database_only_skip(test_dsn, schema):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, day = _seed(conn)
        # Synthetic pre-existing ledger entry; no actor under test uses replica.
        conn.execute("SET LOCAL session_replication_role = replica")
        conn.execute(
            """INSERT INTO fund_nav_reexpression_events
               (instrument_id,event_kind,first_changed_date,last_changed_date,
                source_run_id,source_provider,revision_head,recorded_at,reason_code)
               VALUES (%s,'DETECTED',%s,%s,%s,'tiingo',0,clock_timestamp(),
                       'ADJUSTED_HISTORY_REEXPRESSION')""",
            (iid, day - dt.timedelta(days=100), day, uuid.uuid4()),
        )
        conn.commit()
        before = _rows(conn, iid)
        plan = _plan(conn, schema, _limits())
        fake = FakeProvider()
        result = _run(conn, plan, [iid], fake)
        assert result["exit_code"] == 0, result
        assert result["changed_rows"] == 0
        assert result["instruments"]
        assert fake.calls == []
        assert _rows(conn, iid) == before
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)


def test_plan_digest_mismatch_refused_before_fetch(test_dsn, schema):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, _ = _seed(conn)
        plan = _plan(conn, schema, _limits())
        fake = FakeProvider()
        result = repair.run_repair(
            conn, plan, instrument_ids=[str(iid)], supplied_sha256="0" * 64,
            limits=_limits(), client=fake,
        )
        assert result["exit_code"] == 2, result
        assert fake.calls == []
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)


def test_budget_stop_commits_one_instrument_and_resumes(test_dsn, schema):
    base._bootstrap(test_dsn, schema)
    limits = _limits(requests=1)
    with base._connect(test_dsn, schema) as conn:
        first, _ = _seed(conn, ticker="FIRST")
        second, _ = _seed(conn, ticker="SECOND")
        plan = _plan(conn, schema, limits)
        fake = FakeProvider()
        result = _run(conn, plan, [first, second], fake, limits=limits)
        assert result["exit_code"] == 5, result
        assert result["status"] == "stopped"
        assert result["changed_rows"] == 1
        assert result["requests"] == 1
        remaining = _plan(conn, schema, limits)
        assert len(remaining["items"]) == 1
        remaining_id = remaining["items"][0]["instrument_id"]
        resumed = _run(conn, remaining, [remaining_id], fake, limits=limits)
        assert resumed["exit_code"] == 0, resumed
        assert resumed["changed_rows"] == 1
        assert _plan(conn, schema, limits)["items"] == []
        assert _ledger(conn) == (2, 2, 2, 2, 2, 0)


def test_validate_only_fetches_without_writing_ledgers(test_dsn, schema):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, _ = _seed(conn)
        before = _rows(conn, iid)
        plan = _plan(conn, schema, _limits())
        fake = FakeProvider()
        result = _run(conn, plan, [iid], fake, validate_only=True)
        assert result["exit_code"] == 0, result
        assert result["requests"] == 1
        assert result["changed_rows"] == 0
        assert len(fake.calls) == 1
        assert _rows(conn, iid) == before
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)
        assert len(_plan(conn, schema, _limits())["items"]) == 1


def test_two_dates_share_final_head_evidence_and_one_transaction(test_dsn, schema):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, day = _seed(conn)
        _add_earlier_bad_date(conn, iid, day)
        before = _rows(conn, iid)
        plan = _plan(conn, schema, _limits())
        fake = FakeProvider()
        result = _run(conn, plan, [iid], fake)
        assert result["exit_code"] == 0, result
        assert result["changed_rows"] == 2
        assert result["requests"] == 2
        expected = [dict(row) for row in before]
        for index in (1, 3):
            expected[index]["return_source_boundary"] = None
        assert _rows(conn, iid) == expected
        assert _ledger(conn) == (2, 2, 2, 2, 1, 0)
        evidence = conn.execute(
            """SELECT count(*),bool_and(e.revision_head=h.revision_id),
                      count(DISTINCT e.commit_xid),bool_and(e.commit_xid=a.commit_xid)
               FROM nav_ingestion_row_evidence e
               JOIN fund_nav_data_heads h ON h.instrument_id=e.instrument_id
               JOIN nav_ingestion_attempts a ON a.instrument_id=e.instrument_id
                 AND a.run_id=e.run_id AND a.provider=e.provider
               WHERE e.instrument_id=%s""", (iid,),
        ).fetchone()
        assert evidence == (2, True, 1, True)
        now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        for repaired_day in (day - dt.timedelta(days=2), day):
            with conn.cursor(row_factory=dict_row) as cur:
                verified, lineage = readiness._per_date_lineage(
                    cur, iid, [repaired_day], repaired_day, now,
                )
            assert verified, lineage


def test_unexpected_neighbour_change_rolls_back_nav_and_all_ledgers(
    test_dsn, schema, monkeypatch,
):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, day = _seed(conn)
        before = _rows(conn, iid)
        plan = _plan(conn, schema, _limits())
        original = ingest._write_instrument_nav_tx

        def corrupt_neighbour(connection, rows, **kwargs):
            written = original(connection, rows, **kwargs)
            connection.execute(
                "UPDATE nav_timeseries SET return_1d=return_1d+0.001 "
                "WHERE instrument_id=%s AND nav_date=%s",
                (iid, day + dt.timedelta(days=1)),
            )
            return written

        monkeypatch.setattr(ingest, "_write_instrument_nav_tx", corrupt_neighbour)
        result = _run(conn, plan, [iid], FakeProvider())
        assert result["exit_code"] == 2, result
        assert result["code"] == "UNEXPECTED_NAV_CHANGE"
        assert result["changed_rows"] == 0
        assert _rows(conn, iid) == before
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)


def _change_stored_level(dsn, schema, iid, day):
    with base._connect(dsn, schema) as other:
        other.execute("SET LOCAL session_replication_role = replica")
        other.execute(
            "UPDATE nav_timeseries SET source='yahoo',nav=150,source_nav=150 "
            "WHERE instrument_id=%s AND nav_date=%s", (iid, day),
        )
        other.commit()


@pytest.mark.parametrize("changed", ["before_apply", "during_fetch"])
def test_stale_instrument_is_skipped_and_the_batch_continues(test_dsn, schema, changed):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        first, day = _seed(conn, ticker="FIRST")
        second, _ = _seed(conn, ticker="SECOND")
        plan = _plan(conn, schema, _limits())

        class ConcurrentChange(FakeProvider):
            def fetch(self, provider, ticker, start, end, *, remaining):
                if ticker == "FIRST":  # committed while phase A holds no lock
                    _change_stored_level(test_dsn, schema, first, day)
                return super().fetch(provider, ticker, start, end, remaining=remaining)

        fake = ConcurrentChange() if changed == "during_fetch" else FakeProvider()
        if changed == "before_apply":
            _change_stored_level(test_dsn, schema, first, day)
        # An upper-case allowlist entry is the same instrument once normalized.
        result = _run(conn, plan, [str(first).upper(), second], fake)
        assert result["exit_code"] == 0, result
        assert result["status"] == "completed"
        assert [(o["instrument_id"], o["status"], o["code"]) for o in result["instruments"]] == [
            (str(first), "skipped", "PLAN_STALE"), (str(second), "committed", None)]
        assert result["residual_counts"] == {"PLAN_STALE": 1}
        assert result["changed_rows"] == 1
        assert result["unprocessed_instruments"] == 0
        tickers = [ticker for _, ticker, _, _ in fake.calls]
        assert tickers == (["FIRST", "SECOND"] if changed == "during_fetch" else ["SECOND"])
        stale_row = next(r for r in _rows(conn, first) if r["nav_date"] == day.isoformat())
        assert (stale_row["nav"], stale_row["return_source_boundary"]) == (150, True)
        assert _ledger(conn) == (1, 1, 1, 1, 1, 0)
        assert conn.execute(
            "SELECT count(*) FROM nav_ingestion_attempts WHERE instrument_id=%s", (first,),
        ).fetchone()[0] == 0


def test_daily_ingestion_moving_head_and_last_date_does_not_stale_the_plan(
    test_dsn, schema,
):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, day = _seed(conn)
        plan = _plan(conn, schema, _limits())
        base._provider_write(conn, ingest.build_rows(
            (NavObservation(day + dt.timedelta(days=2), 103.0, "adjusted"),), [(iid, "USD")]))
        approved, current = plan["items"][0], _plan(conn, schema, _limits())["items"][0]
        assert current["revision_head"] != approved["revision_head"]
        assert current["last_nav_date"] != approved["last_nav_date"]
        assert current != approved
        before = _rows(conn, iid)
        result = _run(conn, plan, [iid], FakeProvider())
        assert result["exit_code"] == 0, result
        assert result["instruments"][0]["status"] == "committed"
        assert result["changed_rows"] == 1
        expected = [dict(row) for row in before]
        expected[1]["return_source_boundary"] = None
        assert _rows(conn, iid) == expected


def test_provider_fetch_runs_outside_the_writer_locks(test_dsn, schema, monkeypatch):
    keys = [LOCK_INSTRUMENT_INGESTION, LOCK_FUND_NAV_READINESS]
    held = ("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND pid=%s "
            "AND classid=0 AND objid::bigint = ANY(%s)")
    events = []
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, day = _seed(conn)
        _add_earlier_bad_date(conn, iid, day)
        plan = _plan(conn, schema, _limits())
        pid = conn.info.backend_pid

        class LockProbe(FakeProvider):
            def fetch(self, provider, ticker, start, end, *, remaining):
                with psycopg.connect(test_dsn, autocommit=True) as probe:
                    locks = probe.execute(held, (pid, keys)).fetchone()[0]
                    state = probe.execute(
                        "SELECT state FROM pg_stat_activity WHERE pid=%s", (pid,),
                    ).fetchone()[0]
                events.append(("fetch", locks, state))
                return super().fetch(provider, ticker, start, end, remaining=remaining)

        def probed(name):
            original = getattr(ingest, name)

            def wrapper(connection, *args, **kwargs):
                own = connection.execute(held, (pid, keys)).fetchone()[0]
                events.append((name, own, None))
                return original(connection, *args, **kwargs)

            monkeypatch.setattr(ingest, name, wrapper)

        for name in ("_insert_attempt_tx", "_write_instrument_nav_tx", "_insert_row_evidence_tx"):
            probed(name)
        result = _run(conn, plan, [iid], LockProbe())
        assert result["exit_code"] == 0, result
        assert result["changed_rows"] == 2
        assert events[:2] == [("fetch", 0, "idle")] * 2
        assert [e[0] for e in events[2:]] == [
            "_insert_attempt_tx", "_write_instrument_nav_tx",
            "_insert_attempt_tx", "_write_instrument_nav_tx",
            "_insert_row_evidence_tx", "_insert_row_evidence_tx"]
        assert {e[1] for e in events[2:]} == {2}
        with psycopg.connect(test_dsn, autocommit=True) as probe:
            assert probe.execute(held, (pid, keys)).fetchone()[0] == 0


def _calibrated_clock(conn, offset):
    """This host's clock shifted so it reads ``offset`` ahead of the database."""
    real = repair._clock_skew(conn, repair._utc_now)
    return lambda: repair._utc_now() - real + offset


@pytest.mark.parametrize("validate_only", [False, True])
def test_host_clock_ahead_of_database_is_refused_before_any_fetch(
    test_dsn, schema, validate_only,
):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, _ = _seed(conn)
        before = _rows(conn, iid)
        plan = _plan(conn, schema, _limits())
        fake = FakeProvider()
        clock = _calibrated_clock(conn, dt.timedelta(milliseconds=400))
        result = _run(conn, plan, [iid], fake, clock=clock, validate_only=validate_only)
        assert result["exit_code"] == 2, result
        assert (result["status"], result["code"]) == ("failed", "CLOCK_SKEW")
        assert 250 < result["clock_skew_ms"] < 600
        assert result["requests"] == 0 and fake.calls == []
        assert result["instruments"] == [] and result["unprocessed_instruments"] == 1
        assert _rows(conn, iid) == before
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)


@pytest.mark.parametrize("offset_ms", [-5000, 100])
def test_host_clock_behind_or_within_tolerance_applies(test_dsn, schema, offset_ms):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, _ = _seed(conn)
        plan = _plan(conn, schema, _limits())
        clock = _calibrated_clock(conn, dt.timedelta(milliseconds=offset_ms))
        result = _run(conn, plan, [iid], FakeProvider(), clock=clock)
        assert result["exit_code"] == 0, result
        assert result["changed_rows"] == 1
        assert abs(result["clock_skew_ms"] - offset_ms) < 100


def test_interrupt_after_actual_commit_reports_unknown_with_reconciliation_ids(
    test_dsn, schema,
):
    class CommitAcknowledgementLost:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def commit(self):
            self.connection.commit()
            raise KeyboardInterrupt

    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, _ = _seed(conn)
        before = _rows(conn, iid)
        plan = _plan(conn, schema, _limits())
        result = _run(CommitAcknowledgementLost(conn), plan, [iid], FakeProvider())
        assert result["exit_code"] == 2, result
        assert result["code"] == "COMMIT_UNKNOWN"
        assert result["changed_rows"] == 0
        assert result["possibly_changed_rows"] == 1
        outcome = result["instruments"][0]
        assert outcome["status"] == "unknown"
        assert outcome["code"] == "COMMIT_UNKNOWN"
        persisted = conn.execute("SELECT run_id FROM nav_ingestion_runs").fetchone()[0]
        assert outcome["run_ids"] == [str(persisted)]
        expected = [dict(row) for row in before]
        expected[1]["return_source_boundary"] = None
        assert _rows(conn, iid) == expected
        assert _ledger(conn) == (1, 1, 1, 1, 1, 0)


def test_cli_plan_validate_apply_with_real_schema_check(
    test_dsn, schema, monkeypatch, tmp_path, capsys,
):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        iid, _ = _seed(conn)
        before = _rows(conn, iid)
        conn.rollback()
        monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
        plan_file = tmp_path / "reviewed-plan.json"
        common = ["--schema", schema, "--plan-file", str(plan_file)]
        clients = []

        def factory(_limits):
            client = FakeProvider()
            clients.append(client)
            return client

        def invoke(args):
            exit_code = cli.main(args, client_factory=factory)
            captured = capsys.readouterr()
            output = json.loads(captured.out)
            assert captured.out == json.dumps(output, sort_keys=True) + "\n"
            assert captured.err == ""
            assert exit_code == 0, output
            assert test_dsn not in captured.out
            return output

        capsys.readouterr()
        planned = invoke(common)
        assert planned["status"] == "planned"
        assert planned["rows"] == 1
        assert planned["instruments"] == 1
        assert clients == [], "plan must never construct a provider"
        artifact = plan_file.read_bytes()
        plan = json.loads(artifact)
        assert artifact == repair.plan_bytes(plan)
        assert planned["plan_sha256"] == repair.sha256(plan)
        assert hashlib.sha256(artifact).hexdigest() == planned["plan_sha256"]
        assert plan["schema"] == schema
        assert plan["schema_pins"]
        assert _rows(conn, iid) == before
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)
        conn.rollback()

        apply_args = common + [
            "--mode", "apply", "--plan-sha256", planned["plan_sha256"],
            "--instrument-id", str(iid), "--confirm", repair.CONFIRM_TOKEN,
        ]
        validated = invoke(apply_args + ["--validate-only"])
        assert validated["validate_only"] is True
        assert validated["validated_rows"] == 1
        assert validated["changed_rows"] == 0
        assert len(clients) == 1 and len(clients[0].calls) == 1
        assert _rows(conn, iid) == before
        assert _ledger(conn) == (0, 0, 0, 0, 0, 0)
        conn.rollback()

        applied = invoke(apply_args)
        assert applied["validate_only"] is False
        assert applied["changed_rows"] == 1
        assert len(clients) == 2 and len(clients[1].calls) == 1
        expected = [dict(row) for row in before]
        expected[1]["return_source_boundary"] = None
        assert _rows(conn, iid) == expected
        assert _ledger(conn) == (1, 1, 1, 1, 1, 0)
        assert plan_file.read_bytes() == artifact

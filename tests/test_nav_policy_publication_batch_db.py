"""Publication batching preserves the real PostgreSQL governance boundary."""

from __future__ import annotations

import copy
import json
import uuid

import pytest
from psycopg.pq import TransactionStatus
from test_fund_nav_readiness_db import (
    _bootstrap,
    _connect,
    _grid,
    _ops_env,
    _plan,
    _policy_document,
    _unpublished_version,
    _w1_state,
)
from test_fund_nav_readiness_db import schema as _schema
from test_fund_nav_readiness_db import test_dsn as _test_dsn

from scripts import fund_nav_readiness_schema as operator
from src.db import LOCK_INSTRUMENT_INGESTION, advisory_lock

schema = _schema
test_dsn = _test_dsn


class _CountedConnection:
    def __init__(self, conn):
        self.conn = conn
        self.statements = []

    def execute(self, statement, *args, **kwargs):
        self.statements.append(statement)
        return self.conn.execute(statement, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.conn, name)


def _cohort(size):
    document = _policy_document(_grid(), uuid.uuid4())
    first = document["instrument_evidence"][0]
    document["instrument_evidence"] = [
        {**first, "instrument_id": str(uuid.uuid4())} for _ in range(size)
    ]
    return document


def test_publication_and_replay_roundtrips_do_not_scale_with_cohort(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    counts = []
    with _connect(test_dsn, schema) as conn:
        counted = _CountedConnection(conn)
        for size in (1, 512):
            document = _cohort(size)
            document["policy_version"] = f"batch-{size}"
            counted.statements.clear()
            _hash, changed = operator._publish_policy(counted, document)
            assert changed
            # This is a network round-trip bound, not an elapsed-time benchmark.
            assert len(counted.statements) <= 24
            counts.append(len(counted.statements))
            counted.statements.clear()
            assert operator._policy_facts_exact(counted, document)
            assert len(counted.statements) <= 10
            stamp = conn.execute("SELECT published_at FROM nav_policy_current").fetchone()
            counted.statements.clear()
            assert operator._publish_policy(counted, document) == (_hash, False)
            assert len(counted.statements) <= 24
            assert conn.execute("SELECT published_at FROM nav_policy_current").fetchone() == stamp
            altered = copy.deepcopy(document)
            altered["instrument_evidence"][-1]["evidence_reference"] = "unverified-change"
            assert not operator._policy_facts_exact(conn, altered)
        assert counts[0] == counts[1]


def test_copy_finishes_before_writer_locks_and_lock_busy_leaves_no_shared_writes(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    _iid, _dates, _combined, policy_only = _ops_env(
        test_dsn, schema, tmp_path, monkeypatch
    )
    plan = _plan(policy_only, capsys)
    baseline = _w1_state(test_dsn, schema)
    real_stage = operator._stage_policy
    boundaries = []

    def staged(conn, evidence):
        boundaries.append(conn.info.transaction_status)
        result = real_stage(conn, evidence)
        boundaries.append(conn.info.transaction_status)
        assert _w1_state(test_dsn, schema) == baseline
        return result

    monkeypatch.setattr(operator, "_stage_policy", staged)
    with (
        _connect(test_dsn, schema, autocommit=True) as holder,
        advisory_lock(holder, LOCK_INSTRUMENT_INGESTION),
    ):
        assert operator.main([
            *policy_only, "--mode", "apply", "--plan-sha256", plan
        ]) == operator.EXIT_LOCK_BUSY
    assert boundaries == [TransactionStatus.IDLE, TransactionStatus.IDLE]
    assert _w1_state(test_dsn, schema) == baseline


def test_batch_evidence_conflict_rolls_back_without_repointing(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    document = _cohort(3)
    with _connect(test_dsn, schema) as conn:
        operator._publish_policy(conn, document)
    baseline = _w1_state(test_dsn, schema)
    altered = copy.deepcopy(document)
    altered["instrument_evidence"].insert(0, {
        **altered["instrument_evidence"][0], "instrument_id": str(uuid.uuid4())
    })
    altered["instrument_evidence"][-1]["evidence_reference"] = "conflicting-reference"
    with (
        pytest.raises(ValueError, match="immutable_instrument_evidence_conflict"),
        _connect(test_dsn, schema) as conn,
    ):
        operator._publish_policy(conn, altered)
    assert _w1_state(test_dsn, schema) == baseline


def test_staged_document_must_match_the_publication_document(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    document = _cohort(2)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        operator._stage_policy(conn, document)
        document["instrument_evidence"][0]["evidence_reference"] = "different-bytes"
        with (
            pytest.raises(ValueError, match="policy_stage_document_mismatch"),
            conn.transaction(),
        ):
            operator._publish_policy_tx(conn, document)
        assert conn.execute("SELECT count(*) FROM nav_policy_versions").fetchone() == (0,)


def test_state_changed_during_copy_is_revalidated_after_writer_locks(
    test_dsn, schema, tmp_path, monkeypatch, capsys
):
    _iid, _dates, _combined, policy_only = _ops_env(
        test_dsn, schema, tmp_path, monkeypatch
    )
    plan = _plan(policy_only, capsys)
    real_stage = operator._stage_policy

    def stage_then_change(conn, evidence):
        real_stage(conn, evidence)
        _unpublished_version(test_dsn, schema, policy_only[-1])

    monkeypatch.setattr(operator, "_stage_policy", stage_then_change)
    assert operator.main([
        *policy_only, "--mode", "apply", "--plan-sha256", plan
    ]) == operator.EXIT_BLOCKED
    result = json.loads(capsys.readouterr().out)
    assert result["code"] == "PLAN_STALE"
    assert result["policy"] == "rolled_back"
    assert result["dml_committed"] is False
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT (SELECT count(*) FROM nav_policy_current), "
            "(SELECT count(*) FROM nav_valuation_schedules), "
            "(SELECT count(*) FROM nav_instrument_policy_evidence), "
            "(SELECT count(*) FROM nav_policy_publication_receipts)"
        ).fetchone() == (0, 0, 0, 0)


def test_calendar_insert_race_cannot_adopt_a_conflicting_source(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    document = _cohort(2)

    class CalendarRace(_CountedConnection):
        def execute(self, statement, *args, **kwargs):
            if isinstance(statement, str) and statement.startswith(
                "INSERT INTO nav_valuation_schedules"
            ):
                first = document["sessions"][0]
                with _connect(test_dsn, schema) as other:
                    other.execute(
                        "INSERT INTO nav_valuation_schedules VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (
                            document["calendar_id"], document["calendar_version"],
                            first["session_date"], first["valuation_close_at"],
                            first["nav_due_at"], "conflicting-source",
                            document["source_reference"],
                        ),
                    )
            return super().execute(statement, *args, **kwargs)

    with (
        pytest.raises(ValueError, match="immutable_session_conflict"),
        _connect(test_dsn, schema) as conn,
    ):
        operator._publish_policy(CalendarRace(conn), document)
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT (SELECT count(*) FROM nav_policy_versions), "
            "(SELECT count(*) FROM nav_policy_current), "
            "(SELECT count(*) FROM nav_instrument_policy_evidence)"
        ).fetchone() == (0, 0, 0)
        assert conn.execute("SELECT count(*) FROM nav_valuation_schedules").fetchone() == (1,)

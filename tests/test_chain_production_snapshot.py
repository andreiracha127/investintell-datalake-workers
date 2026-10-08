"""Synthetic connections only: production URLs and credentials are never used."""
from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import chain_production_snapshot as snapshot
from src.input_packs.hashing import canonical_json_sha256
from src.workers import open_macro_v03_chain as worker

PACK_SHA = "a" * 64
BOUNDARY = datetime(2014, 3, 31, tzinfo=UTC)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        conn = self.connection
        conn.calls.append((sql, params))
        if sql.startswith("BEGIN"):
            assert not conn.active
            conn.active = True
        elif sql == "ROLLBACK":
            assert conn.active
            conn.active = False
        else:
            assert conn.active, "Every query must belong to the single transaction"
        if "current_setting('transaction_read_only')" in sql:
            self.result = [(conn.read_only, conn.isolation)]
        elif sql == snapshot.ROLE_SQL:
            self.result = [("analytics_ro", params[0] == conn.write_relation)]
        elif "pg_current_snapshot" in sql:
            self.result = [("10:15:12",)]
        elif sql == worker.READ_CHAIN_SQL:
            self.result = [(date(2014, 3, 31), "Q1", "Q1", "valid",
                            Decimal("0.7"), Decimal("0.1"), Decimal("0.2"),
                            Decimal(1), False, worker.BASIS, PACK_SHA,
                            worker.CHAIN_START)]
        elif sql == worker.ARM_FRESHNESS_SQL:
            self.result = [(arm, datetime(2014, 5, 1, tzinfo=UTC))
                           for arm in worker.arm_series_ids()]
        elif sql == worker.MARKET_FRESHNESS_SQL:
            self.result = [(date(2014, 5, 1),)]
        elif sql == worker.MACRO_DELTA_SQL:
            self.result = [(worker.arm_series_ids()[0], date(2014, 4, 1),
                            datetime(2014, 4, 15, tzinfo=UTC), date(2014, 4, 15),
                            2, Decimal("101.25"))]
        elif sql == worker.EOD_DELTA_SQL:
            self.result = [(date(2014, 4, 1), Decimal("101.5"))]

    def fetchone(self):
        return self.result[0]

    def fetchall(self):
        return self.result


class FakeConnection:
    def __init__(self, *, read_only="on", isolation="repeatable read", write_relation=None):
        self.read_only = read_only
        self.isolation = isolation
        self.write_relation = write_relation
        self.calls = []
        self.active = False
        self.connect_calls = []

    def connect(self, *args, **kwargs):
        self.connect_calls.append((args, kwargs))
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        assert not self.active
        return False

    def cursor(self):
        return FakeCursor(self)


@pytest.fixture
def pack(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://analytics_ro:fake@localhost/test")
    macro = [{"series_id": worker.arm_series_ids()[0], "observation_period": "2014-03-01",
              "available_at": BOUNDARY.isoformat(), "vintage_date": "2014-03-31",
              "revision_number": 1, "value": 100.0}]
    eod = [{"ticker": worker.market_ticker(), "date": "2014-03-31", "adjusted_close": 100.0}]
    calls = Counter()

    def load_pack():
        calls["load_pack_inputs"] += 1
        return macro.copy(), eod.copy(), BOUNDARY, BOUNDARY.date()

    monkeypatch.setattr(worker, "verify_pack", lambda: {
        "input_pack_id": "synthetic-pack", "input_pack_sha256": PACK_SHA,
    })
    monkeypatch.setattr(worker, "load_pack_inputs", load_pack)
    monkeypatch.setattr(snapshot, "input_contract_sha256", lambda: "b" * 64)
    return SimpleNamespace(macro=macro, eod=eod, calls=calls)


@pytest.mark.parametrize("relation", snapshot.RELATIONS)
def test_refuses_write_capable_role_before_reading_inputs(pack, relation):
    conn = FakeConnection(write_relation=relation)
    with pytest.raises(snapshot.SnapshotRefused, match="can write"):
        snapshot.capture_snapshot(date(2014, 5, 2), connect=conn.connect)
    assert worker.READ_CHAIN_SQL not in [sql for sql, _ in conn.calls]
    assert pack.calls["load_pack_inputs"] == 0
    assert conn.calls[-1][0] == "ROLLBACK"


@pytest.mark.parametrize("read_only,isolation", [("off", "repeatable read"), ("on", "read committed")])
def test_refuses_session_that_is_not_read_only_repeatable_read(pack, read_only, isolation):
    conn = FakeConnection(read_only=read_only, isolation=isolation)
    with pytest.raises(snapshot.SnapshotRefused, match="REPEATABLE READ READ ONLY"):
        snapshot.capture_snapshot(date(2014, 5, 2), connect=conn.connect)
    assert snapshot.ROLE_SQL not in [sql for sql, _ in conn.calls]
    assert conn.calls[-1][0] == "ROLLBACK"


@pytest.mark.parametrize("constant", ["CHAIN_TABLE", "VINTAGE_TABLE", "EOD_TABLE"])
def test_refuses_unreviewed_relation_before_connecting(pack, monkeypatch, constant):
    monkeypatch.setattr(worker, constant, "unreviewed_relation")
    conn = FakeConnection()
    with pytest.raises(snapshot.SnapshotRefused, match="privilege-check coverage"):
        snapshot.capture_snapshot(date(2014, 5, 2), connect=conn.connect)
    assert conn.connect_calls == []


def test_failed_input_read_rolls_back_the_only_transaction(pack, monkeypatch):
    def fail_read(_connection, _boundary):
        raise RuntimeError("synthetic failed read")

    monkeypatch.setattr(worker, "read_macro_delta", fail_read)
    conn = FakeConnection()
    with pytest.raises(RuntimeError, match="synthetic failed read"):
        snapshot.capture_snapshot(date(2014, 5, 2), connect=conn.connect)
    assert conn.calls[-1][0] == "ROLLBACK"
    assert not conn.active
    assert len(conn.connect_calls) == 1


def test_loads_once_in_one_transaction_and_matches_worker_assembly(pack):
    conn = FakeConnection()
    captured = snapshot.capture_snapshot(date(2014, 5, 2), connect=conn.connect)
    sql_counts = Counter(sql for sql, _ in conn.calls)
    assert len(conn.connect_calls) == 1
    assert conn.connect_calls[0][1] == {
        "options": "-c default_transaction_read_only=on -c statement_timeout=120000",
        "autocommit": True,
    }
    assert sql_counts["BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"] == 1
    assert sql_counts["ROLLBACK"] == 1
    queries = [sql for sql, _ in conn.calls]
    assert max(i for i, query in enumerate(queries) if query == snapshot.ROLE_SQL) < (
        queries.index(worker.READ_CHAIN_SQL)
    )
    for query in (worker.READ_CHAIN_SQL, worker.MACRO_DELTA_SQL, worker.EOD_DELTA_SQL,
                  worker.ARM_FRESHNESS_SQL, worker.MARKET_FRESHNESS_SQL):
        assert sql_counts[query] == 1
    assert pack.calls["load_pack_inputs"] == 1
    macro_params = next(params for sql, params in conn.calls if sql == worker.MACRO_DELTA_SQL)
    assert macro_params == {"ids": worker.arm_series_ids(), "boundary": BOUNDARY}
    eod_params = next(params for sql, params in conn.calls if sql == worker.EOD_DELTA_SQL)
    assert eod_params == {"ticker": worker.market_ticker(), "boundary": BOUNDARY.date()}

    # Invoke exactly the worker's input assembly on the same synthetic DB rows.
    # This checks normalization (UTC, date strings, Decimal -> float, revision)
    # and pack-before-delta ordering, without invoking run or any write function.
    baseline = FakeConnection()
    baseline.active = True
    expected_macro = pack.macro + worker.read_macro_delta(baseline, BOUNDARY)
    expected_eod = pack.eod + worker.read_eod_delta(baseline, BOUNDARY.date())
    assert captured["inputs"]["macro_rows"] == expected_macro
    assert captured["inputs"]["eod_rows"] == expected_eod
    assert captured["reference_date"] == "2014-05-02"
    assert captured["target_date"] == "2014-04-30"
    assert captured["cron_status"] == "ready"
    assert captured["pointers"]["publication_ids"] == []
    assert captured["pointers"]["chain_latest"] == "2014-03-31"
    assert captured["row_counts"]["macro_rows"] == 2
    assert captured["row_counts"]["stored_rows"] == 1
    assert captured["inputs_sha256"] == canonical_json_sha256(captured["inputs"])
    for name, rows in captured["inputs"].items():
        assert captured["input_digests"][name] == canonical_json_sha256(rows)


def test_caught_up_cron_is_explicit_and_snapshot_still_has_inputs(pack):
    captured = snapshot.capture_snapshot(date(2014, 4, 15), connect=FakeConnection().connect)
    assert captured["cron_status"] == "month_in_progress"
    assert captured["horizon_date"] == "2014-03-31"
    assert captured["inputs"]["macro_rows"]


def test_dsn_only_from_environment_and_internal_host_rewrite(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://ro:p%40ss@db.railway.internal:5432/db?sslmode=require&host=wrong&port=12")
    assert snapshot.database_url() == (
        "postgresql://ro:p%40ss@centerbeam.proxy.rlwy.net:36616/db?sslmode=require"
    )
    monkeypatch.setenv("DATABASE_URL", "postgresql://ro:fake@localhost:5433/db")
    assert snapshot.database_url() == "postgresql://ro:fake@localhost:5433/db"
    monkeypatch.delenv("DATABASE_URL")
    with pytest.raises(snapshot.SnapshotRefused, match="DATABASE_URL"):
        snapshot.database_url()


def test_contract_changes_when_a_reader_query_changes(monkeypatch):
    original = snapshot.input_contract_sha256(worker)
    monkeypatch.setattr(worker, "EOD_DELTA_SQL", worker.EOD_DELTA_SQL + " LIMIT 10")
    assert snapshot.input_contract_sha256(worker) != original


def test_contract_excludes_pure_computation_changes(monkeypatch):
    original = snapshot.input_contract_sha256(worker)
    monkeypatch.setattr(worker, "compute_series", lambda *_: [])
    assert snapshot.input_contract_sha256(worker) == original


@pytest.mark.parametrize("old,new", [
    ("target = next_month_end(latest)", "target = latest"),
    ('MACRO_JSON = PACK / "data"', 'MACRO_JSON = PACK / "other_data"'),
])
def test_contract_detects_assembly_and_pack_path_changes(monkeypatch, tmp_path, old, new):
    original = snapshot.input_contract_sha256(worker)
    source = Path(worker.__file__).read_text(encoding="utf-8")
    assert old in source
    changed_source = tmp_path / "changed_worker.py"
    changed_source.write_text(source.replace(old, new), encoding="utf-8")
    monkeypatch.setattr(worker, "__file__", str(changed_source))
    assert snapshot.input_contract_sha256(worker) != original

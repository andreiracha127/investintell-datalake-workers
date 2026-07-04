"""Unit tests for the open_macro_v03 B5 monitor (missing_output_slo + guards).

No real Postgres: the pre-activation/weekend short-circuits need no DB, and every
DB-touching path is driven through a duck-typed fake conn. The alert mechanism is
a raised OpenMacroV03MonitorAlert (non-zero exit = the alarm)."""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

import src.workers.open_macro_v03_monitor as mon

AS_OF = "2026-07-06"  # Monday
AS_OF_DATE = _dt.date(2026, 7, 6)
FUTURE_VU = _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(hours=6)
PAST_VU = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(hours=1)


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn
        self._rows: list = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None) -> None:
        self.conn.executed.append((sql, params))
        self._rows = self.conn.responder(sql, params)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, responder) -> None:
        self.executed: list = []
        self.responder = responder
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True


def _responder(*, decision=None, allocation=None, ledger=None, future=0,
               incoherent=0, schema_ok=True):
    """Build a SQL-shape-aware responder for the monitor's read-only queries."""
    def responder(sql, params):
        if "to_regclass" in sql:
            return [(params[0] if schema_ok else None,)]
        if "invalidated_at IS NULL" in sql:
            return [(incoherent,)]
        if "as_of > " in sql:
            return [(future,)]
        if "open_macro_v03_staleness_blocks" in sql:
            return [ledger] if ledger is not None else []
        if "open_macro_v03_decisions" in sql:
            return [decision] if decision is not None else []
        if "open_macro_v03_allocations" in sql:
            return [allocation] if allocation is not None else []
        raise AssertionError(f"unexpected SQL: {sql}")
    return responder


def _arm(monkeypatch, responder):
    """Activate the monitor (envelope flip simulated) over a fake conn."""
    monkeypatch.setattr(mon, "_load_json", lambda path: {"runtime_activation": True})
    conn = _FakeConn(responder)
    monkeypatch.setattr(mon, "connect", lambda dsn: conn)
    return conn


def _published(vu=FUTURE_VU):
    return ("published", "valid", vu)


# --------------------------------------------------------------------------- #
# Pre-activation / non-business-day short-circuits
# --------------------------------------------------------------------------- #
def test_pre_activation_with_committed_blocked_envelope_no_db(monkeypatch):
    # the REAL committed envelope is fully blocked -> pre_activation, no DB
    def _no_connect(*a, **k):
        raise AssertionError("must not connect pre-activation")

    monkeypatch.setattr(mon, "connect", _no_connect)
    assert mon.run("unused-dsn") == {"status": "pre_activation"}


def test_non_business_day_exits_clean_without_db(monkeypatch):
    monkeypatch.setattr(mon, "_load_json", lambda path: {"runtime_activation": True})
    monkeypatch.setattr(mon, "resolve_as_of", lambda *a, **k: None)

    def _no_connect(*a, **k):
        raise AssertionError("must not connect on a weekend")

    monkeypatch.setattr(mon, "connect", _no_connect)
    assert mon.run("unused-dsn") == {"status": "non_business_day"}


# --------------------------------------------------------------------------- #
# Legal states
# --------------------------------------------------------------------------- #
def test_healthy_pair_exits_clean(monkeypatch):
    conn = _arm(monkeypatch, _responder(decision=_published(), allocation=_published()))
    result = mon.run("dsn", as_of=AS_OF)
    assert result["status"] == "healthy"
    assert result["as_of"] == AS_OF
    assert result["checks"]["missing_output_slo"] == "pass"
    assert conn.closed


def test_justified_staleness_block_exits_clean(monkeypatch):
    ledger = ("v" * 64, "p" * 64)
    conn = _arm(monkeypatch, _responder(ledger=ledger))
    monkeypatch.setattr(mon, "compose_inputs", lambda c, d: (["vr"], ["pr"]))
    monkeypatch.setattr(mon, "staleness_report",
                        lambda v, p, d: {"breaches": [{"kind": "series"}]})
    monkeypatch.setattr(mon, "_canonical_sha256",
                        lambda rows: "v" * 64 if rows == ["vr"] else "p" * 64)
    result = mon.run("dsn", as_of=AS_OF)
    assert result["status"] == "staleness_block_day"
    assert result["checks"]["missing_output_slo"] == "staleness_block"
    assert result["checks"]["staleness_block_justified"] is True
    assert conn.closed


# --------------------------------------------------------------------------- #
# missing_output_slo classifications
# --------------------------------------------------------------------------- #
def test_missing_output_raises(monkeypatch):
    _arm(monkeypatch, _responder())
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="missing_output"):
        mon.run("dsn", as_of=AS_OF)


def test_partial_pair_raises(monkeypatch):
    _arm(monkeypatch, _responder(decision=_published()))
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="partial_pair"):
        mon.run("dsn", as_of=AS_OF)


def test_block_and_output_coexist_raises(monkeypatch):
    _arm(monkeypatch, _responder(decision=_published(), ledger=("v" * 64, "p" * 64)))
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="block_and_output_coexist"):
        mon.run("dsn", as_of=AS_OF)


def test_expired_current_raises(monkeypatch):
    _arm(monkeypatch, _responder(decision=_published(PAST_VU),
                                 allocation=_published(PAST_VU)))
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="expired_current"):
        mon.run("dsn", as_of=AS_OF)


def test_unpublished_row_raises(monkeypatch):
    _arm(monkeypatch, _responder(decision=("publishing", "valid", FUTURE_VU),
                                 allocation=_published()))
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="unpublished_row"):
        mon.run("dsn", as_of=AS_OF)


def test_invalidated_current_raises(monkeypatch):
    _arm(monkeypatch, _responder(decision=("published", "invalidated", FUTURE_VU),
                                 allocation=_published()))
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="invalidated_current"):
        mon.run("dsn", as_of=AS_OF)


# --------------------------------------------------------------------------- #
# Staleness-block cross-check (justified vs unjustified)
# --------------------------------------------------------------------------- #
def test_unjustified_staleness_block_raises(monkeypatch):
    _arm(monkeypatch, _responder(ledger=("v" * 64, "p" * 64)))
    monkeypatch.setattr(mon, "compose_inputs", lambda c, d: (["vr"], ["pr"]))
    monkeypatch.setattr(mon, "staleness_report", lambda v, p, d: {"breaches": []})
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="unjustified_staleness_block"):
        mon.run("dsn", as_of=AS_OF)


def test_block_input_hash_mismatch_raises(monkeypatch):
    _arm(monkeypatch, _responder(ledger=("x" * 64, "p" * 64)))  # vintage hash differs
    monkeypatch.setattr(mon, "compose_inputs", lambda c, d: (["vr"], ["pr"]))
    monkeypatch.setattr(mon, "staleness_report",
                        lambda v, p, d: {"breaches": [{"kind": "series"}]})
    monkeypatch.setattr(mon, "_canonical_sha256",
                        lambda rows: "v" * 64 if rows == ["vr"] else "p" * 64)
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="block_input_hash_mismatch"):
        mon.run("dsn", as_of=AS_OF)


# --------------------------------------------------------------------------- #
# Re-scoped attempt detectors
# --------------------------------------------------------------------------- #
def test_future_as_of_write_raises(monkeypatch):
    _arm(monkeypatch, _responder(decision=_published(), allocation=_published(),
                                 future=1))
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="future_as_of_write"):
        mon.run("dsn", as_of=AS_OF)


def test_incoherent_invalidation_raises(monkeypatch):
    _arm(monkeypatch, _responder(decision=_published(), allocation=_published(),
                                 incoherent=1))
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="incoherent_invalidation"):
        mon.run("dsn", as_of=AS_OF)


def test_schema_missing_raises_and_never_creates(monkeypatch):
    conn = _arm(monkeypatch, _responder(schema_ok=False))
    with pytest.raises(mon.OpenMacroV03MonitorAlert, match="schema_missing"):
        mon.run("dsn", as_of=AS_OF)
    # the monitor must never try to create anything
    assert all(sql.lstrip().upper().startswith("SELECT") for sql, _ in conn.executed)


# --------------------------------------------------------------------------- #
# Registration
# --------------------------------------------------------------------------- #
def test_run_worker_help_lists_the_monitor():
    root = Path(__file__).resolve().parents[1]
    text = (root / "src" / "run_worker.py").read_text(encoding="utf-8")
    assert "open_macro_v03_monitor" in text

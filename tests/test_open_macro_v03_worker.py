"""Unit tests for the open_macro_v03 Stage B runtime worker.

No real Postgres: the gate short-circuits (flag_off / governance_blocked / pin
mismatch) need no DB, and the DB-touching helpers (publish, staleness ledger,
invalidate, staleness-block run path) are driven through a duck-typed fake conn.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

import src.workers.open_macro_v03 as w


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn
        self.rowcount = -1
        self._rows: list = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None) -> None:
        self.conn.executed.append((sql, params))
        resp = self.conn.responder(sql, params)
        self.rowcount = resp.get("rowcount", 1)
        self._rows = resp.get("rows", [])

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, responder=None) -> None:
        self.executed: list = []
        self.commits = 0
        self.closed = False
        self.responder = responder or (lambda sql, params: {})

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def _lock_responder(inner=None):
    """Wrap a responder so advisory-lock acquire/release work on the fake conn."""
    def responder(sql, params):
        if "pg_try_advisory_lock" in sql:
            return {"rows": [(True,)]}
        if "pg_advisory_unlock" in sql:
            return {"rows": [(1,)]}
        return (inner or (lambda s, p: {}))(sql, params)
    return responder


@dataclass
class _FakeDecision:
    as_of: _dt.date
    quadrant: str | None
    status: str = "valid"
    candidate_confidence: float | None = 0.5
    coverage_quality: float = 0.9
    growth_score: float | None = 0.1
    inflation_score: float | None = -0.2

    def has_valid_quadrant(self) -> bool:
        return self.status == "valid" and self.quadrant is not None


# --------------------------------------------------------------------------- #
# Gate 1/2/3 short-circuits (no DB)
# --------------------------------------------------------------------------- #
def test_flag_off_is_inert_without_db(monkeypatch):
    monkeypatch.delenv("open_macro_v03_runtime_activation", raising=False)

    def _no_connect(*a, **k):
        raise AssertionError("must not connect when the flag is off")

    monkeypatch.setattr(w, "connect", _no_connect)
    assert w.run("unused-dsn") == {"status": "flag_off"}


def test_committed_blocked_envelope_blocks_without_db(monkeypatch):
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")

    def _no_connect(*a, **k):
        raise AssertionError("must not connect while governance is blocked")

    monkeypatch.setattr(w, "connect", _no_connect)
    result = w.run("unused-dsn")
    assert result["status"] == "governance_blocked"


def _active_envelope() -> dict:
    return {
        "runtime_activation": True, "activation_allowed": True, "allow_db_write": True,
        "db_write_official": True, "db_write_mode": "open_macro_v03_new_tables_only",
        "allocator_publish": True, "allow_allocator_publish": True,
        "official_result": True, "A5": "active",
        "allowed_tables": sorted(w.ALLOWED_TABLES),
        "environment": {"railway_service_name": "open-macro-v03-worker"},
    }


def test_check_governance_all_gates(monkeypatch):
    assert w.check_governance(_active_envelope()) is None
    # each single missing gate blocks
    for key, bad in [
        ("runtime_activation", False), ("activation_allowed", False),
        ("allow_db_write", False), ("db_write_official", False),
        ("db_write_mode", "none"), ("allocator_publish", False),
        ("allow_allocator_publish", False), ("official_result", False),
        ("A5", "blocked"),
    ]:
        env = _active_envelope()
        env[key] = bad
        assert w.check_governance(env) is not None, key
    # string 'true' must not spoof a boolean flip
    env = _active_envelope()
    env["runtime_activation"] = "true"
    assert w.check_governance(env) == "runtime_activation!=true"
    # allowed_tables must be exactly the three
    env = _active_envelope()
    env["allowed_tables"] = ["open_macro_v03_decisions"]
    assert w.check_governance(env) is not None
    # environment must name the service
    env = _active_envelope()
    env["environment"] = None
    assert w.check_governance(env) is not None


def test_active_envelope_pin_mismatch_raises_before_db(tmp_path, monkeypatch):
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    env_path = tmp_path / "activation_envelope.json"
    env_path.write_text(json.dumps(_active_envelope()), encoding="utf-8")
    pins_path = tmp_path / "module_pins.json"
    pins_path.write_text(json.dumps({
        "modules": {"src/quadrant_score.py": "0" * 64},  # deliberately wrong
        "module_pins_sha256": "deadbeef",
    }), encoding="utf-8")
    monkeypatch.setattr(w, "ENVELOPE_PATH", env_path)
    monkeypatch.setattr(w, "PINS_PATH", pins_path)

    def _no_connect(*a, **k):
        raise AssertionError("must not connect on a pin mismatch")

    monkeypatch.setattr(w, "connect", _no_connect)
    with pytest.raises(w.OpenMacroV03Error, match="module pin mismatch"):
        w.run("unused-dsn")


def test_verify_module_pins_accepts_the_committed_pins():
    pins = json.loads(w.PINS_PATH.read_text(encoding="utf-8"))
    w.verify_module_pins(pins, w.ROOT)  # must not raise


# --------------------------------------------------------------------------- #
# Gate 6 — as_of resolution
# --------------------------------------------------------------------------- #
def test_resolve_as_of_business_day_and_weekend():
    assert w.resolve_as_of(today=_dt.date(2026, 7, 6)) == _dt.date(2026, 7, 6)  # Mon
    assert w.resolve_as_of(today=_dt.date(2026, 7, 4)) is None                  # Sat
    assert w.resolve_as_of(today=_dt.date(2026, 7, 5)) is None                  # Sun
    assert w.resolve_as_of("2026-06-30") == _dt.date(2026, 6, 30)               # explicit


# --------------------------------------------------------------------------- #
# Gate 7 — prefix hash gate
# --------------------------------------------------------------------------- #
def test_prefix_hash_matches_pack_pin():
    pins = w.prefix_pins()
    mv = json.loads((w.PACK / "data" / "canonical" / "macro_observation_vintage.json")
                    .read_text(encoding="utf-8"))
    ep = json.loads((w.PACK / "data" / "canonical" / "eod_prices.json")
                    .read_text(encoding="utf-8"))
    w.verify_prefix_hashes(mv, ep, pins)  # must not raise


def test_prefix_hash_mismatch_raises():
    pins = w.prefix_pins()
    mv = json.loads((w.PACK / "data" / "canonical" / "macro_observation_vintage.json")
                    .read_text(encoding="utf-8"))
    tampered = list(mv)
    tampered[0] = {**tampered[0], "value": tampered[0]["value"] + 1}
    with pytest.raises(w.OpenMacroV03Error, match="prefix hash"):
        w.verify_prefix_hashes(tampered, [], pins)


# --------------------------------------------------------------------------- #
# Gate 8 — staleness breach ⇒ ledger + NO output rows (drives run())
# --------------------------------------------------------------------------- #
def _stale_inputs():
    old = "2026-01-01T00:00:00+00:00"
    vintages = [{"series_id": s, "observation_period": "2025-12-01",
                 "vintage_date": "2026-01-01", "value": 1.0, "available_at": old,
                 "revision_number": 0, "source": "alfred", "source_spec_version": "v1"}
                for s in ("ACOGNO", "AHETPI", "CPILFESL", "INDPRO", "MICH",
                          "PAYEMS", "PCEC96", "PPIFIS")]
    prices = [{"ticker": t, "date": "2026-01-02", "close": 10.0,
               "adjusted_close": 10.0, "volume": 1000}
              for t in ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")]
    return vintages, prices


def test_staleness_breach_writes_ledger_and_no_output(monkeypatch):
    monkeypatch.setenv("open_macro_v03_runtime_activation", "true")
    monkeypatch.setattr(w, "verify_module_pins", lambda *a, **k: None)
    monkeypatch.setattr(w, "_load_json", _patched_load_json(monkeypatch))
    monkeypatch.setattr(w, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(w, "compose_inputs", lambda conn, as_of: _stale_inputs())
    monkeypatch.setattr(w, "code_commit", lambda: "a" * 40)

    def responder(sql, params):
        if "SELECT 1 FROM open_macro_v03_staleness_blocks" in sql:
            return {"rows": [(1,)]}
        return {}

    conn = _FakeConn(_lock_responder(responder))
    monkeypatch.setattr(w, "connect", lambda dsn: conn)

    result = w.run("dsn", as_of="2026-07-06")
    assert result["status"] == "staleness_block"
    assert result["as_of"] == "2026-07-06"
    dml = " ".join(sql for sql, _ in conn.executed)
    assert "INSERT INTO open_macro_v03_staleness_blocks" in dml
    assert "INSERT INTO open_macro_v03_decisions" not in dml
    assert "INSERT INTO open_macro_v03_allocations" not in dml


def _patched_load_json(monkeypatch):
    real = w._load_json

    def loader(path):
        if Path(path) == Path(w.ENVELOPE_PATH):
            return _active_envelope()
        if Path(path) == Path(w.PINS_PATH):
            return {"modules": {}, "module_pins_sha256": "stub"}
        return real(path)
    return loader


# --------------------------------------------------------------------------- #
# Gate 9 — fresh vs carried
# --------------------------------------------------------------------------- #
def test_consumable_fresh_when_decided_on_as_of():
    as_of = _dt.date(2026, 6, 30)  # month-end
    chain = [_FakeDecision(_dt.date(2026, 5, 31), "expansion"),
             _FakeDecision(as_of, "slowdown")]
    last, validity, seed = w.consumable_today(chain, as_of)
    assert validity == "fresh" and seed == as_of and last.quadrant == "slowdown"


def test_consumable_carried_when_last_decision_predates_as_of():
    as_of = _dt.date(2026, 7, 6)  # not a month-end
    chain = [_FakeDecision(_dt.date(2026, 6, 30), "recovery")]
    last, validity, seed = w.consumable_today(chain, as_of)
    assert validity == "carried" and seed == _dt.date(2026, 6, 30)


# --------------------------------------------------------------------------- #
# Gate 10 — allocation weights / risk cap / defensive floor
# --------------------------------------------------------------------------- #
def _priced_rows(date="2026-06-30"):
    return [{"ticker": t, "date": date, "close": 100.0, "adjusted_close": 100.0,
             "volume": 1000} for t in ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")]


@pytest.mark.parametrize("quadrant", ["recovery", "expansion", "slowdown", "contraction"])
def test_build_allocation_respects_sum_cap_floor(quadrant):
    alloc = w.build_allocation(quadrant, _priced_rows())
    weights = alloc["weights"]
    assert abs(sum(weights.values()) - 1.0) < 1e-9
    assert alloc["risk_assets_weight"] <= 0.65 + 1e-9
    assert alloc["defensive_assets_weight"] >= 0.20 - 1e-9
    assert set(weights) == {"SPY", "TLT", "TIP", "GLD", "DBC", "SHY"}


def test_build_allocation_fails_loud_on_nan_price():
    rows = _priced_rows()
    rows[0]["adjusted_close"] = None  # SPY unusable
    with pytest.raises(w.OpenMacroV03Error, match="price gate"):
        w.build_allocation("expansion", rows)


# --------------------------------------------------------------------------- #
# Gate 11 — publish never resurrects an invalidated row
# --------------------------------------------------------------------------- #
def _decision_row():
    return {"as_of": _dt.date(2026, 6, 30), "quadrant": "expansion",
            "decision_validity": "fresh", "carry_seed_as_of": _dt.date(2026, 6, 30),
            "candidate_confidence": 0.5, "coverage_quality": 0.9, "growth_score": 0.1,
            "inflation_score": -0.2, "input_vintage_sha256": "a" * 64,
            "input_prices_sha256": "b" * 64, "pack_v2_sha256": "c" * 64,
            "module_pins_sha256": "d" * 64, "judgment_ref": "j", "threshold_ref": "t",
            "code_commit": "e" * 40, "run_id": "rid", "valid_until": _dt.datetime.now()}


def _allocation_row():
    return {"as_of": _dt.date(2026, 6, 30), "book": "compressed_50", "w_spy": 0.5,
            "w_tlt": 0.1, "w_tip": 0.1, "w_gld": 0.1, "w_dbc": 0.1, "w_shy": 0.1,
            "risk_assets_weight": 0.6, "defensive_assets_weight": 0.3, "risk_cap": 0.65,
            "defensive_floor": 0.20, "priced_at": _dt.date(2026, 6, 30),
            "input_prices_sha256": "b" * 64, "pack_v2_sha256": "c" * 64,
            "module_pins_sha256": "d" * 64, "code_commit": "e" * 40, "run_id": "rid",
            "valid_until": _dt.datetime.now()}


def test_publish_writes_both_and_commits():
    conn = _FakeConn(lambda sql, params: {"rowcount": 1})
    w.publish(conn, _decision_row(), _allocation_row())
    tables = " ".join(sql for sql, _ in conn.executed)
    assert "INSERT INTO open_macro_v03_decisions" in tables
    assert "INSERT INTO open_macro_v03_allocations" in tables
    assert conn.commits == 1


def test_publish_raises_when_row_is_invalidated():
    # decision upsert conflicts with an invalidated row (rowcount 0)
    def responder(sql, params):
        return {"rowcount": 0 if "open_macro_v03_decisions" in sql else 1}

    conn = _FakeConn(responder)
    with pytest.raises(w.OpenMacroV03Error, match="resurrect an invalidated decision"):
        w.publish(conn, _decision_row(), _allocation_row())
    assert conn.commits == 0  # never committed


# --------------------------------------------------------------------------- #
# invalidate CLI — updates both output tables, never the ledger
# --------------------------------------------------------------------------- #
def test_invalidate_updates_both_tables_not_ledger(monkeypatch):
    conn = _FakeConn(_lock_responder(lambda sql, params: {"rowcount": 2}))
    monkeypatch.setattr(w, "connect", lambda dsn: conn)
    result = w.invalidate("dsn", as_of="2026-06-30", to="2026-07-06", reason="abort")
    assert result["status"] == "invalidated"
    assert result["decisions_invalidated"] == 2
    assert result["allocations_invalidated"] == 2
    updates = [sql for sql, _ in conn.executed if sql.startswith("UPDATE")]
    assert any("open_macro_v03_decisions" in s for s in updates)
    assert any("open_macro_v03_allocations" in s for s in updates)
    assert not any("staleness_blocks" in s for s, _ in conn.executed)


# --------------------------------------------------------------------------- #
# valid_until — next business day at 14:00 UTC
# --------------------------------------------------------------------------- #
def test_valid_until_next_business_day_1400_utc():
    # Tue -> Wed
    assert w.valid_until(_dt.date(2026, 6, 30)) == \
        _dt.datetime(2026, 7, 1, 14, 0, tzinfo=_dt.timezone.utc)
    # Fri -> Mon (skips the weekend)
    assert w.valid_until(_dt.date(2026, 7, 3)) == \
        _dt.datetime(2026, 7, 6, 14, 0, tzinfo=_dt.timezone.utc)

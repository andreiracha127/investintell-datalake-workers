"""Unit tests for the Stage C independent verifier.

No real Postgres and no network: the DB reads run through a duck-typed fake conn
and the recompute/route legs are monkeypatched at the module boundary (the same
pattern as the worker/monitor tests)."""

from __future__ import annotations

import datetime as _dt
import json
import re
from pathlib import Path

import pytest

import harness.direct_activation.stage_c_verifier as sv
from src.workers.open_macro_v03 import valid_until

AS_OF = _dt.date(2026, 7, 6)  # Monday
VU = valid_until(AS_OF)

VINTAGE_SHA = "v" * 64
PRICES_SHA = "p" * 64

WEIGHTS = {"SPY": 0.39, "TLT": 0.11, "TIP": 0.14, "GLD": 0.11, "DBC": 0.11, "SHY": 0.14}


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        self._rows = self.conn.responder(sql, params)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, responder):
        self.executed = []
        self.responder = responder

    def cursor(self):
        return _FakeCursor(self)

    def close(self):
        pass


def _decision_tuple(**over):
    base = {
        "quadrant": "expansion", "decision_validity": "carried",
        "carry_seed_as_of": _dt.date(2026, 6, 30), "candidate_confidence": 0.81,
        "coverage_quality": 1.0, "growth_score": 0.2, "inflation_score": -0.1,
        "input_vintage_sha256": VINTAGE_SHA, "input_prices_sha256": PRICES_SHA,
        "publish_state": "published", "valid_status": "valid", "valid_until": VU,
    }
    base.update(over)
    return tuple(base[c] for c in sv._DECISION_COLS)


def _allocation_tuple(**over):
    base = {
        "book": "compressed_50", "w_spy": WEIGHTS["SPY"], "w_tlt": WEIGHTS["TLT"],
        "w_tip": WEIGHTS["TIP"], "w_gld": WEIGHTS["GLD"], "w_dbc": WEIGHTS["DBC"],
        "w_shy": WEIGHTS["SHY"], "risk_assets_weight": 0.50,
        "defensive_assets_weight": 0.39, "priced_at": _dt.date(2026, 7, 2),
        "input_prices_sha256": PRICES_SHA, "publish_state": "published",
        "valid_status": "valid", "valid_until": VU,
    }
    base.update(over)
    return tuple(base[c] for c in sv._ALLOCATION_COLS)


def _ledger_tuple(**over):
    base = {"reason": "staleness SLO breach: MICH",
            "input_vintage_sha256": VINTAGE_SHA, "input_prices_sha256": PRICES_SHA}
    base.update(over)
    return tuple(base[c] for c in sv._LEDGER_COLS)


def _responder(*, decision=None, allocation=None, ledger=None):
    def responder(sql, params):
        if "open_macro_v03_staleness_blocks" in sql:
            return [ledger] if ledger is not None else []
        if "open_macro_v03_decisions" in sql:
            return [decision] if decision is not None else []
        if "open_macro_v03_allocations" in sql:
            return [allocation] if allocation is not None else []
        raise AssertionError(f"unexpected SQL: {sql}")
    return responder


def _recompute(breaches=(), **over):
    rec = {
        "input_vintage_sha256": VINTAGE_SHA,
        "input_prices_sha256": PRICES_SHA,
        "pack_v2_sha256": sv.PACK_SHA256_PIN,
        "staleness_breaches": list(breaches),
    }
    if not breaches:
        rec.update({
            "quadrant": "expansion", "decision_validity": "carried",
            "carry_seed_as_of": "2026-06-30", "candidate_confidence": 0.81,
            "coverage_quality": 1.0, "growth_score": 0.2, "inflation_score": -0.1,
            "weights": dict(WEIGHTS), "risk_assets_weight": 0.50,
            "defensive_assets_weight": 0.39, "priced_at": "2026-07-02",
        })
    rec.update(over)
    return rec


@pytest.fixture(autouse=True)
def _pin_commit(monkeypatch):
    monkeypatch.setattr(sv, "code_commit", lambda: "f" * 40)


def _patch_recompute(monkeypatch, rec):
    monkeypatch.setattr(sv, "recompute", lambda conn, as_of: rec)


# --------------------------------------------------------------------------- #
# Path (a): published pair
# --------------------------------------------------------------------------- #
def test_recompute_match_yields_verified(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "verified"
    assert record["abort_reasons"] == []
    assert record["route_evidence"] == "unavailable"
    assert record["recompute"]["input_vintage_sha256"] == VINTAGE_SHA


def test_divergent_weight_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple(w_spy=0.99)))
    _patch_recompute(monkeypatch, _recompute())
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("field_mismatch: w_spy" in r for r in record["abort_reasons"])


def test_divergent_quadrant_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(quadrant="slowdown"),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("field_mismatch: quadrant" in r for r in record["abort_reasons"])


def test_input_hash_mismatch_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(input_vintage_sha256="x" * 64),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("input_hash_mismatch: decision.input_vintage_sha256" in r
               for r in record["abort_reasons"])


def test_staleness_bypass_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch,
                     _recompute(breaches=[{"kind": "series", "series_id": "MICH"}]))
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("staleness_bypass" in r for r in record["abort_reasons"])


def test_nan_in_published_numeric_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(growth_score=float("nan")),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("nan_inf" in r for r in record["abort_reasons"])


def test_valid_until_divergence_aborts(monkeypatch):
    wrong_vu = VU + _dt.timedelta(days=3)
    conn = _FakeConn(_responder(decision=_decision_tuple(valid_until=wrong_vu),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("valid_until_mismatch: decision" in r for r in record["abort_reasons"])


# --------------------------------------------------------------------------- #
# Path (b): staleness-block day
# --------------------------------------------------------------------------- #
def test_justified_block_passes(monkeypatch):
    conn = _FakeConn(_responder(ledger=_ledger_tuple()))
    _patch_recompute(monkeypatch,
                     _recompute(breaches=[{"kind": "series", "series_id": "MICH"}]))
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "staleness_block_justified"
    assert record["abort_reasons"] == []


def test_false_block_aborts(monkeypatch):
    conn = _FakeConn(_responder(ledger=_ledger_tuple()))
    _patch_recompute(monkeypatch, _recompute())  # fresh inputs
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("false_block" in r for r in record["abort_reasons"])


def test_block_hash_divergence_does_not_abort_but_is_pinned(monkeypatch):
    """Round-8 semantics (main): the ledger's input hashes are a FROZEN first-write
    snapshot — a later same-day arrival that leaves the breach standing must not turn
    a still-justified block into a false abort. Both hash sets are pinned side by
    side in the day record as evidence."""
    conn = _FakeConn(_responder(ledger=_ledger_tuple(input_prices_sha256="x" * 64)))
    _patch_recompute(monkeypatch,
                     _recompute(breaches=[{"kind": "series", "series_id": "MICH"}]))
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "staleness_block_justified"
    assert record["abort_reasons"] == []
    assert record["ledger_input_hashes"] == {
        "input_vintage_sha256": VINTAGE_SHA,
        "input_prices_sha256": "x" * 64,
    }
    assert record["recompute"]["input_prices_sha256"] == PRICES_SHA  # divergent, pinned


def test_block_and_output_coexist_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(), ledger=_ledger_tuple()))
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("block_and_output_coexist" in r for r in record["abort_reasons"])


# --------------------------------------------------------------------------- #
# missing / partial
# --------------------------------------------------------------------------- #
def test_missing_output_aborts():
    conn = _FakeConn(_responder())
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("missing_output" in r for r in record["abort_reasons"])


def test_partial_pair_aborts():
    conn = _FakeConn(_responder(decision=_decision_tuple()))
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("partial_pair" in r for r in record["abort_reasons"])


# --------------------------------------------------------------------------- #
# Backend route leg
# --------------------------------------------------------------------------- #
def _route_payload(**over):
    payload = {"as_of": AS_OF.isoformat(), "quadrant": "expansion",
               "decision_validity": "carried", "book": "compressed_50",
               "weights": dict(WEIGHTS)}
    payload.update(over)
    return payload


def test_route_match_records_evidence(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    monkeypatch.setattr(sv, "_fetch_route", lambda url: (200, _route_payload()))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "verified"
    assert record["route_evidence"]["status_code"] == 200
    assert record["route_evidence"]["payload"]["quadrant"] == "expansion"


def test_route_weight_divergence_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    bad = _route_payload(weights={**WEIGHTS, "SPY": 0.99})
    monkeypatch.setattr(sv, "_fetch_route", lambda url: (200, bad))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("route_divergence: weights.SPY" in r for r in record["abort_reasons"])


def test_route_404_aborts_as_inactive(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    monkeypatch.setattr(sv, "_fetch_route", lambda url: (404, None))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("route_inactive_during_window" in r for r in record["abort_reasons"])


def test_no_backend_env_marks_route_unavailable(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    record = sv.verify_day(conn, AS_OF, backend_url=None)
    assert record["route_evidence"] == "unavailable"


def test_recompute_matches_the_current_worker_contract():
    """Signature-drift guard: recompute() calls the worker's own helpers positionally;
    a hardened worker changing these signatures must fail HERE, not silently at the
    first live verification (the round-8 build_allocation as_of addition is the
    precedent)."""
    import inspect
    assert list(inspect.signature(sv.build_allocation).parameters) == [
        "quadrant", "price_rows", "as_of"]
    assert list(inspect.signature(sv.compose_inputs).parameters) == ["conn", "as_of"]
    assert list(inspect.signature(sv.valid_until).parameters) == ["as_of"]
    assert list(inspect.signature(sv.staleness_report).parameters) == [
        "vintage_rows", "price_rows", "as_of"]


# --------------------------------------------------------------------------- #
# Day record schema + writing
# --------------------------------------------------------------------------- #
def test_day_record_schema_and_serialization(monkeypatch, tmp_path):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    record = sv.verify_day(conn, AS_OF)
    assert set(record) == {
        "artifact_type", "schema_version", "stage", "stage_c_id", "as_of", "outcome",
        "abort_reasons", "recompute", "ledger_input_hashes", "route_evidence",
        "verifier_commit",
    }
    assert record["ledger_input_hashes"] is None  # no ledger row on a published day
    assert record["verifier_commit"] == "f" * 40
    out = sv.write_day_record(record, tmp_path)
    assert out.name == f"day_{AS_OF.isoformat()}.json"
    raw = out.read_bytes()
    assert raw.endswith(b"\n") and b"\r\n" not in raw
    assert json.loads(raw) == record


# --------------------------------------------------------------------------- #
# Window report
# --------------------------------------------------------------------------- #
def _write_day(dirpath: Path, as_of: str, outcome: str, reasons=()):
    (dirpath / f"day_{as_of}.json").write_text(json.dumps({
        "as_of": as_of, "outcome": outcome, "abort_reasons": list(reasons),
    }), encoding="utf-8")


def _business_days(start: _dt.date, n: int) -> list[str]:
    out, day = [], start
    while len(out) < n:
        if day.weekday() < 5:
            out.append(day.isoformat())
        day += _dt.timedelta(days=1)
    return out


def test_window_completes_with_ten_verified_post_cutover(tmp_path):
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    for day in _business_days(_dt.date(2026, 7, 6), 10):
        _write_day(window, day, "verified")
    report = sv.build_window_report(window, cutover)
    assert report["counted_days"] == 10
    assert report["aborts"] == []
    assert report["window_complete"] is True


def test_block_pauses_the_window_without_aborting(tmp_path):
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    days = _business_days(_dt.date(2026, 7, 6), 10)
    for day in days[:9]:
        _write_day(window, day, "verified")
    _write_day(window, days[9], "staleness_block_justified")
    report = sv.build_window_report(window, cutover)
    assert report["counted_days"] == 9
    assert report["staleness_blocks_paused"] == 1
    assert report["aborts"] == []
    assert report["window_complete"] is False  # paused, not aborted


def test_abort_invalidates_the_window(tmp_path):
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    days = _business_days(_dt.date(2026, 7, 6), 11)
    for day in days[:10]:
        _write_day(window, day, "verified")
    _write_day(window, days[10], "abort", ["field_mismatch: quadrant"])
    report = sv.build_window_report(window, cutover)
    assert report["counted_days"] == 10
    assert len(report["aborts"]) == 1
    assert report["window_complete"] is False


def test_no_cutover_record_counts_zero_days(tmp_path):
    window = tmp_path / "window"
    window.mkdir()
    for day in _business_days(_dt.date(2026, 7, 6), 10):
        _write_day(window, day, "verified")
    report = sv.build_window_report(window, tmp_path / "missing_cutover.json")
    assert report["cutover"]["present"] is False
    assert report["counted_days"] == 0
    assert report["window_complete"] is False
    assert all(d["note"] == "verified_but_not_consumed: route still inert (pre-cutover)"
               for d in report["days"])


def test_pre_cutover_days_do_not_count(tmp_path):
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-08"}), encoding="utf-8")
    for day in _business_days(_dt.date(2026, 7, 6), 4):  # 07-06..07-09
        _write_day(window, day, "verified")
    report = sv.build_window_report(window, cutover)
    assert report["counted_days"] == 2  # only 07-08 and 07-09


# --------------------------------------------------------------------------- #
# Read-only guard
# --------------------------------------------------------------------------- #
def test_verifier_is_read_only_by_construction():
    text = (Path(sv.__file__)).read_text(encoding="utf-8")
    forbidden = re.findall(
        r"\b(INSERT|UPDATE|DELETE|UPSERT|TRUNCATE|ALTER|DROP|CREATE|COPY|GRANT|MERGE)\b",
        text)
    assert forbidden == [], f"verifier contains write-capable keywords: {forbidden}"
    imports = re.findall(r"from src\.workers\.open_macro_v03 import \(([^)]*)\)", text, re.S)
    assert imports
    for write_helper in ("publish", "record_staleness_block", "invalidate",
                         "ensure_schema", "_invalidate_both"):
        assert write_helper not in imports[0], f"verifier imports write helper {write_helper}"

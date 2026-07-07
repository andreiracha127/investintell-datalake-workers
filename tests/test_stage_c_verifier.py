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
def _positions(weights=None):
    """The REAL API shape: positions list, zero-weight positions OMITTED."""
    weights = weights if weights is not None else WEIGHTS
    return [{"ticker": t, "weight": v, "asset_class": "etf",
             "strategy_label": "open_macro_v03"}
            for t, v in weights.items() if v != 0.0]


def _route_payload(**over):
    payload = {"as_of": AS_OF.isoformat(), "quadrant": "expansion",
               "decision_validity": "carried",
               "carry_seed_as_of": "2026-06-30",
               "candidate_confidence": 0.81, "book": "compressed_50",
               "priced_at": "2026-07-02",
               "risk_assets_weight": 0.50, "defensive_assets_weight": 0.39,
               "valid_until": VU.isoformat(),
               "positions": _positions()}
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


def test_route_zero_weight_position_omitted_compares_as_zero(monkeypatch):
    """The API omits zero-weight positions: a recomputed 0.0 for a ticker absent
    from positions is a MATCH, never a divergence."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    zero_dbc = {**WEIGHTS, "DBC": 0.0}
    _patch_recompute(monkeypatch, _recompute(weights=dict(zero_dbc)))
    payload = _route_payload(positions=_positions(zero_dbc))  # DBC omitted
    assert all(p["ticker"] != "DBC" for p in payload["positions"])
    monkeypatch.setattr(sv, "_fetch_route", lambda url: (200, payload))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    route_reasons = [r for r in record["abort_reasons"] if "route" in r]
    assert route_reasons == []


def test_route_weight_divergence_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    bad = _route_payload(positions=_positions({**WEIGHTS, "SPY": 0.99}))
    monkeypatch.setattr(sv, "_fetch_route", lambda url: (200, bad))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("route_divergence: positions[SPY].weight" in r
               for r in record["abort_reasons"])


def test_route_confidence_divergence_aborts(monkeypatch):
    """candidate_confidence is served from the DB: with exact NUMERIC write fidelity
    in production, float64 equality is the contract — the 15-digit truncated value
    the day-1 abort observed must fail here."""
    exact = 0.8121545618518331
    truncated = 0.812154561851833
    conn = _FakeConn(_responder(
        decision=_decision_tuple(candidate_confidence=exact),
        allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute(candidate_confidence=exact))
    monkeypatch.setattr(sv, "_fetch_route",
                        lambda url: (200, _route_payload(candidate_confidence=truncated)))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("route_divergence: candidate_confidence" in r
               for r in record["abort_reasons"])


def test_route_missing_positions_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    legacy = _route_payload()
    legacy.pop("positions")
    legacy["weights"] = dict(WEIGHTS)  # the OLD shape must not satisfy the check
    monkeypatch.setattr(sv, "_fetch_route", lambda url: (200, legacy))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("positions missing" in r for r in record["abort_reasons"])


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


@pytest.mark.parametrize("field, bad", [
    ("carry_seed_as_of", "2026-06-29"),
    ("priced_at", "2026-07-01"),
    ("valid_until", "2026-07-08T14:00:00+00:00"),
])
def test_route_freshness_field_divergence_aborts(monkeypatch, field, bad):
    """A correct weight vector served with a stale carry seed / priced_at / valid_until
    is still a consumer-visible divergence (a wrong valid_until misreports freshness)."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    monkeypatch.setattr(sv, "_fetch_route",
                        lambda url: (200, _route_payload(**{field: bad})))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any(f"route_divergence: {field}" in r for r in record["abort_reasons"])


def test_route_duplicate_position_ticker_aborts(monkeypatch):
    """Two rows for the same sleeve ticker must abort — never silently keep the last."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    positions = _positions()
    positions.append({"ticker": "SPY", "weight": WEIGHTS["SPY"],
                      "asset_class": "etf", "strategy_label": "open_macro_v03"})
    monkeypatch.setattr(sv, "_fetch_route",
                        lambda url: (200, _route_payload(positions=positions)))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("duplicate position ticker 'SPY'" in r for r in record["abort_reasons"])


def test_route_stringified_numeric_aborts(monkeypatch):
    """A JSON string where a number is expected is a consumer wire regression that
    numeric coercion would otherwise hide."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    monkeypatch.setattr(
        sv, "_fetch_route",
        lambda url: (200, _route_payload(risk_assets_weight="0.50")))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("risk_assets_weight not a JSON number" in r
               for r in record["abort_reasons"])


def test_route_stringified_position_weight_aborts(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    positions = _positions({**WEIGHTS, "SPY": "0.39"})  # SPY weight as a string
    monkeypatch.setattr(sv, "_fetch_route",
                        lambda url: (200, _route_payload(positions=positions)))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("positions[SPY].weight not a JSON number" in r
               for r in record["abort_reasons"])


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
        "abort_reasons", "recompute", "ledger_reason", "ledger_input_hashes",
        "route_evidence", "verifier_commit",
    }
    assert record["ledger_input_hashes"] is None  # no ledger row on a published day
    assert record["ledger_reason"] is None
    assert record["recompute"]["staleness_breaches"] == []  # clean recompute, pinned
    assert record["verifier_commit"] == "f" * 40
    out = sv.write_day_record(record, tmp_path)
    assert out.name == f"day_{AS_OF.isoformat()}.json"
    raw = out.read_bytes()
    assert raw.endswith(b"\n") and b"\r\n" not in raw
    assert json.loads(raw) == record


# --------------------------------------------------------------------------- #
# Window report
# --------------------------------------------------------------------------- #
def _write_day(dirpath: Path, as_of: str, outcome: str, reasons=(), *, route=True):
    record = {"as_of": as_of, "outcome": outcome, "abort_reasons": list(reasons)}
    # a counted day carries route evidence (a dict); pass route=False to model a
    # DB-only verified day whose route_evidence was "unavailable".
    record["route_evidence"] = ({"status_code": 200, "payload": {}} if route
                                else "unavailable")
    (dirpath / f"day_{as_of}.json").write_text(json.dumps(record), encoding="utf-8")


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


def test_verified_without_route_evidence_does_not_count(tmp_path):
    """A DB-only verified day (route_evidence unavailable) must not close the REAL
    window: only days carrying the consumer route leg are counted."""
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    days = _business_days(_dt.date(2026, 7, 6), 10)
    for day in days[:9]:
        _write_day(window, day, "verified")
    _write_day(window, days[9], "verified", route=False)  # DB-only leg
    report = sv.build_window_report(window, cutover)
    assert report["counted_days"] == 9
    assert report["window_complete"] is False
    dbonly = next(d for d in report["days"] if d["as_of"] == days[9])
    assert dbonly["counted"] is False
    assert dbonly["note"] == "verified_without_route_evidence: not counted (DB-only leg)"


def test_missing_business_day_is_a_gap_and_blocks_completion(tmp_path):
    """A skipped post-cutover weekday is otherwise invisible; the report must surface
    it as a supervision gap and refuse window_complete even with >= 10 verified days."""
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    days = _business_days(_dt.date(2026, 7, 6), 11)
    skipped = days[3]
    for day in days:
        if day == skipped:
            continue
        _write_day(window, day, "verified")
    report = sv.build_window_report(window, cutover)
    assert report["counted_days"] == 10
    assert report["supervision_gaps"] == [skipped]
    assert report["window_complete"] is False


def test_contiguous_window_has_no_gaps(tmp_path):
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    for day in _business_days(_dt.date(2026, 7, 6), 10):
        _write_day(window, day, "verified")
    report = sv.build_window_report(window, cutover)
    assert report["supervision_gaps"] == []
    assert report["window_complete"] is True


# --------------------------------------------------------------------------- #
# Round-2 review hardening
# --------------------------------------------------------------------------- #
def test_recompute_exception_becomes_an_abort_record(monkeypatch):
    """A recompute()/build_allocation() raise on a published day must be PRESERVED as an
    abort day record (not escape verify_day and lose the evidence to a later gap)."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))

    def _boom(conn, as_of):
        raise RuntimeError("prefix-hash drift")

    monkeypatch.setattr(sv, "recompute", _boom)
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("recompute_error: RuntimeError: prefix-hash drift" in r
               for r in record["abort_reasons"])
    # never falsely verified, and the record is still writable/strict-JSON
    assert record["route_evidence"] is None


def test_route_nonfinite_number_diverges_and_stays_valid_json(monkeypatch, tmp_path):
    """A NaN/Infinity route numeric must abort AND never produce an invalid day_*.json:
    the evidence payload is persisted strict-JSON-safe and the writer pins allow_nan."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    monkeypatch.setattr(
        sv, "_fetch_route",
        lambda url: (200, _route_payload(risk_assets_weight=float("inf"))))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("risk_assets_weight non-finite" in r for r in record["abort_reasons"])
    # evidence preserved as a strict-JSON string, artifact round-trips under strict parse
    assert record["route_evidence"]["payload"]["risk_assets_weight"] == "inf"
    out = sv.write_day_record(record, tmp_path)
    reparsed = json.loads(out.read_text(encoding="utf-8"),
                          parse_constant=lambda c: pytest.fail(f"non-strict JSON: {c}"))
    assert reparsed == record


def test_route_nonfinite_position_weight_diverges(monkeypatch):
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    positions = _positions({**WEIGHTS, "SPY": float("nan")})
    monkeypatch.setattr(sv, "_fetch_route",
                        lambda url: (200, _route_payload(positions=positions)))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("positions[SPY].weight non-finite" in r
               for r in record["abort_reasons"])


def test_route_non_string_ticker_diverges_without_crashing(monkeypatch):
    """A non-string (unhashable) ticker must become a route_divergence, not a TypeError
    that aborts verify_day WITHOUT writing the day record."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    positions = _positions()
    positions.append({"ticker": ["SPY"], "weight": 0.1,  # list ticker (unhashable)
                      "asset_class": "etf", "strategy_label": "open_macro_v03"})
    monkeypatch.setattr(sv, "_fetch_route",
                        lambda url: (200, _route_payload(positions=positions)))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("non-string position ticker" in r for r in record["abort_reasons"])


@pytest.mark.parametrize("field, bad", [
    ("carry_seed_as_of", "2026-06-30T00:00:00Z"),
    ("priced_at", "2026-07-02T00:00:00Z"),
])
def test_route_date_timestamp_form_diverges_not_sliced(monkeypatch, field, bad):
    """A date-only wire field served as a timestamp whose first ten chars still match
    must diverge — the route comparison checks the EXACT wire string (no _date_iso)."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    monkeypatch.setattr(sv, "_fetch_route",
                        lambda url: (200, _route_payload(**{field: bad})))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any(f"route_divergence: {field}" in r for r in record["abort_reasons"])


def test_pre_cutover_abort_does_not_block_completion(tmp_path):
    """A pre-cutover (non-consumed) abort stays visible but must NOT permanently block a
    clean ten-day consumed window."""
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-08"}), encoding="utf-8")
    _write_day(window, "2026-07-06", "abort", ["field_mismatch: quadrant"])  # pre-cutover
    for day in _business_days(_dt.date(2026, 7, 8), 10):
        _write_day(window, day, "verified")
    report = sv.build_window_report(window, cutover)
    assert report["counted_days"] == 10
    assert len(report["aborts"]) == 1               # still surfaced
    assert report["aborts"][0]["consumed"] is False
    assert report["window_complete"] is True         # not blocked by the pre-cutover abort
    pre = next(d for d in report["days"] if d["as_of"] == "2026-07-06")
    assert pre["note"] == "pre_cutover_abort: outside the production window (not blocking)"


def test_post_cutover_abort_still_blocks_completion(tmp_path):
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    days = _business_days(_dt.date(2026, 7, 6), 11)
    for day in days[:10]:
        _write_day(window, day, "verified")
    _write_day(window, days[10], "abort", ["field_mismatch: quadrant"])  # consumed abort
    report = sv.build_window_report(window, cutover)
    assert report["aborts"][0]["consumed"] is True
    assert report["window_complete"] is False


# --------------------------------------------------------------------------- #
# Round-3 review hardening
# --------------------------------------------------------------------------- #
def test_route_transport_failure_fails_loud_without_a_record(monkeypatch):
    """A verifier-side transport failure reaching the sanctioned route (httpx.RequestError:
    DNS/TLS/connect/read timeout, raised BEFORE any response) must FAIL LOUD — re-raise and
    write NO abort record — so a network outage is never recorded as a route_divergence-style
    abort that blocks the window with no route response captured."""
    import httpx

    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())

    def _boom(url):
        raise httpx.ConnectError("name resolution failed")

    monkeypatch.setattr(sv, "_fetch_route", _boom)
    with pytest.raises(httpx.RequestError):
        sv.verify_day(conn, AS_OF, backend_url="https://backend.example")


def test_recompute_error_still_records_when_not_transport(monkeypatch):
    """The transport carve-out is narrow: a non-transport raise on the route leg (or any
    recompute/comparison raise) is still PRESERVED as an abort day record, unchanged."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())

    def _boom(url):
        raise ValueError("not a transport error")

    monkeypatch.setattr(sv, "_fetch_route", _boom)
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("recompute_error: ValueError: not a transport error" in r
               for r in record["abort_reasons"])


def test_duplicate_day_file_counts_once_and_blocks_completion(tmp_path):
    """Two day_*.json files naming the same as_of (a copied/attempt file beside the
    canonical record) must count as ONE distinct verified day, surface a duplicate, and
    refuse window_complete — never inflate counted_days past the distinct dates."""
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    days = _business_days(_dt.date(2026, 7, 6), 10)
    for day in days:
        _write_day(window, day, "verified")
    # a copied/attempt file naming an already-counted business day (matches day_*.json)
    dup = {"as_of": days[0], "outcome": "verified", "abort_reasons": [],
           "route_evidence": {"status_code": 200, "payload": {}}}
    (window / f"day_{days[0]}_attempt.json").write_text(json.dumps(dup), encoding="utf-8")
    report = sv.build_window_report(window, cutover)
    assert report["counted_days"] == 10  # not 11: the duplicate date is counted once
    assert report["duplicate_dates"] == [days[0]]
    assert report["window_complete"] is False


def test_no_duplicate_dates_on_a_clean_window(tmp_path):
    """The distinct-date guard is inert on a normal one-file-per-date window."""
    window = tmp_path / "window"
    window.mkdir()
    cutover = tmp_path / "backend_cutover_record.json"
    cutover.write_text(json.dumps({"cutover_date": "2026-07-06"}), encoding="utf-8")
    for day in _business_days(_dt.date(2026, 7, 6), 10):
        _write_day(window, day, "verified")
    report = sv.build_window_report(window, cutover)
    assert report["duplicate_dates"] == []
    assert report["window_complete"] is True


# --------------------------------------------------------------------------- #
# Round-4 review hardening
# --------------------------------------------------------------------------- #
def test_db_abort_skips_the_route_leg_and_survives_transport_failure(monkeypatch):
    """Once the DB comparison has found a divergence, the route leg must NOT run: a
    transport failure there would re-raise (fail-loud, no artifact) and destroy the
    already-detected DB abort evidence."""
    import httpx

    conn = _FakeConn(_responder(decision=_decision_tuple(quadrant="contraction"),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    calls: list[str] = []

    def _boom(url):
        calls.append(url)
        raise httpx.ConnectError("network down")

    monkeypatch.setattr(sv, "_fetch_route", _boom)
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert calls == []  # route leg never consulted on a DB-divergent day
    assert record["outcome"] == "abort"
    assert any("field_mismatch: quadrant" in r for r in record["abort_reasons"])
    assert not any("recompute_error" in r for r in record["abort_reasons"])


def test_staleness_bypass_still_skips_the_route_leg(monkeypatch):
    """The old explicit staleness_bypass guard is subsumed: a bypass is an abort
    reason, so the route leg is skipped the same way."""
    breach = [{"series_id": "MICH", "age_days": 99}]
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute(breaches=breach))
    calls: list[str] = []
    monkeypatch.setattr(sv, "_fetch_route",
                        lambda url: calls.append(url) or (200, _route_payload()))
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert calls == []
    assert record["outcome"] == "abort"
    assert any("staleness_bypass" in r for r in record["abort_reasons"])


def test_nonfinite_recomputed_weight_abort_record_still_serializes(monkeypatch, tmp_path):
    """A non-finite RECOMPUTED weight is flagged nan_inf by _compare_pair; the day
    record must still be writable (allow_nan=False) with the evidence preserved as a
    strict-JSON string — never crash the writer and lose the abort artifact."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    bad = _recompute(weights={**WEIGHTS, "SPY": float("nan")})
    _patch_recompute(monkeypatch, bad)
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("nan_inf: recompute.weights.SPY" in r for r in record["abort_reasons"])
    assert record["recompute"]["weights"]["SPY"] == "nan"
    out = sv.write_day_record(record, tmp_path)
    reparsed = json.loads(out.read_text(encoding="utf-8"),
                          parse_constant=lambda c: pytest.fail(f"non-strict JSON: {c}"))
    assert reparsed == record


def test_duplicate_json_keys_in_route_payload_diverge():
    """Duplicate JSON object keys are an ambiguous wire contract (consumers may read
    divergent values from the same bytes; Python keeps the last silently) — the parse
    hook must reject them at every object level."""
    with pytest.raises(sv._AmbiguousRoutePayload) as exc_info:
        sv._parse_route_payload(200, '{"as_of": "2026-07-06", "as_of": "2026-07-07"}')
    assert "duplicate JSON key 'as_of'" in exc_info.value.detail
    assert exc_info.value.status_code == 200
    # nested objects are covered too
    with pytest.raises(sv._AmbiguousRoutePayload):
        sv._parse_route_payload(200, '{"p": {"weight": 0.1, "weight": 0.39}}')
    # clean payloads and non-JSON bodies keep their existing behaviour
    assert sv._parse_route_payload(200, '{"a": 1}') == {"a": 1}
    assert sv._parse_route_payload(502, "Bad Gateway") is None


def test_route_duplicate_key_payload_aborts_with_wire_evidence(monkeypatch):
    """check_route converts _AmbiguousRoutePayload into a route_divergence abort with
    the raw wire text pinned as evidence (never a transport-style re-raise)."""
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    wire = '{"quadrant": "expansion", "quadrant": "contraction"}'

    def _dup(url):
        raise sv._AmbiguousRoutePayload("duplicate JSON key 'quadrant'",
                                        status_code=200, raw_text=wire)

    monkeypatch.setattr(sv, "_fetch_route", _dup)
    record = sv.verify_day(conn, AS_OF, backend_url="https://backend.example")
    assert record["outcome"] == "abort"
    assert any("route_divergence: ambiguous wire payload — duplicate JSON key "
               "'quadrant'" in r for r in record["abort_reasons"])
    assert record["route_evidence"]["raw_wire_text"] == wire
    assert record["route_evidence"]["payload"] is None


# --------------------------------------------------------------------------- #
# Round-5 review hardening
# --------------------------------------------------------------------------- #
def test_justified_block_record_preserves_the_justification_evidence(monkeypatch):
    """A staleness_block_justified day pauses the window; the day record must carry
    the WHY — the recomputed breach details and the ledger row's frozen reason — so an
    auditor validates the artifact itself instead of re-running against live inputs
    that have since moved."""
    breaches = [{"kind": "series", "series_id": "MICH", "age_days": 42}]
    conn = _FakeConn(_responder(ledger=_ledger_tuple()))
    _patch_recompute(monkeypatch, _recompute(breaches=breaches))
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "staleness_block_justified"
    assert record["recompute"]["staleness_breaches"] == breaches
    assert record["ledger_reason"] == "staleness SLO breach: MICH"


def test_bypass_abort_record_preserves_the_recomputed_breaches(monkeypatch):
    """A staleness_bypass abort must pin WHAT the worker ignored: the recomputed
    breaches ride in the day record beside the abort reason."""
    breaches = [{"kind": "series", "series_id": "MICH", "age_days": 42}]
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute(breaches=breaches))
    record = sv.verify_day(conn, AS_OF)
    assert record["outcome"] == "abort"
    assert any("staleness_bypass" in r for r in record["abort_reasons"])
    assert record["recompute"]["staleness_breaches"] == breaches


# --------------------------------------------------------------------------- #
# Round-6 review hardening
# --------------------------------------------------------------------------- #
def test_route_evidence_pins_the_resolved_url(monkeypatch):
    """Every route_evidence dict pins the resolved URL that was actually exercised —
    an auditor can see WHICH host served the evidence (sanctioned production vs a
    mispointed staging/local clone) — on the verified, 404, and ambiguous paths."""
    url = "https://backend.example"
    expected = "https://backend.example/macro/open-macro-v03/allocation"

    # verified path
    conn = _FakeConn(_responder(decision=_decision_tuple(),
                                allocation=_allocation_tuple()))
    _patch_recompute(monkeypatch, _recompute())
    monkeypatch.setattr(sv, "_fetch_route", lambda u: (200, _route_payload()))
    record = sv.verify_day(conn, AS_OF, backend_url=url)
    assert record["outcome"] == "verified"
    assert record["route_evidence"]["url"] == expected

    # 404 path
    monkeypatch.setattr(sv, "_fetch_route", lambda u: (404, None))
    record = sv.verify_day(conn, AS_OF, backend_url=url)
    assert record["route_evidence"]["url"] == expected

    # ambiguous-wire path
    def _dup(u):
        raise sv._AmbiguousRoutePayload("duplicate JSON key 'weight'",
                                        status_code=200, raw_text="{}")
    monkeypatch.setattr(sv, "_fetch_route", _dup)
    record = sv.verify_day(conn, AS_OF, backend_url=url)
    assert record["route_evidence"]["url"] == expected

    # trailing slash normalizes to the same resolved URL
    assert sv._route_url("https://backend.example/") == expected


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

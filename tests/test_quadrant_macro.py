from __future__ import annotations

import datetime as dt

from src import quadrant_assemble as qa
from src.workers import quadrant_macro as qm


def test_quadrant_from_signs_maps_four_quadrants() -> None:
    assert qa.quadrant_from_signs(1, -1) == "recovery"
    assert qa.quadrant_from_signs(1, 1) == "expansion"
    assert qa.quadrant_from_signs(-1, 1) == "slowdown"
    assert qa.quadrant_from_signs(-1, -1) == "contraction"
    assert qa.quadrant_from_signs(None, 1) is None
    assert qa.quadrant_from_signs(1, None) is None


def _kw(**over):
    """Shared build_snapshot kwargs for a strong, full-quality, valid expansion."""
    av = dt.datetime(2024, 3, 5, tzinfo=dt.timezone.utc)
    hist = [0.05 + 0.01 * i for i in range(30)]  # 30 distinct >= MIN_UNCERTAINTY_VINTAGES (24)
    base = dict(
        as_of=dt.date(2024, 3, 1), computed_at=av, previous_snapshot_id=None,
        growth_score=0.30, growth_history=hist, growth_prev_sign=1,
        growth_coverage=1.0, growth_freshness=1.0, growth_health=1.0,
        growth_contributions={"INDPRO": 0.30}, growth_u_floor=0.01,
        inflation_score=0.30, inflation_history=hist, inflation_prev_sign=1,
        inflation_coverage=1.0, inflation_freshness=1.0, inflation_health=1.0,
        inflation_contributions={"CPILFESL": 0.30}, inflation_u_floor=0.01,
        input_available_ats=[av],
        critical_expiries=[dt.datetime(2024, 4, 15, tzinfo=dt.timezone.utc)],
        model_version="macro_quadrant_us_v1",
        confidence_method="rolling_score_mad_distinct_vintages_v1",
        source_vintage_hash="deadbeefcafe1234",
    )
    base.update(over)
    return base


def test_build_snapshot_valid_when_both_axes_confirmed_and_confident() -> None:
    snap = qa.build_snapshot(**_kw())
    assert snap.status_at_compute == "valid"
    assert snap.quadrant == "expansion"
    assert snap.candidate_quadrant == "expansion"
    assert snap.candidate_confidence is not None and snap.candidate_confidence >= 0.70
    assert snap.previous_snapshot_id is None  # genesis
    # snapshot_id is the deterministic uuid5 over the canonical key + GENESIS.
    import uuid as _uuid

    from src.quadrant_snapshot import REGIME_SNAPSHOT_NAMESPACE
    assert snap.snapshot_id == str(_uuid.uuid5(
        REGIME_SNAPSHOT_NAMESPACE,
        "macro_quadrant_us_v1|2024-03-01|deadbeefcafe1234|GENESIS"))
    # latched memory is persisted even though it equals the effective sign here.
    assert snap.growth.internal_sign == 1 and snap.inflation.internal_sign == 1


def test_build_snapshot_unavailable_when_coverage_low_carries_null_quadrant() -> None:
    snap = qa.build_snapshot(**_kw(growth_coverage=0.50))  # below 0.80
    assert snap.status_at_compute == "unavailable"
    assert snap.quadrant is None
    assert snap.candidate_confidence is None  # §7: unavailable carries no confidence


def test_build_snapshot_low_confidence_on_axis_transition() -> None:
    # growth deadband (prev +1, tiny score) -> transition pending -> low_confidence.
    snap = qa.build_snapshot(**_kw(growth_score=0.05,
                                   growth_contributions={"INDPRO": 0.05}))
    assert snap.status_at_compute == "low_confidence"
    assert snap.quadrant is None
    assert snap.transition_pending is True
    # latched memory of the prior +1 is preserved across the deadband.
    assert snap.growth.internal_sign == 1 and snap.growth.sign is None


def test_build_snapshot_threads_previous_id_into_uuid() -> None:
    import uuid as _uuid

    from src.quadrant_snapshot import REGIME_SNAPSHOT_NAMESPACE
    prev = str(_uuid.uuid5(REGIME_SNAPSHOT_NAMESPACE, "seed"))
    snap = qa.build_snapshot(**_kw(previous_snapshot_id=prev))
    assert snap.previous_snapshot_id == prev
    assert snap.snapshot_id == str(_uuid.uuid5(
        REGIME_SNAPSHOT_NAMESPACE,
        f"macro_quadrant_us_v1|2024-03-01|deadbeefcafe1234|{prev}"))


def test_snapshot_to_record_and_audit_shapes() -> None:
    snap = qa.build_snapshot(**_kw())
    rec = qa.snapshot_to_record(snap)
    assert rec[0] == snap.snapshot_id  # first column is snapshot_id
    assert rec[1] == snap.previous_snapshot_id  # second column is previous_snapshot_id
    audit = qa.audit_records(snap.snapshot_id, {"growth": {"INDPRO": 0.30},
                                                "inflation": {"CPILFESL": 0.30}})
    assert {a[1] for a in audit} == {"growth", "inflation"}  # axis column
    assert all(a[0] == snap.snapshot_id for a in audit)


def test_macro_worker_exposes_versions() -> None:
    assert qm.MODEL_VERSION == "macro_quadrant_us_v1"
    assert qm.CONFIDENCE_METHOD == "rolling_score_mad_distinct_vintages_v1"


def test_macro_run_returns_lock_busy_sentinel(monkeypatch) -> None:
    import contextlib

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def commit(self): pass

    @contextlib.contextmanager
    def _busy(conn, lock_id):
        yield False

    monkeypatch.setattr(qm, "connect", lambda dsn: _Conn())
    monkeypatch.setattr(qm, "advisory_lock", _busy)
    monkeypatch.setattr(qa, "ensure_schema", lambda conn: None)
    out = qm.run("postgresql://unused")
    assert out["skipped"] == "lock_busy"


def test_load_previous_snapshot_reads_latest_row() -> None:
    class _Cur:
        def __init__(self, row): self._row = row
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params): self.sql, self.params = sql, params
        def fetchone(self): return self._row

    class _Conn:
        def __init__(self, row): self._row = row
        def cursor(self): return _Cur(self._row)

    # latest row -> {previous_snapshot_id, growth_internal_sign, inflation_internal_sign}
    out = qa.load_previous_snapshot(_Conn(("uuid-abc", 1, -1)), "macro_quadrant_us_v1")
    assert out == {"previous_snapshot_id": "uuid-abc",
                   "growth_internal_sign": 1, "inflation_internal_sign": -1}
    # genesis (no prior row) -> None
    assert qa.load_previous_snapshot(_Conn(None), "macro_quadrant_us_v1") is None


def test_score_axis_applies_direction_minus_one_before_aggregation(monkeypatch) -> None:
    """Obligation 1: a direction=-1 series flips the sign of its z BEFORE axis_score.

    Exercise the real ``_score_axis`` seam with a synthetic direction=-1 spec whose
    standardized z is POSITIVE; the per-series contribution and the axis score must
    both come out NEGATIVE (direction was applied per-series before aggregation).
    """
    from src.macro_sources import _macro

    spec = _macro("FAKEDIR", "growth", "synthetic", 0.25, "log_3m3m_ann_v1",
                  direction=-1)
    assert spec.direction == -1

    # one-spec registry + matching weights; stub the PIT read and the standardizer
    # so the only thing under test is the (z * spec.direction) sign-flow.
    monkeypatch.setattr(qm, "SEED_SOURCES", (spec,))
    monkeypatch.setattr(qm, "axis_weights", lambda axis: {"FAKEDIR": 1.0})
    monkeypatch.setattr(qm, "latest_vintage_as_of",
                        lambda conn, series_ids, t: {"FAKEDIR": {}})
    # positive raw z; direction=-1 must invert it before it reaches axis_score.
    monkeypatch.setattr(qm, "standardized_latest",
                        lambda spec, series, as_of: 2.0)

    t = dt.datetime(2024, 3, 5, tzinfo=dt.timezone.utc)
    score, contributions, z_by_series, _av, _exp = qm._score_axis(None, "growth", t)

    # raw z was +2.0; the stored per-series z is direction-flipped to -2.0.
    assert z_by_series["FAKEDIR"] == -2.0
    # contribution and axis score therefore carry the NEGATIVE sign.
    assert contributions["FAKEDIR"] < 0.0
    assert score is not None and score < 0.0
    assert score == -2.0  # w=1.0 over the single available series


def test_score_axis_raises_clear_error_when_no_critical_specs(monkeypatch) -> None:
    """Obligation 3 (worker-level): a registry with no critical specs fails loud.

    With every spec critical=False the axis yields an EMPTY critical_expiries; the
    worker's guard raises a clear ValueError rather than letting compute_stale_after
    fail deep inside build_snapshot.
    """
    from src.macro_sources import _macro

    spec = _macro("NONCRIT", "growth", "synthetic", 0.25, "log_3m3m_ann_v1",
                  critical=False)
    assert spec.critical is False

    monkeypatch.setattr(qm, "SEED_SOURCES", (spec,))
    monkeypatch.setattr(qm, "axis_weights", lambda axis: {"NONCRIT": 1.0})
    monkeypatch.setattr(qm, "latest_vintage_as_of",
                        lambda conn, series_ids, t: {"NONCRIT": {}})
    monkeypatch.setattr(qm, "standardized_latest",
                        lambda spec, series, as_of: 1.0)

    t = dt.datetime(2024, 3, 5, tzinfo=dt.timezone.utc)
    _score, _contrib, _z, _av, critical_expiries = qm._score_axis(None, "growth", t)
    assert critical_expiries == []  # no critical specs -> no expiries

    import pytest as _pytest
    with _pytest.raises(ValueError, match="critical source expiry"):
        qm._require_critical_expiries(critical_expiries)


def test_build_snapshot_stale_degrades_to_low_confidence() -> None:
    """Obligation 2: a compute-time 'stale' (critical source expired) is degraded to
    low_confidence with quadrant=NULL before INSERT, so the raw 'stale' literal never
    reaches the schema's ck_rqs_status_domain CHECK."""
    # freshness=0.0 drives critical_source_expired -> resolve_status returns 'stale';
    # coverage stays full (>= 0.80) so 'unavailable' does NOT pre-empt the stale branch.
    snap = qa.build_snapshot(**_kw(growth_freshness=0.0, inflation_freshness=0.0))
    assert snap.status_at_compute == "low_confidence"
    assert snap.quadrant is None


import os as _os

import pytest


@pytest.mark.skipif(not _os.getenv("DATABASE_URL"),
                    reason="needs DATABASE_URL with macro_observation_vintage populated")
def test_smoke_macro_run_emits_a_snapshot() -> None:
    out = qm.run(_os.environ["DATABASE_URL"])
    assert out["model_version"] == "macro_quadrant_us_v1"
    assert out["status"] in {"valid", "low_confidence", "unavailable", "invalid"}
    if out["status"] == "valid":
        assert out["quadrant"] in {"recovery", "expansion", "slowdown", "contraction"}

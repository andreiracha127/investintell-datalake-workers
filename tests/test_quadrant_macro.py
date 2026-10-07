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
    """Stream identity after the 2026-10-07 quant audit: the coverage semantics (§6
    historyCoverage factor) and the provenance-hash layout changed, so each worker
    publishes under a NEW minor model_version — the frozen v1/v2/v3 rows, and the
    pinned harness that still computes v1/v3 under the old layout, are never
    overwritten. The confidence identifiers are unchanged on purpose: estimator
    and policy did not move, their coverage INPUT did."""
    from harness.phase0q import decision as hd
    from harness.phase0q import decision_v3 as hd3
    from src.quadrant_confidence_v2 import (
        CONFIDENCE_METHOD_V2,
        CONFIDENCE_METHOD_V3_FUSED,
        CONFIDENCE_MODEL_VERSION_V2,
    )
    from src.workers import quadrant_macro_v2 as qm2
    from src.workers import quadrant_macro_v3 as qm3

    assert qm.MODEL_VERSION == "macro_quadrant_us_v1.1"
    assert qm.CONFIDENCE_METHOD == "rolling_score_mad_distinct_vintages_v1"
    assert qm2.MODEL_VERSION == "macro_quadrant_us_v2.1"
    assert qm3.MODEL_VERSION == "macro_quadrant_us_v3.1"
    assert CONFIDENCE_MODEL_VERSION_V2 == "confidence_v2.0"
    assert CONFIDENCE_METHOD_V2 == "kalman_joint_posterior_v2"
    assert CONFIDENCE_METHOD_V3_FUSED == "kalman_fused_joint_posterior_v3"
    # the frozen labels stay the pinned harness's (certified chain) identity
    assert hd.MODEL_VERSION == "macro_quadrant_us_v1"
    assert hd3.MODEL_VERSION == "macro_quadrant_us_v3"


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


class _CaptureCur:
    """Fake cursor that records the executed SQL + params and returns a row."""

    def __init__(self, row): self._row = row
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params): self.sql, self.params = sql, params
    def fetchone(self): return self._row


class _CaptureConn:
    def __init__(self, row):
        self._row = row
        self.last_cur = None

    def cursor(self):
        self.last_cur = _CaptureCur(self._row)
        return self.last_cur


def test_load_previous_snapshot_reads_latest_row() -> None:
    # latest row -> {previous_snapshot_id, growth_internal_sign, inflation_internal_sign}
    as_of = dt.date(2024, 3, 1)
    out = qa.load_previous_snapshot(
        _CaptureConn(("uuid-abc", 1, -1)), "macro_quadrant_us_v1", as_of)
    assert out == {"previous_snapshot_id": "uuid-abc",
                   "growth_internal_sign": 1, "inflation_internal_sign": -1}
    # genesis (no prior row) -> None
    assert qa.load_previous_snapshot(
        _CaptureConn(None), "macro_quadrant_us_v1", as_of) is None


def test_load_previous_snapshot_filters_strictly_before_target_as_of() -> None:
    """Regression guard (non-env-gated): the predecessor query MUST constrain
    ``as_of < target`` and BIND the target as_of, so a same-day rerun reproduces
    the same predecessor (idempotent uuid5) and an out-of-order backfill never
    chains from a FUTURE snapshot (point-in-time, freeze §8)."""
    conn = _CaptureConn(("uuid-prev", 1, 1))
    target = dt.date(2024, 3, 1)
    qa.load_previous_snapshot(conn, "macro_quadrant_us_v1", target)

    sql = conn.last_cur.sql
    # the filter clause must be present (not just `WHERE model_version = %s`).
    assert "as_of < %s" in sql, f"predecessor query lacks target filter: {sql!r}"
    # the target as_of must actually be a bound parameter (not a tautology fake).
    assert target in conn.last_cur.params
    # model_version is still bound and ordering remains newest-first.
    assert "macro_quadrant_us_v1" in conn.last_cur.params
    assert "ORDER BY as_of DESC" in sql


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
    score, contributions, z_by_series, _av, _exp, _nvalid = qm._score_axis(
        None, "growth", t)

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
    _score, _contrib, _z, _av, critical_expiries, _nvalid = qm._score_axis(
        None, "growth", t)
    assert critical_expiries == []  # no critical specs -> no expiries

    import pytest as _pytest
    with _pytest.raises(ValueError, match="critical source expiry"):
        qm._require_critical_expiries(critical_expiries)


# --------------------------------------------------------------------------- #
# Coverage (freeze §6): historyCoverage factor — quant audit 2026-10-07, MR-2   #
# --------------------------------------------------------------------------- #
def _monthly(values: list[float], start: dt.date) -> dict[dt.date, float]:
    out: dict[dt.date, float] = {}
    y, m = start.year, start.month
    for v in values:
        out[dt.date(y, m, 1)] = v
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def _two_specs():
    from src.macro_sources import _macro

    a = _macro("YOUNG", "growth", "synthetic", 0.5, "log_3m3m_ann_v1")
    b = _macro("GONE", "growth", "synthetic", 0.5, "log_3m3m_ann_v1")
    assert a.minimum_valid_observations == b.minimum_valid_observations == 24
    return a, b


def _pre_audit_coverage(z_by_series, specs) -> float:
    """The formula the worker shipped before MR-2: Σ|w|·I(valid) / Σ|w|."""
    total = sum(abs(s.weight) for s in specs)
    have = sum(abs(s.weight) for s in specs if z_by_series.get(s.series_id) is not None)
    return have / total


def test_coverage_applies_the_frozen_history_coverage_factor() -> None:
    """Regression MR-2: freeze §6 coverage is Σ|w|·I(valid)·min(1, nValid/24) / Σ|w|
    (the frozen formula restated in src/quadrant_confidence.py). Series A valid
    with 12 valid months, series B missing, weights 0.5/0.5 -> 0.5·(12/24) = 0.25.
    The pre-audit code returned 0.5: the historyCoverage factor was dropped."""
    a, b = _two_specs()
    z = {"YOUNG": 1.0, "GONE": None}
    assert abs(qm._coverage(z, (a, b), {"YOUNG": 12, "GONE": 0}) - 0.25) < 1e-9
    assert abs(_pre_audit_coverage(z, (a, b)) - 0.5) < 1e-9  # what MR-2 corrects
    # a missing series contributes nothing even with a full history
    assert abs(qm._coverage(z, (a, b), {"YOUNG": 12, "GONE": 119}) - 0.25) < 1e-9
    # the factor is linear in nValid below the minimum and capped at 1 above it
    assert abs(qm._coverage(z, (a, b), {"YOUNG": 6}) - 0.125) < 1e-9
    assert abs(qm._coverage(z, (a, b), {"YOUNG": 24}) - 0.5) < 1e-9
    assert abs(qm._coverage(z, (a, b), {"YOUNG": 119}) - 0.5) < 1e-9
    # a valid series absent from the counts is fail-safe: uncovered, never full
    assert qm._coverage(z, (a, b), {}) == 0.0


def test_coverage_is_invariant_for_full_histories_and_without_counts() -> None:
    """Production invariance: every series in the 10-year vintage store carries
    >= 24 valid months, so the §6 factor is exactly 1 and the result equals the
    pre-audit Σ|w|·I(valid)/Σ|w| bit-for-bit; history_counts=None (callers that
    cannot supply counts) keeps that behaviour as well."""
    a, b = _two_specs()
    full = {"YOUNG": 119, "GONE": 24}
    for z in ({"YOUNG": 1.0, "GONE": -0.5}, {"YOUNG": 1.0, "GONE": None},
              {"YOUNG": None, "GONE": None}):
        expected = _pre_audit_coverage(z, (a, b))
        assert qm._coverage(z, (a, b), full) == expected
        assert qm._coverage(z, (a, b), None) == expected
        assert qm._coverage(z, (a, b)) == expected
    # the real registry, each seed series valid with a full history -> 1.0 per axis,
    # and with one series missing the renormalized pre-audit value is reproduced.
    for axis in ("growth", "inflation"):
        specs = qm._axis_specs(axis)
        counts = {s.series_id: 119 for s in specs}
        z = {s.series_id: 0.3 for s in specs}
        assert qm._coverage(z, specs, counts) == _pre_audit_coverage(z, specs) == 1.0
        z[specs[0].series_id] = None
        assert qm._coverage(z, specs, counts) == _pre_audit_coverage(z, specs) < 1.0


def test_valid_history_count_mirrors_the_standardizer_window() -> None:
    """nValid is the length of the eligible list standardized_latest builds:
    transform-dropped warmup periods are not valid, periods after as_of or before
    the 10-year cutoff are outside the window, and nValid == 0 exactly when the
    standardizer has nothing to standardize (returns None)."""
    from src.macro_sources import SEED_SOURCES
    from src.quadrant_score import standardized_latest

    indpro = next(s for s in SEED_SOURCES if s.series_id == "INDPRO")  # log_3m3m_ann_v1
    series = _monthly([100.0 + 0.1 * i + (0.5 if i % 7 == 0 else 0.0) for i in range(150)],
                      dt.date(2010, 1, 1))  # 2010-01 .. 2022-06
    # log_3m3m needs 5 prior months: transformed periods are 2010-06 .. 2022-06 (145)
    assert qm._valid_history_count(indpro, series, dt.date(2022, 6, 1)) == 121  # 2012-06 .. 2022-06
    assert qm._valid_history_count(indpro, series, dt.date(2010, 12, 1)) == 7    # 2010-06 .. 2010-12
    assert qm._valid_history_count(indpro, series, dt.date(2010, 5, 1)) == 0     # before the first
    assert standardized_latest(indpro, series, dt.date(2010, 5, 1)) is None
    assert standardized_latest(indpro, series, dt.date(2010, 12, 1)) is not None
    assert qm._valid_history_count(indpro, {}, dt.date(2022, 6, 1)) == 0


def test_score_axis_threads_the_valid_history_count(monkeypatch) -> None:
    """_score_axis reports nValid per series from the SAME PIT series it
    standardizes, and the worker's coverage of a young axis is 12/24, not 1."""
    from src.macro_sources import _macro

    spec = _macro("YOUNG", "growth", "synthetic", 0.25, "log_3m3m_ann_v1")
    series = _monthly([100.0 + 0.1 * i + (0.5 if i % 7 == 0 else 0.0) for i in range(17)],
                      dt.date(2023, 1, 1))  # transformed periods 2023-06 .. 2024-05 (12)
    monkeypatch.setattr(qm, "SEED_SOURCES", (spec,))
    monkeypatch.setattr(qm, "axis_weights", lambda axis: {"YOUNG": 1.0})
    monkeypatch.setattr(qm, "latest_vintage_as_of",
                        lambda conn, series_ids, t: {"YOUNG": series})

    t = dt.datetime(2024, 5, 15, tzinfo=dt.timezone.utc)
    score, _contrib, z, _av, _exp, history_counts = qm._score_axis(None, "growth", t)
    assert history_counts == {"YOUNG": 12}
    assert z["YOUNG"] is not None and score is not None
    assert abs(qm._coverage(z, (spec,), history_counts) - 0.5) < 1e-9


def test_v2_axis_observations_consume_the_six_tuple_with_history_counts(monkeypatch) -> None:
    """quadrant_macro_v2 (and v3 through it) import _score_axis/_coverage from this
    worker: the walk-back q_data must carry the §6 factor and the tuple unpack must
    match the six-tuple (no test imported the v2/v3 workers before)."""
    from src.workers import quadrant_macro_v2 as qm2

    a, b = _two_specs()
    monkeypatch.setattr(qm2, "_axis_specs", lambda axis: (a, b))
    scored = (1.0, {"YOUNG": 1.0}, {"YOUNG": 1.0, "GONE": None}, [], [],
              {"YOUNG": 12, "GONE": 0})
    calls: list[dt.datetime] = []

    def _fake_score_axis(conn, axis, t):
        calls.append(t)
        return scored

    monkeypatch.setattr(qm2, "_score_axis", _fake_score_axis)
    t = dt.datetime(2024, 5, 15, tzinfo=dt.timezone.utc)
    observations, current = qm2._axis_observations(None, "growth", t)
    assert len(observations) == qm2.V2_FILTER_HISTORY_MONTHS == len(calls)
    assert calls[-1] == t  # current month last
    assert all(abs(q - 0.25) < 1e-9 for _score, q in observations)  # 0.5·(12/24), not 0.5
    assert current == scored


# --------------------------------------------------------------------------- #
# Provenance: the valid-history counts enter source_vintage_hash (PR #161 review) #
# --------------------------------------------------------------------------- #
_UTC = dt.timezone.utc
_AV = [dt.datetime(2024, 3, 5, tzinfo=_UTC)]
_EXP = [dt.datetime(2024, 4, 15, tzinfo=_UTC)]
_G_NOW = (0.3, {"INDPRO": 0.3}, {"INDPRO": 0.3, "PAYEMS": None}, _AV, _EXP,
          {"INDPRO": 119, "PAYEMS": 7})
_I_NOW = (-0.2, {"CPILFESL": -0.2}, {"CPILFESL": -0.2}, _AV, _EXP, {"CPILFESL": 30})


_HIST = (("growth_history", [0.1, 0.2, 0.3]), ("inflation_history", [-0.1, 0.0]))


def test_vintage_hash_moves_with_the_history_count() -> None:
    """Coverage now depends on nValid, so provenance must too: a PIT correction that
    changes a history count with the standardized z unchanged changes the hash
    (hence the deterministic snapshot_id) instead of silently replacing the prior
    row under the same identity. Counts and confidence inputs are mandatory."""
    as_of = dt.date(2024, 3, 1)
    g_z, i_z = {"INDPRO": 0.3, "PAYEMS": None}, {"CPILFESL": -0.2}
    g_n, i_n = {"INDPRO": 119, "PAYEMS": 0}, {"CPILFESL": 119}
    base = qm._vintage_hash(g_z, i_z, as_of, g_n, i_n, confidence_inputs=_HIST)
    assert len(base) == 64
    # deterministic and independent of dict insertion order
    assert base == qm._vintage_hash(dict(reversed(list(g_z.items()))), i_z, as_of,
                                    {"PAYEMS": 0, "INDPRO": 119}, i_n,
                                    confidence_inputs=_HIST)
    # a count change ALONE moves the hash, on either axis
    assert base != qm._vintage_hash(g_z, i_z, as_of, {"INDPRO": 118, "PAYEMS": 0}, i_n,
                                    confidence_inputs=_HIST)
    assert base != qm._vintage_hash(g_z, i_z, as_of, g_n, {"CPILFESL": 23},
                                    confidence_inputs=_HIST)
    # z and as_of still bind
    assert base != qm._vintage_hash({"INDPRO": 0.31, "PAYEMS": None}, i_z, as_of, g_n, i_n,
                                    confidence_inputs=_HIST)
    assert base != qm._vintage_hash(g_z, i_z, dt.date(2024, 4, 1), g_n, i_n,
                                    confidence_inputs=_HIST)
    # the frozen (z, z, as_of) layout the pinned harness keeps is a different identity
    from harness.phase0q import decision as hd
    assert base != hd._vintage_hash(g_z, i_z, as_of)
    with pytest.raises(TypeError):
        qm._vintage_hash(g_z, i_z, as_of, g_n, i_n)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        qm._vintage_hash(g_z, i_z, as_of)  # type: ignore[call-arg]


def test_vintage_hash_binds_every_historical_confidence_input() -> None:
    """PR #161 review: the v2/v3 filter observations (and v1's score histories)
    carry history-dependent coverage at EVERY walk-back month, so a PIT backfill
    that moves an earlier month — current z-maps and counts unchanged — must
    change the identity; the label matters too, so a v3 auxiliary sequence can
    never alias a macro one, and numpy scalars hash like Python floats."""
    import numpy as np

    as_of = dt.date(2024, 3, 1)
    g_z, i_z = {"INDPRO": 0.3}, {"CPILFESL": -0.2}
    g_n, i_n = {"INDPRO": 119}, {"CPILFESL": 119}
    obs = [(0.10 + 0.01 * k, 1.0) for k in range(36)]
    inputs = (("growth_observations", obs), ("inflation_observations", obs))
    base = qm._vintage_hash(g_z, i_z, as_of, g_n, i_n, confidence_inputs=inputs)
    # one earlier month's q_data moves (12/24 instead of 1.0): new identity
    moved = list(obs)
    moved[3] = (obs[3][0], 0.5)
    assert base != qm._vintage_hash(g_z, i_z, as_of, g_n, i_n, confidence_inputs=(
        ("growth_observations", moved), ("inflation_observations", obs)))
    # a missing month, or a moved score, is a new identity as well
    gap = list(obs)
    gap[7] = (None, None)
    assert base != qm._vintage_hash(g_z, i_z, as_of, g_n, i_n, confidence_inputs=(
        ("growth_observations", obs), ("inflation_observations", gap)))
    # the v3 auxiliary sequence is bound under its own label
    assert base != qm._vintage_hash(g_z, i_z, as_of, g_n, i_n, confidence_inputs=(
        *inputs, ("growth_auxiliary_observations", obs)))
    assert qm._vintage_hash(g_z, i_z, as_of, g_n, i_n, confidence_inputs=(
        ("inflation_observations", obs), ("growth_observations", obs))) != base
    # tuples vs lists and numpy vs Python scalars are the same identity
    np_obs = [(np.float64(s), np.float64(q)) for s, q in obs]
    assert base == qm._vintage_hash(
        {"INDPRO": np.float64(0.3)}, i_z, as_of, {"INDPRO": np.int64(119)}, i_n,
        confidence_inputs=(("growth_observations", tuple(np_obs)),
                           ("inflation_observations", np_obs)))
    with pytest.raises(TypeError, match="provenance payload"):
        qm._vintage_hash(g_z, i_z, as_of, g_n, i_n, confidence_inputs=(("x", object()),))


def _stub_db(monkeypatch, module) -> None:
    import contextlib

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def commit(self): pass

    @contextlib.contextmanager
    def _got(conn, lock_id):
        yield True

    monkeypatch.setattr(module, "connect", lambda dsn: _Conn())
    monkeypatch.setattr(module, "advisory_lock", _got)
    monkeypatch.setattr(qa, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(qa, "upsert_snapshot", lambda conn, record, audit: None)


def _capture_builder(monkeypatch, module, name) -> dict:
    captured: dict = {}
    real = getattr(module, name)

    def _build(**kw):
        captured.update(kw)
        return real(**kw)

    monkeypatch.setattr(module, name, _build)
    return captured


def test_v1_run_stamps_the_new_version_and_hashes_the_history_counts(monkeypatch) -> None:
    _stub_db(monkeypatch, qm)
    monkeypatch.setattr(qa, "load_previous_snapshot", lambda conn, mv, as_of: None)
    monkeypatch.setattr(qm, "_score_axis",
                        lambda conn, axis, t: _G_NOW if axis == "growth" else _I_NOW)
    g_hist = [0.05 + 0.01 * i for i in range(30)]
    i_hist = [0.04 + 0.01 * i for i in range(30)]
    monkeypatch.setattr(qm, "_score_history",
                        lambda conn, axis, t: g_hist if axis == "growth" else i_hist)
    captured = _capture_builder(monkeypatch, qa, "build_snapshot")

    out = qm.run("postgresql://unused", calc_date="2024-03-05T00:00:00")

    as_of = dt.date(2024, 3, 5)
    assert out["model_version"] == captured["model_version"] == "macro_quadrant_us_v1.1"
    assert captured["growth_history"] == g_hist
    assert captured["source_vintage_hash"] == qm._vintage_hash(
        _G_NOW[2], _I_NOW[2], as_of, _G_NOW[5], _I_NOW[5],
        confidence_inputs=(("growth_history", g_hist), ("inflation_history", i_hist)))
    assert captured["growth_coverage"] == qm._coverage(
        _G_NOW[2], qm._axis_specs("growth"), _G_NOW[5])
    assert captured["inflation_coverage"] == qm._coverage(
        _I_NOW[2], qm._axis_specs("inflation"), _I_NOW[5])


def _v2_observations(n: int) -> list[tuple[float, float]]:
    return [(0.10 + 0.01 * k, 1.0) for k in range(n)]


def test_v2_run_stamps_the_new_version_and_hashes_the_history_counts(monkeypatch) -> None:
    from src import quadrant_assemble_v2 as qa2
    from src.workers import quadrant_macro_v2 as qm2

    _stub_db(monkeypatch, qm2)
    monkeypatch.setattr(qa2, "load_previous_state_v2", lambda conn, mv, as_of: {
        "previous_snapshot_id": None, "prev_published_quadrant": None})
    n = qm2.V2_FILTER_HISTORY_MONTHS
    g_obs, i_obs = _v2_observations(n), [(-0.05 + 0.01 * k, 1.0) for k in range(n)]
    monkeypatch.setattr(qm2, "_axis_observations", lambda conn, axis, t: (
        (g_obs, _G_NOW) if axis == "growth" else (i_obs, _I_NOW)))
    captured = _capture_builder(monkeypatch, qa2, "build_snapshot_v2")

    out = qm2.run("postgresql://unused", calc_date="2024-03-05T00:00:00")

    assert out["model_version"] == captured["model_version"] == "macro_quadrant_us_v2.1"
    assert captured["growth_observations"] == g_obs
    assert captured["inflation_observations"] == i_obs
    assert captured["source_vintage_hash"] == qm._vintage_hash(
        _G_NOW[2], _I_NOW[2], dt.date(2024, 3, 5), _G_NOW[5], _I_NOW[5],
        confidence_inputs=(("growth_observations", g_obs),
                           ("inflation_observations", i_obs)))


def test_v3_run_stamps_the_new_version_and_hashes_the_history_counts(monkeypatch) -> None:
    from src import quadrant_assemble_v2 as qa2
    from src.workers import quadrant_macro_v3 as qm3

    _stub_db(monkeypatch, qm3)
    monkeypatch.setattr(qa2, "load_previous_state_v2", lambda conn, mv, as_of: {
        "previous_snapshot_id": None, "prev_published_quadrant": None})
    n = qm3.V2_FILTER_HISTORY_MONTHS
    g_obs, i_obs = _v2_observations(n), [(-0.05 + 0.01 * k, 1.0) for k in range(n)]
    aux = [(0.20 + 0.01 * k, 1.0) for k in range(n)]
    monkeypatch.setattr(qm3, "_axis_observations", lambda conn, axis, t: (
        (g_obs, _G_NOW) if axis == "growth" else (i_obs, _I_NOW)))
    monkeypatch.setattr(qm3, "_market_growth_observations", lambda conn, t: aux)
    captured = _capture_builder(monkeypatch, qa2, "build_snapshot_v2")

    out = qm3.run("postgresql://unused", calc_date="2024-03-05T00:00:00")

    assert out["model_version"] == captured["model_version"] == "macro_quadrant_us_v3.1"
    assert captured["confidence_method"] == qm3.CONFIDENCE_METHOD_V3_FUSED
    assert captured["growth_auxiliary_observations"] == aux
    assert captured["source_vintage_hash"] == qm._vintage_hash(
        _G_NOW[2], _I_NOW[2], dt.date(2024, 3, 5), _G_NOW[5], _I_NOW[5],
        confidence_inputs=(("growth_observations", g_obs),
                           ("inflation_observations", i_obs),
                           ("growth_auxiliary_observations", aux)))


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
    assert out["model_version"] == qm.MODEL_VERSION
    assert out["status"] in {"valid", "low_confidence", "unavailable", "invalid"}
    if out["status"] == "valid":
        assert out["quadrant"] in {"recovery", "expansion", "slowdown", "contraction"}


def _env() -> dict[str, str]:
    """Read DATABASE_URL from the repo .env (or environment), mirroring the other
    env-gated smokes. Returns {} when nothing is configured (-> integration skip)."""
    import pathlib

    env_file = pathlib.Path(__file__).resolve().parents[1] / ".env"
    out: dict[str, str] = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"')
    out.update({k: v for k, v in _os.environ.items() if k == "DATABASE_URL"})
    return out


def _insert_snapshot_row(cur, *, snapshot_id, model_version, as_of, vintage_hash):
    """Minimal valid snapshot row honoring every NOT NULL / CHECK constraint
    (ck_rqs_asof_le_available, ck_rqs_computed_ge_available, stale_after ordering).
    status='unavailable' so quadrant/confidence are NULL (coherence-safe)."""
    available_at = dt.datetime.combine(as_of, dt.time(0, 0), tzinfo=dt.timezone.utc)
    far = dt.datetime(2999, 1, 1, tzinfo=dt.timezone.utc)
    cur.execute(
        "INSERT INTO regime_quadrant_snapshot ("
        " snapshot_id, previous_snapshot_id, status_at_compute,"
        " coverage_quality, freshness_quality, source_health_quality,"
        " transition_pending, as_of, available_at, computed_at,"
        " data_stale_after, pipeline_stale_after, stale_after,"
        " model_version, confidence_model_version, confidence_method,"
        " source_vintage_hash, growth_internal_sign, inflation_internal_sign"
        ") VALUES (%s, NULL, 'unavailable', 0.0, 0.0, 0.0, false,"
        " %s, %s, %s, %s, %s, %s, %s, 'confidence_v1.0', 'm', %s, 1, 1)",
        (snapshot_id, as_of, available_at, available_at, far, far, far,
         model_version, vintage_hash),
    )


@pytest.mark.skipif(not _env().get("DATABASE_URL"),
                    reason="needs DATABASE_URL (Tiger) for the schema integration test")
def test_load_previous_snapshot_is_target_aware_against_real_schema() -> None:
    """Integration (BEGIN/ROLLBACK, nothing persists): insert D1<D2<D3 for the same
    model_version, then prove load_previous_snapshot(conn, mv, D2) returns the D1
    row — NOT D3 (no look-ahead) and NOT D2 (strict <) — and that querying at D1
    returns None (genesis, nothing precedes it)."""
    import uuid as _uuid

    import psycopg

    dsn = _env()["DATABASE_URL"]
    mv = f"itest_predecessor_{_uuid.uuid4().hex[:8]}"
    d1, d2, d3 = dt.date(2024, 1, 2), dt.date(2024, 2, 1), dt.date(2024, 3, 1)
    id1, id2, id3 = str(_uuid.uuid4()), str(_uuid.uuid4()), str(_uuid.uuid4())

    import pathlib

    schema_sql = (pathlib.Path(__file__).resolve().parents[1]
                  / "schemas" / "regime_quadrant_snapshot.sql").read_text(encoding="utf-8")

    conn = psycopg.connect(dsn, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            # apply the Task 1 schema inside this transaction (idempotent CREATE ...
            # IF NOT EXISTS); the final ROLLBACK discards the table too if it was new,
            # so this test NEVER persists anything to prod.
            cur.execute(schema_sql)
            _insert_snapshot_row(cur, snapshot_id=id1, model_version=mv,
                                 as_of=d1, vintage_hash="h1")
            _insert_snapshot_row(cur, snapshot_id=id2, model_version=mv,
                                 as_of=d2, vintage_hash="h2")
            _insert_snapshot_row(cur, snapshot_id=id3, model_version=mv,
                                 as_of=d3, vintage_hash="h3")

        # target D2: newest snapshot STRICTLY BEFORE it is D1 (not D3=future, not D2=self).
        prev = qa.load_previous_snapshot(conn, mv, d2)
        assert prev is not None
        assert prev["previous_snapshot_id"] == id1

        # target D1: nothing precedes it -> genesis (None).
        assert qa.load_previous_snapshot(conn, mv, d1) is None
    finally:
        conn.rollback()  # discard the test rows — nothing persists
        conn.close()

"""Unit tests for the confidence_v2.0 production path (src.quadrant_confidence_v2 +
src.quadrant_assemble_v2) and the phase0q_006 amended timeline-gate semantics.

Fast and synthetic — the certified-pack replay evidence lives in the harness run
artifacts, not in CI.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path

from harness.phase0q import metrics as hp_metrics
from harness.phase0q import runner as hp_runner
from src import quadrant_confidence_v2 as c2
from src.quadrant_assemble import quadrant_from_signs
from src.quadrant_assemble_v2 import build_snapshot_v2, classify_axis_v2

REPO = Path(__file__).resolve().parents[1]

UTC = dt.timezone.utc
AS_OF = dt.date(2026, 6, 30)
COMPUTED_AT = dt.datetime(2026, 6, 30, tzinfo=UTC)
EXPIRY = [COMPUTED_AT + dt.timedelta(days=45)]


def _obs(scores, coverage=1.0):
    return [(s, coverage if s is not None else None) for s in scores]


def _steady_scores(level, n=36, wobble=0.05):
    return [level + (wobble if i % 2 else -wobble) for i in range(n)]


def _snapshot(g_scores, i_scores, prev_published=None, **overrides):
    kwargs = dict(
        as_of=AS_OF, computed_at=COMPUTED_AT, previous_snapshot_id=None,
        prev_published_quadrant=prev_published,
        growth_observations=_obs(g_scores),
        growth_coverage=1.0, growth_freshness=1.0, growth_health=1.0,
        inflation_observations=_obs(i_scores),
        inflation_coverage=1.0, inflation_freshness=1.0, inflation_health=1.0,
        input_available_ats=[COMPUTED_AT], critical_expiries=EXPIRY,
        model_version="macro_quadrant_us_v2", source_vintage_hash="deadbeef",
    )
    kwargs.update(overrides)
    return build_snapshot_v2(**kwargs)


# --------------------------------------------------------------------------- #
# Kalman filter                                                                #
# --------------------------------------------------------------------------- #

def test_kalman_posterior_variance_shrinks_below_measurement_noise():
    out = c2.kalman_filter_series(_obs(_steady_scores(1.0)))
    m, P, R = out[-1]
    assert 0.85 < m < 1.15
    assert P < R  # posterior tighter than one observation

def test_kalman_missing_observation_is_predict_only():
    obs = _obs(_steady_scores(1.0)) + [(None, None)]
    full = c2.kalman_filter_series(obs)
    m_last, p_last, r_last = full[-1]
    m_prev, p_prev, _ = full[-2]
    assert m_last == m_prev          # mean held
    assert p_last > p_prev           # variance grew
    assert r_last is None

def test_kalman_degraded_quality_inflates_noise_at_most_16x_variance():
    clean = c2.kalman_filter_series(_obs(_steady_scores(0.5), coverage=1.0))
    dirty = c2.kalman_filter_series(_obs(_steady_scores(0.5), coverage=0.05))
    r_clean = clean[-1][2]
    r_dirty = dirty[-1][2]
    assert r_dirty > r_clean
    assert r_dirty <= r_clean / (c2.Q_DATA_FLOOR ** 2) + 1e-12

def test_kalman_adapts_to_level_shift():
    scores = [0.5] * 20 + [-1.5] * 10
    out = c2.kalman_filter_series(_obs(scores))
    assert out[-1][0] < -0.5


# --------------------------------------------------------------------------- #
# Posterior + publish rule                                                     #
# --------------------------------------------------------------------------- #

def test_quadrant_posterior_sums_to_one_and_maps_signs():
    probs = c2.quadrant_posterior(0.9, 0.8)
    assert abs(sum(probs.values()) - 1.0) < 1e-12
    assert max(probs, key=probs.get) == quadrant_from_signs(1, 1)
    assert abs(probs[quadrant_from_signs(-1, -1)] - 0.02) < 1e-12

def test_publish_decision_requires_tau():
    weak = c2.quadrant_posterior(0.55, 0.55)
    publish, published, candidate, max_p = c2.publish_decision(weak, None)
    assert not publish and published is None
    assert max_p < c2.TAU_PUBLISH
    strong = c2.quadrant_posterior(0.95, 0.9)
    publish, published, candidate, _ = c2.publish_decision(strong, None)
    assert publish and published == candidate == quadrant_from_signs(1, 1)

def test_publish_decision_sticky_incumbent_within_delta():
    # The sticky mechanic, exercised at an explicit tau where it can bind. (At the
    # frozen tau=0.60 with independent axes, any publishable challenger already
    # beats the incumbent by > delta — the rule is a dormant safety valve there,
    # which the pack _003 evidence confirmed: delta had no effect on the timeline.)
    posterior = c2.quadrant_posterior(0.95, 0.48)   # (+,-) 0.494 vs (+,+) 0.456
    incumbent = quadrant_from_signs(1, 1)
    publish, published, candidate, _ = c2.publish_decision(
        posterior, incumbent, tau=0.40)
    assert publish
    assert candidate == quadrant_from_signs(1, -1)
    assert published == incumbent                    # gap 0.038 < delta 0.10
    # a decisive challenger wins
    decisive = c2.quadrant_posterior(0.95, 0.05)
    _, published2, _, _ = c2.publish_decision(decisive, incumbent, tau=0.40)
    assert published2 == quadrant_from_signs(1, -1)


# --------------------------------------------------------------------------- #
# Status order + assembler                                                     #
# --------------------------------------------------------------------------- #

def test_resolve_status_v2_order():
    base = dict(critical_structural_failure=False, coverage=1.0,
                critical_source_expired=False, source_health=1.0,
                filter_available=True, max_quadrant_posterior=0.9)
    assert c2.resolve_status_v2(**base) == "valid"
    assert c2.resolve_status_v2(**{**base, "critical_structural_failure": True}) == "invalid"
    assert c2.resolve_status_v2(**{**base, "coverage": 0.79}) == "unavailable"
    assert c2.resolve_status_v2(**{**base, "critical_source_expired": True}) == "stale"
    assert c2.resolve_status_v2(**{**base, "source_health": 0.89}) == "low_confidence"
    assert c2.resolve_status_v2(**{**base, "filter_available": False}) == "low_confidence"
    assert c2.resolve_status_v2(**{**base, "max_quadrant_posterior": 0.59}) == "low_confidence"

def test_classify_axis_v2_insufficient_observations():
    diag, ok, reason, p = classify_axis_v2(
        observations=_obs(_steady_scores(1.0, n=10)),
        coverage=1.0, freshness=1.0, source_health=1.0)
    assert not ok and reason == "insufficient_vintages" and p is None
    assert diag.candidate_confidence is None

def test_build_snapshot_v2_publishes_decisive_axes():
    snap = _snapshot(_steady_scores(1.0), _steady_scores(-1.0))
    assert snap.status_at_compute == "valid"
    assert snap.quadrant == quadrant_from_signs(1, -1)
    assert snap.candidate_confidence is not None
    assert snap.candidate_confidence >= c2.TAU_PUBLISH
    assert snap.confidence_model_version == c2.CONFIDENCE_MODEL_VERSION_V2
    assert snap.confidence_method == c2.CONFIDENCE_METHOD_V2
    assert not snap.transition_pending
    # per-axis diagnostics carry the filter posterior
    assert snap.growth.sign == 1 and snap.inflation.sign == -1
    assert snap.growth.uncertainty_adjusted is not None
    assert snap.growth.uncertainty_adjusted < snap.growth.uncertainty_raw

def test_build_snapshot_v2_neutral_axis_abstains_via_tau():
    # a sign-ambiguous oscillating axis observed through degraded-quality data:
    # the filtered level hovers near zero while the quality-inflated posterior
    # variance stays wide -> the joint posterior cannot clear tau.
    neutral = [2.0 * math.sin(i * 2.1) for i in range(36)]
    neutral_obs = [(s, 0.25) for s in neutral]
    # self-check the construction against the module's own policy functions
    m, P, _ = c2.kalman_filter_series(neutral_obs)[-1]
    p_neutral = c2.axis_sign_probability(m, P)
    assert max(p_neutral, 1 - p_neutral) < c2.TAU_PUBLISH
    snap = _snapshot(_steady_scores(1.0), neutral,
                     inflation_observations=neutral_obs)
    assert snap.status_at_compute == "low_confidence"
    assert snap.quadrant is None


def test_build_snapshot_v2_status_matches_publish_decision_wiring():
    """The assembler must faithfully apply the core policy: for filterable axes the
    snapshot is valid iff publish_decision publishes, with the same quadrant."""
    for g_scores, i_scores in (
        (_steady_scores(1.0), _steady_scores(-1.0)),
        (_steady_scores(0.4), _steady_scores(0.3)),
        ([3.0 if i % 2 else -3.0 for i in range(35)] + [0.1], _steady_scores(1.0)),
    ):
        mg, Pg, _ = c2.kalman_filter_series(_obs(g_scores))[-1]
        mi, Pi, _ = c2.kalman_filter_series(_obs(i_scores))[-1]
        posterior = c2.quadrant_posterior(
            c2.axis_sign_probability(mg, Pg), c2.axis_sign_probability(mi, Pi))
        publish, published, candidate, max_p = c2.publish_decision(posterior, None)
        snap = _snapshot(g_scores, i_scores)
        if publish:
            assert snap.status_at_compute == "valid"
            assert snap.quadrant == published
            assert snap.candidate_confidence == max_p
        else:
            assert snap.status_at_compute == "low_confidence"
            assert snap.quadrant is None
        assert snap.candidate_quadrant == candidate

def test_build_snapshot_v2_low_coverage_is_unavailable():
    snap = _snapshot(_steady_scores(1.0), _steady_scores(1.0),
                     growth_coverage=0.79)
    assert snap.status_at_compute == "unavailable"
    assert snap.quadrant is None and snap.candidate_confidence is None

def test_build_snapshot_v2_sticky_publish_keeps_incumbent():
    g = _steady_scores(1.0)
    # inflation drifts just below zero: challenger (+,-) edges incumbent (+,+)
    i = _steady_scores(0.4, n=30) + [0.1, 0.05, 0.0, -0.03, -0.05, -0.06]
    snap = _snapshot(g, i, prev_published=quadrant_from_signs(1, 1))
    if snap.status_at_compute == "valid":
        cand = snap.candidate_quadrant
        pub = snap.quadrant
        post = c2.quadrant_posterior(
            c2.axis_sign_probability(snap.growth.margin,
                                     snap.growth.uncertainty_adjusted ** 2),
            c2.axis_sign_probability(snap.inflation.margin,
                                     snap.inflation.uncertainty_adjusted ** 2))
        if post[cand] - post.get(quadrant_from_signs(1, 1), 0.0) < c2.DELTA_STICKY:
            assert pub == quadrant_from_signs(1, 1)


# --------------------------------------------------------------------------- #
# Amended same-quadrant-run metric + judge                                     #
# --------------------------------------------------------------------------- #

class _Row:
    def __init__(self, as_of, status, quadrant):
        self.as_of, self.status, self.quadrant = as_of, status, quadrant

    def has_valid_quadrant(self):
        return self.status == "valid" and self.quadrant is not None


def _month(i):
    year, month = divmod(i, 12)
    return dt.date(2021 + year, month + 1, 28)


def test_low_density_run_metric_flags_carry_anchor_not_fresh_persistence():
    # fresh persistence: 17 fresh expansion months (density 1.0)
    fresh = [_Row(_month(i), "valid", "expansion") for i in range(17)]
    tl = hp_metrics.regime_timeline_metrics(fresh)
    assert tl["max_same_quadrant_run_months"] == 17
    assert tl["max_low_density_same_quadrant_run_months"] == 0
    # carry anchor: 1 fresh seed + 16 carried months (density 1/17 < 0.50)
    anchor = [_Row(_month(0), "valid", "contraction")]
    anchor += [_Row(_month(i), "low_confidence", None) for i in range(1, 17)]
    tl = hp_metrics.regime_timeline_metrics(anchor)
    assert tl["max_same_quadrant_run_months"] == 17
    assert tl["max_low_density_same_quadrant_run_months"] == 17

def test_judge_uses_amended_semantics_when_policy_has_density_param():
    policy = json.loads(
        (REPO / "artifacts" / "quant" / "open_macro_v03_phase0q_006"
         / "timeline_gate_policy.json").read_text(encoding="utf-8"))
    assert hp_runner.validate_ratified_policy(policy) == []
    fresh = [_Row(_month(i), "valid", "expansion") for i in range(17)]
    timeline = {
        "regime_timeline_metrics": hp_metrics.regime_timeline_metrics(fresh),
        "upside_capture_by_calendar_year": {},
    }
    judgment = hp_runner.judge_timeline_gates(timeline, policy)
    entry = judgment["per_gate"]["max_same_quadrant_run_months"]
    assert entry["semantics"] == "low_fresh_density_runs_only"
    assert entry["measured"] == 0 and entry["go"] is True
    assert entry["raw_max_same_quadrant_run_months"] == 17
    assert judgment["gates_enforced"] is True

def test_superseded_phase0q_005_policy_no_longer_gates():
    old = json.loads(
        (REPO / "artifacts" / "quant" / "open_macro_v03_phase0q_005"
         / "timeline_gate_policy.json").read_text(encoding="utf-8"))
    violations = hp_runner.validate_ratified_policy(old)
    assert violations  # id + pin mismatch: a stale artifact can never gate again

def test_committed_policy_file_matches_pin():
    policy = hp_runner.load_timeline_gate_policy()
    assert policy is not None
    assert policy["phase0q_id"] == hp_runner.RATIFIED_TIMELINE_GATE_POLICY_PHASE0Q_ID
    assert hp_runner._policy_canonical_sha256(policy) == (
        hp_runner.RATIFIED_TIMELINE_GATE_POLICY_CANONICAL_SHA256)
    assert math.isclose(
        policy["gate_parameters"]["same_quadrant_run_min_fresh_density"], 0.50)

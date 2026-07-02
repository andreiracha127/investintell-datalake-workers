"""TDD suite for the phase0q_003 gate-amendment package.

Implements and locks the four quant_owner decisions (Andrei Rachadel, 2026-07-02):

  DECISION 1 - carry semantics: ``consumable_position_coverage`` (fresh valid OR
    carry-forward of the last valid decision of the GLOBAL latched chain, with
    provenance) becomes the blocking stress metric; fresh_decision_rate /
    abstention_rate / deadband_count / hold_low_confidence_count are DIAGNOSTICS.
  DECISION 2 - turnover context split (signal-design 0.60 vs reference-sleeve
    candidate 2.00) + compressed-sleeve alternative measurement.
  DECISION 3 - OOS fold seeding: each fold starts from the last valid position
    before fold start and the initial empty->position acquisition trade is NOT
    counted as fold economic turnover; verdict stays no_go_bounds_under_review.
  DECISION 4 - deliverable artifacts + evidence_002 + this test file.

Governance: evidence_001 is IMMUTABLE (hash-pinned); a new traceable judgment
supersedes the old one. Every new artifact keeps A5 blocked and activation false.

Network-free, DB-free, deterministic.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from harness.phase0q import amendments, decision, metrics, runner, sleeve

ROOT = Path(__file__).resolve().parents[1]
PACK_DIR = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"
EVIDENCE_001 = ROOT / "artifacts" / "quant" / "open_macro_v03_metric_evidence_001"
EVIDENCE_002 = ROOT / "artifacts" / "quant" / "open_macro_v03_metric_evidence_002"
PHASE0Q_003 = ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_003"


# --------------------------------------------------------------------------- #
# DECISION 1 - carry-coverage metric (consumable_position_coverage)           #
# --------------------------------------------------------------------------- #

class _D:
    """Minimal decision stub: (as_of, quadrant, valid?, transition_reason)."""

    def __init__(self, as_of, quadrant, valid=True, reason=None):
        self.as_of = as_of
        self.quadrant = quadrant
        self._valid = valid
        self.transition_reason = reason

    def has_valid_quadrant(self):
        return self._valid and self.quadrant is not None


def test_consumable_coverage_fresh_only_window_is_full():
    """A window where every scheduled date carries a FRESH valid decision has
    consumable_position_coverage == 1.0 and every date is provenance 'fresh'."""
    chain = [
        _D(dt.date(2020, 1, 31), "expansion"),
        _D(dt.date(2020, 2, 29), "expansion"),
        _D(dt.date(2020, 3, 31), "contraction"),
    ]
    scheduled = [d.as_of for d in chain]
    out = metrics.consumable_position_coverage(chain, scheduled)
    assert out["consumable_position_coverage"] == pytest.approx(1.0)
    assert all(e["source"] == "fresh" for e in out["per_date"])
    assert out["fresh_count"] == 3
    assert out["carry_count"] == 0


def test_consumable_coverage_carry_window_uses_last_valid_with_provenance():
    """A window whose scheduled dates ABSTAIN (no fresh valid) is still fully
    covered by carry-forward of the last valid decision of the global chain, and
    each carried date records WHICH decision date it carries."""
    chain = [
        _D(dt.date(2019, 12, 31), "contraction"),          # last valid before window
        _D(dt.date(2020, 1, 31), None, valid=False),       # abstains
        _D(dt.date(2020, 2, 29), None, valid=False),       # abstains
    ]
    scheduled = [dt.date(2020, 1, 31), dt.date(2020, 2, 29)]
    out = metrics.consumable_position_coverage(chain, scheduled)
    assert out["consumable_position_coverage"] == pytest.approx(1.0)
    assert out["fresh_count"] == 0
    assert out["carry_count"] == 2
    for e in out["per_date"]:
        assert e["source"] == "carry"
        assert e["carried_from"] == "2019-12-31"
        assert e["carried_quadrant"] == "contraction"


def test_consumable_coverage_no_prior_valid_position_is_no_go():
    """DECISION 1: a window that starts with NO prior valid latched position and no
    fresh valid decision has an absent consumable position for those dates ->
    coverage < 1.0 (no artificial per-window re-warmup)."""
    chain = [
        _D(dt.date(2020, 1, 31), None, valid=False),
        _D(dt.date(2020, 2, 29), None, valid=False),
        _D(dt.date(2020, 3, 31), "expansion"),   # first-ever valid, mid-window
    ]
    scheduled = [dt.date(2020, 1, 31), dt.date(2020, 2, 29), dt.date(2020, 3, 31)]
    out = metrics.consumable_position_coverage(chain, scheduled)
    # dates 1 and 2 have no consumable position; date 3 is fresh -> 1/3.
    assert out["consumable_position_coverage"] == pytest.approx(1 / 3)
    sources = {e["date"]: e["source"] for e in out["per_date"]}
    assert sources["2020-01-31"] == "absent"
    assert sources["2020-02-29"] == "absent"
    assert sources["2020-03-31"] == "fresh"


def test_carry_diagnostics_are_reported_not_gating():
    """fresh_decision_rate, abstention_rate, deadband_count and
    hold_low_confidence_count are reported as DIAGNOSTICS (not gate inputs)."""
    chain = [
        _D(dt.date(2020, 1, 31), "expansion"),
        _D(dt.date(2020, 2, 29), None, valid=False),
        _D(dt.date(2020, 3, 31), None, valid=False),
    ]
    scheduled = [d.as_of for d in chain]
    diag = metrics.carry_diagnostics(chain, scheduled)
    assert diag["fresh_decision_rate"] == pytest.approx(1 / 3)
    assert diag["abstention_rate"] == pytest.approx(2 / 3)
    assert diag["scheduled_count"] == 3
    assert diag["fresh_count"] == 1


def test_carry_diagnostics_count_deadband_and_hold_low_confidence():
    """DECISION 1: deadband_count and hold_low_confidence_count are reported per
    window from the decision rows' transition_reason audit tags."""
    chain = [
        _D(dt.date(2020, 1, 31), "expansion"),
        _D(dt.date(2020, 2, 29), None, valid=False, reason="deadband"),
        _D(dt.date(2020, 3, 31), None, valid=False,
           reason="hold_low_confidence,deadband"),
        _D(dt.date(2020, 4, 30), None, valid=False, reason="hold_low_confidence"),
    ]
    scheduled = [d.as_of for d in chain]
    diag = metrics.carry_diagnostics(chain, scheduled)
    assert diag["deadband_count"] == 2
    assert diag["hold_low_confidence_count"] == 2
    # only scheduled dates count: restrict to the first two dates.
    diag2 = metrics.carry_diagnostics(chain, scheduled[:2])
    assert diag2["deadband_count"] == 1
    assert diag2["hold_low_confidence_count"] == 0


def test_no_per_window_re_warmup_window_slice_equals_global_chain_slice():
    """Regression: the carry position for a window date is the last valid decision
    of the GLOBAL chain up to that date - identical to slicing the global chain,
    with no per-window re-seeding/re-warmup."""
    chain = [
        _D(dt.date(2019, 6, 30), "recovery"),
        _D(dt.date(2019, 9, 30), "expansion"),
        _D(dt.date(2019, 12, 31), "contraction"),
        _D(dt.date(2020, 1, 31), None, valid=False),
        _D(dt.date(2020, 2, 29), None, valid=False),
    ]
    scheduled = [dt.date(2020, 1, 31), dt.date(2020, 2, 29)]
    out = metrics.consumable_position_coverage(chain, scheduled)
    # global last-valid-before each scheduled date is 2019-12-31/contraction.
    for e in out["per_date"]:
        assert e["carried_from"] == "2019-12-31"
        assert e["carried_quadrant"] == "contraction"
    # slicing only the window (dropping pre-window chain) would lose the seed and
    # wrongly report absence: assert the metric did NOT do that.
    window_only = metrics.consumable_position_coverage(
        [d for d in chain if d.as_of in scheduled], scheduled)
    assert window_only["consumable_position_coverage"] == pytest.approx(0.0)
    assert out["consumable_position_coverage"] == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# DECISION 3 - fold turnover excludes the initial acquisition                 #
# --------------------------------------------------------------------------- #

def test_fold_turnover_excludes_initial_acquisition_unit():
    """The initial empty->position acquisition trade (the seed rebalance) is not
    counted as fold economic turnover. Given per-date turnover with a seed of 0.5
    on the first trade date, the fold-economic annualized turnover drops the seed."""
    dates = [dt.date(2020, 1, d) for d in range(1, 11)]
    turnover_by_date = {dates[0]: 0.5, dates[5]: 0.3}  # seed 0.5 + one real trade 0.3
    seed_date = dates[0]
    out = metrics.fold_turnover_excluding_seed(dates, turnover_by_date, seed_date)
    assert out["total_one_way_excl_seed"] == pytest.approx(0.3)
    assert out["seed_one_way"] == pytest.approx(0.5)
    assert out["max_trailing_252_excl_seed"] == pytest.approx(0.3)


def test_fold_turnover_single_seed_only_yields_zero_economic_turnover():
    """A fold whose ONLY trade is the initial acquisition has zero economic
    turnover once the seed is excluded (folds 6-8 in the real measurement)."""
    dates = [dt.date(2020, 1, d) for d in range(1, 11)]
    seed_date = dates[0]
    turnover_by_date = {seed_date: 0.5}
    out = metrics.fold_turnover_excluding_seed(dates, turnover_by_date, seed_date)
    assert out["total_one_way_excl_seed"] == pytest.approx(0.0)
    assert out["max_trailing_252_excl_seed"] == pytest.approx(0.0)


def test_simulate_reports_seed_rebalance_date():
    """The sleeve simulation must expose which rebalance is the initial acquisition
    (the seed), so fold turnover can exclude it deterministically."""
    start = dt.date(2020, 1, 1)
    prices = sleeve.PriceFrame(_flat_prices(
        sleeve.SLEEVE_TICKERS, start, 40, {t: 0.0 for t in sleeve.SLEEVE_TICKERS}))
    decisions = [_D(dt.date(2020, 1, 31), "expansion")]
    res = sleeve.simulate(prices, decisions, _sp(), start=start,
                          end=dt.date(2020, 2, 9), cost_bps=0)
    assert res.seed_rebalance_date == res.rebalance_dates[0]
    assert res.seed_rebalance_date is not None


def _flat_prices(tickers, start, days, rate_by_ticker):
    rows = []
    for t in tickers:
        p = 100.0
        d = start
        for _ in range(days):
            rows.append({"ticker": t, "date": d.isoformat(), "close": p,
                         "adjusted_close": p, "volume": 1000})
            p *= (1.0 + rate_by_ticker[t])
            d = d + dt.timedelta(days=1)
    return rows


def _sp(**over):
    return sleeve.SleeveParams(candidate_id=over.pop("candidate_id", "baseline_current"), **over)


# --------------------------------------------------------------------------- #
# DECISION 2 - compressed sleeve variant (sleeve_compressed_50)               #
# --------------------------------------------------------------------------- #

def test_compressed_sleeve_moves_each_quadrant_halfway_to_the_mean():
    """sleeve_compressed_50: each quadrant weight vector is moved 50% toward the
    mean of the four quadrant vectors, then renormalized."""
    compressed = sleeve.compressed_quadrant_weights(0.5)
    baseline = sleeve.PER_QUADRANT_BASELINE_WEIGHTS
    tickers = sleeve.SLEEVE_TICKERS
    mean = {t: sum(baseline[q].get(t, 0.0) for q in baseline) / len(baseline)
            for t in tickers}
    for q in baseline:
        # pre-renormalization: each weight moved 50% toward the cross-quadrant mean.
        moved = {t: baseline[q].get(t, 0.0) + 0.5 * (mean[t] - baseline[q].get(t, 0.0))
                 for t in tickers}
        moved = {t: w for t, w in moved.items() if w > 0.0}
        total = sum(moved.values())
        expected = {t: w / total for t, w in moved.items()}
        for t in tickers:
            assert compressed[q].get(t, 0.0) == pytest.approx(expected.get(t, 0.0), abs=1e-12)


def test_compressed_sleeve_weights_sum_to_one_and_respect_constraints():
    """DECISION 2 constraint re-check: every compressed quadrant target sums to 1
    and satisfies risk_cap / defensive_floor after the standard enforcement."""
    for quadrant in ("recovery", "expansion", "slowdown", "contraction"):
        w = sleeve.target_weights(
            quadrant, _sp(), sleeve.SLEEVE_TICKERS, compressed=True)
        assert abs(sum(w.values()) - 1.0) < 1e-12
        risk = sum(w.get(t, 0.0) for t in sleeve.RISK_ASSETS)
        defensive = sum(w.get(t, 0.0) for t in sleeve.DEFENSIVE_ASSETS)
        assert risk <= sleeve.RISK_CAP_BASELINE + 1e-9
        assert defensive >= sleeve.DEFENSIVE_FLOOR_BASELINE - 1e-9


def test_compressed_sleeve_reduces_cross_quadrant_dispersion():
    """Compression pulls the four quadrant vectors toward their common mean, so the
    max cross-quadrant weight spread strictly shrinks vs the baseline sleeve."""
    baseline = sleeve.PER_QUADRANT_BASELINE_WEIGHTS
    compressed = sleeve.compressed_quadrant_weights(0.5)

    def max_spread(book):
        spread = 0.0
        for t in sleeve.SLEEVE_TICKERS:
            vals = [book[q].get(t, 0.0) for q in book]
            spread = max(spread, max(vals) - min(vals))
        return spread

    assert max_spread(compressed) < max_spread(baseline)


# --------------------------------------------------------------------------- #
# DECISION 2 / judgment - turnover context split                              #
# --------------------------------------------------------------------------- #

def test_turnover_context_split_records_both_bounds_and_measured():
    """The judgment splits the 0.60 signal-design bound from the 2.00 reference-
    sleeve candidate bound, cites the MEASURED turnover from evidence_001, and marks
    pass_candidate_under_reference_sleeve_policy (NOT institutional approval)."""
    turnover_amendment = _json(PHASE0Q_003 / "turnover_threshold_context_amendment.json")
    assert turnover_amendment["signal_design_turnover_bound"] == 0.60
    assert turnover_amendment["reference_sleeve_turnover_candidate_bound"] == 2.00
    assert turnover_amendment["measured_turnover"] == pytest.approx(1.610346885365)
    assert turnover_amendment["status"] == "pass_candidate_under_reference_sleeve_policy"
    assert turnover_amendment["institutional_approval"] is False
    assert turnover_amendment["approved"] is False


def test_turnover_amendment_shows_cost_sensitivity_and_compressed_alternative():
    """DECISION 2: the 0/5/10/25 bps cost sensitivity must be SHOWN and the
    compressed-sleeve alternative measurement must be labeled alternative_measurement
    (not a replacement), side-by-side with the baseline sleeve."""
    amend = _json(PHASE0Q_003 / "turnover_threshold_context_amendment.json")
    assert amend["cost_sensitivity_bps"] == [0, 5, 10, 25]
    sens = amend["cost_sensitivity"]
    ev002 = _json(EVIDENCE_002 / "compressed_sleeve_alternative.json")
    for cb in ("0", "5", "10", "25"):
        assert sens["baseline_sleeve"][cb]["annualized_turnover"] == pytest.approx(
            ev002["baseline_sleeve"]["by_cost_bps"][cb]["annualized_turnover"])
        assert sens["sleeve_compressed_50"][cb]["annualized_turnover"] == pytest.approx(
            ev002["sleeve_compressed_50"]["by_cost_bps"][cb]["annualized_turnover"])
    assert amend["compressed_sleeve_alternative"]["measurement_class"] == "alternative_measurement"
    assert amend["compressed_sleeve_alternative"]["replaces_baseline"] is False
    # side-by-side trade-off is real: compressed turnover strictly below baseline.
    assert (sens["sleeve_compressed_50"]["5"]["annualized_turnover"]
            < sens["baseline_sleeve"]["5"]["annualized_turnover"])


# --------------------------------------------------------------------------- #
# evidence_001 IMMUTABILITY (sha256 pins of all 22 files)                     #
# --------------------------------------------------------------------------- #

# sha256 pins captured from the immutable measured evidence at branch base.
EVIDENCE_001_SHA256 = amendments.EVIDENCE_001_SHA256


def test_evidence_001_is_immutable_all_files_hash_pinned():
    """Every file under evidence_001 must match its pinned sha256: the measured
    evidence is IMMUTABLE and this package must never edit it."""
    files = sorted(p for p in EVIDENCE_001.rglob("*") if p.is_file())
    rel = {p.relative_to(EVIDENCE_001).as_posix() for p in files}
    assert rel == set(EVIDENCE_001_SHA256), (
        "evidence_001 file set changed vs the immutability pin")
    assert len(EVIDENCE_001_SHA256) >= 22
    for p in files:
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        key = p.relative_to(EVIDENCE_001).as_posix()
        assert digest == EVIDENCE_001_SHA256[key], f"evidence_001 mutated: {key}"


# --------------------------------------------------------------------------- #
# Judgment consistency - judged values equal referenced evidence values       #
# --------------------------------------------------------------------------- #

def test_judgment_gate_values_equal_referenced_evidence_values():
    """Every judged gate value in the new judgment equals the value in the
    referenced evidence (evidence_001 for the unchanged gates; evidence_002 for the
    re-measured stress/OOS numbers)."""
    judgment = _json(PHASE0Q_003 / "quantitative_gate_judgment.phase0q_003.json")
    ev001 = _json(EVIDENCE_001 / "quantitative_gate_report.measured.json")
    base = ev001["per_cost_level"]["5"]["per_gate"]

    gates = judgment["gates"]
    # turnover / drawdown / volatility measured values are pinned to evidence_001.
    assert gates["turnover"]["measured"] == pytest.approx(
        base["turnover"]["by_candidate"]["baseline_current"]["measured"])
    assert gates["drawdown"]["measured"] == pytest.approx(
        base["drawdown"]["by_candidate"]["baseline_current"]["measured"])
    assert gates["volatility"]["measured"] == pytest.approx(
        base["volatility"]["by_candidate"]["baseline_current"]["measured"])

    # unchanged verdicts for drawdown / volatility.
    assert gates["drawdown"]["verdict"] == "go"
    assert gates["volatility"]["verdict"] == "go"

    # stress verdict is go under carry semantics; OOS stays under review.
    assert gates["stress"]["verdict"] == "go"
    assert gates["out_of_sample"]["verdict"] == "no_go_bounds_under_review"

    # provenance references present + supersedes points at evidence_001's report.
    assert judgment["supersedes"].endswith("quantitative_gate_report.measured.json")
    assert judgment["approved"] is False


def test_stress_windows_pass_on_realized_risk_under_carry_semantics():
    """Under carry semantics every full_basket stress window has full consumable
    coverage AND its realized risk (worst_5d, window_MDD) is within the base
    envelope -> stress go. Values are pinned to evidence_002 measurements."""
    judgment = _json(PHASE0Q_003 / "quantitative_gate_judgment.phase0q_003.json")
    stress = judgment["gates"]["stress"]
    ev002_stress = _json(EVIDENCE_002 / "stress_carry_measurement.json")

    for wid in ("COVID_2020", "INFLATION_SHOCK_2022", "SVB_2023", "Q4_2018"):
        w = stress["windows"][wid]
        m = ev002_stress["windows"][wid]
        assert w["consumable_position_coverage"] == pytest.approx(1.0)
        assert w["worst_5d_return"] == pytest.approx(m["worst_5d_return"])
        assert w["worst_5d_return"] >= -0.10
        assert w["window_MDD"] == pytest.approx(m["window_MDD"])
        assert w["go"] is True


# --------------------------------------------------------------------------- #
# DECISION 4 - amendment + governance pins                                    #
# --------------------------------------------------------------------------- #

def test_stress_gate_semantics_amendment_cites_superseded_rule():
    """The stress amendment defines all four carry rules, cites the superseded
    fresh-coverage==1.0 rule from stress_oos_policy.json, and stays candidate."""
    amend = _json(PHASE0Q_003 / "stress_gate_semantics_amendment.json")
    assert amend["status"] == "candidate_not_approved"
    assert amend["ratified_by"] == "quant_owner"
    assert amend["decision_date"] == "2026-07-02"
    superseded = amend["supersedes_rule"]
    assert "decision_coverage_min" in json.dumps(superseded)
    rules = amend["carry_semantics"]
    assert "consumable_position_coverage" in rules
    assert rules["diagnostics_not_gating"]
    assert "no_per_window_re_warmup" in rules
    assert "realized_risk_judged_at_base_profile" in rules


def test_oos_fold_seeding_fix_report_documents_before_after():
    """The OOS report documents the P1 seeding fix (PR #20 commit f87c8d2), the
    additional fold-turnover-seeding semantics implemented here, and before/after
    OOS numbers - but the verdict stays under review."""
    report = _json(PHASE0Q_003 / "oos_fold_seeding_fix_report.json")
    assert report["p1_seeding_fix"]["pr"] == 20
    assert report["p1_seeding_fix"]["commit"] == "f87c8d2"
    assert "before" in report["fold_turnover_semantics"]
    assert "after" in report["fold_turnover_semantics"]
    assert report["verdict"] == "no_go_bounds_under_review"
    assert report["bounds_changed"] is False


def _GOVERNANCE_FILES():
    return [
        PHASE0Q_003 / "stress_gate_semantics_amendment.json",
        PHASE0Q_003 / "turnover_threshold_context_amendment.json",
        PHASE0Q_003 / "oos_fold_seeding_fix_report.json",
        PHASE0Q_003 / "quantitative_gate_judgment.phase0q_003.json",
        EVIDENCE_002 / "stress_carry_measurement.json",
        EVIDENCE_002 / "oos_remeasured.json",
    ]


def test_all_new_artifacts_keep_a5_blocked_and_activation_false():
    """Every new artifact keeps A5=blocked, runtime_activation=false,
    activation_allowed=false, allocator_publish=false, official_result=false,
    db_write_mode=none (governance frame)."""
    for path in _GOVERNANCE_FILES():
        payload = _json(path)
        gov = payload.get("governance", payload)
        assert gov.get("A5", payload.get("A5")) == "blocked", path.name
        assert gov.get("runtime_activation", payload.get("runtime_activation")) is False, path.name
        assert gov.get("activation_allowed", payload.get("activation_allowed")) is False, path.name
        assert gov.get("allocator_publish", payload.get("allocator_publish")) is False, path.name
        assert gov.get("official_result", payload.get("official_result")) is False, path.name
        assert gov.get("db_write_mode", payload.get("db_write_mode")) == "none", path.name


def test_new_artifacts_contain_no_activation_or_approval_markers():
    """No new artifact may contain an activation/approval marker (mirrors the
    phase0q immutability guard)."""
    forbidden = (
        '"runtime_activation": true', '"activation_allowed": true',
        '"allocator_publish": true', '"official_result": true',
        '"freeze_ready": true', '"approved": true', "A5=unblocked",
        '"A5": "unblocked"', '"db_write_mode": "write"',
    )
    for root in (PHASE0Q_003, EVIDENCE_002):
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8")
            for marker in forbidden:
                assert marker not in text, f"{path.name} contains {marker}"


def test_no_forbidden_records_created():
    """This package must NOT create review_closure_record.json, must NOT unblock
    Task 2, must NOT approve final thresholds."""
    assert not (PHASE0Q_003 / "review_closure_record.json").exists()
    for root in (PHASE0Q_003, EVIDENCE_002):
        for path in root.rglob("*"):
            if path.is_file():
                assert path.name != "review_closure_record.json"


# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #

def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))

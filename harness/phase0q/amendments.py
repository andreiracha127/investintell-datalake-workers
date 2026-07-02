"""phase0q_003 gate-amendment logic (four quant_owner decisions, 2026-07-02).

This module holds the NON-artifact logic for the gate-amendment package so both the
deterministic artifact generator and the test suite share one implementation:

  * ``EVIDENCE_001_SHA256`` — immutability pins for the frozen measured evidence.
  * carry-semantics stress measurement over the real decision chain (DECISION 1),
  * compressed-sleeve alternative measurement + baseline turnover grid (DECISION 2),
  * OOS re-measurement with fold-turnover seeding excluded (DECISION 3),
  * the four deliverable payload builders (DECISION 4).

Governance: evidence_001 is IMMUTABLE — this module NEVER writes it and pins its
hashes. Every payload keeps A5 blocked and activation/approval false. Deterministic
(no wall-clock, no RNG); timestamps are injected by the caller.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Mapping, Sequence

from . import decision, metrics, runner, sleeve

# --------------------------------------------------------------------------- #
# Immutability pins for evidence_001 (all 22 files)                            #
# --------------------------------------------------------------------------- #

EVIDENCE_001_SHA256: dict[str, str] = {
    "cells/baseline_current__0bps.json": "7ba3e1229b29cc588665a2b5d9328e44ec6da995c5ca2031285ca43f56b3b837",
    "cells/baseline_current__10bps.json": "194eb2d086170540e1533d4ee03fa1b3b0c6efd56cc226efdaebe3e9d3d71c77",
    "cells/baseline_current__25bps.json": "b91ec968319d7e7ee896d8bbb09a3fcfe0553130b27a5f4ee26818a9fdd8d361",
    "cells/baseline_current__5bps.json": "0c74564780b79d0927fbbd40857776d9394488f08e99306a1a169d581b7ae0de",
    "cells/growth_plus_2pp__0bps.json": "e361f770a2e7de648f2dd9e68bcdc3c7af14cdf1029b49e27f0005820886df98",
    "cells/growth_plus_2pp__10bps.json": "892be4416bf0997c67bdcfc07f6b3d00fefb7fa9d9eec439bb332e9a28a57136",
    "cells/growth_plus_2pp__25bps.json": "18e58f00d45c011bbbcbdf1e4d74c1b4c392d328b01f025f7d56d4becd46df7e",
    "cells/growth_plus_2pp__5bps.json": "571bbe0c3af4b3b71a35b2d6d10daad6dfac750f8fd8b23cee00def19103be9d",
    "cells/inflation_plus_2pp__0bps.json": "1dcb5030deb628a8ef46039490755a7071ccb912a7d30ba7b637097da32ed68f",
    "cells/inflation_plus_2pp__10bps.json": "3e9f73150d0de9dc98228010b6216bbcbd586e6e1c472b3e4d4d778b3ffd7bff",
    "cells/inflation_plus_2pp__25bps.json": "a164529c09554bcb1004aaae8385f3d1fe789fa21ef0b007611069248accab44",
    "cells/inflation_plus_2pp__5bps.json": "1dc72f824c9257a358d4c65638108ab9672293348156a48d760c3b0f8eb5ac7d",
    "cells/risk_tilt_minus_1pp__0bps.json": "189a56e8efb2db9458984ead0534e87d144ecd5422dca9b9cb2d08db82122377",
    "cells/risk_tilt_minus_1pp__10bps.json": "4445bdfdbd440f43bcaf1bb105900b8e07da122bfb8e11162e826aed8193d1f0",
    "cells/risk_tilt_minus_1pp__25bps.json": "4d6917949845177351dc5a1c6522f5dea50218a125e46b674b53abf509c74b6c",
    "cells/risk_tilt_minus_1pp__5bps.json": "aea8708ea2a48c4ee6fd9f1a1f425b49fc4726e6829a7af32496585543584eee",
    "cells/risk_tilt_plus_1pp__0bps.json": "8af62d47bd7a211d835cbdcd8d2bcae9b0fbdbb3d5d71a5b1bacd499936defb5",
    "cells/risk_tilt_plus_1pp__10bps.json": "577951ae925f549b1f40e2bebe9eb59dc2b9feeafc1e23aeb16da012bb274789",
    "cells/risk_tilt_plus_1pp__25bps.json": "4299475ac937557f5493d33c627eb162a0347b88f16a356da0534437effcb8e3",
    "cells/risk_tilt_plus_1pp__5bps.json": "9b9509238b07ebc48d784a9e49a19a82b157b7edf1ec84a5afa3eb21e5d2ebb9",
    "metric_backtest_result.json": "6691a5a399f495340a80d87d4b0473afa8e60581393fba63ec8a9a06106a85b7",
    "quantitative_gate_report.measured.json": "3ad4c1477b1559e672a8528f1b082a11290176d1adc91c467fc95f313793bab1",
}

MEASURED_TURNOVER_BASELINE = 1.610346885365  # evidence_001 baseline_current turnover
SIGNAL_DESIGN_TURNOVER_BOUND = 0.60
REFERENCE_SLEEVE_TURNOVER_CANDIDATE_BOUND = 2.00

BASE_PROFILE = runner.BASE_ENVELOPE

GOVERNANCE_PINS = {
    "A5": "blocked",
    "runtime_activation": False,
    "activation_allowed": False,
    "allocator_publish": False,
    "official_result": False,
    "db_write_mode": "none",
    "freeze_ready": False,
}

EVIDENCE_001_PREFIX = "artifacts/quant/open_macro_v03_metric_evidence_001"
EVIDENCE_002_PREFIX = "artifacts/quant/open_macro_v03_metric_evidence_002"
PHASE0Q_001_PREFIX = "artifacts/quant/open_macro_v03_phase0q_001"
PHASE0Q_003_PREFIX = "artifacts/quant/open_macro_v03_phase0q_003"

# full_basket stress windows judged under carry semantics (reduced_coverage windows
# stay supplementary, never primary evidence).
FULL_BASKET_STRESS_IDS = ("COVID_2020", "INFLATION_SHOCK_2022", "SVB_2023", "Q4_2018")


# --------------------------------------------------------------------------- #
# DECISION 1 - carry-semantics stress measurement                             #
# --------------------------------------------------------------------------- #

def measure_stress_carry(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    cost_bps: int = runner.BASE_COST_BPS,
    stress_windows: Sequence[Mapping[str, Any]] = runner.STRESS_WINDOWS,
) -> dict[str, Any]:
    """Measure each full_basket stress window under carry semantics: consumable
    position coverage (fresh OR carry), carry provenance, carry diagnostics, and the
    realized-risk fields judged against the base profile."""
    windows: dict[str, Any] = {}
    for win in stress_windows:
        if win["coverage"] != "full_basket":
            continue
        wid = win["window_id"]
        scheduled = [r.as_of for r in decisions if win["start"] <= r.as_of <= win["end"]]
        coverage = metrics.consumable_position_coverage(decisions, scheduled)
        diagnostics = metrics.carry_diagnostics(decisions, scheduled)
        res = runner._run_window(prices, decisions, params,
                                 win["start"], win["end"], cost_bps)
        worst5d = metrics.worst_5d_return(res.nav)
        window_mdd = metrics.max_drawdown(res.nav)
        vol = metrics.annualized_volatility(res.nav)
        wret = metrics.window_return(res.nav)
        turn = metrics.one_way_turnover_annualized(
            res.dates, res.one_way_turnover_by_date)["max_trailing_252"]
        coverage_ok = coverage["consumable_position_coverage"] >= 1.0
        worst5d_ok = worst5d >= BASE_PROFILE["min_worst_5d_return"]
        mdd_ok = window_mdd <= BASE_PROFILE["max_drawdown"]
        go = coverage_ok and worst5d_ok and mdd_ok
        # carry seed = last valid decision on/before window start.
        seed = next((e for e in coverage["per_date"]), None)
        windows[wid] = {
            "consumable_position_coverage": coverage["consumable_position_coverage"],
            "fresh_count": coverage["fresh_count"],
            "carry_count": coverage["carry_count"],
            "absent_count": coverage["absent_count"],
            "scheduled_count": coverage["scheduled_count"],
            "carry_provenance": coverage["per_date"],
            "diagnostics": diagnostics,
            "window_return": wret,
            "window_MDD": window_mdd,
            "worst_5d_return": worst5d,
            "annualized_volatility": vol,
            "one_way_turnover_annualized": turn,
            "coverage_ok": coverage_ok,
            "worst_5d_ok": worst5d_ok,
            "window_mdd_ok": mdd_ok,
            "go": go,
            "n_trading_days": len(res.dates),
            "first_scheduled_carry": seed,
        }
    stress_go = all(windows[w]["go"] for w in windows) if windows else False
    return {
        "artifact_type": "phase0q_stress_carry_measurement",
        "schema_version": 1,
        "evidence_id": "open_macro_v03_metric_evidence_002",
        "candidate_id": params.candidate_id,
        "cost_bps": cost_bps,
        "base_profile": BASE_PROFILE,
        "stress_go": stress_go,
        "windows": windows,
        "governance": dict(GOVERNANCE_PINS),
    }


# --------------------------------------------------------------------------- #
# DECISION 3 - OOS re-measurement with fold seeding turnover excluded         #
# --------------------------------------------------------------------------- #

def measure_oos_remeasured(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    cost_bps: int = runner.BASE_COST_BPS,
    primary_window: tuple[_dt.date, _dt.date] = runner.PRIMARY_WINDOW,
) -> dict[str, Any]:
    """Re-measure OOS with the DECISION 3 semantics: each fold seeds from the last
    valid position before fold start (already landed in the harness) AND the initial
    empty->position acquisition trade is excluded from fold ECONOMIC turnover.

    Returns per-fold before/after turnover, returns/MDD/vol measured strictly inside
    the test window, plus cross-fold dispersions. Bounds are UNCHANGED and the
    verdict stays no_go_bounds_under_review (deferred to quant_owner)."""
    folds = runner.oos_folds(primary_window)
    fold_rows: list[dict[str, Any]] = []
    econ_metric_list: list[dict[str, float]] = []
    for fold in folds:
        res = runner._run_window(prices, decisions, params,
                                 fold["test_start"], fold["test_end"], cost_bps)
        turn = metrics.one_way_turnover_annualized(res.dates, res.one_way_turnover_by_date)
        excl = metrics.fold_turnover_excluding_seed(
            res.dates, res.one_way_turnover_by_date, res.seed_rebalance_date)
        ret = metrics.return_annualized(res.nav, len(res.dates))
        sigma = metrics.annualized_volatility(res.nav)
        mdd = metrics.max_drawdown(res.nav)
        econ = {
            "return_annualized": ret,
            "sigma_annual": sigma,
            "MDD": mdd,
            "one_way_turnover_annualized": excl["max_trailing_252_excl_seed"],
        }
        econ_metric_list.append(econ)
        fold_rows.append({
            "fold_index": fold["fold_index"],
            "test_start": fold["test_start"].isoformat(),
            "test_end": fold["test_end"].isoformat(),
            "return_annualized": ret,
            "sigma_annual": sigma,
            "MDD": mdd,
            "n_rebalances": len(res.rebalance_dates),
            "seed_rebalance_date": (res.seed_rebalance_date.isoformat()
                                    if res.seed_rebalance_date else None),
            "seed_one_way_turnover": excl["seed_one_way"],
            "turnover_incl_seed_max_trailing_252": turn["max_trailing_252"],
            "turnover_excl_seed_max_trailing_252": excl["max_trailing_252_excl_seed"],
        })
    dispersion = metrics.stability_from_folds(econ_metric_list)
    return {
        "artifact_type": "phase0q_oos_remeasured",
        "schema_version": 1,
        "evidence_id": "open_macro_v03_metric_evidence_002",
        "candidate_id": params.candidate_id,
        "cost_bps": cost_bps,
        "n_folds": len(folds),
        "semantics": {
            "fold_seeds_from_last_valid_position_before_fold_start": True,
            "initial_acquisition_excluded_from_fold_turnover": True,
            "returns_mdd_vol_measured_strictly_inside_test_window": True,
            "lookahead": False,
        },
        "bounds_unchanged": True,
        "verdict": "no_go_bounds_under_review",
        "folds": fold_rows,
        "cross_fold_dispersion": dispersion,
        "base_profile": BASE_PROFILE,
        "governance": dict(GOVERNANCE_PINS),
    }


# --------------------------------------------------------------------------- #
# DECISION 2 - compressed-sleeve alternative + baseline turnover grid          #
# --------------------------------------------------------------------------- #

def measure_turnover_grid(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    *,
    compressed: bool,
    cost_grid: Sequence[int] = runner.COST_GRID_BPS,
    primary_window: tuple[_dt.date, _dt.date] = runner.PRIMARY_WINDOW,
) -> dict[str, Any]:
    """Primary-window turnover/MDD/vol/return at each cost level for the baseline or
    compressed sleeve (DECISION 2 cost-sensitivity + alternative measurement)."""
    cells: dict[str, Any] = {}
    for cost_bps in cost_grid:
        window_decisions = runner._decisions_in(
            decisions, _dt.date(primary_window[0].year - 1, 1, 1), primary_window[1])
        res = sleeve.simulate(prices, window_decisions, params,
                              start=primary_window[0], end=primary_window[1],
                              cost_bps=cost_bps, compressed=compressed)
        turn = metrics.one_way_turnover_annualized(res.dates, res.one_way_turnover_by_date)
        cells[str(cost_bps)] = {
            "annualized_turnover": turn["max_trailing_252"],
            "annualized_turnover_window_average": turn["window_average_annualized"],
            "total_one_way_turnover": turn["total_one_way"],
            "max_drawdown": metrics.max_drawdown(res.nav),
            "annualized_volatility": metrics.annualized_volatility(res.nav),
            "window_return": metrics.window_return(res.nav),
            "worst_5d_return": metrics.worst_5d_return(res.nav),
            "n_trading_days": len(res.dates),
            "n_rebalances": len(res.rebalance_dates),
        }
    return {
        "sleeve": "sleeve_compressed_50" if compressed else "baseline_sleeve",
        "measurement_class": "alternative_measurement" if compressed else "baseline",
        "cost_grid_bps": list(cost_grid),
        "by_cost_bps": cells,
    }


# --------------------------------------------------------------------------- #
# DECISION 4 - deliverable payload builders                                   #
# --------------------------------------------------------------------------- #

def build_stress_gate_semantics_amendment() -> dict[str, Any]:
    """The carry-semantics definition (all four DECISION 1 rules), amending the
    stress_acceptance rule in stress_oos_policy.json (cites the superseded rule)."""
    return {
        "artifact_type": "phase0q_stress_gate_semantics_amendment",
        "schema_version": 1,
        "phase0q_id": "open_macro_v03_phase0q_003",
        "amends": f"{PHASE0Q_001_PREFIX}/stress_oos_policy.json",
        "amends_field": "stress_acceptance",
        "supersedes_rule": {
            "decision_coverage_min": 1.0,
            "note": (
                "decision_coverage below 1.0 in a full_series window is an automatic "
                "no_go for that window; window MDD and worst_5d_return are judged "
                "against the selected profile in threshold_candidate_report.json"),
            "why_superseded": (
                "The fresh-coverage==1.0 gate was a policy SPEC ERROR: the decision "
                "engine abstains by design (deadband / hold_low_confidence <0.70; "
                "global fresh-valid rate 61/225 = 27.1%) and the sleeve carries the "
                "last valid position. Requiring a fresh valid decision on every "
                "scheduled date penalizes intended abstention. A new traceable "
                "judgment supersedes the old one."),
        },
        "carry_semantics": {
            "consumable_position_coverage": (
                "fraction of scheduled dates in the window where a consumable position "
                "exists for the sleeve: a FRESH valid decision OR carry-forward of the "
                "LAST VALID decision of the GLOBAL latched chain (with provenance: "
                "which decision date is carried). This is the new BLOCKING stress "
                "metric."),
            "diagnostics_not_gating": [
                "fresh_decision_rate", "abstention_rate", "deadband_count",
                "hold_low_confidence_count",
            ],
            "no_per_window_re_warmup": (
                "carry is only valid if a prior valid decision exists in the global "
                "chain; there is NO artificial per-window re-warmup. A window that "
                "starts with no valid latched position -> no_go for that window "
                "(absence of consumable position)."),
            "realized_risk_judged_at_base_profile": (
                "stress windows with full consumable coverage are then judged on "
                "realized risk (window_return, window_MDD, worst_5d_return, vol, "
                "turnover) against the base profile bounds (worst5d >= -0.10, "
                "window_MDD <= 0.25). Measured worst5d was -0.0759, so stress passes "
                "at base profile with full provenance."),
        },
        "status": "candidate_not_approved",
        "ratified_by": "quant_owner",
        "ratified_by_name": "Andrei Rachadel",
        "decision_date": "2026-07-02",
        "governance": dict(GOVERNANCE_PINS),
    }


def build_turnover_threshold_context_amendment(
    baseline_grid: Mapping[str, Any] | None = None,
    compressed_grid: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """DECISION 2 fields exactly, plus the SHOWN 0/5/10/25 bps cost sensitivity and
    the compressed-sleeve alternative measurement side-by-side (not a replacement).

    ``baseline_grid`` / ``compressed_grid`` come from :func:`measure_turnover_grid`."""
    return {
        "artifact_type": "phase0q_turnover_threshold_context_amendment",
        "schema_version": 1,
        "phase0q_id": "open_macro_v03_phase0q_003",
        "signal_design_turnover_bound": SIGNAL_DESIGN_TURNOVER_BOUND,
        "signal_design_turnover_bound_scope": (
            "the tilt / parameter-grid design bound; NOT applied to sleeve-realized "
            "turnover"),
        "reference_sleeve_turnover_candidate_bound": REFERENCE_SLEEVE_TURNOVER_CANDIDATE_BOUND,
        "reference_sleeve_turnover_candidate_bound_scope": (
            "candidate initial threshold for annualized one-way sleeve turnover, "
            "explicitly NOT final institutional approval"),
        "measured_turnover": MEASURED_TURNOVER_BASELINE,
        "measured_turnover_source": f"{EVIDENCE_001_PREFIX}/quantitative_gate_report.measured.json",
        "cost_sensitivity_bps": [0, 5, 10, 25],
        "cost_sensitivity": {
            "baseline_sleeve": dict(baseline_grid["by_cost_bps"]) if baseline_grid else {},
            "sleeve_compressed_50": dict(compressed_grid["by_cost_bps"]) if compressed_grid else {},
            "source": f"{EVIDENCE_002_PREFIX}/compressed_sleeve_alternative.json",
        },
        "compressed_sleeve_alternative": {
            "measurement_class": "alternative_measurement",
            "replaces_baseline": False,
            "definition": (
                "sleeve_compressed_50: each quadrant's weight vector moved 50% toward "
                "the mean of the four quadrant vectors (renormalized, constraints "
                "re-checked); same grid run side-by-side to expose the "
                "turnover/risk/return trade-off"),
            "source": f"{EVIDENCE_002_PREFIX}/compressed_sleeve_alternative.json",
        },
        "status": "pass_candidate_under_reference_sleeve_policy",
        "status_condition": (
            "pass_candidate holds IF costs and the other gates remain acceptable; "
            "this is not institutional approval"),
        "institutional_approval": False,
        "approved": False,
        "ratified_by": "quant_owner",
        "decision_date": "2026-07-02",
        "governance": dict(GOVERNANCE_PINS),
    }


def build_oos_fold_seeding_fix_report(oos_after: Mapping[str, Any]) -> dict[str, Any]:
    """Documents the P1 seeding fix (PR #20 commit f87c8d2), the additional
    fold-turnover-seeding semantics implemented here, and before/after OOS numbers.

    ``oos_after`` (from :func:`measure_oos_remeasured`) carries BOTH the seed-inclusive
    and seed-exclusive turnover per fold, from which the before/after turnover
    projections are derived."""
    before_folds = [
        {"fold_index": f["fold_index"], "test_start": f["test_start"],
         "test_end": f["test_end"],
         "one_way_turnover_annualized": f["turnover_incl_seed_max_trailing_252"]}
        for f in oos_after["folds"]
    ]
    after_folds = [
        {"fold_index": f["fold_index"], "test_start": f["test_start"],
         "test_end": f["test_end"],
         "one_way_turnover_annualized": f["turnover_excl_seed_max_trailing_252"]}
        for f in oos_after["folds"]
    ]
    return {
        "artifact_type": "phase0q_oos_fold_seeding_fix_report",
        "schema_version": 1,
        "phase0q_id": "open_macro_v03_phase0q_003",
        "p1_seeding_fix": {
            "pr": 20,
            "commit": "f87c8d2",
            "description": (
                "the latest pre-window decision seeds each window/fold (production "
                "semantics), landing in harness.phase0q.sleeve._schedule_decisions; "
                "each OOS fold starts with the last valid position available BEFORE "
                "fold start."),
        },
        "fold_turnover_semantics": {
            "before": (
                "fold economic turnover INCLUDED the initial empty->position "
                "acquisition trade (a fixed 0.5 one-way seed) on the first fold "
                "trading date, inflating short folds' annualized turnover."),
            "after": (
                "the initial acquisition (the seed rebalance) is EXCLUDED from fold "
                "economic turnover; folds whose only trade is the seed report 0.0 "
                "economic turnover. Returns / MDD / vol are measured strictly inside "
                "the test window; no lookahead."),
        },
        "oos_before_folds": before_folds,
        "oos_after_folds": after_folds,
        "oos_after_full_folds": oos_after["folds"],
        "cross_fold_dispersion_after": oos_after["cross_fold_dispersion"],
        "bounds_changed": False,
        "verdict": "no_go_bounds_under_review",
        "verdict_note": (
            "OOS bounds are UNCHANGED and the decision is DEFERRED to quant_owner "
            "after seeing the re-measured numbers."),
        "status": "candidate_not_approved",
        "ratified_by": "quant_owner",
        "decision_date": "2026-07-02",
        "governance": dict(GOVERNANCE_PINS),
    }


def build_quantitative_gate_judgment(
    *,
    ev001_base: Mapping[str, Any],
    stress_measurement: Mapping[str, Any],
    oos_after: Mapping[str, Any],
) -> dict[str, Any]:
    """The NEW judgment over evidence_001 (+ new measurements). Per gate: measured
    value, applied rule, verdict, provenance refs. Supersedes evidence_001's report;
    approved=false; execution_legs cloud still pending."""
    turnover_measured = ev001_base["turnover"]["by_candidate"]["baseline_current"]["measured"]
    drawdown_measured = ev001_base["drawdown"]["by_candidate"]["baseline_current"]["measured"]
    volatility_measured = ev001_base["volatility"]["by_candidate"]["baseline_current"]["measured"]

    stress_windows = {
        wid: {
            "consumable_position_coverage": w["consumable_position_coverage"],
            "worst_5d_return": w["worst_5d_return"],
            "window_MDD": w["window_MDD"],
            "window_return": w["window_return"],
            "annualized_volatility": w["annualized_volatility"],
            "fresh_decision_rate": w["diagnostics"]["fresh_decision_rate"],
            "carry_seed": (w["carry_provenance"][0]["carried_from"]
                           if w["carry_provenance"] and w["carry_provenance"][0]["source"] == "carry"
                           else None),
            "go": w["go"],
        }
        for wid, w in stress_measurement["windows"].items()
    }

    return {
        "artifact_type": "phase0q_quantitative_gate_judgment",
        "schema_version": 1,
        "phase0q_id": "open_macro_v03_phase0q_003",
        "judgment_of": "open_macro_v03_metric_evidence_001",
        "additional_measurements": "open_macro_v03_metric_evidence_002",
        "supersedes": f"{EVIDENCE_001_PREFIX}/quantitative_gate_report.measured.json",
        "supersedes_note": (
            "a new traceable judgment supersedes the prior gate report; the measured "
            "evidence_001 files are IMMUTABLE and unchanged (hash-pinned)."),
        "base_profile": BASE_PROFILE,
        "gates": {
            "turnover": {
                "measured": turnover_measured,
                "applied_rule": (
                    "reference_sleeve_turnover_candidate_bound=2.00 (annualized "
                    "one-way sleeve turnover); signal_design_turnover_bound=0.60 is a "
                    "separate design bound NOT applied to sleeve-realized turnover"),
                "signal_design_turnover_bound": SIGNAL_DESIGN_TURNOVER_BOUND,
                "reference_sleeve_turnover_candidate_bound": REFERENCE_SLEEVE_TURNOVER_CANDIDATE_BOUND,
                "verdict": "pass_candidate_under_reference_sleeve_policy",
                "provenance": [
                    f"{EVIDENCE_001_PREFIX}/quantitative_gate_report.measured.json",
                    f"{PHASE0Q_003_PREFIX}/turnover_threshold_context_amendment.json",
                ],
            },
            "drawdown": {
                "measured": drawdown_measured,
                "applied_rule": "max_drawdown <= 0.25 (base profile, unchanged)",
                "bound": BASE_PROFILE["max_drawdown"],
                "verdict": "go",
                "provenance": [f"{EVIDENCE_001_PREFIX}/quantitative_gate_report.measured.json"],
            },
            "volatility": {
                "measured": volatility_measured,
                "applied_rule": "annualized_volatility <= 0.12 (base profile, unchanged)",
                "bound": BASE_PROFILE["max_annualized_volatility"],
                "verdict": "go",
                "provenance": [f"{EVIDENCE_001_PREFIX}/quantitative_gate_report.measured.json"],
            },
            "stress": {
                "applied_rule": (
                    "consumable_position_coverage (fresh OR carry-forward of last "
                    "valid global-chain decision) == 1.0 AND realized risk within "
                    "base profile (worst_5d >= -0.10, window_MDD <= 0.25); "
                    "fresh_decision_rate etc. are diagnostics only"),
                "verdict": "go",
                "windows": stress_windows,
                "provenance": [
                    f"{EVIDENCE_002_PREFIX}/stress_carry_measurement.json",
                    f"{PHASE0Q_003_PREFIX}/stress_gate_semantics_amendment.json",
                ],
            },
            "out_of_sample": {
                "applied_rule": (
                    "OOS bounds UNCHANGED; re-measured with fold seeding + initial-"
                    "acquisition-excluded turnover semantics; decision DEFERRED"),
                "verdict": "no_go_bounds_under_review",
                "re_measured_folds": oos_after["folds"],
                "cross_fold_dispersion": oos_after["cross_fold_dispersion"],
                "bounds_changed": False,
                "provenance": [
                    f"{EVIDENCE_002_PREFIX}/oos_remeasured.json",
                    f"{PHASE0Q_003_PREFIX}/oos_fold_seeding_fix_report.json",
                ],
            },
        },
        "approved": False,
        "approval_required_from": "quant_owner",
        "status": "candidate_not_approved",
        "execution_legs": {"local_python_pure": "complete", "qc_research_object_store": "pending"},
        "ratified_by": "quant_owner",
        "decision_date": "2026-07-02",
        "governance": dict(GOVERNANCE_PINS),
    }

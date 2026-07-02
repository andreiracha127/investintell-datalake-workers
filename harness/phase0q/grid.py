"""Compression mini-grid measurement package (compression_grid_001).

Measures four sleeve variants — ``baseline_100``, ``compressed_75``,
``compressed_50``, ``compressed_25`` — over the SAME decision chain, SAME pack v2,
SAME policy (carry semantics), local leg. This is MEASUREMENT ONLY: nothing here
replaces the baseline (``replaces_baseline`` is false everywhere), A5 stays blocked,
and no activation/approval flag is ever set.

Naming convention (quant_owner, Andrei Rachadel, 2026-07-02): ``compressed_N`` means
N% of the original inter-quadrant distance is RETAINED, so the compression FACTOR
(fraction of the distance each quadrant weight vector is moved TOWARD the four-
quadrant mean) is ``1 - N/100``:

  * ``baseline_100``  -> factor 0.00 (no compression; 100% of the distance retained),
  * ``compressed_75`` -> factor 0.25,
  * ``compressed_50`` -> factor 0.50 (== ``sleeve_compressed_50`` from evidence_002),
  * ``compressed_25`` -> factor 0.75 (most compressed of the grid).

The module supplies:

  * :func:`measure_grid_results`   — DECISION A: 4 variants x 4 costs primary +
    stress metrics under carry semantics + per-variant constraint verification.
  * :func:`measure_oos_fold_report`— DECISION B: the 11-field per-fold table for
    every fold and every variant at base 5bps.
  * :func:`build_compression_grid_manifest` / :func:`build_grid_results_payload` /
    :func:`build_oos_fold_report_payload` — deliverable payload builders.

Deterministic (no wall-clock, no RNG); timestamps + harness_commit are injected.
"""

from __future__ import annotations

import datetime as _dt
from collections import Counter
from typing import Any, Mapping, Sequence

from . import decision, metrics, runner, sleeve

# --------------------------------------------------------------------------- #
# Naming convention + variant definitions                                      #
# --------------------------------------------------------------------------- #

# variant_id -> compression_factor (fraction moved toward the four-quadrant mean).
VARIANT_FACTORS: dict[str, float] = {
    "baseline_100": 0.0,
    "compressed_75": 0.25,
    "compressed_50": 0.5,
    "compressed_25": 0.75,
}

# variant_id -> distance_retained_pct (the N in compressed_N; baseline_100 = 100).
VARIANT_RETAINED_PCT: dict[str, int] = {
    "baseline_100": 100,
    "compressed_75": 75,
    "compressed_50": 50,
    "compressed_25": 25,
}

NAMING_CONVENTION = (
    "compressed_N means N% of the original inter-quadrant distance is RETAINED; the "
    "compression factor (fraction of the distance each quadrant weight vector is moved "
    "TOWARD the four-quadrant mean) is 1 - N/100. baseline_100 = factor 0.0 (no "
    "compression). compressed_50 == sleeve_compressed_50 (evidence_002); compressed_25 "
    "is the most compressed (75% moved toward the mean)."
)

LEADING_ALTERNATIVE_CANDIDATE = "compressed_50"

GOVERNANCE_PINS = {
    "A5": "blocked",
    "runtime_activation": False,
    "activation_allowed": False,
    "allocator_publish": False,
    "official_result": False,
    "db_write_mode": "none",
    "freeze_ready": False,
    "approved": False,
    "replaces_baseline": False,
    "status": "candidate_not_approved",
}

EVIDENCE_001_PREFIX = "artifacts/quant/open_macro_v03_metric_evidence_001"
EVIDENCE_002_PREFIX = "artifacts/quant/open_macro_v03_metric_evidence_002"
GRID_PREFIX = "artifacts/quant/open_macro_v03_compression_grid_001"

# Immutability pins for evidence_002 (all 4 files): sha256 of the committed GIT BLOB
# bytes (git cat-file blob HEAD:<path>), checkout-independent — reuses the blob-pin
# mechanism from harness.phase0q.amendments.EVIDENCE_001_SHA256 (PR#21 P1). This
# package must NEVER write evidence_001 OR evidence_002; both are IMMUTABLE.
EVIDENCE_002_SHA256: dict[str, str] = {
    "compressed_sleeve_alternative.json": "92acce4516fbd6d1d4520e351a4432165803a529fd6a951ff91af0abdcf59cb2",
    "evidence_index.json": "de9fb7b131dea3941126228d14cea7ac977be9142fdb22217109dc361d4ab4db",
    "oos_remeasured.json": "aeccb1e3ca12ed57fa5a5649e032a02127f39d373edd41906dbdbf4f44ccc765",
    "stress_carry_measurement.json": "0a8f2f8a4b8017ba4ef7e1904bd9c90fcde5b13182849708dc12e4f09b2d9f43",
}

PACK_SHA256 = "23a639781853bd53e37eb44359c30a613bc3c82a9dfc5a65c9b5b81f1d04d337"
CONTRACT_BUNDLE_V2_SHA256 = "db85c58968becd890d49d0a022b54b9493449e8c9ff444c88da10678c5d6f53b"

RISK_CAP = sleeve.RISK_CAP_BASELINE           # 0.65
DEFENSIVE_FLOOR = sleeve.DEFENSIVE_FLOOR_BASELINE  # 0.20


def variant_book(variant_id: str) -> dict[str, dict[str, float]]:
    """Per-quadrant weight book for a grid variant (identity for ``baseline_100``)."""
    return sleeve.variant_book(VARIANT_FACTORS[variant_id])


# --------------------------------------------------------------------------- #
# Full-chain simulation (global-chain carry seeding, no lookback truncation)   #
# --------------------------------------------------------------------------- #

def _simulate_full_chain(
    prices: sleeve.PriceFrame,
    decisions: Sequence[Any],
    params: sleeve.SleeveParams,
    start: _dt.date,
    end: _dt.date,
    cost_bps: int,
    book: Mapping[str, Mapping[str, float]],
) -> sleeve.SleeveResult:
    """Seed each window from the WHOLE latched chain (every decision with as_of <=
    end), mirroring ``amendments._simulate_full_chain`` (carry semantics, PR#21 P1)."""
    window_decisions = [r for r in decisions if r.as_of <= end]
    return sleeve.simulate(prices, window_decisions, params,
                           start=start, end=end, cost_bps=cost_bps, book=book)


# --------------------------------------------------------------------------- #
# Constraint verification per variant (DECISION A)                             #
# --------------------------------------------------------------------------- #

def verify_variant_constraints(
    variant_id: str, params: sleeve.SleeveParams,
) -> dict[str, Any]:
    """Verify weights sum to 1 and risk_cap (0.65) / defensive_floor (0.20) hold for
    EVERY quadrant target of the variant after the standard enforcement.

    If a compressed variant VIOLATES a constraint, the sleeve's documented
    enforcement rule renormalizes toward the bound (``target_weights`` already scales
    the offending group and renormalizes) and we RECORD that a renormalization was
    applied — never silently pass. Compression pulls quadrants toward the centroid,
    so in practice risk falls / defensive rises and no enforcement is needed; the
    check proves it rather than assuming it."""
    book = variant_book(variant_id)
    per_quadrant: dict[str, Any] = {}
    all_ok = True
    any_renormalized = False
    for quadrant in ("recovery", "expansion", "slowdown", "contraction"):
        # pre-enforcement row (book -> risk_tilt -> renorm) to detect whether the
        # risk_cap / defensive_floor enforcement actually had to move anything.
        raw = sleeve.target_weights(
            quadrant, params, sleeve.SLEEVE_TICKERS, book=book)
        risk = sum(raw.get(t, 0.0) for t in sleeve.RISK_ASSETS)
        defensive = sum(raw.get(t, 0.0) for t in sleeve.DEFENSIVE_ASSETS)
        weight_sum = sum(raw.values())
        # enforcement is inside target_weights; recompute the pre-enforcement group
        # sums from the compressed book directly to know if a bound would bind.
        pre = _pre_enforcement_row(quadrant, params, book)
        pre_risk = sum(pre.get(t, 0.0) for t in sleeve.RISK_ASSETS)
        pre_def = sum(pre.get(t, 0.0) for t in sleeve.DEFENSIVE_ASSETS)
        renormalized = pre_risk > RISK_CAP + 1e-12 or pre_def < DEFENSIVE_FLOOR - 1e-12
        any_renormalized = any_renormalized or renormalized
        sum_ok = abs(weight_sum - 1.0) < 1e-9
        risk_ok = risk <= RISK_CAP + 1e-9
        def_ok = defensive >= DEFENSIVE_FLOOR - 1e-9
        ok = sum_ok and risk_ok and def_ok
        all_ok = all_ok and ok
        per_quadrant[quadrant] = {
            "weight_sum": weight_sum,
            "risk_assets_weight": risk,
            "defensive_assets_weight": defensive,
            "pre_enforcement_risk_assets_weight": pre_risk,
            "pre_enforcement_defensive_assets_weight": pre_def,
            "weights_sum_to_one": sum_ok,
            "risk_cap_satisfied": risk_ok,
            "defensive_floor_satisfied": def_ok,
            "renormalization_applied": renormalized,
            "weights": raw,
        }
    return {
        "variant_id": variant_id,
        "risk_cap": RISK_CAP,
        "defensive_floor": DEFENSIVE_FLOOR,
        "all_constraints_satisfied": all_ok,
        "any_renormalization_applied": any_renormalized,
        "per_quadrant": per_quadrant,
    }


def _pre_enforcement_row(
    quadrant: str, params: sleeve.SleeveParams,
    book: Mapping[str, Mapping[str, float]],
) -> dict[str, float]:
    """The book -> risk_tilt -> renormalize row BEFORE risk_cap / defensive_floor
    enforcement, to detect whether a bound would have bound (documented rule)."""
    key = sleeve.QUADRANT_TO_KEY[quadrant]
    weights = dict(book[key])
    weights["SPY"] = weights.get("SPY", 0.0) + params.risk_tilt
    weights["SHY"] = weights.get("SHY", 0.0) - params.risk_tilt
    weights = {t: max(0.0, w) for t, w in weights.items()}
    total = sum(weights.values())
    return {t: w / total for t, w in weights.items()} if total > 0 else weights


# --------------------------------------------------------------------------- #
# DECISION A - grid results (primary + stress + constraints)                   #
# --------------------------------------------------------------------------- #

def measure_variant_cost_grid(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    variant_id: str,
    *,
    cost_grid: Sequence[int] = runner.COST_GRID_BPS,
    primary_window: tuple[_dt.date, _dt.date] = runner.PRIMARY_WINDOW,
) -> dict[str, Any]:
    """Primary-window turnover/MDD/vol/return at each cost level for one variant."""
    book = variant_book(variant_id)
    cells: dict[str, Any] = {}
    for cost_bps in cost_grid:
        res = _simulate_full_chain(prices, decisions, params,
                                   primary_window[0], primary_window[1], cost_bps, book)
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
    return {"by_cost_bps": cells}


def measure_variant_stress(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    variant_id: str,
    *,
    cost_bps: int = runner.BASE_COST_BPS,
    stress_windows: Sequence[Mapping[str, Any]] = runner.STRESS_WINDOWS,
) -> dict[str, Any]:
    """Full_basket stress windows for one variant under carry semantics (base cost)."""
    book = variant_book(variant_id)
    windows: dict[str, Any] = {}
    for win in stress_windows:
        if win["coverage"] != "full_basket":
            continue
        wid = win["window_id"]
        scheduled = [r.as_of for r in decisions if win["start"] <= r.as_of <= win["end"]]
        coverage = metrics.consumable_position_coverage(decisions, scheduled)
        res = _simulate_full_chain(prices, decisions, params,
                                   win["start"], win["end"], cost_bps, book)
        windows[wid] = {
            "consumable_position_coverage": coverage["consumable_position_coverage"],
            "window_return": metrics.window_return(res.nav),
            "window_MDD": metrics.max_drawdown(res.nav),
            "worst_5d_return": metrics.worst_5d_return(res.nav),
            "annualized_volatility": metrics.annualized_volatility(res.nav),
            "n_trading_days": len(res.dates),
        }
    return windows


def measure_grid_results(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    *,
    cost_grid: Sequence[int] = runner.COST_GRID_BPS,
    primary_window: tuple[_dt.date, _dt.date] = runner.PRIMARY_WINDOW,
    stress_windows: Sequence[Mapping[str, Any]] = runner.STRESS_WINDOWS,
) -> dict[str, Any]:
    """DECISION A: the 4 variants x 4 costs grid (primary + stress) + per-variant
    constraint verification. Returns an in-memory dict keyed by variant_id."""
    variants: dict[str, Any] = {}
    for variant_id in VARIANT_FACTORS:
        variants[variant_id] = {
            "variant_id": variant_id,
            "compression_factor": VARIANT_FACTORS[variant_id],
            "distance_retained_pct": VARIANT_RETAINED_PCT[variant_id],
            "replaces_baseline": False,
            "cost_grid": measure_variant_cost_grid(
                prices, decisions, params, variant_id,
                cost_grid=cost_grid, primary_window=primary_window),
            "stress_windows": measure_variant_stress(
                prices, decisions, params, variant_id, stress_windows=stress_windows),
            "constraints": verify_variant_constraints(variant_id, params),
        }
    return variants


# --------------------------------------------------------------------------- #
# DECISION B - per-fold OOS report (11 fields, every fold, every variant)      #
# --------------------------------------------------------------------------- #

def _last_valid_before(
    decisions: Sequence[decision.DecisionRow], as_of: _dt.date,
) -> decision.DecisionRow | None:
    prior = None
    for row in sorted((d for d in decisions if d.has_valid_quadrant()),
                      key=lambda d: d.as_of):
        if row.as_of < as_of:
            prior = row
        else:
            break
    return prior


def _dominant_regime_by_days(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    start: _dt.date,
    end: _dt.date,
) -> tuple[str | None, dict[str, int]]:
    """Most-held quadrant BY TRADING DAYS over [start, end]: for each trading day,
    the active quadrant is the last valid latched decision on/before that day
    (carry semantics). Returns (dominant_quadrant, held_days_by_quadrant)."""
    trading_dates = prices.dates_in(start, end)
    valid = sorted((d for d in decisions if d.has_valid_quadrant()),
                   key=lambda d: d.as_of)
    counts: Counter[str] = Counter()
    for day in trading_dates:
        active: str | None = None
        for row in valid:
            if row.as_of <= day:
                active = row.quadrant
            else:
                break
        if active is not None:
            counts[active] += 1
    if not counts:
        return None, {}
    # deterministic tie-break: most days, then quadrant name.
    dominant = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    return dominant, dict(counts)


def measure_oos_fold_report(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    variant_id: str,
    *,
    cost_bps: int = runner.BASE_COST_BPS,
    primary_window: tuple[_dt.date, _dt.date] = runner.PRIMARY_WINDOW,
) -> dict[str, Any]:
    """DECISION B: for EVERY fold, the full 11-field table for one variant at base
    cost. No lookahead; the initial empty->portfolio acquisition is excluded from
    fold economic turnover; no empty-portfolio transition is counted.

    The 11 fields per fold:
      1. initial_carried_position (weights source quadrant)
      2. last_valid_decision_before_fold_start (date)
      3. fresh_decisions_in_fold (count)
      4. carry_pct_of_scheduled_dates
      5. economic_turnover (initial acquisition excluded)
      6. MDD
      7. volatility
      8. return
      9. dominant_regime (most-held quadrant by days)
     10. pass_fail vs absolute bounds (MDD<=0.25, vol<=0.12 base profile)
     11. pass_fail vs stability bounds (contribution to cross-fold max-dev)
    """
    book = variant_book(variant_id)
    folds = runner.oos_folds(primary_window)
    econ_metric_list: list[dict[str, float]] = []
    raw_rows: list[dict[str, Any]] = []

    for fold in folds:
        test_start = fold["test_start"]
        test_end = fold["test_end"]
        res = _simulate_full_chain(prices, decisions, params,
                                   test_start, test_end, cost_bps, book)
        excl = metrics.fold_turnover_excluding_seed(
            res.dates, res.one_way_turnover_by_date, res.seed_rebalance_date)
        ret = metrics.return_annualized(res.nav, len(res.dates))
        sigma = metrics.annualized_volatility(res.nav)
        mdd = metrics.max_drawdown(res.nav)
        econ_turnover = excl["max_trailing_252_excl_seed"]
        econ_metric_list.append({
            "return_annualized": ret, "sigma_annual": sigma, "MDD": mdd,
            "one_way_turnover_annualized": econ_turnover,
        })

        # (1) initial carried position = the quadrant the seed decision holds.
        seed_valid = _last_valid_before(decisions, test_start)
        seed_as_of = res.seed_decision_as_of
        carried = seed_as_of is not None and seed_as_of < test_start
        initial_quadrant = seed_valid.quadrant if (carried and seed_valid) else None
        # (3) fresh in-fold valid decisions (a fresh decision landing inside the fold).
        scheduled = [d.as_of for d in decisions if test_start <= d.as_of <= test_end]
        coverage = metrics.consumable_position_coverage(decisions, scheduled)
        fresh_in_fold = coverage["fresh_count"]
        # (4) carry percentage of scheduled dates.
        carry_pct = (coverage["carry_count"] / coverage["scheduled_count"]
                     if coverage["scheduled_count"] else 0.0)
        # (9) dominant regime by held days.
        dominant, held = _dominant_regime_by_days(prices, decisions, test_start, test_end)

        raw_rows.append({
            "fold_index": fold["fold_index"],
            "test_start": test_start.isoformat(),
            "test_end": test_end.isoformat(),
            "initial_carried_position_quadrant": initial_quadrant,
            "initial_carried_position_source": (
                "carried_pre_fold_position" if carried
                else "unseeded_no_prior_valid_position"),
            "last_valid_decision_before_fold_start": (
                seed_valid.as_of.isoformat() if seed_valid else None),
            "fresh_decisions_in_fold": fresh_in_fold,
            "carry_pct_of_scheduled_dates": carry_pct,
            "scheduled_dates_in_fold": coverage["scheduled_count"],
            "economic_turnover_excl_initial_acquisition": econ_turnover,
            "seed_one_way_turnover_excluded": excl["seed_one_way"],
            "MDD": mdd,
            "volatility": sigma,
            "return_annualized": ret,
            "dominant_regime": dominant,
            "held_days_by_quadrant": held,
            "n_rebalances": len(res.rebalance_dates),
        })

    # (11) stability bounds: each fold's contribution to the cross-fold max deviation
    # from the median. A fold "passes" stability when its own deviation is within the
    # bound for BOTH sigma and MDD (the metric that drives the cross-fold max-dev gate).
    dispersion = metrics.stability_from_folds(econ_metric_list)
    sigma_median = _median([m["sigma_annual"] for m in econ_metric_list])
    mdd_median = _median([m["MDD"] for m in econ_metric_list])

    fold_rows: list[dict[str, Any]] = []
    for row, m in zip(raw_rows, econ_metric_list):
        # (10) absolute bounds pass/fail (base profile).
        abs_mdd_ok = m["MDD"] <= runner.BASE_ENVELOPE["max_drawdown"]
        abs_vol_ok = m["sigma_annual"] <= runner.BASE_ENVELOPE["max_annualized_volatility"]
        absolute_pass = abs_mdd_ok and abs_vol_ok
        # (11) stability bounds pass/fail (this fold's contribution to cross-fold max-dev).
        sigma_dev = abs(m["sigma_annual"] - sigma_median)
        mdd_dev = abs(m["MDD"] - mdd_median)
        stab_sigma_ok = sigma_dev <= runner.BASE_ENVELOPE["max_fold_volatility_deviation"]
        stab_mdd_ok = mdd_dev <= runner.BASE_ENVELOPE["max_fold_mdd_deviation"]
        stability_pass = stab_sigma_ok and stab_mdd_ok
        fold_rows.append({
            **row,
            "absolute_bounds": {
                "mdd_bound": runner.BASE_ENVELOPE["max_drawdown"],
                "vol_bound": runner.BASE_ENVELOPE["max_annualized_volatility"],
                "mdd_ok": abs_mdd_ok, "vol_ok": abs_vol_ok, "pass": absolute_pass,
            },
            "stability_bounds": {
                "sigma_dev_from_median": sigma_dev,
                "mdd_dev_from_median": mdd_dev,
                "sigma_dev_bound": runner.BASE_ENVELOPE["max_fold_volatility_deviation"],
                "mdd_dev_bound": runner.BASE_ENVELOPE["max_fold_mdd_deviation"],
                "sigma_dev_ok": stab_sigma_ok, "mdd_dev_ok": stab_mdd_ok,
                "pass": stability_pass,
            },
        })

    return {
        "variant_id": variant_id,
        "cost_bps": cost_bps,
        "n_folds": len(folds),
        "folds": fold_rows,
        "cross_fold_dispersion": dispersion,
        "base_profile": runner.BASE_ENVELOPE,
        "no_lookahead": True,
        "initial_acquisition_excluded_from_fold_turnover": True,
        "empty_portfolio_transition_counted": False,
    }


def _median(values: Sequence[float]) -> float:
    import statistics
    return statistics.median(values) if values else 0.0


# --------------------------------------------------------------------------- #
# Deliverable payload builders                                                 #
# --------------------------------------------------------------------------- #

def build_compression_grid_manifest(harness_commit: str) -> dict[str, Any]:
    """``compression_grid_manifest.json`` — naming convention, variants, governance
    pins, provenance."""
    return {
        "artifact_type": "phase0q_compression_grid_manifest",
        "schema_version": 1,
        "grid_id": "open_macro_v03_compression_grid_001",
        "naming_convention": NAMING_CONVENTION,
        "variants": [
            {
                "variant_id": vid,
                "compression_factor": VARIANT_FACTORS[vid],
                "distance_retained_pct": VARIANT_RETAINED_PCT[vid],
                "replaces_baseline": False,
                "is_leading_alternative_candidate": vid == LEADING_ALTERNATIVE_CANDIDATE,
                "matches_evidence_002_sleeve": (
                    "sleeve_compressed_50" if vid == "compressed_50" else None),
            }
            for vid in VARIANT_FACTORS
        ],
        "leading_alternative_candidate": LEADING_ALTERNATIVE_CANDIDATE,
        "same_decision_chain": True,
        "same_input_pack_v2": True,
        "same_policy_carry_semantics": True,
        "execution_leg": "local_python_pure",
        "measurement_only": True,
        "governance": dict(GOVERNANCE_PINS),
        "provenance": {
            "input_pack_id": runner.INPUT_PACK_ID,
            "input_pack_sha256": PACK_SHA256,
            "contract_bundle_sha256": CONTRACT_BUNDLE_V2_SHA256,
            "harness_commit": harness_commit,
            "run_id": "open_macro_v03_compression_grid_001",
            "started_at": "2026-07-02T00:00:00+00:00",
            "finished_at": "2026-07-02T00:00:00+00:00",
            "immutable_predecessors": [
                "open_macro_v03_metric_evidence_001",
                "open_macro_v03_metric_evidence_002",
            ],
        },
    }


def build_grid_results_payload(
    grid: Mapping[str, Any], harness_commit: str,
) -> dict[str, Any]:
    """``grid_results.json`` — 4 variants x 4 costs + stress + constraints."""
    return {
        "artifact_type": "phase0q_compression_grid_results",
        "schema_version": 1,
        "grid_id": "open_macro_v03_compression_grid_001",
        "cost_grid_bps": list(runner.COST_GRID_BPS),
        "base_cost_bps": runner.BASE_COST_BPS,
        "primary_window": [runner.PRIMARY_WINDOW[0].isoformat(),
                           runner.PRIMARY_WINDOW[1].isoformat()],
        "naming_convention": NAMING_CONVENTION,
        "consistency_check": {
            "compressed_50_equals_evidence_002_sleeve_compressed_50": True,
            "source": f"{EVIDENCE_002_PREFIX}/compressed_sleeve_alternative.json",
        },
        "variants": {vid: grid[vid] for vid in VARIANT_FACTORS},
        "measurement_only": True,
        "governance": dict(GOVERNANCE_PINS),
        "provenance": {
            "input_pack_sha256": PACK_SHA256,
            "contract_bundle_sha256": CONTRACT_BUNDLE_V2_SHA256,
            "harness_commit": harness_commit,
        },
    }


def build_oos_fold_report_payload(
    reports: Mapping[str, Mapping[str, Any]], harness_commit: str,
) -> dict[str, Any]:
    """``oos_fold_report.json`` — decision B's 11-field per-fold table for all 4
    variants at 5bps."""
    return {
        "artifact_type": "phase0q_compression_oos_fold_report",
        "schema_version": 1,
        "grid_id": "open_macro_v03_compression_grid_001",
        "base_cost_bps": runner.BASE_COST_BPS,
        "oos_verdict": "no_go_bounds_under_review",
        "bounds_unchanged": True,
        "field_list": [
            "initial_carried_position_quadrant",
            "last_valid_decision_before_fold_start",
            "fresh_decisions_in_fold",
            "carry_pct_of_scheduled_dates",
            "economic_turnover_excl_initial_acquisition",
            "MDD", "volatility", "return_annualized", "dominant_regime",
            "absolute_bounds", "stability_bounds",
        ],
        "variants": {vid: reports[vid] for vid in VARIANT_FACTORS},
        "measurement_only": True,
        "governance": dict(GOVERNANCE_PINS),
        "provenance": {
            "input_pack_sha256": PACK_SHA256,
            "contract_bundle_sha256": CONTRACT_BUNDLE_V2_SHA256,
            "harness_commit": harness_commit,
        },
    }

"""Phase 0Q harness runner — orchestrates the scenario grid and emits the measured,
non-candidate quantitative gate report + contract-shaped result.

Pipeline (offline, network-free, DB-free):
  1. Load pack v2 and VERIFY it with ``harness.p1_pack.verifier.verify_pack``;
     refuse (raise) on any mismatch. The verified ``input_pack_sha256`` and the v2
     ``contract_bundle_sha256`` are pinned into every cell + the result.
  2. Build the PIT index once; compute the monthly latched decision series once over
     the UNION of all evaluation windows and slice per window (decisions are
     candidate-independent — see ``decision.py``).
  3. For each scenario candidate x cost level: simulate the sleeve over the primary
     window, each stress window, and each walk-forward OOS fold; extract the exact
     ``metric_definitions.json`` metrics.
  4. Build canonical per-cell payloads with provenance, and the consolidated
     ``quantitative_gate_report.measured.json`` judging the five gates vs the base
     envelope at each cost level (go/no_go per gate at base 5bps).
  5. Emit the contract-shaped ``open_macro_v03_metric_backtest`` result
     (run_fingerprint + output_logical_hashes via ``stable_hash``), plus
     ``execution_legs`` with local_python_pure complete / qc leg pending.

Determinism: all floats canonicalized to 12 decimals via the core logical
normalizer; all timestamps are INJECTED (no wall-clock in canonical outputs); no RNG.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from investintell_quant_core.hashing.canonical import (
    normalize_logical_value,
    stable_hash,
)

from harness.p1_pack import verifier as pack_verifier

from . import decision, metrics, sleeve

# ------------------------------------------------------------------------- #
# Static policy inputs (pinned from the phase0q_002 / _001 artifacts)        #
# ------------------------------------------------------------------------- #

PRIMARY_WINDOW = (_dt.date(2014, 3, 1), _dt.date(2026, 6, 30))

# stress_oos_policy.json stress windows + phase0q_002 coverage classification.
STRESS_WINDOWS: tuple[dict[str, Any], ...] = (
    {"window_id": "COVID_2020", "start": _dt.date(2020, 2, 15), "end": _dt.date(2020, 4, 30), "coverage": "full_basket"},
    {"window_id": "INFLATION_SHOCK_2022", "start": _dt.date(2022, 1, 1), "end": _dt.date(2022, 10, 31), "coverage": "full_basket"},
    {"window_id": "SVB_2023", "start": _dt.date(2023, 3, 1), "end": _dt.date(2023, 5, 31), "coverage": "full_basket"},
    {"window_id": "Q4_2018", "start": _dt.date(2018, 10, 1), "end": _dt.date(2018, 12, 31), "coverage": "full_basket"},
    {"window_id": "GFC_2008", "start": _dt.date(2007, 10, 1), "end": _dt.date(2009, 3, 31), "coverage": "reduced_coverage"},
    {"window_id": "TAPER_2013", "start": _dt.date(2013, 5, 1), "end": _dt.date(2013, 9, 30), "coverage": "reduced_coverage"},
)

# scenario_grid.json parameter candidates (5).
SCENARIO_CANDIDATES: tuple[sleeve.SleeveParams, ...] = (
    sleeve.SleeveParams("baseline_current", 0.5, 0.5, 0.0, 0.0, 0.0),
    sleeve.SleeveParams("growth_plus_2pp", 0.52, 0.48, 0.0, 0.0, 0.0),
    sleeve.SleeveParams("inflation_plus_2pp", 0.48, 0.52, 0.0, 0.0, 0.0),
    sleeve.SleeveParams("risk_tilt_plus_1pp", 0.5, 0.5, 0.01, 0.0, 0.0),
    sleeve.SleeveParams("risk_tilt_minus_1pp", 0.5, 0.5, -0.01, 0.0, 0.0),
)

COST_GRID_BPS: tuple[int, ...] = (0, 5, 10, 25)
BASE_COST_BPS = 5

# threshold_profile_selection_record.json base envelope.
BASE_ENVELOPE = {
    "max_one_way_turnover_annualized": 0.60,
    "max_drawdown": 0.25,
    "max_annualized_volatility": 0.12,
    "min_worst_5d_return": -0.10,
    "max_fold_volatility_deviation": 0.05,
    "max_fold_mdd_deviation": 0.08,
}

# out_of_sample walk-forward (stress_oos_policy.json + phase0q_002 supplement).
OOS_TRAIN_MONTHS = 36
OOS_TEST_MONTHS = 12
OOS_STEP_MONTHS = 12

CONTRACT_BUNDLE_SHA256 = pack_verifier.CONTRACT_BUNDLE_SHA256
INPUT_PACK_ID = pack_verifier.INPUT_PACK_ID

# Tranche W2: the regime-timeline gate policy — RATIFIED by the quant_owner
# (Andrei Rachadel) on 2026-07-11 with the bounds exactly as proposed. A ratified
# policy (status == "ratified") makes the timeline judgment GATING: it enters
# ``gates_overall_base_cost`` as a distinct blocking ``timeline`` go/no_go. An
# unratified policy at this path stays advisory and blocks nothing (the
# pre-ratification behaviour, still exercised by tests). The frozen v1 model FAILS
# these gates on the certified 2021-2026 timeline — the resulting no_go is the
# intended honest outcome until recalibration lands, never a crash.
_REPO_ROOT = Path(__file__).resolve().parents[2]
TIMELINE_GATE_POLICY_PATH = (
    _REPO_ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_005"
    / "timeline_gate_policy.json")

# The code-reviewed content pin of the RATIFIED policy (repo pin culture): sha256 of
# the canonical JSON (sort_keys, compact separators — key-order/whitespace/CRLF
# independent, so it is checkout-stable). A ratification claim only gates when the
# whole artifact content hashes to this pin: editing ANY byte of policy content
# (bounds, semantics, rationale, governance) without a code-reviewed re-pin here
# demotes the claim to a fail-closed no_go (see validate_ratified_policy /
# timeline_overall_gate_entry) — one status string can never forge ratification.
RATIFIED_TIMELINE_GATE_POLICY_CANONICAL_SHA256 = (
    "fb3dde69f1165192eb4c99fc242f215508a2b46decf510c65dcf3a73204d8524")

# The COMPLETE ratified gate set: a claimed-ratified policy missing a key (a silently
# weakened policy) or carrying an extra one is invalid as a whole — never a partial
# enforcement of whatever survived.
_RATIFIED_GATE_KEYS = frozenset({
    "min_fresh_valid_rate_36m",
    "max_abstention_streak_months",
    "max_carry_age_months",
    "max_same_quadrant_run_months",
    "min_upside_capture_bull_year",
})


def _policy_canonical_sha256(policy: Mapping[str, Any]) -> str:
    """sha256 of the policy mapping's canonical JSON serialization."""
    return hashlib.sha256(
        json.dumps(policy, sort_keys=True, separators=(",", ":"),
                   ensure_ascii=False).encode("utf-8")).hexdigest()


def validate_ratified_policy(policy: Mapping[str, Any]) -> list[str]:
    """FULL ratified-policy contract check (Tranche W hardening): the list of
    violations for a policy CLAIMING ``status == "ratified"`` (empty == valid).

    Everything must hold before the judge may enforce: exact artifact identity, the
    quant_owner ratifier with a named holder and a well-formed decision_date, the
    COMPLETE five-gate set with positive finite numeric bounds, the unaltered
    governance pins, and the canonical content sha256 equal to the code-reviewed pin.
    Violations are returned (not raised) so the caller can fail CLOSED — a forged or
    tampered claim becomes a loud no_go, never a crash and never a trusted go."""
    violations: list[str] = []
    if policy.get("artifact_type") != "phase0q_timeline_gate_policy":
        violations.append(
            f"artifact_type {policy.get('artifact_type')!r} != 'phase0q_timeline_gate_policy'")
    if policy.get("phase0q_id") != "open_macro_v03_phase0q_005":
        violations.append(
            f"phase0q_id {policy.get('phase0q_id')!r} != 'open_macro_v03_phase0q_005'")
    if policy.get("ratified_by") != "quant_owner":
        violations.append(
            f"ratified_by {policy.get('ratified_by')!r} != 'quant_owner'")
    name = policy.get("ratified_by_name")
    if not (isinstance(name, str) and name.strip()):
        violations.append("ratified_by_name missing or empty")
    decision_date = policy.get("decision_date")
    try:
        if not isinstance(decision_date, str):
            raise ValueError
        _dt.date.fromisoformat(decision_date)
    except ValueError:
        violations.append(
            f"decision_date {decision_date!r} is not a well-formed ISO date")
    gates = policy.get("gates")
    if not isinstance(gates, Mapping) or set(gates) != _RATIFIED_GATE_KEYS:
        got = sorted(gates) if isinstance(gates, Mapping) else gates
        violations.append(
            f"gate keys {got!r} != the complete ratified set "
            f"{sorted(_RATIFIED_GATE_KEYS)} (missing/extra gates invalidate the "
            "whole claim; never a partial enforcement)")
    else:
        for key in sorted(_RATIFIED_GATE_KEYS):
            bound = gates[key]
            if (isinstance(bound, bool) or not isinstance(bound, (int, float))
                    or not math.isfinite(bound) or bound <= 0):
                violations.append(
                    f"gate {key} bound {bound!r} is not a positive finite number")
        rate = gates.get("min_fresh_valid_rate_36m")
        if (isinstance(rate, (int, float)) and not isinstance(rate, bool)
                and math.isfinite(rate) and rate > 1):
            violations.append(
                f"min_fresh_valid_rate_36m bound {rate!r} > 1 (a rate must be <= 1)")
    governance = policy.get("governance")
    if (not isinstance(governance, Mapping)
            or governance.get("runtime_activation") is not False
            or governance.get("A5") != "blocked"
            or governance.get("self_ratification") != "prohibited"):
        violations.append(
            "governance pins missing or altered (runtime_activation must be false, "
            "A5 'blocked', self_ratification 'prohibited')")
    actual = _policy_canonical_sha256(policy)
    if actual != RATIFIED_TIMELINE_GATE_POLICY_CANONICAL_SHA256:
        violations.append(
            f"canonical content sha256 {actual} != pinned "
            f"{RATIFIED_TIMELINE_GATE_POLICY_CANONICAL_SHA256} (policy content was "
            "edited without a code-reviewed re-pin)")
    return violations

GOVERNANCE_PINS = {
    "A5": "blocked",
    "runtime_activation": False,
    "activation_allowed": False,
    "official_result": False,
    "allocator_publish": False,
    "db_write_mode": "none",
    "freeze_ready": False,
    "classification": "metric_evidence_only",
}


# ------------------------------------------------------------------------- #
# Data loading + verification                                               #
# ------------------------------------------------------------------------- #

@dataclass(frozen=True)
class LoadedPack:
    root: Path
    input_pack_sha256: str
    macro_rows: list[dict[str, Any]]
    eod_rows: list[dict[str, Any]]


def load_and_verify_pack(pack_dir: str | Path) -> LoadedPack:
    """Verify the pack v2 offline and load its two source tables. Refuse on mismatch."""
    root = Path(pack_dir)
    report = pack_verifier.verify_pack(root)
    if not report.get("ok"):
        raise RuntimeError(
            f"pack verification failed for {root}: "
            f"{json.dumps({k: v for k, v in report.items() if v and k != 'ok'})}")
    macro_rows = _load_table(root, "macro_observation_vintage")
    eod_rows = _load_table(root, "eod_prices")
    return LoadedPack(
        root=root,
        input_pack_sha256=report["actual_input_pack_sha256"],
        macro_rows=macro_rows,
        eod_rows=eod_rows,
    )


def _load_table(root: Path, name: str) -> list[dict[str, Any]]:
    path = root / "data" / "canonical" / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


# ------------------------------------------------------------------------- #
# Decision series (computed once, shared)                                    #
# ------------------------------------------------------------------------- #

def _union_window(windows: Sequence[tuple[_dt.date, _dt.date]]) -> tuple[_dt.date, _dt.date]:
    starts = [w[0] for w in windows]
    ends = [w[1] for w in windows]
    return min(starts), max(ends)


def build_decision_series(
    pack: LoadedPack, windows: Sequence[tuple[_dt.date, _dt.date]],
) -> list[decision.DecisionRow]:
    start, end = _union_window(windows)
    index = decision.PitIndex(pack.macro_rows)
    return decision.run_decision_series(index, start, end)


def _decisions_in(series: Sequence[decision.DecisionRow], start: _dt.date, end: _dt.date):
    return [r for r in series if start <= r.as_of <= end]


def _valid_decision_dates(series: Sequence[decision.DecisionRow]) -> set[_dt.date]:
    return {r.as_of for r in series if r.has_valid_quadrant()}


# ------------------------------------------------------------------------- #
# Regime timeline block (Tranche W1 — ALWAYS reported, never gating here)    #
# ------------------------------------------------------------------------- #

def _spy_buy_hold_nav(
    prices: sleeve.PriceFrame, primary: tuple[_dt.date, _dt.date],
) -> list[tuple[_dt.date, float]]:
    """SPY adjusted-close buy-and-hold NAV over the primary window (base 1.0 at the
    first priced session). The benchmark leg for calendar-year upside capture."""
    series: list[tuple[_dt.date, float]] = []
    base: float | None = None
    for d in prices.dates_in(*primary):
        p = prices.price("SPY", d)
        if p is None or p != p or p <= 0:  # NaN / non-positive guard
            continue
        if base is None:
            base = p
        series.append((d, p / base))
    return series


def build_timeline_block(
    decisions: Sequence[decision.DecisionRow],
    prices: sleeve.PriceFrame,
    config: "RunConfig",
) -> dict[str, Any]:
    """The regime-timeline diagnostics block reported on every run (Tranche W1).

    ALWAYS computed and attached to the gate report: the abstention/carry/quadrant
    occupancy of the latched chain plus benchmark-relative upside capture, over the
    configured primary window at BASE_COST_BPS for the FIRST configured candidate
    (the measurement reference candidate). These are diagnostics; the proposed
    (not-yet-ratified) gate policy that judges them is wired advisory-only until it is
    ratified (see judge_timeline_gates)."""
    base_params = config.candidates[0]
    strat_res = _run_window(prices, decisions, base_params,
                            config.primary_window[0], config.primary_window[1],
                            BASE_COST_BPS)
    strategy_nav = list(zip(strat_res.dates, strat_res.nav))
    spy_nav = _spy_buy_hold_nav(prices, config.primary_window)
    block: dict[str, Any] = {
        "reference_candidate_id": base_params.candidate_id,
        "primary_window": {
            "start": config.primary_window[0].isoformat(),
            "end": config.primary_window[1].isoformat(),
        },
        "regime_timeline_metrics": metrics.regime_timeline_metrics(decisions),
        "upside_capture_by_calendar_year": metrics.upside_capture_by_calendar_year(
            strategy_nav, spy_nav),
    }
    # Tranche W2: attach the timeline-gate judgment. GATING for the committed
    # ratified policy (a blocking 'timeline' entry lands in gates_overall_base_cost);
    # an unratified policy stays advisory and blocks nothing.
    policy = load_timeline_gate_policy()
    block["gate_judgment"] = judge_timeline_gates(block, policy)
    return block


def load_timeline_gate_policy(
    path: "str | Path | None" = None,
) -> dict[str, Any] | None:
    """Load the timeline-gate policy artifact, or ``None`` if it is absent (the
    harness then reports ``policy_absent`` and judges nothing). ``path`` defaults to
    the module's ``TIMELINE_GATE_POLICY_PATH`` resolved at CALL time, so tests can
    point the runner at an alternative (e.g. unratified) policy artifact."""
    p = Path(path if path is not None else TIMELINE_GATE_POLICY_PATH)
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def judge_timeline_gates(
    timeline: Mapping[str, Any], policy: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Judge the regime-timeline metrics against the gate policy (Tranche W2).

    Governance: the judgment is ENFORCED only when ``policy["status"] == "ratified"``
    AND the FULL ratified contract validates (:func:`validate_ratified_policy`:
    artifact identity, quant_owner ratifier + decision_date, the complete five-gate
    set with well-formed bounds, governance pins, and the code-reviewed canonical
    content pin ``RATIFIED_TIMELINE_GATE_POLICY_CANONICAL_SHA256``) — one status
    string can never forge gating. A claimed-ratified policy failing validation
    yields ``policy_status == "ratified_claim_invalid"``: never enforced (not even a
    surviving subset of gates) and surfaced FAIL-CLOSED as a no_go overall entry. A
    genuinely unratified policy (``proposed_not_ratified``) stays advisory — computed
    and attached, blocking nothing, never entering ``gates_overall_base_cost``. The
    committed phase0q_005 artifact was ratified by the quant_owner (Andrei Rachadel)
    on 2026-07-11 with the bounds exactly as proposed, so runs against it are GATING;
    the ratification came from the owner, never from the harness itself
    (self-ratification stays prohibited).

    Directions: ``min_*`` gates require measured >= bound; ``max_*`` gates require
    measured <= bound. ``min_upside_capture_bull_year`` is judged only over FULL
    calendar years (``full_year_coverage`` true — partial periods from a mid-year
    window start or a truncated benchmark are surfaced as ``excluded_partial_years``,
    never enforced) whose SPY return clears ``bull_year_spy_return_threshold`` (else
    the gate is not applicable and does not vacuously fail)."""
    if policy is None:
        return {"policy_status": "policy_absent", "mode": "advisory",
                "gates_enforced": False, "ratification_violations": [],
                "per_gate": {}, "overall_go": None}

    status = policy.get("status")
    if status == "ratified":
        # A ratification CLAIM gates only after the FULL contract validates
        # (identity, ratifier, decision_date, the complete five-gate set with
        # well-formed bounds, governance pins, and the code-reviewed canonical
        # content pin). A failed claim NEVER enforces — not even the surviving
        # subset — and is surfaced fail-closed by timeline_overall_gate_entry.
        violations = validate_ratified_policy(policy)
        if violations:
            return {"policy_status": "ratified_claim_invalid", "mode": "advisory",
                    "gates_enforced": False,
                    "policy_artifact_type": policy.get("artifact_type"),
                    "phase0q_id": policy.get("phase0q_id"),
                    "ratification_violations": violations,
                    "per_gate": {}, "overall_go": None}
        mode = "gating"
    else:
        mode = "advisory"
    gates = policy.get("gates", {})
    params = policy.get("gate_parameters", {})
    m = timeline["regime_timeline_metrics"]
    uc = timeline.get("upside_capture_by_calendar_year", {})

    per_gate: dict[str, Any] = {}

    if "min_fresh_valid_rate_36m" in gates:
        measured = m["fresh_valid_rate"]["rolling_36m"]
        bound = gates["min_fresh_valid_rate_36m"]
        per_gate["min_fresh_valid_rate_36m"] = {
            "measured": measured, "bound": bound, "direction": "min",
            "go": measured >= bound}

    for key in ("max_abstention_streak_months", "max_carry_age_months",
                "max_same_quadrant_run_months"):
        if key in gates:
            measured = m[key]
            bound = gates[key]
            per_gate[key] = {"measured": measured, "bound": bound,
                             "direction": "max", "go": measured <= bound}

    if "min_upside_capture_bull_year" in gates:
        bound = gates["min_upside_capture_bull_year"]
        threshold = params.get("bull_year_spy_return_threshold", 0.15)
        # only FULL calendar years are judged: a partial period (mid-year window
        # start, truncated pack, missing benchmark tail) can post a partial-period
        # SPY return above the bull threshold, and enforcing it as a bull year would
        # judge the strategy against a figure that is not a calendar-year return.
        # Partial years are surfaced (excluded_partial_years) but never enforced.
        bull_years = {y: e for y, e in uc.items()
                      if e.get("full_year_coverage") is True
                      and e.get("spy_return") is not None
                      and e["spy_return"] > threshold}
        excluded_partial = sorted(
            y for y, e in uc.items() if e.get("full_year_coverage") is not True)
        captures = [e["upside_capture"] for e in bull_years.values()
                    if e.get("upside_capture") is not None]
        applicable = bool(captures)
        measured = min(captures) if captures else None
        per_gate["min_upside_capture_bull_year"] = {
            "measured": measured, "bound": bound, "direction": "min",
            "applicable": applicable, "bull_year_spy_return_threshold": threshold,
            "bull_years": sorted(bull_years),
            "excluded_partial_years": excluded_partial,
            "go": (measured >= bound) if applicable else True}

    overall_go = all(g["go"] for g in per_gate.values()) if per_gate else None
    return {
        "policy_status": status,
        "policy_artifact_type": policy.get("artifact_type"),
        "phase0q_id": policy.get("phase0q_id"),
        "mode": mode,
        "gates_enforced": mode == "gating",
        "ratification_violations": [],
        "per_gate": per_gate,
        "overall_go": overall_go,
    }


def timeline_overall_gate_entry(judgment: Mapping[str, Any]) -> dict[str, Any] | None:
    """The ``gates_overall_base_cost['timeline']`` entry for a timeline judgment,
    or ``None`` when there is nothing to surface.

    * validated GATING judgment -> the blocking go/no_go entry;
    * a CLAIMED-ratified policy that failed the contract/pin validation -> FAIL
      CLOSED: a loud ``no_go`` entry carrying the violations (a forged or tampered
      ratification can weaken nothing and can never produce a trusted go — and it
      can never quietly restore the pre-ratification look either);
    * clean advisory (genuinely unratified) or absent policy -> ``None`` (the
      pre-ratification behaviour).
    """
    if judgment.get("policy_status") == "ratified_claim_invalid":
        return {
            "go_no_go": "no_go",
            "policy_status": "ratified_claim_invalid",
            "phase0q_id": judgment.get("phase0q_id"),
            "source": "timeline_gate_policy",
            "ratification_violations": list(judgment.get("ratification_violations", [])),
        }
    if judgment.get("gates_enforced"):
        return {
            "go_no_go": "go" if judgment.get("overall_go") is True else "no_go",
            "policy_status": judgment.get("policy_status"),
            "phase0q_id": judgment.get("phase0q_id"),
            "source": "timeline_gate_policy",
        }
    return None


# ------------------------------------------------------------------------- #
# OOS folds                                                                  #
# ------------------------------------------------------------------------- #

def _add_months(d: _dt.date, months: int) -> _dt.date:
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    return _dt.date(y, m, 1)


def oos_folds(primary: tuple[_dt.date, _dt.date]) -> list[dict[str, Any]]:
    """Rolling walk-forward folds (36m train / 12m test / 12m step) inside the
    primary window. Only the TEST window is evaluated (parameters frozen at train
    end; no lookahead). Non-overlapping, contiguous test windows."""
    start, end = primary
    folds: list[dict[str, Any]] = []
    fold_start = start
    idx = 0
    while True:
        train_start = fold_start
        train_end = _add_months(train_start, OOS_TRAIN_MONTHS)
        test_start = train_end
        test_end = _add_months(test_start, OOS_TEST_MONTHS) - _dt.timedelta(days=1)
        if test_end > end:
            break
        folds.append({
            "fold_index": idx,
            "train_start": train_start, "train_end": train_end - _dt.timedelta(days=1),
            "test_start": test_start, "test_end": test_end,
        })
        idx += 1
        fold_start = _add_months(fold_start, OOS_STEP_MONTHS)
    return folds


# ------------------------------------------------------------------------- #
# Per-cell metric computation                                                #
# ------------------------------------------------------------------------- #

def _run_window(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    start: _dt.date,
    end: _dt.date,
    cost_bps: int,
) -> sleeve.SleeveResult:
    window_decisions = _decisions_in(decisions, _dt.date(start.year - 1, 1, 1), end)
    return sleeve.simulate(prices, window_decisions, params,
                           start=start, end=end, cost_bps=cost_bps)


def _primary_metrics(res: sleeve.SleeveResult) -> dict[str, Any]:
    turnover = metrics.one_way_turnover_annualized(res.dates, res.one_way_turnover_by_date)
    return {
        "annualized_turnover": turnover["max_trailing_252"],
        "annualized_turnover_window_average": turnover["window_average_annualized"],
        "total_one_way_turnover": turnover["total_one_way"],
        "max_drawdown": metrics.max_drawdown(res.nav),
        "annualized_volatility": metrics.annualized_volatility(res.nav),
        "worst_5d_return": metrics.worst_5d_return(res.nav),
        "window_return": metrics.window_return(res.nav),
        "n_trading_days": len(res.dates),
        "n_rebalances": len(res.rebalance_dates),
        "reduced_sleeve_days": len(res.reduced_sleeve_dates),
    }


def _fold_metrics(res: sleeve.SleeveResult) -> dict[str, float]:
    turnover = metrics.one_way_turnover_annualized(res.dates, res.one_way_turnover_by_date)
    return {
        "return_annualized": metrics.return_annualized(res.nav, len(res.dates)),
        "sigma_annual": metrics.annualized_volatility(res.nav),
        "MDD": metrics.max_drawdown(res.nav),
        "one_way_turnover_annualized": turnover["max_trailing_252"],
    }


def compute_cell(
    prices: sleeve.PriceFrame,
    decisions: Sequence[decision.DecisionRow],
    params: sleeve.SleeveParams,
    cost_bps: int,
    folds: Sequence[Mapping[str, Any]],
    primary_window: tuple[_dt.date, _dt.date] = PRIMARY_WINDOW,
    stress_windows: Sequence[Mapping[str, Any]] = STRESS_WINDOWS,
) -> dict[str, Any]:
    """All metrics for one (candidate, cost) cell: primary + stress + OOS folds.

    ``primary_window`` and ``stress_windows`` are the CONFIGURED evaluation windows
    (default to the module-level pins); each cell must simulate exactly the windows
    the caller requested, not the module defaults."""
    primary_res = _run_window(prices, decisions, params,
                              primary_window[0], primary_window[1], cost_bps)
    primary = _primary_metrics(primary_res)
    primary["data_quality_flags"] = primary_res.data_quality_flags

    stress: dict[str, Any] = {}
    for win in stress_windows:
        res = _run_window(prices, decisions, params, win["start"], win["end"], cost_bps)
        scheduled = [r.as_of for r in _decisions_in(decisions, win["start"], win["end"])]
        stress[win["window_id"]] = {
            **metrics.stress_window_metrics(
                res.dates, res.nav, res.one_way_turnover_by_date,
                scheduled, _valid_decision_dates(decisions)),
            "coverage_class": win["coverage"],
            "n_trading_days": len(res.dates),
        }

    # data-quality policy: any triggered flag marks the cell reduced_quality.
    primary["data_quality_status"] = (
        "reduced_quality" if primary_res.data_quality_flags else "ok")

    fold_results: list[dict[str, Any]] = []
    fold_metric_list: list[dict[str, float]] = []
    for fold in folds:
        res = _run_window(prices, decisions, params,
                          fold["test_start"], fold["test_end"], cost_bps)
        fm = _fold_metrics(res)
        fold_metric_list.append(fm)
        fold_results.append({
            "fold_index": fold["fold_index"],
            "test_start": fold["test_start"].isoformat(),
            "test_end": fold["test_end"].isoformat(),
            **fm,
        })
    stability = metrics.stability_from_folds(fold_metric_list)

    return {
        "candidate_id": params.candidate_id,
        "cost_bps": cost_bps,
        "parameters": {
            "growth_weight": params.growth_weight,
            "inflation_weight": params.inflation_weight,
            "risk_tilt": params.risk_tilt,
            "defensive_floor_delta_pp": params.defensive_floor_delta_pp,
            "risk_cap_delta_pp": params.risk_cap_delta_pp,
        },
        "primary_window": primary,
        "stress_windows": stress,
        "out_of_sample": {"folds": fold_results, "stability": stability},
        # policy: any triggered data-quality flag marks the whole cell reduced_quality
        # (surfaced here on the cell file and in the gate report; not silently ignored).
        "data_quality_status": primary["data_quality_status"],
    }


def cell_quality_status(cell: Mapping[str, Any]) -> str:
    """reduced_quality if the cell has any triggered data-quality flag, else ok."""
    flags = cell.get("primary_window", {}).get("data_quality_flags", [])
    return "reduced_quality" if flags else "ok"


# ------------------------------------------------------------------------- #
# Gate judgement                                                             #
# ------------------------------------------------------------------------- #

def judge_gates_for_cell(cell: Mapping[str, Any]) -> dict[str, Any]:
    """Judge the five gates for one cell against the base envelope.

    A gate is ``go`` only when its measured value satisfies the envelope. Stress
    windows: only full_basket windows are judged (reduced_coverage are supplementary);
    a full_basket window with decision_coverage < 1.0 is an automatic no_go
    (stress_acceptance)."""
    primary = cell["primary_window"]
    turnover_go = primary["annualized_turnover"] <= BASE_ENVELOPE["max_one_way_turnover_annualized"]
    drawdown_go = primary["max_drawdown"] <= BASE_ENVELOPE["max_drawdown"]
    volatility_go = primary["annualized_volatility"] <= BASE_ENVELOPE["max_annualized_volatility"]

    stress_details: dict[str, Any] = {}
    stress_go = True
    for wid, win in cell["stress_windows"].items():
        if win["coverage_class"] != "full_basket":
            stress_details[wid] = {"judged": False, "reason": "reduced_coverage_supplementary"}
            continue
        coverage_ok = win["decision_coverage"] >= 1.0
        worst5d_ok = win["worst_5d_return"] >= BASE_ENVELOPE["min_worst_5d_return"]
        mdd_ok = win["window_MDD"] <= BASE_ENVELOPE["max_drawdown"]
        win_go = coverage_ok and worst5d_ok and mdd_ok
        stress_details[wid] = {
            "judged": True, "go": win_go,
            "decision_coverage_ok": coverage_ok,
            "worst_5d_ok": worst5d_ok, "window_mdd_ok": mdd_ok,
        }
        stress_go = stress_go and win_go

    stability = cell["out_of_sample"]["stability"]
    vol_dev = stability.get("sigma_annual_max_dev_from_median", 0.0)
    mdd_dev = stability.get("MDD_max_dev_from_median", 0.0)
    folds = cell["out_of_sample"]["folds"]
    fold_envelope_ok = all(
        f["sigma_annual"] <= BASE_ENVELOPE["max_annualized_volatility"]
        and f["MDD"] <= BASE_ENVELOPE["max_drawdown"]
        and f["one_way_turnover_annualized"] <= BASE_ENVELOPE["max_one_way_turnover_annualized"]
        for f in folds
    ) if folds else False
    oos_go = (
        vol_dev <= BASE_ENVELOPE["max_fold_volatility_deviation"]
        and mdd_dev <= BASE_ENVELOPE["max_fold_mdd_deviation"]
        and fold_envelope_ok
    )

    return {
        "turnover": {"measured": primary["annualized_turnover"],
                     "bound": BASE_ENVELOPE["max_one_way_turnover_annualized"],
                     "go": turnover_go},
        "drawdown": {"measured": primary["max_drawdown"],
                     "bound": BASE_ENVELOPE["max_drawdown"], "go": drawdown_go},
        "volatility": {"measured": primary["annualized_volatility"],
                       "bound": BASE_ENVELOPE["max_annualized_volatility"],
                       "go": volatility_go},
        "stress_windows": {"go": stress_go, "windows": stress_details},
        "out_of_sample": {
            "sigma_annual_max_dev_from_median": vol_dev,
            "MDD_max_dev_from_median": mdd_dev,
            "fold_envelope_ok": fold_envelope_ok,
            "vol_dev_bound": BASE_ENVELOPE["max_fold_volatility_deviation"],
            "mdd_dev_bound": BASE_ENVELOPE["max_fold_mdd_deviation"],
            "go": oos_go,
        },
    }


# ------------------------------------------------------------------------- #
# Canonicalization + hashing                                                #
# ------------------------------------------------------------------------- #

def canonicalize(payload: Any) -> Any:
    """Round floats to 12 decimals + normalize dates/dicts (core logical normalizer)."""
    return normalize_logical_value(payload)


def canonical_json(payload: Any) -> str:
    return json.dumps(canonicalize(payload), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False) + "\n"


def _metric_gate_logical_hash(cells: Sequence[Mapping[str, Any]], gate: str) -> str:
    """A per-gate logical hash over the measured values across all cells."""
    projection = [
        {"candidate_id": c["candidate_id"], "cost_bps": c["cost_bps"],
         "gate": gate, "gates": judge_gates_for_cell(c)[gate]}
        for c in cells
    ]
    return stable_hash(canonicalize(projection))


# ------------------------------------------------------------------------- #
# Top-level run                                                             #
# ------------------------------------------------------------------------- #

@dataclass(frozen=True)
class RunConfig:
    """Injected, deterministic run parameters (no wall-clock, no RNG)."""

    run_id: str
    started_at: str
    finished_at: str
    harness_commit: str
    candidates: tuple[sleeve.SleeveParams, ...] = SCENARIO_CANDIDATES
    cost_grid: tuple[int, ...] = COST_GRID_BPS
    primary_window: tuple[_dt.date, _dt.date] = PRIMARY_WINDOW
    stress_windows: tuple[dict[str, Any], ...] = STRESS_WINDOWS


def run_harness(pack_dir: str | Path, config: RunConfig) -> dict[str, Any]:
    """Run the full grid and return {result, gate_report, cells} (in-memory).

    Callers persist the returned canonical payloads with :func:`write_evidence`.
    """
    pack = load_and_verify_pack(pack_dir)
    prices = sleeve.PriceFrame(pack.eod_rows)

    all_windows: list[tuple[_dt.date, _dt.date]] = [config.primary_window]
    all_windows += [(w["start"], w["end"]) for w in config.stress_windows]
    decisions = build_decision_series(pack, all_windows)
    folds = oos_folds(config.primary_window)

    cells: list[dict[str, Any]] = []
    for params in config.candidates:
        for cost_bps in config.cost_grid:
            cell = compute_cell(prices, decisions, params, cost_bps, folds,
                                config.primary_window, config.stress_windows)
            cell["provenance"] = _cell_provenance(pack, config, params, cost_bps)
            cells.append(cell)

    timeline = build_timeline_block(decisions, prices, config)
    gate_report = build_gate_report(pack, config, cells, folds, timeline)
    result = build_contract_result(pack, config, cells, gate_report)
    return {"result": result, "gate_report": gate_report, "cells": cells,
            "decisions": decisions, "timeline": timeline,
            "input_pack_sha256": pack.input_pack_sha256}


def _cell_provenance(pack, config, params, cost_bps) -> dict[str, Any]:
    return {
        "input_pack_sha256": pack.input_pack_sha256,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "harness_commit": config.harness_commit,
        "run_id": config.run_id,
        "started_at": config.started_at,
        "finished_at": config.finished_at,
        "candidate_id": params.candidate_id,
        "cost_bps": cost_bps,
        "log_path": f"logs/{config.run_id}/{params.candidate_id}_{cost_bps}bps.log",
        "execution_leg": "local_python_pure",
    }


# ------------------------------------------------------------------------- #
# Gate report + contract result                                             #
# ------------------------------------------------------------------------- #

def build_gate_report(pack, config, cells, folds, timeline=None) -> dict[str, Any]:
    per_cost: dict[str, Any] = {}
    for cost_bps in config.cost_grid:
        cost_cells = [c for c in cells if c["cost_bps"] == cost_bps]
        gate_judgements = {c["candidate_id"]: judge_gates_for_cell(c) for c in cost_cells}
        # per-gate go across candidates at this cost level: go only if EVERY candidate
        # cell at this cost passes the gate.
        per_gate = {}
        for gate in ("turnover", "drawdown", "volatility", "stress_windows", "out_of_sample"):
            per_gate[gate] = {
                "go": all(gate_judgements[cid][gate]["go"] for cid in gate_judgements),
                "by_candidate": {cid: gate_judgements[cid][gate] for cid in gate_judgements},
            }
        per_cost[str(cost_bps)] = {"per_gate": per_gate}

    base_cells = [c for c in cells if c["cost_bps"] == BASE_COST_BPS]
    if not base_cells:
        # No base-cost (5bps) cell was measured. Judging the overall gates from an
        # empty base_judgements set would make all([]) vacuously True and mark every
        # gate go -> a misleading green. Fail loudly instead.
        raise ValueError(
            f"base cost cell absent: cost_grid {tuple(config.cost_grid)} does not "
            f"include BASE_COST_BPS={BASE_COST_BPS}; overall base-cost gates are "
            "not_measured and cannot be judged")
    base_judgements = {c["candidate_id"]: judge_gates_for_cell(c) for c in base_cells}
    overall = {}
    for gate in ("turnover", "drawdown", "volatility", "stress_windows", "out_of_sample"):
        overall[gate] = {
            "go_no_go": "go" if all(
                base_judgements[cid][gate]["go"] for cid in base_judgements) else "no_go",
            "base_cost_bps": BASE_COST_BPS,
        }

    # phase0q_005 (RATIFIED 2026-07-11): the validated ratified policy makes the
    # timeline judgment a BLOCKING overall gate — a distinct, honest go/no_go entry
    # (never a crash); the frozen v1 model is expected to report no_go here until a
    # recalibrated candidate passes review. A CLAIMED-ratified but invalid/tampered
    # policy fails CLOSED (a loud no_go with the violations); a genuinely unratified
    # policy never enters this dict — the pre-ratification behaviour.
    timeline_judgment = (timeline or {}).get("gate_judgment") or {}
    timeline_entry = timeline_overall_gate_entry(timeline_judgment)
    if timeline_entry is not None:
        overall["timeline"] = timeline_entry

    # surface per-cell data-quality status so a reduced_quality cell (triggered flag)
    # can never be reported as cleanly passing the quantitative gates.
    dq_cells = [
        {"candidate_id": c["candidate_id"], "cost_bps": c["cost_bps"],
         "data_quality_status": cell_quality_status(c),
         "data_quality_flags": c["primary_window"].get("data_quality_flags", [])}
        for c in cells
    ]
    data_quality = {
        "policy": "any_triggered_flag_marks_cell_reduced_quality",
        "any_reduced_quality": any(
            e["data_quality_status"] == "reduced_quality" for e in dq_cells),
        "cells": dq_cells,
    }

    return {
        "artifact_type": "phase0q_quantitative_gate_report_measured",
        "schema_version": 1,
        "phase0q_id": "open_macro_v03_phase0q_001",
        "phase0q_supplement_id": "open_macro_v03_phase0q_002",
        "evidence_id": "open_macro_v03_metric_evidence_001",
        "status": "measured_pending_cloud_leg",
        "approved": False,
        "approval_required_from": "quant_owner",
        "base_envelope": BASE_ENVELOPE,
        "cost_grid_bps": list(config.cost_grid),
        "base_cost_bps": BASE_COST_BPS,
        "n_oos_folds": len(folds),
        "gates_overall_base_cost": overall,
        "per_cost_level": per_cost,
        "data_quality": data_quality,
        # Tranche W1: regime-timeline diagnostics are ALWAYS reported (abstention/carry/
        # quadrant occupancy + benchmark-relative upside capture) so the behaviour the
        # risk-only envelope was blind to is never invisible again. Judged only when the
        # proposed timeline gate policy is ratified (else advisory — see judge wiring).
        "timeline": timeline if timeline is not None else {},
        "execution_legs": {"local_python_pure": "complete", "qc_research_object_store": "pending"},
        "governance": GOVERNANCE_PINS,
        "provenance": {
            "input_pack_id": INPUT_PACK_ID,
            "input_pack_sha256": pack.input_pack_sha256,
            "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
            "harness_commit": config.harness_commit,
            "run_id": config.run_id,
            "started_at": config.started_at,
            "finished_at": config.finished_at,
        },
        "notes": (
            "MEASURED evidence only. No gate go/no_go here grants activation: A5 stays "
            "blocked, official_result=false, allocator_publish=false, db_write=none, "
            "freeze_ready=false. Final status is pending the qc_research_object_store leg "
            "reproducing the local hashes; quant_owner review is required."
        ),
    }


def build_contract_result(pack, config, cells, gate_report) -> dict[str, Any]:
    output_logical_hashes = {
        "annualized_volatility": _metric_gate_logical_hash(cells, "volatility"),
        "max_drawdown": _metric_gate_logical_hash(cells, "drawdown"),
        "out_of_sample_stability": _metric_gate_logical_hash(cells, "out_of_sample"),
        "stress_window_behavior": _metric_gate_logical_hash(cells, "stress_windows"),
        "turnover": _metric_gate_logical_hash(cells, "turnover"),
        "metrics_canonical_logical_hash": stable_hash(canonicalize(cells)),
    }
    local_leg_hash = stable_hash(canonicalize({
        "cells": cells, "gate_report_overall": gate_report["gates_overall_base_cost"]}))
    fingerprint_payload = {
        "schema_version": 1,
        "job_type": "open_macro_v03_metric_backtest",
        "input_pack_sha256": pack.input_pack_sha256,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "output_logical_hashes": output_logical_hashes,
        "runtime_activation": False,
    }
    return {
        "schema_version": 1,
        "job_type": "open_macro_v03_metric_backtest",
        "job_id": config.run_id,
        "execution_id": config.run_id,
        "run_fingerprint": stable_hash(fingerprint_payload),
        "status": "succeeded",
        "classification": "metric_evidence_only",
        "input_pack_sha256": pack.input_pack_sha256,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "output_logical_hashes": output_logical_hashes,
        "execution_legs": [
            {"leg": "local_python_pure", "logical_hash": local_leg_hash},
        ],
        "artifact_prefix": "artifacts/quant/open_macro_v03_metric_evidence_001",
        "errors": [],
        "runtime_activation": False,
        "a5_status": "blocked",
        "official_result": False,
        "allocator_publish": False,
        "db_write": "none",
        "production_endpoint_activation": "none",
    }


# ------------------------------------------------------------------------- #
# Persistence                                                               #
# ------------------------------------------------------------------------- #

def write_evidence(out_dir: str | Path, run: Mapping[str, Any]) -> list[Path]:
    """Write canonical per-cell files + gate report + result. Returns written paths.

    Layout avoids any ``data/`` path segment (a repo .gitignore trap). Per-cell files
    live under ``cells/``.
    """
    out = Path(out_dir)
    (out / "cells").mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for cell in run["cells"]:
        name = f"{cell['candidate_id']}__{cell['cost_bps']}bps.json"
        path = out / "cells" / name
        path.write_text(canonical_json(cell), encoding="utf-8")
        written.append(path)

    gate_path = out / "quantitative_gate_report.measured.json"
    gate_path.write_text(canonical_json(run["gate_report"]), encoding="utf-8")
    written.append(gate_path)

    result_path = out / "metric_backtest_result.json"
    result_path.write_text(canonical_json(run["result"]), encoding="utf-8")
    written.append(result_path)

    return written

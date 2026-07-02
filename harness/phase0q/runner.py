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
import json
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
) -> dict[str, Any]:
    """All metrics for one (candidate, cost) cell: primary + stress + OOS folds."""
    primary_res = _run_window(prices, decisions, params,
                              PRIMARY_WINDOW[0], PRIMARY_WINDOW[1], cost_bps)
    primary = _primary_metrics(primary_res)
    primary["data_quality_flags"] = primary_res.data_quality_flags

    stress: dict[str, Any] = {}
    for win in STRESS_WINDOWS:
        res = _run_window(prices, decisions, params, win["start"], win["end"], cost_bps)
        scheduled = [r.as_of for r in _decisions_in(decisions, win["start"], win["end"])]
        stress[win["window_id"]] = {
            **metrics.stress_window_metrics(
                res.dates, res.nav, res.one_way_turnover_by_date,
                scheduled, _valid_decision_dates(decisions)),
            "coverage_class": win["coverage"],
            "n_trading_days": len(res.dates),
        }

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
    }


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
            cell = compute_cell(prices, decisions, params, cost_bps, folds)
            cell["provenance"] = _cell_provenance(pack, config, params, cost_bps)
            cells.append(cell)

    gate_report = build_gate_report(pack, config, cells, folds)
    result = build_contract_result(pack, config, cells, gate_report)
    return {"result": result, "gate_report": gate_report, "cells": cells,
            "decisions": decisions, "input_pack_sha256": pack.input_pack_sha256}


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

def build_gate_report(pack, config, cells, folds) -> dict[str, Any]:
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
    base_judgements = {c["candidate_id"]: judge_gates_for_cell(c) for c in base_cells}
    overall = {}
    for gate in ("turnover", "drawdown", "volatility", "stress_windows", "out_of_sample"):
        overall[gate] = {
            "go_no_go": "go" if all(
                base_judgements[cid][gate]["go"] for cid in base_judgements) else "no_go",
            "base_cost_bps": BASE_COST_BPS,
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

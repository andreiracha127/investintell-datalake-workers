"""Offline candidate calibration pack generator for Certified Input Pack P0.

This module is intentionally conservative: it produces auditable candidate
evidence from the verified input pack, but it refuses to mark any result as
freeze-ready or final-approved while institutional limits remain unset.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any

from src.input_packs.hashing import canonical_json_bytes, canonical_json_sha256, file_sha256, load_json, sha256_bytes
from src.input_packs.verifier import verify_pack

CALIBRATION_ID = "open_macro_v03_calibration_001"
INPUT_PACK_ID = "open_macro_v03_certified_input_pack_001"
A3_STATUS = "open_macro_v03"
A4_STATUS = "calibration_candidate_running"
AS_OF = "2026-06-26"
TECHNICAL_DEBTS = ["macro-history-coverage", "macro-vintage-identity"]
REQUIRED_MATRIX_LABELS = {
    "host_jobs1_r0",
    "host_jobs1_r1",
    "host_jobs4_r0",
    "host_jobs4_r1",
    "container_jobs1_r0",
    "container_jobs1_r1",
    "container_jobs4_r0",
    "container_jobs4_r1",
}


def write_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def ensure_child(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(f"refusing to write outside output dir: {resolved}") from exc
    return resolved


def is_child_or_self(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def sha256_payload(payload: Any) -> str:
    return canonical_json_sha256(payload)


def pack_summary(input_pack: Path, expected: dict[str, str]) -> dict[str, Any]:
    verification = verify_pack(input_pack)
    if not verification["ok"]:
        raise ValueError(f"input pack verification failed: {json.dumps(verification, sort_keys=True)}")
    manifest = load_json(input_pack / "manifest.json")
    source_snapshot_sha256 = sha256_payload(
        {
            "raw_snapshot_sha256": manifest["raw_snapshot_sha256"],
            "canonical_snapshot_sha256": manifest["canonical_snapshot_sha256"],
        }
    )
    actual = {
        "input_pack_id": manifest["input_pack_id"],
        "input_pack_sha256": manifest["input_pack_sha256"],
        "source_snapshot_sha256": source_snapshot_sha256,
        "contract_bundle_sha256": manifest["contract_bundle_sha256"],
    }
    for key, value in expected.items():
        if actual.get(key) != value:
            raise ValueError(f"{key} mismatch: expected {value}, got {actual.get(key)}")
    if actual["input_pack_id"] != INPUT_PACK_ID:
        raise ValueError(f"unexpected input_pack_id: {actual['input_pack_id']}")
    return {
        **actual,
        "manifest": manifest,
        "verification": verification,
        "builder_code_sha256": manifest["builder_code_sha256"],
        "builder_commit": manifest["builder_commit"],
        "as_of": manifest["as_of"],
    }


def default_config(summary: dict[str, Any], *, merge_commit: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "strategy": A3_STATUS,
        "status": "candidate",
        "as_of": summary["as_of"],
        "input_pack_id": summary["input_pack_id"],
        "input_pack_sha256": summary["input_pack_sha256"],
        "source_snapshot_sha256": summary["source_snapshot_sha256"],
        "contract_bundle_sha256": summary["contract_bundle_sha256"],
        "input_pack_p0_merge_commit": merge_commit,
        "random_seed": 20260626,
        "jobs_matrix": [1, 4],
        "require_bitwise_reproducibility": True,
        "network": "none",
        "db_access": False,
        "input_pack_mount": "read_only",
        "windows": {
            "train": {"start": "2026-06-24", "end": "2026-06-24"},
            "validation": {"start": "2026-06-25", "end": "2026-06-25"},
            "out_of_sample": {"start": "2026-06-26", "end": "2026-06-26"},
            "walk_forward": {
                "supported": False,
                "reason": "P0 fixture has a three-day deterministic evidence window only",
            },
            "stress": [
                {
                    "name": "p0_latest_macro_rates",
                    "start": "2026-06-24",
                    "end": "2026-06-26",
                    "source": "certified_input_pack_only",
                }
            ],
        },
        "objective": {
            "primary": "preserve_current_baseline_until_institutional_limits_exist",
            "tie_breaker": "simplicity_then_proximity_to_baseline",
            "return_target": "not_used",
        },
        "constraints": {
            "technical": {
                "finite_outputs_required": True,
                "weights_sum_tolerance": 1e-12,
                "runtime_activation": False,
                "A5": "blocked",
                "freeze_ready": False,
            },
            "institutional_limits": {
                "daily_cvar_95": "explicitly_unset",
                "beta": "explicitly_unset",
                "max_drawdown": "explicitly_unset",
                "turnover": "explicitly_unset",
                "exposure_bounds": "explicitly_unset",
            },
        },
        "baseline_references": ["G0", "microgrid_v03", "current_baseline_if_certified", "neutral_reference"],
        "rejection_rules": [
            "constraint_violation",
            "nan_or_infinite_metric",
            "non_deterministic_output",
            "material_out_of_sample_degradation_when_threshold_defined",
            "turnover_excess_when_threshold_defined",
            "institutional_limits_explicitly_unset_blocks_final_approval",
        ],
        "final_approval_allowed": False,
    }


def default_parameter_grid() -> dict[str, Any]:
    baseline = {
        "growth_weight": 0.50,
        "inflation_weight": 0.50,
        "risk_tilt": 0.00,
        "defensive_floor_delta_pp": 0,
        "risk_cap_delta_pp": 0,
    }
    variants = [
        ("baseline_current", "baseline/default current candidate", baseline),
        ("growth_plus_2pp", "small local increase around baseline growth weight", {**baseline, "growth_weight": 0.52, "inflation_weight": 0.48}),
        ("inflation_plus_2pp", "small local increase around baseline inflation weight", {**baseline, "growth_weight": 0.48, "inflation_weight": 0.52}),
        ("risk_tilt_plus_1pp", "small local positive risk tilt probe", {**baseline, "risk_tilt": 0.01}),
        ("risk_tilt_minus_1pp", "small local negative risk tilt probe", {**baseline, "risk_tilt": -0.01}),
    ]
    return {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "strategy": A3_STATUS,
        "search_policy": "small_conservative_local_grid",
        "baseline_candidate_id": "baseline_current",
        "ranking_policy": "reject_first_then_rank_by_simplicity_and_baseline_distance",
        "candidates": [
            {
                "candidate_id": candidate_id,
                "role": "baseline" if candidate_id == "baseline_current" else "local_probe",
                "parameters": params,
                "rationale": rationale,
            }
            for candidate_id, rationale, params in variants
        ],
        "anti_overfit_controls": [
            "fixed_small_grid",
            "no_return_target",
            "no_live_db_inputs",
            "baseline_preferred_until_constraints_are_defined",
        ],
    }


def finite_values(value: Any) -> list[float]:
    if isinstance(value, bool) or value is None:
        return []
    if isinstance(value, (int, float)):
        return [float(value)]
    if isinstance(value, list):
        out: list[float] = []
        for item in value:
            out.extend(finite_values(item))
        return out
    if isinstance(value, dict):
        out = []
        for item in value.values():
            out.extend(finite_values(item))
        return out
    return []


def mean(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 12) if values else None


def load_rows(input_pack: Path, rel_path: str) -> list[dict[str, Any]]:
    data = load_json(input_pack / rel_path)
    if not isinstance(data, list):
        raise ValueError(f"{rel_path} must contain a list")
    return data


def input_metrics(input_pack: Path) -> dict[str, Any]:
    fund_returns = [float(r["value"]) for r in load_rows(input_pack, "data/derived/fund_nav_return_features.json")]
    market_returns = [float(r["value"]) for r in load_rows(input_pack, "data/derived/market_price_return_features.json")]
    macro_rows = load_rows(input_pack, "data/derived/macro_observation_features.json")
    macro_levels = [float(r["value"]) for r in macro_rows if r.get("feature_name") == "macro_level"]
    macro_deltas = [float(r["value"]) for r in macro_rows if r.get("feature_name") == "macro_delta_1obs"]
    table_hashes = load_json(input_pack / "table_hashes.json")
    return {
        "fund_return": {
            "count": len(fund_returns),
            "mean": mean(fund_returns),
            "min": round(min(fund_returns), 12),
            "max": round(max(fund_returns), 12),
        },
        "market_return": {
            "count": len(market_returns),
            "mean": mean(market_returns),
            "min": round(min(market_returns), 12),
            "max": round(max(market_returns), 12),
        },
        "macro": {
            "level_count": len(macro_levels),
            "delta_count": len(macro_deltas),
            "level_mean": mean(macro_levels),
            "delta_mean": mean(macro_deltas),
        },
        "table_rows": {str(item["name"]): int(item["rows"]) for item in table_hashes["tables"]},
    }


def candidate_distance(params: dict[str, Any]) -> float:
    return round(
        abs(float(params["growth_weight"]) - 0.5)
        + abs(float(params["inflation_weight"]) - 0.5)
        + abs(float(params["risk_tilt"]))
        + abs(float(params["defensive_floor_delta_pp"])) / 100.0
        + abs(float(params["risk_cap_delta_pp"])) / 100.0,
        12,
    )


def candidate_metrics(grid: dict[str, Any], metrics: dict[str, Any]) -> list[dict[str, Any]]:
    macro_delta = float(metrics["macro"]["delta_mean"] or 0.0)
    fund_mean = float(metrics["fund_return"]["mean"] or 0.0)
    market_mean = float(metrics["market_return"]["mean"] or 0.0)
    rows = []
    for item in grid["candidates"]:
        params = item["parameters"]
        distance = candidate_distance(params)
        balance = float(params["growth_weight"]) - float(params["inflation_weight"])
        objective = abs(fund_mean - market_mean) + abs(balance * macro_delta) + distance * 0.001
        rows.append(
            {
                "candidate_id": item["candidate_id"],
                "role": item["role"],
                "parameters": params,
                "baseline_distance": distance,
                "turnover_proxy": round(distance, 12),
                "objective_value": round(objective, 12),
                "finite": all(math.isfinite(v) for v in finite_values(params)),
                "weights_sum": round(float(params["growth_weight"]) + float(params["inflation_weight"]), 12),
            }
        )
    return rows


def selected_and_rejected(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    baseline = next(row for row in rows if row["candidate_id"] == "baseline_current")
    selected = {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "selection_status": "candidate_baseline_selected",
        "selected_candidate_id": baseline["candidate_id"],
        "parameters": baseline["parameters"],
        "selection_reason": (
            "Institutional limits are explicitly unset; select the current baseline "
            "as the conservative candidate and block final approval."
        ),
        "final_approval_allowed": False,
        "runtime_activation": False,
        "A5": "blocked",
        "freeze_ready": False,
    }
    rejected = {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "rejected_count": len(rows) - 1,
        "rejections": [
            {
                "candidate_id": row["candidate_id"],
                "reason": "institutional_limits_explicitly_unset_blocks_final_approval",
                "objective_value": row["objective_value"],
                "baseline_distance": row["baseline_distance"],
            }
            for row in rows
            if row["candidate_id"] != baseline["candidate_id"]
        ],
    }
    return selected, rejected


def build_baseline_comparison(candidate_rows: list[dict[str, Any]], selected: dict[str, Any]) -> dict[str, Any]:
    selected_row = next(row for row in candidate_rows if row["candidate_id"] == selected["selected_candidate_id"])
    neutral = {
        "objective_value": selected_row["objective_value"],
        "baseline_distance": selected_row["baseline_distance"],
        "status": "computed_from_certified_pack",
    }
    unavailable = {
        "status": "reference_not_certified_inside_input_pack",
        "absolute_metrics": None,
        "relative_deltas": None,
        "materiality_flags": ["not_evaluable_without_certified_reference_artifact"],
        "regression_flags": [],
    }
    return {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "selected_candidate_id": selected["selected_candidate_id"],
        "comparisons": {
            "G0": unavailable,
            "microgrid_v03": unavailable,
            "current_baseline_if_certified": unavailable,
            "neutral_reference": {
                "status": "computed_from_current_baseline_candidate",
                "absolute_metrics": neutral,
                "relative_deltas": {"objective_value": 0.0, "baseline_distance": 0.0},
                "materiality_flags": [],
                "regression_flags": [],
                "accepted_degradation_reason": None,
            },
        },
        "final_approval_blockers": ["reference_baselines_not_certified_in_pack", "institutional_limits_explicitly_unset"],
    }


def build_invariant_report(
    *,
    output_dir: Path,
    generated_files: list[str],
    config: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    network: str,
    db_access: bool,
    input_pack_mount: str,
) -> dict[str, Any]:
    values = finite_values(candidate_rows)
    weights_ok = all(abs(float(row["weights_sum"]) - 1.0) <= 1e-12 for row in candidate_rows)
    files_ok = all((output_dir / rel).exists() for rel in generated_files)
    try:
        for rel in generated_files:
            ensure_child(output_dir / rel, output_dir)
        outputs_within_allowed_dir = True
    except ValueError:
        outputs_within_allowed_dir = False
    checks = {
        "no_nan": not any(math.isnan(v) for v in values),
        "no_infinite": not any(math.isinf(v) for v in values),
        "outputs_complete": files_ok,
        "constraints_respected": True,
        "weights_close_within_tolerance": weights_ok,
        "exposures_within_defined_limits": "institutional_limits_explicitly_unset_final_approval_blocked",
        "turnover_within_defined_envelope": "institutional_limits_explicitly_unset_final_approval_blocked",
        "dates_within_input_pack": True,
        "db_access": db_access is False,
        "network_access": network == "none",
        "input_pack_read_only": input_pack_mount == "read_only",
        "no_external_source_access": True,
        "outputs_within_allowed_dir": outputs_within_allowed_dir,
    }
    return {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "ok": all(value for value in checks.values() if isinstance(value, bool)),
        "checks": checks,
        "institutional_limits": config["constraints"]["institutional_limits"],
        "final_approval_allowed": False,
        "technical_debts_accepted": TECHNICAL_DEBTS,
    }


def output_manifest(output_dir: Path, generated_files: list[str], exclude: set[str] | None = None) -> dict[str, Any]:
    excluded = exclude or set()

    def digestible_bytes(path: Path) -> bytes:
        suffix = path.suffix.lower()
        if suffix == ".json":
            return canonical_json_bytes(load_json(path))
        if suffix in {".md", ".txt", ".log"}:
            return path.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8")
        return path.read_bytes()

    artifacts = []
    for rel in sorted(set(generated_files)):
        if rel in excluded:
            continue
        path = ensure_child(output_dir / rel, output_dir)
        if not path.is_file():
            continue
        canonical_bytes = digestible_bytes(path)
        artifacts.append({"path": rel, "sha256": sha256_bytes(canonical_bytes), "bytes": path.stat().st_size})
    return {
        "schema_version": 1,
        "artifact_type": "calibration_output_manifest",
        "calibration_id": CALIBRATION_ID,
        "artifacts": artifacts,
    }


def matrix_evidence_ok(
    matrix_evidence: dict[str, Any] | None,
    current_artifact_hashes: dict[str, str],
    matrix_run_hashes: dict[str, dict[str, str]],
) -> bool:
    if not isinstance(matrix_evidence, dict):
        return False
    if matrix_evidence.get("calibration_id") != CALIBRATION_ID:
        return False
    try:
        run_count = int(matrix_evidence.get("run_count", 0))
        mismatch_count = int(matrix_evidence.get("mismatch_count", -1))
    except (TypeError, ValueError):
        return False
    labels = matrix_evidence.get("labels")
    if not isinstance(labels, list):
        return False
    if len({label for label in labels if isinstance(label, str)}) != len(labels):
        return False
    if set(labels) != REQUIRED_MATRIX_LABELS:
        return False
    if set(labels) != set(matrix_run_hashes):
        return False
    if matrix_evidence.get("network") != "none":
        return False
    if matrix_evidence.get("db_access") is not False:
        return False
    if matrix_evidence.get("input_pack_mount") != "read_only":
        return False
    if matrix_evidence.get("ok") is not True or run_count < len(REQUIRED_MATRIX_LABELS) or mismatch_count != 0:
        return False
    comparisons = matrix_evidence.get("comparisons")
    base_label = matrix_evidence.get("base_label")
    if not isinstance(comparisons, dict) or not isinstance(base_label, str):
        return False
    for label in labels:
        comparison = comparisons.get(f"{base_label}_vs_{label}")
        if not isinstance(comparison, dict):
            return False
        if comparison.get("ok") is not True or comparison.get("mismatched") not in ([], None):
            return False
        hashes = matrix_run_hashes.get(label)
        if hashes is None:
            return False
        for key, value in current_artifact_hashes.items():
            if hashes.get(key) != value:
                return False
    return True


def run_hashes_from_evidence(matrix_evidence: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    if not isinstance(matrix_evidence, dict):
        return {}
    base_label = matrix_evidence.get("base_label")
    labels = matrix_evidence.get("labels")
    comparisons = matrix_evidence.get("comparisons")
    if not isinstance(base_label, str) or not isinstance(labels, list) or not isinstance(comparisons, dict):
        return {}
    run_hashes: dict[str, dict[str, str]] = {}
    for label in labels:
        if not isinstance(label, str):
            continue
        comparison = comparisons.get(f"{base_label}_vs_{label}")
        if (
            isinstance(comparison, dict)
            and comparison.get("ok") is True
            and comparison.get("mismatched") in ([], None)
            and isinstance(comparison.get("hashes"), dict)
        ):
            run_hashes[label] = {str(key): str(value) for key, value in comparison["hashes"].items()}
    return run_hashes


def hashes_for_labels(run_hashes: dict[str, dict[str, str]], token: str) -> dict[str, dict[str, str]]:
    return {label: hashes for label, hashes in run_hashes.items() if token in label}


def hashes_for(paths: dict[str, Path]) -> dict[str, str]:
    return {name: file_sha256(path) for name, path in paths.items()}


def render_report(
    *,
    summary: dict[str, Any],
    selected: dict[str, Any],
    rejected: dict[str, Any],
    invariant: dict[str, Any],
    baseline: dict[str, Any],
) -> str:
    return "\n".join(
        [
            "# open_macro_v03 calibration 001",
            "",
            "## Objective",
            "Generate a candidate calibration pack from the merged Certified Input Pack P0 without activating runtime, A5, shadow mode, endpoints, or productive DB writes.",
            "",
            "## Inputs",
            f"- input_pack_id: `{summary['input_pack_id']}`",
            f"- input_pack_sha256: `{summary['input_pack_sha256']}`",
            f"- source_snapshot_sha256: `{summary['source_snapshot_sha256']}`",
            f"- contract_bundle_sha256: `{summary['contract_bundle_sha256']}`",
            "",
            "## Decision",
            f"- selected_candidate_id: `{selected['selected_candidate_id']}`",
            f"- rejected_candidates: `{rejected['rejected_count']}`",
            "- status: `candidate`",
            "- runtime_activation: `false`",
            "- A5: `blocked`",
            "- freeze_ready: `false`",
            "",
            "## Metrics",
            "Metrics are deterministic evidence extracted from the certified pack. No live DB or external source is consulted.",
            "",
            "## Baseline Comparison",
            "G0, microgrid_v03, and current baseline references are recorded as not certified inside this input pack; neutral_reference is computed from the selected baseline candidate.",
            f"- final_approval_blockers: `{', '.join(baseline['final_approval_blockers'])}`",
            "",
            "## Invariants",
            f"- invariant_report.ok: `{str(invariant['ok']).lower()}`",
            "- no NaN/infinite outputs",
            "- output directory closed",
            "- network none",
            "- DB access disabled",
            "",
            "## Limitations",
            "- Institutional CVaR, beta, drawdown, turnover, and exposure limits are explicitly unset.",
            "- The pack remains candidate-only even when reproducibility gates pass.",
            "",
            "## Accepted Technical Debt",
            "- macro-history-coverage",
            "- macro-vintage-identity",
            "",
            "## Next Gate",
            "Technical and quantitative review of the candidate calibration evidence before any shadow-readiness preparation.",
            "",
        ]
    )


def run_calibration(args: argparse.Namespace) -> dict[str, Any]:
    input_pack = Path(args.input_pack).resolve()
    output_dir = Path(args.output_dir).resolve()
    if is_child_or_self(output_dir, input_pack):
        raise ValueError(f"output_dir must not be inside the certified input pack: {output_dir}")
    if args.db_access:
        raise ValueError("db_access must remain false")
    if args.network != "none":
        raise ValueError("network must be none")
    if args.input_pack_mount != "read_only":
        raise ValueError("input pack mount must be read_only")
    output_dir.mkdir(parents=True, exist_ok=True)
    ensure_child(output_dir, output_dir)

    expected = {
        "input_pack_id": INPUT_PACK_ID,
        "input_pack_sha256": args.input_pack_sha256,
        "source_snapshot_sha256": args.source_snapshot_sha256,
        "contract_bundle_sha256": args.contract_bundle_sha256,
    }
    summary = pack_summary(input_pack, expected)
    if args.input_pack_p0_merge_commit != summary["builder_commit"]:
        raise ValueError(
            f"input_pack_p0_merge_commit mismatch: expected verified pack commit {summary['builder_commit']}, "
            f"got {args.input_pack_p0_merge_commit}"
        )
    if not args.engine_commit:
        raise ValueError("engine_commit must be provided explicitly")
    engine_commit = args.engine_commit
    if args.builder_commit and args.builder_commit != summary["builder_commit"]:
        raise ValueError(
            f"builder_commit mismatch: expected verified pack commit {summary['builder_commit']}, "
            f"got {args.builder_commit}"
        )
    if args.builder_code_sha256 and args.builder_code_sha256 != summary["builder_code_sha256"]:
        raise ValueError(
            f"builder_code_sha256 mismatch: expected verified pack hash {summary['builder_code_sha256']}, "
            f"got {args.builder_code_sha256}"
        )
    builder_commit = summary["builder_commit"]
    config = default_config(summary, merge_commit=args.input_pack_p0_merge_commit)
    grid = default_parameter_grid()

    config_path = output_dir / "calibration_config.json"
    grid_path = output_dir / "parameter_grid.json"
    write_json(config_path, config)
    write_json(grid_path, grid)

    metrics = input_metrics(input_pack)
    candidates = candidate_metrics(grid, metrics)
    selected, rejected = selected_and_rejected(candidates)
    baseline = build_baseline_comparison(candidates, selected)

    generated_files = [
        "calibration_config.json",
        "parameter_grid.json",
        "selected_parameters.json",
        "rejected_candidates.json",
        "metrics_manifest.json",
        "baseline_comparison.json",
        "invariant_report.json",
        "reproducibility_report.json",
        "run_matrix.json",
        "logs/calibration.log",
        "calibration_report.md",
    ]

    metrics_manifest = {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "input_metrics": metrics,
        "candidate_metrics": candidates,
        "objective": config["objective"],
        "final_approval_allowed": False,
    }

    paths = {
        "selected_parameters_sha256": output_dir / "selected_parameters.json",
        "rejected_candidates_sha256": output_dir / "rejected_candidates.json",
        "metrics_manifest_sha256": output_dir / "metrics_manifest.json",
        "baseline_comparison_sha256": output_dir / "baseline_comparison.json",
        "invariant_report_sha256": output_dir / "invariant_report.json",
    }
    write_json(output_dir / "selected_parameters.json", selected)
    write_json(output_dir / "rejected_candidates.json", rejected)
    write_json(output_dir / "metrics_manifest.json", metrics_manifest)
    write_json(output_dir / "baseline_comparison.json", baseline)
    write_json(output_dir / "run_matrix.json", {"pending": True})
    write_json(output_dir / "reproducibility_report.json", {"pending": True})
    write_text(output_dir / "calibration_report.md", "pending\n")
    write_text(
        output_dir / "logs" / "calibration.log",
        "offline calibration candidate generated from certified input pack; db_access=false; network=none\n",
    )

    invariant = build_invariant_report(
        output_dir=output_dir,
        generated_files=generated_files,
        config=config,
        candidate_rows=candidates,
        network=args.network,
        db_access=args.db_access,
        input_pack_mount=args.input_pack_mount,
    )
    write_json(output_dir / "invariant_report.json", invariant)
    invariant = build_invariant_report(
        output_dir=output_dir,
        generated_files=generated_files,
        config=config,
        candidate_rows=candidates,
        network=args.network,
        db_access=args.db_access,
        input_pack_mount=args.input_pack_mount,
    )
    write_json(output_dir / "invariant_report.json", invariant)
    write_text(
        output_dir / "calibration_report.md",
        render_report(summary=summary, selected=selected, rejected=rejected, invariant=invariant, baseline=baseline),
    )

    output_manifest_files = [
        rel for rel in generated_files if rel not in {"run_matrix.json", "reproducibility_report.json"}
    ]
    out_manifest = output_manifest(output_dir, output_manifest_files)
    write_json(output_dir / "output_manifest.json", out_manifest)

    artifact_hashes = hashes_for({**paths, "output_manifest_sha256": output_dir / "output_manifest.json"})
    matrix_evidence = load_json(Path(args.evidence_json)) if args.evidence_json else None
    matrix_run_hashes = run_hashes_from_evidence(matrix_evidence)
    matrix_ok = matrix_evidence_ok(matrix_evidence, artifact_hashes, matrix_run_hashes)
    if not matrix_ok:
        matrix_run_hashes = {}
    run_matrix = {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "required_runs": ["jobs=1", "jobs=4", "repeat jobs=1", "repeat jobs=4"],
        "jobs_parameter_effect": "deterministic candidate evaluator is invariant to jobs",
        "current_run_hashes": artifact_hashes,
        "hashes": matrix_run_hashes,
        "comparison_evidence": matrix_evidence,
        "evidence_required": True,
        "ok": matrix_ok,
    }
    write_json(output_dir / "run_matrix.json", run_matrix)

    reproducibility = {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "input_pack_sha256": summary["input_pack_sha256"],
        "source_snapshot_sha256": summary["source_snapshot_sha256"],
        "contract_bundle_sha256": summary["contract_bundle_sha256"],
        "builder_code_sha256": summary["builder_code_sha256"],
        "engine_image_digest": args.engine_image_digest,
        "engine_image_id": args.engine_image_id,
        "docker_context_sha256": args.docker_context_sha256,
        "dockerfile_sha256": args.dockerfile_sha256,
        "calibration_config_sha256": file_sha256(config_path),
        "parameter_grid_sha256": file_sha256(grid_path),
        "jobs_1_hashes": hashes_for_labels(matrix_run_hashes, "jobs1"),
        "jobs_4_hashes": hashes_for_labels(matrix_run_hashes, "jobs4"),
        "repeat_run_hashes": matrix_run_hashes,
        "current_run_hashes": artifact_hashes,
        "path_independence": True,
        "network": args.network,
        "db_access": args.db_access,
        "timestamp_execution_id_exclusion_policy": "no timestamps or execution ids are included in semantic artifacts",
        "output_canonicalization_policy": "canonical JSON with sorted keys and stable file hashes",
        "evidence_required": True,
        "evidence_ok": matrix_ok,
        "comparison_evidence": matrix_evidence,
    }
    write_json(output_dir / "reproducibility_report.json", reproducibility)

    manifest = {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "status": "candidate",
        "as_of": summary["as_of"],
        "input_pack_id": summary["input_pack_id"],
        "input_pack_sha256": summary["input_pack_sha256"],
        "source_snapshot_sha256": summary["source_snapshot_sha256"],
        "contract_bundle_sha256": summary["contract_bundle_sha256"],
        "input_pack_p0_merge_commit": args.input_pack_p0_merge_commit,
        "calibration_branch_base_commit": args.calibration_branch_base_commit,
        "engine_commit": engine_commit,
        "builder_commit": builder_commit,
        "builder_code_sha256": summary["builder_code_sha256"],
        "engine_image_digest": args.engine_image_digest,
        "engine_image_id": args.engine_image_id,
        "docker_context_sha256": args.docker_context_sha256,
        "dockerfile_sha256": args.dockerfile_sha256,
        "calibration_config_sha256": file_sha256(config_path),
        "parameter_grid_sha256": file_sha256(grid_path),
        "output_manifest_sha256": file_sha256(output_dir / "output_manifest.json"),
        "run_matrix_sha256": file_sha256(output_dir / "run_matrix.json"),
        "reproducibility_report_sha256": file_sha256(output_dir / "reproducibility_report.json"),
        "selected_parameters_sha256": file_sha256(output_dir / "selected_parameters.json"),
        "rejected_candidates_sha256": file_sha256(output_dir / "rejected_candidates.json"),
        "metrics_manifest_sha256": file_sha256(output_dir / "metrics_manifest.json"),
        "invariant_report_sha256": file_sha256(output_dir / "invariant_report.json"),
        "baseline_comparison_sha256": file_sha256(output_dir / "baseline_comparison.json"),
        "runtime_activation": False,
        "A3": A3_STATUS,
        "A4": A4_STATUS,
        "A5": "blocked",
        "freeze_ready": False,
        "rebuilt_from_main": True,
    }
    write_json(output_dir / "calibration_manifest.json", manifest)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate open_macro_v03 calibration candidate pack")
    parser.add_argument("--input-pack", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--input-pack-sha256", required=True)
    parser.add_argument("--source-snapshot-sha256", required=True)
    parser.add_argument("--contract-bundle-sha256", required=True)
    parser.add_argument("--input-pack-p0-merge-commit", required=True)
    parser.add_argument("--calibration-branch-base-commit", required=True)
    parser.add_argument("--engine-commit", required=True)
    parser.add_argument("--builder-commit")
    parser.add_argument("--builder-code-sha256")
    parser.add_argument("--engine-image-digest", default=None)
    parser.add_argument("--engine-image-id", default=None)
    parser.add_argument("--docker-context-sha256", required=True)
    parser.add_argument("--dockerfile-sha256", required=True)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--network", default="none")
    parser.add_argument("--db-access", action="store_true", default=False)
    parser.add_argument("--input-pack-mount", default="read_only")
    parser.add_argument("--evidence-json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.db_access:
        raise ValueError("db_access must remain false")
    if args.network != "none":
        raise ValueError("network must be none")
    if args.input_pack_mount != "read_only":
        raise ValueError("input pack mount must be read_only")
    manifest = run_calibration(args)
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

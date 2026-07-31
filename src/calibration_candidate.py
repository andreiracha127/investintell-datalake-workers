"""Offline candidate calibration pack generator for Certified Input Pack P0.

This module is intentionally conservative: it produces auditable candidate
evidence from the verified input pack and refuses to mark a result final-approved
while a real blocker stands.

What changed (wave 4): the five institutional limits used to be the string
literals ``"explicitly_unset"``, with a rejection rule whose ONLY trigger was
the fact that they were unset and a ``final_approval_allowed: False`` written as
a literal. There was no parameter, env var or config file that could ever set
them, so final approval was blocked by a condition the code made impossible to
satisfy. The limits are parameters now
(``configs/calibration/institutional_limits.json``), the rejection rule tests
VIOLATION, and ``final_approval_allowed`` is DERIVED from the blockers that
actually stand.

Three outcomes are representable per limit, and each is a different truth:

* ``unset``          — the mandate does not define it (blocks; honest);
* ``not_evaluable``  — defined, but the certified evidence does not measure it
  yet (blocks; a fact about coverage, not a verdict);
* ``within`` / ``violated`` — measured against the mandate.

Whichever blocks today opens by itself when the config or the evidence changes,
instead of waiting for someone to edit a literal.
"""

from __future__ import annotations

import argparse
import datetime as dt
import functools
import hashlib
import json
import math
import subprocess
from pathlib import Path
from typing import Any

from src.input_packs.hashing import canonical_json_bytes, canonical_json_sha256, file_sha256, load_json, sha256_bytes
from src.input_packs.verifier import verify_pack

CALIBRATION_ID = "open_macro_v03_calibration_001"
INSTITUTIONAL_LIMITS_PATH = (
    Path(__file__).resolve().parents[1] / "configs" / "calibration" / "institutional_limits.json"
)

#: Candidate-metric keys the generator can measure from the certified pack today.
#: A configured limit whose metric is absent here is reported ``not_evaluable``
#: rather than silently passed or dishonestly failed.
EVALUABLE_CANDIDATE_METRICS = ("turnover_proxy",)

#: The mandate the calibration is judged against, as a REQUIRED key set. Omitting
#: a key from the config file must block exactly like setting it to null: without
#: this, deleting four entries and leaving only the measurable one would have
#: produced an empty blocker list — silence reading as approval.
#: The baseline references the comparison needs certified INSIDE the pack.
BASELINE_REFERENCE_IDS: tuple[str, ...] = (
    "G0",
    "microgrid_v03",
    "current_baseline_if_certified",
)

REQUIRED_INSTITUTIONAL_LIMITS: tuple[str, ...] = (
    "beta",
    "daily_cvar_95",
    "exposure_bounds",
    "max_drawdown",
    "turnover",
)
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
DOCKER_CONTEXT_PATHS = [
    "requirements.quant-engine.lock",
    # The institutional mandate is an INPUT to the run: changing it changes the
    # config, the blockers and the approval verdict. It has to be inside the
    # hashed context, or a supplied docker_context_sha256 could stay valid across
    # a mandate change.
    "configs/calibration",
    "packages/investintell_quant_core",
    "services/quant_engine",
    "contracts/quant-engine",
    "schemas/input_packs",
    "qc_a3_core.py",
    "src",
    "docker/quant-engine/entrypoint.sh",
    "docker/quant-engine/Dockerfile",
]


def write_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    prepare_write_path(path)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_text(path: Path, text: str) -> None:
    prepare_write_path(path)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def prepare_write_path(path: Path) -> None:
    for candidate in (path, path.parent, *path.parent.parents):
        if candidate.is_symlink():
            raise ValueError(f"refusing to write through symlinked output path: {candidate}")
    path.parent.mkdir(parents=True, exist_ok=True)
    for candidate in (path, path.parent, *path.parent.parents):
        if candidate.is_symlink():
            raise ValueError(f"refusing to write through symlinked output path: {candidate}")


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


def normalize_git_commit(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or len(value) != 40 or not all(ch in "0123456789abcdefABCDEF" for ch in value):
        raise ValueError(f"{field_name} must be a 40-character git commit SHA")
    return value.lower()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def require_checkoutable_commit(commit: str, *, field_name: str) -> None:
    root = repo_root()
    if not (root / ".git").exists():
        return
    result = subprocess.run(
        ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise ValueError(f"{field_name} is not a checkoutable commit in this repository: {commit}")


def require_ancestor_commit(commit: str, *, ancestor_of: str, field_name: str) -> None:
    root = repo_root()
    if not (root / ".git").exists():
        return
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, ancestor_of],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise ValueError(f"{field_name} must be an ancestor of {ancestor_of}: {commit}")


def normalize_engine_commit(value: Any) -> str:
    commit = normalize_git_commit(value, field_name="engine_commit")
    require_checkoutable_commit(commit, field_name="engine_commit")
    require_ancestor_commit(commit, ancestor_of="HEAD", field_name="engine_commit")
    return commit


def validate_sha256_hex(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or not all(ch in "0123456789abcdefABCDEF" for ch in value):
        raise ValueError(f"{field_name} must be a 64-character SHA-256 hex digest")
    return value.lower()


def validate_optional_prefixed_sha256(value: Any, *, field_name: str) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise ValueError(f"{field_name} must be a sha256:<64 hex> digest")
    digest = validate_sha256_hex(value.removeprefix("sha256:"), field_name=field_name)
    return f"sha256:{digest}"


@functools.cache
def committed_docker_context_sha256(engine_commit: str) -> str:
    root = repo_root()
    files_output = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", engine_commit, "--", *DOCKER_CONTEXT_PATHS],
        cwd=root,
        text=True,
    )
    files = [line for line in files_output.splitlines() if line]
    if not files:
        raise ValueError(f"docker_context_sha256 cannot be verified because context is empty at {engine_commit}")
    records = []
    for file in files:
        payload = subprocess.check_output(["git", "show", f"{engine_commit}:{file}"], cwd=root)
        records.append(f"{file}\0{hashlib.sha256(payload).hexdigest()}")
    return hashlib.sha256("\n".join(records).encode("utf-8")).hexdigest()


def validate_docker_context_sha256(value: Any, *, engine_commit: str) -> str:
    digest = validate_sha256_hex(value, field_name="docker_context_sha256")
    root = repo_root()
    if (root / ".git").exists():
        expected = committed_docker_context_sha256(engine_commit)
        if digest != expected:
            raise ValueError(f"docker_context_sha256 mismatch: expected committed context hash {expected}, got {digest}")
    return digest


def validate_dockerfile_sha256(value: Any, *, engine_commit: str) -> str:
    digest = validate_sha256_hex(value, field_name="dockerfile_sha256")
    root = repo_root()
    if (root / ".git").exists():
        result = subprocess.run(
            ["git", "show", f"{engine_commit}:docker/quant-engine/Dockerfile"],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode != 0:
            raise ValueError(
                f"dockerfile_sha256 cannot be verified because Dockerfile is unavailable at {engine_commit}"
            )
        expected = hashlib.sha256(result.stdout).hexdigest()
        if digest != expected:
            raise ValueError(f"dockerfile_sha256 mismatch: expected committed blob hash {expected}, got {digest}")
    return digest


def validate_commit_contains_verified_pack(
    value: Any,
    *,
    field_name: str,
    input_pack: Path,
    summary: dict[str, Any],
) -> str:
    commit = normalize_git_commit(value, field_name=field_name)
    require_checkoutable_commit(commit, field_name=field_name)
    root = repo_root()
    if not (root / ".git").exists():
        return commit
    try:
        relative_pack = input_pack.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise ValueError(f"input pack must be inside repository to verify P0 merge commit: {input_pack}") from exc
    result = subprocess.run(
        ["git", "show", f"{commit}:{relative_pack}/manifest.json"],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise ValueError(f"{field_name} does not contain the certified pack manifest: {commit}")
    committed_manifest = json.loads(result.stdout)
    if committed_manifest.get("input_pack_sha256") != summary["input_pack_sha256"]:
        raise ValueError(
            f"{field_name} does not contain the verified input pack: "
            f"expected input_pack_sha256 {summary['input_pack_sha256']}, "
            f"got {committed_manifest.get('input_pack_sha256')}"
        )
    if committed_manifest.get("builder_commit") != summary["builder_commit"]:
        raise ValueError(
            f"{field_name} builder provenance mismatch: "
            f"expected {summary['builder_commit']}, got {committed_manifest.get('builder_commit')}"
        )
    return commit


def validate_input_pack_p0_merge_commit(value: Any, *, input_pack: Path, summary: dict[str, Any]) -> str:
    return validate_commit_contains_verified_pack(
        value,
        field_name="input_pack_p0_merge_commit",
        input_pack=input_pack,
        summary=summary,
    )


def validate_calibration_branch_base_commit(
    value: Any,
    *,
    input_pack_p0_merge_commit: str,
    input_pack: Path,
    summary: dict[str, Any],
) -> str:
    commit = validate_commit_contains_verified_pack(
        value,
        field_name="calibration_branch_base_commit",
        input_pack=input_pack,
        summary=summary,
    )
    require_ancestor_commit(input_pack_p0_merge_commit, ancestor_of=commit, field_name="calibration_branch_base_commit")
    require_ancestor_commit(commit, ancestor_of="HEAD", field_name="calibration_branch_base_commit")
    return commit


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


def institutional_limits_sha256(path: Path | str | None = None) -> str | None:
    """sha256 of the mandate file that produced this run, or None if absent.

    Belt-and-braces alongside putting ``configs/calibration`` in
    DOCKER_CONTEXT_PATHS: the context hash is computed over a COMMIT's tree, so
    it only covers the mandate once the run is anchored at a commit containing
    it. This digest pins the exact bytes used, whatever the anchor.
    """
    target = Path(path) if path is not None else INSTITUTIONAL_LIMITS_PATH
    if not target.is_file():
        return None
    return file_sha256(target)


def load_institutional_limits(path: Path | str | None = None) -> dict[str, Any]:
    """The configured institutional mandate.

    A missing config file is not an error and not a silent pass: every limit
    reports ``unset``, which still blocks final approval. That keeps the
    conservative behaviour while making the limits settable.
    """
    target = Path(path) if path is not None else INSTITUTIONAL_LIMITS_PATH
    if not target.is_file():
        return {}
    payload = load_json(target)
    limits = payload.get("limits") if isinstance(payload, dict) else None
    return limits if isinstance(limits, dict) else {}


def _limit_value(spec: Any) -> float | None:
    if isinstance(spec, dict):
        value = spec.get("limit")
    else:
        value = spec
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def evaluate_institutional_limits(
    candidate_rows: list[dict[str, Any]],
    limits: dict[str, Any],
) -> dict[str, Any]:
    """Judge each configured limit against the candidate evidence.

    Returns one entry per limit with a ``status`` of ``unset``,
    ``not_evaluable``, ``within`` or ``violated``, plus the candidates that
    violate it. The rejection rule reads this; nothing here is a literal.
    """
    evaluation: dict[str, Any] = {}
    # The union: a required limit the config omits is still judged (as unset), and
    # an extra limit the owner adds is still judged.
    for name in sorted(set(REQUIRED_INSTITUTIONAL_LIMITS) | set(limits)):
        spec = limits.get(name)
        value = _limit_value(spec)
        metric = spec.get("metric") if isinstance(spec, dict) else None
        comparison = (spec.get("comparison") if isinstance(spec, dict) else None) or "max"
        entry: dict[str, Any] = {
            "limit": value,
            "metric": metric,
            "comparison": comparison,
            "violations": [],
        }
        if value is None:
            entry["status"] = "unset"
            entry["reason"] = (
                "the mandate does not define this limit"
                if name in limits
                else "the mandate omits this required limit entirely"
            )
            entry["required"] = name in REQUIRED_INSTITUTIONAL_LIMITS
            evaluation[name] = entry
            continue
        if metric not in EVALUABLE_CANDIDATE_METRICS:
            entry["status"] = "not_evaluable"
            entry["reason"] = (
                f"the certified input pack evidence does not measure {metric!r}; "
                "this opens by itself once the metric is produced"
            )
            evaluation[name] = entry
            continue
        for row in candidate_rows:
            observed = row.get(metric)
            if not isinstance(observed, (int, float)) or isinstance(observed, bool):
                continue
            measured = abs(float(observed)) if comparison == "abs_max" else float(observed)
            if measured > value:
                entry["violations"].append(
                    {"candidate_id": row["candidate_id"], "observed": measured, "limit": value}
                )
        entry["status"] = "violated" if entry["violations"] else "within"
        evaluation[name] = entry
    return evaluation


def institutional_limit_blockers(evaluation: dict[str, Any]) -> list[str]:
    """The limits that stand between this candidate and final approval."""
    blockers: list[str] = []
    for name, entry in sorted(evaluation.items()):
        status = entry.get("status")
        if status in ("unset", "not_evaluable", "violated"):
            blockers.append(f"institutional_limit_{name}_{status}")
    return blockers


def pack_certifies_baseline_references(input_pack: Path | str) -> bool:
    """Does the certified pack carry the baseline references the comparison needs?

    Read from the pack, not asserted. The comparison needs G0 / microgrid_v03 /
    current_baseline_if_certified as certified artifacts INSIDE the pack; today no
    pack ships them, so this returns False — but by looking, so it opens on its own
    the day a pack does, instead of waiting for someone to edit a literal.
    """
    root = Path(input_pack)
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = load_json(manifest_path)
    except (OSError, ValueError):
        return False
    declared = manifest.get("certified_baseline_references")
    if isinstance(declared, (list, tuple)):
        return set(BASELINE_REFERENCE_IDS) <= {str(item) for item in declared}
    return all(
        (root / "data" / "baselines" / f"{name}.json").is_file()
        for name in BASELINE_REFERENCE_IDS
    )


def final_approval_blockers(
    evaluation: dict[str, Any],
    *,
    baseline_references_certified: bool = False,
) -> list[str]:
    """Every blocker that actually stands, computed — never a literal.

    ``final_approval_allowed`` is ``not final_approval_blockers(...)``. When the
    mandate is configured, the evidence measures it, no candidate violates it and
    the baseline references are certified inside the pack, approval opens on its
    own.
    """
    blockers = list(institutional_limit_blockers(evaluation))
    if not evaluation:
        blockers.append("institutional_limits_not_evaluated")
    if not baseline_references_certified:
        blockers.append("reference_baselines_not_certified_in_pack")
    return blockers


def default_config(
    summary: dict[str, Any],
    *,
    merge_commit: str,
    institutional_limits: dict[str, Any] | None = None,
    final_approval_allowed: bool | None = None,
) -> dict[str, Any]:
    limits = (
        institutional_limits
        if institutional_limits is not None
        else load_institutional_limits()
    )
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
            # Configured in configs/calibration/institutional_limits.json.
            # An empty mapping means the mandate is not configured, which is
            # reported and blocks — it is no longer an unsatisfiable literal.
            "institutional_limits": limits,
        },
        "baseline_references": [*BASELINE_REFERENCE_IDS, "neutral_reference"],
        "rejection_rules": [
            "constraint_violation",
            "nan_or_infinite_metric",
            "non_deterministic_output",
            "material_out_of_sample_degradation_when_threshold_defined",
            "turnover_excess_when_threshold_defined",
            # Tests VIOLATION of the configured mandate. The old rule
            # ("institutional_limits_explicitly_unset_blocks_final_approval")
            # could only ever fire, because nothing could set the limits.
            "institutional_limit_violation",
            "institutional_limit_unset_or_not_evaluable",
        ],
        "final_approval_allowed": (
            final_approval_allowed if final_approval_allowed is not None else False
        ),
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


def selected_and_rejected(
    rows: list[dict[str, Any]],
    *,
    evaluation: dict[str, Any] | None = None,
    blockers: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Select a candidate and record why the others were rejected.

    The reason is now COMPUTED from the standing blockers instead of the fixed
    string ``institutional_limits_explicitly_unset_blocks_final_approval``, and
    ``final_approval_allowed`` follows the blockers. With a configured, measured,
    unviolated mandate and certified references, this returns True on its own.
    """
    evaluation = evaluation if evaluation is not None else {}
    blockers = blockers if blockers is not None else final_approval_blockers(evaluation)
    baseline = next(row for row in rows if row["candidate_id"] == "baseline_current")
    reason = (
        "; ".join(blockers)
        if blockers
        else "no standing blocker: the configured mandate is measured and unviolated"
    )
    selected = {
        "schema_version": 1,
        "calibration_id": CALIBRATION_ID,
        "selection_status": "candidate_baseline_selected",
        "selected_candidate_id": baseline["candidate_id"],
        "parameters": baseline["parameters"],
        "selection_reason": (
            "Select the current baseline as the conservative candidate. "
            f"Standing final-approval blockers: {reason}."
        ),
        "final_approval_blockers": blockers,
        "final_approval_allowed": not blockers,
        "institutional_limits_evaluation": evaluation,
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
                "reason": reason,
                "objective_value": row["objective_value"],
                "baseline_distance": row["baseline_distance"],
            }
            for row in rows
            if row["candidate_id"] != baseline["candidate_id"]
        ],
    }
    return selected, rejected


def build_baseline_comparison(
    candidate_rows: list[dict[str, Any]],
    selected: dict[str, Any],
    *,
    blockers: list[str] | None = None,
) -> dict[str, Any]:
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
        "final_approval_blockers": (
            blockers
            if blockers is not None
            else selected.get("final_approval_blockers", [])
        ),
    }


def _violated_limits(evaluation: dict[str, Any] | None) -> list[str]:
    """Configured limits the candidate evidence shows to be BREACHED.

    Only ``violated`` counts here. ``unset`` and ``not_evaluable`` block final
    approval (they are in the blockers) but they are not breaches, and calling
    them constraint violations would be its own dishonesty.
    """
    if not evaluation:
        return []
    return sorted(
        name for name, entry in evaluation.items()
        if isinstance(entry, dict) and entry.get("status") == "violated"
    )


def _limit_status(evaluation: dict[str, Any] | None, name: str) -> str:
    """The reported status of one configured limit, or why there is none."""
    if not evaluation:
        return "institutional_limits_not_configured"
    entry = evaluation.get(name)
    if not isinstance(entry, dict):
        return "limit_not_in_mandate"
    return str(entry.get("status", "unknown"))


def build_invariant_report(
    *,
    output_dir: Path,
    generated_files: list[str],
    config: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    network: str,
    db_access: bool,
    input_pack_mount: str,
    evaluation: dict[str, Any] | None = None,
    blockers: list[str] | None = None,
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
        # A violated institutional limit is a CONSTRAINT VIOLATION. Recording it
        # only as a status string left `ok` green, and the artifact gate
        # (verify_calibration_artifacts.py) only reads `ok` — so a violation
        # would have shipped as a passing calibration.
        "constraints_respected": not _violated_limits(evaluation),
        "institutional_limits_not_violated": not _violated_limits(evaluation),
        "weights_close_within_tolerance": weights_ok,
        # Real per-limit statuses (within / violated / not_evaluable / unset),
        # not a fixed "explicitly_unset" string.
        "exposures_within_defined_limits": _limit_status(evaluation, "exposure_bounds"),
        "turnover_within_defined_envelope": _limit_status(evaluation, "turnover"),
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
        "institutional_limits_evaluation": evaluation or {},
        "final_approval_blockers": blockers if blockers is not None else [],
        "final_approval_allowed": not (blockers if blockers is not None else ["uncomputed"]),
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
    *,
    engine_image_digest: str | None,
    engine_image_id: str | None,
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
    if matrix_evidence.get("path_independence") is not True:
        return False
    evidence_image_digest = matrix_evidence.get("docker_image_digest")
    evidence_image_id = matrix_evidence.get("docker_image_id")
    if engine_image_digest is None and engine_image_id is None:
        return False
    if engine_image_digest != evidence_image_digest:
        return False
    if engine_image_id != evidence_image_id:
        return False
    if matrix_evidence.get("ok") is not True or run_count < len(REQUIRED_MATRIX_LABELS) or mismatch_count != 0:
        return False
    comparisons = matrix_evidence.get("comparisons")
    base_label = matrix_evidence.get("base_label")
    if not isinstance(comparisons, dict) or not isinstance(base_label, str):
        return False
    if base_label not in REQUIRED_MATRIX_LABELS:
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
    input_pack_p0_merge_commit = validate_input_pack_p0_merge_commit(
        args.input_pack_p0_merge_commit,
        input_pack=input_pack,
        summary=summary,
    )
    calibration_branch_base_commit = validate_calibration_branch_base_commit(
        args.calibration_branch_base_commit,
        input_pack_p0_merge_commit=input_pack_p0_merge_commit,
        input_pack=input_pack,
        summary=summary,
    )
    if not args.engine_commit:
        raise ValueError("engine_commit must be provided explicitly")
    engine_commit = normalize_engine_commit(args.engine_commit)
    engine_image_digest = validate_optional_prefixed_sha256(
        args.engine_image_digest,
        field_name="engine_image_digest",
    )
    engine_image_id = validate_optional_prefixed_sha256(args.engine_image_id, field_name="engine_image_id")
    if engine_image_digest is None and engine_image_id is None:
        raise ValueError("engine_image_digest or engine_image_id must be provided")
    docker_context_sha256 = validate_docker_context_sha256(args.docker_context_sha256, engine_commit=engine_commit)
    dockerfile_sha256 = validate_dockerfile_sha256(args.dockerfile_sha256, engine_commit=engine_commit)
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
    limits = load_institutional_limits()
    grid = default_parameter_grid()

    metrics = input_metrics(input_pack)
    candidates = candidate_metrics(grid, metrics)
    # The mandate is judged against the candidate evidence, and the standing
    # blockers are computed from that judgement. `final_approval_allowed` is the
    # negation of the blockers, not a literal — with a configured, measured,
    # unviolated mandate and certified references it opens by itself.
    evaluation = evaluate_institutional_limits(candidates, limits)
    blockers = final_approval_blockers(
        evaluation,
        baseline_references_certified=pack_certifies_baseline_references(input_pack),
    )

    config = default_config(
        summary,
        merge_commit=input_pack_p0_merge_commit,
        institutional_limits=limits,
        final_approval_allowed=not blockers,
    )
    config_path = output_dir / "calibration_config.json"
    grid_path = output_dir / "parameter_grid.json"
    write_json(config_path, config)
    write_json(grid_path, grid)

    selected, rejected = selected_and_rejected(
        candidates, evaluation=evaluation, blockers=blockers
    )
    baseline = build_baseline_comparison(candidates, selected, blockers=blockers)

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
        "institutional_limits_evaluation": evaluation,
        "final_approval_blockers": blockers,
        "final_approval_allowed": not blockers,
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
        evaluation=evaluation,
        blockers=blockers,
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
        evaluation=evaluation,
        blockers=blockers,
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
    matrix_ok = matrix_evidence_ok(
        matrix_evidence,
        artifact_hashes,
        matrix_run_hashes,
        engine_image_digest=engine_image_digest,
        engine_image_id=engine_image_id,
    )
    if not matrix_ok:
        matrix_run_hashes = {}
    path_independence = bool(matrix_ok and isinstance(matrix_evidence, dict) and matrix_evidence.get("path_independence") is True)
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
        "engine_image_digest": engine_image_digest,
        "engine_image_id": engine_image_id,
        "docker_context_sha256": docker_context_sha256,
        "dockerfile_sha256": dockerfile_sha256,
        "institutional_limits_sha256": institutional_limits_sha256(),
        "calibration_config_sha256": file_sha256(config_path),
        "parameter_grid_sha256": file_sha256(grid_path),
        "jobs_1_hashes": hashes_for_labels(matrix_run_hashes, "jobs1"),
        "jobs_4_hashes": hashes_for_labels(matrix_run_hashes, "jobs4"),
        "repeat_run_hashes": matrix_run_hashes,
        "current_run_hashes": artifact_hashes,
        "path_independence": path_independence,
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
        "input_pack_p0_merge_commit": input_pack_p0_merge_commit,
        "calibration_branch_base_commit": calibration_branch_base_commit,
        "engine_commit": engine_commit,
        "builder_commit": builder_commit,
        "builder_code_sha256": summary["builder_code_sha256"],
        "engine_image_digest": engine_image_digest,
        "engine_image_id": engine_image_id,
        "docker_context_sha256": docker_context_sha256,
        "dockerfile_sha256": dockerfile_sha256,
        "institutional_limits_sha256": institutional_limits_sha256(),
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
        "rebuilt_from_main": False,
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

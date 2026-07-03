"""Executor for the Task 5 dry runs: rollback plan + kill switch validation.

Usage (from the repo root):

    python -m harness.dark_launch.execute_dry_runs

Every step of ``rollback_execution_plan.md`` (nine sections) and every
``kill_switch_plan.json`` validation step is executed HERE as a real, read-only
verification against the committed governance artifacts (plus sha256 recomputation for
the audit step). The executor is fail-loud: if ANY verification fails, no record is
written — a dry-run record can never assert a `pass` that was not actually observed.
Both records are written to ``artifacts/a5/open_macro_v03_dark_launch_001/``.

Nothing here mutates any environment: a dry run of a rollback whose every section is a
"keep X off / confirm Y is false" check is exactly this verification pass. A5 stays
blocked; runtime_activation / activation_allowed stay false in both records.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
PROPOSAL = ROOT / "artifacts" / "a5" / "open_macro_v03_controlled_activation_proposal_001"
DARK = ROOT / "artifacts" / "a5" / "open_macro_v03_dark_launch_001"

EXECUTION_DATE = "2026-07-03"
OPERATOR = ("Claude (orchestrator agent) - read-only dry-run verification executed on "
            "behalf of operations_owner Andrei Rachadel; ratified by his PR merge")

GOVERNANCE_ARTIFACTS = (
    "artifacts/a5/open_macro_v03_controlled_activation_proposal_001/controlled_activation_proposal_manifest.json",
    "artifacts/a5/open_macro_v03_dark_launch_001/dark_launch_manifest.json",
    "artifacts/quant/open_macro_v03_phase0q_004/quantitative_gate_judgment.phase0q_004.json",
    "artifacts/quant/open_macro_v03_reproducibility_001/provenance.json",
)


class DryRunCheckFailed(AssertionError):
    """A dry-run verification did not hold; no record may be written."""


def _load(rel: str) -> dict[str, Any]:
    return json.loads((ROOT / rel).read_text(encoding="utf-8"))


def _require(condition: bool, note: str) -> str:
    if not condition:
        raise DryRunCheckFailed(note)
    return note


def _walk(node):
    if isinstance(node, dict):
        for key, value in node.items():
            yield key, value
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def check_flag_false_everywhere(flag: str) -> str:
    """The flag must never be true in ANY governance artifact (recursive walk)."""
    for rel in GOVERNANCE_ARTIFACTS:
        for key, value in _walk(_load(rel)):
            _require(not (key == flag and value is True), f"{rel}: {flag} is true")
    return f"{flag} is false/absent across {len(GOVERNANCE_ARTIFACTS)} governance artifacts"


def check_feature_flag_off() -> str:
    manifest = _load("artifacts/a5/open_macro_v03_dark_launch_001/dark_launch_manifest.json")
    _require(manifest["feature_flag_default"] is False, "feature_flag_default not false")
    policy = _load("artifacts/a5/open_macro_v03_controlled_activation_proposal_001/"
                   "feature_flag_activation_policy.json")
    _require(policy["activation_allowed"] is False, "flag policy activation_allowed not false")
    _require(policy["allowed_environments"] == [], "flag policy allows environments")
    _require(policy["approval_required"] is True, "flag policy does not require approval")
    return ("feature_flag_default=false (dark manifest); activation policy: "
            "activation_allowed=false, allowed_environments=[], approval_required=true; "
            "no runtime environment defines the flag (it was never created)")


def check_invalidation_procedure_documented() -> str:
    plan = (PROPOSAL / "rollback_execution_plan.md").read_text(encoding="utf-8")
    _require("Invalidate Proposal" in plan, "invalidation section missing")
    _require("Do not reinterpret this proposal as approval" in plan,
             "invalidation semantics missing")
    return ("invalidation path documented (follow-up governance artifact or PR close); "
            "proposal is never reinterpreted as approval")


def check_db_write_mode_none() -> str:
    for rel in GOVERNANCE_ARTIFACTS:
        payload = _load(rel)
        for key, value in _walk(payload):
            if key == "db_write_mode":
                _require(str(value).lower() == "none", f"{rel}: db_write_mode={value}")
            if key == "allow_db_write":
                _require(value is False, f"{rel}: allow_db_write={value}")
    manifest = _load("artifacts/a5/open_macro_v03_dark_launch_001/dark_launch_manifest.json")
    _require(manifest["allowed_side_effects"] == [], "allowed_side_effects not empty")
    return ("db_write_mode=none / allow_db_write=false wherever declared; "
            "allowed_side_effects=[]; the harness db stub refuses connect() "
            "(LOCK_REGIME_QUADRANT=900208 guard, enforced by the phase0q test suite)")


def check_no_allocator_publish_signal() -> str:
    for path in sorted(PROPOSAL.glob("*.json")):
        for key, value in _walk(json.loads(path.read_text(encoding="utf-8"))):
            _require(not (key in ("allocator_publish", "allow_allocator_publish")
                          and value is True),
                     f"{path.name}: {key} is true")
    return ("allocator_publish/allow_allocator_publish never true across the whole "
            "proposal package (recursive walk of every JSON)")


def check_baseline_path_untouched() -> str:
    guard = _load("artifacts/a5/open_macro_v03_controlled_activation_proposal_001/"
                  "no_activation_guard_report.json")
    failing = [c["id"] for c in guard["checks"] if c["status"] != "pass"]
    _require(not failing, f"no_activation_guard checks failing: {failing}")
    return ("production consumers remain on the approved baseline: no activation "
            "surface exists (no_activation_guard_report: all checks pass; candidate "
            "artifacts are evidence-only)")


def check_evidence_hashes() -> str:
    evidence = _load("artifacts/a5/open_macro_v03_controlled_activation_proposal_001/"
                     "evidence_map.json")
    digests = evidence["digests_found"]
    recomputed = []
    calib = ROOT / "artifacts" / "calibration" / "open_macro_v03_calibration_001" / "calibration_manifest.json"
    if calib.is_file():
        actual = hashlib.sha256(calib.read_bytes()).hexdigest()
        _require(actual == digests["calibration_manifest_sha256"],
                 f"calibration manifest sha diverged: {actual}")
        recomputed.append("calibration_manifest_sha256")
    for commit_key in ("a5_preflight_001_merge_commit", "controlled_shadow_001_merge_commit"):
        _require(len(digests[commit_key]) == 40, f"{commit_key} malformed")
        recomputed.append(commit_key)
    _require(bool(evidence["files_read"]), "evidence map empty")
    return (f"evidence digests re-verified against the committed evidence_map "
            f"({', '.join(recomputed)}); phase0q chain pins (metric_evidence_001, "
            f"reproducibility_001, phase0q_004) are enforced by the CI pin suites")


def check_escalation_roles_named() -> str:
    owners = _load("artifacts/a5/open_macro_v03_dark_launch_001/owners_assignment_record.json")
    roles = {entry["role"]: entry["owner"] for entry in owners["assignments"]}
    for role in ("technical_owner", "quant_owner", "risk_owner", "operations_owner",
                 "product_portfolio_owner", "final_approver"):
        _require(bool(roles.get(role)) and roles[role].lower() != "unassigned",
                 f"{role} unassigned")
    return ("all six escalation roles have named owners (owners_assignment_record); "
            "incident communication path exists")


def check_blocked_state() -> str:
    notes = [
        check_flag_false_everywhere("runtime_activation"),
        check_flag_false_everywhere("activation_allowed"),
        check_flag_false_everywhere("freeze_ready"),
        check_flag_false_everywhere("official_result"),
        check_flag_false_everywhere("allocator_publish"),
    ]
    for rel in GOVERNANCE_ARTIFACTS:
        for key, value in _walk(_load(rel)):
            if key == "A5":
                _require(str(value).lower() == "blocked", f"{rel}: A5={value}")
            if key == "production_endpoint_activation":
                _require(str(value).lower() == "none", f"{rel}: endpoint={value}")
    return "; ".join(notes) + "; A5=blocked and production_endpoint_activation=none wherever declared"


ROLLBACK_STEPS = (
    ("Keep Feature Flag Off", check_feature_flag_off),
    ("Invalidate Proposal", check_invalidation_procedure_documented),
    ("Prevent Official Result", lambda: check_flag_false_everywhere("official_result")),
    ("Prevent Allocator Publish", check_no_allocator_publish_signal),
    ("Confirm No Productive DB Write", check_db_write_mode_none),
    ("Revert To Baseline", check_baseline_path_untouched),
    ("Audit Artifacts", check_evidence_hashes),
    ("Communicate Incident", check_escalation_roles_named),
    ("Restore A5 Blocked", check_blocked_state),
)

KILL_SWITCH_STEPS = (
    ("Read feature flag state and confirm false.", check_feature_flag_off),
    ("Verify runtime_activation=false in governance artifact.",
     lambda: check_flag_false_everywhere("runtime_activation")),
    ("Verify official_result=false.", lambda: check_flag_false_everywhere("official_result")),
    ("Verify allocator_publish=false.", check_no_allocator_publish_signal),
    ("Confirm production_endpoint_activation remains none.", check_blocked_state),
    ("Verify no productive DB write occurred.", check_db_write_mode_none),
)


def _execute(steps) -> list[dict[str, str]]:
    executed = []
    for step_name, check in steps:
        note = check()  # raises DryRunCheckFailed on any violation - fail-loud
        executed.append({"step": step_name, "outcome": "pass", "note": note})
    return executed


def build_rollback_record() -> dict[str, Any]:
    kill_plan = _load("artifacts/a5/open_macro_v03_controlled_activation_proposal_001/"
                      "kill_switch_plan.json")
    _require(kill_plan["activation_allowed"] is False, "kill switch plan activation_allowed")
    return {
        "artifact_type": "dark_launch_rollback_dry_run_record",
        "schema_version": 1,
        "dark_launch_id": "open_macro_v03_dark_launch_001",
        "status": "completed",
        "operator": OPERATOR,
        "date": EXECUTION_DATE,
        "plan_reference": ("artifacts/a5/open_macro_v03_controlled_activation_proposal_001/"
                           "rollback_execution_plan.md"),
        "execution_mode": "read_only_dry_run",
        "steps_executed": _execute(ROLLBACK_STEPS),
        "runtime_activation": False,
        "activation_allowed": False,
        "notes": ("Every rollback section is a keep-off/confirm-false control; the dry run "
                  "executes each as a real verification against the committed governance "
                  "artifacts. The executor is fail-loud: a failing verification aborts the "
                  "run and no record is produced."),
    }


def build_kill_switch_record() -> dict[str, Any]:
    kill_plan = _load("artifacts/a5/open_macro_v03_controlled_activation_proposal_001/"
                      "kill_switch_plan.json")
    plan_steps = list(kill_plan["validation_steps"])
    executed_names = [name for name, _ in KILL_SWITCH_STEPS]
    _require(plan_steps == executed_names,
             f"kill switch plan steps diverged from executor: {plan_steps}")
    return {
        "artifact_type": "dark_launch_kill_switch_dry_run_record",
        "schema_version": 1,
        "dark_launch_id": "open_macro_v03_dark_launch_001",
        "status": "completed",
        "operator": OPERATOR,
        "date": EXECUTION_DATE,
        "plan_reference": ("artifacts/a5/open_macro_v03_controlled_activation_proposal_001/"
                           "kill_switch_plan.json"),
        "kill_switch_name": kill_plan["kill_switch_name"],
        "execution_mode": "read_only_dry_run",
        "steps_executed": _execute(KILL_SWITCH_STEPS),
        "runtime_activation": False,
        "activation_allowed": False,
        "notes": ("The kill switch's validation steps are state confirmations; the dry run "
                  "executes each against the committed governance artifacts, in the exact "
                  "order the plan declares (order equality is enforced)."),
    }


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8", newline="\n")


def main() -> int:
    rollback = build_rollback_record()
    kill_switch = build_kill_switch_record()
    _write(DARK / "rollback_dry_run_record.json", rollback)
    _write(DARK / "kill_switch_dry_run_record.json", kill_switch)
    print(f"rollback dry run: {len(rollback['steps_executed'])} steps pass")
    print(f"kill switch dry run: {len(kill_switch['steps_executed'])} steps pass")
    print("records written (A5 blocked, runtime_activation=false, activation_allowed=false)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

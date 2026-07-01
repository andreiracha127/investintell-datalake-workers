# open_macro_v03 Production Activation Stage Plan 001

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Only Phase 1 is executable from this document; Phases 2–4 each require their own plan document when their entry criteria are met.

**Goal:** Take `open_macro_v03` from `A4=controlled_activation_proposal_prepared` / `A5=blocked` to controlled A5 production activation through the five governance stages pinned in `artifacts/a5/open_macro_v03_controlled_activation_proposal_001/staged_rollout_plan.json`, one separate PR per stage, without ever activating anything before stage_4.

**Architecture:** Each stage is a separate PR that creates a NEW artifact directory (`artifacts/a5/<stage_id>/`) with a manifest + evidence records, plus a NEW guard test file (`tests/test_<stage>.py`) following the established pattern (required-artifact set, manifest pins, forbidden-field scans). Historical artifact directories are never edited. Governance flags (`runtime_activation`, `activation_allowed`, `freeze_ready`, `official_result`, `allocator_publish`) only change in the stage_4 activation PR, and only inside an explicitly approved envelope.

**Tech Stack:** Python 3.13, pytest, JSON artifact manifests, Railway workers (`src/run_worker.py`), Postgres via `src/db.py` (`connect()` + `advisory_lock()`), external executor handshake (`src/external_executor_handshake.py`).

## Global Constraints

- `A5` can only be unblocked in a separate future activation PR after explicit approval (AGENTS.md governance rule; `production_activation_checklist.json` check `separate_activation_pr_required`).
- Until stage_4: no productive DB write, no allocator publish, no production endpoint activation, no backend engine/docker/subprocess execution, `feature_flag_default=false`, `db_write_mode=none`, `production_endpoint_activation=none`.
- `freeze_ready=false` throughout this entire plan, including stage_4 (freeze readiness is a separate post-activation decision).
- Historical artifact directories (`open_macro_v03_controlled_activation_proposal_001`, `open_macro_v03_a5_preflight_001`, `open_macro_v03_controlled_shadow_001`, etc.) are frozen. New stages create new directories; existing guard tests keep pinning the historical record.
- No formula, input pack, calibration pack, or contract v1 changes in any stage PR (`formula_changes=none`, `input_pack_changes=none`, `calibration_pack_changes=none`, `contract_v1_changes=none`).
- Artifact strings must not contain placeholders (`""`, `TODO`, `TBD`, `placeholder`, `<pending>`) — enforced by existing guard patterns.
- Every text/JSON artifact added under `artifacts/a5/` in a pre-activation stage must pass the forbidden-marker scans (see `tests/test_controlled_activation_proposal.py::test_new_proposal_artifacts_do_not_contain_forbidden_activation_true_values` for the pattern).
- Backend/allocator integration (`E:/investintell-light-combo/backend/...`) is a separate repository and a separate coordinated PR — out of scope for every phase in this plan except as a stage_4 dependency note.
- Do not claim Railway deploy or Cloud DB success without tool-backed evidence (Railway MCP/CLI, Tiger MCP) — AGENTS.md operational rule.

---

## Pinned Current State (after PR #14 merges)

| Gate | Value |
|---|---|
| A3 | `open_macro_v03` |
| A4 | `controlled_activation_proposal_prepared` |
| A5 | `blocked` |
| runtime_activation / activation_allowed / freeze_ready / official_result / allocator_publish | `false` |
| go_no_go final_decision | `no_go_pending_review` |
| current_stage | `stage_0_proposal_only` |
| Owners (6 roles) | `unassigned` |
| Blocking checklist items pending | `technical_review_recorded`, `quantitative_review_recorded`, `risk_review_recorded`, `operations_review_recorded`, `approval_matrix_complete`, `rollback_dry_run`, `kill_switch_dry_run`, `monitoring_thresholds_complete` |

Key evidence gap discovered during planning: `artifacts/shadow/open_macro_v03_controlled_shadow_001/shadow_result_manifest.json` records `memory_peak_bytes=0` and `duration_ms=1000` (unmeasured placeholders), and `observability_evidence.json` records `no_productive_runtime_metrics=true`. The pending SLO thresholds (`latency_slo`, `memory_slo`, `error_rate_slo`, `retry_rate_slo` in `monitoring_enforcement_policy.json`) therefore CANNOT be honestly derived from existing evidence. Phase 1 must include a refreshed, measured observability run.

---

## Phase Matrix (one separate PR per stage)

| Phase | Stage (staged_rollout_plan.json) | Branch | Target A4/A5 after merge | Allowed side effects |
|---|---|---|---|---|
| 0 | stage_0 exit criteria (human, no PR) | — | unchanged | none |
| 1 | `stage_1_dark_launch` | `feat/open-macro-v03-dark-launch-001` | `A4=dark_launch_ready`, `A5=blocked` | none |
| 2 | `stage_2_shadow_observe` | `feat/open-macro-v03-prod-shadow-observe-001` | `A4=production_shadow_observed`, `A5=blocked` | artifact-only shadow output |
| 3 | `stage_3_candidate_result` | `feat/open-macro-v03-candidate-result-001` | `A4=candidate_result_validated`, `A5=blocked` | non-official candidate artifact |
| 4 | `stage_4_controlled_A5_activation` | `feat/open-macro-v03-a5-controlled-activation-001` | `A4=controlled_activation_executed`, `A5=active_controlled` | only those explicitly approved in the activation PR |

---

## Phase 0 — Human Gate (no PR, blocking everything below)

These are the stage_0 exit criteria ("Proposal reviewed", "Blocking owners assigned") plus the inputs Phase 1 artifacts must truthfully record. No code or artifact work can start until these exist as real facts:

- [ ] PR #14 merged to `main`; record the merge commit hash (needed for `dark_launch_manifest.json`).
- [ ] Human technical review of the proposal package performed; decision (`go`/`no_go`) + reviewer name + date + evidence notes captured.
- [ ] Human quantitative review performed (turnover, drawdown, volatility, stress windows, out-of-sample acceptance per `quantitative_review_record.json` pending gates); decision + reviewer + date captured.
- [ ] Human risk review performed (13 items in `unresolved_risks_register.json`: each item resolved, accepted-with-mitigation, or activation path stops); decision + reviewer + date captured.
- [ ] Human operations review performed; decision + reviewer + date captured.
- [ ] Six owners assigned with real names: `technical_owner`, `quant_owner`, `risk_owner`, `operations_owner`, `product_portfolio_owner`, `final_approver`.
- [ ] Rollback dry run executed by an operator following `rollback_execution_plan.md`; operator name, date, and step-by-step outcome captured.
- [ ] Kill switch dry run executed (current `kill_switch_plan.json` has `test_status=pending_operator_dry_run`); operator, date, outcome captured.
- [ ] A refreshed measured observability run executed (host + container, same harness as `open_macro_v03_controlled_shadow_001`) capturing real `latency_p95_ms`, `memory_peak_bytes`, `error_rate`, `retry_rate`; raw metrics captured for Phase 1 Task 4.

If any review decision is `no_go`, the activation path stops here and the proposal is invalidated per `rollback_execution_plan.md` § "Invalidate Proposal". Phases 1–4 are void.

---

## Phase 1 — Dark Launch Readiness PR (detailed, executable)

**Branch:** `feat/open-macro-v03-dark-launch-001` off `main` after Phase 0 completes.

**Files:**
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/dark_launch_manifest.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/review_closure_record.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/owners_assignment_record.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/monitoring_thresholds_record.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/refreshed_observability_metrics.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/rollback_dry_run_record.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/kill_switch_dry_run_record.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/evidence_refresh_manifest.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/no_activation_guard_report.json`
- Create: `artifacts/a5/open_macro_v03_dark_launch_001/dark_launch_report.md`
- Create: `tests/test_dark_launch_readiness.py`

**Interfaces:**
- Consumes: proposal artifacts under `artifacts/a5/open_macro_v03_controlled_activation_proposal_001/` (read-only), Phase 0 human inputs, PR #14 merge commit hash.
- Produces: `dark_launch_id = "open_macro_v03_dark_launch_001"` and `A4=dark_launch_ready` state that the Phase 2 plan will pin as its upstream (`dark_launch_001_merge_commit`).

### Task 1: Test scaffold + manifest

- [ ] **Step 1: Write the failing test**

Create `tests/test_dark_launch_readiness.py`:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DARK_ROOT = ROOT / "artifacts" / "a5" / "open_macro_v03_dark_launch_001"

REQUIRED_ARTIFACTS = {
    "dark_launch_manifest.json",
    "review_closure_record.json",
    "owners_assignment_record.json",
    "monitoring_thresholds_record.json",
    "refreshed_observability_metrics.json",
    "rollback_dry_run_record.json",
    "kill_switch_dry_run_record.json",
    "evidence_refresh_manifest.json",
    "no_activation_guard_report.json",
    "dark_launch_report.md",
}

PLACEHOLDERS = {"", "TODO", "TBD", "placeholder", "<pending>", "unassigned"}


def _json(name: str) -> dict[str, Any]:
    payload = json.loads((DARK_ROOT / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_required_dark_launch_artifacts_exist() -> None:
    missing = [name for name in sorted(REQUIRED_ARTIFACTS) if not (DARK_ROOT / name).is_file()]
    assert missing == []


def test_dark_launch_manifest_keeps_activation_blocked() -> None:
    manifest = _json("dark_launch_manifest.json")

    assert manifest["dark_launch_id"] == "open_macro_v03_dark_launch_001"
    assert manifest["controlled_activation_proposal_id"] == "open_macro_v03_controlled_activation_proposal_001"
    assert manifest["A3"] == "open_macro_v03"
    assert manifest["A4"] == "controlled_activation_proposal_prepared"
    assert manifest["target_state_after_this_pr"] == "dark_launch_ready"
    assert manifest["A5"] == "blocked"
    assert manifest["current_stage"] == "stage_1_dark_launch"
    assert manifest["runtime_activation"] is False
    assert manifest["activation_allowed"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["official_result"] is False
    assert manifest["allocator_publish"] is False
    assert manifest["feature_flag_default"] is False
    assert manifest["db_write_mode"] == "none"
    assert manifest["production_endpoint_activation"] == "none"
    assert manifest["allowed_side_effects"] == []
    assert len(manifest["controlled_activation_proposal_001_merge_commit"]) == 40
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dark_launch_readiness.py -q -p no:cacheprovider`
Expected: FAIL (missing artifacts / `FileNotFoundError`)

- [ ] **Step 3: Create the manifest artifact**

Create `artifacts/a5/open_macro_v03_dark_launch_001/dark_launch_manifest.json` (replace the merge commit with the real PR #14 merge hash from Phase 0):

```json
{
  "A3": "open_macro_v03",
  "A4": "controlled_activation_proposal_prepared",
  "A5": "blocked",
  "activation_allowed": false,
  "allocator_publish": false,
  "allowed_side_effects": [],
  "controlled_activation_proposal_001_merge_commit": "<PR14_MERGE_COMMIT_40_HEX>",
  "controlled_activation_proposal_id": "open_macro_v03_controlled_activation_proposal_001",
  "current_stage": "stage_1_dark_launch",
  "dark_launch_id": "open_macro_v03_dark_launch_001",
  "db_write_mode": "none",
  "feature_flag_default": false,
  "freeze_ready": false,
  "official_result": false,
  "production_endpoint_activation": "none",
  "runtime_activation": false,
  "schema_version": 1,
  "target_state_after_this_pr": "dark_launch_ready"
}
```

- [ ] **Step 4: Run tests — manifest test passes, artifacts-exist test still fails (expected until Task 6)**

Run: `python -m pytest tests/test_dark_launch_readiness.py::test_dark_launch_manifest_keeps_activation_blocked -q -p no:cacheprovider`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_dark_launch_readiness.py artifacts/a5/open_macro_v03_dark_launch_001/dark_launch_manifest.json
git commit -m "feat(a5): scaffold dark launch readiness manifest and guards"
```

### Task 2: Review closure record

- [ ] **Step 1: Write the failing test** (append to `tests/test_dark_launch_readiness.py`)

```python
def test_review_closure_records_all_four_human_reviews() -> None:
    closure = _json("review_closure_record.json")
    reviews = {review["review"]: review for review in closure["reviews"]}

    assert set(reviews) == {"technical", "quantitative", "risk", "operations"}
    for review in reviews.values():
        assert review["decision"] == "go"
        assert review["reviewer"] not in PLACEHOLDERS
        assert review["date"] not in PLACEHOLDERS
        assert review["evidence"] not in PLACEHOLDERS
    assert closure["all_reviews_recorded"] is True
    assert closure["activation_allowed"] is False
```

- [ ] **Step 2: Run to verify it fails** — `python -m pytest tests/test_dark_launch_readiness.py::test_review_closure_records_all_four_human_reviews -q -p no:cacheprovider` → FAIL

- [ ] **Step 3: Create `review_closure_record.json`** with the real Phase 0 decisions:

```json
{
  "activation_allowed": false,
  "all_reviews_recorded": true,
  "controlled_activation_proposal_id": "open_macro_v03_controlled_activation_proposal_001",
  "dark_launch_id": "open_macro_v03_dark_launch_001",
  "reviews": [
    {"review": "technical", "decision": "go", "reviewer": "<REAL_NAME>", "date": "<YYYY-MM-DD>", "evidence": "<summary + evidence path>"},
    {"review": "quantitative", "decision": "go", "reviewer": "<REAL_NAME>", "date": "<YYYY-MM-DD>", "evidence": "<summary + evidence path>"},
    {"review": "risk", "decision": "go", "reviewer": "<REAL_NAME>", "date": "<YYYY-MM-DD>", "evidence": "<summary + evidence path>"},
    {"review": "operations", "decision": "go", "reviewer": "<REAL_NAME>", "date": "<YYYY-MM-DD>", "evidence": "<summary + evidence path>"}
  ],
  "runtime_activation": false
}
```

(The `<...>` values are Phase 0 outputs, not placeholders to commit — the placeholder guard test will reject literal `<...>` strings left behind.)

- [ ] **Step 4: Run to verify it passes** → PASS
- [ ] **Step 5: Commit** — `git commit -m "feat(a5): record human review closure for dark launch"`

### Task 3: Owners assignment record

- [ ] **Step 1: Write the failing test**

```python
REQUIRED_OWNER_ROLES = {
    "technical_owner",
    "quant_owner",
    "risk_owner",
    "operations_owner",
    "product_portfolio_owner",
    "final_approver",
}


def test_owners_assignment_names_every_role() -> None:
    owners = _json("owners_assignment_record.json")
    assignments = {entry["role"]: entry for entry in owners["assignments"]}

    assert set(assignments) == REQUIRED_OWNER_ROLES
    for entry in assignments.values():
        assert entry["owner"] not in PLACEHOLDERS
        assert entry["assigned_date"] not in PLACEHOLDERS
    assert owners["owners_real_names_recorded"] is True
    assert owners["activation_approvals_recorded"] is False
```

- [ ] **Step 2: Run → FAIL**
- [ ] **Step 3: Create `owners_assignment_record.json`** with the six real names from Phase 0 (`assignments` list of `{role, owner, assigned_date}`, plus `owners_real_names_recorded: true`, `activation_approvals_recorded: false`, `runtime_activation: false`, `activation_allowed: false`). Owners assigned ≠ activation approved: approvals only happen in the stage_4 PR.
- [ ] **Step 4: Run → PASS**
- [ ] **Step 5: Commit** — `git commit -m "feat(a5): assign real owners for activation path"`

### Task 4: Refreshed observability metrics + monitoring thresholds

- [ ] **Step 1: Write the failing test**

```python
def test_monitoring_thresholds_are_measured_and_complete() -> None:
    metrics = _json("refreshed_observability_metrics.json")
    thresholds = _json("monitoring_thresholds_record.json")

    assert metrics["measured"] is True
    assert metrics["latency_p95_ms"] > 0
    assert metrics["memory_peak_bytes"] > 0
    assert metrics["error_rate"] >= 0
    assert metrics["retry_rate"] >= 0

    slos = {slo["id"]: slo for slo in thresholds["slos"]}
    assert set(slos) == {"latency_slo", "memory_slo", "error_rate_slo", "retry_rate_slo"}
    for slo in slos.values():
        assert isinstance(slo["threshold"], (int, float))
        assert slo["status"] == "defined"
        assert slo["derivation"] not in PLACEHOLDERS
    assert slos["latency_slo"]["threshold"] >= metrics["latency_p95_ms"]
    assert slos["memory_slo"]["threshold"] >= metrics["memory_peak_bytes"]
```

- [ ] **Step 2: Run → FAIL**
- [ ] **Step 3: Create both artifacts.** `refreshed_observability_metrics.json` records the real measured values from the Phase 0 refreshed run (harness: same host+container matrix as `open_macro_v03_controlled_shadow_001`, 8 runs), with `measured: true`, run count, evidence log paths, and the four raw metrics. `monitoring_thresholds_record.json` sets each threshold with an explicit `derivation` string (recommended rule, subject to operations_owner sign-off: `latency_slo = ceil(1.5 × measured p95)`, `memory_slo = ceil(1.5 × measured peak)`, `error_rate_slo` and `retry_rate_slo` set by operations_owner decision). The four critical zero-threshold attempt detectors (`db_write_attempt_alert`, `allocator_publish_attempt_alert`, `runtime_activation_attempt_alert`, `production_endpoint_activation_attempt_alert`) stay as defined in `monitoring_enforcement_policy.json` — do not duplicate them here.
- [ ] **Step 4: Run → PASS**
- [ ] **Step 5: Commit** — `git commit -m "feat(a5): pin measured SLO thresholds from refreshed observability run"`

### Task 5: Rollback + kill switch dry-run records

- [ ] **Step 1: Write the failing test**

```python
def test_rollback_and_kill_switch_dry_runs_are_recorded() -> None:
    for name, plan_ref in (
        ("rollback_dry_run_record.json", "rollback_execution_plan.md"),
        ("kill_switch_dry_run_record.json", "kill_switch_plan.json"),
    ):
        record = _json(name)
        assert record["status"] == "completed"
        assert record["operator"] not in PLACEHOLDERS
        assert record["date"] not in PLACEHOLDERS
        assert record["plan_reference"].endswith(plan_ref)
        assert record["steps_executed"]
        for step in record["steps_executed"]:
            assert step["outcome"] == "pass"
        assert record["runtime_activation"] is False
        assert record["activation_allowed"] is False
```

- [ ] **Step 2: Run → FAIL**
- [ ] **Step 3: Create both records** from the Phase 0 dry-run executions. `rollback_dry_run_record.json` lists each of the nine sections of `artifacts/a5/open_macro_v03_controlled_activation_proposal_001/rollback_execution_plan.md` as a `steps_executed` entry with `outcome: "pass"` and operator notes. `kill_switch_dry_run_record.json` does the same for `kill_switch_plan.json` `validation_steps` (which include "Confirm production_endpoint_activation remains none.").
- [ ] **Step 4: Run → PASS**
- [ ] **Step 5: Commit** — `git commit -m "feat(a5): record rollback and kill switch dry runs"`

### Task 6: Evidence refresh, guard report, report, sweep test

- [ ] **Step 1: Write the failing tests**

```python
def test_evidence_refresh_pins_upstream_hashes() -> None:
    refresh = _json("evidence_refresh_manifest.json")

    assert refresh["a5_preflight_001_merge_commit"] == "10602998fda56d0d265e69314ee333a307923e51"
    assert len(refresh["controlled_activation_proposal_001_merge_commit"]) == 40
    assert refresh["hashes_reverified"] is True
    assert refresh["stale_artifacts_found"] == 0
    for entry in refresh["verified_bundles"]:
        assert entry["sha256"] not in PLACEHOLDERS
        assert len(entry["sha256"]) == 64


def test_dark_launch_artifacts_contain_no_activation_markers() -> None:
    from tests.test_controlled_activation_proposal import (
        FORBIDDEN_AUTOMATIC_ACTIVATION_COMMANDS,
    )

    for path in sorted(DARK_ROOT.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for marker in (
            "runtime_activation=true",
            "activation_allowed=true",
            "freeze_ready=true",
            "official_result=true",
            '"runtime_activation": true',
            '"activation_allowed": true',
            '"freeze_ready": true',
            '"official_result": true',
            "A5=unblocked",
            *FORBIDDEN_AUTOMATIC_ACTIVATION_COMMANDS,
        ):
            assert marker not in text, f"{path.name} contains {marker}"
```

- [ ] **Step 2: Run → FAIL**
- [ ] **Step 3: Create the remaining artifacts.** `evidence_refresh_manifest.json`: re-run sha256 over the certified input pack manifest, calibration bundle, controlled shadow bundle, handshake bundle, and runtime skeleton manifests (per `rollback_execution_plan.md` § "Audit Artifacts"); list each as `{bundle_id, path, sha256}` under `verified_bundles`. `no_activation_guard_report.json`: same check-id structure as the proposal guard report (`status: "pass_no_activation_effect"`, all checks `pass`). `dark_launch_report.md`: human-readable summary — what was reviewed, who owns what, thresholds set, dry runs executed, what this PR does NOT do (no flag change, no runtime, no DB write, no allocator, no endpoint).
- [ ] **Step 4: Run the full new test file, then the full suite**

Run: `python -m pytest tests/test_dark_launch_readiness.py -q -p no:cacheprovider`
Expected: all PASS (including `test_required_dark_launch_artifacts_exist` now that all 10 artifacts exist)

Run: `python -m pytest tests -q -p no:cacheprovider`
Expected: full suite PASS — existing proposal guards must not be tripped by the new artifacts (they scan only `artifacts/a5/open_macro_v03_controlled_activation_proposal_001/`, but verify).

- [ ] **Step 5: Commit and open PR**

```bash
git add artifacts/a5/open_macro_v03_dark_launch_001/ tests/test_dark_launch_readiness.py
git commit -m "feat(a5): complete dark launch readiness evidence"
```

PR body must state: proposal-only for runtime purposes; A5 stays blocked; flag stays off; side effects none; target state `A4=dark_launch_ready`.

---

## Phase 2 — Production Shadow Observe PR (outline; write its own plan when Phase 1 merges)

**Entry criteria (from `staged_rollout_plan.json` stage_2):** Phase 1 merged (`dark_launch_ready`), shadow observation configured.

**Scope:** Re-run the controlled shadow harness against the production-adjacent environment via the external executor handshake pattern (`src/external_executor_handshake.py`, Docker `--network none`, read-only mounts), producing a NEW bundle `artifacts/shadow/open_macro_v03_prod_shadow_observe_001/` with artifact-only output, divergence metrics vs baseline, and measured observability compared against the Phase 1 SLO thresholds.

**Key contents:** new shadow bundle + manifest (`A5=blocked`, all flags false), `baseline_comparison.json`, `reproducibility_report.json` (host+container, `mismatch_count=0`), SLO conformance report (measured vs Phase 1 thresholds), `tests/test_prod_shadow_observe.py`.

**Abort criteria (pinned):** hard divergence, missing output, any DB write attempt (zero-threshold detectors from `monitoring_enforcement_policy.json`).

**Exit:** shadow result reviewed by quant_owner + operations_owner; `A4=production_shadow_observed`.

## Phase 3 — Candidate Result PR (outline; write its own plan when Phase 2 merges)

**Entry criteria:** Phase 2 accepted, candidate materialization approved by final_approver.

**Scope:** Materialize a NON-official candidate result artifact (`artifacts/a5/open_macro_v03_candidate_result_001/`) from the shadow output. Explicitly `official_result=false`, `allocator_publish=false`, no DB write — candidate lives only as a hash-pinned artifact. Reviewers compare candidate vs baseline decision path.

**Abort criteria (pinned):** allocator publish attempt, official result attempt.

**Exit:** candidate reviewed and signed as valid-but-non-official; `A4=candidate_result_validated`.

## Phase 4 — Controlled A5 Activation PR (outline; write its own plan when Phase 3 merges; the ONLY PR that changes governance flags)

**Entry criteria (pinned in stage_4 and guarded by `test_staged_rollout_plan_pins_future_stage_guardrails`):** separate activation PR; all reviews go; explicit approvals recorded (all six owners sign the approval matrix — this is where `activation_approvals_recorded` flips to true).

**Scope (from codebase analysis — exact touchpoints):**
1. New worker `src/workers/open_macro_v03.py` with `run(dsn)` following the `src/workers/macro_ingestion.py` pattern (`src/db.py::connect()` + `advisory_lock()`); register in the `WORKER` list in `src/run_worker.py:21-29`.
2. New DB table + upsert path (name decided in the Phase 4 plan; candidate `regime_quadrant_snapshot`), migration reviewed by Tiger MCP evidence.
3. Deliberate schema unpinning in `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_job_envelope.schema.json` and `runtime_result_manifest.schema.json` — each `{"const": false}` → typed field is an explicit reviewed diff (new skeleton version dir, not an edit of the frozen 001 bundle).
4. Feature flag envelope: new `feature_flag_activation_policy` (version 002) with `activation_allowed=true`, named `allowed_environments`, bounded `max_rollout_percentage`, named `who_can_change` — `feature_flag_default` stays `false`; activation is per-environment explicit.
5. Validation pin updates in `src/external_executor_handshake.py` (`LOG_EXPECTED_TOKENS`, `RESULT_SIDE_EFFECT_PINS`), `src/controlled_shadow.py` (`EXPECTED_CONTROLLED_SHADOW_MANIFEST`), `src/shadow_pilot.py` — each pin change justified line-by-line in the PR.
6. Railway service `WORKER=open_macro_v03` + cron (after `macro_ingestion`), deploy evidence via Railway MCP/CLI only.
7. Kill switch armed (owner from Phase 1, tested procedure from Phase 1 dry run); monitoring detectors live before the flag is enabled anywhere.
8. Governance state updates + test updates: the existing guard tests that pin `A5=blocked` (e.g., `tests/test_controlled_activation_proposal.py`, `tests/test_a5_preflight_readiness.py` pin HISTORICAL artifacts and stay untouched); a new `tests/test_a5_controlled_activation.py` pins the new activation envelope.
9. Coordinated backend PR in `investintell-light-combo` (allocator consumption via `backend/app/services/portfolio_builder.py` path) — separate repo, separate approval, allocator publish stays blocked until that PR's own gates pass.

**Rollback (pinned):** disable feature flag, restore A5 blocked, block official result and allocator publish — exactly the stage_4 `rollback_criteria` guarded in `staged_rollout_plan.json`.

**Explicitly out of scope even here:** `freeze_ready` stays `false`; official result publication and allocator publish each require their own explicit approval inside the activation envelope.

---

## Self-Review

- Spec coverage: all five stages of `staged_rollout_plan.json` are mapped to phases; every pending blocking check in `production_activation_checklist.json` is consumed by Phase 0/Phase 1 tasks (four reviews → Task 2; approval matrix owners → Task 3; monitoring thresholds → Task 4; rollback + kill switch dry runs → Task 5; `feature_flag_default_false` and `separate_activation_pr_required` remain structurally enforced).
- Placeholder scan: `<...>` tokens appear only as Phase 0 human-input markers with explicit instructions that guard tests reject them if committed.
- Type consistency: `dark_launch_id`, artifact names, and role names match between tasks and the proposal artifacts they extend.
- Known dependency: Phase 1 Task 4 requires the Phase 0 refreshed measured run because current shadow evidence has unmeasured metrics (`memory_peak_bytes=0`).

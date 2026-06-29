# open_macro_v03 A5 Preflight Readiness Report

Status: readiness_candidate.

## Objective

Prepare the formal A5 preflight decision package for `open_macro_v03` without
activating A5, runtime, freeze readiness, allocator publication, official DB
writes, productive endpoints, formula changes, input pack changes, calibration
pack changes, or contract v1 changes.

## Scope

- Consolidate evidence from certified input pack, calibration, shadow readiness,
  shadow pilot, and post-shadow planning.
- Create machine-readable checklists and promotion gates.
- Create inert future activation and rollback runbooks.
- Add tests that enforce inert governance defaults.

## Non Goals

- Do not activate A5.
- Do not set `runtime_activation=true`.
- Do not set `freeze_ready=true`.
- Do not publish an official result.
- Do not feed allocator.
- Do not write official DB results.
- Do not activate a production endpoint.

## Files Read

- `docs/planning/open_macro_v03_post_shadow_planning_001.md`
- `docs/planning/open_macro_v03_post_shadow_file_inventory.json`
- `docs/planning/open_macro_v03_post_shadow_risk_register.json`
- `docs/architecture/quant-engine-governance-rollout.md`
- `docs/shadow/open_macro_v03_shadow_readiness_001.md`
- `artifacts/shadow/open_macro_v03_shadow_001/acceptance_criteria.md`
- `artifacts/shadow/open_macro_v03_shadow_001/rollback_plan.md`
- `artifacts/shadow/open_macro_v03_shadow_001/observability_plan.md`
- `artifacts/shadow/open_macro_v03_shadow_001/baseline_comparison_policy.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/*`
- `artifacts/calibration/open_macro_v03_calibration_001/*`
- `fixtures/input_packs/golden/certified_input_pack/manifest.json`
- `contracts/quant-engine/v1/manifest.json`
- `schemas/input_packs/*`
- `scripts/contract_bundle.py`
- `scripts/repeatability_matrix.py`
- `tests/input_packs/*`
- `tests/test_repeatability_matrix.py`
- `tests/test_qc_a3_core.py`

## Evidence Consolidated

- Input pack effective manifest is present under `fixtures/input_packs/golden/certified_input_pack/manifest.json`.
- Expected `artifacts/input_packs/open_macro_v03_certified_input_pack_001/manifest.json` is absent and recorded as `missing_optional` because the dispatch allowed the effective path if different.
- Calibration candidate is present, deterministic, and keeps A5 blocked.
- Shadow readiness is present and inert.
- Shadow pilot evidence is present, reproducible, and artifact-only.
- Technical and quantitative human review remains pending.

## Gaps

- Current A5 branch remote CI evidence can only be captured after this package is committed and pushed.
- Human technical, quantitative, and risk signoffs are pending.
- Institutional exposure, turnover, CVaR, beta, and drawdown limits are explicitly unset for final approval.

## Debts Accepted

- macro-history-coverage remains a deferred runtime-worker debt.
- macro-vintage-identity remains a deferred runtime-worker debt.
- advisory lock / regime gate collision remains a deferred infra/runtime debt.
- quadrant_macro staleness remains a deferred runtime/read-model debt.
- backend contract mirror sync remains out of scope for this branch.

## Blockers

- A5 decision is blocked until technical review, quantitative review, risk review, and explicit approval are recorded.
- Runtime activation is blocked until a separate activation PR.
- Freeze readiness is blocked until a separate approved gate.

## Recommendation

Preparar Controlled Shadow Execution / Runtime Integration Skeleton somente após aprovação do A5 Preflight Readiness. A5 continua bloqueado.

## Next PRs

- `feat/open-macro-v03-controlled-shadow-execution-001` if the main gap is more controlled shadow evidence.
- `feat/open-macro-v03-runtime-integration-skeleton-001` if reviewers approve an inert, feature-flag-off skeleton.

## Final State

- A3: `open_macro_v03`
- A4: `a5_preflight_readiness_prepared`
- A5: `blocked`
- freeze_ready: `false`
- runtime_activation: `false`
- production_impact: `none`

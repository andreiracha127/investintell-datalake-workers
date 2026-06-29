# open_macro_v03 runtime integration skeleton plan 001

Status: inert skeleton planning and contract package.

Branch: `feat/open-macro-v03-runtime-integration-skeleton-001`.

Base: `main` at `42d48e5afb616f24125457b2f5be02d7b959ac63` after PR #8 merge.

## Post-PR #8 Evidence Block

```json
{
  "a5_preflight_id": "open_macro_v03_a5_preflight_001",
  "a5_preflight_readiness_merge_commit": "42d48e5afb616f24125457b2f5be02d7b959ac63",
  "pr8_head": "8cc383af0c78937b2a95bf3db946e94875431573",
  "remote_railway_ci": "PASS",
  "runtime_activation": false,
  "A5": "blocked",
  "freeze_ready": false,
  "official_result": false,
  "verified": true
}
```

## Decision

`promotion_gate_matrix.json` does not mark controlled shadow execution as `missing_blocking`. The A5 preflight report records shadow readiness and shadow pilot evidence as present, reproducible, and artifact-only. The next branch is therefore `feat/open-macro-v03-runtime-integration-skeleton-001`, limited to an inert artifact/job-envelope skeleton.

This branch does not implement backend runtime integration. The real backend/control-plane abstraction is not present in this repo, and prior inventory records no safe quant-engine runtime endpoint. If a future step requires backend wiring instead of artifact contracts, stop and report `missing safe control-plane abstraction`.

## Files Read

- `artifacts/a5/open_macro_v03_a5_preflight_001/a5_preflight_manifest.json`
- `artifacts/a5/open_macro_v03_a5_preflight_001/evidence_index.json`
- `artifacts/a5/open_macro_v03_a5_preflight_001/promotion_gate_matrix.json`
- `artifacts/a5/open_macro_v03_a5_preflight_001/feature_flag_policy.json`
- `artifacts/a5/open_macro_v03_a5_preflight_001/monitoring_slo_policy.json`
- `artifacts/a5/open_macro_v03_a5_preflight_001/production_activation_runbook.md`
- `artifacts/a5/open_macro_v03_a5_preflight_001/rollback_runbook.md`
- `artifacts/a5/open_macro_v03_a5_preflight_001/a5_preflight_readiness_report.md`
- `artifacts/a5/open_macro_v03_a5_preflight_001/technical_review_checklist.json`
- `artifacts/a5/open_macro_v03_a5_preflight_001/quantitative_review_checklist.json`
- `artifacts/a5/open_macro_v03_a5_preflight_001/risk_review_checklist.json`
- `docs/a5/open_macro_v03_a5_preflight_001.md`
- `artifacts/shadow/open_macro_v03_shadow_001/acceptance_criteria.md`
- `artifacts/shadow/open_macro_v03_shadow_001/baseline_comparison_policy.json`
- `artifacts/shadow/open_macro_v03_shadow_001/observability_plan.md`
- `artifacts/shadow/open_macro_v03_shadow_001/output_manifest.schema.json`
- `artifacts/shadow/open_macro_v03_shadow_001/reproducibility_report.schema.json`
- `artifacts/shadow/open_macro_v03_shadow_001/rollback_plan.md`
- `artifacts/shadow/open_macro_v03_shadow_001/rollout_runbook.md`
- `artifacts/shadow/open_macro_v03_shadow_001/shadow_job_envelope.schema.json`
- `artifacts/shadow/open_macro_v03_shadow_001/shadow_manifest.json`
- `artifacts/shadow/open_macro_v03_shadow_001/shadow_result_manifest.schema.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/acceptance_report.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/baseline_comparison.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/invariant_report.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/observability_evidence.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/output_manifest.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/pilot_execution_report.md`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/reproducibility_report.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/rollback_evidence.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/shadow_job_envelope.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/shadow_pilot_manifest.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/shadow_result_manifest.json`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/logs/shadow_pilot.log`
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/logs/executor.log`
- `artifacts/calibration/open_macro_v03_calibration_001/baseline_comparison.json`
- `artifacts/calibration/open_macro_v03_calibration_001/calibration_config.json`
- `artifacts/calibration/open_macro_v03_calibration_001/calibration_manifest.json`
- `artifacts/calibration/open_macro_v03_calibration_001/calibration_report.md`
- `artifacts/calibration/open_macro_v03_calibration_001/invariant_report.json`
- `artifacts/calibration/open_macro_v03_calibration_001/metrics_manifest.json`
- `artifacts/calibration/open_macro_v03_calibration_001/output_manifest.json`
- `artifacts/calibration/open_macro_v03_calibration_001/parameter_grid.json`
- `artifacts/calibration/open_macro_v03_calibration_001/rejected_candidates.json`
- `artifacts/calibration/open_macro_v03_calibration_001/reproducibility_report.json`
- `artifacts/calibration/open_macro_v03_calibration_001/run_matrix.json`
- `artifacts/calibration/open_macro_v03_calibration_001/selected_parameters.json`
- `artifacts/calibration/open_macro_v03_calibration_001/logs/calibration.log`
- `docs/planning/open_macro_v03_post_shadow_planning_001.md`
- `docs/planning/open_macro_v03_post_shadow_file_inventory.json`
- `docs/planning/open_macro_v03_post_shadow_risk_register.json`
- `contracts/quant-engine/v1/manifest.json`
- `contracts/quant-engine/v1/job-request.schema.json`
- `contracts/quant-engine/v1/job-result.schema.json`
- `contracts/quant-engine/v1/engine-manifest.schema.json`
- `src/run_worker.py`
- `src/run.py`
- `src/run_all.py`
- `src/db.py`
- `src/calibration_harness.py`
- `src/shadow_pilot.py`
- `docker/railway-ci/Dockerfile`
- `railway.toml`

## Real Entrypoints Found

- `railway.toml` starts productive workers with `python -m src.run_worker`.
- `src/run_worker.py::main` imports `src.workers.<WORKER>`, resolves `DATABASE_URL`, and calls worker `run` functions.
- `src/run.py::main` runs a named worker with a resolved DSN.
- `src/run_all.py::main` loops over DB-writing workers with a shared DSN.
- `src/db.py::connect` opens the cloud DB connection, and `src/db.py::advisory_lock` manages worker locks.
- `src/calibration_harness.py::main` can run offline but otherwise opens a DB connection through `connect(resolve_dsn(os.getenv("DATABASE_URL")))`.
- `src/shadow_pilot.py` contains the safe artifact-only envelope/result validators used as design reference, not a productive runtime path.

## Control-Plane Path Current State

No safe quant-engine runtime/control-plane job abstraction exists in this workers repo. The only safe existing abstraction is the artifact-only shadow envelope and result manifest under `artifacts/shadow/open_macro_v03_shadow_001/`, plus offline quant-engine contract schemas. Backend inventory from the post-shadow plan records a stale contract mirror and live allocator endpoints outside this repo.

## Skeleton Connection Point

The skeleton connects only at the artifact boundary:

- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_job_envelope.schema.json`
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_result_manifest.schema.json`
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_skeleton_manifest.json`

No source runtime entrypoint, worker, SQL path, backend endpoint, allocator path, or Docker execution hook is connected in this branch.

## Activation Guards

- Feature flag: `open_macro_v03_runtime_activation`.
- Default and production default remain `false`.
- No allowed environments are defined.
- `runtime_activation=false`, `A5=blocked`, and `freeze_ready=false` are schema constants.
- `official_result=false`, `allow_db_write=false`, `allow_allocator_publish=false`, `allocator_publish=false`, `db_write_official=false`, and `production_endpoint_activation=none` are schema constants.
- `docker_execution_from_backend=false` is a schema constant.

## Preventing Side Effects

- Allocator publish is blocked by `allow_allocator_publish=false` and `allocator_publish=false`.
- Official DB writes are blocked by `allow_db_write=false` and `db_write_official=false`.
- Productive endpoints are blocked by `production_endpoint_activation=none`.
- Docker execution from backend runtime is blocked by `docker_execution_from_backend=false`.
- Formula, input pack, calibration pack, and contract v1 changes are all pinned to `none`.

## Files Changed In This Branch

- `docs/planning/open_macro_v03_runtime_integration_skeleton_plan_001.md`
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_skeleton_manifest.json`
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_job_envelope.schema.json`
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_result_manifest.schema.json`
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/feature_flag_guard_report.json`
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/no_side_effects_report.json`
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/integration_readiness_report.md`
- `tests/test_runtime_integration_skeleton.py`
- `docker/railway-ci/Dockerfile`

## Tests Added

- Manifest governance constants remain inert.
- Runtime job envelope schema rejects identity drift and activation attempts.
- Runtime result manifest schema rejects official results and side-effect attempts.
- Feature flag default remains false.
- No-side-effects report covers allocator, DB, endpoint, Docker, formula, input, calibration, and contract guards.
- Railway CI includes the runtime skeleton artifacts and test.

## Out Of Scope Risks

- Backend contract mirror sync remains out of scope.
- Actual backend/control-plane wiring remains blocked until a safe job-envelope abstraction exists in the backend repo.
- Macro history coverage, macro vintage identity, advisory lock/regime gate collision, and quadrant staleness debts remain out of scope.
- Human technical, quantitative, and risk signoffs remain pending; A5 remains blocked.

## Stop Criteria

Stop without implementing runtime integration if any real backend/control-plane work is required in this branch, if any schema needs `runtime_activation=true`, if any allocator/DB/endpoint path must be enabled, if Docker execution from productive backend is required, or if formula/input/calibration/contract changes are requested.

## Final State

- A3: `open_macro_v03`
- A4: `runtime_integration_skeleton_prepared`
- A5: `blocked`
- freeze_ready: `false`
- runtime_activation: `false`
- official_result: `false`
- production_impact: `none`

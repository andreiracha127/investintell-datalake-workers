# open_macro_v03 External Executor Handshake Plan 001

## Decision

Prepare a workers-side, artifact-only handshake between the backend/control-plane
contract merged in `investintell-light#3` and an external executor. This phase
validates file artifacts and provenance only. It must not activate runtime, unblock
A5, publish allocator output, write official DB results, create a production
endpoint, or start controlled shadow execution.

## Dependency Baseline

```json
{
  "control_plane_shadow_contract_id": "open_macro_v03_control_plane_shadow_contract_001",
  "control_plane_contract_merge_commit": "ba7bc6c2f2f472fdf9e8318de5fd3804efc2cc71",
  "runtime_skeleton_id": "open_macro_v03_runtime_skeleton_001",
  "runtime_skeleton_001_merge_commit": "87e69a8cfb7aa646d1d0c7c9d7610ce914514cc7",
  "shadow_id": "open_macro_v03_shadow_001",
  "calibration_id": "open_macro_v03_calibration_001",
  "mode": "shadow",
  "runtime_activation": false,
  "A5": "blocked",
  "freeze_ready": false,
  "official_result": false,
  "backend_runtime_execution": "none",
  "db_write_mode": "none",
  "allocator_impact": "none",
  "production_endpoint_activation": "none"
}
```

## Files Read

Backend/control plane, post-merge at `ba7bc6c2f2f472fdf9e8318de5fd3804efc2cc71`:

- `backend/app/contracts/open_macro_v03_runtime_skeleton.py`: stdlib-only validator,
  pinned input/calibration/contract/engine identity, inert governance pins, no
  FastAPI/DB/Docker/subprocess imports.
- `backend/scripts/verify_open_macro_v03_runtime_skeleton.py`: standalone offline
  verifier with required valid/invalid fixtures.
- `backend/contracts/runtime/open_macro_v03_runtime_skeleton_001/*`: mirrored schemas,
  fixtures, and `SOURCE.json` from workers PR #9.
- `backend/tests/test_open_macro_v03_runtime_skeleton_contracts.py`: guardrail tests for
  optional pins, drift evidence, no endpoint registration, no DB write contract, and
  no runtime imports.
- `docs/planning/open_macro_v03_control_plane_shadow_contract_plan_001.md`: PR #3 plan,
  prohibited scope, and explicit no backend executor decision.
- `docs/architecture/open_macro_v03_control_plane_shadow_contract_runbook_001.md`: allowed
  backend behavior and next-phase boundary.
- `backend/app/core/config.py`: feature flags default false pattern; no
  `open_macro_v03` runtime flag wiring exists.
- `backend/app/api/routes/builder.py`: existing `/builder/optimize` and `/builder/save` routes.
- `backend/app/api/routes/jobs.py`: existing job status read endpoint only.
- `backend/app/main.py`: route registration; no `open_macro_v03` route exists.
- `backend/app/services/builder_save.py` and `backend/app/services/portfolio_builder.py`:
  productive builder/DB paths that remain out of scope.

Workers/executor side, `main` at `87e69a8cfb7aa646d1d0c7c9d7610ce914514cc7`:

- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_skeleton_manifest.json`:
  inert runtime skeleton manifest with A5 blocked and runtime activation false.
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_job_envelope.schema.json`:
  runtime skeleton envelope schema, artifact-only external orchestrator policy, and
  no backend Docker execution.
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/runtime_result_manifest.schema.json`:
  inert result manifest schema; backend PR #3 hardened the mirrored version.
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/no_side_effects_report.json`:
  runtime, DB, allocator, endpoint, backend Docker, formula/input/calibration/contract
  changes all blocked.
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/feature_flag_guard_report.json`:
  feature flag `open_macro_v03_runtime_activation` default false with no allowed envs.
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/integration_readiness_report.md`:
  documents missing safe control-plane abstraction and no backend wiring.
- `artifacts/shadow/open_macro_v03_shadow_001/shadow_manifest.json`: shadow readiness
  metadata, `execution_status=not_started`, A5 blocked, no production impact.
- `artifacts/shadow/open_macro_v03_shadow_001/shadow_job_envelope.schema.json`: shadow
  envelope schema with `execution_policy=isolated_external_executor_no_productive_runtime_docker`.
- `artifacts/shadow/open_macro_v03_shadow_001/shadow_result_manifest.schema.json`: shadow
  result schema, no official result, side-effect rejection classes, zero-divergence success gate.
- `artifacts/shadow/open_macro_v03_shadow_001/output_manifest.schema.json`: required output
  manifest shape and logs list.
- `artifacts/shadow/open_macro_v03_shadow_001/reproducibility_report.schema.json`: expected
  host/container, jobs=1/4, network none, DB false, input read-only gates.
- `artifacts/shadow/open_macro_v03_shadow_001/rollout_runbook.md`: later pilot sequence and non-goals.
- `artifacts/calibration/open_macro_v03_calibration_001/calibration_manifest.json`: candidate
  calibration pins for input pack, config, engine commit, and output hashes.
- `artifacts/calibration/open_macro_v03_calibration_001/calibration_config.json`: network none,
  DB false, input read-only, jobs matrix `[1, 4]`, runtime activation false.
- `artifacts/calibration/open_macro_v03_calibration_001/output_manifest.json`: calibration output paths.
- `artifacts/calibration/open_macro_v03_calibration_001/reproducibility_report.json`: existing
  jobs=1/4 host/container evidence, run_count 8, mismatch_count 0.
- `artifacts/calibration/open_macro_v03_calibration_001/run_matrix.json`: green run-matrix comparisons.
- `artifacts/calibration/open_macro_v03_calibration_001/invariant_report.json`: invariant checks green.
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/*`: existing artifact-only pilot evidence used only
  as a reference pattern; this branch must not start controlled shadow.
- `src/shadow_pilot.py`: artifact-only producer/validator entrypoints, including
  `build_shadow_job_envelope`, `validate_shadow_job_envelope`, `validate_shadow_result_manifest`,
  `validate_reproducibility_report`, `validate_pilot_output_manifest`, and `verify_final_pilot_bundle`.
- `src/calibration_candidate.py`: calibration candidate builder and guards for network none, DB false,
  read-only input pack, output isolation, and run matrix evidence.
- `scripts/repeatability_matrix.py`: container isolation command builder with `--network none`,
  `--read-only`, read-only input mount, writable output mount, and subprocess use only in external/CI script.
- `docker/railway-ci/verify_input_pack.py`: CI input pack verifier.
- `docker/railway-ci/verify_calibration_artifacts.py`: CI calibration artifact verifier.
- `docker/railway-ci/Dockerfile`: copies runtime/shadow/calibration artifacts and runs relevant tests.
- `tests/test_runtime_integration_skeleton.py`: runtime skeleton schema/governance tests.
- `tests/test_shadow_readiness.py`: shadow readiness artifact tests.
- `tests/test_shadow_pilot.py`: shadow envelope/result/output/reproducibility guard tests.
- `tests/test_repeatability_matrix.py`: Docker isolation command tests.

## Real Entrypoints Found

- Backend validator: `app.contracts.open_macro_v03_runtime_skeleton.validate_job_envelope`.
- Backend validator: `app.contracts.open_macro_v03_runtime_skeleton.validate_result_manifest`.
- Backend verifier: `scripts/verify_open_macro_v03_runtime_skeleton.py::main`.
- Workers shadow validator: `src.shadow_pilot.validate_shadow_job_envelope`.
- Workers shadow validator: `src.shadow_pilot.validate_shadow_result_manifest`.
- Workers output validator: `src.shadow_pilot.validate_pilot_output_manifest`.
- Workers reproducibility validator: `src.shadow_pilot.validate_reproducibility_report`.
- Workers final bundle verifier: `src.shadow_pilot.verify_final_pilot_bundle`.
- External/CI isolation helper: `scripts.repeatability_matrix._container_docker_base`.
- External/CI isolation probe: `scripts.repeatability_matrix._container_isolation_probe_script`.

## Proposed Handshake Flow

1. Backend/control-plane validates or emits an inert `control_plane_request.json` and
   `shadow_job_envelope.json` artifact only.
2. Backend/control-plane records no DB rows, creates no route, publishes no allocator output,
   and starts no Docker/subprocess.
3. External executor accepts the envelope by validating pinned provenance and side-effect fields.
4. External executor policy requires network none, read-only input/calibration mounts, and a single
   writable output artifact directory.
5. External executor writes `executor_acceptance.json`, `shadow_result_manifest.json`,
   `output_manifest.json`, logs, no-side-effect evidence, reproducibility evidence, and a report.
6. Control plane receives only an artifact URI/reference in `executor_result_reference.json`;
   no official DB/result/allocator publication is produced.

## Files To Change

Expected workers-only changes:

- Add `docs/planning/open_macro_v03_external_executor_handshake_plan_001.md`.
- Add `artifacts/handshake/open_macro_v03_external_executor_handshake_001/` with handshake fixtures
  and evidence artifacts.
- Add a small offline validator module, likely `src/external_executor_handshake.py`, if tests need
  reusable checks beyond JSON Schema validation.
- Add focused tests, likely `tests/test_external_executor_handshake.py`.
- Update `docker/railway-ci/Dockerfile` only if the new focused tests must run in Railway-equivalent CI.

Backend changes are not expected in this branch. If backend fixtures are later needed, use a separate
backend PR named `feat/open-macro-v03-external-executor-handshake-contract-001`.

## Files Not To Touch

- Backend repo productive routes, jobs, DB models, migrations, frontend, and builder/allocator paths.
- Workers `contracts/quant-engine/v1/*` and bundle digest.
- Certified input pack artifacts or schemas.
- Calibration pack contents under `artifacts/calibration/open_macro_v03_calibration_001/`.
- Existing shadow readiness schemas unless a test reveals a scoped handshake-specific gap.
- Existing `artifacts/shadow/open_macro_v03_shadow_pilot_001/` evidence, except read-only reference.
- Productive Dockerfiles or runtime worker deployment files outside Railway CI test inclusion.

## Contracts Involved

- Backend `open_macro_v03_runtime_skeleton_001` inert mirror from PR #3.
- Workers runtime skeleton schemas from PR #9.
- Workers shadow readiness envelope/result/output/reproducibility schemas.
- Calibration artifact pins from `open_macro_v03_calibration_001`.
- Quant-engine contract v1 identity only as a pinned hash, not as a modified bundle.

## Artifacts To Generate

Under `artifacts/handshake/open_macro_v03_external_executor_handshake_001/`:

- `handshake_manifest.json`.
- `control_plane_request.json`.
- `shadow_job_envelope.json`.
- `executor_acceptance.json`.
- `executor_result_reference.json`.
- `shadow_result_manifest.json`.
- `output_manifest.json`.
- `validation_report.json`.
- `no_side_effects_report.json`.
- `reproducibility_report.json`.
- `handshake_report.md`.
- `logs/control_plane_validator.log`.
- `logs/external_executor.log`.

## Tests Required

Control-plane artifact tests:

- valid envelope passes.
- `runtime_activation=true` fails.
- `allow_db_write=true` fails.
- `allow_allocator_publish=true` fails.
- `production_endpoint_activation != none` fails.
- `official_result=true` fails.
- divergent `engine_commit`, `engine_image_digest`, `input_pack_sha256`,
  `calibration_config_sha256`, or `contract_bundle_sha256` fails.
- invalid `output_artifact_uri` fails.
- validator imports do not include Docker, subprocess, DB connectors, backend jobs, allocator, or FastAPI.
- feature flags remain default false.

External executor/artifact tests:

- executor acceptance rejects provenance mismatch.
- dangling symlink is rejected.
- input and calibration mounts are read-only.
- output mount is the only writable mount.
- Docker command policy requires `--network none`.
- shadow result with side-effect attempt fails success validation.
- succeeded shadow result with non-zero divergence fails.
- logs are required by `output_manifest.json`.
- jobs matrix remains `[1, 4]`, expected run_count matches, and missing/unexpected/duplicates/mismatch_count stay zero.

## Gates

Minimum local gates before PR:

- `python -m pytest tests/test_external_executor_handshake.py -q`.
- `python -m pytest tests/test_runtime_integration_skeleton.py tests/test_shadow_readiness.py tests/test_shadow_pilot.py tests/test_calibration_candidate.py tests/test_repeatability_matrix.py -q`.
- `python scripts/contract_bundle.py verify`.
- `python docker/railway-ci/verify_input_pack.py` with CI `PYTHONPATH` if needed.
- `python docker/railway-ci/verify_calibration_artifacts.py` with CI `PYTHONPATH` if needed.
- `python -m compileall src/calibration_candidate.py src/shadow_pilot.py src/input_packs services/quant_engine packages/investintell_quant_core`.
- `git diff --check`.
- Railway-equivalent Docker gate if local Docker is available; otherwise document runner/Docker unavailability explicitly.

## Risks

- Mixing this handshake with backend route/job/DB work would create an activation surface.
- Reusing `open_macro_v03_shadow_pilot_001` as if it were a new controlled run would overstate this phase.
- Touching calibration or input pack artifacts would invalidate existing hashes and move scope beyond handshake.
- Treating a local artifact reference as an official result would undermine A5 blocked governance.
- Schema-only validation and Python validator validation may diverge unless tests cover both.

## Prohibited Scope

- No A5 unblock.
- No `freeze_ready=true`.
- No runtime activation.
- No official result.
- No production endpoint.
- No productive DB write.
- No allocator publish.
- No backend Docker/subprocess execution.
- No backend route/job/control-plane executor wiring.
- No formula, input pack, calibration pack, or contract v1 changes.
- No controlled shadow execution in this phase.

## Relationship To Prior/Future Work

- Depends on backend PR #3 merge commit
  `ba7bc6c2f2f472fdf9e8318de5fd3804efc2cc71`.
- Depends on workers runtime skeleton PR #9 merge commit
  `87e69a8cfb7aa646d1d0c7c9d7610ce914514cc7`.
- Uses shadow readiness schemas and existing shadow pilot artifacts only as validation patterns.
- Prepares but does not start `feat/open-macro-v03-controlled-shadow-execution-001`.

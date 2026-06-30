# open_macro_v03 Controlled Shadow Execution Plan 001

## Decision

Prepare the planning-only boundary for a future controlled shadow execution of
`open_macro_v03` after the merged External Executor Handshake 001. This branch
does not execute shadow mode, does not produce new shadow result evidence, and
does not authorize A5 activation.

The purpose of this document is to define the future execution window, artifact
contract, isolation policy, acceptance gates, and stop criteria before any later
implementation PR is opened.

## Current Baseline

```json
{
  "A3": "open_macro_v03",
  "A4": "external_executor_handshake_validated",
  "A5": "blocked",
  "freeze_ready": false,
  "runtime_activation": false,
  "official_result": false,
  "backend_runtime_execution": "none",
  "allocator_impact": "none",
  "production_impact": "none",
  "external_executor_handshake_id": "open_macro_v03_external_executor_handshake_001",
  "external_executor_handshake_001_merge_commit": "ab081183389dbe62e03d56dd493c443263f334e9",
  "control_plane_contract_merge_commit": "ba7bc6c2f2f472fdf9e8318de5fd3804efc2cc71",
  "runtime_skeleton_001_merge_commit": "87e69a8cfb7aa646d1d0c7c9d7610ce914514cc7"
}
```

Post-merge PR #10 validation on `main` at
`ab081183389dbe62e03d56dd493c443263f334e9` passed:

- `python -m pytest tests/test_external_executor_handshake.py -q`: 192 passed.
- `python -m pytest tests/test_runtime_integration_skeleton.py tests/test_shadow_readiness.py tests/test_shadow_pilot.py tests/test_shadow_pilot_binding.py tests/test_calibration_candidate.py tests/test_repeatability_matrix.py -q`: 212 passed.
- `python scripts/contract_bundle.py verify`: ok.
- `docker/railway-ci/verify_input_pack.py` with CI `PYTHONPATH`: ok.
- `docker/railway-ci/verify_calibration_artifacts.py` with CI `PYTHONPATH`: ok.
- `src.external_executor_handshake.verify_handshake()`: validated.
- `python -m compileall src/external_executor_handshake.py src/calibration_candidate.py src/shadow_pilot.py src/input_packs services/quant_engine packages/investintell_quant_core`: pass.
- Host repeatability jobs 1/4, two repeats: `mismatch_count=0`, `run_count=4`.
- `git diff --check`: pass.
- Remote Railway-equivalent CI: `REMOTE_CI_STATUS=PASS`,
  `REMOTE_CI_SHA=ab081183389dbe62e03d56dd493c443263f334e9`.

## Files Read

Post-merge handshake evidence:

- `docs/planning/open_macro_v03_external_executor_handshake_plan_001.md`.
- `docs/shadow/open_macro_v03_external_executor_handshake_001.md`.
- `artifacts/handshake/open_macro_v03_external_executor_handshake_001/handshake_manifest.json`.
- `artifacts/handshake/open_macro_v03_external_executor_handshake_001/control_plane_request.json`.
- `artifacts/handshake/open_macro_v03_external_executor_handshake_001/shadow_job_envelope.json`.
- `artifacts/handshake/open_macro_v03_external_executor_handshake_001/executor_acceptance.json`.
- `artifacts/handshake/open_macro_v03_external_executor_handshake_001/executor_result_reference.json`.
- `artifacts/handshake/open_macro_v03_external_executor_handshake_001/no_side_effects_report.json`.
- `artifacts/handshake/open_macro_v03_external_executor_handshake_001/reproducibility_report.json`.

Shadow readiness and prior pilot reference:

- `docs/shadow/open_macro_v03_shadow_readiness_001.md`.
- `docs/shadow/open_macro_v03_shadow_pilot_001.md`.
- `artifacts/shadow/open_macro_v03_shadow_001/acceptance_criteria.md`.
- `artifacts/shadow/open_macro_v03_shadow_001/rollout_runbook.md`.
- `artifacts/shadow/open_macro_v03_shadow_001/baseline_comparison_policy.json`.
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/*`, as reference-only prior artifact evidence.

A5 preflight and activation blockers:

- `docs/a5/open_macro_v03_a5_preflight_001.md`.
- `artifacts/a5/open_macro_v03_a5_preflight_001/promotion_gate_matrix.json`.

Entrypoints and gates:

- `src/external_executor_handshake.py`.
- `src/shadow_pilot.py`.
- `scripts/repeatability_matrix.py`.
- `docker/railway-ci/verify_input_pack.py`.
- `docker/railway-ci/verify_calibration_artifacts.py`.
- `docker/railway-ci/Dockerfile`.
- `tests/test_external_executor_handshake.py`.
- `tests/test_runtime_integration_skeleton.py`.
- `tests/test_shadow_readiness.py`.
- `tests/test_shadow_pilot.py`.
- `tests/test_shadow_pilot_binding.py`.
- `tests/test_repeatability_matrix.py`.

Productive paths reviewed as prohibited for this planning branch:

- `src/run_worker.py`.
- `src/run.py`.
- `src/run_all.py`.
- `src/db.py`.
- `railway.toml`.

## Real Entrypoints

Handshake validators already merged:

- `src.external_executor_handshake.verify_handshake`.
- `src.external_executor_handshake.validate_control_plane_request`.
- `src.external_executor_handshake.validate_shadow_job_envelope`.
- `src.external_executor_handshake.validate_executor_acceptance`.
- `src.external_executor_handshake.validate_executor_result_reference`.
- `src.external_executor_handshake.validate_shadow_result_manifest`.
- `src.external_executor_handshake.validate_output_manifest`.
- `src.external_executor_handshake.validate_reproducibility_report`.
- `src.external_executor_handshake.validate_no_side_effects_report`.

Shadow artifact producer and validators available for a future execution PR:

- `src.shadow_pilot.build_shadow_job_envelope`.
- `src.shadow_pilot.validate_shadow_job_envelope`.
- `src.shadow_pilot.validate_shadow_result_manifest`.
- `src.shadow_pilot.validate_reproducibility_report`.
- `src.shadow_pilot.validate_pilot_output_manifest`.
- `src.shadow_pilot.verify_final_pilot_bundle`.
- `src.shadow_pilot.run_shadow_pilot`, reference-only for this planning branch.

External isolation references:

- `scripts.repeatability_matrix._container_docker_base`.
- `scripts.repeatability_matrix._container_isolation_probe_script`.

Productive entrypoints that must not be used by this planning branch:

- `src.run_worker.main`.
- `src.run.main`.
- `src.run_all.main`.
- `src.db.resolve_dsn`.
- `src.db.connect`.
- `src.db.advisory_lock`.

## Proposed Future Controlled Shadow Boundary

The future controlled shadow execution should be an isolated external-executor
artifact run. It may consume the validated handshake envelope and shadow
readiness schemas, but it must not become production runtime or A5 activation.

The future implementation PR should:

- Define a new controlled shadow id, likely `open_macro_v03_controlled_shadow_001`.
- Define a fixed execution window and population before execution.
- Use `open_macro_v03_external_executor_handshake_001` as the control-plane/executor contract anchor.
- Use `open_macro_v03_calibration_001` and the certified input pack as immutable inputs.
- Execute only in an isolated external runner with `--network none`.
- Mount input pack, calibration, and contract bundle read-only.
- Write only to a dedicated controlled-shadow output artifact directory.
- Produce inert artifacts and logs only.
- Return only an artifact URI/reference to the control-plane boundary.

It must not:

- Activate runtime.
- Unblock A5.
- Mark `freeze_ready=true`.
- Publish an official result.
- Publish to allocator.
- Write official DB results.
- Create or activate a production endpoint.
- Execute Docker/subprocess from backend.
- Change formulas, input packs, calibration packs, or contract v1.

## Proposed Future Artifact Model

The future implementation should create a new directory, not modify existing
handshake or pilot evidence:

`artifacts/shadow/open_macro_v03_controlled_shadow_001/`

Expected future files:

- `controlled_shadow_manifest.json`.
- `control_plane_request.json`.
- `shadow_job_envelope.json`.
- `executor_acceptance.json`.
- `shadow_result_manifest.json`.
- `output_manifest.json`.
- `baseline_comparison.json`.
- `invariant_report.json`.
- `reproducibility_report.json`.
- `no_side_effects_report.json`.
- `acceptance_report.json`.
- `observability_evidence.json`.
- `rollback_evidence.json`.
- `controlled_shadow_report.md`.
- `logs/control_plane_validator.log`.
- `logs/external_executor.log`.

The future `controlled_shadow_manifest.json` should pin:

```json
{
  "controlled_shadow_id": "open_macro_v03_controlled_shadow_001",
  "external_executor_handshake_id": "open_macro_v03_external_executor_handshake_001",
  "external_executor_handshake_001_merge_commit": "ab081183389dbe62e03d56dd493c443263f334e9",
  "runtime_skeleton_id": "open_macro_v03_runtime_skeleton_001",
  "shadow_id": "open_macro_v03_shadow_001",
  "calibration_id": "open_macro_v03_calibration_001",
  "input_pack_id": "open_macro_v03_certified_input_pack_001",
  "mode": "shadow",
  "runtime_activation": false,
  "A5": "blocked",
  "freeze_ready": false,
  "official_result": false,
  "allow_db_write": false,
  "allow_allocator_publish": false,
  "production_endpoint_activation": "none",
  "backend_executes_engine": false,
  "backend_executes_docker": false,
  "backend_executes_subprocess": false
}
```

## Execution Window And Population To Define Later

This planning branch does not choose the final run window. The future execution
PR must explicitly record:

- `as_of` date.
- Input pack identity and hash.
- Calibration config hash.
- Engine commit and image digest.
- Executor identity and owner.
- Execution window start and finish timestamps.
- Population or fixture scope.
- Expected run count.
- Expected output artifact URI.
- Rollback owner.

No ambient production DB state should be treated as an input. Any input must be
from immutable certified artifacts or explicitly documented read-only fixtures.

## Acceptance Gates For Future Execution PR

The future controlled-shadow PR should pass at minimum:

- `python -m pytest tests/test_external_executor_handshake.py -q`.
- `python -m pytest tests/test_runtime_integration_skeleton.py tests/test_shadow_readiness.py tests/test_shadow_pilot.py tests/test_shadow_pilot_binding.py tests/test_calibration_candidate.py tests/test_repeatability_matrix.py -q`.
- New focused controlled-shadow tests, if new validators/artifacts are added.
- `python scripts/contract_bundle.py verify`.
- `docker/railway-ci/verify_input_pack.py` with CI `PYTHONPATH`.
- `docker/railway-ci/verify_calibration_artifacts.py` with CI `PYTHONPATH`.
- `python -m compileall src/external_executor_handshake.py src/calibration_candidate.py src/shadow_pilot.py src/input_packs services/quant_engine packages/investintell_quant_core`.
- Host repeatability jobs 1/4 with two repeats, `mismatch_count=0`.
- Container repeatability jobs 1/4 with two repeats, `mismatch_count=0`, when Docker is available.
- Remote Railway-equivalent CI or documented runner unavailability.
- `git diff --check`.

Future result acceptance must reject:

- Missing output.
- Unexpected output.
- Non-zero mismatch count.
- NaN or inf numeric output.
- Constraint violation.
- Inconsistent run fingerprint.
- Incomplete output manifest.
- Non-reproducible result.
- Any runtime activation attempt.
- Any official DB write attempt.
- Any allocator publish attempt.
- Any invariant failure.
- Any relative delta above hard-reject threshold.

## Stop Criteria

Stop before execution or merge if any of these occur:

- A5 is changed from `blocked`.
- `runtime_activation` becomes true.
- `freeze_ready` becomes true.
- `official_result` becomes true.
- DB write mode becomes productive or official.
- Allocator publish becomes enabled.
- A production endpoint is introduced.
- Backend executes engine, Docker, or subprocess.
- Formula, input pack, calibration pack, or contract v1 hashes drift.
- Remote CI or repeatability evidence is unavailable and not explicitly documented.
- Human review requests activation or production impact in the controlled-shadow PR.

## Prohibited Scope For This Branch

This planning branch is limited to this document. It must not add or modify:

- `artifacts/shadow/open_macro_v03_controlled_shadow_001/`.
- `artifacts/shadow/open_macro_v03_shadow_pilot_001/`.
- `artifacts/handshake/open_macro_v03_external_executor_handshake_001/`.
- `artifacts/calibration/open_macro_v03_calibration_001/`.
- `artifacts/runtime/open_macro_v03_runtime_skeleton_001/`.
- `contracts/quant-engine/v1/`.
- `schemas/input_packs/`.
- `src/run_worker.py`, `src/run.py`, `src/run_all.py`, or `src/db.py`.
- Backend/control-plane code in `investintell-light`.
- Railway production deployment config.

## Next PR Outline

Only after this planning branch is reviewed, the next implementation PR may be
opened as `feat/open-macro-v03-controlled-shadow-execution-001` or a successor
branch if this plan branch is merged first.

Expected future commits:

1. `feat(shadow): add controlled shadow artifact bundle for open_macro_v03`.
2. `test(shadow): enforce controlled shadow no-side-effect gates`.
3. `docs(shadow): record controlled shadow execution evidence`.

The implementation PR must still keep:

- A5 blocked.
- `freeze_ready=false`.
- `runtime_activation=false`.
- `official_result=false`.
- no allocator publish.
- no productive DB write.
- no backend runtime execution.
- no production endpoint activation.

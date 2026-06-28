# open_macro_v03 shadow readiness 001

Status: readiness_candidate

This readiness package prepares the inert shadow contract for
`open_macro_v03_calibration_001`, which was merged through PR #4 at
`08fccef698195decaf814fcdd03c45e249bae8ad`.

It does not start shadow execution. It defines the envelope, result manifest,
comparison policy, observability, rollback, and acceptance criteria required
before a later shadow pilot PR can run any candidate job.

## Current State

- A3: `open_macro_v03`
- A4: `shadow_readiness_prepared`
- A5: `blocked`
- freeze_ready: `false`
- runtime_activation: `false`
- shadow execution: not started
- allocator impact: none
- official DB writes: none
- production endpoint activation: none

## Calibration Anchor

- calibration_id: `open_macro_v03_calibration_001`
- calibration_001_merge_commit:
  `08fccef698195decaf814fcdd03c45e249bae8ad`
- pr_head: `10a49e1489661070986e241d9e04a8b890b54937`
- engine_commit: `ee39adbe6cb6541d4fdfa78f1428478ffffaf638`
- railway_deployment_id: `60bbd720-73cc-44e6-becd-d8e274ea0534`
- railway_image_digest:
  `sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b`

## Prepared Artifacts

- `artifacts/shadow/open_macro_v03_shadow_001/shadow_manifest.json`
- `artifacts/shadow/open_macro_v03_shadow_001/shadow_job_envelope.schema.json`
- `artifacts/shadow/open_macro_v03_shadow_001/shadow_result_manifest.schema.json`
- `artifacts/shadow/open_macro_v03_shadow_001/baseline_comparison_policy.json`
- `artifacts/shadow/open_macro_v03_shadow_001/acceptance_criteria.md`
- `artifacts/shadow/open_macro_v03_shadow_001/observability_plan.md`
- `artifacts/shadow/open_macro_v03_shadow_001/rollback_plan.md`
- `artifacts/shadow/open_macro_v03_shadow_001/rollout_runbook.md`

## Allowed Scope

- Define the shadow run contract and job envelope.
- Define the shadow result manifest.
- Validate output references against `open_macro_v03_calibration_001`.
- Prepare a disabled-by-default feature flag contract.
- Prepare artifact-only output/read-model expectations.
- Prepare CI gates, rollback, observability, and comparison policy.
- Keep every productive effect disabled.

## Forbidden Scope

- Do not activate A5.
- Do not set `runtime_activation=true`.
- Do not make calibration output the official result.
- Do not feed the official allocator.
- Do not create a public productive endpoint.
- Do not write official DB results.
- Do not run shadow jobs against production with side effects.
- Do not change the quantitative formula.
- Do not change the input pack or calibration pack.
- Do not alter contract v1 without a new bundle.
- Do not set `freeze_ready=true`.

## Control Plane Rule

The backend or control plane may construct a shadow envelope and store inert
artifact metadata. It must not execute Docker directly inside the productive
runtime. Execution belongs to a later isolated runner or CI surface that records
the envelope, artifacts, hashes, and correlation identifiers without official
side effects.

## Gates Before Any Later Shadow Pilot

- Validate `shadow_job_envelope.schema.json` and
  `shadow_result_manifest.schema.json`.
- Confirm `runtime_activation=false`.
- Confirm `A5=blocked`.
- Confirm `freeze_ready=false`.
- Confirm `feature_flag_default=false`.
- Confirm `allow_db_write=false`.
- Confirm `allow_allocator_publish=false`.
- Confirm No official DB writes.
- Confirm No allocator publish path.
- Confirm no new productive endpoint exists.
- Confirm no formula, input pack, calibration pack, or contract v1 change.
- Confirm comparison policy rejects missing or unexpected outputs,
  non-zero mismatches, NaN/inf, invariant failures, unreproducible output, and
  any activation attempt.

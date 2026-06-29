# open_macro_v03 External Executor Handshake 001

## Objective

Validate an artifact-only handshake between the backend/control-plane contract and
an external executor without activating runtime, A5, allocator publication,
productive DB writes, backend Docker/subprocess execution, or production endpoints.

## Scope

- Backend/control plane validates and exchanges artifacts only.
- External executor policy is isolated with network `none` and read-only inputs.
- Output artifact directory is the only writable mount.
- Result references remain unofficial and discardable.

## Non Goals

- No A5 activation.
- No runtime activation.
- No official result.
- No allocator publish.
- No productive DB write.
- No production endpoint activation.
- No controlled shadow execution in this phase.

## Evidence

- control-plane contract merge commit: `ba7bc6c2f2f472fdf9e8318de5fd3804efc2cc71`
- runtime skeleton merge commit: `87e69a8cfb7aa646d1d0c7c9d7610ce914514cc7`
- runtime skeleton: `open_macro_v03_runtime_skeleton_001`
- shadow envelope: `shadow_job_envelope.json`
- executor acceptance: `executor_acceptance.json`
- output manifest: `output_manifest.json`
- logs: `logs/control_plane_validator.log`, `logs/external_executor.log`

## Result

The handshake is a candidate artifact gate. It preserves A5 blocked,
`freeze_ready=false`, `runtime_activation=false`, and `official_result=false`.

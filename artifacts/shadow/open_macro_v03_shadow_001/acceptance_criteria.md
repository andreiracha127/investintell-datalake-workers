# Shadow Acceptance Criteria

This package is accepted only as readiness work. It does not authorize a shadow
pilot or productive activation.

## Required For This Branch

- `shadow_manifest.json` pins `open_macro_v03_calibration_001`.
- `calibration_001_merge_commit` is
  `08fccef698195decaf814fcdd03c45e249bae8ad`.
- `runtime_activation=false`.
- `A5=blocked`.
- `freeze_ready=false`.
- `feature_flag_default=false`.
- `allocator_impact=none`.
- `db_write_mode=none_or_artifact_only`.
- `official_result=false`.
- No formula, input pack, calibration pack, or contract v1 change.
- No public productive endpoint.
- No official DB write path.
- No allocator publish path.

## Required Before A Later Shadow Pilot

- Job envelope validates against `shadow_job_envelope.schema.json`.
- Result manifest validates against `shadow_result_manifest.schema.json`.
- Successful result manifests include reproducible output, invariant report, and
  baseline comparison hashes.
- Failed or rejected result manifests record the `failure_class` without
  fabricating artifact hashes that were not produced.
- The pilot result validator rejects a non-positive execution window
  (`finished_at` earlier than `started_at`) and a `duration_ms` inconsistent
  with the recorded timestamps. JSON Schema cannot compare two fields, so this
  is enforced at the gate, not in `shadow_result_manifest.schema.json`.
- Invariant report hash is present and green for successful executions.
- Baseline comparison hash is present and green for successful executions.
- Baseline comparison policy rejects all hard failure classes.
- Technical and quantitative review records the exact pilot window, executor,
  artifact location, and rollback owner.

## Hard Rejections

- Missing output.
- Unexpected output.
- Non-zero mismatch count.
- NaN or inf in numeric outputs.
- Constraint violation.
- Inconsistent run fingerprint.
- Incomplete output manifest.
- Non-reproducible result.
- Any attempt to activate runtime.
- Any attempt to write official DB results.
- Any attempt to publish to the allocator.
- Any invariant failure.
- Any relative delta above the hard-reject threshold.

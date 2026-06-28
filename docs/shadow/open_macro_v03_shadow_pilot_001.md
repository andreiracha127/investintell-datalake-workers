# open_macro_v03 shadow pilot 001

Status: artifact-only pilot validated

This note records the first controlled Shadow Pilot for `open_macro_v03`.
The machine-readable evidence bundle lives in
`artifacts/shadow/open_macro_v03_shadow_pilot_001/`.

## Inputs

- shadow_id: `open_macro_v03_shadow_001`
- shadow_pilot_id: `open_macro_v03_shadow_pilot_001`
- calibration_id: `open_macro_v03_calibration_001`
- input_pack_id: `open_macro_v03_certified_input_pack_001`
- engine_commit: `ee39adbe6cb6541d4fdfa78f1428478ffffaf638`
- railway_image_digest: `sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b`
- shadow_readiness_merge_commit: `a644bbd72e530ffa5555e41a2553639332b65902`

## Governance

- A3: `open_macro_v03`
- A4: `shadow_pilot_validated`
- A5: `blocked`
- freeze_ready: `false`
- runtime_activation: `false`
- official_result: `false`
- allocator_impact: `none`
- db_write_mode: `none_or_artifact_only`
- production_endpoint_activation: `none`

## Runbook

1. Generate the artifact bundle with:

   ```powershell
   python -m src.shadow_pilot --shadow-readiness-merge-commit a644bbd72e530ffa5555e41a2553639332b65902 --shadow-pilot-branch-base-commit a644bbd72e530ffa5555e41a2553639332b65902
   ```

2. Validate `shadow_job_envelope.json` against
   `artifacts/shadow/open_macro_v03_shadow_001/shadow_job_envelope.schema.json`.
3. Validate `shadow_result_manifest.json` against
   `artifacts/shadow/open_macro_v03_shadow_001/shadow_result_manifest.schema.json`.
4. Confirm `baseline_comparison.json` has zero hard counters and
   `max_relative_delta_pct < 2.0`.
5. Confirm `acceptance_report.json` leaves
   `technical_and_quantitative_review_recorded` pending and keeps A5 blocked.

## Decision

The pilot is technical artifact evidence only. It does not unblock A5, does not
mark freeze readiness, and does not publish a result to allocator, DB, or any
production endpoint.

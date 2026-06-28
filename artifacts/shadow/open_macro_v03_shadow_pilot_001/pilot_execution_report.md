# open_macro_v03 shadow pilot 001

## Objective
Execute an artifact-only Shadow Pilot for open_macro_v03 without production effects.

## Scope
Generated, validated, compared, and audited shadow artifacts only.

## Non Goals
- No A5 activation
- No runtime activation
- No official result
- No allocator publish
- No productive DB write
- No production endpoint activation

## Commits And Digests
- shadow_readiness_merge_commit: `a644bbd72e530ffa5555e41a2553639332b65902`
- shadow_pilot_branch_base_commit: `a644bbd72e530ffa5555e41a2553639332b65902`
- engine_commit: `ee39adbe6cb6541d4fdfa78f1428478ffffaf638`
- railway_image_digest: `sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b`

## Shadow Job Envelope Summary
- shadow_id: `open_macro_v03_shadow_001`
- calibration_id: `open_macro_v03_calibration_001`
- run_fingerprint: `078cef19bdb6ad0de1716dd73a6e6807d45ca4cb6c675838947e2531832c8106`

## Execution Matrix
- expected_run_count: `8`
- run_count: `8`
- mismatch_count: `0`
- network: `none`

## Output Manifest Summary
- output_manifest_sha256: `597aba8630845d7cb0db8dd1f8854fd5339be9d94c1bda13417f4de7c153fa01`

## Baseline Comparison Summary
- status: `pass`
- max_relative_delta_pct: `0.0`

## Invariant Summary
- ok: `true`

## Divergences
- missing=0
- unexpected=0
- duplicates=0
- mismatch_count=0

## Rejection And Material Divergence Flags
- rejection_rules_triggered: `[]`
- material_divergence: `False`

## Observability
- logs/shadow_pilot.log
- logs/executor.log

## Rollback Evidence
- No official publication occurred; artifacts are discardable without production rollback.

## Limitations
- Human technical and quantitative review remains pending for the next gate.

## Decision Proposed
- Accept artifact-only pilot evidence as technical shadow-pilot validation.
- Keep A5 blocked and freeze_ready=false.

## Next Gate
- Pending promotion rule(s): `['technical_and_quantitative_review_recorded']`
- Shadow Pilot Review.

# Shadow Rollout Runbook

This runbook is readiness-only. It prepares a later review gate; it does not run
shadow mode.

## Preconditions

- `open_macro_v03_calibration_001` is merged to `main`.
- `railway/quant-engine-ci` is green for the calibration merge.
- Calibration artifacts validate against committed hashes.
- `runtime_activation=false`.
- `A5=blocked`.
- `freeze_ready=false`.
- Shadow feature flag defaults to false.

## Later Pilot Sequence

1. Open a dedicated shadow pilot PR.
2. Record the pilot window, executor, artifact URI, and owner.
3. Construct an envelope that validates against
   `shadow_job_envelope.schema.json`.
4. Execute only in an isolated runner with official side effects disabled.
5. Write only inert artifacts and manifests.
6. Compare against the official baseline using
   `baseline_comparison_policy.json`.
7. Validate the result manifest, which records the baseline comparison hash,
   materiality summary, and divergence summary produced by the previous step
   for successful runs.
8. Review material divergence and invariant status before any next gate.

## Explicit Non-Goals

- No runtime activation.
- No allocator publication.
- No official DB writes.
- No productive endpoint.
- No formula change.
- No input pack change.
- No calibration pack change.
- No contract v1 change without a new bundle.

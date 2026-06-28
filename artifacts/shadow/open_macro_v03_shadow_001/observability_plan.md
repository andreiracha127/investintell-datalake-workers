# Shadow Observability Plan

Shadow execution is not started by this branch. These fields are required for a
later isolated pilot.

## Structured Fields

- `shadow_id`
- `calibration_id`
- `request_id`
- `correlation_id`
- `execution_id`
- `run_fingerprint`
- `status`
- `started_at`
- `finished_at`
- `input_pack_sha256`
- `engine_commit`
- `engine_image_digest`
- `output_artifact_uri`
- `output_manifest_sha256` when produced
- `invariant_report_sha256` when produced
- `baseline_comparison_sha256` when produced
- `duration_ms`
- `memory_peak_bytes`
- `cpu_time_ms`
- `failure_class`
- `retry_count`
- `runtime_activation`
- `allow_db_write`
- `allow_allocator_publish`
- `production_endpoint_activation`
- `official_result`

## Alerts

- Material baseline divergence.
- Invariant failure.
- Missing or unexpected output.
- Non-zero mismatch count.
- NaN or inf.
- Constraint violation.
- Run fingerprint inconsistency.
- Output manifest incompleteness.
- Any `runtime_activation` attempt.
- Any DB write attempt.
- Any allocator publish attempt.
- Any production endpoint activation attempt.
- Latency p95 regression beyond the policy review threshold.
- Memory peak regression beyond the policy review threshold.
- Retry-rate delta beyond the policy review threshold.

## Healthy Signals

- `runtime_activation=false` on every event.
- `allow_db_write=false` on every envelope.
- `allow_allocator_publish=false` on every envelope.
- Successful result manifests include immutable artifact hashes.
- Failed or rejected result manifests preserve the concrete `failure_class`
  without fabricated artifact hashes.
- Correlation identifiers join envelope, execution log, result manifest, and
  comparison output.

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
- `input_pack_sha256`
- `engine_commit`
- `engine_image_digest`
- `output_artifact_uri`
- `output_manifest_sha256`
- `invariant_report_sha256`
- `baseline_comparison_sha256`
- `duration_ms`
- `memory_peak_bytes`
- `cpu_time_ms`
- `failure_class`
- `retry_count`
- `runtime_activation`
- `allow_db_write`
- `allow_allocator_publish`

## Alerts

- Material baseline divergence.
- Invariant failure.
- Missing or unexpected output.
- Non-zero mismatch count.
- NaN or inf.
- Run fingerprint inconsistency.
- Output manifest incompleteness.
- Any `runtime_activation` attempt.
- Any DB write attempt.
- Any allocator publish attempt.

## Healthy Signals

- `runtime_activation=false` on every event.
- `allow_db_write=false` on every envelope.
- `allow_allocator_publish=false` on every envelope.
- Artifact hashes are present and immutable.
- Correlation identifiers join envelope, execution log, result manifest, and
  comparison output.

# open_macro_v03 A5 Rollback Runbook

Status: preparatory and not executed.

## Immediate Default

- Keep `open_macro_v03_runtime_activation=false`.
- Keep `A5=blocked`.
- Keep `freeze_ready=false`.
- Keep `runtime_activation=false`.

## Disable Candidate Paths

1. Keep the future runtime feature flag false.
2. Disable any shadow or pilot envelope intake if unexpected activity appears.
3. Invalidate pending envelopes by `shadow_id`, `shadow_pilot_id`, `execution_id`, and artifact URI.
4. Preserve current official baseline and ignore candidate artifacts for productive decisions.

## Prevent Productive Effects

- Prevent allocator publish.
- Prevent official DB writes.
- Prevent production endpoint activation.
- Confirm no formula, input pack, calibration pack, or contract v1 mutation occurred.

## Candidate Artifact Handling

- Preserve candidate artifacts for audit by default.
- Remove or quarantine candidate artifacts only if they are unsafe or misleading.
- Record any removal with path, hash, reason, and approver.

## Audit Side Effects

- Search logs for `runtime_activation=true`.
- Search logs for `allow_db_write=true`.
- Search logs for `allow_allocator_publish=true`.
- Search logs for `production_endpoint_activation` values other than `none`.
- Search DB audit logs for official writes tied to open_macro_v03 candidate artifacts.
- Search allocator logs for candidate publish attempts.

## Confirmation

- Confirm A5 remains blocked.
- Confirm runtime activation remains false.
- Confirm freeze readiness remains false.
- Confirm official result remains false.

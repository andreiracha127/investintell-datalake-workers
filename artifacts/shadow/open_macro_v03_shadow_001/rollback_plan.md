# Shadow Rollback Plan

Shadow execution is not enabled by this branch. This plan defines how to return
to the baseline if a later pilot is prepared or started.

## Disable Entry Points

1. Keep or reset `open_macro_v03_shadow_readiness_enabled=false`.
2. Refuse new envelopes for `open_macro_v03_shadow_001`.
3. Mark queued envelopes invalid by `shadow_id` and `execution_id`.
4. Preserve existing artifacts for audit unless security requires quarantine.

## Remove Candidate Consumption

1. Confirm the official allocator still reads only the current baseline.
2. Confirm no shadow result has `official_result=true`.
3. Confirm no productive DB table consumed a shadow result.
4. Remove candidate artifact references from any non-production read model.

## Return To Baseline

1. Continue serving the current official baseline.
2. Ignore `open_macro_v03_calibration_001` outputs for productive decisions.
3. Keep A5 blocked.
4. Keep `freeze_ready=false`.
5. Keep `runtime_activation=false`.

## Audit For Accidental Publication

- Search execution logs for `allow_allocator_publish=true`.
- Search execution logs for `allow_db_write=true`.
- Search DB audit logs for `shadow_id=open_macro_v03_shadow_001`.
- Search allocator logs for `calibration_id=open_macro_v03_calibration_001`.
- Verify no public productive endpoint was added for the shadow result.

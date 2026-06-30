# open_macro_v03 A5 Rollback Runbook

Status: preparatory and not executed.

## Immediate Default

- Keep `open_macro_v03_runtime_activation=false`.
- Keep `A5=blocked`.
- Keep `freeze_ready=false`.
- Keep `runtime_activation=false`.
- Keep `activation_allowed=false`.

## Keep Feature Flag Off

1. Confirm the feature flag default is false.
2. Confirm no allowed environment is listed for this readiness PR.
3. Reject any proposal that changes the default outside a separate activation PR.

## Disable Runtime If A Future Activation Is Approved

1. Return the feature flag to off.
2. Stop accepting candidate runtime envelopes.
3. Preserve the last accepted artifact bundle for audit.
4. Confirm A5 remains blocked until a new governance decision is recorded.

## Prevent Allocator Publish

1. Block allocator publication from any candidate artifact.
2. Preserve allocator publish attempt logs.
3. Confirm allocator impact remains none.

## Invalidate Artifacts

1. Mark any suspect candidate bundle as invalid in the future decision record.
2. Preserve the rejected bundle hash and reason.
3. Re-run contract, input pack, calibration, and controlled shadow verifiers before reconsideration.

## Return To Baseline

1. Use the existing baseline decision path.
2. Ignore candidate artifacts for productive decisions.
3. Keep official result publication disabled.

## Audit DB And Results

1. Verify no productive DB write occurred.
2. Verify no official result was published.
3. Verify no allocator publication occurred.
4. Verify no production endpoint was exposed.

## Block A5 Again

1. Confirm A5 remains blocked.
2. Confirm runtime activation remains false.
3. Confirm freeze readiness remains false.
4. Record rollback evidence in a follow-up governance artifact.

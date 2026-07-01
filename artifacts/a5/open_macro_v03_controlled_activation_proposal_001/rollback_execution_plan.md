# open_macro_v03 Controlled Activation Rollback Execution Plan

Status: proposal-only. This plan is not executed in this PR.

## Keep Feature Flag Off

Keep `open_macro_v03_runtime_activation=false` for this proposal. Do not add an environment override, rollout percentage, or automated activation command.

## Invalidate Proposal

If the proposal is rejected, mark `open_macro_v03_controlled_activation_proposal_001` invalid in a follow-up governance artifact or close the PR. Do not reinterpret this proposal as approval.

## Prevent Official Result

Confirm `official_result=false` in the proposal manifest and no candidate output is marked official. Candidate artifacts remain non-official until a future activation PR explicitly changes governance state.

## Prevent Allocator Publish

Confirm `allocator_publish=false` and `allow_allocator_publish` is never true in this proposal package. Preserve any attempted publish signal as an incident artifact.

## Confirm No Productive DB Write

Confirm `db_write_mode=none`. No manual DB edits are allowed. Any productive DB write attempt is a critical incident and aborts future activation consideration.

## Revert To Baseline

Use the existing baseline decision path and ignore candidate proposal artifacts for production decisions. Keep all productive consumers pointed at the existing approved baseline.

## Audit Artifacts

Recompute evidence hashes before any future activation PR. Confirm the effective input pack, calibration pack, controlled shadow bundle, handshake bundle, runtime skeleton, and preflight evidence remain unchanged or are explicitly re-reviewed.

## Communicate Incident

Notify technical, quantitative, risk, operations, and final approver roles. If owners remain unassigned, escalation remains blocked and no activation can proceed.

## Restore A5 Blocked

Confirm `A5=blocked`, `freeze_ready=false`, `runtime_activation=false`, `activation_allowed=false`, `official_result=false`, `allocator_publish=false`, and `production_endpoint_activation=none` after rollback validation.

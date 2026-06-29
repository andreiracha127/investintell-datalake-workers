# open_macro_v03 A5 Production Activation Runbook

Este runbook é preparatório. Não autoriza A5. Não ativa runtime.

Status: not executed.

## Purpose

This runbook documents the future activation procedure that can only be used after
an explicit A5 decision record approves controlled activation in a separate PR.

## Preconditions

- A5 preflight readiness PR accepted.
- Technical review recorded.
- Quantitative review recorded.
- Risk review recorded.
- Feature flag policy reviewed and still default false.
- Monitoring SLO policy approved.
- Rollback runbook approved.
- No formula, input pack, calibration pack, or contract v1 change in the activation PR.

## Approvals

- Technical approver: pending.
- Quantitative approver: pending.
- Risk approver: pending.
- Operations approver: pending.

## Future Feature Flag

- Proposed flag: `open_macro_v03_runtime_activation`.
- Default: false.
- Production default: false.
- Activation is not allowed in this PR.

## Staged Rollout

1. Confirm `A5=blocked` before any separate activation PR starts.
2. Create a separate controlled activation PR with explicit approvals.
3. Enable only the approved staging or canary target if the decision record allows it.
4. Monitor SLOs and side-effect attempt counters continuously.
5. Stop on any hard threshold breach or forbidden side-effect attempt.

## Canary

- Canary scope must be zero until explicit activation approval exists.
- Canary must not publish to allocator without a separate approved activation gate.
- Canary must not write official DB results without a separate approved activation gate.

## Monitoring

- Track run success rate, latency, memory, retry rate, divergence rate, invariant failures, missing outputs, allocator publish attempts, DB write attempts, and endpoint activation attempts.

## Stop Conditions

- Any `runtime_activation=true` outside the approved activation PR.
- Any official DB write attempt before approval.
- Any allocator publish attempt before approval.
- Any production endpoint activation attempt before approval.
- Any missing output, unexpected output, NaN, infinity, constraint violation, or invariant failure.

## Evidence Capture

- Capture flag state, deployment digest, logs, result manifests, monitoring snapshots, incident notes, and rollback confirmation.

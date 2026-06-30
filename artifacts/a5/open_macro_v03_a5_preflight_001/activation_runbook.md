# open_macro_v03 A5 Activation Runbook

Status: preparatory only. This runbook does not execute activation and does not authorize A5.

## Purpose

This document describes the future controlled activation procedure that can only be considered after a separate activation proposal PR is reviewed and approved. The default state remains inactive.

## Preconditions For A Future A5 Proposal

- Technical review is recorded.
- Quantitative review is recorded.
- Risk review is recorded.
- Operations review is recorded.
- A separate controlled activation proposal PR exists.
- The feature flag policy is still default-off.
- Monitoring and SLO policy is approved.
- Rollback runbook is approved.
- No formula, input pack, calibration pack, or contract v1 changes are included in the activation proposal.

## Feature Flag

- Flag name: `open_macro_v03_runtime_activation`.
- Default: `false`.
- Production default: `false`.
- Allowed environments in this readiness PR: none.

## Proposed Staged Rollout For A Future PR

1. Keep the feature flag off while reviewers inspect the proposal.
2. Confirm controlled shadow evidence still matches the pinned bundle.
3. Confirm monitoring dashboards and alert routing are live.
4. Propose a bounded canary window in the separate activation PR.
5. Require explicit approval before changing any runtime state.

## Kill Switch

- Keep the feature flag off by default.
- If a future rollout is approved and later paused, immediately return the flag to off.
- Block allocator publication while the incident is assessed.
- Preserve all artifacts and logs for audit.

## Monitoring

- Watch latency, memory, retry/error rate, divergence, stale artifacts, missing outputs, allocator publish attempts, DB write attempts, runtime activation attempts, and production endpoint exposure attempts.
- Escalate critical alerts to `quant-engine-governance`.

## Rollback

- Follow `rollback_runbook.md`.
- Return to the baseline decision path.
- Verify no official result was published.
- Verify no allocator publication occurred.
- Verify no productive DB write occurred.

## Pause Criteria

- Any missing output.
- Any unexpected output.
- Any hard divergence.
- Any NaN or infinity in decision artifacts.
- Any constraint or invariant violation.
- Any productive side-effect attempt.

## Abort Criteria

- Any side-effect attempt involving DB writes, allocator publication, backend execution, or production endpoint exposure.
- Any mismatch between pinned input, calibration, contract, controlled shadow, and runtime evidence.
- Any reviewer revokes approval.

## Success Criteria For A Future Proposal

- No hard divergences.
- No missing or unexpected outputs.
- No productive side effects.
- SLOs remain within the approved envelope.
- Audit trail is complete.

## Communication Plan

- Record approval in the future activation PR.
- Notify technical, quantitative, risk, operations, and incident owners before any future runtime state change.
- Attach monitoring snapshots and evidence hashes.

## Audit Plan

- Store feature flag state, deployment digest, logs, result manifests, monitoring snapshots, incident notes, and rollback confirmation.
- Preserve the controlled shadow bundle as immutable pre-activation evidence.

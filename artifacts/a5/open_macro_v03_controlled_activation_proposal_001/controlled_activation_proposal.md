# open_macro_v03 Controlled Activation Proposal 001

## Executive Summary

This package proposes a future controlled activation path for `open_macro_v03`. It does not activate A5, runtime execution, freeze readiness, official result publication, allocator publication, productive DB writes, or production endpoints.

## Scope

The proposal consolidates technical, quantitative, risk, and operations review records; staged rollout; rollback; kill switch; monitoring enforcement; go/no-go state; approval requirements; unresolved risks; and no-activation guard evidence.

## Consolidated Evidence

The proposal is based on PR #13 merged into `main` at `10602998fda56d0d265e69314ee333a307923e51`. Evidence includes A5 preflight readiness, controlled shadow evidence, external executor handshake, runtime skeleton, calibration artifacts, and the effective certified input pack under `fixtures/input_packs/golden/certified_input_pack`.

## Technical Review

Automated technical evidence is mostly green: input pack, calibration, controlled shadow, handshake, runtime skeleton, no backend Docker/subprocess, no DB write, no allocator publish, no production endpoint, repeatability, output manifests, logs, Railway image digest, contract bundle, provenance, rollback plan, and feature flag default are documented. Human technical review remains pending and blocking.

## Quantitative Review

Controlled shadow and baseline evidence indicate no hard threshold breach, no material divergence, mismatch count zero, no NaN, no infinity, and constraints respected. Formal quantitative review remains pending for turnover, drawdown, volatility, risk envelope, stress windows, out-of-sample acceptance, selected parameters, and regression summary.

## Risk Review

Known risks remain blocking, including macro history coverage, macro vintage identity, advisory lock/regime gate review, quadrant macro staleness/source availability, baseline global failures, stale artifacts, feature flag risk, DB/allocator side effects, rollback test status, and production exposure risk.

## Operations Review

Operations readiness is incomplete. The proposal defines rollback steps, kill switch plan, staged rollout, monitoring enforcement, audit trail, and production safety controls. Real owners, on-call escalation, log retention, rollback dry run, kill switch dry run, and current PR CI evidence remain pending.

## Proposed Decision

The proposed state for this PR is `no_go_pending_review`. This is intentionally not an activation decision.

## Why This PR Does Not Activate

This PR is proposal-only because formal reviews, owners, approvals, rollback dry run, kill switch dry run, and several SLO thresholds remain pending. A5 remains blocked and any activation must be handled by a future separate PR.

## Criteria For Future Activation PR

A future activation PR must record technical, quantitative, risk, and operations approvals; assign real owners; refresh evidence hashes; complete monitoring thresholds; document rollback and kill switch dry runs; pass CI; and explicitly justify any change to `activation_allowed` or `freeze_ready`.

## Remaining Risks

Remaining risks are listed in `unresolved_risks_register.json` and `risk_review_record.json`. They are blocking for activation.

## Pending Approvals

Technical owner, quant owner, risk owner, operations owner, product/portfolio owner, and final approver are unassigned. No approval is recorded.

## Next Steps

Review this proposal, assign owners, resolve blocking risks, and only then prepare a separate `feat/open-macro-v03-a5-controlled-activation-001` branch if reviewers approve proceeding.

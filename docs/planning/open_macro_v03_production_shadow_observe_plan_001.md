# open_macro_v03 — Phase 2: Production Shadow Observe (plan 001)

Status: plan-only. Written after Phase 1 merged (`A4=dark_launch_ready`, PR #30,
merge `4ef0691`), per the stage plan's instruction to write Phase 2's own plan at
that point. Nothing in this document changes governance state.

**Non-negotiable governance (unchanged):** A5 stays **blocked**;
`runtime_activation=false`, `activation_allowed=false`, `allocator_publish=false`,
`official_result=false`, `db_write_mode=none`, `production_endpoint_activation=none`.
The shadow observes; it never acts.

## Entry criteria (all met)

- Phase 1 merged: `A4=dark_launch_ready` (PR #30, `4ef0691`).
- Signed candidate: `compressed_50` sleeve under the signed threshold envelope
  (`artifacts/quant/open_macro_v03_threshold_signoff_001/`), judgment phase0q_004
  `go_candidate` on all five gates.
- Measured SLO thresholds to conform against
  (`artifacts/a5/open_macro_v03_dark_launch_001/monitoring_thresholds_record.json`:
  latency 37,826 ms; memory 2,047,279,104 bytes; error/retry 0.0).
- A REAL execution+measurement harness exists and is deterministic
  (`harness/dark_launch/measure_*`: 3 independent rounds produced the identical
  job fingerprint). Phase 2 must NOT repeat Phase 0's mistake of attesting
  artifacts without executing anything — the prior "controlled shadow" was an
  artifact-only validator with fixed 1s/0-byte pins.

## Scope

One new evidence bundle, `artifacts/shadow/open_macro_v03_prod_shadow_observe_001/`,
produced by REAL executions under the external-executor isolation profile
(`--network none`, `--read-only`, non-root, read-only input mounts — the hardened
profile `scripts/repeatability_matrix.py` already encodes), containing:

1. a hash-pinned **production-adjacent input snapshot** (as-of "today" vintages),
2. the candidate's decision computed on that snapshot (host + container),
3. **divergence metrics vs the production baseline decision path**,
4. a **reproducibility report** (host × container, `mismatch_count=0`),
5. an **SLO conformance report** (measured vs the Phase 1 thresholds),
6. review records (quant_owner + operations_owner) → exit `A4=production_shadow_observed`.

## Tasks (each TDD: failing test → executor → artifact → green)

### Task 1: Scaffold + manifest + guards

`prod_shadow_observe_manifest.json` (A5 blocked, all flags false,
`shadow_id: open_macro_v03_prod_shadow_observe_001`,
`consumes: open_macro_v03_dark_launch_001`), `tests/test_prod_shadow_observe.py`
with the required-artifacts gate, the recursive governance walk (duplicate-key
rejection + string-"true" as truthy — reuse the Phase 1 semantics), and the
forbidden-marker sweep. Regeneration-equality pins for every generated artifact
(committed record == what the committed executor derives), as in Phase 1.

### Task 2: Production-adjacent input snapshot (read-only)

A committed executor exports the CURRENT PIT vintages + sleeve prices needed by the
candidate decision (same source tables as certified pack v2: `macro_observation_vintage`
seed basket + eod prices `adj_close`) via a **read-only** production query, as-of a
pinned date, into `input_snapshot/` with per-file sha256 and a snapshot manifest.
The snapshot is evidence, not a certified pack — it feeds the shadow only.
Fail-loud staleness gate at export time: every series within its cadence
(the Phase 1 `staleness_verification_record` criteria), else abort.

### Task 3: Candidate shadow execution (host + container)

Run the candidate decision chain + `compressed_50` sleeve position for the as-of date
on the snapshot, via the committed harness pattern (`measure_child.py` in-process
measurement; container leg on the locally built engine image with content-id pinned).
Repeat N=8 per leg. Enforce: identical job identity across every run (the Phase 1
determinism rule), clean compute tree, image id + tree hashes + combo/snapshot hashes
recorded. Output: `shadow_decision_result.json` (artifact-only; the decision, the
position, the chain provenance) + `observability_measurements.json`.

### Task 4: Divergence vs production baseline

Executor reads the CURRENT production baseline decision path (read-only: the live
`quadrant_macro`/regime snapshot the allocator actually consumes) and computes the
divergence block of `shadow_materiality_v1`
(`classification_rate_delta_pct`, `allocation_weight_delta_pct`, realized metric
deltas where defined). Verdict fields only — no thresholds are relaxed, no baseline
is modified. **Abort criteria (pinned, from `monitoring_enforcement_policy.json`):**
hard divergence (NaN/Inf, missing output, constraint violation), any DB write
attempt, any allocator publish attempt → the bundle records the abort and Phase 2
STOPS (the activation path halts per the rollback plan).

### Task 5: Reproducibility + SLO conformance reports

`reproducibility_report.json`: host leg hash == container leg hash over the decision
outputs (`mismatch_count=0` required; the Phase 0Q canonicalization: 12-decimal
floats, stable hash). `slo_conformance_report.json`: measured p95/peak/error/retry
vs the Phase 1 thresholds — every metric within its SLO or the report says
`conformant: false` and Phase 2 stops (thresholds are NOT recalibrated here; a
breach is a finding, not a tuning opportunity).

### Task 6: Review + exit

`shadow_review_record.json`: quant_owner review (divergence acceptable? candidate
decision sane vs baseline?) + operations_owner review (SLO conformance, isolation
profile respected, no side-effect signals). Both must be explicit signature acts in
the activation thread (verbatim quotes recorded), ratified by the PR merge.
Exit state: `A4=production_shadow_observed`. Task 2 of Phase 3 (candidate result
materialization) requires the final_approver's separate act — out of scope here.

## What Phase 2 explicitly does NOT do

No feature flag creation or change. No runtime scheduling (the shadow is run
manually/one-shot by the operator). No DB write of any result. No allocator
input. No SLO recalibration. No A5 change.

## Estimated shape

Two PRs: (1) snapshot + execution + reports (Tasks 1–5) once the executors are
green locally; (2) review closure + exit-state flip (Task 6) after both owner
signatures. Cloud/QC is NOT involved in Phase 2 (the reproducibility axis here is
host × container; the QC axis was closed in Phase 0Q).

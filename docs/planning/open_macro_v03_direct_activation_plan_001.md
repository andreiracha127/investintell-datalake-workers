# open_macro_v03 — Direct Activation Plan 001

Status: plan. Supersedes Phases 2–3 of
`open_macro_v03_production_activation_stage_plan_001.md` per the owner's recorded
decision (`artifacts/a5/open_macro_v03_direct_activation_001/shadow_elimination_decision_record.json`):
there is no reliable production baseline to shadow against, so the candidate goes to
production directly, with the shadow's baseline-independent protections relocated —
live-data validation BEFORE activation, behavioral observation AFTER activation with
the kill switch armed.

**Evidentiary base (unchanged):** the Phase 1 dark launch readiness package
(`A4=dark_launch_ready`, PR #30): signed thresholds on the `compressed_50` candidate,
go_candidate judgment on all five gates, closed local×cloud reproducibility matrix,
13/13 risks resolved, 4/4 domain reviews signed, executed dry runs, measured SLOs.

## Stage A — Pre-activation live validation (1 PR, no governance change)

The baseline-independent half of the eliminated shadow, as a hard gate:

- **A1. Live input snapshot (read-only):** committed executor exports the CURRENT
  PIT vintages (SEED_SOURCES basket) + sleeve prices as-of the validation date from
  production (read-only), hash-pinned, with the Phase 1 staleness criteria as a
  fail-loud gate at export time.
- **A2. Candidate decision on live data:** the signed candidate (decision chain +
  `compressed_50` sleeve) computes today's consumable position on the snapshot,
  N=8 host + N=8 container under the Phase 1 measurement machinery (enforced
  job-identity determinism, clean-tree/worker-commit/image-id/tree-hash provenance).
- **A3. Gates:** decision is well-formed (fresh or carried with provenance; no
  NaN/Inf; weights within risk_cap/defensive_floor), reproducibility
  host==container (`mismatch_count=0`), SLO conformance vs the Phase 1 thresholds
  (breach = STOP and investigate, never recalibrate).
- **Deliverable:** `artifacts/a5/open_macro_v03_direct_activation_001/`
  `live_validation_record.json` + snapshot manifest + tests (Phase 1 guard
  semantics: regeneration pins, recursive governance walk, duplicate-key rejection,
  string-truthy).

## Stage B — Activation PR (the governance flip + the real runtime + the product)

Per the owner's plan-GO decisions
(`plan_go_decision_record.json`): the allocator IS the product (the regime decision
is its input), it ships in this activation mandatorily, and the old model is
decommissioned at activation — open_macro_v03 becomes the ONLY model.

- **B1. Runtime worker (new production code):** a daily job that (i) reads the PIT
  vintages as-of the run date, (ii) computes the latched decision chain + the
  `compressed_50` consumable position via the SAME pure modules the evidence chain
  used (`src/quadrant_score.py`, harness sleeve semantics — parity by construction),
  (iii) writes the decision row to `open_macro_v03_decisions` (as_of, quadrant,
  decision_validity fresh|carried, carry_provenance, input hashes,
  judgment/threshold refs, code commit) AND the allocation row to
  `open_macro_v03_allocations` (as_of, per-ETF weights of the consumable
  compressed_50 position, cost/risk-cap parameters, provenance) — the allocation
  is the product output — and (iv) refuses to write when inputs breach the
  staleness SLO. Both tables are NEW; the old model's tables are never written.
- **B2. Feature flag:** `open_macro_v03_runtime_activation` created on the worker's
  service only, read at job start; kill switch = set false (procedure already
  dry-run in Phase 1). Absent or false ⇒ the job exits without side effects.
- **B3. Old model decommission:** the incumbent model's scheduled execution is
  switched off at activation (owner decision: practically nonexistent), recorded in
  a `old_model_decommission_record.json` (what was stopped, where, when, by whom,
  how to re-enable in an emergency). Its historical tables remain readable and
  untouched.
- **B4. Governance flip via the documented promotion gates:** new
  `activation_record.json` carrying the final_approver's explicit verbatim act;
  A5 blocked→active for the TWO new tables only; `db_write_mode:
  open_macro_v03_new_tables_only`; `allocator_publish` flips to true FOR THE NEW
  ALLOCATIONS TABLE in as-if mode; `official_result` stays **false until Stage C
  closes** — during the window the pipeline runs exactly as production (single
  model, full output) without the official stamp. Every historical artifact stays
  byte-frozen with its blocked-state pins; the activation state lives in NEW
  artifacts; guard tests are updated through the promotion-gate path the preflight
  package defined, never weakened silently.
- **B5. Active monitoring:** the four measured SLOs + the four zero-threshold
  attempt detectors become live alerts (the DB-write detector's allowlist becomes
  exactly the two new tables); staleness SLO enforcement blocks publication.

## Stage C — Post-activation observation window (as-if production, single model)

- **10 business days** (owner-approved), with open_macro_v03 as the ONLY model
  running: the worker publishes the full pipeline daily (decision + allocation)
  exactly as production, without the official stamp; a committed verifier
  re-computes each published decision AND allocation independently (host, from the
  same PIT inputs) and asserts byte-equality of the logical outputs; SLO and
  staleness alerts on; kill switch armed.
- **Pinned abort criteria:** any verifier mismatch, any NaN/Inf, any staleness
  bypass, any SLO breach, any write outside the two new tables ⇒ kill switch +
  rollback per the dry-run plan; activation is invalidated traceably.
- **Exit:** window report with zero aborts → `official_result: true` for BOTH
  published series (decisions and allocations become the official product),
  `A4=production_active_official`, and consumer cutover to the new tables.

## Owner decisions at plan GO (RESOLVED 2026-07-03, `plan_go_decision_record.json`)

1. **Publication target:** NEW dedicated tables. ✔
2. **Old model:** decommissioned AT activation — practically nonexistent;
   open_macro_v03 is the only model from activation day. ✔
3. **Cadence:** daily consumable position (fresh month-end, carried otherwise). ✔
4. **Observation window:** 10 business days, single model, full as-if-production
   output; official only at clean close. ✔
5. **Allocator:** IN SCOPE, MANDATORY — the allocation IS the product; the regime
   decision is its input. Ships in Stage B, official at Stage C close. ✔

## What never changes in this plan

The evidence chain stays immutable. No threshold moves outside a signed amendment.
Every flag flip is an explicit, recorded human act ratified by a PR merge. The kill
switch and rollback paths — already dry-run — precede any write.

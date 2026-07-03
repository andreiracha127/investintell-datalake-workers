# open_macro_v03 — Direct Activation Plan 001

Status: plan. Supersedes Phases 2–5 of
`open_macro_v03_production_activation_stage_plan_001.md` per the owner's recorded
decision — Stage B of this plan INHERITS Phase 4's role as the ONLY PR that may
flip governance state, including its promotion-gate requirements; Phase 5's
consumer-cutover obligations move to Stage C exit (`artifacts/a5/open_macro_v03_direct_activation_001/shadow_elimination_decision_record.json`):
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

- **A1. Live input snapshot (read-only):** committed executor exports the DELTA of
  PIT vintages (SEED_SOURCES basket) + sleeve prices since the certified pack v2
  cut, as-of the validation date, from production (read-only), hash-pinned, with
  the Phase 1 staleness criteria as a fail-loud gate at export time. The decision
  is computed over **committed pack v2 + pinned delta**, reconstituting the ENTIRE
  latched global chain — so on a non-month-end date the carried position's seed
  (the last valid decision in the chain) is fully pinned with provenance, exactly
  as the ratified carry semantics require. A validation run whose carry seed
  cannot be reconstructed from pinned inputs FAILS.
- **A2. Candidate decision on live data:** the signed candidate (decision chain +
  `compressed_50` sleeve) computes today's consumable position on the snapshot,
  N=8 host + N=8 container under the Phase 1 measurement machinery (enforced
  job-identity determinism, clean-tree/worker-commit/image-id/tree-hash provenance).
- **A3. Gates:** decision is well-formed (fresh or carried with provenance; no
  NaN/Inf; weights within risk_cap/defensive_floor), reproducibility
  host==container (`mismatch_count=0`), SLO conformance vs the Phase 1 thresholds
  (breach = STOP and investigate, never recalibrate).
- **Deliverable:** a NEW stage-scoped artifact root
  `artifacts/a5/open_macro_v03_direct_activation_stage_a_001/` with
  `live_validation_record.json` + snapshot manifest + tests (Phase 1 guard
  semantics: regeneration pins, recursive governance walk, duplicate-key rejection,
  string-truthy). Each stage (A/B/C) creates its OWN artifact directory;
  `open_macro_v03_direct_activation_001/` holds only the two decision records of
  this plan PR and stays immutable.

## Stage B — Activation PR (the governance flip + the real runtime + the product)

Per the owner's plan-GO decisions
(`plan_go_decision_record.json`): the allocator IS the product (the regime decision
is its input), it ships in this activation mandatorily, and the old model is
decommissioned at activation — open_macro_v03 becomes the ONLY model.

- **B1. Runtime worker (new production code):** a daily job that (i) reads the PIT
  vintages AND the sleeve `eod_prices` (`adj_close`, with the certified path's
  data-quality flags) as-of the run date, (ii) computes the latched decision chain + the
  `compressed_50` consumable position via the SAME pure modules the evidence chain
  used (`src/quadrant_score.py`, harness sleeve semantics — parity by construction),
  (iii) writes the decision row to `open_macro_v03_decisions` (as_of, quadrant,
  decision_validity fresh|carried, carry_provenance, input hashes,
  judgment/threshold refs, code commit) AND the allocation row to
  `open_macro_v03_allocations` (as_of, per-ETF weights of the consumable
  compressed_50 position, cost/risk-cap parameters, provenance) — the allocation
  is the product output — and (iv) refuses to write when inputs breach the
  staleness SLO. Both tables are NEW; the old model's tables are never written.
- **B1b. Schema migration with evidence (inherited Phase 4 requirement):** the DDL
  for both new tables is committed, applied through a reviewed migration path, and
  verified against the production DB with a committed
  `schema_migration_record.json` (tables exist, columns/types/constraints match
  the committed DDL, write permissions scoped to the worker role, idempotent
  upsert semantics documented and tested).
- **B2. Feature flag:** `open_macro_v03_runtime_activation` created on the worker's
  service only, read at job start; kill switch = set false (procedure already
  dry-run in Phase 1). Absent or false ⇒ the job exits without side effects.
- **B3. Immediate switch-over (owner decision, `immediate_activation_decision_record.json`):**
  the incumbent producer is switched OFF and the live backend consumer is switched
  ON to the new tables IN THIS SAME PR — no stagnant snapshot, no phased cutover.
  The owner resolved the live-consumer tension explicitly: the incumbent's output
  is worthless, so protecting its reader protects nothing; the first reliable
  model must be consumed from day one. `old_model_decommission_record.json`
  documents what was stopped, where, when, by whom, and the emergency re-enable
  procedure; the incumbent's historical tables remain readable and untouched.
- **B4. Governance flip via the documented promotion gates:** new
  `activation_record.json` carrying the final_approver's explicit verbatim act;
  A5 blocked→active for the TWO new tables only; `db_write_mode:
  open_macro_v03_new_tables_only`; `allocator_publish` flips to true for the new
  allocations table WITH REAL CONSUMPTION from day one (the backend cutover ships
  in this same PR — the owner eliminated the as-if mode); `official_result` is
  **true from activation**: the published decision and allocation ARE the
  system's official output (there is no other model). Every historical artifact stays
  byte-frozen with its blocked-state pins; the activation state lives in NEW
  artifacts; guard tests are updated through the promotion-gate path the preflight
  package defined, never weakened silently.
- **B5. Active monitoring:** the four measured SLOs + the zero-threshold attempt
  detectors become live alerts, RE-SCOPED for the activated state: the DB-write
  detector's allowlist becomes exactly the two new tables, and the
  `allocator_publish_attempt_alert` re-scopes to fire on any allocation publish
  OUTSIDE `open_macro_v03_allocations` (publishing to the sanctioned table is the
  product, not an attempt); `runtime_activation_attempt_alert` and
  `production_endpoint_activation_attempt_alert` re-scope analogously to their
  sanctioned surfaces. The `missing_output_slo` of the inherited monitoring policy
  goes live: every business day must produce BOTH rows (decision + allocation) or
  a recorded staleness-block — anything else alerts. Staleness SLO enforcement
  blocks publication.

## Stage C — Intensive supervision window over LIVE production

- **10 business days** (owner-approved) of intensive supervision over the REAL,
  CONSUMED, OFFICIAL output (the owner eliminated the as-if staging): a committed
  verifier re-computes each published decision AND allocation independently
  (host, from the same PIT inputs) and asserts byte-equality of the logical
  outputs; SLO and staleness alerts on; kill switch armed.
- **Pinned abort criteria:** any verifier mismatch, any NaN/Inf, any staleness
  bypass, any SLO breach, any write outside the two new tables, AND any **missing
  or partial daily output** — every business day of the window must carry BOTH
  rows (decision + allocation) or a recorded staleness-block; a silent worker
  exit or a one-of-two partial write is an abort (the inherited
  `missing_output_slo`), because a verifier that only checks published rows would
  otherwise let absence pass ⇒ kill switch + rollback per the dry-run plan;
  activation is invalidated traceably.
- **Abort fallback posture (explicit, owner-accepted):** an abort fires the kill
  switch and stops the only model; the backend then has no fresh feed. The owner
  accepts this explicitly as a return to the honest status quo — "we have no
  model today" — which he judges strictly better than consuming unreliable
  output. The old model's emergency re-enable stays documented as an OWNER
  OPTION, never an automatic fallback. Every abort invalidates the activation
  traceably.
- **Exit:** window report with zero aborts → `A4=production_active_official`
  (the official stamp was already live from activation; the clean window
  RATIFIES it and closes the intensive-supervision posture into normal
  operations with the same alerts).

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

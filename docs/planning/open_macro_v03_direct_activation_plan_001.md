# open_macro_v03 — Direct Activation Plan 001

Status: plan. Supersedes Phases 2–5 of
`open_macro_v03_production_activation_stage_plan_001.md` per the owner's recorded
decisions — Stage B of this plan INHERITS Phase 4's role as the ONLY PR that may
flip governance state, including its promotion-gate requirements. The owner then
ratified FULL IMMEDIATE ACTIVATION
(`artifacts/a5/open_macro_v03_direct_activation_001/immediate_activation_decision_record.json`),
which supersedes the earlier as-if/phased-cutover shape of this plan: Phase 5's
consumer cutover collapses INTO Stage B (the backend consumes the new tables from
day one) and `official_result` is true from activation, not deferred to Stage C.
(`shadow_elimination_decision_record.json`): there is no reliable production baseline
to shadow against, so the candidate goes to production directly, with the shadow's
baseline-independent protections relocated — live-data validation BEFORE activation,
behavioral observation AFTER activation with the kill switch armed. Stage C is the
intensive-supervision window that RATIFIES the already-official output; it does not
introduce a second governance flip.

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
  `open_macro_v03_direct_activation_001/` holds only the decision records of
  this plan PR and stays immutable.
- **A4. Bounded freshness (binding on Stage B):** `live_validation_record.json`
  carries the validation date and is valid for a MAX AGE of **5 business days**
  before the Stage B governance flip. If Stage B would land after that window,
  Stage A is re-run (fresh snapshot + N=8 host/N=8 container reproducibility + SLO
  gate) and re-pinned first — the first official production write never fires on a
  validation that has aged past the bound. The bound is not purely time-based: even
  INSIDE the 5-day window, if a scheduled month-end decision date falls between the
  `validation_date` and the activation as-of date, Stage A is re-run (or the artifact
  rejected) first — otherwise the first official write would be a FRESH scheduled
  decision the live-validation artifact never recomputed, Stage A having covered only
  the earlier carried/fresh state.

## Stage B — Activation PR (the governance flip + the real runtime + the product)

Per the owner's plan-GO decisions
(`plan_go_decision_record.json`): the allocator IS the product (the regime decision
is its input), it ships in this activation mandatorily, and the old model is
decommissioned at activation — open_macro_v03 becomes the ONLY model.

- **B1. Runtime worker (new production code):** a daily job that (i) reads the PIT
  vintages AND the sleeve `eod_prices` (`adj_close`, with the certified path's
  data-quality flags) as-of the run date on the SAME input basis Stage A validated —
  the certified pack v2 prefix plus a pinned delta since the pack-v2 cut, NOT the raw
  live tables for the full history; the pre-cut prefix read from production is
  hash-compared to pack v2 and the run FAILS LOUD on any mismatch, so a backfill or
  correction of a pre-cut PIT vintage or sleeve-price row can never silently shift the
  input basis away from the one the evidence chain and Stage A certified,
  (ii) computes the latched decision chain + the
  `compressed_50` consumable position via the SAME pure modules the evidence chain
  used (`src/quadrant_score.py` + the full decision-chain module set it composes, and
  the harness sleeve semantics — parity by construction; and "the SAME modules" is
  ENFORCED, not asserted: Stage B pins the sha256 of EACH consumed pure module — the
  decision chain `src/quadrant_score.py`, `src/macro_sources.py` (SEED axis
  specs/weights), `src/quadrant_confidence.py`, `src/quadrant_hysteresis.py`,
  `src/quadrant_assemble.py` (classify/latch/coverage semantics, per
  `harness/phase0q/decision.py`), plus the harness sleeve modules — and restates the
  inherited immutability constraint —
  `formula_changes`/`input_pack_changes`/`calibration_pack_changes`/`contract_v1_changes`
  all `none`, matching the runtime envelope — so a Stage B PR cannot silently alter the
  scoring, confidence, hysteresis, source weights, latch semantics, or sleeve behavior
  and leave Stage A / dark-launch evidence validating a different candidate than
  production consumes),
  (iii) writes the decision row to `open_macro_v03_decisions` (as_of, quadrant,
  decision_validity fresh|carried, carry_provenance, input hashes,
  judgment/threshold refs, code commit) AND the allocation row to
  `open_macro_v03_allocations` (as_of, per-ETF weights of the consumable
  compressed_50 position, cost/risk-cap parameters, provenance) — the allocation
  is the product output — and (iv) refuses to write the decision/allocation when
  inputs breach the staleness SLO, instead writing a durable row to the
  `open_macro_v03_staleness_blocks` ledger (`as_of` + input hashes + reason) so a
  block is a POSITIVE, replayable record rather than an absence. The run follows the
  inherited worker pattern (`src/db.py::connect()` + a DEDICATED `advisory_lock()`
  lock id, like `src/workers/macro_ingestion.py`, registered in `src/run_worker.py`),
  so a Railway retry or an overlapping cron cannot start two `open_macro_v03` runs for
  the same business day and race the decision/allocation publish, the staleness-block
  ledger write, or the invalidation path — at most one run holds the lock and the rest
  exit without side effects. Both tables are NEW; the old model's tables are never
  written.
  Because the backend consumes both tables from day one, the two rows are
  published **atomically**: a single transaction commits decision + allocation
  together (or, where the store cannot span both, each row carries a
  `publish_state` that the backend filter requires to be `published`, and the
  worker only flips both to `published` after both upserts succeed) — a run that
  writes the decision and then fails before the allocation NEVER leaves a
  consumer-visible one-of-two state. Each row also carries a `valid_status`
  (`valid` | `invalidated`) and a `valid_until` field; the sanctioned backend read
  path filters on `valid_status = valid` AND honors as-of freshness — it serves a row
  only while `valid_until` has not lapsed — so a kill-switch abort that stamps the
  latest rows `invalidated` (below) actually removes aborted output from consumers,
  not merely stops future writes. Honoring `valid_until` also closes the
  staleness-block hole: on a fresh business day the daily write renews `valid_until`,
  but when B1 refuses to write because inputs breach the staleness SLO (B5/Stage C
  record a staleness-block rather than abort), no new row lands and the previous
  row's `valid_until` lapses, so the reader stops serving the now-stale allocation
  instead of leaving it readable as current official output — recording a
  staleness-block thereby expires the last rows rather than freezing them `valid`.
- **B1b. Schema migration with evidence (inherited Phase 4 requirement):** the DDL
  for both new output tables AND a durable `open_macro_v03_staleness_blocks` ledger
  (one row per blocked day: `as_of`, input hashes, reason, worker commit) is
  committed, applied through a reviewed migration path, and verified against the
  production DB with a committed `schema_migration_record.json` (tables exist,
  columns/types/constraints match the committed DDL, write permissions scoped to the
  worker role, idempotent upsert semantics documented and tested). The ledger is what
  makes a "recorded staleness-block" durable: the Stage C verifier and the
  `missing_output_slo` read it to tell an INTENTIONAL block (a ledger row for that
  `as_of`) apart from a silent worker exit (no output rows AND no ledger row ⇒ abort).
- **B2. Feature flag:** `open_macro_v03_runtime_activation` created on the worker's
  service only, read at job start; kill switch = set false (procedure already
  dry-run in Phase 1). Absent or false ⇒ the job exits without side effects.
- **B3. Immediate switch-over (owner decision, `immediate_activation_decision_record.json`):**
  the incumbent producer is switched OFF and the live backend consumer is switched
  ON to the new tables as ONE coordinated activation — no stagnant snapshot, no
  phased cutover. The owner resolved the live-consumer tension explicitly: the
  incumbent's output is worthless, so protecting its reader protects nothing; the
  first reliable model must be consumed from day one. The live reader is in the
  separate `investintell-light-combo` backend, which this datalake-workers PR
  cannot modify or review, so the switch-over is a COORDINATED cross-repo change.
  Because a committed record in THIS repo cannot enforce a foreign-repo merge order —
  and cannot truthfully attest a first verified row that only exists AFTER this very
  PR creates the worker/tables and fires the first sanctioned write — the cutover
  evidence is SPLIT across the merge boundary into two records:
  (a) PRE-MERGE, committed in the Stage B PR: `backend_flag_inert_record.json` pins
  the merged (or approved) backend PR with its new read route FLAG-GATED and INERT —
  deployed but not yet serving from the new tables, still safe on the old snapshot —
  so backend-merge ORDER is irrelevant and an early backend merge exposes no live
  reader to absent or empty tables; and
  (b) POST-MERGE, created only after Stage B lands and the worker has written AND
  verified the first sanctioned row: `backend_cutover_record.json` records flipping
  the backend flag to serve the NAMED route from
  `open_macro_v03_decisions`/`open_macro_v03_allocations` and the confirmation it no
  longer reads `regime_quadrant_snapshot`.
  The incumbent producer is NOT decommissioned until that POST-MERGE cutover record
  exists — so the backend is never changed out of band, never left reading the old
  snapshot after switch-off, and never pointed at tables before their first verified
  row. `old_model_decommission_record.json`
  documents what was stopped, where, when, by whom, and the emergency re-enable
  procedure; the incumbent's historical tables remain readable and untouched.
- **B4. Governance flip via the documented promotion gates:** new
  `activation_record.json` carrying the final_approver's explicit verbatim act AND
  the FULL inherited Phase 4 approval matrix — explicit sign-off from all six owner
  roles by their EXACT pinned ids (`technical_owner`, `quant_owner`, `risk_owner`,
  `operations_owner`, `product_portfolio_owner`, `final_approver` — the set pinned by
  `tests/test_dark_launch_readiness.py` / `tests/test_controlled_activation_proposal.py`;
  the matrix uses `technical_owner`, never `engineering` or any unrecognized role, so
  `approval_matrix_complete: true` can never be claimed while the technical-owner
  sign-off is missing)
  with `approval_matrix_complete: true`, even where one person holds several roles
  (each role named against its holder); an absent or stale approval matrix blocks
  the flip. A5 blocked→active for the TWO new tables only; `db_write_mode:
  open_macro_v03_new_tables_only` — and, because the inherited runtime envelope
  hard-blocks productive writes on `allow_db_write=false`/`db_write_official=false`
  independently of `db_write_mode`, those companion gates flip in the SAME activation
  envelope: `allow_db_write` true scoped to exactly the two output tables AND the
  `open_macro_v03_staleness_blocks` ledger (the worker must write the ledger on a
  staleness-block day, so it is in the sanctioned write scope — otherwise the block
  write would be blocked or flagged and the verifier could not tell an intentional
  block from a silent exit) and `db_write_official` true (flipping `db_write_mode`
  alone would leave the worker still forbidden to write); `activation_allowed` flips to true with the
  NAMED allowed environment (exactly the production worker service — the
  inherited feature-flag envelope requires named environments, never a blanket
  true); `open_macro_v03_runtime_activation` (the B2 job-start feature flag) is set
  true for that SAME named worker service, sequenced AFTER B5 monitoring is live and
  BEFORE the first sanctioned write — without this flip the worker exits without side
  effects while the backend is already switched to official output, so only the
  `missing_output_slo` would fire; `allocator_publish` flips to true for the new
  allocations table together with its companion `allow_allocator_publish` gate scoped
  to `open_macro_v03_allocations` (the envelope hard-blocks publication on
  `allow_allocator_publish=false` independently of `allocator_publish`), with the
  backend cutover AUTHORIZED and landing with this PR (executed at the post-merge
  cutover — no as-if mode, no deferral to Stage C); `production_endpoint_activation`
  STAYS `none` at the Stage B flip, because the B3 split leaves the backend route
  flag-gated/inert until the POST-MERGE `backend_cutover_record.json` (first verified
  row) flips it — recording the endpoint active here would certify a false state.
  Stage B AUTHORIZES the named read path; the post-merge cutover record EXECUTES that
  authorization (the execution of the Stage B activation, NOT a new governance
  decision or a Stage C flip), moving `production_endpoint_activation` from `none` to
  the NAMED read path (scoped, never a blanket value) and re-scoping the
  `production_endpoint_activation_attempt_alert` at that same point; `official_result`
  is **true from activation**: the published decision and allocation ARE the system's
  official output (there is no other model); and A4 advances to
  `production_active_official`
  in THIS Stage B flip (official from activation) — Stage C does not re-flip it. Every historical artifact stays
  byte-frozen with its blocked-state pins; the activation state lives in NEW
  artifacts; guard tests are updated through the promotion-gate path the preflight
  package defined, never weakened silently.
- **B5. Deploy evidence + monitoring BEFORE first write (ordering is binding):**
  the worker is registered and deployed on its production service with committed
  deploy evidence (`deploy_record.json`: service, image/commit, schedule, env,
  flag state), and the monitoring below is LIVE AND VERIFIED **before** the B4
  governance flip enables the first sanctioned write — the first production row
  must land under full alerting, never before it. The four measured SLOs + the
  zero-threshold attempt detectors become live alerts, RE-SCOPED for the
  activated state: the DB-write
  detector's allowlist becomes exactly the two output tables PLUS the
  `open_macro_v03_staleness_blocks` ledger (the sanctioned staleness-block write), and the
  `allocator_publish_attempt_alert` re-scopes to fire on any allocation publish
  OUTSIDE `open_macro_v03_allocations` (publishing to the sanctioned table is the
  product, not an attempt); `runtime_activation_attempt_alert` re-scopes analogously
  to its sanctioned surface, and `production_endpoint_activation_attempt_alert`
  re-scopes only when the POST-MERGE cutover record flips the route live — not at the
  Stage B flip, when the endpoint is deliberately still `none`. The `missing_output_slo` of the inherited monitoring policy
  goes live: every business day must produce BOTH rows (decision + allocation) or
  a recorded staleness-block — anything else alerts. Staleness SLO enforcement
  blocks publication.

## Stage C — Intensive supervision window over LIVE production

- **10 business days** (owner-approved) of intensive supervision over the REAL,
  CONSUMED, OFFICIAL output (the owner eliminated the as-if staging): a committed
  verifier re-computes each published decision AND allocation independently
  (host, from the same PIT vintages AND the same sleeve `eod_prices`/quality
  flags the worker read) and asserts byte-equality of the logical outputs; SLO
  and staleness alerts on; kill switch armed. The count STARTS only once the
  POST-MERGE `backend_cutover_record.json` has flipped the backend route live (B3):
  days on which rows are written while the route is still inert are
  verified-but-not-consumed and do NOT count, so Stage C always exercises ten business
  days of the REAL, CONSUMED backend path, not merely ten days of published rows. The
  10 days are 10 days of
  ACTUALLY-VERIFIED output: a business day that records a staleness-block (no
  decision/allocation rows to replay) does NOT count toward the window and does
  NOT satisfy the zero-abort exit — it PAUSES and EXTENDS the count until both rows
  are again published and verified. The window closes only after ten distinct
  business days each carried both rows and passed the verifier.
- **Pinned abort criteria:** any verifier mismatch, any NaN/Inf, any staleness
  bypass, any SLO breach, any write outside the two new tables, AND any **missing
  or partial daily output** — every business day of the window must carry BOTH
  rows (decision + allocation) or a recorded staleness-block (a durable
  `open_macro_v03_staleness_blocks` ledger row for that `as_of`, with input hashes);
  a silent worker exit or a one-of-two partial write — no rows AND no ledger row — is
  an abort (the inherited
  `missing_output_slo`), because a verifier that only checks published rows would
  otherwise let absence pass ⇒ kill switch + rollback per the dry-run plan;
  activation is invalidated traceably. **Reader-enforceable invalidation:** an
  abort does not merely stop future writes — it stamps the affected published rows
  `valid_status = invalidated` (with `valid_until` set), and because the sanctioned
  backend read path filters on `valid_status = valid` (B1), the aborted output is
  actually removed from consumers rather than left readable as current. The
  kill-switch + invalidation stamp is itself part of the dry-run rollback plan.
- **Abort fallback posture (explicit, owner-accepted):** an abort fires the kill
  switch and stops the only model; the backend then has no fresh feed. The owner
  accepts this explicitly as a return to the honest status quo — "we have no
  model today" — which he judges strictly better than consuming unreliable
  output. The old model's emergency re-enable stays documented as an OWNER
  OPTION, never an automatic fallback. Every abort invalidates the activation
  traceably.
- **Exit:** window report with zero aborts. A4 is ALREADY
  `production_active_official` — it was set at the Stage B governance flip (official
  from activation) — so this exit performs NO second governance flip: it RATIFIES the
  already-official output and closes the intensive-supervision posture into normal
  operations (same alerts). The clean-window report is an operational close, not a
  governance-state transition — consistent with the header rule and the pinned
  `plan_go_decision_record.json` note that no flag ever flips outside the Stage B
  activation PR.

## Owner decisions at plan GO (RESOLVED 2026-07-03, `plan_go_decision_record.json`)

1. **Publication target:** NEW dedicated tables. ✔
2. **Old model:** decommissioned AT activation — practically nonexistent;
   open_macro_v03 is the only model from activation day. ✔
3. **Cadence:** daily consumable position (fresh month-end, carried otherwise). ✔
4. **Observation window:** 10 business days, single model, over REAL, CONSUMED,
   OFFICIAL production output (the immediate-activation decision eliminated the
   as-if staging — `official_result` is true from activation, and the clean-close
   RATIFIES it rather than first conferring it). ✔
5. **Allocator:** IN SCOPE, MANDATORY — the allocation IS the product; the regime
   decision is its input. Ships in Stage B and is official from activation. ✔

## What never changes in this plan

The evidence chain stays immutable. No threshold moves outside a signed amendment.
Every flag flip is an explicit, recorded human act ratified by a PR merge. The kill
switch and rollback paths — already dry-run — precede any write.

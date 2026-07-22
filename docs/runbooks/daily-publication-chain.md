# Runbook — Daily publication chain

Increment 2, Task 6. Frozen contract: `.superpowers/sdd/frozen-contract-spec.md` §5.
Engine: `src/bonds/daily_chain.py`. Worker entry point:
`src/workers/daily_publication_chain.py`. Bookkeeping DDL: `schemas/bond_daily_chain.sql`.

The chain **orchestrates** the existing, review-approved publication workers — it
reimplements none of them. It holds one chain-run advisory lock for the whole
run, drives the eight frozen stages per eligible source-day, checkpoints every
stage, and writes exactly ONE summary per run.

> **No deploy in this deliverable.** No cron/schedule/deploy file is created. The
> scheduling path is verified live at the future, explicit activation (see below).

---

## Stages (spec §5, binding order)

| # | Stage        | Orchestrates (existing worker/mechanism)                         |
|---|--------------|-----------------------------------------------------------------|
| 1 | `ingest`     | `ncen_ingestion` / `rr1_ingestion` / `nport_ingestion` (raw landing; `sec_ingestion_runs`/`sec_source_packages` state machine) |
| 2 | `pit_update` | `bond_security_master` + `bond_price_observations` (immutable `*_observation` PIT) |
| 3 | `materialize`| `ncen_derived_profiles` + `rr1_derived_profiles` (derived publications) |
| 4 | `mixed_build`| `mixed_quant_publication` (builds the inactive publication; promote is separate) |
| 5 | `validate`   | read-only reconciliation of current derived pointers (`sec_derived_publication_is_validated`) |
| 6 | `promote`    | atomic promotion of the ready `mixed_quant_v1` publication (`promote_quant_publication`); the derived/bond products self-promote atomically inside their own stages via `sec_set_current_derived_publication` |
| 7 | `refresh`    | `sec_regulatory_serving` + `bond_serving` serving projections |
| 8 | `probe`      | read-only smoke over current pointers |

The individual materializers (bond security/price, N-CEN/RR1 derived, serving)
each build→validate→promote atomically through the derived-publication protocol,
so stages 5–7 for those products are satisfied inside their stage; the explicit
`promote` stage handles only `mixed_quant_v1`, which separates build from promotion.

## Required behaviors (all testable without deploy)

- **Advisory lock** `LOCK_DAILY_PUBLICATION_CHAIN = 900_344` (`src/db.py`,
  ingestion band 900_3xx) is held session-level for the entire run, so two chain
  runs can never interleave stages. It sits ABOVE the per-worker locks; each
  invoked worker still takes its own lock.
- **Deterministic run identity** `uuid5(chain, source_day, code_revision,
  config_version)` (`chain_run_id_for`, mirroring `publication_id_for`). A replay
  resolves to the same `run_id` and never forks a second run row.
- **Watermarks per source**: the engine supports per-source input watermarks via
  the `watermarks_for` hook (persisted to `input_watermarks`). No production wiring
  passes it yet, so today `input_watermarks` is `{}`; the real wiring plus a
  staleness metric/alert land at the pre-activation gate (see "Scheduling path" and
  the deferred pre-activation items).
- **Per-stage checkpoints** (`bond_daily_chain_stage_runs`): a restart honours a
  stage already at `succeeded`/`skipped` and resumes at the first unfinished
  stage. Replay of a `completed` run re-invokes nothing.
- **Bounded retries with backoff** for *transient* failures (typed
  `TransientStageError`, plus psycopg `OperationalError`/`InterfaceError`);
  *terminal* failures (`TerminalStageError`, coverage/integrity/contract errors,
  anything unrecognised) fail closed immediately with no retry.
- **Typed quarantine for a bad source row** is delegated to the underlying
  ingestion workers (`sec_source_packages` quarantine state machine); the chain
  records the stage outcome and does not re-implement quarantine.
- **Deterministic catch-up**: eligible source-days are processed in ascending
  order, same mechanism, no special mode. Already-completed days are replayed as
  no-ops.
- **"A partial run never stays current" by COMPENSATION**: each product
  self-promotes atomically per-publication inside its own stage (the Inc.1-approved
  architecture — `pit_update`, `materialize` and the serving `refresh` products
  promote through `sec_set_current_derived_publication`; `mixed_quant_v1` through
  `promote_quant_publication`; "partial never becomes current" holds
  per-publication). If a LATER stage fails terminally, the chain rolls back EVERY
  product it advanced during THIS run to its pre-run pointer, so the stable
  post-failure state is "no product advanced". See "Compensation & visibility"
  below — this replaces any notion that a mid-chain failure "promoted nothing".
- **Per-product promotion enumeration**: the summary's `promoted` lists every
  product the run made current (`product`, `previous_publication_id` →
  `publication_id`, `stage`) plus a `promoted_count`. It is `[]` on a failed run
  (all advances were compensated).
- **Pointer rollback available**: `daily_chain.rollback_pointer(conn, product)`
  restores the product's prior current pointer via the atomic, validated-only
  `sec_set_current_derived_publication`, using the promotion ledger
  (`bond_daily_chain_promotions`). The automatic compensation above uses the same
  ledger; this manual entry point is the operator fallback.
- **One summary per run** (`bond_daily_chain_runs.summary`) with per-stage
  status/timings/attempts/detail/watermarks, a `promoted` enumeration, a
  `compensations` list, a `compensation_failures` list, `skips`/`failures` lists,
  and a single actionable `alert` line on failure/compensation/dark mode.

## Skip vs. failure (the one distinction that matters)

`estágio requerido pulado = falha do run` is reconciled with dark mode by an
explicit allow-list — `ALLOWED_SKIP_REASONS = {dark_no_source, input_unchanged}`:

- **`dark_no_source`** — the underlying worker no-oped because there is **NO
  AUTHORIZED SOURCE** (`no_source`/`no_observations`/`no_securities`/…). This is a
  **REPORTED skip** (dark mode), surfaced in the summary + `alert`, never a silent
  success and never a failure.
- **`input_unchanged`** — the stage's inputs match the current pinned fingerprint,
  so the product is deliberately not rebuilt. Reported skip.
- **Any other skip of a required stage** (e.g. an unexpectedly missing stage
  result) is upgraded to a **terminal failure** `required_stage_skipped` — the run
  fails, nothing is promoted.

Under Global Constraint 9 (no production source authorized; fixtures only) every
stage currently reports `dark_no_source` and the run completes in DARK mode with
nothing promoted (in dark mode there is genuinely nothing to promote). That is the
expected steady state until activation.

## Compensation & the safe visibility window

Because each product self-promotes atomically inside its stage, a terminal failure
in a LATER stage can occur *after* one or more products were already made current.
The chain closes this at the run level by **compensation**:

1. Around each stage the chain snapshots the current pointers (`sec_derived_current_pointers`
   + `active_quant_publication_v1`) and enumerates what the stage made current.
2. On a terminal failure it rolls each advanced product back to its **pre-run**
   pointer (derived via `sec_set_current_derived_publication`; mixed via
   `promote_quant_publication`), recording each restoration in
   `summary.compensations` + the ledger. Stable post-failure state = **no product
   advanced**.
3. The advancing stages' checkpoints are reset, so a **restart** re-executes
   (re-promotes) them consistently rather than resuming past an undone promotion.
4. A rollback that itself fails is recorded in `summary.compensation_failures` and
   raises a **distinct, louder `alert`** (`COMPENSATION FAILED … MANUAL rollback
   required`) — never silence.

**Visibility window is safe by design.** App readers pin an **exact
`publication_id`** (families/bonds repositories read a pinned publication, never a
live "current" resolution for serving a request). The only consumer of the
"current" pointer is the next publication cycle. So the brief interval between an
auto-promotion and its compensation is invisible to readers — no request is served
from a to-be-rolled-back pointer.

### Manual per-product fallback

If `compensation_failures` is non-empty (or you must intervene), roll back each
at-risk product by hand:

```python
from src.bonds import daily_chain
from src.db import connect
with connect(DSN) as conn:
    daily_chain.rollback_pointer(conn, "<product>")   # restore prior current
    conn.commit()
```

Products at risk **by stage** (what each stage can make current, so what to check
after a terminal failure downstream of it):

| Advancing stage | Products it can make current |
|---|---|
| `pit_update`  | `bond_security_v1`, `bond_price_observation_v1` |
| `materialize` | `ncen_*` derived profiles, `rr1_*` derived profiles |
| `promote`     | `mixed_quant_v1` (active pointer) |
| `refresh`     | `sec_regulatory_serving_v1`, `bond_serving_v1` |

Cross-check `summary.promoted` and `summary.compensations` against this table to
confirm every advanced product is back to its prior pointer.

---

## Operating procedures

Run (holds the lock for the whole run):

```
WORKER=daily_publication_chain python -m src.run_worker
# or a single pinned day:
python -m src.run daily_publication_chain --calc-date YYYY-MM-DD
```

Result envelope: `{"state": "ok"|"failed"|"locked"|"no_source_days", "runs": [<summary>...]}`.

### Failure modes

| Symptom | Meaning | Action |
|---|---|---|
| `state=locked` | another chain run holds `900_344` | wait; do not force. Overlap is prevented by design |
| a stage `status=failed`, `classification=terminal`, `compensations` non-empty | coverage/integrity/contract gate tripped (e.g. `BondServingSurfaceCoverageError`, `BondFundExposureMultiplicationError`, `required_stage_skipped`) after some products were promoted | the chain already rolled every advanced product back to its prior pointer (check `summary.compensations`); stable state = no product advanced. Fix inputs, then **restart** (below) |
| `alert` contains `COMPENSATION FAILED` / `compensation_failures` non-empty | a self-promoted product could NOT be auto-rolled-back | **act now**: roll back each listed product with the manual per-product fallback above, then restart |
| a stage `status=failed`, `classification=transient`, `attempts=max` | transient (DB blip / busy sub-lock) exhausted its retries | re-run the same command; the checkpoint resumes at that stage |
| all stages `status=skipped`, `reason=dark_no_source` | dark mode: no authorized source | expected pre-activation; nothing to do |

### Restart (idempotent)

Re-run the identical command. The deterministic `run_id` resolves to the same
run; stages already `succeeded`/`skipped` are honoured from their checkpoints and
the run resumes at the first unfinished stage. A `completed` run is a no-op.

### Catch-up

Run without `--calc-date`. Eligible watermark days are discovered and processed
in ascending order under the same mechanism; already-completed days are skipped.

### Pointer rollback

```python
from src.bonds import daily_chain
from src.db import connect
with connect(DSN) as conn:
    daily_chain.rollback_pointer(conn, "bond_serving_v1")   # restore prior current
    conn.commit()
```

Restores the product's immediately-prior current pointer (recorded at promotion
in `bond_daily_chain_promotions`). Raises if there is no prior target (the current
pointer was the product's first publication). The prior publication is immutable
and validated, so it is always restorable.

---

## ACTIVATION NOTE (inherited — read before the first live serving build)

- **ETF snapshots must be rebuilt with post-`f38544a` code BEFORE the first
  serving build.** The current-pointer ETF snapshots produced before commit
  `f38544a` are stale relative to the serving projection; a serving build over
  them would publish stale ETF facts. Rebuild the ETF snapshots on the
  post-`f38544a` code first, then let the chain's `refresh` stage run.
- **No production price/holdings source is authorized** (the 144A pricing pilot
  authorizes none). Until a source is authorized, activation keeps the chain in
  dark mode.
- **Set a build stamp for `code_revision`.** The run identity includes
  `code_revision`; the worker reads it from `CODE_REVISION` / `GIT_SHA` /
  `SOURCE_COMMIT` / `RAILWAY_GIT_COMMIT_SHA` (in that order) before falling back to
  `git rev-parse`, and only then to the literal `"unknown"`. Containers usually
  lack a git checkout, so inject one of those env vars at deploy time — otherwise
  every run identity collapses onto `code_revision="unknown"` and a genuine code
  change will not mint a fresh run for the same source-day.

## Scheduling path (verify LIVE at activation — do not schedule now)

Production runs on **Google Cloud** (project `investintell-research-analisys`,
region `southamerica-east1`); the current scheduling path per repository state is
the GCP production path. **The Railway dashboard is stale and is NOT the
authority.** At activation, verify the live scheduling wiring against GCP
(`gcloud`/Cloud Run/Cloud Scheduler) before creating any schedule. This delivery
creates **no** cron/schedule/deploy artifact.

## Tests

`tests/test_daily_publication_chain.py` (engine: lock/replay/restart/catch-up/
retry-backoff/terminal-fail-closed/skip-vs-failure/one-summary/rollback/
per-product promotion enumeration/multi-product compensation on terminal
mid-chain failure/compensation-failure alerting) and
`tests/test_daily_publication_chain_wiring.py` (stage order, worker-result
classification, ingest dark handling, lock-overlap via the worker, real bond-lane
dark smoke). DSN-agnostic; run against the disposable PG:

```
SEC_TEST_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:65431/postgres \
  python -m pytest tests/test_daily_publication_chain.py tests/test_daily_publication_chain_wiring.py -q
```

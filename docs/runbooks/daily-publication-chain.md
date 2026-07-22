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
- **Watermarks per source** recorded on the run row (`input_watermarks`).
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
- **Promotion only of a complete run**: a failing stage aborts the run BEFORE the
  `promote`/`refresh` stages are reachable, so a partial build never becomes
  current and the prior current pointer stays intact.
- **Pointer rollback available**: `daily_chain.rollback_pointer(conn, product)`
  restores the product's prior current pointer via the atomic, validated-only
  `sec_set_current_derived_publication`, using the promotion ledger
  (`bond_daily_chain_promotions`) recorded by `promote_derived`.
- **One summary per run** (`bond_daily_chain_runs.summary`) with per-stage
  status/timings/attempts/detail/watermarks, a `skips` list, a `failures` list,
  a `promoted` flag, and a single actionable `alert` line on failure/dark mode.

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
nothing promoted. That is the expected steady state until activation.

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
| a stage `status=failed`, `classification=terminal` | coverage/integrity/contract gate tripped (e.g. `BondServingSurfaceCoverageError`, `BondFundExposureMultiplicationError`, `required_stage_skipped`) | investigate the source data; the run promoted NOTHING and the prior pointer is intact. Fix inputs, then **restart** (below) |
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
- **No production price/holdings source is authorized** (the TRACE 144A pilot
  authorizes none). Until a source is authorized, activation keeps the chain in
  dark mode.

## Scheduling path (verify LIVE at activation — do not schedule now)

Production runs on **Google Cloud** (project `investintell-research-analisys`,
region `southamerica-east1`); the current scheduling path per repository state is
the GCP production path. **The Railway dashboard is stale and is NOT the
authority.** At activation, verify the live scheduling wiring against GCP
(`gcloud`/Cloud Run/Cloud Scheduler) before creating any schedule. This delivery
creates **no** cron/schedule/deploy artifact.

## Tests

`tests/test_daily_publication_chain.py` (engine: lock/replay/restart/catch-up/
retry-backoff/terminal-fail-closed/skip-vs-failure/one-summary/partial-promotion/
rollback) and `tests/test_daily_publication_chain_wiring.py` (stage order,
worker-result classification, ingest dark handling, lock-overlap via the worker,
real bond-lane dark smoke). DSN-agnostic; run against the disposable PG:

```
SEC_TEST_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:65431/postgres \
  python -m pytest tests/test_daily_publication_chain.py tests/test_daily_publication_chain_wiring.py -q
```

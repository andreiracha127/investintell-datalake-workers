# Runbook: return-lineage flag repair lane

## What it does

NAV rows written before the writer fix carry `return_source_boundary` with a
NULL `return_1d` and no return lineage, so readiness cannot verify them. The
operator `scripts/repair_nav_return_lineage_v1.py` (library
`src/workers/nav_return_lineage_repair.py`) confirms each such row against its
own provider (Tiingo or Yahoo, one request per bad row) and clears the flag
with an attributed revision, one transaction per instrument. Provider levels
are never written.

`scripts/repair_nav_return_lineage_cohort.py` drives that library in-process
over the whole queue. On `maintenance-runner` it runs as the `run_worker` lane
`WORKER=nav_return_lineage_repair_lane`, next to the database: from a laptop
each instrument costs about 6 to 13 s of proxy round trips, and production has
about 9,600 instruments.

1. **Plan.** Schema check, then one read-only REPEATABLE READ plan. Items
   without a plan reason are eligible; the rest are reported, never submitted.
2. **Limits.** Batch 20. `max_requests` (a per-batch request budget) is the
   largest bad-row count of an item to apply, at least 20 and at most 10,000.
   Allowlists hold at most 20 instruments whose bad rows fit `max_requests` and
   half of `NAV_LINEAGE_REPAIR_MAX_SECONDS` at the configured rate.
3. **Second pass.** Instruments skipped as `PLAN_STALE` are re-planned once
   and retried, only after a pass that committed something.
4. **Stops.** Operator exit 3, exit 4 (exit 5 once the run has written), exit
   5 (`TIME_BUDGET`, `REQUEST_BUDGET`, `PROVIDER_UNAVAILABLE`,
   `PROVIDER_BUDGET`, `INTERRUPTED`, `LOCK_BUSY` after work), `CLOCK_SKEW`,
   `COMMIT_UNKNOWN`, or 3 failed batches in a row. Earlier commits stay.
5. **Recount.** `remaining_bad_rows` re-counts the bad predicate, read-only.

## When to run

- Deploy the writer fix first.
- Run **before** `nav-current-daily-chain`: when no run is pinned, readiness
  picks the latest attempt by `attempted_at`, and a repair attempt (historical
  `requested_end`) must not become the latest after the day's ingestion.
- Run **shortly before a full risk run**: historical revisions invalidate
  pinned risk publications.
- Never overlap ingestion or the daily chain (writer locks: `LOCK_BUSY`).
- The Tiingo budget is shared; the default rate is 1.0 request/s. Pacing
  alone takes `to_apply_rows / rate` seconds (see a dry run's plan line), so
  pick a window that ends before the daily chain.

## Service variables (`maintenance-runner`)

| variable | validate canary | real canary | full run |
|---|---|---|---|
| `WORKER` | `nav_return_lineage_repair_lane` | same | same |
| `NAV_LINEAGE_REPAIR_CONFIRM` | `nav_return_lineage_repair_v1` | same | same |
| `NAV_LINEAGE_REPAIR_ALLOW_DATABASE_URL` | `1` | `1` | `1` |
| `TIINGO_API_KEY` | the key | the key | the key |
| `WORKER_LIMIT` | `20` | `20` | unset |
| `NAV_LINEAGE_REPAIR_VALIDATE_ONLY` | `1` | unset | unset |

`NAV_READINESS_DATABASE_URL`, when set, wins over `DATABASE_URL`. Optional:
`NAV_LINEAGE_REPAIR_DRY_RUN=1` (plan only, no HTTP),
`NAV_LINEAGE_REPAIR_MAX_BATCHES`, `NAV_LINEAGE_REPAIR_RATE_PER_SECOND`
(default 1.0, at most 2.5), `NAV_LINEAGE_REPAIR_MAX_SECONDS` (per batch,
default 900), `NAV_LINEAGE_REPAIR_SCHEMA` (default `public`). Without the
confirmation the lane refuses with `CONFIRMATION_REQUIRED` and touches nothing.

Set the variables, **redeploy** (captures the env), then **deploymentRestart**
(runs it once).

## Procedure

1. **Validate canary.** Expect a `plan` line, one `batch` line and the summary
   with `validated_instruments` near 20 and nothing written. Read
   `skipped_rows_by_code` and the plan line's `not_eligible_rows_by_reason`.
2. **Real canary.** Unset `NAV_LINEAGE_REPAIR_VALIDATE_ONLY`. Check
   `committed_instruments`, then:

   ```sql
   SELECT count(*) FROM nav_ingestion_runs
   WHERE reason_code = 'RETURN_LINEAGE_REPAIR'
     AND started_at > now() - interval '1 hour';
   ```
3. **Full run.** Unset `WORKER_LIMIT`. After a stop, fix the cause and
   restart: repaired rows are no longer planned. A `TIME_BUDGET` stop on a
   one-instrument batch means that instrument has more bad rows than one batch
   can pace: raise `NAV_LINEAGE_REPAIR_MAX_SECONDS` above its rows / rate (the
   plan line's `max_requests` is the largest item), or every rerun stops there.
4. **Close out.** Unset `NAV_LINEAGE_REPAIR_CONFIRM` and the other
   `NAV_LINEAGE_REPAIR_*` variables and `WORKER_LIMIT`, and set `WORKER` back.
   The daily chain republishes readiness (`readiness_republish_required` is
   always `false` here).

## Reading the summary

`state` is `complete` only when every submitted instrument was committed,
validated or already repaired. Residual skips, failures and stops give
`failed` (stops also `aborted: true`); a refusal before any batch gives
`blocked`. So a full run that leaves residuals ends red by design: review them,
do not retry them.

Key fields: `committed_instruments`/`committed_rows`, `validated_*`,
`skipped_rows_by_code`, `failed_by_code`, `unknown_instruments`, `stop_code`,
`remaining_instruments` (eligible, never finished) and `remaining_bad_rows`.
A `COMMIT_UNKNOWN` batch line names the instrument and its run ids: check
whether those runs committed before restarting.

Residual codes (rows, never written):

| code | meaning |
|---|---|
| `OPEN_REEXPRESSION` | unresolved reexpression hold; resolve it first |
| `MISSING_TICKER`, `INACTIVE_INSTRUMENT` | no usable identity |
| `NULL_KIND`, `UNSUPPORTED_KIND` | row has no supported `source_nav_kind` |
| `UNSUPPORTED_SOURCE`, `UNSUPPORTED_FLAG` | source not Tiingo/Yahoo, or the flag is not `true` |
| `INVALID_STORED_LEVEL` | stored `source_nav` missing or not positive |
| `RETURN_RECOMPUTE_REQUIRED` | the return must be recomputed, not just the flag cleared |
| `PROVIDER_DATE_MISSING`, `PROVIDER_NOT_FOUND`, `PROVIDER_INVALID_PAYLOAD` | provider has no usable observation for that date |
| `KIND_MISMATCH`, `LEVEL_MISMATCH` | provider kind or level differs from the stored row |
| `PROVIDER_INVALID_TIMESTAMPS` | provider attempt timestamps unusable |
| `PLAN_STALE` | the row or its neighbours moved, still after the re-plan |
| `UNATTRIBUTED_CLEAR` | cleared by something else without attribution; investigate |
| `REQUEST_BUDGET_TOO_SMALL` | more than 10,000 bad rows in one instrument |

The first six rows are plan reasons (`not_eligible_rows_by_reason`); the rest
are apply skips (`skipped_rows_by_code`).

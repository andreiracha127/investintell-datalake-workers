# Runbook: governed NAV rebase of the readiness cohort

## Why this exists

Readiness admits a fund only with 401 consecutive due sessions whose NAV rows
carry full provenance: `source_nav_kind`, `nav_repair_kind`, the calendar
stamps, the return semantics/verification/boundary flags, and one source. NAV
history written before 2026-09-25 has none of that, and some windows mix
`yahoo` rows. Until those windows are rebased, the fund builder admits zero
funds.

The governed fix is the existing operator `scripts/rebase_fund_nav_window.py`
(library `src/workers/nav_economic_rebase.py`). It fetches one adjusted Tiingo
full-window snapshot per instrument and reconciles each instrument in its own
transaction, with a receipt. It works in batches of at most 20 instruments and
needs a plan file plus its SHA256 for every apply.

`scripts/rebase_fund_nav_cohort.py` drives that operator over the whole cohort.
It calls the operator's `main(argv)` in-process and leaves it unchanged. On
`maintenance-runner` it runs as the `run_worker` lane `WORKER=nav_rebase_cohort`.

## Why it must run inside Railway

A plan does about 10 sequential queries per ACTIVE fund and 2 per inactive
fund, and `funds_profile_mv` holds about 8,300 candidates. Measured through the
public proxy on 2026-10-06, the median round trip was 210 ms. A 5-fund plan
took 26 s (101 statements, about 14 s of it the fixed schema check). Each extra
fund added about 10.5 statements and 2.4 s. Over the full cohort that comes to
about 41,000 statements, roughly 2.5 hours of round trips before any apply. On
the private network a round trip is under 1 ms, so the same plan should take a
few minutes.

## What the driver does

1. **Plan.** Builds one read-only plan over the cohort. The budgets are pinned
   in the plan: batch = max-instruments = max-requests = 20,
   `NAV_REBASE_MAX_SECONDS` per batch (default 600), and `NAV_REBASE_RATE_PER_SECOND`
   (at most 2.5, the default). It checks that the plan file's SHA256 equals
   the hash the operator reported.
2. **Apply.** Sends allowlists of at most 20 funds, in plan order. Every apply
   uses the same plan file, SHA256 and budgets.
3. **Stale plans.** There are two cases:
   - The batch is blocked with `PLAN_STALE` before any instrument ran. This
     happens when the due or closed session rolled, the policy pointer was
     republished, or the schema pins changed. The driver re-plans all the
     remaining funds and continues.
   - A single fund fails with `PLAN_STALE`. This happens when its NAV head,
     lifecycle evidence or holds moved, or the pins moved while its fetch was
     in flight. Nothing is written for that fund. It goes back into the next
     re-plan, at most twice.

   Re-plans are capped at 10 in total and at 3 in a row without progress.
4. **Failures.** Any other per-fund failure (`REPAIR_REQUIRED`,
   `GRID_INCOMPLETE`, `PROVIDER_NOT_FOUND`, and so on) is recorded by the
   operator as a failure attempt. The driver counts it and never retries it.
   Funds that the operator did not try in a stopped batch go back to the front
   of the queue.
5. **Stops.** The driver stops cleanly on any of these:
   - operator exit 3: schema, access or dependency incompatible;
   - operator exit 4: lock busy (the driver exits 5 instead when it already
     committed something);
   - operator exit 5: budget, lock or interrupt after work;
   - a Tiingo 429 (`PROVIDER_RATE_LIMITED`), exit 5;
   - a configuration error code (`PROVIDER_NOT_CONFIGURED`, `DSN_REQUIRED`,
     plan hash or limits mismatch, and similar), exit 2;
   - 3 batches in a row without progress, exit 2.

   SIGTERM from a Railway stop takes the operator's interrupt path: the
   in-flight fund rolls back, the run is finalized and the mutex is released.

**Output.** One JSON line per plan and per batch, then the summary. The
summary holds:

- counts: `committed`, `committed_unverified`, `already_applied`, `unknown`,
  `failed`;
- by code: `failed_by_code`, `not_planned_by_code` (excluded by the first
  plan) and `replan_excluded_by_code`;
- `remaining`, `requests_used`, `elapsed_s` and `readiness_republish_required`.

Output never contains a DSN, URL, payload or exception text.

**Exit and service state.** The run exits 0 only when every planned fund was
committed or already applied, or a requested cap was reached cleanly. Failed
or unknown funds, a fund dropped by a re-plan for any reason other than
`ALREADY_RECONCILED`, or a stop all give a non-zero exit. In those cases
`run_worker` marks the deploy failed and the summary explains why.

The driver is resumable by construction. A fund that was reconciled is
excluded as `ALREADY_RECONCILED` by the next plan, so after a stop you just
run the driver again.

## When to run

- **Start after the day's `nav-current-daily-chain` has published.** The plan
  excludes a fund as `NAV_STALE` when its last NAV date is before the due
  session.
- **Finish before 18:05 America/New_York, when the due session rolls.** After
  that roll every fund is `NAV_STALE` until the next chain run. A re-plan after
  the roll drops the rest of the queue as `NAV_STALE`, and the run ends
  non-zero.
- **The NYSE close (16:00 ET, earlier on half days) rolls the closed session.**
  That makes the plan stale once; the driver re-plans automatically.
- **Do not overlap with `instrument_ingestion` / `nav-current-daily-chain`.**
  Both hold the NAV writer locks, so the driver would stop with `LOCK_BUSY`.
- **Watch the shared Tiingo budget (10,000 requests/hour).** The rebase spends
  one request per fund. When `eod_prices_warmer` or ingestion may run at the
  same time, set `NAV_REBASE_RATE_PER_SECOND=1.0`.

Expect about 145 batches for about 2,900 ACTIVE funds. A full run should take
a couple of hours; check the per-batch `elapsed_s` of the canary before
deciding.

## Service variables (`maintenance-runner`)

`railway.toml` fixes the start command (`python -m src.run_worker`), so the
lane is configured only through variables. `DATABASE_URL` on this service is
`worker_writer` on the private host.

| variable | canary | full run |
|---|---|---|
| `WORKER` | `nav_rebase_cohort` | `nav_rebase_cohort` |
| `NAV_REBASE_CONFIRM` | `nav_rebase_cohort_v1` | `nav_rebase_cohort_v1` |
| `NAV_REBASE_ALLOW_DATABASE_URL` | `1` | `1` |
| `TIINGO_API_KEY` | the key | the key |
| `NAV_REBASE_MAX_BATCHES` | `1` | unset |
| `WORKER_LIMIT` | optional, e.g. `5` | unset |
| `NAV_REBASE_RATE_PER_SECOND` | optional (default 2.5) | optional, e.g. `1.0` |

`NAV_REBASE_ALLOW_DATABASE_URL=1` lets the lane use the service's
`DATABASE_URL`. Instead you can set `NAV_READINESS_DATABASE_URL` explicitly;
when it is set, it wins. With neither variable the lane refuses with
`DSN_REQUIRED`. Without `NAV_REBASE_CONFIRM` it refuses with
`CONFIRMATION_REQUIRED` and touches nothing. Optional variables:
`NAV_REBASE_MAX_SECONDS` (per batch, default 600), `NAV_REBASE_SCHEMA`
(default `public`) and `NAV_REBASE_DRY_RUN=1` (plan only).

The lane runs once per container start. Following the service note: set the
variables, **redeploy** (this captures the env), then **deploymentRestart**
(this executes the run).

## Procedure

### 0. Dry run (optional, read-only)

Set `NAV_REBASE_DRY_RUN=1` along with the canary variables. The run makes one
plan line and a summary with `status: planned`. It sends no Tiingo request and
writes nothing. Check `planned_initial`, `not_planned_by_code` and the plan
line's `needs_counts`, and how long the plan took.

### 1. Canary: one batch

Set `NAV_REBASE_MAX_BATCHES=1`, unset `NAV_REBASE_DRY_RUN`, then redeploy and
restart. Expect three log lines:

- `{"event": "plan", ...}`
- one `{"event": "batch", ...}` with at most 20 funds
- the `run_worker` line `{"worker": "nav_rebase_cohort", "status": "limit_reached", "state": "complete", ...}`

Check:

- `committed + already_applied` is 20, or close to it. Read `failed_by_code`
  for any per-fund failure.
- In the database, those funds have receipts:

  ```sql
  SELECT count(*) FROM nav_rebase_receipts
  WHERE run_id = '<run_id from the batch line>';
  ```
- The batch `elapsed_s` is well under `NAV_REBASE_MAX_SECONDS`.

If the canary stops with `LOCK_BUSY`, a writer is running; retry after it
finishes. If it stops with `PROVIDER_RATE_LIMITED`, the Tiingo account is
saturated; retry later with a lower rate.

### 2. Full run

Unset `NAV_REBASE_MAX_BATCHES` and `WORKER_LIMIT`, then redeploy and restart.
Follow the batch lines. If the run stops (`"status": "stopped"`), read
`stop_code`, fix the cause and simply restart: reconciled funds are not
planned again.

### 3. Republish readiness

The rebase never publishes risk, MV or readiness. When the summary says
`readiness_republish_required: true`, re-run `nav-current-daily-chain`
(`WORKER=nav_current_daily_chain`). It ingests the due session, refreshes
coverage, publishes risk, and then flips the readiness pointer. It reports
`published: true` only when readiness was published for the due session. Then
check readiness:

```sql
SELECT reason_code, count(*) FROM fund_nav_readiness_current_v1
WHERE fund_status = 'ACTIVE' GROUP BY 1 ORDER BY 2 DESC;
```

### 4. Close out

Unset `NAV_REBASE_CONFIRM` and set `WORKER` back to whatever lane
`maintenance-runner` ran before. The lane refuses without the confirmation,
so a later redeploy or restart cannot start another rebase by accident.

## Local use (operators)

The same driver works from the repository root:

```bash
NAV_READINESS_DATABASE_URL=... TIINGO_API_KEY=... \
  python -m scripts.rebase_fund_nav_cohort --max-batches 1
python -m scripts.rebase_fund_nav_cohort --dry-run --instrument-id <uuid> ...
```

Through the public proxy a full-cohort plan is impractical (see above), so
use a small `--instrument-id` scope there.

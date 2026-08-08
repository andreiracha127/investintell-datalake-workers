# Runbook — `bond_live_daily`

The daily worker that keeps the bond product fresh with no human in the loop:
it loads the day's prices, the yield curve and the trade tape, then republishes
what the app serves.

Railway service: **`bond-live-daily`** (project `investintell-db`, env
`production`), `WORKER=bond_live_daily`, `restartPolicy=NEVER`,
cron **`30 7 * * *` UTC**.

---

## 1. What it does, and why in that order

| # | Stage | Writes | Notes |
|---|-------|--------|-------|
| 1 | `candles` | `bond_observation_daily` | Per-CUSIP delta from that CUSIP's own watermark. ~10k calls at ~190/min ≈ 55 min. |
| 2 | `curve` | `bond_yield_curve_daily` | 13 tenors, one call each. Backfills itself on a cold table. |
| 3 | `ticks` | `bond_tick_daily` | Previous session, top `BOND_TICK_TOP_N` (500) by recent activity. |
| 4 | `matview` | — | `REFRESH MATERIALIZED VIEW CONCURRENTLY bond_curated_securities`. |
| 5 | `republish` | `bond_metric_v1`, `bond_serving_v1` | Invokes `bond_metrics.run()` then `bond_serving.run()`. |

**Stage 5 is not optional.** `daily_chain` keys a run by `(chain, source_day,
code_revision, config_version)` and returns a *completed* run's summary verbatim
without re-executing it. `source_day` is the max watermark over the
security/price/N-CEN/RR1 landing tables — none of which move when candles land.
So on a day with no deploy and no filing, the 11:00 chain finds its own run
already complete and executes nothing. Without stage 5 every byte loaded here
would sit unserved.

Double invocation is safe by construction: both publication workers are
self-anchored, take their own advisory locks, and are idempotent on their input
fingerprint, so the chain re-running them at 11:00 re-points instead of
rebuilding.

## 2. Cron placement

`30 7 * * *` UTC. The sweep plus the republication runs ~80 min, finishing
around 09:00 — a two-hour buffer before the chain at `0 11 * * *`. The two
cannot corrupt each other (distinct advisory locks) but overlapping them would
mean paying for the same publication twice.

## 3. Environment

| Variable | Required | Meaning |
|----------|----------|---------|
| `DATABASE_URL` | yes | The datalake (project-private network). |
| `FINNHUB_API_KEY` | yes | Absent ⇒ the run reports `no_api_key` and writes nothing. |
| `WORKER_LIMIT` | no | Caps the universe swept in one run (budget-bounded catch-up). |
| `WORKER_CALC_DATE` | no | Pins "today" for the window arithmetic (replay). |
| `BOND_TICK_TOP_N` | no | Tick cohort size (default 500). |

## 3a. One-time privilege prerequisite (already applied 2026-08-07)

Postgres requires **ownership** to refresh a materialized view — a `GRANT` is not
enough — and `bond_curated_securities` was owned by `postgres` while the worker
connects as `worker_writer`. Stage 4 would have failed on every run. Applied on
the production datalake, aligning it with the other bond relations
(`bond_metric_v1`, `bond_reference_terms`, `bond_serving_facts` were already
`worker_writer`-owned):

```sql
ALTER MATERIALIZED VIEW bond_curated_securities OWNER TO worker_writer;
```

Verified by refreshing it `SET ROLE worker_writer` (10,073 rows). If the matview
is ever rebuilt by an operator running as `postgres`, this has to be re-applied —
the run reports `matview_failed` and exits non-zero if it is not.

## 4. Reading the result

`state` is `ok`, or `aborted` when the provider cut the sweep short — the
top-level `aborted` key makes `run_worker` exit non-zero, so a truncated day
shows up as a failed deploy rather than a log line. Reported no-ops:
`no_observation_table`, `no_universe`, `no_api_key`, `locked`.

**Verify in the tables, never on the dashboard** — and note that on this project
`railway redeploy` reports SUCCESS *without executing*; only
`railway service restart` runs the job.

```sql
SELECT max(day) FROM bond_observation_daily;                     -- last business day
SELECT max(day) FROM bond_yield_curve_daily;                     -- same
SELECT count(*) FROM bond_tick_daily WHERE day = <prev session>; -- > 0
SELECT max(observation_date) FROM bond_serving_facts_v
 WHERE surface = 'observations' AND lane = 'latest';             -- the header's date
```

One caveat when reading the FIRST morning: at 07:30 UTC the previous session's
tape has not necessarily landed at the provider. A `max(day)` of D-2 on the
first run is plausibly the provider's timing rather than a bug — the window
re-reads its own watermark day, so the next run picks it up. Judge it on the
second day, not the first.

### Retention reports quietly — read it every run

`_prune_superseded_facts` is wrapped in try/except (a retention failure must
never roll back a promotion), so a problem shows up as a field, not an alarm.
The mechanism itself is covered by executing tests in
`tests/test_bond_serving_worker.py` (the purge, the token, the batching, the
kept build row, the refusals, a schema replay); what the runbook checks is the
production effect:

```sql
-- 1. the run's own report. Healthy: retention.state='purged' with pruned_rows,
--    or the bare {pruned_publications:0, pruned_rows:0, kept:N} of a steady
--    state. NOT state='failed', and NOT state='blocked_by_write_guard' (that
--    one means the database is running DDL older than the purge routine --
--    re-apply schemas/bond_serving_v1.sql; see §6).

-- 2. the WORST failure available: the app still points at facts that exist
SELECT count(*) FROM bond_serving_facts f
  JOIN bond_serving_publications s ON s.worker_publication_id = f.publication_id
  JOIN bond_serving_app_current_pointer p ON p.app_publication_id = s.app_publication_id;
-- must be > 0 (2,021,178 per complete publication)

-- 3. steady state is about three publications, not seven and not one
SELECT count(DISTINCT publication_id) FROM bond_serving_facts;

-- 4. every publication keeps its build row, purged or not (the as_of guard reads it)
SELECT count(*) FROM sec_derived_publications p
  LEFT JOIN bond_serving_builds b USING (publication_id)
WHERE p.product='bond_serving_v1' AND b.publication_id IS NULL;  -- must be 0
```

## 5. The header-freshness mechanism (decided 2026-08-07)

**Problem.** `bond_price_latest_v1` projects the governed price publication,
whose landing table stops at **2025-03-31**. The detail header and the catalog's
price/yield columns were built from it, so the product stated a sixteen-month-old
price next to a chart drawn through yesterday.

**Chosen: the dense daily series becomes an optional input to the two
publication builds, and the serving `as_of` follows it.** Three parts:

1. `bond_metrics` reads `bond_observation_daily` **per field** — the freshest
   priced day for the price, the freshest yielded day for the yield, each
   carrying its own date, duration settling on the yield's. Folding them
   together would let a fresh price erase an older bond's yield *and* its
   duration.
2. `serving_materializer` resolves `_bond_latest_observation` once per build:
   the governed lane, with each security whose dense observation is *strictly*
   newer replaced by it. Both the inline `latest_price_pct` and the observations
   `latest` lane read that table. The `fund_asof` (point-in-time) lane is
   untouched.
3. `bond_metrics._resolve_as_of` and `bond_serving._resolve_as_of` take the
   **greatest** of their existing anchor and the series' last day.

Part 3 is what makes the other two do anything. Measured before the change:
`max(as_of)` on the governed landing table is 2025-03-31, so the
`observation_date <= as_of` no-look-ahead guard excluded every fresh row; and
the serving publication id is `uuid5(product | as_of | code_revision)` with
`as_of` frozen at the security master's 2026-07-23 and `CODE_REVISION` moving
only on a deploy — so every run resolved to the same publication, which
`materialize` treats as already built and merely re-points.

**Rejected: salting the serving publication identity with an input
fingerprint.** It would mint a new publication daily while the payload still
*claimed* 2026-07-23. A fresh price stamped with a stale date is worse than a
stale price.

**Rejected: appending the day into `bond_price_observation`** (the governed
landing table) so the existing chain republishes it. `_PUBLISH_SQL` builds each
publication from ONE `as_of` cohort, so a daily landing shrinks
`bond_price_latest_v1` to that day's cohort. Re-landing each CUSIP's latest known
row daily to repair the coverage is worse still: a bond that does not trade gets
the same `(security_id, observation_date)` landed under successive cohorts, and
`bond_price_fund_asof_v1` recomputes duplicates across the whole landing table —
so every non-trading bond's observations would degrade to ambiguous.

**Measured on production, 2026-08-07 (rollback-only, SQL generated from the
shipping constants):**

| | before | after |
|---|---|---|
| securities with a served price | 11,964 | 21,935 |
| securities priced in 2026-08 | 0 | 9,149 |
| latest lane max observation_date | 2025-03-31 | 2026-08-06 |
| alias disagreements on the latest day | — | 0 |
| max eligible rows per security (inline subquery must stay scalar) | — | 1 |

Cost: the dense-lane resolution adds ~87 s to the serving build and ~4 min to the
metric build.

## 6. Retention (why it had to ship with this)

A complete serving publication is ~2.0M facts / ~1.2 GB. That was harmless while
a rebuild needed a deploy; now that `as_of` follows the daily series a rebuild
happens most days. `bond_serving.run()` therefore prunes, in batched commits, the
facts of every publication outside a three-part keep-set:

* the worker's own current pointer;
* whatever the **app's** current pin references — the pin advance can honestly
  fail and leave the app on an older publication, and deleting that one breaks
  the product silently;
* the immediately-prior worker publication — `daily_chain` compensation restores
  the *pre-run* pointer on a failed run.

### How the rows can go at all: the purge token

`bond_serving_facts` is immutable by construction — the write guard rejects every
non-INSERT, the `publication_id` FK is `ON DELETE RESTRICT`, and
`sec_derived_publication_delete_guard` refuses to delete a validated publication,
so deleting the parent is not a way around it either. Retention does **not**
relax that. It uses ONE sanctioned exception, in the protocol's own idiom
(`sec_derived_publication_tokens` / `sec_derived_pointer_tokens`):

| piece | where |
| --- | --- |
| `bond_serving_purge_tokens(publication_id, backend_pid)`, revoked from PUBLIC | `schemas/bond_serving_v1.sql` |
| write-guard DELETE branch: returns OLD only for `bond_serving_facts`, only when THIS backend holds the token | `bond_serving_write_guard()` |
| `bond_purge_serving_publication(uuid, batch int)`: takes the token, deletes one bounded batch, drops the token | `schemas/bond_serving_v1.sql` |
| the call site: `_prune_superseded_facts` loops it, **committing per batch** | `src/workers/bond_serving.py` |

Consequences worth knowing before touching any of it:

* **The `bond_serving_builds` row is kept on purpose.**
  `sec_derived_publication_as_of` reads it and that feeds the current-pointer
  as_of regression guard. A purged publication stays in the ledger, with its
  build metadata, holding no facts. Deleting the build row would break the guard.
  Verified after the first production purge: all 9 publications still resolve an
  `as_of` (v1 → 2026-07-23, v4–v7 → 2025-03-31, …) while 7 of them hold 0 facts.
* **One transaction per batch, never one big DELETE.** A long transaction holds
  back VACUUM for the WHOLE database — see the trap measured below.
* **An UPDATE stays forbidden**, token or no token, and a token minted by another
  backend authorises nothing.
* `bond_purge_serving_publication` refuses the worker-current and the app-pinned
  publication itself, so a hand-run purge cannot empty the served surface either.
  The wider margin (the immediately-prior publication) is worker policy.
* The worker probes for the **capability** (`bond_serving_purge_tokens` +
  `to_regprocedure('bond_purge_serving_publication(uuid,integer)')`), never for
  the guard's absence — the guard survives the purge, that is the design. A
  database that predates the routine still gets the typed
  `retention.state=blocked_by_write_guard` report, now naming the DDL that fixes it.

### Disk: re-used, NOT returned (measured, production, 2026-08-07)

`bond_serving_facts` is a **plain table, not a hypertable** — there are no chunks
to drop. A DELETE only marks tuples dead; VACUUM then makes that space reusable
by this table and does not give it back to the OS (only `VACUUM FULL` would, and
it takes an ACCESS EXCLUSIVE lock on a table the app reads — do not).

First production purge, run by hand through the recipe below:

| | before | after purge | after VACUUM |
| --- | --- | --- | --- |
| rows in `bond_serving_facts` | 5,988,124 | 4,042,356 | 4,042,356 |
| publications holding facts | 7 | 2 (v8 + v9, the keep-set) | 2 |
| `n_dead_tup` | 0 | 1,945,768 | **0** |
| `pg_total_relation_size` | 3446 MB | 3446 MB | 3446 MB |
| volume (`/var/lib/postgresql`, 458 GB) | 220.8 GB free | 212.1 GB free | 208.1 GB free |

1,945,768 rows across 5 unreachable publications went in 50k-row batches — 40
batches carrying rows, 0.6–15.0 s each, 107.6 s of database time in total, and
not one transaction longer than a batch. `n_dead_tup` then went from 1,945,768 to
**0** once the horizon cleared (final pass 1.6 s, 1,363,958 dead item identifiers
removed from the indexes, 21,110 + 6,487 index pages now reusable; an autovacuum
pass took the rest) — and the table is still
3446 MB, to the megabyte. That is the whole shape of the result: ~1.1 GB inside
the table became free space the next publication is written INTO instead of
extending the file. The number that says retention is working is
`count(DISTINCT publication_id)`, not `pg_total_relation_size`.

Read the volume row correctly: it kept FALLING (220.8 → 212.1 → 208.1 GB free)
right through the purge, and none of that is the purge's to give back — a DELETE
only writes WAL, and VACUUM wrote 11 MB more. The window was shared with a
TimescaleDB compression policy and other writers, so the movement is theirs.
**There is no measurement in which purging returns disk to the OS.**

**Trap paid on this very run — budget for it.** The first
`VACUUM (ANALYZE, VERBOSE)` right after the purge reported
`1945768 are dead but not yet removable` and freed nothing. Nothing was wrong
with the purge: an unrelated hour-old transaction (`CREATE TEMP TABLE
stock_fundamentals_statements_scope ...`, pid 113314) was pinning the global xmin
horizon, and a dead tuple cannot be reclaimed while any snapshot older than its
deletion exists. It stayed pinned for ~20 further minutes; the moment it cleared,
one `VACUUM` took the whole 1,945,768 to zero in 1.6 s. Diagnose before
re-vacuuming rather than re-running the purge:

```sql
SELECT pid, usename, state, age(backend_xmin) AS xmin_age, now()-xact_start AS xact_age,
       left(regexp_replace(query,'\s+',' ','g'), 80) AS q
FROM pg_stat_activity WHERE backend_xmin IS NOT NULL ORDER BY 4 DESC LIMIT 5;
SELECT slot_name, active, age(xmin) FROM pg_replication_slots;  -- the other classic holder
```

Evidence it was the horizon and not the purge: `n_dead_tup` sat at exactly
1,945,768 — the purged row count, to the row — across one manual `VACUUM` and
several autovacuum passes while the blocking transaction stayed open, then went
to 0 in a single pass afterwards. Autovacuum retries on its own, so doing nothing
also works; the only requirement is not mistaking it for a failed purge. Watch:

```sql
SELECT n_dead_tup, last_autovacuum FROM pg_stat_user_tables
WHERE relname='bond_serving_facts';   -- drops to 0 once the pin clears
```

### Manual purge recipe

Only needed to catch up a database that accumulated publications before the
routine existed — the daily run does this by itself. Two things are load-bearing:

* **`SET ROLE worker_writer`**. Every bond object in production is owned by
  `worker_writer`, and the worker re-applies this DDL on EVERY run with
  `CREATE OR REPLACE`. Applying it as `postgres` leaves postgres-owned functions
  and bricks every future run.
* **Apply the schema and purge in the SAME session/script run.** A deployed image
  that still carries the old DDL will `CREATE OR REPLACE` the guard back to its
  no-DELETE body on its next run; re-apply the schema first if you resume later.

Build ONE script locally and pipe it in. The DDL has to be **inlined** — `\i`
resolves inside the container, which has no repo checkout:

```sh
{
  printf '\\set ON_ERROR_STOP on\nSET ROLE worker_writer;\n'
  # BEGIN/COMMIT around the DDL is not decoration: the file DROPs the write
  # guard before re-creating it, so a half-applied run leaves the table UNGUARDED.
  printf "SET lock_timeout = '15s';\nBEGIN;\n"
  tr -d '\r' < schemas/bond_serving_v1.sql
  printf 'COMMIT;\n'
  # One statement per batch -- psql autocommit gives one transaction per batch.
  # Repeat per stale publication until it returns 0. NEVER wrap the loop in a DO
  # block or a generate_series: that collapses every batch into ONE transaction,
  # which is the VACUUM trap this batching exists to avoid.
  for i in $(seq 1 40); do
    printf "SELECT bond_purge_serving_publication('%s'::uuid, 50000);\n" "$PUB"
  done
} > apply_and_purge.sql

railway ssh --project 35fa36a3-2641-42b2-b48b-540eac0597c6 --environment production \
  --service market-clean-serial -- psql -U postgres -d market -f - < apply_and_purge.sql
```

(Extra calls past exhaustion are harmless: they return 0.)

The stale set is the same arithmetic the worker uses — note it asks the 9-row
publications table which ids are stale and only THEN counts those through the
index. A bare `GROUP BY publication_id` over the facts table reads the whole
3.4 GB relation (4m28s measured) for the same answer:

```sql
WITH keep AS (
  SELECT publication_id FROM sec_derived_current_pointers WHERE product='bond_serving_v1'
  UNION SELECT s.worker_publication_id FROM bond_serving_app_current_pointer p
        JOIN bond_serving_publications s ON s.app_publication_id=p.app_publication_id
  UNION SELECT publication_id FROM (SELECT publication_id FROM sec_derived_publications
        WHERE product='bond_serving_v1' ORDER BY publication_version DESC LIMIT 2) r),
stale AS (
  SELECT publication_id FROM sec_derived_publications
  WHERE product='bond_serving_v1' AND publication_id NOT IN (SELECT publication_id FROM keep))
SELECT f.publication_id, count(*) FROM bond_serving_facts f
WHERE f.publication_id = ANY(ARRAY(SELECT publication_id FROM stale)) GROUP BY 1;
```

The sibling question — `bond_metric_v1` now mints one publication a day —
is a **known follow-up**, deliberately not bundled here. Measured 2026-08-07:
8 publications, 5.40M rows, 1807 MB, i.e. ~1.27M rows (211,406 securities x 6
metrics) and **~270 MB per day**. That is smaller than the serving facts in
BYTES (no jsonb payload) but comparable in ROWS — do not carry over the serving
keep-set by analogy. Its restore targets are different (the chain compensates
metric pointers too, and `bond_serving` consumes the metric view at build time,
so a pruned metric publication can strand a serving rebuild) and that reasoning
has to be done on its own terms.

## 7. Free property

On a day the series does not advance (a weekend), `as_of` does not move, both
publication identities replay, and the republication is a cheap re-point rather
than a needless 2M-row rewrite.

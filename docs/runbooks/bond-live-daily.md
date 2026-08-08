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

Disk is re-used, not returned (`bond_serving_facts` is a plain table): steady
state is ~3 publications instead of unbounded growth.

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

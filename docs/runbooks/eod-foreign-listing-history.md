# EOD prices: full history for W1c foreign listings

Light sizes historical equity priors as SEC share counts × `eod_prices` closes.
W1c (`public.sec_foreign_listing_at`) evidences US-listed foreign lines (ADS and
direct ordinary listings), but on 2026-10-10 only 199 of the 1,676 symbols it
resolves had any `eod_prices` rows, and 85 of those only had the screener's
745-day window. `eod_prices_warmer` now covers these lines with their full Tiingo
history. The screener universe (`universe_constituents`) does not change.

## Source set

Each run, in one query with `SET LOCAL jit = off`:

- lines: distinct `(cik, symbol)` of active facts in
  `public.sec_foreign_listing_evidence` (`retired_on IS NULL`) whose symbol has a
  US-exchange shape, `^[A-Z]{1,5}(-[A-Z])?$`. This drops home-market codes
  (SANB11, VIVT3), note lines (AMX22) and preferred or warrant suffixes (NM-PG).
- a symbol is covered when `sec_foreign_listing_at(cik, symbol, d)` returns
  `listing_status = 'resolved'` for the run date or any of 2010-12-31,
  2015-12-31, 2020-12-31 and 2025-12-31 that is not after the run date (a
  historical `WORKER_CALC_DATE` run probes no later year-end, so it has no
  look-ahead). The ratio status does not matter: prices are needed whether or
  not the ADS ratio is evidenced.

Tiingo decides existence. Without the W1c resolver (a fresh environment) the
source reports `absent` and the ring runs unchanged. The worker asserts symbol
coverage, not issuer ownership: a reused ticker (GOLD was Randgold, then
Barrick) carries the current security's history, so Light must keep using the
line's evidenced dates.

## Verification pass

Every covered ticker gets exactly one verification pass before it is
`history_complete`. There is no shortcut from `min(date)`.

1. Meta call (`GET /tiingo/daily/{ticker}`) for a fresh `startDate`. It fills
   NULL `name`, `exchange_code` and Tiingo dates in `instruments`, or inserts
   the row with `asset_type = 'stock'`; non-null fields and an existing
   `asset_type` are never changed.
2. The verification interval is fixed BEFORE the fetch:
   `[startDate, min(as_of, max(meta endDate, last stored date on or before
   as_of))]`. The request asks for exactly that interval, and the response is
   held to it; it can never shrink its own obligations. Nothing after the run's
   `as_of` is in scope: stored rows after it are neither compared nor touched.
   In order:
   - an empty response, or one that returns a date twice, is
     `history_incomplete`;
   - every shared (stored and returned) date must match on raw close
     (relative 1e-6). Any difference is `history_conflict`, even if the
     response is also short;
   - a stored date outside the interval (for example before a `startDate`
     Tiingo has since advanced) is `history_conflict`
     (`stored_outside_provider_range`), never silently exempted;
   - stored rows with no shared date while dates would be inserted is
     `history_incomplete` (`no_shared_sessions`): there is no vacuous pass;
   - coverage, in NYSE sessions (`exchange_calendars` XNYS, already a pinned
     dependency, built from 1900 so it covers SONY from 1974 and NVO from
     1982; a date outside the calendar raises instead of being clamped): a bar
     outside the request, any session between `startDate` and the first bar,
     or a last bar before the last stored date or with any session between it
     and the interval end is `history_incomplete`. Only weekends and exchange
     holidays may separate the ends from the bars;
   - every stored date in the interval must be in the response, else
     `history_incomplete` (`stored_sessions_missing`);
   - missing dates are returned dates that are not stored: the prefix, interior
     gaps and the tail. None missing → `history_complete`, raw-verified,
     nothing inserted;
   - otherwise the adjusted open/high/low/close must also match on ALL shared
     dates. Any difference (a split or dividend re-based Tiingo's history since
     the rows were stored) is `adjustment_rebase_required` with the observed
     ratio, and nothing is inserted. If they match, every missing date is
     inserted and `history_complete` is written in the same transaction.

Inserts use `INSERT … ON CONFLICT (ticker, date) DO NOTHING` in 500-row
statements inside ONE transaction per ticker (about 7.5k rows for a 30-year
ticker), so an interrupted load leaves neither rows nor status and the next
pass starts over. That transaction first locks the ticker's `instruments` row
`FOR UPDATE` and re-reads its stored rows; if another writer changed them since
the verification read, nothing is written and the ticker is retried
(`stored_rows_changed_during_pass`). The lock blocks other writers' inserts for
the ticker (their foreign-key check) and the Light API's metadata upsert until
the load commits. It does not block an API upsert that rewrites existing rows,
but those writes carry the same day's Tiingo values. Existing rows are never rewritten. A response with any
non-conforming element or a non-list body is `history_incomplete`: an element
that is not an object (`[null]`, `["bad"]`), has no parseable date, misses a
field, or has a value that is not a finite number (`"N/A"`, a numeric string,
a bool, NaN, infinity), a price or split factor that is not positive, or a
negative volume or dividend. Meta without an `endDate` is also retried. Any other unexpected exception while handling one ticker
records `history_incomplete` with `unexpected:<ExceptionType>` and backoff, and
the phase moves on to the next ticker; only the 429 breaker stops the phase.

## Status, scheduling and the ring

`eod_warmer_ticker_status`, created and migrated by the worker:

| Status | Meaning | Next attempt |
|---|---|---|
| `history_complete` | verified; missing dates inserted if any | after 90 days (re-verification), or at once for a later as-of when `complete_through` is set |
| `history_incomplete` | transient HTTP (5xx, timeout, invalid body or element), response not covering the interval, omitted stored sessions, no shared sessions, unexpected exception | 12 h, doubling per consecutive failure, up to 7 days |
| `adjustment_rebase_required` | adjusted basis moved; detail has the ratio | after 30 days |
| `history_conflict` | raw closes differ, or stored rows outside Tiingo's range | after 30 days |
| `tiingo_unknown` | meta 404, no `startDate`, prices 404 | after 30 days |

`history_complete` means the series matched Tiingo at `checked_at` for
`[history_start, end]`. When a historical run (`WORKER_CALC_DATE` before
Tiingo's end) verified it, `complete_through` records the as-of it covered; it
counts as complete only for runs at or before that date, so the next real-date
run re-verifies the tail. Every completion is verified again after 90 days, so
a later change on Tiingo's side (an earlier `startDate`, a revised close, a new
gap) is found; a re-verification inserts nothing unless the series is still
coherent.

Pending tickers are processed in order of their next attempt (never tried
first), then ticker. A ticker that keeps failing backs off behind the others,
so it cannot hold a cap slot. A 429 is an account-wide budget signal and a
missing API key is a configuration fault: both count as errors with nothing
recorded for the ticker, and the client's 30×429 breaker aborts the phase.

The ring skips only covered tickers with no rows that this run's history phase
will load (the first `cap` pending tickers); those get their whole series in
one transaction instead of the 745-day cold start, and have no rows until it
commits. Every ticker with rows keeps its daily ring refresh whatever its
history status, and a zero-row screener constituent the phase will not reach
this run gets normal ring warming. `EOD_HISTORY_TICKERS_PER_RUN=0` therefore
leaves the ring exactly as before this change.

Run stats report `completed`, `verified_without_insert`, `deferred`,
`history_rows`, `errors` / `error_tickers`, `fail_closed` /
`fail_closed_tickers`, `tiingo_unknown` and `waiting` per status. `deferred` is
the pending tickers that did not settle this run: not attempted, retrying with
backoff, or blocked by 429s or a missing key. Only `history_complete`, a
fail-closed status or `tiingo_unknown` settles a ticker, so `deferred = 0`
means every pending ticker reached a recorded outcome — not that every one is
complete.

## Status table schema and readers

`CREATE TABLE IF NOT EXISTS` does not migrate an older shape, so every run (under
the warmer's advisory lock) adds any missing column, drops every CHECK that
mentions `status` and adds exactly the expected one, then verifies the column
types and probes the CHECK: each of the five statuses must insert and `'bogus'`
must be rejected (probe rows are rolled back). Anything else fails the run
loudly (`RuntimeError`).

The warmer connects as `worker_writer`. Its default ACL in `public` grants only
`app_analytics_ro` SELECT, so a table it creates is not readable by
`app_runtime` or `mcp_ro` by default (`eod_warmer_cursor` has explicit grants).
The DDL grants SELECT on `eod_warmer_ticker_status` to `app_runtime`,
`app_analytics_ro` and `mcp_ro` where those roles exist.

## Budget

The history phase runs after the ring, on the same `TokenBucket` at
`DEFAULT_RATE_PER_S` (2.5 req/s, unchanged), and is skipped when the ring
aborted on Tiingo's 429 breaker. Each ticker costs two logical requests (meta,
one full-range price window); the client retries each up to three times, so at
most six HTTP attempts. The cap is `EOD_HISTORY_TICKERS_PER_RUN` (default 25,
never above `WORKER_LIMIT`, `0` turns the phase off):

| Cap | Logical requests per run | HTTP attempts per run (worst case) |
|---:|---:|---:|
| 25 | ≤ 50 | ≤ 150 |
| 250 | ≤ 500 | ≤ 1,500 |

With the ring at `WORKER_LIMIT=2300` (up to 6,900 HTTP attempts), a run's worst
case is 7,050, 7,500 and 8,400 attempts at caps 25, 100 and 250, before other
consumers of the shared key. Check fleet headroom and observed attempts and
429s before raising the cap.

A budget abort inside the phase sets `aborted`, so the run exits non-zero like
a ring abort.

## Compression

`eod_prices` is a hypertable with monthly chunks (a 360-day interval since
2026-07), segmentby `ticker`, orderby `date DESC`, and a 90-day columnstore
policy every 12 hours. On 2026-10-10, 786 of its 787 chunks were compressed, so
almost every history row lands in a compressed chunk.

A key older than the ticker's first row, or of a ticker without rows, matches no
compressed batch (segmentby ticker, date min/max), so nothing is decompressed.
An interior gap decompresses only the ticker's own batch around it, bounded by
the ticker's row count and far below the per-transaction limit of 100,000. The
rows go to the chunks' uncompressed part; the columnstore policy recompresses
those partial chunks.

Measured on `timescale/timescaledb:2.27.2-pg18` with 5,000 tickers × 12 monthly
chunks (110,000 rows per chunk), through `load_ticker_history` with one
transaction per ticker (largest 8,290 rows): 502,150 history rows for 20 new and
50 truncated tickers inserted at about 10,700 rows/s. This held at the
production limit and at a limit of 1: compressed batch counts were unchanged, no
existing row reached a chunk heap, and the existing rows' checksum was
identical. As a control, the ring's `DO UPDATE` on one existing compressed key
failed at a limit of 1 and decompressed that ticker's 21-row batch at the
default. The policy job recompressed every partial chunk in 8.5 seconds.
`tests/test_eod_foreign_listing_coverage_db.py` holds the same checks plus the
interior-gap bound.

## Production numbers (read-only, 2026-10-10)

- Covered symbols: 1,676. Listing-resolved lines: 17, 156, 760 and 1,474 at the
  2010, 2015, 2020 and 2025 year-ends, and 1,565 at the run date. With both
  listing and ratio resolved, the counts are 2, 83, 520, 1,150 and 1,226.
- 1,477 have no rows: one full load each, kept out of the ring while in the
  run's history batch. 199 have rows and stay in the ring: 85 from 2024-06-11 (the screener's 745-day
  cohort) and 114 whose rows reach their stored start. Each gets the
  verification pass. Expect part of the 85 to fail closed as
  `adjustment_rebase_required`: any dividend or split since their cold start
  moves Tiingo's adjusted basis, and they have a prefix to insert.
- Requests: at most 1,676 meta and 1,676 price windows, 3,352 logical and at
  most 10,056 HTTP attempts. That is 68 runs at the default 25 (about 23 days at
  the production cron), or 7 runs at 250 (about 2.3 days).
- Rows: at least about 2.1M, estimated from each ticker's first SEC observation.
  The ten-ticker meta sample averages 29.8 years of history, which would give
  about 11M if every new ticker were that old.
- Ring size: 5,198 tickers today, up to 6,675 once the new tickers have rows.

## Ring freshness

The cron is `10 1,13,21 * * *`, so the gaps between runs are 12, 8 and 4 hours.
With 12 priority tickers and 6,675 in the ring:

| `WORKER_LIMIT` | Tail slots per run | Average revisit | Worst-case nominal gap |
|---:|---:|---:|---:|
| 2000 (today) | 1,988 | ~26.8 h | 36 h |
| 2300 | 2,288 | ~23.3 h | 24 h |

These assume successful runs and stable membership. A strict sub-24-hour bound
with this cron needs the tail covered in two runs, at least 3,344 per run, and a
fleet budget check first. `WORKER_LIMIT` is a Railway setting for the owner.

## Read-only preview

From `E:\tmp-deploy\api\backend`, outside 06:00–08:30 UTC, preview the source
set, the recorded statuses and how many pending tickers are `new` or
`existing`. This makes no Tiingo call and no write. The path must be a checkout
that has this change: `E:\investintell-datalake-workers-sep` on `main` after
merge, or the PR worktree before it.

```powershell
@'
import collections, datetime as dt, os, sys
from urllib.parse import urlsplit, urlunsplit
import psycopg
sys.path.insert(0, r"E:\investintell-datalake-workers-sep")
from src.workers import eod_prices_warmer as w
u = urlsplit(os.environ["DATABASE_URL"])
dsn = urlunsplit((u.scheme.replace("+asyncpg", ""), u.netloc.rsplit("@", 1)[0] + "@centerbeam.proxy.rlwy.net:36616", u.path, u.query, ""))
with psycopg.connect(dsn, options="-c default_transaction_read_only=on -c statement_timeout=300000") as conn:
    symbols = w.foreign_listing_tickers(conn, dt.date.today())
    plan = w.plan_foreign_history(conn, symbols, now=dt.datetime.now(dt.UTC), as_of=dt.date.today())
print(len(symbols), len(plan["complete"]), {k: len(v) for k, v in plan["waiting"].items()},
      dict(collections.Counter(w.classify_history_task(t) for t in plan["pending"])))
'@ | railway run --service api -- uv run --no-project --with "psycopg[binary]" --with httpx python -
```

## Staged rollout

No earlier revision of this PR may run after activation: only the final merged
image. A read-only check on 2026-10-10 (`app_runtime`,
`default_transaction_read_only=on`) found no `eod_warmer_ticker_status` relation
or constraint in any schema; only `eod_warmer_cursor` exists. Every
`eod-prices-warmer` deployment so far came from a `main` merge (latest
`beb631f5`, 2026-10-01), so no PR image has run. No `history_complete` record
can exist before activation; the merged image creates the table on the first
run with a positive cap.

1. Set `EOD_HISTORY_TICKERS_PER_RUN=0` on `eod-prices-warmer`, then merge and
   deploy the merged `main` SHA (`railway redeploy --service eod-prices-warmer
   --from-source --yes` builds; on this project a cron deployment does not
   execute). With cap 0 the ring behaves exactly as before; the run creates and
   verifies `eod_warmer_ticker_status` only when the phase runs.
2. Owner decisions: `WORKER_LIMIT=2300` (see the freshness table), and confirm
   the Light deployment that reads this history includes the D6 alive-span
   checks.
3. Set the cap to 25 for at least three successful cron slots. To start at once,
   `railway service restart --service eod-prices-warmer` (same advisory lock as
   the cron). After each run check:
   - completeness: `SELECT status, detail, count(*) FROM eod_warmer_ticker_status GROUP BY 1, 2;`
   - fail-closed tickers and their ratios (`fail_closed_tickers`);
   - retry fairness: `waiting` and `error_tickers` rotate rather than repeat;
   - `aborted`, HTTP attempts and 429s in the stats and logs;
   - ring freshness: the oldest `max(date)` among ring tickers;
   - compression recovery: partial chunks fall back after the next policy run:
     `SELECT count(*) FROM _timescaledb_catalog.chunk ch JOIN _timescaledb_catalog.hypertable h ON h.id = ch.hypertable_id WHERE h.table_name = 'eod_prices' AND (ch.status & 8) = 8;`
4. Raise the cap to 100, then 250, after the same checks (worst-case HTTP
   attempts per run at `WORKER_LIMIT=2300`: 7,500 and 8,400). Expected first dates
   once loaded: TSM 1997-10-09, ASML 1995-03-16, NVO 1982-01-04, SAP
   1995-09-18, NTES 2000-06-30; QGEN 1996-06-28 if its pass finds one basis.

To pause, set the cap back to 0. Rollback is the previous image: the new table
holds only progress state, and the inserted history is ordinary `eod_prices`
rows.

## Follow-up (pre-existing, not changed here)

The ring refreshes only `max(date)` − 5 days. After a split or dividend, Tiingo
re-bases every older adjusted value, but stored older rows keep the basis they
were fetched on, so each corporate action leaves an adjusted-price seam at the
overlap boundary for every ticker. Raw prices are unaffected. Fixing it needs a
governed re-base of stored adjusted history; the same re-base would let
`adjustment_rebase_required` tickers pass their verification.

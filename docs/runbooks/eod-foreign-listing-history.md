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
  historical `WORKER_CALC_DATE` run has no look-ahead). The ratio status does
  not matter.

Tiingo decides existence. Without the W1c resolver the source reports `absent`
and the ring runs unchanged. The worker asserts symbol coverage, not issuer
ownership: a reused ticker carries the current security's history, so Light
must keep using the line's evidenced dates (D6).

## Code map

| Piece | Where | Role |
|---|---|---|
| `validate_series` | `src/workers/eod_history_validation.py` | Pure (no DB, network or clock). Holds every rule; nothing else decides. |
| `promote` | `src/workers/eod_prices_warmer.py` | The only writer of history rows and of `history_complete` / `complete_through`. |
| `cover_foreign_history` | `src/workers/eod_prices_warmer.py` | Meta call, the fixed interval, one price request, the verdict, `promote` or a status. |
| `reference_verdict` | `tests/_eod_history_reference.py` | Independent, set-based reference used by the differential fuzz. |

## Verification

Per pending ticker: a meta call gives a fresh `startDate` and fills NULL
`instruments` metadata (non-null fields and `asset_type` are never changed). The
interval is fixed before the fetch:
`[startDate, min(as_of, max(meta endDate, last stored date on or before as_of))]`.
One price request asks for exactly that interval. Nothing after the run's
`as_of` is in scope. Meta without an `endDate` is retried.

`validate_series(ticker, interval, provider_bars, stored_rows, calendar,
accepted_gaps)` applies these rules in order; the first that fails decides:

1. `start > end` → `history_incomplete` (`empty_interval`).
2. Interval outside the XNYS calendar (built from 1900) →
   `history_incomplete` (`interval_outside_calendar`), checked on the interval
   itself.
3. Empty response or non-list body → `history_incomplete`.
4. Any unusable element → `history_incomplete` (`unusable_bar`): not an object;
   a `date` other than exactly `YYYY-MM-DD` or Tiingo's
   `YYYY-MM-DDT00:00:00[.000]Z` (nothing truncated or coerced, ASCII digits
   only); a required field missing or not a finite number (a bool or a numeric
   string is not a number); a price or split factor that is not positive; a
   negative volume or dividend; raw or adjusted
   `low ≤ min(open, close) ≤ max(open, close) ≤ high` violated; or the
   adjustment factors implied by adjOpen/open, adjHigh/high and adjLow/low more
   than 1% from adjClose/close.
5. A date returned twice → `history_incomplete` (`duplicate_dates`).
6. A bar outside the interval → `history_incomplete` (`bars_outside_request`).
7. A bar on a non-session → `history_incomplete` (`off_session_bar`).
8. Raw open/high/low/close differing from a stored row on any shared date
   (relative 1e-6) → `history_conflict` (`raw_differs`).
9. A stored date outside the interval → `history_conflict`
   (`stored_outside_provider_range`); a stored date on a non-session →
   `history_conflict` (`stored_off_session`).
10. Adjusted open/high/low/close differing on any shared date (relative 1e-6)
    → `adjustment_rebase_required` with the observed ratio. Checked whether or
    not anything is missing: a mixed adjusted basis is never complete.
11. A stored date absent from the response → `history_incomplete`
    (`stored_sessions_missing`).
12. Any session of the interval, minus `accepted_gaps`, in neither the store nor
    the response → `history_incomplete` (`sessions_missing`): interior sessions
    as much as the ends.
13. Otherwise the returned dates are exactly the interval's sessions: nothing new
    → `complete_nothing_to_insert`; else `load` with exactly the rows for the
    returned dates that are not stored.

Every verdict carries a digest of the stored snapshot it judged.

Tolerances come from production data (2,126,745 stored rows, 2026-10-10):
- OHLC ordering: no violation, raw or adjusted, so the check is strict.
- Values: no non-positive price, negative volume or dividend, non-positive split factor or NaN.
- Off-session rows: none.
- Within-bar adjustment factor: Tiingo rounds adjusted fields, often to three decimals. 192 rows deviate by more than 1e-6; the largest deviation is 0.17% (AAMRQ). A 1% tolerance accepts all of them and still rejects a misapplied split or an incoherent bar. Sub-dollar prices with three-decimal rounding could exceed it; none do in the sample.

## Promotion

`promote(conn, ticker, verdict)` certifies `load` and
`complete_nothing_to_insert` alike, in one transaction:
1. Lock the ticker's `instruments` row `FOR UPDATE`.
2. Re-read the stored rows and recompute the digest. If it differs, write nothing; the caller records `history_incomplete: stored_rows_changed_during_pass`.
3. Otherwise insert exactly the verdict's rows (`INSERT … ON CONFLICT DO NOTHING`, 500-row statements) and write `history_complete` and `complete_through` together.

An error anywhere leaves neither rows nor status. `record_ticker_status`
refuses `history_complete`. Tests check structurally (a static source scan and
an intercepted run of the real entrypoint) that no other code path inserts
history rows or writes completion.

The lock also serializes with Light's `ingest_one_ticker`, which upserts the
ticker's `instruments` row first and then its prices in the same transaction:
whichever commits second sees the other's work. There is no residual race
between the two writers.

## What `history_complete` certifies

`history_complete` certifies that, at `checked_at`, the stored series for
`[history_start, end]` was exactly the interval's sessions, every bar coherent,
and raw and adjusted values equal to Tiingo's on every date. It does not
certify adjusted coherence after a later corporate action.

After a split or dividend, Tiingo re-bases every older adjusted value, while the ring's 5-day overlap, Light's 7-day refresh and this worker's
inserts each leave older stored rows on the basis they were fetched on. That
ownership question for adjusted history predates this change. The 90-day
re-verification reports such a ticker as `adjustment_rebase_required`; it never
rewrites rows. Resolving it needs a governed re-base of stored adjusted
history.

## Session gaps (decision pending)

Rule 12 is strict and `accepted_gaps` is empty: no session may be missing. In
production, Tiingo's own series has holes on illiquid no-trade days, halts and
reused tickers:
- 17 of the 199 covered tickers with rows have 4,892 missing sessions between their first and last stored row (AHG 2,025, UN 1,404, CCU 18 scattered days);
- 76 of a 535-ticker sample (14%) have holes.

Under the strict rule, roughly a tenth of covered tickers stay
`history_incomplete` on weekly retries with no history loaded. The validator
takes evidenced gaps as an explicit input. One way to supply them: accept a
hole only when two independent fetches at least 12 hours apart return the same
gap set. That is not built here; it is the owner's decision.

## Status, scheduling and the ring

`eod_warmer_ticker_status` is created and migrated by `ensure_status_table()` at
the start of every run, before any read of it. It adds missing columns, sets
NOT NULL on `ticker`, `source`, `status`, `attempts` and `checked_at`, and
replaces the status CHECK exactly. It then verifies every column's type and
nullability through `information_schema`, probes the CHECK (each status
inserts; `'bogus'` and NULL are rejected), and raises on any mismatch. It grants
SELECT to `app_runtime`, `app_analytics_ro` and `mcp_ro`; the warmer connects as
`worker_writer`, whose default ACL in `public` grants only `app_analytics_ro`.

| Status | Next attempt |
|---|---|
| `history_complete` | after 90 days; at once for a later as-of when `complete_through` is set |
| `history_incomplete` | 12 h, doubling per consecutive failure, up to 7 days |
| `adjustment_rebase_required`, `history_conflict`, `tiingo_unknown` | after 30 days |

- **Historical runs:** a run before Tiingo's end records `complete_through = as_of`, so the next real-date run re-verifies the tail.
- **Ordering:** pending tickers run by next attempt (never-tried first), then ticker, so a failing ticker cannot hold a cap slot.
- **429s and a missing key:** both count as errors and record nothing, and the 30×429 breaker aborts the phase.
- **Unexpected exceptions:** any exception while handling one ticker records `unexpected:<Type>` with backoff, and the phase moves on.

The ring is unchanged by this source. A covered screener constituent without rows gets its usual 745-day warming, and the history phase later extends it backward through the same verification (its overlap is on the same adjusted basis). Covered tickers outside the screener without rows are not in the ring anyway. `EOD_HISTORY_TICKERS_PER_RUN=0` leaves the ring exactly as before.

Run stats report:
- `completed` and `verified_without_insert`;
- `deferred`: pending tickers that did not settle this run;
- `history_rows`;
- `errors` / `error_tickers` and `fail_closed` / `fail_closed_tickers`;
- `tiingo_unknown`, and `waiting` per status.

## Budget

The history phase runs after the ring, on the same `TokenBucket` at 2.5 req/s,
and is skipped when the ring aborted on the 429 breaker. Each ticker costs two
logical requests (meta and one price window), at most six HTTP attempts with
retries. The cap is `EOD_HISTORY_TICKERS_PER_RUN` (default 25, never above
`WORKER_LIMIT`, 0 = off):

| Cap | HTTP attempts per run, worst case | With `WORKER_LIMIT=2300` |
|---:|---:|---:|
| 25 | ≤ 150 | 7,050 |
| 100 | ≤ 600 | 7,500 |
| 250 | ≤ 1,500 | 8,400 |

## Compression

`eod_prices` is a hypertable with monthly chunks (360-day since 2026-07),
segmentby `ticker`, orderby `date DESC`, and a 90-day columnstore policy. 786 of
787 chunks were compressed on 2026-10-10. A key older than the ticker's first
row, or of a ticker without rows, matches no compressed batch, so nothing is
decompressed. An interior gap decompresses only the ticker's own batch around
it, far below the per-transaction limit of 100,000. Through `promote` (one
transaction per ticker, the largest 8,290 rows), 502,150 rows went into 13
compressed chunks of 110,000 rows each at about 9,000 rows/s:
- **Decompression:** none at the production limit or at a limit of 1. Batch counts were unchanged and the existing checksum was identical.
- **Control:** the ring's `DO UPDATE` on one compressed key tripped a limit of 1.
- **Replay:** every replayed (stale) verdict was refused.
- **Recompression:** the policy job recompressed every partial chunk in 10.5 s.

## Production numbers (read-only, 2026-10-10)

- Covered symbols: 1,676. Listing-resolved lines: 17, 156, 760 and 1,474 at the 2010, 2015, 2020 and 2025 year-ends, and 1,565 at the run date.
- 1,477 have no rows. 199 have rows: 85 from the screener's 745-day cohort and 114 whose rows reach their stored start.
- Requests: at most 1,676 meta and 1,676 price windows. That is 68 runs at cap 25, or 7 at 250.
- Rows: at least about 2.1M; about 11M if every new ticker were as old as the 29.8-year sample mean.
- Ring size: up to 6,675 tickers.

The `eod_warmer_ticker_status` table does not exist in production, and every
`eod-prices-warmer` deployment so far came from a `main` merge (latest
`beb631f5`, 2026-10-01). No earlier revision of this PR may run after
activation.

## Ring freshness

The cron is `10 1,13,21 * * *`. With 12 priority tickers and 6,675 in the ring:

| `WORKER_LIMIT` | Average revisit | Worst-case nominal gap |
|---:|---:|---:|
| 2000 (today) | ~26.8 h | 36 h |
| 2300 | ~23.3 h | 24 h |

A strict sub-24-hour bound with this cron needs at least 3,344 per run.

## Read-only preview

From `E:\tmp-deploy\api\backend`, outside 06:00–08:30 UTC, with a checkout that
has this change (`main` after merge, or the PR worktree before it):

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
'@ | railway run --service api -- uv run --no-project --with "psycopg[binary]" --with httpx --with exchange_calendars==4.13.2 python -
```

## Staged rollout

1. Set `EOD_HISTORY_TICKERS_PER_RUN=0` and `WORKER_LIMIT=2300` on `eod-prices-warmer`, then merge and deploy (`railway redeploy --service eod-prices-warmer --from-source --yes` builds; a cron deployment does not execute).
   - The first run creates and verifies the status table; check its shape and grants.
   - Confirm the Light deployment includes the D6 alive-span checks, and decide the session-gap rule.
2. Set the cap to 25 for at least three successful cron slots (`railway service restart --service eod-prices-warmer` runs one at once). After each, check:
   - status counts and reasons;
   - fail-closed ratios;
   - that retries rotate;
   - HTTP attempts and 429s;
   - the oldest ring `max(date)`;
   - that partial chunks fall back after the next policy run.
3. Raise the cap to 100, then 250. Return it to 0 to pause.

Rollback is the previous image: the status table holds only progress state, and
inserted history is ordinary `eod_prices` rows.

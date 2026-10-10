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
| `reference_verdict` | `tests/_eod_history_reference.py` | Independent reference (own parser, predicate, exact arithmetic and hard-coded NYSE calendar) used by the differential fuzz. |

## Verification

Per pending ticker: a meta call gives a fresh `startDate` and fills NULL
`instruments` metadata (non-null fields and `asset_type` are never changed). The
interval is fixed before the fetch:
`[max(startDate, CALENDAR_SUPPORTED_FROM), min(as_of, max(meta endDate, last stored date in scope))]`.
One price request asks for exactly that interval. Nothing after the run's
`as_of` is in scope. Meta without an `endDate` is retried.

### Calendar domain

`CALENDAR_SUPPORTED_FROM` is 1970-01-01. The NYSE has traded a five-day week
since September 1952, and the oldest covered ticker starts on 1973-05-03, but
the calendar's holiday history before 1970 is not certified here. Bars before
1970 are not requested, compared or inserted, and that is not an error: a
ticker whose Tiingo `startDate` is earlier is verified from 1970-01-01 and its
status detail records `history_from=1970-01-01; tiingo_start=<date> (earlier
history not certified)`. Every `history_complete` detail ends with the
effective `history_from`. An interval starting before 1970 is refused
(`interval_outside_calendar`). The XNYS calendar's first session is 1970-01-02
(New Year's Day was closed), and `exchange_calendars` refuses a range that
starts before it, so the calendar queries from that session: 1970-01-01 is a
non-session, not out of range.

### Rules

`validate_series(ticker, interval, provider_bars, stored_rows, calendar,
accepted_gaps)` applies these rules in order; the first that fails decides:

1. `start > end` → `history_incomplete` (`empty_interval`).
2. Interval outside the calendar domain (from 1970-01-01) →
   `history_incomplete` (`interval_outside_calendar`), checked on the interval
   itself.
3. Empty response or non-list body → `history_incomplete`.
4. Any unusable element → `history_incomplete` (`unusable_bar`): not an object;
   a `date` other than exactly `YYYY-MM-DD` or Tiingo's
   `YYYY-MM-DDT00:00:00[.000]Z` (nothing truncated or coerced, ASCII digits
   only); or values that are not a valid bar (below).
5. A date returned twice → `history_incomplete` (`duplicate_dates`).
6. A bar outside the interval → `history_incomplete` (`bars_outside_request`).
7. A bar on a non-session → `history_incomplete` (`off_session_bar`).
8. Any stored row in scope that is not a valid bar → `history_conflict`
   (`stored_bar_invalid: <problem> on <date>`), before any comparison uses it.
   Every retained row is validated, not only the ones the response touches.
9. Raw open/high/low/close differing from a stored row on any shared date
   (relative 1e-6) → `history_conflict` (`raw_differs`).
10. A stored date outside the interval → `history_conflict`
    (`stored_outside_provider_range`); a stored date on a non-session →
    `history_conflict` (`stored_off_session`).
11. Adjusted open/high/low/close differing on any shared date (relative 1e-6)
    → `adjustment_rebase_required` with the observed ratio. Checked whether or
    not anything is missing: a mixed adjusted basis is never complete.
12. A stored date absent from the response → `history_incomplete`
    (`stored_sessions_missing`).
13. Any session of the interval, minus `accepted_gaps`, in neither the store nor
    the response → `history_incomplete` (`sessions_missing`): interior sessions
    as much as the ends.
14. Otherwise the returned dates are exactly the interval's sessions: nothing new
    → `complete_nothing_to_insert`; else `load` with exactly the rows for the
    returned dates that are not stored.

### A valid bar

Provider bars and stored rows pass the same predicate, on all twelve fields
(open, high, low, close, volume, adjusted OHLC, adjusted volume, dividend,
split factor), checked in this order:
- every field present and a finite number: not a bool, a string, NULL, NaN,
  ±inf, or an integer too large for a float (`non_numeric_<field>`), so no
  later comparison sees a non-finite value;
- prices and the split factor positive (`non_positive`); volumes and the
  dividend non-negative (`negative`);
- magnitude bounds, inclusive (`value_out_of_bounds`): prices ≤ 1e7 (BRK-A, the
  highest US share price, is below 1e6), volumes ≤ 1e13, dividend ≤ 1e7, split
  factor ≤ 1e4;
- raw and adjusted `low ≤ min(open, close) ≤ max(open, close) ≤ high`
  (`ohlc_order`);
- the adjustment factors adjOpen/open, adjHigh/high and adjLow/low within 1% of
  adjClose/close (`adjustment_factor`).

Every tolerance comparison is exact rational arithmetic (`fractions.Fraction`),
cross-multiplied: no quotient is formed in floating point, so nothing can
overflow to inf or NaN (raw 1e-308 with adjusted 1e6 and 1e7 is a tenfold
factor mismatch and is refused), and a difference exactly at a tolerance is
accepted.

### Tolerances

From production data (2,126,745 stored rows, 2026-10-10):
- OHLC ordering: no violation, raw or adjusted, so the check is strict.
- Values: no non-positive price, negative volume or dividend, non-positive split factor or NaN.
- Off-session rows: none.
- Within-bar adjustment factor: Tiingo rounds adjusted fields, often to three decimals. 192 rows deviate by more than 1e-6; the largest deviation is 0.17% (AAMRQ). A 1% tolerance accepts all of them and still rejects a misapplied split or an incoherent bar. Sub-dollar prices with three-decimal rounding could exceed it; none do in the sample.

What the 1% tolerance does not catch: it compares fields within one bar. A
small corporate-action misapplication applied coherently to a whole bar (every
adjusted field off by the same factor) passes it. Adjusted-only errors do not
affect Light's sizing, which uses raw closes × share counts, but they do affect
everything computed from adjusted closes: returns, volatility, beta.

## The stored-row contract

The verification snapshot, the verdict's digest and `promote`'s locked re-read
use one projection, `STORED_FIELDS`: all twelve value columns of the ticker's
rows in `[CALENDAR_SUPPORTED_FROM, as_of]`. The digest hashes every date and
the exact representation of every value. A change to any column — including
volume, adjusted volume, dividend or split factor, which the provider
comparison does not use — between the snapshot and the promotion is seen, and
nothing is written.

## Promotion

`promote(conn, ticker, verdict)` certifies `load` and
`complete_nothing_to_insert` alike, in one transaction:
1. Lock the ticker's `instruments` row `FOR UPDATE`.
2. Re-read the stored rows (the same twelve-column projection and range) and recompute the digest. If it differs, write nothing; the caller records `history_incomplete: stored_rows_changed_during_pass`.
3. Otherwise insert exactly the verdict's rows (`INSERT … ON CONFLICT DO NOTHING`, 500-row statements) and write `history_complete` and `complete_through` together.

An error anywhere leaves neither rows nor status. `record_ticker_status`
refuses `history_complete`. Tests check structurally (a static source scan and
an intercepted run of the real entrypoint) that no other code path inserts
history rows or writes completion.

The lock also serializes with Light's `ingest_one_ticker`, which upserts the
ticker's `instruments` row first and then its prices (all twelve fields) in the
same transaction: whichever commits second sees the other's work. There is no
residual race between the two writers.

"One promotion path" is a property of the history phase. The ring remains a
separate writer: its own `INSERT … ON CONFLICT DO UPDATE` through the inherited
mapper (`build_eod_rows`), which truncates dates to their first ten characters
and checks neither bar coherence nor sessions. Bringing the ring under the same
validation is a pre-existing follow-up, not part of this change.

A promotion holds its locks for one transaction. Measured on the production
layout (disposable PG18 TimescaleDB, 692 monthly chunks, 688 compressed), a
1970-to-date promotion of 14,315 rows took 2.5 s and held 3,457 locks, five per
chunk. Production's lock table holds 256 × 100 = 25,600 entries, 3,973 of them
in use when sampled (2026-10-10). A container at the TimescaleDB image defaults
(128 × 25) cannot hold it, so the end-to-end pre-1970 test replays as of
1970-12-31.

## What `history_complete` certifies

`history_complete` certifies that, at `checked_at`, the stored series for
`[history_start, end]` (with `history_start` no earlier than 1970-01-01) was
exactly the interval's sessions; every stored and inserted bar was a valid bar
(all twelve fields, rule 8 and the predicate above); and raw and adjusted OHLC
equalled Tiingo's on every date to 1e-6. Volume, adjusted volume, dividend and
split factor of retained rows are validated but not compared with Tiingo. It
does not certify adjusted coherence after a later corporate action.

After a split or dividend, Tiingo re-bases every older adjusted value, while the ring's 5-day overlap, Light's 7-day refresh and this worker's
inserts each leave older stored rows on the basis they were fetched on. That
ownership question for adjusted history predates this change. The 90-day
re-verification reports such a ticker as `adjustment_rebase_required`; it never
rewrites rows. Resolving it needs a governed re-base of stored adjusted
history.

## Session gaps (decision pending)

Rule 13 is strict and `accepted_gaps` is empty: no session may be missing. In
production, Tiingo's own series has holes on illiquid no-trade days, halts and
reused tickers:
- 17 of the 199 covered tickers with rows have 4,892 missing sessions between their first and last stored row (AHG 2,025, UN 1,404, CCU 18 scattered days);
- 76 of a 535-ticker sample (14%) have holes.

Under the strict rule, roughly a tenth of covered tickers stay
`history_incomplete` on weekly retries with no history loaded. The validator
takes evidenced gaps as an explicit input; nothing supplies them yet, and this
change does not implement accepted gaps. A follow-up must not rest on two
fetches agreeing 12 hours apart alone — the same provider can repeat its own
hole. It needs:
- a fixed identity for what is accepted: the security (not just the symbol, which can be reused) and the exact interval;
- persisted observations with response provenance (when fetched, what was requested, a digest of what came back);
- a finalization policy: when an observed gap becomes final, and what re-opens it;
- and either independent no-trade or halt evidence for each accepted session, or narrower semantics: a distinct status that certifies only that the provider's series has these gaps, not that the sessions had no trading.

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

## Tests and their independence

- `tests/test_eod_history_fuzz.py`: differential fuzz of `validate_series`
  against `tests/_eod_history_reference.py`. The corpus is a deterministic sweep
  (1,016 cases: every field × every corruption kind on provider and stored
  data, every date variant, every tolerance edge at, one ulp inside and one ulp
  outside, the bounds, extreme magnitudes, series and store shapes, the 1970
  edge, and a window around every reference holiday) followed by seeded random
  cases (3,000 in CI).
  Verdict, reason code and rows must match; certifying verdicts must satisfy a
  test-local exact invariant; the digest must be order-independent and change
  when any stored value changes or a row is deleted.
- `tests/test_eod_history_mutants.py`: 39 mutants of the validation module,
  each an explicit text patch (the review gate's 26 plus 13 of this suite's
  own). Each must be killed by its deterministic targeted counterexample and by
  the seeded corpus (sweep + 1,500 random cases); the score is 39/39, and the
  sweep alone kills all 39.
- Calendar independence: the reference does not use `exchange_calendars`. Its
  sessions are weekdays minus a hard-coded list of NYSE closures for 1970,
  1973, 1974, 1982, 1985, 2001, 2012 and 2023–2026 (special closures included:
  1973-01-25, 1985-09-27, 2001-09-11..14, 2012-10-29/30, 2025-01-09), and a test
  checks both calendars agree on every one of those years. Fuzz intervals are
  drawn from those years. Residual dependence: for every other year, production
  sessions come from `exchange_calendars` 4.13.2 (pinned) alone, and the
  test-local session invariant uses the same library.

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

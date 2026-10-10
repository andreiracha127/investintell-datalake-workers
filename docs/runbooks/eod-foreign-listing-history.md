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
  2015-12-31, 2020-12-31 and 2025-12-31. The ratio status does not matter:
  prices are needed whether or not the ADS ratio is evidenced.

Tiingo decides existence. Without the W1c resolver (a fresh environment) the
source reports `absent` and the ring runs unchanged. The worker asserts symbol
coverage, not issuer ownership: a reused ticker (GOLD was Randgold, then
Barrick) carries the current security's history, so Light must keep using the
line's evidenced dates.

## Loading rule

Every pending ticker gets one meta call (`GET /tiingo/daily/{ticker}`) for a
fresh `startDate`. It also fills NULL `name`, `exchange_code` and Tiingo dates
in `instruments`, or inserts the row with `asset_type = 'stock'`; non-null
fields and an existing `asset_type` are never changed. Then:

| Covered ticker | Price window | Written |
|---|---|---|
| no `eod_prices` rows | `startDate` → run date | every row |
| rows start after `startDate` (+5 days tolerance) | `startDate` → first stored date + 21 days | only rows older than the first stored row, after the basis check |
| rows reach `startDate` | none | status only |

Basis check: on the first 10 stored sessions inside the window, Tiingo's raw
close and adjusted open/high/low/close must equal the stored values within a
relative 1e-6. If they do, the older prefix continues the stored series on the
same basis. If the adjusted values moved (a split or dividend since the stored
rows were fetched), nothing is inserted and the ticker is
`adjustment_rebase_required` with the observed ratio. If the raw closes differ,
it is `history_conflict`. Existing rows are never rewritten.

A ticker's history rows and its `history_complete` status are written in ONE
transaction, in 500-row statements with `INSERT … ON CONFLICT (ticker, date) DO
NOTHING` (about 7.5k rows for a 30-year ticker). An interrupted load leaves
neither rows nor status, so the next run refetches the whole window. A response
with any unusable bar (a missing field), or an empty window while meta says
history exists, is `history_incomplete`: nothing is written, it counts as an
error and is retried after 12 hours.

## Status and ring admission

`eod_warmer_ticker_status` (created by the worker next to `eod_warmer_cursor`):

| Status | Meaning | Next attempt |
|---|---|---|
| `history_complete` | loaded atomically, or rows already reach `startDate` | never |
| `history_incomplete` | unusable bar, empty window, no overlap to compare | after 12 h |
| `adjustment_rebase_required` | adjusted basis moved; detail has the ratio | after 30 days |
| `history_conflict` | raw closes differ | after 30 days |
| `tiingo_unknown` | meta 404, no `startDate`, prices 404 | after 30 days |

Completion is only this recorded status, never inferred from `min(date)`. The
ring admits a covered ticker once it is `history_complete`. It also keeps
refreshing two kinds of existing series without touching their history: a
screener or benchmark ticker that already has rows, and a fail-closed
(`adjustment_rebase_required` / `history_conflict`) ticker. Every other covered
ticker without a complete status stays out of the ring, so none takes the
745-day cold start and a failed load is never refreshed into looking complete.
The history queue serves first the lines the ring would serve but excludes
(rows outside the screener, screener lines without rows), then new coverage.

Transient Tiingo failures write nothing and are retried next run. Run stats
report `completed`, `deferred`, `history_rows`, `errors` / `error_tickers`,
`fail_closed` / `fail_closed_tickers`, `tiingo_unknown` and `waiting` per status.
`deferred = 0` means the queue is drained, not that every ticker is complete.

## Readers

The warmer connects as `worker_writer`. Its default ACL in `public` grants only
`app_analytics_ro` SELECT, so a table it creates is not readable by
`app_runtime` or `mcp_ro` by default (`eod_warmer_cursor` has explicit grants).
The worker's idempotent DDL therefore grants SELECT on
`eod_warmer_ticker_status` to `app_runtime`, `app_analytics_ro` and `mcp_ro`
where those roles exist.

## Budget

The history phase runs after the ring, on the same `TokenBucket` at
`DEFAULT_RATE_PER_S` (2.5 req/s, unchanged), and is skipped when the ring
aborted on Tiingo's 429 breaker. Each ticker costs two logical requests (meta,
one price window), and the client retries each up to three times, so at most
six HTTP attempts. The cap is `EOD_HISTORY_TICKERS_PER_RUN` (default 25, never
above `WORKER_LIMIT`, `0` turns the phase off):

| Cap | Logical requests per run | HTTP attempts per run (worst case) |
|---:|---:|---:|
| 25 | ≤ 50 | ≤ 150 |
| 250 | ≤ 500 | ≤ 1,500 |

A budget abort inside the phase sets `aborted`, so the run exits non-zero like
a ring abort.

## Compression

`eod_prices` is a hypertable with monthly chunks (a 360-day interval since
2026-07), segmentby `ticker`, orderby `date DESC`, and a 90-day columnstore
policy every 12 hours. On 2026-10-10, 786 of its 787 chunks were compressed, so
almost every history row lands in a compressed chunk.

History rows are keys older than the ticker's first existing row. A new key
matches no compressed batch (segmentby ticker, date min/max), so nothing is
decompressed and `timescaledb.max_tuples_decompressed_per_dml_transaction`
(100,000 in production) is never approached, even with a whole ticker in one
transaction. The rows go to the chunks' uncompressed part; the columnstore
policy recompresses those partial chunks.

Measured on `timescale/timescaledb:2.27.2-pg18` with 5,000 tickers × 12 monthly
chunks (110,000 rows per chunk), through `load_ticker_history` with one
transaction per ticker (largest 8,290 rows): 502,150 history rows for 20 new and
50 truncated tickers inserted at about 10,700 rows/s. This held at the
production limit and at a limit of 1: compressed batch counts were unchanged, no
existing row reached a chunk heap, and the existing rows' checksum was
identical. As a control, the ring's `DO UPDATE` on one existing compressed key
failed at a limit of 1 and decompressed that ticker's 21-row batch at the
default. The policy job recompressed every partial chunk in 8.5 seconds.
`tests/test_eod_foreign_listing_coverage_db.py` holds the same checks.

## Production numbers (read-only dry run, 2026-10-10)

- Covered symbols: 1,676. Listing-resolved lines: 17, 156, 760 and 1,474 at the
  2010, 2015, 2020 and 2025 year-ends, and 1,565 at the run date. With both
  listing and ratio resolved, the counts are 2, 83, 520, 1,150 and 1,226.
- 1,477 have no rows: meta plus a full load. 199 have rows: 85 from 2024-06-11
  with no Tiingo dates in `instruments` (the screener's 745-day cohort; each
  gets the overlap window and basis check) and 114 whose rows reach the stored
  start (each needs only the meta call if Tiingo's start agrees). Expect part of
  the 85 to fail closed as `adjustment_rebase_required`: any dividend or split
  since their cold start moves Tiingo's adjusted basis.
- Requests: at most 1,676 meta and 1,562 price windows, 3,238 logical and at
  most 9,714 HTTP attempts. That is 68 runs at the default 25 (about 23 days at
  the production cron), or 7 runs at 250 (about 2.3 days).
- Rows: at least about 2.1M, estimated from each ticker's first SEC observation.
  The ten-ticker meta sample averages 29.8 years of history, which would give
  about 11M if every new ticker were that old.
- Ring size: 5,198 tickers today, up to 6,675 afterwards.
- On the first run, 29 covered tickers that have rows but are not screener
  constituents leave the ring until their history settles (complete or fail
  closed). They are first in the history queue, so at the default cap of 25
  they are processed within the first two runs.

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
    plan = w.plan_foreign_history(conn, symbols, now=dt.datetime.now(dt.UTC))
print(len(symbols), len(plan["complete"]), {k: len(v) for k, v in plan["waiting"].items()},
      dict(collections.Counter(w.classify_history_task(t) for t in plan["pending"])))
'@ | railway run --service api -- uv run --no-project --with "psycopg[binary]" --with httpx python -
```

## Staged rollout

1. Merge. Deploy the merged `main` SHA to `eod-prices-warmer`
   (`railway redeploy --service eod-prices-warmer --from-source --yes` builds;
   on this project a cron deployment does not execute). Without any variable
   change the phase runs at the default cap of 25. To deploy the code dark
   first, set `EOD_HISTORY_TICKERS_PER_RUN=0` before the deploy.
2. Owner decision before or with the deploy: `WORKER_LIMIT=2300` (see the
   freshness table). Confirm the Light deployment that reads this history
   includes the D6 alive-span checks.
3. Run at 25 for at least a day (three cron slots; to start at once,
   `railway service restart --service eod-prices-warmer`, which takes the same
   advisory lock as the cron). Check after each run:
   - completeness: `SELECT status, detail, count(*) FROM eod_warmer_ticker_status GROUP BY 1, 2;`
     and, for completed new tickers, that no Tiingo session is missing between
     `history_start` and the first ring date;
   - rebase and conflict counts, with their ratios (`fail_closed_tickers`);
   - `aborted` and 429s in the stats and logs;
   - ring freshness: the oldest `max(date)` among ring tickers;
   - compression recovery: partial chunks drop back after the next policy run:
     `SELECT count(*) FROM _timescaledb_catalog.chunk ch JOIN _timescaledb_catalog.hypertable h ON h.id = ch.hypertable_id WHERE h.table_name = 'eod_prices' AND (ch.status & 8) = 8;`
4. Raise `EOD_HISTORY_TICKERS_PER_RUN` to 250 once those hold. Expected first
   dates once loaded: TSM 1997-10-09, ASML 1995-03-16, NVO 1982-01-04, SAP
   1995-09-18, NTES 2000-06-30, QGEN 1996-06-28 (if its basis check passes).

To pause, set `EOD_HISTORY_TICKERS_PER_RUN=0`. Rollback is the previous image:
the new table holds only progress state, and the inserted history is ordinary
`eod_prices` rows.

## Follow-ups (pre-existing, not changed here)

- The ring refreshes only `max(date)` − 5 days. After a split or dividend,
  Tiingo re-bases every older adjusted value, but the stored older rows keep the
  basis they were fetched on, so each corporate action leaves an adjusted-price
  seam at the overlap boundary for every ticker. Raw prices are unaffected.
  Fixing it needs a governed re-base of stored adjusted history; the same re-base
  would clear `adjustment_rebase_required` tickers here.
- `history_complete` covers the history older than a ticker's first stored row.
  Interior gaps in rows written by the ring or the API are not checked.

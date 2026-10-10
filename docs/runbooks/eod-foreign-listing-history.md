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
source reports `absent` and the ring runs unchanged.

## Cold-start rule

| Covered ticker | Fetch | Write |
|---|---|---|
| no `eod_prices` rows | Tiingo `startDate` → run date | all rows |
| rows start after `startDate` (+5 days tolerance) | `startDate` → `min(date)` − 1 | older rows only |
| rows reach `startDate` | nothing | recorded complete |

Screener and benchmark tickers keep the 745-day cold start. A covered ticker
without rows is kept out of the ring for that run, so it never takes the 745-day
window. Once it has rows, the `eod_prices` term of the warming universe refreshes
it like any other ticker (`max(date)` − 5 days → today).

`instruments`: a ticker without a row, or whose row lacks `exchange_code` or a
Tiingo date, gets one meta call (`GET /tiingo/daily/{ticker}`). A new row is
inserted with `name`, `exchange_code`, `asset_type = 'stock'`, `tiingo_start_date`
and `tiingo_end_date`. An existing row only has its NULL `name`,
`exchange_code` and dates filled; non-null fields and `asset_type` are never
changed, and a row with nothing to fill is not touched.

## State, resume and unknown tickers

The worker creates `eod_warmer_ticker_status` next to `eod_warmer_cursor`:

- `history_complete`: the full window was fetched and inserted (or the rows
  already reached `startDate`), with `history_start`.
- `tiingo_unknown`: meta 404 (`not_found`), meta without `startDate`
  (`no_start_date`), prices 404 (`prices_not_found`), or a known ticker with no
  usable bar (`no_prices`). These are reported in the run's `foreign_history`
  stats, are not errors, and are checked again after 30 days.

Everything else is pending, so the next run resumes where the previous one
stopped. Transient Tiingo failures write nothing and are retried next run.

## Budget

The history phase runs after the ring, on the same `TokenBucket` at
`DEFAULT_RATE_PER_S`. A ring that aborted on Tiingo's 429 breaker skips it. It
stops at 250 tickers per run (each costs at most a meta call and one price
request), lowered to `WORKER_LIMIT` when that is smaller. Set
`EOD_HISTORY_TICKERS_PER_RUN` to change the cap; `0` turns the phase off. A
budget abort inside the phase sets `aborted`, so the run exits non-zero like a
ring abort.

## Compression

`eod_prices` is a hypertable with monthly chunks (a 360-day interval since
2026-07), segmentby `ticker`, orderby `date DESC`, and a 90-day columnstore
policy every 12 hours. On 2026-10-10, 786 of its 787 chunks were compressed, so
almost every history row lands in a compressed chunk.

History rows use `INSERT … ON CONFLICT (ticker, date) DO NOTHING` in 500-row
transactions, only for keys older than the ticker's first existing row. A new key
matches no compressed batch (segmentby ticker, date min/max), so nothing is
decompressed and `timescaledb.max_tuples_decompressed_per_dml_transaction`
(100,000 in production) is never approached. The rows go to the chunks'
uncompressed part; the columnstore policy recompresses those partial chunks.

Measured on `timescale/timescaledb:2.27.2-pg18` with 5,000 tickers × 12 monthly
chunks (110,000 rows per chunk): 502,150 history rows for 20 new and 50
truncated tickers inserted at about 10,000 rows/s. This held at the production
limit and at a limit of 1: compressed batch counts were unchanged, no existing
row reached a chunk heap, and the existing rows' checksum was identical. As a
control, the ring's `DO UPDATE` on one existing compressed key failed at a limit
of 1 and decompressed that ticker's 21-row batch at the default. Running the
policy job recompressed every partial chunk in 8 seconds.
`tests/test_eod_foreign_listing_coverage_db.py` holds the same checks.

## Production numbers (read-only dry run, 2026-10-10)

- Covered symbols: 1,676. Listing-resolved lines: 17, 156, 760 and 1,474 at the
  2010, 2015, 2020 and 2025 year-ends, and 1,565 at the run date. With both
  listing and ratio resolved, the counts are 2, 83, 520, 1,150 and 1,226.
- 1,477 have no rows and need a meta call and a full fetch. 85 have rows from
  2024-06-11 and no Tiingo dates in `instruments`, so they need a meta call and
  probably an older-history fetch. 114 already reach their Tiingo start and are
  recorded without a request. None of the covered tickers without rows is a
  screener constituent.
- Requests: at most 1,562 meta calls and 1,562 price calls. That is 7 runs at
  250 tickers, about 2.3 days at the production cron (`10 1,13,21 * * *`).
- Rows: at least about 2.1M, estimated from each ticker's first SEC
  observation. The ten-ticker meta sample averages 29.8 years of history, which
  would give about 11M if every new ticker were that old. Per run, that is
  roughly 0.3M to 1.9M rows, or 0.5 to 3 minutes at the measured insert rate.
- Ring size: 5,198 tickers today, up to 6,675 afterwards. With
  `WORKER_LIMIT=2000` and three runs a day, each ticker is then refreshed about
  every 27 hours instead of every 21. A `WORKER_LIMIT` of about 2,300 keeps the
  ring under a day. That is a Railway setting for the owner and is not part of
  this change.

## Read-only preview

From `E:\tmp-deploy\api\backend`, outside 06:00–08:30 UTC, preview the source
set and classify each ticker as `new`, `needs_meta`, `truncated` or `complete`.
This makes no Tiingo call and no write:

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
    plan = w.plan_foreign_history(conn, symbols, now=dt.datetime.now(dt.timezone.utc))
print(len(symbols), len(plan["complete"]), len(plan["unknown"]),
      dict(collections.Counter(w.classify_history_task(t) for t in plan["pending"])))
'@ | railway run --service api -- uv run --no-project --with "psycopg[binary]" --with httpx python -
```

## Production procedure

1. Merge. Deploy the merged `main` SHA to `eod-prices-warmer`. On this project
   `railway redeploy --service eod-prices-warmer --from-source --yes` builds but
   does not run a cron service. No variable or schedule change is needed.
2. The next cron slot (01:10, 13:10 or 21:10 UTC) runs the ring, then covers up
   to 250 tickers, creating `eod_warmer_ticker_status` on its first run. To start
   right away, run `railway service restart --service eod-prices-warmer`, which
   executes the deployed job once. It takes the same advisory lock as the cron,
   so the two cannot overlap.
3. Each run prints its stats line with `foreign_history`: `completed`,
   `deferred`, `history_rows`, `tiingo_unknown` and `error_tickers`. `deferred`
   reaches 0 after about seven runs.
4. Verify in the tables, not on the dashboard:

   ```sql
   SELECT status, detail, count(*) FROM eod_warmer_ticker_status GROUP BY 1, 2;
   SELECT ticker, min(date), max(date), count(*) FROM eod_prices
   WHERE ticker IN ('TSM', 'ASML', 'NVO', 'SAP', 'NTES', 'QGEN') GROUP BY 1;
   SELECT count(*) FILTER (WHERE is_compressed), count(*)
   FROM timescaledb_information.chunks WHERE hypertable_name = 'eod_prices';
   ```

   Expected: TSM from 1997-10-09, ASML from 1995-03-16, NVO from 1982-01-04,
   SAP from 1995-09-18, NTES from 2000-06-30, QGEN from 1996-06-28. The
   compressed-chunk count returns to its previous level after the next
   columnstore policy run.

To pause the phase without a deploy, set `EOD_HISTORY_TICKERS_PER_RUN=0` on the
service. Rollback is the previous image: the new table holds only progress
state, and the inserted history is ordinary `eod_prices` rows.

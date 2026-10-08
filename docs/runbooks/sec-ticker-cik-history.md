# SEC ticker -> issuer (CIK, class) history

Point-in-time answer to "which issuer, and which of its share classes, traded as
ticker T on date D", from public SEC data only. Light's walk-forward market prior
uses it to size equities at each fold (`sec_ticker_issuer_at`,
`sec_issuer_line_at`, `sec_cover_class_shares_at`).

| Object | What it holds |
|---|---|
| `sec_ticker_cik_observations` | One row per filing x class context x ticker: the registrant's cover-page `dei:TradingSymbol`, with `dei:Security12bTitle`, `dei:SecurityExchangeName`, the class segments and a `security_kind` |
| `sec_cover_share_counts` | Cover-page `dei:EntityCommonStockSharesOutstanding`, per class or in total, with the date it is stated as of |
| `sec_registration_events` | Forms 15-12B, 15-12G, 15-15D, 25 and 25-NSE from the EDGAR form indexes |
| `sec_ticker_cik_packages` | One row per loaded DERA package or EDGAR index: digest, size and the loader's counts |
| `sec_ticker_intervals` (view) | Every hold as known today, for diagnostics; decisions use the functions |
| `sec_ticker_price_span(ticker, cik[, class_key])` | Price lineage: the runs in which the issuer's line held the ticker, with the end of the previous holder (another CIK) and the start of the next; uses everything known today |

Sources: the DERA [Financial Statement and Notes data sets](https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets)
(`sub.tsv`, `txt.tsv`, `num.tsv`, `dim.tsv`, streamed from each zip) and the
[EDGAR full-index](https://www.sec.gov/Archives/edgar/full-index/) `form.gz` files.
Both are fetched with the SEC User-Agent, one request at a time.

## Semantics

**Line.** A security line is `(cik, class_key)`. `class_key` is the cover
context's dimension segments without the listing-exchange and legal-entity axes
(`ClassOfStock=CommonClassA;`); `''` when the fact has no dimensions. Symbols and
share counts of the same filing join on it.

**Knowledge date.** `available_on` is the EDGAR acceptance date when DERA carries
it, else the filing date + 1. A fact is usable from its knowledge date only, so a
late-reported change counts from when it became public. The fact's `ddate` is
stored but never dates a symbol statement (DERA rounds it). `sub.prevrpt` is not
applied: an amended original keeps its own row and date.

**Intervals.** A line holds its symbol from the first filing that shows it until
the first later filing of the same line that shows another symbol, or until a
deregistration applies to it. At D, using only what was public at D:

- the line's *statement* is its latest filing available on or before D;
- the hold has **ended** if that statement shows another symbol, or if after it a
  15-12G or 15-15D (the registrant stops reporting) or a 15-12B, 25 or 25-NSE
  became public. The class-specific forms end the hold only when the statement's
  filing listed a single symbol: a 25-NSE for one of several listed securities
  (often notes or a preferred series) cannot be attributed to a class without
  reading the form;
- an open hold whose statement is older than 400 days is **stale** (no recent
  confirmation);
- otherwise it is **active**.

`sec_ticker_issuer_at(T, D)` returns `resolved` (exactly one CIK holds an active
interval; `class_key` is its line), `ambiguous` (two or more CIKs hold
overlapping active intervals; `active_ciks` lists them), `stale`, `ended` or
`missing` (no filing public by D showed T).

`sec_issuer_line_at(cik, class_key, D)` returns what a known line traded as at D.
It follows a rename backwards. An issuer whose latest cover filing by D lists
exactly one equity or depositary line is followed through that line whatever
its member was called; a multi-class issuer only through its own member
(`ambiguous_class` when that member has no statement by D). `equity_lines` is the
number of listed equity/depositary lines in that filing.

`sec_ticker_price_span(T, cik, class_key)` answers a different question: which
stored vendor price rows of T belong to that issuer. It is lineage, not a
decision at D, so it uses everything known today. One row per run of a line of
the CIK holding T (all of its lines when `class_key` is NULL), with the same end
rules as above, plus `prior_holder_end` (latest end evidence of a run of another
CIK that started earlier) and `next_holder_start` (earliest start of another
CIK's run). Lines of the same CIK with the same symbol are one security
relabelled, so other holders are other CIKs. Equity and depositary lines decide
whenever any showed the ticker, in both the point functions and the span: filers
also tag their common symbol on notes lines.

`sec_cover_class_shares_at(cik, class_key, D)` returns the class's own cover
count: the latest stated date (`ddate <= D`, filing public by D), then the latest
filing; distinct values at that point are `ambiguous`; a date older than 400
days is `stale`. `class_key = ''` is the issuer total.

## Edge cases

| Case | Behaviour |
|---|---|
| Ticker reassignment (AT&T Corp, CIK 5907, today `T1` at sec-api; AT&T Inc, CIK 732717) | The old holder's interval ends at its deregistration (Form 15) or its own move to another symbol; the new holder's starts at its first statement. Without an end event the two overlap and D is `ambiguous` until the old hold is stale |
| Renamed ticker (SQ -> XYZ, same CIK) | The rename filing ends `SQ` and starts `XYZ`. `XYZ` is `missing` before the rename; `sec_issuer_line_at(1512673, '', D)` shows `SQ` |
| Multi-class issuer (BRK-A/BRK-B, BF-A/BF-B, GOOGL/GOOG) | Each class is its own line with its own symbol and, when the cover reports it, its own share count. An issuer total is never spread over several listed classes |
| Class shares written without a separator (`BFB`) | Resolution matches on the separator-free key (`BF-B` = `BF.B` = `BFB`); a key shared by two issuers is `ambiguous`, never merged |
| Dual listing (same class on two exchanges) | The exchange axis is not part of the class: both listings are the same line |
| ADRs (20-F/40-F filers) | The ADS line is `security_kind = 'depositary'`. The cover counts ordinary shares; the ADS ratio is not on the cover, so consumers must not size an ADS with an ordinary-share count |
| Notes, preferred, warrants, units, rights | Kept with their own symbols and `security_kind`; consumers filter to `equity` |
| Co-registrant facts (`coreg`) | Skipped: they describe another entity than `sub.cik` |
| Before mandatory cover tagging | Mandatory for periods ending on or after 2019-06-15 (large accelerated), 2020-06-15 (accelerated), 2021-06-15 (others). Earlier, about 1,500-3,000 issuers per quarter tagged `TradingSymbol` voluntarily; coverage is lower there |

## Load

```powershell
# Fetch any missing DERA packages and EDGAR form indexes, then load everything.
$env:PYTHONPATH = "."
python -m scripts.load_sec_ticker_cik_history --download `
  --packages-dir E:/Edgard/fsn --index-dir E:/Edgard/edgar-index --dsn "$env:DATABASE_URL"
# Parse only, no database:
python -m scripts.load_sec_ticker_cik_history --dry-run --packages-dir E:/Edgard/fsn
```

Each package and index is one transaction; a re-run converges (rows of a
package's filings that the current rules no longer produce are removed). The
loader refuses to run before the governed schema exists, and holds advisory lock
900_368 so it never interleaves with the recurring worker.

Full local run (2026-10-08, postgres:16 on the same workstation): 79 DERA packages
(2009q1 to 2026_09, 25 GB of zips) and 72 EDGAR quarterly indexes in **12.6 min**
(parsing 700 s, database writes 42 s): 885,052 submissions, 866,258
`TradingSymbol` facts, **859,862** observations, **480,903** cover share counts,
**47,512** registration events.

## Recurring worker

`src/workers/sec_ticker_cik_history.py` (`WORKER=sec_ticker_cik_history`), Railway
config `railway.sec-ticker-cik-history.toml`, cron **`0 10 * * 1`** (Mondays
10:00 UTC; DERA publishes one package a month). Each run takes lock 900_368,
loads every listed package not yet in `sec_ticker_cik_packages` (oldest first),
reloads the newest one when its size changed, and refreshes the EDGAR form
indexes of the current and previous quarter. Packages are fetched, loaded and
deleted one at a time (about 0.6 GB of disk at most) unless
`SEC_TICKER_CACHE_DIR` points at a volume. `WORKER_LIMIT` caps packages per run;
the backlog resumes on the next run. The worker never applies DDL and refuses to
run without the schema. Exit is non-zero on an error or `lock_busy`. Env:
`DATABASE_URL` (`worker_writer`); no API key.

## Production steps (owner)

1. Apply `schemas/sec_ticker_cik_history_v1.sql` as `postgres` (or `worker_writer`)
   with `psql -v ON_ERROR_STOP=1 -f schemas/sec_ticker_cik_history_v1.sql`. It
   creates the four tables, the view and five functions, sets the owner to
   `worker_writer`, revokes PUBLIC and grants SELECT/EXECUTE to `app_runtime`,
   `app_analytics_ro` and `mcp_ro`.
2. Run the initial load as `worker_writer`, from the workstation that holds
   `E:/Edgard/fsn` and `E:/Edgard/edgar-index` (add `--download` to fetch what is
   missing): `python -m scripts.load_sec_ticker_cik_history --dsn <worker_writer DSN>`.
   Expect about 860k observations, 481k share counts and 47.5k events, and
   15-25 minutes (parsing is local; the writes are 1.4M rows of COPY).
3. Check: `SELECT count(*), max(available_on) FROM sec_ticker_cik_observations;`,
   `SELECT * FROM sec_ticker_issuer_at('BRK-B', current_date);` (resolved, CIK
   1067983, class B line).
4. Create the Railway service `sec-ticker-cik-history` from this repository with
   `railway.sec-ticker-cik-history.toml`, `WORKER=sec_ticker_cik_history` and the
   `worker_writer` `DATABASE_URL`. The cron is in the config file.
5. Light's walk-forward equity sizing calls these functions: deploy the Light
   change only after steps 1-2.

Rollback: `schemas/sec_ticker_cik_history_v1.rollback.sql` (as the same role),
and remove the Railway service.

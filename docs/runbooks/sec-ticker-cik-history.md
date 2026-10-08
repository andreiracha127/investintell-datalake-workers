# SEC ticker -> issuer (CIK, class) history

Point-in-time answer to "which issuer, and which of its share classes, traded as
ticker T on date D", from public SEC data only. Light's walk-forward market prior
uses it to size equities at each fold (`sec_ticker_issuer_at`,
`sec_issuer_line_at`, `sec_cover_class_shares_at`).

| Object | What it holds |
|---|---|
| `sec_ticker_cik_observations` | One row per filing x class context x ticker: the registrant's cover-page `dei:TradingSymbol`, with `dei:Security12bTitle`, `dei:SecurityExchangeName`, the class segments and a `security_kind` |
| `sec_cover_share_counts` | Cover-page `dei:EntityCommonStockSharesOutstanding`, per class or in total, with the date the cover states it as of (`stated_on`) and DERA's rounded month end (`ddate_rounded`) |
| `sec_registration_events` | Forms 15-12B, 15-12G, 15-15D, 25 and 25-NSE and their `/A` amendments from the EDGAR form indexes |
| `sec_ticker_cik_packages` | One row per loaded DERA package or EDGAR index: digest, size and the loader's counts |
| `sec_ticker_cik_package_members` | The (accession, CIK) pairs each package or index contains, for replacement reconciliation |
| `sec_ticker_intervals` (view) | Every hold as known today, for diagnostics; decisions use the functions |
| `sec_ticker_price_span(ticker, cik[, class_key])` | Price lineage: the runs in which the issuer's line held the ticker, with the end of the previous holder (another CIK) and the start of the next; uses everything known today |

Sources: the DERA [Financial Statement and Notes data sets](https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets)
(`sub.tsv`, `txt.tsv`, `num.tsv`, `dim.tsv`, streamed from each zip) and the
[EDGAR full-index](https://www.sec.gov/Archives/edgar/full-index/) `form.gz` files.
Both are fetched with the SEC User-Agent, one request at a time.

## Semantics

**Class and line.** A class is `(cik, class_key)`. `class_key` is the cover
context's dimension segments without the listing-exchange axis, and without the
legal-entity axis when it names a registrant (`ClassOfStock=CommonClassA;`); `''`
when the fact has no dimensions. Symbols and share counts of the same filing join
on it. A **line** is `(cik, line_key)`: the class, except that the one
equity/depositary class of a filing that lists exactly one is the issuer's sole
equity line `'*'`, whatever its member is called. Single-class filers add, drop
and rename the class dimension between filings; on the sole equity line a rename
in the same step as a relabel still ends the old symbol, in resolution and in
lineage alike.

**Whose fact.** A fact without a co-registrant (`coreg`) belongs to `sub.cik`. A
legal-entity context whose member carries its own `dei:EntityCentralIndexKey`
belongs to that CIK: a genuine co-registrant (SDG&E in Sempra's combined 10-K) or
the registrant itself. A member without one is the registrant's own class only in
a single-registrant filing in which no member names a registrant and the member's
context titles a security; Renalytix (`RNLX`) and Nano Dimension (`NNDM`) name
their ADS that way. Any other legal-entity fact is unattributable and counted as
rejected.

**Knowledge date.** `available_on` is the EDGAR acceptance date when DERA carries
it, else the filing date + 1. A fact is usable from its knowledge date only, so a
late-reported change counts from when it became public. The fact's `ddate` is
stored but never dates a symbol statement (DERA rounds it to a month end). A cover
share count is dated by the day the cover states (`stated_on = ddate - datp`,
checked against the exact XBRL context dates of the same accessions for 352 of
353 sampled counts in 2010-2024 packages). `sub.prevrpt` is not applied: an
amended original keeps its own row and date.

**Intervals.** A line holds its symbol from the first filing that shows it until
the first later filing of the same line that shows another symbol, or until a
deregistration applies to it. At D, using only what was public at D:

- the line's *statement* is its latest filing available on or before D;
- the hold has **ended** if that statement shows another symbol, or if after it a
  15-12G or 15-15D (the registrant stops reporting) or a 15-12B, 25 or 25-NSE
  became public. The class-specific forms end the hold only when the statement's
  filing listed a single symbol: a 25-NSE for one of several listed securities
  (often notes or a preferred series) cannot be attributed to a class without
  reading the form. An amendment (`25-NSE/A`, `15-12G/A`...) supersedes the latest
  earlier original of its form for the CIK from its own knowledge date: the index
  cannot say whether it corrects or withdraws the original (Minim's 25-NSE/A of
  2025-04-09 withdrew its 2024 delisting), so from then on the original ends
  nothing;
- an open hold whose statement is older than 400 days is **stale** (no recent
  confirmation);
- otherwise it is **active**.

`sec_ticker_issuer_at(T, D)` returns `resolved` (exactly one CIK holds an active
interval; `class_key` is its line), `ambiguous` (two or more CIKs hold
overlapping active intervals; `active_ciks` lists them), `stale`, `ended` or
`missing` (no filing public by D showed T).

`sec_issuer_line_at(cik, class_key, D)` returns what a known class traded as at
D. It follows a rename backwards along the class's line today: through the sole
equity line when today's filings list one equity class, else through its own
class (`ambiguous_class` when the class has no statement by D and the issuer then
listed several). `equity_lines` is the number of listed equity/depositary classes
in the issuer's latest cover filing by D.

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

`sec_cover_ticker_shares_at(T, cik, D)` returns the cover count of the class that
trades as T, joined inside each filing: a count whose class is tagged with T in
the same filing, or the filing's total when its one equity class (never a
depositary line) shows T. Member names change between filings (Berkshire's
10-Q counts `CommonClassB` while its 8-Ks tag BRK.B on `ClassBCommonStock`), so the
filing, not the member, ties a count to a symbol. This is the per-class count
consumers should use.

`sec_cover_class_shares_at(cik, class_key, D)` returns the class's own cover
count: the latest stated date (`stated_on <= D`, filing public by D), then the
latest filing (acceptance time, then accession); distinct values within that one
filing are `ambiguous`; a date older than 400 days is `stale`. `class_key = ''`
is the issuer total.

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
| Co-registrant facts (`coreg`) | Attributed to the CIK the legal-entity member names (SDG&E's preferred is SDG&E's line, not Sempra's); a member used by a single registrant as a class name (Renalytix's ADS) is that registrant's class; anything else is rejected |
| Parent symbol on a subsidiary's cover | Wholly owned subsidiaries filing their own covers (NSP and PSCo with XEL, American Airlines Inc with AAL) tag the parent's symbol with their own `dei:EntityCentralIndexKey`; the data attributes it to the subsidiary, so the parent and subsidiary overlap and the symbol is `ambiguous` at those dates (about 0.5% of tickers). Follow-up: resolve the pair through the parent's Exhibit 21 subsidiary list |
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

Each package and index is one transaction that replaces what the package
previously contributed: rows of its accessions converge to what it now contains,
and an accession (or index event) the previous version contained and this one
does not is removed with its rows, unless another loaded package still contains
it. A CIK corrected in a rebuilt index is a removal plus an insertion. The loader
refuses to run before the governed schema exists, and holds advisory lock 900_368
so it never interleaves with the recurring worker.

Full local run (2026-10-08, postgres:16 on the same workstation): 79 DERA packages
(2009q1 to 2026_09, 25 GB of zips) and 72 EDGAR quarterly indexes in **11.4 min**
(after the review fixes: 11.4 min, parsing 610 s, database writes 61 s): 885,052
submissions, 866,258 `TradingSymbol` facts, **860,525** observations (504
attributed to a co-registrant's own CIK, 220 legal-entity facts rejected),
**485,238** cover share counts, **48,450** registration events (938 of them `/A`
amendments).

## Recurring worker

`src/workers/sec_ticker_cik_history.py` (`WORKER=sec_ticker_cik_history`), Railway
config `railway.sec-ticker-cik-history.toml`, cron **`0 10 * * 1`** (Mondays
10:00 UTC; DERA publishes one package a month). Each run takes lock 900_368,
loads every listed package not yet in `sec_ticker_cik_packages` (oldest first),
reloads the newest one when its size changed (downloading it again even over
a cached copy), and refreshes the EDGAR form indexes of the current and previous
quarter. Packages are fetched, loaded and deleted one at a time (about 0.6 GB of
disk at most) unless `SEC_TICKER_CACHE_DIR` points at a volume; a cached copy
whose size differs from the remote one is replaced before it is loaded. `WORKER_LIMIT` caps packages per run;
the backlog resumes on the next run. The worker never applies DDL and refuses to
run without the schema. Exit is non-zero on an error or `lock_busy`. Env:
`DATABASE_URL` (`worker_writer`); no API key.

## Production steps (owner)

1. Apply `schemas/sec_ticker_cik_history_v1.sql` as `postgres` (or `worker_writer`)
   with `psql -v ON_ERROR_STOP=1 -f schemas/sec_ticker_cik_history_v1.sql`. It
   creates the five tables, the view and seven functions, sets the owner to
   `worker_writer`, revokes PUBLIC and grants SELECT/EXECUTE to `app_runtime`,
   `app_analytics_ro` and `mcp_ro`.
2. Run the initial load as `worker_writer`, from the workstation that holds
   `E:/Edgard/fsn` and `E:/Edgard/edgar-index` (add `--download` to fetch what is
   missing): `python -m scripts.load_sec_ticker_cik_history --dsn <worker_writer DSN>`.
   Expect about 861k observations, 485k share counts and 48.5k events, and
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

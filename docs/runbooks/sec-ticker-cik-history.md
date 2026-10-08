# SEC ticker -> issuer (CIK, class) history

Point-in-time answer to "which issuer, and which of its share classes, traded as
ticker T on date D", from public SEC data only. Light's walk-forward market prior
uses it to size equities at each fold (`sec_ticker_issuer_at`,
`sec_issuer_line_at`, `sec_cover_class_shares_at`).

| Object | What it holds |
|---|---|
| `sec_ticker_cik_observations` | One row per filing x class context x ticker: the registrant's cover-page `dei:TradingSymbol`, with `dei:Security12bTitle`, `dei:SecurityExchangeName`, the class segments, a `security_kind`, and the filing's equity-class count and whether it reports a share count |
| `sec_cover_share_counts` | Cover-page `dei:EntityCommonStockSharesOutstanding`, per class or in total, with the date the cover states it as of (`stated_on`) and DERA's rounded month end (`ddate_rounded`) |
| `sec_registration_events` | From the EDGAR form indexes: ends (15-12B, 15-12G, 15-15D, 25, 25-NSE), starts (8-A12B, 8-A12G, 10-12B, 10-12G) and their `/A`; for the ends of CIKs with cover data, what the filing states (class, rule provision, exchange, amendment effect) and the parser version |
| `sec_ticker_cik_packages` | One row per loaded DERA package or EDGAR index: digest, size and the loader's counts |
| `sec_ticker_cik_package_members` / `_package_facts` | The (accession, CIK) pairs and the fact versions each package or index carries, for reconciliation |
| `sec_ticker_intervals` (view) | Every class's symbol runs as known today, for diagnostics; decisions use the functions |
| `sec_ticker_price_span(ticker, cik[, class_key])` | Price lineage: the runs in which the issuer held the ticker, bounded by other CIKs' runs; uses everything known today |

Sources: the DERA [Financial Statement and Notes data sets](https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets)
(`sub.tsv`, `txt.tsv`, `num.tsv`, `dim.tsv`, streamed from each zip), the
[EDGAR full-index](https://www.sec.gov/Archives/edgar/full-index/) `form.gz` files,
and the Form 15/25 filings themselves (`Archives/edgar/data/<cik>/<adsh>.txt`).
All are fetched with the SEC User-Agent, at most 10 requests per second, with
back-off on 429/5xx; every fetched filing is cached on disk.

## Semantics

**Bitemporal storage.** No fact is deleted or overwritten. A package or index is
reconciled in one transaction against the fact versions it carried before: a
version it no longer carries, and no other loaded package carries, gets
`retired_on` = the reconciliation date; a new version is inserted. A version
first loaded with its accession is knowable from the filing's public date; a
version added to an accession already loaded (a correction, a CIK fixed in a
rebuilt index, a re-derived event class) is knowable from the later of that date
and the reconciliation date. Point-in-time functions see a row at D iff
`available_on <= D` and (`retired_on` is NULL or after D), so an answer at D never
changes once D has passed. History starts at the first load: corrections DERA or
EDGAR folded in before then are invisible.

**Cover evidence.** Only periodic and current reports state the filer's own
listed securities: 10-K, 10-Q, 8-K (and 8-K12B/8-K12G3/8-K15D5), 20-F, 40-F, 6-K,
10-KT, 10-QT and their amendments. Registration statements (S-1, S-3, S-4, S-8,
F-1, F-4, POS AM...) state securities of other or future entities (Medtronic plc's
S-4 named MDT months before it existed; a Park-Ohio S-4 named PKOH under another
CIK) and are skipped.

**Symbols.** Brackets and quotes are unwrapped (`(SIRI)`, `[USG]`, `"WM"`,
`(NYSE:FBC)`), exchange prefixes and OTC suffixes dropped (`NYSE: KO`, `NYSE/TRN`,
`WELPP.OB`), and a field lists several symbols when they are separated by `,`
`;` `&` `AND`, or by a space or slash before a full symbol (`JWA/JWB`,
`CRDA CRDB`), not before a class, series, warrant, unit or note-year suffix
(`BRK B`, `USB PrA`, `ACHR WS`, `USB/28`). Prose and placeholders (`None`, `N/A`,
`true`, `OTCBB`, `XXXXXXXXXX`, a CIK typed as the symbol) are rejected and counted.

**Class.** A class is `(cik, class_key)`: the cover context's dimension segments
without the listing-exchange axis, and without the legal-entity axis when it
names a registrant (`ClassOfStock=CommonClassA;`); `''` without dimensions. Each
filing also records how many equity classes it shows in total
(`filing_equity_classes`: its equity symbols' classes plus the dimensioned
classes of its share counts and of titled equity classes without a symbol, an
undimensioned symbol beside a counted class no symbol names being one of them)
and whether it reports a share count (`filing_complete`). Both come from that
filing alone, so they are point-in-time.

**Whose fact.** A fact without a co-registrant (`coreg`) belongs to `sub.cik`. A
legal-entity context whose member carries its own `dei:EntityCentralIndexKey`
belongs to that CIK (SDG&E in Sempra's combined 10-K). A member without one is
the registrant's own class only in a single-registrant filing in which no member
names a registrant and the member's context titles a security (Renalytix `RNLX`
and Nano Dimension `NNDM` name their ADS that way). Anything else is rejected.

**Knowledge date.** The EDGAR acceptance date when DERA carries it, else the
filing date + 1 (index rows: filing date + 1). The fact's `ddate` never dates a
symbol statement. A share count is dated by the day the cover states
(`stated_on = ddate - datp`, checked against the exact XBRL contexts for 352 of
353 sampled counts). Within a day, filings are ordered by acceptance time, then
accession, everywhere a latest statement is chosen.

**Holds.** For each CIK that showed T by D, its hold is followed through the
filings that tag a class that showed T; when its latest complete filing showing
T listed one equity class, through every complete filing (a single-class filer
renames its member or adds a class); and through every complete filing that
shows one equity class in total (classes merged). The hold's *statement* is the
latest such filing. The hold has **ended** when the statement shows another
symbol (`other_symbol`) or an applying end filing is newer than the statement; it
is **stale** when the statement is older than 400 days; else **active**. Equity
and depositary rows decide whenever any showed T (filers also tag their common
symbol on notes lines).

**End filings.** For CIKs with cover data the loader reads each Form 15/25 for
the class it concerns. An end applies unless it concerns another class (notes,
preferred, warrants, units, rights plans, employee-plan interests). A 25, 25-NSE
or 15-12B also does not apply when the issuer showed several symbols and the
filing names fewer classes than the issuer has, when the exchange is a secondary
one (Chicago, Boston, Philadelphia, National, NYSE Arca/Pacific: IDEX and
Weyerhaeuser dropping a Chicago listing), or when a registration (8-A12B, 8-A12G,
10-12B, 10-12G) filed from 30 days before to 10 days after it makes it a transfer
(PepsiCo's 2017 NYSE to Nasdaq move), unless the 25-NSE says the class was
extinguished. A filing that states no class (`class_kind = 'unknown'`, counted as
`class_unknown`) or was not read keeps the structural gate (15-12G/15-15D always;
the 12(b) forms after a single-symbol filing); a parse failure is never read as
another class. An applying end is **definitive** when it names an equity class
and at least as many classes as the issuer had, and either the 25-NSE cites Rule
12d2-2(a) (the class was redeemed, retired, substituted in a merger or its rights
extinguished) or a delisting (25/25-NSE) and a termination (15-12B, 15-12G,
15-15D) of the equity are both on file within 120 days. After a definitive end a
later statement reopens the hold only if its row for T carries a 12(b) title, a
registration filed after the end was public by then, or T first appeared after
the end (Swift's SWFT ended in the merger and the same CIK traded as KNX).
American Greetings tagged AM on 10-Qs for three years after its 2013 merger
delisting and Form 15; those no longer hold AM, and Antero Midstream resolves from
November 2014. Other applying ends close the hold until a later statement shows T
again (a delisting to OTC, a stale 12(g) registration). An amendment applies to
the latest original of its form filed on or before it, from its own knowledge
date: one that says the removal will not happen or is withdrawn cancels it
(Minim's 25-NSE/A of 2025-04-09); one that restates it replaces its class; one
not read leaves the original in force.

`sec_ticker_issuer_at(T, D)` returns `resolved` (exactly one CIK holds T
actively), `ambiguous` (two or more; `active_ciks` lists them), `stale`, `ended`
or `missing`. An active hold whose claims lie strictly inside another active
holder's (that holder showed T before the first and after the last of them) does
not count against it: a 10-Q misfiled under a shell CIK (ANDE under ANDES 7 INC
in 2016), or another issuer typing the symbol (EMR, NI). Until the true holder
files again after the stray, nothing known at D tells them apart and the answer
is `ambiguous` (known residual).

`sec_issuer_line_at(cik, class_key, D)` returns what a class traded as at D,
following it through the filings that tag it and, when its latest complete filing
listed one equity class (or the class is not known yet at D), through later
complete one-class filings (`ambiguous_class` when the class has no statement by
D and the issuer then listed several). `equity_lines` is the equity-class count of
the issuer's latest complete filing by D.

`sec_ticker_price_span(T, cik, class_key)` answers which stored vendor price rows
of T belong to the issuer. It is lineage, so it uses today's truth: current rows
at their filing's public date, every current amendment, and transfers or paired
Form 15s filed later. One row per run of the CIK's hold of T (`line_key` = the
run's latest class), with `prior_holder_end` (NULL when no other CIK's run
started earlier; this run's `valid_from` when such a run has no end evidence,
because a last confirmation is not an end; else the latest `valid_to` of those
runs) and `next_holder_start` (the earliest start of another CIK's run on or after
this one). Each date is admitted for at most one CIK. A run whose claims lie
strictly inside a run of a different CIK bounds nothing.

`sec_cover_ticker_shares_at(T, cik, D)` returns the cover count of the class that
trades as T, joined inside each filing: a count whose class is tagged with T in
the same filing, or the filing's total when the filing shows exactly one equity
class in total and that class is an equity (never a depositary) line showing T.
Member names change between filings (Berkshire's 10-Q counts `CommonClassB` while
its 8-Ks tag BRK.B on `ClassBCommonStock`), so the filing, not the member, ties a
count to a symbol.

`sec_cover_class_shares_at(cik, class_key, D)` returns the class's own count: the
latest stated date, then the latest filing (knowledge date, acceptance time,
accession); distinct values within that filing are `ambiguous`; older than 400
days is `stale`. `class_key = ''` is the issuer total.

## Edge cases

| Case | Behaviour |
|---|---|
| Ticker reassignment (AT&T Corp, CIK 5907; AT&T Inc, CIK 732717) | The old hold ends at its end filing or its own move to another symbol; the new hold starts at its first statement. Without an end the two overlap and D is `ambiguous` until the old hold is stale; in lineage the new holder admits nothing before its start and the old one nothing from it |
| Renamed ticker (SQ -> XYZ, same CIK) | The rename filing ends `SQ` and starts `XYZ`; `sec_issuer_line_at(1512673, '', D)` shows `SQ` before it |
| Merger delisting with stale covers afterwards (American Greetings, AM) | Definitive end (25-NSE under 12d2-2(a) plus the Form 15 for both classes); later untitled covers do not reopen |
| Exchange transfer (PepsiCo 2017); secondary listing removed (IDEX, Weyerhaeuser on Chicago) | No end |
| Delisting or deregistration of notes, preferred, a rights plan, warrants or plan interests | No end |
| Delisting to OTC (25-NSE under 12d2-2(b)) | Ends the hold; a later cover showing the symbol reopens it |
| Multi-class issuer (BRK-A/BRK-B, BF-A/BF-B, GOOGL/GOOG) | Each class has its own symbol and, when the cover reports it, its own count. An issuer total is never spread over several classes, or given to a depositary line |
| Class shares written without a separator (`BFB`) | Resolution matches on the separator-free key (`BF-B` = `BF.B` = `BFB`) |
| ADRs (20-F/40-F filers) | `security_kind = 'depositary'`; the cover counts ordinary shares, so no total is returned for the ADS |
| Co-registrant facts (`coreg`) | Attributed to the CIK the legal-entity member names; a single registrant's class-naming member (Renalytix's ADS) is its own class; anything else is rejected |
| Parent symbol on a subsidiary's cover | Subsidiaries filing their own covers (NSP and PSCo with XEL) tag the parent's symbol under their own CIK; the symbol is `ambiguous` at those dates (about 0.5% of tickers). Follow-up: the parent's Exhibit 21 |
| Before mandatory cover tagging | Mandatory for periods ending on or after 2019-06-15 (large accelerated), 2020-06-15 (accelerated), 2021-06-15 (others). Earlier, 1,500-3,000 issuers per quarter tagged `TradingSymbol` voluntarily and no cover carried a 12(b) title |

## Load

```powershell
# Fetch any missing DERA packages and EDGAR form indexes, then load everything.
$env:PYTHONPATH = "."
python -m scripts.load_sec_ticker_cik_history --download `
  --packages-dir E:/Edgard/fsn --index-dir E:/Edgard/edgar-index --dsn "$env:DATABASE_URL"
# Parse only, no database:
python -m scripts.load_sec_ticker_cik_history --dry-run --packages-dir E:/Edgard/fsn
```

Each package and index is one bitemporal reconciliation in its own transaction
(see Semantics): nothing is deleted, a dropped fact version is retired unless
another loaded package still carries it, and a corrected one is knowable from the
reconciliation date. `--reconciled-on YYYY-MM-DD` dates them (default: today).
The end filings of CIKs with cover data are read from `--event-docs-dir`
(default `E:/Edgard/edgar-event-docs`), fetched there when missing (`--no-fetch`
reads only the cache). A run ends by re-deriving the class of current end events
read by another parser version (`EVENT_PARSER_VERSION`) or not read yet, as
corrections. The loader refuses to run before the governed schema exists, and
holds advisory lock 900_368 so it never interleaves with the recurring worker.

FULL_LOAD_PLACEHOLDER

## Recurring worker

`src/workers/sec_ticker_cik_history.py` (`WORKER=sec_ticker_cik_history`), Railway
config `railway.sec-ticker-cik-history.toml`, cron **`0 10 * * 1`** (Mondays
10:00 UTC; DERA publishes one package a month). Each run takes lock 900_368,
reconciles every listed package not yet in `sec_ticker_cik_packages` (oldest
first), reconciles the newest one again when its size changed (downloading it
again even over a cached copy), refreshes the EDGAR form indexes of the current
and previous quarter, reads the end filings of CIKs with cover data (kept under
`<cache>/event-docs`), and re-derives end events read by another parser version.
Packages are fetched, loaded and deleted one at a time (about 0.6 GB of disk at
most) unless `SEC_TICKER_CACHE_DIR` points at a volume; a cached copy whose size
differs from the remote one is replaced before it is loaded. `WORKER_LIMIT` caps
packages per run; the backlog resumes on the next run. `state` is `ok` when any
observation, share count or event version was inserted or retired, else `noop`.
The worker never applies DDL and refuses to run without the schema. Exit is
non-zero on an error or `lock_busy`. Env: `DATABASE_URL` (`worker_writer`); no
API key.

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

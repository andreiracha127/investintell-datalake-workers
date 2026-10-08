# SEC ticker -> issuer (CIK, class) history

Point-in-time answer to "which issuer, and which of its share classes, traded as
ticker T on date D", from public SEC data only. Light's walk-forward market prior
uses it to size equities at each fold (`sec_ticker_issuer_at`,
`sec_issuer_line_at`, `sec_cover_ticker_shares_at`).

| Object | What it holds |
|---|---|
| `sec_ticker_cik_observations` | One row per filing x class context x ticker: the registrant's cover-page `dei:TradingSymbol`, with `dei:Security12bTitle`, `dei:SecurityExchangeName`, the class segments, a `security_kind` (`unknown`: a line on a foreign private issuer's form that no title, segment or symbol suffix identifies), and the filing's equity-class count and whether it reports a share count |
| `sec_cover_share_counts` | Cover-page `dei:EntityCommonStockSharesOutstanding`, per class or in total, with the date the cover states it as of (`stated_on`) and DERA's rounded month end (`ddate_rounded`) |
| `sec_registration_events` | From the EDGAR form indexes: ends (15-12B, 15-12G, 15-15D, 15F-12B, 15F-12G, 15F-15D, 25, 25-NSE), starts (8-A12B, 8-A12G, 10-12B, 10-12G) and their `/A`; for the ends and the Forms 8-A of CIKs with cover data, what the filing states (class, rule provision, exchange, amendment effect) and the parser version |
| `sec_ticker_cik_packages` | One row per loaded DERA package or EDGAR index: digest, size and the loader's counts |
| `sec_ticker_cik_package_members` / `_package_facts` | The (accession, CIK) pairs and the fact versions each package or index carries, for reconciliation |
| `sec_ticker_intervals` (view) | Every class's symbol runs as known today, for diagnostics; decisions use the functions |
| `sec_ticker_price_span(ticker, cik[, class_key])` | Price lineage: the runs in which the issuer held the ticker, bounded by other CIKs' runs; uses everything known today |

Sources: the DERA [Financial Statement and Notes data sets](https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets)
(`sub.tsv`, `txt.tsv`, `num.tsv`, `dim.tsv`, streamed from each zip), the
[EDGAR full-index](https://www.sec.gov/Archives/edgar/full-index/) `form.gz` files,
and the Form 15/15F/25 and 8-A filings themselves
(`Archives/edgar/data/<cik>/<adsh>.txt`). All are fetched with the SEC User-Agent,
at most 10 requests per second, with back-off on 429/5xx; every fetched filing is
cached on disk once its SEC header names the requested accession (a 200
maintenance page is retried, never cached, and counted as `filings_rejected`).

The schema is two governed files applied in order:
`schemas/sec_ticker_cik_history_v1.sql` (tables, view, functions; production
since 2026-10-08) and `schemas/sec_ticker_cik_history_v2.sql` (functions only:
end effective dates, class-scoped ends, the rules below marked v2).

## Semantics

**Bitemporal storage.** Point-in-time means *public at D*. No fact is deleted or
overwritten. A package or index is reconciled in one transaction against the fact
versions it carried before: a version it no longer carries, and no other loaded
package carries, gets `retired_on` = the reconciliation date and a
`retired_reason`; a new version is inserted. The reason decides what the change
means:

- `source` (also NULL on rows retired before v2): **the public record changed**.
  DERA republished a package with other content, an index dropped or reassigned
  a row (a CIK fixed in a rebuilt index), or a quarterly package superseded its
  months. The old version stays visible before its retirement; a new version of
  an accession already loaded is knowable from the later of its filing's public
  date and the reconciliation date. History starts at the first load:
  corrections DERA or EDGAR folded in before then are invisible.
- `parser_correction`: **our reading changed, not the public record**. The same
  package bytes (the same SHA-256 as the version loaded) read by a fixed parser,
  an index row (same accession, CIK, form and filing date) read differently, or
  an event re-derived by a new parser version. The old reading was never true:
  it is visible at no date. The new reading is knowable from the filing's own
  public date, so answers at past dates take the corrected reading.
  `parser_version` records the parser that read each fact version and each
  package version (NULL: read before v2); `loaded_on` and the old row's
  `retired_on` date the re-derivation.

A version first loaded with its accession is knowable from the filing's public
date. Point-in-time functions see a row at D iff `available_on <= D` and
(`retired_on` is NULL, or after D with a reason other than
`parser_correction`). An answer at D never changes because of later public
information; it changes when a parser correction restates the reading, which
consumers version (Light binds the W1 schema and parser versions into its
results' fingerprint).

**Cover evidence.** Only periodic and current reports state the filer's own
listed securities: 10-K, 10-Q, 8-K (and 8-K12B/8-K12G3/8-K15D5), 20-F, 40-F, 6-K,
10-KT, 10-QT and their amendments. Registration statements (S-1, S-3, S-4, S-8,
F-1, F-4, POS AM...) state securities of other or future entities (Medtronic plc's
S-4 named MDT months before it existed; a Park-Ohio S-4 named PKOH under another
CIK) and are skipped.

**Symbols.** Brackets and quotes are unwrapped (`(SIRI)`, `[USG]`, `"WM"`,
`(NYSE:FBC)`), exchange prefixes and OTC suffixes dropped (`NYSE: KO`, `NYSE/TRN`,
`OTC Pink: IRRX`, `WELPP.OB`), and a field lists several symbols when they are
separated by `,` `;` `&` `AND`, or by a space or slash before a full symbol
(`JWA/JWB`, `jwa/jwb`, `CRDA CRDB`), not before a class, series, warrant, unit or
note-year suffix (`BRK B`, `USB PrA`, `ACHR WS`, `USB/28`). Prose and placeholders
(`None`, `N/A`, `NA`, `XXXXXXXXXX`, a CIK typed as the symbol) are rejected and
counted. Placeholders are read in the form the filer wrote them: the XBRL booleans
`true`/`True`/`False` are placeholders, TrueCar's `TRUE` is a symbol (all 94 FSN
facts written that way are TrueCar's). An exchange or market name is dropped only
when it qualifies another symbol (`BAX NYSE`, `EDLG, OB`); alone it is a
placeholder (`OTCQB`, `NYSE`), except a venue whose operator lists under its name
(`CBOE`, Cboe Global Markets' 113 facts), and `OB` alone is Outbrain's symbol.

**Class.** A class is `(cik, class_key)`: the cover context's dimension segments
without the listing-exchange axis, and without the legal-entity axis when it
names a registrant (`ClassOfStock=CommonClassA;`); `''` without dimensions. Each
filing also records how many equity classes it shows in total
(`filing_equity_classes`: its equity symbols' classes plus the dimensioned
classes of its share counts and of titled equity classes without a symbol;
distinct symbols on one context being distinct classes, as Google Inc's
undimensioned `GOOG, GOOGL` or a `JWA/JWB` fact; undimensioned symbols beside
counted classes no symbol names being those classes)
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
is **stale** when the statement is older than 400 days; else **active**. Listed
rows (equity, depositary or unknown) decide: a non-listed row showing T (filers
also tag their common symbol on notes lines) counts only while no listed row
showing T was known at or before it (v2). A ticker only ever shown on preferred
lines resolves through them, and an earlier holder that showed T only on rows read
as debt keeps its run when another issuer lists T years later.

**End filings.** For CIKs with cover data the loader reads each Form 15, 15F or
25 for the class it concerns, and each Form 8-A12B or 8-A12G for the class it
registers (v2; a Form 10 is not read). A Form 15F (15F-12B, 15F-12G, 15F-15D: a
foreign private issuer's termination under Rule 12h-6, such as PetroChina's
15F-12B of 2024-02-05) counts as the Form 15 it stands for. A class count reads
Class/Series enumerations and, since parser v4, classes named without a label
(`Common Stock; Non-Voting Common Stock` is two; an ADS and the shares it
represents, or a parenthesized alias, are one).

An end that names its classes by letter (`Class B common stock`, `Class A and
B`), when every listed class of the issuer's prior cover has a letter (its 12(b)
title, else its member: `CommonClassB`, `ClassBCommonStock`, `CapitalClassC`),
applies to the listed classes it names only: `sec_issuer_end_events.class_keys`,
and holds, lines and lineage runs of the other classes continue (v2). Naming only
classes the issuer does not list, it ends nothing. Such an end of some classes is
definitive only when its 25-NSE says they were extinguished.

An end of an equity class ends the listed lines (equity, depositary, unknown),
and a preferred, warrant, unit, right or notes line of the same CIK only when its
description names that instrument (`named_kinds`): Triton's 25-NSE of its common
shares (2023) does not end its preferred shares, which stayed listed, while a
SPAC's "Units; Class A common stock; Warrants" ends all three (v2).

Any other end applies to every line of the CIK unless it concerns another class
(notes, preferred, warrants, units, rights plans, employee-plan interests), or the
issuer showed several symbols and the filing names fewer classes than the issuer
has (a 15-12G or 15-15D for one class of a multi-class issuer too), or it is a
15-12G or 15-15D (or 15F) of an equity class naming fewer equity classes than the
issuer's latest complete cover showed, listed or not: a termination may concern
an unlisted class (listed class A, counted unlisted class B) (v2). The symbols and
classes the issuer had are those of its covers filed before the end, among those
known at D; a version of the end re-derived years later is judged the same way. A
25, 25-NSE or 15-12B also does not apply when the exchange is a secondary one
(Chicago, Boston, Philadelphia, National, NYSE Arca/Pacific: IDEX and Weyerhaeuser
dropping a Chicago listing), or when a registration of an equity class, or of a
class not read (8-A12B, 8-A12G, 10-12B, 10-12G, 8-K12B, 8-K12G3), filed from 30
days before to 10 days after it makes it a transfer (PepsiCo's 2017 NYSE to
Nasdaq move), unless the 25-NSE says the class was extinguished. No end applies
when a successor registered the CIK's class under the same CIK (8-K12B or
8-K12G3, Rules 12g-3 and 12b) in that window, extinguished or not: a
holding-company reorganization that keeps the CIK continues its line (KKR's
8-K12B of 2022-05-31, the day before NYSE's 25-NSE of the old common stock; its
count rose from 593 to 860 million shares, so the base check alone would read a
definitive end; ODP 2020 and ADTRAN 2022 did the same) (v2). A Form 8-A of notes, preferred or
warrants is no transfer (v2: Statera's 8-A12G of its Series B Preferred Stock,
filed the day Nasdaq delisted its common stock). A filing that states no class
(`class_kind = 'unknown'`, counted as `class_unknown`) or was not read applies only
after a single-symbol filing; a parse failure is never read as another class.

An applying end is **definitive** when it names an equity class and at least as
many classes as the issuer had, and either the 25-NSE cites Rule 12d2-2(a) (the
class was redeemed, retired, substituted in a merger or its rights extinguished)
or a delisting (25/25-NSE) and a termination (15-12B, 15-12G, 15-15D or their 15F)
of the equity are both on file within 120 days, unless the shareholder base
continued: the first cover count filed after the end and stated on or after it
(v2; a 10-Q filed after a merger that states the pre-merger count proves
nothing) is within 0.8-1.25 times the last one before it. After a definitive end
a later statement does not reopen the hold, even with a 12(b) title (v2); only a
registration filed after the end (an 8-A, a Form 10 or a successor's 8-K12B),
public by then, does, or T first appearing after the end (Swift's SWFT ended in
the merger and the same CIK traded as KNX).

Every end has an **effective date** (`effective_on`: its filing date + 1, or,
point-in-time, the restating amendment's when it applies only as restated) apart
from its knowledge date (`available_on`, the visibility gate). Holds, lines and
runs order ends against statements by the effective date (v2): an end that the
public record adds later (an index rebuilt years after, moving the end to this
CIK) is visible from that correction and takes effect at its filing, so a cover
filed between the two still counts after it.
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

**Foreign private issuers.** A 20-F, 40-F, 6-K or 20-FR (or its `/A`) counts the
underlying shares, and its cover may tag the ADS symbol on that class:

- TSM's 20-Fs tag `TSM` untitled (2018) or titled "Common Shares" (2020-2026),
  beside 25.9 billion common shares. Each ADS is five common shares.
- America Movil's 2021 20-F tags `AMOV` and `AMX` on its A and L shares. Each ADS
  is twenty of them.
- FUTU's 2022 20-F tags `FUTU` on its Class A ordinary shares as well as on its
  ADR class. Each ADS is eight Class A shares.
- America Movil's 2023 20-F titles `AMX` "American Depositary Shares, each
  representing 20 B Shares" on its B shares' member, which carries the B share
  count.

So, from such a filing, two kinds of count are refused:

- the filing's total;
- a class count whose member is not an explicit depositary member (`Adr`,
  `AmericanDepositaryShares`, `DepositoryShares` and the like). A depositary
  title on the underlying class's member is not enough.

A refused count returns status `refused`, `shares` NULL and `refusal =
'foreign_issuer_listing_unverified'`, with `shares_as_of`, `adsh` and `basis`
kept for audit. Statuses are `resolved | stale | ambiguous | missing | refused`.
Within one filing, an admissible count wins over a refused one. A depositary
line is still sized only with a ratio (Light: `depositary_ratio_unsourced`).
W1c lifts the guard per line once the cover page evidences what is listed.

`sec_cover_class_shares_at(cik, class_key, D)` returns the class's own count: the
latest stated date, then the latest filing (knowledge date, acceptance time,
accession); distinct values within that filing are `ambiguous`; older than 400
days is `stale`. `class_key = ''` is the issuer total; an issuer total from a
foreign private issuer's filing is `refused` (`foreign_issuer_listing_unverified`).

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
| ADRs and other foreign listings (20-F, 40-F, 6-K) | A depositary title or member gives `security_kind = 'depositary'`. That includes titles written without spaces (VALE's `AmericanDepositaryShares(...)`), titles spelled "Depository" (14 issuers' 8-K/10-Q/10-K covers), and `Adr` members. An untitled line with no telling segment or suffix is `unknown`. No total from these forms sizes a line, and no class count does unless it is on an explicit depositary member: such counts are `refused` (`foreign_issuer_listing_unverified`) |
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
The end filings and Forms 8-A of CIKs with cover data are read from
`--event-docs-dir` (default `E:/Edgard/edgar-event-docs`), fetched there when
missing (`--no-fetch` reads only the cache); an event already read by the current
parser version is carried without a read. A run ends by re-deriving the class of
current end and 8-A events read by another parser version (`EVENT_PARSER_VERSION`,
now `sec_event_class_v4`) or not read yet, as corrections. With `--verify-cache`
the file verified is the one loaded, by path (a package named on the command line
included), and its validators are recorded in the package's own transaction; a
package loaded without them is recorded without validators, and the worker
checks it once by digest. The loader refuses to run before both governed schema
files are applied, and holds advisory lock 900_368 so it never interleaves with
the recurring worker.

Full local run (2026-10-08, postgres:16 on the same workstation, filings
cached): 79 DERA packages (2009q1 to 2026_09, 25 GB of zips) and 72 EDGAR
quarterly indexes in **about 11 min** (670 s): 885,052 submissions, 866,258
`TradingSymbol` facts, **858,894** observations, **483,745** cover share counts,
**80,225** registration events (ends, starts and amendments; 532 of them Forms
15F). 15,880 end filings of CIKs with cover data were read: 9,832 name an
equity class, 5,956 another class, 92 state none (`class_unknown`); 0 cache
misses, 0 fetch failures. No fact is dated after its filing's public date on a
first load. Filling an empty filing cache takes about 15,900 requests (about
70 min at the observed sequential rate, never above 10 per second).

## Lineage for price rows

`sec_line_price_evidence(T, cik, class_key)` is the decision-grade lineage
contract: a stored price row of T at date t belongs to issuer `cik`, line L (the
line of `class_key`; pass the class of today's latest equity row showing T) only
if an `alive` interval of L contains t and no `other_holder` interval does.
Intervals are `[valid_from, valid_to)`; an open run ends at its stale cutoff
(last statement + 401 days). `source` is `sec_cover` for cover-page statements
and end filings; insider filings (W1b) are the planned second source and will
add rows of their own. Lines (`sec_issuer_lines`) link the classes of one issuer
that showed the same symbol and never appear side by side (Berkshire's
CommonClassB / ClassBCommonStock), and the one class of consecutive complete
filings; other lines of the same issuer under the same ticker are other holders
(GOOG moved from class A to class C in 2014). A shared symbol does not link an
undimensioned class shown only on one-class covers to a dimensioned class shown
only beside other classes (v2): the symbol moved in a recapitalization and does
not say which new class continues the old one (a class C line is not alive under
GOOG before class C existed). An end naming some of the issuer's classes ends only
their lines. `sec_ticker_price_span` remains the
run-and-neighbour view of the same engine (`sec_ticker_line_runs`). Lineage
functions run in 15-65 ms per ticker on the full load (JIT is off for them; it
cost 1.5 s per call).

## Known residuals

| Case | Behaviour | Source that closes it |
|---|---|---|
| A true holder with no cover tag before 2019 and a stray claim (ANDE under ANDES 7 INC in 2016, EMR Technology Solutions 2017, CIK 1703975 tagging NI 2017-18) | The stray is the only claimant and resolves for up to 400 days | W1b insider filings of the true holder |
| A stray claim between an end and the old holder's next statement | The successor rule treats it as the new holder | W1b |
| Alphabet's GOOG between Google Inc's end (2015-10-03) and Alphabet's first tagged cover (2015-10-29) | In no evidence interval: price rows refused | W1b (Alphabet insiders filed with GOOG from October 2015) |
| A symbol moving between classes of one issuer before 2019, when covers tagged every symbol undimensioned (Google 2014) | One line | Dimensioned covers from 2019 |
| A tracking or secondary class delisted while the main symbol continues, pre-2019 (FNF / FNFV 2017) | The main symbol's hold ends until its next statement | Covers listing both symbols (2019+) |
| Antero Midstream Partners (AM, 2014-2019) and other holders that never tagged a symbol | `ended` or `missing` | W1b |
| Foreign private issuers' lines: ADSs tagged as the underlying class (TSM, AMOV, AMX, FUTU) and true direct listings (ZIM, QGEN, Canadian 40-F filers) alike | No total and no class count except on an explicit depositary member: `refused`, `foreign_issuer_listing_unverified` | W1c: the 20-F/40-F cover page (12(b) table and its footnotes) and F-6 ratios |

**W1c (follow-up).** Read the 20-F and 40-F cover page, which gives the listed
security type in its 12(b) table and footnotes. TSM's says "Not for trading, but
only in connection with the listing ... of American Depositary Shares". F-6
filings give the ADS ratio. Both are effective-dated:

1. Find the primary document through the filing's `-index.html` Type column,
   because `index.json` does not mark it.
2. Stream it and stop after the cover, because sec.gov ignores `Range` (a 20-F
   `.txt` runs to tens of MB).
3. Store what the cover states per filing.

With that evidence, the guard lifts per line, and depositary lines get their
ratios.

## Recurring worker

`src/workers/sec_ticker_cik_history.py` (`WORKER=sec_ticker_cik_history`), Railway
config `railway.sec-ticker-cik-history.toml`, cron **`0 10 * * 1`** (Mondays
10:00 UTC; DERA publishes one package a month). Each run takes lock 900_368,
first completes any monthly supersession a stopped run left, then reconciles
every listed package not yet in `sec_ticker_cik_packages` (oldest first; a
monthly package whose quarterly is loaded or listed is not queued), and reloads
every loaded, current and listed package the SEC republished (the SEC updated
2010q1-2013q4 in 2024): its size, else the SEC's ETag or Last-Modified compared
with the recorded ones, else the SHA-256 of a fresh download, differs. That check
costs one HEAD per listed package (about 80, spaced under 10 per second); a
download whose digest matches records its validators, so a package recorded
without them is downloaded once, not every week. Every package the worker loads
is downloaded by that run, never taken from a cache, and recorded with the
validators of that download in the load's own transaction (`republished` lists
the packages reloaded). It then supersedes the monthly packages of a loaded
quarterly, refreshes the EDGAR form indexes of the current and previous quarter,
reads the end filings and Forms 8-A of CIKs with cover data that the current
parser version has not read (kept under `<cache>/event-docs`; a filing that
cannot be fetched, or a body that is not the filing, is counted and never
replaces a class already read), and re-derives events read by another parser
version.
Packages are fetched, loaded and deleted one at a time (about 0.6 GB of disk at
most) unless `SEC_TICKER_CACHE_DIR` points at a volume, which keeps the zips and
the filing cache. `WORKER_LIMIT` caps packages per run; the backlog resumes on
the next run. `state` is `ok` when any observation, share count or event version
was inserted or retired (supersession included), else `noop`.
The worker never applies DDL and refuses to run without both schema files. Exit
is non-zero on an error or `lock_busy`. Env: `DATABASE_URL` (`worker_writer`); no
API key.

## Production steps (owner)

1. Apply `schemas/sec_ticker_cik_history_v1.sql` as `postgres` (or `worker_writer`)
   with `psql -v ON_ERROR_STOP=1 -f schemas/sec_ticker_cik_history_v1.sql`. It
   creates the six tables, the view and fifteen functions, sets the owner to
   `worker_writer`, revokes PUBLIC and grants SELECT/EXECUTE to `app_runtime`,
   `app_analytics_ro` and `mcp_ro`.
2. Run the initial load as `worker_writer`, from the workstation that holds
   `E:/Edgard/fsn`, `E:/Edgard/edgar-index` and the filing cache
   `E:/Edgard/edgar-event-docs` (add `--download` to fetch packages or indexes
   that are missing; filings missing from the cache are fetched at most 10 per
   second). Precondition: the package cache must match the SEC listing, so the
   run starts with `--verify-cache` (HEAD every listed package; fetch again any
   cached zip that is missing, changed size or ETag, or is older than its
   Last-Modified; record only those fresh validators; the first log line reports
   `fetched_again`): `PYTHONPATH=. python -m scripts.load_sec_ticker_cik_history
   --verify-cache --download --dsn <worker_writer DSN>`. On 2026-10-08 the cache
   was current (79 listed, 0 fetched again). Expect 858,894 observations,
   483,745 share counts and 80,225 events, `class_unknown` 92, `filings_missing`
   and `filings_failed` 0; 11-13 minutes locally, about 20-30 minutes against the remote database (the
   writes are 1.4M rows of COPY).
3. Check: `SELECT count(*), max(available_on) FROM sec_ticker_cik_observations;`,
   `SELECT * FROM sec_ticker_issuer_at('BRK-B', current_date);` (resolved, CIK
   1067983, class B line), `SELECT * FROM sec_ticker_issuer_at('GOOG', '2015-11-15');`
   (resolved, Alphabet 1652044), `SELECT count(*) FROM sec_registration_events
   WHERE available_on > source_available_on;` (0 after a first load).
4. Only after the follow-up PR on republication checks of every loaded package
   (connector thread 4221867196), validator recovery after a failed load
   (4221867184), digest validation of a same-size cached copy (4222197135),
   path-level verification of a positional package (4222924629) and carrying
   events already parsed under the current parser version (4223252991)
   merges: create the Railway service `sec-ticker-cik-history` from
   this repository with `railway.sec-ticker-cik-history.toml`,
   `WORKER=sec_ticker_cik_history` and the `worker_writer` `DATABASE_URL`. The
   cron is in the config file.
5. Light's walk-forward equity sizing calls these functions: deploy the Light
   change only after steps 1-2 and after the follow-up PR (connector threads
   4222086431, 4222086445, 4222086476, 4222376247, 4222376271, 4222376284,
   4222924612, 4223111409, 4223111418, 4223252982) merges. Light must handle `security_kind = 'unknown'` and the share status
   `refused` (`foreign_issuer_listing_unverified`). W1c (above) is a separate
   follow-up that restores sizing for foreign issuers' lines.

Rollback: `schemas/sec_ticker_cik_history_v1.rollback.sql` (as the same role),
and remove the Railway service.

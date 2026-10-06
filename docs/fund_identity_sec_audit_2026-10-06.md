# Fund identity audit against SEC data (2026-10-06)

The portfolio builder admits a fund only when the NAV lifecycle evidence from
`scripts/generate_fund_nav_policy_v1.py` says `ACTIVE`. This audit looks for
every fund/ETF identity mismatch left after the catalog repair rules A and B
(`fix/fund-catalog-identity-repair`), judges each one against SEC data, and
ships `scripts/repair_fund_identity_sec_v1.py` to correct what SEC proves. The
cases SEC cannot settle go to the owner review lists at the end.

## Result

Classifier simulation on the live snapshot of 2026-10-06 05:55 UTC
(`funds_profile_mv`, 8,268 funds), with A and B applied in memory exactly as
`simulate.py` does:

| state | ACTIVE | ACTIVE AUM (USD bn, `attributes.aum_usd`) |
|---|---:|---:|
| production today | 2,896 | 10,386 |
| A + B | 7,348 | 33,275 |
| A + B + R1, R2, R4, R5, R6, R7 (default plan) | 7,484 | 35,385 |
| same, plus R3 (`--include-class-repoint`) | 7,504 | 35,474 |

No fund that is ACTIVE under A + B loses that status under either plan. R7
adds 17 funds to `funds_v` (their series pass the N-PORT eligibility
gate); they enter `funds_profile_mv` on its next refresh and are counted in
the rows above.

Rule B as simulated has a defect: it matches the IU ticker against every row
of `sec_company_tickers_mf`, including rows the SEC sync stopped refreshing
(the generator ignores rows older than 7 days). For 38 instruments B aligns
the registry to a share class that SEC no longer lists, which turns 21 funds
in `funds_profile_mv` from `ticker.mismatch` into `sec.stale`
(`sec.stale` goes 15 → 36). With B restricted to fresh rows those funds stay
correctable by R3. See [B with fresh rows only](#b-with-fresh-rows-only).

## Sources and pins

| source | role | pin |
|---|---|---|
| `public.sec_company_tickers_mf` | current SEC class/series/ticker (daily sync of `company_tickers_mf.json`), judged with the generator's 7-day freshness window in the plan's own snapshot | read in the same `REPEATABLE READ` snapshot as the catalog |
| SEC *Investment Company Series and Class Information* 2023–2026 | every series/class not yet reclassified inactive, with tickers; the 2026 file (SEC update 2026-06-01) is "current-year" | sha256 in `SERIES_CLASS_FILES` of the script |
| sec-api.io Query API (`485BPOS`, `497`, `497K`, `497J`, `N-CEN`, `N-CSR`, `NPORT-P`, `N-14`) | first/last filing showing a class under a ticker; last filing listing a terminated class or series | accession numbers in `contracts/fund-identity-sec/evidence_v1.json` |
| Tiingo daily meta (`/tiingo/daily/<ticker>`) | whether a ticker has a current price history (activation only) | `tiingo_meta` in the evidence bundle, with `observed_at` |
| `nav_ingestion_attempts` run `244e8eea-5b3c-4d7d-b74d-2a29fc0428f1`, `nav_timeseries` | production Tiingo outcome and newest NAV date (deactivation only) | same snapshot |
| `company_tickers_mf.json` (2026-10-06 download) | cross-check of the DB sync: 28,608 rows, 1 row differs from the fresh DB rows | sha256 in the evidence bundle |

sec-api bandwidth used for this audit: 4.9 MB (October 2026 account usage).

## Findings

Counts are instruments. "MV" is `funds_profile_mv`; "overall" is every
`instruments_universe` fund. Status is the first failure after A + B.

| # | finding | MV | overall | disposition |
|---|---|---:|---:|---|
| 1 | IU `isin` holds an EDGAR id that A does not cover (CIK, or a series id other than the registry series) | 22 | 849 | R1: set NULL |
| 2 | ticker renamed, same SEC class | 26 | 29 | R2: rename IU (and registry) ticker |
| 3 | IU ticker is a terminated share class; registry holds a live class of the same series | 20 | 22 | R3 (opt-in): repoint IU ticker, then NAV rebase |
| 4 | registry `conflict_state` on ticker/class that SEC now settles | 2 | 2 | R4: drop the settled keys |
| 5 | `is_active=false` but the fund is live (SEC current + Tiingo current) and its series has no active instrument | 74 | 74 | R5: activate |
| 6 | `is_active=true` but ticker and series are gone from SEC and NAV stopped > 90 days ago | 7 | 152 | R6: deactivate |
| 7 | registry row has ticker but no series/class (benchmark-proxy ETFs) | 0 | 20 | R7: fill series/class/CIK from SEC |
| 8 | registry aligned to a terminated class (B on stale rows, or historic) | 22 | 41 | review |
| 9 | series reorganized into a new trust/series | 6 | 6 | review |
| 10 | ticker in the SEC June dataset but missing from `company_tickers_mf` | 33 | 33 | review (SEC source gap) |
| 11 | phase-B historical share-class siblings, inactive by design | 512 | 512 | none |
| 12 | no ticker anywhere (variable-insurance portfolios) | 125 | 125 | review |
| 13 | inactive fund whose live ticker has no Tiingo price history | 60 | 60 | review |
| 14 | `cardinality.funds_v_missing` | — | 2,830 | classified below; R6/R7 touch 165 |
| 15 | registry SEC ids that do not exist / point elsewhere | 0 | 0 | none (checked) |

### 1. EDGAR identifiers stored as ISIN (R1)

A NULLs `instruments_universe.isin` only when it equals the registry series.
The residue is 849 rows (22 in MV, all `isin.unsupported_prefix`): 478 hold a
zero-padded CIK and 371 a series id that is not the registry series (the
fund's predecessor series after a reorganization, or a fund with no registry
series). No ISIN can match `^S\d{9}$`, `^C\d{9}$` or `^\d{1,10}$` (an ISIN
starts with a two-letter country code), so the value is provably not an ISIN.

| ticker | IU `isin` | what it is |
|---|---|---|
| DGEFX | `0001688680` | CIK of Brinker Capital Destinations Trust |
| MPGVX | `0001651872` | CIK of Gallery Trust |
| HBSGX | `S000004108` | old series; SEC lists HBSGX under S000084804 (registry series) |
| OIODX | `S000057227` | old series; registry series S000075333 |
| FICIX | `S000065928` | old series; registry series S000075628 |

Rule: `isin := NULL` when the trimmed value matches one of those patterns.
Rows: 849 `instruments_universe` rows. 20 MV funds become ACTIVE.

### 2. Ticker renamed, same class (R2)

The SEC class did not change; its ticker did. The IU ticker (which drives the
Tiingo fetch) still holds the old symbol, so NAV ingestion gets `empty` or
`not_found`, and the registry is either already on the new ticker
(`ticker.mismatch`) or still on the old one with no class
(`sec.missing`/`sec.contradiction`).

| class | old → new | evidence (last filing with old ticker → first with new) |
|---|---|---|
| C000244148 (Hartford) | QUVU → ACVU | 497 0001193125-26-189330 (2026-04-29) → 497K 0001193125-26-190894 (2026-04-29); 2026 dataset already ACVU |
| C000012097 (iShares) | ILCB → MLRG | NPORT-P 0000940400-26-038484 (2026-09-25) → 497 0001193125-26-412628 (2026-10-02) |
| C000216993 (First Trust) | MARB → NTRL | 497K 0001445546-26-004352 (2026-06-12) → 485BPOS 0001445546-26-004477 (2026-06-18) |
| C000053051 (Invesco) | PHB → IFLN | 485BPOS 0001104659-26-018152 (2026-02-20) → 497J 0001193125-26-062447 (2026-02-23) |
| C000057274 (VanEck) | BJK → GENZ | 497 0001137360-26-000296 (2026-03-20) → 485BPOS 0001137360-26-000365 (2026-04-08) |

All 29 renames have both filings pinned; the 26 MV cases are STRV→STXF,
ILCB→MLRG, ISCB→MSML, KRMA→CPTL, MUSI→ABND, RFDI→AFDM, RFEM→AFEM,
MBCC→MBCE, GBF→AGGM, TUGN→SEPQ, TMET→ISTM, MAPP→MATR, XFIX→ZHOG, PHB→IFLN,
SPVU→QVMT, KBWR→FDIQ, SNPV→XOEX, FILL→POWR, RAYD→RWLC, RAYE→RWEM,
RVRB→VOXP, BJK→GENZ, NSRKK→NSRKX, MMLG→AFGR, EASG→DMXU, QUVU→ACVU.

Rule: the IU ticker has no fresh SEC row; the registry class (or, when the
registry has none, the single class the pinned dataset ties to the old ticker
in the registry series) has exactly one fresh SEC row with another ticker in
the same series; the old ticker is proven to be that class by a pinned dataset
row or filing; the new ticker belongs to no other IU or registry row. Then
`instruments_universe.ticker` and, when different, `instrument_identity.ticker`
(and an empty `sec_class_id`) take the SEC values, with `identity_sources`
stamped `sec_company_tickers_mf`. Same class, same NAV series: no rebase.
Rows: 29 `instruments_universe` + 17 `instrument_identity`.

Two renames are left to review because the new ticker already belongs to
another IU instrument: BEMO→ADME and DVP→DEEP (stale duplicate instruments of
a class the catalog already carries under its current ticker).

### 3. IU ticker on a terminated share class (R3, opt-in)

The IU instrument points at a class the fund terminated, while the registry
(and `funds_v`) already declare a live class of the same series. NAV
ingestion keeps fetching the dead class (`success_no_new` with a 2024 date,
or `empty`).

| IU ticker (dead class) | registry class → ticker | SEC evidence for termination |
|---|---|---|
| PINUX (C000111522) | C000069149 → PINZX | in the 2023–2025 datasets, absent from 2026; last listing N-CEN 0001752724-25-001707 (2025-01-13); NAV stops 2024-12-26 |
| LTFLX (C000063450) | C000063447 → LTFDX | same N-CEN; absent from 2026 dataset; NAV stops 2024-12-26 |
| PMGRX (C000113840) | C000038760 → CMPGX | same N-CEN; absent from 2026 dataset |
| FMRGX (C000213719) | C000213718 → FMREX | last listing N-CEN 0001752724-24-227615 (2024-10-15); Tiingo `empty` |
| SUBSX (C000193403) | C000193399 → SUBDX | last listing NPORT-P 0001145549-24-029756 (2024-05-24) |

Rule: the IU ticker has no fresh SEC row and is not in the 2026 dataset; the
pinned datasets tie it to exactly one class of the registry series; that class
is neither in the 2026 dataset nor fresh; the registry class has exactly one
fresh row in the same series and the registry ticker is (or was) its ticker;
the target ticker is free. Then the IU ticker takes the registry class's
current ticker. 22 instruments (20 MV, all `ticker.mismatch`); 22 IU rows.

This is opt-in because the instrument's NAV history belongs to the dead class.
After the repoint the ingestion appends the live class from the old
watermark, so every repointed instrument needs a governed NAV rebase
(`scripts/rebase_fund_nav_window.py`, ≤ 20 instruments per batch) before its
risk metrics are trusted. Without the flag the plan lists them under
`class_repoint_candidate`.

### 4. Registry conflicts SEC now settles (R4)

| ticker | conflict | SEC now |
|---|---|---|
| VTCLX | ticker VMCAX/VTCLX, class C000012134/C000012135 (Vanguard converted Investor shares into Admiral; C000012134 left the 2025 dataset) | VTCLX → C000012135; B aligns the registry, R4 drops both keys |
| QUVU | ticker QUVU/ACVU | ACVU (R2 renames first) |

Rule: when the IU ticker equals the registry ticker and maps to exactly one
fresh SEC row matching the registry series/class, drop each conflict key among
ticker/class/series/CIK whose registry value equals the SEC value and appears
among the observed values. Other keys stay. Rows: 2 registry rows.

Not resolvable from SEC: FLDBX, FACBX, FASBX (`sec_private_fund_id` from Form
ADV) and GMCHX (registry class/ticker disagree with the single SEC row).

### 5. Inactive but live (R5)

74 inactive funds in `funds_profile_mv` are live: their ticker is the single
current SEC class of the registry series, Tiingo has prices through
2026-10-02/05, and no other instrument of the series is active, so the series
is invisible to the builder. Most were never ingested (no NAV row at all)
because ingestion skips inactive instruments.

| ticker | series | SEC (`sec_company_tickers_mf`, 2026-10-06) | Tiingo `endDate` |
|---|---|---|---|
| VTI (Vanguard Total Stock Market ETF) | S000002848 | VTI → C000007808, CIK 36405; 2026 dataset "ETF Shares" | 2026-10-05 |
| VSTSX (same series, Institutional Select) | S000002848 | VSTSX → C000170276 | 2026-10-05 |
| SPYI (NEOS S&P 500 High Income ETF) | S000077194 | SPYI → C000237368 | 2026-10-05 |
| FFNMX (Floating Rate High Income Portfolio) | S000044870 | FFNMX → C000259786 | 2026-10-01 |
| PGOVX (PIMCO Long-Term U.S. Government) | S000009690 | PGOVX → C000026570 | 2026-10-02 |

S000002848 has two live candidates (VTI, VSTSX) and both are activated: each
is proven on its own, and the catalog already carries an ETF class next to the
canonical class for VNQ/VGSNX and BND/VBTLX. FLDBX is activated but stays
UNKNOWN on its unrelated `sec_private_fund_id` conflict.

Rule: `is_active=false`; the instrument has a `funds_v` row; no other
instrument of its registry series is active; it is not a phase-B historical
sibling; it carries no product exclusion (`exclusion_reason`,
`strategic_excluded_reason`, `is_institutional=false`); the IU ticker equals
the registry ticker and maps to exactly one fresh SEC row of the registry
series/class; Tiingo meta shows an `endDate` within 7 days of the observation,
observed at most 30 days before the plan; only one such candidate in the
series. Then `is_active := true`. Rows: 74 IU rows.

Left for review: 60 live-in-SEC orphans with no current Tiingo
history (e.g. FEOTX/FEITX First Eagle Class T and HMCDX Harbor: Tiingo knows
the symbol but has no prices; insurance-only portfolio symbols such as
XAOKX), 9 orphans with a product exclusion (Municipal Bond, sub-scale), 17
phase-B siblings of series with no active class (choose the canonical class),
and 130 orphans whose identity is not SEC-current (dead class, no ticker).

### 6. Active but terminated (R6)

152 active instruments (7 MV, 145 outside `funds_v`) point at series that SEC
dropped: neither the ticker nor the series is in `sec_company_tickers_mf` or
the 2026 dataset, the series' last filing is from 2020–2025, and NAV stopped
more than 90 days ago (Tiingo `success_no_new` with an old date, `empty` or
`not_found` in today's run).

| ticker | series | newest NAV | last filing for the series |
|---|---|---|---|
| PIPPX (Principal MidCap Growth) | S000007078 | 2024-12-27 | NPORT-P 0000898745-25-000535 (2025-09-24) |
| PPIMX (Principal MidCap Growth III) | S000007125 | 2025-09-23 | NPORT-P 0000898745-25-000560 (2025-09-24) |
| LLINX (Longleaf Partners International) | S000009313 | 2025-12-22 | N-14/A 0001580642-25-007492 (2025-11-28) |
| JINTX (Johnson International) | S000024217 | 2025-11-21 | NPORT-P 0000910472-25-005154 (2025-12-01) |
| PGIPX (PGIM ESG Short Duration) | S000076422 | 2025-09-23 | in the evidence bundle |

Rule: as above; tickers never seen by SEC (UCITS, grantor trusts such as
GLD) are never touched. Then `is_active := false`, which stops the daily
`not_found`/`empty` fetches. No ACTIVE fund changes: all 152 already fail the
SEC gate. Six funds whose series left the SEC data but whose NAV is still
current (five Hodges funds, FAKDX) are listed under
`series_terminated_nav_current` instead.

### 7. Registry without series (R7)

20 registry rows created by `backfill_benchmark_proxy_etfs` carry a ticker
but no series/class/CIK: TIP, IWS, BIZD, IWN, MBB, SGOV, MUB, HYG, GOVT, VTIP,
IWO, ICVT, AFIF, IWP, QAI, PCLO, BIL, ASMF, EMB, LQD. Each ticker has exactly
one fresh SEC row (e.g. HYG → S000016772/C000045000 under CIK 1100663). R7
fills `sec_series_id`, `sec_class_id`, `cik_padded`, `cik_unpadded`.
17 of the 20 series pass the existing N-PORT eligibility gate, so
those funds appear in `funds_v`. Rows: 20 registry rows.

### 8. Registry on a terminated class (review)

41 instruments (22 MV) have IU ticker = registry ticker = a class SEC
terminated, in a series that is still live: the B-on-stale-rows cases
(FSVJX, FHRCX, FSZOX, FHJCX, FHDCX Fidelity Z6 classes; PSLBX, PGNBX, PNSBX
Putnam B classes) and a few historic ones. The correct class has to be chosen
among the live classes. With B restricted to fresh rows, 18 of them keep their
live registry class, and 11 of those become R3 repoints.

### 9. Series reorganizations (review)

SEC's current row for the ticker sits under a different series and registrant
than the registry:

| ticker | registry series | SEC series (CIK) |
|---|---|---|
| VVPLX | S000027283 | S000091565 (1936157) |
| VVPSX | S000027284 | S000091566 (1936157) |
| HSPCX | S000036390 | S000093696 (831114) |
| OIODX | S000075333 | S000057227 (831114) |
| PLABX | S000002972 | S000017840 (1395397) |
| WINC/STNC | S000064209 / S000079062 | S000107974 / S000070925 |

Re-pointing the registry series would re-key `funds_v` and the N-PORT
eligibility gate to the successor series, which has no eight-quarter
look-through history yet: the fund would fall to `cardinality.funds_v_missing`.
A reorganization is not a termination, so these stay as they are until the
eligibility gate can follow a predecessor series.

### 10. SEC source gaps (review)

33 MV funds have a ticker the SEC June 2026 dataset lists for the registry
series, but `company_tickers_mf.json` (and therefore the generator's SEC gate)
does not: e.g. OBBCX (JPMorgan MBS Class C), ABREX, FAMYX, SOUCX. Some are
terminations after June (OBBCX: Tiingo `empty`), others are gaps in SEC's own
ticker file. No catalog edit can satisfy the gate; the owner decides whether
the gate may fall back to the series/class dataset.

### 11. Phase-B siblings (no action)

512 of the 674 `activity.not_active` MV funds are inactive share-class
siblings (`attributes.historical_nav_ticker`) of a series whose canonical
class is active (e.g. the 18 inactive American Funds Growth Fund of America
classes next to the active R-6 class). That is the catalog design, not an
identity error.

### 12. No ticker (review)

125 MV funds (all `nport_firm5bn_backfill`, inactive) have no ticker in IU or
the registry. For 93 of them SEC lists no ticker for any class of the series
(variable-insurance portfolios: T. Rowe Price Equity Income Portfolio, MFS
Total Return Bond Series...). 8 series have exactly one ticker-bearing class
and 23 have several; assigning one would create a second instrument for a
class the catalog may already carry (e.g. the Dreyfus/BNY Mellon Natural
Resources classes), so it is left to the owner.

### 13. Tiingo ingestion failures

Today's chain run recorded 458 Tiingo failures (278 `not_found`, 180 `empty`)
and 458 `eodhd not_configured` (no EODHD key; not an instrument signal).

| disposition | count | MV |
|---|---:|---:|
| ticker never in SEC (UCITS `.PA/.L/...`, Yahoo serves 242 of them) | 278 | 0 |
| identity SEC-consistent; Tiingo does not carry the symbol | 106 | 53 |
| R6 deactivate (series terminated) | 16 | 0 |
| R2 rename (QUVU, PHB, SPVU, KBWR, XFIX, STRV, MAPP, SNPV, VLLU) | 9 | 8 |
| R3 repoint (Fidelity Managed Retirement Z6 classes) | 5 | 5 |
| SEC source gap (review 10) or not in `company_tickers_mf` | 20 | 4 |
| registry on terminated class (review 8) | 5 | 5 |
| no registry row / other | 19 | 0 |

### 14. `cardinality.funds_v_missing` (2,830, outside MV)

| class | count | what it is |
|---|---:|---|
| registry series present, series fails the N-PORT eligibility gate | 1,779 | new funds (< 8 quarters), leveraged/derivative funds below 80 % look-through coverage (TQQQ), and terminated series (R6 deactivates 145 of them) |
| registry row without series | 744 | 631 UCITS (ESMA identity, outside SEC); 20 with a unique current SEC row (R7: 17 benchmark-proxy ETFs + AFIF, PCLO, ASMF); 93 without a unique current SEC row |
| no registry row | 307 | 306 without a unique current SEC row (UCITS/foreign listings, closed-end funds, test rows such as Q86TST); 1 with a SEC row whose series is not eligible (NWXJX) |

None of them belongs in `funds_v` except the 17 R7 funds whose series pass the
gate: the others fail the eligibility gate by its own rules or are not SEC
'40-Act registrants.

### 15. Registry SEC identifiers

Checked for every registry row against the four datasets and the SEC sync: no
class sits in a series other than the registry series, no registry CIK
disagrees with the series' CIK, no registry CUSIP fails its checksum or maps
(via `sec_cusip_ticker_map`) to another ticker. 102 registry classes are
unknown to the 2023–2026 data; all are phase-B siblings of classes closed
before 2023 (American Funds Class B / 529-B converted in 2017). 458 registry
series are unknown to SEC data; all are `INACTIVE` instruments outside
`funds_v`.

## B with fresh rows only

Simulated with B matching only SEC rows fresh at the decision instant:

| plan | ACTIVE | lost vs A + B |
|---|---:|---:|
| A + B(fresh) | 7,348 | — |
| A + B(fresh) + default plan | 7,484 | 0 |
| A + B(fresh) + default plan + R3 | 7,512 | 0 |

B(fresh) leaves 20 more funds on `ticker.mismatch` with their live registry
class, R3 picks 33 repoints instead of 22, and `registry_class_terminated`
drops from 41 to 23. The recommendation to the A/B owner is to judge SEC rows
with the generator's freshness window.

## Top funds gained (default plan + R3, by `aum_usd`)

| ticker | fund | `aum_usd` (bn) | status after A + B | rule |
|---|---|---:|---|---|
| VTI | Vanguard Total Stock Market ETF | 2,056.6 | activity.not_active | R5 |
| VTCLX | Vanguard Tax-Managed Capital Appreciation | 9.8 | registry.conflict_state_not_empty | R4 |
| FFNMX | Floating Rate High Income Portfolio | 6.1 | activity.not_active | R5 |
| PINUX → PINZX | Principal Overseas Fund | 5.8 | ticker.mismatch | R3 |
| PSBGX → SABPX | Principal SAM Balanced | 5.8 | ticker.mismatch | R3 |
| LTFLX → LTFDX | Principal LifeTime 2055 | 5.8 | ticker.mismatch | R3 |
| PIOPX → CMPIX | Principal Core Fixed Income | 5.8 | ticker.mismatch | R3 |
| DGEFX | Brinker Capital Destinations Trust | 4.3 | isin.unsupported_prefix | R1 |
| SUBSX → SUBDX | Carillon Reams Unconstrained Bond | 3.2 | ticker.mismatch | R3 |
| QAAAJX | T. Rowe Price Blue Chip Growth Portfolio | 2.6 | activity.not_active | R5 |
| STRV → STXF | Strive 500 ETF | 1.6 | sec.missing | R2 |
| ILCB → MLRG | iShares Morningstar U.S. Equity ETF | 1.3 | sec.missing | R2 |

Without R3 the list is the same minus the R3 rows. The 17 R7 funds (HYG, MBB,
SGOV, BIL, MUB, LQD, EMB, TIP, GOVT, IWN, IWO, IWP, IWS, ICVT, BIZD, QAI,
VTIP) carry `aum_usd = 0` on IU and so do not show in this ordering.

`aum_usd` on IU is firm- or series-level for many rows, so this order is
indicative.

## Repair script

`scripts/repair_fund_identity_sec_v1.py` (ledger:
`schemas/fund_identity_sec_repair_v1.sql`, evidence:
`contracts/fund-identity-sec/evidence_v1.json`).

* `--mode plan` (default): one `REPEATABLE READ READ ONLY` snapshot on a
  read-only session; prints counts per rule (overall and in `funds_v`),
  review counts, the plan sha256 and the generator's ACTIVE count before and
  after the plan in memory. `--plan-file` writes the full plan.
* `--mode apply --confirm repair_fund_identity_sec_v1 --expect-plan-sha256 H`:
  one read-write `REPEATABLE READ` transaction holding the instrument-ingestion
  advisory lock (900_331); re-plans on its own snapshot and refuses a different
  digest; writes each row with a compare-and-swap on its before-values; stores
  a receipt per row (before/after of every written column, rules, evidence);
  re-plans on the written state and aborts unless it is empty; commits.
* `--mode rollback --rollback-run-id R --confirm repair_fund_identity_sec_v1`:
  restores run R's before-values (compare-and-swap on its after-values) and
  records the rollback; one rollback per apply.

`instruments_universe` and `instrument_identity` have no triggers; the writes
respect `uq_iu_ticker`/`uq_iu_isin` (every new ticker is checked against all
IU and registry rows). The ledger tables are append-only (row and truncate
triggers).

## Dry run on production (before A + B)

`--mode plan --include-class-repoint` on the live database at 2026-10-06
06:59 UTC, with A and B not yet applied (read-only session):

| rule | rows (overall) | in `funds_v` |
|---|---:|---:|
| R1 | 6,134 | 4,684 |
| R2 | 46 | 41 |
| R3 | 33 | 31 |
| R4 | 1 | 1 |
| R5 | 54 | 54 |
| R6 | 152 | 7 |
| R7 | 20 | 0 |

Generator ACTIVE 2,896 → 5,166 in memory; plan sha256
`3673e2841cfc358fc6665922c50df8eefccc44d649e5fcba356fd2ea0ee499c8`. R1 is a
superset of rule A, so before A it also NULLs A's 5,285 rows; once A and B are
applied the plan shrinks to the simulated one above (R4 then also clears
VTCLX, R5 reaches 74). The digest is only valid for the state it was computed
on: re-run the plan after A + B and apply with that digest.

## Apply order

Prerequisites: the SEC sync ran within 7 days; the evidence bundle is less
than 30 days old for R5 (re-collect Tiingo meta otherwise); no NAV ingestion
run is active.

1. Rules A + B (`fix/fund-catalog-identity-repair`), preferably with B on
   fresh SEC rows.
2. This repair, from `E:\tmp-deploy\api`:

   ```bash
   railway run --service risk-metrics -- uv run --no-project \
     --directory E:/investintell-datalake-workers-identity-audit \
     --with "psycopg[binary]" --with exchange_calendars==4.13.2 \
     python -m scripts.repair_fund_identity_sec_v1 \
     --sec-cache-dir E:/tmp-deploy/sec-cache \
     --db-host centerbeam.proxy.rlwy.net:36616 \
     --include-class-repoint --plan-file C:/path/outside/repo/plan.json
   # review the plan, then the same command with
   #   --mode apply --confirm repair_fund_identity_sec_v1 --expect-plan-sha256 <plan_sha256>
   ```

   The SEC dataset files must be in `--sec-cache-dir` with the pinned sha256
   (download URLs are in the evidence bundle). Drop `--include-class-repoint`
   to leave R3 for later.
3. If R3 was applied: governed NAV rebase of the repointed instruments
   (`scripts/rebase_fund_nav_window.py --mode plan`, then `--mode apply` in
   batches of ≤ 20; instrument ids are in the receipts with rule
   `R3_iu_class_terminated_repoint`).
4. Refresh `funds_profile_mv` (its owning job) so the R7 funds join the
   readiness cohort.
5. Regenerate and publish the NAV policy (`generate_fund_nav_policy_v1`
   build/verify, then the existing publish step).
6. Re-run the NAV chain: renamed tickers and activated funds fetch from
   Tiingo; deactivated funds stop.

Rollback: `--mode rollback --rollback-run-id <run_id> --confirm repair_fund_identity_sec_v1`
with the same `railway run` prefix.

## Open items for the owner

* Choose a class (or accept R3) for the 41 `registry_class_terminated` funds.
* Decide whether the eligibility gate may follow a predecessor series, so the
  six reorganized funds can be re-keyed without leaving `funds_v`.
* Decide whether the SEC gate may fall back to the series/class dataset for
  the 33 `sec_current_source_gap` funds.
* Pick canonical classes for the 17 phase-B-only orphan series and the 31
  ticker-less series that have ticker-bearing classes.
* 106 SEC-consistent funds whose symbols Tiingo does not carry need another
  NAV provider; identity is not the problem there.

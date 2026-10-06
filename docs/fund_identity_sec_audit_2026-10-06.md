# Fund identity audit against SEC data (2026-10-06)

The portfolio builder admits a fund only when the NAV lifecycle evidence from
`scripts/generate_fund_nav_policy_v1.py` says `ACTIVE`, and the next policy
can only be published if the identity audit
(`scripts/verify_fund_nav_identity_v2.py`, `configs/nav_identity_audit_v3.json`)
finds zero SEC integrity failures. This audit looks for every fund/ETF
identity mismatch left after the catalog repair rules A and B (PR #149,
`scripts/repair_fund_catalog_identity_v1.py`), judges each one against SEC
data, and ships `scripts/repair_fund_identity_sec_v1.py` to correct what SEC
proves. The cases SEC cannot settle go to the owner review lists.

## Result

Live snapshot of 2026-10-06 05:55 UTC. A and B are planned with PR #149's own
`plan_repairs` (A nulls 5,285 IU ISINs, B aligns 2,490 registry rows), then
this repair is planned on that state; every count comes from the generator's
`classify_catalog` and the A4/A8 previews PR #149 computes with the auditor's
own code. `funds_v` re-keying follows the N-PORT eligibility gate (series set
read from the same CTE as `fund_catalog_eligible_instruments_v`).

| state | ACTIVE | ACTIVE in `funds_profile_mv` | A4 P (structural daily) | A4 B (baseline) | A8 SEC integrity |
|---|---:|---:|---:|---:|---:|
| production today | 2,896 | 2,896 | 5,102 | 5,103 | 1 |
| A + B | 7,348 | 7,348 | 7,410 | 7,411 | 6 |
| A + B + default plan (R1, R2, R4–R8) | 7,466 | 7,444 | 7,487 | 7,488 | 2 |
| **A + B + default + R9 (`--quarantine-sec-contradictions`): the approved plan** | **7,466** | **7,444** | **7,485** | **7,488** | **0** |
| A + B + default + R3 (`--include-class-repoint`, later phase) + R9 | 7,489 | 7,467 | 7,508 | 7,511 | 0 |

No fund that is ACTIVE in the published policy or under A + B loses that
status under any of these plans. The approved plan gains 118 funds over
A + B: 96 already in `funds_profile_mv` and 22 new to `funds_v` (17
benchmark-proxy ETFs through R7, 5 Grandeur Peak funds through R8).
Owner decisions (2026-10-06): R9 approved for apply; R3 deferred to a later
phase together with its NAV rebase; R5 approved except share classes offered
only through insurance-company separate accounts.

The two integrity failures the default plan leaves (STNC, OIODX) are funds
whose own filings and SEC's ticker file disagree about the live series; see
[section 8](#8-ticker-moved-to-another-series-r8-and-sec-self-contradictions-r9).
R9 records that disagreement in `conflict_state` so they stop before the SEC
stage; without R9 the policy cannot be published.

### Audit config re-pin

`structural_daily_ceiling` bounds P and must be at least |P|;
`accepted_structural_delta` must equal |B| − `structural_baseline` (5,103).
Values at this snapshot (they move with any later catalog or SEC change, so
recompute them on the generation snapshot before pinning):

| plan applied after A + B | `structural_daily_ceiling` ≥ | `accepted_structural_delta` |
|---|---:|---:|
| A + B only | 7,410 | 2,308 |
| **default + R9 (approved)** | **7,485** | **2,385** |
| default + R3 + R9 | 7,508 | 2,408 |

A8 at default + R9: N = 7,484 funds reach the SEC stage, bound ⌊N/10⌋ = 748,
`sec.stale` 18, `sec.missing` 0, integrity 0. Without R9 integrity stays 2.

## Sources and pins

| source | role | pin |
|---|---|---|
| `public.sec_company_tickers_mf` | current SEC class/series/ticker (daily sync of `company_tickers_mf.json`). Every rule reads only its newest daily batch (one `updated_at` per sync, 28,607 of the 28,620 rows younger than 7 days on 2026-10-06): a row left behind by the upsert-only sync was withdrawn from SEC's file even while the generator still calls it fresh. The plan refuses to run when the newest batch holds less than 95 % of those rows (a `limit` run of the sync commits only a prefix) | read in the plan's own `REPEATABLE READ` snapshot |
| SEC *Investment Company Series and Class Information* 2023–2026 | every series/class not yet reclassified inactive, with tickers; the 2026 file (SEC update 2026-06-01) is "current-year" | sha256 in `SERIES_CLASS_FILES` of the script |
| sec-api.io Query API (`485BPOS`, `485APOS`, `497`, `497K`, `497J`, `N-CEN`, `N-CSR(S)`, `NPORT-P`, `N-14`) | first/last filing showing a class under a ticker; newest filing listing a terminated class or series | accession numbers in `contracts/fund-identity-sec/evidence_v1.json` |
| Tiingo daily meta (`/tiingo/daily/<ticker>`) | whether a ticker has a current price history (R2, R3, R5) | `tiingo_meta` in the evidence bundle, with `observed_at` |
| `nav_ingestion_attempts` run `244e8eea-5b3c-4d7d-b74d-2a29fc0428f1`, `nav_timeseries` | production Tiingo outcome and newest NAV date (R6) | same snapshot |
| `company_tickers_mf.json` (2026-10-06 download) | cross-check of the DB sync: 28,608 rows, 1 row differs from the newest DB batch | sha256 in the evidence bundle |

sec-api bandwidth used: 4.92 MB in October 2026 (about 600 Query API calls).
Tiingo: about 590 meta requests, at most 0.5 requests per second.

## Findings

Counts are instruments. "MV" is `funds_profile_mv` (8,268); "all" is every
`instruments_universe` fund. Status is the first failure after A + B.

| # | finding | MV | all | disposition |
|---|---|---:|---:|---|
| 1 | IU `isin` holds an EDGAR id A does not cover (CIK, or a series id other than the registry series) | 22 | 849 | R1: NULL |
| 2 | ticker renamed, same SEC class | 26 | 28 | R2: rename IU (and registry) ticker |
| 3 | IU ticker on a terminated share class; registry holds a live class of the series | 26 | 26 | R3 (opt-in, deferred to a later phase) + NAV rebase |
| 4 | registry `conflict_state` on ticker/class that SEC settles | 2 | 2 | R4 |
| 5 | inactive but live (SEC + Tiingo current), series has no active instrument, not insurance-only | 51 | 51 | R5: activate |
| 5b | same, but the class is offered only through insurance separate accounts (N-CEN / 485BPOS) | 23 | 23 | stays inactive (owner decision) |
| 6 | active but ticker and series gone from SEC, NAV stopped > 90 days | 7 | 152 | R6: deactivate |
| 7 | registry row has ticker but no series/class | 0 | 20 | R7: fill from SEC |
| 8 | ticker moved to another series (reorganization), fund files under the new one | 4 | 13 | R8: move registry series/class/CIK |
| 9 | SEC's ticker file contradicts the fund's own newer filings | 2 | 3 | R9 quarantine (opt-in, approved); OPTCX review |
| 10 | terminated IU class whose live-class target Tiingo no longer prices | 5 | 7 | review |
| 11 | IU and registry both on a terminated class | 9 | 23 | review |
| 12 | ticker in the SEC June dataset but missing from SEC's current ticker file | 37 | 37 | review (SEC source gap) |
| 13 | phase-B historical share-class siblings, inactive by design | 512 | 512 | none |
| 14 | no ticker anywhere (variable-insurance portfolios) | 125 | 125 | review |
| 15 | inactive fund with a SEC-current ticker but no Tiingo price history | 60 | 60 | review |
| 16 | `cardinality.funds_v_missing` | — | 2,830 | classified below |
| 17 | registry SEC ids that do not exist or point elsewhere | 0 | 0 | none (checked) |

### 1. EDGAR identifiers stored as ISIN (R1)

A NULLs `instruments_universe.isin` only when it equals the registry series.
The residue is 849 rows (22 in MV, all `isin.unsupported_prefix`): 478 hold a
zero-padded CIK and 371 a series id that is not the registry series (a
predecessor series after a reorganization, or a fund with no registry series).
No ISIN can match `^S\d{9}$`, `^C\d{9}$` or `^\d{1,10}$` (an ISIN starts with
a two-letter country code).

| ticker | IU `isin` | what it is |
|---|---|---|
| DGEFX | `0001688680` | CIK of Brinker Capital Destinations Trust |
| MPGVX | `0001651872` | CIK of Gallery Trust |
| CCBFX | `0001841440` | CIK of Capital Group Central Fund Series II |
| HBSGX | `S000004108` | old series; SEC lists HBSGX under S000084804 (registry series) |
| FICIX | `S000065928` | old series; registry series S000075628 |

Rows: 849 `instruments_universe`. 20 MV funds become ACTIVE. OIODX is one of
the 22: nulling its `S000057227` exposes the SEC contradiction of section 8.

### 2. Ticker renamed, same class (R2)

The SEC class did not change; its ticker did. The IU ticker (which drives the
Tiingo fetch) still holds the old symbol, so ingestion gets `empty`/`not_found`,
and the registry is either already on the new ticker (`ticker.mismatch`) or
still on the old one (`sec.missing`/`sec.contradiction`).

| class | old → new | last filing with old ticker → first with new |
|---|---|---|
| C000244148 (Hartford) | QUVU → ACVU | 497 0001193125-26-189330 (2026-04-29) → 497K 0001193125-26-190894 (2026-04-29) |
| C000012097 (iShares) | ILCB → MLRG | NPORT-P 0000940400-26-038484 (2026-09-25) → 497 0001193125-26-412628 (2026-10-02) |
| C000196638 (Xtrackers) | EASG → DMXU | 485BPOS 0000088053-26-000744 (2026-09-11) → 497K 0000088053-26-000745 (2026-09-11) |
| C000216581 (First Trust) | MMLG → AFGR | N-CSRS 0001445546-26-003573 (2026-05-08) → 485BPOS 0001445546-26-004172 (2026-06-04) |
| C000057274 (VanEck) | BJK → GENZ | 497 0001137360-26-000296 (2026-03-20) → 485BPOS 0001137360-26-000365 (2026-04-08) |

The 26 MV renames: STRV→STXF, ILCB→MLRG, ISCB→MSML, KRMA→CPTL, MUSI→ABND,
RFDI→AFDM, RFEM→AFEM, MBCC→MBCE, GBF→AGGM, TUGN→SEPQ, TMET→ISTM, MAPP→MATR,
XFIX→ZHOG, PHB→IFLN, SPVU→QVMT, KBWR→FDIQ, SNPV→XOEX, FILL→POWR, RAYD→RWLC,
RAYE→RWEM, RVRB→VOXP, BJK→GENZ, NSRKK→NSRKX, MMLG→AFGR, EASG→DMXU,
QUVU→ACVU; outside MV MARB→NTRL, VLLU→AELV. EASG and MMLG are two of the six
integrity failures A + B unmasks; the rename clears both.

Rule: the IU ticker has no fresh SEC row; the registry class (or, without one,
the single class the pinned dataset ties to the old ticker in the registry
series) has exactly one fresh SEC row with another ticker in the same series;
a pinned dataset row or filing proves the old ticker was that class; the new
ticker belongs to no other IU or registry row; Tiingo has a current history
for the new ticker. Then `instruments_universe.ticker` and, when different,
`instrument_identity.ticker` (and an empty `sec_class_id`) take the SEC values,
with `identity_sources` stamped `sec_company_tickers_mf`. Same class, same NAV
series: no rebase (Tiingo opened some new symbols on the rename date, e.g.
AFEM from 2026-09-14; ingestion continues from the instrument's watermark).
Rows: 28 `instruments_universe` + 17 `instrument_identity`.

Held back: RBON→RTHY (Tiingo has no RTHY prices; inactive, outside
`funds_v`), BEMO→ADME and DVP→DEEP (the new ticker already belongs to another
IU instrument: stale duplicates).

### 3. IU ticker on a terminated share class (R3, opt-in)

The IU instrument points at a class the fund terminated, while the registry
(and `funds_v`) already declare a live class of the same series.

| IU ticker (dead class) | registry class → ticker | evidence of termination |
|---|---|---|
| PINUX (C000111522) | C000069149 → PINZX | in the 2023–2025 datasets, absent from 2026; newest listing N-CEN 0001752724-25-001707 (2025-01-13); NAV stops 2024-12-26 |
| LTFLX (C000063450) | C000063447 → LTFDX | same N-CEN; absent from 2026; NAV stops 2024-12-26 |
| PMGRX (C000113840) | C000038760 → CMPGX | same N-CEN; absent from 2026 |
| PINLX (C000019056) | C000019054 → PINRX | same N-CEN; absent from 2026; NAV stops 2024-12-27 |
| SUBSX (C000193403) | C000193399 → SUBDX | newest listing NPORT-P 0001145549-24-029756 (2024-05-24) |
| FSVJX (C000242538, Z6) | C000242536 → FSVMX (K6) | in the 2023–2025 datasets, absent from 2026; newest listing N-CEN 0001752724-25-134309 (2025-06-12); NAV stops 2024-12-27 |
| PSLBX (C000010788, B) | C000010787 → PSLAX (A) | in the 2023–2025 datasets, absent from 2026; newest listing 497 0001193125-25-201855 (2025-09-12); NAV stops 2025-09-23 |

Rule: the IU ticker has no fresh SEC row and is not in the 2026 dataset; the
pinned datasets tie it to exactly one class of the registry series; that class
is neither in the 2026 dataset nor fresh; the registry class has exactly one
fresh row in the same series and the registry ticker is (or was) its ticker;
the target is free and Tiingo prices it. Then the IU ticker takes the
registry class's ticker. 26 instruments, all in MV and all `ticker.mismatch`
after A + B: 14 Principal classes (R-1/R-4 and others), 3 Carillon (SUBSX,
EISRX, HSRYX), 6 Fidelity (Sustainable Target Date and Freedom Blend Z6, FFRFX)
and 3 Putnam B classes. The Putnam ones are inactive orphans that R5 then
activates on the class A ticker. Rows: 26 IU rows.

It is opt-in because the instrument's NAV history belongs to the dead class:
each repointed instrument needs a governed NAV rebase
(`scripts/rebase_fund_nav_window.py`, ≤ 20 per batch) before its risk metrics
are trusted. Without the flag they are listed as `class_repoint_candidate`.

### 4. Registry conflicts SEC settles (R4)

| ticker | conflict | resolution |
|---|---|---|
| VTCLX | ticker VMCAX/VTCLX, class C000012134/C000012135; B skips conflicted rows, so the registry stays on VMCAX, the Investor class Vanguard converted (C000012134 is in the 2024 dataset, absent since 2025) | registry → VTCLX/C000012135 (single current SEC row, one of the observed values), both keys dropped |
| QUVU | ticker QUVU/ACVU | R2 renames, the key is dropped |

Rule: drop each ticker/class/series/CIK conflict key whose registry value
equals the single current SEC row of the IU ticker and appears among the
observed values. A conflict only on ticker/class whose registry class SEC
terminated is first aligned to that row (the B alignment). Other keys stay:
FLDBX, FACBX, FASBX (`sec_private_fund_id` from Form ADV) are left for review.
Rows: 2 registry rows.

### 5. Inactive but live (R5)

74 inactive funds in `funds_profile_mv` are live: the ticker is the single
current SEC class of the registry series, Tiingo prices it through
2026-10-01..05, and no other instrument of the series is active, so the
series is invisible to the builder. Per the owner's decision, 23 of them
(share classes offered only through insurance-company separate accounts)
stay inactive and 51 are reactivated, VTI among them.

| ticker | series | SEC (`sec_company_tickers_mf`) | Tiingo `endDate` |
|---|---|---|---|
| VTI (Vanguard Total Stock Market ETF) | S000002848 | VTI → C000007808, CIK 36405; 2026 dataset "ETF Shares" | 2026-10-05 |
| VSTSX (same series, Institutional Select) | S000002848 | VSTSX → C000170276 | 2026-10-05 |
| SPYI (NEOS S&P 500 High Income ETF) | S000077194 | SPYI → C000237368 | 2026-10-05 |
| FFNMX (Floating Rate High Income Portfolio) | S000044870 | FFNMX → C000259786 | 2026-10-01 |
| PGOVX (PIMCO Long-Term U.S. Government) | S000009690 | PGOVX → C000026570 | 2026-10-02 |

Both live classes of S000002848 are activated, as the catalog already carries
an ETF class next to the canonical class for VNQ/VGSNX and BND/VBTLX. FLDBX
is activated but stays UNKNOWN on its unrelated `sec_private_fund_id`
conflict.

**Insurance-only exclusion.** Decided per series from pinned SEC evidence,
never from names:

* the series' newest N-CEN, Item C.3 fund types: "Underlying fund" (underlying
  fund of a registered insurance separate account offering variable annuity
  and variable life contracts) without "Exchange-Traded Fund"; or
* a pinned 485BPOS whose EDGAR header lists the series and whose text
  restricts the shares to insurance separate accounts or variable contracts
  (a sentence with "only/exclusively/solely" and an insurance channel, and
  no other channel such as funds of funds or collective trusts).

| registrant (CIK) | funds kept inactive | evidence |
|---|---|---|
| T. Rowe Price Equity Series (918294), Fixed Income Series (920467), International Series (918292) | QAAAJX, QAMWEX, QAAGZX, QAAGRX, QAOSWX, QAAHAX, QAAGWX, QAAGYX (8) | 485BPOS 0001999371-26-008879 / -008880 / -008882 (2026-04-24): "The fund is generally available only through variable annuity or variable life insurance contracts." (their N-CEN does not tick C.3) |
| Russell Investment Funds (824036) | RIFAX, RIFBX, RIFCX, RIFDX, RIFGX, RIFHX, RIFIX, RIFJX, RIFSX (9) | N-CEN 2026-03-12 "Underlying fund"; 485BPOS 0001193125-26-196319: shares "sold only to Insurance Companies" |
| Fidelity Variable Insurance Products Fund IV (720318) | FFNHX, FFNJX, FFNKX, FFNLX (4) | N-CEN "Underlying fund"; 485BPOS 0000720318-26-000055: "Each fund offers its shares only to separate accounts of insurance companies…" |
| Variable Insurance Products Fund (356494) | FFNMX (1) | N-CEN "Underlying fund"; 485BPOS 0000356494-26-000030 |
| Voya Variable Products Trust (916403) | IIMOX (1) | N-CEN 0000940400-26-010020 "Underlying fund" |

The 51 reactivated funds all have a current N-CEN (filed 2025-10 to 2026-09)
without the insurance flag: VTI and 8 other Vanguard index classes, 21
Fidelity Covington ETFs, SPYI, ONEQ, 4 PIMCO Funds classes and retail funds
such as DRFAX, RDVIX, COMIX, NSRKX. Transamerica Funds' 485BPOS mentions insurance
separate accounts only for class I3 alongside funds of funds and collective
trusts, which does not make CSGTX or TLCDX insurance-only. A candidate without
a pinned N-CEN for its series (none at this snapshot) is not reactivated
(`orphan_insurance_status_unverified`).

For the owner: 70 of the 74 were last written by one `universe_sync` pass on
2026-03-30 (32 have NAV up to 2026-03-27, 37 never had NAV) and no exclusion
attribute records why.

Rule: `is_active=false`; a `funds_v` row; no active instrument of the registry
series; not a phase-B sibling; no product exclusion (`exclusion_reason`,
`strategic_excluded_reason`, `is_institutional=false`); IU ticker = registry
ticker = the single fresh SEC row of the registry series/class; Tiingo
`endDate` within 7 days of an observation at most 30 days old; a pinned N-CEN
for the series at most two years old; not insurance-only as above. Rows: 51.

Review: 60 SEC-current orphans with no Tiingo prices (FEOTX/FEITX First
Eagle Class T, HMCDX Harbor, XAOKX...: Tiingo knows the symbol but has no
prices), 9 with a product exclusion, 18 phase-B siblings of series with no
active class, 130 whose identity is not SEC-current (125 without ticker).

### 6. Active but terminated (R6)

152 active instruments (7 MV, 145 outside `funds_v`): neither ticker nor
series is in `sec_company_tickers_mf` or the 2026 dataset, the series' newest
filing is from 2020–2025, and NAV stopped more than 90 days ago (Tiingo
`success_no_new` with an old date, `empty` or `not_found`).

| ticker | series | newest NAV | newest filing for the series |
|---|---|---|---|
| PIPPX (Principal MidCap Growth) | S000007078 | 2024-12-27 | NPORT-P 0000898745-25-000535 (2025-09-24) |
| PPIMX (Principal MidCap Growth III) | S000007125 | 2025-09-23 | NPORT-P 0000898745-25-000560 (2025-09-24) |
| LLINX (Longleaf Partners International) | S000009313 | 2025-12-22 | N-14/A 0001580642-25-007492 (2025-11-28) |
| JINTX (Johnson International) | S000024217 | 2025-11-21 | NPORT-P 0000910472-25-005154 (2025-12-01) |
| PGIPX (PGIM ESG Short Duration) | S000076422 | 2025-09-23 | NPORT-P 0001752724-25-072036 (2025-03-27) |

Tickers SEC never listed (UCITS, grantor trusts such as GLD) are never
touched, and a deactivation always needs a dated NAV (newest NAV row or Tiingo
observation) older than 90 days; without one the fund is only reported
(`series_terminated_no_dated_nav`, none at this snapshot). `is_active := false` stops the daily `not_found`/`empty` fetches; no
ACTIVE fund changes (all 152 already fail the SEC gate). Six funds whose
series left SEC data but whose NAV is current (five Hodges funds, FAKDX) are
listed under `series_terminated_nav_current`.

### 7. Registry without series (R7)

20 registry rows carry a ticker but no series/class/CIK (17 created by
`backfill_benchmark_proxy_etfs`, plus AFIF, PCLO, ASMF): TIP, IWS, BIZD, IWN,
MBB, SGOV, MUB, HYG, GOVT, VTIP, IWO, ICVT, AFIF, IWP, QAI, PCLO, BIL, ASMF,
EMB, LQD. Each ticker has exactly one fresh SEC row (HYG →
S000016772/C000046846, SGOV → S000068768/C000219740, both CIK 1100663) and
no other registry row holds that class (otherwise `registry_class_taken`). R7
fills series, class and CIK; 17 of the series pass the eligibility gate, so
those funds join `funds_v` and become ACTIVE. Rows: 20 registry rows.

### 8. Ticker moved to another series (R8) and SEC self-contradictions (R9)

After A + B the audit sees six SEC integrity failures. R2 clears EASG and MMLG
(renames). The other four, plus OIODX (unmasked by R1), are funds whose ticker
SEC's ticker file now lists under a different series and registrant than the
registry. The fund's own filings decide whether that is a real move.

**Real moves (R8).** SEC's newest sync lists the ticker once, under the new
series, and a prospectus or N-PORT filing (not an N-CEN) shows the ticker
under the new class and series after the newest filing showing it under the
old class and registry series:

| ticker | registry series (old) | SEC series (CIK) | new-series filing | newest old-class filing |
|---|---|---|---|---|
| VVPLX | S000027283 | S000091565 (1936157) | NPORT-P 0001936157-26-000484 (2026-09-28); new trust via N-14 0001999371-25-006327 | N-CEN 0001049169-26-001803 (2026-07-13) |
| VVPSX | S000027284 | S000091566 (1936157) | NPORT-P 0001936157-26-000481 (2026-09-28) | N-CEN 0001049169-26-001803 (2026-07-13) |
| WINC | S000064209 | S000107974 (2137497) | 497 0001193125-26-407189 (2026-09-29) | N-CEN 0000940400-26-023423 (2026-06-12) |
| GPEOX | S000040698 | S000080366 (1965454) | NPORT-P 0000910472-26-015646 (2026-09-29) | N-CEN 0001049169-26-001803 (2026-07-13) |

R8 moves registry series, class and CIK to SEC's mapping (13 instruments: 4
in `funds_v`; also GPGOX, GPROX, GPMCX, GPIOX, SDFI, QUP, QDWN, DASX, PLABX).
`funds_v` re-keys to the new series through the eligibility gate: the five
Grandeur Peak funds (2023 trust) pass it and become ACTIVE; VVPLX, VVPSX and
WINC do not yet (the new series lack eight quarters of look-through) and leave
`funds_v` until they do. None of them was ACTIVE. 16 inactive instruments
outside `funds_v` with the same pattern are only reported
(`series_moved_inactive_instrument`).

**SEC contradicts itself (R9, review).** For STNC and OIODX SEC's ticker file
points to a predecessor series, because one N-CEN filed on 2026-07-14 by CIK
831114 (0001193125-26-302746) still lists the old series with the tickers,
while the funds' own filings say they left that trust years ago:

| ticker | registry series (fund's filings) | SEC ticker file | NAV |
|---|---|---|---|
| STNC (Hennessy Sustainable ETF) | S000079062: N-14 0000897069-22-000553 (2022-09-23) created it; NPORT-P 0001193125-26-404854 (2026-09-28) | S000070925 (Stance ETF, CIK 831114): newest prospectus filing 497 (2021-03-19), then only the N-CEN | current (2026-10-05) |
| OIODX (Orinda Income Opportunities) | S000075333: N-14 0001398344-22-003912 (2022-02-24); N-CSRS 0001398344-26-010534 (2026-06-08) | S000057227 (CIK 831114): newest prospectus 485BPOS (2017-05-01), then only the N-CEN | stops 2026-04-17 |

Moving them would point the registry at a series the fund no longer files
under, so R8 refuses (`series_moved_unproven`). They stay SEC integrity
failures until SEC corrects its ticker file or the owner decides. R9
(`--quarantine-sec-contradictions`, approved) records the disagreement as
`conflict_state.sec_series_id` (both values, both filings), so they stop at
`registry.conflict_state_not_empty` and the integrity count reaches 0; R4 keeps
that key while SEC still disagrees, and a rollback removes it. R9 only acts
when a pinned filing of the fund itself still lists the ticker under the
registry class and no newer prospectus/N-PORT lists it under SEC's class; an
unproven move without filings is only reported. OPTCX (outside
`funds_v`) shows the same pattern and is only reported.

HSPCX (Emerald Growth, registry still on HSPGX) is the same N-CEN pattern but
is not an integrity failure (it stops at `ticker.mismatch`); it is listed under
`series_reorganized`.

### 9. Review lists without a rule

* **Terminated IU class, target not priced** (5 MV / 7): Tiingo stopped
  pricing the live class too: the five Fidelity Managed Retirement funds
  (FMREX, FIRQX, FIXRX, FMRJX, FIRVX end 2026-06-08/15, likely a merger after
  the June dataset) and TGHYX (2024-12-18), TAKJX (2025-05-05).
* **IU and registry on a terminated class** (9 MV / 23): JINTX, LLINX,
  FFFVX, FHQDX, DPUAX/DPUCX/DPUIX/DPUYX, CRERX/CRECX/CRSRX and others; most
  sit in series R6 deactivates or that already fail the gate; a live class
  has to be chosen where the series lives on.
* **SEC source gap** (37 MV): the ticker is in the June 2026 dataset for the
  registry series but not in the newest `company_tickers_mf.json` sync: OBBCX (Tiingo `empty`,
  probably terminated after June), ABREX, FAMYX, SOUCX. No catalog edit can
  satisfy the gate.
* **Phase-B siblings** (512 MV): inactive share-class siblings
  (`attributes.historical_nav_ticker`) of a series whose canonical class is
  active (18 American Funds Growth Fund of America classes next to the active
  R-6). Catalog design, not an identity error.
* **No ticker** (125 MV, `nport_firm5bn_backfill`, inactive): for 93 SEC lists
  no ticker for any class of the series (variable-insurance portfolios);
  8 series have exactly one ticker-bearing class and 23 several.

### 10. Tiingo ingestion failures

The chain run logged 458 Tiingo failures (278 `not_found`, 180 `empty`) and
458 `eodhd not_configured` (no EODHD key; not an instrument signal).

| disposition | count | MV |
|---|---:|---:|
| ticker never in SEC (UCITS `.PA/.L/...`; Yahoo serves 242) | 278 | 0 |
| identity SEC-consistent; Tiingo does not carry the symbol | 106 | 53 |
| R6 deactivate | 16 | 0 |
| R2 rename (QUVU, PHB, SPVU, KBWR, XFIX, STRV, MAPP, SNPV, VLLU) | 9 | 8 |
| terminated class, R3 repoint in the later phase (Fidelity Sustainable/Freedom Blend Z6 → K6) | 5 | 5 |
| terminated class, target not priced (Fidelity Managed Retirement, review) | 5 | 5 |
| SEC source gap / not in `company_tickers_mf` | 20 | 4 |
| no registry row / other (incl. QUP, QDWN moved by R8) | 19 | 0 |

### 11. `cardinality.funds_v_missing` (2,830, outside MV)

| class | count | what it is |
|---|---:|---|
| registry series present, series fails the N-PORT eligibility gate | 1,779 | new funds (< 8 quarters), leveraged/derivative funds below 80 % look-through coverage (TQQQ), terminated series (R6 deactivates 145) |
| registry row without series | 744 | 631 UCITS (ESMA identity, outside SEC); 20 with a unique SEC row (R7); 93 without |
| no registry row | 307 | 306 without a unique SEC row (UCITS/foreign listings, closed-end funds, test rows such as Q86TST); NWXJX (series not eligible) |

Only the 17 R7 funds and the 5 Grandeur Peak funds (R8) belong in `funds_v`;
the others fail the eligibility gate by its own rules or are not SEC '40-Act
registrants.

### 12. Registry SEC identifiers

Checked for every registry row against the four datasets and the SEC sync: no
class sits in a series other than the registry series, no registry CIK
disagrees with the series' CIK, no registry CUSIP fails its checksum or maps
(via `sec_cusip_ticker_map`) to another ticker. 102 registry classes are
unknown to 2023–2026 data, all phase-B siblings of classes closed before 2023
(American Funds Class B / 529-B, converted 2017). 458 registry series are
unknown to SEC data, all `INACTIVE` instruments outside `funds_v`.

## Rule B and stale SEC rows

The `simulate.py` sketch of B matched every `sec_company_tickers_mf` row,
including rows the sync stopped refreshing, and aligned 38 registries to
classes SEC no longer lists. PR #149's B uses fresh rows only and the
generator's own SEC judge (2,490 rows), so that defect is not in the shipped
repair; all numbers above use PR #149's planner.

## Top funds gained (approved plan: default + R9, by `aum_usd`)

| ticker | fund | `aum_usd` (bn) | status after A + B | rule |
|---|---|---:|---|---|
| VTI | Vanguard Total Stock Market ETF | 2,056.6 | activity.not_active | R5 |
| VTCLX | Vanguard Tax-Managed Capital Appreciation | 9.8 | registry.conflict_state_not_empty | R4 |
| DGEFX | Brinker Capital Destinations Trust | 4.3 | isin.unsupported_prefix | R1 |
| STRV → STXF | Strive 500 ETF | 1.6 | sec.missing | R2 |
| ILCB → MLRG | iShares Morningstar U.S. Equity ETF | 1.3 | sec.missing | R2 |
| PCFAX | PIMCO RAE PLUS Small Fund | 1.2 | activity.not_active | R5 |
| PTSIX | PIMCO RAE PLUS International Fund | 1.2 | activity.not_active | R5 |
| PEDPX | PIMCO Extended Duration Fund | 1.2 | activity.not_active | R5 |
| PGOVX | PIMCO Long-Term U.S. Government Fund | 1.2 | activity.not_active | R5 |
| HBSGX | Hartford Small Cap Growth HLS Fund | 1.0 | isin.unsupported_prefix | R1 |
| MPGVX | Gallery Trust (Mondrian Global Equity Value) | 0.9 | isin.unsupported_prefix | R1 |
| DRFAX | Davis Research Fund | 0.8 | activity.not_active | R5 |

Gained: 118 (96 in MV, 22 new to `funds_v`). The later R3 phase adds 23
(e.g. the Principal R-1/R-4 classes, the Fidelity Z6 classes, and the Putnam
B classes that R5 then activates on class A).

The R7/R8 additions (HYG, MBB, SGOV, BIL, MUB, LQD, EMB, TIP, GOVT, IWN, IWO,
IWP, IWS, ICVT, BIZD, QAI, VTIP; GPEOX, GPGOX, GPROX, GPMCX, GPIOX) carry
`aum_usd = 0` on IU and do not show in this ordering. `aum_usd` on IU is
firm- or series-level for many rows, so the order is indicative.

## Repair script

`scripts/repair_fund_identity_sec_v1.py` (ledger
`schemas/fund_identity_sec_repair_v1.sql`, evidence
`contracts/fund-identity-sec/evidence_v1.json`).

* `--mode plan` (default): one `REPEATABLE READ READ ONLY` snapshot on a
  read-only session; counts per rule (all and in `funds_v`), review counts,
  the plan sha256 and the generator's ACTIVE count before/after in memory
  (that preview keeps `funds_v` rows for moved series; the eligibility gate's
  effect shows up only in the real generation). `--plan-file` writes the full
  plan.
* `--mode apply --confirm repair_fund_identity_sec_v1 --expect-plan-sha256 H`:
  takes the session advisory locks of the NAV ingestion run (900_331) and of
  the SEC ticker sync (900_309) before its snapshot exists, so neither writer
  can change the catalog or the crosswalk until COMMIT; then one read-write
  `REPEATABLE READ` transaction re-plans on its own snapshot and refuses a
  different digest; compare-and-swap on every row's before-values; a receipt
  per row (before/after of every written column, rules, evidence); re-plans on
  the written state and aborts unless it is empty; commits.
* `--mode rollback --rollback-run-id R --confirm repair_fund_identity_sec_v1`:
  restores run R's before-values byte for byte (compare-and-swap on its
  after-values) and records the rollback; one rollback per apply. A run with
  R3 repoints is refused once any NAV was written for a repointed instrument
  after the apply (the NAV would then belong to the new class), and stays
  refused: NAV attempts are append-only, so a later reverse rebase cannot be
  told apart. Such a repoint is undone forward, with a new reviewed repoint
  and its own rebase.
* `--include-class-repoint` enables R3, `--quarantine-sec-contradictions`
  enables R9. Both are part of the digest.

`instruments_universe` and `instrument_identity` have no triggers; every new
ticker is checked against all IU and registry rows (`uq_iu_ticker`). The
ledger tables are append-only (row and truncate triggers).

`scripts/collect_fund_identity_sec_evidence.py` regenerates the bundle: it
reads a plan file, derives the Tiingo tickers, (class, ticker) pairs, classes,
series and R5 (series, registrant) pairs the plan rests on, fetches the
missing ones (sec-api Query API, Form N-CEN API and full-text search, EDGAR
documents and headers with `--sec-user-agent`, Tiingo meta; keys from
`SEC_API_IO_KEY`/`TIINGO_API_KEY`), and writes a new bundle. Prospectus
search scans every 485BPOS hit across all phrases, newest first, until every
targeted series is covered by a filing header; the newest covering filing
with a restriction sentence decides. `--refresh-sec` re-fetches every
time-varying answer (newest/last filings, N-CEN, prospectus). A query that
still fails after its retries aborts the run without writing a bundle;
`--offline`
reassembles the committed bundle byte for byte from the caches. A new bundle
needs a reviewed change of `EVIDENCE_SHA256`.

CI: `tests/test_repair_fund_identity_sec_v1.py`,
`tests/test_collect_fund_identity_sec_evidence.py` and
`tests/test_repair_fund_identity_sec_v1_db.py` run in both
`workers-new-surfaces` lanes; the DB test creates and drops its own database
on the postgres lane's service (`SEC_TEST_DATABASE_URL`) and skips without
it.

## Dry run on production (before A + B)

`--mode plan --quarantine-sec-contradictions` (the approved flags) on the
live database at 2026-10-06 16:54 UTC, A and B not yet applied:

| rule | rows (all) | in `funds_v` |
|---|---:|---:|
| R1 | 6,134 | 4,684 |
| R2 | 45 | 41 |
| R4 | 2 | 2 |
| R5 | 33 | 33 |
| R6 | 152 | 7 |
| R7 | 20 | 0 |
| R8 | 13 | 4 |
| R9 | 2 | 2 |

Generator ACTIVE 2,896 → 5,120 in memory (21 insurance-only classes held
back); plan sha256
`62dc8709d2a5113cc3fd7740140f08c48b12d48f35050b5041ba0741d14241ca`.
R1 is a superset of rule A, so before A it also NULLs A's 5,285 rows; after
A + B the plan shrinks to the one simulated above. A digest is only valid for
the state it was computed on: re-run the plan after A + B and apply with that
digest.

## Apply order

Prerequisites: the SEC sync ran within 7 days; the evidence bundle is less
than 30 days old (R2, R3, R5 need its Tiingo observations; after 2026-11-05
re-collect with `collect_fund_identity_sec_evidence --refresh-tiingo
--refresh-sec` and re-pin); no NAV ingestion run or SEC ticker sync is active
(apply refuses otherwise).

1. PR #149 (A + B): its dry run, then `--apply`.
2. This repair, from `E:\tmp-deploy\api`:

   ```bash
   railway run --service risk-metrics -- uv run --no-project \
     --directory E:/investintell-datalake-workers-identity-audit \
     --with "psycopg[binary]" --with exchange_calendars==4.13.2 \
     python -m scripts.repair_fund_identity_sec_v1 \
     --sec-cache-dir E:/tmp-deploy/sec-cache \
     --db-host centerbeam.proxy.rlwy.net:36616 \
     --quarantine-sec-contradictions \
     --plan-file C:/path/outside/repo/plan.json
   # review, then the same command with
   #   --mode apply --confirm repair_fund_identity_sec_v1 --expect-plan-sha256 <plan_sha256>
   ```

   The SEC dataset files must be in `--sec-cache-dir` with the pinned sha256
   (URLs in the evidence bundle).
3. (Later phase, not now) R3 with `--include-class-repoint`, followed by the
   governed NAV rebase of the repointed instruments
   (`scripts/rebase_fund_nav_window.py --mode plan`, then `--mode apply` in
   batches of ≤ 20; ids are in the receipts with rule
   `R3_iu_class_terminated_repoint`).
4. Refresh `funds_profile_mv` (owned outside this repository) so the R7/R8
   funds join the readiness cohort and the moved ones leave it.
5. Re-pin `configs/nav_identity_audit_v3.json`
   (`structural_daily_ceiling`, `accepted_structural_delta`) from the
   generation snapshot, then generate, verify and publish the NAV policy.
6. Re-run the NAV chain: renamed and activated funds fetch from Tiingo,
   deactivated ones stop.

Rollback: `--mode rollback --rollback-run-id <run_id> --confirm repair_fund_identity_sec_v1`
with the same `railway run` prefix.

## Open items for the owner

* STNC and OIODX are quarantined by R9 (approved); ask SEC to correct the
  ticker file, or close OIODX, whose NAV stopped in April.
* Choose classes for the 23 `registry_class_terminated` funds and the 7
  terminated-class funds whose targets Tiingo stopped pricing.
* Decide whether the eligibility gate may follow a predecessor series, so
  VVPLX, VVPSX and WINC return to `funds_v` before their new series have
  eight quarters of look-through.
* Decide whether the SEC gate may fall back to the series/class dataset for
  the 37 `sec_current_source_gap` funds.
* Confirm no product reason lay behind the 2026-03-30 deactivation reversed by
  R5 for the 51 non-insurance funds.
* 106 SEC-consistent funds whose symbols Tiingo does not carry need another
  NAV provider.

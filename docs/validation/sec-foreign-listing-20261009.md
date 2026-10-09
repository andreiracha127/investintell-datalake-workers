# W1c foreign listing evidence: 2026-10-09 validation

**Archived run1 baseline.** The [run2 validation report](sec-foreign-listing-20261009-run2.md)
supersedes this report for production artifact selection after the PR #176
review fixes. Keep this report and its original artifact for comparison; use
the exact paths and hashes in the run2 report for the owner's load.

This report covers evidence collection and the dated resolver only. W1 admission,
share sizing, and the two existing refusal codes remain unchanged. Production
access for this run was read-only; no production evidence was loaded.

## Production baseline

The universe export began at **2026-10-09 01:37:50 UTC**, using `mcp_ro` against
`127.0.0.1:65432/market`, with
`PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=30000'`.
It contains every current W1 observation version with a form matching
`^(20-F|40-F|6-K)`, including amendments and historical listings.

- **1,770 CIKs; 3,036 normalized (CIK, ticker_key) lines.** The 3,053 raw ticker
  pairs include punctuation aliases.
- **12,938 foreign observations.** A separate all-versions export found zero
  retired rows at extraction time.
- The fixed universe includes debt, preferred securities, warrants, rights and
  units as well as ordinary and ADS lines. A line is not assumed to remain listed
  today merely because it appears in this historical universe.

The actual production `sec_cover_ticker_shares_at(symbol,cik,'2026-10-09',400)`
was called in 61 sequential batches of at most 50 lines:

| Production result | Lines |
| --- | ---: |
| `refused`: `foreign_issuer_listing_unverified` | 1,373 |
| `missing` | 1,536 |
| `resolved` | 64 |
| `stale` | 63 |
| Total | 3,036 |

These are observed API statuses on the extraction date across the fixed
historical universe, not a count of confirmed live listings. The Light refusal
`depositary_ratio_unsourced` is not returned by this SQL function; no production
count for that separate consumer code is claimed.

For context, 1,635 lines have an observation `ddate` (falling back to `period`,
then `filed`) within 400 days: 724 refused, 890 missing, 11 resolved and 10 stale.
This freshness subset is not a verified active-listing population.

Historical W1 availability denominators use
`available_on <= D AND (retired_on IS NULL OR retired_on > D)`. The local
all-versions export exactly reproduces the production `sec_observations_at`
counts:

| Year-end | W1-evidenced lines | CIKs |
| --- | ---: | ---: |
| 2010 | 20 | 20 |
| 2015 | 272 | 255 |
| 2020 | 964 | 741 |
| 2025 | 2,799 | 1,657 |

## Corpus and the two date clocks

The verified parent manifest contains **31,573 documents / 27,558 accessions**.
All accession filing dates are resolved: 27,556 from SEC quarterly master
indexes, one from an official daily index, and one from the same-accession W1
export. There are no unresolved dates, ambiguous dates, or index retrieval
errors in that manifest.

SEC API discovery dates are retained as `query_accepted_on`; they are not assumed
to be legal filing dates. Official-date enrichment corrected **2,882 document
dates / 2,665 accessions**. Independent verification checked 34,984 index proof
rows against 124 cached source index files, including their SHA-256 hashes,
literal row presence, accession identity and filing date: zero mismatches.

Of 8,507 accessions also present in W1, 807 original discovery dates differed
from W1's filing date. After official enrichment, five W1 dates still differ
from the official index; the evidence follows the exact SEC index rows:

| Accession | Official index date | W1 date |
| --- | --- | --- |
| `0001193125-26-132456` | 2026-03-30 | 2026-03-31 |
| `0001213900-22-048236` | 2022-08-15 | 2022-08-16 |
| `0000950123-20-006565` | 2020-06-29 | 2020-06-30 |
| `0001292814-20-002418` | 2020-06-29 | 2020-06-30 |
| `0001104659-20-096199` | 2020-08-17 | 2020-08-18 |

A corrected or republished accession can have a backdated legal filing date.
Therefore source availability is
`greatest(filed + 1, publication_floor_on)`, where the floor retains the latest
original discovery-reported publication date for that accession. Corrections to
previously loaded evidence also respect their reconciliation date. The legal
assertion/effective date remains separate from this publication clock.

The publication floor postpones **171 documents / 162 accessions**; 142 of those
accessions contain only paper documents and 20 contain non-paper documents.
None has both its legal filing date and all reported publication dates before
its accession year. Two republications would otherwise leak into the requested
2020 year-end:

| Accession | Legal filing date | Publication floor |
| --- | --- | --- |
| Vodafone `0001104659-22-116238` | 2018-06-08 | 2022-11-09 |
| Mexican Petroleum `9999999997-23-003670` | 2020-07-10 | 2023-07-26 |

An independent replay of the cached Vodafone source produced its 10/1 ratio
with `effective_from=2018-06-09` and `available_on=2022-11-09`. It cannot enter
2020 evidence. Publication delays do not promote a republished historical
assertion ahead of a newer legal assertion.

## Final local coverage and manual precision

The complete final offline replay processed **31,573 source entries / 31,294
unique source URLs**, producing **28,639 evidence rows with zero download or
parsing failures**. Initial application to fresh local PostgreSQL 16 tables
inserted all 28,639 facts and 31,573 source records, with zero retired records.
These figures and all coverage results below use the final artifact, not an
earlier exploratory replay. Production was not loaded.

Of the source entries, 30,632 parsed normally, 593 lacked verified issuer
binding, and 348 downloaded candidates were not Section 12(b) securities
descriptions. These are source dispositions, not added W1 refusal codes.

| Year-end | Both resolved / 3,036 | Type resolved | Ambiguous | None | Both among W1-evidenced lines | Both among 1,373 current refusals |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2010 | 2 | 17 | 0 | 3,034 | 2 / 20 | 1 |
| 2015 | 79 | 154 | 1 | 2,956 | 79 / 272 | 70 |
| 2020 | 497 | 736 | 32 | 2,507 | 358 / 964 | 346 |
| 2025 | 1,127 | 1,461 | 85 | 1,824 | 1,111 / 2,799 | 806 |

The 1,127 resolved 2025 lines comprise **215 ADS lines** with corroborated exact
ratios and **912 direct ordinary lines** with identity ratio 1/1. **806 of today's
1,373 SQL refusals** have resolved type/ratio evidence at that date. Evidence
coverage does not admit or size a line in W1.

[Machine-readable coverage](sec-foreign-listing-20261009-coverage.json) includes
counts and the input, output and implementation hashes.

### Case-by-case source review

**30/30 correct (100% observed sample precision): 15 ADS and 15 direct lines.**
Reviewers inspected the original cached filings, exact security/class/symbol,
listing qualifiers, ratios, and filing/publication/effective dates, and verified
original-byte hashes and official SEC index evidence.

The cohort was drawn with seed **173**, without replacement, from the fully
replayed `reviewed/` snapshot, stratified equally between ADS and direct lines.
It was then **frozen through the final source-audit corrections**. All thirty
answers were rechecked against the final database and remained resolved with
the same exact type and ratio. Every reviewed filing/index URL-and-hash pair is
present in the final manifest or its date proofs. This is measured precision in
a fixed stratified cohort, not a claim that every corpus result is correct.

The validator also generates a fresh suggested review packet on each run;
that packet is not the measured cohort. The exact cohort, selection snapshot,
per-case source checks and final answer verification are preserved in the
[manual audit](sec-foreign-listing-20261009-manual.json).

The review concerns sourced line type and ratio, not continued listing survival.
It records historical/delisted cases explicitly: for example, EXTO's 2025 NYSE
delisting disclosure and BSMX's conditional ADS-delisting disclosure. The fixed
foreign universe itself includes historical listings and non-ordinary securities.

| Symbol | Type | Ordinary-share ratio | Reviewed source |
| --- | --- | ---: | --- |
| LN | ads | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1474274/000119312516641936/d221536df6.htm) |
| VIOT | ads | 3/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1742770/000110465925094717/viot-20241231x20f.htm) |
| TUYA | ads | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1829118/000141057825000850/tuya-20241231x20f.htm) |
| WYHG | ads | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1471515/000095012724000040/f-6.htm) |
| EXTO | ads | 8/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1474274/000119380523001019/e618798_ex99-d.htm) |
| CDLR | ads | 4/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1474274/000110465923110164/tm2323833d10_f6.htm) |
| OTLY | ads | 20/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1474274/000110465925012180/tm254974d1_f6pos.htm) |
| RUHN | ads | 5/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1472033/000119380519000319/e618292_f6-ruhnn.htm) |
| LEGN | ads | 2/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1474274/000119312520155321/d930407df6a.htm) |
| HCM | ads | 5/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1648257/000141057825000377/hcm-20241231x20f.htm) |
| KB | ads | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1445930/000119380515001876/e614274_f6-kb.htm) |
| BSMX | ads | 5/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1472033/000119380522001243/e621914_f6-banco.htm) |
| ARBK | ads | 2160/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1474274/000110465925116524/tm2532323d1_ex99-a.htm) |
| CAJ | ads | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/16988/000119380523000278/e618319_f6pos-canon.htm) |
| YRD | ads | 2/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1471515/000095012715000051/a15-25_f6.htm) |
| NWTN | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1932737/000121390025052905/ea0245063-20f_nwtninc.htm) |
| PHG | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/313216/000031321625000009/phg-20241231.htm) |
| WPM | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1323404/000119312525068900/d893628d40f.htm) |
| MPU | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1953021/000101376225003608/ea0234425-20f_mega.htm) |
| YGMZ | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1782037/000121390025044189/ea0239799-20f_mingzhu.htm) |
| EFUT | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1329365/000117184316009561/f20f_042916p.htm) |
| TIRX | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1782941/000141057825000068/tirx-20241031x20f.htm) |
| IOTR | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1997637/000121390025072615/ea0252015-20fa1_iothree.htm) |
| SGHC | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1878057/000162828025016393/sghc-20241231.htm) |
| FUFU | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1921158/000121390025033733/ea0238119-20f_bitfu.htm) |
| IPCI | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1474835/000165495419002210/a20-FA.htm) |
| MTAL | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1950246/000110465925029012/mtal-20241231x20f.htm) |
| TGH | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1413159/000095017023002788/tgh-20221231.htm) |
| GOGL | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1029145/000102914525000012/gogl-20241231.htm) |
| ATY | ordinary_direct | 1/1 | [SEC filing](https://www.sec.gov/Archives/edgar/data/1861233/000117184323001562/aty20221231_40f.htm) |

### Acceptance and corrections verified on the final corpus

- **TSM:** ADS, 5 ordinary shares per ADS at 2025-12-31.
- **ZIM, QGEN, CNQ:** direct ordinary listings, identity ratio 1/1.
- **AZN:** ADS, exact fractional ratio 1/2 at 2025-12-31. Real 2015 contracts
  and announcements test the 1-to-1/2 change independently; the full corpus
  does not infer a missing 2015 cover-symbol binding.
- **ANPC:** 1/1 on 2022-10-24, 2022-10-25 and 2022-11-03; 20/1 on the announced
  effective date, 2022-11-04. Its placeholder-dated F-6 is deferred by the
  already-public matching 6-K, without rewriting stored source dates.
- **AKTX:** 100/1 on 2023-08-16; ADS type with ratio `none` on the August 17
  effective date; 2,000/1 on August 18 when the new F-6 becomes public. Later
  confirmation filings do not leak into those August answers.
- **OTLY:** its F-6 is available 2025-02-13 and explicitly effective 2025-02-18.
  The 2024 cover says ordinary shares while Item 9 identifies Nasdaq-traded ADSs
  under OTLY. Separate, type-only `listing_description` evidence preserves both
  assertions; February 12/13/17/18 queries return `ambiguous`. A later consistent
  cover resolves ADS 20/1 at 2025-12-31. No cover is silently overridden.

The [named-case checks](sec-foreign-listing-20261009-acceptance.json) and
[ratio-change checks](sec-foreign-listing-20261009-ratio-changes.json) retain
queries, answers and source provenance. Item 12.D extraction is bounded to the
actual section: TOUR's Item 9 Markets passage is no longer mislabeled Item 12.D.
The parser recognizes Depositary/Depository wording and exchange-cell ADS
qualifiers, including footnotes stating that underlying shares are not for trading.

## Focused tests

- `tests/test_sec_foreign_listing_evidence.py`: **124 passed**.
- `tests/test_sec_foreign_listing_loader.py`: **97 passed**.
- Files ran separately and sequentially with `PYTEST_WORKERS=2`, in a separate
  `w1c_tests` database. No full-suite run.
- **41 real filing fixtures**, plus focused synthetic assertions for conflict,
  visibility and reconciliation boundaries.
- The evidence CI job passed on implementation commit
  `4bc29e526bfb534f55714cf6b4e4f01989a99a94`. This does not assert that other CI
  jobs passed or that later documentation commits were checked at that time.

## Reproducibility

Baseline queries and row-level results remain in
`E:/investintell-data/w1c-20261009-baseline/`. All corpus artifacts remain under
`E:/investintell-data/w1c-20261009-run/`; the final run is its `final/` directory.
On this machine the task root is a junction to
`C:/Users/andre/Documents/Codex/w1c-20261009-run-backing/`. Original bytes and
all earlier artifacts are preserved; evidence identity is determined by hashes,
not by that local storage arrangement.

| Artifact | SHA-256 |
| --- | --- |
| `baseline/universe.json` | `06d052a96fdc8759c6d443f67fbf20ecaacac4655dd7cb306e31f558e5c5b43b` |
| `baseline/foreign_observations.json` | `a937b8cc1d7bfdfc5812c30c1a2b2a17961995d5d350554c292f7215b59797f8` |
| `baseline/current_status_rows.json` | `8091c2570436a032b7fd696becfabfc74945d86f11f3b0ab48cea6f32315b3b4` |
| `immutable discovery/date parent` | `cf16232d883ba1b0b35d63615e837ab832c286122451bb95fc0768360507a53c` |
| `final/manifest.json` | `c7bc46335e024377309ada19940400480b524559cb259f4df1004719a62481cb` |
| `final/evidence.jsonl` | `c607fd3b15be11c42a7fe649f0ed1c32422982b69a91678066a0267d32ebdd12` |
| `final/validation/coverage.json` | `b0732fe0ea2e1042b1cef201fd59c627bd3155901f8fececbfb7ec96069280f6` |
| `final/manual-cohort.json` | `1ab51a354827bb500446ff2d88ab1a109ed695718e79e196f9783730b9722af9` |

The immutable parent is retained in `final/shards/parent-<sha>.json`.
The coverage artifact pins each implementation file's hash. Final documentation
commits do not change the implementation or corpus hashes.

## Production procedure and owner questions

The disposable local PostgreSQL container and its task-owned volume were removed
after verification. All source and report artifacts remain preserved.

No production load, DDL, deployment or Railway operation was performed. The
[runbook](../runbooks/sec-foreign-listing-evidence.md#exact-production-procedure-not-executed-by-this-pr)
provides the export, collection/date verification, schema installation,
apply-only load, read-back and rollback commands for a separately authorized
production load. W1 admission/sizing remains a later PR after Workers #173.

No implementation decision remains open for this evidence-only delivery.
Source conflicts remain `ambiguous` as requested; historical type/ratio evidence
does not certify continued listing survival. Production loading and admission
integration are intentionally deferred to the owner-approved later work.

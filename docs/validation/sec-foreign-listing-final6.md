Historical report; superseded by [final7](sec-foreign-listing-final7.md).
# W1c B1: final6 confirmation detector validation

This report supersedes final5 for the next reviewed artifact load. The detector
requires evidence that the ratio change itself completed, or that the relevant
approval actually passed. Admission and market sizing remain separate work.
All collection, source checks and database validation were offline or local;
production was not accessed.

## Implementation and regression evidence

Parser version: `foreign-listing-v10`, SHA-256
`a3adecae5d80d8bcb0c2c4f8dd02ac63674a0f0786d77bebc0ae6700107f2587`.
For B1, the schema was unchanged from the applied final5 schema:
`f334d08d3d3b496bd613495d59ee2a3a12365f58c422b3d51bd77530e61d2957`.
SQL settlement still requires a different later accession, publication by the
query date, and compatible program, exact class and ratio. No migration change
or new production operation was part of B1. B1b adds the v2 restatement migration
and verifies final6 applied over the loaded final5, as measured below.

| Required fix | Regression |
|---|---|
| Completion's direct object is the ratio change | `test_b1_regate_invalid_completion_or_approval_cannot_confirm[b1_preparations_completion]`; preparation, filing, notice, plan and compound-budget variants in `test_b1_completion_requires_the_ratio_change_as_its_completed_object` |
| Confirmation binds to program, class and target ratio | `test_b1_regate_invalid_completion_or_approval_cannot_confirm[b1_different_class_confirmation]`; named/lowercase programs, American versus Global receipts, compact/reversed ratios and delayed class statements in `test_b1_confirmation_cannot_inherit_another_program_ratio_or_preparatory_notice` |
| Approval's direct object is the ratio change or consolidation | `test_b1_regate_invalid_completion_or_approval_cannot_confirm[b1_budget_approval]`; direct approval, consolidation-budget and adopted-subdivision-resolution variants |
| Numeric financial statements never supply confirmation | `test_b1_regate_numeric_financial_table_never_confirms_but_narrative_footnote_does`; clean narrative extraction after an unlabelled numeric table |

The four synthetic HTML fixtures are `b1_preparations_completion.html`,
`b1_different_class_confirmation.html`, `b1_budget_approval.html` and
`b1_financial_table_confirmation.html` under
`tests/fixtures/sec_foreign_listing_evidence/`. All four required regressions
failed when their `parse_filing` reference was replaced with the parser read
from `git show 1572834d:scripts/sec_foreign_listing_parser.py` (4 failed,
0 passed), and pass with v10. The repeatable check and result are at
`C:/investintell-data/w1c-final6-work/check_parser_only.py` and
`parser-checks.json`.

All confirmation paths were reviewed together: completion, past effectiveness,
Depositary statements, same-day taken-effect statements, consolidation approvals
and adopted subdivision resolutions. Guards inspect the full owning sentence
for identity, accounting assumptions and hypothetical language. Stored proofs
exclude numeric table prefixes and trailing table cells. A rejected table or
former-ratio reference cannot manufacture an outstanding condition. Direct
conditions remain pending even if the approving shareholders have a different
voting class.

`test_b1_regate_false_confirmation_keeps_conditional_ratio_ambiguous_in_sql`
checks the conditional 20/1 announcement and pending F-6 through the false later
sources. `test_b1_genuine_anpc_completion_still_settles_after_publication` uses
the real ANPC cover, original F-6, amendment and completion: ambiguous through
2022-12-16, resolved ADS 20/1 from 2022-12-17. Direct completed/effected,
changed-effective and became-effective forms remain accepted. Independent
adversarial review passed 32/32 checks; source-hash-verified ANPC, ANTE, TCOM,
DQ and TAL fixtures retained their genuine confirmations.
Source-based regression cases preserve direct forms such as
"effected a ratio change so that", "effected a change to a new ratio" and
"effected a change of the ADS to Class A ordinary share ratio", as well as
explicit current-side ratios, completed footnotes and linked split dates.
The final independent reviewer passed 64/64 checks, including five coordinated-object and five relative-clause boundaries, and verified 19/19 actual source probes and 3/3 accounting
distinctions: a completed ratio event followed by an EPS adjustment keeps its
clean legal narrative, while a financial assumption supplies no confirmation.

## Artifact identities and offline replay

Final6 directory: `C:/investintell-data/w1c-final6/`.

| Artifact | SHA-256 |
|---|---|
| `manifest.json` | `0c6e6d776c2df22c7738ac6ce748ceee65539204fbedaf92179eaa700fbe53a1` |
| `evidence.jsonl` | `2636dbb882829faf23167f34b88decfe097e3bbd67130ea5803d3cb53355044a` |
| `SHA256SUMS` | `6275e4aa4718500e89d2a3c24993fa5274ad3b96228dee2b11013e5aaffe47ec` |

Final6 contains 47,329 source entries and 29,655
evidence rows, with zero failed sources and complete discovery/parse flags.
The raw documents were read in place through `--raw-cache-dir`; the original
cache and its `documents` junction were neither copied nor written.

Pinned inputs:

- Raw cache: `E:/investintell-data/w1c-20261009-run2/final5`.
- Final5 manifest: `793cc435a00d41309a5b1b72724eb940ae43b8c86b97e2d7412d5fe0439a5646`.
- Final5 evidence: `9ca17573dd649db4075a7eb40065274e7e0b1d536649b553c4b790c3dfc1accc`.
- Universe: `E:/investintell-data/w1c-20261009-baseline/universe.json`, byte SHA-256 `06d052a96fdc8759c6d443f67fbf20ecaacac4655dd7cb306e31f558e5c5b43b`.
- Collector observations: `E:/investintell-data/w1c-20261009-baseline/foreign_observations.json`; canonical JSON SHA-256 `69dbccf9710ecda27132a4eb7389ac3bb2ed8b3ec4810fd9481b8650cae8f73a`.
- Coverage uses the saved `foreign_observations_all_versions.json` and `current_status_rows.json`; it performs no fresh export or discovery.

Replay command, executed by the telemetry wrapper at
`C:/investintell-data/w1c-final6-work/replay_final6.py`:

```powershell
python scripts/load_sec_foreign_listing_evidence.py `
  --offline `
  --universe E:/investintell-data/w1c-20261009-baseline/universe.json `
  --manifest E:/investintell-data/w1c-20261009-run2/final5/manifest.json `
  --raw-cache-dir E:/investintell-data/w1c-20261009-run2/final5 `
  --cache-dir C:/investintell-data/w1c-final6 `
  --output C:/investintell-data/w1c-final6/evidence.jsonl `
  --observations E:/investintell-data/w1c-20261009-baseline/foreign_observations.json `
  --workers 16
```

Measured replay wall time: **531.51 seconds (8 minutes 51.51 seconds)**. Admitted workers: **16**. Peak simultaneous process-tree RSS: **4,622,258,176 bytes (4.305 GiB)**. Peak process count: 26, including launchers and PDF tools. Timed interval is child launch through exit; input pin checks precede it.

The wrapper blocks socket connections in the parent and spawned workers. It
restricts parsing to 20 of the 24 logical CPUs and records simultaneous process
tree RSS once per second. The memory policy was 256 MiB per worker with a
2 GiB reserve; the requested and admitted worker count was 16. Python 3.12.12,
pypdf 6.20.0 and Git-bundled pdftotext were used. Command, resource samples,
stdout/stderr and measured wall/RAM are preserved in
`C:/investintell-data/w1c-final6-work/`.

## Confirmation audit

Final5 has 408 confirmed rows and 178 distinct confirmation strings. All 288
distinct original source documents behind those rows passed offline checksum
verification. The complete audit is at
`C:/investintell-data/w1c-final6-audit/final5-confirmation-audit.json`.

There are **18 invalid numeric financial-statement confirmation rows** in
final5: OTLY (CIK 1843586) 13, NetEase (1110646) 2, ASLN (1722926) 1,
LGHL (1806524) 1, and a fair-value/warrant table (1743340) 1. Their proofs
contain EPS, loss/income, finance-cost or fair-value table cells. The full
source-checked list and literal proofs are at
`final5-invalid-financial-confirmations.json` in that audit directory.

Three further proofs mix legitimate narrative completion with an EPS heading
(CIK 1269238) or trailing market-price cells (1485538, two rows). Five binding
findings include three NetEase former 25/1 rows whose completion changes to
5/1, one SaverOne former 1200/1 row whose completion changes to 3600/1, and
one BIDU null-class row inheriting a Class A confirmation. Two of those five
overlap the 18 numeric financial rows.

Final confirmations: **408 → 415**, comprising **30 unsupported confirmations removed, 37 genuine completions newly recognized, 172 preserved proofs cleaned/revised and 206 unchanged**. The 30 removal reasons are 18 assumed accounting mentions, 9 numeric table mentions outside legal completion, 1 unknown-class mismatch and 2 former-ratio mismatches. All 30 quotes were verified across 28 original documents; all 37 new proofs were source-checked (33 contiguous and 4 composed with both clauses verified). The original 18 financial contaminations become 9 discarded observations and 9 clean narrative confirmations. Final proof contamination and genuine completion loss are both zero. All 30 removals are listed below; the count is below 40. Full individual quotes and reasons: `C:/investintell-data/w1c-final6-audit/final-removed-confirmations-reviewed.json`.

| Final5 line | CIK | Accession | Ratio/class | Reason | Source |
|---:|---:|---|---|---|---|
|47|1780531|0001104659-25-024790|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465925024790/tm259628d2_ex99-1.htm)|
|175|1843586|0001193125-25-256284|20/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1843586/000119312525256284/otly_6k_3q25.htm)|
|734|1843586|0001843586-26-000012|20/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1843586/000184358626000012/otly-ex99_1.htm)|
|2499|1110646|0001104659-21-027746|25/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1110646/000110465921027746/a21-7816_1ex99d1.htm)|
|4050|1843586|0000950170-25-061038|20/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1843586/000095017025061038/otly_6k_1q25.htm)|
|4051|1843586|0000950170-25-061038|20/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1843586/000095017025061038/otly_6k_1q25.htm)|
|8499|1329099|0000950123-10-067501|1/10;unknown|named_class_confirmation_for_unknown_class|[SEC source](https://www.sec.gov/Archives/edgar/data/1329099/000095012310067501/c03699exv99w1.htm)|
|8694|1816007|0001193125-24-225400|2/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1816007/000119312524225400/d855645dex991.htm)|
|9071|1780531|0001104659-23-092183|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465923092183/tm2323915d2_ex99-1.htm)|
|9683|1780531|0001104659-25-038307|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465925038307/tm2513082d1_ex99-1.htm)|
|12028|1816007|0001193125-26-197190|2/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1816007/000119312526197190/d128745dex995.pdf)|
|12730|1843586|0000950170-25-097884|20/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1843586/000095017025097884/otly_6k_2q25.htm)|
|13739|1780531|0001104659-23-097916|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465923097916/tm2325125d1_ex99-1.htm)|
|16152|1843586|0001193125-25-213996|20/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1843586/000119312525213996/otly-20250630.htm)|
|16357|1843586|0001193125-26-046546|20/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1843586/000119312526046546/otly_year-end_report_202.htm)|
|17321|1780531|0001104659-24-050313|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465924050313/tm2412296d1_ex99-1.htm)|
|17938|1894693|0001213900-25-082099|1200/1;unknown|former_ratio_cannot_receive_target_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1894693/000121390025082099/ea025457401ex99-2_saverone.htm)|
|19509|1780531|0001104659-24-090297|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465924090297/tm2421892d1_ex99-1.htm)|
|20605|1780531|0001104659-23-031512|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465923031512/tm239322d2_ex99-1.htm)|
|21632|1816007|0001193125-26-197190|2/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1816007/000119312526197190/d128745dex994.pdf)|
|21770|1816007|0001193125-24-106955|2/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1816007/000119312524106955/d816076dex992.pdf)|
|22209|1780531|0001104659-24-035296|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465924035296/tm249069d2_ex99-1.htm)|
|22624|1780531|0001104659-24-091796|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465924091796/tm2422343d1_ex99-1.htm)|
|23221|1780531|0001104659-23-048663|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465923048663/tm2313309d1_ex99-1.htm)|
|23261|1110646|0001104659-20-127089|25/1;unknown|former_ratio_cannot_receive_target_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1110646/000110465920127089/a20-36341_1ex99d1.htm)|
|23262|1110646|0001104659-20-127089|25/1;unknown|numeric_financial_table_ratio_outside_completion|[SEC source](https://www.sec.gov/Archives/edgar/data/1110646/000110465920127089/a20-36341_1ex99d1.htm)|
|24623|1780531|0001104659-25-081163|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465925081163/tm2524047d1_ex99-1.htm)|
|25667|1816007|0001193125-24-204850|2/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1816007/000119312524204850/d887168dex991.htm)|
|25798|1816007|0001193125-24-073261|2/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1816007/000119312524073261/d813232dex991.htm)|
|27106|1780531|0001104659-25-083309|30/1;unknown|assumed_earliest_period_accounting_ratio|[SEC source](https://www.sec.gov/Archives/edgar/data/1780531/000110465925083309/tm2524458d1_ex99-1.htm)|

Each row’s complete original proof, owning/source rejection quote, exact source hash, source URL and individual rationale are in `final-removed-confirmations-reviewed.json`. New confirmations and exact source/component quotes are in `final-new-confirmations-reviewed.json`. Detailed extraction comparison is in `confirmation-comparison.json`.

The rejected candidate1/candidate2 reviews are preserved separately. Their hashes, counts and verdicts are not used for this final review. SQL changed-answer/source review, acceptance/precision and the complete required tests are recorded by the root validation run.


## Changed answers and source review

The same 3,036 lines were queried at 2010, 2015, 2020 and 2025 year-ends,
plus the same 17 acceptance cases. Cohorts overlap those year-end queries;
there are 12,155 distinct line/date queries. Final5 and final6 were each
initially loaded into separate databases in one task-owned disposable PG18
container for B1. B1b additionally reconciles final6 onto final5 after v2 DDL;
that production-sequence measurement follows below.

Exactly **2/12,155** semantic answers change, both at the 2025 year-end. Both changes are explicitly source-justified and return no numeric ratio. All nine raw-source checksums passed; all admitted facts and quoted witnesses were public before D. A later 2026 issuer-wide audit document is excluded from the 2025 answers. VSA remains resolved 5/1.

| Line | Date | Final5 | Final6 v3 | Verdict |
| --- | --- | --- | --- | --- |
| TRIB / CIK 888721 | 2025-12-31 | ambiguous; no ratio | none; no ratio | Accepted, source-justified |
| SVRE / CIK 1894693 | 2025-12-31 | ambiguous; no ratio | none; no ratio | Accepted, source-justified |

**TRIB**: The newly extracted narrative confirms an actual ADS ratio change from four to twenty ordinary shares per ADS, effective February 23, 2024. It supplies the genuine completed event needed to retire the stale null-class 1/1 registration from 2004. The remaining 20/1 evidence does not satisfy the unchanged exact current-registration/class binding required for a resolved ratio. The listed ADS line remains established; none refers to absence of an admissible ratio and supplies no market-size input. No new pending control was created.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/888721/000117891325003264/exhibit_99-1.htm). Filed 2025-09-08; governed public availability 2025-09-09, before D = 2025-12-31. Raw-source SHA256: `b0ff3ed4f51d5d04ecbeaba11f7fe76671c2db37e904e69db0f79b214fecdde4`.

> we effected an ADS Ratio Change on February 23, 2024

> one ADS representing 20 ordinary shares

**SVRE**: The June 11, 2025 source effects a transition from the former 1200/1 ratio to the new 3600/1 ratio. Final5 incorrectly attached the same completion to both values. Final6 removes the false former-side 1200/1 confirmation and retains the genuine 3600/1 completion. The old March 2025 cover describes the pre-change 1200/1 regime; the unchanged resolver lacks admissible independent current corroboration for the new ratio at the query date. Retiring the old regime therefore changes ambiguous to none without resolving a numeric ratio. The 2026 issuer-wide control document is not admitted at this 2025 query date. No new pending control was created.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/1894693/000121390025082099/ea025457401ex99-2_saverone.htm). Filed 2025-08-29; governed public availability 2025-08-30, before D = 2025-12-31. Raw-source SHA256: `8e5ae9996cd4846c74f5d26a6adce35f5a34d2e99a47b713d07ee449eb1097de`.

> On June 11, 2025, the Company effected the change

> one (1) ADS representing three thousand six hundred (3,600) Ordinary Shares

VSA retains its baseline 5/1 answer and is absent from the final changed-answer set. These conclusions are pinned to the current parser, manifest and evidence identities above.


Coverage and every before/after answer, source fact and changed-source packet
are at `C:/investintell-data/w1c-final6/validation/`. The reusable capture and
comparison helper is `scripts/validate_sec_foreign_listing_final6.py`; it
requires an explicit disposable local database, verifies the exact loaded
manifest/facts, preserves the frozen cohorts and emits all semantic changes.

## B1b: v2 applied over the loaded final5

The project's restatement rule is now implemented in
`schemas/sec_foreign_listing_evidence_v2.sql`. Same-byte parser corrections
make the old reading invisible at every date and inherit its prior availability;
source changes remain prospective. The base SQL remains byte-identical to the
production v1. Both facts and source metadata already store parser versions.

| Governed SQL | SHA-256 |
|---|---|
| v1 base | `f334d08d3d3b496bd613495d59ee2a3a12365f58c422b3d51bd77530e61d2957` |
| v2 | `e17d0523885bc67cc3495e1c9dda1405084f02027c2a6cfc135d5e1e0d5f1a7c` |
| v2 rollback | `136a5baa8b75591d1564ce9dbd5749e13e90b9cb738c63d30b2d4fd071e23c9f` |

The local replay used one task-owned `timescale/timescaledb:2.27.2-pg18`
container, PostgreSQL 18.4, limited to two CPUs and 3 GB RAM. It installed v1,
applied final5 with the main loader in `w1c-prod` and `--observed-on 2026-10-09`,
applied v2 DDL, then applied final6 with this branch's loader and
`--observed-on 2026-10-10`. Neither application reparsed or fetched sources.
The main checkout was `2905146afd69c4c27cc0e71c34f13ab383d000e9`; its loader
SHA-256 was `d56c54172645b656363d968c9653bdf39fee745a0a48d9682bd27f19db7a5762`.

All **12,155/12,155** queries in the immutable final6 snapshot match its isolated
final6 semantic answer, with **zero mismatches**. Exactly two historical answers
change from the measured v1/final5 baseline: TRIB (888721) and SVRE (1894693)
at 2025-12-31, both `ambiguous` to `none`. Both retain resolved ADS listings and
return no ratio. Their source quotes and explanations remain the ones above.
This fixes the v1 probe's zero historical changes without changing the resolver's
ratio-admission rules.

| Measurement | After v1/final5 | After v2/final6 |
|---|---:|---:|
| Sources | 47,329 | 47,329 |
| Total fact versions | 29,709 | 59,364 |
| Active fact versions | 29,709 | 29,655 |
| Retired: `parser_correction` | 0 | 29,709 |
| Retired: `source` | 0 | 0 |
| Retired: NULL | 0 | 0 |

The final6 application reports 29,655 inserted, 29,709 retired and zero unchanged
facts; parser-version hashes re-version the whole set. Every one of the 47,329
source-package hashes agrees between the two manifests. Every active fact's
`available_on` equals its governed `source_available_on`, rather than the replay
date. The retained readings record `foreign-listing-v8`; active readings record
`foreign-listing-v10`. Before/after hashes confirm neither manifest, JSONL,
snapshot nor universe changed.

The two later queries also match a separately initialized v1/final6 database
loaded with the main loader in the same container:

| Line at 2026-10-11 | Applied v2/final6 | Isolated final6 |
|---|---|---|
| TRIB / 888721 | `ambiguous`; ADS listing resolved, ratio ambiguous, no ratio | Same |
| SVRE / 1894693 | `none`; ADS listing resolved, ratio none, no ratio | Same |

The reported expectation that isolated TRIB would return `none` at this later
date is not reproduced with these exact pinned inputs. Its 2026 F-6 contains
both assertions below, which become public on 2026-01-28 and are excluded at
2025-12-31. The unchanged resolver preserves their exact-ratio conflict, so
both load sequences return `ambiguous` after publication. This is source
evidence, independent of load history.

> Each American Depositary Share shall represent one Share

> At the date hereof, each American Depositary Share represents twenty shares

Source: [Trinity F-6 deposit agreement](https://www.sec.gov/Archives/edgar/data/890836/000101915526000024/trinityda.htm),
accession `0001019155-26-000024`, filed 2026-01-27; original raw SHA-256
`22d345b973c116eb5215f070d983ebc684234902a94f7761684d12c783538809` was verified
offline. The two final6 facts carry ratios 1/1 and 20/1 on that same accession.

The complete result and future witness packet are
`C:/investintell-data/w1c-b1b-work/report.json` and
`future-trib-source-witnesses.json`. The reproducible local sequence is
`C:/investintell-data/w1c-b1b-work/restate_acceptance.py` (`preload`, then `replay`),
with all new artifacts on C:. The input caches remain read-only.

## Acceptance, precision and tests

**17/17 acceptance, 30/30 frozen precision and 10/10 changed precision passed**, with no resampling. Final cohort provenance verifies **186/186** supporting source hashes across the same 113 originals; all source/class/ratio/date/text identities are retained from final5. Only OTLY loses five financial extractions. All contiguous/composed quote components and W1 annotations remain verified. AMBR 5/1 on 2025-12-31, FRLN 15/1 on 2023-06-01 and 2025-12-31, and ANPC ambiguous on 2022-12-16 then 20/1 on 2022-12-17 remain unchanged. Year-end resolved counts are 2 / 83 / 513 / 1,151; ambiguous counts are 0 / 1 / 26 / 79; 819 of the saved current refusals remain resolved in 2025.

The acceptance oracle is the final5 report's `final5_answer` for each of its
17 cases. Its older `expected` field is a final4 oracle and is not used to
undo the accepted ANPC/AKTX pending-state corrections. The frozen30 and
changed10 cohorts are the exact saved reviewed cases, without resampling.

The preserved 40-line precision provenance packet checks 191/191 supporting
source hashes against 113 original documents, read in place. It verifies all
meaningful components of composed excerpts and the appended same-accession
W1 annotations. This preserves the original reviewed source evidence; it does
not claim a new independent manual precision study. The packet is at
`C:/investintell-data/w1c-final6-work/validation/precision-source-check/precision-sources.json`.

`timescale/timescaledb:2.27.2-pg18`, PostgreSQL 18.4. One file at a time,
`PYTEST_WORKERS=2`, plugin autoload disabled, test artifacts on C:

| Suite | Result |
|---|---|
| `tests/test_sec_foreign_listing_evidence.py` | 405 passed, 0 failed/errors/skipped |
| `tests/test_sec_foreign_listing_loader.py` | 138 passed, 0 failed/errors/skipped |

The evidence suite includes schema idempotence and the detector-to-resolver
regressions. Since schema bytes are unchanged from `f334d08d`, B1 does not
introduce a migration or require a new changed-schema rollback cycle.
Focused Ruff and whitespace/LF checks passed. Logs are under
`C:/investintell-data/w1c-final6-work/`; the task-owned PG18 container was
removed after validation. The unrelated pre-existing containers were preserved.

The [runbook](../runbooks/sec-foreign-listing-evidence.md) targets final6 with
its exact artifact and v2 schema pins. Final6 has not been loaded into
production by B1 or B1b. The PRs are not merged; no Railway changes were made.

B1b ran the same two files sequentially with `PYTEST_WORKERS=2` and plugin
autoload disabled, using the one disposable PG18 container above:

| B1b suite | Result |
|---|---|
| `tests/test_sec_foreign_listing_evidence.py` | 421 passed, 0 failed/errors/skipped |
| `tests/test_sec_foreign_listing_loader.py` | 153 passed, 0 failed/errors/skipped |

The migration cycle covers loaded v1 -> v2 -> idempotent v2 -> rollback -> v2,
including no table rewrite and exact v1 resolver bytes, comments, owners,
privileges and behavior after rollback. As W1b does, rollback retains reason
data and its CHECK as audit; the loader rejects the restored v1 resolver.
Unit and SQL regressions cover source versus parser retirement, removed and
added confirmations, publication floors, inherited republication availability,
empty parses, new-document protection and eight downstream resolver branches.
An existing idempotence test initially left the shared connection running v1;
it now reapplies base plus v2 and checks the resolver definition. Loader tests
also install both governed files independently.
Focused Ruff and whitespace/LF checks passed. The test logs and acceptance
artifacts are under `C:/investintell-data/w1c-b1b-work/`.
The runbook's read-only 12,155-query check was also executed against the applied
local database and passed with zero mismatches. The task-owned PG18 container
was removed after validation; unrelated existing containers were preserved.

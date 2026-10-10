## Round 2 — gate BLOCK corrections

Date: 2026-10-10. This section supersedes Round 1 completeness and impact figures; production remains untouched.

PR: https://github.com/andreiracha127/investintell-datalake-workers/pull/188
Pinned gate baseline: `9610294308d7d1d6d6b3afe0a1847b6f5c92fdb5`. Same branch/worktree; no rebase, force-push, production operation, merge or GitHub thread reply.

### Correction and structure

The parser now extracts a literal candidate separately from deterministic `check_reading(span_text, cover_region_text, candidate)`. The checker independently verifies source declarations and ignores proposed completeness, refusal flags, evidence snippets and claimed structural proof. Candidate extraction does not certify its own output. Trusted same-filing period metadata remains separate from proposed counts.

The whole cover retains notes after checkmark questions through the source-evidenced next section. Count/class-linked footnotes and qualifying notes refuse; unrelated interactive-data questions, exhibits, financial amounts and body text cannot qualify the census. Explicit document-wide share-count unit adjustments remain quoted with their locations and refuse. Counts are never netted, expanded, repaired from W1 or assigned to unnamed classes.

W1c table cells and source PDF lines retain row/cell boundaries. Blank counts cannot borrow the next row, multiple numeric cells refuse, recognized headings bind column roles, and presentation padding and continued par/date clauses preserve valid readings. Bare/unrecognized class tokens remain incomplete. Dash-like or parenthesized quantities and scaled share quantities refuse. Every accepted custom/standard declaration participates in disagreement checks; equal declarations remain eligible. Founder and Founders use the same proof predicate.

No SQL, migration, rollback, API, sizing contract or W1c evidence apply behavior changed. The census loader preserves PDF lines and publishes full cover/qualification evidence. Parser version is `foreign-share-census-v2`.

### Reproductions and regression evidence

On the immutable baseline, **37 regression tests failed**: 33 false-complete readings and 4 missing exact refusal reasons. The baseline parser and failure XML/log are retained under `baseline-regressions/`. The immutable P3 probe also confirms Founder gives one proof while Founders gives zero despite equal complete counts; corrected tests give identical behavior.

The seven gate families are covered, with additional tests for header-column roles, Unicode notation, unknown class cells and externally supplied proposals. Real source fixtures correct SAP and check ASX, preserve unqualified controls, and retain share-unit qualifications.

Independent in-memory review of the final parser bytes also passed all 36 previously reported parser reproductions and 14 forged-proposal cases. Four adjacency cases refused in both parser and checker: direct treasury/combined count qualifiers stayed blocking beside unrelated ADS listing ratios or hydrocarbon text. This probe evidence is separate from pytest totals and corpus sample precision.

### Artifact and coverage

All **19,009** pinned primary annual filings replayed offline in **238.579 seconds**, with **12** workers, at most 24 pending tasks, zero network requests and zero source gaps. Implementation hashes before/after replay match. Inputs and previous Round 1 artifacts remain hash-identical; no cache document or old artifact file was changed.

Final census SHA256: `636053c6af3b7eab51ce0f0ec12c069ff55352fc9d85e332305442dad6fe4ea4`.

| Status | Round 1 | Round 2 |
| --- | --- | --- |
| complete | 3968 | 3809 |
| incomplete | 13167 | 13420 |
| conflicting | 666 | 580 |
| none | 1208 | 1200 |

#### Filing-year coverage

| Year | Filings | Complete | Incomplete | Conflicting | None |
| --- | --- | --- | --- | --- | --- |
| 1996 | 6 | 0 | 0 | 0 | 6 |
| 1997 | 5 | 0 | 0 | 0 | 5 |
| 1998 | 17 | 0 | 0 | 0 | 17 |
| 1999 | 27 | 0 | 0 | 0 | 27 |
| 2000 | 40 | 0 | 22 | 0 | 18 |
| 2001 | 68 | 4 | 58 | 0 | 6 |
| 2002 | 328 | 10 | 99 | 0 | 219 |
| 2003 | 344 | 45 | 272 | 0 | 27 |
| 2004 | 380 | 46 | 307 | 0 | 27 |
| 2005 | 471 | 77 | 365 | 0 | 29 |
| 2006 | 463 | 69 | 373 | 0 | 21 |
| 2007 | 499 | 72 | 405 | 0 | 22 |
| 2008 | 520 | 83 | 408 | 0 | 29 |
| 2009 | 573 | 105 | 432 | 1 | 35 |
| 2010 | 600 | 103 | 458 | 3 | 36 |
| 2011 | 674 | 133 | 497 | 6 | 38 |
| 2012 | 755 | 174 | 515 | 24 | 42 |
| 2013 | 717 | 159 | 497 | 18 | 43 |
| 2014 | 658 | 130 | 476 | 15 | 37 |
| 2015 | 691 | 148 | 495 | 16 | 32 |
| 2016 | 690 | 144 | 496 | 15 | 35 |
| 2017 | 734 | 159 | 524 | 21 | 30 |
| 2018 | 865 | 190 | 591 | 46 | 38 |
| 2019 | 866 | 195 | 581 | 46 | 44 |
| 2020 | 896 | 212 | 594 | 49 | 41 |
| 2021 | 1013 | 231 | 691 | 51 | 40 |
| 2022 | 1168 | 265 | 811 | 46 | 46 |
| 2023 | 1210 | 270 | 843 | 44 | 53 |
| 2024 | 1234 | 265 | 866 | 53 | 50 |
| 2025 | 1290 | 274 | 905 | 57 | 54 |
| 2026 | 1207 | 246 | 839 | 69 | 53 |

#### Status flip matrix

Full transition matrix (all filings; mutually exclusive status priority remains conflicting, complete, incomplete, none):

```json
{
  "complete": {
    "complete": 3796,
    "incomplete": 165,
    "conflicting": 7,
    "none": 0,
    "missing": 0
  },
  "incomplete": {
    "complete": 13,
    "incomplete": 13154,
    "conflicting": 0,
    "none": 0,
    "missing": 0
  },
  "conflicting": {
    "complete": 0,
    "incomplete": 93,
    "conflicting": 573,
    "none": 0,
    "missing": 0
  },
  "none": {
    "complete": 0,
    "incomplete": 8,
    "conflicting": 0,
    "none": 1200,
    "missing": 0
  },
  "missing": {
    "complete": 0,
    "incomplete": 0,
    "conflicting": 0,
    "none": 0,
    "missing": 0
  }
}
```

#### COMPLETE → INCOMPLETE reasons

Reason counts overlap where one source has several refusals:

```json
{
  "footnoted_or_qualified_count": 141,
  "unsupported_sign_notation": 22,
  "class_count_unresolved": 3,
  "count_class_row_mismatch": 3,
  "candidate_source_mismatch": 3,
  "unparsed_numeric_residue": 2,
  "inconsistent_table_columns": 1,
  "ambiguous_numeric_cells": 1
}
```

### Source reviews

Final flipped-source review: **32/32 correct, 0 incorrect**. The deterministic packet represents every observed COMPLETE→INCOMPLETE reason and includes the exact SAP/ASX controls. Each source was decompressed and SHA-verified in place, then read for the statement, row association, actual cover boundary and referenced notes. Full quotes and judgments are in `validation/flipped-reviewed.json`.

The initial source review found 25 false losses among 32 reviewed flips. That unaccepted census and explicit incorrect judgments remain under `trial-source-review-9dc690bd/`; the parser was corrected and the complete corpus regenerated. The 25 valid sources are restored with exact original classes/counts/dates, while all seven required refusals remain. These failed judgments were not silently recertified.

A second unaccepted replay was also reviewed before publication: 21 of 32 selected refusals met the contract, while 11 falsely attached layout padding, financial-statement-only unit notes, listing receipt definitions, hydrocarbon production terminology or debt-index descriptions to the capital census. Those judgments and source quotes remain under `trial-source-review-8431a360/`. The final replay restores the valid readings while preserving actual marked or qualified capital counts.

The next review found two additional explicit section limits: “Consolidated Audited Financial Statements” and MD&A together with the audited consolidated financial statements. These valid cover counts were restored by recognizing the named section scope, while all earlier 64 source controls stayed unchanged. The rejected artifact and explicit failed judgments remain under `trial-source-review-c55c884f/`; hash-verified source controls are in `last-section-controls.json`.

BNS `9631 / 0000909567-04-000093` remains incomplete under the strict row/header contract: a blank “Preferred Shares” row precedes shifted, indented Series 11 and Series 12 rows. The original parser joined the preferred heading across rows and left Series 12 kind untyped. This is an unsupported hierarchy/type association, not a claim that the disclosed arithmetic is wrong; no preferred-kind or missing-count inference was added. The limitation and source regression are documented.

| CIK | Accession | New reasons | Source judgment |
| --- | --- | --- | --- |
| 1061894 | 0001061894-05-000011 | ambiguous_numeric_cells, class_count_unresolved, count_class_row_mismatch, unparsed_numeric_residue | Physical HTML has Common Shares in one TR and 29,698,706 in a separate TR. The quantity is readable to a person, but assigning across rows violates the required hard-row rule; no borrowing is permitted. |
| 1472072 | 0001193125-12-182261 | candidate_source_mismatch, footnoted_or_qualified_count | 142,353,532 Ordinary Shares* is expressly marked. Its late cover note states that it includes 949,935 repurchased ordinary shares in ADS form pending cancellation. Refusal preserves the original quantity without subtraction. |
| 1931717 | 0001213900-26-079169 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share and per share amounts have been retroactively adjusted to reflect the share consolidation. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 9631 | 0000909567-04-000093 | class_count_unresolved, count_class_row_mismatch, inconsistent_table_columns, unparsed_numeric_residue | Unsupported hierarchical table: Common Shares/count occupy one row; Preferred Shares occupies a blank-count row; indented Series 11/count and Series 12/count occupy later rows in shifted columns. Old reading borrowed the blank preferred heading into Series 11 and left Series 12 as untyped other. Arithmetic is not wrong; exact class/kind association needs hierarchy inference beyond the hard-row contract, so incomplete is appropriate. |
| 1131383 | 0000909567-07-000532 | unsupported_sign_notation | Source literally reads December 31, 2006 – 45,257,451 Common Shares. The explicit user rule refuses any dash-like quantity notation even when it resembles a date delimiter. This is a syntax refusal; it does not assert a negative financial quantity. |
| 715153 | 0001193125-21-196757 | footnoted_or_qualified_count | Common Stock quantity has **** and date header has ***. Later cover notes exclude BIP Trust shares and state that common stock includes 70,044,953 represented by ADSs. Marked count is incomplete under contract. |
| 1403849 | 0001047469-15-004169 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: all share and per share data have been adjusted to reflect a 10-for-1 share split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1815436 | 0001213900-23-081853 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: all references in this Annual Report to share and per share data have been adjusted, including historical data which has been retroactively adjusted, to give effect to these reverse stock splits. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1122411 | 0001193125-22-087341 | footnoted_or_qualified_count | Common Shares quantity has **. Later cover note gives 4,410,443,432 as of January 31, 2022 following employee options, alongside December 2021 count. Count-marker refusal is appropriate without selecting/subtracting another date. |
| 932787 | 0001021231-05-000232 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share data have been adjusted for the 3-for-1 stock split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1596964 | 0001193125-16-747468 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share and per share data have been restated to reflect this share split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1205059 | 0001062993-18-000144 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All common share amounts in this annual report on Form 20-F reflect the reverse split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1337117 | 0001178913-09-001214 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: all share and per share amounts for all periods presented have been retroactively restated to give effect to this share split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1342068 | 0001144204-16-096913 | footnoted_or_qualified_count | Ordinary Shares quantity has **. Later cover note qualifies ADS-underlying share quantities with multiple counts; the marked census quantity must refuse. |
| 1000184 | 0001193125-13-120941 | footnoted_or_qualified_count | Ordinary Shares quantity has **. Later cover note says Including 36,334,516 treasury shares. Marked quantity must refuse without subtracting treasury shares. |
| 1687542 | 0001493152-19-006824 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share and per share amounts for all periods presented herein have been adjusted to reflect the split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1792030 | 0001178913-26-001877 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share numbers in this Annual Report reflect the July 2024 Consolidation. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1419723 | 0001193125-08-143872 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share numbers reflect the 1:10,000 share split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1653606 | 0001493152-20-011093 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share amounts have been adjusted for the split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1595353 | 0001493152-24-013203 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share and per share data has been retroactively adjusted to reflect the 1 for 15 reverse share split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1747661 | 0001213900-22-072178 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share amounts have been retroactively restated to reflect increase in authorized shares and shares reverse split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1005516 | 0001178913-06-001167 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share and per share numbers herein reflect adjustments resulting from this reverse stock split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1489147 | 0001193125-16-615485 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share and per share amounts in this Annual Report have been retroactively adjusted to reflect the share consolidation. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1715497 | 0001140361-20-013837 | footnoted_or_qualified_count | Borr 2019 original cover names 110,818,351 common shares at December 31, 2019. After checklists and the contents list, the presentation conventions explicitly say all Share and per Share data in this annual report is adjusted to give effect to our Reverse Share Split and is approximate due to rounding. This scope includes the cover and differs from the corrected 2023 financial-statements-only declaration. The qualified quantity must remain incomplete. |
| 761238 | 0001178913-11-000991 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: All share and per share amounts have been restated for all prior periods to reflect a one share for three shares reverse stock split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1172494 | 0000950103-08-001511 | footnoted_or_qualified_count | Common Shares quantity has **. Late cover note states March 31, 2008 quantity 7,868,206,737 after exercise of employee options. The marked December census count must refuse. |
| 2010499 | 0001493152-26-037877 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: all share and per-share data (including outstanding shares, options, warrants, and earnings per share) presented in this Annual Report on Form 20-F have been retroactively restated for all periods presented to reflect the execution of the Share Consolidation. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1291855 | 0001144204-09-035100 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: all share and per share amounts for all periods presented (including numbers of options, warrants and convertible bonds) have been retroactively restated to give effect to this reverse split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1962738 | 0001558370-25-004086 | footnoted_or_qualified_count | Source class names, quantities and date were checked against the verified original. The report has a broad, unscoped or report-wide all-share adjustment declaration: all share numbers in this report have been adjusted to reflect the Reverse Stock Split. It does not expressly confine the adjustment to a named financial-statement section. Incomplete is the agreed conservative qualified-count refusal; the value is never fixed. |
| 1833835 | 0001193125-26-088095 | footnoted_or_qualified_count | Paysafe original cover names 51,676,354 common shares, with December 31, 2025 fiscal period. The original body note says Except as otherwise provided herein, all share and per-share amounts of our common stock, equity awards, warrants and other outstanding equity rights have been adjusted to give effect to the Reverse Stock Split for all periods presented. The declaration itself is broad and has no express named-section limitation; the agreed conservative contract treats it as a qualified count, despite its placement near financial statements. The value is not corrected. |
| 1000184 | 0001104659-25-017815 | footnoted_or_qualified_count | SAP count 1,228,504,232 has **; actual late cover note states Including 61,914,771 treasury shares. Incomplete is correct, with no treasury subtraction. |
| 1122411 | 0001193125-25-064603 | footnoted_or_qualified_count | ASX count 4,414,930,537 has **; late cover note states 4,416,485,537 outstanding at January 31, 2025 following employee options. Marked count must refuse; the later quantity never replaces the cover count. |

#### Named issuer source statements after correction

| Issuer / CIK | Accession | Status | Literal source quote | Review |
| --- | --- | --- | --- | --- |
| DLO / 1846832 | 0000950170-25-058197 | incomplete | 285,475,136 Class A and Class B common shares, as of December 31, 2024 | Cover gives only a combined A+B total; per-class allocations absent. |
| BIDU / 1329099 | 0001193125-25-066199 | complete | 2,239,234,372 Class A ordinary shares and 524,340,320 Class B ordinary shares, par value US$0.000000625 per share, as of December 31, 2024. | All stated named-class counts and measurement date match the cached source; cross-checks match. |
| NVO / 353278 | 0001628280-25-003920 | complete | A shares, nominal value DKK 0.10 each: 1,074,872,000 B shares, nominal value DKK 0.10 each: 3,390,128,000 | Both A/B counts match source exactly; ordinary/common kind is unverified in bare-share names and W1 members remain unbound. |
| TSM / 1046179 | 0001193125-25-083423 | complete | As of December 31, 2024, 25,932,733,242 Common Shares, par value NT$10 each were outstanding. | All stated named-class counts and measurement date match the cached source; cross-checks match. |
| ASML / 937966 | 0000937966-25-000009 | complete | 393,283,720 Ordinary Shares (nominal value €0.09 per share) | All stated named-class counts and measurement date match the cached source; cross-checks match. |
| QGEN / 1015820 | 0001015820-25-000027 | complete | The number of outstanding Common Shares as of December 31, 2024 was 222,290,848 . | All stated named-class counts and measurement date match the cached source; cross-checks match. |
| ZIM / 1654126 | 0001178913-25-000793 | incomplete | 120,423,333 . | Cover count has no class name; numeric residue refuses completeness. |
| NTES / 1110646 | 0001410578-25-000728 | complete | 3,167,959,016 ordinary shares, par value US$0.0001 per share. | All stated named-class counts and measurement date match the cached source; cross-checks match. |
| SAP / 1000184 | 0001104659-25-017815 | incomplete | Ordinary Shares, without nominal value: 1,228,504,232 (as of December 31, 2024)** | Exact original count is marked ** and actual below-checkmark note states Including 61,914,771 treasury shares. Round2 correctly changes completeness to incomplete without subtraction; undimensioned W1 equality does not remove this qualifier. |
| CNQ / 1017413 | 0001017413-25-000024 | complete | 2,102,996,000 Common Shares outstanding as of December 31, 2024 | All stated named-class counts and measurement date match the cached source; cross-checks match. |

Final precision sample: **40/40 correct, 0 incorrect**, new seed `workers-pr188-r2-independent-source-v1`. Deterministic form/year strata, distinct CIKs, and exclusion of the preceding 40 and independent gate sample. Original source bytes, count/class associations, dates and whole-cover notes were reviewed. This reports sample precision, not an unmeasured corpus-wide guarantee.

The final 40 selections and readings are identical to the preceding reviewed Round 2 packet. All 40 actual-source judgments transferred only after exact accession, source SHA, quote, class name/kind/count, date and eligibility checks. For the final flip packet, 30 identical judgments transferred and two replacement originals were newly read. Transfer counts and provenance are explicit in the reviewed JSON packets; an artifact hash alone never transfers a judgment.

| # | CIK | Accession | Date | Classes/counts | Judgment |
| --- | --- | --- | --- | --- | --- |
| 1 | 1725964 | 0001193125-19-053855 | 2018-12-31 | Common Shares=608,535,477 | correct |
| 2 | 1172494 | 0001193125-05-139047 | 2004-12-31 | Common Shares=4,958,040,897 | correct |
| 3 | 1119774 | 0001178913-22-000863 | 2021-12-31 | Ordinary Shares=86,433,432 | correct |
| 4 | 1895597 | 0001193125-23-123984 | 2022-12-31 | ordinary shares=117,647,000 | correct |
| 5 | 1430725 | 0001193125-09-137683 | 2008-12-31 | Class A Common Shares=33,968,361; Class B Common Shares=7,405,956; Class C Common Shares=12,375,000 | correct |
| 6 | 793628 | 0001553350-13-000092 | 2012-12-31 | common shares=24,910,916 | correct |
| 7 | 1698535 | 0001193125-25-045899 | 2024-12-31 | common shares=569,088,514 | correct |
| 8 | 886986 | 0000950142-07-000757 | 2006-12-31 | CLASS A COMMON SHARES=4,673,453; CLASS B SUBORDINATE VOTING SHARES=211,153,069 | correct |
| 9 | 1530238 | 0001144204-15-056922 | 2014-12-31 | Class A common shares=706,173,568; Class B common shares=427,352,696 | correct |
| 10 | 1401395 | 0001204459-07-001804 | 2007-05-31 | Common Shares=36,729,547 | correct |
| 11 | 1097362 | 0000909567-03-000662 | 2002-12-31 | common shares=618,367,009 | correct |
| 12 | 1596964 | 0001144204-18-054407 | 2018-06-30 | ordinary shares=412,450,256 | correct |
| 13 | 9631 | 0001193125-25-304643 | 2025-10-31 | Common Shares=1,236,305,738 | correct |
| 14 | 1126874 | 0001047469-14-001311 | 2013-12-31 | Common Shares=202,757,689 | correct |
| 15 | 943003 | 0000950123-11-022520 | 2010-12-31 | Common Shares=157,665,210 | correct |
| 16 | 1061574 | 0000908737-05-000903 | 2005-09-30 | Class A Subordinate Shares=397,448,329; Class B Shares=33,772,168 | correct |
| 17 | 1128173 | 0001193125-04-220808 | 2004-06-30 | common stock=727,682,259 | correct |
| 18 | 1419723 | 0001193125-09-095764 | 2008-12-31 | Ordinary shares=166,831,943 | correct |
| 19 | 1666175 | 0001104659-17-012880 | 2016-12-31 | Common Shares=401,486,414 | correct |
| 20 | 1085921 | 0001062993-19-001412 | 2018-10-31 | Common Shares=259,602,699 | correct |
| 21 | 1616212 | 0001062993-16-008235 | 2015-12-31 | common shares=7,796,137 | correct |
| 22 | 2809 | 0001047469-05-015472 | 2003-12-31 | Common Shares=84,469,804 | correct |
| 23 | 1768946 | 0001104659-23-008032 | 2021-12-31 | Ordinary shares=135,953,657 | correct |
| 24 | 1733868 | 0001213900-23-032494 | 2022-12-31 | ordinary shares=1,371,643,240 | correct |
| 25 | 1167379 | 0001167379-09-000035 | 2008-12-31 | Common Shares=298,648,353 | correct |
| 26 | 1339688 | 0001062993-13-001517 | 2012-12-31 | common shares=162,990,836 | correct |
| 27 | 1540013 | 0001104659-24-042121 | 2023-09-30 | common shares=35,605,280 | correct |
| 28 | 1157806 | 0001047469-06-002148 | 2005-12-31 | common shares=859,253,318 | correct |
| 29 | 1528752 | 0001193125-13-232294 | 2012-12-31 | Class A ordinary shares=141,457,419 | correct |
| 30 | 1001085 | 0000909567-07-000581 | 2006-12-31 | Class A Limited Voting Shares=387,669,896; Class B Limited Voting Shares=85,120 | correct |
| 31 | 733099 | 0000950123-05-008528 | 2004-12-31 | Class A Voting shares=56,235,394; Class B Non-Voting shares=218,979,074 | correct |
| 32 | 1733298 | 0001564590-20-018042 | 2019-12-31 | Class A ordinary shares=39,645,820; Class B ordinary shares=32,937,193 | correct |
| 33 | 1174169 | 0001174169-25-000020 | 2024-12-31 | Common Shares=767,343,863 | correct |
| 34 | 1158041 | 0001193125-13-441197 | 2012-12-31 | Common Shares=1,005,535,530 | correct |
| 35 | 1061894 | 0001062993-11-004843 | 2011-10-02 | Common Shares=121,330,544 | correct |
| 36 | 756894 | 0000909567-05-000696 | 2004-12-31 | Common Shares=533,575,185 | correct |
| 37 | 352960 | 0001047469-04-014108 | 2003-12-31 | Class A shares=640,000; Class B shares=350,000 | correct |
| 38 | 1412558 | 0001193125-09-146885 | 2008-12-31 | ordinary shares=108,838,715 | correct |
| 39 | 1321847 | 0001176256-18-000071 | 2017-12-31 | common shares=233,536,671 | correct |
| 40 | 1740594 | 0000950103-19-005905 | 2018-12-31 | Class A common shares=22,602,737; Class B common shares=27,658,290 | correct |

Newly complete transitions are independently source-reviewed in `validation/new-complete-reviewed.json`. NONE→COMPLETE is also checked explicitly; no body statement is promoted as a cover census.

### Cross-checks and #186 impact

| W1 check | Comparisons | Match | Mismatch | Missing count | Unbound |
| --- | --- | --- | --- | --- | --- |
| undimensioned | 8413 | 3825 | 495 | 4093 | 0 |
| class_dimensioned | 6172 | 3083 | 99 | 431 | 2559 |

Using the same hash-pinned corrected local #186 cohort: **222** candidates among **2031** scope-refusing year-end-2025 lines; **117** also have resolved W1c and non-stale prior counts. These are prospective proof opportunities; no sizing API or admission was activated.

| Category/subset | Lines |
| --- | --- |
| Single ordinary class | 222 |
| Explicit line class count | 0 |
| Same W1 count date | 217 |
| W1c resolved and non-stale | 117 |

SAP and ASX are no longer counted. SAP preserves its literal 1,228,504,232 but its cover note states “Including 61,914,771 treasury shares.” ASX preserves 4,414,930,537 and the later employee-option count/date note; the marked count refuses per contract. No subtraction, date alignment or count correction was inferred.

| Control | Accession | Census status | Proof category/reason |
| --- | --- | --- | --- |
| SAP | 0001104659-25-017815 | incomplete | None / census_incomplete |
| ASX | 0001193125-25-064603 | incomplete | None / census_incomplete |

### Tests, hashes and operational boundary

| File | Passed | Elapsed seconds |
| --- | --- | --- |
| tests/test_sec_foreign_listing_evidence.py | 952 | 10.567 |
| tests/test_sec_foreign_listing_throughput.py | 22 | 1.785 |
| tests/test_sec_foreign_listing_shard_plans.py | 13 | 1.377 |
| tests/test_sec_provider_transport.py | 14 | 5.153 |
| tests/test_sec_foreign_share_census_validation.py | 12 | 0.86 |
| tests/test_sec_foreign_share_census.py | 34 | 1.634 |
| tests/test_sec_foreign_share_census_loader.py | 68 | 2.297 |
| tests/test_sec_foreign_listing_loader.py | 156 | 4.98 |
| tests/test_sec_foreign_share_census_parser.py | 102 | 0.957 |
| tests/test_sec_foreign_share_census_reading.py | 104 | 0.968 |

**1477 passed**, files run sequentially with `PYTEST_WORKERS=2` on the sole task-owned `timescale/timescaledb:2.27.2-pg18` container. Migration cycle, bitemporality, grants, inlining, preservation, actual PDF extraction and malformed artifacts remain covered. The container and anonymous volumes were removed; absence is verified in `container-cleanup.json`.

| Artifact | SHA256 |
| --- | --- |
| manifest.json | b10a468279f14068c91b93c1b7ef897457119a704b96491b529756ba32d5a384 |
| census.jsonl | 636053c6af3b7eab51ce0f0ec12c069ff55352fc9d85e332305442dad6fe4ea4 |
| run-metrics.json | 039500231188b20ca0863f51d7fccc6f7ac06285296bc8fb80873f7ed9936be6 |
| input-hashes.json | 56ae316479b360d0d75edb2760d7e6f1751bac3fd06defd72d0949268251111c |
| w1-counts.json | a6b6b65719f1f4480d128673c82ca31272e00dd2dd6ddb8bcfcdfe3ab397f339 |
| sizing-2025.json | 7a2e26a5a9604a51c59256bb529970631e17107de7fad6633e96c0049d71603c |
| replay-start-implementation-hashes.json | 9bead8f4f6b13a3be7264097278fb7532601de8bb91aed8ec264dc9efda69104 |
| replay-integrity.json | 2f7c0a1766f20c19056e54e9944089460d5e94e7c974f33306ad675794145b4f |
| test-results.json | c8f6571bdd6ff510110e4e853e6139f57519be007253b4ae0521ee56844b99ef |
| container-cleanup.json | b35815e12b626ef48fded7bd8c276cced1f31bd7207739dbdd3df502ee9b5f72 |
| baseline-regressions/gate-regressions-baseline-summary.json | 6da82d336c8a483d95f7a6038877c5bfc8e4657d4399326e80425a838ba3857c |
| baseline-regressions/founder-spelling-baseline.json | b2e05774d8420b6e8b0b0afc2e68200f017f2e32566eaa923e0e9522f9c5cc21 |
| validation/validation.json | 31f1c467563740ad5e2f61ab78df247ee24c7d92812763118b38b4ef4db18d31 |
| validation/status-comparison.json | 757218d4fc433aad5f8137a152e62b71b0d630b6392ede666b53b9c562c75f6d |
| validation/precision-reviewed.json | 5417977ce98f97565a56f9c89e91e78fd4ff53fcf8e40c823941c224596ccdd1 |
| validation/flipped-reviewed.json | 3fbcdef4ccd8247b4f4eb2029cd9aa88253135843d2e7b426224ebed294ed659 |
| validation/new-complete-reviewed.json | 8b7646aea571809c7f857bcd1db67191defbacef1e6cb99d37487a217c8877d2 |
| validation/sizing-impact.json | 466a334da14d91b64400ce6beb7ddd1779fe92201d992c3f97d55fc4dd454b61 |
| validation/impact-controls.json | 1dbf214bb9cd4d176142e2a81f338bb56aaf4b31de97cf2ded63bcce9f434fec |
| validation/source-restoration-review.json | ce97fbded6fdae331197a229459a9e698260a3b8da07d47bdd72eeda18e93fe8 |
| validation/named-reviewed.json | 0782dfe6c26c1b71b2ed6328da786ea3cd0a2df8ca1e6ff0cfdc83844143b00f |
| validation/review-summary.json | c1e725629cef6e9880fc6a7a47c809537a0be9caedb009bff5a7168df4c453f2 |
| validation/validation-artifact-hashes.json | e5ebd6d8e4e2765d91ff2fd55649bb7053b7042cd561bcb3226364dd7a1e67c1 |
| prior-artifact-preservation.json | 44e4eed15464ecbd55e93824dc36887736502558207fd75a79b19b6020c73aef |
| parser-source-fix-check.json | 4be296af7fc76739293c51087f31b96b8ac836ad9a36c45f4e17394d4e537906 |
| parser-final-scope-fix-check.json | 2f299df45a563679ffdeba11a5d8076c3a1f1fffc5c082e6b11e0c7838034a1f |
| global-unit-eight-controls.json | 0ac60533422a148535c8088f8f9292b68baf36fd0053a066968cf0c775364013 |
| last-section-controls.json | 89989afb4f2fd2464ef05657638dc58eab97619042b30f01fb7438828498b6ff |
| independent-parser-review.json | b8945e65b17bf47d5526fd615b20f88e40c4ec51642066a20f2d61fcce5cf332 |
| baseline-regressions/gate-regressions-final.xml | c9265f8deb512eb3f0d7b13dbc2f3102d119ea7930b752a34e79fe5390ea38eb |
| baseline-regressions/gate-regressions-final.log | 92ae47808ad88fd54f6aaba06dc1e50ed2f734d068e82392c5f428bd103a537e |
| baseline-regressions/gate-regressions-v2.xml | 60d9804f5533a0311be9cb5861798a6dd990e3e409c15b3f3a0dafe28f3a44be |
| baseline-regressions/gate-regressions-v2.log | 483501778e0b008322c6942fd1c7360ff7ea851b020d8009e86ee7bea8f389ee |
| trial-source-review-9dc690bd/census.jsonl | 9dc690bd25a3781ab24be18ea7e8687c54c463d83a35740b92a57552d0d4d4d9 |
| trial-source-review-9dc690bd/validation/flipped-reviewed-before-source-fix.json | 017c185c7d8e216c9eb3ed15180dd6436bfc8ed97b9d5fbd134d1d86f77fc44d |
| trial-source-review-8431a360/census.jsonl | 8431a3602e30b022c7198ccbd1b4c62242d3918055edd4528c53a63647254d29 |
| trial-source-review-8431a360/validation/flipped-reviewed-before-final-scope-fix.json | 71bf4c2b42826099b32b192df91f5929ae0180f71a0e1802bd557377496716c0 |
| trial-source-review-c55c884f/census.jsonl | c55c884f51700843b55c8cf9b66a1beb7057a4e7bfd109c1904915f4d2a4de52 |
| trial-source-review-c55c884f/validation/flipped-reviewed-before-last-section-scope-fix.json | 0a98328fcbb4ca9076a6372887d7f16fd3b9591f52b039d64399465eb6955bf4 |

`SHA256SUMS` SHA256: `4057a61d239dd2c6147dda7b1d607bd8817e290f7e397e812900a75689d49584`.

| Changed file | SHA256 |
| --- | --- |
| .github/workflows/sec-foreign-listing-evidence.yml | b496564a209d307dc2e8619f087141de87929650b33699d724432ccae4608189 |
| docs/runbooks/sec-foreign-share-census.md | 83298222c3e0d6b32b7cdcb0cec867778d6605e425457c2940d1e857a02cb56b |
| scripts/load_sec_foreign_share_census.py | 8fcb6ed5fa756f877666f6678a98b78abeab552850e390ed7cf9933001a8a01b |
| scripts/sec_foreign_share_census_parser.py | da9e0985224e4458c0597f9e368523ec670959900722851eb976d23ac6d02f65 |
| scripts/validate_sec_foreign_share_census.py | ac691d9520d0a8efdc7eaef43a5a2da1ebc0a6b60e4136b9c75d41278d434371 |
| tests/fixtures/sec_foreign_share_census/named_cover_statements.json | 060528ba2b474b893b28e62fba87d4afb38611ca28c0a8561b6b4000a9f9a30c |
| tests/test_sec_foreign_share_census_parser.py | e323948781fccedc4c0354cdaca227535948735ca4fb5275560efb90b17643df |
| tests/test_sec_foreign_share_census_reading.py | 60066a17f5c4ce5e5b2a5b745da31cde6445e5b029967e73c3b384b434ea0a62 |
| tests/test_sec_foreign_share_census_validation.py | 32be4fa8dcff826b561113f8b2929793136d6152bac4d5fc54e35eb3a083eb5c |

All changed repository files are LF and the diff excludes `tests/fixtures/open_macro_v4/*`; its existing worktree modifications remain untouched. Current GitHub CI is reported separately from local PG18 evidence.

Production procedure remains deferred: first pass the new gate and pin accepted head/artifacts; then in a separately authorized window install the additive census DDL, publish the accepted artifact under the shared lock, and complete read-only readback before sizing v2 rollout. Follow `docs/runbooks/sec-foreign-share-census.md`; do not discover or reparse during publication, never log the writer DSN, and keep consumer rollback ordering explicit. No production action or GitHub thread reply occurred in Round 2.

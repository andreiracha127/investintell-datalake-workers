# B2 phase 2: foreign share-class census

Date: 2026-10-10. Status: implemented and locally validated; production procedure deferred.

Branch: `feat/sec-foreign-share-census`; created from `origin/main` at `d8a2aae564aedf69655a5b1b37a87fe09dffc439`.

No production writes, deployment, Railway changes or merge occurred. Local database results below use one task-owned PG18 container and existing read-only W1 exports.

## Design and consumer boundary

Workers parses the mandatory annual-cover capital-stock response into exact original class names, normalized Class/Series/Roman/ordinary/common keys, kinds, counts, date, stated total and computed sum. It reuses W1c HTML/PDF extraction. The listing table and undimensioned dei total never supply a missing class name or allocation.

Completeness requires every named class count, a supported date, no unexplained numeric or class/text residue and exact equality with a stated total. Combined A+B amounts, unnamed numbers, generic aggregate totals, alternatives/conditions and unsupported grammar remain incomplete. Nil is zero; preferred and deferred/founder classes are retained. A stated-total inconsistency or same-filing W1 mismatch is conflicting. Cross-checks preserve both values and dates and never repair them. Known W1 classes absent from an otherwise exhaustive census also invalidate its scope.

The additive `public.sec_foreign_share_census` stores fact identity, classes, source accession/URL/SHA256/text/location, parser version, statement/period dates, cross-checks and W1c v2 bitemporality. `sec_foreign_share_census_sources` retains zero-census reconciliation metadata. The PIT API is `public.sec_foreign_share_census_at(bigint,date)`, one fully qualified SQL STABLE statement without function-level SET. It elects the latest visible statement before exposing status; incomplete/conflicting readings cannot cause fallback to an older good reading. Unknown statement dates order by period end or filing date and stay incomplete.

Source changes remain prospective; same-byte parser corrections remove old readings at every historical date and inherit the replaced availability. New same-byte additions and zero-fact replacements follow W1c v2 rules. DDL/apply share lock `(79311,173)`. Owner is `worker_writer`; PUBLIC is revoked and the three reader roles get SELECT/EXECUTE only. Existing W1c apply is unchanged; explicit `--census-manifest` + `--census` opt into census reconciliation in the same transaction.

**Sizing is unchanged.** No edit was made to #186 or `sec_cover_ticker_size_basis_at`. A later sizing v2 must consume this evidence and still prove ordinary kind/units, line binding, price admission, currency, actions, age and duplicate-class identity.

## Corpus, artifacts and run metrics

Pinned raw manifest SHA256: `793cc435a00d41309a5b1b72724eb940ae43b8c86b97e2d7412d5fe0439a5646`. Final5 was read in place through `--raw-cache-dir`, without discovery or downloads. Denominator: 19,009 primary 20-F/20-F/A/40-F/40-F/A filings from the 1,770-CIK pinned universe; 743 annual-form exhibits are excluded. Every primary source decompressed and matched its manifest SHA256. No raw-cache gap was found.

Final replay: **192.522 seconds**, **12 parse workers**, at most 24 queued tasks, 0 network requests and 0 source gaps. New artifacts are under `C:/investintell-data/w1c-census/`. PDF extraction is covered by actual PDF-byte tests; this corpus had no primary PDF URL.

Coverage: **3,968 complete; 13,167 incomplete; 666 conflicting; 1,208 none**. Conflict has priority, so these categories are mutually exclusive.

| Artifact | SHA256 |
| --- | --- |
| manifest.json | 567339931850d5020e6fb9ff80a922d83b36c23afe72e47b3607caf8da93f2c9 |
| census.jsonl | f564c87698f29b9f8da27f3e79a5aae03900acd1c829a210a31ca51afaa14f53 |
| run-metrics.json | 1c3eddb144da26f932e8d30738741d6b3e81998082c375a8c58c66a512909d71 |
| w1-counts.json | a6b6b65719f1f4480d128673c82ca31272e00dd2dd6ddb8bcfcdfe3ab397f339 |
| w1-observations.json | 508f227f300844232b5be6b178a42f81f9f523917ca69ae0de22d6da7cea691b |
| sizing-2025.json | 7a2e26a5a9604a51c59256bb529970631e17107de7fad6633e96c0049d71603c |
| sizing-2025-metrics.json | a4f46a9a517d3a1d06b1083a0e5f6c593466a3abb09cfeca459ec9861ee20041 |
| local-measurement-inputs.json | 2bed329ae661eaac6e5099fa6e7552e7af2a378e5dfbf024ec69ba4b58f9f4b3 |
| local-measurement-install.json | b86e80222a3f0d82c2b47e57fb808595de2ed49ade817586cb1dc5bf1d542a47 |
| local-census-readback.json | 821d5211c269a10d090bc5bbf6ddbcfd02e282b0844b26e3fdd949577299aa39 |
| local-census-explain.json | 59c7af0032f476d054ca715c8e1780418ef6e58e414719753f1e72f67bbea49c |
| test-results.json | e528b0a64a12f6f9c8d3a84d94bff579c8eae8aea56600805d7596837c4a3c48 |
| container-cleanup.json | b7a3f5ffbe9737713bdb67d018d6be76617c93bf679b0127ee50f8c5f96bf0fe |
| validation/validation.json | 0269774988bc1444f74a4cd5027e9210bbe993b932c300dd677c8a7603cb83a0 |
| validation/named-reviewed.json | cb366d32539a5d863570e9141b3b85eca4d442b5fa81a7447713ea491b9f9098 |
| validation/named-source-review.json | c06e25a79ddc798fbf129e1d8af806c91729afa7de99426bb2ed40c6af902e38 |
| validation/precision-reviewed.json | 17541b0f0192184e7bbeb4ddf9e8a6ed497a61be3981ddcf341c912922a3ce6e |
| validation/precision-found-defect.json | ddb297e2835b57d4a33429b196ad20c29d441d426fc536c81b0fd506daf4fcaa |
| validation/sizing-impact.json | ee33f39c8306a7995d721d79587b700434942b90c32472b8fd763937e97fa9b3 |
| validation/incomplete-reasons.json | 8bad2a89a8a739617200a2ea7d61f671f072f8bc8d24db205b486a504c9690d8 |

`SHA256SUMS` SHA256: `b4dfff31973c28c4cf0cfdbdb206973cb2997aac0aa9dfc3e4424d9422e42ea4`. The full changed-file digest map is `changed-file-hashes.json`.

### Implementation SHA256s

| File | SHA256 |
| --- | --- |
| .github/workflows/sec-foreign-listing-evidence.yml | 136b17d5ef9f0d215118064e69e9cd0803fb9d1099df50aab3a014f0e5c8304d |
| docs/runbooks/sec-foreign-listing-evidence.md | 2206f029b36e8a33b7fa8d568469f68ede69ea2651cf3f527d82210d657380d0 |
| docs/runbooks/sec-foreign-share-census.md | 8dbe3ba13da3156f718a358c841d6178ba1dfbf934295b14c7630530ac8ba8a0 |
| schemas/sec_foreign_share_census_v1.rollback.sql | ee703f0860e473912d3387ce288bf57bf06e5a32f47ede2a4b645808fc36b636 |
| schemas/sec_foreign_share_census_v1.sql | da3120f7e030e4fe815e392d8fb026e381b41490735c367504c8cc9f3e4d675f |
| scripts/load_sec_foreign_listing_evidence.py | 6b85546e1f874cc8acfc8a0515f704d1fcddf8ccfd73460270c30a19554dd55e |
| scripts/load_sec_foreign_share_census.py | d9ba6436d32b46843a0619751f322aab84b1874678602e9a6d659fb7423463d3 |
| scripts/sec_foreign_share_census_parser.py | 725159d2267bc264720e4d535c1e63878f2f6292552d47185c54016e6a61b78c |
| scripts/validate_sec_foreign_share_census.py | 4cbd4f24849ddb4c5bb78ace99ce1a0a2747b031dda784c45e7a456abda3f0e8 |
| tests/fixtures/sec_foreign_share_census/named_cover_statements.json | 4f592985c7d60fb8c01f3b98ef00c6f31113cd261df18787480bc9aaf57f0d18 |
| tests/test_sec_foreign_share_census.py | fe8caaf7bb51bac2f4fbe2d1f6cade683a36db383899215a75612027e6c83033 |
| tests/test_sec_foreign_share_census_loader.py | 4ee5628b828b765a45277df6c6d7cd020e38bac5b5d67ad17c7fce1c8b19a3bc |
| tests/test_sec_foreign_share_census_parser.py | 90bea1f6e831e6b9db83cccbb83e2191050addbf83f372bda56653809cf58f2f |

## Named issuers and source quotes

| Issuer | CIK | Accession | Status | Class counts | Date | Cover statement response | W1 cross-check |
|---|---:|---|---|---|---|---|---|
| DLO | 1846832 | 0000950170-25-058197 | incomplete | Class A: unallocated; Class B common shares: unallocated | 2024-12-31 | [285,475,136 Class A and Class B common shares, as of December 31, 2024](https://www.sec.gov/Archives/edgar/data/1846832/000095017025058197/dlo-20241231.htm) | {'match': 1} |
| BIDU | 1329099 | 0001193125-25-066199 | complete | Class A ordinary shares: 2,239,234,372; Class B ordinary shares: 524,340,320 | 2024-12-31 | [2,239,234,372 Class A ordinary shares and 524,340,320 Class B ordinary shares, par value US$0.000000625 per share, as of December 31, 2024.](https://www.sec.gov/Archives/edgar/data/1329099/000119312525066199/d853848d20f.htm) | {'match': 2} |
| NVO | 353278 | 0001628280-25-003920 | complete | A shares: 1,074,872,000; B shares: 3,390,128,000 | 2024-12-31 | [A shares, nominal value DKK 0.10 each: 1,074,872,000 B shares, nominal value DKK 0.10 each: 3,390,128,000](https://www.sec.gov/Archives/edgar/data/353278/000162828025003920/nvo-20241231.htm) | {'unbound': 2} |
| TSM | 1046179 | 0001193125-25-083423 | complete | Common Shares: 25,932,733,242 | 2024-12-31 | [As of December 31, 2024, 25,932,733,242 Common Shares, par value NT$10 each were outstanding.](https://www.sec.gov/Archives/edgar/data/1046179/000119312525083423/d896993d20f.htm) | {'match': 1} |
| ASML | 937966 | 0000937966-25-000009 | complete | Ordinary Shares: 393,283,720 | 2024-12-31 | [393,283,720 Ordinary Shares (nominal value €0.09 per share)](https://www.sec.gov/Archives/edgar/data/937966/000093796625000009/asml-20241231.htm) | {'match': 1} |
| QGEN | 1015820 | 0001015820-25-000027 | complete | Common Shares: 222,290,848 | 2024-12-31 | [The number of outstanding Common Shares as of December 31, 2024 was 222,290,848 .](https://www.sec.gov/Archives/edgar/data/1015820/000101582025000027/qgen-20241231.htm) | {'match': 1} |
| ZIM | 1654126 | 0001178913-25-000793 | incomplete | unnamed amount | 2024-12-31 | [120,423,333 .](https://www.sec.gov/Archives/edgar/data/1654126/000117891325000793/zk2532795.htm) | {'census_count_missing': 1} |
| NTES | 1110646 | 0001410578-25-000728 | complete | ordinary shares: 3,167,959,016 | 2024-12-31 | [3,167,959,016 ordinary shares, par value US$0.0001 per share.](https://www.sec.gov/Archives/edgar/data/1110646/000141057825000728/ntes-20241231x20f.htm) | {'match': 1} |
| SAP | 1000184 | 0001104659-25-017815 | complete | Ordinary Shares: 1,228,504,232 | 2024-12-31 | [Ordinary Shares, without nominal value: 1,228,504,232 (as of December 31, 2024)**](https://www.sec.gov/Archives/edgar/data/1000184/000110465925017815/sap-20241224x20f.htm) | {'match': 1} |
| CNQ | 1017413 | 0001017413-25-000024 | complete | Common Shares: 2,102,996,000 | 2024-12-31 | [2,102,996,000 Common Shares outstanding as of December 31, 2024](https://www.sec.gov/Archives/edgar/data/1017413/000101741325000024/cnq-20241231.htm) | {'match': 1} |

DLO names both A and B but supplies only their combined 285,475,136 total. Its two allocations cannot be extracted from this cover; no exhibit was substituted. ZIM supplies an unnamed 120,423,333 and therefore remains incomplete. BIDU supplies explicit A/B counts. NVO supplies both A and B counts, including unlisted A, but calls them only A/B shares: kind remains `other` and the W1 AShareCapital/BShareCapital keys remain unbound. This is complete census evidence, without invented ordinary classification or inferred W1 labels. All ten quote reviews and document SHA256s are retained in `named-reviewed.json` / `named-source-review.json`.

### Named source hashes

| Issuer | Accession | Document SHA256 |
| --- | --- | --- |
| DLO | 0000950170-25-058197 | 127764a1b5c0d43842f858f3082d313edd83b94239f5f0cb29443a8770947ce6 |
| BIDU | 0001193125-25-066199 | ed3260c2dea5525912126b1f7dadb05f840969e0825e1b761fabd7d037636d67 |
| NVO | 0001628280-25-003920 | dba04b7273b4c0c2602854153c739b088a2a6fda0ace3256f2ab3c5a310d214c |
| TSM | 0001193125-25-083423 | 8239f280b4e9b70fedddd536db181f6ea96fd898ceb2b93cacd730c6aaae2c1e |
| ASML | 0000937966-25-000009 | 48cd4fa41bcd63245bfd1b071e434e7d1bdefb81111fc00a2b99587372ee62d3 |
| QGEN | 0001015820-25-000027 | 833673ced90e45b278e32aae9df4304e64b3f6723bc52f53d9d58b0c24221b8d |
| ZIM | 0001178913-25-000793 | 008c0d2e1cc0a5880b10e13a077c62d290d47b2e124fa0132581c56caa6d1ee2 |
| NTES | 0001410578-25-000728 | 700b5e8dcbf9a8e77c16f1ee73af968b732d6e88a6ba3701a5ddcf185fd244e8 |
| SAP | 0001104659-25-017815 | 4e5e5ee1f3ffd50be0fbbed2faaaee69fb6d12b87270f19369fbe7373333fd3a |
| CNQ | 0001017413-25-000024 | 6c4bcb3f9b75cc14ae25cbe375628222327234b3a1e5a779251c3426059275c6 |

## Coverage by year and form

Years below are official filing years. Economic statement dates are stored separately; no calendar-year or filing-year measurement date is inferred.

| Filing year | Filings | Complete | Incomplete | Conflicting | None |
| --- | --- | --- | --- | --- | --- |
| 1996 | 6 | 0 | 0 | 0 | 6 |
| 1997 | 5 | 0 | 0 | 0 | 5 |
| 1998 | 17 | 0 | 0 | 0 | 17 |
| 1999 | 27 | 0 | 0 | 0 | 27 |
| 2000 | 40 | 0 | 22 | 0 | 18 |
| 2001 | 68 | 4 | 58 | 0 | 6 |
| 2002 | 328 | 11 | 98 | 0 | 219 |
| 2003 | 344 | 47 | 270 | 0 | 27 |
| 2004 | 380 | 48 | 305 | 0 | 27 |
| 2005 | 471 | 79 | 363 | 0 | 29 |
| 2006 | 463 | 71 | 371 | 0 | 21 |
| 2007 | 499 | 77 | 400 | 0 | 22 |
| 2008 | 520 | 91 | 400 | 0 | 29 |
| 2009 | 573 | 111 | 426 | 1 | 35 |
| 2010 | 600 | 109 | 452 | 3 | 36 |
| 2011 | 674 | 137 | 493 | 6 | 38 |
| 2012 | 755 | 182 | 506 | 25 | 42 |
| 2013 | 717 | 165 | 490 | 19 | 43 |
| 2014 | 658 | 135 | 469 | 16 | 38 |
| 2015 | 691 | 154 | 487 | 17 | 33 |
| 2016 | 690 | 152 | 487 | 15 | 36 |
| 2017 | 734 | 164 | 517 | 22 | 31 |
| 2018 | 865 | 196 | 575 | 55 | 39 |
| 2019 | 866 | 201 | 565 | 55 | 45 |
| 2020 | 896 | 219 | 578 | 57 | 42 |
| 2021 | 1013 | 237 | 675 | 60 | 41 |
| 2022 | 1168 | 274 | 791 | 57 | 46 |
| 2023 | 1210 | 279 | 827 | 51 | 53 |
| 2024 | 1234 | 274 | 847 | 63 | 50 |
| 2025 | 1290 | 287 | 881 | 68 | 54 |
| 2026 | 1207 | 264 | 814 | 76 | 53 |

| Form | Filings | Complete | Incomplete | Conflicting | None |
| --- | --- | --- | --- | --- | --- |
| 20-F | 13957 | 2900 | 9530 | 568 | 959 |
| 20-F/A | 2365 | 470 | 1657 | 53 | 185 |
| 40-F | 2337 | 516 | 1723 | 42 | 56 |
| 40-F/A | 350 | 82 | 257 | 3 | 8 |

Unsupported source grammar, missing class allocations/names and ambiguous residue are retained as refusals. `validation/incomplete-reasons.json` lists overlapping refusal reasons. Coverage is intentionally below the initial estimate; the corpus was not expanded or repaired from outside sources.

## Precision and cross-checks

Deterministic sample seed: `foreign-share-census-precision-v1-20261010`. Selection: lowest SHA256(seed|CIK|accession|URL) among 3,968 complete nonconflicting censuses. All 40 original compressed sources were hash verified and their full statement plus nearby cover context read. Fiscal cover headers were separately checked where the measurement date falls back to period end. Final sample: **40/40 correct, 0 incorrect**. This is sample precision, not a guarantee for every census.

The earlier sample revealed an aggregate-only wording variant. The preceding artifact and an honest 39/40 review remain in `validation-before-aggregate-fix/` and `validation/precision-found-defect.json`. The parser was corrected, the entire corpus replayed, and the new deterministic sample reviewed. No failed judgment was silently recertified.

| # | CIK | Accession | Date | Class/count reading | Review |
| --- | --- | --- | --- | --- | --- |
| 1 | 932470 | 0001193125-14-142506 | 2013-12-31 | Class A Ordinary Shares = 502,034,299; Class B Ordinary Shares = 466,857,868; Class C Ordinary Shares = 267,438 | correct |
| 2 | 1371541 | 0001193125-14-141678 | 2013-12-31 | Ordinary Shares = 471,658,155 | correct |
| 3 | 1830162 | 0001493152-21-010214 | 2020-12-31 | Ordinary Shares = 131,467,935 | correct |
| 4 | 1347557 | 0001193125-13-154136 | 2012-12-31 | Series B Shares = 476,850,000; Series BB Shares = 84,150,000 | correct |
| 5 | 1733868 | 0001213900-22-021773 | 2021-12-31 | ordinary shares = 1,371,643,240 | correct |
| 6 | 793628 | 0001553350-18-000439 | 2017-12-31 | common shares = 24,910,916 | correct |
| 7 | 1413855 | 0001213900-24-037144 | 2023-12-31 | ordinary shares = 1,134,236,184 | correct |
| 8 | 1175596 | 0001062993-14-004275 | 2014-03-31 | common shares = 138,724,061 | correct |
| 9 | 316888 | 0001012410-05-000058 | 2002-01-31 | Common Shares = 5,463,525 | correct |
| 10 | 1687451 | 0001410578-24-001331 | 2023-09-30 | ordinary shares = 32,992,740 | correct |
| 11 | 1332639 | 0001193125-06-056923 | 2005-12-31 | Class A Common Shares = 28,846,500; Class B Common Shares = 7,145,000; Class C Common Shares = 100 | correct |
| 12 | 1342338 | 0001104659-24-042156 | 2023-12-31 | Ordinary Shares = 349,448,102 | correct |
| 13 | 1024672 | 0001178913-17-000898 | 2016-12-31 | Ordinary Shares = 10,142,762 | correct |
| 14 | 1639920 | 0001564590-20-004357 | 2019-12-31 | Ordinary Shares = 184,325,957 | correct |
| 15 | 749098 | 0001193125-16-521370 | 2015-12-31 | Common Shares = 402,264,201 | correct |
| 16 | 1158041 | 0001193125-21-331983 | 2020-12-31 | Common Shares = 201,231,446 | correct |
| 17 | 912958 | 0001628280-22-004399 | 2021-12-31 | common shares = 101,739,217 | correct |
| 18 | 1109138 | 0001178913-14-000808 | 2013-12-31 | Ordinary Shares = 29,896,933 | correct |
| 19 | 1624422 | 0001564590-20-011602 | 2019-12-31 | Ordinary shares = 17,940,035 | correct |
| 20 | 1020825 | 0001137171-12-000166 | 2011-12-31 | common shares = 50,348,215 | correct |
| 21 | 1167379 | 0001167379-10-000044 | 2009-12-31 | Common Shares = 299,550,733 | correct |
| 22 | 1740594 | 0000950103-19-004880 | 2018-12-31 | Class A common shares = 22,602,737; Class B common shares = 27,658,290 | correct |
| 23 | 1857475 | 0001857475-23-000013 | 2022-12-31 | Ordinary shares = 94,899,194 | correct |
| 24 | 1612042 | 0001193125-18-099514 | 2017-12-31 | ordinary shares = 36,984,292 | correct |
| 25 | 933267 | 0001615774-18-013501 | 2018-06-30 | common stock = 578,676,460 | correct |
| 26 | 1786182 | 0001213900-24-042283 | 2024-03-31 | Class A ordinary shares = 54,577,170; Class B ordinary shares = 32,261,530 | correct |
| 27 | 1493570 | 0001144204-12-018977 | 2011-03-31 | ordinary shares = 50,000 | correct |
| 28 | 1381640 | 0001193125-19-091845 | 2018-12-31 | ordinary shares = 1,482,999,434 | correct |
| 29 | 1724755 | 0001558370-22-009130 | 2021-12-31 | Class A ordinary shares = 68,286,954; Class B ordinary shares = 34,762,909 | correct |
| 30 | 1046179 | 0000950123-11-035858 | 2010-12-31 | Common Shares = 25,910,078,664 | correct |
| 31 | 1645260 | 0001493152-20-014849 | 2019-12-31 | Ordinary Shares = 103,573,795 | correct |
| 32 | 1637890 | 0001193125-16-534026 | 2015-12-31 | Ordinary shares = 9,313,603 | correct |
| 33 | 1381640 | 0001193125-22-090588 | 2021-12-31 | ordinary shares = 1,456,547,942 | correct |
| 34 | 1173643 | 0001137171-05-000166 | 2004-08-31 | Common Shares = 82,464,037 | correct |
| 35 | 1536196 | 0001213900-22-014672 | 2021-12-31 | ordinary shares = 815,746,293 | correct |
| 36 | 1563411 | 0001563411-23-000005 | 2022-12-31 | Ordinary Shares = 144,301,592 | correct |
| 37 | 1395213 | 0001144204-11-035560 | 2010-12-31 | Class A Ordinary Shares = 462,292,111; Class B Ordinary Shares = 442,210,385; Class C Ordinary Shares = 1,952,604 | correct |
| 38 | 844551 | 0001193125-25-037621 | 2024-12-31 | Ordinary Shares = 1,898,749,771 | correct |
| 39 | 1027664 | 0001178913-11-000774 | 2010-12-31 | Ordinary Shares = 42,693,340 | correct |
| 40 | 1536196 | 0001213900-19-005251 | 2018-12-31 | ordinary shares = 40,399,290 | correct |

Full source quotes, source hashes and individual count/date/coverage notes for the 40 cases are in `validation/precision-reviewed.json`.

| Check | Comparisons | Matches | Mismatches | Missing census count | Unbound | Same-date mismatches | Different-date mismatches |
| --- | --- | --- | --- | --- | --- | --- | --- |
| undimensioned | 8413 | 3891 | 511 | 4011 | 0 | 445 | 66 |
| class_dimensioned | 6172 | 3093 | 218 | 304 | 2557 | 211 | 7 |

8,202 filings have no same-accession active W1 count. Different-date discrepancies are still conflicting per the required strict cross-check rule; their dates are retained. Unknown raw dimension members remain unbound diagnostics. The 666 conflicting filings include total inconsistencies and W1 discrepancies, with overlap rather than additive counts.

## Impact on #186 at 2025 year-end

The original phase-1 measurement predates the gate correction and is unsuitable as the current refusal denominator. We restored its hash-verified local exports, verified base function fingerprints, then installed the corrected sibling SQL bytes into local PG18 and reran all 3,036 pinned lines at 2025-12-31. The sibling checkout had uncommitted gate corrections; HEAD alone does not identify these semantics. The measured sizing schema SHA256 is `a26d4aec8fb7ce1396d27f25724bb47572b6e59c3de85e2c10076f21825a77ea`; this is a local proposal measurement, not a claim about committed #186 or production.

The corrected local cohort has **2,031 scope-refusing lines / 1,267 CIKs**. A complete nonconflicting census from the count's same accession, public by D, with its own positive count measured 0..400 days earlier, supplies:

| Potential proof | Lines |
| --- | --- |
| (a) Sole ordinary class | 234 |
| (b) Explicit count for a positively bound ordinary line class | 0 |
| Total | 234 |
| Distinct CIKs | 170 |
| Also exact W1 count date | 229 |
| Also W1c resolved and prior count non-stale | 125 |

These are class-scope/count potential, not newly admitted sizes. All positively known line labels must agree; an untyped other class cannot prove there is only one ordinary class. The census can provide its own count date, so the exact-W1-date subset is separate. BIDU already has class-dimensioned counts and does not add a new multi-class scope-refusal unlock here; DLO remains incomplete and NVO's ordinary kind stays unverified. No estimate of ~431 was substituted for measured evidence.

| Remaining reason | Lines |
| --- | --- |
| census_conflicting | 131 |
| census_count_stale_or_future | 171 |
| census_incomplete | 1313 |
| census_none | 76 |
| census_not_public_by_cutoff | 1 |
| same_accession_census_missing | 102 |
| sole_ordinary_class_differs_from_line | 3 |

## Tests and local publication/readback

| Test file | Passed | Seconds |
| --- | --- | --- |
| tests/test_sec_foreign_share_census_parser.py | 103 | 0.24 |
| tests/test_sec_foreign_share_census.py | 34 | 0.66 |
| tests/test_sec_foreign_share_census_loader.py | 68 | 1.13 |
| tests/test_sec_foreign_listing_loader.py | 156 | 3.2 |
| tests/test_sec_foreign_listing_evidence.py | 952 | 9.85 |

**1,313 passed, 0 failed**, file by file with `PYTEST_WORKERS=2`, on `timescale/timescaledb:2.27.2-pg18` / PostgreSQL 18.4. Coverage includes observed cover grammars, conflict totals, nil/preferred/deferred shares, multi-line tables, actual PDF bytes and page offsets, bitemporal/source/parser restatement, zero-fact reconciliation, migration cycle, inlining, owner/grants and forged-but-rehashed semantic refusal. Existing W1c evidence/loader regressions pass. CI is reported separately from these local results.

The first focused CI sequence exposed a test-order assumption: its preservation fixture tried to create a W1c table left installed by the earlier suites. The fixture now snapshots an existing W1c table's rows and catalog definition, creating a dummy only when absent. No parser, loader or census artifact behavior changed. The corrected sequence is validated by the subsequent focused PG18 CI run; its observed result is recorded with the final PR snapshot.

Full local apply/readback: 17,801 active census facts and 19,009 source metadata records; artifact classes/status/flags match, existing W1c history fingerprint unchanged. Readback ran as app_runtime, repeatable-read read-only, JIT off. Corpus-volume EXPLAIN contains zero census Function Scans. `local-census-readback.json` and `local-census-explain.json` retain the evidence.

The one task-owned container was removed with its anonymous volumes and absence verified in `container-cleanup.json`. Other containers were left untouched. All changed files are LF and git diff checks passed. Existing open_macro_v4 fixture modifications were excluded from staging.

## Production procedure (not executed)

Follow `docs/runbooks/sec-foreign-share-census.md` for the complete procedure. In an independently authorized window:

1. Pin the reviewed PR head, parser/migration/rollback digests and immutable manifest/census/check inputs. Verify SHA256SUMS and all named/40-source reviews.
2. Stage reviewed artifacts outside raw cache, confirm W1c v2 and the reader/owner roles, set bounded lock/statement timeouts, and apply the additive migration with owner psql -X -v ON_ERROR_STOP=1. It takes the shared advisory lock.
3. Apply only the reviewed artifact with `scripts/load_sec_foreign_share_census.py --manifest <staged manifest> --cache-dir <staging> --output <census.jsonl> --apply --observed-on <actual date>`. Supply FOREIGN_CENSUS_DATABASE_URL securely; never log it. No discovery/download/reparse belongs in publication. An optional combined W1c apply uses explicit census flags and one transaction.
4. Read back through app_runtime/mcp_ro in repeatable-read read-only with JIT off and bounded statement/lock/idle timeouts. Compare source/fact identities, class/count/date/flags/cross-check JSON, public-date boundaries, retirement reasons and API inlining.
5. Keep consumer activation separate. A later sizing v2 must pass its own class/unit/price/action/currency gate; this PR provides no production sizing acceptance.
6. Before rollback, deactivate any future dependent consumer and drain requests, then apply the census rollback with ON_ERROR_STOP. Verify census objects absent and W1/W1c APIs/evidence intact; retain audit artifacts.

No production action or merge is authorized by this report.

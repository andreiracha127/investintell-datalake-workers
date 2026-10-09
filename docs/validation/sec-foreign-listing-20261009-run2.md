# W1c PR #176: run2 validation, 2026-10-09

This report supersedes run1 for production artifact selection. The preserved [run1 report](sec-foreign-listing-20261009.md) remains the historical baseline. This work concerns sourced security type and exact ordinary shares per ADS; W1 admission, share sizing and existing refusal behavior are unchanged.

Generated from final artifact-pinned results at `2026-10-09T15:08:04.023714+00:00`. Fixing commit: `37e3756fd63638652e67ef7da0831f6d9203205a`

## Six review fixes

1. **4227462426:** reject repeated full-text accession/document/type hits and require distinct document hits to meet the exact declared total. Offline replay of 4,061 reviewed run1 queries / 4,064 pages found no repeated pages, repeated document hits or insufficient distinct hits: no evidence of filings lost through this defect in run1.
2. **4227462435:** support genuine 20FR12G and amended EX-2 securities descriptions as ratio-only corroboration; they cannot establish listing type.
3. **4227462447:** compare every child document with the immutable parent before collection, then compare all discovery parsing/binding metadata during combination. Only regenerated parse outputs are excepted, and existing source hashes remain pinned.
4. **4227462440:** discover and collect 40FR12B and 40FR12B/A across the fixed universe.
5. **4227462453:** intersect historical W1-evidenced lines with the fixed universe before coverage counts.
6. **4227462459:** discover 6-Ks using ADS/ADR/GDR/GDS, singular/plural depositary/depository shares and receipts, and American shares evidenced by receipts, then incrementally collect the newly found sources.

The [SEC-API full-text endpoint](https://sec-api.io/api-reference/full-text-search) total counts matching primary documents and exhibits. Distinct documents may share an accession, and distinct SGML component types may share a filing URL. The reviewed cache has 630 query groups with legitimate repeated accessions. Requiring unique accessions to equal a document-hit total would reject complete results; the check uses `(accession, canonical URL, document type)` and rejects duplicate hits/pages. The cache replay rules out this defect in run1's cached responses; it does not independently establish upstream index completeness.

Incremental discovery reused verified exact-query caches and original-document caches; only missing responses and documents were fetched. Discovery was limited to 5 requests per second; the two document-collection shards were limited to 4 each (8 combined), below the requested ceiling of 10. All requests used `InvestIntell-SEP-Ingestion/1.0 (+https://hub.investintell.com)`. The final4 full replay was offline. Run1 remains preserved: all 31,573 original source entries and their original-byte hashes were retained.

## Source-driven parser corrections

The complete final4 corpus was replayed offline through `foreign-listing-v5`; every source disposition and evidence row carries that version. Source review corrected misdated former/current ratios, completed dated transitions and deposited-share-unit classifications, including NNDM, EDU and DQ. The actual overall final4 gate verifies removal of all 59 known false semantic tuples and the required 1/1 source-supported class correction. Publication, legal effectiveness and trading-price effect remain separate.

The actual artifact-pinned unit acceptance verifies 58/58 scoped nonordinary-unit/former assertions excluded from ordinary eligibility, 420/420 supported ordinary-program assertions retained, 55/55 prior false transition tuples absent, and 75/75 supported new tuples plus 9/9 separate dated events retained. The 18/18 coordinate-scope checks required 0 physical source-location moves. Preferred receipts, CPO/basket components and issuer-defined preferred Class B aliases cannot supply an ordinary-share ratio; valid common siblings with equal ratios remain. TAL's Class A primary-unit proof, RBS's dated 20/1 event and Abbey's Section 12(g) provenance pass on actual final evidence. The 512-source preflight against preserved final3 was supporting preparation, not final-corpus proof.

The changed SQL resolver excludes `ads_ratio` facts with `ordinary_candidate=false` before symbol/class/program linking or conflict arbitration, including explicitly symbol-bound nonordinary receipts. The installed fence and 12,190 actual resolver queries returned no nonordinary ratio support. Ratio-only Section 12(g) corroboration does not establish a Section 12(b) listing.

## Artifact and coverage deltas

| Metric | Run1 | Run2 | Delta |
| --- | ---: | ---: | ---: |
| Source entries | 31,573 | 47,329 | +15,756 |
| Unique source URLs | 31,294 | 47,050 | +15,756 |
| Evidence rows | 28,639 | 29,676 | +1,037 |
| Resolved among 1,373 refusals at 2025 year-end | 806 | 819 | +13 |

| Year-end | Both resolved, run1 → run2 (delta) | Type resolved | Ambiguous | None | W1-evidenced denominator | Both among W1-evidenced | Both among 1,373 refusals |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2010 | 2 → 2 (+0) | 17 → 17 (+0) | 0 → 0 (+0) | 3,034 → 3,034 (+0) | 20 → 20 | 2 → 2 (+0) | 1 → 1 (+0) |
| 2015 | 79 → 83 (+4) | 154 → 156 (+2) | 1 → 1 (+0) | 2,956 → 2,952 (-4) | 272 → 272 | 79 → 83 (+4) | 70 → 74 (+4) |
| 2020 | 497 → 513 (+16) | 736 → 746 (+10) | 32 → 26 (-6) | 2,507 → 2,497 (-10) | 964 → 964 | 358 → 365 (+7) | 346 → 358 (+12) |
| 2025 | 1,127 → 1,149 (+22) | 1,461 → 1,471 (+10) | 85 → 75 (-10) | 1,824 → 1,812 (-12) | 2,799 → 2,799 | 1,111 → 1,128 (+17) | 806 → 819 (+13) |

The preserved read-only baseline is unchanged: 1,770 CIKs, 3,036 normalized lines and 1,373 observed SQL refusals. Historical coverage does not certify continued active listing. The complete final manifest contains zero failed source downloads/parses and retains every source disposition.

Source dispositions are fail-closed and explicit: **46,388 parsed**, **593 issuer_binding_unverified**, **348 not_securities_description**, and **zero failed**. The two non-evidence dispositions do not create usable unsupported facts.

## Acceptance and precision

All **17/17 dated acceptance queries passed**: TSM 5/1; ZIM, QGEN and CNQ direct 1/1; AZN 1/2; ANPC 1/1 through November 3, 2022 and 20/1 on November 4; AKTX 100/1 on August 16, 2023, ADS type with no ratio on August 17, and 2,000/1 on August 18; OTLY ambiguous on February 12/13/17/18, 2025 and ADS 20/1 on December 31. The frozen original-source cohort was re-reviewed and checked against run2: **30/30 correct**. A separate actual changed-answer cohort was checked against source: **10/10 correct**. These are observed sample results, not full-corpus precision claims. Selection: 10 distinct changed lines across four requested year-ends: prioritize up to 2 supported by new 40FR12B, then up to 8 ratio-answer changes supported by new 6-K evidence, then fill remaining slots from other changed lines. Prefer 2025 within each pool; preserve a line only once. Actual recorded categories: `{'new_40fr12b': 2, 'new_6k_ratio': 8}`. Every changed case is a distinct line with an actual semantic run1/run2 answer difference; numeric formatting and evidence IDs are ignored.

| Symbol | As of | Type | Ordinary shares per ADS | Discovery category |
| --- | --- | --- | ---: | --- |
| EMA | 2025-12-31 | ordinary_direct | 1 | new_40fr12b |
| TII | 2025-12-31 | ordinary_direct | 1 | new_40fr12b |
| BT | 2025-12-31 | ads | 5 | new_6k_ratio |
| SNN | 2025-12-31 | ads | 2 | new_6k_ratio |
| MBT | 2025-12-31 | ads | 2 | new_6k_ratio |
| MTL | 2025-12-31 | ads | 2 | new_6k_ratio |
| DQ | 2025-12-31 | ads | 5 | new_6k_ratio |
| TAL | 2025-12-31 | ads | 1/3 | new_6k_ratio |
| SKYS | 2025-12-31 | ads | 20 | new_6k_ratio |
| BDRX | 2025-12-31 | ads | 100000 | new_6k_ratio |

The immutable mechanical coverage/delta exports were written before manual-review assembly; their pending manual-precision field is an export-time state. The final artifact-pinned 30/30 and 10/10 manual reports supersede that review status without altering the original exports or their counts.

Repository evidence includes the exact [coverage](sec-foreign-listing-20261009-run2-coverage.json) and [deltas](sec-foreign-listing-20261009-run2-deltas.json), plus derived [acceptance](sec-foreign-listing-20261009-run2-acceptance.json), [frozen30](sec-foreign-listing-20261009-run2-manual30.json) and [changed10](sec-foreign-listing-20261009-run2-manual10.json) records. The [evidence index](sec-foreign-listing-20261009-run2-evidence-index.json) pins the complete external reports; derived summaries preserve source identities, independently checked excerpts, verdicts and original false quote flags.

## Focused validation

Local initial application and resolver validation used `timescale/timescaledb:2.27.2-pg18`, observed PostgreSQL `18.4`. This matches the requested production engine major version; run1 used PostgreSQL 16.

- `tests/test_sec_foreign_listing_evidence.py`: **252 passed**, 0 failed, 0 errors, 0 skipped; observed `2026-10-09T13:42:56.470945+00:00`.
- `tests/test_sec_foreign_listing_loader.py`: **133 passed**, 0 failed, 0 errors, 0 skipped; observed `2026-10-09T13:43:07.291232+00:00`.

The files ran separately and sequentially with `PYTEST_WORKERS=2`. The observed tests and coverage pin the same implementation hashes. No full-suite result or unobserved CI status is claimed.

## Artifact identities

| Artifact | Exact path | SHA-256 |
| --- | --- | --- |
| Schema, run2 changed | `E:/investintell-datalake-workers-sep/.worktrees/w1c-foreign/schemas/sec_foreign_listing_evidence.sql` | `0df7689b4fa5206d5d5b01034b423ade4b20bfae88fc0720526d43f8af842ff9` |
| Universe | `E:/investintell-data/w1c-20261009-baseline/universe.json` | `06d052a96fdc8759c6d443f67fbf20ecaacac4655dd7cb306e31f558e5c5b43b` |
| W1 all-versions observations | `E:/investintell-data/w1c-20261009-baseline/foreign_observations_all_versions.json` | `0cef1602ff75bb3582680d1b633ddf25f4738c0ceb3abd7a178ebb920917852b` |
| Current W1 statuses | `E:/investintell-data/w1c-20261009-baseline/current_status_rows.json` | `8091c2570436a032b7fd696becfabfc74945d86f11f3b0ab48cea6f32315b3b4` |
| Final manifest | `E:/investintell-data/w1c-20261009-run2/final4/manifest.json` | `5a7667cd5679bb99400b1113abcdf3b260ddb831cab12c6942c540f118f3d283` |
| Final evidence JSONL | `E:/investintell-data/w1c-20261009-run2/final4/evidence.jsonl` | `3850894d58fbff69e8e243b6361a18f86f79f1000e5fe405bb00bbfbadc9ec45` |
| Coverage | `E:/investintell-data/w1c-20261009-run2/final4/validation/coverage.json` | `0d538879a5bfa5932de430cdb58e7cbe5abe97e4388c49ee171cb1e03f20b38a` |
| Acceptance | `E:/investintell-data/w1c-20261009-run2/final4/validation/acceptance-run2.json` | `4f506445669650b2a1d4988c0abe6943675c4b106bf38a4145168d62ba884619` |
| Frozen30 final manual review | `E:/investintell-data/w1c-20261009-run2/final4/validation/manual-frozen30-run2.json` | `ea6dcafbb1237511a7ba9eda908b132570765f9a4c2e70263afb47a90cd3d11c` |
| Changed10 final manual review | `E:/investintell-data/w1c-20261009-run2/final4/validation/manual-changed10-run2.json` | `b852d3ec43699316890249e2b4cad1084d025c053b8f8fd6d942429f5d4800b3` |
| Final v5 unit/transition and SQL-fence acceptance | `E:/investintell-data/w1c-20261009-run2/audit/final4-unit-source-acceptance.json` | `c4eb48106af75985c4b9ecd42612948379cede43a0de94f984286c20a0bcb601` |
| Actual final4 overall delivery/source gate | `E:/investintell-data/w1c-20261009-run2/audit/final4-delivery-verification.json` | `fc7b43e4efa05a03bfd8e7ebc7ceeba92cda299e20344471f4027d5a97d31a35` |
| Supporting 512-source v5 preflight against preserved final3 | `E:/investintell-data/w1c-20261009-run2/audit/v5-final-source-preflight.json` | `c2fe543a3f5007841876fb6a77ae31aac2aa2f754688c435ad12d18316d63c32` |

Run1's recorded schema identity is `ab21ec5e4478ec97d15a2c88c52f9ad6fdc8928300c45bb337a53d4c8dd47786`, from the implementation pin inside its preserved coverage report. The actual run2 schema identity is `0df7689b4fa5206d5d5b01034b423ade4b20bfae88fc0720526d43f8af842ff9`; it is changed and matches final coverage and focused tests.

Run1 remains preserved at `E:/investintell-data/w1c-20261009-run/final`. The new owner-load artifact is `E:/investintell-data/w1c-20261009-run2/final4`. Source identity uses original-byte hashes regardless of junction backing path.

## Owner production procedure — not executed

The owner must review the exact artifact hashes above. Run2 schema SHA-256 is `0df7689b4fa5206d5d5b01034b423ade4b20bfae88fc0720526d43f8af842ff9` (changed from run1 `ab21ec5e4478ec97d15a2c88c52f9ad6fdc8928300c45bb337a53d4c8dd47786`). Before loading, verify manifest SHA-256 `5a7667cd5679bb99400b1113abcdf3b260ddb831cab12c6942c540f118f3d283` and evidence JSONL SHA-256 `3850894d58fbff69e8e243b6361a18f86f79f1000e5fe405bb00bbfbadc9ec45`. Use the actual reconciliation date.

```powershell
# Owner-authorized production writer only, outside 06:00-08:30 UTC.
# Supply the writer connection using the operator's credential mechanism.
psql -X -v ON_ERROR_STOP=1 -f schemas/sec_foreign_listing_evidence.sql
# Set FOREIGN_EVIDENCE_DATABASE_URL securely to that authorized writer.
python scripts/load_sec_foreign_listing_evidence.py `
  --universe E:/investintell-data/w1c-20261009-baseline/universe.json `
  --cache-dir E:/investintell-data/w1c-20261009-run2/final4 `
  --output E:/investintell-data/w1c-20261009-run2/final4/evidence.jsonl --apply
```

This is an apply-only load of the reviewed artifact, with no discovery or parsing during application. Production read-back uses `mcp_ro`, `127.0.0.1:65432/market`, and `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=30000'`, outside 06:00-08:30 UTC. Read back the exact source/fact counts, named dated cases and four-year coverage. These production steps are deferred to the owner; this work performed no production writes, DDL, Railway operation, deployment or merge. W1 admission/sizing integration remains separate.

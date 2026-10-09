# W1c PR #176: run2 final5 gate-fix validation, 2026-10-09

This report supersedes final4 for production artifact selection. It fixes the two production-gate P1s found in final4 and reissues the reviewed artifact as `final5`. The preserved [run1 report](sec-foreign-listing-20261009.md) and the [final4 report](sec-foreign-listing-20261009-run2.md) remain historical baselines. W1 admission, share sizing and existing refusal behavior are unchanged.

Generated from artifact-pinned results at `2026-10-09T21:44:33+00:00`. Fixing head: `f028abb6e5427309f847bb111a7b0e8938d405ed` (previous head `99d78516f4b4b290251fb3241af29c71cdd5ebc9`).

## The two gate P1s and their fixes

1. **Contradictory operative dates can no longer disappear into a zero-fact parse or a filing-plus-one fallback.** An F-6, cover, Item 12.D or 6-K whose operative dates conflict now emits `ads_ratio` evidence carrying `operative_date_conflict`, the sorted `operative_date_candidates` and the literal proof, with `effective_from` equal to the earliest stated date. The resolver treats such a source as a control: once its earliest stated date and public availability have arrived, the answer is `ambiguous` for that program, symbol and class until a later public source establishes a single ratio and date. Filing plus one is never substituted for conflicting dates. A separately dated completion ("changed on July 31 … became effective on August 5") uses the source's explicit effective date rather than fabricating a conflict; genuinely contradictory effective dates still conflict.

2. **A conditional announcement can only mark a change pending.** The 6-K path detects outstanding conditions ("subject to shareholder approval", "subject to the consolidation", "if approved", regulatory and depositary conditions, and prospective "plans to change"/"expected to be effective" statements) and emits `ratio_effectiveness_pending` with the literal proof and condition codes. A pending announcement never activates a pending F-6 registration; activation requires a later public confirmation (depositary notice, completion 6-K or meeting result) that discharges the named conditions, or explicit completion of the ratio. Until then, the ratio at D after the conditional date is `ambiguous`. Contract boilerplate ("subject to the terms of the Deposit Agreement") and pro-forma accounting assumptions ("assuming … occurred at the beginning of the earliest period presented") are not conditions. ANTE's conditional consolidation notices still resolve once its later public shareholder result arrives.

Regression coverage: one test per P1 plus the filing-plus-one path, each failing on `99d78516f4b4b290251fb3241af29c71cdd5ebc9` and passing on the new head (verified in a temporary worktree against the old head), and the former wrong expectation at the DQ test was corrected. See the focused test list below.

## Shapes measured in the final4 corpus

Re-parsing the unchanged final4 corpus (47,329 sources, fully offline) with the fixed parser:

- **Conflicting-dates path: 0 sources.** No source in the corpus states genuinely contradictory operative dates. The DQ contract is a clean prior/commencing interval, OTLY's market-session statements both resolve to February 18, and the TC Biopharm "changed July 31 / became effective August 5" pairs use their explicit effective dates. The path remains regression-covered by the synthetic gate scenarios.
- **Conditional-announcement path: 38 pending rows across 37 distinct sources** (35 `ratio_change_6k`, 3 `f6`). Condition codes: 3 consolidation+shareholder-approval (ANTE family), 24 unknown-condition (dated plans/expected changes), 4 regulatory-approval, 3 consolidation, 1 other-approval, 3 F-6 depositary-notice pendings without codes.

## Changed answers (final4 → final5)

Nine semantic answer changes, all at the 2025 year-end, across nine distinct lines:

| Symbol | Date | Before (final4) | After (final5) | Verdict |
| --- | --- | --- | --- | --- |
| CCM | 2025-12-31 | none / ads | ambiguous / ads | correct: 30/1 plan never publicly completed; fail-closed |
| CMMB | 2025-12-31 | none / ads | ambiguous / ads | correct: 80/1 plan pending without public completion; fail-closed |
| AENZ | 2025-12-31 | none / ads | ambiguous / ads | correct: 15/1 plan pending; 2023 cover corroborates but no public completion; fail-closed |
| TEDU | 2025-12-31 | none / none | ambiguous / none | correct: regulatory-approval plan pending; fail-closed |
| AMBR | 2025-12-31 | none / ads | **resolved / ads 5/1** | correct: completion press releases (public 2022-11-30/2023-12-13) plus F-6EF and covers; final4 lost the completed-event date |
| OPRA | 2025-12-31 | none / ads | ambiguous / ads | correct: consolidation-conditional 1/1 proposal without public meeting result; fail-closed |
| FRLN | 2025-12-31 | ambiguous / ads | **resolved / ads 15/1** | correct: F-6 POS 15/1 plus 6-Ks stating completion on 2023-05-12 (public 2023-05-30/31); final4 lost the completed-event date |
| STKH | 2025-12-31 | none / ads | ambiguous / ads | correct: serial announced plans without public completion; fail-closed |
| TCBP | 2025-12-31 | none / ads | ambiguous / ads | correct: the 2025-02-10 6-K carries a plan press release and, in a separate exhibit of the same accession, a completion notice; the plan row stays pending without a later public confirmation; fail-closed |

In addition, three dated acceptance answers change by design under P1-2: **ANPC 2022-11-04** resolved 20/1 → ambiguous (plan pending until the public 2022-12-16 completion; 20/1 resolves on 2022-12-17), **AKTX 2023-08-17** none → ambiguous and **AKTX 2023-08-18** resolved 2000/1 → ambiguous (plan pending until the public 2023-09-29 completion; 2000/1 resolves on 2023-09-30). Every changed line was re-checked against its supporting source excerpts; the complete packet with quotes and verdicts is [run2-final5-source-reviews.json](sec-foreign-listing-20261009-run2-final5-source-reviews.json).

## Acceptance and precision

- 17 dated acceptance queries re-run: **14/17 match final4**; the three exceptions are exactly the ANPC/AKTX transitions above, which are the intended P1-2 corrections. TSM 5/1; ZIM, QGEN and CNQ direct 1/1; AZN 1/2; ANPC 1/1 through November 3, 2022; AKTX 100/1 on August 16, 2023; OTLY ambiguous on February 12/13/17/18, 2025 and ADS 20/1 on December 31.
- Frozen 30-line final4-reviewed cohort re-checked against final5: **30/30 unchanged and matching**.
- Final4 changed10 cohort re-checked against final5: **10/10 unchanged and matching**.

Year-end coverage (final4 → final5): both resolved 2 → 2, 83 → 83, 513 → 513, 1,149 → **1,151**; ambiguous 0 → 0, 1 → 1, 26 → 26, 75 → **81**; resolved among the preserved 1,373 refusals unchanged at 819. Sources 47,329; evidence rows 29,676 → **29,709**; dispositions 46,388 parsed / 593 issuer_binding_unverified / 348 not_securities_description / zero failed.

## Focused tests and migration cycle

`timescale/timescaledb:2.27.2-pg18`, PostgreSQL `18.4`; one file at a time, sequentially, `PYTEST_WORKERS=2`.

- `tests/test_sec_foreign_listing_evidence.py`: **317 passed**, 0 failed, 0 errors, 0 skipped.
- `tests/test_sec_foreign_listing_loader.py`: **135 passed**, 0 failed, 0 errors, 0 skipped.

Migration cycle verified live on PG18.4: schema replay is additive and idempotent with no table rewrite (relfilenodes stable, rows preserved), the rollback removes exactly the resolver function and the two tables, ownership transfers to `worker_writer`, PUBLIC is revoked, and `app_runtime`, `app_analytics_ro` and `mcp_ro` hold SELECT/EXECUTE and were verified to read as `mcp_ro`.

## Artifact identities

| Artifact | Exact path | SHA-256 |
| --- | --- | --- |
| Schema (changed from final4) | `E:/investintell-datalake-workers-sep/.worktrees/w1c-foreign/schemas/sec_foreign_listing_evidence.sql` | `f334d08d3d3b496bd613495d59ee2a3a12365f58c422b3d51bd77530e61d2957` |
| Rollback (unchanged) | `E:/investintell-datalake-workers-sep/.worktrees/w1c-foreign/schemas/sec_foreign_listing_evidence.rollback.sql` | `bd1b954e57c5952ea5a20e483c379b0f3f4899dc3b59abe3d50a1e067b909041` |
| Universe | `E:/investintell-data/w1c-20261009-baseline/universe.json` | `06d052a96fdc8759c6d443f67fbf20ecaacac4655dd7cb306e31f558e5c5b43b` |
| W1 observations (collector) | `E:/investintell-data/w1c-20261009-baseline/foreign_observations.json` | `a937b8cc1d7bfdfc5812c30c1a2b2a17961995d5d350554c292f7215b59797f8` |
| W1 all-versions observations | `E:/investintell-data/w1c-20261009-baseline/foreign_observations_all_versions.json` | `0cef1602ff75bb3582680d1b633ddf25f4738c0ceb3abd7a178ebb920917852b` |
| Current W1 statuses | `E:/investintell-data/w1c-20261009-baseline/current_status_rows.json` | `8091c2570436a032b7fd696becfabfc74945d86f11f3b0ab48cea6f32315b3b4` |
| Final5 manifest | `E:/investintell-data/w1c-20261009-run2/final5/manifest.json` | `793cc435a00d41309a5b1b72724eb940ae43b8c86b97e2d7412d5fe0439a5646` |
| Final5 evidence JSONL | `E:/investintell-data/w1c-20261009-run2/final5/evidence.jsonl` | `9ca17573dd649db4075a7eb40065274e7e0b1d536649b553c4b790c3dfc1accc` |
| Final5 coverage | `E:/investintell-data/w1c-20261009-run2/final5/validation/coverage.json` | `798034fa8869b9cfc2fd651ea7411eadb3ccea2a931e815128a26eb4349eaa57` |

`SHA256SUMS` at `E:/investintell-data/w1c-20261009-run2/final5/SHA256SUMS` pins the manifest, JSONL, summary and validation outputs. The final5 replay was fully offline against the cached sources: zero network requests, so the discovery/collection rate ceilings (5/8 per second) and the `InvestIntell-SEP-Ingestion/1.0` user agent were not exercised.

## Owner production procedure — not executed

Same procedure as final4, replacing the artifact paths and hashes with the final5 identities above. This work performed no production writes, DDL, Railway operation, deployment or merge. W1 admission/sizing integration remains separate.

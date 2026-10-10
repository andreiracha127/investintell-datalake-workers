# W1c B1: detector validated; corrected final6 replay pending

**Status: incomplete. The corrected final6 artifact has not been issued or
approved for loading.** Code is in [draft PR #181](https://github.com/andreiracha127/investintell-datalake-workers/pull/181).
The first full candidate was rejected after source review and is preserved
for audit. Its hashes and answer changes do not certify the corrected parser.

Parser `foreign-listing-v10` SHA-256:
`caae1a92900b1c1d169b7c9f8a12d2ed7feab25e58e0aa14a0f4d8d61d182212`.
Schema unchanged:
`f334d08d3d3b496bd613495d59ee2a3a12365f58c422b3d51bd77530e61d2957`.
SQL retains later-accession, publication, exact-class, program and ratio rules.

The detector requires the ratio change as the completed object and the ratio
change or consolidation as the approved object. It rejects preparatory objects,
budgets, unrelated classes/programs/ratios and financial assumptions. Numeric
table matches do not supply confirmation or invent pending controls. Genuine
direct and coordinated operations retain clean narrative proof.

Four synthetic fixtures were added: `b1_preparations_completion.html`,
`b1_different_class_confirmation.html`, `b1_budget_approval.html` and
`b1_financial_table_confirmation.html`. Their four re-gate regressions fail
with the parser loaded from `1572834d` (4 failed, 0 passed) and pass with v10.
The SQL regressions retain ambiguity through invalid later sources. The real
ANPC cover/F-6/amendment/completion regression resolves 20/1 on 2022-12-17,
after ambiguity through 2022-12-16.

## Final code validation

One task-owned `timescale/timescaledb:2.27.2-pg18`, PostgreSQL 18.4. Files ran
sequentially with `PYTEST_WORKERS=2`, plugin autoload disabled and artifacts on C:.

| Suite | Result |
|---|---|
| `tests/test_sec_foreign_listing_evidence.py` | 390 passed, zero failures/errors/skips |
| `tests/test_sec_foreign_listing_loader.py` | 138 passed, zero failures/errors/skips |

Ruff, whitespace and LF checks passed. Schema idempotence passed in the
evidence suite. Local ownership/ACLs were verified: `worker_writer`, PUBLIC
revoked, readers `app_runtime`, `app_analytics_ro`, `mcp_ro`. No schema bytes
changed, so no new migration/rollback cycle was required.

Independent review passed 54/54 checks: 32 adversarial cases, 5 coordinated
object boundaries, 14 checksum-verified positive sources and 3 accounting
distinctions. Source probes retain AMBR 5/1, FRLN 15/1 and BDRX 5/1 completion.
Evidence is under `C:/investintell-data/w1c-final6-work/` and
`C:/investintell-data/w1c-b1-review/`.

## Confirmation source audit

Final5 has 408 confirmations and 178 distinct strings. All 288 supporting
original documents passed raw-byte checksum verification. **18 numeric
financial-table proofs are invalid:** OTLY 13, NetEase 2, ASLN 1, LGHL 1 and
a fair-value/warrant table 1. Every row and source is in
`C:/investintell-data/w1c-final6-audit/final5-invalid-financial-confirmations.json`.
The frozen corrected parser's focused replay changes those 18 into nine
discarded candidates and nine clean narrative confirmations, zero pending.
Whole-corpus corrected confirmation counts and removal categories are pending.

The first full candidate ran 16 workers in 525.13 seconds, with peak process
tree RSS 4.254 GiB sampled every second and zero parse errors. It was rejected
because it lost 27 genuine completion narratives. Its data and verdicts are
preserved at `C:/investintell-data/w1c-final6-candidate1/`; the grammatical
regressions are fixed in the tested code. Its 17/17 acceptance, 30/30 frozen
and 10/10 changed cohort results describe that rejected candidate only.

The baseline 40-line provenance packet verifies 191/191 supporting source
hashes across 113 originals, all meaningful quote components and W1 annotations.
The new capture/compare helper verifies exact loaded artifact identity and
preserves the 3,036-line universe, 17 acceptance cases and 30+10 cohorts across
12,155 distinct line/date queries.

## Remaining work

Available RAM fell to approximately 4.3 GiB. Two corrected-run attempts were
barred before parsing because resource allocation admitted only 13/14 workers.
The owner was asked to free RAM for the required 16 workers or revise the count.
No corrected manifest or evidence has been published.

Resume with a fresh offline parse to `C:/investintell-data/w1c-final6/`, using
the immutable final5 manifest, `--raw-cache-dir`, original collector observations
and fresh C: staging. Do not run discovery or copy/write the raw cache. Then
verify hashes, load a fresh disposable PG18 database, repeat the year-ends and
17/30/10 checks, and source-check all changed answers and removed confirmations.
Issue final hashes/metrics, complete this report, update the runbook and make
the PR ready only after those checks. Production final5 remains untouched.
No production access, Railway changes or merge occurred.

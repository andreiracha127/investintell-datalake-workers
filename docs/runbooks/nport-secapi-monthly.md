# N-PORT sec-api monthly seeds and atomic loads

This lane feeds `sec_nport_holdings` from monthly `form-nport` containers.
Amounts are USD. Characteristics, N-PORT lookthrough and Light fund classification
consume these rows. No service or cron is attached; scheduling the lane is an
owner decision (`railway.nport-secapi-monthly.toml`).

| Component | Responsibility |
|---|---|
| `download.py` | Require every month, cache by size plus `updatedAt`, retry listing/transfers, atomically publish verified replacement bytes |
| `convert.py` | One winning filing per date/series, one CSV per date, source accounting in `manifest.json` |
| `contract.py` / `validate.py` | Shared exact quality predicates / offline profile and verdict |
| `nport_parallel_load.py` | Read-only preflight, atomic COPY/insert/verify, compression maintenance |
| `nport_secapi_monthly.py` | Monthly window, new-series planning, result reporting, cagg refresh request and alignment check |

## Validation contract

A converter manifest automatically enables the contract in the loader. The lane
also passes `--secapi`, requiring that manifest even after interrupted conversion. Old
monthly manifests require reconversion to obtain source accounting. Plain DERA
CSVs retain their loadability and ISIN checks. Comparisons use exact counts and
Decimal sums; rounding is only for display. Monthly loads cannot use `--no-verify`.
Every sec-api manifest load requires `--new-series-only` or scoped `--delete-first`;
the loader refuses plain conflict-skipping mode before any database work.

On a revisit, value/source checks judge **new series actually inserted**. ISIN
must pass two independent checks: **new rows actually inserted** and the
**whole resulting date**, including existing rows. Both checks run before the
same transaction commits, so high coverage in existing series cannot dilute a
defective new cohort. The worker also checks the selected new-series CSV cohort
before loading; offline validation judges the supplied seed.

| Check | Blocks when | Cohort / threshold |
|---|---|---|
| Malformed holding loss | Dropped structural holdings or invalid non-null numeric fields exceed the allowance | **>1% per winning filing**, and **>1% across selected filings for the date**, before deliberate key deduplication |
| Missing market value | Emitted holdings with NULL market values exceed the allowance | **>1% of selected emitted holdings** |
| Percentage retention | Too few series retain their independently measured source percentage sum | At least **60% within ±5 percentage points** and **70% within ±10 points**; every selected series participates, with no row-count exemption |
| Missing percentages | Any selected series has no percentage values | **Zero permitted**, even if its source sum is zero |
| ISIN fill | Raw `ISIN rows / holdings` falls below either floor | **90% of newly inserted rows**, checked by the shared contract in the transaction, **and 90% of the whole resulting date**, including existing rows; both checks apply at all sizes and use `isin IS NOT NULL AND isin <> ''` |
| Seed integrity / loadability | Empty/partial date, missing/inconsistent source accounting, CSV/manifest row or series-identity mismatch, invalid header/types/NULL constraints/non-finite values, cross-CSV conflict keys, or split series in new-series mode | Structural refusals before maintenance; actual inserted series, row counts, exact conflict keys and target values must match the selected preflight cohort before commit; whole-series skips use the snapshot after the insert lock |
| Optional matview | Requested `mv_nport_sector_attribution` is absent | Use `--skip-matview` on current production schema |
| Other readings | Never introduce additional quality gates | **Reported only:** 100-centered bands, percentage median/over-1000 count, USD totals/share, key mix, deliberate DERA duplicates, amendments/supersessions and excluded dates |

The percentage reference is the winning filing's raw `pctVal` sum, aggregated
independently **before mapping and deduplication**, recorded as
`filing_quality[].source_pct_sum`. A leveraged portfolio can sum to 160% and a
cash-heavy one to 22%; forcing either to sum to 100% is wrong. Missing values
still block, while conversion unit errors or severe derivative-weight losses
fail against their source reference. The old absolute `>1000%` gate and
1,000-row exemption are removed.

### Calibration, 2026-10-07

The Q3 seed was regenerated from the five 2026-06..10 containers into a separate
directory. All five CSV SHA256s match the original seed. Across **13,923 selected
filings / 4,712,431 source holdings / 4,218,901 emitted rows**, malformed source
holdings, invalid non-null numeric fields, missing market values and series
without percentage values were all **zero**.

| Date | Rows | Series | Source ±5 | Source ±10 | ISIN |
|---|---:|---:|---:|---:|---:|
| 2026-05-29 | 58,691 | 197 | 90.86% | 93.40% | 93.92% |
| 2026-05-31 | 940,023 | 2,505 | 85.47% | 88.66% | 99.36% |
| 2026-06-30 | 1,959,593 | 7,026 | 89.17% | 91.84% | 98.28% |
| 2026-07-31 | 1,260,510 | 4,194 | 82.52% | 87.72% | 99.02% |
| 2026-08-31 (partial; do not load) | 84 | 1 | 100% | 100% | 98.81% |

The 1% allowances tolerate isolated defects while refusing broken filings.
Source bands have room below complete-date baselines. Some late cohorts still
fail because DERA deduplication loses actual emitted weights: May September
filings retain only 10% inside either source band; June September retains
50% / 54.55%, and June October 25% / 25%. `CIK:0001681717` loses 320
placeholder-key holdings, moving 85.20% source weight to 27.16% emitted.
Rejected cohorts need operator assessment; the lane does not relax its contract.
Legitimate leverage such as `S000009706` (160.85% source, 160.90% emitted) passes.
For the 36 inspected late filings, raw percentages agree with
`100 × raw USD holdings / netAssets` to within 0.1513 points; this cross-value
comparison is calibration evidence, not an additional uncalibrated gate.

## Source and key semantics

Containers use **filing month**; public reports generally arrive about two
months after report date, with stragglers in the next month. The newest NPORT-P
or NPORT-P/A **with holdings** wins wholesale per `(report_date, series_id)`.
Empty amendments cannot erase earlier filings. Series fall back through
`filerInfo.seriesClassInfo.seriesId` to `CIK:<cik>`.

Default `dera` keys preserve production mapping. Placeholder `999999999` CUSIPs
and `N/A` LEI/ISIN values can collapse holdings; those counts are separate from
malformed losses. `strict` retains them but lowers quarter-end ISIN below 90%,
so activation remains an owner decision. Both key policies are unchanged.
Conversion retains one filing at a time and per-series metadata, not a whole
monthly container. Loader conflict-key indexes use bounded temporary disk
storage rather than millions of Python tuples.

## Manual sequence

Historical seed: `E:\tmp-deploy\nport-q3-seed`. Reconvert to obtain the current
manifest. `convert` refuses a non-empty output unless `--overwrite` is supplied;
a separate output preserves prior evidence.

```powershell
py -3.13 -m tools.nport_secapi.download --from 2026-06 --to 2026-10 `
  --out E:\tmp-deploy\sec-cache\nport-secapi --dotenv E:\investintell-light\backend\.env
py -3.13 -m tools.nport_secapi.convert --out E:\tmp-deploy\nport-q3-current `
  --min-report-date 2026-05-01 --partial-months 2026-10 <the five containers>
py -3.13 -m tools.nport_secapi.validate E:\tmp-deploy\nport-q3-current
```

For an authorized operator load, select exactly the intended CSV/date scope and
use identical dry-run/load arguments. The DSN stays in the operator environment.

```powershell
$loadArgs = @('--seed-dir', 'E:\tmp-deploy\nport-q3-current', '--dsn', $DSN,
  '--workers', '4', '--skip-matview', '--new-series-only', '--secapi',
  '--only', '2026-05-29.csv', '--only-report-dates', '2026-05-29')
py -3.13 -m tools.nport_dera.nport_parallel_load @loadArgs --dry-run
py -3.13 -m tools.nport_dera.nport_parallel_load @loadArgs
```

`--dry-run --dsn` uses read-only connections to model existing series/keys,
cleanup, final ISIN and the optional matview. Without a DSN it models an empty
table; new-series mode needs a DSN. Real loads also enforce preflight, then
repeat shared predicates on actual rows before commit, including trigger changes.

The 2026-10-07 read-only production check found **940,023 rows / 2,505 series**
on `2026-05-31`; the earlier three-series snapshot is stale. Check live state
before operator work. This review made no production writes.

## Atomicity, concurrency and maintenance

CSV/date connected groups are transactions: every CSV contributing to a date
commits or rolls back together. Disjoint groups COPY in parallel. A rejected
`--delete-first` replacement restores its old rows, because DELETE shares the
verified transaction. Placeholder cleanup is also scoped and transactional.
New-series mode cannot be combined with replacement or cleanup. There is no
delete-by-load-timestamp rollback handle.

Session lock **900_365** covers preflight, preparation, workers and restoration.
Transaction lock **900_364** covers new-series insertion through verification
and commit; its INSERT uses a fresh READ COMMITTED snapshot after waiting.
The worker holds **900_363** over the whole run. Order: monthly → lifecycle → insert.

Preparation pauses existing compression jobs, retaining ID/horizon/schedule/config.
Only overlapping compressed chunks in the current schema are decompressed.
Finalization recompresses them and restores each original scheduled state even
when a load or matview fails. A missing policy remains missing. Restoration
failure returns exit 1. Abrupt process termination can leave maintenance paused;
inspect the original job and resume with `SELECT alter_job(<id>, scheduled => true)`
only if it was previously enabled. See
[Timescale policy maintenance](https://docs.tigerdata.com/use-timescale/latest/compression/compression-policy/).

Preserve `PGOPTIONS=-c timescaledb.max_tuples_decompressed_per_dml_transaction=0`
for large compressed-chunk DML; zero allows unlimited decompression per the
[Timescale GUC reference](https://docs.tigerdata.com/api/latest/configuration/gucs/).

Exit **0**: accepted transactions and successful maintenance. Exit **2**:
preflight/transactional quality rejection. Exit **1**: input selection, COPY/SQL
or maintenance failure. Other disjoint dates may have verified commits when
one fails; rejected dates commit no rows. Optional matview failure can follow
verified commits.

## Monthly lane and cagg recovery

Month M downloads filing months M-3..M and targets report dates M-5..M-3.
Every month must be listed. Each date is revisited three times for late filings;
existing series stay intact. No new series means no chunk preparation.
Partial target dates, empty conversions, value failures and loader errors
produce failed stats. One date's exception retains its result and permits
assessment of the remaining dates.

The cagg is owned by `postgres` and is `materialized_only`. The lane runs as
`worker_writer`, which cannot refresh it. After the loads commit, the lane calls
`public.request_nport_series_profile_refresh()` once, on an autocommit
connection, so the request is committed before any poll. The function advances
the owner policy, live job **1078** (every **6 h**, NULL `start_offset`, `1 day`
`end_offset`), which refreshes the whole invalidated window. Its job id is a
receipt, not completion; see [the capability runbook](nport-series-profile-refresh.md).

The lane then polls up to 30 times, 10 seconds apart. Alignment needs each
accepted date's profile to hold every committed series, and the shared cohort
check in `_fund_pipeline_freshness` to pass. A confirmed run is `ok`. An
unconfirmed one is `blocked` with `reason: cagg_refresh_pending` and exits 1;
its loads stay committed. A request that raises makes the run `failed`.
`stats.cagg_refresh` records `report_dates`, `requested`, `job_id`, `aligned`,
`polls`, `pending_report_dates`, the cohort verdict, and `error` on failure.

Rejected loads roll back, so the policy never materializes rejected rows. A
later no-new-series run compares table and profile series counts per date and,
after read-only ISIN checking, requests again for a stale profile. Proposed cron
`0 9 3 * *` remains disabled. The nixpacks image must include `tools/`; it needs
`SEC_API_IO_KEY`. Cache/log paths scrub tokens and failed transfers preserve
the last verified cache.

## Reproduction

CI runs unit/regression checks, Ruff, compileall and the real Timescale harness
on `timescale/timescaledb:2.27.2-pg18` with a disposable loopback database,
512 MB / one CPU cap, explicit disposable marker and isolated schemas.

```powershell
$env:NPORT_TEST_DATABASE_URL = '<local disposable nport_proof DSN>'
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = '1'
py -3.13 -m pytest tests/test_nport_parallel_load_timescale.py -q
# Optional real provider-record gate (2026-05-29 only):
$env:NPORT_REAL_SEED_DIR = 'E:\tmp-deploy\nport-pr153-verified-seed'
py -3.13 -m pytest tests/test_nport_parallel_load_timescale.py -q
```

Cases prove actual post-insert rejection, revisits, concurrent lifecycle/insert
locks, compressed chunks, exact policy preservation, restoration errors,
same-date CSV rollback, rejected replacement, independent dates, post-preflight
market-value/percentage corruption and stale-cagg detection. The real date gate
loads 58,691 rows / 197 series, checks per-series rows/values/exact percentages
against CSV and cagg, then proves an idempotent zero-insert rerun.

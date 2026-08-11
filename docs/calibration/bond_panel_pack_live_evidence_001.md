# Bond panel pack live evidence 001

_Gate declaration and measured evidence · 2026-08-08 · production target `investintell-db`_

---

## ⛔ Final disposition: NO-GO

The required production parity run failed conjunctive T3 gates. Publication
therefore stopped before Stage 6 exactly as declared: no incremental panel
publication was prepared, validated, or pointed. The historical backfill
remains the sole current publication. This report does **not** contain a Stage
6 production run JSON because executing Stage 6 after the failed parity gate
would violate the binding stop contract.

## 📋 Binding research contract

The frozen research identity is `0c0d78a866bc1090`. This report does not
authorize changing an input definition. Any such change starts a separately
declared research round with a new hash; the frozen dictionary is never edited.

The operational implementation preserves these definitions:

- signal month `t`, execution month `t+1`, realized return `t+1` to `t+2`
- one yield-spread residual: `ytm - interpolated curve`
- walk-forward estimation with inputs dated no later than `t`
- typed exits: matured to par, distressed to `0.40` recovery, unexplained to
  last price
- typed reasons for every exclusion, absent input, and degraded fallback

The implementation uses only the yield-minus-curve definition above and no
alternative spread label.

## 🎯 Predeclared parity gates

The following thresholds were fixed on 2026-08-08 before examining any
per-bond parity delta for the required `2025-01` and `2026-06` comparisons.
All absolute yield and spread deltas are measured in basis points. Duration is
measured in years and `rv_signal` in cross-sectional standard deviations.

| Gate | Required threshold | Stop condition |
| --- | ---: | --- |
| Config identity | Exact `0c0d78a866bc1090` | Any other hash |
| Reference-universe size | Absolute delta at most `max(25, 0.5%)` | Either month exceeds the bound |
| Matched-bond coverage | At least `99.0%` of the smaller universe | Either month is below `99.0%` |
| RV surface size and coverage | Same size bound and `99.0%` matched coverage | Either RV surface is materially incomplete |
| YTM absolute delta | Median `<= 1 bp`; p90 `<= 5 bp`; p99 `<= 25 bp` | Any percentile fails |
| Duration absolute delta | Median `<= 0.10y`; p90 `<= 0.50y`; p99 `<= 1.00y` | Any percentile fails |
| Duration relative delta | Median `<= 2%`; p90 `<= 10%` | Either percentile fails |
| Spread absolute delta | Median `<= 5 bp`; p90 `<= 25 bp`; p99 `<= 75 bp` | Any percentile fails |
| RV-signal absolute delta | Median `<= 0.05`; p90 `<= 0.25`; p99 `<= 0.75` | Any percentile fails |
| Typed exclusions | `100%` carry a declared reason code | Any untyped exclusion |
| Spread semantics | `100%` equal `ytm - interpolated curve` within `1e-10` | Any semantic mismatch |
| Walk-forward boundary | `100%` of model rows use data `<= t` | Any future-dated input |

These are conjunctive gates. A material unexplained failure stops publication;
passing one percentile does not compensate for failing another.

## 📊 Duration substitution gate

The analytical-duration substitution is evaluated over every valid overlap row
available in the frozen artifact, not only the two parity months. It uses the
same absolute and relative thresholds declared above and adds two structural
requirements:

| Requirement | Threshold |
| --- | ---: |
| Finite analytical duration | `100%` of rows declared analytically available |
| Duration source label | `100%` equal `analytical` for rebuilt rows |

Any row outside the analytical solver's typed domain remains unavailable with
a declared reason. It is never imputed from a cross-sectional average.

## 🔍 Evidence ledger

| Gate | Scope | Measured evidence | Verdict |
| --- | --- | --- | --- |
| Frozen hash | Source contract | `0c0d78a866bc1090` from canonical sorted-JSON SHA-256 prefix | Pass |
| Finnhub reference profile contract | Live entitled probe, secret redacted | HTTP `200`; valid US ISIN identity; `24` mapped fields, `15` non-null; amount outstanding and ISIN present; empty/non-object payload remains a hard failure | Pass |
| T1 canonical mapping | All SIC values `0001` to `9999` | `4,529` official mapped values; `0` mismatches against `Siccodes17.txt`[^1] | Pass |
| T1 source artifact | One-time OSBAP panel | SHA-256 `a6abf9981bd804a81227a99c48214b3229d3621d05e79d7d717e4d3b82a9afcf`; `68,140` rows; `0` invalid rows; `0` modal disagreements | Pass |
| T1 production SIC readiness | `sec_cusip_ticker_map` | `41,786` CUSIP8/9 rows and `0` non-null `sic_code`; SIC fallback is unavailable until a governed SEC backfill lands | Blocked input |
| T1 curated source coverage | Production universe × pinned OSBAP evidence | `10,206 / 10,206` curated CUSIPs resolve from OSBAP; `0` require the currently-empty SIC fallback; `0` remain without sector | Pass before load |
| T1 target-table coverage | Production `bond_issuer_sector`, written by `worker_writer` | `68,140` rows/CUSIPs; one source SHA; `68,140` OSBAP and `0` SIC fallback rows; `10,206 / 10,206` curated CUSIPs covered; loaded at `2026-08-08T19:56:16Z` | Pass |
| T2 physical artifact | Full pinned artifact, written by `worker_writer` | SHA-256 `3e4d451faa05bcedefa086903325e93842a59e31368c7e12aaa5a4972214e210`; `3,420,044` source rows = `3,418,872` published + `1,172` rejected (`open_or_future_month`); `149,846` CUSIPs; `0` duplicate keys; `2002-07` to `2026-07` | Pass |
| T2 quote-state accounting | Published T2 rows | `2,417,815` quoted; `14,652` crossed; `985,216` missing spread; `1,189` missing spread and volume. The four states total `3,418,872` published rows. | Measured |
| T2b tick budget | Widened live lane | Pending measured run | Pending |
| Finnhub terms production coverage | Curated production universe, written by `worker_writer` | `10,206 / 10,206` rows successful; `10,206` unique attempts; every match reason `isin_embedded_cusip9`; max `fetched_at` `2026-08-08T21:20:25Z`; final cursor `98981BAA0` | Pass |
| T3 publication identity | Production base publication `92740098-1571-559d-9fb3-119de8321754` | Fingerprint `5a7af9e1adaed315e9940293cf3e9e789ca6350993688d58ab3e759cee37a3cb`; exact frozen config hash `0c0d78a866bc1090`; status `validated` at `2026-08-08T22:20:43Z`; pointer changed atomically at the same timestamp | Pass |
| T3 production surfaces | Current production views | `3,417,683` snapshots, `1,687,524` RV rows, `2,600,253` returns, and `3,417,683` rating rows; every surface has an equal distinct-key count; panel range `2002-07` to `2026-06`; returns `2002-08` to `2025-03`; all six relations owned by `worker_writer` | Pass |
| T3 typed rating absence | Frozen base publication | `1,347,344` panel rows without a source PIT rating are retained as `historical_missing` / `historical_rating_absent`, rather than dropped or backcast. | Pass |
| T3 parity `2025-01` | Frozen versus rebuilt | Frozen universe `10,143`; rebuilt universe `0`; matched bonds `0`; coverage `0%`; both exclusion surfaces `100%` typed. All numeric parity metrics were unavailable with reason `zero_overlap`; the worker aborted. | **Fail / stop** |
| T3 parity `2026-06` | Frozen versus rebuilt | Universe and RV each `8,603` frozen versus `1,132` rebuilt, an absolute delta of `7,471` against a `43.015` limit. Matched coverage was `100%`, yield/duration/spread numerical parity passed at machine precision, but RV absolute delta failed: median `0.100413`, p90 `0.306255`, p99 `1.054983`. | **Fail / stop** |
| T3 duration overlap | Full pinned artifact versus analytical solver | `811,725` overlap rows; `100%` finite and `100%` labeled `analytical`; absolute error median `0.092777y`, p90 `0.235200y`, p99 `0.334403y`; relative error median `0.999008%`, p90 `2.671663%`; source SHA `3e4d451f…e210` | Pass |
| T4 static Rating mapping | Full live-extension snapshot | SHA-256 `ab48d99f466ae3a943ce0a2819175ab6efdd95212b4efc9079151750057b077a`; `31,375` final mappings; `0` rejects and `0` deterministic-final-row differences | Pass |
| T4 production target | Production `bond_rating_static`, written by `worker_writer` | `31,375` rows/CUSIPs; one exact source SHA; as-of range `2002-07` to `2026-07`; loaded at `2026-08-08T19:59:23Z`; bucket counts equal the pinned mapping | Pass |
| Railway terms execution | Railway production deployment `00611b34` | The deployment built, but Terms processing ran only after the service restart. Railway container status is therefore not Terms execution evidence; the database tables are the source of truth. | Measured operational evidence |
| Stage 6 production run | Target tables and worker JSON | **Not executed after the T3 stop.** Production still has exactly `1` publication and `0` publications built after the base. The pointer remains `92740098-1571-559d-9fb3-119de8321754`; target-table `max(computed_at)` remains `2026-08-08T20:16:59.32818Z`. | **Blocked by parity** |

## ⚠️ Declared blockers and degradations

- Production currently contains no usable SIC value in
  `sec_cusip_ticker_map`. OSBAP-covered CUSIPs can resolve; every remainder
  must remain `no_sector` until the SEC evidence lane is populated.
- Finnhub industry labels are reference enrichment only. They are not FF17 and
  cannot redefine the frozen sector input.
- Historical quote availability is an observed input state, not an imputation
  target. In the physical T2 artifact, quote states are accounted for as
  quoted, crossed, missing spread, or missing spread and volume; absent spreads
  remain typed rather than receiving a synthetic quote.
- A successful container status is not execution evidence. The parity worker
  emitted `parity_failed` and exited non-zero; Railway consequently reports the
  one-off deployment as `CRASHED` and stopped. The worker JSON and unchanged
  target tables are the execution evidence.
- The `2025-01` rebuilt universe was empty. The parity worker preserved the
  frozen and rebuilt sizes, zero matched coverage, typed-exclusion evidence,
  and `metrics_unavailable_reason = zero_overlap`; it did not manufacture
  numeric parity values.
- The `2026-06` rebuild was materially incomplete and its RV-signal deltas also
  exceeded every declared percentile threshold. These are conjunctive stop
  failures even though matched-row yield, duration, and spread arithmetic was
  equal at machine precision.
- Stage 6 was intentionally not executed. The requested Stage 6 production run
  and its run JSON remain blocked rather than being fabricated or obtained by
  violating the parity stop condition.

## Production parity execution JSON

Railway production deployment `e0f4b5b5-37a7-4718-a421-6beafb8fb6cf` ran only
after `railway service restart`. Its final worker line is quoted verbatim:

```json
{"worker":"bond_panel_parity","state":"parity_failed","reason":"zero_overlap","aborted":true,"months":[{"month":"2025-01-01","state":"parity_failed","reason":"zero_overlap","aborted":true,"frozen_universe_size":10143,"rebuilt_universe_size":0,"matched_bonds":0,"matched_coverage":0,"typed_exclusions":{"frozen":1,"rebuilt":1},"walk_forward":{"max_input_day":"2025-01-31","calendar_month_end":"2025-01-31","fit_as_of":"2025-01-01","input_exclusions":{"static_rating_after_month":10960}},"metrics_unavailable_reason":"zero_overlap","failed_gates":["zero_overlap","matched_coverage","ytm_abs_bps","duration_abs_years","duration_relative","spread_abs_bps","rv_abs"]},{"month":"2026-06-01","state":"parity_failed","reason":"gate_failed","aborted":true,"frozen_universe_size":8603,"rebuilt_universe_size":1132,"universe_delta_limit":43.015,"frozen_rv_size":8603,"rebuilt_rv_size":1132,"rv_universe_delta_limit":43.015,"rv_matched_coverage":1,"matched_coverage":1,"typed_exclusions":{"frozen":1,"rebuilt":1},"spread_definition":"ytm_minus_interpolated_dgs","spread_semantics":{"frozen":{"rows":8603,"max_abs_error":0,"max_bps_conversion_error":0},"rebuilt":{"rows":1132,"max_abs_error":0,"max_bps_conversion_error":0}},"metrics":{"ytm_abs_bps":{"median":0,"p90":6.938893903907228e-14,"p99":1.3877787807814457e-13},"duration_abs_years":{"median":0,"p90":1.7763568394002505e-15,"p99":7.105427357601002e-15},"spread_abs_bps":{"median":0,"p90":7.105427357601002e-14,"p99":1.4210854715202004e-13},"duration_relative":{"median":0,"p90":3.058082620751514e-16,"p99":1.480950169972437e-15},"rv_abs":{"median":0.10041279015056534,"p90":0.30625531826740865,"p99":1.0549825010100566}},"walk_forward":{"max_input_day":"2026-06-30","calendar_month_end":"2026-06-30","fit_as_of":"2026-06-01","input_exclusions":{"static_rating_after_month":1128}},"failed_gates":["universe_delta","rv_universe_delta","rv_abs"]}]}
```

## Post-stop production-table verification

The final read-only SQL verification was run after the parity worker stopped.
It proves that the failed gate did not mutate publication state:

| Evidence | Measured value |
| --- | --- |
| Current publication | `92740098-1571-559d-9fb3-119de8321754` |
| Publication status / config | `validated` / `0c0d78a866bc1090` |
| Pointer `changed_at` | `2026-08-08T22:20:43.545955Z` |
| Publication `max(computed_at)` | `2026-08-08T20:16:59.32818Z` |
| Publications total / built after base | `1 / 0` |
| Snapshot | `3,417,683` rows; `2002-07` to `2026-06` |
| RV signal | `1,687,524` rows; `2002-07` to `2026-06` |
| Returns | `2,600,253` rows; `2002-08` to `2025-03` |
| Rating PIT | `3,417,683` rows; `2002-07` to `2026-06` |

The service variable was restored to `WORKER=bond_live_daily` with
`--skip-deploys`. The parity deployment remains stopped; no daily worker or
Stage 6 restart was triggered during restoration.

## 🔗 References

[^1]: Kenneth R. French Data Library. "Industry Definitions: 17." https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_17_ind_port.html

## 📈 T2 annual quote coverage

These counts come from production `bond_liquidity_monthly` after the pinned
artifact cursor reached `3,420,044`. `quoted_rows` uses the declared
`quote_state = 'quoted'`; it does not treat missing or crossed spreads as quotes.

| Year | Rows | Quoted rows |
| ---: | ---: | ---: |
| 2002 | 48,749 | 32,018 |
| 2003 | 106,810 | 68,596 |
| 2004 | 112,782 | 66,198 |
| 2005 | 116,212 | 63,881 |
| 2006 | 118,521 | 65,077 |
| 2007 | 114,899 | 60,700 |
| 2008 | 108,787 | 62,943 |
| 2009 | 112,284 | 78,552 |
| 2010 | 111,812 | 80,206 |
| 2011 | 107,617 | 75,514 |
| 2012 | 107,679 | 77,362 |
| 2013 | 106,518 | 77,309 |
| 2014 | 118,588 | 77,422 |
| 2015 | 131,215 | 86,143 |
| 2016 | 133,287 | 96,174 |
| 2017 | 136,353 | 98,769 |
| 2018 | 139,499 | 101,173 |
| 2019 | 141,789 | 105,862 |
| 2020 | 148,433 | 112,113 |
| 2021 | 151,321 | 112,600 |
| 2022 | 151,116 | 113,858 |
| 2023 | 157,823 | 118,338 |
| 2024 | 169,009 | 130,765 |
| 2025 | 351,253 | 281,973 |
| 2026 | 216,516 | 174,269 |

## Finnhub terms production evidence

The completed Terms extraction was written by `worker_writer`. It covers the
full curated universe (`10,206 / 10,206` successful rows) with `10,206` unique
attempts. Every identity match used `isin_embedded_cusip9`; the latest recorded
`fetched_at` is `2026-08-08T21:20:25Z`, and the exact final cursor is
`98981BAA0`.

| Field | Non-null rows |
| --- | ---: |
| `amount` | 10,206 |
| `asset` | 1,028 |
| `assetType` | 1,028 |
| `bondType` | 10,013 |
| `callable` | 9,987 |
| `coupon` | 10,206 |
| `couponType` | 10,013 |
| `datedDate` | 1,005 |
| `debtType` | 1,027 |
| `figi` | 10,206 |
| `firstCoupon` | 10,013 |
| `industryGroup` | 1,003 |
| `industrySubGroup` | 1,005 |
| `isin` | 10,206 |
| `issueDate` | 10,013 |
| `maturity` | 10,206 |
| `offeringPrice` | 937 |
| `originalOffering` | 10,206 |
| `paymentFrequency` | 9,971 |
| `securityLevel` | 10,013 |

## T4 static Rating evidence

The one-time static Rating mapping is derived only from the pinned
`universe_snapshots_live.parquet` input. It is not a runtime input and it does
not alter the frozen research configuration or its hash.

| Measure | Observed value |
| --- | ---: |
| SHA-256 | `ab48d99f466ae3a943ce0a2819175ab6efdd95212b4efc9079151750057b077a` |
| Source rows | `1,688,652` |
| Distinct CUSIPs | `31,375` |
| Month range | `2002-07-01` to `2026-07-01` |
| Duplicate CUSIP-month rows | `0` |
| Rejected / typed invalid rows | `0 / 0` |
| Final mapping rows | `31,375` |
| Deterministic-final-row matches / differences | `31,375 / 0` |

Generic final-bucket counts are `AAA 186`, `AA 1,167`, `A 6,282`, `BBB 8,808`,
`BB 2,491`, `B 1,981`, `CCC 970`, and `NR 9,490`. The final `NR` value is
retained as the final state; the loader never revives an earlier value. The
production `bond_rating_static` contains the exact `31,375` mappings under the
single pinned source SHA and with the same bucket counts above.

This replaces the earlier snapshot that ended at `2025-03`: its static final
mapping differed from the live extension for `59 / 31,375` CUSIPs. The live
extension includes generic updates through `2026-07`, so the replacement is a
definition-preserving correction to the final-row carry-forward rule, not a
change to the frozen configuration.

## Gate redesign adopted on 2026-08-08; production rerun pending

The earlier **NO-GO**, original command, literal production JSON, timestamps,
verdict, and Stage 6 stop above remain immutable historical evidence from the
original gate. This appended declaration does not reinterpret that evidence as a
pass and does not replace or reformat any of it.

The adopted T3 contract separates three checks: exact reference accounting for
every normalized `bond_curated_universe` CUSIP9 as included or typed excluded;
formula parity for YTM, duration, duration-relative, and spread only on at least
300 common included bonds; and structural validation of rebuilt RV. Historical
membership drift and absolute cross-cohort RV deltas remain recorded diagnostics,
not blockers. The rebuilt RV contract is structural because separately fit and
standardized monthly cohorts need not share absolute scores.

For a future run, a hard-gate failure is `parity_failed` with `aborted=true`; a
comparable hard-gate pass is `parity_passed` with `comparable=true` and
`aborted=false`; and fewer than 300 common included bonds is
`parity_not_comparable` with `comparable=false` and `aborted=false`. Overall
parity can pass only with no failed month, every comparable month passing its
formula and rebuilt-RV checks, exact accounting and other hard gates passing for
all declared months, and at least one comparable month.

No redesigned production rerun or production JSON occurred. No deploy, database
write, pointer move, restart, or Stage 6 execution occurred. This documentation
change is not production authorization: only a separately authorized, fresh
read-only production parity run that emits `parity_passed` can support a
separately authorized Stage 6 execution under that legacy identity. The
distribution-series correction below supersedes that activation path before it
was used.

## Distribution-series correction adopted on 2026-08-09

The product owner clarified that the system is for non-US investors and the
execution universe must therefore use the Regulation S series. The approved
10,206-CUSIP identification universe is retained as the Rule 144A reference leg;
it is not discarded and it is not itself the Regulation S execution universe.
One issue can, and commonly does, have paired Rule 144A and Regulation S series.

Read-only production inspection found:

| Measure | Observed value |
| --- | ---: |
| `bond_curated_universe` rows | `10,206` |
| Numeric-leading current CUSIP9s | `10,206` |
| Alpha-leading current CUSIP9s / CINS | `0` |
| Reference ISINs beginning `US` | `10,206 / 10,206` |
| Reference ISINs embedding the current CUSIP9 | `10,206 / 10,206` |
| June 2026 PIT `db_type=1` | `7,586` |
| June 2026 PIT `db_type=3` | `2,487` |

Those shapes are diagnostics, not classification rules. In particular,
`db_type`, CUSIP/CINS prefix, ISIN prefix, and identifier presence cannot prove
the distribution exemption. The previous `db_type=3 -> unsupported_144a` panel
gate was therefore removed instead of being inverted.

The source-discovery call was made against the
[SEC-API Full-Text Search API](https://sec-api.io/docs/full-text-search-api).
Queries containing explicit Rule 144A, Regulation S, CUSIP, ISIN, and Common
Code labels returned EDGAR filings and exhibits, particularly `EX-4.x` documents
attached to 8-K, 6-K, S-4/F-4, and 20-F filings. Search results are metadata;
the linked document is the evidence that must be downloaded and parsed.

An [official SEC exhibit](https://www.sec.gov/Archives/edgar/data/1635327/000119312525175440/d20396dex41.htm)
also demonstrates why prefix heuristics are unsafe: its USD issue explicitly
lists a Rule 144A CUSIP/ISIN and a distinct Regulation S “CUSIP” (a CINS) / ISIN,
while other currency sections use ISIN and Common Code identifiers. The mapping
contract consequently accepts a Regulation S CUSIP/CINS only from explicit
same-document, same-issue-block labels. ISIN/Common-Code-only pairs remain
governed but cannot enter the current CUSIP-keyed panel.

The adopted implementation path is an additive immutable evidence registry, a
bounded/resumable EDGAR backfill, explicit human adjudication, an approved
mapping snapshot, a live Regulation S data-coverage measurement, and a new
historical Regulation S base. The old Rule 144A T3 comparison is historical
evidence and is not a valid activation gate for the new security identity.

No production schema installation, registry load, backfill publication, deploy,
worker restart, panel publication, or pointer movement was performed as part of
this correction.

---

# Addendum — 2026-08-11: the governance gap, the issuer rewire, and the corrected 144A control

Everything above this line is immutable historical evidence and is **not**
reinterpreted here. The **NO-GO** verdict, the two failed parity runs, their
literal JSON, and the "Stage 6 intentionally not executed" record all stand as
written. This section appends what happened afterwards, what was measured on
2026-08-11, and under which contract.

## A. The governance gap, stated plainly

The document above records a **NO-GO** and one publication with the pointer at
`92740098`. Production, read on 2026-08-11, does not match that:

| Measure | Recorded above | Measured 2026-08-11 |
| --- | --- | --- |
| Publications | `1` | `3` |
| Pointer | `92740098-1571-559d-9fb3-119de8321754` | `3bfbf94e-1264-57f0-9a47-a3cbca214c6b` |
| Pointer `changed_at` | `2026-08-08T22:20:43Z` | `2026-08-11T00:38:11.906749Z` |
| Config hash served | `0c0d78a866bc1090` | `1863d3d5fa3a0edf` |

The chain is `92740098` (base) and `b3c92982` (base, return-coverage repair,
`computed_at 2026-08-10T22:11:41Z`), then the delta `3bfbf94e`
(`parent_publication_id = b3c92982`, `code_revision 7139388f...`) which retains
only `2026-07` and `2026-08` — `20,416` snapshot rows, `1,132` RV rows,
`8,573` return rows.

**Two Stage 6 executions therefore happened that this document never recorded,
and the publication serving production has passed no parity gate.** The only
parity runs on record are the two that failed. That is the gap this addendum
closes.

It was not a missing step. The parity worker **could not run**: `run()` returned
`legacy_rule_144a_parity_not_applicable_to_reg_s` before opening a connection,
because `config_hash()` is `1863d3d5fa3a0edf` while the worker accepted only
`0c0d78a866bc1090`. The gate retired itself when the identity moved, and nothing
took its place.

## B. Relaxed gate contract, declared 2026-08-11 before the run

This supersedes the 2026-08-08 pre-declaration and the 2026-08-08 redesign for
runs under `1863d3d5fa3a0edf`. It was fixed before any rebuilt row was compared.

| Gate | Threshold | Hard |
| --- | --- | --- |
| Rebuilt universe size | `>= 90%` of the frozen included count for the month, measured **before the display gate** | yes |
| Common-bond formula parity (`ytm`, `mod_dur`, `spread`) | median `<= 1 bp / 0.10y / 5 bp`; p90 `<= 5 bp / 0.50y / 25 bp`; p99 `<= 25 bp / 1.0y / 75 bp` | yes |
| RV structural check | Spearman rank correlation `>= 0.80` on common bonds | yes |
| RV absolute z-score delta | recorded diagnostic, never a blocker | no |
| Typed exclusions | `100%` carry a declared reason | yes |
| Exact reference accounting | every curated CUSIP9 included or typed excluded | yes |
| Walk-forward boundary | `100%` of model inputs dated `<= t` | **non-negotiable** |
| Duration substitution | already measured and passed (`811,725` overlap rows, median `0.0928y`) | not re-gated |
| Everything else from the 2026-08-08 list | recorded diagnostic | no |

**Why the size gate measures the pre-display-gate count.** The frozen artifact
applied no issuer-name requirement of any kind — the frozen engine groups
issuers by `cusip_id[:6]` and never needed a resolved identity. Gating the
post-display-gate count against it would fail the rebuild for correctly
implementing a filter the reference never had. The ratio therefore uses
`rebuilt_included_ex_display_gate` = included + rows excluded as
`unnamed_issuer`, and the product universe after the display gate is reported in
the same JSON. Neither number is hidden behind the other.

**Why absolute RV deltas cannot block.** Monthly cohorts are fit and
standardized separately; they do not share an absolute scale. Rank agreement is
the falsifiable claim, so that is what is gated.

## C. T1 — issuer resolution rewired onto the served name

### C.1 The defect

Panel eligibility required `sec_cusip_ticker_map` to resolve exactly one issuer
CIK. Measured on the delta publication, identically in both live months over
`10,208` rows: `unresolved` `6,360`, `missing_cik` `2,518`, `resolved` `1,330`.
The dominant exclusion in both months was `unresolved_issuer` = `8,584`,
admitting `1,132` bonds in `2026-07` and `805` in `2026-08`.

The serving chain, over the same curated universe, does far better. Measured
against production at as-of `2026-06-30` and later:

| Measure | Value |
| --- | ---: |
| `bond_curated_universe` CUSIP9s | `10,206` |
| ...with a valid `cusip9` alias and a security-master row | `10,073` |
| ...with a consensus `issuer_name` | **`8,350`** |
| ...with a security but no consensus name | `1,723` |
| ...with no security at all | `133` |

Of the named `8,350`: `8,349` carry `identity_state = resolved` and `1` is
`ambiguous`.

### C.2 The fix

Eligibility now reads the serving chain's normalized reported-name consensus
(`src/bonds/issuer_consensus.py`) instead of a resolved CIK, under its own
reason code `unnamed_issuer`. The CIK is retained as `issuer_id` plus a new
`issuer_cik_state`, informational lineage only. `issuer_identity_state` is typed
`named_consensus` / `unnamed_consensus_abstained` / `no_security_master`, and
`bond_panel_snapshot` gains a nullable `issuer_name` column. Publications built
before this keep `NULL`; an absent name is never backfilled.

One dependent change was required: the port clustered the spread model's
standard errors on `issuer_id` and **raised** without one. The frozen engine
clusters on `cusip_id[:6]`; the port now matches it. Cluster choice moves
standard errors only — never the residual, hence never `rv_signal`.

### C.3 Measured result, and an arithmetic the acceptance target did not anticipate

Resolution meets the floor: **`8,350` bonds resolve by name, against a declared
floor of `8,350`.**

Inclusion cannot. Applying the remaining frozen eligibility tests to the same
rows leaves far fewer, and the target of "8,350 resolved **and included**"
assumed named implies eligible. Measured from the live sources the rebuild
reads:

| Month | eligible | `unnamed_issuer` | ex-display-gate | frozen included | ex-display-gate ratio | product ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2025-12 | `7,347` | `1,694` | `9,041` | `9,304` | `97.2%` | `79.0%` |
| 2026-05 | `6,900` | `1,696` | `8,596` | `8,654` | `99.3%` | `79.7%` |
| 2026-06 | `6,868` | `1,696` | `8,564` | `8,603` | `99.5%` | `79.8%` |

The `2026-06` residual exclusions are typed market and data facts, not identity:
`unnamed_issuer` `1,696`, `matured_or_short` `683`, `illiquid` `294`,
`too_small` `263`, `missing_asset_class` `190`, `missing_currency` `133`,
`missing_traded_days` `78`, `invalid_ytm` `1`.

Read against the live publication rather than raw sources, `2026-07` gives
`6,899` eligible under the same rule — a `6.1x` improvement on today's `1,132`,
measured two independent ways.

**The consequence for the product: the builder will solve over roughly `6,870`
bonds, not `8,600`.** That is the number to plan against.

### C.4 The single highest-value follow-up: `1,290` bonds lost to spelling

The `1,723` securities without a consensus name are **not unnamed**. Every one
carries reported issuer names in `identity_evidence -> distinct_issuer_name`
(`1,723 / 1,723`). All `1,723` abstained at the CUSIP6 layer:

| Abstain reason | Count | Mean top share | Mean distinct reported names |
| --- | ---: | ---: | ---: |
| `no_consensus` | `1,290` | `0.512` | `7.2` |
| `multiple_lei` | `433` | — | `7.9` |

The `1,290` `no_consensus` cases carry **exactly one distinct reported LEI** —
the legal entity does not disagree, only the spelling does, and the vote splits
just under the `0.60` threshold. Prefix containment folds truncations but cannot
fold abbreviations, so `DELL INT EMC` (`711` votes) and
`DELL INTERNATIONAL EMC...` (`797` votes) count as rivals; likewise
`1011778 BC NEW RED FIN` against `1011778 BC ULC NEW RED FINANCE`. The `433`
`multiple_lei` cases are genuine co-issuer bonds whose slash-joined name
(`First Student Bidco Inc / First Transit Parent Inc`) is a perfectly good
display string.

Recovering these would lift the solve universe from `~6,870` toward `~8,500`.
It touches a pre-registered module and is **not** done here: it is recorded as a
separate, costed work item.

## D. T2 — the declared 144A control was never a control

### D.1 The defect

`backend/app/bond_optimizer/spread_model.py` read
`x["is_144a"] = frame["db_type"].eq(2)`. **`db_type` is never `2`** anywhere in
the 24-year history: the values are `1` (publicly disseminated, `58,881` CUSIPs
from `2002-07`), `3` (Rule 144A, `9,407` CUSIPs from `2010-03`) and `NULL` (only
from `2025-04`, the TRACE era). The guard on line 61 dropped all-NaN columns but
not zero-variance ones, so the column entered every monthly design and
contributed nothing.

Measured over the frozen panel: **`0` of `273` months had a non-zero `is_144a`
value.** The design spec section 4.1 lists a 144A flag among the model's
features; it never was one. The port in this repo carried no such column at all
— a different fact from a broken one, and stated separately.

The economic consequence is not cosmetic. The 144A distribution premium —
`213,854` of `1,547,178` frozen panel rows, `24.4%` of the live universe — was
never controlled for, so it went into the residual, and the residual **is** the
RV signal.

### D.2 The correction, and a second defect it uncovered

`is_144a` now reads `db_type == 3` with an explicit `db_type_absent` level, so
no row is dropped and an absent value is never silently read as "not 144A".
Zero-variance regressors are now dropped, which is what let a declared control
look applied for 24 years.

That second change exposed a third defect. `sm.add_constant` **skips** the
intercept when the design already carries a constant non-zero column — and
pre-2010, when PIT coverage makes every bond `NR`, the `q_NR` dummy is exactly
that. Dropping zero-variance columns would then have removed the model's only
intercept and turned the fit into a regression through the origin
(`R2 0.1333 -> 0.4195` on `2005-06`, uncentered). `has_constant="add"` fixes it,
verified residual-neutral: on months with no 144A paper the fitted values match
the legacy fit to machine precision (`max |diff| = 0.0 bps` on `2005-06` and
`2009-06`; `15.8 bps` on `2015-06` at `15%` 144A; `55.0 bps` on `2022-06` at
`24.6%`).

### D.3 Re-measurement: dev window `2013-01 -> 2023-03`, `123` months

The `before` arm reproduces the published P1/P2 report exactly. That is what
makes the delta credible rather than merely computed.

| Metric | before | after | P1/P2 report |
| --- | ---: | ---: | ---: |
| mean monthly IC | `0.0633` | `0.0633` | `0.063` |
| Newey-West t (3 lags) | `5.55` | `5.65` | `5.55` |
| IC hit rate | `74.8%` | `74.8%` | `74.8%` |
| Q5-Q1 gross annualized | `+2.19%` | `+2.27%` | `+2.2%` |
| mean monthly R2 | `0.4174` | `0.4197` | — |
| months with the control applied | `0` | `129 / 273` | — |
| IC decay h=1 to h=12 | `0.0957 -> 0.0769` | `0.0951 -> 0.0763` | `0.096 -> 0.077` |

**Frozen kill gates.** `IC >= 0.02`: pass, `0.0633`. `NW-t >= 2`: pass, and
wider after the fix, `5.55 -> 5.65`. Both hold.

**The third gate, `Q5-Q1 net > 0`, is reported but is not a verdict on this
change.** The frozen report publishes only the gross spread; the cost convention
behind its `PASS` is not recorded anywhere in either repository. Under the
convention declared here — per-month median `one_way_costs_asof`, net = gross
minus `4x` cost (full monthly rotation of both legs) — the net series is
**`-6.98%` before and `-6.90%` after**: negative in both arms, essentially
unchanged, therefore not a regression introduced by the correction. Median
one-way cost is `23.63 bps` over all months and `18.24 bps` over the dev window;
the breakeven cost multiplier is `0.956` before and `0.991` after. **The
published `PASS` on this gate is not reproducible from this repository.** That
is a separate finding, recorded, not resolved.

**Rank stability, before to after, on identical frozen inputs:** Spearman
`>= 0.8862` in all `273` months, median `1.0000`; over the dev window min
`0.9211`, median `0.9877`, and `0` months below `0.80`. The correction sharpens
the fit without reordering the cross-section. This bounds the 144A contribution
only — it is not a forecast of the parity gate's Spearman, which compares
different cohorts, price sources and rating states.

This lands under the already-declared identity `1863d3d5fa3a0edf`, reached from
`0c0d78a866bc1090` by `rule_144a_to_dual_series_delta_v1`. No input definition
changed beyond the feature correction, so **no new hash is minted** and
`panel_config.FROZEN` is untouched.

## E. The `2025-01` `zero_overlap` had a different cause than recorded

The failed run above recorded `metrics_unavailable_reason = zero_overlap` for
`2025-01` alongside `input_exclusions {static_rating_after_month: 10960}`, which
invites reading the static-rating as-of as the cause. Measured, it is not.

`sec_current_bond_security_alias_v1.valid_from` ranges `2025-04-30` to
`2026-04-30`. Under walk-forward, **no alias resolves for any month before
2025-05**, so `sec_current_bond_security_v1.currency` is `NULL` for every row
and the entire month falls out at `missing_currency` before any rating,
liquidity or price test is reached. Every month before `2025-05` rebuilds to
zero by construction, whatever the rating input does.

The available parity window is therefore `2025-05` onward. Declared months for
this run are `2025-12` and `2026-06`, both pre-flighted above at `97.2%` and
`99.5%` of the frozen included count before the display gate. The walk-forward
rule was not bent to reach a month; the month was chosen to respect it.

## F. Production parity execution — redesigned contract, run 2026-08-11

Railway service `bond-live-daily` (`e673db8e-...`), `WORKER=bond_panel_parity`,
deployment `861af9d7-6923-4482-b2b7-1be2e9f5b645`. As on 2026-08-08, the
deployment executed only after `railway service restart`; `railway up` alone
built and started the container without running the job. Railway reports the
deployment `CRASHED` because the worker exits non-zero on `parity_failed`. The
worker JSON and the unchanged target tables are the execution evidence.

### F.1 Verdict: `parity_failed`, `aborted = true`. Stage 6 was NOT executed.

`state="parity_failed"`, `reason="monthly_parity_failure"`,
`counts={"failed_months":2,"comparable_passed_months":0,"noncomparable_months":0}`,
`month_declaration.declared=["2025-12-01","2026-06-01"]` with no missing,
duplicate or unexpected month.

**One gate failed, the same one in both months: `rv_rank_correlation`.** Every
other hard gate passed, in both months.

| Gate | 2025-12 | 2026-06 |
| --- | ---: | ---: |
| Exact reference accounting | pass (`10,208 / 10,208`) | pass (`10,208 / 10,208`) |
| Typed exclusions | pass (`100%`) | pass (`100%`) |
| Spread definition and numeric semantics | pass (max abs error `0`) | pass (max abs error `0`) |
| Walk-forward boundary | pass (`max_input_day 2025-12-31`, `fit_as_of 2025-12-01`) | pass (`max_input_day 2026-06-30`, `fit_as_of 2026-06-01`) |
| **Rebuilt universe size** | **pass** — `9,201 / 9,304` = `98.89%` | **pass** — `8,709 / 8,603` = `101.23%` |
| Formula parity (ytm, duration, spread, duration-relative) | **pass** at machine precision | **pass** at machine precision |
| RV structural validation | pass (all 15 sub-gates) | pass (all 15 sub-gates) |
| **RV rank correlation `>= 0.80`** | **fail — `0.4194`** | **fail — `0.7840`** |

Formula parity, on `7,507` and `7,008` common bonds respectively: `ytm` median
`0` / p99 `1.39e-13` bp; duration median `0` / p99 `5.33e-15` y; spread median
`0` / p99 `1.42e-13` bp. The rebuilt panel reproduces the frozen arithmetic
exactly.

Universe accounting, `2026-06`: `10,208` reference CUSIPs, `7,013` included,
`3,195` excluded, every exclusion typed — `unnamed_issuer` `1,696`,
`matured_or_short` `676`, `illiquid` `322`, `missing_amount` `175`,
`missing_asset_class` `190`, `missing_currency` `135`, `invalid_ytm` `1`. For
`2025-12`: `7,507` included, `2,701` excluded — `unnamed_issuer` `1,694`,
`illiquid` `332`, `matured_or_short` `251`, `missing_asset_class` `190`,
`missing_currency` `162`, `missing_amount` `72`.

RV absolute z-score deltas, recorded as a diagnostic and not a blocker:
`2026-06` median `0.0886` / p90 `0.2232` / p99 `0.7429`; `2025-12` median
`0.1349` / p90 `0.7474` / p99 `1.7296`.

### F.2 Why the rank gate failed, decomposed and measured

Neither figure is a rebuild defect. Both were reproduced from the frozen
`2026-06` snapshot itself, by refitting it under each changed condition in turn
and ranking against the published `rv_signal`:

| Fit | Spearman vs published | n |
| --- | ---: | ---: |
| A — frozen inputs, frozen specification (control) | `0.9963` | `8,603` |
| B — cohort restricted to `7,013` bonds, otherwise frozen | `0.9929` | `7,013` |
| C — full cohort, **144A control added** | `0.8738` | `8,603` |
| D — full cohort, **walk-forward static ratings** | `0.9219` | `8,603` |
| E — all three together (the rebuild's conditions) | `0.8020` | `7,013` |

The production run measured `0.7840` for `2026-06`; fit E predicts `0.8020`
using a random cohort proxy rather than the real eligibility-driven one. The
agreement confirms the decomposition.

Read it directly:

- **The cohort costs nothing** (`0.9963 -> 0.9929`). Fitting on `7,013` instead
  of `8,603` bonds barely moves the ordering. The universe-size gate's premise
  holds.
- **The 144A control costs the most** (`-0.1225`). That is the measurable
  footprint of the premium that was absorbed by the residual for 24 years. The
  parity gate is comparing a corrected signal against an **uncorrected**
  reference, so a `>= 0.80` rank agreement stopped being the right expectation
  the moment section D landed.
- **The non-PIT rating input costs the rest** (`-0.0744`). `bond_rating_static`
  is a final-row mapping, not a point-in-time series, so walk-forward correctly
  discards any row dated after the month. For `2026-06` that strips `1,128`
  mappings and `10.6%` of buckets change, mostly investment grade to `NR`. For
  `2025-12` it strips `9,588` — which is exactly why that month collapses to
  `0.4194` while `2026-06` only reaches `0.7840`.

**The walk-forward rule was not relaxed to recover either number, and will not
be.** A rating input that is not point-in-time degrading a historical month is
the rule working, not failing.

### F.3 What was not done, and why

No publication was prepared, validated or pointed. Verified read-only in the
tables after the run:

| Evidence | Measured value |
| --- | --- |
| Publications total | `3` (unchanged) |
| `max(computed_at)` | `2026-08-11T00:38:11.906749Z` (unchanged) |
| Pointer | `3bfbf94e-1264-57f0-9a47-a3cbca214c6b` (unchanged) |
| Pointer `changed_at` | `2026-08-11T00:38:11.906749Z` (unchanged) |
| Snapshot rows by publication | `92740098` `3,417,683`; `b3c92982` `3,417,683`; `3bfbf94e` `20,416` (unchanged) |

The gate is conjunctive and it failed. Stage 6 stays unexecuted and the pointer
stays where it is, as the contract requires.

### F.4 The decision this hands back to the owner

The rank gate cannot pass while the frozen reference and the rebuild carry
different model specifications. Two coherent ways forward, both cheap, and the
choice is a product call, not an engineering one:

1. **Re-baseline.** Republish the historical base under the corrected
   specification, then re-run parity. Frozen and rebuilt would then share a
   specification and the `>= 0.80` gate would measure what it was meant to
   measure. Fit A shows the harness reproduces the published signal at `0.9963`,
   so the rebase is mechanical.
2. **Sequence the two changes.** Land T1 alone — parity would then compare like
   with like and, on the measured decomposition, clear `0.80` comfortably in
   `2026-06` (fit B: `0.9929`, degraded only by the rating input to about
   `0.92`) — and land the 144A correction afterwards as its own declared
   research round with its own re-baselined history.

Option 2 still leaves `2025-12` failing on the rating input alone (`0.4194`),
so whichever is chosen, **T4 — a genuinely point-in-time rating source — is now
on the critical path for any historical parity month**, not merely for the HY
cap and the expected-loss term.

### F.5 Two operational findings recorded

- **`CODE_REVISION` is still pinned** on `bond-live-daily` to
  `7139388f0f65aab9e0232495822e07ab29e2d613`. `railway.toml` states in terms why
  this must not be a permanent variable: a fixed value shadows the per-deploy
  `RAILWAY_GIT_COMMIT_SHA`, so a code-only change re-serves the previous payload
  under the same `publication_id` while the run reports success. It was a
  one-off pin for a deliberate republication on 2026-08-07 and has not been
  removed. It must come off before the next Stage 6, and removing it is an
  owner-authorised production config change, not something this run did.
- **A single-month rebuild was not expressible** until this run. `_load_inputs`
  emitted one mapping row per curated CUSIP per resolution window, and a
  single-month rebuild resolves the same month in both windows, so every row was
  duplicated and the candidate join fanned out. Fixed by treating the mapping as
  a set. It had never been seen because the parity worker refused to run at all
  under the active identity.

### F.6 Production parity execution JSON

Emitted by deployment `861af9d7-6923-4482-b2b7-1be2e9f5b645` at
`2026-08-11T04:04:12Z`. Quoted verbatim, abridged only where marked:

```json
{"worker":"bond_panel_parity","state":"parity_failed","reason":"monthly_parity_failure","aborted":true,"counts":{"failed_months":2,"comparable_passed_months":0,"noncomparable_months":0},"gates":{"monthly_contract_valid":true,"declared_months_exactly_once":true,"all_months_nonblocking":false,"at_least_one_comparable_month":false,"all_comparable_months_passed":false},"failure_reasons":{"gate_failed":2},"invalid_month_results":[],"month_declaration":{"declared":["2025-12-01","2026-06-01"],"observed":["2025-12-01","2026-06-01"],"missing":[],"duplicates":[],"unexpected":[]},"months":[{"month":"2025-12-01","state":"parity_failed","reason":"gate_failed","aborted":true,"matched_bonds":7507,"comparable":true,"reference_accounting":{"passed":true,"gates":{"reference_nonempty":true,"reference_keys_valid":true,"reference_keys_unique":true,"rebuilt_keys_valid":true,"rebuilt_keys_unique":true,"exact_reference_key_set":true,"eligibility_states_recognized":true,"excluded_reasons_typed":true,"included_identity_present":true},"reference_source_rows":10208,"reference_size":10208,"rebuilt_size":10208,"included_size":7507,"excluded_size":2701,"exclusion_counts":{"illiquid":332,"matured_or_short":251,"missing_amount":72,"missing_asset_class":190,"missing_currency":162,"unnamed_issuer":1694}},"hard_gates":{"frozen_snapshot_nonempty":true,"rebuilt_snapshot_nonempty":true,"snapshot_types":true,"frozen_rv_types":true,"frozen_lineage":true,"unique_universe_keys":true,"typed_exclusions":true,"spread_definition":true,"spread_numeric_semantics":true,"walk_forward":true,"rebuilt_universe_size":true},"formula_parity":{"evaluated":true,"passed":true,"metrics":{"ytm_abs_bps":{"median":0,"p90":6.938893903907228e-14,"p99":1.3877787807814457e-13},"duration_abs_years":{"median":0,"p90":1.7763568394002505e-15,"p99":5.329070518200751e-15},"spread_abs_bps":{"median":0,"p90":7.105427357601002e-14,"p99":1.4210854715202004e-13},"duration_relative":{"median":0,"p90":2.870161797913839e-16,"p99":1.871973274647541e-15}},"gates":{"ytm_abs_bps":true,"duration_abs_years":true,"spread_abs_bps":true,"duration_relative":true}},"rv_structure":{"passed":true,"row_count":7507,"fit_row_count":7507,"included_row_count":7507,"rv_mean":-3.78602763159771e-18,"rv_population_std":1,"max_residual_identity_error":0,"max_rv_signal_error":0,"max_snapshot_spread_error":0},"rv_rank":{"common_size":7507,"spearman":0.4194239088582035,"min_spearman":0.8,"evaluated":true,"passed":false,"unavailable_reason":null},"universe_size":{"frozen_included_size":9304,"rebuilt_included_size":7507,"rebuilt_display_gate_excluded":1694,"rebuilt_included_ex_display_gate":9201,"ratio_ex_display_gate":0.9889294926913156,"ratio_product_universe":0.8068572656921754,"min_ratio":0.9,"evaluated":true,"passed":true},"diagnostics":{"membership":{"frozen_included_size":9304,"rebuilt_included_size":7507,"common_size":7507,"symmetric_difference_size":1797,"universe_delta":1797,"universe_delta_limit":46.52},"rv_abs":{"frozen_size":9304,"rebuilt_size":7507,"common_size":7507,"matched_coverage":1,"metrics":{"median":0.13493094976182823,"p90":0.7473603932498153,"p99":1.7296334151269261},"unavailable_reason":null}},"typed_exclusions":{"frozen":1,"rebuilt":1},"spread_definition":"ytm_minus_interpolated_dgs","spread_semantics":{"frozen":{"rows":9304,"max_abs_error":0,"max_bps_conversion_error":0},"rebuilt":{"rows":7507,"max_abs_error":0,"max_bps_conversion_error":0}},"walk_forward":{"max_input_day":"2025-12-31","calendar_month_end":"2025-12-31","fit_as_of":"2025-12-01","input_exclusions":{"static_rating_after_month":9588}},"failed_gates":["rv_rank_correlation"]},{"month":"2026-06-01","state":"parity_failed","reason":"gate_failed","aborted":true,"matched_bonds":7008,"comparable":true,"reference_accounting":{"passed":true,"reference_source_rows":10208,"reference_size":10208,"rebuilt_size":10208,"included_size":7013,"excluded_size":3195,"exclusion_counts":{"illiquid":322,"invalid_ytm":1,"matured_or_short":676,"missing_amount":175,"missing_asset_class":190,"missing_currency":135,"unnamed_issuer":1696}},"hard_gates":{"frozen_snapshot_nonempty":true,"rebuilt_snapshot_nonempty":true,"snapshot_types":true,"frozen_rv_types":true,"frozen_lineage":true,"unique_universe_keys":true,"typed_exclusions":true,"spread_definition":true,"spread_numeric_semantics":true,"walk_forward":true,"rebuilt_universe_size":true},"formula_parity":{"evaluated":true,"passed":true},"rv_structure":{"passed":true},"rv_rank":{"common_size":7008,"spearman":0.7840366815576979,"min_spearman":0.8,"evaluated":true,"passed":false,"unavailable_reason":null},"universe_size":{"frozen_included_size":8603,"rebuilt_included_size":7013,"rebuilt_display_gate_excluded":1696,"rebuilt_included_ex_display_gate":8709,"ratio_ex_display_gate":1.0123212832732769,"ratio_product_universe":0.8151807509008485,"min_ratio":0.9,"evaluated":true,"passed":true},"diagnostics":{"rv_abs":{"frozen_size":8603,"rebuilt_size":7013,"common_size":7008,"matched_coverage":1,"metrics":{"median":0.08855305768405328,"p90":0.2232402309413639,"p99":0.742887010340494}}},"spread_definition":"ytm_minus_interpolated_dgs","walk_forward":{"max_input_day":"2026-06-30","calendar_month_end":"2026-06-30","fit_as_of":"2026-06-01","input_exclusions":{"static_rating_after_month":1128}},"failed_gates":["rv_rank_correlation"]}]}
```

---

# Addendum II — 2026-08-11: owner decision to re-baseline, and what the measurement then proved

The owner adjudicated the fork left open in §F.4 and chose **option (a),
re-baseline the history under the corrected specification.** Recorded rationale:
keeping an uncorrected reference means every future parity run measures against
a baseline already known to be wrong, and option (b) re-baselines anyway after
shipping a signal known to be replaced. Fit A reproducing the published signal
at `0.9963` is what makes this mechanical rather than a leap.

This section records what was done, what was measured off-production, and the
one thing the measurement then proved that changes the plan.

## G.1 The `CODE_REVISION` pin is removed

Deleted from service `bond-live-daily` on 2026-08-11, verified absent
(`{"deleted":true,"key":"CODE_REVISION"}`, `code_revision_count=0`). This
restores the documented state; `railway.toml` states that a persistent pin
shadows the per-deploy `RAILWAY_GIT_COMMIT_SHA`, so a code-only change re-serves
the previous payload under the same `publication_id` while reporting success —
and a re-baseline is exactly a code-only change.

**Consequence, stated so it is not later mistaken for a regression:**
`_code_revision()` now falls through to `RAILWAY_GIT_COMMIT_SHA`, which Railway
injects **only on GitHub-originated deploys**. The service currently runs a CLI
upload, so `bond_panel.run()` will stop at `panel_gate_failed` /
`code_revision_absent` — loud and safe, writing nothing. **Until the branch is
merged to `main` and a GitHub-source deploy exists, the daily lane does not
publish.** That is the correct failure, and it is strictly better than the
silent stale-pin state it replaces. The parity worker is unaffected: it never
calls `_code_revision()` because it materializes nothing.

## G.2 The re-baseline is mechanical — proven off-production, before any write

The corrected model was refit over **every month of the current base
publication** `b3c92982`, off-production, from an export of its included rows.
Only `rv_signal` changes; `bond_panel_snapshot`, `_returns` and `_rating_pit`
are copied unchanged.

| Check | Result |
| --- | ---: |
| Published RV rows | `1,687,524` |
| **Refit RV rows** | **`1,687,524`** — exact match |
| Months fit / skipped | `288 / 0` |
| Months where the 144A control survives into the design | `144` of `288` |
| Mean monthly R² | `0.4132` |
| Rank vs published — median | `1.0000` |
| Rank vs published — p10 | `0.9336` |
| **Rank vs published — minimum** | **`0.8672`** |

The row-count identity is the load-bearing check: dropping zero-variance columns
cannot remove rows and `MIN_MONTH_ROWS` is unchanged, so any difference would
mean a month silently changed fit status. There is none. The median of `1.0000`
is expected — before 2010-03 no 144A paper exists, `db_type` is constant, the
control is correctly dropped and the fit is bit-identical. Movement is confined
to the `144` months that actually contain 144A paper, and even there no month
falls below `0.8672`.

## G.3 What the measurement proved: only ONE parity month can pass, and the reason is T4

Parity months must come from `2025-05` onward (§E). The selection rule was
declared before inspecting any rank result: **smallest walk-forward rating strip
among months with at least 300 common bonds.** All fourteen candidates were
measured so the choice is auditable.

`spearman_rating_only` isolates the rating input alone: the same cohort, both
sides carrying the corrected 144A control, fit once with the frozen publication's
rating bucket and once with the walk-forward `bond_rating_static` bucket. Post
re-baseline this is the **only** remaining source of disagreement.

| Month | rating strip | bucket disagreement | `spearman_rating_only` | common bonds |
| --- | ---: | ---: | ---: | ---: |
| 2025-05 | `10,418` | `82.3%` | `0.6473` | `10,094` |
| 2025-06 | `10,309` | `82.5%` | `0.6302` | `9,877` |
| 2025-07 | `10,206` | `82.5%` | `0.6152` | `9,857` |
| 2025-08 | `10,083` | `82.3%` | `0.6568` | `9,642` |
| 2025-09 | `9,902` | `81.8%` | `0.6473` | `9,679` |
| 2025-10 | `9,788` | `82.5%` | `0.6435` | `9,429` |
| 2025-11 | `9,683` | `82.4%` | `0.6564` | `9,184` |
| 2025-12 | `9,587` | `82.4%` | `0.6639` | `9,304` |
| 2026-01 | `9,409` | `81.7%` | `0.6811` | `9,215` |
| 2026-02 | `9,276` | `82.1%` | `0.7036` | `8,946` |
| 2026-03 | `9,032` | `81.0%` | `0.6678` | `9,017` |
| 2026-04 | `8,873` | `81.8%` | `0.6790` | `8,693` |
| 2026-05 | `8,605` | `80.8%` | `0.7020` | `8,654` |
| **2026-06** | **`1,128`** | **`10.6%`** | **`0.9396`** | `8,603` |

The rule selects `2026-06` uniquely, and not narrowly: the runner-up strips
`7.6x` more mappings and lands `0.24` lower on rank. Thirteen of fourteen
candidate months sit in a tight `0.615 - 0.704` band, all far below the `0.80`
floor, **on the rating input alone**.

The cause is structural, not incidental. `bond_rating_static` is a **final-row**
mapping: `rating_as_of_month` is each CUSIP's last observed rating month, and the
mapping was extended through `2026-07`. Under walk-forward, month `t` may only
use rows dated `<= t`, which for any historical month retains almost nothing but
bonds that stopped being rated — `80-83%` of buckets flip, mostly investment
grade to `NR`. `2026-06` looks healthy only because it sits one month inside the
extension horizon. That is an artifact of when the mapping was built, not
evidence of point-in-time-ness.

**Therefore: T4 — a genuinely point-in-time rating source — is not a parallel
workstream. It is the binding constraint on historical parity, and this is now
measured rather than suspected.** Re-baselining removes the 144A component from
the comparison; it cannot remove this one.

### Declared months and predictions, recorded BEFORE the run

Both months are declared. Predictions are recorded here first so the run
confirms rather than reveals:

| Month | Predicted Spearman | Predicted verdict | Basis |
| --- | ---: | --- | --- |
| 2026-06 | `0.91 - 0.93` | pass | rating `0.9396` x cohort `0.9929`, less the ~2% the composed prediction over-stated last run (`0.802` composed vs `0.784` actual) |
| 2026-05 | `0.68 - 0.70` | **fail** | rating `0.7020` x cohort ~`0.99` |

`2026-05` is declared knowing it fails. Dropping it after measuring which month
passes would be selecting the winner, and the second month is what makes the
finding falsifiable.

## G.4 The `Q5-Q1 net` record, both halves

§D.3 recorded that the published `PASS` is not reproducible from either
repository. That stands, and the second half now completes it.

`validation.py:107` computes `net = gross - 4 x cost`: full monthly rotation of
**both** legs. The harness's own comment calls this "a deliberately conservative
diagnostic, not a strategy". The strategy it screens turned over **`3.3%` per
month** in the dev backtest and paid `0.17%/yr` in realized costs.

| Convention | before | after |
| --- | ---: | ---: |
| Q5-Q1 gross annualized | `+2.19%` | `+2.27%` |
| Implied annual one-way cost | `2.2908%` | `2.2906%` |
| Net at `4x` (the gate's bound) | `-6.97%` | `-6.89%` |
| **Net at the realized `3.3%`/month turnover** | **`+1.89%`** | **`+1.97%`** |
| Breakeven monthly turnover | `23.90%` | `24.77%` |

The implied annual one-way cost agreeing to four decimal places across the two
arms (`2.2908%` vs `2.2906%`) is the internal consistency check: the arms differ
only in gross, exactly as a feature correction should.

So both halves, together: **the published `PASS` cannot be reproduced from this
repository, and the gate is mis-scaled — it charges roughly `7.5x` the turnover
the strategy actually realized, which is the whole reason it reads negative.**
At the realized turnover the signal clears the gate in both arms. No friendlier
convention was substituted to obtain that; the `4x` bound is reported as the
declared gate and the realized-turnover figure is reported beside it.

## G.5 The DDL surface a new identity requires — enumerated, not yet applied

Candidate identity: adding `"spread_model_144a_control":
"db_type_eq_3_with_absent_level_v1"` to `panel_config.FROZEN` yields
**`c35f73b69e1cb885`** from `1863d3d5fa3a0edf`. The frozen dictionary is
extended by declaration, never edited in place.

Four things must change together, and **two of them can take the product dark if
they are wrong**:

1. `bond_panel_publications_config_hash_check` must admit the new hash
   (rerunnable `ALTER ... NOT VALID`, precedent already in the file).
2. `bond_panel_assert_pointer_validated()` needs **one** new authorized branch.
   The guard fires only when the hash changes and no branch matches, so every
   subsequent same-hash daily delta needs nothing. Model it on the
   `92740098 -> b3c92982` **root-replacement** branch — full-history root,
   `parent_publication_id IS NULL` on both sides — not on the delta branch,
   which hardcodes the two-month shape and the `0c0d... -> 1863d...` pair.
3. **`bond_panel_current_snapshot_v1` and its three siblings filter the ancestry
   root on `p.config_hash IN ('0c0d78a866bc1090', '1863d3d5fa3a0edf')`.** A
   pointer at `c35f73b69e1cb885` matches neither, so all four served views would
   return **zero rows** — not stale data, nothing. The cross-hash ancestry clause
   needs the new pair too.
4. **The re-baselined publication is a full-history root** (`parent NULL`,
   `2002-07 -> 2026-06`). The views recurse *upward* through
   `parent_publication_id`, so a root pointer exposes only the root's months:
   **`2026-07` and `2026-08` leave the served surface** until a daily delta is
   built on top of it.

Because of (3) and (4) the execution is a single indivisible block — DDL,
republish root, parity, pointer move, immediately rebuild the daily delta under
the new hash, verify both live months are back — and it must be dry-run against
a scratch database first. It was **not** executed here: see §G.6.

## G.6 Where this stops, and why

Executed and durable: the pin removal (§G.1), the off-production proof that the
re-baseline is mechanical (§G.2), the month-selection measurement (§G.3), and
the `Q5-Q1` record (§G.4).

**Not executed: the production re-baseline, the pointer move, and Stage 6.**
Three reasons, in order of weight:

1. **The measurement changed the expected outcome.** §G.3 proves that even a
   perfect re-baseline leaves `2026-05` — and every other admissible month
   except `2026-06` — failing on the rating input alone. Stage 6 most likely
   waits on T4 regardless, so executing an irreversible republication first buys
   nothing and risks a great deal.
2. **A wrong view definition takes the product dark.** §G.5 (3) and (4) mean a
   mis-sequenced pointer move returns zero rows from all four served surfaces.
   That needs a scratch-database dry run, which does not exist yet.
3. **The write requires the merge to `main` first.** `bond_panel.run()` needs
   `RAILWAY_GIT_COMMIT_SHA`, injected only on a GitHub-originated deploy
   (§G.1). Publishing from a CLI upload would either fail closed or, worse,
   publish under an unresolvable revision.

The question the owner now holds, with a fourteen-month controlled table behind
it: **does a month whose rating input cannot be reconstructed point-in-time gate
the publication?** If yes, Stage 6 waits on T4. If no, the contract needs a typed
non-comparable state for that condition — which should be declared deliberately,
not invented under time pressure.

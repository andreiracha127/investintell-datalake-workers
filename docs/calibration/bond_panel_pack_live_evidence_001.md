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

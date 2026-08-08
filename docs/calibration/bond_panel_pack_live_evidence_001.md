# Bond panel pack live evidence 001

_Gate declaration and measured evidence · 2026-08-08 · production target `investintell-db`_

---

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

The implementation must not label the yield-spread residual as OAS.

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
| T1 canonical mapping | All SIC values `0001` to `9999` | `4,529` official mapped values; `0` mismatches against `Siccodes17.txt`[^1] | Pass |
| T1 source artifact | One-time OSBAP panel | SHA-256 `a6abf9981bd804a81227a99c48214b3229d3621d05e79d7d717e4d3b82a9afcf`; `68,140` resolved CUSIPs; `0` invalid rows; `0` modal disagreements | Pass |
| T1 production SIC readiness | `sec_cusip_ticker_map` | `41,786` CUSIP8/9 rows and `0` non-null `sic_code`; SIC fallback is unavailable until a governed SEC backfill lands | Blocked input |
| T1 curated coverage | Production | Pending backfill execution | Pending |
| T2a yearly quote coverage | Historical backfill | Pending implementation and measurement | Pending |
| T2b tick budget | Widened live lane | Pending measured run | Pending |
| T3 parity `2025-01` | Frozen versus rebuilt | Thresholds declared above; comparison not yet run | Pending |
| T3 parity `2026-06` | Frozen versus rebuilt | Thresholds declared above; comparison not yet run | Pending |
| T3 duration overlap | Frozen versus analytical | Thresholds declared above; comparison not yet run | Pending |
| T4 rating freshness | `2024-01` through current month | Pending primary-source ingestion and measurement | Pending |
| Stage 6 production run | Target tables and worker JSON | Pending deployment and execution | Pending |

## ⚠️ Declared blockers and degradations

- Production currently contains no usable SIC value in
  `sec_cusip_ticker_map`. OSBAP-covered CUSIPs can resolve; every remainder
  must remain `no_sector` until the SEC evidence lane is populated.
- Finnhub industry labels are reference enrichment only. They are not FF17 and
  cannot redefine the frozen sector input.
- A successful container status is not execution evidence. Final acceptance
  requires target-table timestamps, months, row counts, pointer state, and the
  emitted run JSON.

## 🔗 References

[^1]: Kenneth R. French Data Library. "Industry Definitions: 17." https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_17_ind_port.html

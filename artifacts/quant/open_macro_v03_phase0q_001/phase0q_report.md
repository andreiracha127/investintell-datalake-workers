# open_macro_v03 Phase 0Q Report — Quantitative Evidence Generation (candidate)

Status: candidate_not_approved. A5=blocked, runtime_activation=false, activation_allowed=false. This package produces no approval and no review closure.

## Recommendation

**no_go_pending_metric_harness.**

Data is NOT the blocker: the Cloud DB holds deep history (eod_prices since 1962-01-02, macro_data since 1959-01-01, nav_timeseries since 1970-01-30; full-series axis coverage from 2016-09-07). The blockers are (1) no historical certified input pack — the builder reads only 3-day P0 fixtures — and (2) no metric harness: no code computes turnover, drawdown, volatility, stress-window or out-of-sample metrics for open_macro_v03. `turnover_proxy` is baseline_distance, not turnover, and is disqualified as evidence.

## What this package contains

| Artifact | Content |
|---|---|
| `data_discovery_report.json` | Measured repo/DB/harness findings with query provenance (service id, dates, row counts) |
| `metric_definitions.json` | Exact formulas for the five gates; explicit prohibition of turnover_proxy |
| `stress_oos_policy.json` | 6 named stress windows with real dates (4 full-series, 2 reduced-coverage) and a 36m/12m rolling walk-forward policy (~6 folds) |
| `scenario_grid.json` | Calibration parameter candidates × profiles × windows × reproducibility matrix (local LEAN + QC Cloud) |
| `threshold_candidate_report.json` | Candidate institutional thresholds in conservative/base/aggressive profiles with explicit derivations; base recommended; approved=false |
| `quantitative_gate_report.candidate.json` | Consolidated per-gate status: all five gates no_go_pending_metric_harness, with what exists vs what is missing |
| `lean_harness_spec.md` | Reproducible harness spec: DB export → certified pack v2 → parity module → LEAN backtest → metric extractor |
| `threshold_profile_selection_record.json` | Quant owner selected the base profile as the empirical test envelope (2026-07-02) — explicitly NOT final institutional approval |

## What must happen next (in order)

1. Refresh stale `T10YIE` series (last obs 2026-02-27).
2. Build the p1 historical export + certified input pack v2 (separate PR).
3. Build the decision-engine parity module + golden test vs `quadrant_macro` worker (separate PR).
4. Build and run the LEAN harness over the scenario grid (separate PR); emit measured `quantitative_gate_report.json`.
5. Quant owner (Andrei Rachadel) reviews measured values against the candidate thresholds, selects the profile, signs.
6. Only then: dark launch Task 2 (`review_closure_record.json`) unblocks.

## What this package deliberately does NOT do

- Does not create `review_closure_record.json` and does not execute dark launch Task 2.
- Does not mark any gate as go.
- Does not approve thresholds — the profiles are recommendations for the quant owner to accept or reject.
- Does not touch frozen artifact bundles, formulas, contract v1, input packs, or calibration packs.
- Does not write to the DB (discovery used read-only SELECTs) and produces no productive side effect.

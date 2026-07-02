# open_macro_v03 Phase 0Q — LEAN Harness Specification (candidate)

Status: candidate_not_approved. This document specifies the reproducible metric harness; nothing here is implemented or executed in this PR. A5=blocked, runtime_activation=false, activation_allowed=false, db_write_mode=none.

## Purpose

Produce the measured evidence that `quantitative_gate_report.candidate.json` records as missing: real turnover, max drawdown, annualized volatility, stress-window behavior, and walk-forward out-of-sample metrics for `open_macro_v03`, with full provenance. The harness is evidence-only: artifact outputs, no productive side effects.

## Architecture

```
Tiger Cloud DB (read-only SELECT)
      |
      v
p1 historical source snapshots (JSON/CSV export + sha256 per table)
      |
      v
src/input_packs/build.py extension (new profile: open_macro_v03_p1_historical)
      |  -> certified historical input pack v2 (hash-pinned, read-only)
      v
LEAN backtest project (LEAN CLI local docker + QuantConnect Cloud)
      |  -> decision series + daily NAV per scenario-grid cell
      v
metric extractor (computes metric_definitions.json formulas)
      |
      v
quantitative_gate_report.json (non-candidate, measured) + logs + hashes
```

## Components to build (each in its own future PR)

### 1. Historical data export (`p1_sources`)

- Read-only SELECT export from Tiger service `t83f4np6x4` of: `macro_data` (axis series per `quadrant_macro` specs), `eod_prices` (asset-class proxy instruments), `nav_timeseries` (only if fund-level attribution needed).
- Primary window: `2016-09-07 .. 2026-06-30` (full-series coverage; constraining series: BAML credit OAS family). Supplementary reduced-coverage window for GFC_2008/TAPER_2013 per `stress_oos_policy.json`.
- Every exported table gets `sha256`, row count, min/max date, export query text, export timestamp, and DB service id recorded in a `SOURCE.json` — same pattern as the P0 pack.
- Refresh `T10YIE` before export (stale since 2026-02-27 per `data_discovery_report.json`).

### 2. Input pack builder extension

- New profile `open_macro_v03_p1_historical` in `src/input_packs/build.py` reading the p1 snapshots (mechanical extension of the existing P0 path; same manifest/hash/provenance schema, `input_pack_version: 2`).
- Output: `open_macro_v03_certified_input_pack_002` — certified by the existing verifier (`docker/railway-ci/verify_input_pack.py`) plus new coverage checks (min window span, required series present).

### 3. Decision engine parity module

- The backtest must use the SAME axis scoring as production: extract the scoring core of `src/workers/quadrant_macro.py` (`_score_axis`, `axis_score`, `standardized_latest`, PIT vintage selection) into a pure importable module with no DB dependency (inputs injected).
- Parity gate: golden test comparing module output vs existing worker output on the overlap evidence window before any backtest result is admissible.

### 4. LEAN backtest project

- Project name: `open_macro_v03_phase0q_harness`. Runs via LEAN CLI (pinned engine docker image digest) locally AND on QuantConnect Cloud (driven via the QuantConnect MCP tools; results archived as artifacts).
- Custom data: LEAN custom-data readers consuming the certified pack v2 files read-only — the backtest never touches the DB.
- Algorithm: monthly decision cadence; quadrant classification from the parity module; maps quadrant + parameters (scenario grid cell) to asset-class proxy weights; applies `defensive_floor`/`risk_cap` exactly as the contract defines; models transaction costs (candidate: 5 bps one-way, quant_owner to confirm).
- Determinism: `random_seed=20260626`; reproducibility matrix per `scenario_grid.json` (2× local + 2× cloud, identical decision series bitwise, float metrics within 1e-12).

### 5. Metric extractor + report generator

- Computes exactly the formulas in `metric_definitions.json` from the NAV/weights series; emits per-cell JSON with provenance block: `{input_pack_sha256, contract_bundle_sha256, builder_commit, harness_commit, lean_engine_digest, seed, run_id, started_at, finished_at, log_paths}`.
- Aggregates into `quantitative_gate_report.json` (non-candidate): each of the five gates with measured values, the quant_owner-selected threshold profile, and per-gate `go`/`no_go`.

## Hard rules

- `turnover_proxy` is never read by the harness.
- A gate result without a provenance block is invalid.
- Any NaN/Inf metric, non-deterministic rerun, or `decision_coverage < 1.0` in a full_series window → that cell is `no_go`, no exceptions.
- The harness writes only artifacts. No DB writes, no allocator, no endpoints, no feature flag reads.
- Frozen artifact directories (calibration 001, shadow 001, proposal 001, preflight 001) are never modified.

## Definition of done for Phase 0Q execution (future PR)

1. Certified pack v2 exists and verifies.
2. Parity gate green.
3. Full scenario grid executed with reproducibility matrix green.
4. `quantitative_gate_report.json` (measured, non-candidate) generated.
5. Quant owner reviews measured values against `threshold_candidate_report.json`, selects/adjusts the profile, and signs — only then does Task 2 of the dark launch plan unblock.

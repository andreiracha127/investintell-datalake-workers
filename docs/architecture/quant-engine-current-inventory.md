# Quant Engine Current Inventory

Date: 2026-06-26

This inventory is the mandatory pre-extraction checkpoint for the quant-engine
isolation lane. It records the current A3/A4 calibration surface, the Plan C
backend allocator surface, and the stop conditions that must remain in force
before any runtime activation or freeze decision.

## Repository State

Workers worktree:

- Path: `E:\investintell-datalake-workers-quant-engine`
- Branch: `feat/quant-engine-isolation`
- Base local branch: `feat/combo-regime-gate`
- Base SHA: `285b0586213b` (`feat(calibration): add v03 microgrid benchmark support`)
- Remote branch note: GitHub only exposed `main` for the workers repository during
  verification. The local A3 branch is ahead and is the source of truth per the
  operator instruction: "NAO PRECISA FAZER BASE REMOTA - LOCAL ESTA AHEAD".

Backend worktree:

- Path: `E:\investintell-light-quant-engine-contracts`
- Branch: `feat/quant-engine-contracts`
- Base local branch: `feat/combo-regime-allocator`
- Base SHA: `a6c6f3e7fae6` (`docs(combo): archive regime allocator handoffs`)

Governance state preserved by this lane:

- A3: `open_macro_v03`
- A4: `harness_ready_provisional_A3`
- A5: `blocked`
- `freeze_ready=false`
- `runtime_activation=false`

## Workers: Current A3/A4 Surface

Canonical implementation files:

- `src/calibration_harness.py`
- `src/qc_a3_core.py`
- `configs/a31_v03_revision_robust_g1.yaml`
- `configs/a31_a32_selected_v01.yaml`
- `qc-a3-parity/README.md`
- `tests/test_calibration_harness.py`
- `tests/test_qc_a3_core.py`

Important symbols in `src/calibration_harness.py`:

- Config dataclasses: `A31Config`, `A32Config`, `A31GridConfig`,
  `A32GridConfig`, `MarketGridConfig`, `A3ScopeDecisionConfig`,
  `RevisionUncertaintyConfig`.
- Macro primitives and metrics: `replay_macro`, `build_macro_metrics`,
  `build_revision_attribution`, `replay_revision_diagnostics`,
  `operational_state_metrics`.
- L3/L4 pipeline: `build_l3_score_panel`, `aggregate_l3_axis`,
  `apply_revision_soft_threshold`, `run_l4_state_machine`,
  `l4_axis_status_payload`, `resolve_candidate_status_with_config`.
- Hashes: `canonical_config_hash`, `a31_config_hash`, `a32_config_hash`,
  `evaluation_hash`, `logical_records_hash`, `logical_payload_hash`.
- Batch runners: `run_a31_grid`, `run_a32_grid`, `run_market_grid`,
  `run_a3_scope_decision`.
- Explicit I/O paths: `run_v02_fetch_alfred`, `fetch_v02_candidate_vintages`,
  `replay_market_tiingo`, `write_parquet`, `write_json`, `read_parquet_records`,
  and default CLI paths that connect with `DATABASE_URL`.

Important symbols in `src/qc_a3_core.py`:

- Contract/config: `A3ParityConfig`, `validate_feature_manifest_contract`.
- Pure bridge: `compute_a3_case`, `canonical_metric_rows`,
  `metric_rows_logical_hash`, `metric_rows_raw_sha256`,
  `bundle_evaluation_hash`.
- Entry points: `run_parity`, `export_bundle`, `parity_report`,
  `upload_object_store_bundle`.

## Workers: Pure Versus I/O Classification

Candidate pure core:

- Manifest contract validation.
- Bundle-level A3 case computation over immutable inputs.
- L2 to L3 scoring.
- L4 state machine.
- Metric row canonicalization and logical hashing.
- Evaluation hash/config hash helpers.
- A31/A32 parameter expansion and deterministic task enumeration.

I/O and orchestration that must remain outside `quant-core`:

- Postgres connection through `DATABASE_URL`.
- FRED/ALFRED/Tiingo/httpx/network fetches.
- QuantConnect/QC project entrypoints and notebook-only glue.
- Object-store uploads.
- Filesystem writes for reports, manifests, Parquet, JSON, and temp dirs.
- Git metadata collection and wall-clock timestamps.
- ProcessPool execution and resume/checkpoint mechanics.

External dependencies observed in `requirements.txt`:

- Numeric/data: `numpy`, `pandas`, `pyarrow`, `scipy`, `arch`, `PyYAML`.
- I/O/backend: `psycopg[binary]`, `httpx`, `websockets`.

`quant-core` must only accept explicit data/config objects and return explicit
result objects. It must not read environment variables, connect to databases,
fetch network data, inspect git state, use implicit wall-clock time, or write
outside caller-provided output directories.

## Workers: Current Config Semantics

`configs/a31_v03_revision_robust_g1.yaml` defines the v03 microgrid used by the
A31 lane, including:

- Control candidate `V03-G0-CONTROL`.
- Consensus 60/67 variants.
- Revision-soft P50/P75 variants.
- Family weighted-median policy.
- Macro component weights for `C`, `F`, `A`, and `V`.

`configs/a31_a32_selected_v01.yaml` carries the selected A31/A32 bridge used by
the provisional A4 lane, including:

- A31 candidate `G2-CREDIT6040-15-SURVEY05`.
- A31 config `V02B-G1-CREDIT-6040-15`.
- A32 policy `A31-C-TEMPORAL-STABLE`.

These configs are input candidates only. This lane must not freeze, promote, or
activate them in runtime.

## Workers: Baseline Artifacts

The active isolated worktree intentionally starts without `_tmp_*` generated
artifacts. The sibling local worktree contains historical generated bundles,
including reports with the required canonical metric hash:

- Expected governance: A4 `harness_ready_provisional_A3`, A5 `blocked`,
  `freeze_ready=false`, `runtime_activation=false`.
- Expected row counts: runtime rows `3221`, counterfactual rows `3221`, metric
  rows `5`.
- Expected parity: `mismatch_count=0`.
- Expected current canonical metric hash family:
  `70014a0a04fa26faf8aec88227f0f1fea381091acb6ac307fae30b77172300d3`.

Before extraction, the baseline must be reproduced or copied into the isolated
worktree and revalidated with the current code. If that cannot be done, the lane
must stop before functional extraction.

## Backend: Current Plan C Allocator Surface

Canonical implementation files:

- `backend/app/services/portfolio_builder.py`
- `backend/app/services/effective_policy.py`
- `backend/app/services/quadrant_policy.py`
- `backend/app/optimizer/gate_overlay.py`
- `backend/app/optimizer/engine.py`
- `backend/app/optimizer/sleeves.py`
- `backend/app/api/routes/builder.py`

Important tests:

- `backend/tests/test_builder_regime_two_level.py`
- `backend/tests/test_builder_regime_cvar.py`
- `backend/tests/test_builder_regime_aware.py`
- `backend/tests/test_builder_regime_aware_schema.py`
- `backend/tests/test_effective_policy.py`
- `backend/tests/test_gate_overlay.py`
- `backend/tests/test_quadrant_policy.py`
- `backend/tests/test_optimizer_engine.py`
- `backend/tests/test_optimizer_momentum_view.py`
- `backend/tests/test_optimizer_sleeves.py`

Important semantics:

- `QUADRANT_MODEL_VERSION = "macro_quadrant_us_v1"` in
  `portfolio_builder.py`.
- `build_effective_policy` separates quadrant and gate reads, fails loud on
  missing/non-consumable data, applies gate overlay, and returns the effective
  policy object.
- `optimizer/sleeves.py` maps final instruments into canonical sleeves with
  benchmark proxies: `BIL`, `IVV`, `GOVT`, `XLK`, `QAI`, `GLD`, and `FTLS`.
- `CompiledRegimeProblem` keeps decision variables in category space:
  `x` categories, `S` category-to-sleeve exposure, `M` category-to-final-book
  instrument exposure, and published final book `y = Mx`.
- Instrument caps, final-book floors, sleeve budgets, beta cap, risk-assets cap,
  defensive floor, and daily-loss CVaR are enforced in the compiled problem and
  verified after solve.
- The service preflights feasibility with `solve_min_cvar`, uses the primary
  BL utility/CVaR solve, and falls back to min-CVaR only under the same compiled
  constraints.

## Backend Boundary

The backend must not import the new `quant-core` package in this phase. It may
add frozen contract documents, schemas, and test fixtures that define the future
boundary between the allocator API and an offline quant-engine artifact. Runtime
activation remains out of scope.

Plan C allocator extraction is a future backend refactor. It must preserve:

- `S` versus `M` semantics.
- Final-book floors over `M`.
- Fail-loud policy behavior.
- Daily-loss CVaR conventions.
- Existing broad-universe builder behavior.

## Extraction Non-goals

This lane must not:

- Freeze A3/A4 parameters.
- Advance A5.
- Enable runtime activation.
- Change production DB tables or run production writes.
- Modify model formulas or candidate-selection rules.
- Move frontend behavior.
- Merge to `main`.
- Treat notebook output as canonical runtime code.

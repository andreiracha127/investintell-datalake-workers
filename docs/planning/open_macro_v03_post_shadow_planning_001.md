# open_macro_v03 post-shadow planning 001

Status: planning-only candidate.

Branch: `feat/open-macro-v03-post-shadow-planning-001`.

Base: `origin/main` at `09a42a0d80513a8ccfab432317eec6949b3c97cd` (`Merge PR #6: execute open_macro_v03 shadow pilot (artifact-only, A5 blocked)`).

This document is a local/read-only technical planning package for the next phase after the Shadow Pilot. It does not implement Shadow Execution, A5, runtime activation, allocator publication, productive DB writes, production endpoints, formulas, input packs, calibration packs, or contract bundle changes. The only non-planning-file exception in this branch is `.gitignore`, updated after validation to ignore local `.kilo/` session/config state and keep remote CI hooks from seeing local tooling files as dirty.

Pinned state for this planning branch:

| Gate | Value |
|---|---|
| A3 | `open_macro_v03` |
| A4 | `post_shadow_planning_completed` after this planning PR is accepted; source evidence from PR #6 is `shadow_pilot_validated` |
| A5 | `blocked` |
| `freeze_ready` | `false` |
| `runtime_activation` | `false` |
| `official_result` | `false` |
| `allocator_impact` | `none` |
| `production_endpoint_activation` | `none` |

Supporting inventories are tracked in:

| File | Purpose |
|---|---|
| `docs/planning/open_macro_v03_post_shadow_file_inventory.json` | Machine-readable file inventory for the read-only inspection. |
| `docs/planning/open_macro_v03_post_shadow_risk_register.json` | Machine-readable risk register for the next phase. |

## Code and Docs Actually Read

This section records the code, docs, artifacts, contracts, tests, CI, Docker, Railway, backend, and sibling worktree files inspected before planning. The detailed JSON inventory repeats these entries in a machine-readable form. `AGENTS.md` and `.codex/AGENTS.md` are local-only untracked environment evidence, not files in the PR evidence tree.

| Path | Why It Was Relevant | Functions, Classes, Entrypoints | Observations |
|---|---|---|---|
| `AGENTS.md` | Local-only untracked repo instructions and governance baseline. | n/a | Local environment evidence only, not part of the PR evidence tree. Requires Serena first and Auggie scouting before deeper inspection. It contains older open-PR state but preserves `A5=blocked`, `freeze_ready=false`, `runtime_activation=false`. |
| `.codex/AGENTS.md` | Local-only untracked mirrored instructions. | n/a | Local environment evidence only, not part of the PR evidence tree. Same content as root `AGENTS.md`. |
| `README.md` | Repository principles, normal worker write path, Railway entrypoint. | `src.run_worker`, `src/db.py::connect`, `src/db.py::advisory_lock` | Normal workers can write to TimescaleDB; quant-engine post-shadow work must stay artifact-only. |
| `docs/architecture/quant-engine-governance-rollout.md` | Rollout governance and A5 sequencing. | n/a | Main now documents Railway-native CI, A5 blocked, and deferred runtime follow-ups. `.github/workflows` is empty. |
| `docs/specs/certified_input_packs_v1.md` | Certified input pack contract and official input boundary. | `python -m src.input_packs.build` | Official calibration/shadow paths must consume immutable packs, not live DB fallbacks. |
| `docs/calibration/open_macro_v03_calibration_001.md` | Calibration candidate governance note. | n/a | Calibration is candidate only; no runtime activation, A5 blocked, freeze false. |
| `docs/shadow/open_macro_v03_shadow_readiness_001.md` | Shadow readiness scope and forbidden scope. | n/a | Defines inert envelope/result/comparison policy and forbids runtime, allocator, DB, endpoint, formula/input/calibration/contract changes. |
| `docs/shadow/open_macro_v03_shadow_pilot_001.md` | PR #6 pilot record. | `python -m src.shadow_pilot` | A4 is `shadow_pilot_validated`; A5 remains blocked; pilot is technical artifact evidence only. |
| `docs/architecture/quant-engine-current-inventory.md` | Prior current-state map of worker/backend boundary. | `src/calibration_harness.py`, `src/qc_a3_core.py`, `portfolio_builder.run_optimize` | Documents quant-core purity boundary and backend allocator non-goals. |
| `docs/architecture/quant-engine-supply-chain-sandbox.md` | Docker digest and sandbox controls. | `docker/quant-engine/Dockerfile`, `scripts/repeatability_matrix.py` | Pinned base image and hardened container profile are already documented. |
| `src/input_packs/build.py` | Certified input pack builder. | `build_pack`, `build_parser`, `main`, `write_report`, `builder_code_sha256`, `contract_bundle_sha256` | Builds `open_macro_v03`, writes `runtime_activation=false`, copies schemas, writes provenance, then verifies with `verify_pack`. |
| `src/input_packs/verifier.py` | Offline pack verifier. | `verify_pack`, `RowSchema` | Checks required files/dirs, schemas, component hashes, table hashes, duplicates, P0 content, aggregate pack hash, `runtime_activation is False`, and provenance completeness. |
| `src/input_packs/manifest.py` | Manifest and aggregate input pack hash. | `build_manifest`, `compute_input_pack_sha256`, `write_manifest` | `build_manifest` forces `runtime_activation=false` while filling component hashes and aggregate hash. |
| `services/quant_engine/src/investintell_quant_engine/cli.py` | Quant-engine CLI entrypoints. | `build_parser`, `main`, `dry-run-input-pack`, `run-parity` | `dry-run-input-pack` invokes the offline runner with expected hashes and optional closed output manifests. |
| `services/quant_engine/src/investintell_quant_engine/runners/input_pack.py` | Certified input pack dry-run. | `run_input_pack_dry_run`, `current_contract_bundle_sha256` | Verifies pack without DB/network, validates expected hashes, returns `runtime_activation=false`, `freeze_ready=false`, and `a5_status=blocked`. |
| `src/calibration_candidate.py` | Calibration candidate producer and guards. | `run_calibration`, `build_baseline_comparison`, `build_invariant_report`, `output_manifest`, `matrix_evidence_ok`, `build_parser` | Rejects `db_access`, network other than `none`, writable input-pack mount, and output inside input pack. Produces candidate artifacts only. |
| `src/shadow_pilot.py` | Shadow pilot producer and validators. | `run_shadow_pilot`, `build_shadow_job_envelope`, `validate_shadow_job_envelope`, `build_shadow_result_manifest`, `build_acceptance_report`, `build_baseline_comparison`, `build_invariant_report`, `verify_final_pilot_bundle`, `build_parser`, `EvidenceError` | Builds and validates artifact-only pilot bundle. The acceptance report hardcodes technical/quant review as pending. |
| `contracts/quant-engine/v1/manifest.json` | Worker-side contract bundle identity. | n/a | Current bundle is `sha256:4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a`. |
| `contracts/quant-engine/v1/job-request.schema.json` | Quant-engine request contract. | `a3_qc_parity_request`, `certified_input_pack_dry_run_request` | Requires `offline=true`, `engine_image_digest`, and hash-pinned certified input pack dry-run fields. |
| `contracts/quant-engine/v1/job-result.schema.json` | Quant-engine result contract. | `a3_qc_parity_result`, `certified_input_pack_dry_run_result` | Pins `runtime_activation=false`; dry-run pins `freeze_ready=false`, `a3_status=open_macro_v03`, `a5_status=blocked`. |
| `contracts/quant-engine/v1/engine-manifest.schema.json` | Engine manifest contract. | n/a | Requires `offline=true` and `runtime_activation=false`. |
| `schemas/input_packs/input_pack_manifest.schema.json` | Input pack manifest schema. | n/a | Requires `input_pack_version=v1`, source repo const, SHA fields, and `runtime_activation=false`. |
| `schemas/input_packs/provenance.schema.json` | Input pack provenance schema. | n/a | Requires datasets, jobs, runs, and sources. |
| `schemas/input_packs/source.schema.json` | Input pack SOURCE schema. | n/a | Pins builder/source commit and builder code hash; optional builder image digest must be sha256. |
| `artifacts/calibration/open_macro_v03_calibration_001/calibration_manifest.json` | Calibration identity and artifact hashes. | n/a | Status candidate; `A5=blocked`, `freeze_ready=false`, `runtime_activation=false`; pins input pack and artifact hashes. |
| `artifacts/calibration/open_macro_v03_calibration_001/baseline_comparison.json` | Calibration baseline comparison. | n/a | `baseline_current` selected; final blockers are `reference_baselines_not_certified_in_pack` and `institutional_limits_explicitly_unset`. |
| `artifacts/calibration/open_macro_v03_calibration_001/output_manifest.json` | Calibration output manifest. | n/a | Lists artifact paths and hashes for generated calibration files. |
| `artifacts/shadow/open_macro_v03_shadow_001/shadow_manifest.json` | Shadow readiness manifest. | n/a | `execution_status=not_started`, `feature_flag_default=false`, no formula/input/calibration/contract changes. |
| `artifacts/shadow/open_macro_v03_shadow_001/shadow_job_envelope.schema.json` | Shadow job envelope schema. | n/a | Pins calibration/input/contract/engine identities and side-effect const false fields. |
| `artifacts/shadow/open_macro_v03_shadow_001/shadow_result_manifest.schema.json` | Shadow result manifest schema. | n/a | Succeeded result requires artifact hashes, summaries, `runtime_activation=false`, `official_result=false`; side-effect attempts reject/non-retryable. |
| `artifacts/shadow/open_macro_v03_shadow_001/baseline_comparison_policy.json` | Shadow comparison policy. | n/a | Zero tolerance for missing/unexpected/mismatch/NaN/constraint/invariant failures; hard relative delta threshold is 2.0 percent. |
| `artifacts/shadow/open_macro_v03_shadow_001/output_manifest.schema.json` | Shadow output manifest schema. | n/a | Requires succeeded status, artifacts with path/hash/bytes, logs_required, and unexpected_outputs. |
| `artifacts/shadow/open_macro_v03_shadow_pilot_001/shadow_job_envelope.json` | Committed pilot envelope. | n/a | `runtime_activation=false`, `allow_db_write=false`, `allow_allocator_publish=false`, `production_endpoint_activation=none`, run fingerprint `078cef19bdb6ad0de1716dd73a6e6807d45ca4cb6c675838947e2531832c8106`. |
| `artifacts/shadow/open_macro_v03_shadow_pilot_001/shadow_result_manifest.json` | Committed pilot result. | n/a | Status succeeded; all hard counters zero; material divergence false; official result false. |
| `artifacts/shadow/open_macro_v03_shadow_pilot_001/acceptance_report.json` | Pilot acceptance report. | n/a | Automated checks pass; `technical_and_quantitative_review_recorded` remains pending and blocking. |
| `artifacts/shadow/open_macro_v03_shadow_pilot_001/baseline_comparison.json` | Pilot baseline comparison output. | n/a | Status pass; no forbidden effects; all relative deltas are `0.0`. |
| `docker/railway-ci/Dockerfile` | Railway CI gate. | `verify_input_pack.py`, `contract_bundle.py verify`, `pytest`, `compileall`, `verify_calibration_artifacts.py` | Uses pinned python base image and copies tracked calibration/shadow artifacts. |
| `docker/railway-ci/verify_input_pack.py` | Input pack CI verification. | `verify_pack` | Verifies golden certified input pack and exits nonzero on failure. |
| `docker/railway-ci/verify_calibration_artifacts.py` | Calibration artifact CI verification. | `output_manifest`, `file_sha256` | Recomputes artifact hashes and asserts `runtime_activation=false`, `A5=blocked`, `freeze_ready=false`, green run matrix and invariant. |
| `docker/quant-engine/Dockerfile` | Quant-engine image build path. | `/app/docker/quant-engine/entrypoint.sh` | Pinned base image, non-root user 65532, copies contracts/schemas/src/package code. |
| `docker/quant-engine/entrypoint.sh` | Quant-engine container entrypoint. | `python -m investintell_quant_engine.cli` | Executes offline CLI. |
| `compose.quant-engine.yml` | Local hardened container profile. | n/a | `network_mode: none`, `read_only: true`, non-root, no-new-privileges, cap drop all, read-only input mount. |
| `railway.toml` | Normal production worker deploy config. | `python -m src.run_worker` | Separate from quant-engine artifact-only planning. |
| `scripts/ci/run_remote_railway_ci.ps1` | Remote Railway CI runner. | `Invoke-DockerBuild`, `Wait-DockerReady` | Archives current commit and builds `docker/railway-ci/Dockerfile` remotely. |
| `scripts/repeatability_matrix.py` | Repeatability and container isolation matrix. | `_cli_args`, `_run_host`, `_run_container`, `_container_docker_base`, `_container_isolation_probe_script`, `main` | Container leg uses network none, read-only root, non-root, no caps, read-only input mount. |
| `scripts/contract_bundle.py` | Contract bundle CLI wrapper. | `main` | Exposes build/verify commands. |
| `tests/input_packs/test_builder.py` | Input pack builder coverage. | `test_build_cli_creates_verified_open_macro_v03_pack`, `test_build_is_deterministic_and_path_independent`, `test_p0_pack_does_not_use_derived_tables_as_official_inputs` | Covers determinism, source inputs, unsupported profile, force safety, and contract bundle matching. |
| `tests/input_packs/test_verifier.py` | Input pack verifier coverage. | `test_golden_pack_verifies_offline`, `test_verifier_rejects_runtime_activation`, `test_input_pack_code_has_no_db_connector_imports` | Covers offline verification, tampering, component schemas, provenance, runtime activation rejection, and no DB connector imports. |
| `tests/quant_engine/test_input_pack_dry_run.py` | Dry-run coverage. | `test_dry_run_consumes_verified_pack_without_runtime_activation`, `test_dry_run_cli_outputs_are_canonical_jobs_independent`, `test_dry_run_runner_has_no_db_or_network_connector_imports` | Covers no runtime activation, contract conformity, hash mismatches, jobs independence, and no DB/network imports. |
| `tests/test_calibration_candidate.py` | Calibration candidate coverage. | `test_calibration_candidate_generates_required_artifacts`, `test_run_matrix_requires_external_evidence`, `test_run_calibration_enforces_runtime_guards_for_direct_calls`, `test_output_manifest_excludes_stale_files_and_records_disk_size` | Covers artifact generation, external evidence, image/digest guards, runtime guards, and output isolation. |
| `tests/test_shadow_readiness.py` | Shadow readiness coverage. | `test_shadow_manifest_pins_validated_calibration_without_activation`, `test_shadow_job_envelope_schema_is_inert`, `test_shadow_result_manifest_schema_keeps_result_unofficial`, `test_railway_ci_runs_shadow_readiness_gate` | Covers inert manifests, no runtime/A5 activation, observability/rollback docs, Railway gate. |
| `tests/test_shadow_pilot.py` | Shadow pilot coverage. | `test_shadow_pilot_runner_generates_valid_artifact_bundle`, `test_committed_shadow_pilot_artifacts_validate`, `test_acceptance_report_blocks_when_invariant_report_is_red`, `test_output_manifest_rejects_extra_artifact_paths_and_files` | Covers artifact generation/validation, thresholds, reproducibility, output manifests, observability and rollback evidence. |
| `tests/test_shadow_pilot_binding.py` | Trust-boundary and side-effect coverage. | `test_invariant_detects_allocator_publish_attempt_in_log`, `test_invariant_detects_runtime_activation_attempt_in_log`, `test_invariant_detects_db_write_attempt_in_log`, `test_invariant_detects_endpoint_activation_attempt_in_log`, `test_result_requires_inert_envelope_pins` | Covers stale evidence and whole-token log attestation for no allocator, DB, runtime, endpoint attempts. |
| `tests/test_repeatability_matrix.py` | Repeatability matrix coverage. | `test_container_runner_preflights_isolated_mounts` | Covers Docker isolation flags. |
| `tests/quant_engine/*` | Quant-engine contract/comparator/output/repeatability suites. | `test_contract_bundle.py`, `test_contract_bundle_real.py`, `test_contract_schemas.py`, `test_contract_fixtures.py`, `test_outputs_manifest.py`, `test_repeatability.py`, `test_comparator.py` | Directory inventory confirms contract and closed manifest coverage. |
| `E:/investintell-light-quant-engine-contracts/backend/app/contracts/quant_engine_v1.py` | Backend mirrored contract verifier. | `verify_bundle`, `verify_schema_hashes`, `bundle_sha256`, `iter_bundle_files`, `EXPECTED_BUNDLE_SHA256` | Source commit `5ffea2fca17a4ae03258dfcccb7cc34a123d9936`. Mirror is inert; expected digest is stale relative to current workers main and must be resynced later before backend claims. |
| `E:/investintell-light-quant-engine-contracts/backend/contracts/quant-engine/v1/SOURCE.json` | Backend mirror governance metadata. | n/a | Source commit `5ffea2fca17a4ae03258dfcccb7cc34a123d9936`. Says no engine execution, no container invocation, no allocator/builder runtime change. |
| `E:/investintell-light-quant-engine-contracts/backend/tests/test_quant_engine_contracts.py` | Backend contract mirror tests. | `test_quant_engine_contract_bundle_sha256_matches_manifest`, `test_source_metadata_pins_inert_runtime_and_bundle`, `test_valid_fixtures_pass_their_schema`, `test_invalid_fixtures_are_rejected_by_their_schema` | Source commit `5ffea2fca17a4ae03258dfcccb7cc34a123d9936`. Covers bundle drift, SOURCE governance, and fixture validation. |
| `E:/investintell-light-combo/backend/app/api/routes/builder.py` | Current allocator endpoint. | `POST /builder/optimize`, `GET /builder/optimize/{job_id}`, `optimize`, `optimize_job_status`, `_run_optimize_job` | Source commit `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`. Builder optimization API, not quant-engine activation. |
| `E:/investintell-light-combo/backend/app/services/portfolio_builder.py` | Current allocator runtime path. | `run_optimize`, `_preflight_compiled_problem`, `_post_verify_compiled_solution`, `CompiledRegimeProblem`, `QUADRANT_MODEL_VERSION` | Source commit `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`. Regime-aware path reads quadrant/gate, solves, and post-verifies. Keep out of PR 7. |
| `E:/investintell-light-combo/backend/app/services/quadrant_reader.py` | Backend consumable quadrant read model. | `QuadrantSnapshotRow`, `effective_status`, `fetch_quadrant_snapshot` | Source commit `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`. Reads `regime_quadrant_snapshot` with status, confidence, PIT, and staleness filters. |
| `E:/investintell-light-combo/backend/app/services/effective_policy.py` | Backend policy composition. | `EffectiveRegimePolicy`, `EffectivePolicyError`, `build_effective_policy` | Source commit `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`. Fails loud on missing/non-consumable quadrant or gate. |
| `E:/investintell-light-combo/backend/app/services/taa_bands.py` | Backend regime gate reader. | `GateRegimeSnapshot`, `fetch_gate_regime` | Source commit `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`. Latest `regime_gate_daily` row reader; staleness/future controls are runtime concerns outside PR 7. |
| `E:/investintell-light-combo/backend/app/services/optimize_jobs.py` | Existing job state machine. | `create_job`, `get_job`, `mark_running`, `mark_succeeded`, `mark_failed` | Source commit `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`. Persists builder optimize jobs, not quant-engine jobs. |
| `E:/investintell-light-combo/backend/app/models/optimize_job.py` | Existing optimize job model. | `OptimizeJob`, `JOB_STATUSES` | Source commit `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`. JSONB request/result/error for builder optimization jobs. Do not silently reuse for quant-engine. |
| `E:/investintell-light-combo/backend/app/core/config.py` | Backend feature flag surface. | `Settings`, `get_settings` | Source commit `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`. No quant-engine runtime flag in inspected combo worktree. |
| `E:/investintell-light-main-benchmark/backend/app/api/routes/jobs.py` | Legacy/main async job polling route. | `GET /jobs/{job_id}`, `get_job_status` | Source commit `9ef29899864ed683291717872c5f3caef2527df0`. Backtest/MC job API, not quant-engine activation. |
| `E:/investintell-light-main-benchmark/backend/app/core/config.py` | Legacy/main feature flags. | `Settings`, `use_async_jobs`, `get_settings` | Source commit `9ef29899864ed683291717872c5f3caef2527df0`. `use_async_jobs` is for backtest/MC jobs, not quant-engine. |
| `E:/investintell-datalake-workers-quant-engine/docs/architecture/quant-engine-current-inventory.md` | Sibling quant-engine worktree inventory. | n/a | Historical isolation worktree on `feat/quant-engine-isolation`; current main has newer PR #6 state. |
| `E:/investintell-datalake-workers-quant-engine/docs/architecture/quant-engine-governance-rollout.md` | Sibling quant-engine worktree rollout. | n/a | Historical version still mentioned proposed GitHub Actions; current main uses Railway-native CI. |

## Current Execution Map

| Stage | Current Flow | Producers | Consumers | Evidence |
|---|---|---|---|---|
| Certified input pack build | `src.input_packs.build` reads local P0 source snapshots, writes raw/canonical/derived data, schemas, SOURCE, provenance, table hashes, and manifest, then verifies offline. | `src/input_packs/build.py::build_pack` | `src/input_packs/verifier.py::verify_pack`, dry-run runner, calibration candidate. | `docs/specs/certified_input_packs_v1.md`, `tests/input_packs/*`. |
| Certified input pack dry-run | Quant-engine CLI verifies a certified pack with expected pack/source/contract hashes and returns a closed result with no activation. | `services/quant_engine/.../cli.py`, `run_input_pack_dry_run` | Tests and future contract consumers. | `tests/quant_engine/test_input_pack_dry_run.py`. |
| Calibration candidate | `src.calibration_candidate` consumes the verified pack, requires no DB, no network, read-only input pack mount, deterministic matrix evidence, and writes candidate artifacts. | `src/calibration_candidate.py::run_calibration` | Shadow readiness and shadow pilot. | `artifacts/calibration/open_macro_v03_calibration_001/*`, `docs/calibration/open_macro_v03_calibration_001.md`. |
| Baseline comparison | Calibration comparison selects `baseline_current` but records unresolved reference blockers. Shadow comparison evaluates pilot outputs against policy and all hard counters. | `build_baseline_comparison` in calibration and shadow pilot. | Acceptance report and future A5 review. | Calibration `baseline_comparison.json`; shadow pilot `baseline_comparison.json`. |
| Shadow readiness | Readiness package defines envelope/result schemas, comparison policy, observability, rollback, rollout, and forbidden effects. It does not execute a job. | `src/shadow_pilot.py` validators and committed artifacts under `artifacts/shadow/open_macro_v03_shadow_001`. | `run_shadow_pilot`. | `docs/shadow/open_macro_v03_shadow_readiness_001.md`. |
| Shadow pilot | `src.shadow_pilot` validates readiness and calibration hashes, builds an envelope, writes executor logs, baseline/repro/invariant/output/result/acceptance/observability/rollback artifacts, and validates the final bundle. | `src/shadow_pilot.py::run_shadow_pilot` | Post-shadow review and future A5 preflight. | `docs/shadow/open_macro_v03_shadow_pilot_001.md`, `artifacts/shadow/open_macro_v03_shadow_pilot_001/*`. |
| Output manifest | Calibration and shadow use closed manifests with path/hash/byte metadata. | `calibration_candidate.output_manifest`, `shadow_pilot.build_pilot_output_manifest` | CI, acceptance, reproducibility review. | Calibration and shadow `output_manifest.json` files. |
| Railway CI | Railway CI image verifies input pack, contract bundle, pytest suites, compileall, and calibration artifact integrity. | `docker/railway-ci/Dockerfile` | Railway connected service/status. | `docs/architecture/quant-engine-governance-rollout.md`, `scripts/ci/run_remote_railway_ci.ps1`. |
| Docker execution | Quant-engine image uses pinned base, non-root user, and CLI entrypoint. Repeatability matrix runs container with no network/read-only profile. | `docker/quant-engine/Dockerfile`, `entrypoint.sh`, `compose.quant-engine.yml`, `scripts/repeatability_matrix.py` | Repeatability and future controlled external execution. | `docs/architecture/quant-engine-supply-chain-sandbox.md`. |
| Backend/control plane | Current backend has an inert contract mirror and a live allocator builder path; no quant-engine runtime endpoint was found in inspected worktrees. | `investintell-light-quant-engine-contracts` at `5ffea2fca17a4ae03258dfcccb7cc34a123d9936`, `investintell-light-combo` at `a6c6f3e7fae6e2e63a0f3a9214f89c12e0819174`, `investintell-light-main-benchmark` at `9ef29899864ed683291717872c5f3caef2527df0` | Future backend sync/runtime skeleton only. | `quant_engine_v1.py`, `builder.py`, `portfolio_builder.py`. |

## Contract Map

| Contract or Schema | Role | Const or Pinned Fields | Validations | Producers | Consumers |
|---|---|---|---|---|---|
| `schemas/input_packs/input_pack_manifest.schema.json` | Certified input pack root manifest. | `input_pack_version=v1`, `source_repo=investintell-datalake-workers`, `runtime_activation=false`. | SHA regexes, required pack/source/builder/component hash fields. | `build_manifest`, `build_pack`. | `verify_pack`, dry-run, calibration. |
| `schemas/input_packs/provenance.schema.json` | Pack provenance. | `schema_version=v1`, source repo const. | Requires datasets, jobs, runs, sources. | `write_source_and_provenance`. | `verify_pack`. |
| `schemas/input_packs/source.schema.json` | Pack SOURCE metadata. | source repo const, builder/source commit, builder code hash. | Optional image digest pattern. | `write_source_and_provenance`. | `verify_pack`. |
| `contracts/quant-engine/v1/manifest.json` | Worker-side contract bundle identity. | `contract_version=1.0.0`, `bundle_sha256=sha256:4ff92...`. | Bundle file hash set. | `scripts/contract_bundle.py build`. | `contract_bundle.py verify`, input pack builder, backend mirror after sync. |
| `contracts/quant-engine/v1/job-request.schema.json` | Offline engine request. | `offline=true`, sha256 engine image digest. | Request oneOf for parity or certified input pack dry-run. | Contract bundle. | CLI/backend mirror/tests. |
| `contracts/quant-engine/v1/job-result.schema.json` | Offline engine result. | `runtime_activation=false`, `a5_status=blocked`; dry-run also `freeze_ready=false`. | Result oneOf for parity or certified input pack dry-run. | Dry-run/parity runners. | Contract tests/backend mirror. |
| `contracts/quant-engine/v1/engine-manifest.schema.json` | Engine manifest. | `offline=true`, `runtime_activation=false`. | Requires job/environment/version fields. | CLI runners. | Contract tests. |
| `artifacts/calibration/.../calibration_manifest.json` | Calibration artifact identity. | `A3=open_macro_v03`, `A5=blocked`, `freeze_ready=false`, `runtime_activation=false`. | Artifact hash pins and input/contract/source hash pins. | `run_calibration`. | Shadow readiness/pilot and Railway verifier. |
| `artifacts/calibration/.../baseline_comparison.json` | Calibration comparison. | selected candidate `baseline_current`. | Records final blockers. | `build_baseline_comparison`. | Human review and shadow planning. |
| `artifacts/shadow/open_macro_v03_shadow_001/shadow_manifest.json` | Shadow readiness identity. | `runtime_activation=false`, `A5=blocked`, `freeze_ready=false`, `feature_flag_default=false`, `production_endpoint_activation=none`. | `validate_shadow_readiness_manifest_is_inert`. | Shadow readiness PR. | `run_shadow_pilot`. |
| `artifacts/shadow/open_macro_v03_shadow_001/shadow_job_envelope.schema.json` | Shadow job request envelope. | Pins shadow/calibration/input/contract/engine/as_of/mode; side effects const false. | JSON schema and `validate_shadow_job_envelope`. | Shadow readiness PR. | Shadow pilot runner and tests. |
| `artifacts/shadow/open_macro_v03_shadow_001/shadow_result_manifest.schema.json` | Shadow result envelope. | `runtime_activation=false`, `allow_db_write=false`, `allow_allocator_publish=false`, `production_endpoint_activation=none`, `official_result=false`. | Rejection classes, non-retryable side-effect attempts, succeeded-result hash requirements. | Shadow readiness PR. | Shadow pilot result and tests. |
| `artifacts/shadow/open_macro_v03_shadow_001/baseline_comparison_policy.json` | Shadow comparison policy. | Official baseline remains official; candidate remains unofficial. | Hard zero counters, relative delta thresholds, forbidden effects, promotion rules. | Shadow readiness PR. | Shadow pilot baseline/acceptance. |
| `artifacts/shadow/open_macro_v03_shadow_pilot_001/shadow_job_envelope.json` | Pilot execution envelope. | `mode=shadow`, `runtime_activation=false`, no DB/allocator/endpoint. | Validated against readiness schema. | `run_shadow_pilot`. | Result/acceptance review. |
| `artifacts/shadow/open_macro_v03_shadow_pilot_001/shadow_result_manifest.json` | Pilot result. | `status=succeeded`, `official_result=false`, `runtime_activation=false`. | All hard counters zero and artifact hashes present. | `run_shadow_pilot`. | Post-shadow review. |
| `artifacts/shadow/open_macro_v03_shadow_pilot_001/acceptance_report.json` | Pilot gate verdict. | `A5=blocked`, `freeze_ready=false`, `runtime_activation=false`. | Automated rules pass except `technical_and_quantitative_review_recorded=pending`. | `build_acceptance_report`. | PR 7 preflight readiness. |
| `investintell-light-quant-engine-contracts/backend/contracts/quant-engine/v1/SOURCE.json` at `5ffea2fca17a4ae03258dfcccb7cc34a123d9936` | Backend mirror source metadata. | `a5_status=blocked`, `freeze_ready=false`, `runtime_activation=false`. | Backend `verify_bundle`. | Backend mirror PR. | Backend contract tests. |

## Test Map

| Suite | What It Guarantees | Gaps | Command | Cost |
|---|---|---|---|---|
| `tests/input_packs` | Pack builder/verifier determinism, tamper detection, runtime activation rejection, no DB connector imports. | Does not run productive DB paths. | `python -m pytest tests/input_packs -q` | Light to medium. |
| `tests/quant_engine` | Contract bundle, schemas, fixtures, dry-run, output manifests, comparator, repeatability logic. | Backend mirror sync is not tested here. | `python -m pytest tests/quant_engine -q` | Light. |
| `tests/test_calibration_candidate.py` | Calibration artifact generation, external evidence, runtime guards, image/digest guards, output isolation. | Does not prove quantitative approval; baseline blockers remain. | `python -m pytest tests/test_calibration_candidate.py -q` | Medium. |
| `tests/test_qc_a3_core.py` | A3 pure core/parity behavior. | Historical parity only, not A5 review. | `python -m pytest tests/test_qc_a3_core.py -q` | Medium. |
| `tests/test_repeatability_matrix.py` | Docker isolation flags and preflight. | Does not execute full Docker matrix by itself. | `python -m pytest tests/test_repeatability_matrix.py -q` | Light. |
| `tests/test_shadow_readiness.py` | Inert readiness manifest, schemas, policy, docs, Railway CI gate references. | Does not perform live Railway verification. | `python -m pytest tests/test_shadow_readiness.py -q` | Light. |
| `tests/test_shadow_pilot.py` | Shadow pilot bundle generation/validation, acceptance, output manifest, observability/rollback. | Human technical/quantitative review remains pending by design. | `python -m pytest tests/test_shadow_pilot.py -q` | Medium. |
| `tests/test_shadow_pilot_binding.py` | Evidence binding and side-effect attempt detection for DB, allocator, runtime, endpoint. | Does not activate runtime. | `python -m pytest tests/test_shadow_pilot_binding.py -q` | Light. |
| `tests/test_remote_ci_runner.py` | Remote CI runner behavior. | Does not prove actual Railway deployment unless script is run against Railway/remote host. | `python -m pytest tests/test_remote_ci_runner.py -q` | Light. |
| `docker/railway-ci/verify_input_pack.py` | Golden pack verifies under CI image. | Only checks golden pack, not live DB. | `python docker/railway-ci/verify_input_pack.py` | Light. |
| `scripts/contract_bundle.py verify` | Worker contract bundle manifest matches files. | Does not sync backend mirror. | `python scripts/contract_bundle.py verify` | Light. |
| `docker/railway-ci/verify_calibration_artifacts.py` | Calibration artifact hashes and governance pins are intact. | Calibration is still candidate, not official result. | `python docker/railway-ci/verify_calibration_artifacts.py` | Light. |
| `.github/workflows/*` | No GitHub Actions workflow exists on current main. | Railway status must be proven externally. | n/a | n/a. |

## Risk Map

| Priority | Risk | Evidence | Required Handling |
|---|---|---|---|
| P0 | A5 could be accidentally treated as unblocked by artifact-only pilot success. | `acceptance_report.json` still has `technical_and_quantitative_review_recorded` pending and blocking. | PR 7 must keep A5 blocked and produce decision artifacts only. |
| P0 | Productive side effects could be introduced too early. | Shadow envelope/result schemas pin all side-effect permissions false and endpoint activation none. | No runtime, DB write, allocator publish, endpoint, formula, input pack, calibration pack, or contract bundle changes in PR 7. |
| P0 | Human technical/quant/risk reviews are not recorded. | Acceptance status is `technical_pass_promotion_review_pending`. | PR 7 should create explicit technical, quantitative, and risk checklists. |
| P1 | Backend mirror is stale relative to workers main. | Backend mirror digest `sha256:a077...`; workers main digest `sha256:4ff92...`. | Keep backend sync out of PR 7; schedule later contract-sync/runtime skeleton PR. |
| P1 | Calibration baseline references are not certified inside pack. | Calibration baseline blockers are `reference_baselines_not_certified_in_pack` and `institutional_limits_explicitly_unset`. | Treat as review risk; do not convert pilot into A5 approval. |
| P1 | Railway PR-head status is not proven by this branch. | `.github/workflows` empty; rollout doc says Railway PR Environments/status integration pending. | Do not claim Railway deploy success without Railway-backed evidence. |
| P1 | Backend allocator runtime is live and should not be mixed into planning or PR 7. | `portfolio_builder.run_optimize` drives regime-aware allocator with quadrant/gate reads and solver checks. | Keep backend/control-plane runtime out of PR 7. |
| P2 | Macro history coverage remains unresolved. | Deferred in governance rollout and calibration notes. | Separate runtime-worker PR. |
| P2 | Macro vintage identity remains unresolved. | Deferred in governance rollout and calibration notes. | Separate runtime-worker PR. |
| P2 | Advisory lock/regime gate and quadrant staleness debts remain unresolved. | Deferred runtime follow-ups in governance rollout. | Separate SQL/runtime PRs. |
| P3 | Historical docs use older A4 strings. | Local-only untracked `AGENTS.md` and older inventory predate PR #6. | Planning doc should cite PR #6 artifacts as current post-shadow source of truth. |

## Debts Out Of Scope

These items must not be fixed in the planning branch or mixed into PR 7:

| Debt | Why Out Of Scope | Future Home |
|---|---|---|
| `macro-history-coverage` | Changes runtime macro scoring/coverage behavior. | Dedicated runtime-worker PR before activation. |
| `macro-vintage-identity` | Changes productive snapshot identity semantics. | Dedicated runtime-worker PR before activation. |
| Advisory lock / regime gate collision | Requires runtime scheduling and lock-id changes. | Dedicated infra/runtime PR. |
| `quadrant_macro` staleness | Changes runtime freshness semantics and read model behavior. | Dedicated runtime/read-model PR. |
| Runtime read model SQL | Could affect production quadrant selection. | Dedicated SQL/runtime PR. |
| Backend allocator runtime path | Live endpoint and optimizer path are too large for PR 7. | Runtime integration skeleton PR only after approval. |
| Backend contract mirror sync | Mirror is stale relative to workers main; sync changes backend review scope. | Separate backend contract-sync PR. |
| Formula or quantitative model changes | Activation must not change formula or candidate-selection rules. | Separate quant-design approval, not A5 preflight. |
| Input pack/calibration pack/contract bundle edits | PR 7 is review/preflight only, not source boundary mutation. | Separate certified input pack/calibration/contract PR if explicitly approved. |

## Recommended Next Phase

Recommended next real execution branch after this planning PR is approved:

`feat/open-macro-v03-a5-preflight-readiness-001`

Name: `Shadow Review & A5 Preflight Readiness`.

Objective: consolidate Shadow Pilot evidence, prepare objective promotion criteria, and create the A5 decision package without unblocking A5.

The future PR should produce inert files only, likely under a new review/preflight artifact directory plus docs:

| Proposed Future File | Purpose | Behavior Impact |
|---|---|---|
| `a5_preflight_manifest.json` | Hash-pinned index of reviewed shadow pilot evidence. | None. |
| `shadow_review_report.md` | Human-readable technical review of PR #6 evidence. | None. |
| `technical_review_checklist.json` | Technical criteria and pass/fail/pending records. | None. |
| `quantitative_review_checklist.json` | Quantitative criteria and pass/fail/pending records. | None. |
| `risk_review_checklist.json` | Risk review criteria and sign-off status. | None. |
| `promotion_gate_matrix.json` | A5 discussion matrix, with A5 still blocked. | None. |
| `production_activation_runbook.md` | Future activation procedure, not executed. | None. |
| `rollback_runbook.md` | Future rollback procedure, not executed. | None. |
| `feature_flag_policy.json` | Future flag policy, default false. | None. |
| `monitoring_slo_policy.json` | Future monitoring and SLO policy. | None. |

Forbidden scope for PR 7:

| Forbidden Item | Required Value |
|---|---|
| A5 activation | `blocked` |
| Runtime activation | `false` |
| Freeze readiness | `false` |
| Official result | `false` |
| Allocator publish | `none` |
| Productive DB write | `none` |
| Productive endpoint | `none` |
| Formula changes | `none` |
| Input pack changes | `none` |
| Calibration pack changes | `none` |
| Contract bundle changes | `none` |
| Backend allocator/runtime changes | `none` |

Likely files to alter in PR 7:

| Path Pattern | Reason |
|---|---|
| `docs/a5/open_macro_v03_a5_preflight_readiness_001.md` or `docs/shadow/open_macro_v03_a5_preflight_readiness_001.md` | Human-readable preflight and review summary. |
| `artifacts/a5_preflight/open_macro_v03_a5_preflight_001/*.json` | Machine-readable review/checklist/gate artifacts. |
| `artifacts/a5_preflight/open_macro_v03_a5_preflight_001/*.md` | Runbooks and review reports. |
| `tests/test_a5_preflight_readiness.py` | If artifacts are generated by code or schema-validated. |
| `schemas/a5_preflight/*.schema.json` | Only if JSON artifact contracts need schema enforcement. |

Files not to alter in PR 7:

| Path Pattern | Reason |
|---|---|
| `src/workers/**`, `src/run*.py`, productive worker modules | Would change runtime behavior. |
| `backend/**` in `investintell-light*` | Backend runtime/control plane must remain out of PR 7. |
| `contracts/quant-engine/v1/**` | Contract v1 changes require a separate bundle/sync PR. |
| `schemas/input_packs/**` | Input pack contract must remain pinned. |
| `artifacts/input_packs/**` | Input pack mutation out of scope. |
| `artifacts/calibration/open_macro_v03_calibration_001/**` | Calibration pack mutation out of scope. |
| `artifacts/shadow/open_macro_v03_shadow_001/**` | Readiness contract mutation out of scope. |
| `artifacts/shadow/open_macro_v03_shadow_pilot_001/**` | PR #6 evidence must remain immutable. |
| SQL migrations and runtime read-model SQL | Productive DB/runtime changes out of scope. |

## A5 Criteria For Future Discussion

These are minimum criteria to discuss A5 later. Meeting them in PR 7 must still not unblock A5 automatically.

| Area | Minimum Criteria |
|---|---|
| Technical | Shadow pilot reproducible; stable run fingerprint; execution_id excluded from semantic outputs; no official DB write; no allocator publish; no productive endpoint; complete output manifests; logs and observability complete; rollback testable. |
| Quantitative | Baseline comparison reviewed; material divergence false or resolved; hard thresholds have no violations; out-of-sample and stress windows accepted; drawdown, volatility, and turnover within envelope; no NaN/inf; constraints respected; every delta justified. |
| Operational | Railway image digest fixed; Railway CI green with tool-backed evidence; feature flag default false; rollout runbook; rollback plan; alerts; artifact audit. |
| Governance | Technical review recorded; quantitative review recorded; risk review recorded; A5 remains blocked until explicit approval; freeze_ready remains false until a separate gate. |

## Future PR Matrix

| PR | Branch | Scope | Prohibited Scope | Exit Gate |
|---|---|---|---|---|
| PR 7 | `feat/open-macro-v03-a5-preflight-readiness-001` | Consolidate review and promotion gates; produce inert A5 preflight package. | No runtime activation, no A5 unblock, no backend runtime, no allocator, no DB, no formula/input/calibration/contract changes. | Review package accepted with no P0/P1 lack-of-evidence comments. |
| PR 8 | `feat/open-macro-v03-controlled-shadow-execution-001` | Controlled external/artifact-only shadow execution if more evidence is required. | No allocator publish, no official DB write, no endpoint, no A5. | External execution evidence accepted and reproducible. |
| PR 9 | `feat/open-macro-v03-runtime-integration-skeleton-001` | Feature flag off, inert backend/control-plane wiring skeleton if approved. | No official result, no default-on flag, no allocator decision, no productive write. | Backend contract tests and feature flag guards accepted. |
| PR 10 | `feat/open-macro-v03-a5-controlled-activation-001` | Controlled activation, rollback, monitoring, gradual flag if and only if explicitly approved. | No formula changes; no unreviewed activation; no missing rollback. | A5 approval recorded, rollout and rollback evidence green. |

## Gates For This Planning Branch

Branch-level checks before commit:

| Gate | Command or Evidence | Result |
|---|---|---|
| JSON parse | `python -m json.tool docs/planning/open_macro_v03_post_shadow_file_inventory.json` | Pass. |
| JSON parse | `python -m json.tool docs/planning/open_macro_v03_post_shadow_risk_register.json` | Pass. |
| Diff whitespace | `git diff --check` | Pass. |
| Markdown lint | Repository has no markdownlint config or package outside untracked `.kilo/`; no markdown lint command is available. | Not available, documented. |
| Contract bundle | `python scripts/contract_bundle.py verify` | Pass. |
| Input pack verifier | `$env:PYTHONPATH='.'; python docker/railway-ci/verify_input_pack.py` | Pass. Local note: direct script invocation without `PYTHONPATH` fails because the Dockerfile normally sets `PYTHONPATH=/app:...`. |
| Calibration artifact verifier | `$env:PYTHONPATH='.'; python docker/railway-ci/verify_calibration_artifacts.py` | Pass. Local note: direct script invocation without `PYTHONPATH` fails for the same local import-path reason. |
| Targeted shadow tests | `python -m pytest tests/test_shadow_readiness.py tests/test_shadow_pilot.py tests/test_shadow_pilot_binding.py tests/test_remote_ci_runner.py tests/test_repeatability_matrix.py -q` | Pass: 134 passed. |
| Prohibited-file check | `git status --short --branch` before final commit | Pass. Final branch changes are `docs/planning/*` plus `.gitignore` for local `.kilo/` ignore only. No runtime, SQL, allocator, endpoint, formula, input pack, calibration pack, shadow artifact, or contract path changed. |

## Acceptance For This Planning PR

| Acceptance Criterion | Evidence Expected |
|---|---|
| Plan approved. | Review accepts this document and inventories. |
| No P0/P1 comments about missing code reading. | `Code and Docs Actually Read` section and JSON inventory cite actual files/symbols. |
| Next PRs are small and sequenced. | Future PR matrix separates PR 7, PR 8, PR 9, PR 10. |
| Next execution scope accepted. | PR 7 remains `Shadow Review & A5 Preflight Readiness`, inert and review-only. |
| State after this branch. | `A3=open_macro_v03`, `A4=post_shadow_planning_completed`, `A5=blocked`, `freeze_ready=false`, `runtime_activation=false`. |

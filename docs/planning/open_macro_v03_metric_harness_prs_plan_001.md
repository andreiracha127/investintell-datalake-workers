# open_macro_v03 Metric Harness PRs Plan 001

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans per PR. Strict TDD. Each PR is separate, merges only with green CI, and changes no governance flag: A5=blocked, runtime_activation=false, activation_allowed=false, freeze_ready=false, official_result=false, allocator_publish=false, db_write_mode=none throughout.

**Goal:** produce the measured, non-candidate `quantitative_gate_report.json` for open_macro_v03 via three PRs: (A) additive contract job type, (B) certified input pack v2, (C) parity harness + metric extractor.

**Decisions this plan implements (quant_owner, 2026-07-02):** additive versioned contract change BEFORE pack v2; no mutation of historical artifacts/bundles; execution model = QC Research + Object Store cloud leg × pure-python local leg; sleeve/cost/window per phase0q_002 artifacts.

## Global constraints

- Guarded paths NEVER modified: `src/input_packs/`, `fixtures/input_packs/`, `qc_a3_core.py`, `qc-a3-parity/`, `src/quadrant_score.py`, `src/workers/quadrant_macro.py`, `src/macro_pit.py`, frozen artifact dirs. Reuse is by import only (precedent: `runners/parity.py:15`, `runners/input_pack.py:17`).
- `services/quant_engine/` and `packages/` are ALSO on the preflight guard list — the new runner therefore lives in a NEW top-level package `harness/` (guard-free), importing shared code read-only. If reviewers prefer `services/`, that requires an explicit guard revision PR first (Andrei's call — default is `harness/`).
- Historical pins stay valid: pack-001/calibration-001 remain validated against their pinned historical `contract_bundle_sha256`; only the new pack v2 pins the new bundle.

---

## PR-A — `feat/open-macro-v03-contract-metric-backtest-001` (contract additive)

**Scope:** add job type `open_macro_v03_metric_backtest` to `contracts/quant-engine/v1/` as new `$defs` variants in job-request/job-result `oneOf` (additive; existing defs byte-unchanged). Result schema consts pin evidence-only semantics: `runtime_activation: {const: false}`, `a5_status: {const: "blocked"}`, `official_result: {const: false}`, `allocator_publish: {const: false}`, `db_write: {const: "none"}`, `production_endpoint_activation: {const: "none"}`, `classification: {const: "metric_evidence_only"}`; request pins `offline: {const: true}`, `input_pack_sha256`, `contract_bundle_sha256`, sleeve/window/cost-grid ids.

**Artifacts:** `artifacts/contracts/open_macro_v03_contract_delta_001/` with `contract_delta_report.json`: `{old_bundle_sha256: "4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a" (pinned by calibration_config/pack-001), new_bundle_sha256, delta: additive-only file list + json-diff summary, historical_pins_unaffected: [...pinned constants list...]}` + governance pins.

**Tests (Andrei's three, mandatory):**
1. `test_historical_pack_validates_against_pinned_historical_bundle` — golden pack-001 dry-run semantics against the OLD bundle sha (fetch old bundle content via `git show <commit>:contracts/...` exactly like `test_a5_preflight_readiness._git_show_bytes`), asserting `verify_pack` + old-sha re-derivation still pass.
2. `test_metric_backtest_request_and_result_validate_against_new_bundle` — fixture request/result JSONs validate against the NEW schemas; new bundle sha matches `contract_delta_report.json`.
3. `test_metric_backtest_is_evidence_only` — jsonschema rejects any request/result with `runtime_activation: true`, `a5_status != "blocked"`, `official_result: true`, allocator/db/endpoint fields non-inert (parametrized negative cases).
Plus: `test_existing_contract_defs_byte_unchanged` (old `$defs` subtrees identical to `git show` of the pre-PR commit).

**Order:** merges BEFORE PR-B. After merge, note in the delta report the exact new bundle sha for PR-B to pin.

---

## PR-B — `feat/open-macro-v03-input-pack-v2-001` (certified pack v2)

**Scope:** new builder package `harness/p1_pack/` (imports `src/input_packs/p0_contract.py`, `hashing.py`, `verifier.py` unmodified) consuming the p1 snapshots produced by `scripts/p1_export/` (already merged in PR #17). Output: `open_macro_v03_certified_input_pack_002` under `fixtures/p1_packs/` (NOT `fixtures/input_packs/` — guarded) with the same manifest/provenance schema, `input_pack_version: 2`, pinning the NEW bundle sha from PR-A.

**Steps:** (1) run `scripts/p1_export` via `railway run` (read-only SELECTs) → commit snapshots + SOURCE.json; (2) TDD builder: table specs for `macro_observation_vintage` (new TableSpec, key `(series_id, observation_period, vintage_date)`) + `eod_prices` sleeve subset; canonical raw/canonical/derived layout, manifests, sha256 tree; (3) coverage gates as tests: vintage coverage ≥ window policy requirements (PPIFIS from 2014-02, 8/8 in primary window; sleeve prices from 2006-02); (4) verify with `verify_pack` + a v2 dry-run equivalent pinning the new bundle; (5) guard test file `tests/test_p1_pack.py`.

**Non-negotiables:** builder never writes outside its output dir; pack immutable after merge (hash-pinned); the export run's SOURCE.json SQL/params/rowcounts committed as evidence.

---

## PR-C — `feat/open-macro-v03-parity-harness-001` (harness + metrics + parity gate)

**Scope:** `harness/phase0q/` package, pure Python:
1. **PIT adapter:** in-memory `latest_vintage_as_of` reimplementation over pack-v2 vintage rows (DISTINCT ON semantics of `src/macro_pit.py:14-18`). Golden parity test: adapter output == expected fixtures derived from `tests/test_macro_pit.py` semantics AND == live worker behavior on the overlap evidence window (fixture-captured, not DB-coupled in CI).
2. **Decision engine:** imports `src/quadrant_score.py` functions UNMODIFIED (parity by construction) + hysteresis/latch semantics replicated from `quadrant_macro` with golden tests against recorded `quadrant_macro` snapshots.
3. **Sleeve simulator:** monthly rebalance per `reference_sleeve_proposal.json` (weights, risk_cap/defensive_floor baselines + delta_pp offsets from the scenario grid, 5pp drift band, cost sensitivity grid 0/5/10/25 bps, fallback/pre-inception renormalization, data-quality flags).
4. **Metric extractor:** exact `metric_definitions.json` formulas; per-cell provenance block (pack sha, bundle sha, harness commit, run ids, timestamps, log paths).
5. **Runner:** contract-shaped results (`open_macro_v03_metric_backtest` from PR-A) mirroring `runners/parity.py` fingerprint/logical-hash construction; canonical writers + stable_hash + 12-decimal floats + no RNG.
6. **Reproducibility matrix:** `local_python_pure` (CI job) × `qc_research_object_store` (new QC project `open_macro_v03_phase0q_harness`, object-store upload with drift refusal, immutable prefix `investintell/open_macro_v03/phase0q/<commit>/<pack_sha>/`). Cloud leg evidence archived as artifacts.
7. **Output:** measured `quantitative_gate_report.json` (non-candidate) in a NEW artifact dir `artifacts/quant/open_macro_v03_metric_evidence_001/`, judged against `threshold_profile_selection_record.json` (base envelope) across the cost grid — each of the five gates gets measured values and go/no_go.

**Exit:** quant_owner (Andrei) reviews measured report → approves/adjusts/rejects thresholds → only then dark launch Task 2 unblocks.

---

## Self-review

- Andrei's decision constraints all mapped: additive+versioned contract before pack v2 (PR-A→PR-B order), historical bundles untouched (test 1 + byte-unchanged test), evidence-only enforcement (test 3), Research+ObjectStore model (PR-C §6), sleeve requirements (PR-C §3 consumes the 002 proposal incl. sensitivity grid and fallback policy), discovery amendment (merged via PR #17).
- Guard collision resolved without weakening: new `harness/` top-level package; `services/`/`packages/` untouched.
- Open item deliberately deferred: QC project creation credentials/config for the cloud leg (needs Andrei's QC account action when PR-C reaches §6).

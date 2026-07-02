# Quant Engine Contract v2 — Compatibility Policy & Changelog

The worker repository owns these schemas; see `../v1/CHANGELOG.md` for the
versioning policy (SemVer over the public schema surface). This bundle is
versioned and verifiable via its own `manifest.json`. Regenerate with
`python scripts/contract_bundle.py build --bundle-dir contracts/quant-engine/v2
--contract-version 2.0.0`; gate with
`python scripts/contract_bundle.py verify --bundle-dir contracts/quant-engine/v2`.

Delivery note: v1 (`contract_version` 1.0.0) is a released, immutable contract
surface — it is hash-pinned as a byte-frozen read-only input by the
controlled_shadow / handshake / runtime-skeleton governance guards and by the
pack-001 and calibration-001 evidence. Per the versioning policy ("a released
`contract_version` is immutable... ships as a new version; never edit a released
schema in place"), the additive `open_macro_v03_metric_backtest` job type ships
here as a NEW versioned bundle directory instead of mutating v1.

## Changelog

### 2.0.0 — 2026-07-02

- New bundle directory `contracts/quant-engine/v2/`: a strict superset of v1.
  All three schemas and every 1.0.0 fixture are carried over from v1; the only
  content change to the carried-over schemas is the `$id` URL bump from `/v1/`
  to `/v2/` (schema identity hygiene). Every 1.0.0 `$defs` subtree is
  deep-equal to its v1 counterpart (guarded by
  `tests/test_contract_metric_backtest.py::test_existing_contract_defs_byte_unchanged`).
- Added the `open_macro_v03_metric_backtest` job type as a new `oneOf` variant
  in `job-request` (`open_macro_v03_metric_backtest_request`) and `job-result`
  (`open_macro_v03_metric_backtest_result`).
- The result variant pins evidence-only semantics as `const`:
  `runtime_activation: false`, `a5_status: "blocked"`, `official_result: false`,
  `allocator_publish: false`, `db_write: "none"`,
  `production_endpoint_activation: "none"`,
  `classification: "metric_evidence_only"`. It requires full provenance:
  `input_pack_sha256`, `contract_bundle_sha256`, `run_fingerprint`, per-metric
  `output_logical_hashes` (the five phase0q metrics + canonical hash), and
  `execution_legs` (`local_python_pure` / `qc_research_object_store`).
- The request variant pins `offline: true` and the phase0q_002 identifiers:
  reference sleeve proposal, harness window policy (primary full-basket window
  2014-03-01 → 2026-06-30), and the cost sensitivity grid `[0, 5, 10, 25]` bps
  (base 5).
- Added positive fixtures `fixtures/valid/job-request.metric-backtest.json` and
  `fixtures/valid/job-result.metric-backtest.json`.
- Compatibility: the change relative to 1.0.0 is additive (new optional
  variant; no existing constraint tightened). It ships as a MAJOR version
  directory solely because 1.0.0 is released/immutable and live-pinned by
  governance guards; producers/consumers of the two 1.0.0 job types are
  unaffected and may continue to pin v1.
- v1 remains the live pinned bundle
  (`sha256:4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a`)
  for all historical governance guards and pack-001. The certified input pack
  v2 (PR-B) must pin THIS bundle's sha; see
  `artifacts/contracts/open_macro_v03_contract_delta_001/contract_delta_report.json`.

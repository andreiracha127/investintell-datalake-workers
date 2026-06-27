# open_macro_v03 calibration 001

## Objective
Generate a candidate calibration pack from the merged Certified Input Pack P0 without activating runtime, A5, shadow mode, endpoints, or productive DB writes.

## Inputs
- input_pack_id: `open_macro_v03_certified_input_pack_001`
- input_pack_sha256: `15601edef4d72a11769c5533459884467e0e7828de439eb374b1ae98a5a97df0`
- source_snapshot_sha256: `8947bf94003403de8ffaece43ea423afe635410b0b3ccbd92bf80443a7497234`
- contract_bundle_sha256: `4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a`

## Decision
- selected_candidate_id: `baseline_current`
- rejected_candidates: `4`
- status: `candidate`
- runtime_activation: `false`
- A5: `blocked`
- freeze_ready: `false`

## Metrics
Metrics are deterministic evidence extracted from the certified pack. No live DB or external source is consulted.

## Baseline Comparison
G0, microgrid_v03, and current baseline references are recorded as not certified inside this input pack; neutral_reference is computed from the selected baseline candidate.
- final_approval_blockers: `reference_baselines_not_certified_in_pack, institutional_limits_explicitly_unset`

## Invariants
- invariant_report.ok: `true`
- no NaN/infinite outputs
- output directory closed
- network none
- DB access disabled

## Limitations
- Institutional CVaR, beta, drawdown, turnover, and exposure limits are explicitly unset.
- The pack remains candidate-only even when reproducibility gates pass.

## Accepted Technical Debt
- macro-history-coverage
- macro-vintage-identity

## Next Gate
Technical and quantitative review of the candidate calibration evidence before any shadow-readiness preparation.

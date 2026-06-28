# open_macro_v03 calibration 001

Status: candidate

This note records the candidate calibration pack generated from the post-merge
Certified Input Pack P0 baseline. PR #4 merged this pack to `main` at
`08fccef698195decaf814fcdd03c45e249bae8ad`; the pack is validated as a
calibration candidate only. The full machine-readable report lives in
`artifacts/calibration/open_macro_v03_calibration_001/`.

## Inputs

- input_pack_id: `open_macro_v03_certified_input_pack_001`
- input_pack_sha256: `ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f`
- source_snapshot_sha256: `8947bf94003403de8ffaece43ea423afe635410b0b3ccbd92bf80443a7497234`
- contract_bundle_sha256: `4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a`
- input_pack_p0_merge_commit: `50511d78a34ebc6b3b2a54ba82ad157d2c13be15`

## Governance

- runtime_activation: `false`
- A5: `blocked`
- freeze_ready: `false`
- shadow mode: not started
- productive DB writes: none

The accepted technical debt remains:

- `macro-history-coverage`
- `macro-vintage-identity`

Next gate: inert Shadow Readiness preparation. This does not start shadow
execution, activate runtime, unblock A5, mark freeze readiness, or publish any
official allocator result.

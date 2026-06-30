# open_macro_v03 Controlled Shadow 001

This bundle is artifact-only evidence for `open_macro_v03_controlled_shadow_001`.
It does not activate runtime, unblock A5, publish official results, write to a
productive database, publish to allocator, or expose a production endpoint.

## Boundary

- `A5=blocked`
- `runtime_activation=false`
- `freeze_ready=false`
- `official_result=false`
- `allow_db_write=false`
- `allow_allocator_publish=false`
- `production_endpoint_activation=none`
- `backend_executes_engine=false`
- `backend_executes_docker=false`
- `backend_executes_subprocess=false`

## Immutable Inputs

- Input pack: `open_macro_v03_certified_input_pack_001`
- Input pack SHA-256: `ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f`
- Calibration: `open_macro_v03_calibration_001`
- Calibration config SHA-256: `869e392bd49c8f7e0bf60890d1658ef3cf0483655af3a1c9f105b99cd29c268c`
- Calibration run matrix SHA-256: `58b056ba7af0b419427de8ef6f9fbb718afca9bcd576224bf557d16401ab38ac`
- Contract bundle SHA-256: `4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a`

## Acceptance

The controlled-shadow gate requires `mismatch_count=0`, all immutable input
hashes to match the checked-in artifacts, no missing or unexpected outputs, and
no side-effect attempt markers in manifests or logs.

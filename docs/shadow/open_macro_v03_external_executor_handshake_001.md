# open_macro_v03 External Executor Handshake 001

## Status

Candidate artifact-only handshake evidence.

## Dependencies

- Backend control-plane PR: `andreiracha127/investintell-light#3`.
- Backend control-plane merge commit: `ba7bc6c2f2f472fdf9e8318de5fd3804efc2cc71`.
- Workers runtime skeleton merge commit: `87e69a8cfb7aa646d1d0c7c9d7610ce914514cc7`.

## Governance

- A3: `open_macro_v03`.
- A4: `external_executor_handshake_validated` only after this PR is merged.
- A5: `blocked`.
- `freeze_ready=false`.
- `runtime_activation=false`.
- `official_result=false`.
- Backend runtime execution: `none`.
- Allocator impact: `none`.
- Production impact: `none`.

## Evidence Artifacts

Artifacts live under:

`artifacts/handshake/open_macro_v03_external_executor_handshake_001/`

The bundle includes a control-plane request, shadow job envelope, executor
acceptance, artifact-only result reference, shadow result manifest, output
manifest, validation report, no-side-effects report, reproducibility report,
human-readable report, and required logs.

## Explicit Non-Goals

- No A5 activation.
- No runtime activation.
- No controlled shadow execution.
- No official result.
- No allocator publish.
- No productive DB write.
- No production endpoint activation.
- No backend Docker/subprocess execution.
- No formula, input pack, calibration pack, or contract v1 change.

## Validation

Primary local gate:

```powershell
python -m pytest tests/test_external_executor_handshake.py -q
```

The focused gate validates schema compatibility, provenance pins, side-effect
pins, output manifest hashes/logs, read-only input/calibration mounts, output-only
writability, Docker `--network none` policy, repeatability evidence, and absence
of runtime-side-effect imports in the handshake validator.

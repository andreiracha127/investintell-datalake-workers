# Quant Engine Contract — Compatibility Policy & Changelog

The worker repository owns these schemas; the backend mirrors their hashes
(`backend/app/contracts/quant_engine_v1.py`). The bundle is versioned and
verifiable via `manifest.json` (`contract_version` + per-file `sha256` +
`bundle_sha256`). Regenerate with `python scripts/contract_bundle.py build`;
gate with `python scripts/contract_bundle.py verify`.

## Versioning policy (SemVer for the contract surface)

`contract_version` follows Semantic Versioning over the **public** schema surface
(required fields, enums/consts, value constraints, `additionalProperties` policy):

| Change | Example | Compatibility | Version bump |
|---|---|---|---|
| New required field | `required += "x"` | Breaks existing producers | **MAJOR** |
| New optional field | added property, readers tolerant | Compatible | **MINOR** |
| Tighter value constraint | enum reduced, narrower bounds | May break producers/consumers | **MAJOR** |
| Loosened constraint | enum widened, wider bounds | Compatible for existing data | **MINOR** |
| New fixture / example | added positive/negative case | Compatible | PATCH or MINOR by scope |
| Docs / description only | comment, `$comment`, changelog | Compatible | **PATCH** |

Rules:

- A released `contract_version` is immutable. Any change to a published schema
  ships as a new version; never edit a released schema in place.
- Every schema change must be classified against this table in the PR description
  and reflected here before merge.
- `manifest.json` must be regenerated and `verify` must pass in CI for the bundle
  to be considered consistent across the worker and backend repositories.

## Changelog

### 1.0.0 — 2026-06-26

- Initial frozen contract surface: `job-request`, `job-result`, and
  `engine-manifest` JSON Schemas (Draft 2020-12).
- Governance invariants encoded as `const` in `job-result`: `runtime_activation`
  is `false`, `a3_status` `open_macro_v03`, `a4_status`
  `harness_ready_provisional_A3`, `a5_status` `blocked`.
- `job-request` and `engine-manifest` require `offline: true`.
- Added positive fixtures (`fixtures/valid/`) and negative fixtures
  (`fixtures/invalid/`, one clear violation each) covering all three schemas.
- Added `manifest.json` with per-file `sha256` and a `bundle_sha256`, and the
  `scripts/contract_bundle.py` build/verify tool.

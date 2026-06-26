# Certified Input Packs v1

## Decision

Certified Input Packs v1 defines the immutable data boundary for official
quant-engine calibration and shadow runs. The first wave only introduces the
contract, schemas, hashing rules, verifier, and deterministic fixtures. It does
not migrate every worker and it does not start calibration.

The quant-engine official path must receive:

```text
--input-pack certified_input_pack/
```

instead of consulting live database tables. Any certified path that needs a
legacy DB fallback must fail closed until that source has been captured into the
pack.

## Minimal Pack Layout

```text
certified_input_pack/
  manifest.json
  SOURCE.json
  raw_snapshot_manifest.json
  canonical_snapshot_manifest.json
  derived_feature_manifest.json
  table_hashes.json
  provenance.json
  schemas/
  data/
  fixtures/
```

`manifest.json` is the canonical index. The minimum v1 shape is:

```json
{
  "input_pack_id": "open_macro_v03_input_pack_001",
  "input_pack_version": "v1",
  "as_of": "YYYY-MM-DD",
  "contract_bundle_sha256": "2cdea4d4...",
  "source_repo": "investintell-datalake-workers",
  "source_commit": "<commit>",
  "builder_commit": "<commit>",
  "builder_image_digest": "sha256:<digest>",
  "raw_snapshot_sha256": "<sha256>",
  "canonical_snapshot_sha256": "<sha256>",
  "derived_feature_sha256": "<sha256>",
  "input_pack_sha256": "<sha256>",
  "runtime_activation": false
}
```

The manifest deliberately keeps `runtime_activation=false`; activation belongs
to later shadow/release gates, not to the pack contract.

## Hashing Contract

All JSON files are hashed after canonical JSON serialization:

- UTF-8 encoding.
- Objects sorted by key.
- Compact separators.
- No path-dependent metadata in the digest.
- Lists preserve order because order can be semantically meaningful.

Non-JSON artifacts are hashed as bytes.

`input_pack_sha256` avoids circularity by hashing:

- every pack file except `manifest.json`, recorded as sorted
  `{path, sha256}` entries using relative POSIX paths;
- `manifest.json` normalized with `input_pack_sha256` set to the empty string.

This means a material data change, a schema change, a provenance change, or a
manifest-field change all alter the aggregate pack hash, while absolute checkout
paths do not.

## Required Verifier Behavior

The standalone verifier must fail closed when:

- any required top-level file is missing;
- a required directory is missing;
- `runtime_activation` is not exactly `false`;
- `raw_snapshot_sha256`, `canonical_snapshot_sha256`, or
  `derived_feature_sha256` does not match its corresponding manifest file;
- any `table_hashes.json` entry points to a missing artifact or mismatched hash;
- `input_pack_sha256` does not match the recomputed aggregate pack hash;
- `manifest.json` does not satisfy `schemas/input_packs/input_pack_manifest.schema.json`;
- provenance is absent or lacks dataset/job/run/source identity.

The verifier must not connect to the DB. v1 verification is offline by design.

## Certification Waves

Wave 1: pack skeleton and source of truth.

- Define schemas, manifest format, canonical hashing, verifier, fixtures, golden
  pack, and tampering tests.
- Do not migrate formulas yet.

Wave 2: P0 sources that feed `fund_risk_metrics`.

- `risk_metrics`
- `active_share_metrics`
- `momentum_metrics`

The pack must reconstruct the required inputs from raw/canonical sources rather
than consuming `fund_risk_metrics` as the source of truth.

Wave 3: factors and characteristics.

- `characteristics`
- `factor_model`
- `fund_factors`

Any certified route must remove or block fallback to legacy DB paths.

Wave 4: macro, regime, and prices.

- `macro_ingestion`
- `credit_regime`
- `regime_composite`
- `eod_prices_warmer`

These become point-in-time hashed snapshots.

Wave 5: quant-engine integration.

The quant-engine official interface accepts only:

- `certified_input_pack`
- `contract_bundle_sha256`
- `calibration_config`

Official execution refuses to run unless the pack verifies.

## Calibration Gate

Calibration may start only after a pack has:

- valid `input_pack_sha256`;
- closed manifest;
- complete provenance;
- tampering fixtures;
- raw/canonical source hashes;
- schema version;
- explicit `as_of`;
- standalone verifier;
- reproducible golden pack;
- quant-engine consumption without DB access.

Until then, calibration remains blocked because live derived tables would
contaminate the official input boundary.

## Current Branch Scope

This branch owns only the v1 skeleton:

- `docs/specs/certified_input_packs_v1.md`
- `src/input_packs/`
- `tests/input_packs/`
- `schemas/input_packs/`
- `fixtures/input_packs/`

The expected sequence is:

1. `docs: define certified input pack v1 architecture`
2. `feat(input-packs): add manifest and hashing primitives`
3. `test(input-packs): capture deterministic pack golden`


# Quant Engine Contract v3 — Compatibility Policy & Changelog

The worker repository owns these schemas; see `../v1/CHANGELOG.md` for the
versioning policy (SemVer over the public schema surface). This bundle is
versioned and verifiable via its own `manifest.json`. Regenerate with
`python scripts/contract_bundle.py build --bundle-dir contracts/quant-engine/v3
--contract-version 3.0.0`; gate with
`python scripts/contract_bundle.py verify --bundle-dir contracts/quant-engine/v3`.

Delivery note: v1 and v2 are released, immutable contract surfaces. v2 in
particular is the bundle the live certified input packs are hash-bound to
(`contract_bundle_sha256` inside every pack manifest, recorded per entry in
`contracts/input-packs/registry.json`), so editing it in place would falsify
those pins. Per the versioning policy, v3 ships as a NEW versioned bundle
directory.

## Changelog

### 3.0.0 — 2026-07-31

**An activated result is representable.**

v2 pinned the blocked state into the schema with `const`:

```json
"runtime_activation": {"const": false},
"a5_status":          {"const": "blocked"},
"official_result":    {"const": false},
"allocator_publish":  {"const": false},
"db_write":           {"const": "none"},
"production_endpoint_activation": {"const": "none"}
```

The `oneOf` had exactly three variants and none of them admitted an activated
result, so a job that produced `runtime_activation: true` was *invalid by
contract*. That is not an invariant about data: `db_write: "none"` as a `const`
does not prevent a bad write, it prevents **any** write. Promoting the engine
out of evidence-only would have required writing a whole new contract — which is
precisely what this version is, done once, so it never has to happen again.

v3 replaces those `const` pins with the real domains:

| field | v2 | v3 |
|---|---|---|
| `runtime_activation` | `const: false` | `type: boolean` |
| `freeze_ready` | `const: false` | `type: boolean` |
| `a5_status` | `const: "blocked"` | `enum: ["blocked", "active"]` |
| `official_result` | `const: false` | `type: boolean` |
| `allocator_publish` | `const: false` | `type: boolean` |
| `db_write` | `const: "none"` | `enum: ["none", "publication"]` |
| `production_endpoint_activation` | `const: "none"` | `enum: ["none", "live"]` |
| `classification` (metric backtest) | `const: "metric_evidence_only"` | `enum: ["metric_evidence_only", "productive_result"]` |
| `classification` (pack dry run) | `const: "input_pack_verified"` | `enum: ["input_pack_verified", "input_pack_rejected"]` |
| `status` (pack dry run) | `const: "succeeded"` | `enum: ["succeeded", "failed"]` |
| `errors` (pack dry run) | `maxItems: 0` | `type: array` |
| engine manifest `runtime_activation` | `const: false` | `type: boolean` |

`offline: {"const": true}` on the request and the engine manifest is
**unchanged**: no-network is a genuine invariant of this engine, not a
governance flag.

**The decision moves to the execution envelope.** Whether a run may be activated
is a governance question with an owner and an approval matrix — the same place
`src/workers/open_macro_v03.py::check_governance` already answers it. The schema
only says what shapes are expressible. `investintell_quant_engine.preflight`
therefore gains `validate_runtime_mode(report, mode=...)`, which is still
fail-closed:

* `offline_evidence` (the default) — every activation field must be off, exactly
  the guarantee `validate_runtime_disabled` used to give;
* `activated` — the activation fields must be consistently ON.

A job that asked for `offline_evidence` and reported `db_write != "none"` fails,
and so does a job that asked for `activated` and reported a blocked A5. The
inconsistency, not the state, is the error.

**Fixtures.** `fixtures/invalid/job-result.runtime-activation-true.json` and
`fixtures/invalid/engine-manifest.runtime-activation-true.json` encoded nothing
but the unrepresentability; they are now `fixtures/valid/job-result.activated.json`
and `fixtures/valid/engine-manifest.activated.json`, joined by
`fixtures/valid/job-result.metric-backtest.activated.json`. The negative cases
are now genuine schema violations: a value outside `a5_status`'s enum and a value
outside `db_write`'s enum.

**Migration.** Nothing is forced onto v3. Every existing pack stays bound to the
bundle its registry entry records; the engine resolves the bundle per pack. A
pack certified under v3 simply records the v3 digest.

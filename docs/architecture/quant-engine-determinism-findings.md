# Quant Engine — Determinism Repeatability Findings

Date: 2026-06-26
Lane: `feat/quant-engine-isolation`
Gate: report "Gates técnicos de determinismo e repetibilidade" (closed comparator
+ host/container × jobs matrix).

## What was built

- `investintell_quant_engine.comparator.compare_manifests` — closed comparator
  reporting `missing` / `unexpected` / `mismatched` / `duplicate_paths` /
  `status_match`. Hardened so an empty or partial actual manifest never passes as
  "0 mismatches" (it iterates the union of paths, not the intersection).
- `investintell_quant_engine.outputs_manifest.build_outputs_manifest` — closed
  manifest of every artifact in an output dir (`path`, `sha256`, `bytes`).
  - Raw view (`canonical=False`): byte digest, for provenance/audit envelopes.
  - Canonical view (`canonical=True`): strips non-semantic fields before hashing
    so semantically-identical runs match bit-a-bit. Non-semantic fields are split
    into `VOLATILE_FIELDS` (nondeterministic noise: timestamps, ids, env, host
    paths) and `OPERATIONAL_FIELDS` (deterministic knobs echoed in the envelope:
    `jobs`). Their union is `NON_SEMANTIC_FIELDS`.
- `investintell_quant_engine.repeatability.compare_run_group` — aggregates a
  group of runs against a baseline into a determinism verdict (`mismatch_count`,
  `divergent`, `sufficient`).
- `scripts/repeatability_matrix.py` — runs the G0 case across host/container and
  jobs=1/4, N repetitions, builds canonical manifests, and emits a matrix report.

## Evidence (G0 case)

| Matrix | Runs | mismatch_count | Verdict |
|---|---:|---:|---|
| Within-host (jobs 1 & 4, ×3) | 6 | 0 | PASS — bit-a-bit reproducible, incl. jobs=1 vs jobs=4 |
| Within-container (jobs 1 & 4, ×3) | 6 | 0 | PASS — bit-a-bit reproducible, incl. jobs=1 vs jobs=4 |
| Cross-env (host vs container) | 8 | 4 | DIVERGENT — see finding below |

Host G0 reproduces the golden hashes exactly:
`run_fingerprint=d1c8e0da…3024`, `metrics=70014a0a…00d3`, `runtime=de46dfb7…3024`.

The six core semantic hashes are identical host == container:
`model_evaluation_hash`, `metrics_canonical_logical_hash`,
`runtime_replay_logical_hash`, `counterfactual_replay_logical_hash`,
`a31_config_hash`, `a32_config_hash`.

The pre-fix cross-environment run isolated divergence to exactly two fields of
`qc_a3_parity_report.json` (canonical view): `parent_hashes/config_catalog_hash`
and `evaluation_hash` (plus the purely diagnostic `comparison/expected_metrics_path`,
a raw filesystem path treated as non-semantic). Investigation split this into two
independent root causes, F1a and F1b.

## Finding F1a — absolute input path leaked into the catalog hash (FIXED)

`src/calibration_harness.py:normalize_a31_catalog` baked the absolute input path
into the hashed payload:

```python
normalized = {
    "schema_version": A31_GRID_SCHEMA_VERSION,
    "source_path": str(source_path),   # <-- absolute input path
    ...
    "configs": normalized_configs,
}
return normalized, logical_payload_hash(normalized)   # catalog_hash includes the path
```

So `config_catalog_hash` depended on where the catalog file lived (`E:\…` on host
vs `/input/combo/…` in container).

**Fix applied (under sign-off):** `source_path` is excluded from the hashed payload
(kept only as out-of-band diagnostic metadata):

```python
hashable = {k: v for k, v in normalized.items() if k != "source_path"}
return normalized, logical_payload_hash(hashable)
```

Test: `tests/test_catalog_hash_path_independence.py` proves the hash is invariant
to `source_path`. Golden re-captured under change control: `config_catalog_hash`
`a01049…2d3d` → `43ecdc…2e4d`. A controlled diff confirmed this is the **only**
field that changed; all model/metric/runtime/counterfactual hashes and the bundle
`evaluation_hash` are unchanged (see `recapture_note` in the golden manifest).

## Finding F1b — bundle evaluation_hash depends on ambient git (operationally pinned)

`evaluation_hash` is **not** a path leak. `bundle_evaluation_hash`
(`investintell_quant_core.a3.metrics`) hashes a fixed field set whose only
environment-sensitive input is `worker_commit`. And
`qc_a3_core.run_parity` computes `worker_commit = config.worker_commit or
current_git_commit()` — reading **ambient git** when not supplied. The host has a
`.git`; the container image does not, so the two derived different commits and
therefore different `evaluation_hash` values.

This matters because `evaluation_hash` is the **object-store prefix key**
(`qc_a3_core.immutable_object_store_prefix(worker_commit, evaluation_hash)`):
ambient-git dependence would scatter the same logical computation across prefixes
by environment. The project inventory already lists "Git metadata collection" as
I/O that must stay outside the core.

**Resolution:** provenance is injected by the dispatcher, not read from ambient
git. `scripts/repeatability_matrix.py` now resolves `worker_commit` once and passes
it (`--worker-commit`) to every host and container run. With this, cross-env is
bit-a-bit identical (`mismatch_count=0`, 8 runs). **Recommended follow-up
(not done here):** make the engine require an explicit `worker_commit` (or record
ambient git only as a separate `observed_git` field, never folded into the bundle
hash), so the object-store key can never depend on the execution environment.

## Post-fix evidence

After F1a (code fix + golden re-capture) and F1b (pinned provenance), the cross-env
matrix is bit-a-bit identical:

| Matrix | Runs | mismatch_count | Verdict |
|---|---:|---:|---|
| Cross-env host vs container (jobs 1 & 4, ×2), post-fix | 8 | 0 | PASS |

Governance unchanged: A4 `harness_ready_provisional_A3`, A5 `blocked`,
`runtime_activation=false`. No model formula or candidate-selection rule was
modified; F1a/F1b are hashing-provenance defects, not model changes.

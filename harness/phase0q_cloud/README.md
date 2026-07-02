# phase0q cloud-leg PREPARATION (build-only)

Prepares — but never executes — the `qc_research_object_store` leg of the
open_macro_v03 phase0q reproducibility matrix
(`local_python_pure` × `qc_research_object_store`), targeting QuantConnect project
`open_macro_v03_phase0q_harness` (id **33679769**). Mirrors the proven
`qc-a3-parity` pattern: immutable Object Store prefix, per-object `content_sha256`,
drift refusal, and a fail-loud `src/db.py` stub.

**No component here performs any network call, any `lean` invocation, or any Object
Store upload.** The orchestrator runs the reviewed upload / push / fetch commands
separately in the main session. Governance stays pinned throughout: A5 blocked;
`runtime_activation` / `activation_allowed` / `allocator_publish` / `official_result`
all false; `db_write_mode` none; `status` candidate_not_approved; `approved` false.

## Modules

| module | purpose |
|---|---|
| `bundle.py` | build a deterministic, byte-identical LOCAL bundle (pack v2 tree, gzipped harness + `src` + `investintell_quant_core` closure, scenario/config, expected-results manifest, object-store manifest with immutable prefix + per-object sha256). Drift refusal: fails if any shipped source differs from its git HEAD blob. |
| `upload_plan.py` | emit (does NOT run) the ordered `lean cloud object-store set <key> <path>` plan (manifest LAST) as JSON + a human-readable review script, plus post-upload verification commands. |
| `phase0q_cloud_leg.ipynb` | QC Research notebook: pull objects via QuantBook object store with sha verification, materialize the harness/src with the fail-loud db stub, re-run the local-leg computation (decision chain + `baseline_100` / `compressed_50` sleeves at base 5bps minimum, full grid if runtime permits), recompute canonical logical hashes, compare to the expected manifest (exact hash / 1e-12), emit a verdict JSON back to the object store. |
| `fetch_results.py` | validate a fetched verdict JSON + complete `consolidated_reproducibility_report.json`. |
| `qc_project/` | QC project workspace for `lean cloud push` (`config.json` → 33679769, placeholder `main.py` that raises, notebook copy, manifest-key file). |

## Reviewed commands the orchestrator runs (separately, in the main session)

The immutable prefix is
`investintell/open_macro_v03/phase0q/<harness_commit>/<pack_sha>/`.

```sh
# (a) build the LOCAL bundle (no network, no upload)
python -m harness.phase0q_cloud.bundle <harness_commit> \
    --bundle-dir build/phase0q_cloud_bundle

# emit the upload plan (no lean, no network)
python -m harness.phase0q_cloud.upload_plan --bundle-dir build/phase0q_cloud_bundle

# (b) upload — run the emitted, reviewed plan (manifest LAST)
sh build/phase0q_cloud_bundle/upload_plan.sh

# (c) push the QC project workspace
lean cloud push --project harness/phase0q_cloud/qc_project

# ... open and run phase0q_cloud_leg.ipynb in QC Research; it emits the verdict ...

# (d) fetch the verdict + complete the consolidated report
lean cloud object-store get \
    investintell/open_macro_v03/phase0q/<harness_commit>/<pack_sha>/results/phase0q_cloud_verdict.json \
    > phase0q_cloud_verdict.json
python -m harness.phase0q_cloud.fetch_results \
    --verdict phase0q_cloud_verdict.json \
    --expected-manifest build/phase0q_cloud_bundle/expected_results_manifest.json \
    --out artifacts/quant/open_macro_v03_cloud_leg_001/consolidated_reproducibility_report.json
```

`build/phase0q_cloud_bundle/` is LOCAL and never committed (and never under any
`data/` path segment).

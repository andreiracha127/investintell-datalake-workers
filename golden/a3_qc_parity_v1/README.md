# A3 QC Parity Golden

This directory versions only small golden summaries for the G0 parity case. It
does not include proprietary bundles, NPZ panels, Parquet files, replay rows, or
object-store payloads.

Run from the workers repository root:

```powershell
python qc_a3_core.py run-parity --feature-manifest "E:\investintell-datalake-workers-combo\_tmp_qc_a3_parity_1138754_cloud_20260625\manifests\feature_manifest.json" --revision-uncertainty-manifest "E:\investintell-datalake-workers-combo\_tmp_qc_a3_parity_1138754_cloud_20260625\manifests\revision_uncertainty_manifest.json" --config-catalog "E:\investintell-datalake-workers-combo\_tmp_qc_a3_parity_1138754_cloud_20260625\manifests\config_catalog.normalized.json" --a32-grid-dir "E:\investintell-datalake-workers-combo\_tmp_a32_grid_selected_4827ce4_20260625" --expected-v03-grid-dir "E:\investintell-datalake-workers-combo\_tmp_a31_v03_revision_robust_g1_e6a72c3_20260625" --macro-l2-npz "E:\investintell-datalake-workers-combo\_tmp_qc_a3_parity_1138754_cloud_20260625\panels\macro_l2_union_numeric.npz" --revision-uncertainty-npz "E:\investintell-datalake-workers-combo\_tmp_qc_a3_parity_1138754_cloud_20260625\panels\revision_uncertainty_numeric.npz" --a31-name V03-G0-CONTROL --a32-name A32-G0.35-I0.35-X0.10-C0.60-D1.25 --worker-commit b01af4d621b4c09842ceec093679075cc30906cb --output-dir _tmp_quant_engine_golden_baseline_current
```

Expected status:

- `mismatch_count=0`
- `runtime_row_count=3221`
- `counterfactual_row_count=3221`
- `metric_row_count=5`
- `runtime_activation=false`
- `freeze_ready=false`
- `a4_status=harness_ready_provisional_A3`
- `a5_status=blocked`


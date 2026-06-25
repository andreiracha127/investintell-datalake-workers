# qc-a3-parity

QuantConnect Research parity pilot for the Investintell A3 macro harness.

This project is diagnostic-only:

- core A3 inputs come from immutable Investintell PIT panels in Object Store;
- QC FRED/History must not feed the macro classifier;
- returns are not an A3 selection objective;
- `runtime_activation=false`;
- `A4=harness_ready_provisional_A3`;
- `A5=blocked`.

Object Store manifest:

```text
investintell/a3/qc-a3-parity/25375bb/10198d7603036c3327ac9e67/object_store_manifest.json
```

Run `qc_a3_parity.ipynb` twice after a kernel restart. It writes:

```text
results/qc_cloud_parity_report.json
results/qc_cloud_environment.json
```

Approval gate:

```text
runtime rows        = 3221
counterfactual rows = 3221
metric rows         = 5
mismatch_count      = 0
external_macro_access = false
```

The large `src/calibration_harness.py` file is materialized at runtime from a
SHA-verified `calibration_harness.py.gz` Object Store object because QC cloud
source files have a size limit. The notebook also writes a short `src/db.py`
stub at runtime that fails loudly if anything tries to access a database from
Research.

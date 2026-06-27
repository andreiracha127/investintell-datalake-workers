# Railway CI image for quant-engine calibration PR gates.
# This is intentionally separate from production worker Dockerfiles.
FROM python:3.13-slim@sha256:2b7445fb71ca9cb15e9aab053fe8cb3162796f8e1d92ada12a49c766a811bc1e

WORKDIR /app

ENV PYTHONPATH=/app:/app/packages/investintell_quant_core/src:/app/services/quant_engine/src \
    PYTEST_DISABLE_PLUGIN_AUTOLOAD=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.quant-engine.lock /app/requirements.quant-engine.lock
RUN pip install --no-cache-dir -r /app/requirements.quant-engine.lock jsonschema pytest

COPY packages/investintell_quant_core /app/packages/investintell_quant_core
COPY services/quant_engine /app/services/quant_engine
COPY contracts/quant-engine /app/contracts/quant-engine
COPY schemas/input_packs /app/schemas/input_packs
COPY scripts /app/scripts
COPY src /app/src
COPY tests /app/tests
COPY fixtures/input_packs /app/fixtures/input_packs
COPY artifacts/calibration/open_macro_v03_calibration_001 /app/artifacts/calibration/open_macro_v03_calibration_001
COPY artifacts/shadow/open_macro_v03_shadow_001 /app/artifacts/shadow/open_macro_v03_shadow_001
COPY docs/calibration /app/docs/calibration
COPY docs/shadow /app/docs/shadow
COPY qc-a3-parity /app/qc-a3-parity
COPY docker/railway-ci /app/docker/railway-ci
COPY docker/quant-engine /app/docker/quant-engine
COPY qc_a3_core.py /app/qc_a3_core.py

RUN git init . \
    && git config user.email "ci@investintell.local" \
    && git config user.name "Investintell CI" \
    && git add . \
    && git commit -m "railway ci synthetic checkout" \
    && git rev-parse --verify HEAD

RUN python - <<'PY'
from pathlib import Path
import json
from src.input_packs.verifier import verify_pack

pack = Path("fixtures/input_packs/golden/certified_input_pack")
result = verify_pack(pack)
print(json.dumps({
    "ok": result["ok"],
    "input_pack_sha256_match": result["input_pack_sha256_match"],
    "expected_input_pack_sha256": result["expected_input_pack_sha256"],
    "actual_input_pack_sha256": result["actual_input_pack_sha256"],
}, sort_keys=True))
if not result["ok"] or not result["input_pack_sha256_match"]:
    raise SystemExit(1)
PY

RUN python scripts/contract_bundle.py verify
RUN python -m pytest tests/input_packs tests/quant_engine tests/quant_core \
    tests/test_calibration_candidate.py tests/test_qc_a3_core.py \
    tests/test_catalog_hash_path_independence.py tests/test_repeatability_matrix.py \
    tests/test_shadow_readiness.py -q
RUN python -m compileall src/calibration_candidate.py src/input_packs services/quant_engine packages/investintell_quant_core

RUN python - <<'PY'
from pathlib import Path
import json
from src.input_packs.hashing import file_sha256
from src.calibration_candidate import output_manifest

root = Path("artifacts/calibration/open_macro_v03_calibration_001")
manifest = json.loads((root / "calibration_manifest.json").read_text(encoding="utf-8"))
run_matrix = json.loads((root / "run_matrix.json").read_text(encoding="utf-8"))
invariant = json.loads((root / "invariant_report.json").read_text(encoding="utf-8"))
stored_output_manifest = json.loads((root / "output_manifest.json").read_text(encoding="utf-8"))
checks = {
    "calibration_config_sha256": "calibration_config.json",
    "parameter_grid_sha256": "parameter_grid.json",
    "output_manifest_sha256": "output_manifest.json",
    "run_matrix_sha256": "run_matrix.json",
    "reproducibility_report_sha256": "reproducibility_report.json",
    "selected_parameters_sha256": "selected_parameters.json",
    "rejected_candidates_sha256": "rejected_candidates.json",
    "metrics_manifest_sha256": "metrics_manifest.json",
    "invariant_report_sha256": "invariant_report.json",
    "baseline_comparison_sha256": "baseline_comparison.json",
}
for key, rel in checks.items():
    actual = file_sha256(root / rel)
    if manifest[key] != actual:
        raise SystemExit(f"{key} mismatch: expected {manifest[key]}, got {actual}")
required_output_manifest_paths = {
    "calibration_config.json",
    "parameter_grid.json",
    "selected_parameters.json",
    "rejected_candidates.json",
    "metrics_manifest.json",
    "invariant_report.json",
    "baseline_comparison.json",
    "calibration_report.md",
    "logs/calibration.log",
}
actual_output_manifest_paths = {entry.get("path") for entry in stored_output_manifest.get("artifacts", [])}
if actual_output_manifest_paths != required_output_manifest_paths:
    raise SystemExit(
        "output_manifest artifacts mismatch: "
        f"expected {sorted(required_output_manifest_paths)}, got {sorted(actual_output_manifest_paths)}"
    )
expected_output_manifest = output_manifest(root, sorted(required_output_manifest_paths))
if stored_output_manifest != expected_output_manifest:
    raise SystemExit("output_manifest entries do not match current artifact contents")
required = {
    "input_pack_sha256": "ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f",
    "runtime_activation": False,
    "A5": "blocked",
    "freeze_ready": False,
}
for key, expected in required.items():
    if manifest[key] != expected:
        raise SystemExit(f"{key} mismatch: expected {expected}, got {manifest[key]}")
if not run_matrix["ok"] or run_matrix["comparison_evidence"]["mismatch_count"] != 0:
    raise SystemExit("run_matrix evidence is not green")
if not invariant["ok"]:
    raise SystemExit("invariant report is not green")
print(json.dumps({"railway_ci": "ok", "head_engine_commit": manifest["engine_commit"]}, sort_keys=True))
PY

CMD ["sh", "-c", "python -m http.server \"${PORT:-8080}\" --bind 0.0.0.0"]

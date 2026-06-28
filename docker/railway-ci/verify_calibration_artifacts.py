"""Railway CI gate: verify open_macro_v03 calibration artifact integrity.

Extracted from a Dockerfile ``RUN`` heredoc so the image builds under both the
BuildKit and legacy Docker builders (heredoc ``RUN`` blocks are BuildKit-only
syntax). Run from the image WORKDIR (``/app``) via ``python
docker/railway-ci/verify_calibration_artifacts.py``.
"""

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

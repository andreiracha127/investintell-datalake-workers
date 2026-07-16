from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERIFIER = ROOT / "scripts" / "ci" / "verify_quant_requirements.py"


def _run_verifier(input_path: Path, lock_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(VERIFIER),
            "--input",
            str(input_path),
            "--lock",
            str(lock_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_verifier_accepts_synced_lock_and_rejects_stale_lock(tmp_path: Path) -> None:
    input_path = tmp_path / "requirements.in"
    lock_path = tmp_path / "requirements.lock"
    input_path.write_text("alpha>=1\nbeta>=2\n", encoding="utf-8")
    lock_path.write_text("alpha==1.5\nbeta==2.1\ngamma==3.0\n", encoding="utf-8")

    synced = _run_verifier(input_path, lock_path)
    assert synced.returncode == 0, synced.stderr

    input_path.write_text("alpha>=2\nbeta>=2\n", encoding="utf-8")
    stale = _run_verifier(input_path, lock_path)
    assert stale.returncode != 0
    assert "alpha==1.5 does not satisfy alpha>=2" in stale.stderr

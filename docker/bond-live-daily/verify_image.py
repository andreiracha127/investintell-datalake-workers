"""Build-time (and operator) proof that the bond-live-daily image is the pinned stack.

Run inside the image: ``python docker/bond-live-daily/verify_image.py``. It
asserts the exact interpreter and numeric-stack versions the hash lock was
compiled for, that the lock shipped in the image matches its recorded sha256,
and that the whole daily closure imports (statsmodels through panel_resolvers
included). Stdlib + the pinned packages only; no database, no network, no
environment dumps. Exit code 0 is the only pass.
"""
from __future__ import annotations

import hashlib
import importlib
import sys
from pathlib import Path

IMAGE_DIR = Path("/app/docker/bond-live-daily")
EXPECTED_PYTHON = (3, 13, 12)
EXPECTED_STACK = {
    "numpy": "2.5.1",
    "pandas": "3.0.3",
    "scipy": "1.18.0",
    "pyarrow": "25.0.0",
    "psycopg": "3.3.3",
}
DAILY_CLOSURE = (
    "src.run_worker",
    "src.workers.bond_live_daily",
    "src.workers.bond_metrics",
    "src.workers.bond_serving",
    "src.workers.bond_panel",
    "src.workers.bond_market_implied_rating",
    "src.bonds.panel_resolvers",
    "src.bonds.implied_rating",
    "src.bonds.implied_rating_materializer",
    "scripts.backfill_bond_market_implied_rating",
)


def _recorded_sha256(sidecar: Path) -> str:
    return sidecar.read_text(encoding="ascii").split()[0]


def main() -> int:
    assert sys.version_info[:3] == EXPECTED_PYTHON, sys.version
    assert sys.implementation.name == "cpython", sys.implementation.name
    actual = {
        name: importlib.import_module(name).__version__ for name in EXPECTED_STACK
    }
    assert actual == EXPECTED_STACK, (actual, EXPECTED_STACK)
    for name in ("requirements.lock", "Dockerfile"):
        path = IMAGE_DIR / name
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        recorded = _recorded_sha256(IMAGE_DIR / f"{name}.sha256")
        assert digest == recorded, (name, digest, recorded)
    for name in DAILY_CLOSURE:
        importlib.import_module(name)
    print(
        "bond-live-daily image verified:",
        f"python={'.'.join(map(str, sys.version_info[:3]))}",
        " ".join(f"{k}={v}" for k, v in actual.items()),
        f"lock_sha256={_recorded_sha256(IMAGE_DIR / 'requirements.lock.sha256')[:16]}...",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

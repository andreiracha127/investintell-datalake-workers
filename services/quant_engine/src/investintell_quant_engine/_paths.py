"""Local source path bootstrap for repo-local execution."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
QUANT_CORE_SRC = REPO_ROOT / "packages" / "investintell_quant_core" / "src"
QUANT_ENGINE_SRC = REPO_ROOT / "services" / "quant_engine" / "src"


def ensure_repo_paths() -> None:
    for path in (REPO_ROOT, QUANT_CORE_SRC, QUANT_ENGINE_SRC):
        text = str(path)
        if path.exists() and text not in sys.path:
            sys.path.insert(0, text)

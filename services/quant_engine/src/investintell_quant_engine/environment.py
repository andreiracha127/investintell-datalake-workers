"""Environment manifest helpers."""

from __future__ import annotations

import importlib.metadata as md
import platform
import sys
from typing import Any


def collect_environment() -> dict[str, Any]:
    packages: dict[str, str | None] = {}
    for name in ("numpy", "pandas", "pyarrow", "scipy", "arch", "PyYAML"):
        try:
            packages[name] = md.version(name)
        except md.PackageNotFoundError:
            packages[name] = None
    return {
        "python_version": sys.version.split()[0],
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "packages": packages,
    }


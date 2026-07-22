"""Pytest path shim.

Make ``src.*`` and the sibling ``tests/_*_fixtures`` helpers importable without a
manually-exported ``PYTHONPATH``. The console ``pytest`` entry point (as opposed to
``python -m pytest``) does not add the repo root to ``sys.path``, so
``from src.bonds import daily_chain`` and the ``_daily_chain_fixtures`` helper imports
the chain suites rely on would fail. This conftest — loaded before any test module
under ``tests/`` — prepends the repo root and the tests directory so those suites
import cleanly under either invocation.
"""
from __future__ import annotations

import sys
from pathlib import Path

_TESTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _TESTS_DIR.parent

for _path in (_REPO_ROOT, _TESTS_DIR):
    _entry = str(_path)
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

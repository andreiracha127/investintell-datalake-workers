"""The cash-flow and pricing motors are pure, DB-free algorithm modules.

Same fresh-interpreter idiom as ``test_bonds_db_free.py``: importing either
module must not pull in any database driver, connection module, pyarrow, or
numpy, and must not read the wall clock (settlement / horizon are always
explicit inputs, never a clock read — proven by asserting the module source
never touches any clock/epoch reader).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_PURE_MODULES = ("cashflows", "pricing", "oas")


@pytest.mark.parametrize("module", _PURE_MODULES)
def test_fresh_import_is_db_free(module: str) -> None:
    program = (
        "import sys;"
        f"import src.bonds.{module};"
        "assert 'src.db' not in sys.modules, sorted(m for m in sys.modules if m.startswith('src.'));"
        "assert 'psycopg' not in sys.modules;"
        "assert not any(m == 'psycopg' or m.startswith('psycopg.') for m in sys.modules);"
        "assert 'pyarrow' not in sys.modules;"
        "assert 'numpy' not in sys.modules"
    )
    completed = subprocess.run([sys.executable, "-c", program], check=False)
    assert completed.returncode == 0


@pytest.mark.parametrize("module", _PURE_MODULES)
def test_module_reads_no_wall_clock(module: str) -> None:
    source = (
        Path(__file__).resolve().parents[2] / "src" / "bonds" / f"{module}.py"
    ).read_text(encoding="utf-8")
    # Every temporal anchor (settlement, horizon) is an explicit argument; the
    # module must never sample the wall clock. Scan for the full family of
    # clock/epoch readers, not just the three most common ones.
    for forbidden in (
        "date.today",
        "datetime.today",
        "datetime.now",
        "datetime.utcnow",
        "utcnow",
        "fromtimestamp",
        "time.time",
        "time.monotonic",
        "time.perf_counter",
        "perf_counter",
        "monotonic",
    ):
        assert forbidden not in source, f"{module}: {forbidden}"

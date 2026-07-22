"""The cash-flow motor is a pure, DB-free algorithm module.

Same fresh-interpreter idiom as ``test_bonds_db_free.py``: importing the module
must not pull in any database driver, connection module, or pyarrow, and must
not read the wall clock (settlement is always an explicit input, never a clock
read — proven by asserting the module source never touches ``datetime.today`` /
``date.today`` / ``datetime.now``).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_fresh_import_is_db_free() -> None:
    program = (
        "import sys;"
        "import src.bonds.cashflows;"
        "assert 'src.db' not in sys.modules, sorted(m for m in sys.modules if m.startswith('src.'));"
        "assert 'psycopg' not in sys.modules;"
        "assert not any(m == 'psycopg' or m.startswith('psycopg.') for m in sys.modules);"
        "assert 'pyarrow' not in sys.modules;"
        "assert 'numpy' not in sys.modules"
    )
    completed = subprocess.run([sys.executable, "-c", program], check=False)
    assert completed.returncode == 0


def test_module_reads_no_wall_clock() -> None:
    source = (
        Path(__file__).resolve().parents[2] / "src" / "bonds" / "cashflows.py"
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
        assert forbidden not in source, forbidden

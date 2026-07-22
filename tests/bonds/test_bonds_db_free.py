"""The bonds library is a pure, DB-free algorithm package.

Mirrors the repo idiom (see test_sec_class_factors's fresh-import assertion):
a fresh interpreter imports the package and every submodule and proves that no
database driver or connection module is loaded as a side effect of import.
"""

from __future__ import annotations

import subprocess
import sys


def test_fresh_import_is_db_free() -> None:
    program = (
        "import sys;"
        "import src.bonds;"
        "import src.bonds.identifiers, src.bonds.debt_mapping, src.bonds.panel_states, src.bonds.matching, src.bonds.cashflows;"
        "assert 'src.db' not in sys.modules, sorted(m for m in sys.modules if m.startswith('src.'));"
        "assert 'psycopg' not in sys.modules;"
        "assert not any(m == 'psycopg' or m.startswith('psycopg.') for m in sys.modules);"
        "assert 'pyarrow' not in sys.modules"
    )
    completed = subprocess.run([sys.executable, "-c", program], check=False)
    assert completed.returncode == 0


def test_public_exports_are_importable() -> None:
    import src.bonds as bonds

    for name in (
        "BondError",
        "FieldState",
        "IdentifierState",
        "DebtState",
        "MatchState",
        "NormalizedCusip",
        "normalize_cusip9",
        "DebtMapping",
        "PanelBuildResult",
        "ObservedPanel",
        "build_observed_panel_rows",
        "HoldingRecord",
        "Observation",
        "MatchResult",
        "ObservationIndex",
        "match_holding",
        "BondTerms",
        "Schedule",
        "generate_schedule",
        "accrued_interest",
        "day_count_days",
        "year_fraction",
    ):
        assert hasattr(bonds, name), name

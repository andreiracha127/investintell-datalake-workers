"""Worker-level tests for ``src.workers.bond_serving.run`` error routing.

Regression guard for the review Important: an integrity/coverage violation
(``BondFundExposureMultiplicationError`` / ``BondServingSurfaceCoverageError`` --
both ``RuntimeError`` subclasses) must be an ACTIONABLE signal that PROPAGATES out
of ``run()`` (spec §5: never a silent success), never laundered into the
``no_source`` dark state that a genuinely absent source produces. The worker opens
its own connection, so these tests commit an isolated schema and point ``run()`` at
it via a ``search_path`` DSN.

DSN-agnostic (Global Constraint): reads ``SEC_TEST_DATABASE_URL``.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from src.bonds import serving_materializer as materializer
from src.workers import bond_serving

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_serving_fixtures import (  # noqa: E402
    SEC2,
    connect,
    protocol_only_schema,
    setup,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)


def _search_path_dsn(schema: str) -> str:
    # The worker opens its OWN connection; route it into the isolated test schema.
    base = os.environ["SEC_TEST_DATABASE_URL"]
    if base.startswith("postgres"):  # URL form
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}options=-c%20search_path%3D{schema}"
    return f"{base} options='-c search_path={schema}'"  # keyword form


def test_run_propagates_integrity_error_and_never_reports_no_source() -> None:
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        # Force the multiplication: SEC2 now shares SEC1's CUSIP -> a holding lot
        # maps to two securities -> the fund_exposure guard must hard-fail.
        cur.execute(
            "UPDATE sec_current_bond_security_alias_v1 SET alias_value='037833100' WHERE security_id=%s",
            (SEC2,),
        )
        admin.commit()

        with pytest.raises(materializer.BondFundExposureMultiplicationError):
            bond_serving.run(_search_path_dsn(schema))
        # nothing promoted (the actionable failure never became a serving version).
        pointer = admin.execute(
            f'SELECT publication_id FROM "{schema}".sec_derived_current_pointers '
            "WHERE product='bond_serving_v1'"
        ).fetchone()
        assert pointer is None
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_run_reports_no_source_when_snapshot_is_genuinely_absent() -> None:
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = protocol_only_schema(cur)  # no sec_current_bond_security_v1
        admin.commit()
        result = bond_serving.run(_search_path_dsn(schema))
        assert result == {"state": "no_source", "rows": 0}
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()

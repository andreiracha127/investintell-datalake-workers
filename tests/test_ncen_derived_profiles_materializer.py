from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ncen_derived_fixtures import ROOT, base_fixture, dsn, fund, raw, registrant, submission  # noqa: E402

from src.ncen import derived_profiles  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

ALL_DDL = (
    "ncen_derived_common.sql",
    "ncen_structure_profiles.sql",
    "ncen_provider_network_profiles.sql",
    "ncen_operational_event_profiles.sql",
    "ncen_liquidity_backstop_profiles.sql",
    "ncen_securities_lending_profiles.sql",
    "ncen_etf_primary_market_profiles.sql",
)


def test_materializer_publishes_every_product_prepared_to_validated_to_current():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, _ = base_fixture(cur, None, ALL_DDL, create_publication=False)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="Y", IS_RELYON_RULE_6C_11="Y")
        raw(cur, run_id, "ADVISER.tsv", {"FUND_ID": "F1", "ADVISER_NAME": "Adv", "ADVISER_LEI": "LEI-A"}, fund_id="F1")
        registrant(cur, run_id, "A1", IS_MATERIAL_WEAKNESS_NOTED="Y")

        results = derived_profiles.materialize_all(
            conn, as_of=date(2026, 6, 30), source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        assert {r["product"] for r in results} == set(derived_profiles.PRODUCTS)
        assert all(r["state"] == "current" for r in results)

        for product in derived_profiles.PRODUCTS:
            cur.execute("SELECT lifecycle_state FROM sec_current_derived_publications WHERE product=%s", (product,))
            assert cur.fetchone() == ("validated",)
        for view in ("sec_current_ncen_structure_profiles", "sec_current_ncen_provider_network_profiles",
                     "sec_current_ncen_operational_event_profiles", "sec_current_ncen_liquidity_backstop_profiles",
                     "sec_current_ncen_securities_lending_profiles", "sec_current_ncen_etf_primary_market_profiles"):
            cur.execute(f"SELECT count(*) FROM {view}")
            assert cur.fetchone()[0] == 1

        # Re-running is idempotent: no new publications, pointers unchanged.
        derived_profiles.materialize_all(
            conn, as_of=date(2026, 6, 30), source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        cur.execute("SELECT count(*) FROM sec_derived_publications")
        assert cur.fetchone()[0] == len(derived_profiles.PRODUCTS)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_materializer_products_map_to_installed_build_functions():
    installed = "\n".join(
        (ROOT / "schemas" / name).read_text(encoding="utf-8") for name in ALL_DDL
    )
    for product, build_fn in derived_profiles.PRODUCTS.items():
        assert f"FUNCTION {build_fn}(" in installed, product
    # The registered worker uses the shared derived lock, not an ingestion lane.
    worker = (ROOT / "src" / "workers" / "ncen_derived_profiles.py").read_text(encoding="utf-8")
    assert "LOCK_NCEN_DERIVED_PROFILES" in worker

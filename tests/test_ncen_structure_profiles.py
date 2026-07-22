from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ncen_derived_fixtures import (  # noqa: E402
    ROOT, base_fixture, dsn, fund, prepare_second_run, raw, submission,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

DDL = ("ncen_derived_common.sql", "ncen_structure_profiles.sql")
PRODUCT = "ncen_structure_profile_v1"


def _fixture(cur):
    return base_fixture(cur, PRODUCT, DDL)


def test_structure_profile_prefers_amendment_and_encodes_flags_as_tristate():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = prepare_second_run(cur)
        submission(cur, base_run, "BASE", filing_date="2026-01-10")
        submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10", lt_12month="N")
        # The superseded base filing disagrees on IS_ETF; the accepted amendment must win.
        fund(cur, base_run, "BASE", "F1", "S1", IS_ETF="N")
        fund(cur, amended_run, "AMEND", "F1", "S1",
             IS_ETF="Y", IS_INDEX="N", IS_NON_DIVERSIFIED="Y",
             IS_RELYON_RULE_6C_11="Y", IS_RELYON_RULE_18F_4="N")
        cur.execute("SELECT build_ncen_structure_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute(
            """SELECT accession_number,report_period_lt_12month,structure_state,reliance_state,
                      structure_flags,regulatory_reliance,coverage FROM ncen_structure_profiles"""
        )
        acc, lt12, sstate, rstate, flags, reliance, coverage = cur.fetchone()
        assert acc == "AMEND"
        assert lt12 == "false"
        assert sstate == "available"
        assert rstate == "available"
        assert flags["normalized"]["etf"] == "true"
        assert flags["normalized"]["index"] == "false"
        assert flags["normalized"]["non_diversified"] == "true"
        assert flags["normalized"]["money_market"] == "not_reported"
        assert flags["source_lexical"]["etf"] == "Y"
        assert reliance["normalized"]["rule_6c_11"] == "true"
        assert reliance["normalized"]["rule_18f_4"] == "false"
        assert reliance["normalized"]["rule_17a_7"] == "not_reported"
        assert coverage["structure_flags_reported"] == 3
        assert coverage["reliance_rules_reported"] == 2
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_structure_profile_empty_flags_are_not_reported_never_coerced_to_negative():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1")  # every structure/reliance column blank
        cur.execute("SELECT build_ncen_structure_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute(
            """SELECT structure_state,structure_reason_code,reliance_state,reliance_reason_code,
                      structure_flags,regulatory_reliance FROM ncen_structure_profiles"""
        )
        sstate, sreason, rstate, rreason, flags, reliance = cur.fetchone()
        assert (sstate, sreason) == ("unavailable", "fund_structure_not_reported")
        assert (rstate, rreason) == ("unavailable", "regulatory_reliance_not_reported")
        # Tri-state: blanks are not_reported, and NONE are coerced to the negative 'false'.
        assert set(flags["normalized"].values()) == {"not_reported"}
        assert "false" not in set(flags["normalized"].values())
        assert set(reliance["normalized"].values()) == {"not_reported"}
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_structure_profile_unexpected_flag_code_is_not_reported_not_true_or_false():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="X", IS_INTERVAL="Y")
        cur.execute("SELECT build_ncen_structure_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT structure_flags FROM ncen_structure_profiles")
        flags = cur.fetchone()[0]
        assert flags["normalized"]["etf"] == "not_reported"
        assert flags["normalized"]["interval"] == "true"
        cur.execute("SELECT ncen_tristate_flag('Y'),ncen_tristate_flag('N'),ncen_tristate_flag('X'),ncen_tristate_flag(NULL)")
        assert cur.fetchone() == ("true", "false", "not_reported", "not_reported")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_structure_profile_short_period_is_flagged_from_submission():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1", lt_12month="Y")
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="Y")
        cur.execute("SELECT build_ncen_structure_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT report_period_lt_12month FROM ncen_structure_profiles")
        assert cur.fetchone()[0] == "true"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_structure_profile_fails_closed_on_missing_fund_identity_and_is_immutable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        with pytest.raises(psycopg.Error, match="missing N-CEN fund identity"):
            cur.execute("SELECT build_ncen_structure_profiles(%s,'2026-06-30')", (publication_id,))
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="Y")
        cur.execute("SELECT build_ncen_structure_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))
        with pytest.raises(psycopg.Error, match="structure profile is immutable"):
            cur.execute("UPDATE ncen_structure_profiles SET series_id='S9' WHERE publication_id=%s", (publication_id,))
        cur.execute("SELECT accession_number FROM sec_current_ncen_structure_profiles")
        assert cur.fetchone()[0] == "A1"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_structure_profile_ddl_is_ncen_native_and_leaks_no_sources():
    ddl = (ROOT / "schemas" / "ncen_structure_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_structure_profile_v1", "ncen_effective_filings", "ncen_tristate_flag",
                  "IS_RELYON_RULE_6C_11", "IS_NON_DIVERSIFIED", "sec_derived_current_pointers"):
        assert token in ddl
    for banned in ("vendor", "sha256", "cik:"):
        assert banned not in lower
    current_view = lower.split("create or replace view sec_current_ncen_structure_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view

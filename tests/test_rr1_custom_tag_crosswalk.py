from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rr1_derived_fixtures import ROOT, base_fixture, dsn, fact  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

DDL = ("rr1_custom_tag_crosswalk.sql",)
ACC = "0001234567-25-000001"  # a filer accession = the version of a custom tag


def _insert(cur, *, tag="CustomTurnover", version=ACC, xver="v1", concept="turnover_rate",
            confidence="0.95", method="documentation", review="approved"):
    cur.execute(
        """INSERT INTO rr1_custom_tag_crosswalk
        (custom_tag,custom_version,crosswalk_version,canonical_concept,confidence,method,review_status)
        VALUES(%s,%s,%s,%s,%s,%s,%s)""",
        (tag, version, xver, concept, confidence, method, review),
    )


def _resolve(cur, *, tag="CustomTurnover", version=ACC, min_conf="0.80"):
    return cur.execute(
        "SELECT rr1_crosswalk_resolve(%s,%s,%s)", (tag, version, min_conf)
    ).fetchone()[0]


def test_crosswalk_is_born_empty_of_approved_mappings():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = base_fixture(cur, None, DDL, create_publication=False)
        assert cur.execute("SELECT count(*) FROM rr1_custom_tag_crosswalk").fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_approved_high_confidence_mapping_resolves():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = base_fixture(cur, None, DDL, create_publication=False)
        _insert(cur, confidence="0.95", review="approved")
        assert _resolve(cur, min_conf="0.80") == "turnover_rate"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_low_confidence_mapping_never_resolves():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = base_fixture(cur, None, DDL, create_publication=False)
        # Approved but low confidence -> below the caller's threshold -> no resolution.
        _insert(cur, confidence="0.40", review="approved")
        assert _resolve(cur, min_conf="0.80") is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_unapproved_or_rejected_mapping_never_resolves():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = base_fixture(cur, None, DDL, create_publication=False)
        _insert(cur, tag="P", confidence="0.99", review="proposed")
        _insert(cur, tag="R", confidence="0.99", review="rejected")
        assert _resolve(cur, tag="P", min_conf="0.80") is None
        assert _resolve(cur, tag="R", min_conf="0.80") is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_latest_approved_crosswalk_version_wins():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = base_fixture(cur, None, DDL, create_publication=False)
        _insert(cur, xver="v1", concept="turnover_rate", confidence="0.95", review="approved")
        _insert(cur, xver="v2", concept="avg_annual_return", confidence="0.95", review="approved")
        assert _resolve(cur, min_conf="0.80") == "avg_annual_return"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_crosswalk_version_ordering_is_natural_not_lexical():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = base_fixture(cur, None, DDL, create_publication=False)
        # v10 is a LATER version than v2; a plain text sort would wrongly pick 'v2'
        # (because '2' > '1' lexically). Natural/numeric ordering resolves to v10.
        _insert(cur, xver="v2", concept="turnover_rate", confidence="0.95", review="approved")
        _insert(cur, xver="v10", concept="avg_annual_return", confidence="0.95", review="approved")
        assert _resolve(cur, min_conf="0.80") == "avg_annual_return"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_an_rr_namespaced_version_can_never_be_registered_as_custom():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = base_fixture(cur, None, DDL, create_publication=False)
        # A standard RR tag is not "custom"; the version guard rejects it.
        with pytest.raises(psycopg.Error):
            _insert(cur, tag="NetExpensesOverAssets", version="rr/2025", confidence="0.99", review="approved")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_approved_row_is_immutable_governance():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = base_fixture(cur, None, DDL, create_publication=False)
        _insert(cur, confidence="0.95", review="approved")
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute("UPDATE rr1_custom_tag_crosswalk SET confidence=0.10 WHERE custom_tag='CustomTurnover'")
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute("DELETE FROM rr1_custom_tag_crosswalk WHERE custom_tag='CustomTurnover'")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_approved_crosswalk_still_never_injects_a_custom_fact_into_canonical_metrics():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(
            cur, "rr1_fee_profile_v1",
            ("rr1_fee_profiles.sql", "rr1_custom_tag_crosswalk.sql"),
        )
        # A custom fact (version = accession) carrying an APPROVED, high-confidence
        # crosswalk to a canonical fee concept...
        fact(cur, run_id, "CustomManagementFee", "0.99", version=ACC, raw_row_id=1)
        _insert(cur, tag="CustomManagementFee", version=ACC, concept="management_fee",
                confidence="0.99", review="approved")
        # ...still never enters the canonical snapshot today: canonical builders read
        # only rr/%-namespaced facts.  Admitting the custom fact is a future,
        # deliberate operation, not an automatic effect of an approved crosswalk.
        assert cur.execute(
            "SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,)
        ).fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM rr1_fee_profiles")
        assert cur.fetchone() == (0,)
        # The governance surface resolves it, proving the mapping exists but is inert.
        assert cur.execute(
            "SELECT rr1_crosswalk_resolve('CustomManagementFee',%s,'0.80')", (ACC,)
        ).fetchone()[0] == "management_fee"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_crosswalk_ddl_is_versioned_governance_and_leaks_no_source_identity():
    ddl = (ROOT / "schemas" / "rr1_custom_tag_crosswalk.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_custom_tag_crosswalk", "crosswalk_version", "confidence",
                  "review_status", "rr1_crosswalk_resolve"):
        assert token in ddl
    for method in ("datatype", "documentation", "labels", "calculations", "context"):
        assert method in lower
    for forbidden in ("vendor", "sha256", "cik:", "sec_w1_nport_real", "filename"):
        assert forbidden not in lower

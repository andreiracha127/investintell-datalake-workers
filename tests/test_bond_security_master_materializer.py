"""bond_security_v1 materializer over the sec_derived_publications protocol.

DSN-agnostic (reads SEC_TEST_DATABASE_URL); disposable-schema per test.
Proves: prepared -> validated -> current promotion, same-CUSIP collapse,
placeholder rejection, ambiguous conflicting-ISIN evidence, point-in-time alias
windows, immutable observations, idempotent rerun (same uuid5 + pinned build),
and that a partial (non-validated) build can never become current.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_security_fixtures import base_fixture, dsn, observe  # noqa: E402

from src.bonds import security_master  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

AS_OF = date(2026, 6, 30)
ROOT_SCHEMAS = Path(__file__).resolve().parents[1] / "schemas"


def _install_enrichment_relations(cur, *, lei_rows=(), reference_rows=()):
    """Stand up the two OPTIONAL enrichment inputs the build reads when present.

    Both are optional in production (a build with neither still publishes), so
    they are created here rather than in ``base_fixture``: every other test in
    this file then exercises the absent path for free.
    """
    cur.execute((ROOT_SCHEMAS / "bond_reference_terms.sql").read_text(encoding="utf-8"))
    cur.execute("CREATE TABLE sec_nport_holdings_v2_current(cusip text, issuer_lei text)")
    for cusip9, lei in lei_rows:
        cur.execute(
            "INSERT INTO sec_nport_holdings_v2_current(cusip, issuer_lei) VALUES(%s,%s)",
            (cusip9, lei),
        )
    for row in reference_rows:
        cur.execute(
            "INSERT INTO bond_reference_terms"
            "(cusip9,seniority,callable,amount_outstanding_mm,coupon_rate,batch_label) "
            "VALUES(%s,%s,%s,%s,%s,'test')",
            row,
        )


def test_published_issuer_comes_from_reported_consensus_not_an_arbitrary_pick():
    """End to end: the spelling variance that used to null the issuer now names it.

    Four funds report one bond four ways (case, punctuation, corporate suffix, an
    appended coupon and maturity). The published row must carry ONE issuer name,
    a RESOLVED identity -- the CUSIP9 was never in doubt -- and the evidence
    needed to audit the choice.
    """
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        _install_enrichment_relations(
            cur,
            lei_rows=[("037833100", "5493001KJTIIGC8Y1R12")],
            reference_rows=[("037833100", "Senior", True, 525.0, None)],
        )
        for name in ("AFLAC INC", "Aflac, Inc.", "AFLAC INCORPORATED",
                     "AFLAC INC 3.6% 04/01/30"):
            observe(cur, run_id, as_of=AS_OF, observation_date=AS_OF,
                    cusip9="037833100", issuer_name=name, coupon_type="fixed",
                    coupon_rate="3.60", maturity_date=date(2030, 4, 1))

        result = security_master.materialize(
            conn, as_of=AS_OF, source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        assert result["issuer_attributed"] == 1
        assert result["issuer_abstained"] == 0

        cur.execute(
            "SELECT issuer_name, identity_state, identity_reason_code, seniority, terms,"
            "       identity_evidence, coupon_rate "
            "FROM sec_current_bond_security_v1"
        )
        issuer, state, reason, seniority, terms, evidence, coupon = cur.fetchone()
        assert issuer == "AFLAC INC"
        # The whole point: reported spelling variance is NOT identity ambiguity.
        assert state == "resolved" and reason is None
        attribution = evidence["issuer_attribution"]
        assert attribution["attribution"] == "cusip9_consensus"
        assert attribution["n_sources"] == 4
        assert attribution["top_share"] == 1.0
        assert attribution["distinct_lei"] == ["5493001KJTIIGC8Y1R12"]

        # ...and the reference filled ONLY the terms the filings never carried.
        assert seniority == "Senior"
        assert terms["callable"] is True
        assert terms["amount_outstanding_mm"] == 525.0
        assert evidence["reference_terms"]["basis"] == "vendor_reference"
        assert evidence["reference_terms"]["fields"] == [
            "amount_outstanding_mm", "callable", "seniority",
        ]
        # The chain's own coupon survives: the reference is gap-fill, not override.
        assert float(coupon) == 3.60

        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_disagreeing_reported_legal_entities_abstain_rather_than_name():
    """Two LEIs for one CUSIP9: the filers disagree about WHO issued it.

    A name consensus cannot settle that, so nothing is published -- and the
    reason rides on the row, so the abstention is visible rather than silent.
    """
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        _install_enrichment_relations(cur, lei_rows=[
            ("037833100", "5493001KJTIIGC8Y1R12"),
            ("037833100", "213800LBQA1Y9RPH2N54"),
        ])
        for _ in range(3):
            observe(cur, run_id, as_of=AS_OF, observation_date=AS_OF,
                    cusip9="037833100", issuer_name="ACME CORP")

        result = security_master.materialize(
            conn, as_of=AS_OF, source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        assert result["issuer_attributed"] == 0
        assert result["issuer_abstained"] == 1

        cur.execute("SELECT issuer_name, identity_evidence FROM sec_current_bond_security_v1")
        issuer, evidence = cur.fetchone()
        assert issuer is None
        assert evidence["issuer_attribution"]["abstain_reason"] == "multiple_lei"
        assert len(evidence["issuer_attribution"]["distinct_lei"]) == 2

        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def _seed_full_cohort(cur, run_id):
    # Same CUSIP9 across two lots/dates -> ONE resolved security.
    observe(cur, run_id, as_of=AS_OF, observation_date=date(2026, 3, 31),
            cusip9="037833100", issuer_name="Acme", currency="USD",
            coupon_type="fixed", coupon_rate="4.50", maturity_date=date(2030, 1, 1))
    observe(cur, run_id, as_of=AS_OF, observation_date=date(2026, 6, 30),
            cusip9="037833100", issuer_name="Acme", currency="USD",
            coupon_type="fixed", coupon_rate="4.75", maturity_date=date(2030, 1, 1))
    # Placeholder CUSIP with no ISIN -> rejected, never published.
    observe(cur, run_id, as_of=AS_OF, observation_date=date(2026, 6, 30), cusip9="000000000")
    # Conflicting ISIN for one CUSIP9 at one point in time -> ambiguous.
    observe(cur, run_id, as_of=AS_OF, observation_date=date(2026, 6, 30),
            cusip9="459200101", isin="US4592001014")
    observe(cur, run_id, as_of=AS_OF, observation_date=date(2026, 6, 30),
            cusip9="459200101", isin="US4592001099")


def test_materializer_publishes_prepared_to_validated_to_current_idempotently():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        _seed_full_cohort(cur, run_id)

        result = security_master.materialize(
            conn, as_of=AS_OF, source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        assert result["state"] == "current"
        assert result["securities"] == 2  # one resolved + one ambiguous
        assert result["rejected"] == 1  # placeholder

        cur.execute("SELECT lifecycle_state FROM sec_current_derived_publications WHERE product='bond_security_v1'")
        assert cur.fetchone() == ("validated",)
        cur.execute("SELECT count(*) FROM sec_current_bond_security_v1")
        assert cur.fetchone()[0] == 2
        # The placeholder-only observation produced no security identity.
        cur.execute("SELECT count(*) FROM sec_current_bond_security_v1 WHERE identity_key LIKE '%000000000%'")
        assert cur.fetchone()[0] == 0

        # Idempotent rerun: identical publication id, no new publication, pin held.
        again = security_master.materialize(
            conn, as_of=AS_OF, source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        assert again["publication_id"] == result["publication_id"]
        cur.execute("SELECT count(*) FROM sec_derived_publications WHERE product='bond_security_v1'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM bond_security_v1_builds")
        assert cur.fetchone()[0] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_same_cusip_collapses_and_alias_window_closes_on_supersession():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        # One CUSIP9, ISIN reassigned across two report dates.
        observe(cur, run_id, as_of=AS_OF, observation_date=date(2026, 3, 31),
                cusip9="037833100", isin="US0378331005")
        observe(cur, run_id, as_of=AS_OF, observation_date=date(2026, 6, 30),
                cusip9="037833100", isin="US0378331099")
        security_master.materialize(
            conn, as_of=AS_OF, source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )

        cur.execute("SELECT count(*) FROM sec_current_bond_security_v1")
        assert cur.fetchone()[0] == 1  # collapsed to one security

        cur.execute(
            "SELECT alias_kind, alias_value, valid_from, valid_to "
            "FROM sec_current_bond_security_alias_v1 ORDER BY alias_kind, valid_from"
        )
        rows = cur.fetchall()
        assert ("cusip9", "037833100", date(2026, 3, 31), None) in rows
        # The superseded ISIN window closes exactly when the successor opens.
        assert ("isin", "US0378331005", date(2026, 3, 31), date(2026, 6, 30)) in rows
        assert ("isin", "US0378331099", date(2026, 6, 30), None) in rows
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_us_isin_only_and_cusip9_collapse_to_one_published_security():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        # Same instrument: one lot carries only the US ISIN, another the CUSIP9.
        observe(cur, run_id, as_of=AS_OF, observation_date=AS_OF, isin="US0378331005")
        observe(cur, run_id, as_of=AS_OF, observation_date=AS_OF, cusip9="037833100")
        security_master.materialize(
            conn, as_of=AS_OF, source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        cur.execute("SELECT identity_key FROM sec_current_bond_security_v1")
        assert cur.fetchall() == [("cusip9:037833100",)]  # anchored -> one security
        cur.execute("SELECT DISTINCT alias_kind FROM sec_current_bond_security_alias_v1 ORDER BY alias_kind")
        assert [r[0] for r in cur.fetchall()] == ["cusip9", "isin"]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_conflicting_isin_publishes_ambiguous_with_evidence_and_no_isin_alias():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        observe(cur, run_id, as_of=AS_OF, observation_date=AS_OF, cusip9="459200101", isin="US4592001014")
        observe(cur, run_id, as_of=AS_OF, observation_date=AS_OF, cusip9="459200101", isin="US4592001099")
        security_master.materialize(
            conn, as_of=AS_OF, source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )

        cur.execute(
            "SELECT identity_state, identity_reason_code, identity_evidence "
            "FROM sec_current_bond_security_v1"
        )
        state, reason, evidence = cur.fetchone()
        assert state == "ambiguous"
        assert reason == "conflicting_isin_evidence"
        assert sorted(evidence["conflicts"]["isin"]) == ["US4592001014", "US4592001099"]
        # No arbitrary ISIN winner is published as an alias.
        cur.execute("SELECT alias_kind FROM sec_current_bond_security_alias_v1")
        assert [r[0] for r in cur.fetchall()] == ["cusip9"]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_observation_rows_are_immutable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _ = base_fixture(cur)
        oid = observe(cur, run_id, as_of=AS_OF, observation_date=AS_OF, cusip9="037833100")
        with pytest.raises(psycopg.Error, match="bond_security_observation is immutable"):
            cur.execute("UPDATE bond_security_observation SET issuer_name='x' WHERE observation_id=%s", (oid,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        with pytest.raises(psycopg.Error, match="bond_security_observation is immutable"):
            cur.execute("DELETE FROM bond_security_observation WHERE observation_id=%s", (oid,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_partial_non_validated_build_can_never_become_current():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        publication_id = uuid4()
        # A 'prepared' publication that never completed validation.
        cur.execute(
            "INSERT INTO sec_derived_publications"
            "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) "
            "VALUES(%s,'bond_security_v1',1,%s,%s,%s)",
            (publication_id, run_id, package_id, "a" * 64),
        )
        with pytest.raises(psycopg.Error, match="requires a validated publication"):
            cur.execute("SELECT sec_set_current_derived_publication('bond_security_v1',%s)", (publication_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute("SELECT count(*) FROM sec_derived_current_pointers")
        assert cur.fetchone()[0] == 0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_snapshot_rows_frozen_after_validation():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        observe(cur, run_id, as_of=AS_OF, observation_date=AS_OF, cusip9="037833100")
        security_master.materialize(
            conn, as_of=AS_OF, source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        cur.execute("SELECT publication_id, security_id FROM bond_security_v1 LIMIT 1")
        publication_id, security_id = cur.fetchone()
        # The parent publication is validated; the write guard rejects further writes.
        with pytest.raises(psycopg.Error, match="requires a prepared bond_security_v1 publication"):
            cur.execute(
                "INSERT INTO bond_security_v1"
                "(publication_id,source_run_id,security_id,identity_key,identity_state,terms,"
                " identity_evidence,measured_at,provenance) "
                "VALUES(%s,%s,%s,'cusip9:x','resolved','{}','{}',%s,'{}')",
                (publication_id, run_id, uuid4(), AS_OF),
            )
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

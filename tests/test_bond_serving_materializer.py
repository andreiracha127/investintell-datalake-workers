"""Focused tests for the public ``bond_serving_v1`` materializer.

Uses synthetic bond snapshot stand-ins (``sec_current_bond_security_v1`` (+aliases),
the ``bond_price_latest_v1`` / ``bond_price_fund_asof_v1`` price lanes, and the
N-PORT ``sec_nport_holdings_v2_current`` reverse-lookup source; see
``_bond_serving_fixtures``) so the serving projection is exercised independently of
the Task 3/4 build machinery. Proves:
  * every present surface projects into the public serving surface + atomic promote;
  * catalog/detail identity_state -> serving state mapping incl. ambiguous -> degraded
    with NEUTRAL identity evidence (never the internal contributing_observation_ids);
  * observations carry a mandatory ``lane`` (column + payload) with freshness
    (fund_asof stale >= 31d) and ambiguity (duplicate cohort) states;
  * fund_exposure reverse lookup aggregates at fund (series) grain and HARD-FAILS
    on holding->security row multiplication;
  * Wave 1: catalog serves latest_price_pct/security_ytm/security_ytw and detail
    serves current_yield/security_ytm/security_ytw/wal from the promoted current
    metric view, null-honest (absent/non-available => JSON null, never 0);
  * the coverage gate spans BOTH observation lanes (the fund_asof function too)
    AND the current metric view (all-or-nothing);
  * DATA leak absence: no raw_row_id/source_run_id/hashes/vendor/.tsv names, no
    ``cik:``/``row:`` identifiers, no internal provenance/lineage.

DSN-agnostic (Global Constraint): reads ``SEC_TEST_DATABASE_URL``.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from uuid import UUID

import pytest

from src.bonds import serving_contract as contract
from src.bonds import serving_materializer as materializer

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_serving_fixtures import (  # noqa: E402
    AS_OF,
    FORBIDDEN,
    SEC1,
    SEC2,
    SENT_OBS,
    connect,
    setup,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)


def _materialize(cur, **kw) -> dict:
    return materializer.materialize(cur.connection, as_of=AS_OF, code_revision="test", **kw)


def _dump(cur) -> str:
    return cur.execute(
        "SELECT string_agg(concat_ws('|', surface, security_id::text, lane, fund_key, "
        "fact_key, state, COALESCE(reason_code,''), COALESCE(identity_state,''), "
        "COALESCE(ambiguity_state,''), COALESCE(payload::text,'')), chr(10)) "
        "FROM bond_serving_facts"
    ).fetchone()[0]


def test_materialize_projects_every_surface_and_promotes_current() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        result = _materialize(cur)
        assert result["state"] == "current"
        assert set(result["surfaces_written"]) == set(contract.surface_names())
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers WHERE product='bond_serving_v1'"
        ).fetchone()
        assert pointer is not None
        state = cur.execute(
            "SELECT lifecycle_state FROM sec_derived_publications WHERE product='bond_serving_v1'"
        ).fetchone()[0]
        assert state == "validated"
        version = cur.execute(
            "SELECT publication_version FROM sec_derived_publications WHERE product='bond_serving_v1'"
        ).fetchone()[0]
        assert version == 1
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_catalog_and_detail_state_mapping_and_neutral_ambiguity() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        _materialize(cur)

        def one(surface: str, security_id: UUID) -> dict:
            row = cur.execute(
                "SELECT state, reason_code, identity_state, ambiguity_state, coverage_pct, "
                "payload::text FROM sec_current_bond_serving_facts "
                "WHERE surface=%s AND security_id=%s",
                (surface, security_id),
            ).fetchone()
            return {"state": row[0], "reason": row[1], "identity_state": row[2],
                    "ambiguity_state": row[3], "coverage": row[4], "payload": row[5]}

        cat1 = one("catalog", SEC1)
        assert cat1["state"] == "available" and cat1["reason"] is None
        assert cat1["identity_state"] == "resolved" and cat1["coverage"] == 100
        assert '"display": "Acme Corp 5.25% 2030-06-30"' in cat1["payload"]
        # searchable public alias arrays (only VALID aliases; ISIN absent -> []).
        assert '"aliases_cusip9": ["037833100"]' in cat1["payload"]
        assert '"aliases_isin": []' in cat1["payload"]

        cat2 = one("catalog", SEC2)
        assert cat2["state"] == "degraded" and cat2["reason"] == "identity_ambiguous"
        assert cat2["ambiguity_state"] == "ambiguous" and cat2["coverage"] == 50
        assert '"aliases_cusip9": ["459200101"]' in cat2["payload"]

        det1 = one("detail", SEC1)
        assert det1["state"] == "available"
        assert '"call_price": 101.0' in det1["payload"]  # call schedule surfaced
        assert '"aliases"' in det1["payload"] and "037833100" in det1["payload"]

        det2 = one("detail", SEC2)
        # neutral evidence: the conflicting VALUES surface, the internal
        # contributing_observation_ids never do.
        assert '"conflicts"' in det2["payload"]
        assert "US4592001014" in det2["payload"]
        assert "contributing_observation_ids" not in det2["payload"]
        assert SENT_OBS not in det2["payload"]
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_catalog_serves_computed_metrics_and_latest_price_null_honest() -> None:
    """Wave 1 catalog extension: latest_price_pct + security_ytm + security_ytw.

    SEC1 has one ELIGIBLE latest observation (unique cohort, trade/evaluated,
    clean/dirty, price present) and 'available' metric rows -> values served.
    SEC2's latest cohort is a DUPLICATE (no unambiguous latest price) and its
    metric rows are non-available statuses -> every new key is present with an
    honest JSON null, never a synthetic 0.

    MUTATION LOCK (review IMP-1): SEC2's security_ytm row is POISONED (status
    'no_eligible_price' with value 0.9999 -- a state the real bond_metric_v1
    CHECK forbids, deliberately permitted by the stand-in). The null assertion
    below therefore FAILS if the serving-side ``status = 'available'`` filter is
    removed from ``_metric_value_sql`` -- the guard has coverage of its own.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        _materialize(cur)

        def payload(security_id: UUID) -> dict:
            return json.loads(cur.execute(
                "SELECT payload::text FROM sec_current_bond_serving_facts "
                "WHERE surface='catalog' AND security_id=%s", (security_id,)
            ).fetchone()[0])

        p1 = payload(SEC1)
        assert p1["latest_price_pct"] == 99.5   # % of par, from the latest lane
        assert p1["security_ytm"] == 0.0525     # decimal fraction from metric view
        assert p1["security_ytw"] == 0.0518

        p2 = payload(SEC2)
        for key in ("latest_price_pct", "security_ytm", "security_ytw"):
            assert key in p2, f"catalog key {key!r} must be present even when absent"
            assert p2[key] is None, f"catalog key {key!r} must be NULL, never fabricated"
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_detail_serves_computed_metrics_null_honest() -> None:
    """Wave 1 detail extension: current_yield + security_ytm + security_ytw + wal.

    SEC1 serves all four 'available' values (yields as fractions, wal in years).
    SEC2 exercises every null-honest arm: no_eligible_price / gate_not_passed /
    engine_typed_error rows AND a completely ABSENT current_yield row all project
    the key as JSON null (present, never 0).

    MUTATION LOCK (review IMP-1): SEC2's security_ytm (0.9999) and wal (99.9)
    rows are POISONED -- non-available status with a NON-NULL value, which the
    real bond_metric_v1 CHECK forbids but the stand-in deliberately permits.
    Removing ``status = 'available'`` from ``_metric_value_sql`` serves those
    poisoned values and FAILS the null assertions below.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        _materialize(cur)

        def payload(security_id: UUID) -> dict:
            return json.loads(cur.execute(
                "SELECT payload::text FROM sec_current_bond_serving_facts "
                "WHERE surface='detail' AND security_id=%s", (security_id,)
            ).fetchone()[0])

        d1 = payload(SEC1)
        assert d1["current_yield"] == 0.0531
        assert d1["security_ytm"] == 0.0525
        assert d1["security_ytw"] == 0.0518
        assert d1["wal"] == 4.37
        # coupon stays a reported TERM, independent of the computed yields.
        assert d1["coupon_rate"] == 5.25

        d2 = payload(SEC2)
        for key in ("current_yield", "security_ytm", "security_ytw", "wal"):
            assert key in d2, f"detail key {key!r} must be present even when absent"
            assert d2[key] is None, f"detail key {key!r} must be NULL, never fabricated"
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_missing_metric_view_fails_the_coverage_gate() -> None:
    """catalog/detail now read the promoted current metric view; a build without it
    would silently serve a payload MISSING the contract keys -- all-or-nothing."""
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute("DROP TABLE sec_current_bond_metric_v1")
        with pytest.raises(materializer.BondServingSurfaceCoverageError):
            _materialize(cur)
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers WHERE product='bond_serving_v1'"
        ).fetchone()
        assert pointer is None
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_observations_carry_lane_freshness_and_ambiguity() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        _materialize(cur)
        rows = cur.execute(
            "SELECT security_id, lane, state, reason_code, ambiguity_state, payload::text "
            "FROM sec_current_bond_serving_facts WHERE surface='observations' "
            "ORDER BY security_id, lane, fact_key"
        ).fetchall()
        # every observation row carries a non-empty lane, in the column AND payload.
        assert rows
        for r in rows:
            assert r[1] in ("latest", "fund_asof")
            assert f'"lane": "{r[1]}"' in r[5]
        # SEC2 latest is a duplicate cohort -> both rows retained + observation_ambiguous.
        sec2_latest = [r for r in rows if r[0] == SEC2 and r[1] == "latest"]
        assert len(sec2_latest) == 2
        assert all(r[2] == "degraded" and r[3] == "observation_ambiguous" for r in sec2_latest)
        assert all(r[4] == "ambiguous" for r in sec2_latest)
        # SEC1 fund_asof is stale (age 40d >= 31) -> degraded + observation_stale.
        sec1_asof = [r for r in rows if r[0] == SEC1 and r[1] == "fund_asof"]
        assert len(sec1_asof) == 1
        assert sec1_asof[0][2] == "degraded" and sec1_asof[0][3] == "observation_stale"
        assert '"is_stale": true' in sec1_asof[0][5]
        # SEC1 latest is unique and fresh -> available. Staleness is a fund_asof
        # concept, so the latest lane carries an HONEST NULL is_stale (never false).
        sec1_latest = [r for r in rows if r[0] == SEC1 and r[1] == "latest"]
        assert len(sec1_latest) == 1 and sec1_latest[0][2] == "available"
        assert '"is_stale": null' in sec1_latest[0][5]
        assert '"is_stale": false' not in sec1_latest[0][5]
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_fund_exposure_reverse_lookup_aggregates_at_series_grain() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        _materialize(cur)
        rows = cur.execute(
            "SELECT security_id, fund_key, payload::text FROM sec_current_bond_serving_facts "
            "WHERE surface='fund_exposure' ORDER BY security_id, fund_key"
        ).fetchall()
        # SEC1 held by series S1; the bridge class fan-out was collapsed, two lots
        # (H1=100, H2=50) aggregate to 150 with position_lot_count=2 (no double count).
        assert len(rows) == 1
        assert rows[0][0] == SEC1 and rows[0][1] == "S1"
        payload = json.loads(rows[0][2])
        assert payload["holding_market_value"] == 150.0
        assert payload["position_lot_count"] == 2
        assert payload["series_id"] == "S1"
        assert "as_of" in payload and "report_date" in payload
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_fund_exposure_multiplication_hard_fails_and_promotes_nothing() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        # SEC2 now shares SEC1's CUSIP -> holding H1 maps to two securities.
        cur.execute(
            "UPDATE sec_current_bond_security_alias_v1 SET alias_value='037833100' "
            "WHERE security_id=%s",
            (SEC2,),
        )
        with pytest.raises(materializer.BondFundExposureMultiplicationError):
            _materialize(cur)
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers WHERE product='bond_serving_v1'"
        ).fetchone()
        assert pointer is None
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_serving_data_contains_no_internal_identifiers() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        _materialize(cur)
        dump = _dump(cur)
        assert dump  # non-empty
        for token in FORBIDDEN:
            assert token not in dump, f"leaked internal token {token!r}"
        # a cik: value under a PUBLIC key (distinct_issuer_name) is neutralised by
        # the scrub value rule; no raw cik:/row: identifier survives anywhere.
        assert "cik:" not in dump and "row:" not in dump
        assert "unavailable" in dump  # the neutralised fallback surfaces the sentinel
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_partial_surface_set_fails_closed() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute("DROP TABLE bond_price_latest_v1")  # observations surface source gone
        with pytest.raises(materializer.BondServingSurfaceCoverageError):
            _materialize(cur)
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers WHERE product='bond_serving_v1'"
        ).fetchone()
        assert pointer is None
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_missing_fund_asof_function_fails_the_coverage_gate() -> None:
    """The observations surface reads BOTH lanes; the point-in-time function is part
    of the coverage gate (to_regclass never resolves it) -- dropping it fails closed."""
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute("DROP FUNCTION bond_price_fund_asof_v1(date)")
        with pytest.raises(materializer.BondServingSurfaceCoverageError):
            _materialize(cur)
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers WHERE product='bond_serving_v1'"
        ).fetchone()
        assert pointer is None
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_allow_missing_surfaces_opts_out_of_the_coverage_gate() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute("DROP TABLE sec_nport_holdings_v2_current")  # fund_exposure gone
        result = _materialize(cur, allow_missing_surfaces=True)
        assert result["state"] == "current"
        assert "fund_exposure" not in result["surfaces_written"]
        assert {"catalog", "detail", "observations"} <= set(result["surfaces_written"])
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_materialize_is_idempotent() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        first = _materialize(cur)
        second = _materialize(cur)
        assert first["publication_id"] == second["publication_id"]
        count = cur.execute(
            "SELECT count(*) FROM sec_derived_publications WHERE product='bond_serving_v1'"
        ).fetchone()[0]
        assert count == 1
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()

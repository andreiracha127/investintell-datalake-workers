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
from datetime import timedelta
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


def test_catalog_resolves_issuer_classification_by_reported_consensus() -> None:
    """Wave 1b catalog extension: issuer_country + issuer_sector.

    SEC1 is held by seven lots: four report CORP/US and three dissent with
    MUN/KY, so the projection must serve the reported CONSENSUS -- the value the
    most lots report -- not whichever row the planner happened to reach first.
    SEC2's alias is held by no lot, so both keys are present with a JSON null.

    The CORP/US majority is reported AFTER the publication's as_of, so this also
    pins that the classification is NOT point-in-time filtered: it describes the
    security, not a dated exposure. Reintroducing fund exposure's
    ``report_date <= as_of`` predicate leaves only the MUN/KY lots and this test
    reads MUN/KY -- the failure that shipped in the first build, where the filter
    admitted 491 of 4,507,500 live holdings and every security served NULL.
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
        assert p1["issuer_sector"] == "CORP"   # 4 lots beat the 3 dissenting MUN
        assert p1["issuer_country"] == "US"    # 4 lots beat the 3 dissenting KY

        p2 = payload(SEC2)
        for key in ("issuer_country", "issuer_sector"):
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
    """The opt-out promotes whatever surfaces the present relations can build.

    Since Wave 1b the N-PORT relation is required by the CATALOG too (it is where
    issuer_country / issuer_sector come from), so dropping it takes catalog and
    fund_exposure together: a catalog built without it would serve payloads
    missing two contract keys, which is exactly what the gate exists to stop.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute("DROP TABLE sec_nport_holdings_v2_current")
        result = _materialize(cur, allow_missing_surfaces=True)
        assert result["state"] == "current"
        assert "fund_exposure" not in result["surfaces_written"]
        assert "catalog" not in result["surfaces_written"]
        assert {"detail", "observations"} <= set(result["surfaces_written"])
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


# --------------------------------------------------------------------------- #
# Freshness: the dense daily series advances the latest lane (2026-08-07)
# --------------------------------------------------------------------------- #
def _seed_dense_series(cur, rows) -> None:
    """A plain-table stand-in for the bond_observation_daily hypertable.

    The real relation is a TimescaleDB hypertable owned by the serving
    repository; the columns and the (cusip9, day) key are what this build reads,
    and neither needs time partitioning to be exercised.
    """
    cur.execute("""
        CREATE TABLE bond_observation_daily(
            cusip9 text NOT NULL, day date NOT NULL, price numeric, ytm numeric,
            volume numeric, price_type text, accrued text, source text NOT NULL,
            source_rank smallint NOT NULL, ytm_basis text,
            PRIMARY KEY (cusip9, day))
    """)
    cur.executemany(
        "INSERT INTO bond_observation_daily VALUES(%s,%s,%s,%s,NULL,%s,%s,%s,%s,'reported')",
        rows,
    )


def _observation(cur, security_id, lane):
    return cur.execute(
        "SELECT observation_date, payload FROM bond_serving_facts "
        "WHERE surface='observations' AND security_id=%s AND lane=%s",
        (security_id, lane),
    ).fetchall()


def test_a_newer_dense_observation_advances_the_latest_lane_and_the_price() -> None:
    """The 2e mechanism: header date and catalog price follow the dense series.

    SEC1's governed latest sits on AS_OF; the dense series carries a strictly
    newer day for its CUSIP9. Every surface that speaks about "latest" must move
    to that day -- carrying the REAL observation date, never a re-stamped one.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        fresh_day = AS_OF + timedelta(days=5)
        _seed_dense_series(cur, [
            ("037833100", fresh_day, 101.75, 0.0475, "trade", "clean", "finnhub", 1),
        ])
        result = materializer.materialize(
            cur.connection, as_of=fresh_day, code_revision="test"
        )
        assert result["latest_observation"]["advanced_securities"] == 1

        rows = _observation(cur, SEC1, "latest")
        assert len(rows) == 1
        observation_date, payload = rows[0]
        assert observation_date == fresh_day, "the header must carry the REAL day"
        assert float(payload["price"]) == 101.75
        assert float(payload["ytm"]) == 0.0475

        for surface in ("catalog", "detail"):
            payload = cur.execute(
                "SELECT payload FROM bond_serving_facts "
                "WHERE surface=%s AND security_id=%s", (surface, SEC1),
            ).fetchone()[0]
            assert float(payload["latest_price_pct"]) == 101.75, surface
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_an_older_dense_observation_never_rolls_the_latest_lane_backwards() -> None:
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        _seed_dense_series(cur, [
            ("037833100", AS_OF - timedelta(days=10), 90.0, 0.09,
             "trade", "clean", "finnhub", 1),
        ])
        result = _materialize(cur)
        assert result["latest_observation"]["advanced_securities"] == 0
        observation_date, payload = _observation(cur, SEC1, "latest")[0]
        assert observation_date == AS_OF
        assert float(payload["price"]) == 99.5
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_securities_the_dense_series_does_not_reach_keep_their_governed_lane() -> None:
    """SEC2 has no dense row: its (ambiguous, duplicate) governed rows survive."""
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        fresh_day = AS_OF + timedelta(days=5)
        _seed_dense_series(cur, [
            ("037833100", fresh_day, 101.75, 0.0475, "trade", "clean", "finnhub", 1),
        ])
        materializer.materialize(cur.connection, as_of=fresh_day, code_revision="test")
        rows = _observation(cur, SEC2, "latest")
        assert len(rows) == 2, "both duplicate-cohort rows are retained"
        assert {row[0] for row in rows} == {AS_OF}
        # An ambiguous cohort still has NO unambiguous price to serve.
        payload = cur.execute(
            "SELECT payload FROM bond_serving_facts "
            "WHERE surface='catalog' AND security_id=%s", (SEC2,),
        ).fetchone()[0]
        assert payload["latest_price_pct"] is None
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_aliases_disagreeing_on_the_latest_day_are_marked_ambiguous() -> None:
    """Two CUSIP9s of one security, same day, different prices -> no served price.

    An arbitrary winner here would be a silently fabricated price; the build
    reuses the existing duplicate-cohort state instead, which the surfaces
    already degrade on.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute(
            "INSERT INTO sec_current_bond_security_alias_v1 VALUES "
            "(%s,'cusip9','037833999','2020-01-01',NULL)", (SEC1,),
        )
        fresh_day = AS_OF + timedelta(days=5)
        _seed_dense_series(cur, [
            ("037833100", fresh_day, 101.75, 0.0475, "trade", "clean", "finnhub", 1),
            ("037833999", fresh_day, 95.10, 0.0620, "trade", "clean", "finnhub", 1),
        ])
        materializer.materialize(cur.connection, as_of=fresh_day, code_revision="test")
        state, reason = cur.execute(
            "SELECT state, reason_code FROM bond_serving_facts "
            "WHERE surface='observations' AND security_id=%s AND lane='latest'", (SEC1,),
        ).fetchone()
        assert (state, reason) == ("degraded", "observation_ambiguous")
        payload = cur.execute(
            "SELECT payload FROM bond_serving_facts "
            "WHERE surface='catalog' AND security_id=%s", (SEC1,),
        ).fetchone()[0]
        assert payload["latest_price_pct"] is None
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_aliases_agreeing_on_price_but_not_on_ytm_lose_only_the_ytm() -> None:
    """Per-field refusal: the price is a fact, the yield is a conflict.

    Two CUSIP9s of one security report the SAME price on the same day and two
    DIFFERENT yields. Judging the cohort on price alone marks it unique, and the
    ``DISTINCT ON`` tie-break then publishes whichever alias wins source rank --
    serving one of two conflicting yields as if it were the security's. The
    disagreement is per FIELD, exactly as the metric worker's dense lane already
    treats it: the yield is suppressed, the agreed price is still served, and the
    cohort stays unique so the price-eligibility predicate keeps it.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute(
            "INSERT INTO sec_current_bond_security_alias_v1 VALUES "
            "(%s,'cusip9','037833999','2020-01-01',NULL)", (SEC1,),
        )
        fresh_day = AS_OF + timedelta(days=5)
        _seed_dense_series(cur, [
            ("037833100", fresh_day, 101.75, 0.0475, "trade", "clean", "finnhub", 1),
            ("037833999", fresh_day, 101.75, 0.0620, "trade", "clean", "finnhub", 1),
        ])
        materializer.materialize(cur.connection, as_of=fresh_day, code_revision="test")

        rows = _observation(cur, SEC1, "latest")
        assert len(rows) == 1
        observation_date, payload = rows[0]
        assert observation_date == fresh_day
        # The price all aliases agree on survives, ...
        assert float(payload["price"]) == 101.75
        # ... the conflicting yield does NOT: absent, never an arbitrary winner.
        assert payload["ytm"] is None
        # The cohort is NOT globally ambiguous: only the yield disagreed.
        assert payload["daily_key_state"] == "unique_in_matching_cohort"
        state, reason = cur.execute(
            "SELECT state, reason_code FROM bond_serving_facts "
            "WHERE surface='observations' AND security_id=%s AND lane='latest'", (SEC1,),
        ).fetchone()
        assert (state, reason) == ("available", None)
        # ...so catalog and detail still serve the agreed price.
        for surface in ("catalog", "detail"):
            served = cur.execute(
                "SELECT payload FROM bond_serving_facts "
                "WHERE surface=%s AND security_id=%s", (surface, SEC1),
            ).fetchone()[0]
            assert float(served["latest_price_pct"]) == 101.75, surface
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_aliases_agreeing_on_both_fields_serve_both() -> None:
    """The suppression is a REFUSAL, not a blanket drop of multi-alias yields."""
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute(
            "INSERT INTO sec_current_bond_security_alias_v1 VALUES "
            "(%s,'cusip9','037833999','2020-01-01',NULL)", (SEC1,),
        )
        fresh_day = AS_OF + timedelta(days=5)
        _seed_dense_series(cur, [
            ("037833100", fresh_day, 101.75, 0.0475, "trade", "clean", "finnhub", 1),
            ("037833999", fresh_day, 101.75, 0.0475, "trade", "clean", "finnhub", 1),
        ])
        materializer.materialize(cur.connection, as_of=fresh_day, code_revision="test")

        observation_date, payload = _observation(cur, SEC1, "latest")[0]
        assert observation_date == fresh_day
        assert float(payload["price"]) == 101.75
        assert float(payload["ytm"]) == 0.0475
        assert payload["daily_key_state"] == "unique_in_matching_cohort"
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_a_price_disagreement_still_carries_a_yield_the_aliases_agree_on() -> None:
    """The mirror case, and the reason the refusal is per field on BOTH sides.

    The price cohort is ambiguous (so no price is served) while every alias
    reports the same yield -- which stays. The metric worker refuses exactly the
    price here and publishes the yield; a cohort-wide refusal on either surface
    would make one of them serve a number the other denies.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        cur.execute(
            "INSERT INTO sec_current_bond_security_alias_v1 VALUES "
            "(%s,'cusip9','037833999','2020-01-01',NULL)", (SEC1,),
        )
        fresh_day = AS_OF + timedelta(days=5)
        _seed_dense_series(cur, [
            ("037833100", fresh_day, 101.75, 0.0475, "trade", "clean", "finnhub", 1),
            ("037833999", fresh_day, 95.10, 0.0475, "trade", "clean", "finnhub", 1),
        ])
        materializer.materialize(cur.connection, as_of=fresh_day, code_revision="test")

        payload = _observation(cur, SEC1, "latest")[0][1]
        assert payload["daily_key_state"] == "duplicate_in_matching_cohort"
        assert float(payload["ytm"]) == 0.0475, "the agreed yield is not collateral damage"
        served = cur.execute(
            "SELECT payload FROM bond_serving_facts "
            "WHERE surface='catalog' AND security_id=%s", (SEC1,),
        ).fetchone()[0]
        assert served["latest_price_pct"] is None
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


# --------------------------------------------------------------------------- #
# Retired aliases: the latest lane reads the identity valid AT as_of
# --------------------------------------------------------------------------- #
# The alias snapshot is temporal — a superseded CUSIP9 keeps its row with a
# CLOSED window — while the dense series is keyed by CUSIP9 alone. Joining every
# historical alias lets a row filed against a retired identifier win the
# latest-day ordering and be SERVED as this security's latest price, or mark the
# cohort ambiguous against the identity that actually holds it. The filter is the
# PIT fund-exposure join's: half-open [valid_from, valid_to), valid_from
# inclusive, valid_to exclusive and open when NULL. The metric worker's dense
# lane applies the identical predicate; the two surfaces must agree about which
# numbers exist.
CUSIP_RETIRED = "037833777"


@pytest.mark.parametrize(
    ("valid_to_offset", "retired_alias_serves_the_price"),
    [
        (None, True),  # open window: the alias is current
        (1, True),     # closes the day AFTER as_of: still held on as_of
        (0, False),    # closes ON as_of: valid_to is EXCLUSIVE
        (-3, False),   # closed before as_of
    ],
)
def test_the_latest_lane_reads_only_the_aliases_held_at_as_of(
    valid_to_offset, retired_alias_serves_the_price
) -> None:
    """The freshest dense row must not be served when its CUSIP9 is retired.

    The retired identifier carries the NEWER row, so the alias window is the only
    thing that can decide the outcome: unfiltered, it always wins and the header
    renders a day and a price belonging to an identifier the security no longer
    holds.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        as_of = AS_OF + timedelta(days=5)
        cur.execute(
            "INSERT INTO sec_current_bond_security_alias_v1 VALUES "
            "(%s,'cusip9',%s,'2020-01-01',%s)",
            (SEC1, CUSIP_RETIRED,
             None if valid_to_offset is None else as_of + timedelta(days=valid_to_offset)),
        )
        _seed_dense_series(cur, [
            ("037833100", AS_OF + timedelta(days=2), 101.75, 0.0475,
             "trade", "clean", "finnhub", 1),
            (CUSIP_RETIRED, as_of, 95.10, 0.0620, "trade", "clean", "finnhub", 1),
        ])
        materializer.materialize(cur.connection, as_of=as_of, code_revision="test")

        rows = _observation(cur, SEC1, "latest")
        assert len(rows) == 1
        observation_date, payload = rows[0]
        if retired_alias_serves_the_price:
            assert observation_date == as_of
            assert float(payload["price"]) == 95.10
        else:
            # The HELD identity's own latest day, never the retired alias's.
            assert observation_date == AS_OF + timedelta(days=2)
            assert float(payload["price"]) == 101.75
        assert payload["daily_key_state"] == "unique_in_matching_cohort"
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_a_retired_alias_cannot_manufacture_an_ambiguous_cohort() -> None:
    """A closed window must not cost the security a price it unambiguously has.

    Same day, two values, and the ambiguity rule is blunt by design: unfiltered,
    the surface degrades and serves NO price at all while the held identity
    reports a perfectly good one.
    """
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        as_of = AS_OF + timedelta(days=5)
        cur.execute(
            "INSERT INTO sec_current_bond_security_alias_v1 VALUES "
            "(%s,'cusip9',%s,'2020-01-01',%s)", (SEC1, CUSIP_RETIRED, AS_OF),
        )
        _seed_dense_series(cur, [
            ("037833100", as_of, 101.75, 0.0475, "trade", "clean", "finnhub", 1),
            (CUSIP_RETIRED, as_of, 95.10, 0.0620, "trade", "clean", "finnhub", 1),
        ])
        materializer.materialize(cur.connection, as_of=as_of, code_revision="test")

        state, reason = cur.execute(
            "SELECT state, reason_code FROM bond_serving_facts "
            "WHERE surface='observations' AND security_id=%s AND lane='latest'", (SEC1,),
        ).fetchone()
        assert (state, reason) == ("available", None)
        payload = cur.execute(
            "SELECT payload FROM bond_serving_facts "
            "WHERE surface='catalog' AND security_id=%s", (SEC1,),
        ).fetchone()[0]
        assert float(payload["latest_price_pct"]) == 101.75
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_an_alias_not_yet_in_effect_never_serves_the_latest_price() -> None:
    """valid_from is INCLUSIVE, so a window opening AFTER as_of is out."""
    conn = connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = setup(cur)
        as_of = AS_OF + timedelta(days=5)
        cur.execute(
            "INSERT INTO sec_current_bond_security_alias_v1 VALUES "
            "(%s,'cusip9',%s,%s,NULL)", (SEC1, CUSIP_RETIRED, as_of + timedelta(days=1)),
        )
        _seed_dense_series(cur, [
            ("037833100", AS_OF + timedelta(days=2), 101.75, 0.0475,
             "trade", "clean", "finnhub", 1),
            (CUSIP_RETIRED, as_of, 95.10, 0.0620, "trade", "clean", "finnhub", 1),
        ])
        materializer.materialize(cur.connection, as_of=as_of, code_revision="test")

        observation_date, payload = _observation(cur, SEC1, "latest")[0]
        assert observation_date == AS_OF + timedelta(days=2)
        assert float(payload["price"]) == 101.75
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()

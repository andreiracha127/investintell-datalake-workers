"""Focused tests for the public ``bond_serving_v1`` materializer.

Uses synthetic bond snapshot stand-ins (``sec_current_bond_security_v1`` (+aliases),
the ``bond_price_latest_v1`` / ``bond_price_fund_asof_v1`` price lanes, and the
N-PORT ``sec_nport_holdings_v2_current`` reverse-lookup source) so the serving
projection is exercised independently of the Task 3/4 build machinery. Proves:
  * every present surface projects into the public serving surface + atomic promote;
  * catalog/detail identity_state -> serving state mapping incl. ambiguous -> degraded
    with NEUTRAL identity evidence (never the internal contributing_observation_ids);
  * observations carry a mandatory ``lane`` (column + payload) with freshness
    (fund_asof stale >= 31d) and ambiguity (duplicate cohort) states;
  * fund_exposure reverse lookup aggregates at fund (series) grain and HARD-FAILS
    on holding->security row multiplication;
  * DATA leak absence: no raw_row_id/source_run_id/hashes/vendor/.tsv names, no
    ``cik:``/``row:`` identifiers, no internal provenance/lineage.

DSN-agnostic (Global Constraint): reads ``SEC_TEST_DATABASE_URL``.
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from uuid import UUID, uuid4

import psycopg
import pytest

from src.bonds import serving_contract as contract
from src.bonds import serving_materializer as materializer

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

AS_OF = date(2025, 12, 31)

SEC1 = UUID("11111111-1111-5111-8111-111111111111")
SEC2 = UUID("22222222-2222-5222-8222-222222222222")

# Internal-identifier sentinels embedded in every synthetic snapshot payload/provenance.
SENT_RAW = "RAWROWLEAK123"
SENT_SRC = "SRCRUNLEAK456"
SENT_MD5 = "d41d8cd98f00b204e9800998ecf8427e"
SENT_CIK = "cik:LEAKCIK789"
SENT_ROW = "row:ROWKEYLEAK000"
SENT_FILE = "HOLDINGS_INTERNAL.tsv"
SENT_VENDOR = "InternalVendorNameX"
SENT_OBS = "OBSIDLEAK555"
FORBIDDEN = [
    SENT_RAW, SENT_SRC, SENT_MD5, "LEAKCIK789", "ROWKEYLEAK000", SENT_FILE, SENT_VENDOR,
    SENT_OBS, "raw_row_id", "source_run_id", "source_lineage", "provenance",
    "contributing_observation_ids", "source_typed_projection", "holding_id",
    "accession_number", "identity_key",
]

# Internal provenance blob (must never be projected at all).
PROV = json.dumps({
    "source_run_id": SENT_SRC, "raw_row_id": SENT_RAW, "text_block_md5": SENT_MD5,
    "source_table": SENT_FILE, "vendor": SENT_VENDOR, "registrant_cik": SENT_CIK,
})


def _connect() -> psycopg.Connection:
    return psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"])


def _terms(**extra: object) -> str:
    # coupon/call/put schedules ride here; a nested blocklisted key + a row:/cik:
    # VALUE under a public key must be stripped/neutralised by the scrub.
    base = {
        "coupon_schedule": [{"date": "2026-06-30", "rate": 5.25}],
        "call_schedule": [{"call_date": "2028-01-01", "call_price": 101.0,
                           "raw_row_id": SENT_RAW, "note": SENT_ROW}],
        "put_schedule": None,
    }
    base.update(extra)
    return json.dumps(base)


def _setup(cur) -> str:
    schema = f"bond_serving_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    materializer.install_schema(cur.connection)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    _synthetic_snapshots(cur)
    return schema


def _synthetic_snapshots(cur) -> None:
    # --- securities (catalog/detail source) --------------------------------
    cur.execute("""
        CREATE TABLE sec_current_bond_security_v1(
            security_id uuid, identity_state text, identity_reason_code text,
            issuer_name text, currency text, coupon_type text, coupon_rate numeric,
            maturity_date date, seniority text, secured text, is_144a text,
            day_count text, settlement_convention text, terms jsonb,
            identity_evidence jsonb, measured_at date, provenance jsonb)
    """)
    # resolved security
    cur.execute(
        "INSERT INTO sec_current_bond_security_v1 VALUES "
        "(%s,'resolved',NULL,'Acme Corp','USD','fixed',5.25,'2030-06-30','senior',"
        "'unsecured','true','30/360','T+2',%s,%s,%s,%s)",
        (SEC1, _terms(),
         json.dumps({"distinct_cusip9": ["037833100"], "distinct_isin": [],
                     "distinct_issuer_name": ["Acme Corp"], "conflicts": {},
                     "contributing_observation_ids": [SENT_OBS]}),
         AS_OF, PROV),
    )
    # ambiguous security: conflicting ISIN. Neutral evidence must surface the
    # conflicting VALUES only; a cik: value hides under distinct_issuer_name.
    cur.execute(
        "INSERT INTO sec_current_bond_security_v1 VALUES "
        "(%s,'ambiguous','conflicting_isin_evidence',NULL,'USD','fixed',3.10,"
        "'2029-03-15','senior','secured','false','30/360','T+2',%s,%s,%s,%s)",
        (SEC2, _terms(),
         json.dumps({"distinct_cusip9": ["459200101"],
                     "distinct_isin": ["US4592001014", "US4592001099"],
                     "distinct_issuer_name": ["Real Issuer Inc", SENT_CIK],
                     "conflicts": {"isin": ["US4592001014", "US4592001099"]},
                     "contributing_observation_ids": [SENT_OBS]}),
         AS_OF, PROV),
    )
    # --- aliases (detail + fund_exposure) ----------------------------------
    cur.execute("""
        CREATE TABLE sec_current_bond_security_alias_v1(
            security_id uuid, alias_kind text, alias_value text,
            valid_from date, valid_to date)
    """)
    cur.execute(
        "INSERT INTO sec_current_bond_security_alias_v1 VALUES "
        "(%s,'cusip9','037833100','2020-01-01',NULL),"
        "(%s,'cusip9','459200101','2020-01-01',NULL)",
        (SEC1, SEC2),
    )
    # --- price lanes (observations) ----------------------------------------
    # latest lane: SEC1 unique; SEC2 duplicate cohort (both rows retained).
    cur.execute("""
        CREATE TABLE bond_price_latest_v1(
            lane text, security_id uuid, observation_date date, source_row_number integer,
            price numeric, price_type text, accrued_treatment text, price_state text,
            ytm numeric, is_144a boolean, daily_key_state text)
    """)
    cur.execute(
        "INSERT INTO bond_price_latest_v1 VALUES "
        "('latest',%s,%s,0,99.5,'evaluated','clean','present',0.053,true,'unique_in_matching_cohort'),"
        "('latest',%s,%s,0,100.2,'trade','dirty','present',0.031,false,'duplicate_in_matching_cohort'),"
        "('latest',%s,%s,1,100.4,'trade','dirty','present',0.032,false,'duplicate_in_matching_cohort')",
        (SEC1, AS_OF, SEC2, AS_OF, SEC2, AS_OF),
    )
    # fund_asof lane function stand-in over a backing table: SEC1 stale (>=31d),
    # SEC2 fresh. It filters observation_date <= fund_as_of (no look-ahead).
    cur.execute("""
        CREATE TABLE _fund_asof_backing(
            security_id uuid, observation_date date, source_row_number integer,
            price numeric, price_type text, accrued_treatment text, price_state text,
            ytm numeric, is_144a boolean, daily_key_state text)
    """)
    cur.execute(
        "INSERT INTO _fund_asof_backing VALUES "
        "(%s,%s,0,98.0,'evaluated','clean','present',0.055,true,'unique_in_matching_cohort'),"
        "(%s,%s,0,100.1,'trade','dirty','present',0.031,false,'unique_in_matching_cohort')",
        (SEC1, AS_OF - timedelta(days=40), SEC2, AS_OF - timedelta(days=3)),
    )
    cur.execute("""
        CREATE FUNCTION bond_price_fund_asof_v1(fund_as_of date)
        RETURNS TABLE(lane text, security_id uuid, observation_date date,
            source_row_number integer, price numeric, price_type text,
            accrued_treatment text, price_state text, ytm numeric, is_144a boolean,
            daily_key_state text, observation_age_days integer, is_stale boolean)
        LANGUAGE sql STABLE AS $$
            SELECT 'fund_asof', b.security_id, b.observation_date, b.source_row_number,
                   b.price, b.price_type, b.accrued_treatment, b.price_state, b.ytm,
                   b.is_144a, b.daily_key_state,
                   (fund_as_of - b.observation_date)::integer,
                   ((fund_as_of - b.observation_date) >= 31)
            FROM _fund_asof_backing b WHERE b.observation_date <= fund_as_of
        $$
    """)
    # --- N-PORT reverse-lookup source (fund_exposure) ----------------------
    # SEC1 (cusip 037833100) held by series S1 in two lots; a bridge class fan-out
    # duplicates lot H1 across two classes (same value) -> DISTINCT collapses it.
    cur.execute("""
        CREATE TABLE sec_nport_holdings_v2_current(
            series_id text, class_id text, cusip text, isin text,
            signed_market_value numeric, signed_pct_of_nav numeric,
            report_date date, accession_number text, holding_id text,
            source_typed_projection jsonb)
    """)
    rpt = AS_OF - timedelta(days=5)
    proj = json.dumps({"raw_row_id": SENT_RAW, "vendor": SENT_VENDOR})
    cur.execute(
        "INSERT INTO sec_nport_holdings_v2_current VALUES "
        "('S1','C1','037833100',NULL,100.0,0.10,%s,'A1','H1',%s),"
        "('S1','C2','037833100',NULL,100.0,0.10,%s,'A1','H1',%s),"
        "('S1','C1','037833100',NULL,50.0,0.05,%s,'A1','H2',%s)",
        (rpt, proj, rpt, proj, rpt, proj),
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
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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

        cat2 = one("catalog", SEC2)
        assert cat2["state"] == "degraded" and cat2["reason"] == "identity_ambiguous"
        assert cat2["ambiguity_state"] == "ambiguous" and cat2["coverage"] == 50

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


def test_observations_carry_lane_freshness_and_ambiguity() -> None:
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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
        # SEC1 latest is unique and fresh -> available.
        sec1_latest = [r for r in rows if r[0] == SEC1 and r[1] == "latest"]
        assert len(sec1_latest) == 1 and sec1_latest[0][2] == "available"
    finally:
        if schema:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_fund_exposure_reverse_lookup_aggregates_at_series_grain() -> None:
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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


def test_allow_missing_surfaces_opts_out_of_the_coverage_gate() -> None:
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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
    conn = _connect()
    schema = None
    try:
        cur = conn.cursor()
        schema = _setup(cur)
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

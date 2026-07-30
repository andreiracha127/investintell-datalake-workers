"""Shared synthetic-snapshot helpers for the bond_serving_v1 serving tests.

DSN-agnostic by design (Global Constraint): every caller reads the disposable
Postgres endpoint from ``SEC_TEST_DATABASE_URL`` so the suite runs identically
under the keyword and URL DSN conventions. The leading underscore keeps pytest
from collecting this module as a test file. The snapshots are synthetic
bond-shaped stand-ins (``sec_current_bond_security_v1`` (+aliases), the price
lanes, and the N-PORT reverse-lookup source) carrying embedded leak sentinels --
no production data is read.
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from uuid import UUID, uuid4

import psycopg

from src.bonds import serving_materializer as materializer

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
# Wave 1: vendor-looking token seeded on the metric snapshot rows (provenance +
# engine_error_code) — the serving projects ONLY the numeric value, so this token
# must never reach a served payload.
SENT_METRIC_VENDOR = "MetricVendorNameZ7"
FORBIDDEN = [
    SENT_RAW, SENT_SRC, SENT_MD5, "LEAKCIK789", "ROWKEYLEAK000", SENT_FILE, SENT_VENDOR,
    SENT_OBS, SENT_METRIC_VENDOR, "raw_row_id", "source_run_id", "source_lineage",
    "provenance", "contributing_observation_ids", "source_typed_projection",
    "holding_id", "accession_number", "identity_key", "engine_error_code",
]

# Internal provenance blob (must never be projected at all).
PROV = json.dumps({
    "source_run_id": SENT_SRC, "raw_row_id": SENT_RAW, "text_block_md5": SENT_MD5,
    "source_table": SENT_FILE, "vendor": SENT_VENDOR, "registrant_cik": SENT_CIK,
})


def dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def connect() -> psycopg.Connection:
    return psycopg.connect(dsn())


def terms(**extra: object) -> str:
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


def synthetic_snapshots(cur) -> None:
    """Create the bond snapshot stand-ins the serving projects, with embedded leaks."""
    # --- securities (catalog/detail source) --------------------------------
    cur.execute("""
        CREATE TABLE sec_current_bond_security_v1(
            security_id uuid, identity_state text, identity_reason_code text,
            issuer_name text, currency text, coupon_type text, coupon_rate numeric,
            maturity_date date, seniority text, secured text, is_144a text,
            day_count text, settlement_convention text, terms jsonb,
            identity_evidence jsonb, measured_at date, provenance jsonb)
    """)
    cur.execute(
        "INSERT INTO sec_current_bond_security_v1 VALUES "
        "(%s,'resolved',NULL,'Acme Corp','USD','fixed',5.25,'2030-06-30','senior',"
        "'unsecured','true','30/360','T+2',%s,%s,%s,%s)",
        (SEC1, terms(),
         json.dumps({"distinct_cusip9": ["037833100"], "distinct_isin": [],
                     "distinct_issuer_name": ["Acme Corp"], "conflicts": {},
                     "contributing_observation_ids": [SENT_OBS]}),
         AS_OF, PROV),
    )
    cur.execute(
        "INSERT INTO sec_current_bond_security_v1 VALUES "
        "(%s,'ambiguous','conflicting_isin_evidence',NULL,'USD','fixed',3.10,"
        "'2029-03-15','senior','secured','false','30/360','T+2',%s,%s,%s,%s)",
        (SEC2, terms(),
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
    # --- computed per-security metrics (catalog/detail computed values) -----
    # Stand-in for the promoted current view over bond_metric_v1 (Task 3). SEC2
    # exercises every null-honest arm: non-available statuses AND a missing row
    # (no current_yield) — plus a vendor-looking engine_error_code/provenance
    # token that must never be served.
    #
    # MUTATION LOCK (review IMP-1): two SEC2 rows are POISONED — a non-available
    # status carrying a NON-NULL value. The real bond_metric_v1 CHECK
    # ((status='available') = (value IS NOT NULL)) forbids this state, and this
    # stand-in deliberately has NO such CHECK: the serving-side
    # ``status = 'available'`` filter must be a guard in its own right (defense
    # in depth), never a free ride on the upstream constraint. Removing the
    # filter from ``_metric_value_sql`` serves 0.9999/99.9 and FAILS the
    # projection tests instead of passing silently.
    cur.execute("""
        CREATE TABLE sec_current_bond_metric_v1(
            security_id uuid, metric_id text, value numeric, status text,
            engine_error_code text, as_of date, provenance jsonb)
    """)
    metric_prov = json.dumps({"source_run_id": SENT_SRC, "vendor": SENT_METRIC_VENDOR})
    cur.execute(
        "INSERT INTO sec_current_bond_metric_v1 VALUES "
        "(%s,'security_ytm',0.0525,'available',NULL,%s,%s),"
        "(%s,'security_ytw',0.0518,'available',NULL,%s,%s),"
        "(%s,'current_yield',0.0531,'available',NULL,%s,%s),"
        "(%s,'wal',4.37,'available',NULL,%s,%s),"
        "(%s,'security_ytm',0.9999,'no_eligible_price',NULL,%s,%s),"  # POISONED
        "(%s,'security_ytw',NULL,'gate_not_passed',NULL,%s,%s),"
        "(%s,'wal',99.9,'engine_typed_error',%s,%s,%s)",  # POISONED
        (SEC1, AS_OF, metric_prov, SEC1, AS_OF, metric_prov,
         SEC1, AS_OF, metric_prov, SEC1, AS_OF, metric_prov,
         SEC2, AS_OF, metric_prov, SEC2, AS_OF, metric_prov,
         SEC2, SENT_METRIC_VENDOR, AS_OF, metric_prov),
    )
    # --- N-PORT reverse-lookup source (fund_exposure) ----------------------
    cur.execute("""
        CREATE TABLE sec_nport_holdings_v2_current(
            series_id text, class_id text, cusip text, isin text,
            signed_market_value numeric, signed_pct_of_nav numeric,
            report_date date, accession_number text, holding_id text,
            issuer_category text, source_typed_projection jsonb)
    """)
    rpt = AS_OF - timedelta(days=5)

    def _proj(country: str) -> str:
        return json.dumps({
            "ASSET_CAT": "DBT", "INVESTMENT_COUNTRY": country,
            "raw_row_id": SENT_RAW, "vendor": SENT_VENDOR,
        })

    # The fund_exposure cohort: at/before as_of, the bridge class fan-out on H1
    # (C1+C2) plus lot H2. All three report MUN/KY.
    cur.execute(
        "INSERT INTO sec_nport_holdings_v2_current VALUES "
        "('S1','C1','037833100',NULL,100.0,0.10,%s,'A1','H1','MUN',%s),"
        "('S1','C2','037833100',NULL,100.0,0.10,%s,'A1','H1','MUN',%s),"
        "('S1','C1','037833100',NULL,50.0,0.05,%s,'A1','H2','MUN',%s)",
        (rpt, _proj("KY"), rpt, _proj("KY"), rpt, _proj("KY")),
    )
    # Reported AFTER as_of, and deliberately the classification MAJORITY: four
    # CORP/US lots against the three MUN/KY ones above. The two surfaces must
    # read this cohort differently -- fund_exposure MUST exclude it (a holding
    # reported after as_of is look-ahead for "who held this as of X"), while the
    # issuer classification MUST include it (it describes what the bond IS, not
    # a dated exposure). So SEC1 serves CORP/US while its exposure still
    # aggregates only the pre-as_of lots. Reintroducing the PIT filter on the
    # classification flips it to MUN/KY; that is the bug the first build shipped,
    # where the filter admitted 491 of 4,507,500 live holdings and every security
    # served a NULL classification. SEC2's alias (459200101) is held by no lot at
    # all, so its classification stays an honest NULL.
    later = AS_OF + timedelta(days=20)
    cur.execute(
        "INSERT INTO sec_nport_holdings_v2_current VALUES "
        "('S2','C1','037833100',NULL,10.0,0.01,%s,'A2','H3','CORP',%s),"
        "('S2','C1','037833100',NULL,10.0,0.01,%s,'A2','H4','CORP',%s),"
        "('S3','C1','037833100',NULL,10.0,0.01,%s,'A3','H5','CORP',%s),"
        "('S3','C1','037833100',NULL,10.0,0.01,%s,'A3','H6','CORP',%s)",
        (later, _proj("US"), later, _proj("US"), later, _proj("US"), later, _proj("US")),
    )


def setup(cur) -> str:
    """Stand up an isolated schema, source lineage, the serving DDL, and snapshots."""
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
    synthetic_snapshots(cur)
    return schema


def protocol_only_schema(cur) -> str:
    """Isolated schema with the protocol + serving DDL but NO bond source snapshots."""
    schema = f"bond_serving_{uuid4().hex}"
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    materializer.install_schema(cur.connection)
    return schema

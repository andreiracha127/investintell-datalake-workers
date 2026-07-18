from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


def _seed_fixture(cur):
    schema = f"nport_fixed_income_fixture_{uuid4().hex}"
    run_id, package_id, holdings_publication_id, features_publication_id = (uuid4() for _ in range(4))
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
    for ddl_name in ("sec_derived_publications.sql", "nport_holdings_v2.sql", "nport_fixed_income_features.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    cur.execute("""INSERT INTO sec_derived_publications
        (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
        VALUES(%s,'sec_nport_holdings_v2',1,%s,%s,%s)""", (holdings_publication_id, run_id, package_id, "a" * 64))
    return schema, run_id, package_id, holdings_publication_id, features_publication_id


def _holding(cur, publication_id, run_id, holding_id, series_id, report_date, market_value, projection, *, cusip="", isin=None, lei=None, nav_pct=None):
    cur.execute("""INSERT INTO sec_nport_instrument_class_bridge
        (publication_id,accession_number,holding_id,instrument_id,series_id,class_id,valid_from,resolution_state)
        VALUES(%s,'A1',%s,%s,%s,'C1','2020-01-01','resolved')""",
        (publication_id, holding_id, f"I-{holding_id}", series_id))
    cur.execute("""INSERT INTO sec_nport_holdings_v2
        (publication_id,accession_number,holding_id,source_run_id,report_date,filing_date,source_series_id,
         signed_market_value,signed_pct_of_nav,cusip,isin,issuer_lei,source_typed_projection)
        VALUES(%s,'A1',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
        (publication_id, holding_id, run_id, report_date, report_date, series_id, market_value, nav_pct, cusip, isin, lei, projection))


def _publish_holdings(cur, publication_id):
    cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
    cur.execute("SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)", (publication_id,))


def _prepare_features_publication(cur, publication_id, run_id, package_id):
    cur.execute("""INSERT INTO sec_derived_publications
        (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
        VALUES(%s,'nport_fixed_income_features_v1',1,%s,%s,%s)""",
        (publication_id, run_id, package_id, "b" * 64))


def test_fixed_income_features_builds_complete_degraded_insufficient_and_unavailable_rows():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        # Certified: signed and gross intentionally differ (+100 and -20).
        _holding(cur, holdings_id, run_id, "C1", "CERT", "2026-01-31", 100, '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"5.0","MATURITY_DATE":"2026-12-31"}}', cusip="111111111", nav_pct=10)
        _holding(cur, holdings_id, run_id, "C2", "CERT", "2026-01-31", -20, '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"floating","ANNUALIZED_RATE":"3.0","MATURITY_DATE":"2037-01-31"}}', isin="US2222222222", nav_pct=-2)
        # 80% of DBT market value has the official debt extension.
        _holding(cur, holdings_id, run_id, "D1", "DEG", "2026-01-31", 80, '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"variable","ANNUALIZED_RATE":"4.0","MATURITY_DATE":"2029-01-31"}}', cusip="333333333")
        _holding(cur, holdings_id, run_id, "D2", "DEG", "2026-01-31", 20, '{"ASSET_CAT":"DBT"}')
        # 60% is below the insufficient threshold.
        _holding(cur, holdings_id, run_id, "I1", "INSUFF", "2026-01-31", 60, '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"none","ANNUALIZED_RATE":"0","MATURITY_DATE":"2047-01-31"}}')
        _holding(cur, holdings_id, run_id, "I2", "INSUFF", "2026-01-31", 40, '{"ASSET_CAT":"DBT"}')
        # Full evidence is still insufficient once the pinned as-of date makes it stale.
        _holding(cur, holdings_id, run_id, "S1", "STALE", "2025-01-31", 100, '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"2.0","MATURITY_DATE":"2027-01-31"}}')
        # A resolved non-DBT holding is an explicit unavailable state, not zeros.
        _holding(cur, holdings_id, run_id, "U1", "UNAV", "2026-01-31", 100, '{"ASSET_CAT":"EC"}')
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (5,)
        cur.execute("""SELECT series_id,status,debt_signed_market_value,debt_gross_market_value,
                              debt_market_value_coverage,coupon_weighted_average,coupon_type_mix,
                              maturity_ladder,identifier_market_value_coverage,reason_codes,coverage
                       FROM nport_fixed_income_features ORDER BY series_id""")
        rows = {row[0]: row[1:] for row in cur.fetchall()}
        assert rows["CERT"][0:4] == ("certified", 80, 120, 1)
        assert float(rows["CERT"][4]) == pytest.approx(14 / 3)
        assert rows["CERT"][5]["fixed"] == pytest.approx(100 / 120)
        assert rows["CERT"][5]["floating"] == pytest.approx(20 / 120)
        assert rows["CERT"][6]["lt_1y"] == pytest.approx(100 / 120)
        assert rows["CERT"][6]["10_20y"] == pytest.approx(20 / 120)
        assert rows["CERT"][7] == 1
        assert rows["DEG"][0] == "degraded" and float(rows["DEG"][3]) == pytest.approx(0.8)
        assert rows["INSUFF"][0] == "insufficient" and float(rows["INSUFF"][3]) == pytest.approx(0.6)
        assert rows["STALE"][0] == "insufficient" and "report_age_exceeds_180_days" in rows["STALE"][8]
        assert rows["UNAV"][0] == "unavailable"
        assert rows["UNAV"][1:8] == (None, None, None, None, None, None, None)
        assert rows["UNAV"][8] == ["no_explicit_dbt_positions", "coupon_not_fully_reported", "maturity_not_fully_reported"]
        assert rows["CERT"][9]["debt_market_value"] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fixed_income_features_preserves_missing_coupon_and_maturity_as_unavailable_metrics_and_freezes():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        _holding(cur, holdings_id, run_id, "M1", "MISS", "2026-01-31", 100,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed"}}')
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (1,)
        # Re-running does not mutate or duplicate prepared snapshot rows.
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (0,)
        cur.execute("""SELECT status,coupon_weighted_average,coupon_market_value_coverage,
                              maturity_market_value_coverage,coupon_type_mix,maturity_ladder,reason_codes
                       FROM nport_fixed_income_features WHERE series_id='MISS'""")
        status, coupon, coupon_coverage, maturity_coverage, coupon_mix, ladder, reasons = cur.fetchone()
        assert status == "certified"
        assert coupon is None and coupon_coverage == 0 and maturity_coverage == 0
        assert coupon_mix["fixed"] == 1 and ladder["perpetual_or_missing"] == 1
        assert {"coupon_not_fully_reported", "maturity_not_fully_reported"} <= set(reasons)
        cur.execute("SELECT sec_validate_derived_publication(%s)", (features_id,))
        cur.execute("SELECT sec_set_current_derived_publication('nport_fixed_income_features_v1',%s)", (features_id,))
        with pytest.raises(psycopg.Error, match="prepared fixed-income publication"):
            cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        with pytest.raises(psycopg.Error, match="feature row is immutable"):
            cur.execute("UPDATE nport_fixed_income_features SET status='degraded' WHERE publication_id=%s", (features_id,))
        cur.execute("SELECT series_id,status FROM sec_current_nport_fixed_income_features")
        assert cur.fetchone() == ("MISS", "certified")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fixed_income_features_stays_nport_native_and_excludes_phase_10_metrics():
    ddl = (ROOT / "schemas" / "nport_fixed_income_features.sql").read_text(encoding="utf-8").lower()
    for required in ("sec_nport_holdings_v2_current", "debt_security", "coupon_type", "maturity_date", "methodology_version", "reason_codes", "provenance"):
        assert required in ddl
    for phase_10_metric in ("rating_distribution", "ytm", "ytw", "current_yield", "oas", "z_spread", "effective_duration"):
        assert phase_10_metric not in ddl
    assert "sec_w1_nport_real" not in ddl

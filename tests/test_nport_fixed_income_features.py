from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


BUILDER_SQL = ROOT / "src" / "nport" / "sql" / "nport_fixed_income_features_builder.sql"


def _install_legacy_builder(cur) -> None:
    """Install the sha256-pinned builder -- the same resource the worker installs."""
    cur.execute(BUILDER_SQL.read_text(encoding="utf-8"))


def _seed_fixture(cur):
    schema = f"nport_fixed_income_fixture_{uuid4().hex}"
    run_id, package_id, holdings_publication_id, features_publication_id = (uuid4() for _ in range(4))
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
    cur.execute("""CREATE TABLE nport_raw_rows(
        raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        ingestion_run_id uuid NOT NULL, source_file_id uuid NOT NULL, source_row_number bigint NOT NULL,
        source_table text NOT NULL, accession_number text, holding_id text, typed_projection jsonb NOT NULL,
        UNIQUE(source_file_id,source_row_number))""")
    for relation, source_table in (
        ("nport_interest_rate_risk_raw", "INTEREST_RATE_RISK.tsv"),
        ("nport_fund_reported_info_raw", "FUND_REPORTED_INFO.tsv"),
        ("nport_borrow_aggregate_raw", "BORROW_AGGREGATE.tsv"),
        ("nport_repurchase_agreement_raw", "REPURCHASE_AGREEMENT.tsv"),
        ("nport_repurchase_collateral_raw", "REPURCHASE_COLLATERAL.tsv"),
        ("nport_repurchase_counterparty_raw", "REPURCHASE_COUNTERPARTY.tsv"),
        ("nport_securities_lending_raw", "SECURITIES_LENDING.tsv"),
    ):
        cur.execute(f"""CREATE VIEW {relation} AS
            SELECT r.* FROM nport_raw_rows r JOIN sec_validated_raw_runs v ON v.run_id=r.ingestion_run_id
            WHERE r.source_table='{source_table}'""")
    for ddl_name in ("sec_derived_publications.sql", "nport_holdings_v2.sql", "nport_fixed_income_features.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)
    _install_legacy_builder(cur)
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


def _raw(cur, run_id, source_table, accession, projection, *, holding_id=None):
    cur.execute("""INSERT INTO nport_raw_rows
        (ingestion_run_id,source_file_id,source_row_number,source_table,accession_number,holding_id,typed_projection)
        VALUES(%s,%s,2,%s,%s,%s,%s::jsonb)""", (run_id, uuid4(), source_table, accession, holding_id, projection))


def test_reapplying_production_ddl_never_replaces_the_builder_with_a_stub():
    """The DDL used to re-install a raise-stub, which made the product unbuildable.

    Re-applying the schema over a live database must leave the real builder in
    place; the only thing the DDL may do is re-assert the tables, guards and
    current-pointer views.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        cur.execute((ROOT / "schemas" / "nport_fixed_income_features.sql").read_text(encoding="utf-8"))
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        # No 0A000: the builder runs (an empty snapshot simply inserts nothing).
        cur.execute("SELECT build_nport_fixed_income_features(%s,%s)", (features_id, "2026-07-24"))
        assert cur.fetchone()[0] == 0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


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
        assert rows["UNAV"][8] == ["no_explicit_dbt_positions"]
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
        with pytest.raises(psycopg.Error, match="build identity is immutable"):
            cur.execute("UPDATE nport_fixed_income_feature_builds SET as_of_date='2026-07-01' WHERE publication_id=%s", (features_id,))
        cur.execute("SELECT series_id,status FROM sec_current_nport_fixed_income_features")
        assert cur.fetchone() == ("MISS", "certified")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fixed_income_build_pins_one_source_publication_and_as_of_date():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        _holding(cur, holdings_id, run_id, "P1", "PINNED", "2026-01-31", 100,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"2","MATURITY_DATE":"2027-01-31"}}')
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (0,)
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-07-01')", (features_id,))

        second_holdings_id = uuid4()
        cur.execute("""INSERT INTO sec_derived_publications
            (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
            VALUES(%s,'sec_nport_holdings_v2',2,%s,%s,%s)""",
            (second_holdings_id, run_id, package_id, "c" * 64))
        _holding(cur, second_holdings_id, run_id, "P2", "PINNED", "2026-02-28", 200,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"3","MATURITY_DATE":"2028-02-28"}}')
        _publish_holdings(cur, second_holdings_id)
        with pytest.raises(psycopg.Error, match="already pinned to source publication"):
            cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        cur.execute("""SELECT source_holdings_publication_id,as_of_date,count(*) OVER ()
                       FROM nport_fixed_income_feature_builds WHERE publication_id=%s""", (features_id,))
        assert cur.fetchone() == (holdings_id, date(2026, 6, 30), 1)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_unknown_market_value_zero_extension_and_invalid_maturity_fail_closed():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        _holding(cur, holdings_id, run_id, "N1", "NULL_MV", "2026-01-31", None,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"4","MATURITY_DATE":"2027-01-31"}}')
        _holding(cur, holdings_id, run_id, "Z1", "ZERO_EXT", "2026-01-31", 100,
                 '{"ASSET_CAT":"DBT"}')
        _holding(cur, holdings_id, run_id, "B1", "BAD_DATE", "2026-01-31", 100,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"4","MATURITY_DATE":"2026-02-30"}}')
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (3,)
        cur.execute("""SELECT series_id,status,unknown_debt_market_value_position_count,
                              debt_gross_market_value,debt_market_value_coverage,
                              maturity_market_value_coverage,reason_codes
                       FROM nport_fixed_income_features ORDER BY series_id""")
        rows = {row[0]: row[1:] for row in cur.fetchall()}
        assert rows["NULL_MV"][0:5] == ("insufficient", 1, None, None, None)
        assert "unknown_debt_market_value" in rows["NULL_MV"][5]
        assert rows["ZERO_EXT"][0] == "unavailable"
        assert rows["ZERO_EXT"][1] == 0
        assert "debt_extension_evidence_absent" in rows["ZERO_EXT"][5]
        assert "no_explicit_dbt_positions" not in rows["ZERO_EXT"][5]
        assert rows["BAD_DATE"][0] == "certified"
        assert rows["BAD_DATE"][4] == 0
        assert "maturity_not_fully_reported" in rows["BAD_DATE"][5]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_quality_threshold_boundaries_are_inclusive_and_age_is_pinned():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        for series, report_date, covered, uncovered in (
            ("COV_70", "2026-01-31", 70, 30),
            ("COV_90", "2026-01-31", 90, 10),
            ("AGE_180", "2026-01-01", 100, 0),
            ("AGE_181", "2025-12-31", 100, 0),
        ):
            _holding(cur, holdings_id, run_id, f"{series}-E", series, report_date, covered,
                     '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"2","MATURITY_DATE":"2028-01-01"}}')
            if uncovered:
                _holding(cur, holdings_id, run_id, f"{series}-N", series, report_date, uncovered,
                         '{"ASSET_CAT":"DBT"}')
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (4,)
        cur.execute("SELECT series_id,status,report_age_days FROM nport_fixed_income_features")
        rows = {series: (status, age) for series, status, age in cur.fetchall()}
        assert rows == {
            "COV_70": ("degraded", 150),
            "COV_90": ("certified", 150),
            "AGE_180": ("certified", 180),
            "AGE_181": ("insufficient", 181),
        }
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fixed_income_keeps_resolved_positions_without_security_identity():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        projection = '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"2","MATURITY_DATE":"2028-01-31"}}'
        _holding(cur, holdings_id, run_id, "IDENTIFIED", "FI-NULL-IDENTITY", "2026-01-31", 100, projection, cusip="111111111")
        cur.execute("""INSERT INTO sec_nport_instrument_class_bridge
            (publication_id,accession_number,holding_id,instrument_id,series_id,class_id,valid_from,resolution_state)
            VALUES(%s,'A1','UNIDENTIFIED',NULL,'FI-NULL-IDENTITY',NULL,'2020-01-01','resolved')""", (holdings_id,))
        cur.execute("""INSERT INTO sec_nport_holdings_v2
            (publication_id,accession_number,holding_id,source_run_id,report_date,filing_date,source_series_id,
             signed_market_value,signed_pct_of_nav,source_typed_projection)
            VALUES(%s,'A1','UNIDENTIFIED',%s,'2026-01-31','2026-01-31','FI-NULL-IDENTITY',50,5,%s::jsonb)""",
            (holdings_id, run_id, projection))
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (1,)
        cur.execute("""SELECT position_count,debt_position_count,debt_gross_market_value,
            source_holdings_publication_id FROM nport_fixed_income_features WHERE publication_id=%s""", (features_id,))
        position_count, debt_position_count, gross_market_value, pinned_source = cur.fetchone()
        assert (position_count, debt_position_count, gross_market_value, pinned_source) == (2, 2, 150, holdings_id)
        cur.execute("""SELECT source_holdings_publication_id FROM nport_fixed_income_feature_builds
            WHERE publication_id=%s""", (features_id,))
        assert cur.fetchone() == (holdings_id,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fixed_income_features_stays_nport_native_and_excludes_phase_10_metrics():
    ddl = (ROOT / "schemas" / "nport_fixed_income_features.sql").read_text(encoding="utf-8").lower()
    oracle = BUILDER_SQL.read_text(encoding="utf-8").lower()
    for required in ("sec_nport_holdings_v2_current", "debt_security", "coupon_type", "maturity_date"):
        assert required in oracle
    for required in ("methodology_version", "reason_codes", "provenance"):
        assert required in ddl
    for phase_10_metric in ("rating_distribution", "ytm", "ytw", "current_yield", "oas", "z_spread", "effective_duration"):
        assert phase_10_metric not in oracle
    assert "sec_w1_nport_real" not in oracle
    assert "sec_nport_holdings_v2_current h" not in oracle
    assert "for share of c" in oracle
    assert "from sec_nport_holdings_v2 h" in oracle
    assert "sqlstate '0a000'" not in ddl  # the raise-stub is gone for good
    assert "snapshot_holdings as" not in ddl


def test_fixed_income_adds_weighted_maturity_statistics_without_zero_fill():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        for holding_id, value, maturity in (("M1", 10, "2027-01-31"), ("M2", 80, "2030-01-31"), ("M3", 10, "2035-01-31")):
            _holding(cur, holdings_id, run_id, holding_id, "RICH", "2026-01-31", value,
                     f'{{"ASSET_CAT":"DBT","DEBT_SECURITY":{{"MATURITY_DATE":"{maturity}"}}}}')
        _holding(cur, holdings_id, run_id, "S1", "SPARSE", "2026-01-31", 100,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{}}')
        _holding(cur, holdings_id, run_id, "F1", "FLAG_NULL", "2026-01-31", None,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"IS_DEFAULT":"maybe"}}')
        _holding(cur, holdings_id, run_id, "F2", "FLAG_ZERO", "2026-01-31", 0,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"IS_DEFAULT":" yes "}}')
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        cur.execute("""SELECT series_id,maturity_weighted_mean_years,maturity_weighted_p25_years,
            maturity_weighted_median_years,maturity_weighted_p75_years,maturity_statistics_market_value_coverage
            FROM nport_fixed_income_features ORDER BY series_id""")
        rows = {row[0]: row[1:] for row in cur.fetchall()}
        rich = rows["RICH"]
        assert rich[0] is not None and rich[1] <= rich[2] <= rich[3]
        assert float(rich[4]) == pytest.approx(1)
        assert rows["SPARSE"] == (None, None, None, None, 0)
        cur.execute("""SELECT reported_state,gross_market_value_coverage
            FROM nport_fixed_income_debt_flag_features_v2
            WHERE publication_id=%s AND series_id='SPARSE' AND flag_key='is_default'""", (features_id,))
        assert cur.fetchone() == ("not_reported", 0)
        cur.execute("""SELECT reported_state,unknown_gross_market_value_position_count,gross_market_value_coverage
            FROM nport_fixed_income_debt_flag_features_v2
            WHERE publication_id=%s AND series_id='FLAG_NULL' AND flag_key='is_default'""", (features_id,))
        assert cur.fetchone() == ("not_reported", 1, None)
        cur.execute("""SELECT reported_state FROM nport_fixed_income_debt_flag_features_v2
            WHERE publication_id=%s AND series_id='FLAG_ZERO' AND flag_key='is_default'""", (features_id,))
        assert cur.fetchone() == ("reported_true",)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fixed_income_v2_materializes_governed_raw_facts_with_run_isolation():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        _holding(cur, holdings_id, run_id, "H1", "RAW", "2026-01-31", 100,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"MATURITY_DATE":"2030-01-31",'
                 '"IS_DEFAULT":"Y","ARE_ANY_INTEREST_PAYMENT":"Y",'
                 '"IS_CONVTIBLE_MANDATORY":"N","IS_CONVTIBLE_CONTINGENT":"Y"}}')
        _raw(cur, run_id, "INTEREST_RATE_RISK.tsv", "A1", '{"CURRENCY_CODE":"USD",'
             '"INTEREST_RATE_RISK_ID":"RISK-1","INTRST_RATE_CHANGE_3MON_DV01":"-12","INTRST_RATE_CHANGE_3MON_DV100":"-120"}')
        _raw(cur, run_id, "INTEREST_RATE_RISK.tsv", "A1", '{"CURRENCY_CODE":"USD",'
             '"INTEREST_RATE_RISK_ID":"RISK-2","INTRST_RATE_CHANGE_3MON_DV01":"-8"}')
        _raw(cur, run_id, "FUND_REPORTED_INFO.tsv", "A1", '{"NET_ASSETS":"200",'
             '"CREDIT_SPREAD_3MON_INVEST":"3","CREDIT_SPREAD_3MON_NONINVEST":"-4",'
             '"BORROWING_PAY_WITHIN_1YR":"10","STANDBY_COMMITMENT":"5"}')
        _raw(cur, run_id, "BORROW_AGGREGATE.tsv", "A1", '{"AMOUNT":"10","COLLATERAL":"4","INVESTMENT_CAT":"DBT"}')
        _raw(cur, run_id, "REPURCHASE_COUNTERPARTY.tsv", "A1", '{"NAME":"CP ONE","LEI":"LEI-1"}', holding_id="H1")
        _raw(cur, run_id, "REPURCHASE_COUNTERPARTY.tsv", "A1", '{"REPURCHASE_COUNTERPARTY_ID":"CP-2","NAME":"CP TWO","LEI":"LEI-2"}', holding_id="H1")
        _raw(cur, run_id, "SECURITIES_LENDING.tsv", "A1", '{"LOAN_VALUE":"7","CASH_COLLATERAL_AMOUNT":"5",'
             '"IS_CASH_COLLATERAL":" y ","IS_NON_CASH_COLLATERAL":"No","IS_LOAN_BY_FUND":"wat"}', holding_id="H1")
        other_run = uuid4()
        cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (other_run,))
        _raw(cur, other_run, "INTEREST_RATE_RISK.tsv", "A1", '{"CURRENCY_CODE":"USD",'
             '"INTRST_RATE_CHANGE_3MON_DV01":"999","INTRST_RATE_CHANGE_3MON_DV100":"9999"}')
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        cur.execute("""SELECT interest_rate_risk_id,raw_value,normalized_to_net_assets,unit,sign_semantics
            FROM nport_fixed_income_key_rate_sensitivities_v2
            WHERE publication_id=%s AND sensitivity='dv01' ORDER BY interest_rate_risk_id""", (features_id,))
        rows = cur.fetchall()
        assert (rows[0][0], rows[0][1], float(rows[0][2]), rows[0][3:]) == ("RISK-1", -12, pytest.approx(-0.06), ("reported_currency_value_per_1bp", "reported_signed"))
        assert rows[1][0:2] == ("RISK-2", -8)
        cur.execute("""SELECT metric_key,numerator,denominator,coverage_ratio,availability_state,missing_reason
            FROM nport_fixed_income_metric_coverage_v2 WHERE publication_id=%s ORDER BY metric_key,source_raw_row_id""", (features_id,))
        coverage_rows = cur.fetchall()
        assert ("key_rate.dv01.3mon", 1, 1, 1, "reported_numeric", None) in coverage_rows
        # Absence is no longer a row per position: it is counted in the rollup.
        assert not [row for row in coverage_rows if row[4] != "reported_numeric"]
        cur.execute("""SELECT reported_row_count,source_row_count,availability_state,
                              missing_reason_counts->>'named_field_missing_or_invalid'
            FROM nport_fixed_income_metric_coverage_snapshot_v1
            WHERE publication_id=%s AND metric_key='key_rate.dv100.3mon'""", (features_id,))
        assert cur.fetchone() == (1, 2, None, "1")
        # The repo/securities-lending family is no longer built, so it has no
        # coverage either -- evidence for a metric nobody serves is not evidence.
        cur.execute("""SELECT count(*) FROM nport_fixed_income_metric_coverage_snapshot_v1
            WHERE publication_id=%s AND metric_family='repo_lending'""", (features_id,))
        assert cur.fetchone() == (0,)
        cur.execute("""SELECT investment_bucket,raw_value,measurement_type
                FROM nport_fixed_income_credit_spread_sensitivities_v2
                WHERE publication_id=%s AND raw_value IS NOT NULL ORDER BY investment_bucket""", (features_id,))
        assert cur.fetchall() == [("investment", 3, "reported_credit_spread_sensitivity"), ("noninvestment", -4, "reported_credit_spread_sensitivity")]
        cur.execute("""SELECT primitive_key,raw_value,net_assets_denominator
            FROM nport_fixed_income_balance_sheet_primitives_v2
            WHERE publication_id=%s ORDER BY primitive_key""", (features_id,))
        assert ("net_assets", 200, 200) in cur.fetchall()
        cur.execute("""SELECT flag_key,gross_market_value_coverage FROM nport_fixed_income_debt_flag_features_v2
            WHERE publication_id=%s ORDER BY flag_key""", (features_id,))
        assert ("is_default", 1) in cur.fetchall()
        # The repo/securities-lending raw rows above are deliberately still
        # seeded: the build must simply ignore them now.
        for retired in (
            "nport_fixed_income_repo_lending_primitives_v2",
            "nport_fixed_income_repo_lending_reported_flags_v2",
        ):
            cur.execute(
                f"SELECT count(*) FROM {retired} WHERE publication_id=%s", (features_id,)
            )
            assert cur.fetchone() == (0,), retired
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fixed_income_v2_key_rates_do_not_require_fund_reported_info():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        _holding(cur, holdings_id, run_id, "H1", "NO_FUND", "2026-01-31", 100,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{}}')
        _raw(cur, run_id, "INTEREST_RATE_RISK.tsv", "A1", '{"INTEREST_RATE_RISK_ID":"RISK-ONLY",'
             '"INTRST_RATE_CHANGE_3MON_DV01":"-9","INTRST_RATE_CHANGE_3MON_DV100":"-90"}')
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        cur.execute("""SELECT source_raw_row_id,source_file_id,source_row_number,raw_value,
            normalized_to_net_assets,net_assets_denominator
            FROM nport_fixed_income_key_rate_sensitivities_v2
            WHERE publication_id=%s AND interest_rate_risk_id='RISK-ONLY'
            ORDER BY sensitivity""", (features_id,))
        rows = cur.fetchall()
        assert len(rows) == 2
        assert {row[3:] for row in rows} == {(-9, None, None), (-90, None, None)}
        assert len({row[0] for row in rows}) == 1 and rows[0][0] is not None
        assert len({row[1] for row in rows}) == 1 and rows[0][1] is not None
        assert {row[2] for row in rows} == {2}
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fixed_income_v2_preserves_physical_raw_identity_and_full_coverage_states():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed_fixture(cur)
        _holding(cur, holdings_id, run_id, "H1", "IDENTITY", "2026-01-31", 100,
                 '{"ASSET_CAT":"DBT","DEBT_SECURITY":{}}')
        _raw(cur, run_id, "INTEREST_RATE_RISK.tsv", "A1", '{"INTRST_RATE_CHANGE_3MON_DV01":"0"}')
        _raw(cur, run_id, "FUND_REPORTED_INFO.tsv", "A1", '{"NET_ASSETS":"100","TOTAL_ASSETS":"bad"}')
        _raw(cur, run_id, "REPURCHASE_COUNTERPARTY.tsv", "A1", '{"NAME":"one"}', holding_id="H1")
        _raw(cur, run_id, "REPURCHASE_COUNTERPARTY.tsv", "A1", '{"NAME":"two"}', holding_id="H1")
        _publish_holdings(cur, holdings_id)
        _prepare_features_publication(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        # The repo/securities-lending surfaces are no longer built: a second
        # build over the same raw rows still writes nothing there, and the
        # rebuild below stays idempotent without them.
        cur.execute("""SELECT count(*) FROM nport_fixed_income_repo_lending_primitives_v2
            WHERE publication_id=%s""", (features_id,))
        assert cur.fetchone() == (0,)
        cur.execute("""SELECT count(*) FROM nport_fixed_income_repo_lending_reported_flags_v2
            WHERE publication_id=%s""", (features_id,))
        assert cur.fetchone() == (0,)
        cur.execute("SELECT build_nport_fixed_income_features(%s,'2026-06-30')", (features_id,))
        # Every availability state the per-position rows used to carry is still
        # reported, per metric, by the rollup -- including the two the base
        # table no longer materializes.
        cur.execute("""SELECT metric_family,availability_state
            FROM nport_fixed_income_metric_coverage_snapshot_v1
            WHERE publication_id=%s AND metric_key IN ('key_rate.dv01.3mon','key_rate.dv100.3mon','balance.total_assets')
            ORDER BY metric_key""", (features_id,))
        states = {(row[0], row[1]) for row in cur.fetchall()}
        assert ('key_rate_sensitivity', 'reported_numeric') in states
        assert ('key_rate_sensitivity', 'field_missing_or_invalid') in states
        assert ('balance_sheet', 'field_missing_or_invalid') in states
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
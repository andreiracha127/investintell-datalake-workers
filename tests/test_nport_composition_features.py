from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


def _seed(cur):
    schema = f"nport_composition_fixture_{uuid4().hex}"
    run_id, package_id, holdings_id, features_id = (uuid4() for _ in range(4))
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
    for name in ("sec_derived_publications.sql", "nport_holdings_v2.sql", "nport_composition_features.sql"):
        ddl = (ROOT / "schemas" / name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    cur.execute("""INSERT INTO sec_derived_publications
        (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
        VALUES(%s,'sec_nport_holdings_v2',1,%s,%s,%s)""", (holdings_id, run_id, package_id, "a" * 64))
    return schema, run_id, package_id, holdings_id, features_id


def _holding(cur, publication_id, run_id, holding_id, series_id, report_date, value, *, issuer="", category=None, payoff=None, cusip=None, isin=None, lei=None, nav=None):
    cur.execute("""INSERT INTO sec_nport_instrument_class_bridge
        (publication_id,accession_number,holding_id,instrument_id,series_id,class_id,valid_from,resolution_state)
        VALUES(%s,'A1',%s,%s,%s,'C1','2020-01-01','resolved')""",
        (publication_id, holding_id, f"I-{holding_id}", series_id))
    cur.execute("""INSERT INTO sec_nport_holdings_v2
        (publication_id,accession_number,holding_id,source_run_id,report_date,filing_date,source_series_id,
         issuer_name,issuer_category,cusip,isin,issuer_lei,signed_market_value,signed_pct_of_nav,payoff_profile,source_typed_projection)
        VALUES(%s,'A1',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'{}'::jsonb)""",
        (publication_id, holding_id, run_id, report_date, report_date, series_id, issuer, category, cusip, isin, lei, value, nav, payoff))


def _publish_holdings(cur, publication_id):
    cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
    cur.execute("SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)", (publication_id,))


def _prepare_features(cur, publication_id, run_id, package_id):
    cur.execute("""INSERT INTO sec_derived_publications
        (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
        VALUES(%s,'nport_composition_features_v1',1,%s,%s,%s)""",
        (publication_id, run_id, package_id, "b" * 64))


def test_composition_preserves_shorts_residual_unknowns_and_concentration():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed(cur)
        _holding(cur, holdings_id, run_id, "H1", "COMP", "2026-01-31", 100, issuer="ACME", category="Corporate", payoff="Long", cusip="111111111", nav=10)
        _holding(cur, holdings_id, run_id, "H2", "COMP", "2026-01-31", -20, issuer="BETA", category="Corporate", payoff="Short", isin="US2222222222", nav=-2)
        _holding(cur, holdings_id, run_id, "H3", "COMP", "2026-01-31", 70, issuer="ACME", category="Corporate", payoff="Long", cusip="111111111", nav=7)
        _holding(cur, holdings_id, run_id, "H4", "COMP", "2026-01-31", 10, issuer="GAMMA", category="Government", payoff="Long", nav=1)
        _holding(cur, holdings_id, run_id, "N1", "NULL_MV", "2026-01-31", None, issuer="MISSING", category="Corporate", payoff="Long", cusip="333333333", nav=1)
        _holding(cur, holdings_id, run_id, "L1", "LEI_ONLY", "2026-01-31", 60, issuer="SAME ISSUER", category="Corporate", payoff="Long", lei="LEI-SAME", nav=6)
        _holding(cur, holdings_id, run_id, "L2", "LEI_ONLY", "2026-01-31", 40, issuer="SAME ISSUER", category="Corporate", payoff="Long", lei="LEI-SAME", nav=4)
        _publish_holdings(cur, holdings_id)
        _prepare_features(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (3,)
        cur.execute("""SELECT status,position_count,signed_market_value,gross_market_value,
                              signed_nav_pct,gross_nav_pct,signed_nav_residual_pct,gross_nav_residual_pct,
                              top_5_gross_market_value_share,top_10_gross_market_value_share,
                              issuer_hhi,issuer_effective_position_count,security_hhi,
                              unknown_market_value_position_count,reason_codes
                       FROM nport_composition_features WHERE series_id='COMP'""")
        row = cur.fetchone()
        assert row[:8] == ("certified", 4, 160, 200, 16, 20, 84, 80)
        assert float(row[8]) == pytest.approx(1)
        assert float(row[9]) == pytest.approx(1)
        assert float(row[10]) == pytest.approx(0.735)
        assert float(row[11]) == pytest.approx(1 / 0.735)
        assert float(row[12]) == pytest.approx((100 / 200) ** 2 + (20 / 200) ** 2 + (70 / 200) ** 2 + (10 / 200) ** 2)
        assert row[13] == 0
        assert "security_identifier_coverage_incomplete" not in row[14]
        cur.execute("""SELECT dimension_type,dimension_key,signed_market_value,gross_market_value,gross_market_value_share
                       FROM nport_composition_dimension_features
                       WHERE publication_id=%s AND series_id='COMP' ORDER BY dimension_type,dimension_key""", (features_id,))
        dimensions = {(kind, key): values for kind, key, *values in cur.fetchall()}
        assert dimensions[("issuer_category", "Corporate")][:2] == [150, 190]
        assert dimensions[("payoff_profile", "Short")][:2] == [-20, 20]
        assert dimensions[("identifier_availability", "unavailable")][:2] == [10, 10]
        cur.execute("""SELECT status,signed_market_value,gross_market_value,top_5_gross_market_value_share,
                              issuer_hhi,reason_codes
                       FROM nport_composition_features WHERE series_id='NULL_MV'""")
        null_mv = cur.fetchone()
        assert null_mv[:5] == ("insufficient", None, None, None, None)
        assert {"unknown_market_value", "identifier_coverage_unavailable",
                "issuer_category_coverage_unavailable", "payoff_profile_coverage_unavailable",
                "issuer_coverage_unavailable"} <= set(null_mv[5])
        assert null_mv[5]
        cur.execute("""SELECT security_hhi,security_effective_position_count,
                              identifier_market_value_coverage,security_identity_market_value_coverage
                       FROM nport_composition_features WHERE series_id='LEI_ONLY'""")
        lei_hhi, lei_effective, identifier_coverage, identity_coverage = cur.fetchone()
        assert float(lei_hhi) == pytest.approx(0.52)
        assert float(lei_effective) == pytest.approx(1 / 0.52)
        assert float(identifier_coverage) == pytest.approx(1)
        assert float(identity_coverage) == pytest.approx(1)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_composition_quality_boundaries_and_immutable_build_pin():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed(cur)
        for series, report_date, identified, unidentifed in (
            ("COV_90", "2026-01-31", 90, 10),
            ("COV_70", "2026-01-31", 70, 30),
            ("AGE_180", "2026-01-01", 100, 0),
            ("AGE_181", "2025-12-31", 100, 0),
        ):
            _holding(cur, holdings_id, run_id, f"{series}-I", series, report_date, identified, issuer="ONE", category="Corporate", payoff="Long", cusip="111111111", nav=identified / 10)
            if unidentifed:
                _holding(cur, holdings_id, run_id, f"{series}-U", series, report_date, unidentifed, issuer="TWO", category="Corporate", payoff="Long", nav=unidentifed / 10)
        _publish_holdings(cur, holdings_id)
        _prepare_features(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (4,)
        cur.execute("SELECT series_id,status,report_age_days,identifier_market_value_coverage FROM nport_composition_features")
        rows = {series: values for series, *values in cur.fetchall()}
        assert rows["COV_90"][:2] == ["certified", 150] and float(rows["COV_90"][2]) == pytest.approx(0.9)
        assert rows["COV_70"][:2] == ["degraded", 150] and float(rows["COV_70"][2]) == pytest.approx(0.7)
        assert rows["AGE_180"][:2] == ["certified", 180] and float(rows["AGE_180"][2]) == pytest.approx(1)
        assert rows["AGE_181"][:2] == ["insufficient", 181] and float(rows["AGE_181"][2]) == pytest.approx(1)
        cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (features_id,))
        assert cur.fetchone() == (0,)
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            cur.execute("SELECT build_nport_composition_features(%s,'2026-07-01')", (features_id,))
        second_holdings_id = uuid4()
        cur.execute("""INSERT INTO sec_derived_publications
            (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
            VALUES(%s,'sec_nport_holdings_v2',2,%s,%s,%s)""",
            (second_holdings_id, run_id, package_id, "c" * 64))
        _holding(cur, second_holdings_id, run_id, "NEXT", "COV_90", "2026-02-28", 100,
                 issuer="NEXT", category="Corporate", payoff="Long", cusip="222222222", nav=10)
        _publish_holdings(cur, second_holdings_id)
        with pytest.raises(psycopg.Error, match="already pinned to source publication"):
            cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (features_id,))
        cur.execute("SELECT sec_validate_derived_publication(%s)", (features_id,))
        cur.execute("SELECT sec_set_current_derived_publication('nport_composition_features_v1',%s)", (features_id,))
        with pytest.raises(psycopg.Error, match="prepared composition publication"):
            cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (features_id,))
        with pytest.raises(psycopg.Error, match="composition feature row is immutable"):
            cur.execute("UPDATE nport_composition_features SET status='degraded' WHERE publication_id=%s", (features_id,))
        cur.execute("SELECT series_id,status FROM sec_current_nport_composition_features ORDER BY series_id")
        assert cur.fetchone() == ("AGE_180", "certified")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_composition_uses_only_v2_sidecar_fields_and_never_relabels_sector():
    ddl = (ROOT / "schemas" / "nport_composition_features.sql").read_text(encoding="utf-8").lower()
    for required in ("sec_nport_holdings_v2", "issuer_category", "payoff_profile", "identifier_availability", "reason_codes", "provenance"):
        assert required in ddl
    for prohibited in ("sec_w1_nport_real", "nport_raw_rows", "economic_sector", "sector", "cusip_enrichment", "rating", "ytm", "ytw", "oas", "z_spread", "duration"):
        assert prohibited not in ddl
    assert "from sec_nport_holdings_v2 h" in ddl
    assert "from sec_nport_holdings_v2_current h" not in ddl
    assert "sec_current_nport_composition_features" in ddl


def test_null_quality_coverages_have_explicit_reasons():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed(cur)
        _holding(cur, holdings_id, run_id, "ZERO", "ZERO", "2026-01-31", 0,
                 issuer="ZERO", category="Corporate", payoff="Long", cusip="000000000", nav=0)
        _publish_holdings(cur, holdings_id)
        _prepare_features(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (features_id,))
        cur.execute("SELECT status,reason_codes FROM nport_composition_features WHERE series_id='ZERO'")
        status, reasons = cur.fetchone()
        assert status == "insufficient" and reasons
        assert {"identifier_coverage_unavailable", "issuer_category_coverage_unavailable",
                "payoff_profile_coverage_unavailable", "issuer_coverage_unavailable"} <= set(reasons)
        cur.execute("""SELECT count(*) FROM nport_composition_features
                       WHERE status='insufficient' AND reason_codes='[]'::jsonb""")
        assert cur.fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_composition_rejects_wrong_product_lifecycle_and_covers_top_n_boundaries():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, holdings_id, features_id = _seed(cur)
        for value in range(1, 13):
            _holding(cur, holdings_id, run_id, f"TOP-{value:02d}", "TOP", "2026-01-31", value,
                     issuer=f"ISSUER-{value:02d}", category="Corporate", payoff="Long",
                     cusip=f"{value:09d}", nav=value)
        _publish_holdings(cur, holdings_id)
        _prepare_features(cur, features_id, run_id, package_id)
        cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (features_id,))
        cur.execute("""SELECT top_5_gross_market_value_share,top_10_gross_market_value_share
                       FROM nport_composition_features WHERE series_id='TOP'""")
        top_5, top_10 = cur.fetchone()
        assert float(top_5) == pytest.approx(50 / 78)
        assert float(top_10) == pytest.approx(75 / 78)

        wrong_builder_id, wrong_build_id, wrong_row_id = (uuid4() for _ in range(3))
        for version, publication_id in enumerate((wrong_builder_id, wrong_build_id, wrong_row_id), start=1):
            cur.execute("""INSERT INTO sec_derived_publications
                (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
                VALUES(%s,'wrong_product',%s,%s,%s,%s)""",
                (publication_id, version, run_id, package_id, f"{version + 3}" * 64))
        with pytest.raises(psycopg.Error, match="prepared composition publication"):
            cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (wrong_builder_id,))
        with pytest.raises(psycopg.Error, match="prepared composition publication"):
            cur.execute("""INSERT INTO nport_composition_feature_builds
                (publication_id,source_holdings_publication_id,as_of_date) VALUES(%s,%s,'2026-06-30')""",
                (wrong_build_id, holdings_id))
        with pytest.raises(psycopg.Error, match="prepared composition publication"):
            cur.execute("""INSERT INTO nport_composition_features
                (publication_id,source_holdings_publication_id,series_id,report_date,measured_at,status,
                 reason_codes,provenance,coverage,position_count,report_age_days)
                VALUES(%s,%s,'WRONG','2026-01-31','2026-06-30','unavailable','[]','{}','{}',0,150)""",
                (wrong_row_id, holdings_id))
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_upgrade_from_legacy_security_hhi_fails_closed_and_serves_only_corrected_revision():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema = f"nport_composition_upgrade_{uuid4().hex}"
        run_id, package_id, holdings_id, legacy_current_id, legacy_prepared_id, corrected_id = (uuid4() for _ in range(6))
        cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
        cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
        cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
        cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
        for name in ("sec_derived_publications.sql", "nport_holdings_v2.sql"):
            cur.execute((ROOT / "schemas" / name).read_text(encoding="utf-8"))
        cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
        cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
        cur.execute("""INSERT INTO sec_derived_publications
            (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
            VALUES(%s,'sec_nport_holdings_v2',1,%s,%s,%s)""", (holdings_id, run_id, package_id, "a" * 64))
        _holding(cur, holdings_id, run_id, "OLD-1", "UPGRADE", "2026-01-31", 60,
                 issuer="SAME", category="Corporate", payoff="Long", lei="LEI-SAME", nav=6)
        _holding(cur, holdings_id, run_id, "OLD-2", "UPGRADE", "2026-01-31", 40,
                 issuer="SAME", category="Corporate", payoff="Long", lei="LEI-SAME", nav=4)
        _publish_holdings(cur, holdings_id)

        # Exact pre-fix summary/build table shape: no security-identity coverage
        # and no build methodology revision.
        cur.execute("""CREATE TABLE nport_composition_features (
            publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
            source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
            methodology_version text NOT NULL DEFAULT 'nport_composition_features_v1',
            series_id text NOT NULL, report_date date NOT NULL, measured_at date NOT NULL,
            status text NOT NULL CHECK (status IN ('certified','degraded','insufficient','unavailable')),
            reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb, provenance jsonb NOT NULL, coverage jsonb NOT NULL,
            position_count integer NOT NULL CHECK (position_count >= 0),
            unknown_market_value_position_count integer NOT NULL DEFAULT 0 CHECK (unknown_market_value_position_count >= 0),
            unknown_nav_position_count integer NOT NULL DEFAULT 0 CHECK (unknown_nav_position_count >= 0),
            signed_market_value numeric, gross_market_value numeric, signed_nav_pct numeric, gross_nav_pct numeric,
            signed_nav_residual_pct numeric, gross_nav_residual_pct numeric,
            identifier_market_value_coverage numeric CHECK (identifier_market_value_coverage IS NULL OR identifier_market_value_coverage BETWEEN 0 AND 1),
            issuer_category_market_value_coverage numeric CHECK (issuer_category_market_value_coverage IS NULL OR issuer_category_market_value_coverage BETWEEN 0 AND 1),
            payoff_profile_market_value_coverage numeric CHECK (payoff_profile_market_value_coverage IS NULL OR payoff_profile_market_value_coverage BETWEEN 0 AND 1),
            issuer_market_value_coverage numeric CHECK (issuer_market_value_coverage IS NULL OR issuer_market_value_coverage BETWEEN 0 AND 1),
            top_5_gross_market_value_share numeric CHECK (top_5_gross_market_value_share IS NULL OR top_5_gross_market_value_share BETWEEN 0 AND 1),
            top_10_gross_market_value_share numeric CHECK (top_10_gross_market_value_share IS NULL OR top_10_gross_market_value_share BETWEEN 0 AND 1),
            issuer_hhi numeric CHECK (issuer_hhi IS NULL OR issuer_hhi BETWEEN 0 AND 1), issuer_effective_position_count numeric,
            security_hhi numeric CHECK (security_hhi IS NULL OR security_hhi BETWEEN 0 AND 1), security_effective_position_count numeric,
            report_age_days integer NOT NULL CHECK (report_age_days >= 0), created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (publication_id,series_id,report_date), CHECK (methodology_version='nport_composition_features_v1'),
            CHECK (jsonb_typeof(reason_codes)='array'), CHECK (jsonb_typeof(provenance)='object'), CHECK (jsonb_typeof(coverage)='object'))""")
        cur.execute("""CREATE TABLE nport_composition_feature_builds (
            publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
            source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
            as_of_date date NOT NULL, created_at timestamptz NOT NULL DEFAULT now())""")
        for version, publication_id, fingerprint in (
            (1, legacy_current_id, "b" * 64), (2, legacy_prepared_id, "c" * 64)
        ):
            cur.execute("""INSERT INTO sec_derived_publications
                (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
                VALUES(%s,'nport_composition_features_v1',%s,%s,%s,%s)""",
                (publication_id, version, run_id, package_id, fingerprint))
            cur.execute("INSERT INTO nport_composition_feature_builds VALUES(%s,%s,'2026-06-30',now())",
                        (publication_id, holdings_id))
            cur.execute("""INSERT INTO nport_composition_features
                (publication_id,source_holdings_publication_id,series_id,report_date,measured_at,status,
                 reason_codes,provenance,coverage,position_count,security_hhi,security_effective_position_count,report_age_days)
                VALUES(%s,%s,'UPGRADE','2026-01-31','2026-06-30','certified','[]','{}','{}',2,1,1,150)""",
                (publication_id, holdings_id))
        cur.execute("SELECT sec_validate_derived_publication(%s)", (legacy_current_id,))
        cur.execute("SELECT sec_set_current_derived_publication('nport_composition_features_v1',%s)", (legacy_current_id,))

        ddl = (ROOT / "schemas" / "nport_composition_features.sql").read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)
        cur.execute("""SELECT column_name FROM information_schema.columns
            WHERE table_schema=%s AND table_name='nport_composition_features'
              AND column_name='security_identity_market_value_coverage'""", (schema,))
        assert cur.fetchone() == ("security_identity_market_value_coverage",)
        cur.execute("""SELECT count(*) FROM pg_constraint
            WHERE conrelid='nport_composition_features'::regclass
              AND conname='nport_composition_features_security_identity_coverage_check'""")
        assert cur.fetchone() == (1,)
        cur.execute("SELECT methodology_revision FROM nport_composition_feature_builds WHERE publication_id=%s", (legacy_current_id,))
        assert cur.fetchone() == (None,)
        cur.execute("SELECT count(*) FROM sec_current_nport_composition_features")
        assert cur.fetchone() == (0,)
        cur.execute("SELECT count(*) FROM sec_current_nport_composition_dimension_features")
        assert cur.fetchone() == (0,)
        with pytest.raises(psycopg.Error, match="methodology revision"):
            cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (legacy_prepared_id,))
        with pytest.raises(psycopg.Error, match="methodology revision"):
            cur.execute("""INSERT INTO nport_composition_features
                (publication_id,source_holdings_publication_id,series_id,report_date,measured_at,status,
                 reason_codes,provenance,coverage,position_count,report_age_days)
                VALUES(%s,%s,'LEGACY-DIRECT','2026-01-31','2026-06-30','unavailable','[]','{}','{}',0,150)""",
                (legacy_prepared_id, holdings_id))
        with pytest.raises(psycopg.Error, match="methodology revision"):
            cur.execute("""INSERT INTO nport_composition_dimension_features
                (publication_id,series_id,report_date,dimension_type,dimension_key,position_count)
                VALUES(%s,'UPGRADE','2026-01-31','issuer_category','legacy-mutation',0)""",
                (legacy_prepared_id,))
        cur.execute("SELECT methodology_revision,security_hhi FROM nport_composition_feature_builds b JOIN nport_composition_features f USING(publication_id) WHERE b.publication_id=%s", (legacy_prepared_id,))
        assert cur.fetchone() == (None, 1)

        cur.execute("""INSERT INTO sec_derived_publications
            (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
            VALUES(%s,'nport_composition_features_v1',3,%s,%s,%s)""",
            (corrected_id, run_id, package_id, "d" * 64))
        cur.execute("SELECT build_nport_composition_features(%s,'2026-06-30')", (corrected_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT sec_validate_derived_publication(%s)", (corrected_id,))
        cur.execute("SELECT sec_set_current_derived_publication('nport_composition_features_v1',%s)", (corrected_id,))
        cur.execute("""SELECT f.series_id,f.security_hhi,b.methodology_revision
            FROM sec_current_nport_composition_features f
            JOIN nport_composition_feature_builds b USING(publication_id)""")
        series_id, hhi, revision = cur.fetchone()
        assert series_id == "UPGRADE" and float(hhi) == pytest.approx(0.52)
        assert revision == "security_identity_v2"
        cur.execute("SELECT count(*) FROM sec_current_nport_composition_dimension_features")
        assert cur.fetchone() == (3,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

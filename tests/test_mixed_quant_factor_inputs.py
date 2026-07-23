"""Tests for the mixed_quant_v1 factor-input publication (Task 3, Step 1).

Covers the three factor inputs the app adapters consume:

* canonical direct-security linkage (fund holdings -> security identity),
* fund/equity class exposures (return-estimated) preserving measurement type,
  coverage and IPCA fit/version evidence, distinct from observed look-through,
* named bond factor inputs (curve, duration, credit, inflation, liquidity)
  published ONLY where observed, with absence declared as a coverage state.

Pure-contract tests run everywhere. Database tests run against an isolated
Postgres schema (never a production DSN); set SEC_TEST_DATABASE_URL, e.g.
"host=127.0.0.1 port=65431 dbname=postgres user=postgres".
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import os
from uuid import uuid4

import pytest

from src.quant_data import contracts

psycopg = pytest.importorskip("psycopg")
from psycopg.types.json import Jsonb  # noqa: E402

from src.quant_data import publication as pub  # noqa: E402
from src.workers import mixed_quant_publication as worker  # noqa: E402

AS_OF = date(2024, 3, 31)
LINEAGE = {"source": "nport", "accession": "0001-24-000001"}
OBS_AT = datetime(2024, 4, 1, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- #
# Pure contract tests (no database).
# --------------------------------------------------------------------------- #

def test_named_bond_factor_vocabulary_is_the_fixed_five():
    assert contracts.NAMED_BOND_FACTORS == ("credit", "curve", "duration", "inflation", "liquidity")


def test_validate_bond_factor_row_rejects_unknown_factor():
    with pytest.raises(contracts.ContractError):
        contracts.validate_bond_factor_row(
            {"factor": "yield", "value": 1.0, "method": "observed", "source_lineage": LINEAGE}
        )


def test_validate_bond_factor_row_rejects_non_finite_value():
    with pytest.raises(contracts.ContractError):
        contracts.validate_bond_factor_row(
            {"factor": "duration", "value": float("nan"), "method": "observed", "source_lineage": LINEAGE}
        )


def test_validate_bond_factor_row_requires_lineage():
    with pytest.raises(contracts.ContractError):
        contracts.validate_bond_factor_row(
            {"factor": "duration", "value": 4.5, "method": "observed", "source_lineage": {}}
        )


def test_validate_class_factor_row_rejects_unknown_quality_status():
    with pytest.raises(contracts.ContractError):
        contracts.validate_class_factor_row(
            {"factor": "rates", "value": 0.8, "method": "ols_hac",
             "measurement_type": "estimated", "quality_status": "bogus",
             "source_lineage": LINEAGE}
        )


def test_validate_class_factor_row_rejects_unknown_measurement_type():
    with pytest.raises(contracts.ContractError):
        contracts.validate_class_factor_row(
            {"factor": "rates", "value": 0.8, "method": "ols_hac",
             "measurement_type": "guessed", "quality_status": "certified",
             "source_lineage": LINEAGE}
        )


def test_validate_class_factor_row_accepts_governed_row():
    row = contracts.validate_class_factor_row(
        {"factor": "rates", "value": 0.8, "method": "ols_hac",
         "measurement_type": "estimated", "quality_status": "certified",
         "quality_flags": ["stale_or_smoothed_nav_excluded"],
         "evidence": {"model_id": "m", "model_version": "v1"},
         "source_lineage": LINEAGE}
    )
    assert row["quality_status"] == "certified"
    assert row["quality_flags"] == ["stale_or_smoothed_nav_excluded"]


# --------------------------------------------------------------------------- #
# Database tests.
# --------------------------------------------------------------------------- #

_BASE_DSN = os.getenv("SEC_TEST_DATABASE_URL")
db = pytest.mark.skipif(not _BASE_DSN, reason="SEC_TEST_DATABASE_URL not set")


@pytest.fixture()
def env():
    schema = f"mixed_quant_{uuid4().hex}"
    with psycopg.connect(_BASE_DSN, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
        conn.execute(f'SET search_path TO "{schema}"')
        pub.install_schema(conn)
        dsn = f"{_BASE_DSN} options=-csearch_path={schema}"
        try:
            yield conn, dsn, schema
        finally:
            conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


def _identity(conn, *, alias_type="ticker", alias_value="AAA", instrument_type="fund",
              deterministic_key="series:S1", valid_from=AS_OF, valid_to=None,
              security_id=None, currency="USD"):
    lineage = dict(LINEAGE)
    if instrument_type == "fund" and deterministic_key and deterministic_key.startswith("series:"):
        lineage["series_id"] = deterministic_key.removeprefix("series:")
    conn.execute(
        "INSERT INTO mixed_quant_identity_observation "
        "(observation_id, as_of, instrument_type, currency, issuer_id, security_id, "
        " alias_type, alias_value, deterministic_key, observed_at, valid_from, valid_to, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, instrument_type, currency, None, security_id, alias_type,
         alias_value, deterministic_key, OBS_AT, valid_from, valid_to, Jsonb(lineage)),
    )


def _holdings(conn, *, series_id="S1", holdings=None):
    holdings = holdings if holdings is not None else [
        {"cusip": "037833100", "issuer_name": "APPLE INC", "asset_class": "EC",
         "sector": "Technology", "currency": "USD", "pct_of_nav": 60.0, "payoff_profile": "Long"},
        {"cusip": "594918104", "issuer_name": "MICROSOFT", "asset_class": "EC",
         "sector": "Technology", "currency": "USD", "pct_of_nav": 40.0, "payoff_profile": "Long"},
    ]
    conn.execute(
        "INSERT INTO mixed_quant_holding_observation "
        "(observation_id, as_of, series_id, report_date, holdings, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, series_id, AS_OF, Jsonb(holdings), Jsonb(LINEAGE)),
    )


def _class_factor(conn, *, alias_type="ticker", alias_value="AAA", factor="rates",
                  value=0.8, method="ols_hac", measurement_type="estimated",
                  quality_status="certified", quality_flags=None, evidence=None):
    conn.execute(
        "INSERT INTO mixed_quant_class_factor_observation "
        "(observation_id, as_of, alias_type, alias_value, factor, value, method, "
        " measurement_type, quality_status, quality_flags, evidence, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, alias_type, alias_value, factor, value, method, measurement_type,
         quality_status, Jsonb(quality_flags or []), Jsonb(evidence or {}), OBS_AT, Jsonb(LINEAGE)),
    )


def _bond_factor(conn, *, alias_type="cusip", alias_value="BND000001", factor="duration",
                 value=5.2, method="observed"):
    conn.execute(
        "INSERT INTO mixed_quant_bond_factor_observation "
        "(observation_id, as_of, alias_type, alias_value, factor, value, method, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, alias_type, alias_value, factor, value, method, OBS_AT, Jsonb(LINEAGE)),
    )


def _exposures(conn):
    return {
        (r[0], r[1]): {"value": r[2], "coverage": r[3]}
        for r in conn.execute(
            "SELECT instrument_id::text, factor, value, coverage FROM quant_exposure_v1"
        ).fetchall()
    }


@db
def test_direct_security_linkage_resolves_fund_holdings_to_security_identity(env):
    conn, dsn, _ = env
    # Fund S1 holds Apple; only Apple is minted as a security identity.
    _identity(conn, alias_type="ticker", alias_value="AAA", deterministic_key="series:S1")
    _identity(conn, alias_type="cusip", alias_value="037833100", instrument_type="equity",
              deterministic_key="sec:apple", security_id="APPLE")
    _holdings(conn)
    worker.run(dsn)

    fund_id = str(contracts.mint_instrument_id("series:S1"))
    sec_id = str(contracts.mint_instrument_id("sec:apple"))
    rows = conn.execute(
        "SELECT instrument_id::text, security_instrument_id::text, weight_pct, coverage, source_lineage "
        "FROM quant_holding_link_v1"
    ).fetchall()
    assert len(rows) == 1
    inst, sec, weight, coverage, lineage = rows[0]
    assert inst == fund_id and sec == sec_id
    assert weight == pytest.approx(60.0)
    assert lineage  # non-empty
    # Microsoft has no minted identity -> no link, no fabrication.
    assert coverage.get("resolution") == "direct_security"


@db
def test_linkage_absent_when_holding_alias_collides(env):
    conn, dsn, _ = env
    _identity(conn, alias_type="ticker", alias_value="AAA", deterministic_key="series:S1")
    # Same CUSIP minted twice with no deterministic evidence -> collision.
    _identity(conn, alias_type="cusip", alias_value="037833100", instrument_type="bond",
              deterministic_key=None, security_id="SEC-A")
    _identity(conn, alias_type="cusip", alias_value="037833100", instrument_type="bond",
              deterministic_key=None, security_id="SEC-B")
    _holdings(conn)
    worker.run(dsn)
    n_links = conn.execute("SELECT count(*) FROM quant_holding_link_v1").fetchone()[0]
    assert n_links == 0


@db
def test_fund_class_exposures_preserve_measurement_type_and_coverage(env):
    conn, dsn, _ = env
    _identity(conn, alias_type="ticker", alias_value="AAA", deterministic_key="series:S1")
    _holdings(conn)
    _class_factor(conn, factor="rates", value=0.8, method="ols_hac",
                  quality_flags=["stale_or_smoothed_nav_excluded"])
    _class_factor(conn, factor="credit_spread", value=-0.3, method="ols_hac")
    worker.run(dsn)

    fund_id = str(contracts.mint_instrument_id("series:S1"))
    exposures = _exposures(conn)
    # Return-estimated class factor present with its coverage.
    rates = exposures[(fund_id, "class_factor:rates")]
    assert rates["value"] == pytest.approx(0.8)
    assert rates["coverage"]["measurement_type"] == "estimated"
    assert rates["coverage"]["quality_status"] == "certified"
    assert rates["coverage"]["quality_flags"] == ["stale_or_smoothed_nav_excluded"]
    # Observed look-through exposures are tagged distinctly.
    lookthrough = [c for (i, f), c in exposures.items()
                   if i == fund_id and f.startswith("issuer:")]
    assert lookthrough and all(c["coverage"]["measurement_type"] == "observed" for c in lookthrough)


@db
def test_equity_class_exposures_keep_ipca_fit_and_version_evidence(env):
    conn, dsn, _ = env
    _identity(conn, alias_type="ticker", alias_value="EEE", instrument_type="equity",
              deterministic_key="sec:eq1", security_id="EQ1")
    _class_factor(conn, alias_type="ticker", alias_value="EEE", factor="latent_factor_1",
                  value=1.2, method="instrumented_pca",
                  evidence={"model_id": "ipca-x", "model_version": "v3",
                            "oos_r2": 0.41, "engine": "instrumented_pca"})
    worker.run(dsn)
    eq_id = str(contracts.mint_instrument_id("sec:eq1"))
    exposures = _exposures(conn)
    latent = exposures[(eq_id, "class_factor:latent_factor_1")]
    assert latent["coverage"]["measurement_type"] == "estimated"
    ev = latent["coverage"]["evidence"]
    assert ev["model_id"] == "ipca-x" and ev["model_version"] == "v3"
    assert ev["engine"] == "instrumented_pca"


@db
def test_bond_named_factors_published_only_when_observed(env):
    conn, dsn, _ = env
    _identity(conn, alias_type="cusip", alias_value="BND000001", instrument_type="bond",
              deterministic_key="sec:bnd1", security_id="BND1")
    _bond_factor(conn, factor="duration", value=5.2)
    _bond_factor(conn, factor="credit", value=120.0)
    worker.run(dsn)

    bond_id = str(contracts.mint_instrument_id("sec:bnd1"))
    exposures = _exposures(conn)
    assert (bond_id, "bond_factor:duration") in exposures
    assert (bond_id, "bond_factor:credit") in exposures
    assert exposures[(bond_id, "bond_factor:duration")]["value"] == pytest.approx(5.2)
    # Unobserved factors are NOT fabricated.
    for absent in ("curve", "inflation", "liquidity"):
        assert (bond_id, f"bond_factor:{absent}") not in exposures
    # Absence is declared as an instrument coverage state.
    coverage = conn.execute(
        "SELECT coverage FROM quant_instrument_v1 WHERE instrument_id=%s", (bond_id,)
    ).fetchone()[0]
    assert coverage["bond_factor_coverage"] == {
        "credit": "observed", "curve": "absent", "duration": "observed",
        "inflation": "absent", "liquidity": "absent",
    }


@db
def test_bond_without_observations_declares_all_absent(env):
    conn, dsn, _ = env
    _identity(conn, alias_type="cusip", alias_value="BND000002", instrument_type="bond",
              deterministic_key="sec:bnd2", security_id="BND2")
    worker.run(dsn)
    bond_id = str(contracts.mint_instrument_id("sec:bnd2"))
    n_bond_exp = conn.execute(
        "SELECT count(*) FROM quant_exposure_v1 WHERE instrument_id=%s AND factor LIKE 'bond_factor:%%'",
        (bond_id,),
    ).fetchone()[0]
    assert n_bond_exp == 0
    coverage = conn.execute(
        "SELECT coverage FROM quant_instrument_v1 WHERE instrument_id=%s", (bond_id,)
    ).fetchone()[0]
    assert set(coverage["bond_factor_coverage"].values()) == {"absent"}


@db
def test_factor_inputs_are_idempotent_and_restartable(env):
    conn, dsn, _ = env
    _identity(conn, alias_type="ticker", alias_value="AAA", deterministic_key="series:S1")
    _identity(conn, alias_type="cusip", alias_value="037833100", instrument_type="equity",
              deterministic_key="sec:apple", security_id="APPLE")
    _identity(conn, alias_type="cusip", alias_value="BND000001", instrument_type="bond",
              deterministic_key="sec:bnd1", security_id="BND1")
    _holdings(conn)
    _class_factor(conn, factor="rates", value=0.8, method="ols_hac")
    _bond_factor(conn, factor="duration", value=5.2)
    first = worker.run(dsn)
    second = worker.run(dsn)
    assert first["publication_id"] == second["publication_id"]
    assert first["links"] == second["links"] == 1
    assert first["exposures"] == second["exposures"]


@db
def test_every_factor_input_resolves_to_observation_and_publication(env):
    conn, dsn, _ = env
    _identity(conn, alias_type="ticker", alias_value="AAA", deterministic_key="series:S1")
    _identity(conn, alias_type="cusip", alias_value="037833100", instrument_type="equity",
              deterministic_key="sec:apple", security_id="APPLE")
    _identity(conn, alias_type="cusip", alias_value="BND000001", instrument_type="bond",
              deterministic_key="sec:bnd1", security_id="BND1")
    _holdings(conn)
    _class_factor(conn, factor="rates", value=0.8, method="ols_hac")
    _bond_factor(conn, factor="duration", value=5.2)
    pub_id = worker.run(dsn)["publication_id"]
    for table in ("quant_holding_link_v1", "quant_exposure_v1"):
        missing_pub = conn.execute(
            f"SELECT count(*) FROM {table} t LEFT JOIN quant_publication_v1 p USING (publication_id) "
            f"WHERE p.publication_id IS NULL AND t.publication_id=%s", (pub_id,),
        ).fetchone()[0]
        assert missing_pub == 0
        empty_lineage = conn.execute(
            f"SELECT count(*) FROM {table} WHERE publication_id=%s AND source_lineage='{{}}'::jsonb",
            (pub_id,),
        ).fetchone()[0]
        assert empty_lineage == 0

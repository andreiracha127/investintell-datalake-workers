"""One compact complete mixed_quant_v1 publication fixture (Task 6, Step 1).

Seeds a single publication that exercises every downstream consumer path in one
build: a fund (holdings + return + income + class factor), a single-name equity
(class factor with IPCA evidence, resolves the fund's direct holding), a direct
bond with only SOME named factors observed (the rest declared absent — the
"missing bond factor" case), a currency-mismatched bond (EUR, so the app risk
model must exclude it rather than convert at a fabricated rate) and a stale-NAV
fund (its only return is long before the as-of). Asserts the resulting frozen
publication is complete and carries each characteristic.

Runs against an isolated Postgres schema (never a production DSN); set
SEC_TEST_DATABASE_URL, e.g. "host=127.0.0.1 port=65431 dbname=postgres user=postgres".
No production writes.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import os
from uuid import uuid4

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.types.json import Jsonb  # noqa: E402

from src.quant_data import contracts, publication as pub  # noqa: E402
from src.workers import mixed_quant_publication as worker  # noqa: E402

_BASE_DSN = os.getenv("SEC_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not _BASE_DSN, reason="SEC_TEST_DATABASE_URL not set")

AS_OF = date(2024, 3, 31)
STALE = date(2021, 1, 31)  # long before the as-of → stale NAV downstream
LINEAGE = {"source": "nport", "accession": "0001-24-000042"}
OBS_AT = datetime(2024, 4, 1, tzinfo=timezone.utc)


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


def _identity(conn, *, alias_type, alias_value, instrument_type="fund",
              deterministic_key, valid_from=AS_OF, valid_to=None,
              security_id=None, currency="USD"):
    conn.execute(
        "INSERT INTO mixed_quant_identity_observation "
        "(observation_id, as_of, instrument_type, currency, issuer_id, security_id, "
        " alias_type, alias_value, deterministic_key, observed_at, valid_from, valid_to, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, instrument_type, currency, None, security_id, alias_type,
         alias_value, deterministic_key, OBS_AT, valid_from, valid_to, Jsonb(LINEAGE)),
    )


def _return(conn, *, alias_type="ticker", alias_value, period_end=AS_OF, ret=0.021):
    conn.execute(
        "INSERT INTO mixed_quant_return_observation "
        "(observation_id, as_of, alias_type, alias_value, period_end, frequency, total_return, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,'monthly',%s,%s,%s)",
        (uuid4(), AS_OF, alias_type, alias_value, period_end, ret, OBS_AT, Jsonb(LINEAGE)),
    )


def _income(conn, *, alias_type="ticker", alias_value, event_date=date(2024, 3, 15),
            amount="0.35", event_type="dividend"):
    conn.execute(
        "INSERT INTO mixed_quant_income_observation "
        "(observation_id, as_of, alias_type, alias_value, event_date, cash_amount, currency, event_type, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,'USD',%s,%s,%s)",
        (uuid4(), AS_OF, alias_type, alias_value, event_date, amount, event_type, OBS_AT, Jsonb(LINEAGE)),
    )


def _holdings(conn, *, series_id, holdings):
    conn.execute(
        "INSERT INTO mixed_quant_holding_observation "
        "(observation_id, as_of, series_id, report_date, holdings, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, series_id, AS_OF, Jsonb(holdings), Jsonb(LINEAGE)),
    )


def _class_factor(conn, *, alias_type="ticker", alias_value, factor, value,
                  method="ols_hac", measurement_type="estimated",
                  quality_status="certified", quality_flags=None, evidence=None):
    conn.execute(
        "INSERT INTO mixed_quant_class_factor_observation "
        "(observation_id, as_of, alias_type, alias_value, factor, value, method, "
        " measurement_type, quality_status, quality_flags, evidence, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, alias_type, alias_value, factor, value, method, measurement_type,
         quality_status, Jsonb(quality_flags or []), Jsonb(evidence or {}), OBS_AT, Jsonb(LINEAGE)),
    )


def _bond_factor(conn, *, alias_type="cusip", alias_value, factor, value, method="observed"):
    conn.execute(
        "INSERT INTO mixed_quant_bond_factor_observation "
        "(observation_id, as_of, alias_type, alias_value, factor, value, method, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, alias_type, alias_value, factor, value, method, OBS_AT, Jsonb(LINEAGE)),
    )


def _seed_complete_fixture(conn):
    """One publication: fund + equity + bond + currency-mismatch + stale-NAV."""
    # Fund S1 — holds Apple; fresh return, income, one class factor.
    _identity(conn, alias_type="ticker", alias_value="AAA", deterministic_key="series:S1")
    _return(conn, alias_value="AAA", period_end=AS_OF)
    _income(conn, alias_value="AAA")
    _class_factor(conn, alias_value="AAA", factor="rates", value=0.8,
                  quality_flags=["stale_or_smoothed_nav_excluded"])
    _holdings(conn, series_id="S1", holdings=[
        {"cusip": "037833100", "issuer_name": "APPLE INC", "asset_class": "EC",
         "sector": "Technology", "currency": "USD", "pct_of_nav": 60.0, "payoff_profile": "Long"},
        {"cusip": "594918104", "issuer_name": "MICROSOFT", "asset_class": "EC",
         "sector": "Technology", "currency": "USD", "pct_of_nav": 40.0, "payoff_profile": "Long"},
    ])

    # Equity — the direct-security identity behind the fund's Apple holding, with
    # IPCA fit/version evidence on its return-estimated class factor.
    _identity(conn, alias_type="cusip", alias_value="037833100", instrument_type="equity",
              deterministic_key="sec:apple", security_id="APPLE")
    _class_factor(conn, alias_type="cusip", alias_value="037833100", factor="latent_factor_1",
                  value=1.1, method="instrumented_pca",
                  evidence={"model_id": "ipca-x", "model_version": "v3",
                            "oos_r2": 0.44, "engine": "instrumented_pca"})

    # Bond BND1 (USD) — only duration + credit observed; curve/inflation/liquidity
    # remain the "missing bond factor" (declared absent, never fabricated).
    _identity(conn, alias_type="cusip", alias_value="BND000001", instrument_type="bond",
              deterministic_key="sec:bnd1", security_id="BND1")
    _bond_factor(conn, alias_value="BND000001", factor="duration", value=5.2)
    _bond_factor(conn, alias_value="BND000001", factor="credit", value=120.0)

    # Bond BND2 — a currency mismatch (EUR); the app risk model must exclude it.
    _identity(conn, alias_type="cusip", alias_value="BND000002", instrument_type="bond",
              deterministic_key="sec:bnd2", security_id="BND2", currency="EUR")
    _bond_factor(conn, alias_value="BND000002", factor="duration", value=7.0)

    # Fund S2 — stale NAV only (its single return is long before the as-of).
    _identity(conn, alias_type="ticker", alias_value="OLD", deterministic_key="series:S2")
    _return(conn, alias_value="OLD", period_end=STALE)


def test_complete_fixture_publishes_every_characteristic(env):
    conn, dsn, _ = env
    _seed_complete_fixture(conn)
    result = worker.run(dsn)
    assert result["status"] == "ready"
    assert result["product"] == "mixed_quant_v1"

    fund1 = str(contracts.mint_instrument_id("series:S1"))
    fund2 = str(contracts.mint_instrument_id("series:S2"))
    equity = str(contracts.mint_instrument_id("sec:apple"))
    bond1 = str(contracts.mint_instrument_id("sec:bnd1"))
    bond2 = str(contracts.mint_instrument_id("sec:bnd2"))

    # All five instruments published under exactly one publication.
    types = dict(conn.execute(
        "SELECT instrument_id::text, instrument_type FROM quant_instrument_v1"
    ).fetchall())
    assert types[fund1] == "fund" and types[fund2] == "fund"
    assert types[equity] == "equity"
    assert types[bond1] == "bond" and types[bond2] == "bond"
    assert conn.execute("SELECT count(*) FROM quant_publication_v1").fetchone()[0] == 1

    # Currency mismatch is carried verbatim (never coerced to the base currency).
    currencies = dict(conn.execute(
        "SELECT instrument_id::text, currency FROM quant_instrument_v1"
    ).fetchall())
    assert currencies[bond2] == "EUR"
    assert currencies[fund1] == "USD"

    # Stale NAV: fund S2's only return is the old period_end.
    s2_returns = conn.execute(
        "SELECT period_end FROM quant_return_v1 WHERE instrument_id=%s", (fund2,)
    ).fetchall()
    assert s2_returns == [(STALE,)]

    # Missing bond factor: BND1 declares curve/inflation/liquidity absent.
    cov1 = conn.execute(
        "SELECT coverage FROM quant_instrument_v1 WHERE instrument_id=%s", (bond1,)
    ).fetchone()[0]
    assert cov1["bond_factor_coverage"] == {
        "credit": "observed", "curve": "absent", "duration": "observed",
        "inflation": "absent", "liquidity": "absent",
    }
    # Absent factors are not fabricated as exposure rows.
    for absent in ("curve", "inflation", "liquidity"):
        n = conn.execute(
            "SELECT count(*) FROM quant_exposure_v1 WHERE instrument_id=%s AND factor=%s",
            (bond1, f"bond_factor:{absent}"),
        ).fetchone()[0]
        assert n == 0

    # Return-estimated class factors and observed look-through stay distinct.
    exposures = {
        (r[0], r[1]): r[2]
        for r in conn.execute(
            "SELECT instrument_id::text, factor, coverage FROM quant_exposure_v1"
        ).fetchall()
    }
    assert exposures[(fund1, "class_factor:rates")]["measurement_type"] == "estimated"
    assert exposures[(equity, "class_factor:latent_factor_1")]["evidence"]["model_id"] == "ipca-x"
    assert any(
        f.startswith("issuer:") and exposures[(i, f)]["measurement_type"] == "observed"
        for (i, f) in exposures if i == fund1
    )

    # Direct-security linkage resolves the fund's Apple holding to the equity id.
    link = conn.execute(
        "SELECT security_instrument_id::text FROM quant_holding_link_v1 WHERE instrument_id=%s",
        (fund1,),
    ).fetchall()
    assert (equity,) in link

    # Income published for the fund; no inferred-yield columns exist at all.
    assert conn.execute(
        "SELECT count(*) FROM quant_income_v1 WHERE instrument_id=%s", (fund1,)
    ).fetchone()[0] == 1
    cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='quant_income_v1'"
    ).fetchall()}
    assert cols.isdisjoint(contracts.INFERRED_YIELD_FIELDS)


def test_complete_fixture_is_idempotent(env):
    conn, dsn, _ = env
    _seed_complete_fixture(conn)
    first = worker.run(dsn)
    second = worker.run(dsn)
    assert first["publication_id"] == second["publication_id"]
    for key in ("instruments", "returns", "exposures", "income", "links"):
        assert first[key] == second[key]

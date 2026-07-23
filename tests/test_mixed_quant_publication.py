"""Database tests for the mixed_quant_v1 publication worker.

Runs against an isolated Postgres schema (never a production DSN). Set
SEC_TEST_DATABASE_URL, e.g. "host=127.0.0.1 port=65431 dbname=postgres user=postgres".
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
LINEAGE = {"source": "nport", "accession": "0001-24-000001"}


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
         alias_value, deterministic_key, datetime(2024, 4, 1, tzinfo=timezone.utc),
         valid_from, valid_to, Jsonb(lineage)),
    )


def _return(conn, *, alias_type="ticker", alias_value="AAA", period_end=AS_OF, ret=0.021):
    conn.execute(
        "INSERT INTO mixed_quant_return_observation "
        "(observation_id, as_of, alias_type, alias_value, period_end, frequency, total_return, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,'monthly',%s,%s,%s)",
        (uuid4(), AS_OF, alias_type, alias_value, period_end, ret,
         datetime(2024, 4, 1, tzinfo=timezone.utc), Jsonb(LINEAGE)),
    )


def _income(conn, *, alias_type="ticker", alias_value="AAA", event_date=date(2024, 3, 15),
            amount="0.35", event_type="dividend"):
    conn.execute(
        "INSERT INTO mixed_quant_income_observation "
        "(observation_id, as_of, alias_type, alias_value, event_date, cash_amount, currency, event_type, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,'USD',%s,%s,%s)",
        (uuid4(), AS_OF, alias_type, alias_value, event_date, amount, event_type,
         datetime(2024, 4, 1, tzinfo=timezone.utc), Jsonb(LINEAGE)),
    )


def _holdings(conn, *, series_id="S1"):
    holdings = [
        {"cusip": "037833100", "issuer_name": "APPLE INC", "asset_class": "DBT",
         "sector": "Technology", "currency": "USD", "pct_of_nav": 60.0, "payoff_profile": "Long"},
        {"cusip": "594918104", "issuer_name": "MICROSOFT", "asset_class": "DBT",
         "sector": "Technology", "currency": "USD", "pct_of_nav": 40.0, "payoff_profile": "Long"},
    ]
    conn.execute(
        "INSERT INTO mixed_quant_holding_observation "
        "(observation_id, as_of, series_id, report_date, holdings, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (uuid4(), AS_OF, series_id, AS_OF, Jsonb(holdings), Jsonb(LINEAGE)),
    )


def _seed_basic(conn):
    _identity(conn, alias_type="ticker", alias_value="AAA", deterministic_key="series:S1")
    _identity(conn, alias_type="cusip", alias_value="CUS-AAA", deterministic_key="series:S1")
    _return(conn)
    _income(conn)
    _holdings(conn)


# --------------------------------------------------------------------------- #

def test_build_is_idempotent_across_reruns(env):
    conn, dsn, _ = env
    _seed_basic(conn)
    first = worker.run(dsn)
    second = worker.run(dsn)
    assert first["status"] == "ready"
    assert first["publication_id"] == second["publication_id"]
    for key in ("instruments", "aliases", "returns", "exposures", "income"):
        assert first[key] == second[key]
    assert first["exposures"] > 0 and first["returns"] == 1 and first["income"] == 1
    n_pub = conn.execute("SELECT count(*) FROM quant_publication_v1").fetchone()[0]
    assert n_pub == 1


def test_restart_after_checkpoint_resumes_same_publication(env):
    conn, dsn, _ = env
    _seed_basic(conn)
    # Build only the first two stages, as if the worker crashed before exposures.
    pub_id = pub.open_publication(
        conn, product=pub.PRODUCT, as_of=AS_OF,
        code_revision=(os.getenv("CODE_REVISION") or worker._git_revision() or "dev"),
        config_version="v1", input_watermarks={},
    )
    resolved = worker._stage_identities(conn, pub_id, AS_OF)
    worker._stage_returns(conn, pub_id, AS_OF, worker._alias_index(resolved))
    assert pub.get_checkpoint(conn, pub_id, "exposures") is None
    # Resume through the worker: same publication, remaining stages filled.
    result = worker.run(dsn)
    assert result["publication_id"] == str(pub_id)
    assert result["status"] == "ready"
    assert result["exposures"] > 0 and result["income"] == 1
    assert pub.get_checkpoint(conn, pub_id, "exposures") is not None


def test_alias_change_produces_interval_history(env):
    conn, dsn, _ = env
    _identity(conn, alias_type="ticker", alias_value="OLD", valid_from=date(2020, 1, 1),
              deterministic_key="series:S1")
    _identity(conn, alias_type="ticker", alias_value="NEW", valid_from=date(2022, 6, 1),
              deterministic_key="series:S1")
    worker.run(dsn)
    rows = dict(conn.execute(
        "SELECT alias_value, valid_to FROM quant_instrument_alias_v1 WHERE alias_type='ticker'"
    ).fetchall())
    assert rows["OLD"] == date(2022, 6, 1)
    assert rows["NEW"] is None


def test_collision_preserved_as_separate_instruments(env):
    conn, dsn, _ = env
    # Same CUSIP, no deterministic evidence => two unresolved instruments.
    _identity(conn, alias_type="cusip", alias_value="DUP", instrument_type="bond",
              deterministic_key=None, security_id="SEC-A")
    _identity(conn, alias_type="cusip", alias_value="DUP", instrument_type="bond",
              deterministic_key=None, security_id="SEC-B")
    worker.run(dsn)
    n_inst = conn.execute("SELECT count(*) FROM quant_instrument_v1").fetchone()[0]
    n_alias = conn.execute(
        "SELECT count(*) FROM quant_instrument_alias_v1 WHERE alias_value='DUP'"
    ).fetchone()[0]
    assert n_inst == 2 and n_alias == 2


def test_inactive_isolation_blocks_writes_after_promotion(env):
    conn, dsn, _ = env
    _seed_basic(conn)
    result = worker.run(dsn)
    pub_id = result["publication_id"]
    pub.promote(conn, pub.PRODUCT, pub_id)
    assert str(pub.active_publication_id(conn, pub.PRODUCT)) == pub_id
    inst_id = conn.execute("SELECT instrument_id FROM quant_instrument_v1 LIMIT 1").fetchone()[0]
    with pytest.raises(psycopg.errors.RaiseException):
        pub.write_return(conn, pub_id, inst_id, period_end=AS_OF, frequency="monthly",
                         total_return=0.9, observed_at=datetime.now(timezone.utc), source_lineage=LINEAGE)


def test_pointer_promotion_is_atomic_and_supersedes_incumbent(env):
    conn, dsn, _ = env
    _seed_basic(conn)
    first = worker.run(dsn)["publication_id"]
    pub.promote(conn, pub.PRODUCT, first)
    # Amended build (new config version) -> distinct publication; promote swaps.
    os.environ["MIXED_QUANT_CONFIG_VERSION"] = "v2"
    try:
        second = worker.run(dsn)["publication_id"]
    finally:
        del os.environ["MIXED_QUANT_CONFIG_VERSION"]
    assert second != first
    pub.promote(conn, pub.PRODUCT, second)
    pointer_rows = conn.execute("SELECT count(*) FROM active_quant_publication_v1").fetchone()[0]
    assert pointer_rows == 1
    assert str(pub.active_publication_id(conn, pub.PRODUCT)) == second
    statuses = dict(conn.execute("SELECT publication_id::text, status FROM quant_publication_v1").fetchall())
    assert statuses[first] == "superseded"
    assert statuses[second] == "active"


def test_every_published_value_resolves_to_observation_and_publication(env):
    conn, dsn, _ = env
    _seed_basic(conn)
    pub_id = worker.run(dsn)["publication_id"]
    for table in ("quant_instrument_alias_v1", "quant_return_v1", "quant_exposure_v1", "quant_income_v1"):
        # every row resolves to its publication and carries non-empty lineage
        missing_pub = conn.execute(
            f"SELECT count(*) FROM {table} t LEFT JOIN quant_publication_v1 p "
            f"USING (publication_id) WHERE p.publication_id IS NULL AND t.publication_id=%s",
            (pub_id,),
        ).fetchone()[0]
        assert missing_pub == 0
        empty_lineage = conn.execute(
            f"SELECT count(*) FROM {table} WHERE publication_id=%s AND source_lineage='{{}}'::jsonb",
            (pub_id,),
        ).fetchone()[0]
        assert empty_lineage == 0
    # income carries no inferred-yield columns
    cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='quant_income_v1'"
    ).fetchall()}
    assert cols.isdisjoint(contracts.INFERRED_YIELD_FIELDS)


def test_write_income_rejects_inferred_yield(env):
    conn, _, _ = env
    pub_id = pub.open_publication(conn, product=pub.PRODUCT, as_of=AS_OF, code_revision="dev",
                                  config_version="probe", input_watermarks={})
    inst = contracts.resolve_identities([contracts.IdentityObservation(
        observation_id=uuid4(), instrument_type="bond", currency="USD", alias_type="cusip",
        alias_value="C1", valid_from=AS_OF, observed_at=datetime.now(timezone.utc),
        source_lineage=LINEAGE, deterministic_key="k1")])[0]
    pub.write_instrument(conn, pub_id, inst)
    with pytest.raises(contracts.ContractError):
        pub.write_income(conn, pub_id, inst.instrument_id, {
            "event_date": AS_OF, "cash_amount": "1", "currency": "USD",
            "event_type": "coupon", "ytm": 0.05, "source_lineage": LINEAGE})

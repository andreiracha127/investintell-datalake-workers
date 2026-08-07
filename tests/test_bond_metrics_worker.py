"""DB tests for the bond_metrics compute+persist worker (source projection).

Runs against the disposable Postgres at ``SEC_TEST_DATABASE_URL`` (PG 65431),
DSN-agnostic (keyword and URL forms). Proves, against the REAL publication
protocol:

  * happy path: a promoted ``bond_metric_v1`` build whose ``security_ytm`` is
    the yield the qualified price source itself delivered (PROJECTED, never
    recomputed from terms the universe does not carry);
  * per-security typed statuses (``no_eligible_price`` / ``terms_insufficient``)
    — never NaN, never a fabricated value, value present iff available;
  * no-look-ahead: a future observation never enters the build;
  * the sibling dark ladder (``no_source`` / ``no_securities`` /
    ``no_observations``) publishes NOTHING (chain dark_no_source semantics);
  * build-versioned publication: idempotent replay, input-change mints a new
    publication, ``daily_chain.rollback_pointer`` restores the prior current;
  * write guard: the published snapshot is immutable after validation;
  * schema enum carries EXACTLY the Wave-1 metrics (no OAS/z-spread/duration).

The Phase-10 qualification registry is deliberately ABSENT from every path
here: the write path no longer consults it (the activation ceremony is
retired), so a build must publish real values with no qualification row at all.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_price_fixtures import price_input  # noqa: E402

from src.bonds import daily_chain, price_observations, security_master  # noqa: E402
from src.bonds.security_master import NAMESPACE_BOND_SECURITY  # noqa: E402
from src.db import advisory_lock  # noqa: E402
from src.workers.bond_metrics import SERVED_METRICS  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL not set"
)

ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2025, 1, 1)

CUSIP_FIX = "BNDFIX001"  # fixed 10%, priced 96.23, source ytm 0.076
CUSIP_NOP = "BNDNOP002"  # sufficient terms, NO eligible price
CUSIP_NOY = "BNDNOY003"  # eligible price, source delivered NO yield
CUSIP_NCP = "BNDNCP004"  # no coupon rate published: current_yield insufficient

SEC_FIX = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{CUSIP_FIX}")
SEC_NOP = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{CUSIP_NOP}")
SEC_NOY = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{CUSIP_NOY}")
SEC_NCP = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{CUSIP_NCP}")

FIX_SCHEDULE = [{"date": "2025-07-01", "rate": 10.0}, {"date": "2026-01-01", "rate": 10.0}]


def base_dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def search_path_dsn(schema: str) -> str:
    base = base_dsn()
    if base.startswith("postgres"):
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}options=-c%20search_path%3D{schema}"
    return f"{base} options='-c search_path={schema}'"


def admin_connect() -> psycopg.Connection:
    return psycopg.connect(base_dsn(), autocommit=True)


def work_conn(schema: str) -> psycopg.Connection:
    conn = psycopg.connect(base_dsn())
    conn.execute(f'SET search_path TO "{schema}"')
    conn.commit()
    return conn


def new_env(admin: psycopg.Connection, *, seed_lineage: bool = True) -> tuple[str, UUID, UUID]:
    """Isolated schema + lineage + the sibling DDL the metrics worker consumes."""
    schema = f"bond_metrics_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    admin.execute(f'CREATE SCHEMA "{schema}"')
    if not seed_lineage:
        return schema, run_id, package_id
    with work_conn(schema) as conn:
        conn.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
        conn.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
        conn.execute(
            "CREATE VIEW sec_validated_raw_runs AS "
            "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
        )
        for ddl_name in (
            "sec_derived_publications.sql",
            "bond_security_v1.sql",
            "bond_price_observations_v1.sql",
            "bond_price_eligibility_v1.sql",
        ):
            conn.execute((ROOT / "schemas" / ddl_name).read_text(encoding="utf-8"))
        conn.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
        conn.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
        conn.commit()
    return schema, run_id, package_id


def observe_security(
    conn, run_id: UUID, *, cusip9: str, coupon_type: object, coupon_rate: object,
    maturity: object, day_count: object, coupon_schedule: object = None,
    call_schedule: object = None, as_of: date = AS_OF,
) -> None:
    oid = uuid4()
    conn.execute(
        "INSERT INTO bond_security_observation "
        "(observation_id, as_of, observation_date, source_run_id, cusip9_input, "
        " coupon_type, coupon_rate, maturity_date, day_count, coupon_schedule, "
        " call_schedule, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
        (
            oid, as_of, as_of, run_id, cusip9, coupon_type, coupon_rate, maturity,
            day_count,
            None if coupon_schedule is None else json.dumps(coupon_schedule),
            None if call_schedule is None else json.dumps(call_schedule),
            json.dumps({"engine": "fixture", "observation_id": str(oid)}),
        ),
    )


def publish_security_master(conn, run_id: UUID, package_id: UUID, *, as_of: date = AS_OF) -> None:
    security_master.materialize(
        conn, as_of=as_of, source_run_id=run_id, source_package_id=package_id,
        code_revision="revtest",
    )
    conn.commit()


def land_price(
    conn, run_id: UUID, *, cusip9: str, price: object, ytm: object = None,
    observation_date: date = AS_OF, as_of: date = AS_OF,
) -> None:
    price_observations.load_price_observations(
        conn,
        [price_input(observation_date=observation_date, cusip9=cusip9, price=price,
                     price_type="trade", accrued_treatment="clean", ytm=ytm)],
        as_of=as_of, source_run_id=run_id,
    )
    conn.commit()


def seed_fix_bond(conn, run_id: UUID, package_id: UUID, *, source_ytm: object = 0.076,
                  observation_date: date = AS_OF) -> None:
    """One fixed 10% bond, clean trade price 96.23, source yield 0.076."""
    observe_security(conn, run_id, cusip9=CUSIP_FIX, coupon_type="fixed",
                     coupon_rate=Decimal("10.0"), maturity=date(2030, 1, 1),
                     day_count="30/360 US", coupon_schedule=FIX_SCHEDULE)
    publish_security_master(conn, run_id, package_id)
    land_price(conn, run_id, cusip9=CUSIP_FIX, price=Decimal("96.23"), ytm=source_ytm,
               observation_date=observation_date)


def current_rows(conn, security_id: UUID | None = None):
    sql = (
        "SELECT security_id, metric_id, value, status, engine_error_code, as_of "
        "FROM sec_current_bond_metric_v1"
    )
    params: tuple = ()
    if security_id is not None:
        sql += " WHERE security_id=%s"
        params = (security_id,)
    rows = conn.execute(sql + " ORDER BY security_id, metric_id", params).fetchall()
    return rows


def rows_by_metric(rows):
    return {r[1]: r for r in rows}


def current_pointer(conn) -> UUID | None:
    row = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product='bond_metric_v1'"
    ).fetchone()
    return row[0] if row else None


@pytest.fixture()
def env():
    admin = admin_connect()
    schema, run_id, package_id = new_env(admin)
    conn = work_conn(schema)
    try:
        yield conn, schema, run_id, package_id, admin
    finally:
        conn.close()
        admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


# --------------------------------------------------------------------------- #
# Happy path: projected source yield, promoted publication
# --------------------------------------------------------------------------- #

def test_worker_publishes_the_source_delivered_yield(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id, source_ytm=0.076)

    result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert result["state"] == "ok"
    assert result["product"] == "bond_metric_v1"
    assert result["securities"] == 1
    assert result["rows"] == 6
    # ytm, current_yield, wal, security_effective_duration, latest_price_pct
    assert result["available"] == 5
    assert result["terms_insufficient"] == 1  # ytw: no call schedule published

    rows = rows_by_metric(current_rows(conn, SEC_FIX))
    assert set(rows) == set(SERVED_METRICS)
    ytm = rows["security_ytm"]
    assert ytm[3] == "available" and ytm[4] is None and ytm[5] == AS_OF
    # PROJECTED: exactly the source's yield lane, never a terms recomputation.
    assert float(ytm[2]) == pytest.approx(0.076, rel=1e-9)
    # current_yield is a decimal FRACTION (the app registry declares it so).
    assert float(rows["current_yield"][2]) == pytest.approx(10.0 / 96.23, rel=1e-6)
    assert float(rows["wal"][2]) == pytest.approx((date(2030, 1, 1) - AS_OF).days / 365.0, rel=1e-9)
    assert rows["security_ytw"][3] == "terms_insufficient"
    assert rows["security_ytw"][2] is None
    # latest_price_pct is % of par, straight from the eligible latest lane.
    assert float(rows["latest_price_pct"][2]) == pytest.approx(96.23, rel=1e-9)
    # Analytic modified duration of a semiannual 10% bullet at 7.6%, five years
    # out. The expected value is the closed form evaluated independently here, so
    # a change to the SQL port has to justify itself against the mathematics
    # rather than against its own output.
    y, c = 0.076 / 2.0, 10.0 / 200.0
    n = round(2 * ((date(2030, 1, 1) - AS_OF).days / 365.25))
    v = (1 + y) ** (-n)
    ann = (1 - v) / y
    px = c * ann + v
    expected = (((c * ((1 + y) / y * ann - n * v / y) + n * v) / px) / 2.0) / (1 + y)
    assert float(rows["security_effective_duration"][2]) == pytest.approx(expected, rel=1e-9)
    assert rows["security_effective_duration"][3] == "available"

    assert current_pointer(conn) == UUID(result["publication_id"])


def test_no_qualification_registry_is_needed_to_publish(env):
    """The retired gate must not be consulted: the env installs NO
    bond_source_qualification table and the build still publishes real values."""
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    assert conn.execute(
        "SELECT to_regclass('bond_source_qualification')"
    ).fetchone()[0] is None
    seed_fix_bond(conn, run_id, package_id)

    result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert result["state"] == "ok"
    assert result["available"] == 5
    rows = rows_by_metric(current_rows(conn, SEC_FIX))
    assert rows["security_ytm"][3] == "available"


# --------------------------------------------------------------------------- #
# Per-security typed statuses
# --------------------------------------------------------------------------- #

def test_per_security_typed_statuses_never_a_fabricated_value(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    observe_security(conn, run_id, cusip9=CUSIP_FIX, coupon_type="fixed",
                     coupon_rate=Decimal("10.0"), maturity=date(2030, 1, 1),
                     day_count="30/360 US", coupon_schedule=FIX_SCHEDULE)
    observe_security(conn, run_id, cusip9=CUSIP_NOP, coupon_type="fixed",
                     coupon_rate=Decimal("5.0"), maturity=date(2031, 1, 1),
                     day_count="30/360 US", coupon_schedule=FIX_SCHEDULE)
    observe_security(conn, run_id, cusip9=CUSIP_NOY, coupon_type="fixed",
                     coupon_rate=Decimal("4.0"), maturity=date(2031, 1, 1),
                     day_count=None)
    observe_security(conn, run_id, cusip9=CUSIP_NCP, coupon_type="fixed",
                     coupon_rate=None, maturity=date(2030, 1, 1),
                     day_count=None)
    publish_security_master(conn, run_id, package_id)
    land_price(conn, run_id, cusip9=CUSIP_FIX, price=Decimal("96.23"), ytm=0.076)
    land_price(conn, run_id, cusip9=CUSIP_NOY, price=Decimal("101.5"))  # no yield lane
    land_price(conn, run_id, cusip9=CUSIP_NCP, price=Decimal("88.0"), ytm=0.09)
    # NOP gets no price at all.

    result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert result["state"] == "ok"
    assert result["securities"] == 4 and result["rows"] == 24

    fix = rows_by_metric(current_rows(conn, SEC_FIX))
    for metric in ("security_ytm", "current_yield", "wal"):
        assert fix[metric][3] == "available"

    nop = rows_by_metric(current_rows(conn, SEC_NOP))
    for metric in ("security_ytm", "current_yield"):
        assert nop[metric][3] == "no_eligible_price" and nop[metric][2] is None
    assert nop["wal"][3] == "available"  # WAL needs no price

    # A price the source delivered WITHOUT a yield: no yield is served (never
    # recomputed from terms), while price-derived current_yield still is.
    noy = rows_by_metric(current_rows(conn, SEC_NOY))
    assert noy["security_ytm"][3] == "no_eligible_price" and noy["security_ytm"][2] is None
    assert noy["current_yield"][3] == "available"
    assert float(noy["current_yield"][2]) == pytest.approx(4.0 / 101.5, rel=1e-6)

    # No coupon rate published: current_yield is honestly terms_insufficient,
    # while the source's own yield still serves.
    ncp = rows_by_metric(current_rows(conn, SEC_NCP))
    assert ncp["current_yield"][3] == "terms_insufficient" and ncp["current_yield"][2] is None
    assert ncp["security_ytm"][3] == "available"
    assert float(ncp["security_ytm"][2]) == pytest.approx(0.09, rel=1e-9)

    # ytw is uniformly terms_insufficient: nothing to compute it from.
    for rows in (fix, nop, noy, ncp):
        assert rows["security_ytw"][3] == "terms_insufficient"

    # The two additions follow the same honesty ladder, each for its own reason.
    assert fix["security_effective_duration"][3] == "available"
    assert fix["latest_price_pct"][3] == "available"
    assert float(fix["latest_price_pct"][2]) == pytest.approx(96.23, rel=1e-9)
    # NOP has no price at all: no yield to measure duration against, no price.
    assert nop["security_effective_duration"][3] == "no_eligible_price"
    assert nop["latest_price_pct"][3] == "no_eligible_price"
    assert nop["latest_price_pct"][2] is None
    # NOY has a price but the source delivered no yield: duration needs the
    # yield, the price metric does not.
    assert noy["security_effective_duration"][3] == "no_eligible_price"
    assert noy["latest_price_pct"][3] == "available"
    # NCP has price AND yield but no published coupon: a TYPED terms refusal
    # carrying its reason code, never a duration computed off a guessed coupon.
    assert ncp["security_effective_duration"][3] == "terms_insufficient"
    assert ncp["security_effective_duration"][2] is None
    assert ncp["security_effective_duration"][4] == "coupon_rate_unpublished"

    # Structural honesty in the DB itself: value present iff available.
    bad = conn.execute(
        "SELECT count(*) FROM sec_current_bond_metric_v1 "
        "WHERE (status='available') <> (value IS NOT NULL)"
    ).fetchone()[0]
    assert bad == 0


def test_future_observation_never_enters_the_build(env):
    """No-look-ahead: only the latest eligible observation AT/BEFORE the
    calc-date feeds the projection; a future print never enters, even when the
    eligibility view itself (which knows nothing about the calc-date) admits it.
    """
    from datetime import timedelta

    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    as_of = date(2025, 2, 1)
    stale_date = as_of - timedelta(days=10)   # inside the bond's life
    future_date = as_of + timedelta(days=5)   # must NEVER enter

    observe_security(conn, run_id, cusip9=CUSIP_FIX, coupon_type="fixed",
                     coupon_rate=Decimal("10.0"), maturity=date(2030, 1, 1),
                     day_count="30/360 US", coupon_schedule=FIX_SCHEDULE, as_of=as_of)
    publish_security_master(conn, run_id, package_id, as_of=as_of)
    land_price(conn, run_id, cusip9=CUSIP_FIX, price=Decimal("96.23"), ytm=0.076,
               observation_date=stale_date, as_of=as_of)
    land_price(conn, run_id, cusip9=CUSIP_FIX, price=Decimal("55.0"), ytm=0.21,
               observation_date=future_date, as_of=as_of)
    # BOTH observations are eligible: only the <= as_of predicate excludes the
    # future one.
    eligible = conn.execute(
        "SELECT count(*) FROM bond_price_eligibility_v1 WHERE is_eligible"
    ).fetchone()[0]
    assert eligible == 2
    # Release the view lock this SELECT's open transaction holds: the worker's
    # own connection re-applies the eligibility DDL and would block on it.
    conn.commit()

    result = bond_metrics.run(search_path_dsn(schema), calc_date=as_of.isoformat())
    assert result["state"] == "ok" and result["available"] == 5

    rows = rows_by_metric(current_rows(conn, SEC_FIX))
    # The STALE print's yield and price serve; the future print's never do.
    assert float(rows["security_ytm"][2]) == pytest.approx(0.076, rel=1e-9)
    assert float(rows["current_yield"][2]) == pytest.approx(10.0 / 96.23, rel=1e-6)
    # WAL settles at the chain calc-date, from the published maturity (no price).
    assert float(rows["wal"][2]) == pytest.approx(
        (date(2030, 1, 1) - as_of).days / 365.0, rel=1e-9)


# --------------------------------------------------------------------------- #
# Dark ladder (sibling dark_no_source semantics: nothing published)
# --------------------------------------------------------------------------- #

def test_dark_ladder_publishes_nothing():
    from src.workers import bond_metrics

    admin = admin_connect()
    schema, run_id, package_id = new_env(admin, seed_lineage=False)
    try:
        dsn = search_path_dsn(schema)
        # (a) bare schema: no validated source registered anywhere.
        result = bond_metrics.run(dsn, calc_date=AS_OF.isoformat())
        assert result["state"] == "no_source"

        with work_conn(schema) as conn:
            conn.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
            conn.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
            conn.execute(
                "CREATE VIEW sec_validated_raw_runs AS "
                "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
            )
            conn.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
            conn.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
            conn.commit()

            # (b) validated source, but no published security universe.
            result = bond_metrics.run(dsn, calc_date=AS_OF.isoformat())
            assert result["state"] == "no_securities"

            # (c) securities exist; no observations and no pinned calc_date.
            conn.execute((ROOT / "schemas" / "bond_security_v1.sql").read_text(encoding="utf-8"))
            conn.commit()
            observe_security(conn, run_id, cusip9=CUSIP_FIX, coupon_type="fixed",
                             coupon_rate=Decimal("10.0"), maturity=date(2030, 1, 1),
                             day_count="30/360 US", coupon_schedule=FIX_SCHEDULE)
            publish_security_master(conn, run_id, package_id)
            result = bond_metrics.run(dsn)
            assert result["state"] == "no_observations"

            # NOTHING was ever published on any dark path.
            assert current_pointer(conn) is None
    finally:
        admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_contended_lock_returns_locked(env):
    from src.workers import bond_metrics
    from src.db import LOCK_BOND_METRICS

    conn, schema, _, _, _ = env
    assert LOCK_BOND_METRICS == 900_350  # additive, next free ingestion-band id
    with advisory_lock(conn, LOCK_BOND_METRICS) as got:
        assert got
        result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
        assert result["state"] == "locked"


# --------------------------------------------------------------------------- #
# Publication protocol: replay, input change, rollback, immutability
# --------------------------------------------------------------------------- #

def test_replay_is_idempotent_and_input_change_mints_a_new_publication(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id, observation_date=date(2024, 12, 20))

    first = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    replay = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert first["publication_id"] == replay["publication_id"]
    n = conn.execute(
        "SELECT count(*) FROM bond_metric_v1 WHERE publication_id=%s",
        (UUID(first["publication_id"]),),
    ).fetchone()[0]
    assert n == len(SERVED_METRICS)  # no duplicate rows on replay

    # A fresher eligible observation changes the build inputs: a NEW publication.
    land_price(conn, run_id, cusip9=CUSIP_FIX, price=Decimal("97.10"), ytm=0.071,
               observation_date=AS_OF, as_of=AS_OF)
    second = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert second["publication_id"] != first["publication_id"]
    assert current_pointer(conn) == UUID(second["publication_id"])


def test_rollback_pointer_restores_the_prior_current(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id, source_ytm=0.076,
                  observation_date=date(2024, 12, 20))
    first = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    land_price(conn, run_id, cusip9=CUSIP_FIX, price=Decimal("97.10"), ytm=0.071,
               observation_date=AS_OF, as_of=AS_OF)
    second = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    pub1, pub2 = UUID(first["publication_id"]), UUID(second["publication_id"])
    assert current_pointer(conn) == pub2

    # The chain records promotions in its ledger; mirror that, then roll back
    # through the EXISTING daily_chain.rollback_pointer path.
    daily_chain.install_schema(conn)
    daily_chain.record_promotion(conn, product="bond_metric_v1", publication_id=pub2,
                                 previous_publication_id=pub1, action="promote")
    outcome = daily_chain.rollback_pointer(conn, "bond_metric_v1")
    conn.commit()
    assert outcome["restored_to"] == str(pub1)
    assert current_pointer(conn) == pub1
    # The rolled-back-to build serves the OLD projected values.
    rows = rows_by_metric(current_rows(conn, SEC_FIX))
    assert float(rows["security_ytm"][2]) == pytest.approx(0.076, rel=1e-9)


def test_published_snapshot_is_immutable_after_validation(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id)
    result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    pub = UUID(result["publication_id"])

    with pytest.raises(psycopg.Error):
        conn.execute(
            "INSERT INTO bond_metric_v1 (publication_id, security_id, metric_id, value, "
            "status, engine_error_code, as_of, provenance) "
            "VALUES (%s,%s,'wal',1.0,'available',NULL,%s,'{}'::jsonb)",
            (pub, uuid4(), AS_OF),
        )
    conn.rollback()
    with pytest.raises(psycopg.Error):
        conn.execute("UPDATE bond_metric_v1 SET value=0 WHERE publication_id=%s", (pub,))
    conn.rollback()


# --------------------------------------------------------------------------- #
# Determinism + Wave-1 schema honesty
# --------------------------------------------------------------------------- #

def test_same_inputs_produce_the_same_publication_across_schemas():
    from src.workers import bond_metrics

    admin = admin_connect()
    payloads, pub_ids, schemas = [], [], []
    try:
        for _ in range(2):
            schema, run_id, package_id = new_env(admin)
            schemas.append(schema)
            with work_conn(schema) as conn:
                seed_fix_bond(conn, run_id, package_id)
                result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
                assert result["state"] == "ok"
                pub_ids.append(result["publication_id"])
                payloads.append(current_rows(conn))
    finally:
        for schema in schemas:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()
    # Deterministic identity AND byte-identical payload from identical inputs.
    assert pub_ids[0] == pub_ids[1]
    assert payloads[0] == payloads[1]


def test_live_metric_vocabulary_is_exactly_the_served_set(env):
    """The vocabulary is checked on the LIVE constraint, not on the DDL text.

    The inline CHECK inside ``CREATE TABLE IF NOT EXISTS`` is a no-op once the
    table exists, so only the idempotent ALTER block actually widens a deployed
    table. Reading the constraint back from ``pg_constraint`` is the only
    assertion that would have caught a migration that silently did nothing.
    """
    import re

    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id)
    from src.workers import bond_metrics

    assert bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())["state"] == "ok"

    definitions = [
        row[0] for row in conn.execute(
            "SELECT pg_get_constraintdef(con.oid) FROM pg_constraint con "
            "WHERE con.conrelid = 'bond_metric_v1'::regclass AND con.contype='c'"
        ).fetchall()
        if "metric_id" in row[0]
    ]
    assert len(definitions) == 1, definitions
    declared = set(re.findall(r"'([a-z_0-9]+)'", definitions[0]))
    assert declared == set(SERVED_METRICS)


def test_the_metric_vocabulary_still_refuses_the_unmodelled_families(env):
    """OAS, z-spread and a callable YTW have no validated model here.

    Widening the vocabulary for duration and price must not have opened the door
    to the families that would require one — an unmodelled spread is a guess, and
    the CLOSED enum is what makes that unwritable rather than merely unwritten.
    """
    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id)
    from src.workers import bond_metrics

    assert bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())["state"] == "ok"

    # Exercise the CHECK itself on a structural copy: the product table's write
    # guard (publication must be 'prepared') would otherwise refuse the insert
    # first and the constraint would never be reached, so the copy is what makes
    # this test about the VOCABULARY rather than about the guard.
    conn.execute(
        "CREATE TEMP TABLE _metric_vocabulary_probe "
        "(LIKE bond_metric_v1 INCLUDING CONSTRAINTS INCLUDING DEFAULTS) ON COMMIT DROP"
    )
    for allowed in SERVED_METRICS:
        conn.execute(
            "INSERT INTO _metric_vocabulary_probe"
            "(publication_id,security_id,metric_id,value,status,as_of,"
            " methodology_version,provenance) "
            "VALUES (%s,%s,%s,NULL,'no_eligible_price',%s,'bond_metric_v1','{}'::jsonb)",
            (uuid4(), SEC_FIX, allowed, AS_OF),
        )
    for forbidden in ("security_oas", "security_zspread", "effective_duration", "rating_bucket"):
        with conn.transaction(force_rollback=True):
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute(
                    "INSERT INTO _metric_vocabulary_probe"
                    "(publication_id,security_id,metric_id,value,status,as_of,"
                    " methodology_version,provenance) "
                    "VALUES (%s,%s,%s,NULL,'no_eligible_price',%s,'bond_metric_v1','{}'::jsonb)",
                    (uuid4(), SEC_FIX, forbidden, AS_OF),
                )
    conn.rollback()


def test_new_surfaces_carry_no_vendor_identity():
    sources = [
        ROOT / "src" / "bonds" / "metrics_engine_runner.py",
        ROOT / "src" / "workers" / "bond_metrics.py",
        ROOT / "schemas" / "bond_metric_v1.sql",
    ]
    for path in sources:
        text = path.read_text(encoding="utf-8").lower()
        for token in ("osbap", "openbondassetpricing", "trace", "wrds", "n-port", "nport"):
            assert token not in text, f"{path.name} leaks vendor token {token!r}"


# --------------------------------------------------------------------------- #
# Freshness: the dense daily series carries the build forward (2026-08-07)
# --------------------------------------------------------------------------- #
def _seed_dense_series(conn, rows) -> None:
    """Plain-table stand-in for the bond_observation_daily hypertable.

    The CUSIP9 -> security_id join runs through the alias view the security
    master already publishes (``publish_security_master`` seeds it), so nothing
    identity-shaped is invented here.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bond_observation_daily(
            cusip9 text NOT NULL, day date NOT NULL, price numeric, ytm numeric,
            volume numeric, price_type text, accrued text, source text NOT NULL,
            source_rank smallint NOT NULL, ytm_basis text,
            PRIMARY KEY (cusip9, day))
    """)
    for row in rows:
        conn.execute(
            "INSERT INTO bond_observation_daily "
            "VALUES(%s,%s,%s,%s,NULL,'trade','clean','finnhub',1,'reported') "
            "ON CONFLICT (cusip9, day) DO NOTHING", row,
        )
    conn.commit()


def test_a_newer_dense_observation_prices_the_metrics(env):
    """latest_price_pct / current_yield / security_ytm all move to the newer day.

    This is the whole 2e mechanism on the metric side: the governed landing
    table stops, the dense series continues, and the published numbers follow it
    instead of freezing at the landing table's last day.
    """
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id, source_ytm=0.076)
    fresh_day = AS_OF + timedelta(days=30)
    _seed_dense_series(conn, [(CUSIP_FIX, fresh_day, Decimal("104.50"), Decimal("0.0410"))])

    result = bond_metrics.run(search_path_dsn(schema))

    assert result["state"] == "ok"
    # The build's own anchor followed the data (it was AS_OF before).
    assert result["as_of"] == fresh_day.isoformat()
    metrics = rows_by_metric(current_rows(conn, SEC_FIX))
    assert metrics["latest_price_pct"][2] == Decimal("104.50")
    assert metrics["security_ytm"][2] == Decimal("0.0410")
    # coupon 10.0 over the NEW price, not the old 96.23.
    assert round(metrics["current_yield"][2], 8) == round(
        Decimal("10.0") / Decimal("104.50"), 8
    )
    assert metrics["security_effective_duration"][3] == "available"


def test_price_and_yield_resolve_on_their_own_latest_days(env):
    """A fresh price with no yield must not erase the yield -- nor the duration.

    The dense series carries a newer PRICED day whose yield is absent. Folding
    both fields onto one latest row would publish a yield of NULL (and, since
    duration is solved from the yield, no duration either).
    """
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id, source_ytm=0.076)
    _seed_dense_series(conn, [
        (CUSIP_FIX, AS_OF + timedelta(days=10), Decimal("100.00"), Decimal("0.0500")),
        (CUSIP_FIX, AS_OF + timedelta(days=20), Decimal("101.00"), None),
    ])

    bond_metrics.run(search_path_dsn(schema))

    metrics = rows_by_metric(current_rows(conn, SEC_FIX))
    assert metrics["latest_price_pct"][2] == Decimal("101.00"), "freshest PRICED day"
    assert metrics["security_ytm"][2] == Decimal("0.0500"), "freshest YIELDED day"
    assert metrics["security_effective_duration"][3] == "available"


def test_an_older_dense_observation_never_rolls_a_metric_backwards(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fix_bond(conn, run_id, package_id, source_ytm=0.076)
    _seed_dense_series(conn, [
        (CUSIP_FIX, AS_OF - timedelta(days=5), Decimal("50.00"), Decimal("0.4000")),
    ])

    bond_metrics.run(search_path_dsn(schema))

    metrics = rows_by_metric(current_rows(conn, SEC_FIX))
    assert metrics["latest_price_pct"][2] == Decimal("96.23")
    assert metrics["security_ytm"][2] == Decimal("0.076")

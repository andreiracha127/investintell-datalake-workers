"""DB tests for the bond_metrics compute+persist worker (activation Wave 1, Task 3).

Runs against the disposable Postgres at ``SEC_TEST_DATABASE_URL`` (PG 65431),
DSN-agnostic (keyword and URL forms). Proves, against the REAL publication
protocol:

  * happy path: qualified metrics -> a promoted ``bond_metric_v1`` build whose
    yields are RECOMPUTED by the validated engines (never copied from the
    source's raw ``ytm`` lane);
  * gate honesty: unqualified metrics publish ``gate_not_passed`` rows with a
    NULL value (fail-closed, still truthful);
  * per-security typed statuses (``no_eligible_price`` / ``terms_insufficient``
    / ``engine_typed_error``) — never NaN, never a fabricated value;
  * the sibling dark ladder (``no_source`` / ``no_securities`` /
    ``no_observations``) publishes NOTHING (chain dark_no_source semantics);
  * build-versioned publication: idempotent replay, input-change mints a new
    publication, ``daily_chain.rollback_pointer`` restores the prior current;
  * write guard: the published snapshot is immutable after validation;
  * schema enum carries EXACTLY the Wave-1 metrics (no OAS/z-spread/duration).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_price_fixtures import price_input  # noqa: E402

from src.bonds import daily_chain, price_observations, security_master  # noqa: E402
from src.bonds.metrics_engine_runner import WAVE1_METRICS  # noqa: E402
from src.bonds.security_master import NAMESPACE_BOND_SECURITY  # noqa: E402
from src.db import advisory_lock  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL not set"
)

ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2025, 1, 1)
SOURCE_REF = "bond_price_source_v1@aaaaaaaaaaaa"

CUSIP_FIX = "BNDFIX001"  # fixed 10% semiannual (Fabozzi-style), priced 96.23
CUSIP_NOP = "BNDNOP002"  # sufficient terms, NO eligible price
CUSIP_FLT = "BNDFLT003"  # floating coupon: terms_insufficient
CUSIP_STB = "BNDSTB004"  # off-grid coupon schedule: front_stub_unsupported

SEC_FIX = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{CUSIP_FIX}")
SEC_NOP = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{CUSIP_NOP}")
SEC_FLT = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{CUSIP_FLT}")
SEC_STB = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{CUSIP_STB}")

FIX_SCHEDULE = [{"date": "2025-07-01", "rate": 10.0}, {"date": "2026-01-01", "rate": 10.0}]
STB_SCHEDULE = [{"date": "2025-08-15"}, {"date": "2026-02-15"}]


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
            "bond_source_qualification.sql",
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


def qualify(conn, metrics) -> None:
    for metric in metrics:
        conn.execute(
            "INSERT INTO bond_source_qualification "
            "(metric_id, source_contract_ref, qualified_from, qualified_to) "
            "VALUES (%s, %s, now(), NULL) "
            "ON CONFLICT (metric_id, source_contract_ref) DO NOTHING",
            (metric, SOURCE_REF),
        )
    conn.commit()


def seed_fabozzi_bond(conn, run_id: UUID, package_id: UUID, *, source_ytm: object = 0.076) -> None:
    """One fixed 10% semiannual bond, clean trade price 96.23 at AS_OF."""
    observe_security(conn, run_id, cusip9=CUSIP_FIX, coupon_type="fixed",
                     coupon_rate=Decimal("10.0"), maturity=date(2030, 1, 1),
                     day_count="30/360 US", coupon_schedule=FIX_SCHEDULE)
    publish_security_master(conn, run_id, package_id)
    land_price(conn, run_id, cusip9=CUSIP_FIX, price=Decimal("96.23"), ytm=source_ytm)


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
# Happy path: recomputed yields, promoted publication
# --------------------------------------------------------------------------- #

def test_worker_publishes_available_rows_and_recomputes_yields(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fabozzi_bond(conn, run_id, package_id, source_ytm=0.076)
    qualify(conn, WAVE1_METRICS)

    result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert result["state"] == "ok"
    assert result["product"] == "bond_metric_v1"
    assert result["securities"] == 1
    assert result["rows"] == 4
    assert result["available"] == 4

    rows = rows_by_metric(current_rows(conn, SEC_FIX))
    assert set(rows) == set(WAVE1_METRICS)
    ytm = rows["security_ytm"]
    assert ytm[3] == "available" and ytm[4] is None and ytm[5] == AS_OF
    # RECOMPUTED by the engine (Fabozzi ~11%), never the source's raw 0.076.
    assert float(ytm[2]) == pytest.approx(0.11, abs=1e-3)
    assert float(ytm[2]) != pytest.approx(0.076, abs=1e-3)
    assert float(rows["security_ytw"][2]) == pytest.approx(float(ytm[2]), rel=1e-9)
    assert float(rows["current_yield"][2]) == pytest.approx(10.0 / 96.23, rel=1e-6)
    assert float(rows["wal"][2]) == pytest.approx((date(2030, 1, 1) - AS_OF).days / 365.0, rel=1e-9)

    assert current_pointer(conn) == UUID(result["publication_id"])


# --------------------------------------------------------------------------- #
# Gate honesty (fail-closed rows, still published)
# --------------------------------------------------------------------------- #

def test_unqualified_metrics_publish_gate_not_passed_rows(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fabozzi_bond(conn, run_id, package_id)
    qualify(conn, ["security_ytm", "wal"])  # ytw / current_yield stay unqualified

    result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert result["state"] == "ok"
    rows = rows_by_metric(current_rows(conn, SEC_FIX))
    assert rows["security_ytm"][3] == "available"
    assert rows["wal"][3] == "available"
    for gated in ("security_ytw", "current_yield"):
        assert rows[gated][3] == "gate_not_passed"
        assert rows[gated][2] is None
        assert rows[gated][4] is None


def test_fully_unqualified_world_publishes_a_fully_gated_build(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fabozzi_bond(conn, run_id, package_id)
    # NO qualification row at all: the gate fails with no_qualified_source.
    result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert result["state"] == "ok"
    assert result["gate_not_passed"] == 4
    rows = current_rows(conn, SEC_FIX)
    assert len(rows) == 4
    assert all(r[3] == "gate_not_passed" and r[2] is None for r in rows)


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
    observe_security(conn, run_id, cusip9=CUSIP_FLT, coupon_type="floating",
                     coupon_rate=Decimal("4.0"), maturity=date(2031, 1, 1),
                     day_count="30/360 US")
    observe_security(conn, run_id, cusip9=CUSIP_STB, coupon_type="fixed",
                     coupon_rate=Decimal("6.0"), maturity=date(2030, 1, 1),
                     day_count="30/360 US", coupon_schedule=STB_SCHEDULE)
    publish_security_master(conn, run_id, package_id)
    for cusip in (CUSIP_FIX, CUSIP_FLT, CUSIP_STB):  # NOP gets no price
        land_price(conn, run_id, cusip9=cusip, price=Decimal("96.23"))
    qualify(conn, WAVE1_METRICS)

    result = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert result["state"] == "ok"
    assert result["securities"] == 4 and result["rows"] == 16

    fix = rows_by_metric(current_rows(conn, SEC_FIX))
    assert all(fix[m][3] == "available" for m in WAVE1_METRICS)

    nop = rows_by_metric(current_rows(conn, SEC_NOP))
    for metric in ("security_ytm", "security_ytw", "current_yield"):
        assert nop[metric][3] == "no_eligible_price" and nop[metric][2] is None
    assert nop["wal"][3] == "available"  # WAL is schedule-only

    flt = rows_by_metric(current_rows(conn, SEC_FLT))
    for metric in WAVE1_METRICS:
        assert flt[metric][3] == "terms_insufficient"
        assert flt[metric][4] == "coupon_type_unsupported"
        assert flt[metric][2] is None

    stb = rows_by_metric(current_rows(conn, SEC_STB))
    for metric in WAVE1_METRICS:
        assert stb[metric][3] == "engine_typed_error"
        assert stb[metric][4] == "front_stub_unsupported"
        assert stb[metric][2] is None

    # Structural honesty in the DB itself: value present iff available.
    bad = conn.execute(
        "SELECT count(*) FROM sec_current_bond_metric_v1 "
        "WHERE (status='available') <> (value IS NOT NULL)"
    ).fetchone()[0]
    assert bad == 0


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
    seed_fabozzi_bond(conn, run_id, package_id)
    qualify(conn, ["security_ytm"])

    first = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    replay = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert first["publication_id"] == replay["publication_id"]
    n = conn.execute(
        "SELECT count(*) FROM bond_metric_v1 WHERE publication_id=%s",
        (UUID(first["publication_id"]),),
    ).fetchone()[0]
    assert n == 4  # no duplicate rows on replay

    # Qualifying another metric changes the build inputs: a NEW publication.
    qualify(conn, ["security_ytw"])
    second = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    assert second["publication_id"] != first["publication_id"]
    assert current_pointer(conn) == UUID(second["publication_id"])
    rows = rows_by_metric(current_rows(conn, SEC_FIX))
    assert rows["security_ytw"][3] == "available"  # the change is served


def test_rollback_pointer_restores_the_prior_current(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fabozzi_bond(conn, run_id, package_id)
    qualify(conn, ["security_ytm"])
    first = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    qualify(conn, ["security_ytw"])
    second = bond_metrics.run(search_path_dsn(schema), calc_date=AS_OF.isoformat())
    pub1, pub2 = UUID(first["publication_id"]), UUID(second["publication_id"])
    assert current_pointer(conn) == pub2

    # The chain records promotions in its ledger; mirror that, then roll back
    # through the EXISTING daily_chain.rollback_pointer path (requirement 4).
    daily_chain.install_schema(conn)
    daily_chain.record_promotion(conn, product="bond_metric_v1", publication_id=pub2,
                                 previous_publication_id=pub1, action="promote")
    outcome = daily_chain.rollback_pointer(conn, "bond_metric_v1")
    conn.commit()
    assert outcome["restored_to"] == str(pub1)
    assert current_pointer(conn) == pub1
    # The rolled-back-to build serves the OLD gate state (ytw not yet passing).
    rows = rows_by_metric(current_rows(conn, SEC_FIX))
    assert rows["security_ytw"][3] == "gate_not_passed"


def test_published_snapshot_is_immutable_after_validation(env):
    from src.workers import bond_metrics

    conn, schema, run_id, package_id, _ = env
    seed_fabozzi_bond(conn, run_id, package_id)
    qualify(conn, WAVE1_METRICS)
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
                seed_fabozzi_bond(conn, run_id, package_id)
                qualify(conn, WAVE1_METRICS)
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


def test_schema_enum_carries_exactly_the_wave1_metrics_and_no_forbidden_family():
    ddl = (ROOT / "schemas" / "bond_metric_v1.sql").read_text(encoding="utf-8").lower()
    for metric in WAVE1_METRICS:
        assert metric in ddl
    for forbidden in ("oas", "zspread", "z_spread", "duration"):
        assert forbidden not in ddl


def test_new_surfaces_carry_no_vendor_identity():
    sources = [
        ROOT / "src" / "bonds" / "metrics_engine_runner.py",
        ROOT / "src" / "workers" / "bond_metrics.py",
        ROOT / "schemas" / "bond_metric_v1.sql",
    ]
    for path in sources:
        text = path.read_text(encoding="utf-8").lower()
        for token in ("osbap", "openbondassetpricing", "trace", "wrds", "n-port", "nport"):
            assert token not in text, f"{token!r} leaked into {path.name}"

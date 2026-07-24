"""Compute+persist worker for the bond_metric_v1 product (activation Wave 1, Task 3).

Runs the VALIDATED pure engines (:mod:`src.bonds.metrics_engine_runner` over
:mod:`src.bonds.cashflows` / :mod:`src.bonds.pricing`) for every security in the
current published universe and lands one complete ``bond_metric_v1`` snapshot
through the shared derived-publication protocol (prepared -> validated ->
current pointer), under an advisory lock.

Inputs (read-only):
  * ``sec_current_bond_security_v1`` — the published terms per security;
  * ``bond_price_eligibility_v1`` over ``bond_price_observation`` — the
    eligible latest CLEAN price (% of par) per security at/before ``as_of``
    (deterministic tie-break: latest observation_date, then latest landing
    as_of, then observation_id);
  * ``bond_source_qualification`` via the Phase-10 ``gate_status`` — the gate
    is evaluated per metric ONCE per run and every row of a non-passing metric
    is published ``gate_not_passed`` with a NULL value (gate-honest: the chain
    stays truthful when only partially qualified).

Dark-mode semantics (decision, matching the sibling ``dark_no_source``
conventions in ``daily_chain.py`` — see the Task 3 report): with NO validated
source, NO published security universe, or NO observation day to anchor
``as_of``, the worker is a REPORTED no-op (``no_source`` / ``no_securities`` /
``no_observations``) and publishes NOTHING — the chain's fully-dark steady
state stays "nothing promoted". Once a validated source and universe exist, the
worker always publishes, carrying per-metric gate honesty inside the build.

Determinism: ``as_of`` is the chain's calc-date (or the latest observation
landing day when unpinned); no wall-clock value enters the payload. The
publication identity is ``uuid5(product | as_of | code_revision |
input_fingerprint)`` where the fingerprint covers the terms, eligible prices
and per-metric gate outcomes — identical inputs replay the SAME publication
byte-for-byte; changed inputs (e.g. a new qualification) mint a NEW build,
keeping ``daily_chain.rollback_pointer`` meaningful.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

from src.bonds.metrics_engine_runner import (
    WAVE1_METRICS,
    EligiblePrice,
    MetricRow,
    SecurityTermsInput,
    compute_security_metrics,
)
from src.bonds.phase10_gate import gate_status, install_gate_schema
from src.db import LOCK_BOND_METRICS, advisory_lock, connect, resolve_dsn

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_metric_v1.sql"
ELIGIBILITY_SCHEMA_PATH = ROOT / "schemas" / "bond_price_eligibility_v1.sql"
DERIVED_PROTOCOL_PATH = ROOT / "schemas" / "sec_derived_publications.sql"

PRODUCT = "bond_metric_v1"
METHODOLOGY_VERSION = "bond_metric_v1"

# Deterministic namespace for the metric publication identity (distinct
# constant, sibling style; suffix spells 'metric' in hex).
_NAMESPACE_PUBLICATION = UUID("b0d5ec00-0000-5000-a000-6d6574726963")


def _code_revision() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def install_schema(conn: psycopg.Connection) -> None:
    """Apply the publication protocol + product DDL (+ gate registry) idempotently.

    The eligibility predicate/view is re-applied only when its underlying
    observation table exists (in the chain it is installed by the pit_update
    stage's own workers before this one runs).
    """
    with conn.cursor() as cur:
        cur.execute(DERIVED_PROTOCOL_PATH.read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
    install_gate_schema(conn)
    if _relation_exists(conn, "bond_price_observation"):
        with conn.cursor() as cur:
            cur.execute(ELIGIBILITY_SCHEMA_PATH.read_text(encoding="utf-8"))


def _relation_exists(conn: psycopg.Connection, name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()
    return bool(row and row[0] is not None)


def _latest_validated_source(conn: psycopg.Connection) -> tuple[Any, Any] | None:
    if not (_relation_exists(conn, "sec_validated_raw_runs")
            and _relation_exists(conn, "sec_source_packages")):
        return None
    row = conn.execute(
        "SELECT r.run_id, p.package_id "
        "FROM sec_validated_raw_runs r "
        "JOIN sec_source_packages p ON p.run_id=r.run_id "
        "ORDER BY r.raw_validated_at DESC, p.package_id LIMIT 1"
    ).fetchone()
    return (row[0], row[1]) if row else None


def _resolve_as_of(conn: psycopg.Connection, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    if not _relation_exists(conn, "bond_price_observation"):
        return None
    row = conn.execute("SELECT max(as_of) FROM bond_price_observation").fetchone()
    return row[0] if row else None


def _load_securities(conn: psycopg.Connection) -> list[SecurityTermsInput]:
    """The current published universe, deterministic order, terms as published."""
    rows = conn.execute(
        "SELECT security_id, coupon_type, coupon_rate, maturity_date, day_count, "
        "       terms -> 'coupon_schedule', terms -> 'call_schedule' "
        "FROM sec_current_bond_security_v1 ORDER BY security_id"
    ).fetchall()
    return [
        SecurityTermsInput(
            security_id=r[0], coupon_type=r[1], coupon_rate=r[2],
            maturity_date=r[3], day_count=r[4], coupon_schedule=r[5],
            call_schedule=r[6],
        )
        for r in rows
    ]


def _eligible_latest_prices(conn: psycopg.Connection, as_of: date) -> dict[UUID, EligiblePrice]:
    """Eligible latest clean price per security at/before ``as_of`` (typed lane).

    Reads the eligibility view only; the deterministic tie-break (latest
    observation_date, then latest landing as_of, then observation_id) makes a
    replay byte-identical.
    """
    if not (_relation_exists(conn, "bond_price_eligibility_v1")
            and _relation_exists(conn, "bond_price_observation")):
        return {}
    rows = conn.execute(
        "SELECT DISTINCT ON (e.security_id) e.security_id, o.price, e.observation_date "
        "FROM bond_price_eligibility_v1 e "
        "JOIN bond_price_observation o ON o.observation_id = e.observation_id "
        "WHERE e.is_eligible AND e.observation_date <= %s "
        "ORDER BY e.security_id, e.observation_date DESC, e.as_of DESC, e.observation_id DESC",
        (as_of,),
    ).fetchall()
    return {r[0]: EligiblePrice(price=r[1], observation_date=r[2]) for r in rows}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _input_fingerprint(
    as_of: date,
    gates: dict[str, Any],
    securities: list[SecurityTermsInput],
    prices: dict[UUID, EligiblePrice],
) -> str:
    """Product-salted fingerprint over EVERY build input (terms, prices, gate).

    The gate outcomes are inputs: qualifying a metric changes the honest payload,
    so it must mint a new publication rather than silently replay the gated one.
    """
    parts = [f"{PRODUCT}|{as_of.isoformat()}|{METHODOLOGY_VERSION}"]
    for metric in WAVE1_METRICS:
        status = gates[metric]
        parts.append(f"gate|{metric}|{status.passed}|{','.join(status.reasons)}")
    for sec in securities:
        parts.append("|".join(str(x) for x in (
            "sec", sec.security_id, sec.coupon_type, sec.coupon_rate,
            sec.maturity_date, sec.day_count,
            _canonical_json(sec.coupon_schedule), _canonical_json(sec.call_schedule),
        )))
    for security_id in sorted(prices, key=str):
        quote = prices[security_id]
        parts.append(f"price|{security_id}|{quote.price}|{quote.observation_date.isoformat()}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def publication_id_for(as_of: date, code_revision: str, fingerprint: str) -> UUID:
    return uuid5(_NAMESPACE_PUBLICATION,
                 f"{PRODUCT}|{as_of.isoformat()}|{code_revision}|{fingerprint}")


def _materialize(
    conn: psycopg.Connection,
    *,
    as_of: date,
    source_run_id: Any,
    source_package_id: Any,
    code_revision: str,
    fingerprint: str,
    security_count: int,
    metric_rows: list[MetricRow],
) -> dict[str, Any]:
    """Prepare -> pin -> write snapshot -> validate -> current, idempotently.

    A partial/failed build never becomes current: snapshot rows are written only
    while the publication is 'prepared', the pin is verified before validate,
    and the current pointer advances only after validation (the shared
    publication protocol's fail-closed guards enforce this).
    """
    publication_id = publication_id_for(as_of, code_revision, fingerprint)

    existing = conn.execute(
        "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
        (publication_id,),
    ).fetchone()
    if existing is None:
        version = conn.execute(
            "SELECT COALESCE(max(publication_version),0)+1 FROM sec_derived_publications WHERE product=%s",
            (PRODUCT,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO sec_derived_publications"
            "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) "
            "VALUES(%s,%s,%s,%s,%s,%s)",
            (publication_id, PRODUCT, version, source_run_id, source_package_id, fingerprint),
        )
        lifecycle = "prepared"
    else:
        lifecycle = existing[0]

    if lifecycle == "prepared":
        conn.execute(
            "INSERT INTO bond_metric_v1_builds"
            "(publication_id,input_fingerprint,as_of_date,security_input_count,metric_row_count) "
            "VALUES(%s,%s,%s,%s,%s) ON CONFLICT (publication_id) DO NOTHING",
            (publication_id, fingerprint, as_of, security_count, len(metric_rows)),
        )
        pinned = conn.execute(
            "SELECT input_fingerprint, as_of_date FROM bond_metric_v1_builds WHERE publication_id=%s",
            (publication_id,),
        ).fetchone()
        if pinned[0] != fingerprint:
            raise RuntimeError(f"{PRODUCT} publication already pinned to fingerprint {pinned[0]}")
        if pinned[1] != as_of:
            raise RuntimeError(f"{PRODUCT} publication already pinned to as_of {pinned[1]}")
        for row in metric_rows:
            conn.execute(
                "INSERT INTO bond_metric_v1"
                "(publication_id,security_id,metric_id,value,status,engine_error_code,as_of,provenance) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (publication_id,security_id,metric_id) DO NOTHING",
                (
                    publication_id, row.security_id, row.metric_id, row.value,
                    row.status, row.engine_error_code, row.as_of,
                    Jsonb({"engine_runner": "metrics_engine_runner",
                           "methodology_version": METHODOLOGY_VERSION}),
                ),
            )
        conn.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))

    current = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s", (PRODUCT,)
    ).fetchone()
    if current is None or current[0] != publication_id:
        conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))

    status_counts = {status: 0 for status in (
        "available", "no_eligible_price", "terms_insufficient",
        "engine_typed_error", "gate_not_passed")}
    for row in metric_rows:
        status_counts[row.status] += 1
    return {
        "product": PRODUCT,
        "publication_id": str(publication_id),
        "as_of": as_of.isoformat(),
        "securities": security_count,
        "rows": len(metric_rows),
        **status_counts,
    }


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_METRICS) as acquired:
        # Serialize BEFORE the self-installing DDL (fleet idiom: CREATE TABLE IF
        # NOT EXISTS is not race-safe on first concurrent creation).
        if not acquired:
            return {"state": "locked", "product": PRODUCT}
        # Dark-first: with no validated source there is nothing to publish, so
        # the no-op leaves NO side effects at all (the publication-protocol DDL
        # also requires the ingestion lineage tables a validated source implies).
        source = _latest_validated_source(conn)
        if source is None:
            conn.commit()
            return {"state": "no_source", "product": PRODUCT}
        install_schema(conn)
        source_run_id, source_package_id = source
        if not _relation_exists(conn, "sec_current_bond_security_v1"):
            conn.commit()
            return {"state": "no_securities", "product": PRODUCT}
        securities = _load_securities(conn)
        if not securities:
            conn.commit()
            return {"state": "no_securities", "product": PRODUCT}
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_observations", "product": PRODUCT}

        gates = {metric: gate_status(metric, conn) for metric in WAVE1_METRICS}
        gate_passed = {metric: status.passed for metric, status in gates.items()}
        prices = _eligible_latest_prices(conn, as_of)

        metric_rows: list[MetricRow] = []
        for sec in securities:
            metric_rows.extend(
                compute_security_metrics(
                    sec, prices.get(sec.security_id), as_of=as_of, gate_passed=gate_passed,
                )
            )

        fingerprint = _input_fingerprint(as_of, gates, securities, prices)
        result = _materialize(
            conn, as_of=as_of, source_run_id=source_run_id,
            source_package_id=source_package_id, code_revision=_code_revision(),
            fingerprint=fingerprint, security_count=len(securities),
            metric_rows=metric_rows,
        )
        conn.commit()
    return {"state": "ok", **result}

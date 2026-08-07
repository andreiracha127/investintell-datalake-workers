"""Compute+persist worker for the bond_metric_v1 product (source projection).

Publishes one complete ``bond_metric_v1`` snapshot through the shared
derived-publication protocol (prepared -> validated -> current pointer), under
an advisory lock. Values are PROJECTED from what the qualified inputs already
deliver — never recomputed from insufficient terms:

  * ``security_ytm``  — the yield the qualified price source itself publishes on
    the security's latest eligible observation at/before ``as_of``;
  * ``current_yield`` — coupon rate over the latest eligible clean price, as a
    decimal fraction (the app registry declares this metric a ``fraction``);
  * ``wal``           — years from ``as_of`` to maturity (bullet convention; the
    published terms carry no amortization schedule to weight);
  * ``security_ytw``  — honestly ``terms_insufficient``: no call schedule is
    published and the source does not deliver a worst-case yield.

This replaces the terms-driven engine path: the published universe carries no
day-count and no coupon schedule (0% coverage), so recomputing yields from
terms structurally produced ``terms_insufficient`` for every security while the
source's own yield lane sat unread. The standing rule applies: before
rebuilding a metric from terms, check whether the source already delivers it.

Null honesty is unchanged: a metric with no basis serves a typed status and a
NULL value, never a fabricated number. The Phase-10 qualification registry is
no longer consulted on the write path — the activation ceremony it encoded is
retired; provenance and the publication protocol carry the honesty.

Dark-mode semantics (unchanged): with NO validated source, NO published
security universe, or NO observation day to anchor ``as_of``, the worker is a
REPORTED no-op (``no_source`` / ``no_securities`` / ``no_observations``) and
publishes NOTHING.

Determinism: ``as_of`` is the chain's calc-date (or the latest observation
landing day when unpinned); no wall-clock value enters the payload. The
publication identity is ``uuid5(product | as_of | code_revision |
input_fingerprint)`` where the fingerprint digests every projected input row —
identical inputs replay the SAME publication byte-for-byte; changed inputs (or
changed code) mint a NEW build, keeping ``daily_chain.rollback_pointer``
meaningful.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import psycopg

from src.db import LOCK_BOND_METRICS, advisory_lock, connect, resolve_dsn

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_metric_v1.sql"
ELIGIBILITY_SCHEMA_PATH = ROOT / "schemas" / "bond_price_eligibility_v1.sql"
DERIVED_PROTOCOL_PATH = ROOT / "schemas" / "sec_derived_publications.sql"

PRODUCT = "bond_metric_v1"
# Bumped 2026-08-07: the projection now also publishes latest_price_pct and the
# analytic security_effective_duration. The fingerprint is salted with this
# string precisely so a semantics change alone mints a NEW build rather than
# replaying the old one.
METHODOLOGY_VERSION = "bond_metric_v1_source_projection_v2"

SERVED_METRICS = (
    "security_ytm", "security_ytw", "current_yield", "wal",
    "security_effective_duration", "latest_price_pct",
)

# Deterministic namespace for the metric publication identity (unchanged).
_NAMESPACE_PUBLICATION = UUID("b0d5ec00-0000-5000-a000-6d6574726963")

# Build stamps a deploy may inject (the container image carries no ``.git``).
_REVISION_ENV_VARS = ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA")


def _code_revision() -> str:
    for var in _REVISION_ENV_VARS:
        value = os.getenv(var)
        if value:
            return value.strip()
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        stamped = out.stdout.strip()
        if stamped:
            return stamped
    except Exception:
        pass
    return "unknown"


def install_schema(conn: psycopg.Connection) -> None:
    """Apply the publication protocol + product DDL idempotently.

    The eligibility predicate/view is re-applied only when its underlying
    observation table exists (in the chain it is installed by the pit_update
    stage's own workers before this one runs).
    """
    with conn.cursor() as cur:
        cur.execute(DERIVED_PROTOCOL_PATH.read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
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


# One row per security: published terms joined to the latest eligible price
# observation at/before ``as_of`` (deterministic tie-break: latest
# observation_date, then latest landing as_of, then observation_id — a replay
# is byte-identical). The ``observation_date <= as_of`` predicate is the
# no-look-ahead guard: the eligibility view itself knows nothing about the
# calc-date.
_INPUTS_SQL = """
CREATE TEMP TABLE _bond_metric_inputs ON COMMIT DROP AS
WITH latest_price AS (
    SELECT DISTINCT ON (e.security_id)
           e.security_id, o.price, o.ytm, e.observation_date
    FROM bond_price_eligibility_v1 e
    JOIN bond_price_observation o ON o.observation_id = e.observation_id
    WHERE e.is_eligible AND e.observation_date <= %(as_of)s
    ORDER BY e.security_id, e.observation_date DESC, e.as_of DESC, e.observation_id DESC
)
SELECT s.security_id, s.coupon_rate, s.coupon_type, s.maturity_date,
       p.price, p.ytm, p.observation_date
FROM sec_current_bond_security_v1 s
LEFT JOIN latest_price p USING (security_id)
"""

_EMPTY_INPUTS_SQL = """
CREATE TEMP TABLE _bond_metric_inputs ON COMMIT DROP AS
SELECT s.security_id, s.coupon_rate, s.coupon_type, s.maturity_date,
       NULL::numeric AS price, NULL::numeric AS ytm, NULL::date AS observation_date
FROM sec_current_bond_security_v1 s
"""

_FINGERPRINT_SQL = """
SELECT coalesce(
    md5(string_agg(
        md5(security_id::text
            || '|' || coalesce(coupon_rate::text, '')
            || '|' || coalesce(coupon_type, '')
            || '|' || coalesce(maturity_date::text, '')
            || '|' || coalesce(price::text, '')
            || '|' || coalesce(ytm::text, '')
            || '|' || coalesce(observation_date::text, '')),
        '' ORDER BY security_id)),
    'empty') AS digest,
    count(*) AS securities
FROM _bond_metric_inputs
"""

# --------------------------------------------------------------------------- #
# Analytic modified duration (security_effective_duration)
# --------------------------------------------------------------------------- #
# Closed form for a SEMIANNUAL fixed-rate bullet, priced from its own observed
# yield. Ported field-for-field from the panel's ``analytical_mod_dur``; the
# mathematics is copied, never imported across repositories.
#
#     y  = ytm / 2                      (observed yield is a decimal FRACTION)
#     c  = coupon_rate / 200            (published coupon is PERCENT of par)
#     n  = round(2 * years_to_maturity), floored at 1 half-periods
#     v  = (1 + y)^(-n)
#     ann= (1 - v) / y
#     px = c*ann + v
#     D  = (c * ((1+y)/y*ann - n*v/y) + n*v) / px      [Macaulay, half-periods]
#     modified_duration = (D / 2) / (1 + y)            [years]
#
# Two differences from the pandas original, both forced by SQL semantics and
# both fail-closed: numpy silently yields NaN on y = 0 and the caller's mask
# discards it, whereas Postgres RAISES on division by zero, so y = 0 is excluded
# by ``nullif`` BEFORE any division; and a non-finite result is never stored (the
# pandas version would have written NaN) but becomes a typed engine error. The
# published domain guard is the original's: -0.02 < ytm < 0.60 and a maturity
# strictly after settlement.
#
# Settlement is the PRICE observation date (trade-date settlement), matching the
# yield the duration is measured against. No price enters the formula: the bond
# is priced from its own coupon/yield/maturity, so a security with a yield but a
# stale price still gets an honest duration.
_DURATION_LATERALS = """
CROSS JOIN LATERAL (
    SELECT CASE
             WHEN i.ytm IS NULL OR i.observation_date IS NULL
               OR i.coupon_rate IS NULL OR i.maturity_date IS NULL
               OR lower(btrim(coalesce(i.coupon_type, ''))) <> 'fixed'
               OR i.maturity_date <= i.observation_date
               OR i.ytm <= -0.02 OR i.ytm >= 0.60
             THEN NULL
             ELSE (nullif(i.ytm, 0) / 2.0)::double precision
           END AS y,
           (i.coupon_rate / 200.0)::double precision AS c,
           CASE WHEN i.maturity_date IS NULL OR i.observation_date IS NULL THEN NULL
                ELSE greatest(round(2.0 * ((i.maturity_date - i.observation_date)::numeric
                                           / 365.25)), 1)::double precision END AS n
) dp
CROSS JOIN LATERAL (
    SELECT dp.y, dp.c, dp.n,
           CASE WHEN dp.y IS NOT NULL AND dp.n IS NOT NULL
                THEN power(1.0 + dp.y, -dp.n) END AS v
) dv
CROSS JOIN LATERAL (
    SELECT dv.y, dv.c, dv.n, dv.v,
           CASE WHEN dv.v IS NOT NULL THEN (1.0 - dv.v) / dv.y END AS ann
) da
CROSS JOIN LATERAL (
    SELECT da.y, da.c, da.n, da.v, da.ann,
           CASE WHEN da.ann IS NOT NULL AND da.c IS NOT NULL
                THEN da.c * da.ann + da.v END AS px
) dx
CROSS JOIN LATERAL (
    SELECT CASE WHEN dx.px IS NOT NULL AND dx.px > 0 THEN
             (((dx.c * ((1.0 + dx.y) / dx.y * dx.ann - dx.n * dx.v / dx.y) + dx.n * dx.v)
               / dx.px) / 2.0) / (1.0 + dx.y)
           END AS raw_dur
) dd
CROSS JOIN LATERAL (
    -- Fail-closed finiteness: NaN fails the self-equality test and an infinity
    -- fails the bounds, so neither can be stored as a value.
    SELECT CASE WHEN dd.raw_dur IS NOT NULL AND dd.raw_dur = dd.raw_dur
                 AND dd.raw_dur > 0
                 AND dd.raw_dur < 'Infinity'::double precision
                THEN dd.raw_dur::numeric END AS effective_duration
) de
"""

# The six metric projections in one set-based insert. Value present iff
# status='available' (the product CHECK also enforces it), and a typed reason
# code accompanies BOTH reason-bearing statuses (terms_insufficient and
# engine_typed_error) exactly as the product CHECK requires.
_METRIC_ROWS_SQL = f"""
INSERT INTO bond_metric_v1
    (publication_id, security_id, metric_id, value, status, engine_error_code, as_of, provenance)
SELECT %(pub)s, i.security_id, m.metric_id, m.value, m.status,
       CASE WHEN m.status IN ('terms_insufficient', 'engine_typed_error') THEN m.reason END,
       %(as_of)s,
       jsonb_build_object('origin', m.origin, 'methodology_version', %(methodology)s::text,
                          'code_revision', %(code_revision)s::text)
FROM _bond_metric_inputs i
{_DURATION_LATERALS}
CROSS JOIN LATERAL (
    VALUES
        ('security_ytm',
         CASE WHEN i.ytm IS NOT NULL THEN i.ytm END,
         CASE WHEN i.ytm IS NOT NULL THEN 'available' ELSE 'no_eligible_price' END,
         NULL,
         'qualified_price_source'),
        ('security_ytw',
         NULL::numeric,
         'terms_insufficient',
         'call_schedule_unpublished',
         'derived_terms'),
        ('current_yield',
         CASE WHEN i.coupon_rate IS NOT NULL AND i.price IS NOT NULL AND i.price > 0
              THEN i.coupon_rate / i.price END,
         CASE WHEN i.price IS NULL OR i.price <= 0 THEN 'no_eligible_price'
              WHEN i.coupon_rate IS NULL THEN 'terms_insufficient'
              ELSE 'available' END,
         'coupon_rate_unpublished',
         'derived_terms'),
        ('wal',
         CASE WHEN i.maturity_date IS NOT NULL
              THEN (i.maturity_date - %(as_of)s::date) / 365.0 END,
         CASE WHEN i.maturity_date IS NOT NULL THEN 'available'
              ELSE 'terms_insufficient' END,
         'maturity_unpublished',
         'derived_terms'),
        -- Analytic modified duration (years). OAS/z-spread and a callable YTW
        -- stay absent: no validated model publishes them here.
        ('security_effective_duration',
         de.effective_duration,
         CASE WHEN de.effective_duration IS NOT NULL THEN 'available'
              WHEN i.ytm IS NULL OR i.observation_date IS NULL THEN 'no_eligible_price'
              WHEN i.coupon_rate IS NULL THEN 'terms_insufficient'
              WHEN lower(btrim(coalesce(i.coupon_type, ''))) <> 'fixed' THEN 'terms_insufficient'
              WHEN i.maturity_date IS NULL THEN 'terms_insufficient'
              ELSE 'engine_typed_error' END,
         CASE WHEN i.coupon_rate IS NULL THEN 'coupon_rate_unpublished'
              WHEN lower(btrim(coalesce(i.coupon_type, ''))) <> 'fixed' THEN 'coupon_type_unsupported'
              WHEN i.maturity_date IS NULL THEN 'maturity_unpublished'
              WHEN i.maturity_date <= i.observation_date THEN 'settlement_after_maturity'
              WHEN i.ytm <= -0.02 OR i.ytm >= 0.60 OR i.ytm = 0 THEN 'yield_out_of_domain'
              ELSE 'non_finite_result' END,
         'analytic_modified_duration'),
        -- The eligible latest CLEAN price in percent of par, as its own metric
        -- row so the value is auditable next to the yields it prices. (No literal
        -- percent sign in this string: psycopg would read it as a placeholder.)
        ('latest_price_pct',
         CASE WHEN i.price IS NOT NULL AND i.price > 0 THEN i.price END,
         CASE WHEN i.price IS NOT NULL AND i.price > 0 THEN 'available'
              ELSE 'no_eligible_price' END,
         NULL,
         'qualified_price_source')
) AS m(metric_id, value, status, reason, origin)
ON CONFLICT (publication_id, security_id, metric_id) DO NOTHING
"""


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
            (publication_id, fingerprint, as_of, security_count,
             security_count * len(SERVED_METRICS)),
        )
        pinned = conn.execute(
            "SELECT input_fingerprint, as_of_date FROM bond_metric_v1_builds WHERE publication_id=%s",
            (publication_id,),
        ).fetchone()
        if pinned[0] != fingerprint:
            raise RuntimeError(f"{PRODUCT} publication already pinned to fingerprint {pinned[0]}")
        if pinned[1] != as_of:
            raise RuntimeError(f"{PRODUCT} publication already pinned to as_of {pinned[1]}")
        conn.execute(_METRIC_ROWS_SQL, {
            "pub": publication_id, "as_of": as_of,
            "methodology": METHODOLOGY_VERSION, "code_revision": code_revision,
        })
        conn.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))

    current = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s", (PRODUCT,)
    ).fetchone()
    if current is None or current[0] != publication_id:
        conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))

    status_counts = {status: 0 for status in (
        "available", "no_eligible_price", "terms_insufficient", "engine_typed_error")}
    for status, count in conn.execute(
        "SELECT status, count(*) FROM bond_metric_v1 WHERE publication_id=%s GROUP BY status",
        (publication_id,),
    ).fetchall():
        status_counts[status] = count
    row_count = sum(status_counts.values())
    return {
        "product": PRODUCT,
        "publication_id": str(publication_id),
        "as_of": as_of.isoformat(),
        "securities": security_count,
        "rows": row_count,
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
        universe = conn.execute(
            "SELECT count(*) FROM sec_current_bond_security_v1"
        ).fetchone()[0]
        if not universe:
            conn.commit()
            return {"state": "no_securities", "product": PRODUCT}
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_observations", "product": PRODUCT}

        if (_relation_exists(conn, "bond_price_eligibility_v1")
                and _relation_exists(conn, "bond_price_observation")):
            conn.execute(_INPUTS_SQL, {"as_of": as_of})
        else:
            conn.execute(_EMPTY_INPUTS_SQL)
        digest, security_count = conn.execute(_FINGERPRINT_SQL).fetchone()
        # The protocol pins sha256 fingerprints (64 hex); salt the row digest
        # with the product identity and methodology so a semantics change alone
        # also mints a new build.
        fingerprint = hashlib.sha256(
            f"{PRODUCT}|{as_of.isoformat()}|{METHODOLOGY_VERSION}|{digest}".encode()
        ).hexdigest()

        result = _materialize(
            conn, as_of=as_of, source_run_id=source_run_id,
            source_package_id=source_package_id, code_revision=_code_revision(),
            fingerprint=fingerprint, security_count=security_count,
        )
        conn.commit()
    return {"state": "ok", **result}

"""Rating-history materializer: point-in-time rating history + an explicit LICENSE GATE.

Two concerns live here, mirroring the Task 3/4 shapes:

* A **pure**, DB-free resolver (``resolve_ratings``) that folds immutable rating
  observations into published rows, validating the half-open ``[valid_from,
  valid_to)`` PIT window (Task-3 convention) and the subject identity.  The agency
  is carried as an OPAQUE ``agency_code``; no agency name is ever surfaced.

* The **publication** wiring (``materialize``) that lands one complete
  ``bond_rating_history_v1`` snapshot through the shared
  ``sec_derived_publications`` protocol (prepared -> validated -> current
  pointer), pinned by a product-salted input fingerprint so reruns are idempotent
  and a partial build can never become current.

**License gate (fail-closed, two layers).**
  1. Input: every observation carries a mandatory ``licensed_source_ref`` (the
     DDL forbids NULL).
  2. Product: ``materialize(..., license_verified=...)``.  When the license is NOT
     verified, the WHOLE product is published as ``product_state='not_applicable'``
     with ``reason_code='no_licensed_source'`` and ZERO rating rows — never data
     without a verified license.  Rating snapshot rows exist ONLY under a verified
     license (the snapshot column ``license_verified`` is CHECK-forced true).

No value produced here reaches any production surface in this increment (Global
Constraint #3); the rating source is synthetic (fixtures only) and no production
rating license is authorized by this shipment.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

from src.bonds.errors import BondError

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_rating_history_v1.sql"
PRODUCT = "bond_rating_history_v1"
METHODOLOGY_VERSION = "bond_rating_history_v1"

# Deterministic namespace for the publication identity (distinct constant).
_NAMESPACE_PUBLICATION = UUID("b0d5ec00-0000-5000-a000-726174696e31")

NO_LICENSE_REASON = "no_licensed_source"


# ---------------------------------------------------------------------------
# Pure resolver
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RatingObservationInput:
    """One raw PIT rating observation as landed (fixture-shaped)."""

    observation_id: str
    subject_kind: str  # 'security' | 'issuer'
    agency_code: str
    rating: str
    valid_from: date
    valid_to: date | None = None
    security_id: UUID | None = None
    issuer_id: str | None = None
    watch: str | None = None
    outlook: str | None = None
    licensed_source_ref: str = ""
    source_lineage: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedRating:
    subject_kind: str
    subject_ref: str
    security_id: UUID | None
    issuer_id: str | None
    agency_code: str
    rating: str
    watch: str | None
    outlook: str | None
    valid_from: date
    valid_to: date | None
    licensed_source_ref: str
    observation_id: str


def _subject_ref(subject_kind: str, security_id: object, issuer_id: object) -> str:
    if subject_kind == "security":
        if security_id is None:
            raise BondError("missing_security_id", {})
        return str(security_id)
    if subject_kind == "issuer":
        if issuer_id is None:
            raise BondError("missing_issuer_id", {})
        return str(issuer_id)
    raise BondError("invalid_subject_kind", {"subject_kind": subject_kind})


def resolve_ratings(observations: Iterable[RatingObservationInput]) -> tuple[ResolvedRating, ...]:
    """Fold rating observations into validated PIT rows (half-open windows)."""
    resolved: list[ResolvedRating] = []
    for obs in observations:
        if not obs.licensed_source_ref:
            # Defense-in-depth: the DDL already forbids a NULL/empty license ref.
            raise BondError("missing_licensed_source_ref", {"observation_id": obs.observation_id})
        if obs.valid_to is not None and obs.valid_to <= obs.valid_from:
            raise BondError("invalid_rating_window", {"observation_id": obs.observation_id})
        subject_ref = _subject_ref(obs.subject_kind, obs.security_id, obs.issuer_id)
        resolved.append(
            ResolvedRating(
                subject_kind=obs.subject_kind,
                subject_ref=subject_ref,
                security_id=obs.security_id,
                issuer_id=obs.issuer_id,
                agency_code=obs.agency_code,
                rating=obs.rating,
                watch=obs.watch,
                outlook=obs.outlook,
                valid_from=obs.valid_from,
                valid_to=obs.valid_to,
                licensed_source_ref=obs.licensed_source_ref,
                observation_id=str(obs.observation_id),
            )
        )
    return tuple(resolved)


# ---------------------------------------------------------------------------
# Loader (fixture rows -> immutable observation table)
# ---------------------------------------------------------------------------
def load_rating_observations(
    conn: psycopg.Connection,
    observations: Iterable[RatingObservationInput],
    *,
    as_of: date,
    source_run_id: UUID,
) -> dict[str, Any]:
    """Land raw rating observations into the immutable ``bond_rating_observation`` table."""
    inserted = 0
    for obs in observations:
        lineage = dict(obs.source_lineage)
        if not lineage:
            raise BondError("missing_source_lineage", {"observation_id": obs.observation_id})
        conn.execute(
            "INSERT INTO bond_rating_observation"
            "(observation_id, as_of, source_run_id, subject_kind, security_id, issuer_id, agency_code, "
            " rating, watch, outlook, valid_from, valid_to, licensed_source_ref, source_lineage) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (observation_id) DO NOTHING",
            (
                obs.observation_id, as_of, source_run_id, obs.subject_kind, obs.security_id,
                obs.issuer_id, obs.agency_code, obs.rating, obs.watch, obs.outlook,
                obs.valid_from, obs.valid_to, obs.licensed_source_ref, Jsonb(lineage),
            ),
        )
        inserted += 1
    return {"observations": inserted}


# ---------------------------------------------------------------------------
# Publication wiring (sec_derived_publications protocol)
# ---------------------------------------------------------------------------
def install_schema(conn: psycopg.Connection) -> None:
    """Apply the publication protocol + bond_rating_history_v1 DDL idempotently."""
    with conn.cursor() as cur:
        cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def publication_id_for(as_of: date, code_revision: str, *, license_verified: bool) -> UUID:
    # The license posture is part of the publication identity so a not_applicable
    # build and a licensed build for the same as_of/revision are distinct
    # publications (the product can never silently flip data in place).
    salt = "licensed" if license_verified else "unlicensed"
    return uuid5(_NAMESPACE_PUBLICATION, f"{PRODUCT}|{as_of.isoformat()}|{code_revision}|{salt}")


def _load_observations(conn: psycopg.Connection, as_of: date) -> list[RatingObservationInput]:
    rows = conn.execute(
        "SELECT observation_id, subject_kind, security_id, issuer_id, agency_code, rating, watch, "
        "outlook, valid_from, valid_to, licensed_source_ref "
        "FROM bond_rating_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    result: list[RatingObservationInput] = []
    for r in rows:
        result.append(
            RatingObservationInput(
                observation_id=str(r[0]), subject_kind=r[1], security_id=r[2], issuer_id=r[3],
                agency_code=r[4], rating=r[5], watch=r[6], outlook=r[7], valid_from=r[8],
                valid_to=r[9], licensed_source_ref=r[10], source_lineage={"loaded": True},
            )
        )
    return result


def _input_fingerprint(as_of: date, observations: Sequence[RatingObservationInput]) -> str:
    parts = [f"{PRODUCT}|{as_of.isoformat()}"]
    for obs in sorted(observations, key=lambda o: str(o.observation_id)):
        parts.append(
            "|".join(
                str(x) for x in (
                    obs.observation_id, obs.subject_kind, obs.security_id, obs.issuer_id,
                    obs.agency_code, obs.rating, obs.watch, obs.outlook,
                    obs.valid_from.isoformat(), obs.valid_to.isoformat() if obs.valid_to else "",
                    obs.licensed_source_ref,
                )
            )
        )
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def materialize(
    conn: psycopg.Connection,
    *,
    as_of: date,
    source_run_id: UUID,
    source_package_id: UUID,
    code_revision: str,
    license_verified: bool,
) -> dict[str, Any]:
    """Prepare -> build -> validate -> current, idempotently, for one as_of.

    The LICENSE GATE decides the product shape:
      * ``license_verified=True``  -> product_state='active', rating rows published.
      * ``license_verified=False`` -> product_state='not_applicable',
        reason_code='no_licensed_source', ZERO rating rows (whole product).

    A partial/failed build never becomes current (the shared publication protocol's
    fail-closed guards enforce prepared->validated->current).
    """
    publication_id = publication_id_for(as_of, code_revision, license_verified=license_verified)
    observations = _load_observations(conn, as_of)
    fingerprint = _input_fingerprint(as_of, observations)

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

    product_state = "active" if license_verified else "not_applicable"
    reason_code = None if license_verified else NO_LICENSE_REASON
    published = 0
    if lifecycle == "prepared":
        conn.execute(
            "INSERT INTO bond_rating_history_v1_builds"
            "(publication_id,input_fingerprint,as_of_date,observation_input_count,product_state,"
            " reason_code,license_verified) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (publication_id) DO NOTHING",
            (publication_id, fingerprint, as_of, len(observations), product_state, reason_code,
             license_verified),
        )
        pinned = conn.execute(
            "SELECT input_fingerprint, as_of_date FROM bond_rating_history_v1_builds WHERE publication_id=%s",
            (publication_id,),
        ).fetchone()
        if pinned[0] != fingerprint:
            raise RuntimeError(f"{PRODUCT} publication already pinned to fingerprint {pinned[0]}")
        if pinned[1] != as_of:
            raise RuntimeError(f"{PRODUCT} publication already pinned to as_of {pinned[1]}")

        # LICENSE GATE: rating rows are written ONLY when the license is verified.
        # Without a verified license the product is published EMPTY (not_applicable).
        if license_verified:
            for row in resolve_ratings(observations):
                conn.execute(
                    "INSERT INTO bond_rating_history_v1"
                    "(publication_id,source_run_id,subject_kind,subject_ref,security_id,issuer_id,"
                    " agency_code,rating,watch,outlook,valid_from,valid_to,license_verified,"
                    " licensed_source_ref,measured_at,provenance) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (publication_id,subject_kind,subject_ref,agency_code,valid_from) DO NOTHING",
                    (
                        publication_id, source_run_id, row.subject_kind, row.subject_ref,
                        row.security_id, row.issuer_id, row.agency_code, row.rating, row.watch,
                        row.outlook, row.valid_from, row.valid_to, True, row.licensed_source_ref,
                        as_of, Jsonb({"resolver": "ratings", "methodology_version": METHODOLOGY_VERSION}),
                    ),
                )
                published += 1
        conn.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
    else:
        published = conn.execute(
            "SELECT count(*) FROM bond_rating_history_v1 WHERE publication_id=%s", (publication_id,)
        ).fetchone()[0]

    current = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s", (PRODUCT,)
    ).fetchone()
    if current is None or current[0] != publication_id:
        conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))

    return {
        "product": PRODUCT,
        "publication_id": str(publication_id),
        "as_of": as_of.isoformat(),
        "product_state": product_state,
        "reason": reason_code,
        "license_verified": license_verified,
        "published": published,
        "state": "current",
    }

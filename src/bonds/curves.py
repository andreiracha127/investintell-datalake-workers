"""Spot/par curve materializer: point-in-time curve observations + one snapshot.

Three concerns live here, mirroring the Task 3/4 security-master and
price-observation shapes:

* A **pure**, DB-free resolver (``resolve_curves``) that folds immutable curve
  observations into validated curves.  A curve is published only when it is
  NON-DEGENERATE: at least two nodes, strictly increasing in tenor, every rate
  finite, every tenor strictly positive, and a supported interpolation
  (``'linear'`` only for now).  A degenerate observed curve carries a typed
  ``curve_state='degenerate'`` with a stable ``reason_code`` and is NEVER
  published — absence is honest, never repaired.

* A **SpotCurve bridge** (``spot_curve_from_snapshot``) that reads a published
  curve's nodes through the current pointer and constructs a
  :class:`src.bonds.pricing.SpotCurve` WITHOUT adaptation, so the curve product
  feeds the Task-3 pricing motor directly (linear interpolation in the zero rate,
  flat outside the node range; the ACT/365F time basis is the consumer's).

* The **publication** wiring (``materialize``) that lands one complete
  ``bond_curve_v1`` snapshot (curves + typed nodes) through the shared
  ``sec_derived_publications`` protocol (prepared -> validated -> current
  pointer), pinned by a product-salted input fingerprint so reruns are idempotent
  and a partial build can never become current.

Curve identity (documented once here and mirrored in ``schemas/bond_curve_v1.sql``):

    curve_id = uuid5(NAMESPACE_BOND_CURVE, curve_key)

where ``curve_key = currency || '|' || curve_date (ISO) || '|' || curve_type``.
The key never depends on the as-of date, source run, or observed rates.

No value produced here reaches any production surface in this increment (Global
Constraint #3); the curve source is synthetic (fixtures only).
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import date
from numbers import Real
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

from src.bonds.errors import BondError
from src.bonds.pricing import SpotCurve

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_curve_v1.sql"
PRODUCT = "bond_curve_v1"
METHODOLOGY_VERSION = "bond_curve_v1"

# Deterministic namespace for curve_id (distinct constant; do not reuse).
NAMESPACE_BOND_CURVE = UUID("b0d5ec00-0000-5000-a000-637572766531")
# Deterministic namespace for the publication identity.
_NAMESPACE_PUBLICATION = UUID("b0d5ec00-0000-5000-a000-637270756231")

# Supported interpolation vocabulary (declared attribute of the snapshot).
SUPPORTED_INTERPOLATIONS = frozenset({"linear"})
# Supported curve types.
SUPPORTED_CURVE_TYPES = frozenset({"spot", "par"})
# A published curve needs at least this many strictly increasing nodes.
MIN_NODES = 2


# ---------------------------------------------------------------------------
# Pure resolver
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CurveObservationInput:
    """One raw curve observation as landed (fixture-shaped)."""

    observation_id: str
    curve_date: date
    currency: str
    curve_type: str
    interpolation: str
    nodes: Sequence[Sequence[object]]  # raw ((tenor, rate), ...)
    source_lineage: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CurveNode:
    tenor_years: float
    rate: float


@dataclass(frozen=True)
class ResolvedCurve:
    curve_id: UUID
    curve_key: str
    currency: str
    curve_date: date
    curve_type: str
    interpolation: str
    nodes: tuple[CurveNode, ...]
    observation_id: str


@dataclass(frozen=True)
class RejectedCurve:
    observation_id: str
    curve_state: str  # 'degenerate'
    reason_code: str


@dataclass(frozen=True)
class CurveResolutionResult:
    curves: tuple[ResolvedCurve, ...]
    rejected: tuple[RejectedCurve, ...]


def curve_key_for(currency: str, curve_date: date, curve_type: str) -> str:
    return f"{currency}|{curve_date.isoformat()}|{curve_type}"


def curve_id_for(currency: str, curve_date: date, curve_type: str) -> UUID:
    return uuid5(NAMESPACE_BOND_CURVE, curve_key_for(currency, curve_date, curve_type))


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _validate_nodes(raw_nodes: Sequence[Sequence[object]]) -> tuple[tuple[CurveNode, ...] | None, str | None]:
    """Validate a raw node set into strictly-increasing typed nodes, or a reason.

    Returns ``(nodes, None)`` for a valid curve or ``(None, reason_code)`` for a
    typed-degenerate one.  The first failing condition wins (deterministic).
    """
    parsed: list[tuple[float, float]] = []
    for node in raw_nodes:
        if not isinstance(node, (list, tuple)) or len(node) != 2:
            return None, "invalid_node_shape"
        tenor = _finite(node[0])
        rate = _finite(node[1])
        if tenor is None:
            return None, "non_finite_tenor"
        if tenor <= 0:
            return None, "non_positive_tenor"
        if rate is None:
            return None, "non_finite_rate"
        parsed.append((tenor, rate))
    if len(parsed) < MIN_NODES:
        return None, "too_few_nodes"
    ordered = sorted(parsed, key=lambda n: n[0])
    previous: float | None = None
    for tenor, _rate in ordered:
        if previous is not None and tenor <= previous:
            return None, "tenor_not_increasing"
        previous = tenor
    return tuple(CurveNode(tenor_years=t, rate=r) for t, r in ordered), None


def resolve_curves(observations: Iterable[CurveObservationInput]) -> CurveResolutionResult:
    """Fold curve observations into validated curves + typed-degenerate rejects."""
    curves: list[ResolvedCurve] = []
    rejected: list[RejectedCurve] = []
    for obs in observations:
        if obs.curve_type not in SUPPORTED_CURVE_TYPES:
            rejected.append(RejectedCurve(str(obs.observation_id), "degenerate", "unsupported_curve_type"))
            continue
        if obs.interpolation not in SUPPORTED_INTERPOLATIONS:
            rejected.append(RejectedCurve(str(obs.observation_id), "degenerate", "unsupported_interpolation"))
            continue
        nodes, reason = _validate_nodes(obs.nodes)
        if nodes is None:
            rejected.append(RejectedCurve(str(obs.observation_id), "degenerate", reason or "invalid_nodes"))
            continue
        curves.append(
            ResolvedCurve(
                curve_id=curve_id_for(obs.currency, obs.curve_date, obs.curve_type),
                curve_key=curve_key_for(obs.currency, obs.curve_date, obs.curve_type),
                currency=obs.currency,
                curve_date=obs.curve_date,
                curve_type=obs.curve_type,
                interpolation=obs.interpolation,
                nodes=nodes,
                observation_id=str(obs.observation_id),
            )
        )
    return CurveResolutionResult(curves=tuple(curves), rejected=tuple(rejected))


# ---------------------------------------------------------------------------
# SpotCurve bridge (published snapshot -> pricing.SpotCurve, no adaptation)
# ---------------------------------------------------------------------------
def spot_curve_from_snapshot(conn: psycopg.Connection, curve_id: UUID) -> SpotCurve:
    """Build a :class:`SpotCurve` from the CURRENT published curve ``curve_id``.

    Reads the published nodes through the current pointer (never the raw
    observation table) and constructs a ``SpotCurve`` unadapted.  Raises
    ``curve_not_published`` when the id has no node under the current pointer.
    """
    rows = conn.execute(
        "SELECT tenor_years, rate FROM sec_current_bond_curve_node_v1 "
        "WHERE curve_id=%s ORDER BY tenor_years",
        (curve_id,),
    ).fetchall()
    if not rows:
        raise BondError("curve_not_published", {"curve_id": str(curve_id)})
    nodes = tuple((float(t), float(r)) for t, r in rows)
    return SpotCurve(nodes=nodes)


# ---------------------------------------------------------------------------
# Loader (fixture rows -> immutable observation table)
# ---------------------------------------------------------------------------
def _jsonable_nodes(nodes: Sequence[Sequence[object]]) -> list[list[object]]:
    out: list[list[object]] = []
    for node in nodes:
        pair = list(node)
        out.append([_json_number(pair[0]) if len(pair) > 0 else None,
                    _json_number(pair[1]) if len(pair) > 1 else None])
    return out


def _json_number(value: object) -> object:
    """Represent a node component in JSON: a finite float stays a number; a
    non-finite float is stored as its string token so the raw observation is
    preserved losslessly for the downstream typed rejection."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    return value


def load_curve_observations(
    conn: psycopg.Connection,
    observations: Iterable[CurveObservationInput],
    *,
    as_of: date,
    source_run_id: UUID,
) -> dict[str, Any]:
    """Land raw curve observations into the immutable ``bond_curve_observation`` table."""
    inserted = 0
    for obs in observations:
        lineage = dict(obs.source_lineage)
        if not lineage:
            raise BondError("missing_source_lineage", {"observation_id": obs.observation_id})
        conn.execute(
            "INSERT INTO bond_curve_observation"
            "(observation_id, as_of, curve_date, source_run_id, currency, curve_type, interpolation, "
            " nodes, source_lineage) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (observation_id) DO NOTHING",
            (
                obs.observation_id, as_of, obs.curve_date, source_run_id, obs.currency,
                obs.curve_type, obs.interpolation, Jsonb(_jsonable_nodes(obs.nodes)), Jsonb(lineage),
            ),
        )
        inserted += 1
    return {"observations": inserted}


# ---------------------------------------------------------------------------
# Publication wiring (sec_derived_publications protocol)
# ---------------------------------------------------------------------------
def install_schema(conn: psycopg.Connection) -> None:
    """Apply the publication protocol + bond_curve_v1 DDL idempotently."""
    with conn.cursor() as cur:
        cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def publication_id_for(as_of: date, code_revision: str) -> UUID:
    return uuid5(_NAMESPACE_PUBLICATION, f"{PRODUCT}|{as_of.isoformat()}|{code_revision}")


def _load_observations(conn: psycopg.Connection, as_of: date) -> list[CurveObservationInput]:
    rows = conn.execute(
        "SELECT observation_id, curve_date, currency, curve_type, interpolation, nodes "
        "FROM bond_curve_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    result: list[CurveObservationInput] = []
    for r in rows:
        result.append(
            CurveObservationInput(
                observation_id=str(r[0]), curve_date=r[1], currency=r[2], curve_type=r[3],
                interpolation=r[4], nodes=_nodes_from_json(r[5]), source_lineage={"loaded": True},
            )
        )
    return result


def _nodes_from_json(payload: object) -> tuple[tuple[object, object], ...]:
    """Rebuild raw node pairs from JSON, restoring non-finite float tokens."""
    if not isinstance(payload, list):
        return ()
    out: list[tuple[object, object]] = []
    for node in payload:
        if isinstance(node, (list, tuple)) and len(node) == 2:
            out.append((_from_json_number(node[0]), _from_json_number(node[1])))
        else:
            out.append((node, None))
    return tuple(out)


def _from_json_number(value: object) -> object:
    if isinstance(value, str):
        try:
            return float(value)  # restores 'nan'/'inf'/'-inf' tokens
        except ValueError:
            return value
    return value


def _input_fingerprint(as_of: date, observations: Sequence[CurveObservationInput]) -> str:
    parts = [f"{PRODUCT}|{as_of.isoformat()}"]
    for obs in sorted(observations, key=lambda o: str(o.observation_id)):
        node_repr = ";".join(f"{n[0]}:{n[1]}" for n in obs.nodes)
        parts.append(
            "|".join(
                str(x) for x in (
                    obs.observation_id, obs.curve_date.isoformat(), obs.currency,
                    obs.curve_type, obs.interpolation, node_repr,
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
) -> dict[str, Any]:
    """Prepare -> build -> validate -> current, idempotently, for one as_of.

    A partial/failed build never becomes current: the snapshot rows are written
    only while the publication is 'prepared', the pin is verified before validate,
    and the current pointer advances only after validation (the shared publication
    protocol's fail-closed guards enforce this).
    """
    publication_id = publication_id_for(as_of, code_revision)
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

    result = resolve_curves(observations)
    if lifecycle == "prepared":
        conn.execute(
            "INSERT INTO bond_curve_v1_builds"
            "(publication_id,input_fingerprint,as_of_date,observation_input_count) "
            "VALUES(%s,%s,%s,%s) ON CONFLICT (publication_id) DO NOTHING",
            (publication_id, fingerprint, as_of, len(observations)),
        )
        pinned = conn.execute(
            "SELECT input_fingerprint, as_of_date FROM bond_curve_v1_builds WHERE publication_id=%s",
            (publication_id,),
        ).fetchone()
        if pinned[0] != fingerprint:
            raise RuntimeError(f"{PRODUCT} publication already pinned to fingerprint {pinned[0]}")
        if pinned[1] != as_of:
            raise RuntimeError(f"{PRODUCT} publication already pinned to as_of {pinned[1]}")
        for curve in result.curves:
            conn.execute(
                "INSERT INTO bond_curve_v1"
                "(publication_id,source_run_id,curve_id,curve_key,currency,curve_date,curve_type,"
                " interpolation,node_count,measured_at,provenance) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (publication_id,curve_id) DO NOTHING",
                (
                    publication_id, source_run_id, curve.curve_id, curve.curve_key, curve.currency,
                    curve.curve_date, curve.curve_type, curve.interpolation, len(curve.nodes), as_of,
                    Jsonb({"resolver": "curves", "methodology_version": METHODOLOGY_VERSION,
                           "observation_id": curve.observation_id}),
                ),
            )
            for ordinal, node in enumerate(curve.nodes):
                conn.execute(
                    "INSERT INTO bond_curve_node_v1"
                    "(publication_id,curve_id,node_ordinal,tenor_years,rate) "
                    "VALUES(%s,%s,%s,%s,%s) "
                    "ON CONFLICT (publication_id,curve_id,tenor_years) DO NOTHING",
                    (publication_id, curve.curve_id, ordinal, node.tenor_years, node.rate),
                )
        conn.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))

    current = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s", (PRODUCT,)
    ).fetchone()
    if current is None or current[0] != publication_id:
        conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))

    return {
        "product": PRODUCT,
        "publication_id": str(publication_id),
        "as_of": as_of.isoformat(),
        "curves": len(result.curves),
        "rejected": len(result.rejected),
        "state": "current",
    }

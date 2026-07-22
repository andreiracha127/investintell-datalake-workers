"""Price/trade observation materializer: point-in-time observations + two lanes.

Three concerns live here, mirroring the Task 3 security-master shape:

* A **pure**, DB-free resolver (``resolve_price_observations``) that folds raw
  price/trade observation rows into resolved records.  It reuses Task 2's
  ``build_observed_panel_rows`` for the daily-key (ambiguity) state and per-field
  states, Task 2's ``normalize_cusip9`` for lossless identity, and Task 3's
  ``NAMESPACE_BOND_SECURITY`` so a price observation's ``security_id`` equals the
  security master's ``security_id`` for the same CUSIP-anchored identity.  An
  observation whose CUSIP does not qualify carries no fabricated ``security_id``
  — it keeps its raw identifier and an ``unresolved`` state with the
  ``IdentifierState`` reason.  Duplicate ``(security_id, observation_date)`` rows
  from distinct source rows are ALL retained and marked
  ``duplicate_in_matching_cohort`` — never an arbitrary winner.

* A **matcher-facing index builder** (``build_pit_index`` /
  ``pit_observation_rows`` / ``match_fund_holdings_asof``) that feeds Task 2's
  ``ObservationIndex.from_rows`` for the no-look-ahead matching library.  It is
  the structural lane gate: it REFUSES any row stamped with a lane other than
  ``fund_asof`` (e.g. the informative ``latest`` lane), mirroring the library's
  ``isinstance(..., ObservationIndex)`` refusal.  The matcher only ever accepts a
  point-in-time index, never latest-lane output.

* The **publication** wiring (``materialize``) that lands one complete
  ``bond_price_observation_v1`` snapshot through the shared
  ``sec_derived_publications`` protocol (prepared -> validated -> current
  pointer), pinned by a product-salted input fingerprint so reruns are idempotent
  and a partial build can never become current.  Two published lanes read from
  that snapshot and are NEVER interchangeable (see the DDL):
  ``bond_price_latest_v1`` (informative) and ``bond_price_fund_asof_v1(as_of)``
  (point-in-time, no look-ahead, STALE at age >= 31 days).
"""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
import math
from numbers import Real
from pathlib import Path
from typing import Any, Iterable, Mapping
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

from src.bonds.errors import BondError
from src.bonds.matching import HoldingRecord, MatchResult, ObservationIndex, match_holdings_asof
from src.bonds.panel_states import build_observed_panel_rows
from src.bonds.security_master import NAMESPACE_BOND_SECURITY
from src.bonds.states import FieldState, IdentifierState

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_price_observations_v1.sql"
PRODUCT = "bond_price_observation_v1"
METHODOLOGY_VERSION = "bond_price_observation_v1"

# Deterministic namespace for the price publication identity (distinct constant).
_NAMESPACE_PUBLICATION = UUID("b0d5ec00-0000-5000-a000-707269636531")

# Typed, closed vocabularies (mirrored by the DDL CHECK constraints).
_PRICE_TYPES = frozenset({"trade", "evaluated", "model", "not_reported"})
_ACCRUED_TREATMENTS = frozenset({"clean", "dirty", "not_reported"})

# Lane discriminators. The point-in-time matcher accepts ONLY the PIT lane (or
# unstamped raw observation rows); the informative latest lane is refused.
PIT_LANE = "fund_asof"
LATEST_LANE = "latest"

# Age (in days) at/above which a fund_asof observation is STALE. Kept identical to
# the matching library's threshold (match_holding: ``if age >= 31: STALE``).
STALE_AGE_DAYS = 31


# ---------------------------------------------------------------------------
# Pure resolver
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PriceObservationInput:
    """One raw price/trade observation as landed (fixture-shaped)."""

    observation_id: str
    observation_date: date
    cusip9_input: object = None
    price: object = None
    price_type: object = None
    accrued_treatment: object = None
    ytm: object = None
    db_type: object = None
    source_lineage: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedPriceObservation:
    observation_id: str
    observation_date: date
    security_id: UUID | None
    identity_state: str  # 'resolved' | 'unresolved'
    identity_reason_code: str | None
    cusip9_input: object
    normalized_cusip9: str | None
    price: object
    price_state: str
    price_type: str
    accrued_treatment: str
    ytm: object
    db_type: object
    db_type_state: str
    is_144a: bool | None
    daily_key_state: str
    source_row_number: int
    source_lineage: Mapping[str, Any]


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _is_144a(db_type: object, db_type_state: str) -> bool | None:
    """db_type == 3 => 144A, per matching._is_144a. Only when present & integral."""
    if db_type_state != FieldState.PRESENT.value:
        return None
    number = _finite(db_type)
    if number is None or not number.is_integer():
        return None
    return int(number) == 3


def _typed(value: object, allowed: frozenset[str], error_code: str) -> str:
    """Coerce a typed vocabulary field: absent => 'not_reported'; unknown => error."""
    if value is None:
        return "not_reported"
    text = str(value)
    if text not in allowed:
        raise BondError(error_code, {"value": value})
    return text


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise BondError("invalid_observation_date", {"value": value})


def resolve_price_observations(
    rows: Iterable[PriceObservationInput],
) -> list[ResolvedPriceObservation]:
    """Fold raw observations into resolved records with identity + ambiguity state.

    The daily-key (ambiguity) state is derived by ``build_observed_panel_rows`` over
    a self-cohort of every observed CUSIP, so duplicate ``(cusip, date)`` keys —
    hence duplicate ``(security_id, date)`` keys — are marked
    ``duplicate_in_matching_cohort``.  Identity resolution is CUSIP-anchored via the
    Task 3 namespace so it agrees with the security master.
    """
    materialized = list(rows)
    panel_input: list[dict[str, object]] = []
    for r in materialized:
        panel_input.append(
            {
                "cusip_id": r.cusip9_input,
                "trd_exctn_dt": r.observation_date,
                "pr": r.price,
                "ytm": r.ytm,
                "db_type": r.db_type,
            }
        )
    cohort = [r.cusip9_input for r in materialized]
    panel = build_observed_panel_rows(panel_input, cohort)

    resolved: list[ResolvedPriceObservation] = []
    for prow, r in zip(panel.rows, materialized):
        cusip_state = str(prow["cusip_state"])
        normalized = prow["normalized_cusip9"]
        if cusip_state == IdentifierState.VALID_CUSIP9.value and normalized is not None:
            identity_state = "resolved"
            reason: str | None = None
            security_id: UUID | None = uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{normalized}")
            normalized_out: str | None = str(normalized)
        else:
            identity_state = "unresolved"
            reason = cusip_state
            security_id = None
            normalized_out = None
        db_type_state = str(prow["db_type_state"])
        resolved.append(
            ResolvedPriceObservation(
                observation_id=str(r.observation_id),
                observation_date=_as_date(r.observation_date),
                security_id=security_id,
                identity_state=identity_state,
                identity_reason_code=reason,
                cusip9_input=r.cusip9_input,
                normalized_cusip9=normalized_out,
                price=r.price,
                price_state=str(prow["pr_state"]),
                price_type=_typed(r.price_type, _PRICE_TYPES, "invalid_price_type"),
                accrued_treatment=_typed(r.accrued_treatment, _ACCRUED_TREATMENTS, "invalid_accrued_treatment"),
                ytm=r.ytm,
                db_type=r.db_type,
                db_type_state=db_type_state,
                is_144a=_is_144a(r.db_type, db_type_state),
                daily_key_state=str(prow["daily_key_state"]),
                source_row_number=int(prow["source_row_number"]),  # type: ignore[arg-type]
                source_lineage=dict(r.source_lineage),
            )
        )
    return resolved


# ---------------------------------------------------------------------------
# Matcher-facing index builder (structural lane gate)
# ---------------------------------------------------------------------------
def build_pit_index(
    rows: Iterable[Mapping[str, object]], cohort_cusips: Iterable[object]
) -> ObservationIndex:
    """Build a point-in-time ``ObservationIndex`` from PIT rows, refusing latest lane.

    Rows may be unstamped raw observation rows (the canonical PIT source) or rows
    stamped ``lane='fund_asof'``.  A row stamped with any other lane (notably the
    informative ``latest`` lane) is REFUSED with ``non_pit_lane_rows`` — the
    structural mirror of the matching library's ``invalid_observation_index``
    refusal.  The point-in-time matcher never consumes latest-lane output.
    """
    prepared: list[Mapping[str, object]] = []
    for row in rows:
        lane = row.get("lane")
        if lane is not None and lane != PIT_LANE:
            raise BondError("non_pit_lane_rows", {"lane": lane})
        prepared.append(row)
    return ObservationIndex.from_rows(prepared, cohort_cusips)


def pit_observation_rows(conn: psycopg.Connection, as_of: date) -> list[dict[str, object]]:
    """Read resolved observations for ``as_of`` as panel-shaped PIT rows.

    The rows carry the observed-panel column names the ``ObservationIndex``
    expects.  ``source_row_number`` is re-enumerated over a deterministic order so
    duplicate ``(cusip, date)`` rows keep distinct keys inside the as-of index.
    """
    rows = conn.execute(
        "SELECT normalized_cusip9, observation_date, price, price_state, ytm, db_type, "
        "db_type_state, daily_key_state "
        "FROM bond_price_observation WHERE as_of=%s AND identity_state='resolved' "
        "ORDER BY normalized_cusip9, observation_date, observation_id",
        (as_of,),
    ).fetchall()
    out: list[dict[str, object]] = []
    for ordinal, r in enumerate(rows):
        out.append(
            {
                "normalized_cusip9": r[0],
                "observation_date": r[1].isoformat(),
                "observation_date_state": FieldState.PRESENT.value,
                "source_row_number": ordinal,
                "pr": _as_float(r[2]),
                "pr_state": r[3],
                "ytm": _as_float(r[4]),
                "db_type": _as_float(r[5]),
                "db_type_state": r[6],
                "daily_key_state": r[7],
            }
        )
    return out


def match_fund_holdings_asof(
    holdings: Iterable[HoldingRecord],
    debt_mapping: object,
    observation_rows: Iterable[Mapping[str, object]],
    cohort_cusips: Iterable[object],
    global_start: object,
    cutoff: object,
) -> tuple[MatchResult, ...]:
    """Wire fund holdings to the point-in-time price index, applying MatchState.

    ``observation_rows`` MUST be PIT rows (unstamped or ``fund_asof``-stamped);
    passing latest-lane output raises before any matching happens.  MatchState
    precedence (Task 2) is applied verbatim by ``match_holdings_asof``.
    """
    with build_pit_index(observation_rows, cohort_cusips) as index:
        return match_holdings_asof(holdings, debt_mapping, index, global_start, cutoff)


# ---------------------------------------------------------------------------
# Loader (fixture rows -> immutable observation table)
# ---------------------------------------------------------------------------
def _numeric(value: object) -> object:
    """Pass a real number through for a numeric column; otherwise NULL (state kept)."""
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        return None
    return value


def _as_float(value: object) -> float | None:
    """Coerce a DB numeric (incl. Decimal) to float for the sqlite as-of index."""
    number = _finite(value)
    return number


def load_price_observations(
    conn: psycopg.Connection,
    rows: Iterable[PriceObservationInput],
    *,
    as_of: date,
    source_run_id: UUID,
) -> dict[str, Any]:
    """Land raw observations into the immutable ``bond_price_observation`` table."""
    resolved = resolve_price_observations(rows)
    inserted = 0
    for r in resolved:
        lineage = dict(r.source_lineage)
        if not lineage:
            raise BondError("missing_source_lineage", {"observation_id": r.observation_id})
        conn.execute(
            "INSERT INTO bond_price_observation"
            "(observation_id, as_of, observation_date, source_run_id, security_id, cusip9_input, "
            " normalized_cusip9, identity_state, identity_reason_code, price, price_state, price_type, "
            " accrued_treatment, ytm, db_type, db_type_state, daily_key_state, source_row_number, source_lineage) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (observation_id) DO NOTHING",
            (
                r.observation_id, as_of, r.observation_date, source_run_id, r.security_id,
                None if r.cusip9_input is None else str(r.cusip9_input), r.normalized_cusip9,
                r.identity_state, r.identity_reason_code, _numeric(r.price), r.price_state,
                r.price_type, r.accrued_treatment, _numeric(r.ytm), _numeric(r.db_type),
                r.db_type_state, r.daily_key_state, r.source_row_number, Jsonb(lineage),
            ),
        )
        inserted += 1
    return {"observations": inserted}


# ---------------------------------------------------------------------------
# Publication wiring (sec_derived_publications protocol)
# ---------------------------------------------------------------------------
def install_schema(conn: psycopg.Connection) -> None:
    """Apply the publication protocol + price-observation DDL idempotently."""
    with conn.cursor() as cur:
        cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def publication_id_for(as_of: date, code_revision: str) -> UUID:
    return uuid5(_NAMESPACE_PUBLICATION, f"{PRODUCT}|{as_of.isoformat()}|{code_revision}")


@dataclass(frozen=True)
class _PublishedRow:
    security_id: UUID
    observation_date: date
    source_row_number: int
    price: object
    price_state: str
    price_type: str
    accrued_treatment: str
    ytm: object
    db_type: object
    db_type_state: str
    is_144a: bool | None
    daily_key_state: str


def _published_rows(conn: psycopg.Connection, as_of: date) -> list[_PublishedRow]:
    """Project resolved observations for ``as_of`` into publishable rows.

    The daily-key (ambiguity) state is recomputed over the FULL publication cohort
    (the panel_states rule: a ``(security_id, observation_date)`` key seen more than
    once is ``duplicate_in_matching_cohort``), so duplicates that arrive across
    separate loads still coexist and neither is dropped.  ``source_row_number`` is a
    deterministic per-publication ordinal that keeps duplicate rows distinct.
    """
    rows = conn.execute(
        "SELECT security_id, observation_date, price, price_state, price_type, accrued_treatment, "
        "ytm, db_type, db_type_state, observation_id "
        "FROM bond_price_observation WHERE as_of=%s AND identity_state='resolved' "
        "ORDER BY security_id, observation_date, observation_id",
        (as_of,),
    ).fetchall()
    key_counts: Counter[tuple[Any, date]] = Counter((r[0], r[1]) for r in rows)
    published: list[_PublishedRow] = []
    for ordinal, r in enumerate(rows):
        daily_key_state = (
            "duplicate_in_matching_cohort" if key_counts[(r[0], r[1])] > 1 else "unique_in_matching_cohort"
        )
        published.append(
            _PublishedRow(
                security_id=r[0],
                observation_date=r[1],
                source_row_number=ordinal,
                price=r[2],
                price_state=r[3],
                price_type=r[4],
                accrued_treatment=r[5],
                ytm=r[6],
                db_type=r[7],
                db_type_state=r[8],
                is_144a=_is_144a(r[7], r[8]),
                daily_key_state=daily_key_state,
            )
        )
    return published


def _input_fingerprint(as_of: date, conn: psycopg.Connection) -> tuple[str, int]:
    rows = conn.execute(
        "SELECT observation_id, observation_date, cusip9_input, price, source_row_number, identity_state "
        "FROM bond_price_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    parts = [f"{PRODUCT}|{as_of.isoformat()}"]
    for r in rows:
        parts.append("|".join(str(x) for x in r))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest(), len(rows)


def materialize(
    conn: psycopg.Connection,
    *,
    as_of: date,
    source_run_id: UUID,
    source_package_id: UUID,
    code_revision: str,
) -> dict[str, Any]:
    """Prepare -> pin -> write snapshot -> validate -> current, idempotently.

    A partial/failed build never becomes current: snapshot rows are written only
    while the publication is 'prepared', the pin is verified before validate, and
    the current pointer advances only after validation (the shared publication
    protocol's fail-closed guards enforce this).
    """
    publication_id = publication_id_for(as_of, code_revision)
    fingerprint, observation_count = _input_fingerprint(as_of, conn)

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

    published = _published_rows(conn, as_of)
    if lifecycle == "prepared":
        conn.execute(
            "INSERT INTO bond_price_observation_v1_builds"
            "(publication_id,input_fingerprint,as_of_date,observation_input_count) "
            "VALUES(%s,%s,%s,%s) ON CONFLICT (publication_id) DO NOTHING",
            (publication_id, fingerprint, as_of, observation_count),
        )
        pinned = conn.execute(
            "SELECT input_fingerprint, as_of_date FROM bond_price_observation_v1_builds WHERE publication_id=%s",
            (publication_id,),
        ).fetchone()
        if pinned[0] != fingerprint:
            raise RuntimeError(f"{PRODUCT} publication already pinned to fingerprint {pinned[0]}")
        if pinned[1] != as_of:
            raise RuntimeError(f"{PRODUCT} publication already pinned to as_of {pinned[1]}")
        for row in published:
            conn.execute(
                "INSERT INTO bond_price_observation_v1"
                "(publication_id,source_run_id,security_id,observation_date,source_row_number,price,"
                " price_state,price_type,accrued_treatment,ytm,db_type,db_type_state,is_144a,"
                " daily_key_state,measured_at,provenance) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (publication_id,security_id,observation_date,source_row_number) DO NOTHING",
                (
                    publication_id, source_run_id, row.security_id, row.observation_date,
                    row.source_row_number, row.price, row.price_state, row.price_type,
                    row.accrued_treatment, row.ytm, row.db_type, row.db_type_state, row.is_144a,
                    row.daily_key_state, as_of,
                    Jsonb({"resolver": "price_observations", "methodology_version": METHODOLOGY_VERSION}),
                ),
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
        "observations": observation_count,
        "published": len(published),
        "state": "current",
    }

"""The Phase-10 gate machine — recorded ONCE, in code, as testable predicates.

The handoff's honesty mechanism (Increment 3, Global Constraint #5): "record this
once in code/tests, not in a forest of approval packets".  There is no approval
packet, no signature, no artifact and no workflow here — only pure predicates over
observable facts (a source-qualification row, a validated PIT publication, an
engine module's own declared validation status).

For each of the sixteen Phase-10 security metrics (mirroring
``app.metrics.registry`` on the app side), a metric passes the gate ONLY when ALL
hold:

* :func:`source_qualified` — a qualified, ACTIVE source contract exists for the
  metric's inputs.  TODAY this is ALWAYS False (reason ``no_qualified_source``):
  no production bond source is authorized (Global Constraint #3).  The structure
  it consults — :data:`SOURCE_QUALIFICATION_TABLE` — is the minimal registry a
  future activation fills; it is empty now.
* :func:`pit_complete` — every point-in-time input PRODUCT the metric requires has
  a validated, current publication (curves for spread/OAS metrics; a LICENSED,
  active rating history for the rating metrics; at least one ELIGIBLE price for the
  price-derived metrics).  Missing → reason ``pit_inputs_missing``.
* :func:`model_validated` / :func:`cashflow_validated` — the numerical engine the
  metric relies on declares a VALIDATED status, read straight from the engine
  module's own code marker (never re-interpreted here): cash flows are
  ``convention_derived`` (validated); the pricing YIELD family is
  ``authoritative_published`` (validated); the pricing DURATION family is
  ``authoritative_sample_pending`` (an open program item → reason
  ``authoritative_duration_sample_pending``); OAS is ``model_validation_incomplete``
  (reason ``model_validation_incomplete``); metrics whose research-grade engine is
  not built in this increment are ``model_not_implemented`` (reason
  ``model_not_implemented``).

:func:`gate_status` aggregates these into ``(passed, reasons)``.  TODAY no metric
passes, and each carries its SPECIFIC reason set.

READ-ONLY.  Every function here only SELECTs; none writes any serving, publication
or qualification table.  A guard test proves the production serving surfaces and
digests do not move as a function of the gate (Global Constraint #3).

SEAM DECISION (Task 6 point 4): the gate lives ENTIRELY in the workers repo; the
app does NOT call it at runtime.  The app mirrors only the CODE-LEVEL (DB-
independent) reasons statically (see :func:`static_gate_reasons` and
``app.metrics.phase10_gate``).  Nothing crosses the seam, so there is no digest
re-sync for this task.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import psycopg

from . import cashflows, oas, pricing

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_source_qualification.sql"
SOURCE_QUALIFICATION_TABLE = "bond_source_qualification"

# --------------------------------------------------------------------------- #
# Point-in-time input requirements (validated PIT publications a metric consumes).
# --------------------------------------------------------------------------- #
PIT_ELIGIBLE_PRICE = "eligible_price"  # >=1 eligible bond_price_observation
PIT_SPOT_CURVE = "spot_curve"  # validated+current bond_curve_v1
PIT_LICENSED_RATINGS = "licensed_ratings"  # current bond_rating_history_v1, product_state='active'

PIT_INPUTS: frozenset[str] = frozenset(
    {PIT_ELIGIBLE_PRICE, PIT_SPOT_CURVE, PIT_LICENSED_RATINGS}
)

# --------------------------------------------------------------------------- #
# Engine keys and their VALIDATED status set (read from the engine modules).
# --------------------------------------------------------------------------- #
ENGINE_PRICING_YIELD = "pricing_yield"
ENGINE_PRICING_DURATION = "pricing_duration"
ENGINE_OAS = "oas"
ENGINE_CASHFLOW = "cashflow"
ENGINE_NONE = "none"  # pure PIT-derived (e.g. a rating distribution): no model
ENGINE_UNIMPLEMENTED = "unimplemented"  # no research-grade engine this increment

# The engine statuses that COUNT as validated for the gate.  ``convention_derived``
# (cash flows) and ``authoritative_published`` (the pricing yield family) pass; a
# ``*_pending`` / ``*_incomplete`` / ``model_not_implemented`` status does not.
_VALIDATED_STATUSES: frozenset[str] = frozenset(
    {cashflows.VALIDATION_STATUS, pricing.VALIDATION_STATUS_AUTHORITATIVE}
)

_MODEL_NOT_IMPLEMENTED_STATUS = "model_not_implemented"


def _engine_status(engine: str) -> str | None:
    """The declared validation status string of an engine, or ``None`` (no model)."""
    if engine == ENGINE_CASHFLOW:
        return cashflows.VALIDATION_STATUS
    if engine == ENGINE_PRICING_YIELD:
        return pricing.PRICING_VALIDATION_STATUS["yield"]
    if engine == ENGINE_PRICING_DURATION:
        return pricing.PRICING_VALIDATION_STATUS["duration"]
    if engine == ENGINE_OAS:
        return oas.MODEL_VALIDATION_STATUS
    if engine == ENGINE_UNIMPLEMENTED:
        return _MODEL_NOT_IMPLEMENTED_STATUS
    if engine == ENGINE_NONE:
        return None
    raise ValueError(f"unknown engine {engine!r}")


# --------------------------------------------------------------------------- #
# Reason vocabulary (closed; the app vocabulary test mirrors this set).
# --------------------------------------------------------------------------- #
REASON_NO_QUALIFIED_SOURCE = "no_qualified_source"
REASON_PIT_INPUTS_MISSING = "pit_inputs_missing"
REASON_MODEL_VALIDATION_INCOMPLETE = "model_validation_incomplete"
REASON_DURATION_SAMPLE_PENDING = "authoritative_duration_sample_pending"
REASON_MODEL_NOT_IMPLEMENTED = "model_not_implemented"

GATE_REASONS: frozenset[str] = frozenset(
    {
        REASON_NO_QUALIFIED_SOURCE,
        REASON_PIT_INPUTS_MISSING,
        REASON_MODEL_VALIDATION_INCOMPLETE,
        REASON_DURATION_SAMPLE_PENDING,
        REASON_MODEL_NOT_IMPLEMENTED,
    }
)

# Reason emitted when a metric's engine is NOT validated (validated engines never
# reach this map).
_ENGINE_NOT_VALIDATED_REASON: dict[str, str] = {
    ENGINE_PRICING_DURATION: REASON_DURATION_SAMPLE_PENDING,
    ENGINE_OAS: REASON_MODEL_VALIDATION_INCOMPLETE,
    ENGINE_UNIMPLEMENTED: REASON_MODEL_NOT_IMPLEMENTED,
}


# --------------------------------------------------------------------------- #
# Per-metric gate descriptor (the map is the single source of truth).
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MetricGate:
    """One metric's gate inputs: the PIT products it needs and its engine."""

    pit: frozenset[str] = field(default_factory=frozenset)
    engine: str = ENGINE_UNIMPLEMENTED


@dataclass(frozen=True)
class GateStatus:
    """Aggregate gate outcome for one metric: does it pass, and if not, why."""

    metric_id: str
    passed: bool
    reasons: tuple[str, ...]


# The sixteen Phase-10 security metrics, keyed exactly as app.metrics.registry.
# Mapping metric -> (PIT inputs, engine) is declared ONCE here.
REQUIREMENTS: dict[str, MetricGate] = {
    # price-derived yields (validated yield engine)
    "security_ytm": MetricGate(frozenset({PIT_ELIGIBLE_PRICE}), ENGINE_PRICING_YIELD),
    "security_ytw": MetricGate(frozenset({PIT_ELIGIBLE_PRICE}), ENGINE_PRICING_YIELD),
    "current_yield": MetricGate(frozenset({PIT_ELIGIBLE_PRICE}), ENGINE_PRICING_YIELD),
    # curve + price spreads
    "security_zspread": MetricGate(
        frozenset({PIT_ELIGIBLE_PRICE, PIT_SPOT_CURVE}), ENGINE_PRICING_YIELD
    ),
    "security_oas": MetricGate(
        frozenset({PIT_ELIGIBLE_PRICE, PIT_SPOT_CURVE}), ENGINE_OAS
    ),
    "carry_rolldown": MetricGate(frozenset({PIT_SPOT_CURVE}), ENGINE_PRICING_YIELD),
    # duration family (authoritative printed sample pending — open program item)
    "security_effective_duration": MetricGate(
        frozenset({PIT_ELIGIBLE_PRICE}), ENGINE_PRICING_DURATION
    ),
    "spread_duration": MetricGate(
        frozenset({PIT_ELIGIBLE_PRICE, PIT_SPOT_CURVE}), ENGINE_PRICING_DURATION
    ),
    "key_rate_risk_security": MetricGate(
        frozenset({PIT_ELIGIBLE_PRICE, PIT_SPOT_CURVE}), ENGINE_PRICING_DURATION
    ),
    # cash-flow-schedule metric (convention-validated engine)
    "wal": MetricGate(frozenset(), ENGINE_CASHFLOW),
    # rating metrics (licensed rating history required)
    "rating_distribution": MetricGate(frozenset({PIT_LICENSED_RATINGS}), ENGINE_NONE),
    "rating_migration": MetricGate(
        frozenset({PIT_LICENSED_RATINGS}), ENGINE_UNIMPLEMENTED
    ),
    # estimated metrics with no research-grade engine in this increment
    "prepayment_extension": MetricGate(frozenset(), ENGINE_UNIMPLEMENTED),
    "real_yield": MetricGate(frozenset(), ENGINE_UNIMPLEMENTED),
    "relative_value": MetricGate(frozenset(), ENGINE_UNIMPLEMENTED),
    "liquidity_score": MetricGate(frozenset(), ENGINE_UNIMPLEMENTED),
}

METRIC_IDS: tuple[str, ...] = tuple(REQUIREMENTS)


def metric_ids() -> tuple[str, ...]:
    """Every Phase-10 metric the gate governs (registry-mirrored order)."""
    return METRIC_IDS


def _gate_for(metric: str) -> MetricGate:
    try:
        return REQUIREMENTS[metric]
    except KeyError as exc:
        raise ValueError(f"unknown Phase-10 metric {metric!r}") from exc


# --------------------------------------------------------------------------- #
# Schema install (the gate owns ONLY the empty qualification registry).
# --------------------------------------------------------------------------- #
def install_gate_schema(conn: psycopg.Connection) -> None:
    """Create the (empty) source-qualification registry idempotently.

    This is setup, not gate evaluation: the predicate functions below never write.
    """
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def _relation_exists(conn: psycopg.Connection, name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()
    return bool(row and row[0] is not None)


# --------------------------------------------------------------------------- #
# Predicate 1: source qualified (ALWAYS False today).
# --------------------------------------------------------------------------- #
def source_qualified(metric: str, conn: psycopg.Connection) -> bool:
    """True iff an ACTIVE qualified-source contract exists for the metric's inputs.

    Reads :data:`SOURCE_QUALIFICATION_TABLE` only.  An absent table (never
    installed) or an empty one — the state TODAY — means NOT qualified.
    """
    _gate_for(metric)
    if not _relation_exists(conn, SOURCE_QUALIFICATION_TABLE):
        return False
    row = conn.execute(
        f"SELECT EXISTS (SELECT 1 FROM {SOURCE_QUALIFICATION_TABLE} "
        "WHERE metric_id = %s AND qualified_to IS NULL)",
        (metric,),
    ).fetchone()
    return bool(row and row[0])


# --------------------------------------------------------------------------- #
# Predicate 2: PIT inputs complete.
# --------------------------------------------------------------------------- #
def _has_eligible_price(conn: psycopg.Connection) -> bool:
    if not _relation_exists(conn, "bond_price_eligibility_v1"):
        return False
    row = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM bond_price_eligibility_v1 WHERE is_eligible)"
    ).fetchone()
    return bool(row and row[0])


def _has_validated_curve(conn: psycopg.Connection) -> bool:
    if not _relation_exists(conn, "sec_current_derived_publications"):
        return False
    row = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM sec_current_derived_publications "
        "WHERE product = 'bond_curve_v1' AND lifecycle_state = 'validated')"
    ).fetchone()
    return bool(row and row[0])


def _has_licensed_ratings(conn: psycopg.Connection) -> bool:
    # A LICENSED, active rating product: a current publication whose build state is
    # 'active' (license verified).  A 'not_applicable' (license-gated, empty) run is
    # still a validated publication but does NOT satisfy the rating metrics.
    if not _relation_exists(conn, "sec_current_bond_rating_history_v1_status"):
        return False
    row = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM sec_current_bond_rating_history_v1_status "
        "WHERE product_state = 'active')"
    ).fetchone()
    return bool(row and row[0])


_PIT_CHECKS = {
    PIT_ELIGIBLE_PRICE: _has_eligible_price,
    PIT_SPOT_CURVE: _has_validated_curve,
    PIT_LICENSED_RATINGS: _has_licensed_ratings,
}


def missing_pit_inputs(metric: str, conn: psycopg.Connection) -> frozenset[str]:
    """The subset of the metric's required PIT inputs that are NOT present."""
    gate = _gate_for(metric)
    return frozenset(inp for inp in gate.pit if not _PIT_CHECKS[inp](conn))


def pit_complete(metric: str, conn: psycopg.Connection) -> bool:
    """True iff every PIT input product the metric requires is validated/present.

    A metric with no PIT requirement is vacuously complete.
    """
    return not missing_pit_inputs(metric, conn)


# --------------------------------------------------------------------------- #
# Predicate 3: engine validation (code markers; never re-interpreted).
# --------------------------------------------------------------------------- #
def cashflow_validated() -> bool:
    """True iff the cash-flow engine declares a validated status."""
    return cashflows.VALIDATION_STATUS in _VALIDATED_STATUSES


def model_validated(metric: str) -> bool:
    """True iff the numerical engine the metric relies on declares a validated status.

    Routes the cash-flow engine through :func:`cashflow_validated`; a metric with no
    model (``ENGINE_NONE``, e.g. a pure PIT-derived rating distribution) is vacuously
    validated.
    """
    engine = _gate_for(metric).engine
    if engine == ENGINE_CASHFLOW:
        return cashflow_validated()
    status = _engine_status(engine)
    if status is None:
        return True
    return status in _VALIDATED_STATUSES


def _model_reason(metric: str) -> str:
    """The reason a metric's (non-validated) engine emits."""
    engine = _gate_for(metric).engine
    return _ENGINE_NOT_VALIDATED_REASON[engine]


# --------------------------------------------------------------------------- #
# Aggregate gate status.
# --------------------------------------------------------------------------- #
def gate_status(metric: str, conn: psycopg.Connection) -> GateStatus:
    """Aggregate every predicate into ``(passed, reasons)`` for one metric.

    Reasons are emitted in a stable order: source, then PIT, then model.  TODAY no
    metric passes (``source_qualified`` is always False).
    """
    reasons: list[str] = []
    if not source_qualified(metric, conn):
        reasons.append(REASON_NO_QUALIFIED_SOURCE)
    if not pit_complete(metric, conn):
        reasons.append(REASON_PIT_INPUTS_MISSING)
    if not model_validated(metric):
        reasons.append(_model_reason(metric))
    return GateStatus(metric_id=metric, passed=not reasons, reasons=tuple(reasons))


def static_gate_reasons(metric: str) -> tuple[str, ...]:
    """The CODE-LEVEL (DB-independent) reasons a metric is gated, in stable order.

    This is exactly the subset the app registry mirrors statically
    (``app.metrics.phase10_gate``): ``no_qualified_source`` (a standing program
    fact — no source is authorized, Global Constraint #3) plus the engine's own
    ``model_*`` reason when its model is not validated.  It deliberately OMITS
    ``pit_inputs_missing``, which depends on live datalake state.
    """
    _gate_for(metric)
    reasons = [REASON_NO_QUALIFIED_SOURCE]
    if not model_validated(metric):
        reasons.append(_model_reason(metric))
    return tuple(reasons)


__all__ = [
    "GATE_REASONS",
    "GateStatus",
    "METRIC_IDS",
    "MetricGate",
    "PIT_INPUTS",
    "PIT_ELIGIBLE_PRICE",
    "PIT_SPOT_CURVE",
    "PIT_LICENSED_RATINGS",
    "REQUIREMENTS",
    "SOURCE_QUALIFICATION_TABLE",
    "cashflow_validated",
    "gate_status",
    "install_gate_schema",
    "metric_ids",
    "missing_pit_inputs",
    "model_validated",
    "pit_complete",
    "source_qualified",
    "static_gate_reasons",
]

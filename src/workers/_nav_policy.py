"""Canonical NAV policy evidence shared by the publisher and readiness worker.

Leaf module: it imports no worker module, so readiness, ingestion, risk and the
operator can share contracts without import cycles.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

PROFILE = "current_daily_nav_v1"
FEATURE_DEFINITION_VERSION = "risk_metrics_nav_v1"
CALENDAR_FIELDS = ("calendar_id", "calendar_version", "calendar_source")
RISK_RUN_SCOPES = ("current_full", "diagnostic")
# Deterministic precedence when several reasons apply.
RISK_NONPUBLISHING_REASONS = (
    "LIMITED_RUN",
    "POLICY_UNAVAILABLE",
    "POLICY_EXPIRED",
    "NON_CURRENT_SESSION",
)
ADJUSTED_OVERLAP_ABS_TOL = 0.0000005
ADJUSTED_OVERLAP_REL_TOL = 0.00000001
# Identity v3 (registry + ticker + series + optional strict claims + current SEC
# class/series/ticker corroboration). The v1 and v2 generator/query contracts
# are retired: v3 code recognises them only to reject them, so v1/v2 artifacts
# stay immutable and are never re-applied or re-audited here (a v1 artifact is
# still readable as the audited PREVIOUS policy by the independent auditor).
CURRENT_CATALOG_QUERY_VERSION = "nav-current-catalog-snapshot-v3"
GENERATOR_VERSION = "fund-nav-policy-generator-v3"
PROVIDER_CONTRACT_VERSION = "w1-tiingo-adjusted-daily-v1"
IDENTITY_CONTRACT_VERSION = "registry-ticker-series-claims-sec-v3"
RETIRED_GENERATOR_VERSIONS = frozenset(
    {"fund-nav-policy-generator-v1", "fund-nav-policy-generator-v2"}
)
RETIRED_CATALOG_QUERY_VERSIONS = frozenset(
    {"nav-current-catalog-snapshot-v1", "nav-current-catalog-snapshot-v2"}
)
SEC_RELATION = "public.sec_company_tickers_mf"
CATALOG_EVIDENCE_REFERENCE = (
    f"{CURRENT_CATALOG_QUERY_VERSION}:public.instruments_universe+public.funds_v+"
    f"public.instrument_identity+{SEC_RELATION}:{PROVIDER_CONTRACT_VERSION}:"
    f"current_only_not_pit:identity={IDENTITY_CONTRACT_VERSION}"
)
INSTRUMENTS_QUERY = (
    "SELECT instrument_id,instrument_type,ticker,isin,currency,is_active "
    "FROM public.instruments_universe ORDER BY instrument_id LIMIT 100001"
)
FUNDS_QUERY = (
    "SELECT instrument_id,series_id,ticker,isin,cusip,currency,fund_type "
    "FROM public.funds_v ORDER BY instrument_id,series_id LIMIT 100001"
)
IDENTITY_QUERY = (
    "SELECT instrument_id,sec_series_id,sec_class_id,ticker,isin,cusip_9,figi,"
    "resolution_status,conflict_state "
    "FROM public.instrument_identity ORDER BY instrument_id LIMIT 100001"
)
# Fourth source: the SEC class/series/ticker crosswalk. updated_at is refreshed
# on every upsert (fetched_at is not), so it is the freshness column. Same
# projection, order and cap as the audit contract's SEC query (a test pins the
# bytes against the leaf); the lineage aggregate is read in the same snapshot.
SEC_QUERY = (
    "SELECT class_id,series_id,ticker,updated_at AS synced_at "
    "FROM public.sec_company_tickers_mf "
    "ORDER BY class_id,series_id,ticker,updated_at LIMIT 100001"
)
SEC_LINEAGE_QUERY = (
    "SELECT count(*) AS row_count,min(updated_at) AS min_synced_at,"
    "max(updated_at) AS max_synced_at FROM public.sec_company_tickers_mf"
)
SEC_MAX_SYNCED_AGE = dt.timedelta(days=7)
SEC_CLASS_PATTERN = "^C[0-9]{9}$"
SEC_SERIES_PATTERN = "^S[0-9]{9}$"
CATALOG_SOURCE_RELATIONS = (
    "public.instruments_universe",
    "public.funds_v",
    "public.instrument_identity",
    SEC_RELATION,
)
# Ordered digest of the four source SQLs (IU, funds_v, registry, SEC).
_SOURCE_QUERIES = (INSTRUMENTS_QUERY, FUNDS_QUERY, IDENTITY_QUERY, SEC_QUERY)
SOURCE_QUERY_SHA256 = hashlib.sha256(
    "\n".join(_SOURCE_QUERIES).encode("utf-8")
).hexdigest()
# SEC corroboration first failures, evaluated only for a candidate that passes
# every internal gate (so always after activity), in this precedence order.
SEC_FAILURE_CODES = (
    "sec.declared_class_invalid",
    "sec.poisoned_mapping",
    "sec.contradiction",
    "sec.ambiguous",
    "sec.incomplete",
    "sec.stale",
    "sec.missing",
)
SEC_FAILURE_FAMILY = "sec"
# Governed ceilings count FUNDS by first failure (never source rows).
SEC_GAP_CODES = ("sec.incomplete", "sec.stale", "sec.missing")
SEC_CONFLICT_CODES = (
    "sec.declared_class_invalid",
    "sec.poisoned_mapping",
    "sec.contradiction",
    "sec.ambiguous",
)
# Exactly one first failure per UNKNOWN, in this precedence order (families:
# cardinality → registry → instrument_type → ticker → series → ISIN → CUSIP →
# FIGI → currency → fund_type → activity → SEC). INACTIVE/ACTIVE are not reasons.
IDENTITY_FAILURE_CODES = (
    "cardinality.iu_missing",
    "cardinality.iu_duplicate",
    "cardinality.funds_v_missing",
    "cardinality.funds_v_duplicate",
    "cardinality.registry_missing",
    "cardinality.registry_duplicate",
    "registry.status_not_canonical",
    "registry.conflict_state_not_empty",
    "instrument_type.not_fund",
    "ticker.missing",
    "ticker.mismatch",
    "ticker.global_conflict",
    "series.missing",
    "series.mismatch",
    "series.global_conflict",
    "isin.unsupported_prefix",
    "isin.invalid_format",
    "isin.checksum",
    "isin.mismatch",
    "isin.global_conflict",
    "cusip.invalid_format",
    "cusip.checksum",
    "cusip.mismatch",
    "cusip.global_conflict",
    "figi.invalid_format",
    "figi.reserved_prefix",
    "figi.checksum",
    "figi.global_conflict",
    "currency.iu_not_usd",
    "currency.funds_v_not_usd",
    "fund_type.unsupported",
    "activity.not_active",
    "activity.unknown",
    *SEC_FAILURE_CODES,
)
CLAIM_FAILURE_FAMILIES = ("isin", "cusip", "figi")
INACTIVE_REASONS = ("inactive_without_funds_v",)
ISIN_PRESENCE_CATEGORIES = ("both", "iu_only", "registry_only", "neither")
SOURCE_COUNT_KEYS = (
    "instruments_universe",
    "funds_v",
    "instrument_identity",
    "sec_company_tickers_mf",
)
STRUCTURAL_COUNT_KEYS = (
    "structural_pre_claims",
    "structural_pre_claims_daily",
    "structural_claim_failures",
    "structural_sec_failures",
    "structural_daily_claim_failures",
    "structural_daily_sec_failures",
)


def policy_content_digest(policy: dict) -> str:
    """Stable policy identity excludes run metadata and temporal instrument facts."""
    content = {
        key: value
        for key, value in policy.items()
        if key not in ("instrument_evidence", "generation")
    }
    return hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def instrument_evidence_digest(rows: list[dict]) -> str:
    """Snapshot content identity excludes the capture timestamp, not instrument IDs."""
    content = [
        {
            key: value
            for key, value in row.items()
            if key not in ("known_at", "effective_at")
        }
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def generation_metadata_digest(generation: dict) -> str:
    content = {
        key: value for key, value in generation.items() if key != "generation_sha256"
    }
    return hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def uuid_set_digest(instrument_ids: Iterable[str]) -> str:
    """SHA256 of UTF-8 compact JSON of the sorted canonical UUID strings.

    No whitespace, no trailing newline; the empty set is ``[]``. Callers pass
    canonical lowercase UUID strings (validated by ``validate_generation_v3``).
    """
    return hashlib.sha256(
        json.dumps(
            sorted(instrument_ids), separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    ).hexdigest()


def _nonnegative_int(value: Any) -> bool:
    return type(value) is int and value >= 0


def _count_map(value: Any, allowed: Iterable[str]) -> dict[str, int]:
    allowed = set(allowed)
    if not isinstance(value, dict) or any(
        key not in allowed or not _nonnegative_int(count)
        for key, count in value.items()
    ):
        raise ValueError("generation_count_map_invalid")
    return value


def validate_generation_v3(policy: Mapping[str, Any]) -> None:
    """Recount the v3 identity diagnostics from the document itself.

    Verifies lifecycle partition, one evidence row per canonical UUID, flag
    consistency, closed first-failure codes (SEC codes included) summing to
    UNKNOWN, INACTIVE reasons, ISIN presence sums, the structural
    conservation identities (total: ``structural_pre_claims - active =
    claim + SEC failures``; daily population P: ``P - D = daily claim + daily
    SEC failures``, MMF outside it), every SEC first failure being structural,
    and the ACTIVE / ACTIVE-daily set digests. Causal counts can only be
    revalidated against the source snapshot, which the independent verifier
    does. Raises ``ValueError`` with a static code; never returns identifiers.
    """
    generation = policy["generation"]
    counts = generation["counts"]
    evidence = policy["instrument_evidence"]
    if not isinstance(counts, dict) or not isinstance(evidence, list):
        raise ValueError("generation_counts_invalid")
    for key in (
        *SOURCE_COUNT_KEYS,
        "instrument_evidence",
        "active",
        "active_daily",
        *STRUCTURAL_COUNT_KEYS,
    ):
        if not _nonnegative_int(counts.get(key)):
            raise ValueError("generation_count_invalid")
    ids = [row["instrument_id"] for row in evidence]
    if any(
        type(value) is not str or str(uuid.UUID(value)) != value for value in ids
    ) or len(set(ids)) != len(ids):
        raise ValueError("evidence_uuid_not_canonical_or_duplicate")
    statuses: dict[str, int] = {}
    frequencies: dict[str, int] = {}
    active: list[str] = []
    active_daily: list[str] = []
    for row in evidence:
        status, frequency = row["fund_status"], row["valuation_frequency"]
        flags = (
            row["identity_verified"],
            row["currency_verified"],
            row["return_basis_verified"],
        )
        if status == "ACTIVE":
            if frequency not in ("daily", "unknown") or flags != (
                True,
                True,
                frequency == "daily",
            ):
                raise ValueError("active_evidence_flags_invalid")
            active.append(row["instrument_id"])
            if frequency == "daily":
                active_daily.append(row["instrument_id"])
        elif status in ("INACTIVE", "UNKNOWN"):
            if frequency != "unknown" or flags != (False, False, False):
                raise ValueError("nonactive_evidence_flags_invalid")
        else:
            raise ValueError("evidence_status_invalid")
        statuses[status] = statuses.get(status, 0) + 1
        frequencies[frequency] = frequencies.get(frequency, 0) + 1
    if (
        _count_map(counts.get("fund_status"), ("ACTIVE", "INACTIVE", "UNKNOWN"))
        != dict(sorted(statuses.items()))
        or _count_map(counts.get("valuation_frequency"), ("daily", "unknown"))
        != dict(sorted(frequencies.items()))
        or counts["instrument_evidence"] != len(evidence)
        or counts["active"] != len(active)
        or counts["active_daily"] != len(active_daily)
    ):
        raise ValueError("lifecycle_partition_invalid")
    first_failure = _count_map(
        counts.get("identity_first_failure"), IDENTITY_FAILURE_CODES
    )
    inactive_reason = _count_map(counts.get("inactive_reason"), INACTIVE_REASONS)
    if (
        sum(first_failure.values()) != statuses.get("UNKNOWN", 0)
        or sum(inactive_reason.values()) != statuses.get("INACTIVE", 0)
        or list(first_failure) != sorted(first_failure)
        or list(inactive_reason) != sorted(inactive_reason)
    ):
        raise ValueError("first_failure_closure_invalid")
    presence = counts.get("isin_presence_active")
    presence_daily = counts.get("isin_presence_active_daily")
    for mapping, total in ((presence, len(active)), (presence_daily, len(active_daily))):
        if (
            not isinstance(mapping, dict)
            or set(mapping) != set(ISIN_PRESENCE_CATEGORIES)
            or sum(_count_map(mapping, ISIN_PRESENCE_CATEGORIES).values()) != total
        ):
            raise ValueError("isin_presence_invalid")
    claim_first_failures = sum(
        count
        for code, count in first_failure.items()
        if code.split(".", 1)[0] in CLAIM_FAILURE_FAMILIES
    )
    sec_first_failures = sum(
        count
        for code, count in first_failure.items()
        if code.split(".", 1)[0] == SEC_FAILURE_FAMILY
    )
    if (
        any(presence_daily[key] > presence[key] for key in ISIN_PRESENCE_CATEGORIES)
        or counts["structural_pre_claims_daily"] < counts["active_daily"]
        or counts["structural_pre_claims"] < counts["structural_pre_claims_daily"]
        # Structural potential minus ACTIVE is exactly the structural UNKNOWNs,
        # each failing first on an ISIN/CUSIP/FIGI family or on SEC.
        or counts["structural_pre_claims"] - counts["active"]
        != counts["structural_claim_failures"] + counts["structural_sec_failures"]
        # Same identity inside the daily population P (MMF outside it).
        or counts["structural_pre_claims_daily"] - counts["active_daily"]
        != counts["structural_daily_claim_failures"]
        + counts["structural_daily_sec_failures"]
        or counts["structural_daily_claim_failures"]
        > counts["structural_claim_failures"]
        or counts["structural_daily_sec_failures"] > counts["structural_sec_failures"]
        or counts["structural_claim_failures"] > claim_first_failures
        # SEC is evaluated only after every internal gate: every SEC first
        # failure belongs to the structural population.
        or counts["structural_sec_failures"] != sec_first_failures
    ):
        raise ValueError("structural_counts_invalid")
    if generation.get("active_set_sha256") != uuid_set_digest(active) or generation.get(
        "active_daily_set_sha256"
    ) != uuid_set_digest(active_daily):
        raise ValueError("active_set_digest_invalid")


def calendar_digest(
    sessions: list[tuple[dt.date, dt.datetime, dt.datetime, str]],
) -> str:
    normalized = [
        (
            day.isoformat(),
            close.astimezone(dt.timezone.utc).isoformat(),
            due.astimezone(dt.timezone.utc).isoformat(),
            reference,
        )
        for day, close, due, reference in sessions
    ]
    return hashlib.sha256(
        json.dumps(
            normalized,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
    ).hexdigest()


def canonical_digest(value: Any) -> str:
    """SHA256 of JSON(sort_keys, compact separators, ensure_ascii) as UTF-8."""
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def risk_universe_digest(instrument_ids: Iterable[Any]) -> str:
    """Order-independent identity of a risk run's canonical target set."""
    return canonical_digest(sorted({str(value) for value in instrument_ids}))


LEVEL_EVIDENCE_VERSION = "nav-level-evidence-v1"


def level_evidence_digest(
    nav_date: dt.date,
    nav: Any,
    source_nav: Any,
    source: str | None,
    source_nav_kind: str | None,
    currency: str | None,
    nav_repair_kind: str | None,
) -> str:
    """Python mirror of SQL ``nav_level_evidence_digest_v1`` (level projection).

    Numerics are rendered as PostgreSQL ``numeric::text`` of the persisted
    NUMERIC(18,6) value (``Decimal`` quantized to 6 places); NULL is JSON null.
    Calendar and derived-return columns are excluded by design.
    """
    from decimal import Decimal

    def _numeric(value: Any) -> str | None:
        return None if value is None else str(Decimal(str(value)).quantize(Decimal("0.000001")))

    payload = json.dumps(
        [
            LEVEL_EVIDENCE_VERSION,
            nav_date.isoformat(),
            _numeric(nav),
            _numeric(source_nav),
            source,
            source_nav_kind,
            currency,
            nav_repair_kind,
        ],
        separators=(", ", " : "),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ──────────────────────────────────────────────────────────────────────────────
# Current policy/grid resolution shared by readiness, risk and the NAV chain.
# ──────────────────────────────────────────────────────────────────────────────
ENDPOINTS = 401
# Runtime publication outcomes: never stored in fund_nav_risk_runs, whose
# nonpublishing_reason is the immutable pre-write plan.
RISK_RUNTIME_REASONS = (
    "LOCK_BUSY",
    "SUPERSEDED",
    "POLICY_CHANGED",
    "DUE_SESSION_CHANGED",
    "MV_REFRESH_FAILED",
    # fund_risk_latest_mv does not serve this generation for every member with
    # a feature (e.g. a later diagnostic calc_date is still the latest row).
    "MV_RUN_MISMATCH",
)


class PolicyUnavailable(RuntimeError):
    """Expected absence of a usable published policy (typed, not an SQL error).

    ``reason`` is ``POLICY_UNAVAILABLE`` or ``POLICY_EXPIRED``; ``policy`` is the
    current row when one exists (so a diagnostic run can still pin it). The
    message keeps the historical ``NAV_POLICY_UNAVAILABLE:`` prefix.
    """

    def __init__(self, reason: str, detail: str, policy: dict | None = None):
        super().__init__(f"NAV_POLICY_UNAVAILABLE: {detail}")
        self.reason = reason
        self.policy = policy


def resolve_policy_and_grid(
    conn, decision_at: dt.datetime
) -> tuple[dict, list[dt.date], dt.date]:
    """Current published policy, its 401-session due grid and latest closed session.

    Raises ``PolicyUnavailable`` for the expected policy states (absent, expired,
    digest/window/coverage inconsistent); database errors propagate unchanged.
    """
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """SELECT policy.* FROM nav_policy_current current_policy
               JOIN nav_policy_versions policy USING (policy_id, policy_version)
               WHERE current_policy.readiness_profile = %s
                 AND policy.published_at <= %s AND current_policy.published_at <= %s""",
            (PROFILE, decision_at, decision_at),
        )
        policy = cur.fetchone()
        if not policy:
            raise PolicyUnavailable("POLICY_UNAVAILABLE", "no published policy")
        if decision_at > policy["valid_through"]:
            raise PolicyUnavailable(
                "POLICY_EXPIRED", "calendar coverage expired", policy
            )
        cur.execute(
            """SELECT session_date,valuation_close_at,nav_due_at,source_reference
               FROM nav_valuation_schedules WHERE calendar_id=%s AND calendar_version=%s
               ORDER BY session_date""",
            (policy["calendar_id"], policy["calendar_version"]),
        )
        catalog = cur.fetchall()
        if (
            len(catalog) != policy["calendar_session_count"]
            or not catalog
            or catalog[0]["session_date"] != policy["coverage_start"]
            or catalog[-1]["session_date"] != policy["coverage_end"]
            or calendar_digest(
                [
                    (
                        s["session_date"],
                        s["valuation_close_at"],
                        s["nav_due_at"],
                        s["source_reference"],
                    )
                    for s in catalog
                ]
            )
            != policy["calendar_digest"]
        ):
            raise PolicyUnavailable(
                "POLICY_UNAVAILABLE", "calendar digest mismatch", policy
            )
        cur.execute(
            """SELECT session_date, calendar_source FROM nav_valuation_schedules
               WHERE calendar_id = %s AND calendar_version = %s AND nav_due_at <= %s
               ORDER BY session_date DESC LIMIT %s""",
            (policy["calendar_id"], policy["calendar_version"], decision_at, ENDPOINTS),
        )
        sessions = cur.fetchall()
        if (
            len(sessions) != ENDPOINTS
            or sessions[0]["session_date"] < policy["coverage_start"]
            or sessions[0]["session_date"] > policy["coverage_end"]
            or any(s["calendar_source"] != policy["calendar_source"] for s in sessions)
        ):
            raise PolicyUnavailable(
                "POLICY_UNAVAILABLE", "incomplete calendar window", policy
            )
        cur.execute(
            """SELECT session_date FROM nav_valuation_schedules
               WHERE calendar_id=%s AND calendar_version=%s
                 AND valuation_close_at <= %s
               ORDER BY session_date DESC LIMIT 1""",
            (policy["calendar_id"], policy["calendar_version"], decision_at),
        )
        closed = cur.fetchone()
        if not closed:
            raise PolicyUnavailable("POLICY_UNAVAILABLE", "no closed session", policy)
        if closed["session_date"] > policy["coverage_end"]:
            raise PolicyUnavailable(
                "POLICY_UNAVAILABLE", "closed session beyond coverage", policy
            )
    return (
        policy,
        list(reversed([r["session_date"] for r in sessions])),
        closed["session_date"],
    )


# ──────────────────────────────────────────────────────────────────────────────
# W4 calendar tuple: one indivisible assertion; absence of a mapping is no claim.
# ──────────────────────────────────────────────────────────────────────────────
def calendar_tuple(row: Mapping[str, Any]) -> tuple[str, str, str] | None:
    """Return the full tuple, None when absent, and reject partial tuples."""
    values = tuple(row.get(field) for field in CALENDAR_FIELDS)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ValueError("partial NAV calendar tuple")
    return values  # type: ignore[return-value]


def effective_calendar_tuple(
    incoming: Mapping[str, Any], existing: Mapping[str, Any] | None
) -> tuple[str, str, str] | None:
    """Tuple to persist: a missing new mapping preserves the stored assertion."""
    asserted = calendar_tuple(incoming)
    if asserted is not None:
        return asserted
    return calendar_tuple(existing) if existing is not None else None


@dataclass(frozen=True)
class CalendarSessionProof:
    """Why an observed stamp is valid for one current-policy session."""

    session_date: dt.date
    observed_stamp: tuple[str, str, str]
    current_session: tuple[str, str, str, str, str, str]
    proof_policy: tuple[str, str, str]

    def canonical(self) -> list:
        return [
            self.session_date.isoformat(),
            list(self.observed_stamp),
            list(self.current_session),
            list(self.proof_policy),
        ]


CalendarKey = tuple[dt.date, str, str, str]

_EQUIVALENCE_SQL = """
WITH grid AS (SELECT unnest(%(grid)s::date[]) AS session_date),
stamps AS (
    SELECT * FROM unnest(%(ids)s::text[], %(versions)s::text[], %(sources)s::text[])
        AS s(calendar_id, calendar_version, calendar_source)
),
proof AS (
    SELECT DISTINCT ON (s.calendar_id, s.calendar_version, s.calendar_source)
           s.calendar_id, s.calendar_version, s.calendar_source,
           p.policy_id, p.policy_version, p.policy_hash
    FROM stamps s
    JOIN nav_policy_versions p
      ON p.calendar_id = s.calendar_id AND p.calendar_version = s.calendar_version
     AND p.calendar_source = s.calendar_source
    WHERE p.readiness_profile = %(profile)s
      AND p.published_at IS NOT NULL AND p.published_at <= %(decision_at)s
    ORDER BY s.calendar_id, s.calendar_version, s.calendar_source,
             p.published_at, p.policy_id, p.policy_version
),
cur AS (
    SELECT s.session_date, s.valuation_close_at, s.nav_due_at, s.calendar_source,
           s.source_reference
    FROM nav_valuation_schedules s JOIN grid g USING (session_date)
    WHERE s.calendar_id = %(calendar_id)s AND s.calendar_version = %(calendar_version)s
)
SELECT cur.session_date, proof.calendar_id, proof.calendar_version, proof.calendar_source,
       proof.policy_id, proof.policy_version, proof.policy_hash,
       cur.valuation_close_at, cur.nav_due_at, cur.calendar_source AS current_source,
       cur.source_reference AS current_reference,
       old.valuation_close_at AS old_close, old.nav_due_at AS old_due,
       old.calendar_source AS old_source, old.source_reference AS old_reference
FROM cur CROSS JOIN proof
JOIN nav_valuation_schedules old
  ON old.calendar_id = proof.calendar_id AND old.calendar_version = proof.calendar_version
 AND old.session_date = cur.session_date
ORDER BY cur.session_date, proof.calendar_id, proof.calendar_version, proof.calendar_source
"""


def load_calendar_equivalence(
    conn,
    policy: Mapping[str, Any],
    grid: list[dt.date],
    observed_stamps: Iterable[tuple[str, str, str]],
    decision_at: dt.datetime,
) -> Mapping[CalendarKey, CalendarSessionProof]:
    """Map (session_date, calendar_id, version, source) to its current session.

    A stamp is valid per date only if a policy with that exact calendar tuple was
    published by ``decision_at`` (expiry of an older policy does not erase its
    immutable calendar) and its session has the same calendar_id, date, close,
    due, source and reference as the current policy's session. One batched query
    over the distinct stamps and the grid; no per-fund or per-date lookups.
    """
    stamps = sorted({tuple(stamp) for stamp in observed_stamps if stamp is not None})
    current_stamp = (
        policy["calendar_id"],
        policy["calendar_version"],
        policy["calendar_source"],
    )
    if current_stamp not in stamps:
        stamps.append(current_stamp)
    with conn.cursor() as cur:
        cur.execute(
            _EQUIVALENCE_SQL,
            {
                "grid": list(grid),
                "ids": [s[0] for s in stamps],
                "versions": [s[1] for s in stamps],
                "sources": [s[2] for s in stamps],
                "profile": PROFILE,
                "decision_at": decision_at,
                "calendar_id": policy["calendar_id"],
                "calendar_version": policy["calendar_version"],
            },
        )
        rows = cur.fetchall()
    return derive_calendar_equivalence(policy, [tuple(row) for row in rows])


# Column order of ``_EQUIVALENCE_SQL`` rows consumed by the pure derivation.
EQUIVALENCE_ROW_FIELDS = (
    "session_date",
    "observed_calendar_id",
    "observed_calendar_version",
    "observed_calendar_source",
    "proof_policy_id",
    "proof_policy_version",
    "proof_policy_hash",
    "current_close_at",
    "current_due_at",
    "current_source",
    "current_reference",
    "observed_close_at",
    "observed_due_at",
    "observed_source",
    "observed_reference",
)


def derive_calendar_equivalence(
    policy: Mapping[str, Any], rows: Iterable[tuple]
) -> Mapping[CalendarKey, CalendarSessionProof]:
    """Pure core of ``load_calendar_equivalence`` over ``EQUIVALENCE_ROW_FIELDS`` rows.

    Every proof carries the real current session instants/reference and the
    published proof policy; there is no literal current-stamp shortcut.
    Timestamps must be timezone-aware (naive values raise).
    """
    mapping: dict[CalendarKey, CalendarSessionProof] = {}
    for row in rows:
        (
            session_date,
            calendar_id,
            calendar_version,
            calendar_source,
            policy_id,
            policy_version,
            policy_hash,
            close_at,
            due_at,
            current_source,
            current_reference,
            old_close,
            old_due,
            old_source,
            old_reference,
        ) = row
        if close_at.tzinfo is None or due_at.tzinfo is None:
            raise ValueError("naive NAV session timestamp")
        if (
            calendar_id != policy["calendar_id"]
            or current_source != policy["calendar_source"]
            or calendar_source != old_source
            or old_close != close_at
            or old_due != due_at
            or old_source != current_source
            or old_reference != current_reference
        ):
            continue
        mapping[(session_date, calendar_id, calendar_version, calendar_source)] = (
            CalendarSessionProof(
                session_date=session_date,
                observed_stamp=(calendar_id, calendar_version, calendar_source),
                current_session=(
                    policy["calendar_id"],
                    policy["calendar_version"],
                    current_source,
                    close_at.astimezone(dt.timezone.utc).isoformat(),
                    due_at.astimezone(dt.timezone.utc).isoformat(),
                    current_reference,
                ),
                proof_policy=(policy_id, policy_version, policy_hash),
            )
        )
    return MappingProxyType(mapping)


def equivalence_mapping_digest(
    mapping: Mapping[CalendarKey, CalendarSessionProof],
) -> str:
    return canonical_digest(
        [mapping[key].canonical() for key in sorted(mapping, key=lambda k: (k[0], k[1:]))]
    )


def calendar_equivalence_digest(
    grid: list[dt.date],
    rows_by_date: Mapping[dt.date, Mapping[str, Any]],
    mapping: Mapping[CalendarKey, CalendarSessionProof],
) -> str:
    """SHA256 of the ordered per-session proof used for the grid levels."""
    proof: list = []
    for day in grid:
        row = rows_by_date.get(day)
        try:
            stamp = calendar_tuple(row) if row is not None else None
        except ValueError:
            stamp = None
        entry = mapping.get((day, *stamp)) if stamp is not None else None
        proof.append(
            entry.canonical()
            if entry is not None
            else [day.isoformat(), list(stamp) if stamp else None, None, None]
        )
    return canonical_digest(proof)

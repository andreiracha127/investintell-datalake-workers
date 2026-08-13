"""Worker-owned point-in-time evidence materialization for Open Macro v04.

This module deliberately exposes only categorical public evidence.  Numeric values,
vintage timestamps, and decision-chain provenance remain in ``private_lineage`` for
the worker/orchestrator; they are never inserted into the public item relation.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import pathlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping

from src.db import (
    LOCK_OPEN_MACRO_V04_PIT_EVIDENCE,
    advisory_lock,
    connect,
)


EVIDENCE_CATALOG: tuple[tuple[str, str, str, str, str, str], ...] = (
    ("growth", "Growth", "regime_inputs", "INDPRO", "Industrial Production", "regime_input"),
    ("growth", "Growth", "regime_inputs", "PCEC96", "Real Personal Consumption Expenditures", "regime_input"),
    ("growth", "Growth", "regime_inputs", "PAYEMS", "Total Nonfarm Payrolls", "regime_input"),
    ("growth", "Growth", "regime_inputs", "ACOGNO", "Manufacturers’ New Orders for Consumer Goods", "regime_input"),
    ("inflation", "Inflation", "regime_inputs", "CPILFESL", "Core Consumer Price Index", "regime_input"),
    ("inflation", "Inflation", "regime_inputs", "PPIFIS", "Producer Price Index: Final Demand Intermediate Services", "regime_input"),
    ("inflation", "Inflation", "regime_inputs", "AHETPI", "Average Hourly Earnings", "regime_input"),
    ("inflation", "Inflation", "regime_inputs", "MICH", "University of Michigan Inflation Expectations", "regime_input"),
    ("market", "Market", "regime_inputs", "SPY", "Cycle Market Leg", "regime_input"),
    ("fiscal_liquidity", "Fiscal and liquidity", "regime_inputs", "MTSDS133FMS", "Federal Surplus or Deficit", "regime_input"),
    ("fiscal_liquidity", "Fiscal and liquidity", "regime_inputs", "GDP", "Nominal GDP", "regime_input"),
    ("allocation_guard", "Allocation guard", "allocation_evidence", "SUBLPDCILSLGNQ", "Bank Lending Standards", "allocation_guard"),
    ("allocation_guard", "Allocation guard", "allocation_evidence", "M2SL", "M2 Money Stock", "allocation_guard"),
)
CHAIN_SERIES_IDS = tuple(entry[3] for entry in EVIDENCE_CATALOG[:8])
V04_SERIES_IDS = ("MTSDS133FMS", "GDP", "SUBLPDCILSLGNQ", "M2SL")
FRED_SERIES_IDS = (*CHAIN_SERIES_IDS, *V04_SERIES_IDS)
ITEM_COLUMNS = (
    "group_key", "group_label", "group_role", "series_key", "series_label", "role",
    "display_state", "availability_state", "evidence_state", "freshness_state", "pit_state",
)

DECISION_SQL = (
    "SELECT as_of, fiscal_state, fiscal_boundary, guard_level, guard_coverage, quadrant, "
    "quadrant_source, carry_age, decision_validity, decision_basis, "
    "input_digest_sha256, run_id, updated_at "
    "FROM open_macro_v04_decisions WHERE as_of = %(decision_as_of)s "
    "AND valid_status = 'valid' AND publish_state = 'published'"
)
CHAIN_SEED_SQL = (
    "SELECT as_of, status, basis, pack_sha256, loaded_at "
    "FROM open_macro_v03_decision_chain "
    "WHERE as_of = (date_trunc('month', %(decision_as_of)s::date) "
    "- make_interval(months => %(carry_age)s) + interval '1 month' "
    "- interval '1 day')::date"
)
VINTAGE_SQL = (
    "SELECT DISTINCT ON (series_id, observation_period) "
    "series_id, observation_period, vintage_date, value, available_at, "
    "revision_number, source, source_spec_version "
    "FROM macro_observation_vintage WHERE series_id = ANY(%(series_ids)s) "
    "AND available_at <= %(decision_cutoff)s "
    "AND observation_period <= %(decision_as_of)s "
    "ORDER BY series_id, observation_period, available_at DESC, vintage_date DESC, "
    "revision_number DESC"
)
SPY_SQL = (
    "SELECT ticker, date, adj_close FROM eod_prices WHERE ticker = 'SPY' "
    "AND date <= %(decision_as_of)s ORDER BY date"
)
MIRROR_SQL = (
    "SELECT series_id, obs_date, value, source, created_at, updated_at FROM macro_data "
    "WHERE series_id = ANY(%(series_ids)s) AND obs_date <= %(decision_as_of)s "
    "ORDER BY series_id, obs_date"
)
EXISTING_SNAPSHOT_SQL = (
    "SELECT publication_status, coverage_state FROM open_macro_v04_evidence_snapshots "
    "WHERE decision_month = %(decision_month)s"
)
EXISTING_ITEMS_SQL = (
    "SELECT group_key, group_label, group_role, series_key, series_label, role, "
    "display_state, availability_state, evidence_state, freshness_state, pit_state "
    "FROM open_macro_v04_evidence_items WHERE decision_month = %(decision_month)s "
    "ORDER BY group_key, series_key"
)
CATEGORICAL_COLUMNS = (
    "decision_month", "taxonomy_state", "fiscal_state", "fiscal_boundary", "guard_level",
    "guard_coverage", "quadrant", "cycle_direction", "decision_validity", "decision_basis",
    "quadrant_source", "book",
)
EXISTING_TAXONOMY_SQL = (
    "SELECT " + ", ".join(CATEGORICAL_COLUMNS) + " "
    "FROM open_macro_v04_categorical_taxonomy WHERE decision_month = %(decision_month)s"
)
PRIVATE_COMPARE_COLUMNS = (
    "decision_as_of",
    "decision_run_id",
    "decision_input_digest_sha256",
    "decision_basis",
    "series_key",
    "value",
    "unit",
    "observation_period",
    "release_at",
    "ingested_at",
    "vintage",
    "source",
    "source_health",
    "fingerprint",
    "cutoff_at",
    "carry_seed_decision_month",
    "carry_seed_fingerprint",
    "materialization_run_id",
)
EXISTING_PRIVATE_SQL = (
    "SELECT " + ", ".join(PRIVATE_COMPARE_COLUMNS) + " "
    "FROM open_macro_v04_pit_evidence WHERE decision_month = %(decision_month)s "
    "ORDER BY series_key"
)
EVIDENCE_RELATION_PRESENCE_SQL = (
    "SELECT "
    "to_regclass('public.open_macro_v04_pit_evidence'), "
    "to_regclass('public.open_macro_v04_evidence_snapshots'), "
    "to_regclass('public.open_macro_v04_evidence_items'), "
    "to_regclass('public.open_macro_v04_categorical_taxonomy')"
)
INSERT_PRIVATE_SQL = (
    "INSERT INTO open_macro_v04_pit_evidence "
    "(decision_month, " + ", ".join(PRIVATE_COMPARE_COLUMNS) + ") VALUES "
    "(%(decision_month)s, "
    + ", ".join(f"%({column})s" for column in PRIVATE_COMPARE_COLUMNS)
    + ")"
)
INSERT_SNAPSHOT_SQL = (
    "INSERT INTO open_macro_v04_evidence_snapshots "
    "(decision_month, publication_status, coverage_state) "
    "VALUES (%(decision_month)s, %(publication_status)s, %(coverage_state)s)"
)
INSERT_ITEM_SQL = (
    "INSERT INTO open_macro_v04_evidence_items "
    "(decision_month, group_key, group_label, group_role, series_key, series_label, role, "
    "display_state, availability_state, evidence_state, freshness_state, pit_state) "
    "VALUES (%(decision_month)s, %(group_key)s, %(group_label)s, %(group_role)s, "
    "%(series_key)s, %(series_label)s, %(role)s, %(display_state)s, "
    "%(availability_state)s, %(evidence_state)s, %(freshness_state)s, %(pit_state)s)"
)
INSERT_TAXONOMY_SQL = (
    "INSERT INTO open_macro_v04_categorical_taxonomy (" + ", ".join(CATEGORICAL_COLUMNS) + ") "
    "VALUES (" + ", ".join(f"%({column})s" for column in CATEGORICAL_COLUMNS) + ")"
)


class EvidenceConflictError(RuntimeError):
    """An existing evidence month differs from the immutable requested snapshot."""


@dataclass(frozen=True)
class EvidenceMaterialization:
    decision_month: str
    header: dict[str, str]
    public_items: tuple[dict[str, str], ...]
    public_taxonomy: dict[str, Any]
    private_decision: dict[str, Any]
    private_lineage: dict[str, dict[str, Any]]


_SCHEMA = pathlib.Path(__file__).resolve().parents[2] / "schemas" / "open_macro_v04_pit_evidence.sql"


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_SCHEMA.read_text(encoding="utf-8"))
    conn.commit()


def pin_search_path(conn) -> None:
    """Resolve every bare relation name against the canonical public schema."""
    from src.workers.open_macro_v04 import pin_search_path as pin_v04_search_path

    pin_v04_search_path(conn)


def begin_consistent_read(conn) -> None:
    """Use one repeatable-read snapshot for every source leg of a publication."""
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")


def decision_cutoff(decision_as_of: dt.date) -> dt.datetime:
    """Certified chain cutoff: the month-end at 00:00 UTC."""
    return dt.datetime.combine(decision_as_of, dt.time.min, tzinfo=dt.timezone.utc)


def _as_date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def _as_utc(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _iso_utc(value: Any) -> str:
    return _as_utc(value).isoformat()


def _latest(rows: Iterable[Mapping[str, Any]], date_key: str) -> Mapping[str, Any] | None:
    return max(rows, key=lambda row: _as_date(row[date_key]), default=None)


def _selected_vintage_rows(
    rows: Iterable[Mapping[str, Any]], cutoff: dt.datetime, observation_horizon: dt.date
) -> tuple[Mapping[str, Any], ...]:
    """One latest-known vintage through the decision horizon, ordered oldest first."""
    selected: dict[dt.date, Mapping[str, Any]] = {}
    for row in rows:
        available_at = _as_utc(row["available_at"])
        if available_at > cutoff:
            continue
        period = _as_date(row["observation_period"])
        if period > observation_horizon:
            continue
        current = selected.get(period)
        candidate_key = (
            available_at,
            _as_date(row["vintage_date"]),
            int(row.get("revision_number", 0)),
        )
        current_key = (
            _as_utc(current["available_at"]),
            _as_date(current["vintage_date"]),
            int(current.get("revision_number", 0)),
        ) if current is not None else None
        if current_key is None or candidate_key > current_key:
            selected[period] = row
    return tuple(selected[period] for period in sorted(selected))


def _series_fingerprint(rows: Iterable[Mapping[str, Any]]) -> str:
    payload = [
        (
            _as_date(row["observation_period"]).isoformat(),
            format(float(row["value"]), ".17g"),
            _iso_utc(row["available_at"]),
            _as_date(row["vintage_date"]).isoformat(),
        )
        for row in rows
    ]
    canonical = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _mirror_matches_vintages(
    mirror_rows: Iterable[Mapping[str, Any]],
    vintage_rows: Iterable[Mapping[str, Any]],
) -> bool:
    mirror = [
        (_as_date(row["obs_date"]), float(row["value"]))
        for row in sorted(mirror_rows, key=lambda row: _as_date(row["obs_date"]))
    ]
    vintages = [
        (_as_date(row["observation_period"]), float(row["value"]))
        for row in vintage_rows
    ]
    return bool(mirror) and mirror == vintages


def _available_item(entry: tuple[str, str, str, str, str, str], *, pit_state: str) -> dict[str, str]:
    group_key, group_label, group_role, series_key, series_label, role = entry
    return {
        "group_key": group_key,
        "group_label": group_label,
        "group_role": group_role,
        "series_key": series_key,
        "series_label": series_label,
        "role": role,
        "display_state": "ready" if pit_state == "verified" else "limited",
        "availability_state": "available",
        "evidence_state": "observed",
        "freshness_state": "current" if pit_state == "verified" else "unknown",
        "pit_state": pit_state,
    }


def _unavailable_item(entry: tuple[str, str, str, str, str, str]) -> dict[str, str]:
    group_key, group_label, group_role, series_key, series_label, role = entry
    return {
        "group_key": group_key,
        "group_label": group_label,
        "group_role": group_role,
        "series_key": series_key,
        "series_label": series_label,
        "role": role,
        "display_state": "unavailable",
        "availability_state": "not_available",
        "evidence_state": "missing",
        "freshness_state": "unknown",
        "pit_state": "unavailable",
    }


def _coverage_state(items: Iterable[Mapping[str, str]]) -> str:
    values = list(items)
    if all(item["display_state"] == "ready" and item["pit_state"] == "verified" for item in values):
        return "complete"
    if all(item["display_state"] == "unavailable" for item in values):
        return "unavailable"
    return "partial"


def _public_taxonomy(decision: Mapping[str, Any], month: str) -> dict[str, Any]:
    """Derive the closed Light taxonomy without carrying any private decision facts."""
    fiscal_state = str(decision.get("fiscal_state"))
    guard_level = str(decision.get("guard_level"))
    guard_coverage = str(decision.get("guard_coverage"))
    quadrant_source = str(decision.get("quadrant_source"))
    decision_validity = str(decision.get("decision_validity"))
    decision_basis = str(decision.get("decision_basis"))
    quadrant_raw = decision.get("quadrant")
    quadrant = "unavailable" if quadrant_raw is None else str(quadrant_raw)
    fiscal_boundary = decision.get("fiscal_boundary")
    if fiscal_state not in {"dominance", "contained"}:
        raise ValueError("invalid categorical fiscal_state")
    if type(fiscal_boundary) is not bool:
        raise ValueError("invalid categorical fiscal_boundary")
    if guard_level not in {"off", "alert", "severe"}:
        raise ValueError("invalid categorical guard_level")
    if guard_coverage not in {"full", "partial_a", "partial_b", "blind"}:
        raise ValueError("invalid categorical guard_coverage")
    if quadrant not in {"recovery", "expansion", "slowdown", "contraction", "unavailable"}:
        raise ValueError("invalid categorical quadrant")
    if quadrant_source not in {"chain_fresh", "chain_carry", "no_signal", "proxy", "proxy_missing"}:
        raise ValueError("invalid categorical quadrant_source")
    if decision_validity not in {"fresh", "carried", "dominance_baseline", "guard_blind", "no_signal"}:
        raise ValueError("invalid categorical decision_validity")
    if decision_basis not in {"live", "bootstrap_replay"}:
        raise ValueError("invalid categorical decision_basis")
    source_without_quadrant = quadrant_source in {"no_signal", "proxy_missing"}
    if (quadrant == "unavailable") != source_without_quadrant:
        raise ValueError("invalid categorical quadrant_source combination")
    expected_validity = (
        "guard_blind" if guard_coverage == "blind" else
        "dominance_baseline" if fiscal_state == "dominance" else
        "fresh" if quadrant_source == "chain_fresh" else
        "carried" if quadrant_source == "chain_carry" else "no_signal"
    )
    if decision_validity != expected_validity:
        raise ValueError("invalid categorical decision_validity combination")
    cycle_direction = (
        "up" if quadrant in {"recovery", "expansion"} else
        "down" if quadrant in {"slowdown", "contraction"} else "unavailable"
    )
    taxonomy_state = (
        "no_signal" if cycle_direction == "unavailable" and quadrant_source == "no_signal" else
        "liquidity_unread" if cycle_direction == "unavailable" else
        "aligned_expansion" if fiscal_state == "dominance" and cycle_direction == "up" else
        "liquidity_led" if fiscal_state == "dominance" else
        "cycle_led" if cycle_direction == "up" else "aligned_contraction"
    )
    book = (
        "defensive" if guard_level == "severe" else
        "moderated" if guard_level == "alert" else
        "expansionary_baseline" if fiscal_state == "dominance" else "center"
    )
    return {
        "decision_month": month,
        "taxonomy_state": taxonomy_state,
        "fiscal_state": fiscal_state,
        "fiscal_boundary": fiscal_boundary,
        "guard_level": guard_level,
        "guard_coverage": guard_coverage,
        "quadrant": quadrant,
        "cycle_direction": cycle_direction,
        "decision_validity": decision_validity,
        "decision_basis": decision_basis,
        "quadrant_source": quadrant_source,
        "book": book,
    }


def build_materialization(
    decision: Mapping[str, Any],
    *,
    vintages: Iterable[Mapping[str, Any]],
    spy_rows: Iterable[Mapping[str, Any]],
    mirror_rows: Iterable[Mapping[str, Any]],
    chain_seed: Mapping[str, Any] | None = None,
    chain_seed_verified: bool = True,
    input_digest_matches: bool = False,
) -> EvidenceMaterialization:
    """Build one closed public catalogue and its private PIT lineage.

    The caller supplies rows already read with the cutoff.  The pure builder repeats
    the cutoff for vintage rows so a caller cannot accidentally surface later data.
    """
    as_of = _as_date(decision["as_of"])
    v04_cutoff = (
        _as_utc(decision["updated_at"])
        if decision.get("updated_at") is not None
        else dt.datetime.combine(as_of, dt.time.max, tzinfo=dt.timezone.utc)
    )
    seed_as_of = _as_date(chain_seed["as_of"]) if chain_seed is not None else as_of
    chain_cutoff = decision_cutoff(seed_as_of)
    month = as_of.strftime("%Y-%m")
    by_fred: dict[str, list[Mapping[str, Any]]] = {
        series_id: [] for series_id in FRED_SERIES_IDS
    }
    for row in vintages:
        series_id = str(row["series_id"])
        if series_id in by_fred:
            by_fred[series_id].append(row)
    by_mirror: dict[str, list[Mapping[str, Any]]] = {
        series_id: [] for series_id in V04_SERIES_IDS
    }
    for row in mirror_rows:
        series_id = str(row["series_id"])
        if series_id in by_mirror and _as_date(row["obs_date"]) <= as_of:
            by_mirror[series_id].append(row)
    spy = _latest(
        (
            row
            for row in spy_rows
            if row["ticker"] == "SPY" and _as_date(row["date"]) <= seed_as_of
        ),
        "date",
    )

    items: list[dict[str, str]] = []
    lineage: dict[str, dict[str, Any]] = {}
    for entry in EVIDENCE_CATALOG:
        series_id = entry[3]
        if series_id in CHAIN_SERIES_IDS:
            selected_rows = _selected_vintage_rows(by_fred[series_id], chain_cutoff, as_of)
            selected = selected_rows[-1] if selected_rows else None
            if selected is None:
                items.append(_unavailable_item(entry))
                lineage[series_id] = {
                    "source_kind": "certified_chain_vintage",
                    "decision_cutoff": chain_cutoff.isoformat(),
                    "pit_state": "unavailable",
                }
            else:
                pit_state = "verified" if chain_seed_verified else "unverified"
                item = _available_item(entry, pit_state=pit_state)
                if decision.get("quadrant_source") == "chain_carry":
                    item["evidence_state"] = "carried"
                items.append(item)
                lineage[series_id] = {
                    "source_kind": "certified_chain_vintage",
                    "pit_state": pit_state,
                    "decision_cutoff": chain_cutoff.isoformat(),
                    "source_set_fingerprint": _series_fingerprint(selected_rows),
                    "selected_observation_period": _as_date(selected["observation_period"]).isoformat(),
                    "selected_vintage_date": _as_date(selected["vintage_date"]).isoformat(),
                    "selected_available_at": _iso_utc(selected["available_at"]),
                    "selected_value": float(selected["value"]),
                    "revision_number": int(selected["revision_number"]),
                    "source": str(selected["source"]),
                    "source_spec_version": str(selected["source_spec_version"]),
                }
        elif series_id == "SPY":
            if spy is None:
                items.append(_unavailable_item(entry))
                lineage[series_id] = {"source_kind": "eod_prices", "pit_state": "unavailable"}
            else:
                items.append(_available_item(entry, pit_state="unverified"))
                lineage[series_id] = {
                    "source_kind": "eod_prices",
                    "pit_state": "unverified",
                    "selected_date": _as_date(spy["date"]).isoformat(),
                    "selected_value": float(spy["adj_close"]),
                }
        else:
            selected_rows = _selected_vintage_rows(by_fred[series_id], v04_cutoff, as_of)
            selected = selected_rows[-1] if selected_rows else None
            mirror_for_series = by_mirror[series_id]
            mirror_latest = _latest(mirror_for_series, "obs_date")
            if selected is None and mirror_latest is None:
                items.append(_unavailable_item(entry))
                lineage[series_id] = {
                    "source_kind": "macro_observation_vintage",
                    "pit_state": "unavailable",
                    "decision_cutoff": v04_cutoff.isoformat(),
                }
            else:
                proven = (
                    selected is not None
                    and decision.get("decision_basis") == "live"
                    and input_digest_matches
                    and _mirror_matches_vintages(mirror_for_series, selected_rows)
                )
                pit_state = "verified" if proven else "unverified"
                item = _available_item(entry, pit_state=pit_state)
                if selected is not None and mirror_latest is not None and not _mirror_matches_vintages(
                    mirror_for_series, selected_rows
                ):
                    item["evidence_state"] = "invalid"
                items.append(item)
                source_row = selected if selected is not None else mirror_latest
                if source_row is None:
                    raise AssertionError("available evidence must retain a source row")
                lineage[series_id] = {
                    "source_kind": (
                        "macro_observation_vintage" if selected is not None else "macro_data"
                    ),
                    "pit_state": pit_state,
                    "decision_cutoff": v04_cutoff.isoformat(),
                    "selected_observation_period": _as_date(
                        source_row.get("observation_period", source_row.get("obs_date"))
                    ).isoformat(),
                    "selected_value": float(source_row["value"]),
                }
                if selected_rows:
                    if selected is None:
                        raise AssertionError("selected vintage rows require a selected row")
                    lineage[series_id]["source_set_fingerprint"] = _series_fingerprint(
                        selected_rows
                    )
                    lineage[series_id]["selected_vintage_date"] = _as_date(
                        selected["vintage_date"]
                    ).isoformat()
                    lineage[series_id]["selected_available_at"] = _iso_utc(
                        selected["available_at"]
                    )
    return EvidenceMaterialization(
        decision_month=month,
        header={"publication_status": "open", "coverage_state": _coverage_state(items)},
        public_items=tuple(items),
        public_taxonomy=_public_taxonomy(decision, month),
        private_decision={
            "as_of": as_of.isoformat(),
            "quadrant_source": decision.get("quadrant_source"),
            "carry_age": decision.get("carry_age"),
            "decision_validity": decision.get("decision_validity"),
            "decision_basis": decision.get("decision_basis"),
            "decision_run_id": decision.get("run_id"),
            "decision_input_digest_sha256": decision.get("input_digest_sha256"),
            "decision_cutoff": v04_cutoff.isoformat(),
            "carry_seed_as_of": seed_as_of.isoformat(),
            "carry_seed_cutoff": chain_cutoff.isoformat(),
            "carry_seed_fingerprint": (
                chain_seed.get("pack_sha256") if chain_seed is not None else None
            ),
        },
        private_lineage=lineage,
    )


def _record(row: Any, columns: tuple[str, ...]) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(zip(columns, row, strict=True))


def _input_digest_matches(conn, decision: Mapping[str, Any], decision_as_of: dt.date) -> bool:
    """Recompute the exact v04 input digest over the same DB horizon."""
    from src.workers import open_macro_v04

    series = {
        series_id: open_macro_v04.read_macro_series(
            conn, series_id, decision_as_of, required=True
        )
        for series_id in open_macro_v04.REQUIRED_SERIES
    }
    series.update(
        {
            series_id: open_macro_v04.read_macro_series(
                conn, series_id, decision_as_of, required=False
            )
            for series_id in open_macro_v04.PROXY_SERIES
        }
    )
    _, price_rows = open_macro_v04.read_price_frame(conn, decision_as_of)
    _, _, chain_rows = open_macro_v04.read_chain(conn, decision_as_of)
    digest, _ = open_macro_v04.input_digest(series, chain_rows, price_rows)
    return digest == str(decision.get("input_digest_sha256", "")).strip()


def _certified_chain_inputs(
    conn,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    """Load the exact certified chain prefix plus its post-boundary live deltas."""
    from src.workers import open_macro_v03_chain

    pack_identity = open_macro_v03_chain.verify_pack()
    macro_rows, eod_rows, macro_boundary, eod_boundary = (
        open_macro_v03_chain.load_pack_inputs()
    )
    chain_source = {
        "source": str(macro_rows[0]["source"]),
        "source_spec_version": str(macro_rows[0]["source_spec_version"]),
    }
    macro_rows.extend(
        {**chain_source, **row}
        for row in open_macro_v03_chain.read_macro_delta(conn, macro_boundary)
    )
    eod_rows.extend(open_macro_v03_chain.read_eod_delta(conn, eod_boundary))
    return macro_rows, eod_rows, str(pack_identity["input_pack_sha256"])


def materialize_from_connection(conn, decision_as_of: dt.date) -> EvidenceMaterialization:
    """Read one existing v04 decision and its inputs through an acquired connection."""
    with conn.cursor() as cur:
        cur.execute(DECISION_SQL, {"decision_as_of": decision_as_of})
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"no valid open_macro_v04 decision exists for {decision_as_of}")
    decision = _record(
        row,
        (
            "as_of",
            "fiscal_state",
            "fiscal_boundary",
            "guard_level",
            "guard_coverage",
            "quadrant",
            "quadrant_source",
            "carry_age",
            "decision_validity",
            "decision_basis",
            "input_digest_sha256",
            "run_id",
            "updated_at",
        ),
    )
    carry_age = int(decision.get("carry_age") or 0)
    with conn.cursor() as cur:
        cur.execute(
            CHAIN_SEED_SQL,
            {"decision_as_of": decision_as_of, "carry_age": carry_age},
        )
        chain_row = cur.fetchone()
    chain_seed = (
        _record(chain_row, ("as_of", "status", "basis", "pack_sha256", "loaded_at"))
        if chain_row is not None
        else None
    )
    chain_vintages, spy_rows, certified_pack_sha256 = _certified_chain_inputs(conn)
    v04_cutoff = _as_utc(decision["updated_at"])
    with conn.cursor() as cur:
        cur.execute(
            VINTAGE_SQL,
            {
                "series_ids": list(V04_SERIES_IDS),
                "decision_cutoff": v04_cutoff,
                "decision_as_of": decision_as_of,
            },
        )
        v04_vintages = [
            _record(
                row,
                (
                    "series_id", "observation_period", "vintage_date", "value",
                    "available_at", "revision_number", "source", "source_spec_version",
                ),
            )
            for row in cur.fetchall()
        ]
    spy_rows = [
        {
            "ticker": row["ticker"],
            "date": row["date"],
            "adj_close": row.get("adj_close", row.get("adjusted_close")),
        }
        for row in spy_rows
    ]
    with conn.cursor() as cur:
        cur.execute(
            MIRROR_SQL,
            {"series_ids": list(V04_SERIES_IDS), "decision_as_of": decision_as_of},
        )
        mirror_rows = [
            _record(
                row,
                ("series_id", "obs_date", "value", "source", "created_at", "updated_at"),
            )
            for row in cur.fetchall()
        ]
    return build_materialization(
        decision,
        vintages=[*chain_vintages, *v04_vintages],
        spy_rows=spy_rows,
        mirror_rows=mirror_rows,
        chain_seed=chain_seed,
        chain_seed_verified=(
            chain_seed is not None
            and chain_seed.get("status") == "valid"
            and chain_seed.get("basis") == "certified_chain"
            and str(chain_seed.get("pack_sha256", "")).strip()
            == certified_pack_sha256
        ),
        input_digest_matches=_input_digest_matches(conn, decision, decision_as_of),
    )


def _ordered_items(items: Iterable[Mapping[str, str]]) -> tuple[tuple[str, ...], ...]:
    return tuple(sorted((tuple(item[column] for column in ITEM_COLUMNS) for item in items)))


def _private_records(
    materialization: EvidenceMaterialization,
) -> tuple[dict[str, Any], ...]:
    decision = materialization.private_decision
    records: list[dict[str, Any]] = []
    decision_run_id = str(decision.get("decision_run_id") or "").strip()
    if not decision_run_id:
        raise ValueError("decision run_id is required for private PIT lineage")
    input_digest = str(decision.get("decision_input_digest_sha256") or "").strip()
    if len(input_digest) != 64 or any(character not in "0123456789abcdef" for character in input_digest):
        raise ValueError("decision input digest must be a lowercase sha256")
    run_id = f"{decision_run_id}:pit-evidence"
    for item in materialization.public_items:
        series_key = item["series_key"]
        lineage = materialization.private_lineage[series_key]
        selected_period = lineage.get("selected_observation_period") or lineage.get(
            "selected_date"
        )
        cutoff = lineage.get("decision_cutoff") or decision["decision_cutoff"]
        fingerprint_payload = {
            "decision_month": materialization.decision_month,
            "series_key": series_key,
            "decision_run_id": decision.get("decision_run_id"),
            "decision_input_digest_sha256": decision.get("decision_input_digest_sha256"),
            "lineage": lineage,
        }
        fingerprint = hashlib.sha256(
            json.dumps(
                fingerprint_payload, sort_keys=True, separators=(",", ":"), default=str
            ).encode("utf-8")
        ).hexdigest()
        records.append(
            {
                "decision_month": materialization.decision_month,
                "decision_as_of": _as_date(decision["as_of"]),
                "decision_run_id": decision_run_id,
                "decision_input_digest_sha256": input_digest,
                "decision_basis": str(decision.get("decision_basis") or "bootstrap_replay"),
                "series_key": series_key,
                "value": (
                    float(lineage["selected_value"])
                    if lineage.get("selected_value") is not None
                    else None
                ),
                "unit": (
                    str(lineage["unit"]) if lineage.get("unit") is not None else None
                ),
                "observation_period": (
                    _as_date(selected_period) if selected_period is not None else None
                ),
                "release_at": (
                    _as_utc(lineage["selected_available_at"])
                    if lineage.get("selected_available_at") is not None
                    else None
                ),
                "ingested_at": (
                    _as_utc(lineage["ingested_at"])
                    if lineage.get("ingested_at") is not None
                    else None
                ),
                "vintage": lineage.get("selected_vintage_date"),
                "source": str(lineage.get("source_kind") or "unavailable"),
                "source_health": (
                    "invalid"
                    if item["evidence_state"] == "invalid"
                    else str(item["pit_state"])
                ),
                "fingerprint": fingerprint,
                "cutoff_at": _as_utc(cutoff),
                "carry_seed_decision_month": (
                    str(decision.get("carry_seed_as_of"))[:7]
                    if item["evidence_state"] == "carried"
                    else None
                ),
                "carry_seed_fingerprint": (
                    decision.get("carry_seed_fingerprint")
                    if item["evidence_state"] == "carried"
                    else None
                ),
                "materialization_run_id": run_id,
            }
        )
    return tuple(records)


def _ordered_private(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[tuple[Any, ...], ...]:
    def comparable(column: str, value: Any) -> Any:
        if column == "value" and value is not None:
            return Decimal(str(value))
        if isinstance(value, str) and column in {
            "decision_input_digest_sha256",
            "fingerprint",
            "carry_seed_fingerprint",
        }:
            return value.strip()
        return value

    return tuple(
        sorted(
            tuple(comparable(column, row[column]) for column in PRIVATE_COMPARE_COLUMNS)
            for row in rows
        )
    )


def _publication_outcome(
    conn,
    materialization: EvidenceMaterialization,
) -> tuple[str, str | None, tuple[dict[str, Any], ...]]:
    """Classify a materialization without mutating its immutable destination."""
    params = {"decision_month": materialization.decision_month}
    private_records = _private_records(materialization)
    with conn.cursor() as cur:
        cur.execute(EVIDENCE_RELATION_PRESENCE_SQL)
        relation_presence = cur.fetchone()
        if relation_presence is None or len(relation_presence) != 4:
            return "conflict", "evidence relation-presence check is incomplete", private_records
        if not any(relation_presence):
            return "would_publish", None, private_records
        if not all(relation_presence):
            return "conflict", "evidence relations are only partially bootstrapped", private_records
        cur.execute(EXISTING_SNAPSHOT_SQL, params)
        existing_header = cur.fetchone()
        cur.execute(EXISTING_ITEMS_SQL, params)
        existing_items = cur.fetchall()
        cur.execute(EXISTING_TAXONOMY_SQL, params)
        existing_taxonomy = cur.fetchone()
        cur.execute(EXISTING_PRIVATE_SQL, params)
        existing_private = cur.fetchall()
    if existing_header is not None or existing_items or existing_taxonomy is not None or existing_private:
        expected_header = (
            materialization.header["publication_status"],
            materialization.header["coverage_state"],
        )
        if existing_header is None or tuple(existing_header) != expected_header or (
            _ordered_items(materialization.public_items) != tuple(sorted(tuple(row) for row in existing_items))
        ):
            return (
                "conflict",
                f"evidence snapshot {materialization.decision_month} diverges from existing public rows",
                private_records,
            )
        if existing_taxonomy is None or tuple(existing_taxonomy) != tuple(
            materialization.public_taxonomy[column] for column in CATEGORICAL_COLUMNS
        ):
            return (
                "conflict",
                f"evidence snapshot {materialization.decision_month} diverges from existing taxonomy row",
                private_records,
            )
        if _ordered_private(private_records) != tuple(
            sorted(tuple(row) for row in existing_private)
        ):
            return (
                "conflict",
                f"evidence snapshot {materialization.decision_month} private lineage diverges",
                private_records,
            )
        return "no_op", None, private_records
    return "would_publish", None, private_records


def publication_outcome(conn, materialization: EvidenceMaterialization) -> str:
    """Return whether a read-only materialization can write, no-op, or conflicts."""
    outcome, _reason, _private_records = _publication_outcome(conn, materialization)
    return outcome


def publish(conn, materialization: EvidenceMaterialization) -> str:
    """Atomically insert a header and its 13 public status items.

    Exact replays are no-ops.  An existing snapshot with any distinct public status
    fails closed rather than rewriting historical evidence.
    """
    params = {"decision_month": materialization.decision_month}
    with conn.transaction():
        outcome, reason, private_records = _publication_outcome(conn, materialization)
        if outcome == "conflict":
            raise EvidenceConflictError(str(reason))
        if outcome == "no_op":
            return "no_op"
        with conn.cursor() as cur:
            cur.executemany(INSERT_PRIVATE_SQL, private_records)
            cur.executemany(
                INSERT_ITEM_SQL,
                [{**params, **item} for item in materialization.public_items],
            )
            cur.execute(INSERT_TAXONOMY_SQL, materialization.public_taxonomy)
            cur.execute(INSERT_SNAPSHOT_SQL, {**params, **materialization.header})
    return "published"


def run(dsn: str, *, calc_date: str | None = None) -> dict[str, Any]:
    """Materialize one existing month-end v04 decision without logging private data."""
    if os.getenv("OPEN_MACRO_V04_PIT_EVIDENCE_ENABLED", "").strip() != "1":
        return {"status": "disabled"}
    if calc_date is None:
        from zoneinfo import ZoneInfo

        from src.workers.open_macro_v04 import last_complete_month_end

        decision_as_of = last_complete_month_end(
            dt.datetime.now(ZoneInfo("America/New_York")).date()
        )
    else:
        decision_as_of = dt.date.fromisoformat(calc_date)
    conn = connect(dsn)
    try:
        with advisory_lock(conn, LOCK_OPEN_MACRO_V04_PIT_EVIDENCE) as acquired:
            if not acquired:
                return {"status": "lock_busy"}
            pin_search_path(conn)
            ensure_schema(conn)
            begin_consistent_read(conn)
            materialization = materialize_from_connection(conn, decision_as_of)
            status = publish(conn, materialization)
            conn.commit()
            return {
                "status": status,
                "decision_month": materialization.decision_month,
                "coverage_state": materialization.header["coverage_state"],
                "components": len(materialization.public_items),
            }
    finally:
        conn.close()


__all__ = [
    "EVIDENCE_CATALOG", "ITEM_COLUMNS", "EvidenceConflictError", "EvidenceMaterialization",
    "build_materialization", "decision_cutoff", "materialize_from_connection", "publish", "run",
]

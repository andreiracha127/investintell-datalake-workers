"""Fail-closed, deterministic publication writer for the six-relation bond panel."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .panel_config import config_hash

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_panel_v1.sql"
SURFACES = ("snapshot", "returns", "rv_signal", "rating_pit")
_TABLES = {
    "snapshot": "bond_panel_snapshot",
    "returns": "bond_panel_returns",
    "rv_signal": "bond_panel_rv_signal",
    "rating_pit": "bond_panel_rating_pit",
}


class MaterializationError(ValueError):
    """A typed publication failure; no pointer may be advanced."""

    def __init__(self, message: str, *, reason_code: str = "panel_failed") -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True)
class MaterializationResult:
    publication_id: str
    fingerprint: str
    status: str
    row_counts: dict[str, int]
    parent_publication_id: str | None


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def publication_id_for(as_of: date, code_revision: str, input_fingerprint: str) -> str:
    """Stable exact-input identity; revised same-day input creates a new pack."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"bond-panel-v1|{as_of.isoformat()}|{code_revision}|{input_fingerprint}"))


def _fingerprint(as_of: date, code_revision: str, facts: dict[str, list[dict[str, object]]], lineage: dict[str, str], parent: str | None) -> str:
    body = {"as_of": as_of.isoformat(), "code_revision": code_revision, "config_hash": config_hash(), "facts": facts, "source_lineage": lineage, "parent": parent}
    return hashlib.sha256(_canonical(body).encode()).hexdigest()


def _month(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _next_month(month: date) -> date:
    return date(month.year + month.month // 12, month.month % 12 + 1, 1)


def _validate_input(facts: dict[str, list[dict[str, object]]], source_lineage: dict[str, str]) -> tuple[dict[str, int], date, date]:
    if set(facts) != set(SURFACES):
        raise MaterializationError("exactly the four required fact surfaces must be supplied")
    counts = {surface: len(facts[surface]) for surface in SURFACES}
    if any(count == 0 for count in counts.values()):
        raise MaterializationError("zero rows on required surface")
    if not source_lineage:
        raise MaterializationError("source lineage is required")
    keys = {surface: {(str(row.get("month")), str(row.get("cusip_id"))) for row in facts[surface]} for surface in SURFACES}
    if any(len(keys[surface]) != len(facts[surface]) for surface in SURFACES):
        raise MaterializationError("duplicate fact keys", reason_code="panel_gate_failed")
    for row in facts["snapshot"]:
        if row.get("eligibility_state") not in ("included", "excluded") or not row.get("eligibility_reason"):
            raise MaterializationError("snapshot requires typed eligibility", reason_code="panel_gate_failed")
        if row.get("spread_definition", "ytm_minus_interpolated_dgs") != "ytm_minus_interpolated_dgs":
            raise MaterializationError("snapshot spread definition mismatch", reason_code="panel_gate_failed")
    included = {(str(row["month"]), str(row["cusip_id"])) for row in facts["snapshot"] if row.get("eligibility_state") == "included"}
    if not keys["rv_signal"].issubset(included):
        raise MaterializationError("rv signal lacks included snapshot", reason_code="panel_gate_failed")
    if not keys["returns"].issubset(keys["snapshot"]) or keys["rating_pit"] != keys["snapshot"]:
        raise MaterializationError("facts lack snapshot key coverage", reason_code="panel_gate_failed")
    for row in facts["returns"]:
        if row.get("exit_basis", "observed") not in ("observed", "matured", "distressed", "unexplained"):
            raise MaterializationError("invalid return exit basis", reason_code="panel_gate_failed")
    panel_months = [_month(row["month"]) for row in facts["snapshot"]]
    if not panel_months:
        raise MaterializationError("empty monthly panel")
    return counts, min(panel_months), max(panel_months)


def _partition(
    *, facts: dict[str, list[dict[str, object]]], parent: dict[str, Any] | None,
    first_month: date | None, last_closed_month: date | None, open_month: date | None,
    inferred_first: date, inferred_last: date,
) -> tuple[date, date, date | None]:
    if parent is None:
        first, last = first_month or inferred_first, last_closed_month or inferred_last
        if open_month is not None or first != inferred_first or last != inferred_last:
            raise MaterializationError("month partition invalid for base publication", reason_code="panel_gate_failed")
        return first, last, None
    if first_month is None or last_closed_month is None or open_month is None:
        raise MaterializationError("delta month partition must be explicit", reason_code="panel_gate_failed")
    parent_open_month = parent["open_month"]
    next_parent_month = _next_month(parent["last_closed_month"])
    if parent_open_month is not None and parent_open_month != next_parent_month:
        raise MaterializationError("month partition violates parent/open ordering", reason_code="panel_gate_failed")
    expected_closed_month = parent_open_month or next_parent_month
    expected_open_month = _next_month(expected_closed_month)
    if (
        first_month != parent["first_month"]
        or last_closed_month != expected_closed_month
        or open_month != expected_open_month
    ):
        raise MaterializationError("month partition violates parent/open ordering", reason_code="panel_gate_failed")
    allowed = {last_closed_month, open_month}
    if any(_month(row["month"]) not in allowed for surface in SURFACES for row in facts[surface]):
        raise MaterializationError("delta facts violate month partition", reason_code="panel_gate_failed")
    snapshot_months = {_month(row["month"]) for row in facts["snapshot"]}
    if snapshot_months != allowed:
        raise MaterializationError("delta snapshot must cover closed and open months", reason_code="panel_gate_failed")
    if any(_month(row["month"]) == open_month for surface in ("returns", "rv_signal") for row in facts[surface]):
        raise MaterializationError("open month may not carry returns or rv signal", reason_code="panel_gate_failed")
    return first_month, last_closed_month, open_month


class InMemoryPublicationStore:
    """Deterministic test store mirroring immutable delta/current semantics."""

    def __init__(self) -> None:
        self.publications: dict[str, dict[str, Any]] = {}
        self.pointer: str | None = None
        self.events: list[str] = []

    def logical_rows(self, publication_id: str, surface: str) -> list[dict[str, object]]:
        visited: set[str] = set()
        chain: list[dict[str, Any]] = []
        current: str | None = publication_id
        while current is not None:
            if current in visited:
                raise MaterializationError("publication ancestry cycle")
            visited.add(current)
            publication = self.publications.get(current)
            if publication is None or publication["status"] != "validated":
                raise MaterializationError("missing, nonvalidated, or purged parent publication")
            chain.append(publication)
            current = publication["parent_publication_id"]
        latest: dict[tuple[object, ...], dict[str, object]] = {}
        for publication in reversed(chain):
            for row in publication["facts"][surface]:
                key = (row.get("month"), row.get("cusip_id"))
                latest[key] = row
        return [latest[key] for key in sorted(latest, key=str)]

    def promote(
        self,
        publication_id: str,
        *,
        expected_parent_id: str | None,
        record_event: bool = True,
    ) -> None:
        """Advance a base pointer, or compare-and-set a delta pointer."""
        if expected_parent_id is not None and self.pointer != expected_parent_id:
            raise MaterializationError(
                "delta parent is no longer current pointer",
                reason_code="panel_gate_failed",
            )
        self.pointer = publication_id
        if record_event:
            self.events.append("pointer")


def install_schema(conn: Any) -> None:
    """Install the idempotent DDL under the caller's transaction discipline."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def _assert_parent(store: InMemoryPublicationStore, parent_publication_id: str | None) -> None:
    if parent_publication_id is None:
        return
    if store.pointer != parent_publication_id:
        raise MaterializationError("delta parent must be the current pointer", reason_code="panel_gate_failed")
    parent = store.publications.get(parent_publication_id)
    if parent is None or parent["status"] != "validated":
        raise MaterializationError("parent must be a non-purged validated publication")
    for surface in SURFACES:
        if not store.logical_rows(parent_publication_id, surface):
            raise MaterializationError("parent logical surface is empty")


def _materialize_memory(store: InMemoryPublicationStore, *, as_of: date, code_revision: str, facts: dict[str, list[dict[str, object]]], source_lineage: dict[str, str], parent_publication_id: str | None, first_month: date | None, last_closed_month: date | None, open_month: date | None) -> MaterializationResult:
    counts, inferred_first, inferred_last = _validate_input(facts, source_lineage)
    parent = store.publications.get(parent_publication_id) if parent_publication_id else None
    _assert_parent(store, parent_publication_id)
    first_month, last_closed_month, open_month = _partition(facts=facts, parent=parent, first_month=first_month, last_closed_month=last_closed_month, open_month=open_month, inferred_first=inferred_first, inferred_last=inferred_last)
    fingerprint = _fingerprint(as_of, code_revision, facts, source_lineage, parent_publication_id)
    publication_id = publication_id_for(as_of, code_revision, fingerprint)
    existing = store.publications.get(publication_id)
    if existing is not None:
        if existing["fingerprint"] != fingerprint or existing["row_counts"] != counts or existing["status"] != "validated":
            raise MaterializationError("deterministic rerun identity, fingerprint, or row coverage mismatch")
        for surface in SURFACES:
            if not store.logical_rows(publication_id, surface):
                raise MaterializationError("cannot reuse publication with empty logical surface")
        store.promote(
            publication_id,
            expected_parent_id=parent_publication_id,
            record_event=False,
        )
        return MaterializationResult(publication_id, fingerprint, "validated", counts, parent_publication_id)
    store.publications[publication_id] = {
        "status": "prepared", "fingerprint": fingerprint, "row_counts": counts,
        "facts": {surface: [dict(row) for row in facts[surface]] for surface in SURFACES},
        "source_lineage": dict(source_lineage), "gate_evidence": {"config_hash": config_hash(), "counts": counts},
        "parent_publication_id": parent_publication_id, "first_month": first_month,
        "last_closed_month": last_closed_month, "open_month": open_month,
    }
    store.events.extend(["prepared", "write"])
    publication = store.publications[publication_id]
    if publication["gate_evidence"]["config_hash"] != config_hash() or publication["row_counts"] != counts:
        publication["status"] = "failed"
        publication["failure_reason"] = "validation_gate_failed"
        raise MaterializationError("validation gate failed")
    publication["status"] = "validated"
    store.events.append("validated")
    for surface in SURFACES:
        if not store.logical_rows(publication_id, surface):
            publication["status"] = "failed"
            publication["failure_reason"] = "empty_logical_surface"
            raise MaterializationError("empty logical surface")
    try:
        store.promote(publication_id, expected_parent_id=parent_publication_id)
    except MaterializationError:
        publication["status"] = "failed"
        publication["failure_reason"] = "parent_no_longer_current"
        raise
    return MaterializationResult(publication_id, fingerprint, "validated", counts, parent_publication_id)


def _insert_rows(cur: Any, publication_id: str, facts: dict[str, list[dict[str, object]]]) -> None:
    for surface, table in _TABLES.items():
        if surface == "rating_pit":
            rows = [(publication_id, row["month"], row["cusip_id"], _canonical(row)) for row in facts[surface]]
            cur.executemany(f"INSERT INTO {table} (publication_id, month, cusip_id, rating_bucket, rating_as_of_month, rating_state, rating_reason, rating_staleness_months, source_lineage, payload) VALUES (%s, %s, %s, COALESCE(%s::jsonb ->> 'rating_bucket', 'NR'), (%s::jsonb ->> 'rating_as_of_month')::date, COALESCE(%s::jsonb ->> 'rating_state', 'static_missing'), COALESCE(%s::jsonb ->> 'rating_reason', 'static_rating_absent'), (%s::jsonb ->> 'rating_staleness_months')::int, COALESCE(%s::jsonb -> 'source_lineage', '{{}}'::jsonb), %s::jsonb)", [(pub, month, cusip, payload, payload, payload, payload, payload, payload, payload) for pub, month, cusip, payload in rows])
        elif surface == "returns":
            rows = [(publication_id, row["month"], row["cusip_id"], _canonical(row)) for row in facts[surface]]
            cur.executemany(f"INSERT INTO {table} (publication_id, month, cusip_id, total_return, price_return, carry_return, exit_basis, exit_reason, suspect, payload) VALUES (%s, %s, %s, (%s::jsonb ->> 'total_return')::numeric, (%s::jsonb ->> 'price_return')::numeric, (%s::jsonb ->> 'carry_return')::numeric, COALESCE(%s::jsonb ->> 'exit_basis', 'observed'), %s::jsonb ->> 'exit_reason', COALESCE((%s::jsonb ->> 'suspect')::boolean, false), %s::jsonb)", [(pub, month, cusip, payload, payload, payload, payload, payload, payload, payload) for pub, month, cusip, payload in rows])
        elif surface == "rv_signal":
            rows = [(publication_id, row["month"], row["cusip_id"], _canonical(row)) for row in facts[surface]]
            cur.executemany(f"INSERT INTO {table} (publication_id, month, cusip_id, issuer_id, ff17num, eligibility_state, eligibility_reason, price, amount_outstanding_k, maturity_years, traded_days, trade_count, dollar_volume, rel_bid_ask_bps, quoted_days, ytm, ytm_basis, mod_dur, mod_dur_source, spread_final_bps, residual_bps, rv_signal, price_source, flags, source_lineage, payload) VALUES (%s, %s, %s, %s::jsonb ->> 'issuer_id', (%s::jsonb ->> 'ff17num')::int, COALESCE(%s::jsonb ->> 'eligibility_state', 'included'), COALESCE(%s::jsonb ->> 'eligibility_reason', 'eligible'), (%s::jsonb ->> 'pr')::numeric, (%s::jsonb ->> 'amt_outstanding_k')::numeric, (%s::jsonb ->> 'bond_maturity')::numeric, (%s::jsonb ->> 'traded_days')::int, (%s::jsonb ->> 'trade_count')::int, (%s::jsonb ->> 'dollar_volume')::numeric, (%s::jsonb ->> 'rel_bid_ask_bps')::numeric, (%s::jsonb ->> 'quoted_days')::int, (%s::jsonb ->> 'ytm')::numeric, %s::jsonb ->> 'ytm_basis', (%s::jsonb ->> 'mod_dur')::numeric, %s::jsonb ->> 'mod_dur_source', COALESCE((%s::jsonb ->> 'spread_final_bps')::numeric, (%s::jsonb ->> 'spread_final')::numeric * 10000), (%s::jsonb ->> 'residual_bps')::numeric, (%s::jsonb ->> 'rv_signal')::numeric, %s::jsonb ->> 'price_source', COALESCE(%s::jsonb -> 'flags', '{{}}'::jsonb), COALESCE(%s::jsonb -> 'source_lineage', '{{}}'::jsonb), %s::jsonb)", [tuple([pub, month, cusip] + [payload] * 24) for pub, month, cusip, payload in rows])
        elif surface == "snapshot":
            rows = [(publication_id, row["month"], row["cusip_id"], _canonical(row)) for row in facts[surface]]
            cur.executemany(
                f"INSERT INTO {table} (publication_id, month, cusip_id, issuer_id, issuer_identity_state, ff17num, eligibility_state, eligibility_reason, currency, asset_class, amount_outstanding_k, maturity_date, maturity_years, coupon_pct, price, price_source, db_type, ytm, ytm_basis, mod_dur, mod_dur_source, spread_final, spread_final_bps, spread_definition, spread_source, rating_bucket, rating_state, traded_days, trade_count, dollar_volume, rel_bid_ask_bps, quoted_days, terms_source, source_lineage, payload) VALUES (%s, %s, %s, %s::jsonb ->> 'issuer_id', COALESCE(%s::jsonb ->> 'issuer_identity_state', 'unresolved'), (%s::jsonb ->> 'ff17num')::int, COALESCE(%s::jsonb ->> 'eligibility_state', 'included'), COALESCE(%s::jsonb ->> 'eligibility_reason', 'eligible'), %s::jsonb ->> 'currency', %s::jsonb ->> 'asset_class', (%s::jsonb ->> 'amt_outstanding_k')::numeric, (%s::jsonb ->> 'maturity_date')::date, (%s::jsonb ->> 'bond_maturity')::numeric, (%s::jsonb ->> 'coupon_pct')::numeric, (%s::jsonb ->> 'pr')::numeric, %s::jsonb ->> 'price_source', (%s::jsonb ->> 'db_type')::int, (%s::jsonb ->> 'ytm')::numeric, %s::jsonb ->> 'ytm_basis', (%s::jsonb ->> 'mod_dur')::numeric, %s::jsonb ->> 'mod_dur_source', (%s::jsonb ->> 'spread_final')::numeric, (%s::jsonb ->> 'spread_final_bps')::numeric, COALESCE(%s::jsonb ->> 'spread_definition', 'ytm_minus_interpolated_dgs'), %s::jsonb ->> 'spread_source', COALESCE(%s::jsonb ->> 'rating_bucket', 'NR'), COALESCE(%s::jsonb ->> 'rating_state', 'missing'), (%s::jsonb ->> 'traded_days')::int, (%s::jsonb ->> 'trade_count')::int, (%s::jsonb ->> 'dollar_volume')::numeric, (%s::jsonb ->> 'rel_bid_ask_bps')::numeric, (%s::jsonb ->> 'quoted_days')::int, %s::jsonb ->> 'terms_source', COALESCE(%s::jsonb -> 'source_lineage', '{{}}'::jsonb), %s::jsonb)",
                [tuple([pub, month, cusip] + [payload] * 32) for pub, month, cusip, payload in rows],
            )
        else:
            state_column = "eligibility_state"
            rows = [(publication_id, row["month"], row["cusip_id"], _canonical(row)) for row in facts[surface]]
            cur.executemany(f"INSERT INTO {table} (publication_id, month, cusip_id, {state_column}, eligibility_reason, payload) VALUES (%s, %s, %s, COALESCE((%s::jsonb ->> 'eligibility_state'), 'included'), COALESCE((%s::jsonb ->> 'eligibility_reason'), 'eligible'), %s::jsonb)", [(pub, month, cusip, payload, payload, payload) for pub, month, cusip, payload in rows])


def _promote_pointer(cur: Any, publication_id: str, parent_publication_id: str | None) -> None:
    """Advance base publications, and compare-and-set delta publications."""
    if parent_publication_id is None:
        cur.execute(
            "INSERT INTO bond_panel_app_pointer (product, publication_id) VALUES "
            "('bond_panel_v1', %s) ON CONFLICT (product) DO UPDATE SET "
            "publication_id = excluded.publication_id, changed_at = now()",
            (publication_id,),
        )
        return
    cur.execute(
        "UPDATE bond_panel_app_pointer SET publication_id = %s, changed_at = now() "
        "WHERE product = 'bond_panel_v1' AND publication_id = %s",
        (publication_id, parent_publication_id),
    )
    if cur.rowcount != 1:
        raise MaterializationError(
            "delta parent is no longer current pointer",
            reason_code="panel_gate_failed",
        )


def _materialize_postgres(conn: Any, *, as_of: date, code_revision: str, facts: dict[str, list[dict[str, object]]], source_lineage: dict[str, str], parent_publication_id: str | None, first_month: date | None, last_closed_month: date | None, open_month: date | None) -> MaterializationResult:
    counts, inferred_first, inferred_last = _validate_input(facts, source_lineage)
    fingerprint = _fingerprint(as_of, code_revision, facts, source_lineage, parent_publication_id)
    publication_id = publication_id_for(as_of, code_revision, fingerprint)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT input_fingerprint, publication_status FROM bond_panel_publications WHERE publication_id = %s FOR UPDATE", (publication_id,))
            existing = cur.fetchone()
            if existing is not None:
                if existing[0] != fingerprint or existing[1] != "validated":
                    raise MaterializationError("deterministic rerun identity or lifecycle mismatch")
                for surface, table in _TABLES.items():
                    cur.execute(f"SELECT count(*) FROM {table} WHERE publication_id = %s", (publication_id,))
                    if cur.fetchone()[0] != counts[surface]:
                        raise MaterializationError("deterministic rerun row coverage mismatch")
                _promote_pointer(cur, publication_id, parent_publication_id)
                return MaterializationResult(publication_id, fingerprint, "validated", counts, parent_publication_id)
            parent: dict[str, Any] | None = None
            if parent_publication_id is not None:
                cur.execute("SELECT p.publication_status, p.first_month, p.last_closed_month, p.open_month, p.config_hash FROM bond_panel_publications p JOIN bond_panel_app_pointer pointer ON pointer.publication_id = p.publication_id WHERE p.publication_id = %s AND pointer.product = 'bond_panel_v1' FOR KEY SHARE", (parent_publication_id,))
                parent = cur.fetchone()
                if parent is None or parent[0] != "validated":
                    raise MaterializationError(
                        "parent must be the validated current pointer"
                    )
                if parent[4] != config_hash() or last_closed_month is not None and last_closed_month < parent[2]:
                    raise MaterializationError("parent config or month regression", reason_code="panel_gate_failed")
                parent = {"status": parent[0], "first_month": parent[1], "last_closed_month": parent[2], "open_month": parent[3]}
            first_month, last_closed_month, open_month = _partition(facts=facts, parent=parent, first_month=first_month, last_closed_month=last_closed_month, open_month=open_month, inferred_first=inferred_first, inferred_last=inferred_last)
            evidence = {"config_hash": config_hash(), "row_counts": counts, "first_month": first_month.isoformat(), "last_closed_month": last_closed_month.isoformat()}
            cur.execute("INSERT INTO bond_panel_publications (publication_id, parent_publication_id, publication_status, config_hash, input_fingerprint, code_revision, first_month, last_closed_month, open_month, snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows, source_lineage, gate_evidence) VALUES (%s, %s, 'prepared', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)", (publication_id, parent_publication_id, config_hash(), fingerprint, code_revision, first_month, last_closed_month, open_month, counts["snapshot"], counts["rv_signal"], counts["returns"], counts["rating_pit"], _canonical(source_lineage), _canonical(evidence)))
            _insert_rows(cur, publication_id, facts)
            for surface, table in _TABLES.items():
                cur.execute(f"SELECT count(*) FROM {table} WHERE publication_id = %s", (publication_id,))
                if cur.fetchone()[0] != counts[surface]:
                    raise MaterializationError(f"row coverage gate failed for {surface}")
            cur.execute("UPDATE bond_panel_publications SET publication_status = 'validated', validated_at = now() WHERE publication_id = %s AND publication_status = 'prepared' AND config_hash = %s AND input_fingerprint = %s", (publication_id, config_hash(), fingerprint))
            if cur.rowcount != 1:
                raise MaterializationError("config and fingerprint gate failed")
            _promote_pointer(cur, publication_id, parent_publication_id)
    return MaterializationResult(publication_id, fingerprint, "validated", counts, parent_publication_id)


def materialize_panel(store: Any, *, as_of: date, code_revision: str, facts: dict[str, list[dict[str, object]]], source_lineage: dict[str, str], parent_publication_id: str | None = None, first_month: date | None = None, last_closed_month: date | None = None, open_month: date | None = None) -> MaterializationResult:
    if isinstance(store, InMemoryPublicationStore):
        return _materialize_memory(store, as_of=as_of, code_revision=code_revision, facts=facts, source_lineage=source_lineage, parent_publication_id=parent_publication_id, first_month=first_month, last_closed_month=last_closed_month, open_month=open_month)
    return _materialize_postgres(store, as_of=as_of, code_revision=code_revision, facts=facts, source_lineage=source_lineage, parent_publication_id=parent_publication_id, first_month=first_month, last_closed_month=last_closed_month, open_month=open_month)


def materialize(conn: Any, **kwargs: Any) -> MaterializationResult:
    """Production entry point; facts must be DB-shaped rows supplied by the worker."""
    return materialize_panel(conn, **kwargs)

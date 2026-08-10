"""Railway adapter for loading one sealed Regulation S registry draft or approval.

The evidence collector remains dry-run-only.  This worker is the deliberately
separate, operator-authorized database path: ``draft`` loads the immutable
registry composition and ``approve`` closes an already-loaded composition.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import os
from pathlib import Path
from typing import Any

from scripts.backfill_bond_distribution_series import build_registry_bundle
from src.bonds.distribution_series import (
    DistributionMappingSnapshot,
    DistributionPairDecision,
    DistributionPairIdentifier,
    DistributionParserObservation,
    DistributionSourceEvidence,
    approve_mapping_snapshot,
    distribution_snapshot_content_hash,
    load_distribution_registry,
)
from src.db import LOCK_BOND_DISTRIBUTION_REGISTRY_BACKFILL, advisory_lock, connect, resolve_dsn


_ENV_OUTPUT_ROOT = "BOND_DISTRIBUTION_OUTPUT_ROOT"
_ENV_SNAPSHOT_ID = "BOND_DISTRIBUTION_SNAPSHOT_ID"
_ENV_MODE = "BOND_DISTRIBUTION_LOAD_MODE"
_ENV_REVISION = "CODE_REVISION"
_ENV_AUTHORIZATION = "BOND_DISTRIBUTION_LOAD_AUTHORIZATION"
_MODES = {"draft", "approve"}


def _required_environment() -> tuple[Path, str, str]:
    """Validate the complete authorization contract without exposing its values."""
    values = {name: os.environ.get(name) for name in (
        _ENV_OUTPUT_ROOT, _ENV_SNAPSHOT_ID, _ENV_MODE, _ENV_REVISION, _ENV_AUTHORIZATION,
    )}
    mode = str(values[_ENV_MODE])
    if mode not in _MODES:
        raise ValueError("registry load mode is invalid")
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ValueError("registry load environment is incomplete")
    if values[_ENV_AUTHORIZATION] != values[_ENV_REVISION]:
        raise ValueError("registry load authorization is invalid")
    output_root = Path(str(values[_ENV_OUTPUT_ROOT]))
    if not output_root.is_dir():
        raise ValueError("registry output root is unavailable")
    return output_root, str(values[_ENV_SNAPSHOT_ID]), mode


def _parse_datetime(value: object, field: str, *, optional: bool = False) -> datetime | None:
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise ValueError(f"bundle {field} is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"bundle {field} is invalid") from error
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_date(value: object, field: str, *, optional: bool = False) -> date | None:
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise ValueError(f"bundle {field} is invalid")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"bundle {field} is invalid") from error


def _rows(bundle: dict[str, object], snapshot_id: str) -> tuple[
    str,
    tuple[DistributionSourceEvidence, ...],
    tuple[DistributionParserObservation, ...],
    DistributionMappingSnapshot,
    tuple[DistributionPairDecision, ...],
    tuple[DistributionPairIdentifier, ...],
]:
    """Convert a sealed prospective payload to public registry dataclasses."""
    if bundle.get("database_writes") != 0:
        raise ValueError("bundle must be dry-run-only")
    snapshots = bundle.get("mapping_snapshot_rows")
    approvals = bundle.get("snapshot_approval_rows")
    decisions_raw = bundle.get("pair_decision_rows")
    identifiers_raw = bundle.get("pair_identifier_rows")
    sources_raw = bundle.get("source_evidence_rows")
    observations_raw = bundle.get("parser_observation_rows")
    if not all(isinstance(rows, list) for rows in (
        snapshots, approvals, decisions_raw, identifiers_raw, sources_raw, observations_raw,
    )):
        raise ValueError("bundle registry rows are invalid")
    if len(snapshots) != 1 or not isinstance(snapshots[0], dict):
        raise ValueError("bundle requires exactly one draft snapshot")
    snapshot_raw = snapshots[0]
    if (
        snapshot_raw.get("snapshot_id") != snapshot_id
        or snapshot_raw.get("snapshot_status") != "draft"
        or not isinstance(snapshot_raw.get("content_hash"), str)
    ):
        raise ValueError("bundle snapshot does not match authorization")
    if len(approvals) != 1 or not isinstance(approvals[0], dict) or approvals[0] != {
        "snapshot_id": snapshot_id, "content_hash": snapshot_raw["content_hash"],
    }:
        raise ValueError("bundle approval is invalid")
    if not decisions_raw or not identifiers_raw:
        raise ValueError("bundle has an empty approved cohort")

    try:
        sources = tuple(DistributionSourceEvidence(
            str(row["source_evidence_id"]), str(row["sec_accession"]), str(row["form_type"]),
            str(row["document_type"]), str(row["source_url"]),
            _parse_datetime(row["retrieved_at"], "retrieved_at"), str(row["raw_document_sha256"]),
            str(row["parser_version"]), _parse_datetime(row.get("filed_at"), "filed_at", optional=True),
            row.get("search_query_id"), row.get("document_url"),
        ) for row in sources_raw if isinstance(row, dict))
        observations = tuple(DistributionParserObservation(
            str(row["parser_observation_id"]), str(row["source_evidence_id"]), str(row["parser_version"]),
            str(row["block_locator"]), str(row["exact_source_label"]), str(row["source_value"]),
            row.get("normalized_value"), str(row["observation_state"]),
        ) for row in observations_raw if isinstance(row, dict))
        decisions = tuple(DistributionPairDecision(
            str(row["decision_id"]), str(row["snapshot_id"]), str(row["decision_state"]),
            row.get("source_observation_id"), _parse_date(row["valid_from"], "valid_from"),
            _parse_date(row.get("valid_to"), "valid_to", optional=True), row.get("pair_key"),
        ) for row in decisions_raw if isinstance(row, dict))
        identifiers = tuple(DistributionPairIdentifier(
            str(row["identifier_id"]), str(row["decision_id"]), str(row["source_observation_id"]),
            str(row["distribution_rule"]), str(row["identifier_kind"]), str(row["identifier_value"]),
            str(row["identifier_tenure"]), _parse_date(row["valid_from"], "valid_from"),
            _parse_date(row.get("valid_to"), "valid_to", optional=True),
        ) for row in identifiers_raw if isinstance(row, dict))
    except (KeyError, TypeError) as error:
        raise ValueError("bundle registry row is invalid") from error
    if any(not isinstance(row, dict) for rows in (sources_raw, observations_raw, decisions_raw, identifiers_raw) for row in rows):
        raise ValueError("bundle registry row is invalid")
    if any(decision.snapshot_id != snapshot_id or decision.decision_state != "approved" for decision in decisions):
        raise ValueError("bundle decision is not approved for the snapshot")
    decision_ids = {decision.decision_id for decision in decisions}
    source_ids = {source.source_evidence_id for source in sources}
    observation_ids = {observation.parser_observation_id for observation in observations}
    if any(observation.source_evidence_id not in source_ids for observation in observations):
        raise ValueError("bundle observation does not bind source evidence")
    if any(
        decision.source_observation_id not in observation_ids for decision in decisions
    ):
        raise ValueError("bundle decision does not bind source evidence")
    if any(identifier.decision_id not in decision_ids or identifier.source_observation_id not in observation_ids for identifier in identifiers):
        raise ValueError("bundle identifier does not bind approved evidence")
    if distribution_snapshot_content_hash(snapshot_id, decisions, identifiers) != snapshot_raw["content_hash"]:
        raise ValueError("bundle content hash is invalid")
    return (
        str(snapshot_raw["content_hash"]), sources, observations,
        DistributionMappingSnapshot(snapshot_id, "draft", str(snapshot_raw["content_hash"])),
        decisions, identifiers,
    )


def _require_schema(conn: Any) -> None:
    """Fail closed when migrations have not provisioned every guarded registry table."""
    row = conn.execute(
        "SELECT to_regclass('bond_distribution_source_evidence'), "
        "to_regclass('bond_distribution_parser_observation'), "
        "to_regclass('bond_distribution_mapping_snapshot'), "
        "to_regclass('bond_distribution_pair_decision'), "
        "to_regclass('bond_distribution_pair_identifier'), "
        "to_regclass('bond_distribution_snapshot_approval')"
    ).fetchone()
    if not row or not all(row):
        raise RuntimeError("distribution registry schema is unavailable")


def run(dsn: str) -> dict[str, Any]:
    """Load one authorized sealed draft, or approve that already-loaded draft."""
    output_root, snapshot_id, mode = _required_environment()
    bundle = build_registry_bundle(output_root, snapshot_id)
    content_hash, sources, observations, snapshot, decisions, identifiers = _rows(bundle, snapshot_id)
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_DISTRIBUTION_REGISTRY_BACKFILL) as acquired:
        if not acquired:
            return {
                "state": "locked", "mode": mode, "snapshot_id": snapshot_id,
                "content_hash": content_hash, "aborted": True,
            }
        _require_schema(conn)
        if mode == "draft":
            rows = load_distribution_registry(
                conn, source_evidence=sources, parser_observations=observations, snapshots=(snapshot,),
                decisions=decisions, identifiers=identifiers, approvals=(),
            )
            return {"state": "ok", "mode": mode, "snapshot_id": snapshot_id, "content_hash": content_hash, "rows": rows}
        with conn.transaction():
            replay = load_distribution_registry(
                conn, source_evidence=sources, parser_observations=observations, snapshots=(snapshot,),
                decisions=decisions, identifiers=identifiers, approvals=(),
            )
            if any(inserted != 0 for inserted in replay.values()):
                raise RuntimeError("approval requires an already-loaded immutable draft bundle")
            inserted = approve_mapping_snapshot(conn, snapshot_id=snapshot_id, content_hash=content_hash)
        return {"state": "ok", "mode": mode, "snapshot_id": snapshot_id, "content_hash": content_hash, "approval_inserted": inserted}

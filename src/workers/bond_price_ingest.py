"""Artifact-pinned ingest worker for the bond price observation source lane.

Reads ONE operator-pinned source artifact (local path, or an https locator
fetched to a local temp file), verifies its SHA-256 pin BEFORE parsing, then —
in a single transaction — lands every artifact row into the immutable
``bond_price_observation`` table through the EXISTING resolution path
(``price_observations.load_price_observations``; identifier normalization is
reused, never re-implemented) and registers the validated source pair
(``sec_ingestion_runs`` + ``sec_source_packages`` + raw-validated visibility)
that the price materializer discovers via its ``sec_validated_raw_runs`` SELECT
(src/workers/bond_price_observations.py). A refusal at ANY gate writes nothing.

Source identity is confidential (Global Constraint): this module carries no
vendor identity. The artifact locator and pin arrive ONLY through the worker's
environment contract below; those env var names never leave this module's env
handling — envelopes, database values and error details carry at most the
opaque ``source_contract_ref`` token (``bond_price_source_v1@<sha256-prefix>``).

Contract: ``run(dsn=None) -> dict`` (standard worker envelope).
  ok:      ``{"state": "ok", "inserted": N, "skipped_existing": M, ...}``
  refused: ``{"state": "refused", "reason": "sha256_mismatch" |
             "schema_contract_violation" | "artifact_unavailable", ...}``
Idempotent re-run with the same pinned artifact: the run/package pair is
resumed (business-key unique), observation ids are deterministic per
``(artifact sha256, artifact row)`` so re-inserts collide on the primary key
and are skipped — ``skipped_existing`` equals the first run's ``inserted``.
"""
from __future__ import annotations

import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

from src.bonds import price_observations, source_artifact
from src.bonds.errors import BondError
from src.bonds.price_observations import PriceObservationInput
from src.db import LOCK_BOND_PRICE_INGEST, advisory_lock, connect, resolve_dsn
from src.sec_regulatory import manifests

# Environment contract. The names are the ONLY allowed appearance of the vendor
# prefix in this repo and never travel beyond this module's env handling.
ENV_ARTIFACT_PIN = "OSBAP_ARTIFACT_SHA256"
ENV_ARTIFACT_PATH = "OSBAP_ARTIFACT_PATH"
ENV_ARTIFACT_URL = "OSBAP_ARTIFACT_URL"

# Opaque internal registration identity (no vendor name anywhere).
SOURCE_FAMILY = "bond_price"
PARSER_VERSION = "bond_price_ingest_v1"
FILE_RELATIVE_PATH = "source.parquet"

# Deterministic per-artifact-row observation identity (uuid5 over
# ``<artifact sha256>:<row ordinal>``): the idempotent-rerun PK collision path.
_NAMESPACE_PRICE_INGEST = UUID("b0d5ec00-0000-5000-a000-707269636532")

_LOAD_BATCH_SIZE = 5_000
_FETCH_CHUNK_BYTES = 1024 * 1024


def _refusal(error: BondError) -> dict[str, Any]:
    envelope: dict[str, Any] = {"state": "refused", "reason": error.code}
    if error.details:
        envelope["detail"] = error.details
    return envelope


def _fetch_artifact(url: str, target: Path) -> None:
    """Stream the remote artifact to a local file; details never echo the locator."""
    import httpx

    try:
        with httpx.Client(follow_redirects=True, timeout=60.0) as client:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                with target.open("wb") as output:
                    for chunk in response.iter_bytes(_FETCH_CHUNK_BYTES):
                        output.write(chunk)
    except Exception as exc:
        raise BondError(
            source_artifact.REFUSAL_ARTIFACT_UNAVAILABLE, {"detail": "fetch_failed"}
        ) from exc


def _quarter(as_of: date) -> str:
    return f"{as_of.year}Q{(as_of.month - 1) // 3 + 1}"


def _observation_input(
    descriptor: source_artifact.ArtifactDescriptor, row: source_artifact.PriceRow
) -> PriceObservationInput:
    return PriceObservationInput(
        observation_id=str(
            uuid5(_NAMESPACE_PRICE_INGEST, f"{descriptor.sha256}:{row.row_number}")
        ),
        observation_date=row.observation_date,
        cusip9_input=row.cusip_id,
        price=row.price,
        # Declared by the pinned source contract (see source_artifact): pr is a
        # clean transaction price in percent of par — typed, never fabricated.
        price_type=source_artifact.CONTRACT_PRICE_TYPE,
        accrued_treatment=source_artifact.CONTRACT_ACCRUED_TREATMENT,
        ytm=row.ytm,
        db_type=row.db_type,
        source_lineage={
            "source_contract_ref": descriptor.source_contract_ref,
            "artifact_sha256": descriptor.sha256,
            "artifact_row_number": row.row_number,
        },
    )


def _ingest(conn: Any, descriptor: source_artifact.ArtifactDescriptor) -> dict[str, Any]:
    """Land + register one pinned artifact inside the caller's transaction."""
    token = descriptor.source_contract_ref
    as_of = descriptor.last_observation_date
    quarter = _quarter(as_of)

    run = manifests.create_or_resume_run(
        conn,
        source_family=SOURCE_FAMILY,
        package_sha256=descriptor.sha256,
        parser_version=PARSER_VERSION,
        source_quarter=quarter,
        package_relative_path=token,
    )
    state = run.current_state
    if state == "discovered":
        run = manifests.transition_run(
            conn,
            run_id=run.run_id,
            expected_state="discovered",
            target_state="loading",
            detail="bond price artifact loading",
        )
        state = run.current_state
    # ``loading`` = first landing of this artifact: manifest accounting, issue
    # evidence and raw validation happen now. Any later state = idempotent
    # resume: accounting is immutable, so only the PK-collision re-insert path
    # and the package upsert run.
    landing = state == "loading"

    file_id = None
    if landing:
        file_id = manifests.register_file(
            conn,
            run_id=run.run_id,
            relative_path=FILE_RELATIVE_PATH,
            sha256=descriptor.parquet_sha256,
            byte_size=descriptor.parquet_byte_size,
            state="loading",
        )

    # COPY fast path: resolved batches are streamed into a session-local
    # staging table and merged with ONE server-side INSERT — a multi-million
    # row artifact never pays per-row round-trips over TLS. Same transaction,
    # same resolution, same PK-collision idempotency as the per-row protocol.
    price_observations.create_price_observation_staging(conn)
    staged = 0
    quarantined = 0
    batch: list[PriceObservationInput] = []
    for row in source_artifact.iter_price_rows(descriptor):
        if row.observation_date is None:
            # Unparseable trade date: the row cannot land in the NOT NULL date
            # column, so it is QUARANTINED with lexical evidence — the
            # manifests non-resolvable protocol; never dropped silently.
            quarantined += 1
            if landing:
                manifests.record_issue(
                    conn,
                    source_file_id=file_id,
                    source_row_number=row.row_number,
                    typed_error_code="invalid_observation_date",
                    status="quarantined",
                    table_name="bond_price_observation",
                    column_name="trd_exctn_dt",
                    raw_lexical_value=(
                        None
                        if row.observation_date_raw is None
                        else str(row.observation_date_raw)
                    ),
                )
            continue
        # Unparseable identifiers stay IN the batch: the existing resolution
        # path lands them as identity_state='unresolved' (landed, never
        # published) — the bond_price_observation non-resolvable protocol.
        batch.append(_observation_input(descriptor, row))
        if len(batch) >= _LOAD_BATCH_SIZE:
            staged += price_observations.bulk_load_price_observations(
                conn, batch, as_of=as_of, source_run_id=run.run_id
            )
            batch = []
    if batch:
        staged += price_observations.bulk_load_price_observations(
            conn, batch, as_of=as_of, source_run_id=run.run_id
        )

    inserted = price_observations.merge_staged_price_observations(conn)
    skipped_existing = staged - inserted

    if landing:
        manifests.register_file(
            conn,
            run_id=run.run_id,
            relative_path=FILE_RELATIVE_PATH,
            sha256=descriptor.parquet_sha256,
            byte_size=descriptor.parquet_byte_size,
            expected_count=descriptor.row_count,
            data_count=descriptor.row_count,
            lexical_count=descriptor.row_count,
            typed_success_count=staged,
            quarantine_count=quarantined,
            reject_count=0,
            state="accounted",
            source_file_id=file_id,
        )
        manifests.validate_raw_run(conn, run_id=run.run_id)

    package = manifests.register_package_discovery(
        conn,
        source_family=SOURCE_FAMILY,
        source_quarter=quarter,
        package_relative_path=token,
        package_state="loaded",
        package_sha256=descriptor.sha256,
        run_id=run.run_id,
    )

    return {
        "state": "ok",
        "staged": staged,
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "quarantined": quarantined,
        "source_contract_ref": token,
        "run_id": str(run.run_id),
        "package_id": str(package.package_id),
        "as_of": as_of.isoformat(),
    }


def run(dsn: str | None = None) -> dict[str, Any]:
    # Fail-closed env gates BEFORE any database connection: pin first, then the
    # artifact locator. A refusal here trivially writes nothing.
    pin_value = os.getenv(ENV_ARTIFACT_PIN)
    local_path = os.getenv(ENV_ARTIFACT_PATH)
    remote_url = os.getenv(ENV_ARTIFACT_URL)

    fetched_dir: tempfile.TemporaryDirectory[str] | None = None
    descriptor: source_artifact.ArtifactDescriptor | None = None
    try:
        try:
            pin = source_artifact.require_pin(pin_value)
            if local_path:
                artifact_path = Path(local_path)
            elif remote_url:
                fetched_dir = tempfile.TemporaryDirectory(prefix="bond-price-fetch-")
                artifact_path = Path(fetched_dir.name) / "artifact.bin"
                _fetch_artifact(remote_url, artifact_path)
            else:
                raise BondError(
                    source_artifact.REFUSAL_ARTIFACT_UNAVAILABLE,
                    {"detail": "no_artifact_locator"},
                )
            descriptor = source_artifact.validate_artifact(
                artifact_path, expected_sha256=pin
            )
        except BondError as error:
            return _refusal(error)

        with connect(resolve_dsn(dsn)) as conn:
            with advisory_lock(conn, LOCK_BOND_PRICE_INGEST) as acquired:
                if not acquired:
                    return {"state": "locked"}
                manifests.install_schema(conn)
                price_observations.install_schema(conn)
                result = _ingest(conn, descriptor)
                conn.commit()
        return result
    finally:
        if descriptor is not None:
            source_artifact.cleanup_artifact(descriptor)
        if fetched_dir is not None:
            fetched_dir.cleanup()

"""Schema contract + streaming reader for the pinned bond price source artifact.

Pure and DB-free: this module never opens a database connection and never names
the data vendor. The artifact is identified ONLY by its schema contract and its
operator-pinned SHA-256; the sole reference that may travel downstream is the
opaque ``source_contract_ref`` token (``bond_price_source_v1@<sha256-prefix>``).

Attribution: the required/optional column vocabulary is ported verbatim from the
internal bond pilot's source-artifact contract (unmerged pilot branch,
``src/bond_pilot/source_artifact.py:26-40``). Only the column set is ported —
the pilot itself is NOT merged; everything else here is a minimal reimplementation
for the ingest lane.

Fail-closed order of gates (each a typed :class:`BondError` refusal):

1. ``artifact_unavailable`` — the artifact bytes cannot be opened at all.
2. ``sha256_mismatch``      — pin absent/malformed, or the artifact bytes hash
   differently from the pin. Hashing happens BEFORE any parsing.
3. ``schema_contract_violation`` — unrecognized container, invalid zip layout,
   unreadable parquet, missing required columns, or zero parseable observation
   dates (an all-garbage date column violates the contract at data level).

The reader streams parquet record batches (``iter_batches``) — an artifact of
hundreds of MB is never materialized whole. Rows with unparseable identifiers or
dates are YIELDED with their raw values (``observation_date is None`` marks the
quarantine path); they are never dropped silently.
"""
from __future__ import annotations

import hashlib
import re
import shutil
import struct
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

import pyarrow.parquet as pq

from src.bonds.errors import BondError

# Ported column-set contract (see module docstring for attribution).
REQUIRED_COLUMNS = ("cusip_id", "trd_exctn_dt", "pr")
OPTIONAL_COLUMNS = (
    "prfull",
    "acclast",
    "ytm",
    "mod_dur",
    "mac_dur",
    "convexity",
    "bond_maturity",
    "credit_spread",
    "qvolume",
    "dvolume",
    "db_type",
)

# Declared by the pinned source contract: ``pr`` is a clean transaction price in
# percent of par. Stamping these typed vocabulary values is a property of the
# CONTRACT (not per-row fabrication) and is exactly what the eligibility
# predicate requires (price_type IN ('trade','evaluated'), accrued IN
# ('clean','dirty') — schemas/bond_price_eligibility_v1.sql).
CONTRACT_PRICE_TYPE = "trade"
CONTRACT_ACCRUED_TREATMENT = "clean"

# Opaque internal source token. NEVER a vendor name (Global Constraint).
SOURCE_CONTRACT_PREFIX = "bond_price_source_v1"

# Typed refusal reasons (closed vocabulary; mirrored by the ingest worker).
REFUSAL_SHA256_MISMATCH = "sha256_mismatch"
REFUSAL_SCHEMA_CONTRACT_VIOLATION = "schema_contract_violation"
REFUSAL_ARTIFACT_UNAVAILABLE = "artifact_unavailable"

DEFAULT_BATCH_SIZE = 10_000

# Only these columns feed the price-observation mapping; the reader projects
# them so streaming never deserializes unrelated columns.
_MAPPED_COLUMNS = ("cusip_id", "trd_exctn_dt", "pr", "ytm", "db_type")

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_HASH_CHUNK_BYTES = 1024 * 1024
_ZIP_MAGIC = b"PK\x03\x04"
_PARQUET_MAGIC = b"PAR1"


def source_contract_ref(sha256: str) -> str:
    """Build the opaque internal token for one pinned artifact."""
    return f"{SOURCE_CONTRACT_PREFIX}@{sha256[:12]}"


@dataclass(frozen=True)
class ArtifactDescriptor:
    """One validated, pinned artifact ready for streaming ingestion."""

    artifact_path: str
    parquet_path: str
    sha256: str
    parquet_sha256: str
    parquet_byte_size: int
    source_contract_ref: str
    columns: tuple[str, ...]
    optional_columns_present: tuple[str, ...]
    row_count: int
    row_group_count: int
    first_observation_date: date
    last_observation_date: date
    extracted_dir: str | None


@dataclass(frozen=True)
class PriceRow:
    """One artifact row projected onto the price-observation mapping.

    ``observation_date is None`` marks an unparseable trade date (the caller's
    quarantine path); ``observation_date_raw`` keeps the lexical evidence.
    Identifiers are passed through RAW — resolution/normalization belongs to the
    existing ``price_observations`` path, never re-implemented here.
    """

    row_number: int
    cusip_id: object
    observation_date: date | None
    observation_date_raw: object
    price: object
    ytm: object
    db_type: object


def parse_observation_date(value: object) -> date | None:
    """Parse one trade-date cell; ``None`` when unparseable (never raises)."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            if len(text) == 10:
                return date.fromisoformat(text)
            if "T" in text:
                return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


def require_pin(expected_sha256: object) -> str:
    """Fail-closed pin gate: absent or malformed pins refuse before any I/O."""
    if not expected_sha256 or not isinstance(expected_sha256, str):
        raise BondError(REFUSAL_SHA256_MISMATCH, {"detail": "pin_missing"})
    pin = expected_sha256.strip().lower()
    if not _SHA256_HEX.match(pin):
        raise BondError(REFUSAL_SHA256_MISMATCH, {"detail": "pin_malformed"})
    return pin


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def _sniff_magic(path: Path) -> bytes:
    with path.open("rb") as handle:
        return handle.read(4)


def _extract_single_parquet(artifact: Path) -> tuple[Path, str]:
    """Extract the single ``.parquet`` member of a zip artifact to a tempdir."""
    workdir = Path(tempfile.mkdtemp(prefix="bond-price-artifact-"))
    try:
        with zipfile.ZipFile(artifact) as archive:
            members = archive.infolist()
            if len(members) != 1:
                raise BondError(
                    REFUSAL_SCHEMA_CONTRACT_VIOLATION,
                    {"detail": "archive_member_count", "count": len(members)},
                )
            member = members[0]
            name = member.filename
            if (
                member.is_dir()
                or not name.endswith(".parquet")
                or "\\" in name
                or name.startswith("/")
                or ".." in Path(name).parts
                or len(Path(name).parts) != 1
            ):
                raise BondError(REFUSAL_SCHEMA_CONTRACT_VIOLATION, {"detail": "invalid_archive_member"})
            target = workdir / "source.parquet"
            with archive.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output, _HASH_CHUNK_BYTES)
        return target, str(workdir)
    except BondError:
        shutil.rmtree(workdir, ignore_errors=True)
        raise
    except (zipfile.BadZipFile, OSError, struct.error) as exc:
        shutil.rmtree(workdir, ignore_errors=True)
        raise BondError(REFUSAL_SCHEMA_CONTRACT_VIOLATION, {"detail": "invalid_archive"}) from exc


def _date_bounds(parquet_path: Path, batch_size: int) -> tuple[date, date]:
    """Stream min/max over the parseable trade dates (unparseable cells skipped)."""
    minimum: date | None = None
    maximum: date | None = None
    with pq.ParquetFile(parquet_path) as parquet:
        for batch in parquet.iter_batches(columns=["trd_exctn_dt"], batch_size=batch_size):
            for value in batch.column(0).to_pylist():
                candidate = parse_observation_date(value)
                if candidate is None:
                    continue
                minimum = candidate if minimum is None or candidate < minimum else minimum
                maximum = candidate if maximum is None or candidate > maximum else maximum
    if minimum is None or maximum is None:
        raise BondError(
            REFUSAL_SCHEMA_CONTRACT_VIOLATION, {"detail": "no_parseable_observation_dates"}
        )
    return minimum, maximum


def validate_artifact(
    path: str | Path,
    *,
    expected_sha256: str | None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> ArtifactDescriptor:
    """Verify the pin, unwrap the container, and validate the schema contract.

    Returns a descriptor ready for :func:`iter_price_rows`. The caller owns the
    descriptor's extraction workspace — release it with :func:`cleanup_artifact`.
    """
    pin = require_pin(expected_sha256)
    artifact = Path(path)
    if not artifact.is_file():
        raise BondError(REFUSAL_ARTIFACT_UNAVAILABLE, {"detail": "artifact_not_found"})
    try:
        artifact_sha, artifact_size = _hash_file(artifact)
    except OSError as exc:
        raise BondError(REFUSAL_ARTIFACT_UNAVAILABLE, {"detail": "artifact_unreadable"}) from exc
    # Pin gate BEFORE any parsing: unpinned bytes are never interpreted.
    if artifact_sha != pin:
        raise BondError(
            REFUSAL_SHA256_MISMATCH, {"expected": pin, "actual": artifact_sha}
        )

    magic = _sniff_magic(artifact)
    extracted_dir: str | None = None
    if magic == _ZIP_MAGIC:
        parquet_path, extracted_dir = _extract_single_parquet(artifact)
        parquet_sha, parquet_size = _hash_file(parquet_path)
    elif magic == _PARQUET_MAGIC:
        parquet_path = artifact
        parquet_sha, parquet_size = artifact_sha, artifact_size
    else:
        raise BondError(REFUSAL_SCHEMA_CONTRACT_VIOLATION, {"detail": "unrecognized_container"})

    try:
        try:
            with pq.ParquetFile(parquet_path) as parquet:
                columns = tuple(parquet.schema_arrow.names)
                row_count = parquet.metadata.num_rows
                row_group_count = parquet.metadata.num_row_groups
        except BondError:
            raise
        except Exception as exc:
            raise BondError(
                REFUSAL_SCHEMA_CONTRACT_VIOLATION, {"detail": "unreadable_parquet"}
            ) from exc
        missing = [column for column in REQUIRED_COLUMNS if column not in columns]
        if missing:
            raise BondError(
                REFUSAL_SCHEMA_CONTRACT_VIOLATION, {"missing_columns": missing}
            )
        first_date, last_date = _date_bounds(parquet_path, batch_size)
    except BondError:
        if extracted_dir is not None:
            shutil.rmtree(extracted_dir, ignore_errors=True)
        raise

    return ArtifactDescriptor(
        artifact_path=str(artifact),
        parquet_path=str(parquet_path),
        sha256=artifact_sha,
        parquet_sha256=parquet_sha,
        parquet_byte_size=parquet_size,
        source_contract_ref=source_contract_ref(artifact_sha),
        columns=columns,
        optional_columns_present=tuple(c for c in OPTIONAL_COLUMNS if c in columns),
        row_count=row_count,
        row_group_count=row_group_count,
        first_observation_date=first_date,
        last_observation_date=last_date,
        extracted_dir=extracted_dir,
    )


def iter_price_rows(
    descriptor: ArtifactDescriptor, *, batch_size: int = DEFAULT_BATCH_SIZE
) -> Iterator[PriceRow]:
    """Stream every artifact row as a :class:`PriceRow`, in artifact order.

    Record batches are read incrementally (``iter_batches`` over the mapped
    columns only); the artifact is never materialized whole. Row ordinals are
    GLOBAL across batches so a row's identity is stable for idempotent reruns.
    """
    wanted = [c for c in _MAPPED_COLUMNS if c in descriptor.columns]
    ordinal = 0
    with pq.ParquetFile(descriptor.parquet_path) as parquet:
        for batch in parquet.iter_batches(columns=wanted, batch_size=batch_size):
            data = batch.to_pydict()
            length = batch.num_rows
            cusips = data.get("cusip_id", [None] * length)
            dates = data.get("trd_exctn_dt", [None] * length)
            prices = data.get("pr", [None] * length)
            ytms = data.get("ytm", [None] * length)
            db_types = data.get("db_type", [None] * length)
            for i in range(length):
                raw_date = dates[i]
                yield PriceRow(
                    row_number=ordinal,
                    cusip_id=cusips[i],
                    observation_date=parse_observation_date(raw_date),
                    observation_date_raw=raw_date,
                    price=prices[i],
                    ytm=ytms[i],
                    db_type=db_types[i],
                )
                ordinal += 1


def cleanup_artifact(descriptor: ArtifactDescriptor) -> None:
    """Release the extraction workspace owned by a zip-wrapped descriptor."""
    if descriptor.extracted_dir is not None:
        shutil.rmtree(descriptor.extracted_dir, ignore_errors=True)

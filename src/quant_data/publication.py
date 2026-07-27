"""Database lifecycle for the mixed_quant_v1 point-in-time publication.

A publication is opened inactive ('building'), materialized idempotently from
immutable observations, marked 'ready', then promoted atomically through
``active_quant_publication_v1``. Every writer refuses to touch a frozen
(active/superseded) publication because the schema's guard triggers reject it.

All writers are ``INSERT ... ON CONFLICT`` upserts so reruns and post-checkpoint
restarts are idempotent. Every published value carries source_lineage.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping
import uuid

import psycopg
from psycopg.types.json import Jsonb

from src.quant_data.contracts import (
    ContractError,
    ResolvedInstrument,
    require_lineage,
    validate_income_event,
)

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "mixed_quant_v1.sql"
PRODUCT = "mixed_quant_v1"

# Namespace for reproducible publication ids: same build identity -> same id.
_NAMESPACE_PUBLICATION = uuid.UUID("00000000-0000-5000-a000-6d69786564d1")


def install_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def publication_id_for(product: str, as_of: date, code_revision: str, config_version: str) -> uuid.UUID:
    return uuid.uuid5(_NAMESPACE_PUBLICATION, f"{product}|{as_of.isoformat()}|{code_revision}|{config_version}")


def open_publication(
    conn: psycopg.Connection,
    *,
    product: str,
    as_of: date,
    code_revision: str,
    config_version: str,
    input_watermarks: Mapping[str, Any] | None = None,
) -> uuid.UUID:
    """Create or resume the single inactive publication for this build identity.

    Reruns resolve to the same publication_id. A frozen (active/superseded)
    publication for the identity is never reopened.
    """
    publication_id = publication_id_for(product, as_of, code_revision, config_version)
    row = conn.execute(
        "SELECT publication_id, status FROM quant_publication_v1 "
        "WHERE product=%s AND as_of=%s AND code_revision=%s AND config_version=%s",
        (product, as_of, code_revision, config_version),
    ).fetchone()
    if row is not None:
        existing_id, status = row
        if status not in ("building", "ready"):
            raise ContractError(f"publication {existing_id} is {status}; cannot reopen a frozen build")
        conn.execute(
            "UPDATE quant_publication_v1 SET input_watermarks=%s, status='building' "
            "WHERE publication_id=%s",
            (Jsonb(dict(input_watermarks or {})), existing_id),
        )
        return existing_id
    conn.execute(
        "INSERT INTO quant_publication_v1 "
        "(publication_id, product, as_of, code_revision, config_version, input_watermarks, status) "
        "VALUES (%s,%s,%s,%s,%s,%s,'building')",
        (publication_id, product, as_of, code_revision, config_version, Jsonb(dict(input_watermarks or {}))),
    )
    return publication_id


def write_instrument(conn: psycopg.Connection, publication_id: uuid.UUID, inst: ResolvedInstrument) -> None:
    lower, upper = inst.validity
    conn.execute(
        "INSERT INTO quant_instrument_v1 "
        "(publication_id, instrument_id, instrument_type, currency, issuer_id, security_id, validity, coverage) "
        "VALUES (%s,%s,%s,%s,%s,%s,daterange(%s,%s,'[)'),%s) "
        "ON CONFLICT (publication_id, instrument_id) DO UPDATE SET "
        "instrument_type=EXCLUDED.instrument_type, currency=EXCLUDED.currency, "
        "issuer_id=EXCLUDED.issuer_id, security_id=EXCLUDED.security_id, "
        "validity=EXCLUDED.validity, coverage=EXCLUDED.coverage",
        (publication_id, inst.instrument_id, inst.instrument_type, inst.currency,
         inst.issuer_id, inst.security_id, lower, upper, Jsonb(inst.coverage)),
    )
    for alias in inst.aliases:
        conn.execute(
            "INSERT INTO quant_instrument_alias_v1 "
            "(publication_id, instrument_id, alias_type, alias_value, valid_from, valid_to, source_lineage) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (publication_id, instrument_id, alias_type, alias_value, valid_from) DO UPDATE SET "
            "valid_to=EXCLUDED.valid_to, source_lineage=EXCLUDED.source_lineage",
            (publication_id, inst.instrument_id, alias.alias_type, alias.alias_value,
             alias.valid_from, alias.valid_to, Jsonb(require_lineage(alias.source_lineage))),
        )


def write_return(
    conn: psycopg.Connection,
    publication_id: uuid.UUID,
    instrument_id: uuid.UUID,
    *,
    period_end: date,
    frequency: str,
    total_return: float,
    observed_at: Any,
    source_lineage: Mapping[str, Any],
) -> None:
    conn.execute(
        "INSERT INTO quant_return_v1 "
        "(publication_id, instrument_id, period_end, frequency, total_return, observed_at, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (publication_id, instrument_id, period_end, frequency) DO UPDATE SET "
        "total_return=EXCLUDED.total_return, observed_at=EXCLUDED.observed_at, "
        "source_lineage=EXCLUDED.source_lineage",
        (publication_id, instrument_id, period_end, frequency, float(total_return),
         observed_at, Jsonb(require_lineage(source_lineage))),
    )


def write_exposure(
    conn: psycopg.Connection,
    publication_id: uuid.UUID,
    instrument_id: uuid.UUID,
    *,
    factor: str,
    value: float,
    method: str,
    coverage: Mapping[str, Any],
    source_lineage: Mapping[str, Any],
) -> None:
    conn.execute(
        "INSERT INTO quant_exposure_v1 "
        "(publication_id, instrument_id, factor, value, method, coverage, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (publication_id, instrument_id, factor, method) DO UPDATE SET "
        "value=EXCLUDED.value, coverage=EXCLUDED.coverage, source_lineage=EXCLUDED.source_lineage",
        (publication_id, instrument_id, factor, float(value), method,
         Jsonb(dict(coverage)), Jsonb(require_lineage(source_lineage))),
    )


def write_holding_link(
    conn: psycopg.Connection,
    publication_id: uuid.UUID,
    instrument_id: uuid.UUID,
    security_instrument_id: uuid.UUID,
    *,
    alias_type: str,
    alias_value: str,
    weight_pct: float,
    coverage: Mapping[str, Any],
    source_lineage: Mapping[str, Any],
) -> None:
    conn.execute(
        "INSERT INTO quant_holding_link_v1 "
        "(publication_id, instrument_id, security_instrument_id, alias_type, alias_value, "
        " weight_pct, coverage, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (publication_id, instrument_id, security_instrument_id) DO UPDATE SET "
        "alias_type=EXCLUDED.alias_type, alias_value=EXCLUDED.alias_value, "
        "weight_pct=EXCLUDED.weight_pct, coverage=EXCLUDED.coverage, source_lineage=EXCLUDED.source_lineage",
        (publication_id, instrument_id, security_instrument_id, alias_type, alias_value,
         float(weight_pct), Jsonb(dict(coverage)), Jsonb(require_lineage(source_lineage))),
    )


def merge_instrument_coverage(
    conn: psycopg.Connection,
    publication_id: uuid.UUID,
    instrument_id: uuid.UUID,
    patch: Mapping[str, Any],
) -> None:
    """Shallow-merge a coverage patch into an instrument (non-active only)."""
    conn.execute(
        "UPDATE quant_instrument_v1 SET coverage = coverage || %s "
        "WHERE publication_id=%s AND instrument_id=%s",
        (Jsonb(dict(patch)), publication_id, instrument_id),
    )


def write_income(
    conn: psycopg.Connection,
    publication_id: uuid.UUID,
    instrument_id: uuid.UUID,
    event: Mapping[str, Any],
) -> None:
    validate_income_event(event)
    conn.execute(
        "INSERT INTO quant_income_v1 "
        "(publication_id, instrument_id, event_date, cash_amount, currency, event_type, source_lineage) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (publication_id, instrument_id, event_date, event_type) DO UPDATE SET "
        "cash_amount=EXCLUDED.cash_amount, currency=EXCLUDED.currency, "
        "source_lineage=EXCLUDED.source_lineage",
        (publication_id, instrument_id, event["event_date"], event["cash_amount"],
         event["currency"], event["event_type"], Jsonb(require_lineage(event["source_lineage"]))),
    )


def record_checkpoint(conn: psycopg.Connection, publication_id: uuid.UUID, stage: str, cursor: Mapping[str, Any]) -> None:
    conn.execute(
        "INSERT INTO quant_publication_checkpoint_v1 (publication_id, stage, cursor, updated_at) "
        "VALUES (%s,%s,%s,now()) "
        "ON CONFLICT (publication_id, stage) DO UPDATE SET cursor=EXCLUDED.cursor, updated_at=now()",
        (publication_id, stage, Jsonb(dict(cursor))),
    )


def get_checkpoint(conn: psycopg.Connection, publication_id: uuid.UUID, stage: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT cursor FROM quant_publication_checkpoint_v1 WHERE publication_id=%s AND stage=%s",
        (publication_id, stage),
    ).fetchone()
    return None if row is None else row[0]


def mark_ready(conn: psycopg.Connection, publication_id: uuid.UUID, counts: Mapping[str, Any]) -> None:
    conn.execute(
        "UPDATE quant_publication_v1 SET counts=%s, status='ready' "
        "WHERE publication_id=%s AND status IN ('building','ready')",
        (Jsonb(dict(counts)), publication_id),
    )


def promote(
    conn: psycopg.Connection, product: str, publication_id: uuid.UUID,
    *, allow_as_of_regression: bool = False,
) -> None:
    """Atomic pointer promotion; the SQL function holds the per-product lock.

    The SQL function refuses by default to move the active pointer to an OLDER
    ``as_of``; ``allow_as_of_regression`` is the explicit opt-out for a deliberate
    rollback/repoint (never for ordinary promotion).
    """
    conn.execute(
        "SELECT promote_quant_publication(%s,%s,%s)",
        (product, publication_id, allow_as_of_regression),
    )


def active_publication_id(conn: psycopg.Connection, product: str) -> uuid.UUID | None:
    row = conn.execute(
        "SELECT publication_id FROM active_quant_publication_v1 WHERE product=%s", (product,)
    ).fetchone()
    return None if row is None else row[0]


def count_rows(conn: psycopg.Connection, publication_id: uuid.UUID) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, key in (
        ("quant_instrument_v1", "instruments"),
        ("quant_instrument_alias_v1", "aliases"),
        ("quant_return_v1", "returns"),
        ("quant_exposure_v1", "exposures"),
        ("quant_income_v1", "income"),
        ("quant_holding_link_v1", "links"),
    ):
        counts[key] = conn.execute(
            f"SELECT count(*) FROM {table} WHERE publication_id=%s", (publication_id,)
        ).fetchone()[0]
    return counts

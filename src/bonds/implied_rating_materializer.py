"""Publication writer for the ``bond_market_implied_rating_v1`` product.

The product rides the shared derived-publication ledger
(``schemas/sec_derived_publications.sql``): one ``sec_derived_publications`` row
carries identity/lifecycle/immutability, ``bond_market_implied_rating_v1_builds``
pins this product's build (consumed panel publication, frozen policy digest,
input fingerprint and row digests, D counts), and the current pointer moves only
through ``sec_set_current_derived_publication``.

Two disciplines are added on top of the ledger, both fail-closed:

  * the build is idempotent by identity: ``uuid5(product | policy_digest |
    code_revision | input_fingerprint)`` mints the same publication for the same
    inputs + policy + code and a NEW one for anything else, so a replay
    re-points instead of rebuilding and a changed policy can never reuse an old
    build. The builds table's ``UNIQUE (policy_digest, input_fingerprint,
    code_revision)`` enforces the same statement in the database.
  * the pointer move is compare-and-set: the caller passes the pointer it read
    before the build, and the writer re-reads it FOR UPDATE before promoting.
    A concurrent publication that moved the pointer fails this run loudly.

``InMemoryPublicationStore`` mirrors those semantics for DB-free tests, the same
way ``panel_materializer.InMemoryPublicationStore`` does for the panel.
"""
from __future__ import annotations

import hashlib
import math
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Sequence
from uuid import UUID

import pandas as pd
import psycopg

from src.bonds.errors import BondError

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_market_implied_rating_v1.sql"
DERIVED_PROTOCOL_PATH = ROOT / "schemas" / "sec_derived_publications.sql"

PRODUCT = "bond_market_implied_rating_v1"
POLICY_VERSION = "bond_market_implied_rating_policy_v1"

#: Deterministic namespace for the product's publication identity.
_NAMESPACE_PUBLICATION = UUID("b0d5e70b-0000-5000-a000-6d6972726174")

ROW_COLUMNS: tuple[str, ...] = (
    "publication_id",
    "month",
    "cusip_id",
    "implied_bucket",
    "spread_norm_log",
    "market_level_l",
    "witnessed",
    "carry_months",
    "spell_id",
    "d_candidate",
    "d_confirmed",
    "d_event_month",
    "recovery_observed",
    "censoring",
    "policy_version",
    "policy_digest",
)
BUILD_COLUMNS: tuple[str, ...] = (
    "publication_id",
    "panel_publication_id",
    "policy_version",
    "policy_digest",
    "code_revision",
    "panel_last_closed_month",
    "as_of_date",
    "first_month",
    "last_month",
    "input_fingerprint",
    "l_anchor",
    "row_count",
    "rows_digest",
    "d_confirmed_count",
    "d_candidate_count",
)


def publication_id_for(
    policy_digest: str, code_revision: str, input_fingerprint: str
) -> str:
    """Stable exact-input identity: same policy + code + inputs, same build."""
    return str(uuid.uuid5(
        _NAMESPACE_PUBLICATION,
        f"{PRODUCT}|{POLICY_VERSION}|{policy_digest}|{code_revision}|{input_fingerprint}",
    ))


def build_fingerprint(policy_digest: str, code_revision: str, input_fingerprint: str) -> str:
    """The ledger's ``build_fingerprint``: one sha256 over the identity tuple."""
    return hashlib.sha256(
        f"{PRODUCT}|{POLICY_VERSION}|{policy_digest}|{code_revision}|{input_fingerprint}"
        .encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class ImpliedRatingPublication:
    """Everything the writer needs to pin and insert one publication."""

    publication_id: str
    panel_publication_id: str
    policy_version: str
    policy_digest: str
    code_revision: str
    panel_last_closed_month: date
    first_month: date
    last_month: date
    input_fingerprint: str
    l_anchor: float
    rows_digest: str
    d_confirmed_count: int
    d_candidate_count: int
    row_count: int


@dataclass(frozen=True)
class ImpliedRatingResult:
    publication_id: str
    product: str
    lifecycle: str
    row_count: int
    d_confirmed_count: int
    d_candidate_count: int
    reused: bool
    pointer: str


def _nullable(value: Any) -> Any:
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _as_date(value: Any) -> date | None:
    value = _nullable(value)
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


#: Columns the pure builder publishes; ``publication_id`` is stamped by the writer.
FRAME_COLUMNS: tuple[str, ...] = tuple(
    column for column in ROW_COLUMNS if column != "publication_id"
)


def publication_row_tuples(
    publication: ImpliedRatingPublication, rows: pd.DataFrame
) -> list[tuple[Any, ...]]:
    """Convert the pure module's frame into worker-safe INSERT tuples."""
    missing = [column for column in FRAME_COLUMNS if column not in rows.columns]
    if missing:
        raise BondError("publication_rows_missing_columns", {"columns": missing})
    payload: list[tuple[Any, ...]] = []
    for record in rows.to_dict(orient="records"):
        values = {
            "publication_id": publication.publication_id,
            "month": _as_date(record["month"]),
            "cusip_id": str(record["cusip_id"]),
            "implied_bucket": str(record["implied_bucket"]),
            "spread_norm_log": _nullable(record["spread_norm_log"]),
            "market_level_l": _nullable(record["market_level_l"]),
            "witnessed": bool(record["witnessed"]),
            "carry_months": int(record["carry_months"]),
            "spell_id": int(record["spell_id"]),
            "d_candidate": bool(record["d_candidate"]),
            "d_confirmed": bool(record["d_confirmed"]),
            "d_event_month": _as_date(record["d_event_month"]),
            "recovery_observed": _nullable(record["recovery_observed"]),
            "censoring": str(record["censoring"]),
            "policy_version": str(record["policy_version"]),
            "policy_digest": str(record["policy_digest"]),
        }
        payload.append(tuple(values[column] for column in ROW_COLUMNS))
    return payload


# --------------------------------------------------------------------------- #
# In-memory store (DB-free lifecycle tests, mirrors the SQL guards)
# --------------------------------------------------------------------------- #
class InMemoryPublicationStore:
    """Deterministic stand-in for the ledger + builds + rows + pointer."""

    def __init__(self) -> None:
        self.publications: dict[str, dict[str, Any]] = {}
        self.builds: dict[str, dict[str, Any]] = {}
        self.rows: dict[str, list[tuple[Any, ...]]] = {}
        self.pointer: str | None = None
        self.events: list[str] = []

    def _pin_conflict(self, publication: ImpliedRatingPublication) -> bool:
        return any(
            build["policy_digest"] == publication.policy_digest
            and build["input_fingerprint"] == publication.input_fingerprint
            and build["code_revision"] == publication.code_revision
            for build in self.builds.values()
        )

    def pinned_anchor(self) -> float | None:
        """The anchor of the CURRENT publication (the drift guard's reference)."""
        if self.pointer is None:
            return None
        return self.builds[self.pointer]["l_anchor"]

    def materialize(
        self,
        publication: ImpliedRatingPublication,
        rows: Sequence[tuple[Any, ...]],
        *,
        expected_pointer: str | None,
    ) -> ImpliedRatingResult:
        if expected_pointer is not None and self.pointer != expected_pointer:
            raise BondError("pointer_moved", {"expected": expected_pointer, "current": self.pointer})
        existing = self.publications.get(publication.publication_id)
        if existing is not None and existing["lifecycle"] == "validated":
            build = self.builds[publication.publication_id]
            if (
                build["policy_digest"] != publication.policy_digest
                or build["input_fingerprint"] != publication.input_fingerprint
                or build["code_revision"] != publication.code_revision
                or build["row_count"] != publication.row_count
                or build["rows_digest"] != publication.rows_digest
                or build["l_anchor"] != publication.l_anchor
                or len(self.rows[publication.publication_id]) != publication.row_count
            ):
                raise BondError("deterministic_rerun_mismatch", {"publication_id": publication.publication_id})
            self.pointer = publication.publication_id
            self.events.append("pointer")
            return ImpliedRatingResult(
                publication.publication_id, PRODUCT, "validated",
                publication.row_count, publication.d_confirmed_count,
                publication.d_candidate_count, True, self.pointer,
            )
        if existing is None:
            if self._pin_conflict(publication):
                raise BondError("build_pin_conflict", {"publication_id": publication.publication_id})
            self.publications[publication.publication_id] = {"lifecycle": "prepared"}
            self.events.append("prepared")
        else:
            if existing["lifecycle"] != "prepared":
                raise BondError("publication_not_writable", {"lifecycle": existing["lifecycle"]})
        build = self.builds.setdefault(publication.publication_id, {
            "policy_digest": publication.policy_digest,
            "input_fingerprint": publication.input_fingerprint,
            "code_revision": publication.code_revision,
            "row_count": publication.row_count,
            "rows_digest": publication.rows_digest,
            "l_anchor": publication.l_anchor,
        })
        if build["row_count"] != publication.row_count:
            raise BondError("row_coverage_gate_failed", {"publication_id": publication.publication_id})
        if len(rows) != publication.row_count:
            raise BondError("row_coverage_gate_failed", {"publication_id": publication.publication_id})
        self.rows[publication.publication_id] = list(rows)
        self.publications[publication.publication_id]["lifecycle"] = "validated"
        self.events.append("validated")
        self.pointer = publication.publication_id
        self.events.append("pointer")
        return ImpliedRatingResult(
            publication.publication_id, PRODUCT, "validated", publication.row_count,
            publication.d_confirmed_count, publication.d_candidate_count, False, self.pointer,
        )


# --------------------------------------------------------------------------- #
# Postgres writer
# --------------------------------------------------------------------------- #
def install_schema(conn: psycopg.Connection) -> None:
    """Apply the shared protocol + this product's DDL, idempotently."""
    with conn.cursor() as cur:
        cur.execute(DERIVED_PROTOCOL_PATH.read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def _resolve_anchor(
    conn: psycopg.Connection, run_id: Any | None = None, package_id: Any | None = None
) -> tuple[Any, Any]:
    """The validated raw run/package the shared ledger anchors this product to.

    Identical convention to ``bond_serving``'s anchor: the ledger requires a
    validated source run and package, while the PRODUCT lineage (the panel
    publication this build actually read) is pinned in the product's builds row.
    """
    if run_id is not None and package_id is not None:
        return run_id, package_id
    row = conn.execute(
        "SELECT r.run_id, p.package_id "
        "FROM sec_validated_raw_runs r "
        "JOIN sec_ingestion_runs ir ON ir.run_id = r.run_id AND ir.raw_validated_at IS NOT NULL "
        "JOIN sec_source_packages p ON p.run_id = r.run_id "
        "ORDER BY ir.raw_validated_at DESC, p.package_id LIMIT 1"
    ).fetchone()
    if row is None:
        raise BondError("validated_source_anchor_absent")
    return row[0], row[1]


_INSERT_ROWS_SQL = (
    f"INSERT INTO {PRODUCT} ({', '.join(ROW_COLUMNS)}) VALUES "
    f"({', '.join(['%s'] * len(ROW_COLUMNS))})"
)
_INSERT_BUILD_SQL = (
    f"INSERT INTO {PRODUCT}_builds ({', '.join(BUILD_COLUMNS)}) VALUES "
    f"({', '.join(['%s'] * len(BUILD_COLUMNS))}) ON CONFLICT (publication_id) DO NOTHING"
)
_ROW_CHUNK = 50_000


def current_pinned_anchor(conn: psycopg.Connection) -> float | None:
    """The ``l_anchor`` of the publication the pointer currently names."""
    row = conn.execute(
        f"SELECT b.l_anchor FROM sec_derived_current_pointers p "
        f"JOIN {PRODUCT}_builds b USING (publication_id) WHERE p.product = %s",
        (PRODUCT,),
    ).fetchone()
    return None if row is None else float(row[0])


def _materialize_postgres(
    conn: psycopg.Connection,
    publication: ImpliedRatingPublication,
    rows: Sequence[tuple[Any, ...]],
    *,
    expected_pointer: str | None,
) -> ImpliedRatingResult:
    with conn.transaction(), conn.cursor() as cur:
        existing = cur.execute(
            "SELECT lifecycle_state FROM sec_derived_publications "
            "WHERE publication_id = %s FOR UPDATE",
            (publication.publication_id,),
        ).fetchone()
        reused = existing is not None and existing[0] == "validated"
        if existing is None:
            run_id, package_id = _resolve_anchor(conn)
            version = cur.execute(
                "SELECT COALESCE(max(publication_version), 0) + 1 FROM sec_derived_publications "
                "WHERE product = %s",
                (PRODUCT,),
            ).fetchone()[0]
            cur.execute(
                "INSERT INTO sec_derived_publications "
                "(publication_id, product, publication_version, source_run_id, source_package_id, "
                "build_fingerprint) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    publication.publication_id, PRODUCT, version, run_id, package_id,
                    build_fingerprint(
                        publication.policy_digest, publication.code_revision,
                        publication.input_fingerprint,
                    ),
                ),
            )
        elif existing[0] != "validated":
            raise BondError("publication_not_writable", {"lifecycle": existing[0]})
        if not reused:
            cur.execute(_INSERT_BUILD_SQL, (
                publication.publication_id,
                publication.panel_publication_id,
                publication.policy_version,
                publication.policy_digest,
                publication.code_revision,
                publication.panel_last_closed_month,
                publication.panel_last_closed_month,
                publication.first_month,
                publication.last_month,
                publication.input_fingerprint,
                publication.l_anchor,
                publication.row_count,
                publication.rows_digest,
                publication.d_confirmed_count,
                publication.d_candidate_count,
            ))
            pinned = cur.execute(
                f"SELECT input_fingerprint, policy_digest, code_revision, row_count, "
                f"rows_digest, l_anchor FROM {PRODUCT}_builds WHERE publication_id = %s",
                (publication.publication_id,),
            ).fetchone()
            if (
                pinned is None
                or str(pinned[0]) != publication.input_fingerprint
                or str(pinned[1]) != publication.policy_digest
                or str(pinned[2]) != publication.code_revision
                or int(pinned[3]) != publication.row_count
                or str(pinned[4]) != publication.rows_digest
                or float(pinned[5]) != publication.l_anchor
            ):
                raise BondError("build_pin_mismatch", {"publication_id": publication.publication_id})
            written = cur.execute(
                f"SELECT count(*) FROM {PRODUCT} WHERE publication_id = %s",
                (publication.publication_id,),
            ).fetchone()[0]
            if written == 0:
                for start in range(0, len(rows), _ROW_CHUNK):
                    cur.executemany(_INSERT_ROWS_SQL, rows[start:start + _ROW_CHUNK])
            verified = cur.execute(
                f"SELECT count(*), count(*) FILTER (WHERE d_confirmed), "
                f"count(*) FILTER (WHERE d_candidate) FROM {PRODUCT} WHERE publication_id = %s",
                (publication.publication_id,),
            ).fetchone()
            if (
                int(verified[0]) != publication.row_count
                or int(verified[1]) != publication.d_confirmed_count
                or int(verified[2]) != publication.d_candidate_count
            ):
                raise BondError("row_coverage_gate_failed", {
                    "expected_rows": publication.row_count,
                    "actual_rows": int(verified[0]),
                })
            cur.execute("SELECT sec_validate_derived_publication(%s)", (publication.publication_id,))
        current = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers WHERE product = %s FOR UPDATE",
            (PRODUCT,),
        ).fetchone()
        current_id = None if current is None else str(current[0])
        if current_id != publication.publication_id:
            if current_id != expected_pointer:
                raise BondError("pointer_moved", {"expected": expected_pointer, "current": current_id})
            cur.execute(
                "SELECT sec_set_current_derived_publication(%s, %s)",
                (PRODUCT, publication.publication_id),
            )
        lifecycle = "validated"
    return ImpliedRatingResult(
        publication.publication_id, PRODUCT, lifecycle, publication.row_count,
        publication.d_confirmed_count, publication.d_candidate_count, reused,
        publication.publication_id,
    )


def materialize(
    store: Any,
    publication: ImpliedRatingPublication,
    rows: Sequence[tuple[Any, ...]] | pd.DataFrame,
    *,
    expected_pointer: str | None,
) -> ImpliedRatingResult:
    """Write one publication through the store that owns the target.

    A real connection goes to the shared ledger path; an
    ``InMemoryPublicationStore`` mirrors it for tests. Rows are accepted as
    already-converted tuples (the worker converts once, so the retry path does
    not rebuild them).
    """
    if isinstance(store, InMemoryPublicationStore):
        return store.materialize(publication, list(rows), expected_pointer=expected_pointer)
    payload = publication_row_tuples(publication, rows) if isinstance(rows, pd.DataFrame) else list(rows)
    return _materialize_postgres(store, publication, payload, expected_pointer=expected_pointer)

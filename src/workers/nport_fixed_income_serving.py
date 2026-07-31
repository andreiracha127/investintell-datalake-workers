"""Registered materializer for the ``nport_fixed_income_features_v1`` product.

Builds the eight N-PORT fixed-income relations the app's fixed-income dossier
reads (features, key-rate and credit-spread sensitivities, balance-sheet
primitives, debt flags, repo/lending primitives and reported flags, metric
coverage) and promotes ONE complete build through the shared
``prepared -> validated -> current`` derived-publication protocol.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).

WHY THIS EXISTS
---------------
The product had a producer but no *worker*: the builder was defined only in a
separately-attested local PostgreSQL, production shipped a stub that raised
``0A000``, and nothing in ``src/run.py`` could reach it.  The consequence was
that the eight ``_v2`` relations stayed empty in every deployed environment
while the app-side flag, contract pin and adapter were all in place.  This
worker is that missing producer: it installs the SAME sha256-pinned builder SQL
the offline route runs (``fixed_income_local_materializer.install_builder``) and
executes it against the target database.

WHAT IS *NOT* WEAKENED
----------------------
Every honesty guard survives, and each one is fail-closed here:

* the versioned producer contract must match its frozen digest
  (``_contract()`` raises otherwise) -- the same digest the app pins;
* the builder SQL must match ``APPROVED_LOCAL_ORACLE_SHA256`` before it is
  executed;
* the build only runs against a *validated, current* ``sec_nport_holdings_v2``
  publication (enforced twice: here, and again inside the builder);
* the publication is written ``prepared``, validated, and only then promoted --
  readers never see a half-built snapshot;
* the build manifest is written into the immutable manifest table before
  validation, so a promoted publication always carries its own provenance.

IDEMPOTENCE
-----------
``target_publication_id`` is a uuid5 of (product, code revision, source holdings
publication, as_of) so re-running the job with an unchanged source is a no-op
that returns ``already_published`` instead of building a second publication of
the same data.  A code change moves the id, which is exactly the republication
semantics ``bond_serving`` had to learn the hard way on 2026-07-30.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import uuid
from datetime import date
from typing import Any

from src.db import LOCK_NPORT_FIXED_INCOME_SERVING, advisory_lock, connect, resolve_dsn
from src.nport import fixed_income_local_materializer as materializer

PRODUCT = "nport_fixed_income_features_v1"
SOURCE_PRODUCT = "sec_nport_holdings_v2"
MANIFEST_FORMAT = "nport-fixed-income-in-database/v1"
# Stable uuid5 namespace for this product's publication identities. A constant
# (not uuid4) so the same inputs always collapse onto the same publication.
_PUBLICATION_NAMESPACE = uuid.UUID("6f2f1f2c-0f5a-5d3b-9c47-3f2b1c8a54d1")

_REVISION_ENV_VARS = ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA")

_SCHEMA_FILES = ("nport_fixed_income_features.sql",)


def _code_revision() -> str:
    """The revision the publication identity is derived from.

    The deployed container carries no ``.git``, so the ladder ends at the
    explicit env vars; the git fallback only helps a developer running the
    worker from a checkout.  ``unknown`` is a legitimate terminal value -- it
    just means every build of a given ``as_of`` collapses onto one publication,
    which is safe because the payload is a pure function of the pinned source.
    """
    for name in _REVISION_ENV_VARS:
        configured = os.getenv(name)
        if configured:
            return configured
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def install_schema(conn: Any) -> str:
    """Install the product DDL and the pinned builder. Idempotent.

    The DDL file deliberately no longer defines ``build_nport_fixed_income_features``
    (it used to define a raise-stub), so the builder is installed from its
    sha256-pinned resource right after -- ordering that cannot resurrect a stub.
    """
    for name in _SCHEMA_FILES:
        conn.execute((materializer.ROOT / "schemas" / name).read_text(encoding="utf-8"))
    with conn.cursor() as cur:
        return materializer.install_builder(cur)


def _current_source(conn: Any) -> tuple[str, str] | None:
    """The validated, current holdings publication this build must be pinned to."""
    row = conn.execute(
        "SELECT p.publication_id::text, p.source_run_id::text "
        "FROM sec_derived_current_pointers c "
        "JOIN sec_derived_publications p ON p.publication_id = c.publication_id "
        "WHERE c.product = %s AND p.product = %s AND p.lifecycle_state = 'validated'",
        (SOURCE_PRODUCT, SOURCE_PRODUCT),
    ).fetchone()
    return (row[0], row[1]) if row else None


def _resolve_as_of(conn: Any, calc_date: str | None, source_publication_id: str) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    # Index-backed: sec_nport_holdings_v2 (publication_id, report_date DESC, ...).
    row = conn.execute(
        "SELECT max(report_date) FROM sec_nport_holdings_v2 WHERE publication_id = %s",
        (source_publication_id,),
    ).fetchone()
    return row[0] if row and row[0] else None


def _publication_id(code_revision: str, source_publication_id: str, as_of: date) -> str:
    return str(
        uuid.uuid5(
            _PUBLICATION_NAMESPACE,
            f"{PRODUCT}|{code_revision}|{source_publication_id}|{as_of.isoformat()}",
        )
    )


def _relation_counts(conn: Any, publication_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for relation in materializer.TARGET_RELATIONS:
        counts[relation] = conn.execute(
            f"SELECT count(*) FROM {relation} WHERE publication_id = %s",  # noqa: S608 - fixed allowlist
            (publication_id,),
        ).fetchone()[0]
    return counts


def _build_fingerprint(
    *,
    publication_id: str,
    source_publication_id: str,
    as_of: date,
    code_revision: str,
    builder_sha256: str,
) -> str:
    """Fingerprint of the build INPUTS, fixed at INSERT time.

    ``sec_derived_publications`` is immutable after insert (only the
    prepared->validated transition is allowed, and it must not change
    ``build_fingerprint``), so the fingerprint cannot depend on the row counts
    the build has not produced yet.  Inputs fully determine the payload: same
    pinned source publication + same as_of + same builder sha + same contract
    digest => same rows.  The realized counts live in the immutable manifest.
    """
    payload = json.dumps(
        {
            "product": PRODUCT,
            "target_publication_id": publication_id,
            "source_publication_id": source_publication_id,
            "as_of_date": as_of.isoformat(),
            "code_revision": code_revision,
            "builder_sha256": builder_sha256,
            "contract_digest": materializer.CONTRACT_DIGEST,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _build_manifest(
    *,
    publication_id: str,
    source_publication_id: str,
    source_run_id: str,
    as_of: date,
    code_revision: str,
    builder_sha256: str,
    counts: dict[str, int],
) -> tuple[str, str]:
    """Canonical in-database build manifest + its sha256 (the build fingerprint).

    Deliberately a DIFFERENT ``format`` than the offline artifact manifest: this
    one attests a build that happened in the target database, so it cannot claim
    the local PostgreSQL image/fingerprint attestations the artifact route makes.
    Claiming them would be a forged attestation.
    """
    body: dict[str, Any] = {
        "format": MANIFEST_FORMAT,
        "product": PRODUCT,
        "identity": {
            "target_publication_id": publication_id,
            "source_publication_id": source_publication_id,
            "source_run_id": source_run_id,
            "as_of_date": as_of.isoformat(),
            "contract_digest": materializer.CONTRACT_DIGEST,
        },
        "contract_digest": materializer.CONTRACT_DIGEST,
        "engine": {
            "kind": "postgresql-target",
            "builder_sha256": builder_sha256,
            "code_revision": code_revision,
        },
        "outputs": {name: {"count": counts[name]} for name in sorted(counts)},
    }
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    body["manifest_sha256"] = digest
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str), digest


def _state_of(conn: Any, publication_id: str) -> tuple[str, bool] | None:
    row = conn.execute(
        "SELECT p.lifecycle_state, c.publication_id IS NOT NULL "
        "FROM sec_derived_publications p "
        "LEFT JOIN sec_derived_current_pointers c "
        "  ON c.product = p.product AND c.publication_id = p.publication_id "
        "WHERE p.publication_id = %s",
        (publication_id,),
    ).fetchone()
    return (row[0], bool(row[1])) if row else None


def run(
    dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None
) -> dict[str, Any]:
    """Build and promote one fixed-income features publication.

    ``limit`` is accepted for dispatcher-contract parity and ignored: the build
    is one set-based SQL statement over a pinned publication, so a partial build
    would produce an internally inconsistent publication -- exactly what the
    lifecycle protocol exists to prevent.
    """
    del limit
    # Fail closed on the cross-repo contract BEFORE touching the database: a
    # drifted contract file means the app's pinned digest no longer describes
    # what this builder would write.
    materializer._contract()
    code_revision = _code_revision()

    with connect(resolve_dsn(dsn), autocommit=True) as conn:
        with advisory_lock(conn, LOCK_NPORT_FIXED_INCOME_SERVING) as acquired:
            if not acquired:
                return {"state": "locked", "rows": 0}
            builder_sha256 = install_schema(conn)

            source = _current_source(conn)
            if source is None:
                return {"state": "no_source", "reason": "no_current_holdings_publication", "rows": 0}
            source_publication_id, source_run_id = source

            as_of = _resolve_as_of(conn, calc_date, source_publication_id)
            if as_of is None:
                return {"state": "no_source", "reason": "source_publication_has_no_report_date", "rows": 0}

            publication_id = _publication_id(code_revision, source_publication_id, as_of)
            existing = _state_of(conn, publication_id)
            if existing is not None and existing[0] == "validated":
                counts = _relation_counts(conn, publication_id)
                return {
                    "state": "already_published" if existing[1] else "already_validated",
                    "publication_id": publication_id,
                    "as_of": as_of.isoformat(),
                    "rows": sum(counts.values()),
                    "counts": counts,
                }

            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (PRODUCT,)
                    )
                    if existing is None:
                        cur.execute(
                            "INSERT INTO sec_derived_publications "
                            "(publication_id, product, publication_version, source_run_id, "
                            " source_package_id, build_fingerprint) "
                            "SELECT %s, %s, COALESCE(max(publication_version),0)+1, %s, "
                            "       (SELECT source_package_id FROM sec_derived_publications "
                            "        WHERE publication_id=%s), %s "
                            "FROM sec_derived_publications WHERE product=%s",
                            (
                                publication_id,
                                PRODUCT,
                                source_run_id,
                                source_publication_id,
                                _build_fingerprint(
                                    publication_id=publication_id,
                                    source_publication_id=source_publication_id,
                                    as_of=as_of,
                                    code_revision=code_revision,
                                    builder_sha256=builder_sha256,
                                ),
                                PRODUCT,
                            ),
                        )
                    cur.execute(
                        "SELECT build_nport_fixed_income_features(%s,%s)",
                        (publication_id, as_of),
                    )
                    counts = _relation_counts(conn, publication_id)
                    manifest, manifest_sha256 = _build_manifest(
                        publication_id=publication_id,
                        source_publication_id=source_publication_id,
                        source_run_id=source_run_id,
                        as_of=as_of,
                        code_revision=code_revision,
                        builder_sha256=builder_sha256,
                        counts=counts,
                    )
                    # A resumed run must not silently inherit a manifest that
                    # describes a DIFFERENT payload: the manifest table is
                    # immutable, so a divergence is an operator-visible failure.
                    recorded = cur.execute(
                        "SELECT manifest_sha256 FROM nport_fixed_income_publication_manifests "
                        "WHERE publication_id=%s",
                        (publication_id,),
                    ).fetchone()
                    if recorded is None:
                        cur.execute(
                            "INSERT INTO nport_fixed_income_publication_manifests"
                            "(publication_id, manifest_sha256, manifest) VALUES (%s,%s,%s::jsonb)",
                            (publication_id, manifest_sha256, manifest),
                        )
                    elif recorded[0] != manifest_sha256:
                        raise RuntimeError(
                            "fixed-income publication already carries a divergent manifest: "
                            f"{publication_id}"
                        )
                    cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
                    cur.execute(
                        "SELECT sec_set_current_derived_publication(%s,%s)",
                        (PRODUCT, publication_id),
                    )

    return {
        "state": "published",
        "publication_id": publication_id,
        "source_publication_id": source_publication_id,
        "as_of": as_of.isoformat(),
        "code_revision": code_revision,
        "rows": sum(counts.values()),
        "counts": counts,
    }

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
import logging
import os
import subprocess
import uuid
from datetime import date
from pathlib import Path
from typing import Any

from src.db import LOCK_NPORT_FIXED_INCOME_SERVING, advisory_lock, connect, resolve_dsn
from src.nport import fixed_income_local_materializer as materializer
from src.nport import secapi_fixed_income as secapi_parser

LOGGER = logging.getLogger(__name__)

PRODUCT = "nport_fixed_income_features_v1"
SOURCE_PRODUCT = "sec_nport_holdings_v2"
MANIFEST_FORMAT = "nport-fixed-income-in-database/v1"
# Stable uuid5 namespace for this product's publication identities. A constant
# (not uuid4) so the same inputs always collapse onto the same publication.
_PUBLICATION_NAMESPACE = uuid.UUID("6f2f1f2c-0f5a-5d3b-9c47-3f2b1c8a54d1")

_REVISION_ENV_VARS = ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA")
_SECAPI_SIDECAR_SCHEMA = Path(__file__).resolve().parents[2] / "schemas" / "nport_fixed_income_secapi_sidecars_v1.sql"
_SECAPI_FALLBACK_SCHEMA = Path(__file__).resolve().parents[2] / "schemas" / "nport_fixed_income_secapi_fallback_v2.sql"
_SECAPI_FALLBACK_PARSER_VERSION = "nport-secapi-fixed-income/v2"
_SECAPI_FALLBACK_RESOLVER_VERSION = "secapi-query-render/v1"


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
    with conn.cursor() as cur:
        materializer.install_product_schema(cur)
        cur.execute(_SECAPI_SIDECAR_SCHEMA.read_text(encoding="utf-8"))
        cur.execute(_SECAPI_FALLBACK_SCHEMA.read_text(encoding="utf-8"))
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


def _current_product_pointer(conn: Any) -> str | None:
    row = conn.execute(
        "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product=%s",
        (PRODUCT,),
    ).fetchone()
    return row[0] if row else None


def _already_validated_result(
    conn: Any, publication_id: str, as_of: date, source: dict[str, str]
) -> dict[str, Any] | None:
    existing = _state_of(conn, publication_id)
    if existing is None or existing[0] != "validated":
        return None
    with conn.cursor() as cur:
        rollup_state = materializer.ensure_coverage_rollup(cur, publication_id)
    counts = _relation_counts(conn, publication_id)
    return {
        "state": "already_published" if existing[1] else "already_validated",
        "publication_id": publication_id,
        "as_of": as_of.isoformat(),
        "supplemental_source_kind": source["kind"],
        "supplemental_source_hash": source["source_hash"],
        "coverage_rollup": rollup_state,
        "rows": sum(counts.values()),
        "counts": counts,
    }


# The raw evidence four of the six contract relations exist ONLY through.
# ``nport_interest_rate_risk_raw`` feeds the key-rate sensitivities;
# ``nport_fund_reported_info_raw`` feeds the credit-spread sensitivities, the
# balance-sheet primitives and the reported half of the metric coverage. Both are
# views over ``nport_raw_rows``, which production PRUNES once a run is attested
# (schemas/nport_raw.sql). A build over a pruned run is not a smaller build: it
# writes those four relations with ZERO rows and a coverage rollup that is pure
# absence -- and, before this guard, promoted it.
_REQUIRED_RAW_EVIDENCE = (
    "nport_interest_rate_risk_raw",
    "nport_fund_reported_info_raw",
)


def _pinned_raw_evidence(conn: Any, source_run_id: str) -> dict[str, bool]:
    """Whether each required raw view still holds rows for the pinned run.

    O(1) per probe: ``EXISTS`` stops at the first matching row, and the views are
    filtered by ``ingestion_run_id`` -- never a count over a landing table.
    """
    probes = ", ".join(
        f"EXISTS(SELECT 1 FROM {relation} WHERE ingestion_run_id = %s)"
        for relation in _REQUIRED_RAW_EVIDENCE
    )
    row = conn.execute(
        f"SELECT {probes}",  # noqa: S608 - fixed allowlist
        tuple(source_run_id for _ in _REQUIRED_RAW_EVIDENCE),
    ).fetchone()
    return {
        relation: bool(present)
        for relation, present in zip(_REQUIRED_RAW_EVIDENCE, row, strict=True)
    }


def _choose_supplemental_source(
    raw_evidence: dict[str, bool], secapi_state: dict[str, Any]
) -> dict[str, str]:
    """Choose one complete supplemental source without mixing provenance."""
    raw_states = tuple(bool(raw_evidence[name]) for name in _REQUIRED_RAW_EVIDENCE)
    if all(raw_states):
        return {"kind": "dera_raw", "source_hash": "sha256:legacy-raw"}
    if any(raw_states):
        raise RuntimeError("partial legacy raw evidence cannot be mixed with sec-api")
    if bool(secapi_state.get("ready")) and secapi_state.get("source_hash"):
        return {"kind": "sec_api", "source_hash": str(secapi_state["source_hash"])}
    raise RuntimeError("no complete supplemental source exists for the pinned publication")


def _secapi_scope_state(
    conn: Any, source_publication_id: str, source_run_id: str
) -> dict[str, Any]:
    """Return bounded readiness plus a deterministic hash of compact evidence."""
    row = conn.execute(
        "WITH v1_rate_hashes AS ("
        " SELECT accession_number, string_agg(concat_ws(':', provider_ordinal::text, payload_sha256, projection_sha256), ',' "
        " ORDER BY provider_ordinal) AS rate_hashes "
        " FROM nport_fixed_income_secapi_rate_risk_v1 "
        " WHERE source_holdings_publication_id=%s AND source_run_id=%s GROUP BY accession_number), "
        "v2_rate_hashes AS ("
        " SELECT accession_number, string_agg(concat_ws(':', provider_ordinal::text, compact_payload_sha256, projection_sha256), ',' "
        " ORDER BY provider_ordinal) AS rate_hashes "
        " FROM nport_fixed_income_secapi_fallback_rate_risk_v2 "
        " WHERE source_holdings_publication_id=%s AND source_run_id=%s GROUP BY accession_number), "
        "evidence AS ("
        " SELECT m.accession_number, 'v1'::text AS source_version, concat_ws(':', 'v1', m.accession_number, m.extractor_version, "
        " m.provider_response_sha256, m.payload_sha256, f.payload_sha256, f.projection_sha256, COALESCE(q.rate_hashes,'')) AS item "
        " FROM nport_fixed_income_secapi_recovery_v1 m "
        " LEFT JOIN nport_fixed_income_secapi_fund_info_v1 f USING (source_holdings_publication_id,source_run_id,accession_number) "
        " LEFT JOIN v1_rate_hashes q USING (accession_number) "
        " WHERE m.source_holdings_publication_id=%s AND m.source_run_id=%s AND m.status='success' "
        " UNION ALL "
        " SELECT m.accession_number, 'v2'::text AS source_version, concat_ws(':', 'v2', m.accession_number, m.parser_version, m.resolver_version, "
        " m.form_nport_response_sha256, m.query_response_sha256, m.render_raw_sha256, m.compact_payload_sha256, "
        " f.compact_payload_sha256, f.projection_sha256, COALESCE(q.rate_hashes,'')) AS item "
        " FROM nport_fixed_income_secapi_fallback_manifest_v2 m "
        " LEFT JOIN nport_fixed_income_secapi_fallback_fund_info_v2 f USING (source_holdings_publication_id,source_run_id,accession_number) "
        " LEFT JOIN v2_rate_hashes q USING (accession_number) "
        " WHERE m.source_holdings_publication_id=%s AND m.source_run_id=%s AND m.status='success') "
        "SELECT nport_fixed_income_secapi_fallback_scope_ready_v2(%s,%s,%s,%s,%s), "
        " COALESCE(string_agg(item, ',' ORDER BY accession_number, source_version), '') FROM evidence",
        (
            source_publication_id,
            source_run_id,
            source_publication_id,
            source_run_id,
            source_publication_id,
            source_run_id,
            source_publication_id,
            source_run_id,
            source_publication_id,
            source_run_id,
            secapi_parser.EXTRACTOR_VERSION,
            _SECAPI_FALLBACK_PARSER_VERSION,
            _SECAPI_FALLBACK_RESOLVER_VERSION,
        ),
    ).fetchone()
    readiness = dict(row[0]) if row and row[0] else {"ready": False}
    evidence = row[1] if row else ""
    readiness["source_hash"] = (
        _secapi_source_hash(source_publication_id, source_run_id, evidence)
        if readiness.get("ready")
        else None
    )
    return readiness


def _secapi_source_hash(
    source_publication_id: str, source_run_id: str, ordered_evidence: str
) -> str:
    scoped = json.dumps(
        {
            "source_publication_id": source_publication_id,
            "source_run_id": source_run_id,
            "v1_extractor_version": secapi_parser.EXTRACTOR_VERSION,
            "v2_parser_version": _SECAPI_FALLBACK_PARSER_VERSION,
            "resolver_version": _SECAPI_FALLBACK_RESOLVER_VERSION,
            "ordered_projection_evidence": ordered_evidence,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "sha256:" + hashlib.sha256(scoped.encode("utf-8")).hexdigest()


def _resolve_as_of(conn: Any, calc_date: str | None, source_publication_id: str) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    # Index-backed: sec_nport_holdings_v2 (publication_id, report_date DESC, ...).
    row = conn.execute(
        "SELECT max(report_date) FROM sec_nport_holdings_v2 WHERE publication_id = %s",
        (source_publication_id,),
    ).fetchone()
    return row[0] if row and row[0] else None


def _publication_id(
    code_revision: str,
    source_publication_id: str,
    as_of: date,
    *,
    contract_digest: str,
    builder_sha256: str,
    supplemental_source_kind: str,
    supplemental_source_hash: str,
) -> str:
    """Identity of the publication these inputs produce.

    The contract digest and the builder sha are PART of the identity, not
    metadata about it: they are what decides the payload. Deriving identity from
    the code revision alone made correctness depend on an env var -- with the
    documented ``unknown`` fallback, a contract or builder change produced the
    same id, so the run short-circuited as ``already_published`` and the new
    shape was never published. Our jobs do set CODE_REVISION; identity must not
    rely on them doing so.
    """
    return str(
        uuid.uuid5(
            _PUBLICATION_NAMESPACE,
            "|".join(
                (
                    PRODUCT,
                    code_revision,
                    contract_digest,
                    builder_sha256,
                    supplemental_source_kind,
                    supplemental_source_hash,
                    source_publication_id,
                    as_of.isoformat(),
                )
            ),
        )
    )


# The fund x metric coverage rollup the dossier reads. It is not a contract
# relation (the frozen producer contract still describes exactly eight), it is
# the serving projection the builder writes alongside them -- so it is counted
# in the run stats but never in the contract surface.
_COVERAGE_ROLLUP = "nport_fixed_income_metric_coverage_snapshot_v1"


def _relation_counts(conn: Any, publication_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for relation in (*materializer.TARGET_RELATIONS, _COVERAGE_ROLLUP):
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
    supplemental_source_kind: str,
    supplemental_source_hash: str,
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
            "supplemental_source_kind": supplemental_source_kind,
            "supplemental_source_hash": supplemental_source_hash,
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
    supplemental_source_kind: str,
    supplemental_source_hash: str,
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
            "supplemental_source_kind": supplemental_source_kind,
            "supplemental_source_hash": supplemental_source_hash,
        },
        "contract_digest": materializer.CONTRACT_DIGEST,
        "engine": {
            "kind": "postgresql-target",
            "builder_sha256": builder_sha256,
            "code_revision": code_revision,
            "supplemental_source_kind": supplemental_source_kind,
            "supplemental_source_hash": supplemental_source_hash,
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

            # Preserve the repair path for a legacy publication whose raw input
            # was pruned after it had already been validated. The raw probe gates
            # a new build, never maintenance of an immutable existing build.
            legacy_source = {"kind": "dera_raw", "source_hash": "sha256:legacy-raw"}
            legacy_publication_id = _publication_id(
                code_revision,
                source_publication_id,
                as_of,
                contract_digest=materializer.CONTRACT_DIGEST,
                builder_sha256=builder_sha256,
                supplemental_source_kind=legacy_source["kind"],
                supplemental_source_hash=legacy_source["source_hash"],
            )
            repaired = _already_validated_result(
                conn, legacy_publication_id, as_of, legacy_source
            )
            if repaired is not None:
                return repaired

            # Fail closed on pruned evidence BEFORE anything is built, inserted,
            # validated or pinned. On 2026-08-01 this job ran against a database
            # whose nport_raw_rows had been pruned: the build succeeded, four of
            # the six contract relations came out empty, the manifest recorded
            # the zeros, and the pointer moved anyway -- no guard between the
            # build and the pin ever looked at what had been produced. A build
            # that cannot see its own evidence has nothing to publish.
            #
            # Placed AFTER the idempotency short-circuit on purpose: neither the
            # publication identity nor the as_of depends on the raw rows, so a
            # re-run over an ALREADY published identity must still reach
            # ensure_coverage_rollup above. The probe gates the BUILD, not the
            # repair of something already built.
            evidence = _pinned_raw_evidence(conn, source_run_id)
            pruned = sorted(name for name, present in evidence.items() if not present)
            secapi_state = (
                _secapi_scope_state(conn, source_publication_id, source_run_id)
                if pruned
                else {"ready": False, "source_hash": None}
            )
            try:
                supplemental_source = _choose_supplemental_source(evidence, secapi_state)
            except RuntimeError as exc:
                # Production prunes raw by policy, so this state is expected to
                # PERSIST: the job stays green (exit 0 keeps the cron's retry
                # semantics intact) and would otherwise be silent forever. WARNING
                # with the identifiers is what makes the standstill legible in the
                # Cloud Run logs, and the runbook says what to do about it.
                LOGGER.warning(
                    "nport_fixed_income_serving: refusing to build over pruned raw evidence "
                    "(product=%s source_publication_id=%s source_run_id=%s as_of=%s "
                    "pruned_raw_relations=%s); the pinned N-PORT raw rows are gone, so the "
                    "raw-derived relations would publish empty and the SEC-API sidecar is not "
                    "complete (%s). Recover the bounded accession set -- see "
                    "docs/runbooks/fixed-income-publication-closure.md",
                    PRODUCT, source_publication_id, source_run_id, as_of.isoformat(),
                    ",".join(pruned), str(exc),
                )
                return {
                    "state": "no_source",
                    "reason": "pinned_raw_evidence_pruned",
                    "source_publication_id": source_publication_id,
                    "source_run_id": source_run_id,
                    "as_of": as_of.isoformat(),
                    "pruned_raw_relations": pruned,
                    "secapi_readiness": secapi_state,
                    "rows": 0,
                }

            if supplemental_source["kind"] == "sec_api":
                approved_hash = os.getenv("NPORT_FI_SECAPI_APPROVED_SOURCE_HASH", "").strip()
                if approved_hash != supplemental_source["source_hash"]:
                    LOGGER.warning(
                        "nport_fixed_income_serving: SEC-API evidence is complete but its "
                        "source hash is not explicitly approved (product=%s "
                        "source_publication_id=%s source_hash=%s)",
                        PRODUCT,
                        source_publication_id,
                        supplemental_source["source_hash"],
                    )
                    return {
                        "state": "no_source",
                        "reason": "secapi_activation_not_approved",
                        "source_publication_id": source_publication_id,
                        "source_run_id": source_run_id,
                        "supplemental_source_hash": supplemental_source["source_hash"],
                        "rows": 0,
                    }

            publication_id = _publication_id(
                code_revision,
                source_publication_id,
                as_of,
                contract_digest=materializer.CONTRACT_DIGEST,
                builder_sha256=builder_sha256,
                supplemental_source_kind=supplemental_source["kind"],
                supplemental_source_hash=supplemental_source["source_hash"],
            )
            existing = _state_of(conn, publication_id)
            repaired = _already_validated_result(
                conn, publication_id, as_of, supplemental_source
            )
            if repaired is not None:
                return repaired

            previous_product_pointer = _current_product_pointer(conn)

            with conn.transaction():
                with conn.cursor() as cur:
                    # Lock the upstream product first and hold it through our
                    # promotion. sec_set_current_derived_publication uses the
                    # same product-keyed xact lock, closing the source-pointer
                    # TOCTOU window while preserving a canonical lock order.
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        (SOURCE_PRODUCT,),
                    )
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (PRODUCT,)
                    )
                    if _current_source(conn) != (source_publication_id, source_run_id):
                        raise RuntimeError("current N-PORT holdings publication changed during build")
                    if _current_product_pointer(conn) != previous_product_pointer:
                        raise RuntimeError("current fixed-income publication changed during build")
                    if supplemental_source["kind"] == "sec_api":
                        current_secapi_state = _secapi_scope_state(
                            conn, source_publication_id, source_run_id
                        )
                        if (
                            not current_secapi_state.get("ready")
                            or current_secapi_state.get("source_hash")
                            != supplemental_source["source_hash"]
                        ):
                            raise RuntimeError("SEC-API sidecar evidence changed during build")
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
                                    supplemental_source_kind=supplemental_source["kind"],
                                    supplemental_source_hash=supplemental_source["source_hash"],
                                ),
                                PRODUCT,
                            ),
                        )
                    cur.execute(
                        "SELECT build_nport_fixed_income_features(%s,%s,%s)",
                        (publication_id, as_of, supplemental_source["kind"]),
                    )
                    counts = _relation_counts(conn, publication_id)
                    manifest, manifest_sha256 = _build_manifest(
                        publication_id=publication_id,
                        source_publication_id=source_publication_id,
                        source_run_id=source_run_id,
                        as_of=as_of,
                        code_revision=code_revision,
                        builder_sha256=builder_sha256,
                        supplemental_source_kind=supplemental_source["kind"],
                        supplemental_source_hash=supplemental_source["source_hash"],
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
                    # The acceptance criterion promotion never had. The raw
                    # precondition above catches the known cause of a degenerate
                    # build; this catches the CLASS -- any relation of the family
                    # that came out empty while the publication being served has
                    # rows in it. It runs inside this transaction, so a failure
                    # unwinds the whole build: no prepared publication survives,
                    # the pointer does not move, and the job exits non-zero.
                    materializer.assert_publication_complete(cur, publication_id)
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
        "supplemental_source_kind": supplemental_source["kind"],
        "supplemental_source_hash": supplemental_source["source_hash"],
        "rows": sum(counts.values()),
        "counts": counts,
    }

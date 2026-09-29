"""Bond Default Events producer (``bond_credit_evidence_v1``): one-shot operator worker.

Explicit, fail-loud modes over the immutable publication lifecycle of
:mod:`src.bonds.default_events.publication` (prepare -> validate -> promote). The default
mode is ``plan``: a dry run that touches neither the database nor the network.

Modes (``BOND_DEFAULT_EVENTS_MODE`` for ``WORKER=bond_default_events``, or ``--mode``
when run as ``python -m src.workers.bond_default_events``):

``plan`` (default)
    Validate a frozen *invocation manifest* and the hash-pinned local bundle it names
    completely offline: byte hash, strict schema decode, canonical round trip, derivation,
    full semantic check (``check_bundle``) and the pinned identity (publication id, target
    month T, knowledge cutoff K, quality state, build scope). No DB, no network.
``prepare``
    The same offline gate, then persist the bundle as ``prepared`` (idempotent replay).
    Needs an explicit DSN and schema. Never validates or promotes.
``verify``
    ``validate_bundle`` on a prepared publication by ID (moves ``prepared`` to ``validated``
    inside the package's own transaction). Never promotes. With an explicit ``release_id`` it also
    re-verifies that frozen diagnostic release through the diagnostic adapter (read-only).
``promote``
    Compare-and-set the qualified serving pointer. Requires ``publication_id``,
    ``expected_pointer`` (a UUID or the literal ``none``) and ``confirm_promote`` equal to the
    publication id. Refuses anything but a validated, unrevoked, complete and qualified
    build; T/K regression and CAS mismatches are refused by the package/SQL. Never automatic.
    A diagnostic release ID can never reach this mode (``release_id`` is refused here).
``diagnostic-promote``
    Compare-and-set the *diagnostic* serving pointer through the diagnostic adapter only. Requires
    an explicit ``release_id``, ``expected_diagnostic_pointer`` (a UUID or ``none``) and
    ``confirm_diagnostic_promote`` equal to the release id. Never touches the qualified pointer,
    never falls back to qualified promotion, and refuses ``publication_id``/qualified options. An
    optional invocation manifest is cross-checked (release publication id, T and K) before the CAS.
``install-schema``
    Explicit additive DDL as the non-superuser table owner (default ``worker_writer``) on an
    autocommit connection, after an administrator created the four NOLOGIN group roles: the four
    pinned SQL files, the diagnostic SQL, ``harden_installed_privileges`` and
    ``verify_installed_privileges``. Requires ``BOND_DEFAULT_EVENTS_CONFIRM_INSTALL=install-public-v1``
    and schema ``public``; prints a sanitized report (with the code, SQL and diagnostic SQL digests).
    Bound to the reviewed tree: ``BOND_DEFAULT_EVENTS_CODE_DIGEST``, when set, must equal the running
    tree's code digest (``code_digest_env_mismatch``, exit 4, before any DDL). Idempotent only while both
    pointer tables are empty (the read-back runs with ``require_empty_pointers=True``); after a diagnostic
    or qualified promotion a re-harden needs ``BOND_DEFAULT_EVENTS_ALLOW_PROMOTED_POINTERS=1``.

``sources`` inputs additionally pin the producer tree: ``plan`` prints
:func:`source_bundle.code_digest`, ``prepare`` recomputes it and refuses (``code_digest_mismatch``)
when it differs from the invocation's ``code_digest``, refuses a non-``public`` schema, and refuses a
knowledge cutoff earlier than the first day after T or more than 10 minutes in the future.

Input ``kind``:

``bundle``
    A hash-pinned frozen bundle file (the original path; ``prepare`` only persists it).
``sources``
    A sanitized SHA-pinned *source-frontier manifest* plus the pinned panel publication and code
    digest. The bundle is composed by :func:`source_bundle.build_bundle_from_sources` (coverage-only
    diagnostic, ``partial``/``limited``). ``plan`` stays offline unless ``read_panel`` is set (one
    READ ONLY REPEATABLE READ panel read, nothing persisted). ``prepare`` performs exactly one
    credit-store prepare, one structural validate and one diagnostic prepare, prints sanitized
    IDs/counts/digests and **elects no pointer**.

Invariants: the worker installs schema only in the explicit, confirmed ``install-schema`` mode,
never acquires source data (no network in any mode; the DSN is the only endpoint), never reads
``.env`` (configuration comes from the process environment through ``src.db`` only), and
never logs or echoes a DSN. Errors are typed (:class:`WorkerError` subclasses); the CLI maps
them to distinct exit codes, while under ``src.run_worker`` they propagate (non-zero exit).
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import gc
import hashlib
import importlib
import json
import logging
import os
import re
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from src.bonds.default_events import contracts as c
from src.bonds.default_events import publication as pub
from src.bonds.default_events import source_bundle as sb

log = logging.getLogger(__name__)

WORKER = "bond_default_events"
INVOCATION_VERSION = "bond_default_events_invocation_v1"
MODES = ("plan", "prepare", "verify", "promote", "diagnostic-promote", "install-schema")
DEFAULT_MODE = "plan"
PUBLIC_SCHEMA = "public"
INSTALL_CONFIRMATION = "install-public-v1"
DEFAULT_EXPECTED_OWNER = "worker_writer"
DEFAULT_RUNTIME_ROLE = "app_runtime"
DEFAULT_OTHER_ROLES = ("app_analytics_ro",)
#: ``prepare`` refuses a knowledge cutoff more than this far in the future (clock-skew allowance).
CUTOFF_FUTURE_SKEW = dt.timedelta(minutes=10)
MAX_INPUT_BYTES = 1 << 30  # 1 GiB hard ceiling on the frozen bundle file.
MAX_SOURCE_MANIFEST_BYTES = 1 << 20  # 1 MiB: the sanitized frontier manifest is ~30 KB.
NO_POINTER = "none"

ENV_MODE = "BOND_DEFAULT_EVENTS_MODE"
ENV_MANIFEST = "BOND_DEFAULT_EVENTS_MANIFEST"
ENV_SCHEMA = "BOND_DEFAULT_EVENTS_SCHEMA"
ENV_PUBLICATION_ID = "BOND_DEFAULT_EVENTS_PUBLICATION_ID"
ENV_EXPECTED_POINTER = "BOND_DEFAULT_EVENTS_EXPECTED_POINTER"
ENV_CONFIRM_PROMOTE = "BOND_DEFAULT_EVENTS_CONFIRM_PROMOTE"
ENV_RELEASE_ID = "BOND_DEFAULT_EVENTS_RELEASE_ID"
ENV_EXPECTED_DIAGNOSTIC_POINTER = "BOND_DEFAULT_EVENTS_EXPECTED_DIAGNOSTIC_POINTER"
ENV_CONFIRM_DIAGNOSTIC_PROMOTE = "BOND_DEFAULT_EVENTS_CONFIRM_DIAGNOSTIC_PROMOTE"
ENV_PLAN_READ_PANEL = "BOND_DEFAULT_EVENTS_PLAN_READ_PANEL"
ENV_CONFIRM_INSTALL = "BOND_DEFAULT_EVENTS_CONFIRM_INSTALL"
ENV_EXPECTED_OWNER = "BOND_DEFAULT_EVENTS_EXPECTED_OWNER"
ENV_RUNTIME_ROLE = "BOND_DEFAULT_EVENTS_RUNTIME_ROLE"
ENV_OTHER_ROLES = "BOND_DEFAULT_EVENTS_OTHER_ROLES"
ENV_CODE_DIGEST = "BOND_DEFAULT_EVENTS_CODE_DIGEST"
ENV_ALLOW_PROMOTED_POINTERS = "BOND_DEFAULT_EVENTS_ALLOW_PROMOTED_POINTERS"

_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_UNSET: Any = object()


# ---------------------------------------------------------------------------
# Typed errors (CLI exit codes)
# ---------------------------------------------------------------------------
class WorkerError(RuntimeError):
    """Base of every refusal; ``code`` is stable text, ``exit_code`` the CLI status."""

    exit_code = 1

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}:{detail}" if detail else code)
        self.code = code
        self.detail = detail


class ConfigError(WorkerError):
    """Unknown/missing/contradictory worker configuration."""

    exit_code = 2


class InvocationError(WorkerError):
    """The frozen invocation manifest is malformed or unreadable."""

    exit_code = 3


class IntegrityError(WorkerError):
    """Input hash or pinned identity (publication id, T, K, quality, scope) mismatch."""

    exit_code = 4


class ContractViolation(WorkerError):
    """The bundle violates the frozen contract (schema, derivation, semantics)."""

    exit_code = 5


class PublicationRefused(WorkerError):
    """The publication lifecycle refused (CAS, T/K, not validated/qualified, revoked, ...)."""

    exit_code = 6


class PromoteNotAuthorized(WorkerError):
    """Promotion was requested without every explicit safeguard."""

    exit_code = 7


class OrchestrationUnavailable(WorkerError):
    """A required package layer (e.g. the diagnostic release adapter) is not installed."""

    exit_code = 8


class InfrastructureError(WorkerError):
    """Database connection failure or an unexpected internal error (sanitized)."""

    exit_code = 9


class SchemaPrerequisiteError(WorkerError):
    """``install-schema`` prerequisite missing (roles, owner, privileges); an admin step must run first."""

    exit_code = 10


# ---------------------------------------------------------------------------
# Invocation manifest
# ---------------------------------------------------------------------------
class Invocation:
    """A validated frozen invocation manifest (``bond_default_events_invocation_v1``)."""

    def __init__(
        self,
        *,
        path: Path,
        kind: str,
        bundle_path: Path | None,
        bundle_sha256: str | None,
        publication_id: uuid.UUID | None,
        target_month: dt.date,
        knowledge_cutoff: dt.datetime,
        quality_state: str,
        build_scope: str,
        source_manifest_path: Path | None = None,
        source_manifest_sha256: str | None = None,
        panel_publication_id: uuid.UUID | None = None,
        code_digest: str | None = None,
    ) -> None:
        self.path = path
        self.kind = kind
        self.bundle_path = bundle_path
        self.bundle_sha256 = bundle_sha256
        self.source_manifest_path = source_manifest_path
        self.source_manifest_sha256 = source_manifest_sha256
        self.panel_publication_id = panel_publication_id
        self.code_digest = code_digest
        #: ``None`` only for a ``sources`` invocation that has not pinned the derived ID yet
        #: (allowed in ``plan`` so the operator can learn it before freezing).
        self.publication_id = publication_id
        self.target_month = target_month
        self.knowledge_cutoff = knowledge_cutoff
        self.quality_state = quality_state
        self.build_scope = build_scope


def _exact_keys(obj: Any, required: set[str], where: str) -> Mapping[str, Any]:
    if not isinstance(obj, Mapping):
        raise InvocationError("manifest_object_expected", where)
    missing = sorted(required - set(obj))
    extra = sorted(set(obj) - required)
    if missing or extra:
        raise InvocationError(
            "manifest_fields_mismatch", f"{where}:missing={missing}:extra={extra}"
        )
    return obj


def _parse_uuid(
    value: Any, where: str, error: type[WorkerError] = InvocationError
) -> uuid.UUID:
    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError) as exc:
        raise error("uuid_expected", where) from exc
    if not isinstance(value, str) or str(parsed) != value.lower():
        raise error("uuid_not_canonical", where)
    return parsed


def _parse_month(value: Any) -> dt.date:
    try:
        parsed = dt.date.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise InvocationError("target_month_expected", "expected.target_month") from exc
    if parsed.isoformat() != value:
        raise InvocationError("target_month_not_canonical", "expected.target_month")
    return parsed


def _parse_cutoff(value: Any) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise InvocationError(
            "knowledge_cutoff_expected", "expected.knowledge_cutoff"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvocationError(
            "knowledge_cutoff_needs_timezone", "expected.knowledge_cutoff"
        )
    return parsed.astimezone(dt.timezone.utc)


def load_invocation(path: str | os.PathLike[str]) -> Invocation:
    """Read and strictly validate the invocation manifest (no side effects)."""
    manifest_path = Path(path)
    try:
        raw = manifest_path.read_bytes()
    except OSError as exc:
        raise InvocationError("manifest_unreadable", type(exc).__name__) from exc
    try:
        doc = c.load_json_strict(raw)
    except (ValueError, c.ContractError) as exc:
        raise InvocationError("manifest_not_strict_json", str(exc)[:120]) from exc
    top = _exact_keys(
        doc,
        {"invocation_version", "product", "contract_version", "input", "expected"},
        "manifest",
    )
    if top["invocation_version"] != INVOCATION_VERSION:
        raise InvocationError(
            "unsupported_invocation_version", str(top["invocation_version"])[:60]
        )
    if top["product"] != c.PRODUCT or top["contract_version"] != c.CONTRACT_VERSION:
        raise InvocationError("product_or_contract_version_mismatch")
    expected = _exact_keys(
        top["expected"],
        {
            "publication_id",
            "target_month",
            "knowledge_cutoff",
            "quality_state",
            "build_scope",
        },
        "expected",
    )
    quality, scope = expected["quality_state"], expected["build_scope"]
    if quality not in c.ENUMS["quality_state"]:
        raise InvocationError("quality_state_unknown", "expected.quality_state")
    if scope not in c.ENUMS["build_scope"]:
        raise InvocationError("build_scope_unknown", "expected.build_scope")
    source = top["input"]
    if not isinstance(source, Mapping) or "kind" not in source:
        raise InvocationError("input_kind_required", "input")
    kind = source["kind"]
    bundle_path: Path | None = None
    bundle_sha: str | None = None
    source_manifest_path: Path | None = None
    source_manifest_sha: str | None = None
    panel_id: uuid.UUID | None = None
    code_digest: str | None = None
    if kind == "bundle":
        spec = _exact_keys(source, {"kind", "path", "sha256"}, "input")
        if not isinstance(spec["path"], str) or not spec["path"]:
            raise InvocationError("input_path_expected", "input.path")
        if not isinstance(spec["sha256"], str) or not _SHA256_RE.fullmatch(
            spec["sha256"]
        ):
            raise InvocationError("input_sha256_expected", "input.sha256")
        candidate = Path(spec["path"])
        bundle_path = (
            candidate if candidate.is_absolute() else manifest_path.parent / candidate
        )
        bundle_sha = spec["sha256"]
    elif kind == "sources":
        spec = _exact_keys(
            source,
            {"kind", "source_manifest", "expected_panel_publication_id", "code_digest"},
            "input",
        )
        frontier = _exact_keys(
            spec["source_manifest"], {"path", "sha256"}, "input.source_manifest"
        )
        if not isinstance(frontier["path"], str) or not frontier["path"]:
            raise InvocationError("input_path_expected", "input.source_manifest.path")
        if not isinstance(frontier["sha256"], str) or not _SHA256_RE.fullmatch(
            frontier["sha256"]
        ):
            raise InvocationError(
                "input_sha256_expected", "input.source_manifest.sha256"
            )
        if not isinstance(spec["code_digest"], str) or not _DIGEST_RE.fullmatch(
            spec["code_digest"]
        ):
            raise InvocationError("code_digest_expected", "input.code_digest")
        candidate = Path(frontier["path"])
        source_manifest_path = (
            candidate if candidate.is_absolute() else manifest_path.parent / candidate
        )
        source_manifest_sha = frontier["sha256"]
        panel_id = _parse_uuid(
            spec["expected_panel_publication_id"], "input.expected_panel_publication_id"
        )
        code_digest = spec["code_digest"]
    else:
        raise InvocationError("input_kind_unknown", str(kind)[:40])
    raw_publication_id = expected["publication_id"]
    if kind == "sources" and raw_publication_id is None:
        publication_id: uuid.UUID | None = None
    else:
        publication_id = _parse_uuid(raw_publication_id, "expected.publication_id")
    return Invocation(
        path=manifest_path,
        kind=kind,
        bundle_path=bundle_path,
        bundle_sha256=bundle_sha,
        publication_id=publication_id,
        target_month=_parse_month(expected["target_month"]),
        knowledge_cutoff=_parse_cutoff(expected["knowledge_cutoff"]),
        quality_state=quality,
        build_scope=scope,
        source_manifest_path=source_manifest_path,
        source_manifest_sha256=source_manifest_sha,
        panel_publication_id=panel_id,
        code_digest=code_digest,
    )


def _require_identity(
    actual: Mapping[str, Any], inv: Invocation, *, source: str
) -> None:
    """Every pinned identity field must match exactly; the first difference is fatal."""
    for name, want in (
        ("publication_id", inv.publication_id),
        ("target_month", inv.target_month),
        ("knowledge_cutoff", inv.knowledge_cutoff),
        ("quality_state", inv.quality_state),
        ("build_scope", inv.build_scope),
    ):
        if actual[name] != want:
            raise IntegrityError(
                f"{source}_{name}_mismatch", f"expected={want} actual={actual[name]}"
            )


# ---------------------------------------------------------------------------
# Offline gate (shared by plan and prepare)
# ---------------------------------------------------------------------------
def _read_frozen_bundle(inv: Invocation) -> tuple[c.CreditBundle, str]:
    if inv.kind != "bundle":
        raise InvocationError("bundle_kind_expected", inv.kind)
    assert inv.bundle_path is not None and inv.bundle_sha256 is not None
    try:
        size = inv.bundle_path.stat().st_size
        if size > MAX_INPUT_BYTES:
            raise IntegrityError("bundle_too_large", str(size))
        data = inv.bundle_path.read_bytes()
    except OSError as exc:
        raise IntegrityError("bundle_unreadable", type(exc).__name__) from exc
    digest = hashlib.sha256(data).hexdigest()
    if digest != inv.bundle_sha256:
        raise IntegrityError(
            "bundle_sha256_mismatch", f"expected={inv.bundle_sha256} actual={digest}"
        )
    try:
        bundle = c.CreditBundle.from_json_bytes(
            data
        )  # strict JSON + pinned schema + typed decode
        pub.verify_schema(bundle)
        pub.check_bundle(bundle)  # derivation + full semantic rules, same as validate
    except (c.ContractError, ValueError) as exc:
        raise ContractViolation("bundle_contract_violation", str(exc)[:300]) from exc
    _require_identity(
        {
            "publication_id": bundle.publication_id,
            "target_month": bundle.manifest["target_month"],
            "knowledge_cutoff": bundle.manifest["knowledge_cutoff"].astimezone(
                dt.timezone.utc
            ),
            "quality_state": bundle.manifest["quality_state"],
            "build_scope": bundle.manifest["build_scope"],
        },
        inv,
        source="bundle",
    )
    return bundle, digest


def _summary(bundle: c.CreditBundle) -> dict[str, Any]:
    m = bundle.manifest
    return {
        "publication_id": str(bundle.publication_id),
        "target_month": m["target_month"].isoformat(),
        "knowledge_cutoff": m["knowledge_cutoff"]
        .astimezone(dt.timezone.utc)
        .isoformat(),
        "knowledge_mode": m["knowledge_mode"],
        "quality_state": m["quality_state"],
        "build_scope": m["build_scope"],
        "frame_counts": {name: len(rows) for name, rows in bundle.frames.items()},
        "promotable_as_qualified": m["quality_state"] == "qualified"
        and m["build_scope"] == "complete",
    }


# ---------------------------------------------------------------------------
# Store access (explicit DSN + explicit schema; never DDL)
# ---------------------------------------------------------------------------
def _connect(dsn: str) -> Any:
    from src.db import connect  # local import: plan mode never needs a driver

    return connect(dsn, autocommit=False)


def _connect_autocommit(dsn: str) -> Any:
    """Autocommit connection for ``install-schema`` only (the SQL files own BEGIN/COMMIT)."""
    from src.db import connect

    return connect(dsn, autocommit=True)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _require_public_schema(schema: str | None, what: str) -> None:
    if schema != PUBLIC_SCHEMA:
        raise ConfigError("schema_must_be_public", f"{what}:{ENV_SCHEMA}={schema!r}")


@contextlib.contextmanager
def _open_store(dsn: str, schema: str) -> Any:
    """Yield a :class:`~publication.PostgresPublicationStore`; sanitized connect failures."""
    if not _SCHEMA_RE.fullmatch(schema):
        raise ConfigError("schema_identifier_invalid", ENV_SCHEMA)
    try:
        conn = _connect(dsn)
    except Exception as exc:  # noqa: BLE001 - never chain: driver text can echo parameters
        raise InfrastructureError("db_connect_failed", type(exc).__name__) from None
    try:
        yield pub.PostgresPublicationStore(conn, schema)
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def _state_dict(state: pub.PublicationState) -> dict[str, Any]:
    return {
        "publication_id": str(state.publication_id),
        "publication_version": state.publication_version,
        "lifecycle_state": state.lifecycle_state,
        "quality_state": state.quality_state,
        "build_scope": state.build_scope,
        "target_month": state.target_month.isoformat(),
        "knowledge_cutoff": state.knowledge_cutoff.astimezone(
            dt.timezone.utc
        ).isoformat(),
        "revoked": state.revoked,
    }


def _refuse(exc: pub.PublicationError) -> PublicationRefused:
    return PublicationRefused(exc.code, exc.detail[:300])


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class Collaborators:
    """Injectable seams of the ``sources`` path (tests; production uses the defaults).

    ``builder`` is :func:`source_bundle.build_bundle_from_sources`, ``projection_builder`` is
    :func:`source_bundle.build_diagnostic_projection` and ``diagnostic`` is the diagnostic release
    adapter module (:mod:`src.bonds.default_events.diagnostic_publication`), imported lazily.
    """

    builder: Callable[..., c.CreditBundle] | None = None
    projection_builder: Callable[[c.CreditBundle, Mapping[str, Any]], Any] | None = None
    frontier_digest_builder: Callable[[c.CreditBundle], str] | None = None
    code_digest: Callable[[], str] | None = None
    diagnostic: Any = None


def _diagnostic(collab: Collaborators) -> Any:
    if collab.diagnostic is not None:
        return collab.diagnostic
    try:
        diag = importlib.import_module(
            "src.bonds.default_events.diagnostic_publication"
        )
    except ImportError as exc:
        raise OrchestrationUnavailable(
            "diagnostic_layer_unavailable", type(exc).__name__
        ) from None
    return diag


def _diagnostic_errors(diag: Any) -> tuple[type[BaseException], ...]:
    extra = getattr(diag, "DiagnosticError", None)
    return (pub.PublicationError,) + ((extra,) if isinstance(extra, type) else ())


def _refuse_diagnostic(exc: BaseException) -> PublicationRefused:
    code = (
        getattr(exc, "reason", None) or getattr(exc, "code", None) or type(exc).__name__
    )
    detail = getattr(exc, "detail", None) or str(exc)
    return PublicationRefused(str(code)[:120], str(detail)[:300])


#: Sanitized identity/digest fields of a verified release (never raw rows, projections or timestamps
#: that could carry free text).
_RELEASE_REPORT_KEYS = (
    "release_id",
    "publication_id",
    "panel_publication_id",
    "product",
    "tier",
    "display_projection_version",
    "target_month",
    "knowledge_cutoff",
    "publication_fingerprint",
    "policy_digest",
    "contract_digest",
    "source_frontier_manifest_digest",
    "projection_digest",
    "diagnostic_sql_digest",
    "built_at",
    "validated_at",
)


def _release_report(
    diag: Any, conn: Any, schema: str, release_id: uuid.UUID
) -> dict[str, Any]:
    """Re-run every diagnostic guard by ID and return a whitelisted identity/digest report."""
    verify_report = getattr(diag, "verify_diagnostic_report", None)
    if verify_report is not None:
        raw = verify_report(conn, schema=schema, release_id=release_id)
        report: dict[str, Any] = {
            k: str(raw[k]) for k in _RELEASE_REPORT_KEYS if raw.get(k) is not None
        }
        if isinstance(raw.get("is_current"), bool):
            report["is_current"] = raw["is_current"]
        counts = raw.get("counts")
        if isinstance(counts, Mapping):
            report["counts"] = {
                str(k): v for k, v in counts.items() if isinstance(v, int)
            }
        return report
    projection = diag.verify_diagnostic(conn, schema=schema, release_id=release_id)
    return {"projection_digest": getattr(projection, "digest", None)}


def _require_release_publication(report: Mapping[str, Any], pid: uuid.UUID) -> None:
    """The verified release must belong to exactly the requested credit publication."""
    actual = report.get("publication_id")
    if actual is None:
        raise IntegrityError("release_publication_unverifiable", str(pid))
    if str(actual).lower() != str(pid):
        raise IntegrityError(
            "release_publication_mismatch", f"release_of={actual} requested={pid}"
        )


def _require_release_identity(report: Mapping[str, Any], inv: Invocation) -> None:
    """Cross-check publication id, T and K of a verified release against the invocation manifest."""
    if inv.publication_id is None:
        raise IntegrityError("publication_id_pin_required", "invocation lacks the pin")
    _require_release_publication(report, inv.publication_id)
    try:
        target = dt.date.fromisoformat(str(report["target_month"]))
        cutoff = dt.datetime.fromisoformat(str(report["knowledge_cutoff"]))
    except (KeyError, ValueError) as exc:
        raise IntegrityError("release_identity_unverifiable", "target_month/K") from exc
    if cutoff.tzinfo is None:
        raise IntegrityError("release_identity_unverifiable", "K without timezone")
    if target != inv.target_month:
        raise IntegrityError(
            "release_target_month_mismatch",
            f"release={target} invocation={inv.target_month}",
        )
    if cutoff.astimezone(dt.timezone.utc) != inv.knowledge_cutoff.astimezone(
        dt.timezone.utc
    ):
        raise IntegrityError(
            "release_knowledge_cutoff_mismatch",
            f"release={cutoff.astimezone(dt.timezone.utc).isoformat()} "
            f"invocation={inv.knowledge_cutoff.astimezone(dt.timezone.utc).isoformat()}",
        )


def _check_cutoff_window(inv: Invocation) -> None:
    """K must be after month T has closed and no more than 10 minutes in the future."""
    closes = sb.add_months(inv.target_month, 1)
    earliest = dt.datetime(closes.year, closes.month, 1, tzinfo=dt.timezone.utc)
    cutoff = inv.knowledge_cutoff.astimezone(dt.timezone.utc)
    if cutoff < earliest:
        raise IntegrityError(
            "cutoff_before_month_close",
            f"K={cutoff.isoformat()} earliest={earliest.isoformat()}",
        )
    latest = _utcnow() + CUTOFF_FUTURE_SKEW
    if cutoff > latest:
        raise IntegrityError(
            "cutoff_in_the_future",
            f"K={cutoff.isoformat()} latest={latest.isoformat()}",
        )


def _check_code_digest(inv: Invocation, collab: Collaborators) -> str:
    """Recompute the producer-tree digest; the pinned invocation digest must match it."""
    computed = (collab.code_digest or sb.code_digest)()
    if inv.code_digest != computed:
        raise IntegrityError(
            "code_digest_mismatch", f"pinned={inv.code_digest} computed={computed}"
        )
    return computed


def _is_driver_error(exc: BaseException) -> bool:
    try:
        import psycopg
    except ImportError:
        return False
    return isinstance(exc, psycopg.Error)


def _load_source_manifest(inv: Invocation) -> tuple[Mapping[str, Any], str]:
    """Read the sanitized frontier manifest, check its file hash and parse it strictly."""
    assert (
        inv.source_manifest_path is not None and inv.source_manifest_sha256 is not None
    )
    try:
        size = inv.source_manifest_path.stat().st_size
        if size > MAX_SOURCE_MANIFEST_BYTES:
            raise IntegrityError("source_manifest_too_large", str(size))
        data = inv.source_manifest_path.read_bytes()
    except OSError as exc:
        raise IntegrityError("source_manifest_unreadable", type(exc).__name__) from exc
    digest = hashlib.sha256(data).hexdigest()
    if digest != inv.source_manifest_sha256:
        raise IntegrityError(
            "source_manifest_sha256_mismatch",
            f"expected={inv.source_manifest_sha256} actual={digest}",
        )
    try:
        document = c.load_json_strict(data)
    except (ValueError, c.ContractError) as exc:
        raise IntegrityError("source_manifest_not_strict_json", str(exc)[:120]) from exc
    return document, digest


def _validated_source_manifest(
    inv: Invocation,
) -> tuple[Mapping[str, Any], sb.SourceManifest, str]:
    if inv.quality_state != "partial" or inv.build_scope != "limited":
        raise IntegrityError(
            "sources_quality_scope_mismatch", f"{inv.build_scope}/{inv.quality_state}"
        )
    assert inv.panel_publication_id is not None
    document, file_sha = _load_source_manifest(inv)
    try:
        manifest = sb.validate_source_manifest(
            document,
            target_month=inv.target_month,
            knowledge_cutoff=inv.knowledge_cutoff,
            expected_panel_publication_id=inv.panel_publication_id,
        )
    except sb.SourceBundleError as exc:
        raise IntegrityError(exc.code, exc.detail) from exc
    return document, manifest, file_sha


def _build_sources_bundle(
    inv: Invocation, document: Mapping[str, Any], conn: Any, collab: Collaborators
) -> c.CreditBundle:
    assert inv.panel_publication_id is not None and inv.code_digest is not None
    builder = collab.builder or sb.build_bundle_from_sources
    try:
        bundle = builder(
            document,
            conn=conn,
            target_month=inv.target_month,
            knowledge_cutoff=inv.knowledge_cutoff,
            knowledge_mode="current_run",
            expected_panel_publication_id=inv.panel_publication_id,
            code_digest=inv.code_digest,
        )
    except sb.SourceBundleError as exc:
        raise IntegrityError(exc.code, exc.detail) from exc
    except c.ContractError as exc:
        raise ContractViolation("bundle_contract_violation", str(exc)[:300]) from exc
    except Exception as exc:
        if _is_driver_error(exc):
            raise InfrastructureError("panel_read_failed", type(exc).__name__) from None
        raise
    return bundle


def _summary_sources(
    bundle: c.CreditBundle, manifest: sb.SourceManifest, file_sha: str
) -> dict[str, Any]:
    m = bundle.manifest
    return {
        **_summary(bundle),
        "input_kind": "sources",
        "panel_publication_id": str(m["panel_publication_id"]),
        "panel_grid_count": m["panel_grid_count"],
        "panel_grid_digest": m["panel_grid_digest"],
        "rating_rows": len(bundle.frames["ratings"]),
        "coverage_cells": len(bundle.frames["coverage"]),
        "fingerprint_digest": m["fingerprint_digest"],
        "source_manifest_file_sha256": file_sha,
        "source_manifest_digest": manifest.digest,
        "max_grid_rows": manifest.max_grid_rows,
        "max_bundle_bytes": manifest.max_bundle_bytes,
    }


def _plan_sources(
    inv: Invocation,
    dsn: str | None,
    schema: str | None,
    store_factory: Callable[[str, str], Any],
    read_panel: bool,
    collab: Collaborators,
) -> dict[str, Any]:
    document, manifest, file_sha = _validated_source_manifest(inv)
    computed_digest = (collab.code_digest or sb.code_digest)()
    stats: dict[str, Any] = {
        "state": "planned",
        "mode": "plan",
        "input_kind": "sources",
        "code_digest": computed_digest,
        "code_digest_pinned": inv.code_digest,
        "code_digest_matches": inv.code_digest == computed_digest,
        "source_manifest_file_sha256": file_sha,
        "source_manifest_digest": manifest.digest,
        "frontier_records": len(manifest.frontiers),
        "max_grid_rows": manifest.max_grid_rows,
        "max_bundle_bytes": manifest.max_bundle_bytes,
        "panel_publication_id": str(manifest.panel_publication_id),
        "pinned_publication_id": None
        if inv.publication_id is None
        else str(inv.publication_id),
        "database": "not_contacted",
        "network": "not_used",
    }
    if not read_panel:
        return stats
    assert dsn and schema
    if inv.code_digest != computed_digest:  # the pin check must use the pinned tree
        raise IntegrityError(
            "code_digest_mismatch",
            f"pinned={inv.code_digest} computed={computed_digest}",
        )
    with store_factory(dsn, schema) as store:
        bundle = _build_sources_bundle(inv, document, store.conn, collab)
    if inv.publication_id is not None:
        _require_identity(
            {
                "publication_id": bundle.publication_id,
                "target_month": bundle.manifest["target_month"],
                "knowledge_cutoff": bundle.manifest["knowledge_cutoff"].astimezone(
                    dt.timezone.utc
                ),
                "quality_state": bundle.manifest["quality_state"],
                "build_scope": bundle.manifest["build_scope"],
            },
            inv,
            source="bundle",
        )
    stats.update(_summary_sources(bundle, manifest, file_sha))
    stats["database"] = "read_only"
    return stats


def _prepare_sources(
    inv: Invocation,
    dsn: str,
    schema: str,
    store_factory: Callable[[str, str], Any],
    collab: Collaborators,
) -> dict[str, Any]:
    """Exactly one credit-store prepare + structural validate + diagnostic prepare; no election."""
    if inv.publication_id is None:
        raise ConfigError(
            "publication_id_pin_required",
            "prepare needs the frozen expected.publication_id",
        )
    _require_public_schema(schema, "prepare")
    _check_cutoff_window(inv)
    document, manifest, file_sha = _validated_source_manifest(inv)
    _check_code_digest(inv, collab)
    diag = _diagnostic(collab)  # fail before any read/write when the adapter is missing
    projection_builder = collab.projection_builder or sb.build_diagnostic_projection
    frontier_digest_builder = (
        collab.frontier_digest_builder or sb.source_frontier_manifest_digest
    )
    errors = _diagnostic_errors(diag)
    try:
        with store_factory(dsn, schema) as store:
            conn = store.conn
            bundle = _build_sources_bundle(inv, document, conn, collab)
            _require_identity(
                {
                    "publication_id": bundle.publication_id,
                    "target_month": bundle.manifest["target_month"],
                    "knowledge_cutoff": bundle.manifest["knowledge_cutoff"].astimezone(
                        dt.timezone.utc
                    ),
                    "quality_state": bundle.manifest["quality_state"],
                    "build_scope": bundle.manifest["build_scope"],
                },
                inv,
                source="bundle",
            )
            summary = _summary_sources(bundle, manifest, file_sha)
            projection = projection_builder(bundle, document)
            frontier_digest = frontier_digest_builder(bundle)
            pid = bundle.publication_id
            outcome = pub.prepare_bundle(store, bundle)
            del (
                bundle
            )  # the validate step re-reads the persisted rows: do not hold two copies
            gc.collect()
            state = pub.validate_bundle(store, pid)
            with conn.transaction():
                release_id = diag.prepare_diagnostic(
                    conn,
                    schema=schema,
                    publication_id=pid,
                    projection=projection,
                    source_frontier_manifest_digest=frontier_digest,
                )
            with conn.transaction():
                report = _release_report(diag, conn, schema, release_id)
    except pub.PublicationError as exc:
        raise _refuse(exc) from exc
    except errors as exc:
        raise _refuse_diagnostic(exc) from exc
    return {
        "state": "prepared",
        "mode": "prepare",
        "prepare_outcome": outcome,
        "lifecycle_state": state.lifecycle_state,
        "release_id": str(release_id),
        "diagnostic_release_verified": True,
        "diagnostic_release": report,
        "source_frontier_manifest_digest": frontier_digest,
        "credit_pointer_elected": False,
        "diagnostic_pointer_elected": False,
        **summary,
    }


def _plan(
    inv: Invocation,
    dsn: str | None = None,
    schema: str | None = None,
    store_factory: Callable[[str, str], Any] | None = None,
    read_panel: bool = False,
    collab: Collaborators | None = None,
) -> dict[str, Any]:
    if inv.kind == "sources":
        assert store_factory is not None
        return _plan_sources(
            inv, dsn, schema, store_factory, read_panel, collab or Collaborators()
        )
    if read_panel:
        raise ConfigError("read_panel_requires_sources_input", inv.kind)
    bundle, digest = _read_frozen_bundle(inv)
    return {
        "state": "planned",
        "mode": "plan",
        "bundle_sha256": digest,
        **_summary(bundle),
        "database": "not_contacted",
        "network": "not_used",
    }


def _prepare(
    inv: Invocation,
    dsn: str,
    schema: str,
    store_factory: Callable[[str, str], Any],
    collab: Collaborators | None = None,
) -> dict[str, Any]:
    if inv.kind == "sources":
        return _prepare_sources(
            inv, dsn, schema, store_factory, collab or Collaborators()
        )
    bundle, digest = _read_frozen_bundle(
        inv
    )  # nothing is written unless the offline gate passes
    try:
        with store_factory(dsn, schema) as store:
            outcome = pub.prepare_bundle(store, bundle)
    except pub.PublicationError as exc:
        raise _refuse(exc) from exc
    return {
        "state": "prepared",
        "mode": "prepare",
        "prepare_outcome": outcome,
        "bundle_sha256": digest,
        **_summary(bundle),
    }


def _cross_check(state: pub.PublicationState, inv: Invocation | None) -> None:
    if inv is None:
        return
    _require_identity(
        {
            "publication_id": state.publication_id,
            "target_month": state.target_month,
            "knowledge_cutoff": state.knowledge_cutoff.astimezone(dt.timezone.utc),
            "quality_state": state.quality_state,
            "build_scope": state.build_scope,
        },
        inv,
        source="persisted",
    )


def _verify(
    pid: uuid.UUID,
    inv: Invocation | None,
    dsn: str,
    schema: str,
    store_factory: Callable[[str, str], Any],
    release_id: uuid.UUID | None = None,
    collab: Collaborators | None = None,
) -> dict[str, Any]:
    diag = _diagnostic(collab or Collaborators()) if release_id is not None else None
    verified: Any = None
    errors = _diagnostic_errors(diag) if diag is not None else (pub.PublicationError,)
    try:
        with store_factory(dsn, schema) as store:
            _cross_check(
                store.state(pid), inv
            )  # pinned T/K/identity checked before any state change
            if (
                diag is not None
            ):  # the release must belong to this publication before any state change
                with store.conn.transaction():
                    verified = _release_report(diag, store.conn, schema, release_id)
                _require_release_publication(verified, pid)
            state = pub.validate_bundle(store, pid)
    except pub.PublicationError as exc:
        raise _refuse(exc) from exc
    except errors as exc:
        raise _refuse_diagnostic(exc) from exc
    _cross_check(state, inv)
    if state.revoked:
        raise PublicationRefused("bond_credit_verify:revoked", str(pid))
    stats = {"state": "verified", "mode": "verify", **_state_dict(state)}
    if release_id is not None:
        stats["release_id"] = str(release_id)
        stats["diagnostic_release_verified"] = verified is not None
        stats["diagnostic_release"] = verified
    return stats


def _parse_expected_pointer(
    raw: Any, env_name: str = ENV_EXPECTED_POINTER
) -> uuid.UUID | None:
    if raw is _UNSET or raw is None or (isinstance(raw, str) and not raw.strip()):
        raise PromoteNotAuthorized(
            "expected_pointer_required", f"{env_name} (uuid or 'none')"
        )
    if isinstance(raw, uuid.UUID):
        return raw
    if str(raw).strip().lower() == NO_POINTER:
        return None
    return _parse_uuid(str(raw).strip().lower(), "expected_pointer", ConfigError)


def _promote(
    pid: uuid.UUID,
    inv: Invocation | None,
    expected_pointer: Any,
    confirm: str | None,
    dsn: str,
    schema: str,
    store_factory: Callable[[str, str], Any],
) -> dict[str, Any]:
    expected = _parse_expected_pointer(expected_pointer)
    if confirm != str(pid):
        raise PromoteNotAuthorized(
            "confirmation_required",
            f"{ENV_CONFIRM_PROMOTE} must equal the publication id",
        )
    try:
        with store_factory(dsn, schema) as store:
            state = store.state(pid)
            _cross_check(state, inv)
            if state.revoked:
                raise PublicationRefused("bond_credit_promote:revoked", str(pid))
            if state.lifecycle_state != "validated":
                raise PublicationRefused(
                    "bond_credit_promote:not_validated", state.lifecycle_state
                )
            if state.quality_state != "qualified" or state.build_scope != "complete":
                raise PublicationRefused(
                    "bond_credit_promote:not_qualified_complete",
                    f"{state.build_scope}/{state.quality_state}",
                )
            promoted = pub.promote_bundle(store, pid, expected_pointer=expected)
    except pub.PublicationError as exc:
        raise _refuse(exc) from exc
    return {
        "state": "promoted",
        "mode": "promote",
        "pointer": str(promoted),
        "expected_pointer": None if expected is None else str(expected),
        **{k: v for k, v in _state_dict(state).items() if k != "publication_id"},
    }


def _diagnostic_promote(
    release_id: uuid.UUID,
    expected_pointer: Any,
    confirm: str | None,
    dsn: str,
    schema: str,
    store_factory: Callable[[str, str], Any],
    collab: Collaborators,
    inv: Invocation | None = None,
) -> dict[str, Any]:
    """Compare-and-set the diagnostic pointer via the diagnostic adapter only (never qualified).

    When an invocation manifest is supplied, the release's publication id, T and K must equal its pins
    before the pointer is touched.
    """
    expected = _parse_expected_pointer(
        expected_pointer, ENV_EXPECTED_DIAGNOSTIC_POINTER
    )
    if confirm != str(release_id):
        raise PromoteNotAuthorized(
            "confirmation_required",
            f"{ENV_CONFIRM_DIAGNOSTIC_PROMOTE} must equal the release id",
        )
    diag = _diagnostic(collab)
    errors = _diagnostic_errors(diag)
    try:
        with store_factory(dsn, schema) as store:
            conn = store.conn
            with (
                conn.transaction()
            ):  # every guard re-run by ID before any pointer write
                report = _release_report(diag, conn, schema, release_id)
            if inv is not None:
                _require_release_identity(report, inv)
            with conn.transaction():
                diag.promote_diagnostic(
                    conn,
                    schema=schema,
                    release_id=release_id,
                    expected_release_id=expected,
                )
    except errors as exc:
        raise _refuse_diagnostic(exc) from exc
    return {
        "state": "diagnostic_promoted",
        "mode": "diagnostic-promote",
        "release_id": str(release_id),
        "expected_diagnostic_pointer": None if expected is None else str(expected),
        "diagnostic_release": report,
        "qualified_pointer_touched": False,
    }


def _install_schema(
    dsn: str,
    schema: str,
    confirm: str | None,
    env: Mapping[str, str],
    connect_factory: Callable[[str], Any],
    collab: Collaborators,
) -> dict[str, Any]:
    """Explicit additive DDL install as the (non-superuser) table owner, then harden and verify.

    Requires the four NOLOGIN group roles to exist already (an administrator creates them and the
    grants). Runs on an autocommit connection: ``install_schema`` (four pinned files, frozen order),
    ``install_diagnostic_schema``, ``harden_installed_privileges``, ``verify_installed_privileges``.
    The report holds only role names, pins (code, SQL and diagnostic SQL digests), counts and check
    verdicts. Bound to the reviewed tree: when ``BOND_DEFAULT_EVENTS_CODE_DIGEST`` is set it must equal
    :func:`source_bundle.code_digest` of the running tree, else ``code_digest_env_mismatch`` (exit 4)
    before any connection or DDL.

    Idempotent only while both pointer tables are empty: the read-back runs with
    ``require_empty_pointers=True`` and therefore refuses (``privileges_unverified``) once a diagnostic or
    qualified pointer has been promoted. A later re-harden must opt in explicitly with
    ``BOND_DEFAULT_EVENTS_ALLOW_PROMOTED_POINTERS=1`` (``require_empty_pointers=False``).
    """
    _require_public_schema(schema, "install-schema")
    if confirm != INSTALL_CONFIRMATION:
        raise PromoteNotAuthorized(
            "confirmation_required",
            f"{ENV_CONFIRM_INSTALL} must equal {INSTALL_CONFIRMATION}",
        )
    computed_digest = (collab.code_digest or sb.code_digest)()
    pinned_digest = _env(ENV_CODE_DIGEST, env)
    if pinned_digest is not None and pinned_digest != computed_digest:
        raise IntegrityError(
            "code_digest_env_mismatch",
            f"pinned={pinned_digest} computed={computed_digest}",
        )
    allow_promoted = (
        _env(ENV_ALLOW_PROMOTED_POINTERS, env) or ""
    ).lower() in _TRUE_VALUES
    diag = _diagnostic(collab)
    errors = _diagnostic_errors(diag)
    owner_expected = _env(ENV_EXPECTED_OWNER, env) or DEFAULT_EXPECTED_OWNER
    runtime_role = _env(ENV_RUNTIME_ROLE, env) or DEFAULT_RUNTIME_ROLE
    other_roles = tuple(
        r.strip()
        for r in (_env(ENV_OTHER_ROLES, env) or ",".join(DEFAULT_OTHER_ROLES)).split(
            ","
        )
        if r.strip()
    )
    for name in (owner_expected, runtime_role, *other_roles):
        if not _SCHEMA_RE.fullmatch(name):
            raise ConfigError("role_identifier_invalid", name[:40])
    try:
        conn = connect_factory(dsn)
    except Exception as exc:  # noqa: BLE001 - never chain: driver text can echo parameters
        raise InfrastructureError("db_connect_failed", type(exc).__name__) from None
    try:
        if not getattr(conn, "autocommit", False):
            raise ConfigError("autocommit_connection_required", "install-schema")
        current, session, superuser = conn.execute(
            "SELECT current_user::text, session_user::text, "
            "(SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user)"
        ).fetchone()
        if superuser:
            raise SchemaPrerequisiteError("service_role_is_superuser", current)
        if current != owner_expected or session != owner_expected:
            raise SchemaPrerequisiteError(
                "service_role_not_expected_owner",
                f"current={current} session={session} expected={owner_expected}",
            )
        present = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT rolname::text, rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = ANY(%s)",
                [list(diag.INTENDED_ROLES)],
            ).fetchall()
        }
        missing = sorted(set(diag.INTENDED_ROLES) - set(present))
        if missing:
            raise SchemaPrerequisiteError(
                "group_roles_missing",
                "an administrator must first create these NOLOGIN roles: "
                + ",".join(missing),
            )
        login = sorted(r for r, can in present.items() if can)
        if login:
            raise SchemaPrerequisiteError(
                "group_roles_must_be_nologin", ",".join(login)
            )
        conn.execute("SET search_path TO public, pg_temp")
        pub.install_schema(conn, schema)
        diag.install_diagnostic_schema(conn, schema=schema)
        hardened = diag.harden_installed_privileges(conn, schema=schema)
        verified = diag.verify_installed_privileges(
            conn,
            schema=schema,
            runtime_role=runtime_role,
            other_roles=other_roles,
            require_empty_pointers=not allow_promoted,
        )
    except errors as exc:
        raise _refuse_diagnostic(exc) from exc
    except WorkerError:
        raise
    except Exception as exc:
        if _is_driver_error(exc):
            state = getattr(exc, "sqlstate", None) or "unknown"
            if state == "42501":
                raise SchemaPrerequisiteError(
                    "insufficient_privilege", "owner lacks CREATE/USAGE on public"
                ) from None
            raise InfrastructureError(
                "install_failed", f"{type(exc).__name__}:{state}"
            ) from None
        raise
    finally:
        with contextlib.suppress(Exception):
            conn.close()
    return {
        "state": "installed",
        "mode": "install-schema",
        "schema": schema,
        "owner": current,
        "superuser": False,
        "roles": {name: {"present": True, "nologin": True} for name in sorted(present)},
        "runtime_role": runtime_role,
        "other_roles": list(other_roles),
        "code_digest": computed_digest,
        "require_empty_pointers": not allow_promoted,
        "idempotency_note": (
            "rerun is idempotent only while both pointer tables are empty; after a promotion set "
            f"{ENV_ALLOW_PROMOTED_POINTERS}=1 to re-harden"
        ),
        "pins": {
            "contract_version": c.CONTRACT_VERSION,
            "policy_digest": c.POLICY_DIGEST,
            "contract_digest": c.SCHEMA_DIGEST,
            "sql_digest": c.sql_digest(),
            "diagnostic_sql_digest": diag.diagnostic_sql_digest(),
        },
        "harden": {
            "manifest_digest": hardened.get("manifest_digest"),
            "tables": hardened.get("tables"),
            "sequences": hardened.get("sequences"),
            "functions": hardened.get("functions"),
            "revoked_count": len(hardened.get("revoked", ())),
            "intended_grants_added_count": len(
                hardened.get("intended_grants_added", ())
            ),
        },
        "checks": {name: bool(v["ok"]) for name, v in verified["checks"].items()},
        "checks_ok": bool(verified["ok"]),
    }


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------
def _env(name: str, env: Mapping[str, str]) -> str | None:
    value = env.get(name)
    return value.strip() if value is not None and value.strip() else None


def run(
    dsn: str | None = None,
    *,
    mode: str | None = None,
    manifest: str | os.PathLike[str] | None = None,
    schema: str | None = None,
    publication_id: str | None = None,
    expected_pointer: Any = _UNSET,
    confirm_promote: str | None = None,
    release_id: str | None = None,
    expected_diagnostic_pointer: Any = _UNSET,
    confirm_diagnostic_promote: str | None = None,
    read_panel: bool | None = None,
    confirm_install: str | None = None,
    env: Mapping[str, str] | None = None,
    store_factory: Callable[[str, str], Any] | None = None,
    collaborators: Collaborators | None = None,
    connect_factory: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Run one mode. Unset arguments fall back to ``BOND_DEFAULT_EVENTS_*`` in ``env``.

    ``src.run_worker`` passes only the DSN, so operators configure the worker through the
    environment. ``dsn`` is used solely by the database modes (and ``plan`` with ``read_panel``) and
    is never logged. Returns loader stats; raises :class:`WorkerError` subclasses on any refusal.
    """
    environ = os.environ if env is None else env
    mode = mode or _env(ENV_MODE, environ) or DEFAULT_MODE
    if mode not in MODES:
        raise ConfigError("mode_unknown", f"{mode!r} not in {list(MODES)}")
    manifest = manifest or _env(ENV_MANIFEST, environ)
    schema = schema or _env(ENV_SCHEMA, environ)
    publication_id = publication_id or _env(ENV_PUBLICATION_ID, environ)
    if expected_pointer is _UNSET:
        expected_pointer = _env(ENV_EXPECTED_POINTER, environ) or _UNSET
    confirm_promote = confirm_promote or _env(ENV_CONFIRM_PROMOTE, environ)
    release_id = release_id or _env(ENV_RELEASE_ID, environ)
    if expected_diagnostic_pointer is _UNSET:
        expected_diagnostic_pointer = (
            _env(ENV_EXPECTED_DIAGNOSTIC_POINTER, environ) or _UNSET
        )
    confirm_diagnostic_promote = confirm_diagnostic_promote or _env(
        ENV_CONFIRM_DIAGNOSTIC_PROMOTE, environ
    )
    confirm_install = confirm_install or _env(ENV_CONFIRM_INSTALL, environ)
    if read_panel is None:
        read_panel = (_env(ENV_PLAN_READ_PANEL, environ) or "").lower() in _TRUE_VALUES
    factory = store_factory or _open_store
    collab = collaborators or Collaborators()

    if mode != "promote" and (expected_pointer is not _UNSET or confirm_promote):
        raise ConfigError("promote_options_outside_promote_mode", mode)
    if mode != "diagnostic-promote" and (
        expected_diagnostic_pointer is not _UNSET or confirm_diagnostic_promote
    ):
        raise ConfigError("diagnostic_promote_options_outside_mode", mode)
    if release_id and mode not in ("verify", "diagnostic-promote"):
        # A diagnostic release ID can never be handed to qualified promotion (or prepare/plan).
        raise ConfigError("release_id_not_applicable", mode)
    if read_panel and mode != "plan":
        raise ConfigError("read_panel_only_in_plan_mode", mode)
    if confirm_install and mode != "install-schema":
        raise ConfigError("install_options_outside_install_mode", mode)
    if mode == "install-schema" and (publication_id or release_id or read_panel):
        raise ConfigError("install_schema_takes_no_publication_options", mode)
    if mode in ("plan", "prepare", "diagnostic-promote") and publication_id:
        raise ConfigError("publication_id_not_applicable", mode)
    if mode in ("plan", "prepare") and not manifest:
        raise ConfigError("manifest_required", f"{ENV_MANIFEST} for mode {mode}")
    if mode in ("verify", "promote") and not publication_id:
        raise ConfigError(
            "publication_id_required", f"{ENV_PUBLICATION_ID} for mode {mode}"
        )
    if mode == "diagnostic-promote" and not release_id:
        raise ConfigError("release_id_required", f"{ENV_RELEASE_ID} for {mode}")
    if mode != "plan" or read_panel:
        if not dsn:
            raise ConfigError("dsn_required", mode)
        if not schema:
            raise ConfigError("schema_required", ENV_SCHEMA)

    if mode == "install-schema":  # a leftover manifest variable is irrelevant here
        inv = None
        _require_public_schema(schema, mode)
    else:
        inv = load_invocation(manifest) if manifest else None
    sources = inv is not None and inv.kind == "sources"
    code_env = _env(ENV_CODE_DIGEST, environ)
    if sources and inv is not None and code_env and code_env != inv.code_digest:
        raise IntegrityError(
            "code_digest_env_mismatch", "env and invocation pins differ"
        )
    if (
        (mode == "plan" and read_panel and sources)
        or (mode == "prepare" and sources)
        or (mode in ("verify", "diagnostic-promote") and release_id)
    ):
        _require_public_schema(schema, mode)  # refused before any store is opened
    log.info("bond_default_events mode=%s", mode)
    try:
        if mode == "plan":
            assert inv is not None
            stats = _plan(inv, dsn, schema, factory, bool(read_panel), collab)
        elif mode == "prepare":
            assert inv is not None and dsn and schema
            stats = _prepare(inv, dsn, schema, factory, collab)
        elif mode == "install-schema":
            assert dsn and schema
            stats = _install_schema(
                dsn,
                schema,
                confirm_install,
                environ,
                connect_factory or _connect_autocommit,
                collab,
            )
        elif mode == "diagnostic-promote":
            assert dsn and schema and release_id
            rid = _parse_uuid(release_id.lower(), "release_id", ConfigError)
            stats = _diagnostic_promote(
                rid,
                expected_diagnostic_pointer,
                confirm_diagnostic_promote,
                dsn,
                schema,
                factory,
                collab,
                inv,
            )
        else:
            assert dsn and schema and publication_id
            pid = _parse_uuid(publication_id.lower(), "publication_id", ConfigError)
            if inv is not None and inv.publication_id != pid:
                raise IntegrityError(
                    "manifest_publication_id_mismatch",
                    f"manifest={inv.publication_id} arg={pid}",
                )
            if mode == "verify":
                rid = (
                    _parse_uuid(release_id.lower(), "release_id", ConfigError)
                    if release_id
                    else None
                )
                stats = _verify(pid, inv, dsn, schema, factory, rid, collab)
            else:
                stats = _promote(
                    pid, inv, expected_pointer, confirm_promote, dsn, schema, factory
                )
    except WorkerError:
        raise
    except pub.PublicationError as exc:
        raise _refuse(exc) from exc
    except c.ContractError as exc:
        raise ContractViolation("contract_violation", str(exc)[:300]) from exc
    return {"worker": WORKER, **stats}


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.workers.bond_default_events",
        description=__doc__.split("\n", 1)[0],
    )
    p.add_argument(
        "--mode", choices=MODES, default=None, help=f"default: {DEFAULT_MODE} (dry run)"
    )
    p.add_argument(
        "--manifest",
        help="frozen invocation manifest (plan/prepare; optional cross-check elsewhere)",
    )
    p.add_argument(
        "--schema",
        help="target PostgreSQL schema holding the four bond_credit SQL files",
    )
    p.add_argument("--publication-id", help="verify/promote target")
    p.add_argument(
        "--expected-pointer",
        help="promote: current pointer UUID, or 'none' for an empty pointer",
    )
    p.add_argument("--confirm-promote", help="promote: must equal --publication-id")
    p.add_argument(
        "--release-id",
        help="verify (optional) / diagnostic-promote (required): frozen diagnostic release id",
    )
    p.add_argument(
        "--expected-diagnostic-pointer",
        help="diagnostic-promote: current diagnostic release UUID, or 'none' for an empty pointer",
    )
    p.add_argument(
        "--confirm-diagnostic-promote",
        help="diagnostic-promote: must equal --release-id",
    )
    p.add_argument(
        "--confirm-install",
        help=(
            f"install-schema: must equal {INSTALL_CONFIRMATION}. Idempotent only while both pointer "
            f"tables are empty; after a promotion re-harden needs {ENV_ALLOW_PROMOTED_POINTERS}=1. "
            f"{ENV_CODE_DIGEST}, when set, must equal the running tree code digest"
        ),
    )
    p.add_argument(
        "--read-panel",
        action="store_true",
        default=None,
        help="plan with a sources input: perform one READ ONLY panel read (nothing persisted)",
    )
    p.add_argument(
        "--dsn-env",
        default="DATABASE_URL",
        help="name of the environment variable holding the DSN (default DATABASE_URL)",
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    """CLI: prints one JSON object; exits 0 or a typed non-zero status (see :class:`WorkerError`)."""
    try:
        args = _parser().parse_args(argv)
    except SystemExit as exc:
        return ConfigError.exit_code if exc.code else 0
    dsn = os.environ.get(args.dsn_env) or None
    effective_mode = args.mode or _env(ENV_MODE, os.environ) or DEFAULT_MODE
    wants_panel = (
        args.read_panel
        if args.read_panel is not None
        else (_env(ENV_PLAN_READ_PANEL, os.environ) or "").lower() in _TRUE_VALUES
    )
    try:
        if dsn and (
            effective_mode != "plan" or wants_panel
        ):  # plan never resolves a DSN eagerly
            from src.db import resolve_dsn

            dsn = resolve_dsn(dsn)
        stats = run(
            dsn,
            mode=args.mode,
            manifest=args.manifest,
            schema=args.schema,
            publication_id=args.publication_id,
            expected_pointer=_UNSET
            if args.expected_pointer is None
            else args.expected_pointer,
            confirm_promote=args.confirm_promote,
            release_id=args.release_id,
            expected_diagnostic_pointer=_UNSET
            if args.expected_diagnostic_pointer is None
            else args.expected_diagnostic_pointer,
            confirm_diagnostic_promote=args.confirm_diagnostic_promote,
            read_panel=args.read_panel,
            confirm_install=args.confirm_install,
        )
    except WorkerError as exc:
        print(
            json.dumps(
                {
                    "worker": WORKER,
                    "state": "failed",
                    "code": exc.code,
                    "detail": exc.detail,
                    "exit_code": exc.exit_code,
                }
            ),
            flush=True,
        )
        return exc.exit_code
    except Exception as exc:  # noqa: BLE001 - sanitized: class name only, never the message
        print(
            json.dumps(
                {
                    "worker": WORKER,
                    "state": "failed",
                    "code": "unexpected_error",
                    "detail": type(exc).__name__,
                    "exit_code": InfrastructureError.exit_code,
                }
            ),
            flush=True,
        )
        return InfrastructureError.exit_code
    print(json.dumps(stats, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

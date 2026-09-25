"""Verify or publish the exact frozen round-002 implied-rating artifact."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""} and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bonds import implied_rating_artifact_loader as loader
from src.db import resolve_dsn


def _connection_factory() -> psycopg.Connection:
    try:
        dsn = resolve_dsn()
    except (KeyError, OSError, RuntimeError, ValueError) as exc:
        raise loader.ArtifactLoaderError(
            loader.ErrorCode.CONFIGURATION_ERROR,
            field="database.configuration",
            phase="connect",
            transaction_outcome="not_started",
        ) from exc
    return psycopg.connect(
        dsn,
        autocommit=True,
        connect_timeout=loader.CONNECT_TIMEOUT_SECONDS,
    )


def _safe_result(value: Any) -> str:
    if hasattr(value, "__dataclass_fields__"):
        value = asdict(value)
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))


def _record_failure(
    evidence_dir: Path,
    *,
    operation_id: str,
    operation_started_at_utc: str,
    mode: str,
    code: str,
    publication_id: str | None,
    failure_phase: str,
    schema_installed: bool | None,
    transaction_outcome: str | None,
    outcome: str,
) -> str:
    try:
        loader.persist_failure_receipt(
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode=mode,
            code=code,
            publication_id=publication_id,
            failure_phase=failure_phase,
            schema_installed=schema_installed,
            transaction_outcome=transaction_outcome,
            outcome=outcome,
        )
    except (loader.ArtifactLoaderError, OSError):
        pass
    return code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--verify-only", action="store_true", help="offline verification (default)")
    modes.add_argument("--dry-run", action="store_true", help="read-only database preflight")
    modes.add_argument("--apply", action="store_true", help="publish through the existing materializer")
    modes.add_argument(
        "--verify-published", action="store_true", help="read-only commit/replay recovery"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    operation_id = str(uuid4())
    operation_started_at_utc = datetime.now(timezone.utc).isoformat()
    mode = (
        "apply" if args.apply else "dry-run" if args.dry_run else
        "verify-published" if args.verify_published else "verify-only"
    )
    artifact = None
    try:
        artifact = loader.load_verified_artifact(args.artifact_root)
        if args.apply:
            result = loader.publish_verified_artifact(
                artifact,
                evidence_dir=args.evidence_dir,
                connection_factory=_connection_factory,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
            )
        elif args.dry_run:
            result = loader.dry_run_verified_artifact(
                artifact,
                evidence_dir=args.evidence_dir,
                connection_factory=_connection_factory,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
            )
        elif args.verify_published:
            result = loader.recover_published_artifact(
                artifact,
                evidence_dir=args.evidence_dir,
                connection_factory=_connection_factory,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
            )
        else:
            contract = loader.load_frozen_contract()
            receipt = loader._persist_receipt(
                args.evidence_dir,
                phase="verify-only",
                payload=loader._receipt_payload(
                    phase="verify-only",
                    artifact=artifact,
                    outcome="verified_offline",
                    stored=None,
                    operation_id=operation_id,
                    contract=contract,
                    schema_installed=False,
                    operation_started_at_utc=operation_started_at_utc,
                ),
            )
            result = loader.OperationResult(
                "verified_offline",
                artifact.publication.publication_id,
                False,
                None,
                None,
                (receipt,),
            )
        print(_safe_result(result))
        return 0 if result.outcome not in {"not_published", "validated_not_current"} else 3
    except loader.ArtifactLoaderError as exc:
        code = exc.code
        if not exc.receipt_written:
            code = _record_failure(
                args.evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode=mode,
                code=exc.code,
                publication_id=None if artifact is None else artifact.publication.publication_id,
                failure_phase=exc.phase or "offline_verify",
                schema_installed=exc.schema_installed,
                transaction_outcome=exc.transaction_outcome or "not_started",
                outcome=exc.outcome or "failed",
            )
        recovery_codes = {
            loader.ErrorCode.COMMIT_UNKNOWN.value,
            loader.ErrorCode.COMMITTED_EVIDENCE_INCOMPLETE.value,
        }
        state = "recovery_required" if code in recovery_codes else "refused"
        print(_safe_result({"state": state, "code": code}), file=sys.stderr)
        if code in recovery_codes:
            return 5
        if code in {
            loader.ErrorCode.DB_FAILURE.value,
            loader.ErrorCode.LOCK_TIMEOUT.value,
        }:
            return 4
        return 3
    except psycopg.Error:
        code = _record_failure(
            args.evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode=mode,
            code=loader.ErrorCode.DB_FAILURE.value,
            publication_id=None if artifact is None else artifact.publication.publication_id,
            failure_phase="database",
            schema_installed=None,
            transaction_outcome="not_started",
            outcome="failed",
        )
        print(_safe_result({"state": "failed", "code": code}), file=sys.stderr)
        return 4
    except (KeyError, OSError, RuntimeError, ValueError):
        code = _record_failure(
            args.evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode=mode,
            code=loader.ErrorCode.CONFIGURATION_ERROR.value,
            publication_id=None if artifact is None else artifact.publication.publication_id,
            failure_phase="configuration",
            schema_installed=None,
            transaction_outcome="not_started",
            outcome="failed",
        )
        print(_safe_result({"state": "refused", "code": code}), file=sys.stderr)
        return 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

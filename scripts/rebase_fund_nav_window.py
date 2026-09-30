"""Governed economic NAV rebase operator (w1-tiingo-adjusted-daily-v1).

``--mode plan`` (default) is a read-only dry run: no HTTP, no ledger rows; the
canonical plan JSON is written only to an explicit ``--plan-file`` (new file).
``--mode apply`` needs that plan file, its ``--plan-sha256``, an explicit
``--instrument-id`` allowlist (<= batch size <= 20) and the same pinned
budgets; it fetches ONE full-window adjusted Tiingo snapshot per instrument
(no fallback provider, no hidden retries) and reconciles each instrument in
its own transaction. DSN only from ``NAV_READINESS_DATABASE_URL``.

Stdout is one JSON object with sanitized codes (no payload, URL, DSN or
exception text). Exit: 0 planned/completed; 2 validation/provider/instrument
failure, an undetermined COMMIT (``unknown``) or a bookkeeping error listed in
``errors`` (``partial`` when writes may exist); 3 schema/access/dependency
incompatible; 4 lock busy with no instrument committed; 5 budget, lock or
interrupt stop after work. A lost COMMIT acknowledgement is reconciled by
receipt on a fresh connection (``committed_unverified``), never reported as
zero writes. ``--max-seconds`` bounds pacing, the request and the start of each
instrument's DML. Publishing risk/MV/readiness is a separate, existing step,
never a consequence of this command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from scripts import fund_nav_readiness_schema as schema_operator
from src.workers import nav_economic_rebase as rebase

_SAFE = re.compile(r"[A-Z0-9_]{1,48}|[a-z0-9_]{1,64}")


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, default=str))


def _schema_pins() -> dict:
    ddl = schema_operator.DDL.read_bytes()
    manifest = schema_operator.load_manifest(ddl)
    return {
        "sql_sha256": hashlib.sha256(ddl).hexdigest(),
        "catalog_sha256": manifest["signature_sha256"],
        "access_profile": schema_operator.ACCESS_PROFILE,
        "access_sha256": manifest["access_profile"]["signature_sha256"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("plan", "apply"), default="plan")
    parser.add_argument("--schema", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--instrument-id", action="append", default=[])
    parser.add_argument("--batch-size", type=int, default=rebase.MAX_BATCH)
    parser.add_argument("--max-instruments", type=int)
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--max-seconds", type=float)
    parser.add_argument("--rate-per-second", type=float)
    parser.add_argument("--plan-file")
    parser.add_argument("--plan-sha256")
    return parser


def _client(limits: rebase.RebaseLimits):
    """Real Tiingo client paced at the pinned rate; one request per call."""
    from src.workers._tiingo import TiingoClient, TokenBucket

    return TiingoClient(
        bucket=TokenBucket(max_tokens=1.0, refill_rate=limits.rate_per_second)
    )


def main(argv: list[str] | None = None, *, client_factory=None) -> int:
    args = _parser().parse_args(argv)
    result = rebase._empty_result(None)

    def finish(exit_status: int, **updates) -> int:
        result.update(updates)
        _emit(result)
        return exit_status

    try:
        if not schema_operator.SCHEMA_NAME.fullmatch(args.schema):
            raise rebase.RebaseError("SCHEMA_INVALID")
        if args.contract != rebase.CONTRACT_VERSION:
            raise rebase.RebaseError("CONTRACT_UNSUPPORTED")
        if None in (args.max_requests, args.max_seconds, args.rate_per_second):
            raise rebase.RebaseError("BUDGETS_REQUIRED")
        limits = rebase.RebaseLimits(
            batch_size=args.batch_size,
            max_instruments=(
                args.max_instruments
                if args.max_instruments is not None
                else args.batch_size
            ),
            max_requests=args.max_requests,
            max_seconds=args.max_seconds,
            rate_per_second=args.rate_per_second,
        ).validate()
        if args.mode == "apply" and (
            not args.plan_file or not args.plan_sha256 or not args.instrument_id
        ):
            raise rebase.RebaseError("APPLY_REQUIRES_PLAN_HASH_AND_ALLOWLIST")
        dsn = os.environ.get("NAV_READINESS_DATABASE_URL")
        if not dsn:
            raise rebase.RebaseError("DSN_REQUIRED")
        pins = _schema_pins()
    except (rebase.RebaseError, ValueError, OSError) as exc:
        code = exc.code if isinstance(exc, rebase.RebaseError) else type(exc).__name__
        return finish(rebase.EXIT_FAILED, code=code)

    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as check_conn:
            try:
                state = schema_operator._check(check_conn, args.schema)
            except schema_operator.PrerequisiteBlocked as exc:
                return finish(rebase.EXIT_INCOMPATIBLE, code=exc.args[0])
            if not state["ready"]:
                return finish(
                    rebase.EXIT_INCOMPATIBLE, code=state["code"] or "schema_not_ready"
                )
        with psycopg.connect(dsn, autocommit=False, connect_timeout=5) as conn:
            conn.execute(
                sql.SQL("SET search_path TO {}, pg_temp").format(
                    sql.Identifier(args.schema)
                )
            )
            conn.commit()
            if args.mode == "plan":
                return _plan(conn, args, limits, pins, finish)
            return _apply(
                conn,
                args,
                limits,
                pins,
                finish,
                client_factory,
                lambda: _connect_schema(dsn, args.schema),
            )
    except psycopg.Error as exc:
        return finish(rebase.EXIT_FAILED, code="DATABASE_ERROR", sqlstate=exc.sqlstate)


def _connect_schema(dsn: str, schema: str):
    """Fresh non-autocommit session on the target schema (receipt reconcile)."""
    conn = psycopg.connect(dsn, autocommit=False, connect_timeout=5)
    try:
        conn.execute(
            sql.SQL("SET search_path TO {}, pg_temp").format(sql.Identifier(schema))
        )
        conn.commit()
    except BaseException:
        conn.close()
        raise
    return conn


def _plan(conn, args, limits, pins, finish) -> int:
    scope = args.instrument_id or None
    try:
        conn.execute("SET TRANSACTION READ ONLY")
        plan = rebase.build_rebase_plan(
            conn, scope, limits, schema=args.schema, schema_pins=pins
        )
    except rebase.RebaseError as exc:
        return finish(rebase.EXIT_FAILED, code=exc.code)
    finally:
        conn.rollback()
    if args.plan_file:
        try:
            with open(args.plan_file, "xb") as handle:  # never overwrite
                handle.write(rebase.plan_bytes(plan))
        except FileExistsError:
            return finish(
                rebase.EXIT_FAILED, code="PLAN_FILE_EXISTS", plan_sha256=plan.sha256
            )
    excluded: dict[str, int] = {}
    for _iid, code in plan.manifest["excluded"]:
        excluded[code] = excluded.get(code, 0) + 1
    return finish(
        rebase.EXIT_OK,
        status="planned",
        plan_sha256=plan.sha256,
        planned=len(plan.items),
        excluded_counts=dict(sorted(excluded.items())),
        instruments=[
            {
                "instrument_id": item.instrument_id,
                "status": "planned",
                "code": None,
                "receipt_id": None,
                "changed_level_rows": 0,
                "changed_return_rows": 0,
                "revision_count": 0,
                "resolved_events": [],
                "needs": list(item.needs),
                "window_start": item.window_start.isoformat(),
                "window_end": item.window_end.isoformat(),
            }
            for item in plan.items
        ],
    )


def _apply(conn, args, limits, pins, finish, client_factory, reconnect) -> int:
    try:
        manifest = json.loads(Path(args.plan_file).read_bytes().decode("utf-8"))
        plan = rebase.RebasePlan.from_manifest(manifest)
        if manifest.get("schema") != args.schema or manifest.get("schema_pins") != pins:
            raise rebase.RebaseError("PLAN_STALE")
    except rebase.RebaseError as exc:
        return finish(rebase.EXIT_FAILED, code=exc.code)
    except (OSError, ValueError, KeyError, TypeError):
        return finish(rebase.EXIT_FAILED, code="PLAN_FILE_INVALID")
    factory = client_factory or _client
    try:
        client = factory(limits)
    except (RuntimeError, ValueError):  # e.g. TIINGO_API_KEY absent: no request made
        return finish(
            rebase.EXIT_FAILED, code="PROVIDER_NOT_CONFIGURED", plan_sha256=plan.sha256
        )
    try:
        result = rebase.run_rebase(
            conn,
            plan,
            instrument_ids=args.instrument_id,
            limits=limits,
            supplied_sha256=args.plan_sha256,
            client=client,
            reconnect=reconnect,
        )
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()
    code = result.get("code")
    if code is not None and not _SAFE.fullmatch(str(code)):
        result["code"] = "UNSAFE_CODE"
    return finish(rebase.exit_code(result), **result)


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())

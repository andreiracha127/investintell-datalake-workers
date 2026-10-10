"""Governed stored return-lineage flag repair; DSN: NAV_READINESS_DATABASE_URL.

Plan (default) is DB-only, read-only, and writes a plan only to a NEW file.
Apply requires the reviewed plan, SHA256, explicit instrument allowlist and
--confirm repair_nav_return_lineage_v1. --validate-only fetches but writes nothing.
Budgets must equal the plan. Exit: 0 complete (including explicit residuals),
2 guard/provider/SQL failure, 3 incompatible schema/access, 4 lock busy,
5 budget/interrupt or lock stop after work. Stdout is one sanitized JSON object.

Runbook: deploy the writer fix first. Plan, review the new artifact and digest,
validate a small explicit sample, then apply budgeted allowlisted batches shortly
BEFORE a full risk run. Historical revisions invalidate pinned risk publications.
Keep budgets identical to the reviewed plan; a budget stop rolls back the current
instrument and preserves earlier commits. Resume the same allowlist/plan or make
a new plan for the remaining queue. A changed head/identity/target/neighbour needs
a new plan. COMMIT_UNKNOWN includes run IDs: inspect those before retrying.

Residual codes are intentional, not provider data to overwrite: inactive/missing
ticker, open reexpression, NULL/unsupported kind, unknown provider, false flags,
return-recomputation requirements, missing observations, and mismatched levels or
kinds. Old last-NAV dates are inventoried separately: age alone is not a delisting.
No risk, materialized view, calendar, policy, or readiness publication is performed.

The ledger supports only normal and the contract-specific economic rebase. This
operator uses normal runs (terminal reason RETURN_LINEAGE_REPAIR), one per exact
date fetch, in one transaction per instrument. This prevents a synthetic spanning
attempt from masquerading as current provider-session evidence. All row evidence
uses stored levels and the final head; no rebase receipt or RESOLVED event is made.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from scripts import fund_nav_readiness_schema as schema_operator
from scripts.rebase_fund_nav_window import _schema_pins
from src.workers import nav_return_lineage_repair as repair


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise repair.RepairError("ARGUMENT_INVALID")


def _parser():
    parser = _Parser(description=__doc__)
    parser.add_argument("--mode", choices=("plan", "apply"), default="plan")
    parser.add_argument("--schema", default="public")
    parser.add_argument("--plan-file")
    parser.add_argument("--plan-sha256")
    parser.add_argument("--instrument-id", action="append", default=[])
    parser.add_argument("--confirm")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--max-requests", type=int, default=20)
    parser.add_argument("--max-seconds", type=float, default=120)
    parser.add_argument("--rate-per-second", type=float, default=1)
    return parser


def main(argv=None, *, client_factory=None):
    result = {"status": "failed", "code": None, "exit_code": 2}
    try:
        args = _parser().parse_args(argv)
        if not schema_operator.SCHEMA_NAME.fullmatch(args.schema):
            raise repair.RepairError("SCHEMA_INVALID")
        limits = repair.RepairLimits(args.batch_size, args.max_requests,
                                     args.max_seconds, args.rate_per_second).validate()
        if args.mode == "plan" and (args.validate_only or args.confirm or args.plan_sha256
                                    or args.instrument_id):
            raise repair.RepairError("PLAN_ARGUMENT_INVALID")
        if args.mode == "apply" and (not args.plan_file or not args.plan_sha256
                                     or not args.instrument_id
                                     or args.confirm != repair.CONFIRM_TOKEN):
            raise repair.RepairError("APPLY_REQUIRES_PLAN_ALLOWLIST_CONFIRM")
        pins = _schema_pins()
        plan = None
        if args.mode == "apply":
            try:
                plan = json.loads(Path(args.plan_file).read_bytes())
            except (OSError, ValueError):
                raise repair.RepairError("PLAN_FILE_INVALID") from None
            if not isinstance(plan, dict) or repair.sha256(plan) != args.plan_sha256:
                raise repair.RepairError("PLAN_DIGEST_MISMATCH")
            if plan.get("schema") != args.schema or plan.get("schema_pins") != pins:
                raise repair.RepairError("PLAN_SCHEMA_MISMATCH")
            if plan.get("limits") != limits.canonical():
                raise repair.RepairError("PLAN_LIMITS_MISMATCH")
        dsn = os.environ.get("NAV_READINESS_DATABASE_URL")
        if not dsn:
            raise repair.RepairError("DSN_REQUIRED")
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as check_conn:
            try:
                check = schema_operator._check(check_conn, args.schema)
            except schema_operator.PrerequisiteBlocked:
                raise repair.RepairError("SCHEMA_ACCESS_INCOMPATIBLE", 3) from None
            if not check["ready"]:
                raise repair.RepairError("SCHEMA_ACCESS_INCOMPATIBLE", 3)
        with psycopg.connect(dsn, autocommit=False, connect_timeout=5) as conn:
            conn.execute(sql.SQL("SET search_path TO {}, pg_temp").format(sql.Identifier(args.schema)))
            conn.commit()
            if args.mode == "plan":
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                try:
                    plan = repair.build_plan(conn, schema=args.schema, limits=limits, schema_pins=pins)
                finally:
                    conn.rollback()
                if args.plan_file:
                    try:
                        with open(args.plan_file, "xb") as handle:
                            handle.write(repair.plan_bytes(plan))
                    except FileExistsError:
                        raise repair.RepairError("PLAN_FILE_EXISTS") from None
                result = {"status": "planned", "code": None, "exit_code": 0,
                          "plan_sha256": repair.sha256(plan), **repair.summarize(plan)}
            else:
                client = (client_factory or repair.ProviderClient)(limits)
                try:
                    result = repair.run_repair(
                        conn, plan, instrument_ids=args.instrument_id,
                        supplied_sha256=args.plan_sha256, limits=limits,
                        client=client, validate_only=args.validate_only)
                finally:
                    close = getattr(client, "close", None)
                    if close:
                        close()
    except repair.RepairError as exc:
        result.update(code=exc.code, exit_code=exc.exit_code)
    except psycopg.Error as exc:
        incompatible = exc.sqlstate in ("42501", "42P01", "42703", "42883", "0A000")
        result.update(code="SCHEMA_ACCESS_INCOMPATIBLE" if incompatible else "DATABASE_ERROR",
                      exit_code=3 if incompatible else 2)
    except KeyboardInterrupt:
        result.update(code="INTERRUPTED", exit_code=5)
    except Exception:
        result.update(code="VALIDATION_OR_IO_ERROR", exit_code=2)
    print(json.dumps(result, sort_keys=True))
    return result["exit_code"]


if __name__ == "__main__":
    sys.exit(main())

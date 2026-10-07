"""Capture the chain's inputs without importing any database write entrypoint.

Only the worker's pure file readers, SELECT readers and date helpers are reused.
The session setup is deliberately separate: the worker's ``pin_search_path``
commits, which would break this tool's single-snapshot guarantee.
"""
from __future__ import annotations

import ast
import inspect
import os
import textwrap
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from src.input_packs.hashing import canonical_json_sha256

PROXY_NETLOC = "centerbeam.proxy.rlwy.net:36616"
RELATIONS = (
    "public.macro_observation_vintage",
    "public.eod_prices",
    "public.open_macro_v03_decision_chain",
)
ROLE_SQL = (
    "SELECT current_user, "
    "has_table_privilege(current_user, %s, 'INSERT, UPDATE, DELETE')"
)
CONTRACT_FUNCTIONS = (
    "verify_pack", "_verify_digests", "load_pack_inputs", "verify_backfill",
    "_natural_key", "_parse_available_at", "read_chain", "read_macro_delta",
    "read_eod_delta", "readiness", "arm_series_ids", "market_ticker",
    "next_month_end", "last_complete_month_end", "decision_time",
    "verify_is_the_certified_chain",
)
CONTRACT_CONSTANTS = (
    "READ_CHAIN_SQL", "MACRO_DELTA_SQL", "EOD_DELTA_SQL", "ARM_FRESHNESS_SQL",
    "MARKET_FRESHNESS_SQL", "CANONICAL_SHA256", "BACKFILL_SHA256", "CHAIN_TABLE",
    "VINTAGE_TABLE", "EOD_TABLE", "CHAIN_START", "BASIS", "CERTIFIED_PREFIX_CUTOFF",
)


class SnapshotRefused(RuntimeError):
    """A safety or input contract check refused capture."""


def database_url() -> str:
    """Read credentials solely from DATABASE_URL and rewrite internal hosts.

    Keyword DSNs are refused; URL query host/port overrides are removed when
    rewriting, so libpq cannot silently reconnect to the private hostname.
    The URL is never included in the snapshot or an exception message.
    """
    raw = os.environ.get("DATABASE_URL", "")
    parsed = urlsplit(raw.replace("postgresql+asyncpg://", "postgresql://", 1))
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise SnapshotRefused("DATABASE_URL must be a PostgreSQL connection URL")
    if parsed.hostname.endswith(".railway.internal"):
        credentials = parsed.netloc.rsplit("@", 1)[0] + "@" if "@" in parsed.netloc else ""
        query = urlencode([
            (key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key not in {"host", "hostaddr", "port"}
        ])
        parsed = parsed._replace(netloc=credentials + PROXY_NETLOC, query=query)
    return urlunsplit(parsed._replace(scheme="postgresql"))


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def input_contract_sha256(worker=None) -> str:
    """Fingerprint readers/queries and resolved registry settings, not model math.

    Both revision subprocesses check this against the capturing checkout. A
    changed reader must get an explicit loader update instead of a false green
    comparison against inputs the changed worker would never have loaded.
    """
    if worker is None:
        from src.workers import open_macro_v03_chain as worker
    functions = {}
    for name in CONTRACT_FUNCTIONS:
        functions[name] = ast.dump(ast.parse(textwrap.dedent(inspect.getsource(getattr(worker, name)))))
    # Inspect the assembly wiring as source, without importing/calling run. A
    # future change to run's filters, ordering or target selection must refuse
    # until this receipt's loader has been reviewed against the new contract.
    source = ast.parse(Path(worker.__file__).read_text(encoding="utf-8"))
    assembly = next(node for node in source.body
                    if isinstance(node, ast.FunctionDef) and node.name == "run")
    functions["assembly_source"] = ast.dump(assembly)
    # Hash the checkout-relative path expressions, not absolute worktree names.
    # verify_pack checks PACK's bytes; load_pack_inputs also uses MACRO_JSON etc.
    # A reassignment of only one of those paths must not bypass that verification.
    path_names = {"ROOT", "PACK", "MACRO_JSON", "EOD_JSON", "BACKFILL", "BACKFILL_MACRO_JSON"}
    path_bindings = [
        ast.dump(node) for node in source.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id in path_names for target in node.targets)
    ]
    constants = {name: _json_value(getattr(worker, name)) for name in CONTRACT_CONSTANTS}
    return canonical_json_sha256({
        "functions": functions, "constants": constants,
        "pack_path_bindings": path_bindings,
        "arm_series_ids": worker.arm_series_ids(), "market_ticker": worker.market_ticker(),
        "verified_pack_identity": worker.verify_pack(),
    })


def capture_snapshot(reference_date: date, *, connect=None,
                     statement_timeout_ms: int = 120_000) -> dict[str, Any]:
    """Read all inputs exactly once in one REPEATABLE READ READ ONLY transaction.

    ``connect`` is an injection point for fixture tests, never a DSN argument.
    A caught-up/unsettled cron is recorded explicitly; its next target can still
    be replayed for equivalence, without claiming that it is eligible to publish.
    """
    from src.workers.open_macro_v03_chain import (
        CHAIN_TABLE,
        EOD_TABLE,
        VINTAGE_TABLE,
        last_complete_month_end,
        load_pack_inputs,
        next_month_end,
        read_chain,
        read_eod_delta,
        read_macro_delta,
        readiness,
        verify_is_the_certified_chain,
        verify_pack,
    )

    if statement_timeout_ms <= 0:
        raise SnapshotRefused("statement_timeout_ms must be positive")
    # The refusal must cover the relations the imported readers actually use.
    # A future rename must not silently keep checking privileges on old tables
    # when capture and both measured revisions already share the new names.
    if (f"public.{VINTAGE_TABLE}", f"public.{EOD_TABLE}",
            f"public.{CHAIN_TABLE}") != RELATIONS:
        raise SnapshotRefused("worker relations changed; review privilege-check coverage")
    dsn = database_url()
    pack = verify_pack()
    contract = input_contract_sha256()
    if connect is None:
        import psycopg
        connect = psycopg.connect
    options = (
        "-c default_transaction_read_only=on "
        f"-c statement_timeout={statement_timeout_ms}"
    )
    # autocommit prevents psycopg from issuing an implicit BEGIN before this
    # explicit transaction. Nothing is read before its isolation is pinned.
    with connect(dsn, options=options, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        try:
            cur.execute("SELECT current_setting('transaction_read_only'), "
                        "current_setting('transaction_isolation')")
            read_only, isolation = cur.fetchone()
            if read_only != "on" or isolation != "repeatable read":
                raise SnapshotRefused("session is not REPEATABLE READ READ ONLY")
            cur.execute("SET LOCAL search_path TO public")
            for relation in RELATIONS:
                cur.execute(ROLE_SQL, (relation,))
                role, can_write = cur.fetchone()
                if can_write:
                    raise SnapshotRefused(f"role {role} can write {relation}")
            cur.execute("SELECT pg_current_snapshot()::text")
            pg_snapshot = cur.fetchone()[0]
            stored = read_chain(conn)
            verify_is_the_certified_chain(stored, pack["input_pack_sha256"])
            latest = max(stored)
            target = next_month_end(latest)
            horizon = last_complete_month_end(reference_date)
            gate = readiness(conn, target)
            macro, eod, macro_boundary, eod_boundary = load_pack_inputs()
            macro_delta = read_macro_delta(conn, macro_boundary)
            eod_delta = read_eod_delta(conn, eod_boundary)
        finally:
            # End the sole transaction even on refusal. There is no commit
            # path, DDL, advisory lock, worker run, or publication call.
            cur.execute("ROLLBACK")
    stored_rows = [{"as_of": key.isoformat(), **_json_value(row)}
                   for key, row in sorted(stored.items())]
    inputs = {
        "macro_rows": macro + macro_delta, "eod_rows": eod + eod_delta,
        "stored_rows": stored_rows, "readiness": gate,
    }
    counts = {
        "macro_rows": len(inputs["macro_rows"]), "eod_rows": len(inputs["eod_rows"]),
        "stored_rows": len(stored_rows), "pack_macro_rows": len(macro),
        "pack_eod_rows": len(eod), "live_macro_delta_rows": len(macro_delta),
        "live_eod_delta_rows": len(eod_delta), "readiness_arms": len(gate["arms"]),
    }
    cron_status = ("month_in_progress" if target > horizon else
                   "ready" if gate["ready"] else "inputs_not_settled")
    return {
        "schema_version": 1, "reference_date": reference_date.isoformat(),
        "target_date": target.isoformat(), "horizon_date": horizon.isoformat(),
        "cron_status": cron_status, "pack_sha256": pack["input_pack_sha256"],
        "input_contract_sha256": contract, "database_role": role,
        "transaction_snapshot": pg_snapshot,
        "pointers": {
            "input_pack_id": pack["input_pack_id"],
            "input_pack_sha256": pack["input_pack_sha256"],
            "chain_latest": latest.isoformat(),
            "stored_pack_sha256": sorted({row["pack_sha256"] for row in stored.values()}),
            "publication_ids": [],
            "publication_note": "The chain resolves no database publication pointer or ID.",
            "macro_boundary": macro_boundary.isoformat(),
            "eod_boundary": eod_boundary.isoformat(),
        },
        "row_counts": counts, "inputs": inputs,
        "input_digests": {name: canonical_json_sha256(rows) for name, rows in inputs.items()},
        "inputs_sha256": canonical_json_sha256(inputs),
    }

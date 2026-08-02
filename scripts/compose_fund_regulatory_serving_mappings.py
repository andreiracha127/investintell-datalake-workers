#!/usr/bin/env python3
"""Compose (and optionally promote) an app regulatory-serving publication.

The app-owned ``fund_regulatory_serving_*`` layer had no versioned composer:
publication v1 was written by an ops one-off that never resolved ``class_id``.
This script is that composer, with the pre-flip gate embedded.

Default run: compose a new publication carrying the SAME worker artifact pins
as the base publication with ``class_id`` resolved by the documented cascade,
run the gate, and validate.  The app pointer does NOT move without ``--promote``.

    # compose + gate + validate (no flip)
    python scripts/compose_fund_regulatory_serving_mappings.py --dsn "$DATABASE_URL"

    # same, then flip the pointer
    python scripts/compose_fund_regulatory_serving_mappings.py --dsn "$DATABASE_URL" --promote

    # rollback: point back at the previous publication (no data is touched)
    python scripts/compose_fund_regulatory_serving_mappings.py --dsn "$DATABASE_URL" \
        --rollback-to bcee45f9-0b64-5903-8dcd-f3067ab2fb80

Idempotence: the publication id is a UUIDv5 over the recipe revision, the
inherited worker pins and a content fingerprint of the mappings.  Re-running
with unchanged inputs finds the same id and reports an explicit no-op; the
append-only layer is never mutated in place.

Exit codes: 0 success, 1 gate failure or composition error.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import connect  # noqa: E402
from src.sec_serving.app_composition import (  # noqa: E402
    CompositionError,
    GateThresholds,
    compose,
    current_pointer,
    promote_publication,
    rollback_sql,
)


def normalize_dsn(dsn: str | None) -> str | None:
    """Drop a SQLAlchemy driver suffix (``postgresql+asyncpg://``) if present.

    Operators paste the app's ``DATABASE_URL`` straight in; libpq rejects the
    driver-qualified scheme, and silently failing on that would be hostile.
    """
    if not dsn:
        return dsn
    parts = urlsplit(dsn)
    if "+" in parts.scheme:
        parts = parts._replace(scheme=parts.scheme.split("+", 1)[0])
        return urlunsplit(parts)
    return dsn


def _print_report(payload: dict, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print(f"app_publication_id      = {payload['app_publication_id']}")
    print(f"app_publication_version = {payload['app_publication_version']}")
    print(f"base publication        = {payload['base_app_publication_id']} (v{payload['base_app_publication_version']})")
    print(f"recipe_revision         = {payload['recipe_revision']}")
    print(f"mapping_fingerprint     = {payload['mapping_fingerprint']}")
    print(f"created                 = {payload['created']}")
    print("cascade:")
    for key, value in payload["cascade"].items():
        if key == "unresolved_instruments":
            continue
        print(f"  {key:<26} = {value}")
    unresolved = payload["cascade"]["unresolved_instruments"]
    if unresolved:
        print(f"  unresolved_instruments     = {', '.join(unresolved)}")
    print("gate:")
    for key, value in payload["gate_metrics"].items():
        print(f"  {key:<26} = {value}")
    print(f"  gate_ok                    = {payload['gate_ok']}")
    for failure in payload["gate_failures"]:
        print(f"  FAIL: {failure}")
    print(f"validated               = {payload['validated']}")
    print(f"promoted                = {payload['promoted']}")
    print(f"rollback                = {payload['rollback_sql']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dsn", default=None, help="target DSN (default: DATABASE_URL)")
    parser.add_argument(
        "--base-app-publication-id",
        default=None,
        help="app publication whose worker pins are inherited (default: the current pointer)",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="move the app pointer to the composed publication (default: compose + gate + validate only)",
    )
    parser.add_argument(
        "--rollback-to",
        default=None,
        help="point the app pointer back at this validated publication and exit",
    )
    parser.add_argument("--json", action="store_true", help="emit the report as JSON")
    parser.add_argument("--expect-mappings", type=int, default=GateThresholds.mappings)
    parser.add_argument("--min-resolved", type=int, default=GateThresholds.resolved)
    parser.add_argument("--min-class-grain", type=int, default=GateThresholds.class_grain)
    parser.add_argument("--min-fee-matched", type=int, default=GateThresholds.fee_matched_instruments)
    parser.add_argument("--min-mandate-published", type=int, default=GateThresholds.mandate_published)
    args = parser.parse_args(argv)

    dsn = normalize_dsn(args.dsn)
    thresholds = GateThresholds(
        mappings=args.expect_mappings,
        resolved=args.min_resolved,
        class_grain=args.min_class_grain,
        fee_matched_instruments=args.min_fee_matched,
        mandate_published=args.min_mandate_published,
    )

    try:
        with connect(dsn) as conn:
            if args.rollback_to:
                target = UUID(args.rollback_to)
                promote_publication(conn, target)
                conn.commit()
                pointer = current_pointer(conn)
                print(f"app pointer = {pointer}")
                return 0 if pointer == target else 1

            base_app_id = UUID(args.base_app_publication_id) if args.base_app_publication_id else None
            result = compose(conn, base_app_id=base_app_id, thresholds=thresholds, promote=args.promote)
    except CompositionError as exc:
        print(f"composition failed: {exc}", file=sys.stderr)
        return 1

    _print_report(result.as_dict(), as_json=args.json)
    if not result.gate.ok:
        print("gate failed: nothing was validated and the app pointer did not move", file=sys.stderr)
        return 1
    if args.promote and not result.promoted:
        print("promotion did not take effect", file=sys.stderr)
        return 1
    if not args.promote:
        print(f"note: pointer unchanged; re-run with --promote to flip. rollback = {rollback_sql(result.base.app_publication_id)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

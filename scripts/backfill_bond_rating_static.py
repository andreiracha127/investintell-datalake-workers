"""One-time generic static-rating backfill from a pinned local parquet artifact."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_PATH = Path(r"C:\Users\andre\Downloads\stage1_osbap_0k_volume_2025\bond_panel_monthly\universe_snapshots_live.parquet")
EXPECTED_ARTIFACT_SHA256 = "ab48d99f466ae3a943ce0a2819175ab6efdd95212b4efc9079151750057b077a"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bonds.static_ratings import build_static_mapping, mapping_evidence, verify_mapping_against_artifact  # noqa: E402
from src.workers.bond_rating_static_backfill import install_schema, load_static_mapping, render_copy_slice, render_schema_install  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", default=DEFAULT_ARTIFACT_PATH, type=Path)
    parser.add_argument("--sha256", default=EXPECTED_ARTIFACT_SHA256)
    parser.add_argument("--emit-psql", action="store_true", help="emit stdout-only psql/COPY input for railway ssh transport")
    parser.add_argument("--emit-schema", action="store_true", help="emit stdout-only psql schema installation")
    parser.add_argument("--cursor", type=int, default=0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--dsn", help="local/development PostgreSQL DSN only; not a Railway-private production route")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.emit_schema:
        sys.stdout.write(render_schema_install())
        return 0
    result = build_static_mapping(args.artifact, expected_sha256=args.sha256)
    rows = tuple(result.mapping.values())
    if args.emit_psql:
        sys.stdout.write(render_copy_slice(rows, cursor=args.cursor, limit=args.limit))
        return 0
    if args.apply:
        if not args.dsn:
            parser.error("--apply requires --dsn; production transport is railway ssh ... psql -f -")
        from src.db import connect

        with connect(args.dsn) as conn:
            install_schema(conn)
            conn.commit()
            with conn.transaction():
                conn.execute("SET LOCAL ROLE worker_writer")
                counters = load_static_mapping(conn, rows)
        print(counters)
        return 0
    evidence = mapping_evidence(result)
    evidence["parity"] = verify_mapping_against_artifact(
        args.artifact, expected_sha256=args.sha256, mapping=result.mapping
    )
    print(evidence)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

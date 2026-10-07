"""Internal offline child for receipt_chain_production (CWD is the measured commit)."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

from chain_receipt_common import (
    hash_definition,
    project_rows,
    projection_metadata,
    row_digests,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--snapshot-sha256", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    # This process has no reason to connect anywhere, including via a libpq DSN
    # accidentally introduced in a future compute implementation.
    def deny_network(event, arguments):
        if event in {"socket.connect", "socket.getaddrinfo"}:
            raise RuntimeError("network access is forbidden in the offline chain replay")

    sys.addaudithook(deny_network)
    import psycopg

    def deny_connection(*args, **kwargs):
        raise RuntimeError("database access is forbidden in the offline chain replay")

    psycopg.connect = deny_connection
    psycopg.Connection.connect = deny_connection
    psycopg.AsyncConnection.connect = deny_connection
    source_root = Path.cwd().resolve()
    sys.path.insert(0, str(source_root))
    from src.input_packs.hashing import canonical_json_sha256
    from src.workers import open_macro_v03_chain as worker

    if Path(worker.__file__).resolve() != source_root / "src/workers/open_macro_v03_chain.py":
        raise RuntimeError("worker did not resolve to measured checkout")
    raw = args.snapshot.read_bytes()
    snapshot_sha256 = hashlib.sha256(raw).hexdigest()
    if snapshot_sha256 != args.snapshot_sha256:
        raise RuntimeError("snapshot bytes changed before replay")
    snapshot = json.loads(raw)
    from chain_production_snapshot import input_contract_sha256

    if input_contract_sha256(worker) != snapshot["input_contract_sha256"]:
        raise RuntimeError("input loader contract changed; review snapshot loader fidelity")
    target = dt.date.fromisoformat(snapshot["target_date"])
    series = worker.compute_series(
        snapshot["inputs"]["macro_rows"], snapshot["inputs"]["eod_rows"], target,
    )
    from harness.phase0q.decision import month_end_decision_dates

    expected = month_end_decision_dates(worker.CHAIN_START, target)
    if not series or [row.as_of for row in series] != expected:
        raise RuntimeError("replay did not return the complete ordered chain through target")
    rows = project_rows(worker, series, snapshot["pack_sha256"], args.revision)
    result = {
        "commit": args.revision,
        "snapshot_sha256": snapshot_sha256,
        "projection": projection_metadata(worker),
        "hash_definition": hash_definition(),
        **row_digests(rows, canonical_json_sha256),
        "rows": rows,
    }
    args.output.write_text(json.dumps(result, allow_nan=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

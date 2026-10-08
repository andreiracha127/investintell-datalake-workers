"""Recompute the certified chain receipt from the repository in the current directory.

This script can be invoked by absolute path while the current directory is an archive
of an older revision. All compute imports and their fixture paths then come from that
archive, not from this script's checkout. No database or network connection is made.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path

# Also works when this driver is invoked from an older checkout/archive.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from chain_receipt_common import (
    hash_definition,
    project_rows,
    projection_metadata,
    row_digests,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True,
                        help="full commit SHA of the checkout/archive being measured")
    parser.add_argument("--output", type=Path,
                        help="optional local JSON receipt path; parent must exist")
    parser.add_argument("--stage-a", action="store_true",
                        help="also recompute the frozen Stage A logical output")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("--revision must be a full lowercase 40-character commit SHA")
    if args.output is not None and not args.output.parent.is_dir():
        parser.error("--output parent directory must already exist")

    source_root = Path.cwd().resolve()
    worker_path = source_root / "src/workers/open_macro_v03_chain.py"
    if not worker_path.is_file():
        parser.error("run from the root of the checkout/archive being measured")
    # Deliberately prefer CWD over the location of this reusable script.
    sys.path.insert(0, str(source_root))
    from src.input_packs.hashing import canonical_json_sha256
    from src.workers import open_macro_v03_chain as worker

    if Path(worker.__file__).resolve() != worker_path.resolve():
        raise RuntimeError("worker import did not resolve to the requested source root")

    reference_date = dt.date(2026, 6, 30)
    manifest = worker.verify_pack()
    macro_rows, eod_rows, macro_boundary, eod_boundary = worker.load_pack_inputs()
    series = worker.compute_series(macro_rows, eod_rows, reference_date)
    from harness.phase0q.decision import month_end_decision_dates

    expected_dates = month_end_decision_dates(worker.CHAIN_START, reference_date)
    if len(series) != 148 or [row.as_of for row in series] != expected_dates:
        raise RuntimeError("replay must contain all 148 certified months in order")

    rows = project_rows(worker, series, manifest["input_pack_sha256"], args.revision)

    receipt = {
        "revision": args.revision,
        "revision_source": "caller-supplied checkout/archive identity",
        "source_root": str(source_root),
        "reference_date": reference_date.isoformat(),
        "inputs": {
            "source": "worker.load_pack_inputs: pinned certified pack plus vintage backfill",
            "pack_sha256": manifest["input_pack_sha256"],
            "macro_rows": len(macro_rows),
            "eod_rows": len(eod_rows),
            "macro_boundary": macro_boundary.isoformat(),
            "eod_boundary": eod_boundary.isoformat(),
            "production_inputs": False,
        },
        "projection": projection_metadata(worker),
        "hash_definition": hash_definition(),
        "certified_replay": row_digests(rows, canonical_json_sha256),
    }
    if args.stage_a:
        from harness.direct_activation import live_validation
        from harness.direct_activation.measure_stage_a_child import canonical_hash

        record = live_validation.compute(worker_commit_override=args.revision)
        receipt["stage_a"] = {
            "reference_date": live_validation.VALIDATION_AS_OF.isoformat(),
            "logical_output_hash": canonical_hash(record),
            "hash_function": "harness.direct_activation.measure_stage_a_child.canonical_hash",
            "definition": "SHA-256 of sorted-key compact UTF-8 JSON of the entire record, "
                          "excluding only provenance.worker_commit",
        }
    rendered = json.dumps(receipt, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    if args.output is not None:
        args.output.write_text(rendered, encoding="utf-8", newline="\n")
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

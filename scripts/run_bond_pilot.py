"""Manual command line for the unregistered, internal bond pilot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from src.bond_pilot.contracts import PilotError  # noqa: E402
from src.bond_pilot.workflow import qualify, run_calibration, run_fixture, write_stop_report  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Internal-only bond pilot manual workflows")
    commands = parser.add_subparsers(dest="command", required=True)
    qualify_parser = commands.add_parser("qualify")
    qualify_parser.add_argument("--source", required=True)
    qualify_parser.add_argument("--run-dir", required=True)
    qualify_parser.add_argument("--expected-sha256")
    fixture_parser = commands.add_parser("fixture-run")
    fixture_parser.add_argument("--source-manifest", required=True)
    fixture_parser.add_argument("--source-approval", required=True)
    fixture_parser.add_argument("--fixture", required=True)
    fixture_parser.add_argument("--mapping", required=True)
    fixture_parser.add_argument("--run-dir", required=True)
    calibrate_parser = commands.add_parser("calibrate")
    calibrate_parser.add_argument("--source-manifest", required=True)
    calibrate_parser.add_argument("--source-approval", required=True)
    calibrate_parser.add_argument("--mapping", required=True)
    calibrate_parser.add_argument("--mapping-approval", required=True)
    calibrate_parser.add_argument("--phase4-evidence", required=True)
    calibrate_parser.add_argument("--phase4-approval", required=True)
    calibrate_parser.add_argument("--mode", required=True, choices=("calibration", "first_bounded"))
    calibrate_parser.add_argument("--series", action="append", required=True)
    calibrate_parser.add_argument("--run-dir", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "qualify":
            result = qualify(source=args.source, run_dir=args.run_dir, expected_sha256=args.expected_sha256)
        elif args.command == "fixture-run":
            result = run_fixture(source_manifest=args.source_manifest, source_approval=args.source_approval, fixture=args.fixture, mapping=args.mapping, run_dir=args.run_dir)
        else:
            result = run_calibration(source_manifest=args.source_manifest, source_approval=args.source_approval, mapping=args.mapping, mapping_approval=args.mapping_approval, evidence=args.phase4_evidence, evidence_approval=args.phase4_approval, mode=args.mode, series_ids=tuple(args.series), run_dir=args.run_dir)
    except PilotError as error:
        write_stop_report(args.run_dir, error)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

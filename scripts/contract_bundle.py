#!/usr/bin/env python3
"""Build or verify the versioned quant-engine contract bundle.

The worker repository is the single source of truth for the quant-engine
contract. This one script (re)generates `contracts/quant-engine/v1/manifest.json`
(schemas + fixtures, each with its sha256, plus a single bundle_sha256) and
verifies the bundle's integrity. The backend mirrors the schema hashes and runs
the same verifier, so any drift between repositories fails loud.

Usage:
    python scripts/contract_bundle.py build      # regenerate manifest.json
    python scripts/contract_bundle.py verify     # check integrity (CI gate)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))

from investintell_quant_engine.contract_bundle import verify_bundle, write_manifest

DEFAULT_BUNDLE = ROOT / "contracts" / "quant-engine" / "v1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="regenerate manifest.json")
    build.add_argument("--bundle-dir", default=str(DEFAULT_BUNDLE))
    build.add_argument("--contract-version", default="1.0.0")

    verify = sub.add_parser("verify", help="verify bundle integrity")
    verify.add_argument("--bundle-dir", default=str(DEFAULT_BUNDLE))

    args = parser.parse_args(argv)

    if args.command == "build":
        path = write_manifest(args.bundle_dir, contract_version=args.contract_version)
        result = verify_bundle(args.bundle_dir)
        print(f"wrote {path}")
        print(f"contract_version={result['contract_version']} bundle_sha256={result['bundle_sha256']} ok={result['ok']}")
        return 0 if result["ok"] else 1

    result = verify_bundle(args.bundle_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Standalone workers-side ``mixed_quant_v1`` contract verifier.

Recomputes the frozen surface digest from this repo's declarations and checks it
against the pinned ``SURFACE_DIGEST`` (mirrored in the app repo), plus the
publisher vocabulary. Exit code is non-zero on any drift, so this is a usable CI
gate. Mirrors ``scripts/contract_bundle.py verify`` for the quant-engine bundle.

Usage:
    PYTHONPATH=. python scripts/verify_mixed_quant_contract.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.quant_data import mixed_quant_contract as mq  # noqa: E402


def main() -> int:
    verdict = mq.verify_contract()
    print(json.dumps(verdict, indent=2, sort_keys=True))
    ok = bool(verdict["ok"])
    if not ok:
        for mismatch in verdict["mismatches"]:
            print(f"FAIL {mismatch}")
    print("\nRESULT:", "OK" if ok else "DRIFT DETECTED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

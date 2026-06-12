"""CLI dispatcher: ``python -m src.run <worker> [--calc-date YYYY-MM-DD] [--limit N]``.

Loads ``src.workers.<worker>`` dynamically and calls its ``run(dsn, ...)``.
Each worker module is self-contained; this dispatcher never imports them eagerly,
so a missing/in-progress worker never breaks the others.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys

from src.db import resolve_dsn


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("worker", help="module name under src/workers (e.g. risk_metrics)")
    ap.add_argument("--calc-date", default=None)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    try:
        mod = importlib.import_module(f"src.workers.{args.worker}")
    except ModuleNotFoundError as exc:
        sys.exit(f"unknown worker {args.worker!r}: {exc}")

    stats = mod.run(resolve_dsn(), calc_date=args.calc_date, limit=args.limit)
    print(json.dumps({"worker": args.worker, **(stats or {})}, default=str))


if __name__ == "__main__":
    main()

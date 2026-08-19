"""CLI dispatcher: ``python -m src.run <worker> [--calc-date YYYY-MM-DD] [--limit N]``.

Loads ``src.workers.<worker>`` dynamically and calls its ``run(dsn, ...)``.
Each worker module is self-contained; this dispatcher never imports them eagerly,
so a missing/in-progress worker never breaks the others.
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import sys

from src.db import resolve_dsn


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "worker",
        help="module name under src/workers (e.g. risk_metrics, mixed_quant_publication)",
    )
    ap.add_argument("--calc-date", default=None)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    try:
        mod = importlib.import_module(f"src.workers.{args.worker}")
    except ModuleNotFoundError as exc:
        sys.exit(f"unknown worker {args.worker!r}: {exc}")

    # Pass ONLY what this worker's run() declares, exactly as run_worker.py does.
    # Passing both unconditionally raised TypeError before any work for every
    # worker that takes neither -- 22 of them, measured 2026-08-19 -- which made
    # the hand-operated entry point unusable on a third of the fleet while the
    # scheduled one (run_worker.py, which has always filtered by signature) ran
    # them fine. Two dispatchers must not disagree about how a worker is called.
    accepted = inspect.signature(mod.run).parameters
    options = {"calc_date": args.calc_date, "limit": args.limit}
    kwargs = {name: value for name, value in options.items() if name in accepted}
    for name, value in options.items():
        if value is not None and name not in accepted:
            sys.exit(f"worker {args.worker!r} does not take --{name.replace('_', '-')}")
    stats = mod.run(resolve_dsn(), **kwargs)
    print(json.dumps({"worker": args.worker, **(stats or {})}, default=str))


if __name__ == "__main__":
    main()

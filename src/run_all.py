"""Run all metric workers in dependency order (one batch run, then exit).

Order matters: factor_model consumes equity_characteristics produced by the
characteristics worker, which in turn needs raw XBRL + N-PORT already in the
cloud. A failure in one worker is logged and does not abort the others.

Entry point for the Railway cron service. DSN comes from DATABASE_URL.
"""

from __future__ import annotations

import json
import sys

from src.db import resolve_dsn

WORKERS = ["risk_metrics", "characteristics", "factor_model"]


def main() -> None:
    dsn = resolve_dsn()
    import importlib

    failures = 0
    for name in WORKERS:
        print(f"=== {name} ===", flush=True)
        try:
            mod = importlib.import_module(f"src.workers.{name}")
            stats = mod.run(dsn)
            print(json.dumps({"worker": name, **(stats or {})}, default=str), flush=True)
        except Exception as exc:  # noqa: BLE001 — one worker must not abort the batch
            failures += 1
            print(f"FAIL {name}: {exc}", flush=True)
    if failures:
        sys.exit(f"{failures}/{len(WORKERS)} workers failed")


if __name__ == "__main__":
    main()

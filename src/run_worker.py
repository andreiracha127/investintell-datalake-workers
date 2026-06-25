"""Single-worker entry point for Railway (one service per worker).

Each Railway service sets WORKER=<name> and shares DATABASE_URL. The service's
cron schedule triggers this; it runs that one worker against the cloud and exits.
"""

from __future__ import annotations

import importlib
import json
import os
import sys

from src.db import resolve_dsn


def main() -> None:
    worker = os.getenv("WORKER")
    if not worker:
        sys.exit(
            "WORKER env var not set (expected risk_metrics|characteristics|factor_model"
            "|nport_lookthrough|credit_regime|regime_composite|macro_ingestion"
            "|treasury_ingestion|benchmark_ingest|instrument_ingestion"
            "|eod_prices_warmer|sec_13f_ingestion|form345_ingestion"
            "|screener_metrics|fund_factors|fund_institutional_reveal"
            "|matview_refresh|stock_daily_returns)"
        )
    mod = importlib.import_module(f"src.workers.{worker}")
    stats = mod.run(resolve_dsn())
    print(json.dumps({"worker": worker, **(stats or {})}, default=str), flush=True)


if __name__ == "__main__":
    main()

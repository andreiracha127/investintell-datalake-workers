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
            "|nport_lookthrough|credit_regime|regime_composite|regime_gate"
            "|quadrant_macro|quadrant_macro_v2|quadrant_macro_v3|quadrant_market"
            "|macro_ingestion"
            "|macro_vintage|treasury_ingestion|benchmark_ingest|instrument_ingestion"
            "|eod_prices_warmer|sec_13f_ingestion|form345_ingestion"
            "|sec_company_tickers_mf|nport_cusip_enrichment"
            "|nport_ingestion|ncen_ingestion|ncen_derived_profiles|rr1_ingestion"
            "|rr1_derived_profiles|sec_regulatory_serving"
            "|screener_metrics|fund_factors|fund_institutional_reveal"
            "|matview_refresh|stock_daily_returns"
            "|active_share_metrics|momentum_metrics|open_macro_v03"
            "|open_macro_v04"
            "|open_macro_v03_monitor|gamma_drift|ipca_production_gate"
            "|tiingo_fund_meta|mixed_quant_publication|mixed_quant_retention)"
        )
    mod = importlib.import_module(f"src.workers.{worker}")
    stats = mod.run(resolve_dsn())
    print(json.dumps({"worker": worker, **(stats or {})}, default=str), flush=True)


if __name__ == "__main__":
    main()

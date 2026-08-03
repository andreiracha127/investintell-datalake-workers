"""Single-worker entry point for Railway (one service per worker).

Each Railway service sets WORKER=<name> and shares DATABASE_URL. The service's
cron schedule triggers this; it runs that one worker against the cloud and exits.

Optional WORKER_LIMIT=<n> caps the units one run processes, for workers whose
``run()`` takes ``limit``. Combined with a multi-hour cron it batches a single
resumable sweep across several runs — the way to keep a growing universe inside a
provider's hourly budget without touching worker code.

Exit code is the contract with the platform: 0 only when the run finished its
work. A run that stopped early on a provider budget reports ``aborted`` in its
stats and exits non-zero, so a truncated sweep is never painted green.
"""

from __future__ import annotations

import importlib
import inspect
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
            "|nport_ingestion|ncen_ingestion|rr1_ingestion"
            "|rr1_derived_profiles|sec_regulatory_serving"
            "|screener_metrics|fund_factors|fund_institutional_reveal"
            "|matview_refresh|stock_daily_returns"
            "|active_share_metrics|momentum_metrics|open_macro_v03"
            "|open_macro_v04"
            "|open_macro_v03_monitor|gamma_drift|ipca_production_gate"
            "|tiingo_fund_meta|mixed_quant_publication|mixed_quant_retention)"
        )
    mod = importlib.import_module(f"src.workers.{worker}")

    # WORKER_LIMIT caps how many units one run processes. Workers whose sweep is
    # a resumable ring (eod_prices_warmer: priority head + cursor-rotated tail)
    # use it to spread a single sweep over several crons — three runs a day at
    # different hours each stay well inside the provider's hourly budget, where
    # one unbounded run would consume most of it. Config-only: the batching needs
    # no worker change, just this cap plus a multi-hour cron.
    kwargs: dict[str, int] = {}
    raw_limit = os.getenv("WORKER_LIMIT", "").strip()
    if raw_limit:
        try:
            limit = int(raw_limit)
        except ValueError:
            sys.exit(f"WORKER_LIMIT={raw_limit!r} is not an integer")
        if limit < 1:
            sys.exit(f"WORKER_LIMIT={limit} would cap nothing (expected >= 1)")
        if "limit" not in inspect.signature(mod.run).parameters:
            # Silently dropping it would leave the sweep unbounded while the
            # config claims otherwise — the same shape of failure as a budget
            # abort that exits 0.
            sys.exit(f"WORKER_LIMIT is set but {worker}.run() takes no 'limit'")
        kwargs["limit"] = limit

    stats = mod.run(resolve_dsn(), **kwargs) or {}
    print(json.dumps({"worker": worker, **stats}, default=str), flush=True)

    # A sweep that hit the provider budget sets ``stats["aborted"]``, commits what
    # it got and advances its cursor so the next cycle resumes — all correct. What
    # was wrong is exiting 0: the platform then paints the service green while the
    # run was truncated, which is exactly how the 2026-08-02 Tiingo starvation went
    # unnoticed for five days. Emit the stats first (operators need the progress),
    # then fail, so the truncation is visible as a failure and not just a log line.
    if stats.get("aborted"):
        sys.exit(1)


if __name__ == "__main__":
    main()

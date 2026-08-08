"""Single-worker entry point for Railway (one service per worker).

Each Railway service sets WORKER=<name> and shares DATABASE_URL. The service's
cron schedule triggers this; it runs that one worker against the cloud and exits.

Optional WORKER_LIMIT=<n> caps the units one run processes, for workers whose
``run()`` takes ``limit``. Combined with a multi-hour cron it batches a single
resumable sweep across several runs — the way to keep a growing universe inside a
provider's hourly budget without touching worker code.

Optional WORKER_CALC_DATE=YYYY-MM-DD pins the as-of date, for workers whose
``run()`` takes ``calc_date``. Without it, a service that builds this repo can
only ever run each worker at its own default — the root ``railway.toml``
replaces the whole ``[deploy]`` block, so a per-service ``startCommand`` (and
therefore ``python -m src.run <worker> --calc-date ...``) is discarded. That is
why an operator could not reach a historical date: after the 2026-08-05 N-PORT
identifier repair, the weekly ``nport_lookthrough`` cron at
``calc_date = max(report_date)`` revisits only the series whose LAST report is
one of the repaired dates — 499 of 16.774 rows, 3 %. This variable is the
config-only way to aim the same entry point at a chosen date.

Exit code is the contract with the platform: 0 only when the run finished its
work. A run that stopped early on a provider budget reports ``aborted`` in its
stats and exits non-zero, so a truncated sweep is never painted green.
"""

from __future__ import annotations

import datetime as _dt
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
            "|fund_peer_groups"
            "|matview_refresh|stock_daily_returns"
            "|active_share_metrics|momentum_metrics|open_macro_v03"
            "|open_macro_v03_chain"
            "|open_macro_v04"
            "|open_macro_v03_monitor|gamma_drift|ipca_production_gate"
            "|tiingo_fund_meta|mixed_quant_publication|mixed_quant_retention"
            "|bond_live_daily|bond_reference_terms)"
        )
    mod = importlib.import_module(f"src.workers.{worker}")

    # WORKER_LIMIT caps how many units one run processes. Workers whose sweep is
    # a resumable ring (eod_prices_warmer: priority head + cursor-rotated tail)
    # use it to spread a single sweep over several crons — three runs a day at
    # different hours each stay well inside the provider's hourly budget, where
    # one unbounded run would consume most of it. Config-only: the batching needs
    # no worker change, just this cap plus a multi-hour cron.
    kwargs: dict[str, int | str] = {}
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

    # WORKER_CALC_DATE pins the as-of date the same way --calc-date does on
    # ``python -m src.run``, which no Railway service that builds this repo can
    # reach (see the module docstring). The date is passed through as the ISO
    # string the workers already parse themselves, so this adds no second
    # interpretation of what the date means.
    raw_calc_date = os.getenv("WORKER_CALC_DATE", "").strip()
    if raw_calc_date:
        try:
            parsed = _dt.date.fromisoformat(raw_calc_date)
        except ValueError:
            parsed = None
        # ``date.fromisoformat`` also accepts ISO forms the workers do not
        # (``20250131``, ``2025-W05-5``); requiring the round trip keeps the
        # accepted spelling identical to the one ``characteristics`` parses with
        # ``strptime("%Y-%m-%d")``, so a value that passes here cannot fail
        # three layers down.
        if parsed is None or parsed.isoformat() != raw_calc_date:
            sys.exit(f"WORKER_CALC_DATE={raw_calc_date!r} is not a YYYY-MM-DD date")
        if "calc_date" not in inspect.signature(mod.run).parameters:
            # Same failure shape as a silently dropped WORKER_LIMIT: the config
            # would claim a historical run while the worker swept its own
            # default date, and the operator would read the wrong verdict off a
            # green deploy.
            sys.exit(f"WORKER_CALC_DATE is set but {worker}.run() takes no 'calc_date'")
        kwargs["calc_date"] = raw_calc_date

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

"""Rebuild and certify the IPCA factor surface in dependency order."""

from __future__ import annotations

import importlib
import json
import math
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from src.db import LOCK_IPCA_FACTOR_PACK, advisory_lock, connect, resolve_dsn

STEPS = (
    "factor_model",
    "gamma_drift",
    "fund_factors",
    "ipca_production_gate",
)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _emit(worker: str, result: dict[str, Any]) -> None:
    print(json.dumps({"worker": worker, **result}, default=str), flush=True)


def _require_complete(worker: str, result: dict[str, Any]) -> None:
    if worker != "fund_factors" and result.get("status") != "succeeded":
        raise RuntimeError(f"IPCA rebuild incomplete at {worker}: {result}")
    deferred_refresh = result.get("mv_refresh_reason") == "deferred_until_activation"
    if result.get("skipped") or (
        result.get("mv_refreshed") is False and not deferred_refresh
    ):
        raise RuntimeError(f"IPCA rebuild incomplete at {worker}: {result}")


def run_pack(
    dsn: str,
    *,
    include_characteristics: bool | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    include_upstream = (
        _env_bool("IPCA_RUN_CHARACTERISTICS")
        if include_characteristics is None
        else include_characteristics
    )
    if include_upstream:
        characteristics_floor = started_at
    else:
        max_age_minutes = int(
            os.getenv("IPCA_MAX_UPSTREAM_CHARACTERISTICS_AGE_MINUTES", "180")
        )
        if max_age_minutes <= 0:
            raise ValueError(
                "IPCA_MAX_UPSTREAM_CHARACTERISTICS_AGE_MINUTES must be positive"
            )
        characteristics_floor = started_at - timedelta(minutes=max_age_minutes)
    with connect(dsn) as lock_conn:
        with advisory_lock(lock_conn, LOCK_IPCA_FACTOR_PACK) as acquired:
            if not acquired:
                raise RuntimeError("IPCA factor pack rebuild already running")
            return _run_steps(
                dsn,
                characteristics_floor=characteristics_floor,
                include_characteristics=include_upstream,
            )


def _run_steps(
    dsn: str,
    *,
    characteristics_floor: datetime,
    include_characteristics: bool,
) -> dict[str, Any]:
    results: dict[str, dict[str, Any]] = {}
    fit_id: str | None = None
    steps = (("characteristics",) + STEPS) if include_characteristics else STEPS

    for worker in steps:
        module = importlib.import_module(f"src.workers.{worker}")
        if worker == "factor_model":
            result = module.run(dsn, production_fit=False) or {}
        elif worker == "gamma_drift":
            result = module.run(dsn, target_fit_id=fit_id) or {}
        elif worker == "fund_factors":
            result = module.run(dsn, fit_id=fit_id, refresh_mv=False) or {}
        elif worker == "ipca_production_gate":
            result = module.run(
                dsn,
                expected_fit_id=fit_id,
                activate=True,
                min_characteristics_computed_at=characteristics_floor,
            ) or {}
        else:
            result = module.run(dsn) or {}
        if not isinstance(result, dict):
            raise TypeError(
                f"{worker}.run() returned {type(result).__name__}, expected dict"
            )
        _emit(worker, result)
        _require_complete(worker, result)

        if worker == "characteristics":
            if result.get("equity_upserted", 0) <= 0:
                raise RuntimeError(
                    f"characteristics did not rebuild the equity layer: {result}"
                )
        elif worker == "factor_model":
            fit_id = result.get("fit_id")
            oos = result.get("oos_r_squared")
            if (
                not fit_id
                or not result.get("universe_hash")
                or result.get("k_factors") != 6
                or result.get("converged") is not True
                or result.get("degraded") is not False
                or oos is None
                or not math.isfinite(float(oos))
                or float(oos) <= 0
            ):
                raise RuntimeError(f"factor_model did not produce a certified K=6 fit: {result}")
        elif worker == "gamma_drift":
            if (
                result.get("monitored", 0) < 1
                or result.get("alerts") != 0
                or result.get("target_fit_id") != fit_id
            ):
                raise RuntimeError(f"gamma_drift did not certify the new fit: {result}")
        elif worker == "fund_factors":
            if (
                result.get("fit_id") != fit_id
                or result.get("k_factors") != 6
                or result.get("processed", 0) <= 0
                or result.get("upserted") != result.get("processed", 0) * 6
                or result.get("mv_refresh_reason") != "deferred_until_activation"
            ):
                raise RuntimeError(f"fund_factors did not publish the new fit: {result}")
        elif worker == "ipca_production_gate" and result.get("activated") is not True:
            raise RuntimeError(f"IPCA production gate did not activate the fit: {result}")
        results[worker] = result

    return {
        "status": "succeeded",
        "fit_id": fit_id,
        "steps": len(steps),
        "quality_warnings": results["ipca_production_gate"].get(
            "quality_warnings", []
        ),
    }


def main() -> None:
    summary = run_pack(resolve_dsn())
    print(json.dumps(summary, default=str), flush=True)


if __name__ == "__main__":
    main()

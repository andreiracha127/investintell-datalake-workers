"""Read-only calibration harness for A3/A4 candidate work.

The runtime workers keep publishing ``macro_quadrant_us_v1`` and
``market_implied_quadrant_v0``.  This module is deliberately separate: it reads
the vintage store, replays the A3 baseline candidate in memory, and writes
artifact files only.  It never inserts into ``regime_quadrant_snapshot``.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import functools
import hashlib
import importlib.metadata
import json
import math
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from src.db import connect, resolve_dsn
from src.quadrant_confidence import freshness_value
from src.quadrant_staleness import source_deadlines

UTC = dt.timezone.utc

MACRO_MODEL_VERSION = "macro_quadrant_us_v2_candidate"
MACRO_CONFIG_ID = "macro_8_family_map_v01"
POLICY_VERSION = "regime_policy_v0_1_candidate"
GATE_VERSION = "gate_v0_1_candidate"
CONFIDENCE_MODEL_VERSION = "confidence_operational_v0_1_candidate"
RUN_CLASSIFICATION_FAILED = "diagnostic_baseline_failed"
RUN_CLASSIFICATION_PASSED = "diagnostic_baseline_passed"

MIN_MONTHLY_OBS = 60
SERIES_Z_CLIP = 3.0
FAMILY_SCORE_CLIP = 2.5
AXIS_SCORE_CLIP = 2.0
U_FLOOR = 0.65
MIN_CONFIDENCE = 0.70
GROWTH_ENTER = 0.35
INFLATION_ENTER = 0.40
AXIS_EXIT = 0.15
DISPERSION_ABSTAIN = 1.25
MIN_VALID_RATE_FREEZE = 0.75
MAX_VALID_RATE_FREEZE = 0.90
MIN_ABSTAIN_RATE_FREEZE = 0.10
MAX_ABSTAIN_RATE_FREEZE = 0.25
MAX_REVISION_CHANGE_RATE_FREEZE = 0.10
INPUT_CACHE_SCHEMA_VERSION = 1
DEFAULT_INPUT_CACHE_DIR = "_tmp_calibration_input_cache"
L1_SCHEMA_VERSION = 1
L2_SCHEMA_VERSION = 1
L3_SCORER_SCHEMA_VERSION = 1
L3_SCORER_CODE_VERSION = "a3_l3_score_panel_v1"
L4_STATE_SCHEMA_VERSION = 1
L4_STATE_CODE_VERSION = "a3_l4_state_machine_v0"
SMOKE_GRID_SCHEMA_VERSION = 1
A31_GRID_SCHEMA_VERSION = 1

Quadrant = Literal["recovery", "expansion", "slowdown", "contraction"]
Status = Literal["valid", "abstain", "unavailable", "invalid"]
MarketSource = Literal["none", "snapshot", "db_cagg", "tiingo"]


@dataclass(frozen=True)
class SeriesConfig:
    series_id: str
    axis: Literal["growth", "inflation"]
    family: str
    transform_class: Literal["quantity_index", "price_index", "rate_level"]
    direction: Literal[-1, 1] = 1
    weight_in_family: float = 1.0


@dataclass(frozen=True)
class VintageRow:
    series_id: str
    observation_period: dt.date
    vintage_date: dt.date
    value: float
    available_at: dt.datetime
    revision_number: int
    source_spec_version: str


@dataclass(frozen=True)
class SeriesScore:
    score: float | None
    observation_period: dt.date | None
    vintage_date: dt.date | None
    available_at: dt.datetime | None
    revision_number: int | None
    freshness: float
    vintage_quality: float
    reason: str | None = None


@dataclass(frozen=True)
class AxisState:
    internal_sign: int | None
    effective_sign: int | None
    reason: str | None


@dataclass(frozen=True)
class HarnessConfig:
    start_date: dt.date
    end_date: dt.date
    output_dir: Path
    data_snapshot_id: str
    backend_commit: str
    worker_commit: str
    random_seed: int = 0
    decision_calendar: str = "business_days"
    macro_config: str = MACRO_CONFIG_ID
    policy_config: str = POLICY_VERSION
    market_source: MarketSource = "db_cagg"
    qa_repeat_read: bool = True
    input_cache_dir: Path | None = None
    input_cache_key: str | None = None
    refresh_input_cache: bool = False
    offline: bool = False


@dataclass(frozen=True)
class HarnessInputs:
    vintage_rows: list[VintageRow]
    repeat_vintage_rows: list[VintageRow] | None
    market_levels: dict[str, dict[dt.date, float]] | None
    cache_metadata: dict[str, Any]


@dataclass(frozen=True)
class A31Config:
    name: str
    transformation_weights: dict[str, dict[str, float]]
    family_weights: dict[str, dict[str, float]]
    series_weights: dict[str, float]
    aggregation_method: str
    robust_clip: float
    reliability_weighting: str
    score_clip: dict[str, float]
    release_smoothing: str = "none"


@dataclass(frozen=True)
class A32Config:
    name: str
    growth_score_scale: float
    inflation_score_scale: float
    growth_enter: float
    growth_exit: float
    inflation_enter: float
    inflation_exit: float
    u_floor: float
    min_confidence: float
    dispersion_limit: float
    coverage_rules_version: str


@dataclass(frozen=True)
class A31GridConfig:
    feature_manifest: Path
    config_catalog: Path
    output_dir: Path | None = None
    jobs: int = 1
    resume: bool = False
    offline: bool = False
    worker_commit: str | None = None


BASELINE_SERIES: tuple[SeriesConfig, ...] = (
    SeriesConfig("ACOGNO", "growth", "real_activity", "quantity_index"),
    SeriesConfig("INDPRO", "growth", "real_activity", "quantity_index"),
    SeriesConfig("PCEC96", "growth", "real_activity", "quantity_index"),
    SeriesConfig("PAYEMS", "growth", "labor", "quantity_index"),
    SeriesConfig("CPILFESL", "inflation", "consumer_prices", "price_index"),
    SeriesConfig("PPIFIS", "inflation", "producer_prices", "price_index"),
    SeriesConfig("AHETPI", "inflation", "wages", "price_index"),
    SeriesConfig("MICH", "inflation", "expectations", "rate_level"),
)

FAMILY_WEIGHTS: dict[str, dict[str, float]] = {
    "growth": {"real_activity": 0.75, "labor": 0.25},
    "inflation": {
        "consumer_prices": 0.4117647059,
        "producer_prices": 0.2352941176,
        "wages": 0.2352941176,
        "expectations": 0.1176470588,
    },
}

MIN_VALID_FAMILIES = {"growth": 2, "inflation": 3}
ANCHOR_FAMILIES = {
    "growth": {"real_activity", "labor"},
    "inflation": {"consumer_prices"},
}

_QUADRANT_BY_SIGNS: dict[tuple[int, int], Quadrant] = {
    (1, -1): "recovery",
    (1, 1): "expansion",
    (-1, 1): "slowdown",
    (-1, -1): "contraction",
}


def business_days(start: dt.date, end: dt.date) -> list[dt.date]:
    if end < start:
        raise ValueError("end_date must be >= start_date")
    days: list[dt.date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += dt.timedelta(days=1)
    return days


def decision_time(day: dt.date) -> dt.datetime:
    """End-of-day UTC decision cut, so same-day ALFRED vintages are knowable."""
    return dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=UTC)


def read_vintage_rows(conn, *, max_available_at: dt.datetime) -> list[VintageRow]:
    series_ids = [s.series_id for s in BASELINE_SERIES]
    sql = (
        "SELECT series_id, observation_period, vintage_date, value, available_at, "
        "       revision_number, source_spec_version "
        "FROM macro_observation_vintage "
        "WHERE series_id = ANY(%s) AND available_at <= %s "
        "ORDER BY series_id, observation_period, available_at, vintage_date"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (series_ids, max_available_at))
        rows = cur.fetchall()
    return [
        VintageRow(
            series_id=str(r[0]),
            observation_period=r[1],
            vintage_date=r[2],
            value=float(r[3]),
            available_at=_as_utc(r[4]),
            revision_number=int(r[5]),
            source_spec_version=str(r[6]),
        )
        for r in rows
    ]


def input_cache_request_key(config: HarnessConfig, max_available_at: dt.datetime) -> str:
    return stable_hash({
        "schema_version": INPUT_CACHE_SCHEMA_VERSION,
        "data_snapshot_id": config.data_snapshot_id,
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "max_available_at": max_available_at.isoformat(),
        "series_ids": sorted(s.series_id for s in BASELINE_SERIES),
        "market_tickers": market_cache_tickers(config.market_source),
        "source_tables": input_cache_source_tables(config.market_source),
        "market_source": config.market_source,
    })[:24]


def input_cache_key(
    config: HarnessConfig,
    max_available_at: dt.datetime,
    source_data_hashes: dict[str, str],
) -> str:
    return stable_hash({
        "request_key": input_cache_request_key(config, max_available_at),
        "source_data_hashes": source_data_hashes,
        "input_cache_schema_version": INPUT_CACHE_SCHEMA_VERSION,
    })[:24]


def market_cache_tickers(market_source: MarketSource) -> list[str]:
    return ["SPY", "IEF", "TIP"] if market_source in {"db_cagg", "tiingo"} else []


def load_or_create_harness_inputs(
    conn,
    config: HarnessConfig,
    *,
    max_available_at: dt.datetime,
) -> HarnessInputs:
    if config.input_cache_dir is None:
        rows = read_vintage_rows(conn, max_available_at=max_available_at)
        repeat_rows = (
            read_vintage_rows(conn, max_available_at=max_available_at)
            if config.qa_repeat_read else None
        )
        return HarnessInputs(
            vintage_rows=rows,
            repeat_vintage_rows=repeat_rows,
            market_levels=None,
            cache_metadata={"enabled": False},
        )

    request_key = input_cache_request_key(config, max_available_at)
    index_path = config.input_cache_dir / "input_cache_index.json"
    cache_index = read_input_cache_index(index_path)
    cache_key = config.input_cache_key or cache_index.get(request_key) or request_key
    cache_dir = config.input_cache_dir / cache_key
    macro_path = cache_dir / "macro_vintages.parquet"
    market_levels_path = cache_dir / "market_cagg_levels.parquet"
    manifest_path = cache_dir / "input_cache_manifest.json"
    required_paths = [macro_path]
    if config.market_source == "db_cagg":
        required_paths.append(market_levels_path)

    cache_ready = (
        not config.refresh_input_cache
        and manifest_path.exists()
        and all(path.exists() for path in required_paths)
    )
    if cache_ready:
        rows = read_vintage_cache(macro_path)
        repeat_rows = read_vintage_cache(macro_path) if config.qa_repeat_read else None
        market_levels = (
            read_market_levels_cache(market_levels_path)
            if config.market_source == "db_cagg" else None
        )
        metadata = json.loads(manifest_path.read_text(encoding="utf-8"))
        metadata.update({
            "enabled": True,
            "cache_hit": True,
            "cache_dir": str(cache_dir),
            "request_key": request_key,
        })
        return HarnessInputs(rows, repeat_rows, market_levels, metadata)

    if conn is None:
        raise RuntimeError("input cache miss requires a database connection")

    cache_dir.mkdir(parents=True, exist_ok=True)
    rows = read_vintage_rows(conn, max_available_at=max_available_at)

    market_levels = None
    if config.market_source == "db_cagg":
        market_levels = load_market_levels_from_cagg_nav_daily(
            conn, config.start_date, config.end_date)

    source_data_hashes = {"macro_vintages": rows_hash(rows)}
    if market_levels is not None:
        source_data_hashes["market_levels"] = market_levels_hash(market_levels)
    if config.input_cache_key is None:
        cache_key = input_cache_key(config, max_available_at, source_data_hashes)
        cache_dir = config.input_cache_dir / cache_key
        macro_path = cache_dir / "macro_vintages.parquet"
        market_levels_path = cache_dir / "market_cagg_levels.parquet"
        manifest_path = cache_dir / "input_cache_manifest.json"
        cache_dir.mkdir(parents=True, exist_ok=True)

    write_vintage_cache(macro_path, rows)
    if market_levels is not None:
        write_market_levels_cache(market_levels_path, market_levels)

    files = {
        "macro_vintages.parquet": hash_file(macro_path),
    }
    if config.market_source == "db_cagg":
        files["market_cagg_levels.parquet"] = hash_file(market_levels_path)

    metadata = {
        "enabled": True,
        "cache_hit": False,
        "cache_schema_version": INPUT_CACHE_SCHEMA_VERSION,
        "cache_key": cache_key,
        "request_key": request_key,
        "cache_dir": str(cache_dir),
        "created_at": dt.datetime.now(UTC).isoformat(),
        "data_snapshot_id": config.data_snapshot_id,
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "max_available_at": max_available_at.isoformat(),
        "macro_config": config.macro_config,
        "market_source": config.market_source,
        "macro_vintage_rows": len(rows),
        "market_level_rows": (
            sum(len(day_values) for day_values in market_levels.values())
            if market_levels else 0
        ),
        "source_tables": input_cache_source_tables(config.market_source),
        "source_data_hashes": source_data_hashes,
        "files": files,
    }
    write_json(manifest_path, metadata)
    if config.input_cache_key is None:
        cache_index[request_key] = cache_key
        write_json(index_path, cache_index)

    cached_rows = read_vintage_cache(macro_path)
    repeat_rows = read_vintage_cache(macro_path) if config.qa_repeat_read else None
    cached_market_levels = (
        read_market_levels_cache(market_levels_path)
        if config.market_source == "db_cagg" else None
    )
    return HarnessInputs(cached_rows, repeat_rows, cached_market_levels, metadata)


def input_cache_source_tables(market_source: MarketSource) -> list[str]:
    tables = ["macro_observation_vintage"]
    if market_source == "db_cagg":
        tables.extend(["instruments_universe", "cagg_nav_daily"])
    elif market_source == "snapshot":
        tables.append("regime_quadrant_snapshot")
    return tables


def read_input_cache_index(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(k): str(v) for k, v in payload.items()}


def market_levels_hash(levels: dict[str, dict[dt.date, float]]) -> str:
    return stable_hash([
        {
            "ticker": ticker,
            "date": day.isoformat(),
            "level": round(value, 12),
        }
        for ticker in sorted(levels)
        for day, value in sorted(levels[ticker].items())
    ])


def write_vintage_cache(path: Path, rows: list[VintageRow]) -> None:
    write_parquet(path, [{
        "series_id": row.series_id,
        "observation_period": row.observation_period.isoformat(),
        "vintage_date": row.vintage_date.isoformat(),
        "value": row.value,
        "available_at": row.available_at.isoformat(),
        "revision_number": row.revision_number,
        "source_spec_version": row.source_spec_version,
    } for row in rows])


def read_vintage_cache(path: Path) -> list[VintageRow]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required to read calibration input cache") from exc
    frame = pd.read_parquet(path)
    rows: list[VintageRow] = []
    for record in frame.to_dict("records"):
        rows.append(VintageRow(
            series_id=str(record["series_id"]),
            observation_period=cache_date(record["observation_period"]),
            vintage_date=cache_date(record["vintage_date"]),
            value=float(record["value"]),
            available_at=cache_datetime(record["available_at"]),
            revision_number=int(record["revision_number"]),
            source_spec_version=str(record["source_spec_version"]),
        ))
    return rows


def write_market_levels_cache(path: Path, levels: dict[str, dict[dt.date, float]]) -> None:
    write_parquet(path, [
        {
            "ticker": ticker,
            "date": day.isoformat(),
            "level": value,
        }
        for ticker in sorted(levels)
        for day, value in sorted(levels[ticker].items())
    ])


def read_market_levels_cache(path: Path) -> dict[str, dict[dt.date, float]]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required to read calibration input cache") from exc
    frame = pd.read_parquet(path)
    levels: dict[str, dict[dt.date, float]] = {}
    for record in frame.to_dict("records"):
        ticker = str(record["ticker"]).upper()
        levels.setdefault(ticker, {})[cache_date(record["date"])] = float(record["level"])
    return levels


def cache_date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime().date()
    return dt.date.fromisoformat(str(value)[:10])


def cache_datetime(value: Any) -> dt.datetime:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, dt.datetime):
        return _as_utc(value)
    text = str(value).replace("Z", "+00:00")
    return _as_utc(dt.datetime.fromisoformat(text))


def read_market_snapshot_rows(
    conn, start_date: dt.date, end_date: dt.date
) -> list[dict[str, Any]]:
    sql = (
        "SELECT as_of, status_at_compute, quadrant, candidate_quadrant, "
        "       candidate_confidence, growth_sign, inflation_sign, "
        "       growth_score, inflation_score, model_version "
        "FROM regime_quadrant_snapshot "
        "WHERE model_version = 'market_implied_quadrant_v0' "
        "  AND as_of BETWEEN %s AND %s "
        "ORDER BY as_of"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (start_date, end_date))
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "date": r[0].isoformat(),
            "status": "valid" if r[1] == "valid" else "abstain",
            "quadrant": r[2],
            "candidate_quadrant": r[3],
            "candidate_confidence": _float_or_none(r[4]),
            "growth_sign": r[5],
            "inflation_sign": r[6],
            "growth_score": _float_or_none(r[7]),
            "inflation_score": _float_or_none(r[8]),
            "model_version": r[9],
        })
    return out


def group_rows(rows: list[VintageRow]) -> dict[str, list[VintageRow]]:
    grouped: dict[str, list[VintageRow]] = {}
    for row in rows:
        grouped.setdefault(row.series_id, []).append(row)
    for sid in grouped:
        grouped[sid].sort(key=lambda r: (r.available_at, r.observation_period, r.vintage_date))
    return grouped


def select_rows_as_of(
    grouped: dict[str, list[VintageRow]],
    cut: dt.datetime,
    *,
    mode: Literal["latest", "first"] = "latest",
) -> dict[str, dict[dt.date, VintageRow]]:
    selected: dict[str, dict[dt.date, VintageRow]] = {}
    decision_day = cut.date()
    for sid, rows in grouped.items():
        by_period: dict[dt.date, VintageRow] = {}
        for row in rows:
            if row.available_at > cut:
                break
            if row.observation_period > decision_day:
                continue
            prev = by_period.get(row.observation_period)
            if prev is None:
                by_period[row.observation_period] = row
                continue
            if mode == "latest":
                if (row.available_at, row.vintage_date) > (prev.available_at, prev.vintage_date):
                    by_period[row.observation_period] = row
            else:
                if (row.available_at, row.vintage_date) < (prev.available_at, prev.vintage_date):
                    by_period[row.observation_period] = row
        selected[sid] = by_period
    return selected


def selected_hash(selected: dict[str, dict[dt.date, VintageRow]]) -> str:
    h = hashlib.sha256()
    for sid in sorted(selected):
        for period in sorted(selected[sid]):
            row = selected[sid][period]
            h.update(
                "|".join([
                    sid,
                    period.isoformat(),
                    row.vintage_date.isoformat(),
                    row.available_at.isoformat(),
                    str(row.revision_number),
                    f"{row.value:.12g}",
                ]).encode("utf-8")
            )
            h.update(b"\n")
    return h.hexdigest()


def rows_hash(rows: list[VintageRow]) -> str:
    h = hashlib.sha256()
    for row in sorted(rows, key=lambda r: (
        r.series_id, r.observation_period, r.vintage_date, r.available_at
    )):
        h.update(json.dumps({
            "series_id": row.series_id,
            "observation_period": row.observation_period.isoformat(),
            "vintage_date": row.vintage_date.isoformat(),
            "available_at": row.available_at.isoformat(),
            "revision_number": row.revision_number,
            "value": round(row.value, 10),
            "source_spec_version": row.source_spec_version,
        }, sort_keys=True).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def compute_pit_qa(
    rows: list[VintageRow],
    grouped: dict[str, list[VintageRow]],
    *,
    calendar: list[dt.date],
    repeat_rows: list[VintageRow] | None = None,
) -> dict[str, Any]:
    keys: set[tuple[str, dt.date, dt.date]] = set()
    duplicate_keys = 0
    future_available = 0
    for row in rows:
        key = (row.series_id, row.observation_period, row.vintage_date)
        if key in keys:
            duplicate_keys += 1
        keys.add(key)
        if row.available_at.date() < row.vintage_date:
            future_available += 1

    data_hash = rows_hash(rows)
    repeat_hash = rows_hash(repeat_rows) if repeat_rows is not None else None
    selected_future_observations = 0
    leak_probe_passed = True
    if calendar:
        probe_day = calendar[min(len(calendar) - 1, len(calendar) // 2)]
        cut = decision_time(probe_day)
        selected = select_rows_as_of(grouped, cut)
        before_hash = selected_hash(selected)
        injected = dict(grouped)
        sid = BASELINE_SERIES[0].series_id
        injected_rows = list(injected.get(sid, []))
        injected_rows.append(VintageRow(
            series_id=sid,
            observation_period=probe_day,
            vintage_date=probe_day + dt.timedelta(days=30),
            value=999999.0,
            available_at=cut + dt.timedelta(days=30),
            revision_number=99,
            source_spec_version="synthetic_future_revision",
        ))
        injected[sid] = injected_rows
        after_hash = selected_hash(select_rows_as_of(injected, cut))
        leak_probe_passed = before_hash == after_hash
        selected_future_observations = sum(
            1
            for per_series in selected.values()
            for period in per_series
            if period > probe_day
        )

    spot_checks = []
    for sid in sorted(grouped):
        series_rows = grouped[sid]
        if not series_rows:
            continue
        first = min(series_rows, key=lambda r: (r.available_at, r.observation_period))
        last = max(series_rows, key=lambda r: (r.available_at, r.observation_period))
        revisions = [r for r in series_rows if r.revision_number > 0]
        mid = revisions[len(revisions) // 2] if revisions else None
        spot_checks.append(_spot("earliest_available_vintage_in_store", first))
        if mid is not None:
            spot_checks.append(_spot("intermediate_revision", mid))
        spot_checks.append(_spot("latest_vintage", last))

    coverage_by_series = {
        sid: {
            "rows": len(series_rows),
            "observation_min": min((r.observation_period for r in series_rows), default=None),
            "observation_max": max((r.observation_period for r in series_rows), default=None),
            "vintage_min": min((r.vintage_date for r in series_rows), default=None),
            "vintage_max": max((r.vintage_date for r in series_rows), default=None),
        }
        for sid, series_rows in sorted(grouped.items())
    }
    for item in coverage_by_series.values():
        for key in ("observation_min", "observation_max", "vintage_min", "vintage_max"):
            if item[key] is not None:
                item[key] = item[key].isoformat()

    return {
        "row_count": len(rows),
        "unique_key_duplicate_count": duplicate_keys,
        "available_before_vintage_count": future_available,
        "data_hash": data_hash,
        "repeat_read_hash": repeat_hash,
        "idempotent_repeat_read": (repeat_hash == data_hash) if repeat_hash else None,
        "selected_future_observation_count": selected_future_observations,
        "synthetic_future_revision_no_effect": leak_probe_passed,
        "spot_checks": spot_checks,
        "coverage_by_series": coverage_by_series,
    }


def build_pit_selection_panel(
    grouped: dict[str, list[VintageRow]],
    calendar: list[dt.date],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    configs = {cfg.series_id: cfg for cfg in BASELINE_SERIES}
    for day in calendar:
        cut = decision_time(day)
        latest = select_rows_as_of(grouped, cut, mode="latest")
        first_release = select_rows_as_of(grouped, cut, mode="first")
        for sid, cfg in configs.items():
            latest_row = latest_selected_row(latest.get(sid, {}))
            first_row = latest_selected_row(first_release.get(sid, {}))
            rows.append({
                "business_date": day.isoformat(),
                "selection_mode": "latest",
                "selection_role": selection_role_for_mode("latest"),
                "series_id": sid,
                "axis_id": cfg.axis,
                "family_id": cfg.family,
                "selected_observation_period": date_or_none(latest_row.observation_period if latest_row else None),
                "selected_vintage_date": date_or_none(latest_row.vintage_date if latest_row else None),
                "available_at": datetime_or_none(latest_row.available_at if latest_row else None),
                "raw_value": latest_row.value if latest_row else None,
                "revision_number": latest_row.revision_number if latest_row else None,
                "age_days": (cut - latest_row.available_at).days if latest_row else None,
                "freshness_ratio": (
                    series_freshness(cut, latest_row.available_at) if latest_row else 0.0
                ),
                "coverage_flag": latest_row is not None,
                "critical_family_flag": (
                    latest_row is not None and cfg.family in ANCHOR_FAMILIES[cfg.axis]
                ),
                "first_release_observation_period": date_or_none(
                    first_row.observation_period if first_row else None
                ),
                "first_release_vintage_date": date_or_none(
                    first_row.vintage_date if first_row else None
                ),
                "first_release_available_at": datetime_or_none(
                    first_row.available_at if first_row else None
                ),
                "first_release_raw_value": first_row.value if first_row else None,
                "first_release_revision_number": (
                    first_row.revision_number if first_row else None
                ),
                "counterfactual_selection_mode": "first_release",
                "counterfactual_selection_role": selection_role_for_mode("first_release"),
                "counterfactual_only": True,
            })
    return rows


def build_macro_feature_primitives(
    grouped: dict[str, list[VintageRow]],
    calendar: list[dt.date],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    configs = {cfg.series_id: cfg for cfg in BASELINE_SERIES}
    for day in calendar:
        cut = decision_time(day)
        selections = {
            "latest": select_rows_as_of(grouped, cut, mode="latest"),
            "first_release": select_rows_as_of(grouped, cut, mode="first"),
        }
        for selection_mode, selected in selections.items():
            for sid, cfg in configs.items():
                rows_by_period = selected.get(sid, {})
                rows.append(macro_primitive_row(cfg, rows_by_period, day, cut, selection_mode))
    return rows


def macro_primitive_row(
    cfg: SeriesConfig,
    rows_by_period: dict[dt.date, VintageRow],
    day: dt.date,
    cut: dt.datetime,
    selection_mode: str,
) -> dict[str, Any]:
    series = {period: row.value for period, row in rows_by_period.items()}
    periods = sorted(series)
    latest_period = max(periods, default=None)
    latest_row = rows_by_period.get(latest_period) if latest_period else None
    primitives: dict[str, Any] = {
        "business_date": day.isoformat(),
        "selection_mode": selection_mode,
        "selection_role": selection_role_for_mode(selection_mode),
        "counterfactual_only": selection_role_for_mode(selection_mode) != "pit_runtime_candidate",
        "series_id": cfg.series_id,
        "axis_id": cfg.axis,
        "family_id": cfg.family,
        "transform_class": cfg.transform_class,
        "observation_period": date_or_none(latest_period),
        "vintage_date": date_or_none(latest_row.vintage_date if latest_row else None),
        "available_at": datetime_or_none(latest_row.available_at if latest_row else None),
        "raw_value": latest_row.value if latest_row else None,
        "revision_number": latest_row.revision_number if latest_row else None,
        "freshness": series_freshness(cut, latest_row.available_at) if latest_row else 0.0,
        "vintage_quality": (
            vintage_quality(latest_row.revision_number) if latest_row else 0.0
        ),
        "coverage": 1.0 if latest_row else 0.0,
        "reference_series_score": None,
        "reference_transform_reason": "no_pit_value" if latest_row is None else None,
    }
    if latest_period is None:
        return primitives

    idx = periods.index(latest_period)
    current = series[latest_period]
    p3 = shift_months(periods, idx, 3)
    p6 = shift_months(periods, idx, 6)
    p12 = shift_months(periods, idx, 12)
    primitives.update({
        "change_3m_annualized": log_change(series, latest_period, p3, annualizer=4.0),
        "change_6m_annualized": log_change(series, latest_period, p6, annualizer=2.0),
        "change_12m": log_change(series, latest_period, p12, annualizer=1.0),
        "delta_3m": arithmetic_delta(series, latest_period, p3),
        "delta_6m": arithmetic_delta(series, latest_period, p6),
        "delta_12m": arithmetic_delta(series, latest_period, p12),
    })
    primitives["acceleration_3m_vs_12m"] = numeric_delta(
        primitives.get("change_3m_annualized"), primitives.get("change_12m")
    )
    primitives["acceleration_6m_vs_12m"] = numeric_delta(
        primitives.get("change_6m_annualized"), primitives.get("change_12m")
    )
    primitives.update({
        f"z_{name}": value
        for name, value in series_component_z_values(cfg.transform_class, series).items()
    })
    observed_values = [series[p] for p in periods]
    median = statistics.median(observed_values)
    mad = statistics.median([abs(value - median) for value in observed_values])
    primitives.update({
        "expanding_median": median,
        "expanding_mad": mad,
        "level_minus_expanding_median": current - median,
        "level_robust_z": ((current - median) / (1.4826 * mad)) if mad > 0 else None,
        "diffusion_offset": sign_of(current - median),
    })
    reference_score = reference_series_score(cfg, series)
    primitives.update({
        "reference_series_score": reference_score,
        "reference_transform_reason": (
            None if reference_score is not None else "insufficient_transform_history"
        ),
    })
    return primitives


def series_component_z_values(transform_class: str, series: dict[dt.date, float]) -> dict[str, float | None]:
    key = tuple((period.isoformat(), round(float(value), 10)) for period, value in sorted(series.items()))
    return dict(_series_component_z_values_cached(transform_class, key))


@functools.lru_cache(maxsize=20000)
def _series_component_z_values_cached(
    transform_class: str, series_items: tuple[tuple[str, float], ...]
) -> tuple[tuple[str, float | None], ...]:
    series = {dt.date.fromisoformat(period): value for period, value in series_items}
    return tuple(series_component_z_values_uncached(transform_class, series).items())


def series_component_z_values_uncached(
    transform_class: str, series: dict[dt.date, float]
) -> dict[str, float | None]:
    periods = sorted(series)
    if transform_class in {"quantity_index", "price_index"}:
        c3: dict[dt.date, float] = {}
        c6: dict[dt.date, float] = {}
        c12: dict[dt.date, float] = {}
        for i, period in enumerate(periods):
            p3 = shift_months(periods, i, 3)
            p6 = shift_months(periods, i, 6)
            p12 = shift_months(periods, i, 12)
            current = series[period]
            if current <= 0:
                continue
            if p3 is not None and series[p3] > 0:
                c3[period] = 4.0 * (math.log(current) - math.log(series[p3]))
            if p6 is not None and series[p6] > 0:
                c6[period] = 2.0 * (math.log(current) - math.log(series[p6]))
            if p12 is not None and series[p12] > 0:
                c12[period] = math.log(current) - math.log(series[p12])
        a3 = {p: c3[p] - c12[p] for p in c3.keys() & c12.keys()}
        a6 = {p: c6[p] - c12[p] for p in c6.keys() & c12.keys()}
        return {
            "acceleration_3m": latest_component_z(a3),
            "acceleration_6m": latest_component_z(a6),
            "change_12m": latest_component_z(c12),
            "level": None,
            "delta_3m": None,
        }
    delta3: dict[dt.date, float] = {}
    for i, period in enumerate(periods):
        p3 = shift_months(periods, i, 3)
        if p3 is not None:
            delta3[period] = series[period] - series[p3]
    return {
        "acceleration_3m": None,
        "acceleration_6m": None,
        "change_12m": None,
        "level": latest_component_z(series),
        "delta_3m": latest_component_z(delta3),
    }


def selection_role_for_mode(selection_mode: str) -> str:
    if selection_mode == "latest":
        return "pit_runtime_candidate"
    return "revised_vintage_counterfactual"


def reference_series_score(cfg: SeriesConfig, series: dict[dt.date, float]) -> float | None:
    if cfg.transform_class == "quantity_index":
        value = quantity_index_score(series)
    elif cfg.transform_class == "price_index":
        value = price_index_score(series)
    else:
        value = rate_level_score(series)
    return value * cfg.direction if value is not None else None


def build_market_feature_primitives(
    levels: dict[str, dict[dt.date, float]] | None,
    start_date: dt.date,
    end_date: dt.date,
    *,
    price_source: str,
) -> list[dict[str, Any]]:
    if levels is None:
        return []
    from src.workers.quadrant_market import WINDOW

    spy_by = levels.get("SPY", {})
    ief_by = levels.get("IEF", {})
    tip_by = levels.get("TIP", {})
    price_days = sorted(spy_by)
    be_by: dict[dt.date, float] = {}
    last_ief = last_tip = None
    for day in price_days:
        last_ief = ief_by.get(day, last_ief)
        last_tip = tip_by.get(day, last_tip)
        if last_ief and last_tip:
            be_by[day] = last_tip / last_ief
    be_days_sorted = sorted(be_by)

    rows: list[dict[str, Any]] = []
    for day in business_days(start_date, end_date):
        price_history = [price_day for price_day in price_days if price_day <= day]
        be_history = [be_day for be_day in be_days_sorted if be_day <= day]
        growth_past = price_history[-WINDOW - 1] if len(price_history) > WINDOW else None
        inflation_past = be_history[-WINDOW - 1] if len(be_history) > WINDOW else None
        growth_126d_return = (
            spy_by[day] / spy_by[growth_past] - 1.0
            if day in spy_by and growth_past is not None and spy_by[growth_past] > 0
            else None
        )
        inflation_126d_return = (
            be_by[day] / be_by[inflation_past] - 1.0
            if day in be_by and inflation_past is not None and be_by[inflation_past] > 0
            else None
        )
        rows.append({
            "business_date": day.isoformat(),
            "price_source": price_source,
            "lookback_days": WINDOW,
            "trading_session_indicator": day in spy_by,
            "spy_available": day in spy_by,
            "tip_available": day in tip_by,
            "ief_available": day in ief_by,
            "breakeven_available": day in be_by,
            "spy_level": spy_by.get(day),
            "ief_level": ief_by.get(day),
            "tip_level": tip_by.get(day),
            "breakeven_proxy": be_by.get(day),
            "growth_126d_return": growth_126d_return,
            "inflation_126d_return": inflation_126d_return,
            "growth_lookback_date": date_or_none(growth_past),
            "inflation_lookback_date": date_or_none(inflation_past),
            "market_missing_input": day not in spy_by or day not in be_by,
            "market_calendar_reason": "market_closed_or_missing" if day not in spy_by else None,
        })
    return rows


def latest_selected_row(rows_by_period: dict[dt.date, VintageRow]) -> VintageRow | None:
    latest_period = max(rows_by_period, default=None)
    return rows_by_period.get(latest_period) if latest_period else None


def log_change(
    series: dict[dt.date, float],
    current_period: dt.date,
    prior_period: dt.date | None,
    *,
    annualizer: float,
) -> float | None:
    if prior_period is None:
        return None
    current = series[current_period]
    prior = series[prior_period]
    if current <= 0 or prior <= 0:
        return None
    return annualizer * (math.log(current) - math.log(prior))


def arithmetic_delta(
    series: dict[dt.date, float],
    current_period: dt.date,
    prior_period: dt.date | None,
) -> float | None:
    if prior_period is None:
        return None
    return series[current_period] - series[prior_period]


def date_or_none(value: dt.date | None) -> str | None:
    return value.isoformat() if value else None


def datetime_or_none(value: dt.datetime | None) -> str | None:
    return value.isoformat() if value else None


def replay_macro(
    rows: list[VintageRow],
    calendar: list[dt.date],
    *,
    selection_mode: Literal["latest", "first"] = "latest",
) -> list[dict[str, Any]]:
    grouped = group_rows(rows)
    prev_hash: str | None = None
    prev_record: dict[str, Any] | None = None
    growth_internal: int | None = None
    inflation_internal: int | None = None
    published_quadrant: str | None = None
    out: list[dict[str, Any]] = []

    for day in calendar:
        cut = decision_time(day)
        selected = select_rows_as_of(grouped, cut, mode=selection_mode)
        current_hash = selected_hash(selected)
        if prev_record is not None and current_hash == prev_hash:
            carried = dict(prev_record)
            carried["date"] = day.isoformat()
            carried["decision_time"] = cut.isoformat()
            carried["inputs_changed"] = False
            carried["reevaluated"] = False
            carried["selection_mode"] = selection_mode
            out.append(carried)
            prev_record = carried
            continue

        series_scores = score_all_series(selected, cut)
        axis = {
            "growth": aggregate_axis(series_scores, "growth"),
            "inflation": aggregate_axis(series_scores, "inflation"),
        }
        coverage_quality = min(axis["growth"]["coverage"], axis["inflation"]["coverage"])
        freshness_quality = min(axis["growth"]["freshness"], axis["inflation"]["freshness"])
        concordance_quality = min(axis["growth"]["concordance"], axis["inflation"]["concordance"])
        vintage_quality = min(axis["growth"]["vintage_quality"], axis["inflation"]["vintage_quality"])
        u_t = 0.35 * coverage_quality + 0.20 * freshness_quality + 0.25 * concordance_quality + 0.20 * vintage_quality

        g_score = axis["growth"]["score"]
        i_score = axis["inflation"]["score"]
        g_state = transition_axis(
            growth_internal, g_score, enter=GROWTH_ENTER, exit_=AXIS_EXIT)
        i_state = transition_axis(
            inflation_internal, i_score, enter=INFLATION_ENTER, exit_=AXIS_EXIT)
        growth_internal = g_state.internal_sign
        inflation_internal = i_state.internal_sign

        g_margin = axis_margin(g_score, GROWTH_ENTER, AXIS_EXIT)
        i_margin = axis_margin(i_score, INFLATION_ENTER, AXIS_EXIT)
        candidate_confidence = 0.60 * u_t + 0.40 * math.sqrt(g_margin * i_margin)

        candidate_quadrant = quadrant_from_scores(g_score, i_score)
        instant_quadrant = quadrant_from_signs(g_state.effective_sign, i_state.effective_sign)
        coverage_ok = coverage_quality >= 0.80
        freshness_ok = freshness_quality > 0.0
        critical_family_ok = bool(axis["growth"]["has_anchor"] and axis["inflation"]["has_anchor"])
        u_ok = u_t >= U_FLOOR
        confidence_ok = candidate_confidence >= MIN_CONFIDENCE
        dispersion_ok = (
            axis["growth"]["dispersion"] <= DISPERSION_ABSTAIN
            and axis["inflation"]["dispersion"] <= DISPERSION_ABSTAIN
        )

        status, reasons = resolve_candidate_status(
            axis=axis,
            g_state=g_state,
            i_state=i_state,
            u_t=u_t,
            candidate_confidence=candidate_confidence,
        )
        if status == "valid":
            published_quadrant = instant_quadrant

        record = {
            "date": day.isoformat(),
            "decision_time": cut.isoformat(),
            "selection_mode": selection_mode,
            "inputs_changed": True,
            "reevaluated": True,
            "source_vintage_hash": current_hash,
            "status": status,
            "status_reasons": ",".join(reasons),
            "status_reasons_all": ",".join(reasons),
            "status_reason_primary": primary_reason(reasons),
            "candidate_quadrant": candidate_quadrant,
            "instant_quadrant": instant_quadrant,
            "published_quadrant": published_quadrant,
            "candidate_confidence": candidate_confidence,
            "confidence": candidate_confidence,
            "u": u_t,
            "C": coverage_quality,
            "F": freshness_quality,
            "A": concordance_quality,
            "V": vintage_quality,
            "coverage_quality": coverage_quality,
            "freshness_quality": freshness_quality,
            "concordance_quality": concordance_quality,
            "vintage_quality": vintage_quality,
            "growth_score": g_score,
            "inflation_score": i_score,
            "growth_margin": g_margin,
            "inflation_margin": i_margin,
            "m_growth": g_margin,
            "m_inflation": i_margin,
            "growth_sign": g_state.effective_sign,
            "inflation_sign": i_state.effective_sign,
            "growth_internal_sign": growth_internal,
            "inflation_internal_sign": inflation_internal,
            "growth_axis_state": axis_state_label(g_state),
            "inflation_axis_state": axis_state_label(i_state),
            "coverage_ok": coverage_ok,
            "freshness_ok": freshness_ok,
            "critical_family_ok": critical_family_ok,
            "growth_critical_family_ok": bool(axis["growth"]["has_anchor"]),
            "inflation_critical_family_ok": bool(axis["inflation"]["has_anchor"]),
            "u_ok": u_ok,
            "confidence_ok": confidence_ok,
            "dispersion_ok": dispersion_ok,
            "growth_family_count": axis["growth"]["family_count"],
            "inflation_family_count": axis["inflation"]["family_count"],
            "growth_dispersion": axis["growth"]["dispersion"],
            "inflation_dispersion": axis["inflation"]["dispersion"],
            "model_version": MACRO_MODEL_VERSION,
            "macro_config": MACRO_CONFIG_ID,
            "confidence_model_version": CONFIDENCE_MODEL_VERSION,
        }
        for sid, score in series_scores.items():
            record[f"{sid.lower()}_score"] = score.score
            record[f"{sid.lower()}_period"] = (
                score.observation_period.isoformat() if score.observation_period else None
            )
            record[f"{sid.lower()}_vintage"] = (
                score.vintage_date.isoformat() if score.vintage_date else None
            )
        for axis_name in ("growth", "inflation"):
            for family, family_score in axis[axis_name]["family_scores"].items():
                record[f"{axis_name}_family_{family}_score"] = family_score
        out.append(record)
        prev_hash = current_hash
        prev_record = record
    return out


def score_all_series(
    selected: dict[str, dict[dt.date, VintageRow]], cut: dt.datetime
) -> dict[str, SeriesScore]:
    out: dict[str, SeriesScore] = {}
    configs = {cfg.series_id: cfg for cfg in BASELINE_SERIES}
    for sid, cfg in configs.items():
        rows_by_period = selected.get(sid, {})
        series = {period: row.value for period, row in rows_by_period.items()}
        if cfg.transform_class == "quantity_index":
            value = quantity_index_score(series)
        elif cfg.transform_class == "price_index":
            value = price_index_score(series)
        else:
            value = rate_level_score(series)
        latest_period = max(series, default=None)
        latest_row = rows_by_period.get(latest_period) if latest_period else None
        if latest_row is None:
            out[sid] = SeriesScore(
                score=None,
                observation_period=None,
                vintage_date=None,
                available_at=None,
                revision_number=None,
                freshness=0.0,
                vintage_quality=0.0,
                reason="no_pit_value",
            )
            continue
        score = value * cfg.direction if value is not None else None
        out[sid] = SeriesScore(
            score=score,
            observation_period=latest_period,
            vintage_date=latest_row.vintage_date,
            available_at=latest_row.available_at,
            revision_number=latest_row.revision_number,
            freshness=series_freshness(cut, latest_row.available_at),
            vintage_quality=vintage_quality(latest_row.revision_number),
            reason=None if score is not None else "insufficient_transform_history",
        )
    return out


def quantity_index_score(series: dict[dt.date, float]) -> float | None:
    periods = sorted(series)
    g3: dict[dt.date, float] = {}
    g6: dict[dt.date, float] = {}
    g12: dict[dt.date, float] = {}
    for i, period in enumerate(periods):
        p3 = shift_months(periods, i, 3)
        p6 = shift_months(periods, i, 6)
        p12 = shift_months(periods, i, 12)
        current = series[period]
        if current <= 0:
            continue
        if p3 is not None and series[p3] > 0:
            g3[period] = 4.0 * (math.log(current) - math.log(series[p3]))
        if p6 is not None and series[p6] > 0:
            g6[period] = 2.0 * (math.log(current) - math.log(series[p6]))
        if p12 is not None and series[p12] > 0:
            g12[period] = math.log(current) - math.log(series[p12])
    c1 = {p: g3[p] - g12[p] for p in g3.keys() & g12.keys()}
    c2 = {p: g6[p] - g12[p] for p in g6.keys() & g12.keys()}
    z1 = latest_component_z(c1)
    z2 = latest_component_z(c2)
    z3 = latest_component_z(g12)
    return weighted_components([(0.50, z1), (0.30, z2), (0.20, z3)])


def price_index_score(series: dict[dt.date, float]) -> float | None:
    periods = sorted(series)
    pi3: dict[dt.date, float] = {}
    pi6: dict[dt.date, float] = {}
    pi12: dict[dt.date, float] = {}
    for i, period in enumerate(periods):
        p3 = shift_months(periods, i, 3)
        p6 = shift_months(periods, i, 6)
        p12 = shift_months(periods, i, 12)
        current = series[period]
        if current <= 0:
            continue
        if p3 is not None and series[p3] > 0:
            pi3[period] = 4.0 * (math.log(current) - math.log(series[p3]))
        if p6 is not None and series[p6] > 0:
            pi6[period] = 2.0 * (math.log(current) - math.log(series[p6]))
        if p12 is not None and series[p12] > 0:
            pi12[period] = math.log(current) - math.log(series[p12])
    c1 = {p: pi3[p] - pi12[p] for p in pi3.keys() & pi12.keys()}
    c2 = {p: pi6[p] - pi12[p] for p in pi6.keys() & pi12.keys()}
    z1 = latest_component_z(c1)
    z2 = latest_component_z(c2)
    z3 = latest_component_z(pi12)
    return weighted_components([(0.55, z1), (0.30, z2), (0.15, z3)])


def rate_level_score(series: dict[dt.date, float]) -> float | None:
    periods = sorted(series)
    delta3: dict[dt.date, float] = {}
    for i, period in enumerate(periods):
        p3 = shift_months(periods, i, 3)
        if p3 is not None:
            delta3[period] = series[period] - series[p3]
    z_level = latest_component_z(series)
    z_delta = latest_component_z(delta3)
    return weighted_components([(0.70, z_level), (0.30, z_delta)])


def weighted_components(items: list[tuple[float, float | None]]) -> float | None:
    available = [(w, z) for w, z in items if z is not None]
    if not available:
        return None
    total = sum(w for w, _ in available)
    return sum((w / total) * z for w, z in available)


def latest_component_z(values_by_period: dict[dt.date, float]) -> float | None:
    if not values_by_period:
        return None
    latest = max(values_by_period)
    cutoff = dt.date(latest.year - 10, latest.month, 1)
    history = [
        value
        for period, value in values_by_period.items()
        if cutoff <= period <= latest and math.isfinite(value)
    ]
    if len(history) < MIN_MONTHLY_OBS:
        return None
    return robust_z(history, values_by_period[latest], clip=SERIES_Z_CLIP)


def robust_z(history: list[float], current: float, *, clip: float) -> float | None:
    distinct = sorted(set(float(v) for v in history if math.isfinite(v)))
    if len(distinct) < 2:
        return None
    median = statistics.median(distinct)
    mad = statistics.median(abs(v - median) for v in distinct)
    scale = 1.4826 * mad
    if scale <= 0:
        return None
    z = (current - median) / scale
    return max(-clip, min(clip, z))


def aggregate_axis(series_scores: dict[str, SeriesScore], axis: str) -> dict[str, Any]:
    configs = [cfg for cfg in BASELINE_SERIES if cfg.axis == axis]
    by_family: dict[str, list[tuple[float, SeriesScore]]] = {}
    for cfg in configs:
        by_family.setdefault(cfg.family, []).append((cfg.weight_in_family, series_scores[cfg.series_id]))

    family_scores: dict[str, float] = {}
    family_freshness: dict[str, float] = {}
    family_vintage: dict[str, float] = {}
    for family, items in by_family.items():
        values = [(w, s.score) for w, s in items if s.score is not None]
        score = huberized_weighted_mean(values)
        if score is not None:
            family_scores[family] = max(-FAMILY_SCORE_CLIP, min(FAMILY_SCORE_CLIP, score))
        family_freshness[family] = weighted_quality([(w, s.freshness) for w, s in items])
        family_vintage[family] = weighted_quality([(w, s.vintage_quality) for w, s in items])

    weights = FAMILY_WEIGHTS[axis]
    available = {f: score for f, score in family_scores.items() if f in weights}
    active_weight = sum(weights[f] for f in available)
    total_weight = sum(weights.values())
    coverage = active_weight / total_weight if total_weight > 0 else 0.0
    if active_weight > 0:
        score = sum((weights[f] / active_weight) * v for f, v in available.items())
        score = max(-AXIS_SCORE_CLIP, min(AXIS_SCORE_CLIP, score))
    else:
        score = None
    freshness = weighted_quality([(weights[f], family_freshness.get(f, 0.0)) for f in weights])
    vintage = weighted_quality([(weights[f], family_vintage.get(f, 0.0)) for f in weights])
    concordance, dispersion = concordance_quality(available, weights, score)
    return {
        "score": score,
        "family_scores": family_scores,
        "coverage": coverage,
        "freshness": freshness,
        "vintage_quality": vintage,
        "concordance": concordance,
        "dispersion": dispersion,
        "family_count": len(available),
        "has_anchor": bool(ANCHOR_FAMILIES[axis] & set(available)),
    }


def huberized_weighted_mean(items: list[tuple[float, float | None]]) -> float | None:
    values = [(w, float(v)) for w, v in items if v is not None and math.isfinite(v)]
    if not values:
        return None
    raw = [v for _, v in values]
    if len(raw) == 1:
        return raw[0]
    median = statistics.median(raw)
    mad = statistics.median(abs(v - median) for v in raw)
    scale = 1.4826 * mad
    limit = 1.5 * scale if scale > 0 else 1.5
    clipped = [(w, max(median - limit, min(median + limit, v))) for w, v in values]
    total = sum(abs(w) for w, _ in clipped)
    return sum(abs(w) * v for w, v in clipped) / total if total > 0 else None


def weighted_quality(items: list[tuple[float, float]]) -> float:
    total = sum(abs(w) for w, _ in items)
    if total <= 0:
        return 0.0
    return sum(abs(w) * max(0.0, min(1.0, q)) for w, q in items) / total


def concordance_quality(
    family_scores: dict[str, float], weights: dict[str, float], axis_score_value: float | None
) -> tuple[float, float]:
    values = [v for v in family_scores.values() if math.isfinite(v)]
    if not values or axis_score_value is None or axis_score_value == 0:
        return 0.0, 0.0
    sign = 1 if axis_score_value > 0 else -1
    active_weight = sum(weights[f] for f in family_scores if f in weights)
    same = sum(
        weights[f]
        for f, v in family_scores.items()
        if f in weights and ((v > 0 and sign > 0) or (v < 0 and sign < 0))
    )
    agreement = same / active_weight if active_weight > 0 else 0.0
    dispersion = robust_dispersion(values)
    dispersion_quality = max(0.0, min(1.0, 1.0 - dispersion / DISPERSION_ABSTAIN))
    return 0.5 * agreement + 0.5 * dispersion_quality, dispersion


def robust_dispersion(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    median = statistics.median(values)
    return 1.4826 * statistics.median(abs(v - median) for v in values)


def transition_axis(
    prev_sign: int | None,
    score: float | None,
    *,
    enter: float,
    exit_: float,
) -> AxisState:
    if score is None:
        return AxisState(prev_sign, None, "no_score")
    if prev_sign is None:
        if abs(score) < enter:
            return AxisState(None, None, "init_below_enter")
        sign = 1 if score > 0 else -1
        return AxisState(sign, sign, "init")
    signed_margin = prev_sign * score
    if signed_margin <= -enter:
        sign = 1 if score > 0 else -1
        return AxisState(sign, sign, "switch")
    if signed_margin >= exit_:
        return AxisState(prev_sign, prev_sign, "hold")
    return AxisState(prev_sign, None, "deadband")


def axis_margin(score: float | None, enter: float, exit_: float) -> float:
    if score is None:
        return 0.0
    if enter <= exit_:
        return 0.0
    return max(0.0, min(1.0, (abs(score) - exit_) / (enter - exit_)))


def resolve_candidate_status(
    *,
    axis: dict[str, dict[str, Any]],
    g_state: AxisState,
    i_state: AxisState,
    u_t: float,
    candidate_confidence: float,
) -> tuple[Status, list[str]]:
    return resolve_candidate_status_with_config(
        axis=axis,
        g_state=g_state,
        i_state=i_state,
        u_t=u_t,
        candidate_confidence=candidate_confidence,
        u_floor=U_FLOOR,
        min_confidence=MIN_CONFIDENCE,
        dispersion_limit=DISPERSION_ABSTAIN,
    )


def primary_reason(reasons: list[str]) -> str | None:
    if not reasons:
        return None
    priority = [
        "confidence_below_min",
        "u_below_floor",
        "growth_deadband",
        "inflation_deadband",
        "growth_init_below_enter",
        "inflation_init_below_enter",
        "growth_coverage_insufficient",
        "inflation_coverage_insufficient",
        "growth_missing_anchor_family",
        "inflation_missing_anchor_family",
        "growth_freshness_failed",
        "inflation_freshness_failed",
        "growth_family_dispersion",
        "inflation_family_dispersion",
        "growth_no_score",
        "inflation_no_score",
    ]
    for item in priority:
        if item in reasons:
            return item
    return reasons[0]


def reason_groups(reasons: str | list[str] | None) -> set[str]:
    if not reasons:
        return set()
    parts = reasons.split(",") if isinstance(reasons, str) else reasons
    groups: set[str] = set()
    for reason in parts:
        if not reason:
            continue
        if reason == "confidence_below_min" or "confidence_below" in reason:
            groups.add("confidence_below_threshold")
        elif reason == "u_below_floor":
            groups.add("u_below_floor")
        elif "deadband" in reason or "init_below_enter" in reason:
            groups.add("axis_neutral")
        elif "coverage" in reason or "insufficient_families" in reason:
            groups.add("coverage_insufficient")
        elif "missing_anchor_family" in reason:
            groups.add("critical_family_absent")
        elif "freshness" in reason:
            groups.add("freshness_failed")
        elif "dispersion" in reason:
            groups.add("dispersion_failed")
        elif "missing" in reason:
            groups.add("missing_input")
        elif "warmup" in reason:
            groups.add("warmup_or_transform_unavailable")
        elif "no_score" in reason or "insufficient_transform_history" in reason:
            groups.add("warmup_or_transform_unavailable")
        else:
            groups.add(reason)
    return groups


def axis_state_label(state: AxisState) -> str:
    if state.effective_sign == 1:
        return "positive"
    if state.effective_sign == -1:
        return "negative"
    return f"neutral:{state.reason or 'unknown'}"


def quadrant_from_signs(growth: int | None, inflation: int | None) -> Quadrant | None:
    if growth is None or inflation is None:
        return None
    return _QUADRANT_BY_SIGNS[(growth, inflation)]


def quadrant_from_scores(growth: float | None, inflation: float | None) -> Quadrant | None:
    if growth is None or inflation is None or growth == 0 or inflation == 0:
        return None
    return quadrant_from_signs(1 if growth > 0 else -1, 1 if inflation > 0 else -1)


def shift_months(periods: list[dt.date], idx: int, back: int) -> dt.date | None:
    target = periods[idx]
    y, m = target.year, target.month - back
    while m <= 0:
        m += 12
        y -= 1
    candidate = dt.date(y, m, 1)
    return candidate if candidate in set(periods) else None


def series_freshness(cut: dt.datetime, available_at: dt.datetime) -> float:
    # Baseline approximation until release calendars are wired: monthly source,
    # next expected release around +30d, 7d grace, hard max age 45d.
    soft, hard = source_deadlines(
        available_at,
        available_at + dt.timedelta(days=30),
        dt.timedelta(days=7),
        dt.timedelta(days=45),
        dt.timedelta(days=14),
    )
    return freshness_value(cut, soft, hard)


def vintage_quality(revision_number: int | None) -> float:
    if revision_number is None:
        return 0.0
    # First prints are usable but less mature; quality rises with observed
    # revision maturity and caps at 1.0.
    return min(1.0, 0.80 + 0.04 * min(max(revision_number, 0), 5))


def build_macro_metrics(
    replay: list[dict[str, Any]],
    *,
    first_release_replay: list[dict[str, Any]] | None = None,
    pit_qa: dict[str, Any] | None = None,
) -> dict[str, Any]:
    total = len(replay)
    statuses = count_by(replay, "status")
    years = max(1.0, total / 252.0)
    candidate_flips = flips_for_key(replay, "candidate_quadrant")
    valid_rows = [r for r in replay if r.get("status") == "valid"]
    valid_flips = flips_for_key(valid_rows, "published_quadrant")
    latched_flips = flips_for_key(replay, "published_quadrant")
    confidence_values = [r["candidate_confidence"] for r in replay if r.get("candidate_confidence") is not None]
    u_values = [r["u"] for r in replay if r.get("u") is not None]
    primary_reason_counts = count_by(replay, "status_reason_primary")
    reason_any_counts = count_reason_groups(replay)
    stability: dict[str, Any] = {}
    if first_release_replay:
        comparable = 0
        changed = 0
        sign_changed = {"growth": 0, "inflation": 0}
        status_changed = 0
        latched_changed = 0
        for latest, first in zip(replay, first_release_replay):
            lq = latest.get("candidate_quadrant")
            fq = first.get("candidate_quadrant")
            if lq and fq:
                comparable += 1
                changed += int(lq != fq)
                sign_changed["growth"] += int(latest.get("growth_sign") != first.get("growth_sign"))
                sign_changed["inflation"] += int(
                    latest.get("inflation_sign") != first.get("inflation_sign")
                )
                status_changed += int(latest.get("status") != first.get("status"))
                latched_changed += int(
                    latest.get("published_quadrant") != first.get("published_quadrant")
                )
        stability = {
            "candidate_comparable_days": comparable,
            "candidate_quadrant_changed_by_revision_days": changed,
            "candidate_quadrant_changed_by_revision_rate": (
                changed / comparable if comparable else None
            ),
            "growth_axis_sign_changed_by_revision_days": sign_changed["growth"],
            "inflation_axis_sign_changed_by_revision_days": sign_changed["inflation"],
            "status_changed_by_revision_days": status_changed,
            "latched_quadrant_changed_by_revision_days": latched_changed,
        }
    classification = classify_baseline_run(
        valid_rate=statuses.get("valid", 0) / total if total else 0.0,
        abstain_rate=statuses.get("abstain", 0) / total if total else 0.0,
        revision_change_rate=stability.get("candidate_quadrant_changed_by_revision_rate"),
    )
    return {
        "model_version": MACRO_MODEL_VERSION,
        "macro_config": MACRO_CONFIG_ID,
        "run_classification": classification,
        "eligible_days": total,
        "status_counts": statuses,
        "valid_rate": statuses.get("valid", 0) / total if total else 0.0,
        "abstain_rate": statuses.get("abstain", 0) / total if total else 0.0,
        "unavailable_rate": statuses.get("unavailable", 0) / total if total else 0.0,
        "status_reason_primary_counts": primary_reason_counts,
        "status_reason_any_counts": reason_any_counts,
        "abstention_reason_primary_counts": count_by(
            [r for r in replay if r.get("status") != "valid"], "status_reason_primary"
        ),
        "abstention_reason_any_counts": count_reason_groups(
            [r for r in replay if r.get("status") != "valid"]
        ),
        "candidate_quadrant_counts_all_days": count_by(replay, "candidate_quadrant"),
        "valid_published_quadrant_counts": count_by(valid_rows, "published_quadrant"),
        "latched_quadrant_counts_including_abstain": count_by(replay, "published_quadrant"),
        "days_without_latched_state": sum(1 for r in replay if r.get("published_quadrant") is None),
        # Backward-compatible alias, but reports should use the explicit names above.
        "quadrant_counts": count_by(valid_rows, "published_quadrant"),
        "candidate_quadrant_counts": count_by(replay, "candidate_quadrant"),
        "candidate_flips": candidate_flips,
        "candidate_flips_per_year": candidate_flips / years,
        "valid_published_flips": valid_flips,
        "valid_published_flips_per_year": valid_flips / years,
        "latched_flips": latched_flips,
        "latched_flips_per_year": latched_flips / years,
        "official_flips": latched_flips,
        "official_flips_per_year": latched_flips / years,
        "candidate_state_duration_days": duration_summary(
            state_durations(replay, "candidate_quadrant")
        ),
        "valid_published_state_duration_days": duration_summary(
            state_durations(valid_rows, "published_quadrant")
        ),
        "latched_state_duration_days": duration_summary(
            state_durations(replay, "published_quadrant")
        ),
        "official_state_duration_days": duration_summary(
            state_durations(replay, "published_quadrant")
        ),
        "confidence_distribution": distribution(confidence_values),
        "u_distribution": distribution(u_values),
        "coverage_distribution": distribution([r["coverage_quality"] for r in replay]),
        "growth_margin_distribution": distribution([r["growth_margin"] for r in replay]),
        "inflation_margin_distribution": distribution([r["inflation_margin"] for r in replay]),
        "vintage_stability": stability,
        "pit_qa": pit_qa or {},
    }


def compare_macro_market(
    macro_replay: list[dict[str, Any]],
    market_rows: list[dict[str, Any]],
    *,
    source: MarketSource,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    macro_by_date = {r["date"]: r for r in macro_replay}
    market_by_date = {r["date"]: r for r in market_rows}
    dates = sorted(set(macro_by_date) & set(market_by_date))
    rows: list[dict[str, Any]] = []
    confusion: dict[str, dict[str, int]] = {}
    both_valid = 0
    exact_agree = 0
    candidate_common = 0
    candidate_exact_agree = 0
    axis_agree = {"growth": 0, "inflation": 0}
    macro_valid_market_abstain = 0
    market_valid_macro_abstain = 0
    for day in dates:
        macro = macro_by_date[day]
        market = market_by_date[day]
        macro_valid = macro.get("status") == "valid"
        market_valid = market.get("status") == "valid"
        if macro_valid and not market_valid:
            macro_valid_market_abstain += 1
        if market_valid and not macro_valid:
            market_valid_macro_abstain += 1
        if macro_valid and market_valid:
            both_valid += 1
            mq = macro.get("published_quadrant")
            kq = market.get("quadrant") or market.get("published_quadrant")
            exact_agree += int(mq == kq)
            confusion.setdefault(str(mq), {}).setdefault(str(kq), 0)
            confusion[str(mq)][str(kq)] += 1
            axis_agree["growth"] += int(macro.get("growth_sign") == market.get("growth_sign"))
            axis_agree["inflation"] += int(macro.get("inflation_sign") == market.get("inflation_sign"))
        if macro.get("candidate_quadrant") and market.get("candidate_quadrant"):
            candidate_common += 1
            candidate_exact_agree += int(
                macro.get("candidate_quadrant") == market.get("candidate_quadrant")
            )
        rows.append({
            "date": day,
            "macro_status": macro.get("status"),
            "market_status": market.get("status"),
            "macro_quadrant": macro.get("published_quadrant"),
            "market_quadrant": market.get("quadrant") or market.get("published_quadrant"),
            "macro_candidate": macro.get("candidate_quadrant"),
            "market_candidate": market.get("candidate_quadrant"),
            "macro_confidence": macro.get("candidate_confidence"),
            "market_confidence": market.get("candidate_confidence"),
            "both_valid": macro_valid and market_valid,
            "exact_agreement": (
                (macro.get("published_quadrant") == (
                    market.get("quadrant") or market.get("published_quadrant")
                ))
                if macro_valid and market_valid else None
            ),
        })
    divergence_durations = state_durations(
        [r for r in rows if r["both_valid"]], "exact_agreement", false_value=False
    )
    metrics = {
        "market_source": source,
        "common_dates": len(dates),
        "both_valid_dates": both_valid,
        "exact_quadrant_agreement_rate": exact_agree / both_valid if both_valid else None,
        "candidate_common_dates": candidate_common,
        "candidate_exact_quadrant_agreement_rate": (
            candidate_exact_agree / candidate_common if candidate_common else None
        ),
        "candidate_comparison_is_diagnostic_only": True,
        "growth_axis_agreement_rate": axis_agree["growth"] / both_valid if both_valid else None,
        "inflation_axis_agreement_rate": axis_agree["inflation"] / both_valid if both_valid else None,
        "macro_valid_market_abstain_rate": (
            macro_valid_market_abstain / len(dates) if dates else None
        ),
        "market_valid_macro_abstain_rate": (
            market_valid_macro_abstain / len(dates) if dates else None
        ),
        "confusion_matrix": confusion,
        "divergence_duration_days": duration_summary(divergence_durations),
        "macro_transition_dates": transition_dates(macro_replay, "published_quadrant"),
        "market_transition_dates": transition_dates(market_rows, "quadrant"),
    }
    metrics["transition_lead_lag_days"] = nearest_transition_lags(
        metrics["macro_transition_dates"], metrics["market_transition_dates"])
    return rows, metrics


def build_market_metrics(market_rows: list[dict[str, Any]]) -> dict[str, Any]:
    confidence = [
        row["candidate_confidence"]
        for row in market_rows
        if row.get("candidate_confidence") is not None
    ]
    growth_scores = [
        row["growth_score"] for row in market_rows if row.get("growth_score") is not None
    ]
    inflation_scores = [
        row["inflation_score"]
        for row in market_rows
        if row.get("inflation_score") is not None
    ]
    price_sources = sorted({
        str(row["price_source"]) for row in market_rows if row.get("price_source")
    })
    price_conventions = sorted({
        str(row["price_convention"])
        for row in market_rows
        if row.get("price_convention")
    })
    return {
        "market_eligible_days": len(market_rows),
        "market_status_counts": count_by(market_rows, "status"),
        "market_status_reason_primary_counts": count_by(market_rows, "status_reason_primary"),
        "market_status_reason_any_counts": count_reason_groups(market_rows),
        "market_warmup_failures": sum(
            1 for row in market_rows if "warmup" in str(row.get("status_reasons_all"))
        ),
        "market_missing_input_days": sum(
            1 for row in market_rows if "missing" in str(row.get("status_reasons_all"))
        ),
        "market_confidence_distribution": distribution(confidence),
        "market_confidence_formula": "sqrt(growth_margin * inflation_margin)",
        "market_confidence_expected_range": [0.0, 1.0],
        "market_candidate_quadrant_counts": count_by(market_rows, "candidate_quadrant"),
        "market_growth_score_distribution": distribution(growth_scores),
        "market_inflation_score_distribution": distribution(inflation_scores),
        "market_valid_rate": (
            count_by(market_rows, "status").get("valid", 0) / len(market_rows)
            if market_rows else 0.0
        ),
        "market_price_sources": price_sources,
        "market_price_conventions": price_conventions,
        "market_price_convention": "; ".join(price_conventions) if price_conventions else None,
        "market_comparison_note": (
            "No official valid-vs-valid comparison is inferred when market remains mostly "
            "abstained."
        ),
    }


def build_revision_attribution(
    latest_replay: list[dict[str, Any]],
    first_release_replay: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    first_by_date = {row["date"]: row for row in first_release_replay}
    series_rows: list[dict[str, Any]] = []
    family_rows: list[dict[str, Any]] = []
    for latest in latest_replay:
        first = first_by_date.get(latest["date"])
        if first is None:
            continue
        candidate_changed = latest.get("candidate_quadrant") != first.get("candidate_quadrant")
        status_changed = latest.get("status") != first.get("status")
        latched_changed = latest.get("published_quadrant") != first.get("published_quadrant")
        for cfg in BASELINE_SERIES:
            prefix = cfg.series_id.lower()
            latest_score = latest.get(f"{prefix}_score")
            first_score = first.get(f"{prefix}_score")
            series_rows.append({
                "date": latest["date"],
                "series_id": cfg.series_id,
                "axis": cfg.axis,
                "family": cfg.family,
                "latest_score": latest_score,
                "first_release_score": first_score,
                "score_delta": numeric_delta(latest_score, first_score),
                "score_sign_changed": sign_of(latest_score) != sign_of(first_score),
                "latest_period": latest.get(f"{prefix}_period"),
                "first_release_period": first.get(f"{prefix}_period"),
                "latest_vintage": latest.get(f"{prefix}_vintage"),
                "first_release_vintage": first.get(f"{prefix}_vintage"),
                "candidate_quadrant_changed": candidate_changed,
                "status_changed": status_changed,
                "latched_quadrant_changed": latched_changed,
            })
        family_keys = sorted(
            key for key in latest if key.startswith(("growth_family_", "inflation_family_"))
        )
        for key in family_keys:
            if not key.endswith("_score"):
                continue
            axis, family = key.split("_family_", 1)
            family = family.removesuffix("_score")
            latest_score = latest.get(key)
            first_score = first.get(key)
            family_rows.append({
                "date": latest["date"],
                "axis": axis,
                "family": family,
                "latest_family_score": latest_score,
                "first_release_family_score": first_score,
                "family_score_delta": numeric_delta(latest_score, first_score),
                "family_sign_changed": sign_of(latest_score) != sign_of(first_score),
                "axis_sign_changed": latest.get(f"{axis}_sign") != first.get(f"{axis}_sign"),
                "candidate_quadrant_changed": candidate_changed,
                "status_changed": status_changed,
                "latched_quadrant_changed": latched_changed,
            })
    transition_rows = build_transition_revision_deltas(latest_replay, first_release_replay)
    return series_rows, family_rows, transition_rows


def build_transition_revision_deltas(
    latest_replay: list[dict[str, Any]],
    first_release_replay: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("candidate_quadrant", "published_quadrant"):
        latest_dates = transition_dates(latest_replay, key)
        first_dates = transition_dates(first_release_replay, key)
        first_parsed = [dt.date.fromisoformat(day) for day in first_dates]
        for latest_day in latest_dates:
            latest_date = dt.date.fromisoformat(latest_day)
            if first_parsed:
                nearest = min(first_parsed, key=lambda day: abs((day - latest_date).days))
                lag = (latest_date - nearest).days
                nearest_text = nearest.isoformat()
            else:
                nearest_text = None
                lag = None
            rows.append({
                "transition_type": key,
                "latest_transition_date": latest_day,
                "nearest_first_release_transition_date": nearest_text,
                "latest_minus_first_release_days": lag,
            })
    return rows


def reference_a31_config(name: str = "A31-REF") -> A31Config:
    return A31Config(
        name=name,
        transformation_weights={
            "quantity_index": {"acceleration_3m": 0.50, "acceleration_6m": 0.30, "change_12m": 0.20},
            "price_index": {"acceleration_3m": 0.55, "acceleration_6m": 0.30, "change_12m": 0.15},
            "rate_level": {"level": 0.70, "delta_3m": 0.30},
        },
        family_weights=json.loads(json.dumps(FAMILY_WEIGHTS)),
        series_weights={cfg.series_id: cfg.weight_in_family for cfg in BASELINE_SERIES},
        aggregation_method="huberized_weighted_mean",
        robust_clip=1.5,
        reliability_weighting="none",
        score_clip={
            "series": SERIES_Z_CLIP,
            "family": FAMILY_SCORE_CLIP,
            "axis": AXIS_SCORE_CLIP,
        },
        release_smoothing="none",
    )


def reference_a32_config(name: str = "A32-REF") -> A32Config:
    return A32Config(
        name=name,
        growth_score_scale=1.0,
        inflation_score_scale=1.0,
        growth_enter=GROWTH_ENTER,
        growth_exit=AXIS_EXIT,
        inflation_enter=INFLATION_ENTER,
        inflation_exit=AXIS_EXIT,
        u_floor=U_FLOOR,
        min_confidence=MIN_CONFIDENCE,
        dispersion_limit=DISPERSION_ABSTAIN,
        coverage_rules_version="macro_axis_coverage_0_80_v1",
    )


def business_calendar_hash(calendar: list[dt.date]) -> str:
    return stable_hash([day.isoformat() for day in calendar])


def series_family_mapping_hash() -> str:
    return stable_hash([asdict(cfg) for cfg in BASELINE_SERIES])


def canonical_config_hash(config: A31Config | A32Config) -> str:
    return stable_hash(asdict(config))


def a31_config_hash(config: A31Config, l2_macro_logical_hash: str) -> str:
    return stable_hash({
        "l2_macro_logical_hash": l2_macro_logical_hash,
        "canonical_A31Config": asdict(config),
        "scorer_schema_version": L3_SCORER_SCHEMA_VERSION,
        "scorer_code_version": L3_SCORER_CODE_VERSION,
    })[:24]


def a32_config_hash(config: A32Config, l3_config_hash: str) -> str:
    return stable_hash({
        "l3_config_hash": l3_config_hash,
        "canonical_A32Config": asdict(config),
        "state_schema_version": L4_STATE_SCHEMA_VERSION,
        "state_code_version": L4_STATE_CODE_VERSION,
    })[:24]


def validate_parent_hash(name: str, actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"{name} parent hash mismatch: actual={actual} expected={expected}")


def build_l3_score_panel(
    macro_feature_primitives: list[dict[str, Any]],
    a31_config: A31Config,
    *,
    l2_macro_logical_hash: str,
    expected_l2_macro_logical_hash: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    validate_parent_hash(
        "L3 macro_feature_primitives",
        l2_macro_logical_hash,
        expected_l2_macro_logical_hash,
    )
    macro_feature_primitives = enrich_l2_component_z(macro_feature_primitives)
    config_hash = a31_config_hash(a31_config, l2_macro_logical_hash)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in macro_feature_primitives:
        grouped.setdefault((str(row["business_date"]), str(row["selection_mode"])), []).append(row)

    score_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []
    for (business_date, selection_mode), primitive_rows in sorted(grouped.items()):
        role = selection_role_for_mode(selection_mode)
        by_series = {str(row["series_id"]): row for row in primitive_rows}
        axis_payload = {
            "growth": aggregate_l3_axis(by_series, "growth", a31_config),
            "inflation": aggregate_l3_axis(by_series, "inflation", a31_config),
        }
        c_quality = min(axis_payload["growth"]["coverage"], axis_payload["inflation"]["coverage"])
        f_quality = min(axis_payload["growth"]["freshness"], axis_payload["inflation"]["freshness"])
        a_quality = min(axis_payload["growth"]["concordance"], axis_payload["inflation"]["concordance"])
        v_quality = min(
            axis_payload["growth"]["vintage_quality"],
            axis_payload["inflation"]["vintage_quality"],
        )
        u_value = 0.35 * c_quality + 0.20 * f_quality + 0.25 * a_quality + 0.20 * v_quality
        information_hash = l2_information_set_hash(primitive_rows)
        critical_flags = {
            "growth": bool(axis_payload["growth"]["has_anchor"]),
            "inflation": bool(axis_payload["inflation"]["has_anchor"]),
        }
        score_rows.append({
            "business_date": business_date,
            "date": business_date,
            "selection_mode": selection_mode,
            "selection_role": role,
            "counterfactual_only": role != "pit_runtime_candidate",
            "a31_config_hash": config_hash,
            "a31_config_name": a31_config.name,
            "growth_family_scores": json.dumps(
                axis_payload["growth"]["family_scores"], sort_keys=True
            ),
            "inflation_family_scores": json.dumps(
                axis_payload["inflation"]["family_scores"], sort_keys=True
            ),
            "growth_score_unscaled": axis_payload["growth"]["score"],
            "inflation_score_unscaled": axis_payload["inflation"]["score"],
            "C": c_quality,
            "F": f_quality,
            "A": a_quality,
            "V": v_quality,
            "u": u_value,
            "growth_dispersion": axis_payload["growth"]["dispersion"],
            "inflation_dispersion": axis_payload["inflation"]["dispersion"],
            "growth_coverage": axis_payload["growth"]["coverage"],
            "inflation_coverage": axis_payload["inflation"]["coverage"],
            "growth_freshness": axis_payload["growth"]["freshness"],
            "inflation_freshness": axis_payload["inflation"]["freshness"],
            "growth_vintage_quality": axis_payload["growth"]["vintage_quality"],
            "inflation_vintage_quality": axis_payload["inflation"]["vintage_quality"],
            "growth_concordance": axis_payload["growth"]["concordance"],
            "inflation_concordance": axis_payload["inflation"]["concordance"],
            "growth_family_count": axis_payload["growth"]["family_count"],
            "inflation_family_count": axis_payload["inflation"]["family_count"],
            "critical_family_flags": json.dumps(critical_flags, sort_keys=True),
            "growth_has_anchor": critical_flags["growth"],
            "inflation_has_anchor": critical_flags["inflation"],
            "information_set_hash": information_hash,
        })
        contribution_rows.extend(
            l3_contribution_rows(
                business_date, selection_mode, role, config_hash, by_series, axis_payload, a31_config
            )
        )
    apply_release_smoothing(score_rows, a31_config.release_smoothing)
    manifest = {
        "schema_version": L3_SCORER_SCHEMA_VERSION,
        "code_version": L3_SCORER_CODE_VERSION,
        "a31_config_hash": config_hash,
        "a31_config": asdict(a31_config),
        "parent_hashes": {
            "l2_macro_logical_hash": l2_macro_logical_hash,
        },
        "row_count": len(score_rows),
        "contribution_row_count": len(contribution_rows),
        "logical_hash": logical_records_hash(score_rows),
        "contribution_logical_hash": logical_records_hash(contribution_rows),
        "reusable_for_a32": True,
    }
    return score_rows, contribution_rows, manifest


def aggregate_l3_axis(
    by_series: dict[str, dict[str, Any]], axis: str, config: A31Config
) -> dict[str, Any]:
    configs = [cfg for cfg in BASELINE_SERIES if cfg.axis == axis]
    by_family: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for cfg in configs:
        row = by_series.get(cfg.series_id, {})
        weight = config.series_weights.get(cfg.series_id, cfg.weight_in_family)
        by_family.setdefault(cfg.family, []).append((weight, row))

    family_scores: dict[str, float] = {}
    family_freshness: dict[str, float] = {}
    family_vintage: dict[str, float] = {}
    for family, items in by_family.items():
        values = []
        for weight, row in items:
            score = series_score_from_l2_row(row, config)
            if score is not None:
                score = clip(score, config.score_clip.get("series", SERIES_Z_CLIP))
                adjusted_weight = weight * reliability_weight_factor(
                    row.get("vintage_quality"), config.reliability_weighting
                )
                values.append((adjusted_weight, score))
        score = aggregate_values(values, config.aggregation_method, config.robust_clip)
        if score is not None:
            family_scores[family] = clip(score, config.score_clip.get("family", FAMILY_SCORE_CLIP))
        family_freshness[family] = weighted_quality([
            (weight, float(row.get("freshness") or 0.0)) for weight, row in items
        ])
        family_vintage[family] = weighted_quality([
            (weight, float(row.get("vintage_quality") or 0.0)) for weight, row in items
        ])

    weights = config.family_weights[axis]
    score_weights = {
        family: weight * reliability_weight_factor(
            family_vintage.get(family), config.reliability_weighting
        )
        for family, weight in weights.items()
    }
    available = {family: score for family, score in family_scores.items() if family in weights}
    active_weight = sum(score_weights[family] for family in available)
    total_weight = sum(weights.values())
    coverage = active_weight / total_weight if total_weight > 0 else 0.0
    if active_weight > 0:
        score = sum(
            (score_weights[family] / active_weight) * value
            for family, value in available.items()
        )
        score = clip(score, config.score_clip.get("axis", AXIS_SCORE_CLIP))
    else:
        score = None
    freshness = weighted_quality([(weights[family], family_freshness.get(family, 0.0)) for family in weights])
    vintage = weighted_quality([(weights[family], family_vintage.get(family, 0.0)) for family in weights])
    concordance, dispersion = concordance_quality(available, score_weights, score)
    return {
        "score": score,
        "family_scores": family_scores,
        "coverage": coverage,
        "freshness": freshness,
        "vintage_quality": vintage,
        "concordance": concordance,
        "dispersion": dispersion,
        "family_count": len(available),
        "has_anchor": bool(ANCHOR_FAMILIES[axis] & set(available)),
    }


def series_score_from_l2_row(row: dict[str, Any], config: A31Config) -> float | None:
    transform_class = str(row.get("transform_class") or "")
    ref = reference_a31_config()
    weights = config.transformation_weights.get(transform_class)
    if not weights:
        return finite_or_none(row.get("reference_series_score"))
    if weights == ref.transformation_weights.get(transform_class):
        return finite_or_none(row.get("reference_series_score"))
    if transform_class in {"quantity_index", "price_index"}:
        return weighted_components([
            (float(weights.get("acceleration_3m", 0.0)), finite_or_none(row.get("z_acceleration_3m"))),
            (float(weights.get("acceleration_6m", 0.0)), finite_or_none(row.get("z_acceleration_6m"))),
            (float(weights.get("change_12m", 0.0)), finite_or_none(row.get("z_change_12m"))),
        ])
    if transform_class == "rate_level":
        return weighted_components([
            (float(weights.get("level", 0.0)), finite_or_none(row.get("z_level"))),
            (float(weights.get("delta_3m", 0.0)), finite_or_none(row.get("z_delta_3m"))),
        ])
    return finite_or_none(row.get("reference_series_score"))


def enrich_l2_component_z(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = [dict(row) for row in rows]
    if enriched and "z_change_12m" in enriched[0]:
        return enriched
    indexes_by_series: dict[tuple[str, str], list[int]] = {}
    for idx, row in enumerate(enriched):
        indexes_by_series.setdefault(
            (str(row.get("selection_mode")), str(row.get("series_id"))), []
        ).append(idx)
    component_sources = {
        "acceleration_3m": "acceleration_3m_vs_12m",
        "acceleration_6m": "acceleration_6m_vs_12m",
        "change_12m": "change_12m",
        "level": "raw_value",
        "delta_3m": "delta_3m",
    }
    for indexes in indexes_by_series.values():
        histories: dict[str, dict[dt.date, float]] = {
            component: {} for component in component_sources
        }
        for idx in sorted(indexes, key=lambda i: str(enriched[i].get("business_date"))):
            row = enriched[idx]
            period_value = row.get("observation_period")
            period = cache_date(period_value) if period_value not in {None, ""} else None
            for component, source_key in component_sources.items():
                value = finite_or_none(row.get(source_key))
                if period is not None and value is not None:
                    histories[component][period] = value
                row[f"z_{component}"] = component_z_as_of(histories[component], period)
    return enriched


def component_z_as_of(history: dict[dt.date, float], current_period: dt.date | None) -> float | None:
    if current_period is None:
        return None
    cutoff = safe_year_delta(current_period, -10)
    values = {
        period: value
        for period, value in history.items()
        if cutoff <= period <= current_period and math.isfinite(value)
    }
    return latest_component_z(values)


def safe_year_delta(value: dt.date, years: int) -> dt.date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(year=value.year + years, day=28)


def apply_release_smoothing(rows: list[dict[str, Any]], mode: str) -> None:
    if mode in {"none", "off", "disabled"}:
        return
    if mode not in {"ema_half_life_2", "ema_hl2"}:
        raise ValueError(f"unsupported release_smoothing: {mode}")
    alpha = 1.0 - math.exp(math.log(0.5) / 2.0)
    by_mode: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_mode.setdefault(str(row["selection_mode"]), []).append(row)
    for selection_rows in by_mode.values():
        prev_info_hash: str | None = None
        prev_growth: float | None = None
        prev_inflation: float | None = None
        for row in sorted(selection_rows, key=lambda item: str(item["business_date"])):
            current_info_hash = str(row["information_set_hash"])
            growth = finite_or_none(row.get("growth_score_unscaled"))
            inflation = finite_or_none(row.get("inflation_score_unscaled"))
            if current_info_hash == prev_info_hash:
                row["growth_score_unscaled"] = prev_growth
                row["inflation_score_unscaled"] = prev_inflation
                continue
            if prev_growth is not None and growth is not None:
                growth = alpha * growth + (1.0 - alpha) * prev_growth
            if prev_inflation is not None and inflation is not None:
                inflation = alpha * inflation + (1.0 - alpha) * prev_inflation
            row["growth_score_unscaled"] = growth
            row["inflation_score_unscaled"] = inflation
            prev_info_hash = current_info_hash
            prev_growth = growth
            prev_inflation = inflation


def reliability_weight_factor(value: Any, mode: str) -> float:
    if mode in {"none", "off", "disabled"}:
        return 1.0
    quality = finite_or_none(value)
    if quality is None:
        return 1.0
    quality = max(0.0, min(1.0, quality))
    if mode in {"base", "pit_expanding"}:
        return max(0.25, quality)
    if mode in {"reinforced", "strong"}:
        return max(0.10, quality * quality)
    raise ValueError(f"unsupported reliability_weighting: {mode}")


def aggregate_values(
    items: list[tuple[float, float]], method: str, robust_clip: float = 1.5
) -> float | None:
    if method == "huberized_weighted_mean":
        return huberized_weighted_mean_with_limit(
            [(weight, value) for weight, value in items], robust_clip
        )
    values = [value for _, value in items if math.isfinite(value)]
    if not values:
        return None
    if method == "median":
        return statistics.median(values)
    total = sum(abs(weight) for weight, _ in items)
    return sum(abs(weight) * value for weight, value in items) / total if total > 0 else None


def huberized_weighted_mean_with_limit(
    items: list[tuple[float, float | None]], limit_multiplier: float
) -> float | None:
    values = [(w, float(v)) for w, v in items if v is not None and math.isfinite(v)]
    if not values:
        return None
    raw = [v for _, v in values]
    if len(raw) == 1:
        return raw[0]
    median = statistics.median(raw)
    mad = statistics.median(abs(v - median) for v in raw)
    scale = 1.4826 * mad
    multiplier = limit_multiplier if limit_multiplier > 0 else 1.5
    limit = multiplier * scale if scale > 0 else multiplier
    clipped = [(w, max(median - limit, min(median + limit, v))) for w, v in values]
    total = sum(abs(w) for w, _ in clipped)
    return sum(abs(w) * v for w, v in clipped) / total if total > 0 else None


def l2_information_set_hash(rows: list[dict[str, Any]]) -> str:
    h = hashlib.sha256()
    for row in sorted(rows, key=lambda item: str(item.get("series_id"))):
        period = row.get("observation_period")
        if period is None:
            continue
        raw_value = finite_or_none(row.get("raw_value"))
        h.update(
            "|".join([
                str(row["series_id"]),
                str(period),
                str(row.get("vintage_date")),
                str(row.get("available_at")),
                str(int(row.get("revision_number") or 0)),
                f"{raw_value:.12g}" if raw_value is not None else "",
            ]).encode("utf-8")
        )
        h.update(b"\n")
    return h.hexdigest()


def l3_contribution_rows(
    business_date: str,
    selection_mode: str,
    selection_role: str,
    a31_hash: str,
    by_series: dict[str, dict[str, Any]],
    axis_payload: dict[str, dict[str, Any]],
    config: A31Config,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cfg in BASELINE_SERIES:
        source = by_series.get(cfg.series_id, {})
        rows.append({
            "business_date": business_date,
            "selection_mode": selection_mode,
            "selection_role": selection_role,
            "a31_config_hash": a31_hash,
            "contribution_level": "series",
            "axis": cfg.axis,
            "family": cfg.family,
            "series_id": cfg.series_id,
            "score": series_score_from_l2_row(source, config),
            "weight": 1.0,
        })
    for axis_name, payload in axis_payload.items():
        for family, score in payload["family_scores"].items():
            rows.append({
                "business_date": business_date,
                "selection_mode": selection_mode,
                "selection_role": selection_role,
                "a31_config_hash": a31_hash,
                "contribution_level": "family",
                "axis": axis_name,
                "family": family,
                "series_id": None,
                "score": score,
                "weight": FAMILY_WEIGHTS[axis_name].get(family),
            })
    return rows


def run_l4_state_machine(
    l3_score_panel: list[dict[str, Any]],
    a32_config: A32Config,
    *,
    selection_mode: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = [
        row for row in l3_score_panel
        if row["selection_mode"] == selection_mode
    ]
    rows.sort(key=lambda row: row["business_date"])
    if not rows:
        return [], {}
    l3_hash = rows[0]["a31_config_hash"]
    a32_hash = a32_config_hash(a32_config, l3_hash)
    prev_info_hash: str | None = None
    prev_record: dict[str, Any] | None = None
    growth_internal: int | None = None
    inflation_internal: int | None = None
    published_quadrant: str | None = None
    out: list[dict[str, Any]] = []
    for row in rows:
        day = str(row["business_date"])
        info_hash = str(row["information_set_hash"])
        if prev_record is not None and info_hash == prev_info_hash:
            carried = dict(prev_record)
            carried["business_date"] = day
            carried["date"] = day
            carried["input_changed"] = False
            carried["inputs_changed"] = False
            carried["reevaluated"] = False
            carried["transition_occurred"] = False
            out.append(carried)
            prev_record = carried
            continue

        g_score = scale_score(row.get("growth_score_unscaled"), a32_config.growth_score_scale)
        i_score = scale_score(row.get("inflation_score_unscaled"), a32_config.inflation_score_scale)
        prev_published = published_quadrant
        g_state = transition_axis(
            growth_internal,
            g_score,
            enter=a32_config.growth_enter,
            exit_=a32_config.growth_exit,
        )
        i_state = transition_axis(
            inflation_internal,
            i_score,
            enter=a32_config.inflation_enter,
            exit_=a32_config.inflation_exit,
        )
        growth_internal = g_state.internal_sign
        inflation_internal = i_state.internal_sign
        g_margin = axis_margin(g_score, a32_config.growth_enter, a32_config.growth_exit)
        i_margin = axis_margin(i_score, a32_config.inflation_enter, a32_config.inflation_exit)
        confidence = 0.60 * float(row["u"]) + 0.40 * math.sqrt(g_margin * i_margin)
        candidate_quadrant = quadrant_from_scores(g_score, i_score)
        instant_quadrant = quadrant_from_signs(g_state.effective_sign, i_state.effective_sign)
        axis = l4_axis_status_payload(row, g_score, i_score)
        status, reasons = resolve_candidate_status_with_config(
            axis=axis,
            g_state=g_state,
            i_state=i_state,
            u_t=float(row["u"]),
            candidate_confidence=confidence,
            u_floor=a32_config.u_floor,
            min_confidence=a32_config.min_confidence,
            dispersion_limit=a32_config.dispersion_limit,
        )
        if status == "valid":
            published_quadrant = instant_quadrant
        record = {
            "business_date": day,
            "date": day,
            "a31_config_hash": l3_hash,
            "a32_config_hash": a32_hash,
            "a31_config_name": row["a31_config_name"],
            "a32_config_name": a32_config.name,
            "selection_mode": selection_mode,
            "selection_role": row["selection_role"],
            "counterfactual_only": row["counterfactual_only"],
            "information_set_hash": info_hash,
            "source_vintage_hash": info_hash,
            "input_changed": True,
            "inputs_changed": True,
            "reevaluated": True,
            "growth_score": g_score,
            "inflation_score": i_score,
            "growth_score_unscaled": row.get("growth_score_unscaled"),
            "inflation_score_unscaled": row.get("inflation_score_unscaled"),
            "growth_axis_state": axis_state_label(g_state),
            "inflation_axis_state": axis_state_label(i_state),
            "growth_sign": g_state.effective_sign,
            "inflation_sign": i_state.effective_sign,
            "growth_internal_sign": growth_internal,
            "inflation_internal_sign": inflation_internal,
            "candidate_quadrant": candidate_quadrant,
            "instant_quadrant": instant_quadrant,
            "published_quadrant": published_quadrant,
            "latched_quadrant": published_quadrant,
            "confidence": confidence,
            "candidate_confidence": confidence,
            "growth_margin": g_margin,
            "inflation_margin": i_margin,
            "m_growth": g_margin,
            "m_inflation": i_margin,
            "status": status,
            "primary_reason": primary_reason(reasons),
            "status_reason_primary": primary_reason(reasons),
            "all_reasons": ",".join(reasons),
            "status_reasons": ",".join(reasons),
            "status_reasons_all": ",".join(reasons),
            "transition_occurred": (
                published_quadrant is not None and published_quadrant != prev_published
            ),
            "u": row["u"],
            "C": row["C"],
            "F": row["F"],
            "A": row["A"],
            "V": row["V"],
            "coverage_quality": row["C"],
            "freshness_quality": row["F"],
            "concordance_quality": row["A"],
            "vintage_quality": row["V"],
            "growth_dispersion": row["growth_dispersion"],
            "inflation_dispersion": row["inflation_dispersion"],
            "growth_coverage": row["growth_coverage"],
            "inflation_coverage": row["inflation_coverage"],
            "growth_family_count": row["growth_family_count"],
            "inflation_family_count": row["inflation_family_count"],
            "growth_critical_family_ok": row["growth_has_anchor"],
            "inflation_critical_family_ok": row["inflation_has_anchor"],
            "critical_family_ok": bool(row["growth_has_anchor"] and row["inflation_has_anchor"]),
            "u_ok": float(row["u"]) >= a32_config.u_floor,
            "confidence_ok": confidence >= a32_config.min_confidence,
            "dispersion_ok": (
                float(row["growth_dispersion"]) <= a32_config.dispersion_limit
                and float(row["inflation_dispersion"]) <= a32_config.dispersion_limit
            ),
            "model_version": MACRO_MODEL_VERSION,
            "macro_config": MACRO_CONFIG_ID,
            "confidence_model_version": CONFIDENCE_MODEL_VERSION,
        }
        out.append(record)
        prev_info_hash = info_hash
        prev_record = record
    manifest = {
        "schema_version": L4_STATE_SCHEMA_VERSION,
        "code_version": L4_STATE_CODE_VERSION,
        "a31_config_hash": l3_hash,
        "a32_config_hash": a32_hash,
        "a32_config": asdict(a32_config),
        "selection_mode": selection_mode,
        "selection_role": selection_role_for_mode(selection_mode),
        "counterfactual_only": selection_role_for_mode(selection_mode) != "pit_runtime_candidate",
        "parent_hashes": {
            "l3_score_panel_logical_hash": logical_records_hash(l3_score_panel),
        },
        "row_count": len(out),
        "logical_hash": logical_records_hash(out),
    }
    return out, manifest


def l4_axis_status_payload(
    row: dict[str, Any], growth_score: float | None, inflation_score: float | None
) -> dict[str, dict[str, Any]]:
    return {
        "growth": {
            "score": growth_score,
            "coverage": row["growth_coverage"],
            "freshness": row["growth_freshness"],
            "family_count": row["growth_family_count"],
            "has_anchor": row["growth_has_anchor"],
            "dispersion": row["growth_dispersion"],
        },
        "inflation": {
            "score": inflation_score,
            "coverage": row["inflation_coverage"],
            "freshness": row["inflation_freshness"],
            "family_count": row["inflation_family_count"],
            "has_anchor": row["inflation_has_anchor"],
            "dispersion": row["inflation_dispersion"],
        },
    }


def resolve_candidate_status_with_config(
    *,
    axis: dict[str, dict[str, Any]],
    g_state: AxisState,
    i_state: AxisState,
    u_t: float,
    candidate_confidence: float,
    u_floor: float,
    min_confidence: float,
    dispersion_limit: float,
) -> tuple[Status, list[str]]:
    reasons: list[str] = []
    for axis_name in ("growth", "inflation"):
        data = axis[axis_name]
        if data["score"] is None:
            reasons.append(f"{axis_name}_no_score")
        if data["coverage"] < 0.80:
            reasons.append(f"{axis_name}_coverage_insufficient")
        if data["freshness"] <= 0.0:
            reasons.append(f"{axis_name}_freshness_failed")
        if data["family_count"] < MIN_VALID_FAMILIES[axis_name]:
            reasons.append(f"{axis_name}_insufficient_families")
        if not data["has_anchor"]:
            reasons.append(f"{axis_name}_missing_anchor_family")
        if data["dispersion"] > dispersion_limit:
            reasons.append(f"{axis_name}_family_dispersion")
    if g_state.effective_sign is None:
        reasons.append(f"growth_{g_state.reason}")
    if i_state.effective_sign is None:
        reasons.append(f"inflation_{i_state.reason}")
    if u_t < u_floor:
        reasons.append("u_below_floor")
    if candidate_confidence < min_confidence:
        reasons.append("confidence_below_min")

    if any(r.endswith("_no_score") or r.endswith("_missing_anchor_family") for r in reasons):
        return "unavailable", reasons
    if any("dispersion" in r for r in reasons):
        return "abstain", reasons
    if reasons:
        return "abstain", reasons
    return "valid", []


def scale_score(value: Any, scale: float) -> float | None:
    number = finite_or_none(value)
    return number * scale if number is not None else None


def finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def clip(value: float, limit: float) -> float:
    return max(-limit, min(limit, value))


def compare_l4_reference_parity(
    reference_replay: list[dict[str, Any]],
    l4_replay: list[dict[str, Any]],
) -> dict[str, Any]:
    reference_by_date = {row["date"]: row for row in reference_replay}
    l4_by_date = {row["date"]: row for row in l4_replay}
    fields_exact = [
        "date",
        "status",
        "status_reason_primary",
        "status_reasons_all",
        "candidate_quadrant",
        "instant_quadrant",
        "published_quadrant",
        "growth_sign",
        "inflation_sign",
        "growth_axis_state",
        "inflation_axis_state",
        "inputs_changed",
        "reevaluated",
    ]
    fields_float = [
        "growth_score",
        "inflation_score",
        "C",
        "F",
        "A",
        "V",
        "u",
        "growth_margin",
        "inflation_margin",
        "candidate_confidence",
    ]
    mismatches: list[dict[str, Any]] = []
    for day in sorted(reference_by_date):
        ref = reference_by_date[day]
        got = l4_by_date.get(day)
        if got is None:
            mismatches.append({"date": day, "field": "__missing__", "reference": True, "l4": False})
            continue
        for field in fields_exact:
            if ref.get(field) != got.get(field):
                mismatches.append({
                    "date": day,
                    "field": field,
                    "reference": ref.get(field),
                    "l4": got.get(field),
                })
        for field in fields_float:
            lhs = finite_or_none(ref.get(field))
            rhs = finite_or_none(got.get(field))
            if lhs is None and rhs is None:
                continue
            if lhs is None or rhs is None or abs(lhs - rhs) > 1e-12:
                mismatches.append({
                    "date": day,
                    "field": field,
                    "reference": lhs,
                    "l4": rhs,
                })
    return {
        "passed": not mismatches,
        "checked_rows": len(reference_replay),
        "mismatch_count": len(mismatches),
        "mismatch_examples": mismatches[:20],
        "reference_logical_hash": logical_records_hash(reference_replay),
        "l4_logical_hash": logical_records_hash(l4_replay),
    }


def build_smoke_grid_results(
    macro_feature_primitives: list[dict[str, Any]],
    l2_macro_logical_hash: str,
    series_revision_rows: list[dict[str, Any]],
    family_revision_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    a31_configs = smoke_a31_configs(series_revision_rows, family_revision_rows)
    a32_ref = reference_a32_config()
    a32_configs = smoke_a32_configs()
    all_l3_rows: list[dict[str, Any]] = []
    all_metric_rows: list[dict[str, Any]] = []
    all_summary_rows: list[dict[str, Any]] = []
    l3_manifest_rows: list[dict[str, Any]] = []
    for a31 in a31_configs:
        l3_rows, _, l3_manifest = build_l3_score_panel(
            macro_feature_primitives,
            a31,
            l2_macro_logical_hash=l2_macro_logical_hash,
            expected_l2_macro_logical_hash=l2_macro_logical_hash,
        )
        all_l3_rows.extend(l3_rows)
        l3_manifest_rows.append({
            "a31_config_hash": l3_manifest["a31_config_hash"],
            "a31_config_name": a31.name,
            "logical_hash": l3_manifest["logical_hash"],
            "row_count": l3_manifest["row_count"],
        })
        configs_for_a31 = a32_configs if a31.name == "A31-REF" else [a32_ref]
        for a32 in configs_for_a31:
            runtime, _ = run_l4_state_machine(l3_rows, a32, selection_mode="latest")
            counterfactual, _ = run_l4_state_machine(l3_rows, a32, selection_mode="first_release")
            a31_hash = l3_manifest["a31_config_hash"]
            a32_hash = a32_config_hash(a32, a31_hash)
            classification = classify_smoke_result(runtime, counterfactual)
            all_metric_rows.extend(
                evaluation_metric_rows(runtime, counterfactual, a31, a32, a31_hash, a32_hash, classification)
            )
            full_metrics = build_macro_metrics(runtime, first_release_replay=counterfactual)
            all_summary_rows.append({
                "a31_config_hash": a31_hash,
                "a31_config_name": a31.name,
                "a32_config_hash": a32_hash,
                "a32_config_name": a32.name,
                "result_classification": classification,
                "valid_rate": full_metrics["valid_rate"],
                "abstain_rate": full_metrics["abstain_rate"],
                "candidate_revision_change_rate": full_metrics["vintage_stability"].get(
                    "candidate_quadrant_changed_by_revision_rate"
                ),
                "candidate_flips_per_year": full_metrics["candidate_flips_per_year"],
                "published_flips_per_year": full_metrics["valid_published_flips_per_year"],
                "frozen": False,
                "production_candidate": False,
                "activation_ready": False,
            })
    manifest = {
        "schema_version": SMOKE_GRID_SCHEMA_VERSION,
        "serial": True,
        "parallelized": False,
        "selection_policy": "diagnostic smoke only; no parameter selection",
        "a31_config_count": len(a31_configs),
        "evaluation_count": len(all_summary_rows),
        "l3_panels_reused": True,
        "l3_manifest_rows": l3_manifest_rows,
        "metric_rows": len(all_metric_rows),
        "summary_logical_hash": logical_records_hash(all_summary_rows),
        "metrics_logical_hash": logical_records_hash(all_metric_rows),
    }
    return all_l3_rows, all_summary_rows, all_metric_rows, manifest


def smoke_a31_configs(
    series_revision_rows: list[dict[str, Any]],
    family_revision_rows: list[dict[str, Any]],
) -> list[A31Config]:
    ref = reference_a31_config()
    robust = A31Config(**{**asdict(ref), "name": "A31-ROBUST", "aggregation_method": "median"})
    growth_family = top_revision_family(family_revision_rows, "growth")
    inflation_family = top_revision_family(family_revision_rows, "inflation")
    growth_shift = shift_a31_family_weight(ref, "A31-GROWTH-REVISION", "growth", growth_family)
    inflation_shift = shift_a31_family_weight(ref, "A31-INFLATION-REVISION", "inflation", inflation_family)
    clip_cfg = A31Config(
        **{
            **asdict(ref),
            "name": "A31-CLIP",
            "score_clip": {"series": 2.5, "family": FAMILY_SCORE_CLIP, "axis": AXIS_SCORE_CLIP},
        }
    )
    return [ref, robust, growth_shift, inflation_shift, clip_cfg]


def top_revision_family(family_revision_rows: list[dict[str, Any]], axis: str) -> str:
    counts: dict[str, int] = {}
    for row in family_revision_rows:
        if row.get("axis") != axis:
            continue
        if row.get("family_sign_changed") or row.get("axis_sign_changed"):
            family = str(row.get("family"))
            counts[family] = counts.get(family, 0) + 1
    if not counts:
        return next(iter(FAMILY_WEIGHTS[axis]))
    return max(counts, key=lambda family: (counts[family], family))


def shift_a31_family_weight(ref: A31Config, name: str, axis: str, source_family: str) -> A31Config:
    weights = json.loads(json.dumps(ref.family_weights))
    axis_weights = weights[axis]
    delta = min(0.05, max(0.0, axis_weights.get(source_family, 0.0)))
    recipients = [family for family in axis_weights if family != source_family]
    axis_weights[source_family] = axis_weights.get(source_family, 0.0) - delta
    if recipients:
        add = delta / len(recipients)
        for family in recipients:
            axis_weights[family] += add
    return A31Config(**{**asdict(ref), "name": name, "family_weights": weights})


def smoke_a32_configs() -> list[A32Config]:
    configs: list[A32Config] = []
    for growth_enter in (0.30, 0.35):
        for inflation_enter in (0.35, 0.40):
            for axis_exit in (0.10, 0.15):
                for min_confidence in (0.60, 0.65, 0.70):
                    configs.append(A32Config(
                        name=(
                            f"A32-g{growth_enter:.2f}-i{inflation_enter:.2f}-"
                            f"x{axis_exit:.2f}-c{min_confidence:.2f}"
                        ),
                        growth_score_scale=1.0,
                        inflation_score_scale=1.0,
                        growth_enter=growth_enter,
                        growth_exit=axis_exit,
                        inflation_enter=inflation_enter,
                        inflation_exit=axis_exit,
                        u_floor=U_FLOOR,
                        min_confidence=min_confidence,
                        dispersion_limit=DISPERSION_ABSTAIN,
                        coverage_rules_version="macro_axis_coverage_0_80_v1",
                    ))
    return configs


def classify_smoke_result(
    runtime: list[dict[str, Any]], counterfactual: list[dict[str, Any]]
) -> str:
    metrics = build_macro_metrics(runtime, first_release_replay=counterfactual)
    if metrics["run_classification"] == RUN_CLASSIFICATION_FAILED:
        return "diagnostic_failed"
    return "a32_candidate"


def evaluation_metric_rows(
    runtime: list[dict[str, Any]],
    counterfactual: list[dict[str, Any]],
    a31: A31Config,
    a32: A32Config,
    a31_hash: str,
    a32_hash: str,
    classification: str,
) -> list[dict[str, Any]]:
    folds = [
        ("full", None, None),
        ("2014_2017", dt.date(2014, 2, 19), dt.date(2017, 12, 31)),
        ("2018_2021", dt.date(2018, 1, 1), dt.date(2021, 12, 31)),
        ("2022_2026", dt.date(2022, 1, 1), dt.date(2026, 6, 24)),
    ]
    rows: list[dict[str, Any]] = []
    for fold_name, start, end in folds:
        rt = filter_replay_window(runtime, start, end)
        cf = filter_replay_window(counterfactual, start, end)
        metrics = build_macro_metrics(rt, first_release_replay=cf)
        transition_deltas = build_transition_revision_deltas(rt, cf)
        rows.append({
            "fold": fold_name,
            "a31_config_hash": a31_hash,
            "a31_config_name": a31.name,
            "a32_config_hash": a32_hash,
            "a32_config_name": a32.name,
            "result_classification": classification,
            "candidate_revision_change_rate": metrics["vintage_stability"].get(
                "candidate_quadrant_changed_by_revision_rate"
            ),
            "growth_sign_revision_change_days": metrics["vintage_stability"].get(
                "growth_axis_sign_changed_by_revision_days"
            ),
            "inflation_sign_revision_change_days": metrics["vintage_stability"].get(
                "inflation_axis_sign_changed_by_revision_days"
            ),
            "status_revision_change_days": metrics["vintage_stability"].get(
                "status_changed_by_revision_days"
            ),
            "latched_revision_change_days": metrics["vintage_stability"].get(
                "latched_quadrant_changed_by_revision_days"
            ),
            "transition_timing_displacement": json.dumps(
                duration_summary([
                    abs(int(row["latest_minus_first_release_days"]))
                    for row in transition_deltas
                    if row.get("latest_minus_first_release_days") is not None
                ]),
                sort_keys=True,
            ),
            "candidate_flips_per_year": metrics["candidate_flips_per_year"],
            "published_flips_per_year": metrics["valid_published_flips_per_year"],
            "candidate_duration_distribution": json.dumps(
                metrics["candidate_state_duration_days"], sort_keys=True
            ),
            "published_duration_distribution": json.dumps(
                metrics["valid_published_state_duration_days"], sort_keys=True
            ),
            "valid_rate": metrics["valid_rate"],
            "abstain_rate": metrics["abstain_rate"],
            "reason_counts": json.dumps(metrics["status_reason_any_counts"], sort_keys=True),
            "quadrant_occupancy": json.dumps(
                metrics["candidate_quadrant_counts_all_days"], sort_keys=True
            ),
            "days_without_latched_state": metrics["days_without_latched_state"],
            "coverage_distribution": json.dumps(
                metrics["coverage_distribution"], sort_keys=True
            ),
            "growth_dispersion_distribution": json.dumps(
                distribution([row["growth_dispersion"] for row in rt]), sort_keys=True
            ),
            "inflation_dispersion_distribution": json.dumps(
                distribution([row["inflation_dispersion"] for row in rt]), sort_keys=True
            ),
            "frozen": False,
            "production_candidate": False,
            "activation_ready": False,
        })
    return rows


def filter_replay_window(
    rows: list[dict[str, Any]],
    start: dt.date | None,
    end: dt.date | None,
) -> list[dict[str, Any]]:
    if start is None or end is None:
        return rows
    return [
        row for row in rows
        if start <= dt.date.fromisoformat(str(row["date"])) <= end
    ]


def run_harness(conn, config: HarnessConfig) -> dict[str, Any]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    calendar = business_days(config.start_date, config.end_date)
    if not calendar:
        raise ValueError("decision calendar is empty")
    max_available_at = decision_time(calendar[-1])
    inputs = load_or_create_harness_inputs(
        conn, config, max_available_at=max_available_at
    )
    deterministic_input_cache_key = (
        inputs.cache_metadata.get("cache_key")
        or input_cache_request_key(config, max_available_at)
    )
    rows = inputs.vintage_rows
    repeat_rows = inputs.repeat_vintage_rows
    grouped = group_rows(rows)
    pit_qa = compute_pit_qa(rows, grouped, calendar=calendar, repeat_rows=repeat_rows)
    pit_selection_panel = build_pit_selection_panel(grouped, calendar)
    macro_feature_primitives = build_macro_feature_primitives(grouped, calendar)
    market_feature_primitives = build_market_feature_primitives(
        inputs.market_levels,
        config.start_date,
        config.end_date,
        price_source="cagg_nav_daily" if config.market_source == "db_cagg" else config.market_source,
    )
    pit_selection_manifest = {
        "panel": "pit_selection_panel",
        "schema_version": L1_SCHEMA_VERSION,
        "row_count": len(pit_selection_panel),
        "logical_hash": logical_records_hash(pit_selection_panel),
        "input_cache_key": inputs.cache_metadata.get("cache_key"),
        "business_date_calendar_hash": business_calendar_hash(calendar),
        "series_family_mapping_hash": series_family_mapping_hash(),
        "selection_modes": ["latest", "first_release"],
        "selection_roles": {
            "latest": "pit_runtime_candidate",
            "first_release": "revised_vintage_counterfactual",
        },
        "counterfactual_runtime_allowed": False,
        "grain": "business_date x series_id",
    }
    l2_macro_hash = logical_records_hash(macro_feature_primitives)
    l2_market_hash = logical_records_hash(market_feature_primitives)
    feature_manifest = {
        "schema_version": L2_SCHEMA_VERSION,
        "input_cache_key": inputs.cache_metadata.get("cache_key"),
        "business_date_calendar_hash": business_calendar_hash(calendar),
        "series_family_mapping_hash": series_family_mapping_hash(),
        "parent_hashes": {
            "l1_logical_hash": pit_selection_manifest["logical_hash"],
        },
        "selection_roles": {
            "latest": "pit_runtime_candidate",
            "first_release": "revised_vintage_counterfactual",
        },
        "counterfactual_runtime_allowed": False,
        "macro_feature_primitives": {
            "row_count": len(macro_feature_primitives),
            "logical_hash": l2_macro_hash,
            "grain": "business_date x selection_mode x series_id",
        },
        "market_feature_primitives": {
            "row_count": len(market_feature_primitives),
            "logical_hash": l2_market_hash,
            "grain": "business_date",
        },
        "parameter_independent": True,
    }
    macro_replay = replay_macro(rows, calendar, selection_mode="latest")
    first_release = replay_macro(rows, calendar, selection_mode="first")
    metrics = build_macro_metrics(macro_replay, first_release_replay=first_release, pit_qa=pit_qa)

    market_rows: list[dict[str, Any]] = []
    if config.market_source == "snapshot":
        market_rows = read_market_snapshot_rows(conn, config.start_date, config.end_date)
    elif config.market_source == "db_cagg":
        if inputs.market_levels is None:
            market_rows = replay_market_db_cagg(conn, config.start_date, config.end_date)
        else:
            market_rows = build_market_replay_from_levels(
                inputs.market_levels,
                config.start_date,
                config.end_date,
                model_version="market_implied_quadrant_v0_db_cagg_replay",
                price_source="cagg_nav_daily",
                price_convention="uses cagg_nav_daily NAV on as_of when present",
            )
    elif config.market_source == "tiingo":
        market_rows = replay_market_tiingo(config.start_date, config.end_date)
    comparison_rows, comparison_metrics = compare_macro_market(
        macro_replay, market_rows, source=config.market_source)
    market_metrics = build_market_metrics(market_rows)
    series_revision_rows, family_revision_rows, transition_revision_rows = (
        build_revision_attribution(macro_replay, first_release)
    )
    a31_ref = reference_a31_config()
    a32_ref = reference_a32_config()
    l3_ref_rows, l3_ref_contribution_rows, l3_ref_manifest = build_l3_score_panel(
        macro_feature_primitives,
        a31_ref,
        l2_macro_logical_hash=l2_macro_hash,
        expected_l2_macro_logical_hash=feature_manifest["macro_feature_primitives"]["logical_hash"],
    )
    l4_ref_runtime, l4_ref_runtime_manifest = run_l4_state_machine(
        l3_ref_rows,
        a32_ref,
        selection_mode="latest",
    )
    l4_ref_counterfactual, l4_ref_counterfactual_manifest = run_l4_state_machine(
        l3_ref_rows,
        a32_ref,
        selection_mode="first_release",
    )
    l4_metrics = build_macro_metrics(
        l4_ref_runtime,
        first_release_replay=l4_ref_counterfactual,
        pit_qa=pit_qa,
    )
    l4_reference_parity = {
        "runtime": compare_l4_reference_parity(macro_replay, l4_ref_runtime),
        "counterfactual": compare_l4_reference_parity(first_release, l4_ref_counterfactual),
    }
    l4_reference_parity["passed"] = bool(
        l4_reference_parity["runtime"]["passed"]
        and l4_reference_parity["counterfactual"]["passed"]
    )
    (
        smoke_l3_rows,
        smoke_grid_summary_rows,
        smoke_grid_metric_rows,
        smoke_grid_manifest,
    ) = build_smoke_grid_results(
        macro_feature_primitives,
        l2_macro_hash,
        series_revision_rows,
        family_revision_rows,
    )
    env_metadata = collect_environment_metadata(Path.cwd())

    config_hash = stable_hash({
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "data_snapshot_id": config.data_snapshot_id,
        "backend_commit": config.backend_commit,
        "worker_commit": config.worker_commit,
        "random_seed": config.random_seed,
        "decision_calendar": config.decision_calendar,
        "macro_config": config.macro_config,
        "policy_config": config.policy_config,
        "market_source": config.market_source,
        "constants": candidate_constants(),
        "environment": env_metadata,
        "input_cache_key": deterministic_input_cache_key,
    })
    data_hash = pit_qa["data_hash"]
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"investintell/calibration/{config_hash}/{data_hash}"))
    macro_metrics_payload = {
        **metrics,
        "market_comparison": comparison_metrics,
        "market_diagnostics": market_metrics,
        "input_cache": inputs.cache_metadata,
        "l4_reference_parity": l4_reference_parity,
        "smoke_grid": smoke_grid_manifest,
    }

    policy_replay = [{
        "calibration_run_id": run_id,
        "status": "not_run",
        "reason": "A4 remains candidate seed until A3 baseline passes",
        "policy_version": POLICY_VERSION,
        "gate_version": GATE_VERSION,
    }]

    write_parquet(config.output_dir / "pit_selection_panel.parquet", pit_selection_panel)
    write_json(config.output_dir / "pit_selection_manifest.json", pit_selection_manifest)
    write_parquet(
        config.output_dir / "macro_feature_primitives.parquet",
        macro_feature_primitives,
    )
    write_parquet(
        config.output_dir / "market_feature_primitives.parquet",
        market_feature_primitives,
    )
    write_json(config.output_dir / "feature_manifest.json", feature_manifest)
    write_parquet(config.output_dir / "l3_score_panel_ref.parquet", l3_ref_rows)
    write_parquet(
        config.output_dir / "l3_contributions_ref.parquet",
        l3_ref_contribution_rows,
    )
    write_json(config.output_dir / "l3_manifest_ref.json", l3_ref_manifest)
    write_parquet(config.output_dir / "l4_replay_ref.parquet", l4_ref_runtime)
    write_parquet(
        config.output_dir / "l4_counterfactual_ref.parquet",
        l4_ref_counterfactual,
    )
    write_json(config.output_dir / "l4_metrics_ref.json", l4_metrics)
    write_json(config.output_dir / "l4_manifest_ref.json", {
        "runtime": l4_ref_runtime_manifest,
        "counterfactual": l4_ref_counterfactual_manifest,
        "parent_hashes": {
            "l3_score_panel_ref_logical_hash": l3_ref_manifest["logical_hash"],
            "l2_macro_logical_hash": l2_macro_hash,
        },
        "reference_parity": l4_reference_parity,
        "frozen": False,
        "production_candidate": False,
        "activation_ready": False,
    })
    write_json(config.output_dir / "l4_reference_parity.json", l4_reference_parity)
    write_parquet(config.output_dir / "smoke_l3_score_panels.parquet", smoke_l3_rows)
    write_parquet(config.output_dir / "smoke_grid_summary.parquet", smoke_grid_summary_rows)
    write_parquet(config.output_dir / "smoke_grid_metrics.parquet", smoke_grid_metric_rows)
    write_json(config.output_dir / "smoke_grid_manifest.json", smoke_grid_manifest)
    write_parquet(config.output_dir / "macro_replay.parquet", macro_replay)
    write_json(config.output_dir / "macro_metrics.json", macro_metrics_payload)
    write_parquet(config.output_dir / "market_replay.parquet", market_rows)
    write_parquet(config.output_dir / "macro_market_comparison.parquet", comparison_rows)
    write_parquet(config.output_dir / "policy_replay.parquet", policy_replay)
    write_parquet(
        config.output_dir / "revision_attribution_by_series.parquet",
        series_revision_rows,
    )
    write_parquet(
        config.output_dir / "revision_attribution_by_family.parquet",
        family_revision_rows,
    )
    write_parquet(
        config.output_dir / "transition_revision_deltas.parquet",
        transition_revision_rows,
    )
    primary_artifacts = [
        "pit_selection_panel.parquet",
        "pit_selection_manifest.json",
        "macro_feature_primitives.parquet",
        "market_feature_primitives.parquet",
        "feature_manifest.json",
        "l3_score_panel_ref.parquet",
        "l3_contributions_ref.parquet",
        "l3_manifest_ref.json",
        "l4_replay_ref.parquet",
        "l4_counterfactual_ref.parquet",
        "l4_metrics_ref.json",
        "l4_manifest_ref.json",
        "l4_reference_parity.json",
        "smoke_l3_score_panels.parquet",
        "smoke_grid_summary.parquet",
        "smoke_grid_metrics.parquet",
        "smoke_grid_manifest.json",
        "macro_replay.parquet",
        "macro_metrics.json",
        "market_replay.parquet",
        "macro_market_comparison.parquet",
        "policy_replay.parquet",
        "revision_attribution_by_series.parquet",
        "revision_attribution_by_family.parquet",
        "transition_revision_deltas.parquet",
    ]
    primary_hashes = hash_artifacts(config.output_dir, primary_artifacts)
    logical_hashes = {
        "pit_selection_panel.parquet": logical_records_hash(pit_selection_panel),
        "pit_selection_manifest.json": logical_payload_hash(pit_selection_manifest),
        "macro_feature_primitives.parquet": logical_records_hash(macro_feature_primitives),
        "market_feature_primitives.parquet": logical_records_hash(market_feature_primitives),
        "feature_manifest.json": logical_payload_hash(feature_manifest),
        "l3_score_panel_ref.parquet": logical_records_hash(l3_ref_rows),
        "l3_contributions_ref.parquet": logical_records_hash(l3_ref_contribution_rows),
        "l3_manifest_ref.json": logical_payload_hash(l3_ref_manifest),
        "l4_replay_ref.parquet": logical_records_hash(l4_ref_runtime),
        "l4_counterfactual_ref.parquet": logical_records_hash(l4_ref_counterfactual),
        "l4_metrics_ref.json": logical_payload_hash(l4_metrics),
        "l4_manifest_ref.json": logical_payload_hash({
            "runtime": l4_ref_runtime_manifest,
            "counterfactual": l4_ref_counterfactual_manifest,
            "parent_hashes": {
                "l3_score_panel_ref_logical_hash": l3_ref_manifest["logical_hash"],
                "l2_macro_logical_hash": l2_macro_hash,
            },
            "reference_parity": l4_reference_parity,
            "frozen": False,
            "production_candidate": False,
            "activation_ready": False,
        }),
        "l4_reference_parity.json": logical_payload_hash(l4_reference_parity),
        "smoke_l3_score_panels.parquet": logical_records_hash(smoke_l3_rows),
        "smoke_grid_summary.parquet": logical_records_hash(smoke_grid_summary_rows),
        "smoke_grid_metrics.parquet": logical_records_hash(smoke_grid_metric_rows),
        "smoke_grid_manifest.json": logical_payload_hash(smoke_grid_manifest),
        "macro_replay.parquet": logical_records_hash(macro_replay),
        "macro_metrics.json": logical_payload_hash(macro_metrics_payload),
        "market_replay.parquet": logical_records_hash(market_rows),
        "macro_market_comparison.parquet": logical_records_hash(comparison_rows),
        "policy_replay.parquet": logical_records_hash(policy_replay),
        "revision_attribution_by_series.parquet": logical_records_hash(series_revision_rows),
        "revision_attribution_by_family.parquet": logical_records_hash(family_revision_rows),
        "transition_revision_deltas.parquet": logical_records_hash(transition_revision_rows),
    }
    manifest = build_manifest(
        config,
        run_id,
        config_hash,
        data_hash,
        metrics,
        comparison_metrics,
        market_metrics,
        env_metadata,
        inputs.cache_metadata,
        primary_hashes,
        logical_hashes,
    )
    write_yaml(config.output_dir / "parameter_manifest.yaml", manifest)
    write_text(config.output_dir / "calibration_report.md", render_report(
        manifest, macro_metrics_payload, comparison_metrics, market_metrics))
    final_artifacts = [
        *primary_artifacts,
        "parameter_manifest.yaml",
        "calibration_report.md",
    ]
    write_json(
        config.output_dir / "artifact_hashes.json",
        {
            "calibration_run_id": run_id,
            "hash_method": "sha256_file_bytes",
            "artifacts": hash_artifacts(config.output_dir, final_artifacts),
            "logical_hash_method": "sha256_json_schema_and_sorted_rows",
            "logical_artifacts": logical_hashes,
            "note": "artifact_hashes.json excludes its own self-referential hash",
        },
    )

    return {
        "calibration_run_id": run_id,
        "output_dir": str(config.output_dir),
        "macro_days": len(macro_replay),
        "market_common_dates": comparison_metrics["common_dates"],
        "valid_rate": metrics["valid_rate"],
        "run_classification": metrics["run_classification"],
        "status": "ok",
    }


def replay_market_db_cagg(conn, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
    levels = load_market_levels_from_cagg_nav_daily(conn, start_date, end_date)
    return build_market_replay_from_levels(
        levels,
        start_date,
        end_date,
        model_version="market_implied_quadrant_v0_db_cagg_replay",
        price_source="cagg_nav_daily",
        price_convention="uses cagg_nav_daily NAV on as_of when present",
    )


def load_market_levels_from_cagg_nav_daily(
    conn, start_date: dt.date, end_date: dt.date
) -> dict[str, dict[dt.date, float]]:
    history_start = start_date - dt.timedelta(days=420)
    tickers = ["SPY", "IEF", "TIP"]
    sql = (
        "WITH wanted(ticker) AS (SELECT unnest(%s::text[])), "
        "resolved AS ( "
        "  SELECT DISTINCT ON (upper(iu.ticker)) upper(iu.ticker) AS ticker, iu.instrument_id "
        "  FROM instruments_universe iu JOIN wanted w ON upper(iu.ticker) = w.ticker "
        "  ORDER BY upper(iu.ticker), iu.is_active DESC NULLS LAST, iu.updated_at DESC NULLS LAST "
        ") "
        "SELECT r.ticker, c.bucket, c.nav "
        "FROM resolved r "
        "JOIN cagg_nav_daily c ON c.instrument_id = r.instrument_id "
        "WHERE c.bucket BETWEEN %s AND %s AND c.nav IS NOT NULL AND c.nav > 0 "
        "ORDER BY r.ticker, c.bucket"
    )
    out: dict[str, dict[dt.date, float]] = {ticker: {} for ticker in tickers}
    with conn.cursor() as cur:
        cur.execute(sql, (tickers, history_start, end_date))
        for ticker, day, nav in cur.fetchall():
            out[str(ticker).upper()][day] = float(nav)
    return out


def build_market_replay_from_levels(
    levels: dict[str, dict[dt.date, float]],
    start_date: dt.date,
    end_date: dt.date,
    *,
    model_version: str,
    price_source: str,
    price_convention: str,
) -> list[dict[str, Any]]:
    from src.workers.quadrant_market import WINDOW

    spy_by = levels.get("SPY", {})
    ief_by = levels.get("IEF", {})
    tip_by = levels.get("TIP", {})
    price_days = sorted(spy_by)
    be_by: dict[dt.date, float] = {}
    last_ief = last_tip = None
    for d in price_days:
        last_ief = ief_by.get(d, last_ief)
        last_tip = tip_by.get(d, last_tip)
        if last_ief and last_tip:
            be_by[d] = last_tip / last_ief

    prev_g = prev_i = None
    published = None
    rows: list[dict[str, Any]] = []
    for d in business_days(start_date, end_date):
        reasons: list[str] = []
        g_score = None
        i_score = None
        if d not in spy_by:
            reasons.append("market_missing_spy_close")
        price_history = [day for day in price_days if day <= d]
        if d in spy_by and len(price_history) <= WINDOW:
            reasons.append("market_growth_warmup")
        if d in spy_by and len(price_history) > WINDOW:
            past = price_history[-WINDOW - 1]
            g_score = spy_by[d] / spy_by[past] - 1.0 if spy_by[past] > 0 else None
        be_days = [x for x in sorted(be_by) if x <= d]
        if d not in be_by:
            reasons.append("market_missing_tip_ief_close")
        if len(be_days) <= WINDOW:
            reasons.append("market_inflation_warmup_or_missing")
        elif d in be_by:
            prev = be_days[-WINDOW - 1]
            i_score = be_by[d] / be_by[prev] - 1.0 if be_by[prev] > 0 else None
        if g_score is None and "market_missing_spy_close" not in reasons:
            reasons.append("market_growth_missing_score")
        if i_score is None and "market_inflation_warmup_or_missing" not in reasons:
            reasons.append("market_inflation_missing_score")
        g_state = transition_axis(prev_g, g_score, enter=0.25, exit_=0.10)
        i_state = transition_axis(prev_i, i_score, enter=0.25, exit_=0.10)
        prev_g, prev_i = g_state.internal_sign, i_state.internal_sign
        g_margin = axis_margin(g_score, 0.25, 0.10)
        i_margin = axis_margin(i_score, 0.25, 0.10)
        candidate_confidence = math.sqrt(g_margin * i_margin)
        quadrant = quadrant_from_signs(g_state.effective_sign, i_state.effective_sign)
        if g_state.effective_sign is None:
            reasons.append(f"market_growth_{g_state.reason}")
        if i_state.effective_sign is None:
            reasons.append(f"market_inflation_{i_state.reason}")
        if candidate_confidence < MIN_CONFIDENCE:
            reasons.append("market_confidence_below_min")
        status: Status = "valid" if quadrant and not reasons else "abstain"
        if status == "valid":
            published = quadrant
        rows.append({
            "date": d.isoformat(),
            "status": status,
            "status_reason_primary": primary_reason(reasons),
            "status_reasons_all": ",".join(reasons),
            "quadrant": quadrant,
            "published_quadrant": published,
            "candidate_quadrant": quadrant_from_scores(g_score, i_score),
            "candidate_confidence": candidate_confidence,
            "growth_sign": g_state.effective_sign,
            "inflation_sign": i_state.effective_sign,
            "growth_axis_state": axis_state_label(g_state),
            "inflation_axis_state": axis_state_label(i_state),
            "growth_score": g_score,
            "inflation_score": i_score,
            "growth_margin": g_margin,
            "inflation_margin": i_margin,
            "confidence_growth_margin_component": g_margin,
            "confidence_inflation_margin_component": i_margin,
            "confidence_formula": "sqrt(growth_margin * inflation_margin)",
            "confidence_ok": candidate_confidence >= MIN_CONFIDENCE,
            "lookback_days": WINDOW,
            "price_source": price_source,
            "price_convention": price_convention,
            "model_version": model_version,
        })
    return rows


def replay_market_tiingo(start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
    from src.workers._tiingo import TiingoClient

    history_start = start_date - dt.timedelta(days=420)
    with TiingoClient() as client:
        spy = client.fetch_daily_prices("SPY", history_start, end_date)
        ief = client.fetch_daily_prices("IEF", history_start, end_date)
        tip = client.fetch_daily_prices("TIP", history_start, end_date)
    levels = {
        "SPY": {d: float(v) for d, v in spy if v and v > 0},
        "IEF": {d: float(v) for d, v in ief if v and v > 0},
        "TIP": {d: float(v) for d, v in tip if v and v > 0},
    }
    return build_market_replay_from_levels(
        levels,
        start_date,
        end_date,
        model_version="market_implied_quadrant_v0_tiingo_replay",
        price_source="tiingo",
        price_convention="uses Tiingo adjusted close on as_of when present",
    )


def build_manifest(
    config: HarnessConfig,
    run_id: str,
    config_hash: str,
    data_hash: str,
    metrics: dict[str, Any],
    comparison_metrics: dict[str, Any],
    market_metrics: dict[str, Any],
    env_metadata: dict[str, Any],
    input_cache_metadata: dict[str, Any],
    artifact_hashes: dict[str, str],
    logical_artifact_hashes: dict[str, str],
) -> dict[str, Any]:
    return {
        "calibration_run_id": run_id,
        "created_at": dt.datetime.now(UTC).isoformat(),
        "read_only": True,
        "run_classification": metrics["run_classification"],
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "decision_time_convention": (
            "macro vintages dated t are included in a decision on t at 23:59:59 UTC"
        ),
        "decision_calendar": config.decision_calendar,
        "data_snapshot_id": config.data_snapshot_id,
        "backend_commit": config.backend_commit,
        "worker_commit": config.worker_commit,
        "git_dirty": env_metadata.get("git_dirty"),
        "random_seed": config.random_seed,
        "macro_model_version": MACRO_MODEL_VERSION,
        "macro_config": config.macro_config,
        "policy_version": config.policy_config,
        "gate_version": GATE_VERSION,
        "confidence_model_version": CONFIDENCE_MODEL_VERSION,
        "config_hash": config_hash,
        "data_hash": data_hash,
        "artifacts": [
            "pit_selection_panel.parquet",
            "pit_selection_manifest.json",
            "macro_feature_primitives.parquet",
            "market_feature_primitives.parquet",
            "feature_manifest.json",
            "l3_score_panel_ref.parquet",
            "l3_contributions_ref.parquet",
            "l3_manifest_ref.json",
            "l4_replay_ref.parquet",
            "l4_counterfactual_ref.parquet",
            "l4_metrics_ref.json",
            "l4_manifest_ref.json",
            "l4_reference_parity.json",
            "smoke_l3_score_panels.parquet",
            "smoke_grid_summary.parquet",
            "smoke_grid_metrics.parquet",
            "smoke_grid_manifest.json",
            "macro_replay.parquet",
            "macro_metrics.json",
            "market_replay.parquet",
            "macro_market_comparison.parquet",
            "policy_replay.parquet",
            "revision_attribution_by_series.parquet",
            "revision_attribution_by_family.parquet",
            "transition_revision_deltas.parquet",
            "parameter_manifest.yaml",
            "calibration_report.md",
            "artifact_hashes.json",
        ],
        "artifact_hashes": artifact_hashes,
        "logical_artifact_hashes": logical_artifact_hashes,
        "artifact_hashes_file": "artifact_hashes.json",
        "environment": env_metadata,
        "input_cache": input_cache_metadata,
        "feature_panels": {
            "l1": "pit_selection_panel.parquet",
            "l2_macro": "macro_feature_primitives.parquet",
            "l2_market": "market_feature_primitives.parquet",
            "l3_ref": "l3_score_panel_ref.parquet",
            "l4_ref": "l4_replay_ref.parquet",
            "smoke_grid_summary": "smoke_grid_summary.parquet",
        },
        "candidate_constants": candidate_constants(),
        "publication_semantics": {
            "status_gate_version": "a3_v0_1_candidate_structural_gates",
            "axis_coverage_min": 0.80,
            "min_valid_families": MIN_VALID_FAMILIES,
            "anchor_families": {
                axis: sorted(families) for axis, families in ANCHOR_FAMILIES.items()
            },
            "dispersion_abstain": DISPERSION_ABSTAIN,
            "min_confidence": MIN_CONFIDENCE,
            "known_delta_vs_engineering_baseline_4a3c8cd": (
                "113 formerly valid days now abstain because inflation coverage is below "
                "0.80; macro scores, u and confidence are unchanged for those days."
            ),
        },
        "macro_summary": {
            "eligible_days": metrics["eligible_days"],
            "valid_rate": metrics["valid_rate"],
            "abstain_rate": metrics["abstain_rate"],
            "official_flips_per_year": metrics["official_flips_per_year"],
            "candidate_revision_change_rate": metrics["vintage_stability"].get(
                "candidate_quadrant_changed_by_revision_rate"
            ),
        },
        "market_comparison_summary": comparison_metrics,
        "market_diagnostics_summary": market_metrics,
        "freeze_status": {
            "a3": metrics["run_classification"],
            "a4": "candidate_seed_only",
            "a5": "blocked",
            "parameter_freeze": "not_approved",
        },
    }


def render_report(
    manifest: dict[str, Any],
    metrics: dict[str, Any],
    comparison_metrics: dict[str, Any],
    market_metrics: dict[str, Any],
) -> str:
    pit = metrics.get("pit_qa", {})
    stability = metrics.get("vintage_stability", {})
    input_cache = manifest.get("input_cache", {})
    l4_parity = metrics.get("l4_reference_parity", {})
    smoke_grid = metrics.get("smoke_grid", {})
    return "\n".join([
        "# Calibration Replay Report",
        "",
        f"Run: `{manifest['calibration_run_id']}`",
        f"Classification: `{manifest['run_classification']}`",
        f"Window: `{manifest['start_date']}` to `{manifest['end_date']}`",
        f"Macro config: `{manifest['macro_config']}`",
        f"Macro model: `{manifest['macro_model_version']}`",
        f"Worker commit: `{manifest['worker_commit']}`",
        f"Git dirty: `{manifest['git_dirty']}`",
        f"Input cache enabled: `{input_cache.get('enabled')}`",
        f"Input cache hit: `{input_cache.get('cache_hit')}`",
        f"Input cache key: `{input_cache.get('cache_key')}`",
        f"Feature panels: `{json.dumps(manifest.get('feature_panels', {}), sort_keys=True)}`",
        f"L4 reference parity: `{l4_parity.get('passed')}`",
        f"Smoke grid evaluations: `{smoke_grid.get('evaluation_count')}`",
        "",
        "## Status",
        "",
        "This is a read-only A3 diagnostic replay. It is not parameter freeze, does not promote A5, and does not activate runtime v0.1.",
        "",
        "## Macro Replay",
        "",
        f"- Eligible business days: {metrics['eligible_days']}",
        f"- Valid rate: {metrics['valid_rate']:.4f}",
        f"- Abstain rate: {metrics['abstain_rate']:.4f}",
        f"- Candidate flips/year: {metrics['candidate_flips_per_year']:.4f}",
        f"- Valid-published flips/year: {metrics['valid_published_flips_per_year']:.4f}",
        f"- Latched flips/year: {metrics['latched_flips_per_year']:.4f}",
        f"- Status counts: `{json.dumps(metrics['status_counts'], sort_keys=True)}`",
        f"- Candidate counts all days: `{json.dumps(metrics['candidate_quadrant_counts_all_days'], sort_keys=True)}`",
        f"- Valid published counts: `{json.dumps(metrics['valid_published_quadrant_counts'], sort_keys=True)}`",
        f"- Latched counts including abstain: `{json.dumps(metrics['latched_quadrant_counts_including_abstain'], sort_keys=True)}`",
        f"- Days without latched state: {metrics['days_without_latched_state']}",
        f"- Primary abstention reasons: `{json.dumps(metrics['abstention_reason_primary_counts'], sort_keys=True)}`",
        f"- Any abstention reasons: `{json.dumps(metrics['abstention_reason_any_counts'], sort_keys=True)}`",
        "",
        "## Vintage Stability",
        "",
        f"- Candidate comparable days: {stability.get('candidate_comparable_days')}",
        f"- Candidate changes by revision: {stability.get('candidate_quadrant_changed_by_revision_days')}",
        f"- Candidate change rate: {stability.get('candidate_quadrant_changed_by_revision_rate')}",
        f"- Status changes by revision: {stability.get('status_changed_by_revision_days')}",
        f"- Latched changes by revision: {stability.get('latched_quadrant_changed_by_revision_days')}",
        "",
        "## PIT QA",
        "",
        f"- Vintage rows: {pit.get('row_count')}",
        f"- Duplicate PIT keys: {pit.get('unique_key_duplicate_count')}",
        f"- Repeat read idempotent: {pit.get('idempotent_repeat_read')}",
        f"- Synthetic future revision no effect: {pit.get('synthetic_future_revision_no_effect')}",
        f"- Selected future observations: {pit.get('selected_future_observation_count')}",
        "",
        "## Market Comparison",
        "",
        f"- Source: `{comparison_metrics['market_source']}`",
        f"- Common dates: {comparison_metrics['common_dates']}",
        f"- Both valid dates: {comparison_metrics['both_valid_dates']}",
        f"- Exact agreement rate: {comparison_metrics['exact_quadrant_agreement_rate']}",
        f"- Candidate common dates: {comparison_metrics['candidate_common_dates']}",
        f"- Candidate exact agreement rate: {comparison_metrics['candidate_exact_quadrant_agreement_rate']}",
        f"- Macro-valid / market-abstain rate: {comparison_metrics['macro_valid_market_abstain_rate']}",
        f"- Market-valid / macro-abstain rate: {comparison_metrics['market_valid_macro_abstain_rate']}",
        f"- Market eligible days: {market_metrics['market_eligible_days']}",
        f"- Market status counts: `{json.dumps(market_metrics['market_status_counts'], sort_keys=True)}`",
        f"- Market reason counts: `{json.dumps(market_metrics['market_status_reason_any_counts'], sort_keys=True)}`",
        f"- Market candidate counts: `{json.dumps(market_metrics['market_candidate_quadrant_counts'], sort_keys=True)}`",
        "",
        "## A4/A5 Guard",
        "",
        "A4 remains candidate seed only. A5 remains blocked. The A3 grid must not start until the diagnostic gates above are understood.",
        "",
    ])


def candidate_constants() -> dict[str, Any]:
    return {
        "u_floor": U_FLOOR,
        "min_confidence": MIN_CONFIDENCE,
        "growth_enter": GROWTH_ENTER,
        "inflation_enter": INFLATION_ENTER,
        "axis_exit": AXIS_EXIT,
        "series_z_clip": SERIES_Z_CLIP,
        "family_score_clip": FAMILY_SCORE_CLIP,
        "axis_score_clip": AXIS_SCORE_CLIP,
        "dispersion_abstain": DISPERSION_ABSTAIN,
        "family_weights": FAMILY_WEIGHTS,
        "series": [asdict(s) for s in BASELINE_SERIES],
    }


def collect_environment_metadata(repo_root: Path) -> dict[str, Any]:
    return {
        "python_version": sys.version.split()[0],
        "python_executable": sys.executable,
        "pyarrow_version": package_version("pyarrow"),
        "pandas_version": package_version("pandas"),
        "git_head": run_text(["git", "rev-parse", "HEAD"], cwd=repo_root),
        "git_dirty": bool(run_text(["git", "status", "--porcelain"], cwd=repo_root)),
        "requirements_txt_sha256": hash_file(repo_root / "requirements.txt"),
        "pip_freeze_sha256": stable_hash(
            run_text([sys.executable, "-m", "pip", "freeze"], cwd=repo_root)
        ),
    }


def package_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def run_text(command: list[str], *, cwd: Path) -> str:
    try:
        proc = subprocess.run(
            command,
            cwd=str(cwd),
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def hash_artifacts(base: Path, names: list[str]) -> dict[str, str]:
    return {name: hash_file(base / name) for name in names}


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def logical_records_hash(rows: list[dict[str, Any]]) -> str:
    columns = sorted({key for row in rows for key in row})
    normalized_rows = [
        {column: normalize_logical_value(row.get(column)) for column in columns}
        for row in rows
    ]
    normalized_rows.sort(key=lambda row: json.dumps(row, sort_keys=True, default=str))
    return stable_hash({
        "schema": columns,
        "rows": normalized_rows,
    })


def logical_payload_hash(payload: Any) -> str:
    return stable_hash(normalize_logical_value(payload))


def normalize_logical_value(value: Any) -> Any:
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except (AttributeError, ValueError, TypeError):
            pass
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    elif hasattr(value, "to_pydatetime64"):
        value = str(value)
    if isinstance(value, dict):
        return {str(k): normalize_logical_value(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [normalize_logical_value(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_logical_value(item) for item in value]
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if math.isinf(value):
            return str(value)
        return round(value, 12)
    return value


def write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a project dependency
        raise RuntimeError("pandas is required to write calibration parquet artifacts") from exc
    try:
        frame = pd.DataFrame(rows)
        frame.to_parquet(path, index=False)
    except ImportError as exc:
        raise RuntimeError(
            "Parquet output requires pyarrow or fastparquet; install project requirements"
        ) from exc


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(to_yaml(payload), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def to_yaml(value: Any, indent: int = 0) -> str:
    pad = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                lines.append(f"{pad}{key}:")
                lines.append(to_yaml(item, indent + 2))
            else:
                lines.append(f"{pad}{key}: {yaml_scalar(item)}")
        return "\n".join(lines) + ("\n" if indent == 0 else "")
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, (dict, list)):
                lines.append(f"{pad}-")
                lines.append(to_yaml(item, indent + 2))
            else:
                lines.append(f"{pad}- {yaml_scalar(item)}")
        return "\n".join(lines)
    return f"{pad}{yaml_scalar(value)}"


def yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if not text or any(ch in text for ch in ":#[]{}&,*!?|>'\"%@`"):
        return json.dumps(text)
    return text


def count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        out[str(value)] = out.get(str(value), 0) + 1
    return out


def count_reason_groups(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        for group in reason_groups(row.get("status_reasons_all") or row.get("status_reasons")):
            out[group] = out.get(group, 0) + 1
    return out


def flips_for_key(rows: list[dict[str, Any]], key: str) -> int:
    values = [row.get(key) for row in rows]
    return sum(
        1
        for prev, cur in zip(values, values[1:])
        if prev is not None and cur is not None and prev != cur
    )


def classify_baseline_run(
    *,
    valid_rate: float,
    abstain_rate: float,
    revision_change_rate: float | None,
) -> str:
    if not (MIN_VALID_RATE_FREEZE <= valid_rate <= MAX_VALID_RATE_FREEZE):
        return RUN_CLASSIFICATION_FAILED
    if not (MIN_ABSTAIN_RATE_FREEZE <= abstain_rate <= MAX_ABSTAIN_RATE_FREEZE):
        return RUN_CLASSIFICATION_FAILED
    if revision_change_rate is None or revision_change_rate > MAX_REVISION_CHANGE_RATE_FREEZE:
        return RUN_CLASSIFICATION_FAILED
    return RUN_CLASSIFICATION_PASSED


def distribution(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "p10": None, "median": None, "p90": None, "max": None}
    sorted_values = sorted(values)
    return {
        "min": sorted_values[0],
        "p10": percentile(sorted_values, 0.10),
        "median": percentile(sorted_values, 0.50),
        "p90": percentile(sorted_values, 0.90),
        "max": sorted_values[-1],
    }


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        raise ValueError("percentile requires values")
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = p * (len(sorted_values) - 1)
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return sorted_values[lo]
    frac = idx - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def state_durations(
    rows: list[dict[str, Any]], key: str, *, false_value: Any | None = None
) -> list[int]:
    durations: list[int] = []
    current: Any = None
    length = 0
    for row in rows:
        value = row.get(key)
        if false_value is not None:
            value = false_value if value is false_value else None
        if value is None:
            if length:
                durations.append(length)
                length = 0
            current = None
            continue
        if value == current:
            length += 1
        else:
            if length:
                durations.append(length)
            current = value
            length = 1
    if length:
        durations.append(length)
    return durations


def duration_summary(values: list[int]) -> dict[str, float | None]:
    return distribution([float(v) for v in values])


def transition_dates(rows: list[dict[str, Any]], key: str) -> list[str]:
    out: list[str] = []
    prev = None
    for row in rows:
        current = row.get(key)
        if prev is not None and current is not None and current != prev:
            out.append(row["date"])
        if current is not None:
            prev = current
    return out


def nearest_transition_lags(macro_dates: list[str], market_dates: list[str]) -> dict[str, Any]:
    if not macro_dates or not market_dates:
        return {"count": 0, "median_days": None, "lags_days": []}
    market = [dt.date.fromisoformat(d) for d in market_dates]
    lags: list[int] = []
    for m in [dt.date.fromisoformat(d) for d in macro_dates]:
        nearest = min(market, key=lambda k: abs((k - m).days))
        lags.append((nearest - m).days)
    return {
        "count": len(lags),
        "median_days": statistics.median(lags),
        "lags_days": lags,
    }


def sign_of(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number > 0:
        return 1
    if number < 0:
        return -1
    return 0


def numeric_delta(lhs: Any, rhs: Any) -> float | None:
    if lhs is None or rhs is None:
        return None
    return float(lhs) - float(rhs)


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _spot(kind: str, row: VintageRow) -> dict[str, Any]:
    return {
        "kind": kind,
        "series_id": row.series_id,
        "observation_period": row.observation_period.isoformat(),
        "vintage_date": row.vintage_date.isoformat(),
        "available_at": row.available_at.isoformat(),
        "revision_number": row.revision_number,
    }


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _float_or_none(value: Any) -> float | None:
    return None if value is None else float(value)


def parse_a31_grid_args(argv: list[str]) -> A31GridConfig:
    ap = argparse.ArgumentParser(description="Run offline A3.1 grid over materialized L2")
    ap.add_argument("command", choices=["a31-grid"])
    ap.add_argument("--feature-manifest", required=True)
    ap.add_argument("--config-catalog", required=True)
    ap.add_argument("--output-dir")
    ap.add_argument("--jobs", type=int, default=1)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--worker-commit")
    args = ap.parse_args(argv)
    if args.jobs < 1:
        raise SystemExit("--jobs must be >= 1")
    return A31GridConfig(
        feature_manifest=Path(args.feature_manifest),
        config_catalog=Path(args.config_catalog),
        output_dir=Path(args.output_dir) if args.output_dir else None,
        jobs=args.jobs,
        resume=args.resume,
        offline=args.offline,
        worker_commit=args.worker_commit,
    )


def run_a31_grid(config: A31GridConfig) -> dict[str, Any]:
    if not config.offline:
        raise SystemExit("a31-grid requires --offline to make the no-external-access contract explicit")
    started = dt.datetime.now(UTC)
    execution_id = str(uuid.uuid4())
    output_dir = config.output_dir or (config.feature_manifest.parent / "a31_grid")
    results_dir = output_dir / "results"
    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    feature_manifest, l2_path, l2_hash, l2_records = load_l2_macro_from_feature_manifest(
        config.feature_manifest
    )
    catalog_payload = read_catalog_payload(config.config_catalog)
    normalized_catalog, catalog_hash = normalize_a31_catalog(
        catalog_payload,
        l2_macro_logical_hash=l2_hash,
        source_path=config.config_catalog,
    )
    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    git_dirty = bool(run_text(
        ["git", "status", "--porcelain"], cwd=Path(__file__).resolve().parents[1]
    ))
    run_fingerprint = a31_grid_run_fingerprint(
        l2_macro_logical_hash=l2_hash,
        config_catalog_hash=catalog_hash,
        worker_commit=worker_commit,
    )
    write_json(output_dir / "config_catalog.normalized.json", normalized_catalog)
    write_parquet(
        output_dir / "configs.parquet",
        [
            {
                "a31_config_hash": item["a31_config_hash"],
                "a31_config_name": item["config"]["name"],
                "distance_from_ref": item["distance_from_ref"],
                "catalog_index": item["catalog_index"],
                "round": item["metadata"].get("round"),
                "description": item["metadata"].get("description"),
                "config_json": json.dumps(item["config"], sort_keys=True),
                "metadata_json": json.dumps(item["metadata"], sort_keys=True),
            }
            for item in normalized_catalog["configs"]
        ],
    )

    a32_ref = reference_a32_config()
    existing_summary: list[dict[str, Any]] = []
    existing_metrics: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    skipped = 0
    for item in normalized_catalog["configs"]:
        a31_hash = str(item["a31_config_hash"])
        result_dir = results_dir / a31_hash
        if config.resume:
            loaded = load_existing_a31_result(
                result_dir,
                expected_l2_hash=l2_hash,
                expected_a31_hash=a31_hash,
                expected_a32_hash=a32_config_hash(a32_ref, a31_hash),
            )
            if loaded is not None:
                existing_summary.append(loaded["summary"])
                existing_metrics.extend(loaded["metrics_rows"])
                skipped += 1
                continue
        tasks.append({
            "feature_manifest_path": str(config.feature_manifest),
            "l2_path": str(l2_path),
            "l2_macro_logical_hash": l2_hash,
            "result_dir": str(result_dir),
            "execution_id": execution_id,
            "worker_commit": worker_commit,
            "a31_item": item,
        })

    computed: list[dict[str, Any]] = []
    if config.jobs == 1:
        for task in tasks:
            try:
                computed.append(a31_grid_worker(task))
            except Exception as exc:  # pragma: no cover - exercised by CLI failures
                failures.append(a31_failure_record(task, exc))
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=config.jobs) as pool:
            futures = {pool.submit(a31_grid_worker, task): task for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                task = futures[future]
                try:
                    computed.append(future.result())
                except Exception as exc:  # pragma: no cover - defensive per-config isolation
                    failures.append(a31_failure_record(task, exc))

    summary_rows = existing_summary + [row["summary"] for row in computed]
    metric_rows = existing_metrics + [
        metric for row in computed for metric in row["metrics_rows"]
    ]
    summary_rows.sort(key=lambda row: str(row["a31_config_hash"]))
    metric_rows.sort(key=lambda row: (str(row["a31_config_hash"]), str(row["fold"])))
    summary_rows, pareto_rows = mark_a31_pareto(summary_rows)

    write_parquet(output_dir / "a31_grid_summary.parquet", summary_rows)
    write_parquet(output_dir / "a31_grid_metrics.parquet", metric_rows)
    write_parquet(output_dir / "a31_pareto.parquet", pareto_rows)
    write_json(output_dir / "failures.json", {"failures": failures})

    finished = dt.datetime.now(UTC)
    grid_manifest = {
        "schema_version": A31_GRID_SCHEMA_VERSION,
        "run_fingerprint": run_fingerprint,
        "execution_id": execution_id,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "jobs": config.jobs,
        "resume": config.resume,
        "resume_skipped_configs": skipped,
        "offline": True,
        "external_access": "disabled_by_grid_only_path",
        "grid_only": True,
        "skipped_stages": ["DB", "Tiingo", "L0", "L1", "L2"],
        "worker_commit": worker_commit,
        "git_dirty": git_dirty,
        "feature_manifest_path": str(config.feature_manifest),
        "config_catalog_path": str(config.config_catalog),
        "parent_hashes": {
            "l2_macro_logical_hash": l2_hash,
            "l2_schema_version": feature_manifest.get("schema_version"),
            "business_date_calendar_hash": feature_manifest.get("business_date_calendar_hash"),
            "series_family_mapping_hash": feature_manifest.get("series_family_mapping_hash"),
        },
        "config_catalog_hash": catalog_hash,
        "a32_ref_hashes": sorted({row["a32_config_hash"] for row in summary_rows}),
        "config_count": len(normalized_catalog["configs"]),
        "completed_count": len(summary_rows),
        "failure_count": len(failures),
        "summary_logical_hash": logical_records_hash(summary_rows),
        "metrics_logical_hash": logical_records_hash(metric_rows),
        "pareto_logical_hash": logical_records_hash(pareto_rows),
        "artifact_hashes": hash_artifacts(
            output_dir,
            [
                "config_catalog.normalized.json",
                "configs.parquet",
                "a31_grid_summary.parquet",
                "a31_grid_metrics.parquet",
                "a31_pareto.parquet",
                "failures.json",
            ],
        ),
        "stage_timings_seconds": aggregate_stage_timings(summary_rows),
        "notes": [
            "A3.1 uses A32-REF only",
            "No configuration is frozen, production_candidate, or activation_ready",
            "A4 remains candidate_seed_only; A5 remains blocked",
        ],
    }
    write_json(output_dir / "grid_manifest.json", grid_manifest)
    return {
        "status": "ok" if not failures else "partial",
        "output_dir": str(output_dir),
        "run_fingerprint": run_fingerprint,
        "execution_id": execution_id,
        "config_count": len(normalized_catalog["configs"]),
        "completed_count": len(summary_rows),
        "failure_count": len(failures),
        "resume_skipped_configs": skipped,
        "pareto_count": len(pareto_rows),
        "summary_logical_hash": grid_manifest["summary_logical_hash"],
        "metrics_logical_hash": grid_manifest["metrics_logical_hash"],
    }


def load_l2_macro_from_feature_manifest(
    feature_manifest_path: Path,
) -> tuple[dict[str, Any], Path, str, list[dict[str, Any]]]:
    manifest = read_json_dict(feature_manifest_path)
    if not manifest.get("parameter_independent"):
        raise ValueError("feature_manifest must be parameter_independent=true for a31-grid")
    if manifest.get("counterfactual_runtime_allowed"):
        raise ValueError("feature_manifest must forbid counterfactual runtime use")
    roles = manifest.get("selection_roles") or {}
    if roles.get("latest") != "pit_runtime_candidate":
        raise ValueError("feature_manifest.latest must be pit_runtime_candidate")
    if roles.get("first_release") != "revised_vintage_counterfactual":
        raise ValueError("feature_manifest.first_release must be revised_vintage_counterfactual")
    macro_meta = manifest.get("macro_feature_primitives") or {}
    expected_hash = str(macro_meta.get("logical_hash") or "")
    if not expected_hash:
        raise ValueError("feature_manifest is missing macro_feature_primitives.logical_hash")
    l2_path = feature_manifest_path.parent / "macro_feature_primitives.parquet"
    rows = read_parquet_records(l2_path)
    actual_hash = logical_records_hash(rows)
    validate_parent_hash("a31-grid L2 macro_feature_primitives", actual_hash, expected_hash)
    if macro_meta.get("row_count") is not None and int(macro_meta["row_count"]) != len(rows):
        raise ValueError("macro_feature_primitives row_count mismatch")
    return manifest, l2_path, actual_hash, rows


def read_parquet_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a project dependency
        raise RuntimeError("pandas is required to read calibration parquet artifacts") from exc
    frame = pd.read_parquet(path)
    return [
        {str(key): parquet_cell(value) for key, value in record.items()}
        for record in frame.to_dict("records")
    ]


def parquet_cell(value: Any) -> Any:
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except (AttributeError, ValueError, TypeError):
            pass
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, dt.datetime):
        if value.time() == dt.time(0, 0):
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def read_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def read_catalog_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError("PyYAML is required for YAML A31 config catalogs") from exc
        payload = yaml.safe_load(text)
    if isinstance(payload, list):
        payload = {"configs": payload}
    if not isinstance(payload, dict):
        raise ValueError("A31 config catalog must be a mapping or list")
    if "configs" not in payload:
        raise ValueError("A31 config catalog must contain configs")
    return payload


def normalize_a31_catalog(
    payload: dict[str, Any], *, l2_macro_logical_hash: str, source_path: Path
) -> tuple[dict[str, Any], str]:
    configs_payload = payload.get("configs")
    if not isinstance(configs_payload, list) or not configs_payload:
        raise ValueError("A31 config catalog configs must be a non-empty list")
    normalized_configs: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    for idx, raw in enumerate(configs_payload):
        if not isinstance(raw, dict):
            raise ValueError("each A31 config catalog entry must be a mapping")
        cfg, metadata = a31_config_from_catalog_entry(raw)
        config_hash = a31_config_hash(cfg, l2_macro_logical_hash)
        if config_hash in seen_hashes:
            raise ValueError(f"duplicate A31 config hash in catalog: {config_hash}")
        seen_hashes.add(config_hash)
        normalized_configs.append({
            "catalog_index": idx,
            "a31_config_hash": config_hash,
            "config": asdict(cfg),
            "metadata": metadata,
            "distance_from_ref": a31_distance_from_ref(cfg),
        })
    normalized = {
        "schema_version": A31_GRID_SCHEMA_VERSION,
        "source_path": str(source_path),
        "selection_policy": "A3.1 screening only; A32-REF fixed; no freeze",
        "config_count": len(normalized_configs),
        "configs": normalized_configs,
    }
    return normalized, logical_payload_hash(normalized)


def a31_config_from_catalog_entry(entry: dict[str, Any]) -> tuple[A31Config, dict[str, Any]]:
    ref = reference_a31_config()
    data = json.loads(json.dumps(asdict(ref)))
    name = str(entry.get("name") or "").strip()
    if not name:
        raise ValueError("A31 catalog entry missing name")
    if entry.get("extends") not in {None, "A31-REF", "ref", "reference"}:
        raise ValueError(f"unsupported A31 extends: {entry.get('extends')}")
    data["name"] = name
    simple_fields = {
        "aggregation_method",
        "robust_clip",
        "reliability_weighting",
        "release_smoothing",
        "family_weights",
        "series_weights",
    }
    for field in simple_fields:
        if field in entry:
            data[field] = json.loads(json.dumps(entry[field]))
    if "transformation_weights" in entry:
        merged_transforms = json.loads(json.dumps(data["transformation_weights"]))
        for transform, weights in dict(entry["transformation_weights"]).items():
            merged = dict(merged_transforms.get(transform, {}))
            merged.update(weights)
            merged_transforms[transform] = merged
        data["transformation_weights"] = merged_transforms
    if "score_clip" in entry:
        merged = dict(data["score_clip"])
        merged.update(entry["score_clip"])
        data["score_clip"] = merged
    shifts = entry.get("family_weight_shifts") or entry.get("family_weight_shift")
    if shifts:
        if isinstance(shifts, dict):
            shifts = [shifts]
        shifts = canonicalize_shifts(shifts)
        apply_family_weight_shifts_to_data(data, shifts)
    series_shifts = entry.get("series_weight_shifts") or entry.get("series_weight_shift")
    if series_shifts:
        if isinstance(series_shifts, dict):
            series_shifts = [series_shifts]
        series_shifts = canonicalize_shifts(series_shifts)
        apply_series_weight_shifts_to_data(data, series_shifts)
    cfg = A31Config(
        name=str(data["name"]),
        transformation_weights={
            str(k): {str(kk): float(vv) for kk, vv in dict(v).items()}
            for k, v in dict(data["transformation_weights"]).items()
        },
        family_weights={
            str(k): {str(kk): float(vv) for kk, vv in dict(v).items()}
            for k, v in dict(data["family_weights"]).items()
        },
        series_weights={str(k): float(v) for k, v in dict(data["series_weights"]).items()},
        aggregation_method=str(data["aggregation_method"]),
        robust_clip=float(data["robust_clip"]),
        reliability_weighting=str(data["reliability_weighting"]),
        score_clip={str(k): float(v) for k, v in dict(data["score_clip"]).items()},
        release_smoothing=str(data["release_smoothing"]),
    )
    metadata = {
        key: normalize_logical_value(value)
        for key, value in entry.items()
        if key not in {
            "name",
            "extends",
            "aggregation_method",
            "robust_clip",
            "reliability_weighting",
            "release_smoothing",
            "transformation_weights",
            "family_weights",
            "series_weights",
            "score_clip",
            "family_weight_shift",
            "family_weight_shifts",
            "series_weight_shift",
            "series_weight_shifts",
        }
    }
    if shifts:
        metadata["family_weight_shifts"] = normalize_logical_value(shifts)
    if series_shifts:
        metadata["series_weight_shifts"] = normalize_logical_value(series_shifts)
    metadata["resolved_family_weights"] = normalize_logical_value(cfg.family_weights)
    metadata["resolved_series_weights"] = normalize_logical_value(cfg.series_weights)
    return cfg, metadata


def canonicalize_shifts(shifts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for shift in shifts:
        current = dict(shift)
        if "recipients" in current:
            current["recipients"] = sorted(str(item) for item in current["recipients"])
        out.append(current)
    return out


def apply_family_weight_shifts_to_data(data: dict[str, Any], shifts: list[dict[str, Any]]) -> None:
    by_axis: dict[str, list[dict[str, Any]]] = {}
    for shift in shifts:
        by_axis.setdefault(str(shift["axis"]), []).append(shift)
    for axis, axis_shifts in by_axis.items():
        base_weights = {family: float(weight) for family, weight in data["family_weights"][axis].items()}
        deltas = {family: 0.0 for family in base_weights}
        for shift in axis_shifts:
            source = str(shift["source"])
            if source not in base_weights:
                raise ValueError(f"unknown source family for shift: {axis}.{source}")
            delta = min(0.05, abs(float(shift.get("delta", 0.05))))
            delta = min(delta, base_weights[source] + deltas[source])
            recipients = [str(item) for item in shift.get("recipients", [])]
            if not recipients:
                recipients = sorted(family for family in base_weights if family != source)
            if not recipients:
                continue
            for family in recipients:
                if family not in base_weights:
                    raise ValueError(f"unknown recipient family for shift: {axis}.{family}")
            deltas[source] -= delta
            add = delta / len(recipients)
            for family in recipients:
                deltas[family] += add
        resolved = {family: base_weights[family] + deltas[family] for family in base_weights}
        if any(value <= 0.0 for value in resolved.values()):
            raise ValueError(f"family weight shift produced non-positive weight for axis {axis}")
        total = sum(resolved.values())
        data["family_weights"][axis] = {
            family: resolved[family] / total for family in sorted(resolved)
        }


def apply_series_weight_shifts_to_data(data: dict[str, Any], shifts: list[dict[str, Any]]) -> None:
    by_group: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for shift in shifts:
        group = tuple(sorted({str(shift["source"]), *(str(item) for item in shift.get("recipients", []))}))
        by_group.setdefault(group, []).append(shift)
    for group, group_shifts in by_group.items():
        base = {series_id: float(data["series_weights"][series_id]) for series_id in group}
        total_base = sum(base.values())
        shares = {series_id: weight / total_base for series_id, weight in base.items()}
        deltas = {series_id: 0.0 for series_id in shares}
        for shift in group_shifts:
            source = str(shift["source"])
            if source not in shares:
                raise ValueError(f"unknown source series for shift: {source}")
            delta = min(0.05, abs(float(shift.get("delta", 0.05))))
            delta = min(delta, shares[source] + deltas[source] - 0.01)
            recipients = [str(item) for item in shift.get("recipients", []) if str(item) != source]
            if not recipients:
                raise ValueError(f"series shift for {source} requires recipients")
            for series_id in recipients:
                if series_id not in shares:
                    raise ValueError(f"unknown recipient series for shift: {series_id}")
            deltas[source] -= delta
            add = delta / len(recipients)
            for series_id in recipients:
                deltas[series_id] += add
        resolved = {series_id: shares[series_id] + deltas[series_id] for series_id in shares}
        if any(value <= 0.0 for value in resolved.values()):
            raise ValueError(f"series weight shift produced non-positive weight in group {group}")
        for series_id, share in resolved.items():
            data["series_weights"][series_id] = share * total_base


def a31_distance_from_ref(config: A31Config) -> float:
    ref = reference_a31_config()
    distance = 0.0
    for axis, weights in ref.family_weights.items():
        for family, weight in weights.items():
            distance += abs(config.family_weights.get(axis, {}).get(family, 0.0) - weight)
    for series_id, weight in ref.series_weights.items():
        distance += abs(config.series_weights.get(series_id, 0.0) - weight)
    for key, value in ref.score_clip.items():
        distance += abs(config.score_clip.get(key, 0.0) - value) / 10.0
    for transform, weights in ref.transformation_weights.items():
        for component, weight in weights.items():
            distance += abs(
                config.transformation_weights.get(transform, {}).get(component, 0.0) - weight
            )
    distance += 0.01 if config.aggregation_method != ref.aggregation_method else 0.0
    distance += abs(config.robust_clip - ref.robust_clip) / 10.0
    distance += 0.01 if config.reliability_weighting != ref.reliability_weighting else 0.0
    distance += 0.01 if config.release_smoothing != ref.release_smoothing else 0.0
    return round(distance, 12)


def a31_grid_run_fingerprint(
    *, l2_macro_logical_hash: str, config_catalog_hash: str, worker_commit: str
) -> str:
    return stable_hash({
        "parent_l2_hash": l2_macro_logical_hash,
        "config_catalog_hash": config_catalog_hash,
        "worker_commit": worker_commit,
        "schema_versions": {
            "l2": L2_SCHEMA_VERSION,
            "l3": L3_SCORER_SCHEMA_VERSION,
            "l4": L4_STATE_SCHEMA_VERSION,
            "a31_grid": A31_GRID_SCHEMA_VERSION,
        },
        "code_versions": {
            "l3": L3_SCORER_CODE_VERSION,
            "l4": L4_STATE_CODE_VERSION,
        },
    })


def a31_grid_worker(task: dict[str, Any]) -> dict[str, Any]:
    timings: dict[str, float] = {}
    t0 = time.perf_counter()
    l2_records = read_parquet_records(Path(task["l2_path"]))
    l2_hash = logical_records_hash(l2_records)
    validate_parent_hash(
        "a31-grid worker L2 macro_feature_primitives",
        l2_hash,
        str(task["l2_macro_logical_hash"]),
    )
    timings["load_l2"] = time.perf_counter() - t0

    item = task["a31_item"]
    a31 = A31Config(**item["config"])
    a31_hash = str(item["a31_config_hash"])
    a32 = reference_a32_config()
    a32_hash = a32_config_hash(a32, a31_hash)
    result_dir = Path(task["result_dir"])
    temp_dir = result_dir.with_name(f"{result_dir.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=False)

    try:
        t0 = time.perf_counter()
        l3_rows, contribution_rows, l3_manifest = build_l3_score_panel(
            l2_records,
            a31,
            l2_macro_logical_hash=l2_hash,
            expected_l2_macro_logical_hash=str(task["l2_macro_logical_hash"]),
        )
        timings["compute_l3"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        write_parquet(temp_dir / "l3_score_panel.parquet", l3_rows)
        write_parquet(temp_dir / "l3_contributions.parquet", contribution_rows)
        write_json(temp_dir / "l3_manifest.json", l3_manifest)
        timings["write_l3"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        runtime, l4_runtime_manifest = run_l4_state_machine(
            l3_rows, a32, selection_mode="latest"
        )
        counterfactual, l4_counterfactual_manifest = run_l4_state_machine(
            l3_rows, a32, selection_mode="first_release"
        )
        timings["run_l4"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        metrics_full = build_macro_metrics(runtime, first_release_replay=counterfactual)
        classification = classify_a31_grid_result(metrics_full)
        metric_rows = evaluation_metric_rows(
            runtime,
            counterfactual,
            a31,
            a32,
            a31_hash,
            a32_hash,
            classification,
        )
        summary = a31_grid_summary_row(
            item,
            a32_hash,
            metrics_full,
            metric_rows,
            timings,
            classification,
            result_dir,
        )
        timings["compute_metrics"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        write_parquet(temp_dir / "l4_replay_a32_ref.parquet", runtime)
        write_parquet(temp_dir / "l4_counterfactual_a32_ref.parquet", counterfactual)
        write_json(temp_dir / "metrics_full.json", metrics_full)
        write_json(temp_dir / "metrics_by_fold.json", {"rows": metric_rows})
        persisted_hashes = {
            "score_panel_logical_hash": logical_records_hash(
                read_parquet_records(temp_dir / "l3_score_panel.parquet")
            ),
            "contribution_logical_hash": logical_records_hash(
                read_parquet_records(temp_dir / "l3_contributions.parquet")
            ),
            "replay_logical_hash": logical_records_hash(
                read_parquet_records(temp_dir / "l4_replay_a32_ref.parquet")
            ),
            "counterfactual_replay_logical_hash": logical_records_hash(
                read_parquet_records(temp_dir / "l4_counterfactual_a32_ref.parquet")
            ),
        }
        result_manifest = {
            "schema_version": A31_GRID_SCHEMA_VERSION,
            "parent_l2_logical_hash": l2_hash,
            "a31_config_hash": a31_hash,
            "a31_config": asdict(a31),
            "a32_ref_hash": a32_hash,
            "a32_ref_config": asdict(a32),
            **persisted_hashes,
            "metrics_logical_hash": logical_payload_hash(metrics_full),
            "metrics_by_fold_logical_hash": logical_records_hash(metric_rows),
            "worker_commit": task["worker_commit"],
            "execution_id": task["execution_id"],
            "counterfactual_only_flags": {
                "runtime_counterfactual_only_values": sorted({
                    str(row.get("counterfactual_only")) for row in runtime
                }),
                "counterfactual_counterfactual_only_values": sorted({
                    str(row.get("counterfactual_only")) for row in counterfactual
                }),
                "counterfactual_runtime_allowed": False,
            },
            "selection_roles": {
                "latest": "pit_runtime_candidate",
                "first_release": "revised_vintage_counterfactual",
            },
            "l4_runtime_manifest": l4_runtime_manifest,
            "l4_counterfactual_manifest": l4_counterfactual_manifest,
            "timings_seconds": timings,
            "artifacts": {
                "l3_score_panel.parquet": hash_file(temp_dir / "l3_score_panel.parquet"),
                "l3_contributions.parquet": hash_file(temp_dir / "l3_contributions.parquet"),
                "l4_replay_a32_ref.parquet": hash_file(temp_dir / "l4_replay_a32_ref.parquet"),
                "l4_counterfactual_a32_ref.parquet": hash_file(
                    temp_dir / "l4_counterfactual_a32_ref.parquet"
                ),
                "metrics_full.json": hash_file(temp_dir / "metrics_full.json"),
                "metrics_by_fold.json": hash_file(temp_dir / "metrics_by_fold.json"),
            },
        }
        summary.update({
            "score_panel_logical_hash": result_manifest["score_panel_logical_hash"],
            "replay_logical_hash": result_manifest["replay_logical_hash"],
            "counterfactual_replay_logical_hash": result_manifest[
                "counterfactual_replay_logical_hash"
            ],
            "metrics_logical_hash": result_manifest["metrics_logical_hash"],
            "metrics_by_fold_logical_hash": result_manifest["metrics_by_fold_logical_hash"],
        })
        write_json(temp_dir / "result_manifest.json", result_manifest)
        timings["write_artifacts"] = time.perf_counter() - t0
        result_manifest["timings_seconds"] = timings
        write_json(temp_dir / "result_manifest.json", result_manifest)
        summary.update(timing_summary_fields(timings))
        write_json(temp_dir / "result_summary.json", summary)

        if result_dir.exists():
            shutil.rmtree(result_dir)
        temp_dir.rename(result_dir)
        return {
            "summary": summary,
            "metrics_rows": metric_rows,
        }
    except Exception:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        raise


def classify_a31_grid_result(metrics: dict[str, Any]) -> str:
    if metrics.get("run_classification") == RUN_CLASSIFICATION_FAILED:
        return "diagnostic_failed"
    stability = metrics.get("vintage_stability") or {}
    if (
        stability.get("candidate_quadrant_changed_by_revision_rate") is not None
        and stability["candidate_quadrant_changed_by_revision_rate"]
        > MAX_REVISION_CHANGE_RATE_FREEZE
    ):
        return "diagnostic_failed"
    return "a31_candidate"


def a31_grid_summary_row(
    item: dict[str, Any],
    a32_hash: str,
    metrics: dict[str, Any],
    metric_rows: list[dict[str, Any]],
    timings: dict[str, float],
    classification: str,
    result_dir: Path,
) -> dict[str, Any]:
    full = next(row for row in metric_rows if row["fold"] == "full")
    stability = metrics.get("vintage_stability") or {}
    return {
        "a31_config_hash": item["a31_config_hash"],
        "a31_config_name": item["config"]["name"],
        "a32_config_hash": a32_hash,
        "a32_config_name": "A32-REF",
        "catalog_index": item["catalog_index"],
        "round": item["metadata"].get("round"),
        "description": item["metadata"].get("description"),
        "distance_from_ref": item["distance_from_ref"],
        "result_classification": classification,
        "a31_selection_status": "a31_screened_out",
        "candidate_revision_change_rate": full["candidate_revision_change_rate"],
        "growth_sign_revision_change_days": full["growth_sign_revision_change_days"],
        "inflation_sign_revision_change_days": full["inflation_sign_revision_change_days"],
        "status_revision_change_days": full["status_revision_change_days"],
        "latched_revision_change_days": full["latched_revision_change_days"],
        "transition_timing_displacement": full["transition_timing_displacement"],
        "transition_timing_displacement_median": transition_displacement_median(
            full["transition_timing_displacement"]
        ),
        "candidate_flips_per_year": full["candidate_flips_per_year"],
        "published_flips_per_year": full["published_flips_per_year"],
        "valid_rate": full["valid_rate"],
        "abstain_rate": full["abstain_rate"],
        "days_without_latched_state": full["days_without_latched_state"],
        "candidate_quadrant_changed_by_revision_days": stability.get(
            "candidate_quadrant_changed_by_revision_days"
        ),
        "frozen": False,
        "production_candidate": False,
        "activation_ready": False,
        "result_dir": str(result_dir),
        **timing_summary_fields(timings),
    }


def transition_displacement_median(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    payload = json.loads(str(value)) if isinstance(value, str) else value
    median = payload.get("median") if isinstance(payload, dict) else None
    return None if median is None else float(median)


def timing_summary_fields(timings: dict[str, float]) -> dict[str, float]:
    keys = ["load_l2", "compute_l3", "write_l3", "run_l4", "compute_metrics", "write_artifacts"]
    return {f"timing_{key}_seconds": round(float(timings.get(key, 0.0)), 6) for key in keys}


def load_existing_a31_result(
    result_dir: Path,
    *,
    expected_l2_hash: str,
    expected_a31_hash: str,
    expected_a32_hash: str,
) -> dict[str, Any] | None:
    required = [
        "l3_score_panel.parquet",
        "l3_contributions.parquet",
        "l3_manifest.json",
        "l4_replay_a32_ref.parquet",
        "l4_counterfactual_a32_ref.parquet",
        "metrics_full.json",
        "metrics_by_fold.json",
        "result_manifest.json",
        "result_summary.json",
    ]
    if not result_dir.exists() or not all((result_dir / name).exists() for name in required):
        return None
    manifest = read_json_dict(result_dir / "result_manifest.json")
    if manifest.get("parent_l2_logical_hash") != expected_l2_hash:
        return None
    if manifest.get("a31_config_hash") != expected_a31_hash:
        return None
    if manifest.get("a32_ref_hash") != expected_a32_hash:
        return None
    l3_rows = read_parquet_records(result_dir / "l3_score_panel.parquet")
    runtime = read_parquet_records(result_dir / "l4_replay_a32_ref.parquet")
    counterfactual = read_parquet_records(result_dir / "l4_counterfactual_a32_ref.parquet")
    metrics_payload = read_json_dict(result_dir / "metrics_full.json")
    metrics_rows_payload = read_json_dict(result_dir / "metrics_by_fold.json")
    metrics_rows = list(metrics_rows_payload.get("rows") or [])
    if logical_records_hash(l3_rows) != manifest.get("score_panel_logical_hash"):
        return None
    if logical_records_hash(runtime) != manifest.get("replay_logical_hash"):
        return None
    if (
        logical_records_hash(counterfactual)
        != manifest.get("counterfactual_replay_logical_hash")
    ):
        return None
    if logical_payload_hash(metrics_payload) != manifest.get("metrics_logical_hash"):
        return None
    if logical_records_hash(metrics_rows) != manifest.get("metrics_by_fold_logical_hash"):
        return None
    summary = read_json_dict(result_dir / "result_summary.json")
    return {"summary": summary, "metrics_rows": metrics_rows}


def a31_failure_record(task: dict[str, Any], exc: Exception) -> dict[str, Any]:
    item = task.get("a31_item") or {}
    return {
        "a31_config_hash": item.get("a31_config_hash"),
        "a31_config_name": (item.get("config") or {}).get("name"),
        "error_type": type(exc).__name__,
        "error": str(exc),
        "result_classification": "smoke_execution_failed",
    }


def mark_a31_pareto(
    summary_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    eligible = [
        row for row in summary_rows
        if row.get("result_classification") not in {"smoke_execution_failed", "reference_parity_failed"}
    ]
    ranked = sorted(eligible, key=a31_pareto_sort_key)
    pareto_rows: list[dict[str, Any]] = []
    pareto_hashes = {row["a31_config_hash"] for row in ranked[:5]}
    for rank, row in enumerate(ranked[:5], start=1):
        selected = pareto_projection(row)
        selected["pareto_rank"] = rank
        selected["a31_selection_status"] = "a31_pareto_candidate"
        selected["frozen"] = False
        selected["production_candidate"] = False
        selected["activation_ready"] = False
        pareto_rows.append(selected)
    updated = []
    for row in summary_rows:
        current = dict(row)
        if current["a31_config_hash"] in pareto_hashes:
            current["a31_selection_status"] = "a31_pareto_candidate"
            current["pareto_rank"] = next(
                item["pareto_rank"] for item in pareto_rows
                if item["a31_config_hash"] == current["a31_config_hash"]
            )
        else:
            current["a31_selection_status"] = "a31_screened_out"
            current["pareto_rank"] = None
        updated.append(current)
    updated.sort(key=lambda row: str(row["a31_config_hash"]))
    return updated, pareto_rows


def pareto_projection(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in row.items()
        if key != "result_dir" and not key.startswith("timing_")
    }


def a31_pareto_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        none_last(row.get("candidate_revision_change_rate")),
        none_last(row.get("growth_sign_revision_change_days")),
        none_last(row.get("inflation_sign_revision_change_days")),
        none_last(row.get("transition_timing_displacement_median")),
        none_last(row.get("candidate_flips_per_year")),
        none_last(row.get("distance_from_ref")),
        str(row.get("a31_config_hash")),
    )


def none_last(value: Any) -> float:
    if value is None:
        return float("inf")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def aggregate_stage_timings(summary_rows: list[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    for key in ["load_l2", "compute_l3", "write_l3", "run_l4", "compute_metrics", "write_artifacts"]:
        values = [
            float(row[f"timing_{key}_seconds"])
            for row in summary_rows
            if row.get(f"timing_{key}_seconds") is not None
        ]
        out[key] = distribution(values)
    return out


def parse_args(argv: list[str] | None = None) -> HarnessConfig:
    ap = argparse.ArgumentParser(description="Run read-only A3 calibration replay")
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--data-snapshot-id", required=True)
    ap.add_argument("--backend-commit", required=True)
    ap.add_argument("--worker-commit", required=True)
    ap.add_argument("--random-seed", type=int, default=0)
    ap.add_argument("--decision-calendar", default="business_days")
    ap.add_argument("--macro-config", default=MACRO_CONFIG_ID)
    ap.add_argument("--policy-config", default=POLICY_VERSION)
    ap.add_argument(
        "--market-source",
        choices=["none", "snapshot", "db_cagg", "tiingo"],
        default="db_cagg",
    )
    ap.add_argument("--input-cache-dir", default=DEFAULT_INPUT_CACHE_DIR)
    ap.add_argument("--input-cache-key")
    ap.add_argument("--refresh-input-cache", action="store_true")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--no-input-cache", action="store_true")
    ap.add_argument("--no-qa-repeat-read", action="store_true")
    args = ap.parse_args(argv)
    return HarnessConfig(
        start_date=dt.date.fromisoformat(args.start_date),
        end_date=dt.date.fromisoformat(args.end_date),
        output_dir=Path(args.output_dir),
        data_snapshot_id=args.data_snapshot_id,
        backend_commit=args.backend_commit,
        worker_commit=args.worker_commit,
        random_seed=args.random_seed,
        decision_calendar=args.decision_calendar,
        macro_config=args.macro_config,
        policy_config=args.policy_config,
        market_source=args.market_source,
        qa_repeat_read=not args.no_qa_repeat_read,
        input_cache_dir=None if args.no_input_cache else Path(args.input_cache_dir),
        input_cache_key=args.input_cache_key,
        refresh_input_cache=args.refresh_input_cache,
        offline=args.offline,
    )


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] == "a31-grid":
        result = run_a31_grid(parse_a31_grid_args(argv))
        print(json.dumps(result, sort_keys=True))
        return
    config = parse_args(argv)
    if config.macro_config != MACRO_CONFIG_ID:
        raise SystemExit(f"unsupported macro_config: {config.macro_config}")
    if config.policy_config != POLICY_VERSION:
        raise SystemExit(f"unsupported policy_config: {config.policy_config}")
    if config.offline:
        if config.refresh_input_cache:
            raise SystemExit("--offline cannot be combined with --refresh-input-cache")
        if config.input_cache_dir is None:
            raise SystemExit("--offline requires an input cache")
        if config.market_source in {"snapshot", "tiingo"}:
            raise SystemExit(f"--offline does not allow market_source={config.market_source}")
        result = run_harness(None, config)
    else:
        with connect(resolve_dsn(os.getenv("DATABASE_URL"))) as conn:
            result = run_harness(conn, config)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

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
from dataclasses import asdict, dataclass, field as dataclass_field
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
MARKET_GRID_SCHEMA_VERSION = 1
MARKET_GRID_CODE_VERSION = "market_implied_grid_v1"
A3_SCOPE_DECISION_SCHEMA_VERSION = 1
REVISION_UNCERTAINTY_SCHEMA_VERSION = 1
A31_V03_GRID_SCHEMA_VERSION = 1
V02_QUALIFICATION_SCHEMA_VERSION = 1
V02_ALFRED_FETCH_SCHEMA_VERSION = 1
V02_ALFRED_SOURCE_SPEC_VERSION = "macro_family_map_v02_candidate_data_qualification_v1"
PARENT_V01_L2_HASH = "4419f8c041397e16914d1b0a6dcb8244a5144bd98a54a75e67a6832f9d468c85"
MIN_QUARTERLY_SURVEY_OBS = 12
A31_PROGRESSION_POLICY_VERSION = "a3_progression_v2"
MARKET_DIAGNOSTIC_MODEL_VERSION = "market_implied_v1_diagnostic_frozen"
A31_REVISION_PASS_RATE = 0.20
A31_REVISION_CONDITIONAL_RATE = 0.23
A31_RELATIVE_IMPROVEMENT_CONDITIONAL = 0.08
A31_V01_BENCHMARK_REVISION_RATE = 0.25209562247749145
A31_V01_GROWTH_SIGN_BENCHMARK = 1111
A31_V01_VALID_RATE = 0.21204594846321018
A31_V03_CONTROL_REVISION_RATE = 0.19621235641105247
GATE_MAX_LAG_BUSINESS_DAYS = 5
A4_PROVISIONAL_STATUS = "harness_ready_provisional_A3"

Quadrant = Literal["recovery", "expansion", "slowdown", "contraction"]
Status = Literal["valid", "abstain", "unavailable", "invalid"]
MarketSource = Literal["none", "snapshot", "db_cagg", "tiingo"]


@dataclass(frozen=True)
class SeriesConfig:
    series_id: str
    axis: Literal["growth", "inflation"]
    family: str
    transform_class: Literal[
        "quantity_index",
        "price_index",
        "rate_level",
        "claims_log4w",
        "claims_log4w_delta13",
        "diffusion_zero_centered",
        "sentiment_level_delta",
        "quarterly_survey_level_v1",
    ]
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
    axis_aggregation_method: str = "weighted_mean"
    release_smoothing: str = "none"
    series_transform_overrides: dict[str, str] = dataclass_field(default_factory=dict)
    revision_soft_threshold_quantile: str | None = None
    family_consensus_min: float | None = None


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


@dataclass(frozen=True)
class A32GridConfig:
    feature_manifest: Path
    a31_catalog: Path
    output_dir: Path | None = None
    offline: bool = False
    worker_commit: str | None = None


@dataclass(frozen=True)
class A3FreezeReadinessConfig:
    v02b_grid_dir: Path
    g2_grid_dir: Path
    a32_grid_dir: Path
    output_dir: Path
    worker_commit: str | None = None


@dataclass(frozen=True)
class MarketGridConfig:
    feature_manifest: Path
    a31_catalog: Path | None
    a32_grid_dir: Path | None
    macro_feature_manifest: Path | None = None
    output_dir: Path | None = None
    offline: bool = False
    worker_commit: str | None = None
    macro_a31_name: str = "G2-CREDIT6040-15-SURVEY05"
    macro_a32_name: str = "A32-G0.35-I0.35-X0.10-C0.60-D1.25"


@dataclass(frozen=True)
class MarketCalibrationConfig:
    name: str
    growth_score_scale: float
    inflation_score_scale: float
    growth_enter: float
    growth_exit: float
    inflation_enter: float
    inflation_exit: float
    min_confidence: float


@dataclass(frozen=True)
class A3ScopeDecisionConfig:
    freeze_readiness_dir: Path
    market_grid_dir: Path
    output_dir: Path
    worker_commit: str | None = None


@dataclass(frozen=True)
class RevisionUncertaintyConfig:
    feature_manifest: Path
    output_dir: Path | None = None
    offline: bool = False
    worker_commit: str | None = None


@dataclass(frozen=True)
class A31V03GridConfig:
    feature_manifest: Path
    revision_uncertainty_manifest: Path
    config_catalog: Path
    a32_grid_dir: Path
    output_dir: Path | None = None
    jobs: int = 1
    offline: bool = False
    worker_commit: str | None = None
    a32_name: str = "A32-G0.35-I0.35-X0.10-C0.60-D1.25"


@dataclass(frozen=True)
class V02QualificationConfig:
    v01_feature_manifest: Path
    vintage_cache: Path
    output_dir: Path
    start_date: dt.date
    end_date: dt.date
    v01_screen_dir: Path | None = None
    v01_local_dir: Path | None = None
    offline: bool = False
    worker_commit: str | None = None


@dataclass(frozen=True)
class V02FetchAlfredConfig:
    base_vintage_cache: Path
    output_dir: Path
    fred_env_file: Path | None = None
    worker_commit: str | None = None


@dataclass(frozen=True)
class V02SeriesSpec:
    series_id: str
    candidate_family: str
    frequency: str
    freshness_limit_days: int
    config: SeriesConfig
    license_status: str = "fred_alfred_public_citation_required"
    notes: str = ""


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

BASELINE_SERIES_SPECS: tuple[V02SeriesSpec, ...] = tuple(
    V02SeriesSpec(
        series_id=cfg.series_id,
        candidate_family=cfg.family,
        frequency="monthly",
        freshness_limit_days=45,
        config=cfg,
        notes="v01 baseline series preserved for control parity",
    )
    for cfg in BASELINE_SERIES
)

V02_CHALLENGER_SERIES_SPECS: tuple[V02SeriesSpec, ...] = (
    V02SeriesSpec(
        "ICSA",
        "claims_labor",
        "weekly",
        21,
        SeriesConfig("ICSA", "growth", "claims_labor", "claims_log4w"),
        notes="initial claims; intended v02 transform is inverted and smoothed by release",
    ),
    V02SeriesSpec(
        "GACDFSA066MSFRBPHI",
        "survey_diffusion",
        "monthly",
        45,
        SeriesConfig("GACDFSA066MSFRBPHI", "growth", "survey_diffusion", "diffusion_zero_centered"),
        notes="Philadelphia Fed current general activity diffusion index",
    ),
    V02SeriesSpec(
        "NOCDFSA066MSFRBPHI",
        "survey_diffusion",
        "monthly",
        45,
        SeriesConfig("NOCDFSA066MSFRBPHI", "growth", "survey_diffusion", "diffusion_zero_centered"),
        notes="Philadelphia Fed current new orders diffusion index",
    ),
    V02SeriesSpec(
        "GACDISA066MSFRBNY",
        "survey_diffusion",
        "monthly",
        45,
        SeriesConfig("GACDISA066MSFRBNY", "growth", "survey_diffusion", "diffusion_zero_centered"),
        notes="Empire State current general business conditions diffusion index",
    ),
    V02SeriesSpec(
        "NOCDISA066MSFRBNY",
        "survey_diffusion",
        "monthly",
        45,
        SeriesConfig("NOCDISA066MSFRBNY", "growth", "survey_diffusion", "diffusion_zero_centered"),
        notes="Empire State current new orders diffusion index",
    ),
    V02SeriesSpec(
        "BUSAPPWNSAUS",
        "business_formation",
        "weekly",
        21,
        SeriesConfig("BUSAPPWNSAUS", "growth", "business_formation", "quantity_index"),
        notes="business applications impulse candidate",
    ),
    V02SeriesSpec(
        "DRTSCILM",
        "credit_survey",
        "quarterly",
        120,
        SeriesConfig(
            "DRTSCILM",
            "growth",
            "credit_survey",
            "quarterly_survey_level_v1",
            direction=-1,
        ),
        notes="SLOOS tighter commercial and industrial loan standards",
    ),
    V02SeriesSpec(
        "DRSDCILM",
        "credit_survey",
        "quarterly",
        120,
        SeriesConfig("DRSDCILM", "growth", "credit_survey", "quarterly_survey_level_v1"),
        notes="SLOOS commercial and industrial loan demand",
    ),
    V02SeriesSpec(
        "UMCSENT",
        "consumer_survey",
        "monthly",
        60,
        SeriesConfig("UMCSENT", "growth", "consumer_survey", "sentiment_level_delta"),
        notes="consumer sentiment challenger with its own freshness treatment",
    ),
)

V02_UNION_SERIES_SPECS: tuple[V02SeriesSpec, ...] = (
    BASELINE_SERIES_SPECS + V02_CHALLENGER_SERIES_SPECS
)
V02_UNION_SERIES: tuple[SeriesConfig, ...] = tuple(
    spec.config for spec in V02_UNION_SERIES_SPECS
)
V02_SPEC_BY_SERIES_ID = {spec.series_id: spec for spec in V02_UNION_SERIES_SPECS}
V02_EXCLUDED_MARKET_DERIVED_SERIES = ("NFCI", "STLFSI", "STLFSI4")
V02A_GROWTH_SCREEN_SERIES = (
    "ICSA",
    "GACDFSA066MSFRBPHI",
    "NOCDFSA066MSFRBPHI",
    "GACDISA066MSFRBNY",
    "NOCDISA066MSFRBNY",
    "UMCSENT",
)
V02B_SLOOS_SCREEN_SERIES = ("DRTSCILM", "DRSDCILM")
V02B_DEFERRED_SERIES = ("BUSAPPWNSAUS",)
V02A_DIAGNOSTIC_RESULTS = (
    "V02-G1-SURVEY-REGION-COMPOSITE-15",
    "V02-G1-SURVEY-REGION-COMPOSITE-10",
    "V02-G1-ICSA-LOG4W-10",
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


def read_vintage_rows(
    conn,
    *,
    max_available_at: dt.datetime,
    series_configs: tuple[SeriesConfig, ...] = BASELINE_SERIES,
) -> list[VintageRow]:
    series_ids = [s.series_id for s in series_configs]
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
    *,
    series_configs: tuple[SeriesConfig, ...] = BASELINE_SERIES,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    configs = {cfg.series_id: cfg for cfg in series_configs}
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
    *,
    series_configs: tuple[SeriesConfig, ...] = BASELINE_SERIES,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    configs = {cfg.series_id: cfg for cfg in series_configs}
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
        "direction": cfg.direction,
        "observation_period": date_or_none(latest_period),
        "vintage_date": date_or_none(latest_row.vintage_date if latest_row else None),
        "available_at": datetime_or_none(latest_row.available_at if latest_row else None),
        "raw_value": latest_row.value if latest_row else None,
        "revision_number": latest_row.revision_number if latest_row else None,
        "freshness": (
            series_freshness(cut, latest_row.available_at, series_id=cfg.series_id)
            if latest_row else 0.0
        ),
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
    period_set = set(periods)
    p3 = shift_months_with_set(periods, period_set, idx, 3)
    p6 = shift_months_with_set(periods, period_set, idx, 6)
    p12 = shift_months_with_set(periods, period_set, idx, 12)
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
    primitives.update(custom_macro_primitives(cfg, series, periods))
    reference_score = reference_series_score(cfg, series)
    primitives.update({
        "reference_series_score": reference_score,
        "reference_transform_reason": (
            None if reference_score is not None else "insufficient_transform_history"
        ),
    })
    return primitives


def custom_macro_primitives(
    cfg: SeriesConfig,
    series: dict[dt.date, float],
    periods: list[dt.date],
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "claims_log_ma4": None,
        "z_claims_log_ma4": None,
        "claims_delta_13w_log_ma4": None,
        "z_claims_delta_13w_log_ma4": None,
        "diffusion_zero_centered": None,
        "z_diffusion_zero_centered": None,
        "sentiment_level_z": None,
        "sentiment_delta_3m_z": None,
        "quarterly_level_z": None,
        "quarterly_delta_1q_z": None,
    }
    if cfg.transform_class in {"claims_log4w", "claims_log4w_delta13"}:
        log_ma4 = log_moving_average_history(series, periods, window=4)
        latest = max(log_ma4, default=None)
        if latest is not None:
            delta13 = ordered_delta_by_lag(log_ma4, lag=13)
            out.update({
                "claims_log_ma4": log_ma4.get(latest),
                "z_claims_log_ma4": latest_component_z(log_ma4),
                "claims_delta_13w_log_ma4": delta13.get(latest),
                "z_claims_delta_13w_log_ma4": latest_component_z(delta13),
            })
    if cfg.transform_class == "diffusion_zero_centered":
        latest = periods[-1] if periods else None
        if latest is not None:
            out.update({
                "diffusion_zero_centered": series[latest],
                "z_diffusion_zero_centered": zero_centered_robust_z(series, series[latest]),
            })
    if cfg.transform_class == "sentiment_level_delta":
        latest = periods[-1] if periods else None
        if latest is not None:
            delta3: dict[dt.date, float] = {}
            period_set = set(periods)
            for idx, period in enumerate(periods):
                p3 = shift_months_with_set(periods, period_set, idx, 3)
                if p3 is not None:
                    delta3[period] = series[period] - series[p3]
            out.update({
                "sentiment_level_z": latest_component_z(series),
                "sentiment_delta_3m_z": latest_component_z(delta3),
            })
    if cfg.transform_class == "quarterly_survey_level_v1":
        latest = periods[-1] if periods else None
        if latest is not None:
            delta1q = ordered_delta_by_lag(series, lag=1)
            out.update({
                "quarterly_level_z": latest_quarterly_component_z(series),
                "quarterly_delta_1q_z": latest_quarterly_component_z(delta1q),
            })
    return out


def log_moving_average_history(
    series: dict[dt.date, float],
    periods: list[dt.date],
    *,
    window: int,
) -> dict[dt.date, float]:
    out: dict[dt.date, float] = {}
    for idx, period in enumerate(periods):
        if idx + 1 < window:
            continue
        values = [series[p] for p in periods[idx - window + 1: idx + 1]]
        if all(value > 0 for value in values):
            out[period] = math.log(sum(values) / window)
    return out


def ordered_delta_by_lag(values_by_period: dict[dt.date, float], *, lag: int) -> dict[dt.date, float]:
    periods = sorted(values_by_period)
    out: dict[dt.date, float] = {}
    for idx, period in enumerate(periods):
        if idx >= lag:
            out[period] = values_by_period[period] - values_by_period[periods[idx - lag]]
    return out


def zero_centered_robust_z(series: dict[dt.date, float], current: float) -> float | None:
    values = [float(value) for value in series.values() if math.isfinite(value)]
    if len(values) < MIN_MONTHLY_OBS:
        return None
    scale = 1.4826 * statistics.median(abs(value) for value in values)
    if scale <= 0:
        return None
    return clip(current / scale, SERIES_Z_CLIP)


def latest_quarterly_component_z(values_by_period: dict[dt.date, float]) -> float | None:
    if not values_by_period:
        return None
    latest = max(values_by_period)
    history = [
        float(value)
        for period, value in values_by_period.items()
        if period <= latest and math.isfinite(value)
    ]
    if len(history) < MIN_QUARTERLY_SURVEY_OBS:
        return None
    return robust_z(history, values_by_period[latest], clip=SERIES_Z_CLIP)


def series_component_z_values(transform_class: str, series: dict[dt.date, float]) -> dict[str, float | None]:
    return dict(_series_component_z_values_cached(transform_class, series_cache_key(series)))


def series_cache_key(series: dict[dt.date, float]) -> tuple[tuple[str, float], ...]:
    return tuple((period.isoformat(), round(float(value), 10)) for period, value in sorted(series.items()))


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
    period_set = set(periods)
    if transform_class in {"quantity_index", "price_index"}:
        c3: dict[dt.date, float] = {}
        c6: dict[dt.date, float] = {}
        c12: dict[dt.date, float] = {}
        for i, period in enumerate(periods):
            p3 = shift_months_with_set(periods, period_set, i, 3)
            p6 = shift_months_with_set(periods, period_set, i, 6)
            p12 = shift_months_with_set(periods, period_set, i, 12)
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
        p3 = shift_months_with_set(periods, period_set, i, 3)
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
    value = _reference_series_score_cached(
        cfg.transform_class,
        cfg.direction,
        series_cache_key(series),
    )
    return value


@functools.lru_cache(maxsize=20000)
def _reference_series_score_cached(
    transform_class: str,
    direction: int,
    series_items: tuple[tuple[str, float], ...],
) -> float | None:
    series = {dt.date.fromisoformat(period): value for period, value in series_items}
    if transform_class == "quantity_index":
        value = quantity_index_score(series)
    elif transform_class == "price_index":
        value = price_index_score(series)
    elif transform_class == "claims_log4w":
        value = claims_log4w_score(series)
    elif transform_class == "claims_log4w_delta13":
        value = claims_log4w_delta13_score(series)
    elif transform_class == "diffusion_zero_centered":
        value = diffusion_zero_centered_score(series)
    elif transform_class == "sentiment_level_delta":
        value = sentiment_level_delta_score(series)
    elif transform_class == "quarterly_survey_level_v1":
        value = latest_quarterly_component_z(series)
    else:
        value = rate_level_score(series)
    return value * direction if value is not None else None


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
    period_set = set(periods)
    g3: dict[dt.date, float] = {}
    g6: dict[dt.date, float] = {}
    g12: dict[dt.date, float] = {}
    for i, period in enumerate(periods):
        p3 = shift_months_with_set(periods, period_set, i, 3)
        p6 = shift_months_with_set(periods, period_set, i, 6)
        p12 = shift_months_with_set(periods, period_set, i, 12)
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
    period_set = set(periods)
    pi3: dict[dt.date, float] = {}
    pi6: dict[dt.date, float] = {}
    pi12: dict[dt.date, float] = {}
    for i, period in enumerate(periods):
        p3 = shift_months_with_set(periods, period_set, i, 3)
        p6 = shift_months_with_set(periods, period_set, i, 6)
        p12 = shift_months_with_set(periods, period_set, i, 12)
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
    period_set = set(periods)
    delta3: dict[dt.date, float] = {}
    for i, period in enumerate(periods):
        p3 = shift_months_with_set(periods, period_set, i, 3)
        if p3 is not None:
            delta3[period] = series[period] - series[p3]
    z_level = latest_component_z(series)
    z_delta = latest_component_z(delta3)
    return weighted_components([(0.70, z_level), (0.30, z_delta)])


def claims_log4w_score(series: dict[dt.date, float]) -> float | None:
    periods = sorted(series)
    log_ma4 = log_moving_average_history(series, periods, window=4)
    z_level = latest_component_z(log_ma4)
    return -z_level if z_level is not None else None


def claims_log4w_delta13_score(series: dict[dt.date, float]) -> float | None:
    periods = sorted(series)
    log_ma4 = log_moving_average_history(series, periods, window=4)
    delta13 = ordered_delta_by_lag(log_ma4, lag=13)
    z_level = latest_component_z(log_ma4)
    z_delta = latest_component_z(delta13)
    return weighted_components([
        (0.70, -z_level if z_level is not None else None),
        (0.30, -z_delta if z_delta is not None else None),
    ])


def diffusion_zero_centered_score(series: dict[dt.date, float]) -> float | None:
    periods = sorted(series)
    if not periods:
        return None
    return zero_centered_robust_z(series, series[periods[-1]])


def sentiment_level_delta_score(series: dict[dt.date, float]) -> float | None:
    periods = sorted(series)
    period_set = set(periods)
    delta3: dict[dt.date, float] = {}
    for idx, period in enumerate(periods):
        p3 = shift_months_with_set(periods, period_set, idx, 3)
        if p3 is not None:
            delta3[period] = series[period] - series[p3]
    return weighted_components([
        (0.70, latest_component_z(series)),
        (0.30, latest_component_z(delta3)),
    ])


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
    return shift_months_with_set(periods, set(periods), idx, back)


def shift_months_with_set(
    periods: list[dt.date],
    period_set: set[dt.date],
    idx: int,
    back: int,
) -> dt.date | None:
    target = periods[idx]
    y, m = target.year, target.month - back
    while m <= 0:
        m += 12
        y -= 1
    candidate = dt.date(y, m, 1)
    return candidate if candidate in period_set else None


def series_freshness(
    cut: dt.datetime,
    available_at: dt.datetime,
    *,
    series_id: str | None = None,
) -> float:
    spec = V02_SPEC_BY_SERIES_ID.get(series_id or "")
    if spec and spec.frequency == "quarterly":
        soft, hard = source_deadlines(
            available_at,
            available_at + dt.timedelta(days=90),
            dt.timedelta(days=15),
            dt.timedelta(days=spec.freshness_limit_days),
            dt.timedelta(days=30),
        )
        return freshness_value(cut, soft, hard)
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
        common = 0
        comparable = 0
        changed = 0
        sign_changed = {"growth": 0, "inflation": 0}
        status_changed = 0
        published_changed = 0
        latched_changed = 0
        for latest, first in zip(replay, first_release_replay):
            common += 1
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
            published_changed += int(
                latest.get("published_quadrant") != first.get("published_quadrant")
            )
            latched_changed += int(
                (latest.get("latched_quadrant") or latest.get("published_quadrant"))
                != (first.get("latched_quadrant") or first.get("published_quadrant"))
            )
        stability = {
            "revision_common_days": common,
            "candidate_comparable_days": comparable,
            "candidate_quadrant_changed_by_revision_days": changed,
            "candidate_quadrant_changed_by_revision_rate": (
                changed / comparable if comparable else None
            ),
            "growth_axis_sign_changed_by_revision_days": sign_changed["growth"],
            "inflation_axis_sign_changed_by_revision_days": sign_changed["inflation"],
            "status_changed_by_revision_days": status_changed,
            "status_changed_by_revision_rate": status_changed / common if common else None,
            "published_quadrant_changed_by_revision_days": published_changed,
            "published_quadrant_changed_by_revision_rate": (
                published_changed / common if common else None
            ),
            "latched_quadrant_changed_by_revision_days": latched_changed,
            "latched_quadrant_changed_by_revision_rate": (
                latched_changed / common if common else None
            ),
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
    metrics["transition_lead_lag_days"] = monotonic_transition_lags(
        metrics["macro_transition_dates"],
        metrics["market_transition_dates"],
        max_business_day_window=126,
    )
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
        axis_aggregation_method="weighted_mean",
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


def series_family_mapping_hash(
    series_configs: tuple[SeriesConfig, ...] = BASELINE_SERIES,
) -> str:
    return stable_hash([asdict(cfg) for cfg in series_configs])


def v02_series_universe_hash() -> str:
    return stable_hash([asdict(spec) for spec in V02_UNION_SERIES_SPECS])


def canonical_config_hash(config: A31Config | A32Config) -> str:
    return stable_hash(asdict(config))


def a31_config_hash(config: A31Config, l2_macro_logical_hash: str) -> str:
    return stable_hash({
        "l2_macro_logical_hash": l2_macro_logical_hash,
        "canonical_A31Config": asdict(config),
        "scorer_schema_version": L3_SCORER_SCHEMA_VERSION,
        "scorer_code_version": L3_SCORER_CODE_VERSION,
    })[:24]


def a32_config_hash(config: A32Config) -> str:
    return stable_hash({
        "canonical_A32Config": asdict(config),
        "state_schema_version": L4_STATE_SCHEMA_VERSION,
        "state_code_version": L4_STATE_CODE_VERSION,
    })[:24]


def evaluation_hash(a31_config_hash_: str, a32_config_hash_: str) -> str:
    return stable_hash({
        "a31_config_hash": a31_config_hash_,
        "a32_config_hash": a32_config_hash_,
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
    revision_uncertainty_by_key: dict[tuple[str, str, str, str], dict[str, Any]] | None = None,
    revision_uncertainty_logical_hash: str | None = None,
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
        selected_primitive_rows = [
            row for row in primitive_rows if l3_row_selected(row, a31_config)
        ]
        axis_payload = {
            "growth": aggregate_l3_axis(
                by_series,
                "growth",
                a31_config,
                revision_uncertainty_by_key=revision_uncertainty_by_key,
                business_date=business_date,
                selection_mode=selection_mode,
            ),
            "inflation": aggregate_l3_axis(
                by_series,
                "inflation",
                a31_config,
                revision_uncertainty_by_key=revision_uncertainty_by_key,
                business_date=business_date,
                selection_mode=selection_mode,
            ),
        }
        c_quality = min(axis_payload["growth"]["coverage"], axis_payload["inflation"]["coverage"])
        f_quality = min(axis_payload["growth"]["freshness"], axis_payload["inflation"]["freshness"])
        a_quality = min(axis_payload["growth"]["concordance"], axis_payload["inflation"]["concordance"])
        v_quality = min(
            axis_payload["growth"]["vintage_quality"],
            axis_payload["inflation"]["vintage_quality"],
        )
        u_value = 0.35 * c_quality + 0.20 * f_quality + 0.25 * a_quality + 0.20 * v_quality
        information_hash = l2_information_set_hash(selected_primitive_rows)
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
            "growth_family_consensus_ratio": axis_payload["growth"][
                "family_consensus_ratio"
            ],
            "inflation_family_consensus_ratio": axis_payload["inflation"][
                "family_consensus_ratio"
            ],
            "growth_family_consensus_ok": axis_payload["growth"][
                "family_consensus_ok"
            ],
            "inflation_family_consensus_ok": axis_payload["inflation"][
                "family_consensus_ok"
            ],
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
            "revision_uncertainty_logical_hash": revision_uncertainty_logical_hash,
        },
        "row_count": len(score_rows),
        "contribution_row_count": len(contribution_rows),
        "logical_hash": logical_records_hash(score_rows),
        "contribution_logical_hash": logical_records_hash(contribution_rows),
        "reusable_for_a32": True,
    }
    return score_rows, contribution_rows, manifest


def aggregate_l3_axis(
    by_series: dict[str, dict[str, Any]],
    axis: str,
    config: A31Config,
    *,
    revision_uncertainty_by_key: dict[tuple[str, str, str, str], dict[str, Any]] | None = None,
    business_date: str | None = None,
    selection_mode: str | None = None,
) -> dict[str, Any]:
    by_family: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for series_id, row in sorted(by_series.items()):
        if not l3_row_selected(row, config):
            continue
        if str(row.get("axis_id")) != axis:
            continue
        family = str(row.get("family_id"))
        weight = config.series_weights.get(series_id, 1.0)
        by_family.setdefault(family, []).append((weight, row))

    family_scores: dict[str, float] = {}
    family_freshness: dict[str, float] = {}
    family_vintage: dict[str, float] = {}
    for family, items in by_family.items():
        values = []
        for weight, row in items:
            score = series_score_from_l2_row(row, config)
            if score is not None:
                score = apply_revision_soft_threshold(
                    score,
                    row,
                    config,
                    revision_uncertainty_by_key=revision_uncertainty_by_key,
                    business_date=business_date,
                    selection_mode=selection_mode,
                )
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
        if config.axis_aggregation_method == "weighted_median":
            score = weighted_median([
                (score_weights[family], value)
                for family, value in available.items()
            ])
        else:
            if config.axis_aggregation_method != "weighted_mean":
                raise ValueError(
                    f"unsupported axis_aggregation_method: {config.axis_aggregation_method}"
                )
            score = sum(
                (score_weights[family] / active_weight) * value
                for family, value in available.items()
            )
        score = clip(score, config.score_clip.get("axis", AXIS_SCORE_CLIP))
    else:
        score = None
    consensus_ratio, consensus_ok = family_consensus_status(
        available,
        score_weights,
        config.family_consensus_min,
        score,
    )
    if score is not None and not consensus_ok:
        score = 0.0
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
        "family_consensus_ratio": consensus_ratio,
        "family_consensus_ok": consensus_ok,
        "family_count": len(available),
        "has_anchor": bool(ANCHOR_FAMILIES[axis] & set(available)),
    }


def l3_row_selected(row: dict[str, Any], config: A31Config) -> bool:
    series_id = str(row.get("series_id") or "")
    axis = str(row.get("axis_id") or "")
    family = str(row.get("family_id") or "")
    if series_id not in config.series_weights:
        return False
    return family in config.family_weights.get(axis, {})


def apply_revision_soft_threshold(
    score: float,
    row: dict[str, Any],
    config: A31Config,
    *,
    revision_uncertainty_by_key: dict[tuple[str, str, str, str], dict[str, Any]] | None,
    business_date: str | None,
    selection_mode: str | None,
) -> float:
    quantile = config.revision_soft_threshold_quantile
    if not quantile or revision_uncertainty_by_key is None:
        return score
    key = (
        str(business_date or row.get("business_date")),
        str(selection_mode or row.get("selection_mode")),
        "series",
        str(row.get("series_id")),
    )
    uncertainty = revision_uncertainty_by_key.get(key)
    if not uncertainty or not uncertainty.get("sufficient_history"):
        return score
    q_field = {
        "p50": "median_absolute_revision",
        "p75": "p75_absolute_revision",
        "p90": "p90_absolute_revision",
    }.get(str(quantile).lower())
    if q_field is None:
        raise ValueError(f"unsupported revision soft-threshold quantile: {quantile}")
    q_value = finite_or_none(uncertainty.get(q_field))
    if q_value is None:
        return score
    return math.copysign(max(abs(score) - q_value, 0.0), score)


def family_consensus_status(
    family_scores: dict[str, float],
    score_weights: dict[str, float],
    threshold: float | None,
    axis_score_value: float | None,
) -> tuple[float | None, bool]:
    if threshold is None:
        return None, True
    if not family_scores or axis_score_value is None or axis_score_value == 0:
        return 0.0, False
    sign = 1 if axis_score_value > 0 else -1
    active_weight = sum(abs(score_weights.get(family, 0.0)) for family in family_scores)
    if active_weight <= 0:
        return 0.0, False
    same = sum(
        abs(score_weights.get(family, 0.0))
        for family, value in family_scores.items()
        if (value > 0 and sign > 0) or (value < 0 and sign < 0)
    )
    ratio = same / active_weight
    return ratio, ratio >= threshold


def series_score_from_l2_row(row: dict[str, Any], config: A31Config) -> float | None:
    series_id = str(row.get("series_id") or "")
    transform_class = str(
        config.series_transform_overrides.get(series_id)
        or row.get("transform_class")
        or ""
    )
    if transform_class == "claims_log4w":
        z_level = finite_or_none(row.get("z_claims_log_ma4"))
        return -z_level if z_level is not None else None
    if transform_class == "claims_log4w_delta13":
        z_level = finite_or_none(row.get("z_claims_log_ma4"))
        z_delta = finite_or_none(row.get("z_claims_delta_13w_log_ma4"))
        return weighted_components([
            (0.70, -z_level if z_level is not None else None),
            (0.30, -z_delta if z_delta is not None else None),
        ])
    if transform_class == "diffusion_zero_centered":
        return finite_or_none(row.get("z_diffusion_zero_centered"))
    if transform_class == "sentiment_level_delta":
        return weighted_components([
            (0.70, finite_or_none(row.get("sentiment_level_z"))),
            (0.30, finite_or_none(row.get("sentiment_delta_3m_z"))),
        ])
    if transform_class == "quarterly_survey_level_v1":
        weights = config.transformation_weights.get(
            transform_class, {"level": 1.0, "delta_1q": 0.0}
        )
        components = [
            (
                float(weights.get("level", 1.0)),
                finite_or_none(row.get("quarterly_level_z")),
            ),
            (
                float(weights.get("delta_1q", 0.0)),
                finite_or_none(row.get("quarterly_delta_1q_z")),
            ),
        ]
        value = weighted_components([(w, z) for w, z in components if w > 0.0])
        direction = int(row.get("direction") or series_direction(str(row.get("series_id") or "")))
        return value * direction if value is not None else None
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


def series_direction(series_id: str) -> int:
    spec = V02_SPEC_BY_SERIES_ID.get(series_id)
    if spec is None:
        return 1
    return int(spec.config.direction)


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


def weighted_median(items: list[tuple[float, float | None]]) -> float | None:
    values = sorted(
        (float(value), abs(float(weight)))
        for weight, value in items
        if value is not None and math.isfinite(value) and math.isfinite(float(weight))
    )
    total = sum(weight for _, weight in values)
    if total <= 0:
        return None
    midpoint = total / 2.0
    running = 0.0
    for value, weight in values:
        running += weight
        if running >= midpoint:
            return value
    return values[-1][0]


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
    for series_id, source in sorted(by_series.items()):
        if not l3_row_selected(source, config):
            continue
        axis = str(source.get("axis_id"))
        family = str(source.get("family_id"))
        rows.append({
            "business_date": business_date,
            "selection_mode": selection_mode,
            "selection_role": selection_role,
            "a31_config_hash": a31_hash,
            "contribution_level": "series",
            "axis": axis,
            "family": family,
            "series_id": series_id,
            "score": series_score_from_l2_row(source, config),
            "weight": config.series_weights.get(series_id),
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
                "weight": config.family_weights.get(axis_name, {}).get(family),
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
    a32_hash = a32_config_hash(a32_config)
    eval_hash = evaluation_hash(l3_hash, a32_hash)
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
            "evaluation_hash": eval_hash,
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
        "evaluation_hash": eval_hash,
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
            a32_hash = a32_config_hash(a32)
            eval_hash = evaluation_hash(a31_hash, a32_hash)
            classification = classify_smoke_result(runtime, counterfactual)
            all_metric_rows.extend(
                evaluation_metric_rows(
                    runtime,
                    counterfactual,
                    a31,
                    a32,
                    a31_hash,
                    a32_hash,
                    eval_hash,
                    classification,
                )
            )
            full_metrics = build_macro_metrics(runtime, first_release_replay=counterfactual)
            all_summary_rows.append({
                "a31_config_hash": a31_hash,
                "a31_config_name": a31.name,
                "a32_config_hash": a32_hash,
                "a32_config_name": a32.name,
                "evaluation_hash": eval_hash,
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


def a32_grid_configs() -> list[A32Config]:
    configs: list[A32Config] = []
    for growth_enter in (0.30, 0.35):
        for inflation_enter in (0.35, 0.40):
            for axis_exit in (0.10, 0.15):
                for min_confidence in (0.60, 0.65, 0.70):
                    configs.append(A32Config(
                        name=(
                            f"A32-G{growth_enter:.2f}-I{inflation_enter:.2f}-"
                            f"X{axis_exit:.2f}-C{min_confidence:.2f}-D1.25"
                        ),
                        growth_score_scale=1.0,
                        inflation_score_scale=1.0,
                        growth_enter=growth_enter,
                        growth_exit=axis_exit,
                        inflation_enter=inflation_enter,
                        inflation_exit=axis_exit,
                        u_floor=U_FLOOR,
                        min_confidence=min_confidence,
                        dispersion_limit=1.25,
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


def classify_a32_grid_result(metrics: dict[str, Any]) -> str:
    if metrics.get("run_classification") == RUN_CLASSIFICATION_FAILED:
        return "diagnostic_failed"
    return "a32_candidate"


def evaluation_metric_rows(
    runtime: list[dict[str, Any]],
    counterfactual: list[dict[str, Any]],
    a31: A31Config,
    a32: A32Config,
    a31_hash: str,
    a32_hash: str,
    eval_hash: str,
    classification: str,
) -> list[dict[str, Any]]:
    readiness = operational_readiness_dates(runtime)
    post_start = (
        dt.date.fromisoformat(readiness["post_initialization_start_date"])
        if readiness.get("post_initialization_start_date") else None
    )
    folds = [
        ("full", None, None),
        ("post_initialization", post_start, None),
        ("2014_2017", dt.date(2014, 2, 19), dt.date(2017, 12, 31)),
        ("2018_2021", dt.date(2018, 1, 1), dt.date(2021, 12, 31)),
        ("2022_2026", dt.date(2022, 1, 1), dt.date(2026, 6, 24)),
    ]
    rows: list[dict[str, Any]] = []
    for fold_name, start, end in folds:
        rt = filter_replay_window(runtime, start, end)
        cf = filter_replay_window(counterfactual, start, end)
        if fold_name == "post_initialization" and not rt:
            continue
        metrics = build_macro_metrics(rt, first_release_replay=cf)
        transition_deltas = build_transition_revision_deltas(rt, cf)
        revision_diagnostics = replay_revision_diagnostics(rt, cf)
        operational_metrics = operational_state_metrics(rt)
        candidate_metrics = candidate_definition_metrics(rt)
        full_operational_metrics = operational_state_metrics(runtime)
        rows.append({
            "fold": fold_name,
            "history_scope": "full_history" if fold_name == "full" else fold_name,
            "a31_config_hash": a31_hash,
            "a31_config_name": a31.name,
            "a32_config_hash": a32_hash,
            "a32_config_name": a32.name,
            "evaluation_hash": eval_hash,
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
            "status_revision_change_rate": metrics["vintage_stability"].get(
                "status_changed_by_revision_rate"
            ),
            "published_revision_change_days": metrics["vintage_stability"].get(
                "published_quadrant_changed_by_revision_days"
            ),
            "published_revision_change_rate": metrics["vintage_stability"].get(
                "published_quadrant_changed_by_revision_rate"
            ),
            "latched_revision_change_days": metrics["vintage_stability"].get(
                "latched_quadrant_changed_by_revision_days"
            ),
            "latched_revision_change_rate": metrics["vintage_stability"].get(
                "latched_quadrant_changed_by_revision_rate"
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
            **candidate_metrics,
            "days_without_latched_state": metrics["days_without_latched_state"],
            "coverage_distribution": json.dumps(
                metrics["coverage_distribution"], sort_keys=True
            ),
            "first_input_ready_date": full_operational_metrics["first_input_ready_date"],
            "first_latched_date": full_operational_metrics["first_latched_date"],
            "first_operational_date": full_operational_metrics["first_operational_date"],
            "post_initialization_start_date": full_operational_metrics[
                "post_initialization_start_date"
            ],
            "growth_dispersion_distribution": json.dumps(
                distribution([row["growth_dispersion"] for row in rt]), sort_keys=True
            ),
            "inflation_dispersion_distribution": json.dumps(
                distribution([row["inflation_dispersion"] for row in rt]), sort_keys=True
            ),
            **revision_diagnostics,
            **operational_metrics,
            "frozen": False,
            "production_candidate": False,
            "activation_ready": False,
        })
    return rows


def replay_revision_diagnostics(
    runtime: list[dict[str, Any]],
    counterfactual: list[dict[str, Any]],
) -> dict[str, Any]:
    cf_by_date = {str(row["date"]): row for row in counterfactual}
    score_diffs: dict[str, list[float]] = {"growth": [], "inflation": []}
    raw_sign_change_dates: dict[str, list[dt.date]] = {"growth": [], "inflation": []}
    axis_state_change_dates: dict[str, list[dt.date]] = {"growth": [], "inflation": []}
    candidate_change_dates: list[dt.date] = []
    deadband_absorbed_dates: list[dt.date] = []
    common_dates: list[dt.date] = []
    for latest in runtime:
        date_text = str(latest["date"])
        revised = cf_by_date.get(date_text)
        if revised is None:
            continue
        day = dt.date.fromisoformat(date_text)
        common_dates.append(day)
        raw_changed_any_axis = False
        axis_state_changed_any_axis = False
        for axis in ("growth", "inflation"):
            latest_score = finite_or_none(latest.get(f"{axis}_score"))
            revised_score = finite_or_none(revised.get(f"{axis}_score"))
            latest_sign = sign_of(latest_score)
            revised_sign = sign_of(revised_score)
            raw_changed = (
                latest_sign is not None
                and revised_sign is not None
                and latest_sign != revised_sign
            )
            if latest_score is not None and revised_score is not None:
                score_diffs[axis].append(abs(latest_score - revised_score))
            if raw_changed:
                raw_sign_change_dates[axis].append(day)
                raw_changed_any_axis = True
            if latest.get(f"{axis}_axis_state") != revised.get(f"{axis}_axis_state"):
                axis_state_change_dates[axis].append(day)
                axis_state_changed_any_axis = True
        if raw_changed_any_axis and not axis_state_changed_any_axis:
            deadband_absorbed_dates.append(day)
        if latest.get("candidate_quadrant") != revised.get("candidate_quadrant"):
            candidate_change_dates.append(day)

    durations = contiguous_date_durations(candidate_change_dates, sorted(common_dates))
    duration_dist = duration_summary(durations)
    growth_score_dist = distribution(score_diffs["growth"])
    inflation_score_dist = distribution(score_diffs["inflation"])
    return {
        "growth_score_revision_abs_median": growth_score_dist["median"],
        "growth_score_revision_abs_p90": growth_score_dist["p90"],
        "inflation_score_revision_abs_median": inflation_score_dist["median"],
        "inflation_score_revision_abs_p90": inflation_score_dist["p90"],
        "growth_raw_sign_change_days": len(raw_sign_change_dates["growth"]),
        "inflation_raw_sign_change_days": len(raw_sign_change_dates["inflation"]),
        "growth_axis_state_change_days": len(axis_state_change_dates["growth"]),
        "inflation_axis_state_change_days": len(axis_state_change_dates["inflation"]),
        "candidate_quadrant_change_days": len(candidate_change_dates),
        "candidate_quadrant_change_rate_calendar": (
            len(candidate_change_dates) / len(common_dates) if common_dates else None
        ),
        "deadband_absorbed_revision_days": len(deadband_absorbed_dates),
        "revision_episode_count": len(durations),
        "revision_episode_duration_p50": duration_dist["median"],
        "revision_episode_duration_p90": duration_dist["p90"],
    }


def operational_state_metrics(replay: list[dict[str, Any]]) -> dict[str, Any]:
    days_since_last_valid: list[int] = []
    consumed_state_age: list[int] = []
    stale_dates: list[dt.date] = []
    consumable_days = 0
    last_valid_index: int | None = None
    calendar: list[dt.date] = []
    for index, row in enumerate(replay):
        day = dt.date.fromisoformat(str(row["date"]))
        calendar.append(day)
        if row.get("status") == "valid":
            last_valid_index = index
        since = None if last_valid_index is None else index - last_valid_index
        if since is not None:
            days_since_last_valid.append(since)
        latched = row.get("latched_quadrant") or row.get("published_quadrant")
        if latched is not None and since is not None and since <= GATE_MAX_LAG_BUSINESS_DAYS:
            consumable_days += 1
            consumed_state_age.append(since)
        elif latched is not None and since is not None and since > 5:
            stale_dates.append(day)
    stale_durations = contiguous_date_durations(stale_dates, calendar)
    readiness = operational_readiness_dates(replay)
    return {
        "consumable_state_coverage": (
            consumable_days / len(replay) if replay else None
        ),
        "days_since_last_valid_distribution": json.dumps(
            distribution([float(value) for value in days_since_last_valid]),
            sort_keys=True,
        ),
        "consumed_state_age_distribution": json.dumps(
            distribution([float(value) for value in consumed_state_age]),
            sort_keys=True,
        ),
        "stale_days_over_5bd": len(stale_dates),
        "longest_stale_run": max(stale_durations) if stale_durations else 0,
        "latched_duration": json.dumps(
            duration_summary(state_durations(replay, "latched_quadrant")),
            sort_keys=True,
        ),
        **readiness,
    }


def candidate_definition_metrics(replay: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(replay)
    candidate_defined = sum(1 for row in replay if row.get("candidate_quadrant"))
    neutral = total - candidate_defined
    return {
        "candidate_defined_rate": candidate_defined / total if total else None,
        "neutral_rate": neutral / total if total else None,
        "candidate_defined_days": candidate_defined,
        "neutral_days": neutral,
    }


def operational_readiness_dates(replay: list[dict[str, Any]]) -> dict[str, str | None]:
    first_input_ready: str | None = None
    first_latched: str | None = None
    first_operational: str | None = None
    for row in replay:
        day = str(row["date"])
        reasons = reason_groups(row.get("status_reasons_all") or row.get("all_reasons"))
        no_input_blocker = any(
            reason.endswith("_no_score")
            or reason.endswith("_missing_anchor_family")
            or reason.endswith("_insufficient_families")
            or reason.endswith("_coverage_insufficient")
            or reason.endswith("_freshness_failed")
            for reason in reasons
        )
        if first_input_ready is None and not no_input_blocker:
            first_input_ready = day
        latched = row.get("latched_quadrant") or row.get("published_quadrant")
        if first_latched is None and latched is not None:
            first_latched = day
        if first_operational is None and first_input_ready is not None and latched is not None:
            first_operational = day
    return {
        "first_input_ready_date": first_input_ready,
        "first_latched_date": first_latched,
        "first_operational_date": first_operational,
        "post_initialization_start_date": first_operational,
    }


def filter_replay_window(
    rows: list[dict[str, Any]],
    start: dt.date | None,
    end: dt.date | None,
) -> list[dict[str, Any]]:
    if start is None and end is None:
        return rows
    return [
        row for row in rows
        if (start is None or dt.date.fromisoformat(str(row["date"])) >= start)
        and (end is None or dt.date.fromisoformat(str(row["date"])) <= end)
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


def monotonic_transition_lags(
    macro_dates: list[str],
    market_dates: list[str],
    *,
    max_business_day_window: int,
) -> dict[str, Any]:
    macro = [dt.date.fromisoformat(day) for day in macro_dates]
    market = [dt.date.fromisoformat(day) for day in market_dates]
    matched: list[dict[str, Any]] = []
    unmatched_macro: list[str] = []
    used_market: set[int] = set()
    next_market_idx = 0
    for macro_day in macro:
        while next_market_idx < len(market):
            lag = signed_business_day_distance(macro_day, market[next_market_idx])
            if lag < -max_business_day_window:
                next_market_idx += 1
                continue
            break
        candidates: list[tuple[int, int, int]] = []
        idx = next_market_idx
        while idx < len(market):
            lag = signed_business_day_distance(macro_day, market[idx])
            if lag > max_business_day_window:
                break
            if idx not in used_market:
                candidates.append((abs(lag), lag, idx))
            idx += 1
        if not candidates:
            unmatched_macro.append(macro_day.isoformat())
            continue
        _, lag, chosen_idx = min(candidates)
        used_market.add(chosen_idx)
        next_market_idx = chosen_idx + 1
        matched.append({
            "macro_transition_date": macro_day.isoformat(),
            "market_transition_date": market[chosen_idx].isoformat(),
            "market_minus_macro_business_days": lag,
        })
    unmatched_market = [
        day.isoformat() for idx, day in enumerate(market)
        if idx not in used_market
    ]
    lags = [int(row["market_minus_macro_business_days"]) for row in matched]
    return {
        "matching_method": "monotonic_one_to_one",
        "max_business_day_window": max_business_day_window,
        "count": len(lags),
        "median_days": statistics.median(lags) if lags else None,
        "lags_days": lags,
        "matched_transitions": matched,
        "unmatched_macro_transition_dates": unmatched_macro,
        "unmatched_market_transition_dates": unmatched_market,
        "unmatched_macro_count": len(unmatched_macro),
        "unmatched_market_count": len(unmatched_market),
    }


def signed_business_day_distance(start: dt.date, end: dt.date) -> int:
    if start == end:
        return 0
    sign = 1 if end > start else -1
    earlier, later = (start, end) if end > start else (end, start)
    return sign * (len(business_days(earlier, later)) - 1)


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


def parse_v02_fetch_alfred_args(argv: list[str]) -> V02FetchAlfredConfig:
    ap = argparse.ArgumentParser(description="Fetch ALFRED vintages for v02 candidate series")
    ap.add_argument("command", choices=["v02-fetch-alfred"])
    ap.add_argument("--base-vintage-cache", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--fred-env-file")
    ap.add_argument("--worker-commit")
    args = ap.parse_args(argv)
    return V02FetchAlfredConfig(
        base_vintage_cache=Path(args.base_vintage_cache),
        output_dir=Path(args.output_dir),
        fred_env_file=Path(args.fred_env_file) if args.fred_env_file else None,
        worker_commit=args.worker_commit,
    )


def run_v02_fetch_alfred(config: V02FetchAlfredConfig) -> dict[str, Any]:
    started = dt.datetime.now(UTC)
    execution_id = str(uuid.uuid4())
    config.output_dir.mkdir(parents=True, exist_ok=True)
    api_key = load_fred_api_key(config.fred_env_file)
    base_rows = read_vintage_cache(config.base_vintage_cache)
    fetched_rows, series_stats = fetch_v02_candidate_vintages(api_key)
    union_rows = merge_vintage_rows(base_rows + fetched_rows)
    out_path = config.output_dir / "macro_vintages_v02_union.parquet"
    write_vintage_cache(out_path, union_rows)

    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    git_dirty = bool(run_text(
        ["git", "status", "--porcelain"], cwd=Path(__file__).resolve().parents[1]
    ))
    fetched_series = sorted({row.series_id for row in fetched_rows})
    missing_series = sorted(
        spec.series_id
        for spec in V02_CHALLENGER_SERIES_SPECS
        if spec.series_id not in fetched_series
    )
    manifest = {
        "schema_version": V02_ALFRED_FETCH_SCHEMA_VERSION,
        "execution_id": execution_id,
        "started_at": started.isoformat(),
        "finished_at": dt.datetime.now(UTC).isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "worker_commit": worker_commit,
        "git_dirty": git_dirty,
        "network_access": "fred_series_observations_output_type_2",
        "secret_sources": {
            "fred_api_key": "env_or_env_file_present",
            "fred_env_file": str(config.fred_env_file) if config.fred_env_file else None,
        },
        "base_vintage_cache": str(config.base_vintage_cache),
        "base_row_count": len(base_rows),
        "fetched_candidate_row_count": len(fetched_rows),
        "union_row_count": len(union_rows),
        "source_spec_version": V02_ALFRED_SOURCE_SPEC_VERSION,
        "candidate_series": [spec.series_id for spec in V02_CHALLENGER_SERIES_SPECS],
        "fetched_series": fetched_series,
        "missing_series": missing_series,
        "series_stats": series_stats,
        "market_derived_series_excluded": list(V02_EXCLUDED_MARKET_DERIVED_SERIES),
        "vintage_data_hash": rows_hash(union_rows),
        "artifact_hashes": {
            "macro_vintages_v02_union.parquet": hash_file(out_path),
        },
    }
    write_json(config.output_dir / "v02_alfred_fetch_manifest.json", manifest)
    return {
        "status": "ok" if not missing_series else "partial",
        "output_dir": str(config.output_dir),
        "execution_id": execution_id,
        "fetched_candidate_row_count": len(fetched_rows),
        "union_row_count": len(union_rows),
        "missing_series": missing_series,
        "vintage_data_hash": manifest["vintage_data_hash"],
    }


def load_fred_api_key(env_file: Path | None) -> str:
    value = os.environ.get("FRED_API_KEY")
    if value:
        return value.strip().strip('"').strip("'")
    if env_file is not None:
        parsed = read_env_file_value(env_file, "FRED_API_KEY")
        if parsed:
            return parsed
    raise SystemExit("FRED_API_KEY not found in environment or --fred-env-file")


def read_env_file_value(path: Path, key: str) -> str | None:
    if not path.exists():
        raise FileNotFoundError(path)
    prefix = f"{key}="
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if not line.startswith(prefix):
            continue
        value = line[len(prefix):].strip()
        if "#" in value and not value.startswith(('"', "'")):
            value = value.split("#", 1)[0].strip()
        return value.strip().strip('"').strip("'")
    return None


def fetch_v02_candidate_vintages(api_key: str) -> tuple[list[VintageRow], list[dict[str, Any]]]:
    import httpx

    from src.workers.macro_ingestion import TokenBucket
    from src.workers.macro_vintage import fetch_vintages, parse_alfred_vintages

    rows: list[VintageRow] = []
    stats: list[dict[str, Any]] = []
    bucket = TokenBucket()
    with httpx.Client(timeout=60.0) as client:
        for spec in V02_CHALLENGER_SERIES_SPECS:
            payload = fetch_vintages(client, api_key, spec.series_id, bucket)
            parsed = parse_alfred_vintages(spec.series_id, payload)
            series_rows = [
                VintageRow(
                    series_id=str(item["series_id"]),
                    observation_period=item["observation_period"],
                    vintage_date=item["vintage_date"],
                    value=float(item["value"]),
                    available_at=dt.datetime.combine(
                        item["vintage_date"], dt.time(0, 0), tzinfo=UTC
                    ),
                    revision_number=int(item["revision_number"]),
                    source_spec_version=V02_ALFRED_SOURCE_SPEC_VERSION,
                )
                for item in parsed
            ]
            rows.extend(series_rows)
            stats.append({
                "series_id": spec.series_id,
                "candidate_family": spec.candidate_family,
                "frequency": spec.frequency,
                "observation_count": len({
                    row.observation_period for row in series_rows
                }),
                "vintage_row_count": len(series_rows),
                "observation_min": date_or_none(
                    min((row.observation_period for row in series_rows), default=None)
                ),
                "observation_max": date_or_none(
                    max((row.observation_period for row in series_rows), default=None)
                ),
                "vintage_min": date_or_none(
                    min((row.vintage_date for row in series_rows), default=None)
                ),
                "vintage_max": date_or_none(
                    max((row.vintage_date for row in series_rows), default=None)
                ),
            })
    return merge_vintage_rows(rows), stats


def merge_vintage_rows(rows: list[VintageRow]) -> list[VintageRow]:
    merged: dict[tuple[str, dt.date, dt.date], VintageRow] = {}
    for row in rows:
        merged[(row.series_id, row.observation_period, row.vintage_date)] = row
    return sorted(
        merged.values(),
        key=lambda row: (
            row.series_id,
            row.observation_period,
            row.available_at,
            row.vintage_date,
        ),
    )


def parse_v02_qualification_args(argv: list[str]) -> V02QualificationConfig:
    ap = argparse.ArgumentParser(description="Qualify macro_family_map_v02 candidate data")
    ap.add_argument("command", choices=["v02-qualify"])
    ap.add_argument("--v01-feature-manifest", required=True)
    ap.add_argument("--vintage-cache", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--start-date", default="2014-02-19")
    ap.add_argument("--end-date", default="2026-06-24")
    ap.add_argument("--v01-screen-dir")
    ap.add_argument("--v01-local-dir")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--worker-commit")
    args = ap.parse_args(argv)
    return V02QualificationConfig(
        v01_feature_manifest=Path(args.v01_feature_manifest),
        vintage_cache=Path(args.vintage_cache),
        output_dir=Path(args.output_dir),
        start_date=dt.date.fromisoformat(args.start_date),
        end_date=dt.date.fromisoformat(args.end_date),
        v01_screen_dir=Path(args.v01_screen_dir) if args.v01_screen_dir else None,
        v01_local_dir=Path(args.v01_local_dir) if args.v01_local_dir else None,
        offline=args.offline,
        worker_commit=args.worker_commit,
    )


def run_v02_qualification(config: V02QualificationConfig) -> dict[str, Any]:
    if not config.offline:
        raise SystemExit("v02-qualify requires --offline to forbid DB/Tiingo access")
    started = dt.datetime.now(UTC)
    execution_id = str(uuid.uuid4())
    config.output_dir.mkdir(parents=True, exist_ok=True)

    v01_manifest = read_json_dict(config.v01_feature_manifest)
    v01_l2_hash = str(
        (v01_manifest.get("macro_feature_primitives") or {}).get("logical_hash") or ""
    )
    validate_parent_hash("v01 L2 macro_feature_primitives", v01_l2_hash, PARENT_V01_L2_HASH)
    all_vintages = read_vintage_cache(config.vintage_cache)
    union_ids = {cfg.series_id for cfg in V02_UNION_SERIES}
    vintages = [row for row in all_vintages if row.series_id in union_ids]
    calendar = business_days(config.start_date, config.end_date)
    grouped = group_rows(vintages)
    baseline_pit = load_optional_v01_panel(
        config.v01_feature_manifest.parent / "pit_selection_panel.parquet",
        calendar,
    )
    baseline_l2 = load_optional_v01_panel(
        config.v01_feature_manifest.parent / "macro_feature_primitives.parquet",
        calendar,
    )
    challenger_ids = {spec.series_id for spec in V02_CHALLENGER_SERIES_SPECS}
    challenger_rows = [row for row in vintages if row.series_id in challenger_ids]
    challenger_grouped = group_rows(challenger_rows)
    if baseline_pit is None or baseline_l2 is None:
        pit_selection_panel = build_pit_selection_panel(
            grouped,
            calendar,
            series_configs=V02_UNION_SERIES,
        )
        macro_feature_primitives = build_macro_feature_primitives(
            grouped,
            calendar,
            series_configs=V02_UNION_SERIES,
        )
    else:
        pit_selection_panel = baseline_pit + build_pit_selection_panel(
            challenger_grouped,
            calendar,
            series_configs=tuple(spec.config for spec in V02_CHALLENGER_SERIES_SPECS),
        )
        macro_feature_primitives = baseline_l2 + build_macro_feature_primitives(
            challenger_grouped,
            calendar,
            series_configs=tuple(spec.config for spec in V02_CHALLENGER_SERIES_SPECS),
        )
    series_audit = build_v02_series_audit(vintages, macro_feature_primitives, calendar)

    l0_hash = rows_hash(vintages)
    calendar_hash = business_calendar_hash(calendar)
    mapping_hash = series_family_mapping_hash(V02_UNION_SERIES)
    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    git_dirty = bool(run_text(
        ["git", "status", "--porcelain"], cwd=Path(__file__).resolve().parents[1]
    ))
    candidate_status_counts = count_values(
        row["eligibility_status"]
        for row in series_audit
        if row["series_id"] not in {cfg.series_id for cfg in BASELINE_SERIES}
    )
    candidate_sets = build_v02_candidate_sets(series_audit)
    qualification_status = (
        "qualified"
        if ready_for_all_v02(series_audit)
        else "qualified_with_exclusions"
        if candidate_sets["v02a_growth_screen"]["ready_for_grid"]
        else "blocked_data_qualification"
    )
    ready_for_grid = bool(series_audit) and all(
        row["eligibility_status"] in {"baseline_v01_preserved", "eligible_for_v02_screen"}
        for row in series_audit
    )
    blocked_reasons = sorted(
        status for status in candidate_status_counts if status != "eligible_for_v02_screen"
    )

    vintage_path = config.output_dir / "macro_vintages_v02_union.parquet"
    pit_path = config.output_dir / "pit_selection_panel_v02_union.parquet"
    l2_path = config.output_dir / "macro_feature_primitives_v02_union.parquet"
    audit_path = config.output_dir / "v02_series_audit.parquet"
    write_parquet(vintage_path, [
        {
            "series_id": row.series_id,
            "observation_period": row.observation_period.isoformat(),
            "vintage_date": row.vintage_date.isoformat(),
            "value": row.value,
            "available_at": row.available_at.isoformat(),
            "revision_number": row.revision_number,
            "source_spec_version": row.source_spec_version,
        }
        for row in vintages
    ])
    write_parquet(pit_path, pit_selection_panel)
    write_parquet(l2_path, macro_feature_primitives)
    write_parquet(audit_path, series_audit)
    l0_hash = rows_hash(read_vintage_cache(vintage_path))
    l1_hash = logical_records_hash(read_parquet_records(pit_path))
    l2_hash = logical_records_hash(read_parquet_records(l2_path))
    audit_hash = logical_records_hash(read_parquet_records(audit_path))

    v01_exhaustion = build_v01_exhaustion_manifest(config, v01_l2_hash)
    write_json(config.output_dir / "v01_exhaustion_manifest.json", v01_exhaustion)

    pit_selection_manifest = {
        "panel": "pit_selection_panel_v02_union",
        "schema_version": L1_SCHEMA_VERSION,
        "row_count": len(pit_selection_panel),
        "logical_hash": l1_hash,
        "business_date_calendar_hash": calendar_hash,
        "series_family_mapping_hash": mapping_hash,
        "series_universe_hash": v02_series_universe_hash(),
        "vintage_data_hash": l0_hash,
        "selection_modes": ["latest", "first_release"],
        "selection_roles": {
            "latest": "pit_runtime_candidate",
            "first_release": "revised_vintage_counterfactual",
        },
        "counterfactual_runtime_allowed": False,
        "grain": "business_date x series_id",
    }
    write_json(config.output_dir / "pit_selection_manifest_v02_union.json", pit_selection_manifest)

    feature_manifest = {
        "schema_version": L2_SCHEMA_VERSION,
        "panel": "macro_feature_primitives_v02_union",
        "file_name": "macro_feature_primitives_v02_union.parquet",
        "parameter_independent": True,
        "counterfactual_runtime_allowed": False,
        "business_date_calendar_hash": calendar_hash,
        "series_family_mapping_hash": mapping_hash,
        "family_map_candidate_set": "macro_family_map_v02_candidate_growth_qualification",
        "series_universe_hash": v02_series_universe_hash(),
        "vintage_data_hash": l0_hash,
        "release_calendar_hash": calendar_hash,
        "parent_v01_l2_hash": v01_l2_hash,
        "parent_hashes": {
            "parent_v01_l2_hash": v01_l2_hash,
            "l1_logical_hash": l1_hash,
            "vintage_data_hash": l0_hash,
            "v02_series_audit_logical_hash": audit_hash,
        },
        "selection_roles": {
            "latest": "pit_runtime_candidate",
            "first_release": "revised_vintage_counterfactual",
        },
        "macro_feature_primitives": {
            "row_count": len(macro_feature_primitives),
            "logical_hash": l2_hash,
            "grain": "business_date x selection_mode x series_id",
            "file_name": "macro_feature_primitives_v02_union.parquet",
        },
        "market_derived_series_excluded": list(V02_EXCLUDED_MARKET_DERIVED_SERIES),
        "inflation_map_status": "unchanged_from_A31-C-TEMPORAL-STABLE",
        "a32_status": "A32-REF_frozen_not_run",
        "ready_for_grid": ready_for_grid,
        "qualification_status": qualification_status,
        "candidate_sets": candidate_sets,
        "v02_g0_control_ready": True,
        "v02b_sloos_g0_control_ready": True,
        "blocked_reasons": blocked_reasons,
        "worker_commit": worker_commit,
        "git_dirty": git_dirty,
    }
    write_json(config.output_dir / "feature_manifest_v02_union.json", feature_manifest)

    finished = dt.datetime.now(UTC)
    qualification_manifest = {
        "schema_version": V02_QUALIFICATION_SCHEMA_VERSION,
        "status": "ready_for_v02_grid" if ready_for_grid else "blocked_data_qualification",
        "execution_id": execution_id,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "offline": True,
        "external_access": "disabled_by_v02_qualification_path",
        "worker_commit": worker_commit,
        "git_dirty": git_dirty,
        "parent_v01_l2_hash": v01_l2_hash,
        "family_map_candidate_set": feature_manifest["family_map_candidate_set"],
        "series_universe_hash": feature_manifest["series_universe_hash"],
        "vintage_data_hash": l0_hash,
        "release_calendar_hash": calendar_hash,
        "l1_schema_version": L1_SCHEMA_VERSION,
        "l2_schema_version": L2_SCHEMA_VERSION,
        "l1_logical_hash": l1_hash,
        "l2_macro_logical_hash": l2_hash,
        "v02_series_audit_logical_hash": audit_hash,
        "candidate_eligibility_status_counts": candidate_status_counts,
        "ready_for_grid": ready_for_grid,
        "qualification_status": qualification_status,
        "candidate_sets": candidate_sets,
        "v02_g0_control_ready": True,
        "v02b_sloos_g0_control_ready": True,
        "blocked_reasons": blocked_reasons,
        "v02_g0_control_status": (
            "ready_not_run"
            if candidate_sets["v02a_growth_screen"]["ready_for_grid"]
            else "blocked_until_v02a_qualifies"
        ),
        "v02b_sloos_g0_control_status": (
            "ready_not_run"
            if candidate_sets["v02b_sloos_screen"]["ready_for_grid"]
            else "blocked_until_sloos_quarterly_qualifies"
        ),
        "a4_status": "candidate_seed_only",
        "a5_status": "blocked",
        "artifact_hashes": hash_artifacts(config.output_dir, [
            "macro_vintages_v02_union.parquet",
            "pit_selection_panel_v02_union.parquet",
            "pit_selection_manifest_v02_union.json",
            "macro_feature_primitives_v02_union.parquet",
            "feature_manifest_v02_union.json",
            "v02_series_audit.parquet",
            "v01_exhaustion_manifest.json",
        ]),
        "logical_artifacts": {
            "macro_vintages_v02_union.parquet": l0_hash,
            "pit_selection_panel_v02_union.parquet": l1_hash,
            "pit_selection_manifest_v02_union.json": logical_payload_hash(pit_selection_manifest),
            "macro_feature_primitives_v02_union.parquet": l2_hash,
            "feature_manifest_v02_union.json": logical_payload_hash(feature_manifest),
            "v02_series_audit.parquet": audit_hash,
            "v01_exhaustion_manifest.json": logical_payload_hash(v01_exhaustion),
        },
    }
    write_json(config.output_dir / "v02_qualification_manifest.json", qualification_manifest)
    return {
        "status": qualification_manifest["status"],
        "output_dir": str(config.output_dir),
        "execution_id": execution_id,
        "ready_for_grid": ready_for_grid,
        "qualification_status": qualification_status,
        "parent_v01_l2_hash": v01_l2_hash,
        "l2_macro_logical_hash": l2_hash,
        "v02_series_audit_logical_hash": audit_hash,
        "candidate_eligibility_status_counts": candidate_status_counts,
    }


def build_v02_series_audit(
    vintage_rows: list[VintageRow],
    macro_feature_primitives: list[dict[str, Any]],
    calendar: list[dt.date],
) -> list[dict[str, Any]]:
    rows_by_series: dict[str, list[VintageRow]] = {}
    for row in vintage_rows:
        rows_by_series.setdefault(row.series_id, []).append(row)
    primitives_by_series: dict[str, list[dict[str, Any]]] = {}
    for row in macro_feature_primitives:
        primitives_by_series.setdefault(str(row["series_id"]), []).append(row)

    audit_rows: list[dict[str, Any]] = []
    baseline_ids = {cfg.series_id for cfg in BASELINE_SERIES}
    for spec in V02_UNION_SERIES_SPECS:
        sid = spec.series_id
        series_rows = sorted(
            rows_by_series.get(sid, []),
            key=lambda r: (r.observation_period, r.available_at, r.vintage_date),
        )
        primitive_rows = primitives_by_series.get(sid, [])
        latest_rows = [
            row for row in primitive_rows if row.get("selection_mode") == "latest"
        ]
        coverage_rate = (
            sum(1 for row in latest_rows if row.get("coverage")) / len(calendar)
            if calendar else 0.0
        )
        first_eligible = next(
            (
                str(row["business_date"])
                for row in latest_rows
                if row.get("coverage") and row.get("reference_series_score") is not None
            ),
            None,
        )
        release_lag_dist = distribution([
            float((row.available_at.date() - row.observation_period).days)
            for row in series_rows
        ])
        by_observation: dict[dt.date, list[VintageRow]] = {}
        for row in series_rows:
            by_observation.setdefault(row.observation_period, []).append(row)
        revised_observations = [
            period
            for period, rows in by_observation.items()
            if len(rows) > 1 or max(row.revision_number for row in rows) > 0
        ]
        revision_deltas: list[float] = []
        for rows in by_observation.values():
            ordered = sorted(rows, key=lambda r: (r.available_at, r.vintage_date))
            for prev, current in zip(ordered, ordered[1:]):
                revision_deltas.append(abs(current.value - prev.value))
        revision_dist = distribution(revision_deltas)
        sign_stats = v02_sign_revision_stats(primitive_rows, calendar)
        eligibility = v02_eligibility_status(
            spec,
            series_rows,
            coverage_rate,
            first_eligible,
            baseline_ids,
        )
        audit_rows.append({
            "series_id": sid,
            "candidate_family": spec.candidate_family,
            "frequency": spec.frequency,
            "first_eligible_date": first_eligible,
            "coverage_rate": coverage_rate,
            "median_release_lag": release_lag_dist["median"],
            "p90_release_lag": release_lag_dist["p90"],
            "freshness_limit": spec.freshness_limit_days,
            "observations_ever_revised_rate": (
                len(revised_observations) / len(by_observation)
                if by_observation else None
            ),
            "median_abs_revision": revision_dist["median"],
            "p90_abs_revision": revision_dist["p90"],
            "transformed_sign_change_rate": sign_stats["transformed_sign_change_rate"],
            "days_affected_by_revision": sign_stats["days_affected_by_revision"],
            "revision_episode_count": sign_stats["revision_episode_count"],
            "revision_episode_median_duration": sign_stats[
                "revision_episode_median_duration"
            ],
            "alfred_lineage_complete": bool(series_rows) and all(
                bool(row.source_spec_version) for row in series_rows
            ),
            "license_status": spec.license_status,
            "eligibility_status": eligibility,
        })
    audit_rows.sort(key=lambda row: row["series_id"])
    return audit_rows


def load_optional_v01_panel(path: Path, calendar: list[dt.date]) -> list[dict[str, Any]] | None:
    if not path.exists():
        return None
    allowed_dates = {day.isoformat() for day in calendar}
    rows = read_parquet_records(path)
    return [
        row for row in rows
        if str(row.get("business_date") or row.get("date")) in allowed_dates
    ]


def v02_sign_revision_stats(
    primitive_rows: list[dict[str, Any]],
    calendar: list[dt.date],
) -> dict[str, Any]:
    by_date_mode: dict[tuple[str, str], dict[str, Any]] = {}
    for row in primitive_rows:
        by_date_mode[(str(row["business_date"]), str(row["selection_mode"]))] = row
    affected_dates: list[dt.date] = []
    comparable = 0
    for day in calendar:
        key = day.isoformat()
        latest = by_date_mode.get((key, "latest"))
        first = by_date_mode.get((key, "first_release"))
        latest_sign = sign_of(latest.get("reference_series_score")) if latest else None
        first_sign = sign_of(first.get("reference_series_score")) if first else None
        if latest_sign is None or first_sign is None:
            continue
        comparable += 1
        if latest_sign != first_sign:
            affected_dates.append(day)
    durations = contiguous_date_durations(affected_dates, calendar)
    return {
        "transformed_sign_change_rate": (
            len(affected_dates) / comparable if comparable else None
        ),
        "days_affected_by_revision": len(affected_dates),
        "revision_episode_count": len(durations),
        "revision_episode_median_duration": (
            statistics.median(durations) if durations else None
        ),
    }


def contiguous_date_durations(dates: list[dt.date], calendar: list[dt.date]) -> list[int]:
    if not dates:
        return []
    affected = set(dates)
    durations: list[int] = []
    current = 0
    for day in calendar:
        if day in affected:
            current += 1
        elif current:
            durations.append(current)
            current = 0
    if current:
        durations.append(current)
    return durations


def v02_eligibility_status(
    spec: V02SeriesSpec,
    rows: list[VintageRow],
    coverage_rate: float,
    first_eligible_date: str | None,
    baseline_ids: set[str],
) -> str:
    if spec.series_id in V02_EXCLUDED_MARKET_DERIVED_SERIES:
        return "market_derived_excluded"
    if spec.series_id in baseline_ids:
        return "baseline_v01_preserved"
    if not rows:
        return "missing_vintages"
    if first_eligible_date is None:
        return "insufficient_transform_history"
    if coverage_rate < 0.80:
        return "fold_coverage_failed"
    return "eligible_for_v02_screen"


def count_values(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def ready_for_all_v02(series_audit: list[dict[str, Any]]) -> bool:
    return all(
        row["eligibility_status"] in {"baseline_v01_preserved", "eligible_for_v02_screen"}
        for row in series_audit
    )


def build_v02_candidate_sets(series_audit: list[dict[str, Any]]) -> dict[str, Any]:
    by_series = {str(row["series_id"]): row for row in series_audit}
    v02a_ready = all(
        by_series.get(series_id, {}).get("eligibility_status") == "eligible_for_v02_screen"
        for series_id in V02A_GROWTH_SCREEN_SERIES
    )
    v02b_sloos_ready = all(
        by_series.get(series_id, {}).get("eligibility_status") == "eligible_for_v02_screen"
        for series_id in V02B_SLOOS_SCREEN_SERIES
    )
    deferred = {
        series_id: v02_deferred_reason(series_id, by_series.get(series_id, {}))
        for series_id in V02B_DEFERRED_SERIES
    }
    return {
        "v02a_growth_screen": {
            "ready_for_grid": v02a_ready,
            "eligible_series": list(V02A_GROWTH_SCREEN_SERIES),
            "series_set_hash": stable_hash(list(V02A_GROWTH_SCREEN_SERIES)),
        },
        "v02b_sloos_screen": {
            "ready_for_grid": v02b_sloos_ready,
            "eligible_series": list(V02B_SLOOS_SCREEN_SERIES),
            "series_set_hash": stable_hash(list(V02B_SLOOS_SCREEN_SERIES)),
            "transform_class": "quarterly_survey_level_v1",
            "min_history": f"{MIN_QUARTERLY_SURVEY_OBS} quarters",
        },
        "v02b_deferred": {
            "ready_for_grid": False,
            "deferred_series": deferred,
            "series_set_hash": stable_hash(list(V02B_DEFERRED_SERIES)),
        },
    }


def v02_deferred_reason(series_id: str, row: dict[str, Any]) -> str:
    if series_id in {"DRTSCILM", "DRSDCILM"}:
        return "quarterly_transform_required"
    return str(row.get("eligibility_status") or "not_audited")


def build_v01_exhaustion_manifest(
    config: V02QualificationConfig,
    v01_l2_hash: str,
) -> dict[str, Any]:
    return {
        "status": "exhausted_vintage_instability",
        "decision": "A3.1 v01 baseline exhausted; do not advance to A3.2",
        "parent_v01_l2_hash": v01_l2_hash,
        "global_benchmark": load_a31_result_by_name(
            config.v01_screen_dir,
            "A31-C-TEMPORAL-STABLE",
        ),
        "growth_benchmark": load_a31_result_by_name(
            config.v01_local_dir,
            "A31-CLOCAL-TEMPORAL-STABLE-PCEC025",
        ),
        "a32_status": "A32-REF_frozen_not_optimized",
        "a4_status": "candidate_seed_only",
        "a5_status": "blocked",
    }


def load_a31_result_by_name(grid_dir: Path | None, name: str) -> dict[str, Any]:
    if grid_dir is None:
        return {"name": name, "status": "not_provided"}
    results_dir = grid_dir / "results"
    if not results_dir.exists():
        return {"name": name, "status": "missing_results_dir", "grid_dir": str(grid_dir)}
    for summary_path in sorted(results_dir.glob("*/result_summary.json")):
        summary = read_json_dict(summary_path)
        if summary.get("a31_config_name") != name:
            continue
        result_dir = summary_path.parent
        manifest_path = result_dir / "result_manifest.json"
        metrics_by_fold_path = result_dir / "metrics_by_fold.json"
        manifest = read_json_dict(manifest_path) if manifest_path.exists() else {}
        metrics_payload = (
            read_json_dict(metrics_by_fold_path) if metrics_by_fold_path.exists() else {}
        )
        return {
            "name": name,
            "status": "found",
            "result_dir": str(result_dir),
            "a31_config_hash": summary.get("a31_config_hash"),
            "candidate_revision_change_rate": summary.get(
                "candidate_revision_change_rate"
            ),
            "growth_sign_revision_change_days": summary.get(
                "growth_sign_revision_change_days"
            ),
            "inflation_sign_revision_change_days": summary.get(
                "inflation_sign_revision_change_days"
            ),
            "candidate_flips_per_year": summary.get("candidate_flips_per_year"),
            "valid_rate": summary.get("valid_rate"),
            "abstain_rate": summary.get("abstain_rate"),
            "metrics_logical_hash": summary.get("metrics_logical_hash"),
            "metrics_by_fold_logical_hash": summary.get(
                "metrics_by_fold_logical_hash"
            ),
            "result_manifest_metrics_logical_hash": manifest.get(
                "metrics_logical_hash"
            ),
            "fold_rows": metrics_payload.get("rows", []),
        }
    return {"name": name, "status": "not_found", "grid_dir": str(grid_dir)}


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
                expected_a32_hash=a32_config_hash(a32_ref),
                expected_evaluation_hash=evaluation_hash(
                    a31_hash,
                    a32_config_hash(a32_ref),
                ),
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
    progression_decision = build_a31_progression_decision_manifest(
        summary_rows,
        metric_rows,
        config_catalog_path=config.config_catalog,
        failure_count=len(failures),
    )
    write_json(output_dir / "a31_progression_decision.json", progression_decision)

    finished = dt.datetime.now(UTC)
    a32_ref_hashes = sorted({row["a32_config_hash"] for row in summary_rows})
    if len(a32_ref_hashes) != 1:
        raise ValueError(f"A32-REF must be canonical and unique; got {a32_ref_hashes}")
    v02a_statuses = sorted({
        str(item["metadata"].get("v02a_status"))
        for item in normalized_catalog["configs"]
        if item["metadata"].get("v02a_status")
    })
    v02a_diagnostic_references = sorted({
        str(ref)
        for item in normalized_catalog["configs"]
        for ref in item["metadata"].get("v02a_diagnostic_references", [])
    })
    grid_manifest = {
        "schema_version": A31_GRID_SCHEMA_VERSION,
        "status": "ok" if not failures else "partial_with_failures",
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
        "a32_ref_hashes": a32_ref_hashes,
        "evaluation_hashes": sorted({row["evaluation_hash"] for row in summary_rows}),
        "v02a_status": v02a_statuses[0] if len(v02a_statuses) == 1 else None,
        "v02a_diagnostic_references": v02a_diagnostic_references,
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
                "a31_progression_decision.json",
            ],
        ),
        "stage_timings_seconds": aggregate_stage_timings(summary_rows),
        "notes": [
            "A3.1 uses A32-REF only",
            "No configuration is frozen, production_candidate, or activation_ready",
            f"A4 status: {progression_decision.get('a4_status')}",
            "A5 remains blocked",
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


def parse_a31_v03_grid_args(argv: list[str]) -> A31V03GridConfig:
    ap = argparse.ArgumentParser(description="Run macro_v03 revision-robust one-factor grid")
    ap.add_argument("command", choices=["a31-v03-grid"])
    ap.add_argument("--feature-manifest", required=True)
    ap.add_argument("--revision-uncertainty-manifest", required=True)
    ap.add_argument("--config-catalog", required=True)
    ap.add_argument("--a32-grid-dir", required=True)
    ap.add_argument("--output-dir")
    ap.add_argument("--jobs", type=int, default=1)
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--worker-commit")
    ap.add_argument("--a32-name", default="A32-G0.35-I0.35-X0.10-C0.60-D1.25")
    args = ap.parse_args(argv)
    if args.jobs < 1:
        raise SystemExit("--jobs must be >= 1")
    return A31V03GridConfig(
        feature_manifest=Path(args.feature_manifest),
        revision_uncertainty_manifest=Path(args.revision_uncertainty_manifest),
        config_catalog=Path(args.config_catalog),
        a32_grid_dir=Path(args.a32_grid_dir),
        output_dir=Path(args.output_dir) if args.output_dir else None,
        jobs=args.jobs,
        offline=args.offline,
        worker_commit=args.worker_commit,
        a32_name=args.a32_name,
    )


def evaluate_a31_v03_item(
    *,
    item: dict[str, Any],
    l2_records: list[dict[str, Any]],
    l2_hash: str,
    uncertainty_by_key: dict[tuple[str, str, str, str], dict[str, Any]],
    uncertainty_hash: str,
    a32: A32Config,
    a32_hash: str,
) -> dict[str, Any]:
    timings: dict[str, float] = {}
    a31 = A31Config(**item["config"])
    t0 = time.perf_counter()
    l3_rows, _, l3_manifest = build_l3_score_panel(
        l2_records,
        a31,
        l2_macro_logical_hash=l2_hash,
        expected_l2_macro_logical_hash=l2_hash,
        revision_uncertainty_by_key=uncertainty_by_key,
        revision_uncertainty_logical_hash=uncertainty_hash,
    )
    timings["compute_l3"] = time.perf_counter() - t0

    a31_hash = str(l3_manifest["a31_config_hash"])
    eval_hash = evaluation_hash(a31_hash, a32_hash)

    t0 = time.perf_counter()
    runtime, _ = run_l4_state_machine(l3_rows, a32, selection_mode="latest")
    counterfactual, _ = run_l4_state_machine(
        l3_rows, a32, selection_mode="first_release"
    )
    timings["run_l4"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    metrics_full = build_macro_metrics(
        runtime,
        first_release_replay=counterfactual,
    )
    classification = classify_a32_grid_result(metrics_full)
    rows = evaluation_metric_rows(
        runtime,
        counterfactual,
        a31,
        a32,
        a31_hash,
        a32_hash,
        eval_hash,
        classification,
    )
    summary = a31_v03_summary_row(item, a32, a32_hash, eval_hash, rows)
    summary.update({
        "classification": classification,
        "runtime_replay_logical_hash": logical_records_hash(runtime),
        "counterfactual_replay_logical_hash": logical_records_hash(counterfactual),
    })
    timings["compute_metrics"] = time.perf_counter() - t0
    return {"summary": summary, "metrics_rows": rows, "timings_seconds": timings}


def a31_v03_grid_worker(task: dict[str, Any]) -> dict[str, Any]:
    timings: dict[str, float] = {}
    t0 = time.perf_counter()
    feature_manifest, _, l2_hash, l2_records = load_l2_macro_from_feature_manifest(
        Path(task["feature_manifest"])
    )
    validate_parent_hash(
        "a31-v03 worker L2 macro_feature_primitives",
        str(task["l2_macro_logical_hash"]),
        l2_hash,
    )
    uncertainty_manifest, uncertainty_hash, uncertainty_rows = (
        load_revision_uncertainty_from_manifest(Path(task["revision_uncertainty_manifest"]))
    )
    validate_parent_hash(
        "a31-v03 worker revision uncertainty",
        str(task["revision_uncertainty_logical_hash"]),
        uncertainty_hash,
    )
    parent_uncertainty_l2 = (
        uncertainty_manifest.get("parent_hashes") or {}
    ).get("l2_macro_logical_hash")
    validate_parent_hash("a31-v03 worker uncertainty parent L2", str(parent_uncertainty_l2), l2_hash)
    if feature_manifest.get("schema_version") is None:
        raise ValueError("feature manifest missing schema_version")
    uncertainty_by_key = revision_uncertainty_keyed(uncertainty_rows)
    a32 = load_a32_config_from_grid(Path(task["a32_grid_dir"]), str(task["a32_name"]))
    a32_hash = a32_config_hash(a32)
    validate_parent_hash("a31-v03 worker A32 hash", str(task["a32_hash"]), a32_hash)
    timings["load_inputs"] = time.perf_counter() - t0

    result = evaluate_a31_v03_item(
        item=task["a31_item"],
        l2_records=l2_records,
        l2_hash=l2_hash,
        uncertainty_by_key=uncertainty_by_key,
        uncertainty_hash=uncertainty_hash,
        a32=a32,
        a32_hash=a32_hash,
    )
    timings.update(result["timings_seconds"])
    result["timings_seconds"] = timings
    return result


def run_a31_v03_grid(config: A31V03GridConfig) -> dict[str, Any]:
    if not config.offline:
        raise SystemExit("a31-v03-grid requires --offline")
    started = dt.datetime.now(UTC)
    execution_id = str(uuid.uuid4())
    output_dir = config.output_dir or (config.feature_manifest.parent / "a31_v03_grid")
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_manifest, _, l2_hash, l2_records = load_l2_macro_from_feature_manifest(
        config.feature_manifest
    )
    uncertainty_manifest, uncertainty_hash, uncertainty_rows = (
        load_revision_uncertainty_from_manifest(config.revision_uncertainty_manifest)
    )
    parent_uncertainty_l2 = (
        uncertainty_manifest.get("parent_hashes") or {}
    ).get("l2_macro_logical_hash")
    validate_parent_hash("v03 uncertainty parent L2", str(parent_uncertainty_l2), l2_hash)
    uncertainty_by_key = revision_uncertainty_keyed(uncertainty_rows)
    catalog_payload = read_catalog_payload(config.config_catalog)
    normalized_catalog, catalog_hash = normalize_a31_catalog(
        catalog_payload,
        l2_macro_logical_hash=l2_hash,
        source_path=config.config_catalog,
    )
    a32 = load_a32_config_from_grid(config.a32_grid_dir, config.a32_name)
    a32_hash = a32_config_hash(a32)
    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    git_dirty = bool(run_text(
        ["git", "status", "--porcelain"], cwd=Path(__file__).resolve().parents[1]
    ))

    write_json(output_dir / "config_catalog.normalized.json", normalized_catalog)
    summary_rows: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    computed: list[dict[str, Any]] = []
    if config.jobs == 1:
        for item in normalized_catalog["configs"]:
            try:
                computed.append(
                    evaluate_a31_v03_item(
                        item=item,
                        l2_records=l2_records,
                        l2_hash=l2_hash,
                        uncertainty_by_key=uncertainty_by_key,
                        uncertainty_hash=uncertainty_hash,
                        a32=a32,
                        a32_hash=a32_hash,
                    )
                )
            except Exception as exc:  # pragma: no cover - per-config isolation
                failures.append(a31_failure_record({"a31_item": item}, exc))
    else:
        tasks = [
            {
                "feature_manifest": str(config.feature_manifest.resolve()),
                "revision_uncertainty_manifest": str(
                    config.revision_uncertainty_manifest.resolve()
                ),
                "a32_grid_dir": str(config.a32_grid_dir.resolve()),
                "a32_name": config.a32_name,
                "a32_hash": a32_hash,
                "l2_macro_logical_hash": l2_hash,
                "revision_uncertainty_logical_hash": uncertainty_hash,
                "a31_item": item,
            }
            for item in normalized_catalog["configs"]
        ]
        with concurrent.futures.ProcessPoolExecutor(max_workers=config.jobs) as pool:
            futures = {pool.submit(a31_v03_grid_worker, task): task for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                task = futures[future]
                try:
                    computed.append(future.result())
                except Exception as exc:  # pragma: no cover - defensive per-config isolation
                    failures.append(a31_failure_record(task, exc))

    summary_rows = [row["summary"] for row in computed]
    metric_rows = [metric for row in computed for metric in row["metrics_rows"]]
    stage_timings = aggregate_timing_records(
        [row.get("timings_seconds", {}) for row in computed]
    )

    apply_v03_gate_status(summary_rows, metric_rows)
    summary_rows.sort(key=v03_summary_sort_key)
    metric_rows.sort(key=lambda row: (
        str(row["a31_config_hash"]),
        str(row["a32_config_hash"]),
        str(row["fold"]),
    ))
    write_parquet(output_dir / "a31_v03_grid_summary.parquet", summary_rows)
    write_parquet(output_dir / "a31_v03_grid_metrics.parquet", metric_rows)
    write_json(output_dir / "failures.json", {"failures": failures})
    stop_decision = v03_stop_decision(summary_rows)
    finished = dt.datetime.now(UTC)
    manifest = {
        "schema_version": A31_V03_GRID_SCHEMA_VERSION,
        "status": "ok" if not failures else "partial_with_failures",
        "grid": "macro_v03_revision_robust",
        "execution_id": execution_id,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "jobs": config.jobs,
        "offline": True,
        "external_access": "disabled_by_a31_v03_grid_path",
        "worker_commit": worker_commit,
        "git_dirty": git_dirty,
        "feature_manifest_path": str(config.feature_manifest),
        "revision_uncertainty_manifest_path": str(config.revision_uncertainty_manifest),
        "config_catalog_path": str(config.config_catalog),
        "a32_grid_dir": str(config.a32_grid_dir),
        "a32_config_name": a32.name,
        "a32_config_hash": a32_hash,
        "parent_hashes": {
            "l2_macro_logical_hash": l2_hash,
            "revision_uncertainty_logical_hash": uncertainty_hash,
            "business_date_calendar_hash": feature_manifest.get(
                "business_date_calendar_hash"
            ),
            "series_family_mapping_hash": feature_manifest.get(
                "series_family_mapping_hash"
            ),
        },
        "config_catalog_hash": catalog_hash,
        "config_count": len(normalized_catalog["configs"]),
        "evaluation_count": len(summary_rows),
        "failure_count": len(failures),
        "summary_logical_hash": logical_records_hash(summary_rows),
        "metrics_logical_hash": logical_records_hash(metric_rows),
        "stop_decision": stop_decision,
        "a3_status": "open_macro_v03",
        "a4_status": A4_PROVISIONAL_STATUS,
        "a5_status": "blocked",
        "stage_timings_seconds": stage_timings,
        "artifact_hashes": hash_artifacts(
            output_dir,
            [
                "config_catalog.normalized.json",
                "a31_v03_grid_summary.parquet",
                "a31_v03_grid_metrics.parquet",
                "failures.json",
            ],
        ),
    }
    write_json(output_dir / "a31_v03_grid_manifest.json", manifest)
    write_text(
        output_dir / "a31_v03_revision_robust_report.md",
        render_a31_v03_report(manifest, summary_rows),
    )
    return {
        "status": manifest["status"],
        "output_dir": str(output_dir),
        "evaluation_count": len(summary_rows),
        "failure_count": len(failures),
        "jobs": config.jobs,
        "stop_decision": stop_decision["decision"],
        "summary_logical_hash": manifest["summary_logical_hash"],
        "metrics_logical_hash": manifest["metrics_logical_hash"],
    }


def a31_v03_summary_row(
    item: dict[str, Any],
    a32: A32Config,
    a32_hash: str,
    eval_hash: str,
    metric_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    full = next(row for row in metric_rows if row["fold"] == "full")
    post = next(
        (row for row in metric_rows if row["fold"] == "post_initialization"),
        full,
    )
    transition_dist = parse_json_metric(post.get("transition_timing_displacement"))
    return {
        "a31_config_hash": item["a31_config_hash"],
        "a31_config_name": item["config"]["name"],
        "a32_config_hash": a32_hash,
        "a32_config_name": a32.name,
        "evaluation_hash": eval_hash,
        "catalog_index": item["catalog_index"],
        "round": item["metadata"].get("round"),
        "description": item["metadata"].get("description"),
        "distance_from_ref": item["distance_from_ref"],
        "revision_soft_threshold_quantile": item["config"].get(
            "revision_soft_threshold_quantile"
        ),
        "family_consensus_min": item["config"].get("family_consensus_min"),
        "aggregation_method": item["config"].get("aggregation_method"),
        "full_candidate_revision_change_rate": full.get(
            "candidate_quadrant_change_rate_calendar"
        ),
        "post_initialization_candidate_revision_change_rate": post.get(
            "candidate_quadrant_change_rate_calendar"
        ),
        "full_comparable_candidate_revision_change_rate": full.get(
            "candidate_revision_change_rate"
        ),
        "post_initialization_comparable_candidate_revision_change_rate": post.get(
            "candidate_revision_change_rate"
        ),
        "growth_raw_sign_change_days": full.get("growth_raw_sign_change_days"),
        "inflation_raw_sign_change_days": full.get("inflation_raw_sign_change_days"),
        "post_initialization_consumable_coverage": post.get(
            "consumable_state_coverage"
        ),
        "post_initialization_valid_rate": post.get("valid_rate"),
        "post_initialization_neutral_rate": post.get("neutral_rate"),
        "post_initialization_candidate_defined_rate": post.get(
            "candidate_defined_rate"
        ),
        "post_initialization_transition_displacement_p90": transition_dist.get("p90"),
        "post_initialization_episode_duration_p90": post.get(
            "revision_episode_duration_p90"
        ),
        "full_neutral_rate": full.get("neutral_rate"),
        "full_candidate_defined_rate": full.get("candidate_defined_rate"),
        "full_valid_rate": full.get("valid_rate"),
        "full_consumable_coverage": full.get("consumable_state_coverage"),
        "v03_gate_status": "pending",
        "frozen": False,
        "production_candidate": False,
        "activation_ready": False,
    }


def apply_v03_gate_status(
    summary_rows: list[dict[str, Any]], metric_rows: list[dict[str, Any]]
) -> None:
    control = next(
        (row for row in summary_rows if row["a31_config_name"] == "V03-G0-CONTROL"),
        None,
    )
    control_hash = control.get("a31_config_hash") if control else None
    for row in summary_rows:
        fold_deltas = v03_fold_revision_deltas(
            metric_rows,
            str(row["a31_config_hash"]),
            str(control_hash) if control_hash else None,
        )
        row["fold_revision_deltas_vs_control"] = json.dumps(fold_deltas, sort_keys=True)
        improves = [
            delta for delta in fold_deltas.values()
            if delta is not None and delta < 0
        ]
        worsens = [
            delta for delta in fold_deltas.values()
            if delta is not None and delta > 0.02
        ]
        passes = (
            float(row.get("full_candidate_revision_change_rate") or 1.0) < 0.15
            and float(row.get("post_initialization_candidate_revision_change_rate") or 1.0) < 0.15
            and len(improves) >= 2
            and not worsens
            and int(row.get("growth_raw_sign_change_days") or 10**9) < 400
            and int(row.get("inflation_raw_sign_change_days") or 10**9) <= 217
            and float(row.get("post_initialization_consumable_coverage") or 0.0) >= 0.43
            and float(row.get("post_initialization_transition_displacement_p90") or 10**9) < 45.0
            and float(row.get("post_initialization_episode_duration_p90") or 10**9) < 18.0
        )
        row["v03_improved_folds_vs_control"] = len(improves)
        row["v03_fold_worse_over_2pp"] = bool(worsens)
        row["v03_gate_status"] = (
            "v03_intermediate_pass" if passes else "v03_screened_out"
        )


def v03_fold_revision_deltas(
    metric_rows: list[dict[str, Any]], config_hash: str, control_hash: str | None
) -> dict[str, float | None]:
    if control_hash is None:
        return {"2014_2017": None, "2018_2021": None, "2022_2026": None}
    by_key = {
        (str(row["a31_config_hash"]), str(row["fold"])): row
        for row in metric_rows
    }
    out: dict[str, float | None] = {}
    for fold in ("2014_2017", "2018_2021", "2022_2026"):
        lhs = by_key.get((config_hash, fold), {}).get(
            "candidate_quadrant_change_rate_calendar"
        )
        rhs = by_key.get((control_hash, fold), {}).get(
            "candidate_quadrant_change_rate_calendar"
        )
        out[fold] = None if lhs is None or rhs is None else float(lhs) - float(rhs)
    return out


def v03_stop_decision(summary_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not summary_rows:
        return {"decision": "v03_execution_failed", "reason": "no_results"}
    best_post = min(
        summary_rows,
        key=lambda row: none_last(row.get("post_initialization_candidate_revision_change_rate")),
    )
    best_full = min(
        summary_rows,
        key=lambda row: none_last(row.get("full_candidate_revision_change_rate")),
    )
    best_full_rate = float(best_full.get("full_candidate_revision_change_rate") or 1.0)
    if any(row.get("v03_gate_status") == "v03_intermediate_pass" for row in summary_rows):
        decision = "run_local_v03_limited"
    elif 0.15 <= best_full_rate < A31_V03_CONTROL_REVISION_RATE:
        decision = "open_new_family_qualification"
    else:
        decision = "prepare_request_freeze_gate_revision"
    return {
        "decision": decision,
        "best_config": best_full.get("a31_config_name"),
        "best_full_revision_rate": best_full.get("full_candidate_revision_change_rate"),
        "best_post_initialization_config": best_post.get("a31_config_name"),
        "best_post_initialization_revision_rate": best_post.get(
            "post_initialization_candidate_revision_change_rate"
        ),
        "a3_freeze_ready": False,
        "a4_status": A4_PROVISIONAL_STATUS,
        "a5_status": "blocked",
    }


def v03_summary_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        0 if row.get("v03_gate_status") == "v03_intermediate_pass" else 1,
        none_last(row.get("post_initialization_candidate_revision_change_rate")),
        none_last(row.get("growth_raw_sign_change_days")),
        none_last(row.get("inflation_raw_sign_change_days")),
        none_last(row.get("post_initialization_consumable_coverage")) * -1,
        str(row.get("a31_config_hash")),
    )


def render_a31_v03_report(
    manifest: dict[str, Any], summary_rows: list[dict[str, Any]]
) -> str:
    rows = [
        "# A31 v03 Revision Robust Screen",
        "",
        f"- worker_commit: `{manifest['worker_commit']}`",
        f"- git_dirty: `{manifest['git_dirty']}`",
        f"- A32 fixed: `{manifest['a32_config_name']}`",
        f"- stop_decision: `{manifest['stop_decision']['decision']}`",
        "- A4 remains smoke/viability only; A5 remains blocked.",
        "",
        "| config | gate | post-init revision | growth raw | inflation raw | post-init valid | post-init consumable | neutral | candidate defined |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_rows:
        rows.append(
            f"| `{row['a31_config_name']}` | {row['v03_gate_status']} | "
            f"{row['post_initialization_candidate_revision_change_rate']} | "
            f"{row['growth_raw_sign_change_days']} | "
            f"{row['inflation_raw_sign_change_days']} | "
            f"{row['post_initialization_valid_rate']} | "
            f"{row['post_initialization_consumable_coverage']} | "
            f"{row['post_initialization_neutral_rate']} | "
            f"{row['post_initialization_candidate_defined_rate']} |"
        )
    rows.append("")
    return "\n".join(rows)


def parse_a32_grid_args(argv: list[str]) -> A32GridConfig:
    ap = argparse.ArgumentParser(description="Run offline A3.2 grid over selected A31 configs")
    ap.add_argument("command", choices=["a32-grid"])
    ap.add_argument("--feature-manifest", required=True)
    ap.add_argument("--a31-catalog", required=True)
    ap.add_argument("--output-dir")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--worker-commit")
    args = ap.parse_args(argv)
    return A32GridConfig(
        feature_manifest=Path(args.feature_manifest),
        a31_catalog=Path(args.a31_catalog),
        output_dir=Path(args.output_dir) if args.output_dir else None,
        offline=args.offline,
        worker_commit=args.worker_commit,
    )


def run_a32_grid(config: A32GridConfig) -> dict[str, Any]:
    if not config.offline:
        raise SystemExit("a32-grid requires --offline to make the no-external-access contract explicit")
    started = dt.datetime.now(UTC)
    execution_id = str(uuid.uuid4())
    output_dir = config.output_dir or (config.feature_manifest.parent / "a32_grid")
    output_dir.mkdir(parents=True, exist_ok=True)

    feature_manifest, _, l2_hash, l2_records = load_l2_macro_from_feature_manifest(
        config.feature_manifest
    )
    catalog_payload = read_catalog_payload(config.a31_catalog)
    normalized_catalog, catalog_hash = normalize_a31_catalog(
        catalog_payload,
        l2_macro_logical_hash=l2_hash,
        source_path=config.a31_catalog,
    )
    a32_configs = a32_grid_configs()
    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    git_dirty = bool(run_text(
        ["git", "status", "--porcelain"], cwd=Path(__file__).resolve().parents[1]
    ))

    write_json(output_dir / "config_catalog.normalized.json", normalized_catalog)
    write_parquet(
        output_dir / "a32_configs.parquet",
        [
            {
                "a32_config_hash": a32_config_hash(a32),
                "a32_config_name": a32.name,
                "config_json": json.dumps(asdict(a32), sort_keys=True),
            }
            for a32 in a32_configs
        ],
    )

    summary_rows: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for item in normalized_catalog["configs"]:
        a31 = A31Config(**item["config"])
        try:
            l3_rows, _, l3_manifest = build_l3_score_panel(
                l2_records,
                a31,
                l2_macro_logical_hash=l2_hash,
                expected_l2_macro_logical_hash=l2_hash,
            )
            a31_hash = str(l3_manifest["a31_config_hash"])
            for a32 in a32_configs:
                a32_hash = a32_config_hash(a32)
                eval_hash = evaluation_hash(a31_hash, a32_hash)
                runtime, _ = run_l4_state_machine(
                    l3_rows, a32, selection_mode="latest"
                )
                counterfactual, _ = run_l4_state_machine(
                    l3_rows, a32, selection_mode="first_release"
                )
                metrics_full = build_macro_metrics(
                    runtime,
                    first_release_replay=counterfactual,
                )
                classification = classify_a32_grid_result(metrics_full)
                rows = evaluation_metric_rows(
                    runtime,
                    counterfactual,
                    a31,
                    a32,
                    a31_hash,
                    a32_hash,
                    eval_hash,
                    classification,
                )
                metric_rows.extend(rows)
                summary_rows.append(a32_grid_summary_row(
                    item,
                    a32,
                    a32_hash,
                    eval_hash,
                    rows,
                    classification,
                    l3_manifest,
                ))
        except Exception as exc:  # pragma: no cover - defensive per-A31 isolation
            failures.append(a31_failure_record({"a31_item": item}, exc))

    summary_rows.sort(
        key=lambda row: (
            str(row["a31_config_hash"]),
            none_last(row.get("min_confidence")),
            none_last(row.get("growth_enter")),
            none_last(row.get("inflation_enter")),
            none_last(row.get("axis_exit")),
        )
    )
    metric_rows.sort(key=lambda row: (
        str(row["a31_config_hash"]),
        str(row["a32_config_hash"]),
        str(row["fold"]),
    ))
    write_parquet(output_dir / "a32_grid_summary.parquet", summary_rows)
    write_parquet(output_dir / "a32_grid_metrics.parquet", metric_rows)
    write_json(output_dir / "failures.json", {"failures": failures})

    finished = dt.datetime.now(UTC)
    manifest = {
        "schema_version": A31_GRID_SCHEMA_VERSION,
        "status": "ok" if not failures else "partial_with_failures",
        "grid": "A3.2",
        "execution_id": execution_id,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "offline": True,
        "external_access": "disabled_by_a32_grid_path",
        "worker_commit": worker_commit,
        "git_dirty": git_dirty,
        "feature_manifest_path": str(config.feature_manifest),
        "a31_catalog_path": str(config.a31_catalog),
        "parent_hashes": {
            "l2_macro_logical_hash": l2_hash,
            "l2_schema_version": feature_manifest.get("schema_version"),
            "business_date_calendar_hash": feature_manifest.get("business_date_calendar_hash"),
            "series_family_mapping_hash": feature_manifest.get("series_family_mapping_hash"),
        },
        "config_catalog_hash": catalog_hash,
        "a31_config_count": len(normalized_catalog["configs"]),
        "a32_config_count": len(a32_configs),
        "evaluation_count": len(summary_rows),
        "failure_count": len(failures),
        "summary_logical_hash": logical_records_hash(summary_rows),
        "metrics_logical_hash": logical_records_hash(metric_rows),
        "artifact_hashes": hash_artifacts(
            output_dir,
            [
                "config_catalog.normalized.json",
                "a32_configs.parquet",
                "a32_grid_summary.parquet",
                "a32_grid_metrics.parquet",
                "failures.json",
            ],
        ),
        "selection_policy": "A3.2 limited grid over selected provisional A31 panels",
        "a4_status": A4_PROVISIONAL_STATUS,
        "a5_status": "blocked",
    }
    write_json(output_dir / "a32_grid_manifest.json", manifest)
    return {
        "status": manifest["status"],
        "output_dir": str(output_dir),
        "execution_id": execution_id,
        "evaluation_count": len(summary_rows),
        "failure_count": len(failures),
        "summary_logical_hash": manifest["summary_logical_hash"],
        "metrics_logical_hash": manifest["metrics_logical_hash"],
    }


def parse_a3_freeze_readiness_args(argv: list[str]) -> A3FreezeReadinessConfig:
    ap = argparse.ArgumentParser(description="Package A3 freeze-readiness evidence")
    ap.add_argument("command", choices=["a3-freeze-readiness"])
    ap.add_argument("--v02b-grid-dir", required=True)
    ap.add_argument("--g2-grid-dir", required=True)
    ap.add_argument("--a32-grid-dir", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--worker-commit")
    args = ap.parse_args(argv)
    return A3FreezeReadinessConfig(
        v02b_grid_dir=Path(args.v02b_grid_dir),
        g2_grid_dir=Path(args.g2_grid_dir),
        a32_grid_dir=Path(args.a32_grid_dir),
        output_dir=Path(args.output_dir),
        worker_commit=args.worker_commit,
    )


def run_a3_freeze_readiness_package(
    config: A3FreezeReadinessConfig,
) -> dict[str, Any]:
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    short_commit = worker_commit[:7]

    v02b_manifest = read_json_dict(config.v02b_grid_dir / "grid_manifest.json")
    g2_manifest = read_json_dict(config.g2_grid_dir / "grid_manifest.json")
    a32_manifest = read_json_dict(config.a32_grid_dir / "a32_grid_manifest.json")
    for label, manifest in {
        "v02b": v02b_manifest,
        "g2": g2_manifest,
        "a32": a32_manifest,
    }.items():
        validate_packaged_manifest(label, manifest, worker_commit)

    v02b_summary = read_parquet_records(config.v02b_grid_dir / "a31_grid_summary.parquet")
    v02b_metrics = read_parquet_records(config.v02b_grid_dir / "a31_grid_metrics.parquet")
    g2_summary = read_parquet_records(config.g2_grid_dir / "a31_grid_summary.parquet")
    g2_metrics = read_parquet_records(config.g2_grid_dir / "a31_grid_metrics.parquet")
    a32_summary = read_parquet_records(config.a32_grid_dir / "a32_grid_summary.parquet")
    a32_metrics = read_parquet_records(config.a32_grid_dir / "a32_grid_metrics.parquet")

    v02b_decision = read_json_dict(config.v02b_grid_dir / "a31_progression_decision.json")
    g2_decision = read_json_dict(config.g2_grid_dir / "a31_progression_decision.json")
    v02b_result = a31_result_payload(
        "V02B-G1-CREDIT-6040-15",
        v02b_summary,
        v02b_metrics,
        v02b_manifest,
        v02b_decision,
    )
    g2_result = a31_result_payload(
        "G2-CREDIT6040-15-SURVEY05",
        g2_summary,
        g2_metrics,
        g2_manifest,
        g2_decision,
    )
    a32_pareto = a32_freeze_readiness_pareto(a32_summary, a32_metrics)
    a32_pareto_by_fold = a32_freeze_readiness_pareto_by_fold(a32_pareto, a32_metrics)
    current_a32 = next(
        row for row in a32_pareto
        if row["pareto_role"] == "current_stability_preserving"
    )

    basis = {
        "worker_commit": worker_commit,
        "parent_l2_logical_hash": a32_manifest["parent_hashes"]["l2_macro_logical_hash"],
        "v02b_result_hash": logical_payload_hash(v02b_result),
        "g2_result_hash": logical_payload_hash(g2_result),
        "a32_pareto_hash": logical_records_hash(a32_pareto),
        "a32_pareto_by_fold_hash": logical_records_hash(a32_pareto_by_fold),
        "v02b_metrics_logical_hash": v02b_manifest.get("metrics_logical_hash"),
        "g2_metrics_logical_hash": g2_manifest.get("metrics_logical_hash"),
        "a32_metrics_logical_hash": a32_manifest.get("metrics_logical_hash"),
    }
    decision_basis_hash = logical_payload_hash(basis)

    v02b_result_path = output_dir / f"v02b_credit6040_15_result_{short_commit}.json"
    g2_result_path = output_dir / f"g2_credit6040_15_survey05_result_{short_commit}.json"
    a32_pareto_path = output_dir / f"a32_selected_pareto_{short_commit}.parquet"
    a32_pareto_by_fold_path = (
        output_dir / f"a32_selected_pareto_by_fold_{short_commit}.parquet"
    )
    report_path = output_dir / f"a3_freeze_readiness_{short_commit}.md"
    write_json(v02b_result_path, v02b_result)
    write_json(g2_result_path, g2_result)
    write_parquet(a32_pareto_path, a32_pareto)
    write_parquet(a32_pareto_by_fold_path, a32_pareto_by_fold)
    report_text = build_a3_freeze_readiness_report(
        worker_commit=worker_commit,
        v02b_result=v02b_result,
        g2_result=g2_result,
        current_a32=current_a32,
        a32_pareto=a32_pareto,
        a32_pareto_by_fold=a32_pareto_by_fold,
        decision_basis_hash=decision_basis_hash,
    )
    report_path.write_text(report_text, encoding="utf-8")

    progression_manifest = {
        "schema_version": 1,
        "artifact_type": "a3_freeze_readiness_progression_manifest",
        "worker_commit": worker_commit,
        "git_dirty": False,
        "parent_l2_logical_hash": a32_manifest["parent_hashes"]["l2_macro_logical_hash"],
        "a31_config_name": current_a32["a31_config_name"],
        "a31_config_hash": current_a32["a31_config_hash"],
        "a32_config_name": current_a32["a32_config_name"],
        "a32_config_hash": current_a32["a32_config_hash"],
        "progression_policy_version": A31_PROGRESSION_POLICY_VERSION,
        "previous_decision": v02b_decision.get("previous_decision"),
        "new_decision": v02b_decision.get("new_decision"),
        "supersession_reason": [
            "a3_progression_v2 allows G1 -> G2 limited without changing final freeze gates",
            "G2 limited evidence is packaged separately from older e9318e artifacts",
            "A3.2 limited grid is diagnostic and does not imply parameter freeze",
        ],
        "progression_gate_interpretation": {
            "g1_to_g2_limited": "permitted_by_a3_progression_v2",
            "g2_to_a3_2_limited": "permitted",
            "a3_parameter_freeze": "blocked_original_freeze_gates_still_apply",
            "retroactive_reclassification": False,
            "effective_scope": "v02b_and_later",
        },
        "decision_basis_hash": decision_basis_hash,
        "freeze_ready": False,
        "freeze_blockers": freeze_blockers(current_a32),
        "a4_status": A4_PROVISIONAL_STATUS,
        "a4_allowed_scope": [
            "replay_smoke",
            "book_compilation",
            "feasibility_tests",
            "metrics_generation",
            "identity_gate_validation",
            "lineage_tests",
        ],
        "a4_forbidden_scope": [
            "center_selection",
            "policy_selection_by_maxdd_cvar",
            "half_width_calibration",
            "gamma_or_beta_cap_calibration",
            "gate_calibration",
        ],
        "a5_status": "blocked",
        "market_implied_status": "not_operational_in_5ba5217_package",
        "artifact_hashes": hash_artifacts(
            output_dir,
            [
                v02b_result_path.name,
                g2_result_path.name,
                a32_pareto_path.name,
                a32_pareto_by_fold_path.name,
                report_path.name,
            ],
        ),
    }
    manifest_path = output_dir / f"a3_progression_v2_manifest_{short_commit}.json"
    write_json(manifest_path, progression_manifest)
    return {
        "status": "ok",
        "output_dir": str(output_dir),
        "manifest": str(manifest_path),
        "decision_basis_hash": decision_basis_hash,
        "freeze_ready": False,
        "a32_pareto_count": len(a32_pareto),
    }


def validate_packaged_manifest(
    label: str, manifest: dict[str, Any], worker_commit: str
) -> None:
    if manifest.get("worker_commit") != worker_commit:
        raise ValueError(
            f"{label} worker_commit mismatch: {manifest.get('worker_commit')} != {worker_commit}"
        )
    if manifest.get("git_dirty"):
        raise ValueError(f"{label} manifest is dirty and cannot be freeze-readiness evidence")
    if manifest.get("failure_count") not in {0, None}:
        raise ValueError(f"{label} manifest has grid failures: {manifest.get('failure_count')}")
    if manifest.get("status") != "ok":
        raise ValueError(f"{label} manifest status is not ok: {manifest.get('status')}")


def a31_result_payload(
    config_name: str,
    summary_rows: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
    manifest: dict[str, Any],
    decision: dict[str, Any],
) -> dict[str, Any]:
    row = row_by_name(summary_rows, config_name)
    folds = [
        row for row in metric_rows
        if row.get("a31_config_hash") == row_by_name(summary_rows, config_name)["a31_config_hash"]
    ]
    folds.sort(key=lambda item: str(item.get("fold")))
    return {
        "config_name": config_name,
        "summary": normalize_logical_value(row),
        "fold_metrics": normalize_logical_value(folds),
        "grid_manifest": {
            "worker_commit": manifest.get("worker_commit"),
            "git_dirty": manifest.get("git_dirty"),
            "parent_l2_logical_hash": (manifest.get("parent_hashes") or {}).get(
                "l2_macro_logical_hash"
            ),
            "summary_logical_hash": manifest.get("summary_logical_hash"),
            "metrics_logical_hash": manifest.get("metrics_logical_hash"),
            "a32_ref_hashes": manifest.get("a32_ref_hashes"),
        },
        "progression_decision": normalize_logical_value(decision),
    }


def row_by_name(rows: list[dict[str, Any]], config_name: str) -> dict[str, Any]:
    matches = [row for row in rows if row.get("a31_config_name") == config_name]
    if not matches:
        raise ValueError(f"missing result row for {config_name}")
    if len(matches) > 1:
        raise ValueError(f"multiple result rows for {config_name}")
    return dict(matches[0])


def a32_freeze_readiness_pareto(
    summary_rows: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    specs = [
        ("current_stability_preserving", 0.35, 0.35, 0.10, 0.60),
        ("neighbor_confidence_0_65", 0.35, 0.35, 0.10, 0.65),
        ("neighbor_inflation_enter_0_40", 0.35, 0.40, 0.10, 0.60),
        ("neighbor_exit_0_15", 0.35, 0.35, 0.15, 0.60),
        ("neighbor_growth_enter_0_30_high_coverage", 0.30, 0.35, 0.10, 0.60),
    ]
    out: list[dict[str, Any]] = []
    for rank, (role, growth_enter, inflation_enter, axis_exit, confidence) in enumerate(
        specs,
        start=1,
    ):
        summary = find_a32_summary_row(
            summary_rows,
            a31_config_name="G2-CREDIT6040-15-SURVEY05",
            growth_enter=growth_enter,
            inflation_enter=inflation_enter,
            axis_exit=axis_exit,
            min_confidence=confidence,
        )
        full = find_a32_metric_row(
            metric_rows,
            str(summary["a31_config_hash"]),
            str(summary["a32_config_hash"]),
            "full",
        )
        transition_dist = parse_json_metric(full.get("transition_timing_displacement"))
        candidate_duration = parse_json_metric(full.get("candidate_duration_distribution"))
        published_duration = parse_json_metric(full.get("published_duration_distribution"))
        state_age = parse_json_metric(full.get("days_since_last_valid_distribution"))
        consumed_age = parse_json_metric(full.get("consumed_state_age_distribution"))
        enriched = {
            "pareto_rank": rank,
            "pareto_role": role,
            "a31_config_name": summary["a31_config_name"],
            "a31_config_hash": summary["a31_config_hash"],
            "a32_config_name": summary["a32_config_name"],
            "a32_config_hash": summary["a32_config_hash"],
            "evaluation_hash": summary["evaluation_hash"],
            "growth_enter": summary["growth_enter"],
            "inflation_enter": summary["inflation_enter"],
            "axis_exit": summary["axis_exit"],
            "min_confidence": summary["min_confidence"],
            "u_floor": summary["u_floor"],
            "score_scale": summary["growth_score_scale"],
            "dispersion_limit": summary["dispersion_limit"],
            "candidate_revision_change_rate": full["candidate_revision_change_rate"],
            "raw_growth_sign_revision_changes": full.get("growth_raw_sign_change_days"),
            "raw_inflation_sign_revision_changes": full.get(
                "inflation_raw_sign_change_days"
            ),
            "axis_effective_sign_revision_changes_growth": full.get(
                "growth_sign_revision_change_days"
            ),
            "axis_effective_sign_revision_changes_inflation": full.get(
                "inflation_sign_revision_change_days"
            ),
            "axis_state_label_revision_changes_growth": full.get(
                "growth_axis_state_change_days"
            ),
            "axis_state_label_revision_changes_inflation": full.get(
                "inflation_axis_state_change_days"
            ),
            "candidate_quadrant_revision_changes": full.get(
                "candidate_quadrant_change_days"
            ),
            "status_revision_changes": full.get("status_revision_change_days"),
            "status_revision_change_rate": full.get("status_revision_change_rate"),
            "published_quadrant_revision_changes": full.get(
                "published_revision_change_days"
            ),
            "published_quadrant_revision_change_rate": full.get(
                "published_revision_change_rate"
            ),
            "latched_quadrant_revision_changes": full.get("latched_revision_change_days"),
            "latched_quadrant_revision_change_rate": full.get(
                "latched_revision_change_rate"
            ),
            "transition_displacement_median": transition_dist.get("median"),
            "transition_displacement_p90": transition_dist.get("p90"),
            "candidate_flips_per_year": full["candidate_flips_per_year"],
            "published_flips_per_year": full["published_flips_per_year"],
            "candidate_duration_median": candidate_duration.get("median"),
            "candidate_duration_p10": candidate_duration.get("p10"),
            "published_duration_median": published_duration.get("median"),
            "published_duration_p10": published_duration.get("p10"),
            "valid_rate": full["valid_rate"],
            "abstain_rate": full["abstain_rate"],
            "consumable_state_coverage": full["consumable_state_coverage"],
            "stale_days_over_5bd": full["stale_days_over_5bd"],
            "longest_stale_run": full["longest_stale_run"],
            "state_age_since_last_valid_p50": state_age.get("median"),
            "state_age_since_last_valid_p90": state_age.get("p90"),
            "state_age_since_last_valid_max": state_age.get("max"),
            "consumed_state_age_p50": consumed_age.get("median"),
            "consumed_state_age_p90": consumed_age.get("p90"),
            "consumed_state_age_max": consumed_age.get("max"),
            "first_input_ready_date": full.get("first_input_ready_date"),
            "first_latched_date": full.get("first_latched_date"),
            "first_operational_date": full.get("first_operational_date"),
            "post_initialization_start_date": full.get("post_initialization_start_date"),
            "quadrant_occupancy": full["quadrant_occupancy"],
            "abstention_reasons": full["reason_counts"],
            "freeze_ready": False,
            "production_candidate": False,
            "activation_ready": False,
        }
        out.append(enriched)
    return out


def a32_freeze_readiness_pareto_by_fold(
    a32_pareto: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in a32_pareto:
        rows = [
            row for row in metric_rows
            if str(row.get("a31_config_hash")) == str(item["a31_config_hash"])
            and str(row.get("a32_config_hash")) == str(item["a32_config_hash"])
        ]
        rows.sort(key=lambda row: str(row.get("fold")))
        for row in rows:
            transition_dist = parse_json_metric(row.get("transition_timing_displacement"))
            consumed_age = parse_json_metric(row.get("consumed_state_age_distribution"))
            out.append({
                "pareto_rank": item["pareto_rank"],
                "pareto_role": item["pareto_role"],
                "fold": row.get("fold"),
                "history_scope": row.get("history_scope"),
                "a31_config_name": row.get("a31_config_name"),
                "a31_config_hash": row.get("a31_config_hash"),
                "a32_config_name": row.get("a32_config_name"),
                "a32_config_hash": row.get("a32_config_hash"),
                "candidate_revision_change_rate": row.get(
                    "candidate_revision_change_rate"
                ),
                "raw_growth_sign_revision_changes": row.get(
                    "growth_raw_sign_change_days"
                ),
                "raw_inflation_sign_revision_changes": row.get(
                    "inflation_raw_sign_change_days"
                ),
                "axis_effective_sign_revision_changes_growth": row.get(
                    "growth_sign_revision_change_days"
                ),
                "axis_effective_sign_revision_changes_inflation": row.get(
                    "inflation_sign_revision_change_days"
                ),
                "axis_state_label_revision_changes_growth": row.get(
                    "growth_axis_state_change_days"
                ),
                "axis_state_label_revision_changes_inflation": row.get(
                    "inflation_axis_state_change_days"
                ),
                "candidate_quadrant_revision_changes": row.get(
                    "candidate_quadrant_change_days"
                ),
                "status_revision_change_days": row.get("status_revision_change_days"),
                "status_revision_change_rate": row.get("status_revision_change_rate"),
                "published_revision_change_days": row.get(
                    "published_revision_change_days"
                ),
                "published_revision_change_rate": row.get(
                    "published_revision_change_rate"
                ),
                "latched_revision_change_days": row.get("latched_revision_change_days"),
                "latched_revision_change_rate": row.get(
                    "latched_revision_change_rate"
                ),
                "transition_displacement_median": transition_dist.get("median"),
                "transition_displacement_p90": transition_dist.get("p90"),
                "revision_episode_count": row.get("revision_episode_count"),
                "revision_episode_duration_p50": row.get(
                    "revision_episode_duration_p50"
                ),
                "revision_episode_duration_p90": row.get(
                    "revision_episode_duration_p90"
                ),
                "valid_rate": row.get("valid_rate"),
                "abstain_rate": row.get("abstain_rate"),
                "consumable_state_coverage": row.get("consumable_state_coverage"),
                "consumed_state_age_p50": consumed_age.get("median"),
                "consumed_state_age_p90": consumed_age.get("p90"),
                "consumed_state_age_max": consumed_age.get("max"),
                "stale_days_over_5bd": row.get("stale_days_over_5bd"),
                "longest_stale_run": row.get("longest_stale_run"),
                "days_without_latched_state": row.get("days_without_latched_state"),
                "first_operational_date": row.get("first_operational_date"),
            })
    return out


def find_a32_summary_row(
    rows: list[dict[str, Any]],
    *,
    a31_config_name: str,
    growth_enter: float,
    inflation_enter: float,
    axis_exit: float,
    min_confidence: float,
) -> dict[str, Any]:
    matches = [
        row for row in rows
        if row.get("a31_config_name") == a31_config_name
        and float_close(row.get("growth_enter"), growth_enter)
        and float_close(row.get("inflation_enter"), inflation_enter)
        and float_close(row.get("axis_exit"), axis_exit)
        and float_close(row.get("min_confidence"), min_confidence)
    ]
    if not matches:
        raise ValueError(f"missing A3.2 row for {a31_config_name} {growth_enter}/{inflation_enter}/{axis_exit}/{min_confidence}")
    if len(matches) > 1:
        raise ValueError("A3.2 summary row is not unique")
    return dict(matches[0])


def find_a32_metric_row(
    rows: list[dict[str, Any]],
    a31_hash: str,
    a32_hash: str,
    fold: str,
) -> dict[str, Any]:
    matches = [
        row for row in rows
        if str(row.get("a31_config_hash")) == a31_hash
        and str(row.get("a32_config_hash")) == a32_hash
        and row.get("fold") == fold
    ]
    if not matches:
        raise ValueError(f"missing A3.2 metric row for {a31_hash}/{a32_hash}/{fold}")
    if len(matches) > 1:
        raise ValueError("A3.2 metric row is not unique")
    return dict(matches[0])


def float_close(value: Any, expected: float) -> bool:
    return value is not None and abs(float(value) - expected) < 1e-12


def parse_json_metric(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in {None, ""}:
        return {}
    payload = json.loads(str(value))
    return payload if isinstance(payload, dict) else {}


def freeze_blockers(current_a32: dict[str, Any]) -> list[str]:
    blockers = []
    if float(current_a32["candidate_revision_change_rate"]) > MAX_REVISION_CHANGE_RATE_FREEZE:
        blockers.append("candidate_revision_change_rate_above_10pct_freeze_gate")
    if float(current_a32["valid_rate"]) < MIN_VALID_RATE_FREEZE:
        blockers.append("valid_rate_below_original_freeze_band")
    if float(current_a32["consumable_state_coverage"]) < MIN_VALID_RATE_FREEZE:
        blockers.append("consumable_state_coverage_below_operational_target")
    blockers.append("market_implied_valid_vs_valid_comparison_not_operational")
    if current_a32.get("raw_inflation_sign_revision_changes") is None:
        blockers.append("raw_inflation_revision_metric_not_available")
    if current_a32.get("consumed_state_age_p50") is None:
        blockers.append("consumed_state_age_distribution_not_available")
    return blockers


def build_a3_freeze_readiness_report(
    *,
    worker_commit: str,
    v02b_result: dict[str, Any],
    g2_result: dict[str, Any],
    current_a32: dict[str, Any],
    a32_pareto: list[dict[str, Any]],
    a32_pareto_by_fold: list[dict[str, Any]],
    decision_basis_hash: str,
) -> str:
    v02b = v02b_result["summary"]
    g2 = g2_result["summary"]
    blockers = freeze_blockers(current_a32)
    rows = [
        "# A3 Freeze Readiness",
        "",
        f"- worker_commit: `{worker_commit}`",
        "- git_dirty: `false`",
        f"- decision_basis_hash: `{decision_basis_hash}`",
        "- freeze_ready: `false`",
        f"- A4 status: `{A4_PROVISIONAL_STATUS}`",
        "- A5 status: `blocked`",
        "",
        "## Progression vs Freeze",
        "",
        "- G1 -> G2 limited: permitted by `a3_progression_v2`.",
        "- G2 -> A3.2 limited: permitted for diagnostic threshold calibration.",
        "- A3 parameter freeze: blocked; the original freeze gates still apply.",
        "- retroactive_reclassification: `false`.",
        "- effective_scope: `v02b_and_later`.",
        "",
        "## Headline Results",
        "",
        f"- SLOOS 60/40 15pct revision rate: `{v02b['candidate_revision_change_rate']}`; growth axis effective-sign changes: `{v02b['growth_sign_revision_change_days']}`; valid rate: `{v02b['valid_rate']}`.",
        f"- G2 SLOOS+survey revision rate: `{g2['candidate_revision_change_rate']}`; growth axis effective-sign changes: `{g2['growth_sign_revision_change_days']}`; valid rate: `{g2['valid_rate']}`.",
        f"- Current A3.2 candidate: `{current_a32['a32_config_name']}`.",
        f"- Current A3.2 revision rate: `{current_a32['candidate_revision_change_rate']}`; raw growth score-sign changes: `{current_a32['raw_growth_sign_revision_changes']}`; raw inflation score-sign changes: `{current_a32['raw_inflation_sign_revision_changes']}`; axis effective-sign growth changes: `{current_a32['axis_effective_sign_revision_changes_growth']}`; axis state-label growth changes: `{current_a32['axis_state_label_revision_changes_growth']}`; valid rate: `{current_a32['valid_rate']}`; consumable coverage: `{current_a32['consumable_state_coverage']}`.",
        f"- First operational date: `{current_a32['first_operational_date']}`.",
        "",
        "## Consumable Coverage Contract",
        "",
        f"- `GATE_MAX_LAG_BUSINESS_DAYS={GATE_MAX_LAG_BUSINESS_DAYS}`.",
        "- A day is consumable when a latched/published quadrant exists and the last `valid` publication is no more than 5 business-day rows old.",
        "- The metric includes latched state carried during abstention only inside that lag window.",
        "- It does not require the current row itself to be `valid`.",
        "- `consumed_state_age_*` is computed only on consumable days; `state_age_since_last_valid_*` is computed on all days after the first valid publication.",
        "",
        "## Metric Taxonomy",
        "",
        "- `raw_growth_sign_revision_changes`: raw score sign disagreement from revision diagnostics.",
        "- `raw_inflation_sign_revision_changes`: raw inflation score sign disagreement from revision diagnostics.",
        "- `axis_effective_sign_revision_changes_*`: threshold/hysteresis-dependent effective axis-sign disagreement.",
        "- `axis_state_label_revision_changes_*`: more granular axis label disagreement, including neutral-state reason changes when available.",
        "- `candidate_quadrant_revision_changes`: candidate quadrant disagreement.",
        "- `status_revision_changes`: valid/abstain status disagreement.",
        "- `published_quadrant_revision_changes`: published quadrant disagreement.",
        "- `latched_quadrant_revision_changes`: latched published-state disagreement including abstain carry.",
        "",
        "## A3.2 Pareto Shortlist",
        "",
        "| rank | role | A32 | revision | raw growth | raw inflation | axis growth | valid | consumable | consumed age P90 | stale >5bd |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in a32_pareto:
        rows.append(
            f"| {row['pareto_rank']} | {row['pareto_role']} | `{row['a32_config_name']}` | "
            f"{row['candidate_revision_change_rate']:.6f} | {row['raw_growth_sign_revision_changes']} | "
            f"{row['raw_inflation_sign_revision_changes']} | "
            f"{row['axis_effective_sign_revision_changes_growth']} | {row['valid_rate']:.6f} | "
            f"{row['consumable_state_coverage']:.6f} | {row['consumed_state_age_p90']} | "
            f"{row['stale_days_over_5bd']} |"
        )
    rows.extend([
        "",
        "## A3.2 Folds",
        "",
        "| rank | fold | revision | status rev rate | latched rev rate | valid | consumable | transition P90 | episode P90 |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for row in a32_pareto_by_fold:
        rows.append(
            f"| {row['pareto_rank']} | {row['fold']} | "
            f"{row['candidate_revision_change_rate']} | "
            f"{row['status_revision_change_rate']} | "
            f"{row['latched_revision_change_rate']} | "
            f"{row['valid_rate']} | {row['consumable_state_coverage']} | "
            f"{row['transition_displacement_p90']} | "
            f"{row['revision_episode_duration_p90']} |"
        )
    rows.extend([
        "",
        "## Freeze Blockers",
        "",
    ])
    rows.extend(f"- {blocker}" for blocker in blockers)
    rows.extend([
        "",
        "## Market-Implied",
        "",
        "The 5ba5217 package does not contain an operational valid-vs-valid market-implied comparison. This remains a freeze blocker and must be calibrated separately.",
        "",
        "## A4 Guard",
        "",
        "A4 is limited to smoke, book compilation, feasibility, identity-gate validation, metrics generation, and lineage checks. It must not select centers, half-widths, gamma, beta caps, policy winners, or gate parameters before A3 freeze.",
        "",
    ])
    return "\n".join(rows)


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
    l2_file_name = str(macro_meta.get("file_name") or "macro_feature_primitives.parquet")
    l2_path = feature_manifest_path.parent / l2_file_name
    rows = read_parquet_records(l2_path)
    actual_hash = logical_records_hash(rows)
    validate_parent_hash("a31-grid L2 macro_feature_primitives", actual_hash, expected_hash)
    if macro_meta.get("row_count") is not None and int(macro_meta["row_count"]) != len(rows):
        raise ValueError("macro_feature_primitives row_count mismatch")
    return manifest, l2_path, actual_hash, rows


def load_l2_market_from_feature_manifest(
    feature_manifest_path: Path,
) -> tuple[dict[str, Any], Path, str, list[dict[str, Any]]]:
    manifest = read_json_dict(feature_manifest_path)
    if not manifest.get("parameter_independent"):
        raise ValueError("feature_manifest must be parameter_independent=true for market-grid")
    market_meta = manifest.get("market_feature_primitives") or {}
    expected_hash = str(market_meta.get("logical_hash") or "")
    if not expected_hash:
        raise ValueError("feature_manifest is missing market_feature_primitives.logical_hash")
    l2_file_name = str(market_meta.get("file_name") or "market_feature_primitives.parquet")
    l2_path = feature_manifest_path.parent / l2_file_name
    rows = read_parquet_records(l2_path)
    actual_hash = logical_records_hash(rows)
    validate_parent_hash("market-grid L2 market_feature_primitives", actual_hash, expected_hash)
    if market_meta.get("row_count") is not None and int(market_meta["row_count"]) != len(rows):
        raise ValueError("market_feature_primitives row_count mismatch")
    return manifest, l2_path, actual_hash, rows


def parse_revision_uncertainty_args(argv: list[str]) -> RevisionUncertaintyConfig:
    ap = argparse.ArgumentParser(description="Build PIT revision-uncertainty primitives")
    ap.add_argument("command", choices=["revision-uncertainty"])
    ap.add_argument("--feature-manifest", required=True)
    ap.add_argument("--output-dir")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--worker-commit")
    args = ap.parse_args(argv)
    return RevisionUncertaintyConfig(
        feature_manifest=Path(args.feature_manifest),
        output_dir=Path(args.output_dir) if args.output_dir else None,
        offline=args.offline,
        worker_commit=args.worker_commit,
    )


def run_revision_uncertainty(config: RevisionUncertaintyConfig) -> dict[str, Any]:
    if not config.offline:
        raise SystemExit("revision-uncertainty requires --offline")
    started = dt.datetime.now(UTC)
    execution_id = str(uuid.uuid4())
    output_dir = config.output_dir or (config.feature_manifest.parent / "revision_uncertainty")
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_manifest, _, l2_hash, l2_records = load_l2_macro_from_feature_manifest(
        config.feature_manifest
    )
    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    git_dirty = bool(run_text(
        ["git", "status", "--porcelain"], cwd=Path(__file__).resolve().parents[1]
    ))
    rows = build_revision_uncertainty_primitives(l2_records)
    write_parquet(output_dir / "revision_uncertainty_primitives.parquet", rows)
    finished = dt.datetime.now(UTC)
    manifest = {
        "schema_version": REVISION_UNCERTAINTY_SCHEMA_VERSION,
        "artifact_type": "revision_uncertainty_primitives",
        "execution_id": execution_id,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "offline": True,
        "external_access": "disabled_by_revision_uncertainty_path",
        "worker_commit": worker_commit,
        "git_dirty": git_dirty,
        "feature_manifest_path": str(config.feature_manifest),
        "parent_hashes": {
            "l2_macro_logical_hash": l2_hash,
            "l2_schema_version": feature_manifest.get("schema_version"),
            "business_date_calendar_hash": feature_manifest.get(
                "business_date_calendar_hash"
            ),
            "series_family_mapping_hash": feature_manifest.get(
                "series_family_mapping_hash"
            ),
        },
        "grain": "business_date x selection_mode x entity_level x entity_id",
        "strictly_pit": True,
        "history_update_policy": "expanding history before current business_date is used for current estimates",
        "mature_minimums": {
            "weekly": 52,
            "monthly": 36,
            "quarterly": 12,
        },
        "fallback_policy": "insufficient history leaves q_revision null and L3 uses REF behavior",
        "row_count": len(rows),
        "logical_hash": logical_records_hash(rows),
        "artifact_hashes": hash_artifacts(
            output_dir,
            ["revision_uncertainty_primitives.parquet"],
        ),
    }
    write_json(output_dir / "revision_uncertainty_manifest.json", manifest)
    return {
        "status": "ok",
        "output_dir": str(output_dir),
        "row_count": len(rows),
        "logical_hash": manifest["logical_hash"],
    }


def build_revision_uncertainty_primitives(
    l2_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_series_date: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    series_meta: dict[str, dict[str, Any]] = {}
    for row in l2_records:
        series_id = str(row.get("series_id"))
        business_date = str(row.get("business_date"))
        selection_mode = str(row.get("selection_mode"))
        by_series_date.setdefault((business_date, series_id), {})[selection_mode] = row
        series_meta.setdefault(series_id, row)
    history: dict[str, list[dict[str, Any]]] = {series_id: [] for series_id in series_meta}
    seen_revision_events: dict[str, set[tuple[str, str | None, str | None]]] = {
        series_id: set() for series_id in series_meta
    }
    rows: list[dict[str, Any]] = []
    dates = sorted({date for date, _ in by_series_date})
    for business_date in dates:
        series_rows_for_aggregate: list[dict[str, Any]] = []
        for series_id in sorted(series_meta):
            modes = by_series_date.get((business_date, series_id), {})
            latest = modes.get("latest")
            first = modes.get("first_release")
            if latest is None:
                continue
            stats = revision_uncertainty_stats(
                history[series_id],
                transform_frequency(str(latest.get("transform_class"))),
            )
            for selection_mode in ("latest", "first_release"):
                source_row = modes.get(selection_mode)
                if source_row is None:
                    continue
                out = revision_uncertainty_row(
                    business_date,
                    selection_mode,
                    "series",
                    series_id,
                    source_row,
                    stats,
                )
                rows.append(out)
                if selection_mode == "latest":
                    series_rows_for_aggregate.append(out)
            if first is not None:
                event = revision_event_from_pair(latest, first)
                if event is not None:
                    key = (
                        str(latest.get("observation_period")),
                        str(latest.get("vintage_date")),
                        str(first.get("vintage_date")),
                    )
                    if key not in seen_revision_events[series_id]:
                        history[series_id].append(event)
                        seen_revision_events[series_id].add(key)
        rows.extend(aggregate_revision_uncertainty_rows(business_date, series_rows_for_aggregate))
    rows.sort(key=lambda row: (
        str(row["business_date"]),
        str(row["selection_mode"]),
        str(row["entity_level"]),
        str(row["entity_id"]),
    ))
    return rows


def revision_event_from_pair(
    latest: dict[str, Any], first: dict[str, Any]
) -> dict[str, Any] | None:
    latest_score = finite_or_none(latest.get("reference_series_score"))
    first_score = finite_or_none(first.get("reference_series_score"))
    if latest_score is None or first_score is None:
        return None
    revision_delta = latest_score - first_score
    return {
        "revision_delta": revision_delta,
        "abs_revision_delta": abs(revision_delta),
        "sign_flipped": (
            sign_of(latest_score) is not None
            and sign_of(first_score) is not None
            and sign_of(latest_score) != sign_of(first_score)
        ),
    }


def transform_frequency(transform_class: str) -> str:
    if transform_class.startswith("claims_"):
        return "weekly"
    if transform_class.startswith("quarterly_"):
        return "quarterly"
    return "monthly"


def mature_minimum_for_frequency(frequency: str) -> int:
    return {"weekly": 52, "quarterly": 12}.get(frequency, 36)


def revision_uncertainty_stats(
    history: list[dict[str, Any]], frequency: str
) -> dict[str, Any]:
    mature_count = len(history)
    minimum = mature_minimum_for_frequency(frequency)
    if mature_count < minimum:
        return {
            "frequency": frequency,
            "mature_observation_count": mature_count,
            "mature_minimum": minimum,
            "sufficient_history": False,
            "median_revision_bias": None,
            "median_absolute_revision": None,
            "p75_absolute_revision": None,
            "p90_absolute_revision": None,
            "revision_sign_flip_rate": None,
        }
    biases = sorted(float(item["revision_delta"]) for item in history)
    absolutes = sorted(float(item["abs_revision_delta"]) for item in history)
    flips = [bool(item["sign_flipped"]) for item in history]
    return {
        "frequency": frequency,
        "mature_observation_count": mature_count,
        "mature_minimum": minimum,
        "sufficient_history": True,
        "median_revision_bias": statistics.median(biases),
        "median_absolute_revision": percentile(absolutes, 0.50),
        "p75_absolute_revision": percentile(absolutes, 0.75),
        "p90_absolute_revision": percentile(absolutes, 0.90),
        "revision_sign_flip_rate": sum(1 for value in flips if value) / len(flips),
    }


def revision_uncertainty_row(
    business_date: str,
    selection_mode: str,
    entity_level: str,
    entity_id: str,
    source_row: dict[str, Any],
    stats: dict[str, Any],
) -> dict[str, Any]:
    return {
        "business_date": business_date,
        "selection_mode": selection_mode,
        "selection_role": selection_role_for_mode(selection_mode),
        "counterfactual_only": selection_role_for_mode(selection_mode)
        != "pit_runtime_candidate",
        "entity_level": entity_level,
        "entity_id": entity_id,
        "series_id": source_row.get("series_id") if entity_level == "series" else None,
        "family_id": source_row.get("family_id"),
        "axis_id": source_row.get("axis_id"),
        **stats,
    }


def aggregate_revision_uncertainty_rows(
    business_date: str,
    series_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for selection_mode in ("latest", "first_release"):
        mode_rows = [
            dict(row, selection_mode=selection_mode)
            for row in series_rows
        ]
        for level, key_name in (("family", "family_id"), ("axis", "axis_id")):
            groups: dict[str, list[dict[str, Any]]] = {}
            for row in mode_rows:
                groups.setdefault(str(row.get(key_name)), []).append(row)
            for entity_id, items in sorted(groups.items()):
                stats = aggregate_uncertainty_stats(items)
                out.append({
                    "business_date": business_date,
                    "selection_mode": selection_mode,
                    "selection_role": selection_role_for_mode(selection_mode),
                    "counterfactual_only": selection_role_for_mode(selection_mode)
                    != "pit_runtime_candidate",
                    "entity_level": level,
                    "entity_id": entity_id,
                    "series_id": None,
                    "family_id": entity_id if level == "family" else None,
                    "axis_id": entity_id if level == "axis" else items[0].get("axis_id"),
                    **stats,
                })
    return out


def aggregate_uncertainty_stats(items: list[dict[str, Any]]) -> dict[str, Any]:
    sufficient = [row for row in items if row.get("sufficient_history")]
    mature_count = sum(int(row.get("mature_observation_count") or 0) for row in items)
    minimum = sum(int(row.get("mature_minimum") or 0) for row in items)
    def metric_values(name: str) -> list[float]:
        return [
            float(row[name]) for row in sufficient
            if row.get(name) is not None
        ]
    abs_p50 = metric_values("median_absolute_revision")
    abs_p75 = metric_values("p75_absolute_revision")
    abs_p90 = metric_values("p90_absolute_revision")
    biases = metric_values("median_revision_bias")
    flips = metric_values("revision_sign_flip_rate")
    return {
        "frequency": "mixed",
        "mature_observation_count": mature_count,
        "mature_minimum": minimum,
        "sufficient_history": bool(sufficient),
        "median_revision_bias": statistics.median(biases) if biases else None,
        "median_absolute_revision": statistics.median(abs_p50) if abs_p50 else None,
        "p75_absolute_revision": statistics.median(abs_p75) if abs_p75 else None,
        "p90_absolute_revision": statistics.median(abs_p90) if abs_p90 else None,
        "revision_sign_flip_rate": statistics.mean(flips) if flips else None,
    }


def load_revision_uncertainty_from_manifest(
    manifest_path: Path,
) -> tuple[dict[str, Any], str, list[dict[str, Any]]]:
    manifest = read_json_dict(manifest_path)
    expected_hash = str(manifest.get("logical_hash") or "")
    if not expected_hash:
        raise ValueError("revision uncertainty manifest missing logical_hash")
    path = manifest_path.parent / "revision_uncertainty_primitives.parquet"
    rows = read_parquet_records(path)
    actual_hash = logical_records_hash(rows)
    validate_parent_hash("revision_uncertainty_primitives", actual_hash, expected_hash)
    return manifest, actual_hash, rows


def revision_uncertainty_keyed(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    return {
        (
            str(row["business_date"]),
            str(row["selection_mode"]),
            str(row["entity_level"]),
            str(row["entity_id"]),
        ): row
        for row in rows
    }


def read_parquet_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a project dependency
        raise RuntimeError("pandas is required to read calibration parquet artifacts") from exc
    frame = pd.read_parquet(path)
    return [
        {str(key): parquet_cell(value, key=str(key)) for key, value in record.items()}
        for record in frame.to_dict("records")
    ]


def parquet_cell(value: Any, *, key: str | None = None) -> Any:
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except (AttributeError, ValueError, TypeError):
            pass
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, dt.datetime):
        if value.time() == dt.time(0, 0) and not (key and "available_at" in key):
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
        if isinstance(raw.get("config"), dict):
            cfg = A31Config(**raw["config"])
            metadata = dict(raw.get("metadata") or {})
        else:
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
    # Finding F1: the catalog hash must be reproducible regardless of where the
    # catalog file is mounted (host vs container differ only by absolute path).
    # source_path is kept as out-of-band diagnostic metadata but excluded from the
    # hashed payload so config_catalog_hash (and the transitive evaluation_hash /
    # object-store prefix) are path-independent.
    hashable = {key: value for key, value in normalized.items() if key != "source_path"}
    return normalized, logical_payload_hash(hashable)


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
        "axis_aggregation_method",
        "robust_clip",
        "reliability_weighting",
        "release_smoothing",
        "family_weights",
        "series_weights",
        "series_transform_overrides",
        "revision_soft_threshold_quantile",
        "family_consensus_min",
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
        axis_aggregation_method=str(data["axis_aggregation_method"]),
        robust_clip=float(data["robust_clip"]),
        reliability_weighting=str(data["reliability_weighting"]),
        score_clip={str(k): float(v) for k, v in dict(data["score_clip"]).items()},
        release_smoothing=str(data["release_smoothing"]),
        series_transform_overrides={
            str(k): str(v)
            for k, v in dict(data.get("series_transform_overrides", {})).items()
        },
        revision_soft_threshold_quantile=(
            None if data.get("revision_soft_threshold_quantile") in {None, "null", ""}
            else str(data.get("revision_soft_threshold_quantile"))
        ),
        family_consensus_min=(
            None if data.get("family_consensus_min") is None
            else float(data.get("family_consensus_min"))
        ),
    )
    metadata = {
        key: normalize_logical_value(value)
        for key, value in entry.items()
        if key not in {
            "name",
            "extends",
            "aggregation_method",
            "axis_aggregation_method",
            "robust_clip",
            "reliability_weighting",
            "release_smoothing",
            "transformation_weights",
            "family_weights",
            "series_weights",
            "series_transform_overrides",
            "revision_soft_threshold_quantile",
            "family_consensus_min",
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
    metadata["resolved_series_transform_overrides"] = normalize_logical_value(
        cfg.series_transform_overrides
    )
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
    distance += (
        0.01 if config.axis_aggregation_method != ref.axis_aggregation_method else 0.0
    )
    distance += abs(config.robust_clip - ref.robust_clip) / 10.0
    distance += 0.01 if config.reliability_weighting != ref.reliability_weighting else 0.0
    distance += 0.01 if config.release_smoothing != ref.release_smoothing else 0.0
    distance += 0.01 * len(config.series_transform_overrides)
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
    a32_hash = a32_config_hash(a32)
    eval_hash = evaluation_hash(a31_hash, a32_hash)
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
            eval_hash,
            classification,
        )
        summary = a31_grid_summary_row(
            item,
            a32_hash,
            eval_hash,
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
            "evaluation_hash": eval_hash,
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


def a31_progression_payload(full_metric_row: dict[str, Any]) -> dict[str, Any]:
    revision_rate = full_metric_row.get("candidate_revision_change_rate")
    valid_rate = full_metric_row.get("valid_rate")
    growth_changes = full_metric_row.get("growth_sign_revision_change_days")
    if revision_rate is None:
        return {
            "decision_policy_version": A31_PROGRESSION_POLICY_VERSION,
            "progression_level": "fail",
            "a31_provisional_status": "a31_screened_out",
            "risk_flag": None,
            "relative_revision_improvement_vs_v01": None,
            "absolute_target_20pct_met": False,
            "growth_sign_changes_below_benchmark": False,
            "valid_rate_not_degraded": False,
        }
    revision = float(revision_rate)
    relative_improvement = (
        (A31_V01_BENCHMARK_REVISION_RATE - revision)
        / A31_V01_BENCHMARK_REVISION_RATE
    )
    growth_ok = (
        growth_changes is not None
        and int(growth_changes) <= A31_V01_GROWTH_SIGN_BENCHMARK
    )
    valid_ok = valid_rate is not None and float(valid_rate) >= A31_V01_VALID_RATE
    absolute_target_met = revision <= A31_REVISION_PASS_RATE
    conditional = (
        revision <= A31_REVISION_CONDITIONAL_RATE
        and relative_improvement >= A31_RELATIVE_IMPROVEMENT_CONDITIONAL
        and growth_ok
        and valid_ok
    )
    if absolute_target_met:
        level = "pass"
        status = "a31_candidate"
        risk_flag = None
    elif conditional:
        level = "conditional_pass"
        status = "a31_provisional_candidate"
        risk_flag = "elevated_vintage_instability"
    else:
        level = "fail"
        status = "a31_screened_out"
        risk_flag = None
    return {
        "decision_policy_version": A31_PROGRESSION_POLICY_VERSION,
        "progression_level": level,
        "a31_provisional_status": status,
        "risk_flag": risk_flag,
        "relative_revision_improvement_vs_v01": relative_improvement,
        "absolute_target_20pct_met": absolute_target_met,
        "growth_sign_changes_below_benchmark": growth_ok,
        "valid_rate_not_degraded": valid_ok,
    }


def a31_grid_summary_row(
    item: dict[str, Any],
    a32_hash: str,
    eval_hash: str,
    metrics: dict[str, Any],
    metric_rows: list[dict[str, Any]],
    timings: dict[str, float],
    classification: str,
    result_dir: Path,
) -> dict[str, Any]:
    full = next(row for row in metric_rows if row["fold"] == "full")
    stability = metrics.get("vintage_stability") or {}
    progression = a31_progression_payload(full)
    return {
        "a31_config_hash": item["a31_config_hash"],
        "a31_config_name": item["config"]["name"],
        "a32_config_hash": a32_hash,
        "a32_config_name": "A32-REF",
        "evaluation_hash": eval_hash,
        "catalog_index": item["catalog_index"],
        "round": item["metadata"].get("round"),
        "description": item["metadata"].get("description"),
        "distance_from_ref": item["distance_from_ref"],
        "result_classification": classification,
        "a31_selection_status": "a31_screened_out",
        **progression,
        "candidate_revision_change_rate": full["candidate_revision_change_rate"],
        "growth_sign_revision_change_days": full["growth_sign_revision_change_days"],
        "inflation_sign_revision_change_days": full["inflation_sign_revision_change_days"],
        "status_revision_change_days": full["status_revision_change_days"],
        "status_revision_change_rate": full.get("status_revision_change_rate"),
        "published_revision_change_days": full.get("published_revision_change_days"),
        "published_revision_change_rate": full.get("published_revision_change_rate"),
        "latched_revision_change_days": full["latched_revision_change_days"],
        "latched_revision_change_rate": full.get("latched_revision_change_rate"),
        "transition_timing_displacement": full["transition_timing_displacement"],
        "transition_timing_displacement_median": transition_displacement_median(
            full["transition_timing_displacement"]
        ),
        "growth_score_revision_abs_median": full["growth_score_revision_abs_median"],
        "growth_score_revision_abs_p90": full["growth_score_revision_abs_p90"],
        "inflation_score_revision_abs_median": full.get(
            "inflation_score_revision_abs_median"
        ),
        "inflation_score_revision_abs_p90": full.get(
            "inflation_score_revision_abs_p90"
        ),
        "growth_raw_sign_change_days": full["growth_raw_sign_change_days"],
        "inflation_raw_sign_change_days": full.get("inflation_raw_sign_change_days"),
        "growth_axis_state_change_days": full["growth_axis_state_change_days"],
        "inflation_axis_state_change_days": full.get(
            "inflation_axis_state_change_days"
        ),
        "candidate_quadrant_change_days": full["candidate_quadrant_change_days"],
        "deadband_absorbed_revision_days": full["deadband_absorbed_revision_days"],
        "revision_episode_count": full["revision_episode_count"],
        "revision_episode_duration_p50": full["revision_episode_duration_p50"],
        "revision_episode_duration_p90": full["revision_episode_duration_p90"],
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


def a32_grid_summary_row(
    item: dict[str, Any],
    a32: A32Config,
    a32_hash: str,
    eval_hash: str,
    metric_rows: list[dict[str, Any]],
    classification: str,
    l3_manifest: dict[str, Any],
) -> dict[str, Any]:
    full = next(row for row in metric_rows if row["fold"] == "full")
    return {
        "a31_config_hash": item["a31_config_hash"],
        "a31_config_name": item["config"]["name"],
        "a32_config_hash": a32_hash,
        "a32_config_name": a32.name,
        "evaluation_hash": eval_hash,
        "l3_score_panel_logical_hash": l3_manifest["logical_hash"],
        "result_classification": classification,
        "growth_enter": a32.growth_enter,
        "inflation_enter": a32.inflation_enter,
        "axis_exit": a32.growth_exit,
        "min_confidence": a32.min_confidence,
        "u_floor": a32.u_floor,
        "growth_score_scale": a32.growth_score_scale,
        "inflation_score_scale": a32.inflation_score_scale,
        "dispersion_limit": a32.dispersion_limit,
        "candidate_revision_change_rate": full["candidate_revision_change_rate"],
        "growth_sign_revision_change_days": full["growth_sign_revision_change_days"],
        "inflation_sign_revision_change_days": full["inflation_sign_revision_change_days"],
        "status_revision_change_days": full["status_revision_change_days"],
        "status_revision_change_rate": full.get("status_revision_change_rate"),
        "published_revision_change_days": full.get("published_revision_change_days"),
        "published_revision_change_rate": full.get("published_revision_change_rate"),
        "latched_revision_change_days": full["latched_revision_change_days"],
        "latched_revision_change_rate": full.get("latched_revision_change_rate"),
        "candidate_flips_per_year": full["candidate_flips_per_year"],
        "published_flips_per_year": full["published_flips_per_year"],
        "valid_rate": full["valid_rate"],
        "abstain_rate": full["abstain_rate"],
        "consumable_state_coverage": full["consumable_state_coverage"],
        "days_since_last_valid_distribution": full["days_since_last_valid_distribution"],
        "consumed_state_age_distribution": full.get("consumed_state_age_distribution"),
        "stale_days_over_5bd": full["stale_days_over_5bd"],
        "longest_stale_run": full["longest_stale_run"],
        "latched_duration": full["latched_duration"],
        "quadrant_occupancy": full["quadrant_occupancy"],
        "days_without_latched_state": full["days_without_latched_state"],
        "first_input_ready_date": full.get("first_input_ready_date"),
        "first_latched_date": full.get("first_latched_date"),
        "first_operational_date": full.get("first_operational_date"),
        "post_initialization_start_date": full.get("post_initialization_start_date"),
        "reason_counts": full["reason_counts"],
        "frozen": False,
        "production_candidate": False,
        "activation_ready": False,
    }


def parse_market_grid_args(argv: list[str]) -> MarketGridConfig:
    ap = argparse.ArgumentParser(description="Run offline market-implied calibration grid")
    ap.add_argument("command", choices=["market-grid"])
    ap.add_argument("--feature-manifest", required=True)
    ap.add_argument("--macro-feature-manifest")
    ap.add_argument("--a31-catalog")
    ap.add_argument("--a32-grid-dir")
    ap.add_argument("--output-dir")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--worker-commit")
    ap.add_argument("--macro-a31-name", default="G2-CREDIT6040-15-SURVEY05")
    ap.add_argument("--macro-a32-name", default="A32-G0.35-I0.35-X0.10-C0.60-D1.25")
    args = ap.parse_args(argv)
    return MarketGridConfig(
        feature_manifest=Path(args.feature_manifest),
        macro_feature_manifest=Path(args.macro_feature_manifest)
        if args.macro_feature_manifest else None,
        a31_catalog=Path(args.a31_catalog) if args.a31_catalog else None,
        a32_grid_dir=Path(args.a32_grid_dir) if args.a32_grid_dir else None,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        offline=args.offline,
        worker_commit=args.worker_commit,
        macro_a31_name=args.macro_a31_name,
        macro_a32_name=args.macro_a32_name,
    )


def run_market_grid(config: MarketGridConfig) -> dict[str, Any]:
    if not config.offline:
        raise SystemExit("market-grid requires --offline to make the no-external-access contract explicit")
    started = dt.datetime.now(UTC)
    execution_id = str(uuid.uuid4())
    output_dir = config.output_dir or (config.feature_manifest.parent / "market_grid")
    output_dir.mkdir(parents=True, exist_ok=True)
    market_manifest, _, market_l2_hash, market_primitives = load_l2_market_from_feature_manifest(
        config.feature_manifest
    )
    market_configs = market_grid_configs_from_primitives(market_primitives)
    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    git_dirty = bool(run_text(
        ["git", "status", "--porcelain"], cwd=Path(__file__).resolve().parents[1]
    ))

    summary_rows: list[dict[str, Any]] = []
    metrics_rows: list[dict[str, Any]] = []
    replay_by_hash: dict[str, list[dict[str, Any]]] = {}
    for item in market_configs:
        cfg = item["config"]
        cfg_hash = market_config_hash(cfg, market_l2_hash)
        replay = run_market_state_machine_from_primitives(
            market_primitives,
            cfg,
            config_hash=cfg_hash,
        )
        replay_by_hash[cfg_hash] = replay
        metric = market_grid_metric_row(replay, cfg, cfg_hash)
        metrics_rows.append(metric)
        summary_rows.append({
            **metric,
            "quantile_basis": item["quantile_basis"],
            "growth_scale_denominator": item["growth_scale_denominator"],
            "inflation_scale_denominator": item["inflation_scale_denominator"],
            "frozen": False,
            "production_candidate": False,
            "activation_ready": False,
        })
    summary_rows.sort(key=market_grid_sort_key)
    metrics_rows.sort(key=lambda row: str(row["market_config_hash"]))
    selected = summary_rows[0] if summary_rows else None
    selected_replay = replay_by_hash[str(selected["market_config_hash"])] if selected else []
    selected_replay = freeze_market_diagnostic_replay(selected_replay)

    macro_replay: list[dict[str, Any]] = []
    comparison_rows: list[dict[str, Any]] = []
    comparison_metrics: dict[str, Any] = {
        "status": "not_run",
        "reason": "macro replay inputs not provided",
    }
    if (
        config.macro_feature_manifest is not None
        and config.a31_catalog is not None
        and config.a32_grid_dir is not None
    ):
        macro_replay = build_selected_macro_replay_for_market_comparison(
            macro_feature_manifest=config.macro_feature_manifest,
            a31_catalog=config.a31_catalog,
            a32_grid_dir=config.a32_grid_dir,
            a31_name=config.macro_a31_name,
            a32_name=config.macro_a32_name,
        )
        comparison_rows, comparison_metrics = compare_macro_market(
            macro_replay,
            selected_replay,
            source="market_grid",
        )

    write_parquet(output_dir / "market_configs.parquet", [
        {
            "market_config_hash": market_config_hash(item["config"], market_l2_hash),
            "market_config_name": item["config"].name,
            "config_json": json.dumps(asdict(item["config"]), sort_keys=True),
            "quantile_basis": item["quantile_basis"],
            "growth_scale_denominator": item["growth_scale_denominator"],
            "inflation_scale_denominator": item["inflation_scale_denominator"],
        }
        for item in market_configs
    ])
    write_parquet(output_dir / "market_grid_summary.parquet", summary_rows)
    write_parquet(output_dir / "market_grid_metrics.parquet", metrics_rows)
    write_parquet(output_dir / "market_replay_selected.parquet", selected_replay)
    write_parquet(output_dir / "macro_replay_for_market_comparison.parquet", macro_replay)
    write_parquet(output_dir / "macro_market_comparison_selected.parquet", comparison_rows)
    write_json(output_dir / "market_comparison_metrics.json", comparison_metrics)

    finished = dt.datetime.now(UTC)
    manifest = {
        "schema_version": MARKET_GRID_SCHEMA_VERSION,
        "code_version": MARKET_GRID_CODE_VERSION,
        "status": "ok",
        "grid": "market_implied",
        "execution_id": execution_id,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "offline": True,
        "external_access": "disabled_by_market_grid_path",
        "worker_commit": worker_commit,
        "git_dirty": git_dirty,
        "feature_manifest_path": str(config.feature_manifest),
        "macro_feature_manifest_path": (
            str(config.macro_feature_manifest) if config.macro_feature_manifest else None
        ),
        "parent_hashes": {
            "l2_market_logical_hash": market_l2_hash,
            "l2_market_schema_version": market_manifest.get("schema_version"),
            "business_date_calendar_hash": market_manifest.get(
                "business_date_calendar_hash"
            ),
        },
        "config_count": len(market_configs),
        "selected_market_config_hash": selected.get("market_config_hash") if selected else None,
        "selected_market_config_name": selected.get("market_config_name") if selected else None,
        "selected_market_model_version": MARKET_DIAGNOSTIC_MODEL_VERSION,
        "runtime_activation": False,
        "freeze_scope": "diagnostic_comparator_only",
        "selected_valid_years": selected.get("valid_years") if selected else [],
        "selected_has_valid_all_years": selected.get("has_valid_all_years") if selected else False,
        "summary_logical_hash": logical_records_hash(summary_rows),
        "metrics_logical_hash": logical_records_hash(metrics_rows),
        "selected_replay_logical_hash": logical_records_hash(selected_replay),
        "comparison_logical_hash": logical_records_hash(comparison_rows),
        "comparison_metrics_logical_hash": logical_payload_hash(comparison_metrics),
        "selection_policy": (
            "empirical-quantile market calibration; no return target and no "
            "macro-agreement optimization"
        ),
        "market_calendar_policy": (
            "business-day output with carry-forward on inferred market-closed days; "
            "SPY-only gaps are classified as data_missing"
        ),
        "a4_status": A4_PROVISIONAL_STATUS,
        "a5_status": "blocked",
        "artifact_hashes": hash_artifacts(
            output_dir,
            [
                "market_configs.parquet",
                "market_grid_summary.parquet",
                "market_grid_metrics.parquet",
                "market_replay_selected.parquet",
                "macro_replay_for_market_comparison.parquet",
                "macro_market_comparison_selected.parquet",
                "market_comparison_metrics.json",
            ],
        ),
    }
    write_json(output_dir / "market_grid_manifest.json", manifest)
    write_text(
        output_dir / "market_operational_report.md",
        render_market_operational_report(manifest, selected or {}, comparison_metrics),
    )
    return {
        "status": "ok",
        "output_dir": str(output_dir),
        "execution_id": execution_id,
        "selected_market_config_hash": manifest["selected_market_config_hash"],
        "selected_has_valid_all_years": manifest["selected_has_valid_all_years"],
        "comparison_both_valid_dates": comparison_metrics.get("both_valid_dates"),
    }


def market_grid_configs_from_primitives(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    growth_abs = [
        abs(float(row["growth_126d_return"]))
        for row in rows
        if finite_or_none(row.get("growth_126d_return")) is not None
    ]
    inflation_abs = [
        abs(float(row["inflation_126d_return"]))
        for row in rows
        if finite_or_none(row.get("inflation_126d_return")) is not None
    ]
    if not growth_abs or not inflation_abs:
        raise ValueError("market_feature_primitives do not contain usable score history")
    configs: list[dict[str, Any]] = []
    for quantile in (0.60, 0.75):
        g_denom = max(percentile(sorted(growth_abs), quantile), 1e-9)
        i_denom = max(percentile(sorted(inflation_abs), quantile), 1e-9)
        for enter in (0.75, 1.00):
            for exit_ in (0.35, 0.50):
                for confidence in (0.15, 0.25, 0.35):
                    if exit_ >= enter:
                        continue
                    name = (
                        f"MKT-Q{int(quantile * 100)}-E{int(enter * 100):03d}"
                        f"-X{int(exit_ * 100):03d}-C{int(confidence * 100):03d}"
                    )
                    configs.append({
                        "quantile_basis": quantile,
                        "growth_scale_denominator": g_denom,
                        "inflation_scale_denominator": i_denom,
                        "config": MarketCalibrationConfig(
                            name=name,
                            growth_score_scale=1.0 / g_denom,
                            inflation_score_scale=1.0 / i_denom,
                            growth_enter=enter,
                            growth_exit=exit_,
                            inflation_enter=enter,
                            inflation_exit=exit_,
                            min_confidence=confidence,
                        ),
                    })
    return configs


def market_config_hash(config: MarketCalibrationConfig, l2_market_hash: str) -> str:
    return stable_hash({
        "l2_market_logical_hash": l2_market_hash,
        "canonical_MarketCalibrationConfig": asdict(config),
        "market_grid_code_version": MARKET_GRID_CODE_VERSION,
    })[:24]


def run_market_state_machine_from_primitives(
    primitives: list[dict[str, Any]],
    config: MarketCalibrationConfig,
    *,
    config_hash: str,
) -> list[dict[str, Any]]:
    rows = sorted(primitives, key=lambda row: str(row["business_date"]))
    prev_g = prev_i = None
    published: str | None = None
    prev_record: dict[str, Any] | None = None
    out: list[dict[str, Any]] = []
    for primitive in rows:
        day = str(primitive["business_date"])
        session = bool(primitive.get("trading_session_indicator"))
        if not session:
            calendar_reason = market_calendar_reason(primitive)
            if calendar_reason == "market_closed_no_session" and prev_record is not None:
                carried = dict(prev_record)
                carried["business_date"] = day
                carried["date"] = day
                carried["input_changed"] = False
                carried["inputs_changed"] = False
                carried["reevaluated"] = False
                carried["transition_occurred"] = False
                carried["market_session"] = False
                carried["market_calendar_reason"] = calendar_reason
                carried["carried_on_market_closed_day"] = True
                out.append(carried)
                prev_record = carried
                continue
        reasons: list[str] = []
        if not session:
            reasons.append(market_calendar_reason(primitive))
        g_raw = finite_or_none(primitive.get("growth_126d_return"))
        i_raw = finite_or_none(primitive.get("inflation_126d_return"))
        if g_raw is None:
            reasons.append("market_growth_warmup_or_missing")
        if i_raw is None:
            reasons.append("market_inflation_warmup_or_missing")
        if session and not primitive.get("spy_available"):
            reasons.append("market_spy_data_missing")
        if session and (not primitive.get("tip_available") or not primitive.get("ief_available")):
            reasons.append("market_tip_ief_data_missing")
        g_score = g_raw * config.growth_score_scale if g_raw is not None else None
        i_score = i_raw * config.inflation_score_scale if i_raw is not None else None
        prev_published = published
        g_state = transition_axis(
            prev_g,
            g_score,
            enter=config.growth_enter,
            exit_=config.growth_exit,
        )
        i_state = transition_axis(
            prev_i,
            i_score,
            enter=config.inflation_enter,
            exit_=config.inflation_exit,
        )
        prev_g, prev_i = g_state.internal_sign, i_state.internal_sign
        g_margin = axis_margin(g_score, config.growth_enter, config.growth_exit)
        i_margin = axis_margin(i_score, config.inflation_enter, config.inflation_exit)
        confidence = math.sqrt(g_margin * i_margin)
        quadrant = quadrant_from_signs(g_state.effective_sign, i_state.effective_sign)
        if g_state.effective_sign is None:
            reasons.append(f"market_growth_{g_state.reason}")
        if i_state.effective_sign is None:
            reasons.append(f"market_inflation_{i_state.reason}")
        if confidence < config.min_confidence:
            reasons.append("market_confidence_below_min")
        reasons = sorted(set(reasons))
        status: Status = "valid" if quadrant and not reasons else "abstain"
        if status == "valid":
            published = quadrant
        record = {
            "business_date": day,
            "date": day,
            "market_config_hash": config_hash,
            "market_config_name": config.name,
            "model_version": "market_implied_quadrant_v0_calibrated_candidate",
            "status": status,
            "status_reason_primary": primary_reason(reasons),
            "status_reasons_all": ",".join(reasons),
            "quadrant": quadrant if status == "valid" else None,
            "published_quadrant": published,
            "latched_quadrant": published,
            "candidate_quadrant": quadrant_from_scores(g_score, i_score),
            "candidate_confidence": confidence,
            "confidence": confidence,
            "growth_sign": g_state.effective_sign,
            "inflation_sign": i_state.effective_sign,
            "growth_axis_state": axis_state_label(g_state),
            "inflation_axis_state": axis_state_label(i_state),
            "growth_raw_score": g_raw,
            "inflation_raw_score": i_raw,
            "growth_score": g_score,
            "inflation_score": i_score,
            "growth_margin": g_margin,
            "inflation_margin": i_margin,
            "confidence_growth_margin_component": g_margin,
            "confidence_inflation_margin_component": i_margin,
            "confidence_formula": "sqrt(growth_margin * inflation_margin)",
            "confidence_ok": confidence >= config.min_confidence,
            "market_session": session,
            "market_calendar_reason": market_calendar_reason(primitive) if not session else None,
            "carried_on_market_closed_day": False,
            "input_changed": True,
            "inputs_changed": True,
            "reevaluated": True,
            "transition_occurred": (
                published is not None and published != prev_published
            ),
            "lookback_days": primitive.get("lookback_days"),
            "price_source": primitive.get("price_source"),
            "price_convention": "market_feature_primitives 126-day returns; market-closed days carry forward prior state",
        }
        out.append(record)
        prev_record = record
    return out


def market_calendar_reason(row: dict[str, Any]) -> str:
    spy = bool(row.get("spy_available"))
    tip = bool(row.get("tip_available"))
    ief = bool(row.get("ief_available"))
    if not spy and not tip and not ief:
        return "market_closed_no_session"
    if not spy:
        return "market_spy_data_missing"
    return "market_partial_data_missing"


def market_grid_metric_row(
    replay: list[dict[str, Any]],
    config: MarketCalibrationConfig,
    config_hash: str,
) -> dict[str, Any]:
    metrics = build_market_metrics(replay)
    years = sorted({int(str(row["date"])[:4]) for row in replay})
    valid_years = sorted({
        int(str(row["date"])[:4])
        for row in replay
        if row.get("status") == "valid"
    })
    total = len(replay)
    market_closed_days = sum(
        1 for row in replay
        if row.get("market_calendar_reason") == "market_closed_no_session"
    )
    spy_missing_data_days = sum(
        1 for row in replay
        if row.get("market_calendar_reason") == "market_spy_data_missing"
    )
    fixed_income_missing_days = sum(
        1 for row in replay
        if "market_tip_ief_data_missing" in str(row.get("status_reasons_all"))
    )
    years_denominator = max(1.0, total / 252.0)
    valid_rows = [row for row in replay if row.get("status") == "valid"]
    return {
        "market_config_hash": config_hash,
        "market_config_name": config.name,
        "config_json": json.dumps(asdict(config), sort_keys=True),
        "growth_enter": config.growth_enter,
        "growth_exit": config.growth_exit,
        "inflation_enter": config.inflation_enter,
        "inflation_exit": config.inflation_exit,
        "growth_score_scale": config.growth_score_scale,
        "inflation_score_scale": config.inflation_score_scale,
        "min_confidence": config.min_confidence,
        "eligible_days": total,
        "market_valid_rate": metrics["market_valid_rate"],
        "valid_days": count_by(replay, "status").get("valid", 0),
        "abstain_days": count_by(replay, "status").get("abstain", 0),
        "status_counts": json.dumps(metrics["market_status_counts"], sort_keys=True),
        "reason_counts": json.dumps(
            metrics["market_status_reason_any_counts"], sort_keys=True
        ),
        "candidate_quadrant_counts": json.dumps(
            metrics["market_candidate_quadrant_counts"], sort_keys=True
        ),
        "published_quadrant_counts": json.dumps(
            count_by(valid_rows, "published_quadrant"), sort_keys=True
        ),
        "candidate_flips_per_year": (
            flips_for_key(replay, "candidate_quadrant") / years_denominator
        ),
        "published_flips_per_year": (
            flips_for_key(valid_rows, "published_quadrant") / years_denominator
        ),
        "candidate_duration_distribution": json.dumps(
            duration_summary(state_durations(replay, "candidate_quadrant")),
            sort_keys=True,
        ),
        "published_duration_distribution": json.dumps(
            duration_summary(state_durations(valid_rows, "published_quadrant")),
            sort_keys=True,
        ),
        "confidence_distribution": json.dumps(
            metrics["market_confidence_distribution"], sort_keys=True
        ),
        "growth_score_distribution": json.dumps(
            metrics["market_growth_score_distribution"], sort_keys=True
        ),
        "inflation_score_distribution": json.dumps(
            metrics["market_inflation_score_distribution"], sort_keys=True
        ),
        "market_closed_days": market_closed_days,
        "spy_missing_data_days": spy_missing_data_days,
        "fixed_income_missing_days": fixed_income_missing_days,
        "valid_years": json.dumps(valid_years),
        "all_years": json.dumps(years),
        "has_valid_all_years": valid_years == years,
        "frozen": False,
        "production_candidate": False,
        "activation_ready": False,
    }


def market_grid_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    duration = parse_json_metric(row.get("published_duration_distribution"))
    median_duration = duration.get("median")
    flip_penalty = abs(float(row.get("published_flips_per_year") or 0.0) - 4.0)
    return (
        0 if row.get("has_valid_all_years") else 1,
        -float(row.get("market_valid_rate") or 0.0),
        flip_penalty,
        none_last(median_duration),
        none_last(row.get("min_confidence")),
        str(row.get("market_config_hash")),
    )


def freeze_market_diagnostic_replay(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frozen: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["model_version"] = MARKET_DIAGNOSTIC_MODEL_VERSION
        item["diagnostic_frozen"] = True
        item["runtime_activation"] = False
        frozen.append(item)
    return frozen


def build_selected_macro_replay_for_market_comparison(
    *,
    macro_feature_manifest: Path,
    a31_catalog: Path,
    a32_grid_dir: Path,
    a31_name: str,
    a32_name: str,
) -> list[dict[str, Any]]:
    _, _, l2_hash, l2_records = load_l2_macro_from_feature_manifest(macro_feature_manifest)
    catalog_payload = read_catalog_payload(a31_catalog)
    normalized_catalog, _ = normalize_a31_catalog(
        catalog_payload,
        l2_macro_logical_hash=l2_hash,
        source_path=a31_catalog,
    )
    matches = [
        item for item in normalized_catalog["configs"]
        if item["config"]["name"] == a31_name
    ]
    if len(matches) != 1:
        raise ValueError(f"macro A31 config is not unique: {a31_name}")
    a31 = A31Config(**matches[0]["config"])
    l3_rows, _, _ = build_l3_score_panel(
        l2_records,
        a31,
        l2_macro_logical_hash=l2_hash,
        expected_l2_macro_logical_hash=l2_hash,
    )
    a32 = load_a32_config_from_grid(a32_grid_dir, a32_name)
    macro_replay, _ = run_l4_state_machine(l3_rows, a32, selection_mode="latest")
    return macro_replay


def load_a32_config_from_grid(a32_grid_dir: Path, a32_name: str) -> A32Config:
    rows = read_parquet_records(a32_grid_dir / "a32_configs.parquet")
    matches = [row for row in rows if row.get("a32_config_name") == a32_name]
    if len(matches) != 1:
        raise ValueError(f"A32 config is not unique in grid dir: {a32_name}")
    payload = json.loads(str(matches[0]["config_json"]))
    return A32Config(**payload)


def render_market_operational_report(
    manifest: dict[str, Any],
    selected: dict[str, Any],
    comparison_metrics: dict[str, Any],
) -> str:
    return "\n".join([
        "# Market-Implied Calibration Grid",
        "",
        f"- worker_commit: `{manifest['worker_commit']}`",
        f"- git_dirty: `{manifest['git_dirty']}`",
        f"- selected_config: `{manifest['selected_market_config_name']}`",
        f"- selected_has_valid_all_years: `{manifest['selected_has_valid_all_years']}`",
        f"- selected_valid_years: `{manifest['selected_valid_years']}`",
        f"- market_valid_rate: `{selected.get('market_valid_rate')}`",
        f"- valid_days: `{selected.get('valid_days')}`",
        f"- published_flips_per_year: `{selected.get('published_flips_per_year')}`",
        f"- market_closed_days: `{selected.get('market_closed_days')}`",
        f"- spy_missing_data_days: `{selected.get('spy_missing_data_days')}`",
        f"- fixed_income_missing_days: `{selected.get('fixed_income_missing_days')}`",
        "",
        "## Comparison",
        "",
        f"- common_dates: `{comparison_metrics.get('common_dates')}`",
        f"- both_valid_dates: `{comparison_metrics.get('both_valid_dates')}`",
        f"- exact_quadrant_agreement_rate: `{comparison_metrics.get('exact_quadrant_agreement_rate')}`",
        f"- macro_valid_market_abstain_rate: `{comparison_metrics.get('macro_valid_market_abstain_rate')}`",
        f"- market_valid_macro_abstain_rate: `{comparison_metrics.get('market_valid_macro_abstain_rate')}`",
        "",
        "No return series is used as an optimization target, and macro agreement is diagnostic only.",
        "",
    ])


def parse_a3_scope_decision_args(argv: list[str]) -> A3ScopeDecisionConfig:
    ap = argparse.ArgumentParser(description="Write formal A3 scope decision")
    ap.add_argument("command", choices=["a3-scope-decision"])
    ap.add_argument("--freeze-readiness-dir", required=True)
    ap.add_argument("--market-grid-dir", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--worker-commit")
    args = ap.parse_args(argv)
    return A3ScopeDecisionConfig(
        freeze_readiness_dir=Path(args.freeze_readiness_dir),
        market_grid_dir=Path(args.market_grid_dir),
        output_dir=Path(args.output_dir),
        worker_commit=args.worker_commit,
    )


def run_a3_scope_decision(config: A3ScopeDecisionConfig) -> dict[str, Any]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    worker_commit = config.worker_commit or run_text(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1]
    )
    freeze_manifest_path = single_matching_path(
        config.freeze_readiness_dir,
        "a3_progression_v2_manifest_*.json",
    )
    pareto_path = single_matching_path(
        config.freeze_readiness_dir,
        "a32_selected_pareto_???????.parquet",
    )
    pareto_by_fold_path = single_matching_path(
        config.freeze_readiness_dir,
        "a32_selected_pareto_by_fold_*.parquet",
    )
    freeze_manifest = read_json_dict(freeze_manifest_path)
    pareto = read_parquet_records(pareto_path)
    pareto_by_fold = read_parquet_records(pareto_by_fold_path)
    market_manifest = read_json_dict(config.market_grid_dir / "market_grid_manifest.json")
    market_comparison = read_json_dict(
        config.market_grid_dir / "market_comparison_metrics.json"
    )
    current = next(
        row for row in pareto
        if row.get("pareto_role") == "current_stability_preserving"
    )
    current_full_fold = next(
        row for row in pareto_by_fold
        if row.get("pareto_role") == "current_stability_preserving"
        and row.get("fold") == "full"
    )
    outcome = a3_scope_outcome(current, freeze_manifest, market_manifest, market_comparison)
    decision = {
        "schema_version": A3_SCOPE_DECISION_SCHEMA_VERSION,
        "artifact_type": "a3_scope_decision_v1",
        "worker_commit": worker_commit,
        "git_dirty": bool(run_text(
            ["git", "status", "--porcelain"], cwd=Path(__file__).resolve().parents[1]
        )),
        "result": outcome,
        "allowed_results": [
            "freeze_ready",
            "open_macro_v03",
            "request_freeze_gate_revision",
        ],
        "freeze_ready": outcome == "freeze_ready",
        "macro_current": {
            "a31_config_name": current.get("a31_config_name"),
            "a32_config_name": current.get("a32_config_name"),
            "candidate_revision_change_rate": current.get(
                "candidate_revision_change_rate"
            ),
            "consumable_state_coverage": current.get("consumable_state_coverage"),
            "transition_displacement_p90": current.get("transition_displacement_p90"),
            "revision_episode_duration_p90": current_full_fold.get(
                "revision_episode_duration_p90"
            ),
            "revision_episode_count": current_full_fold.get("revision_episode_count"),
            "first_operational_date": current.get("first_operational_date"),
        },
        "market_current": {
            "selected_market_config_name": market_manifest.get(
                "selected_market_config_name"
            ),
            "selected_has_valid_all_years": market_manifest.get(
                "selected_has_valid_all_years"
            ),
            "both_valid_dates": market_comparison.get("both_valid_dates"),
            "exact_quadrant_agreement_rate": market_comparison.get(
                "exact_quadrant_agreement_rate"
            ),
        },
        "decision_basis": {
            "freeze_manifest": freeze_manifest_path.name,
            "pareto_logical_hash": logical_records_hash(pareto),
            "pareto_by_fold_logical_hash": logical_records_hash(pareto_by_fold),
            "market_grid_manifest": "market_grid_manifest.json",
            "market_grid_summary_hash": market_manifest.get("summary_logical_hash"),
            "market_comparison_hash": market_manifest.get("comparison_logical_hash"),
        },
        "reason": a3_scope_reasons(current, market_manifest, market_comparison),
        "a4_status": A4_PROVISIONAL_STATUS,
        "a4_allowed_scope": [
            "replay_smoke",
            "book_compilation",
            "feasibility_tests",
            "metrics_generation",
            "identity_gate_validation",
            "lineage_tests",
        ],
        "a4_forbidden_scope": [
            "center_selection",
            "policy_selection_by_maxdd_cvar",
            "half_width_calibration",
            "gamma_or_beta_cap_calibration",
            "gate_calibration",
        ],
        "a5_status": "blocked",
    }
    decision["decision_basis_hash"] = logical_payload_hash(decision["decision_basis"])
    decision_path = config.output_dir / "a3_scope_decision_v1.json"
    write_json(decision_path, decision)
    return {
        "status": "ok",
        "output_dir": str(config.output_dir),
        "decision": outcome,
        "freeze_ready": decision["freeze_ready"],
        "decision_basis_hash": decision["decision_basis_hash"],
    }


def single_matching_path(base: Path, pattern: str) -> Path:
    matches = sorted(base.glob(pattern))
    if len(matches) != 1:
        raise ValueError(f"expected one {pattern} in {base}, found {len(matches)}")
    return matches[0]


def a3_scope_outcome(
    current: dict[str, Any],
    freeze_manifest: dict[str, Any],
    market_manifest: dict[str, Any],
    market_comparison: dict[str, Any],
) -> str:
    revision = float(current.get("candidate_revision_change_rate") or 1.0)
    consumable = float(current.get("consumable_state_coverage") or 0.0)
    market_operational = bool(market_manifest.get("selected_has_valid_all_years")) and (
        int(market_comparison.get("both_valid_dates") or 0) > 0
    )
    if (
        not freeze_manifest.get("freeze_blockers")
        and revision <= MAX_REVISION_CHANGE_RATE_FREEZE
        and consumable >= MIN_VALID_RATE_FREEZE
        and market_operational
    ):
        return "freeze_ready"
    if revision > MAX_REVISION_CHANGE_RATE_FREEZE or consumable < MIN_VALID_RATE_FREEZE:
        return "open_macro_v03"
    return "request_freeze_gate_revision"


def a3_scope_reasons(
    current: dict[str, Any],
    market_manifest: dict[str, Any],
    market_comparison: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    revision = float(current.get("candidate_revision_change_rate") or 1.0)
    consumable = float(current.get("consumable_state_coverage") or 0.0)
    if revision > MAX_REVISION_CHANGE_RATE_FREEZE:
        reasons.append("candidate_revision_rate_above_10pct_freeze_gate")
    if consumable < MIN_VALID_RATE_FREEZE:
        reasons.append("consumable_coverage_below_operational_freeze_band")
    if not market_manifest.get("selected_has_valid_all_years"):
        reasons.append("market_implied_lacks_valid_observations_in_all_years")
    if int(market_comparison.get("both_valid_dates") or 0) <= 0:
        reasons.append("macro_market_valid_vs_valid_comparison_not_operational")
    reasons.append("A4_limited_to_smoke_and_viability")
    reasons.append("A5_blocked")
    return reasons


def transition_displacement_median(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    payload = json.loads(str(value)) if isinstance(value, str) else value
    median = payload.get("median") if isinstance(payload, dict) else None
    return None if median is None else float(median)


def build_a31_progression_decision_manifest(
    summary_rows: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
    *,
    config_catalog_path: Path,
    failure_count: int = 0,
) -> dict[str, Any]:
    candidates = [
        row for row in summary_rows
        if "CONTROL" not in str(row.get("a31_config_name", "")).upper()
    ]
    ranked = sorted(
        candidates or summary_rows,
        key=lambda row: (
            progression_rank(row.get("progression_level")),
            none_last(row.get("candidate_revision_change_rate")),
            none_last(row.get("growth_sign_revision_change_days")),
            str(row.get("a31_config_hash")),
        ),
    )
    champion = ranked[0] if ranked else None
    control = a31_decision_control_row(summary_rows)
    if champion is None:
        return {
            "decision_policy_version": A31_PROGRESSION_POLICY_VERSION,
            "new_decision": "no_candidate_available",
            "config_catalog_path": str(config_catalog_path),
        }

    progression_level = str(champion.get("progression_level"))
    if progression_level == "pass":
        new_decision = "advance_to_g2"
    elif progression_level == "conditional_pass":
        new_decision = "advance_to_g2_limited"
    else:
        new_decision = "do_not_advance_to_g2"
    previous_decision = (
        "do_not_advance_to_g2"
        if any(row.get("a31_config_name") == "V02B-G1-CREDIT-6040-15" for row in summary_rows)
        else None
    )
    fold_deltas = fold_revision_deltas(
        metric_rows,
        str(champion.get("a31_config_hash")),
        str(control.get("a31_config_hash")) if control else None,
    )
    return {
        "decision_policy_version": A31_PROGRESSION_POLICY_VERSION,
        "previous_decision": previous_decision,
        "new_decision": new_decision,
        "champion": {
            "a31_config_name": champion.get("a31_config_name"),
            "a31_config_hash": champion.get("a31_config_hash"),
            "status": champion.get("a31_provisional_status"),
            "risk_flag": champion.get("risk_flag"),
            "candidate_revision_change_rate": champion.get("candidate_revision_change_rate"),
            "relative_revision_improvement_vs_v01": champion.get(
                "relative_revision_improvement_vs_v01"
            ),
            "growth_sign_revision_change_days": champion.get(
                "growth_sign_revision_change_days"
            ),
            "valid_rate": champion.get("valid_rate"),
            "absolute_target_20pct_met": champion.get("absolute_target_20pct_met"),
        },
        "reason": {
            "relative_revision_improvement_vs_v01": champion.get(
                "relative_revision_improvement_vs_v01"
            ),
            "growth_sign_changes_below_benchmark": champion.get(
                "growth_sign_changes_below_benchmark"
            ),
            "valid_rate_not_degraded": champion.get("valid_rate_not_degraded"),
            "absolute_target_20pct_met": champion.get("absolute_target_20pct_met"),
            "fold_revision_deltas_vs_control": fold_deltas,
            "material_fold_deterioration": any(
                delta is not None and delta > 0.02 for delta in fold_deltas.values()
            ),
        },
        "hard_blocks_checked_by_grid": {
            "a32_canonical_hash_unique": True,
            "grid_failures": failure_count,
            "external_access": "disabled_by_grid_only_path",
        },
        "a3_1_status": "provisional_candidate_selected"
        if new_decision in {"advance_to_g2", "advance_to_g2_limited"}
        else "screen_complete_no_g2",
        "a3_2_status": "ready_to_start"
        if new_decision in {"advance_to_g2", "advance_to_g2_limited"}
        else "blocked",
        "a4_status": (
            A4_PROVISIONAL_STATUS
            if new_decision in {"advance_to_g2", "advance_to_g2_limited"}
            else "candidate_seed_only"
        ),
        "a5_status": "blocked",
    }


def a31_decision_control_row(summary_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    exact_names = {"G2-CREDIT6040-15", "V02B-G0-CONTROL", "V02-G0-CONTROL"}
    exact = next(
        (
            row for row in summary_rows
            if str(row.get("a31_config_name")) in exact_names
        ),
        None,
    )
    if exact is not None:
        return exact
    return next(
        (
            row for row in summary_rows
            if "CONTROL" in str(row.get("a31_config_name", "")).upper()
        ),
        None,
    )


def progression_rank(level: Any) -> int:
    return {"pass": 0, "conditional_pass": 1, "fail": 2}.get(str(level), 3)


def fold_revision_deltas(
    metric_rows: list[dict[str, Any]],
    champion_hash: str,
    control_hash: str | None,
) -> dict[str, float | None]:
    if control_hash is None:
        return {}
    by_key = {
        (str(row.get("a31_config_hash")), str(row.get("fold"))): row
        for row in metric_rows
    }
    out: dict[str, float | None] = {}
    for fold in ("2014_2017", "2018_2021", "2022_2026"):
        lhs = by_key.get((champion_hash, fold), {}).get("candidate_revision_change_rate")
        rhs = by_key.get((control_hash, fold), {}).get("candidate_revision_change_rate")
        out[fold] = None if lhs is None or rhs is None else float(lhs) - float(rhs)
    return out


def timing_summary_fields(timings: dict[str, float]) -> dict[str, float]:
    keys = ["load_l2", "compute_l3", "write_l3", "run_l4", "compute_metrics", "write_artifacts"]
    return {f"timing_{key}_seconds": round(float(timings.get(key, 0.0)), 6) for key in keys}


def aggregate_timing_records(
    timing_records: list[dict[str, float]],
) -> dict[str, dict[str, float | int | None]]:
    keys = sorted({key for row in timing_records for key in row})
    out: dict[str, dict[str, float | int | None]] = {}
    for key in keys:
        values = [float(row[key]) for row in timing_records if key in row]
        out[key] = {
            "count": len(values),
            "total": round(sum(values), 6) if values else None,
            "mean": round(sum(values) / len(values), 6) if values else None,
            "max": round(max(values), 6) if values else None,
        }
    return out


def load_existing_a31_result(
    result_dir: Path,
    *,
    expected_l2_hash: str,
    expected_a31_hash: str,
    expected_a32_hash: str,
    expected_evaluation_hash: str,
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
    if manifest.get("evaluation_hash") != expected_evaluation_hash:
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
    if argv and argv[0] == "revision-uncertainty":
        result = run_revision_uncertainty(parse_revision_uncertainty_args(argv))
        print(json.dumps(result, sort_keys=True))
        return
    if argv and argv[0] == "a31-v03-grid":
        result = run_a31_v03_grid(parse_a31_v03_grid_args(argv))
        print(json.dumps(result, sort_keys=True))
        return
    if argv and argv[0] == "a32-grid":
        result = run_a32_grid(parse_a32_grid_args(argv))
        print(json.dumps(result, sort_keys=True))
        return
    if argv and argv[0] == "a3-freeze-readiness":
        result = run_a3_freeze_readiness_package(parse_a3_freeze_readiness_args(argv))
        print(json.dumps(result, sort_keys=True))
        return
    if argv and argv[0] == "market-grid":
        result = run_market_grid(parse_market_grid_args(argv))
        print(json.dumps(result, sort_keys=True))
        return
    if argv and argv[0] == "a3-scope-decision":
        result = run_a3_scope_decision(parse_a3_scope_decision_args(argv))
        print(json.dumps(result, sort_keys=True))
        return
    if argv and argv[0] == "v02-fetch-alfred":
        result = run_v02_fetch_alfred(parse_v02_fetch_alfred_args(argv))
        print(json.dumps(result, sort_keys=True))
        return
    if argv and argv[0] == "v02-qualify":
        result = run_v02_qualification(parse_v02_qualification_args(argv))
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

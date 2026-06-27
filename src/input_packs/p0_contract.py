"""Shared P0 Certified Input Pack table contract."""

from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


@dataclass(frozen=True)
class TableSpec:
    name: str
    key_columns: tuple[str, ...]
    columns: tuple[str, ...]
    numeric_columns: frozenset[str] = frozenset()
    boolean_columns: frozenset[str] = frozenset()
    date_columns: frozenset[str] = frozenset()
    as_of_column: str | None = None


def normalize_date(value: Any) -> str:
    if isinstance(value, dt.date):
        return value.isoformat()
    if not isinstance(value, str):
        raise ValueError(f"date value must be a string, got {type(value).__name__}")
    return dt.date.fromisoformat(value[:10]).isoformat()


def normalize_number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"numeric value must not be boolean: {value!r}")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"numeric value is invalid: {value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"numeric value must be finite: {value!r}")
    if number == number.to_integral_value():
        return int(number)
    as_float = float(number)
    if not math.isfinite(as_float):
        raise ValueError(f"numeric value must be finite: {value!r}")
    return as_float


def normalize_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "1", "yes", "y"}:
            return True
        if normalized in {"false", "f", "0", "no", "n"}:
            return False
    raise ValueError(f"boolean value is invalid: {value!r}")


def normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): normalize_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    return value


def normalize_row(row: Mapping[str, Any], spec: TableSpec) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for column in spec.columns:
        value = row.get(column)
        if column in spec.date_columns and value is not None:
            normalized[column] = normalize_date(value)
        elif column in spec.numeric_columns:
            normalized[column] = normalize_number(value)
        elif column in spec.boolean_columns and value is not None:
            normalized[column] = normalize_boolean(value)
        elif value is None:
            normalized[column] = None
        elif isinstance(value, (dict, list)):
            normalized[column] = normalize_value(value)
        else:
            normalized[column] = str(value)
    return normalized


def row_sort_key(row: Mapping[str, Any], spec: TableSpec) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in spec.key_columns)


P0_TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        name="nav_timeseries",
        key_columns=("instrument_id", "nav_date"),
        columns=("instrument_id", "nav_date", "nav", "source"),
        numeric_columns=frozenset({"nav"}),
        date_columns=frozenset({"nav_date"}),
        as_of_column="nav_date",
    ),
    TableSpec(
        name="eod_prices",
        key_columns=("ticker", "date"),
        columns=("ticker", "date", "close", "adjusted_close", "volume"),
        numeric_columns=frozenset({"close", "adjusted_close", "volume"}),
        date_columns=frozenset({"date"}),
        as_of_column="date",
    ),
    TableSpec(
        name="macro_data",
        key_columns=("series_id", "obs_date"),
        columns=("series_id", "obs_date", "value", "source", "is_derived"),
        numeric_columns=frozenset({"value"}),
        boolean_columns=frozenset({"is_derived"}),
        date_columns=frozenset({"obs_date"}),
        as_of_column="obs_date",
    ),
    TableSpec(
        name="instruments_universe",
        key_columns=("instrument_id",),
        columns=("instrument_id", "ticker", "asset_class", "strategy", "is_active", "attributes"),
        boolean_columns=frozenset({"is_active"}),
    ),
    TableSpec(
        name="instrument_identity",
        key_columns=("instrument_id",),
        columns=("instrument_id", "cik_unpadded", "sec_series_id", "isin", "cusip"),
    ),
    TableSpec(
        name="fund_strategy_benchmark_proxy_map",
        key_columns=("strategy_label",),
        columns=("strategy_label", "benchmark_ticker", "proxy_source"),
    ),
    TableSpec(
        name="strategy_reclassification_stage",
        key_columns=("instrument_id", "effective_date", "strategy_label"),
        columns=("instrument_id", "strategy_label", "source_table", "effective_date"),
        date_columns=frozenset({"effective_date"}),
        as_of_column="effective_date",
    ),
    TableSpec(
        name="sec_nport_holdings",
        key_columns=("series_id", "report_date", "holding_key"),
        columns=("series_id", "report_date", "holding_key", "ticker", "pct_of_nav", "market_value"),
        numeric_columns=frozenset({"pct_of_nav", "market_value"}),
        date_columns=frozenset({"report_date"}),
        as_of_column="report_date",
    ),
    TableSpec(
        name="sec_nport_fund_monthly_flows",
        key_columns=("series_id", "month_end"),
        columns=("series_id", "month_end", "total_net_assets", "net_flow"),
        numeric_columns=frozenset({"total_net_assets", "net_flow"}),
        date_columns=frozenset({"month_end"}),
        as_of_column="month_end",
    ),
)

P0_TABLES_BY_NAME: Mapping[str, TableSpec] = {spec.name: spec for spec in P0_TABLE_SPECS}

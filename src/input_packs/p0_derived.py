"""P0 Certified Input Pack derived feature contract."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


DERIVED_FEATURE_LINEAGE: tuple[dict[str, Any], ...] = (
    {
        "feature_file": "data/derived/fund_nav_return_features.json",
        "feature_name": "fund_nav_return_1d",
        "sources": [{"table": "nav_timeseries", "columns": ["instrument_id", "nav_date", "nav"]}],
    },
    {
        "feature_file": "data/derived/market_price_return_features.json",
        "feature_name": "market_price_return_1d",
        "sources": [{"table": "eod_prices", "columns": ["ticker", "date", "adjusted_close", "close"]}],
    },
    {
        "feature_file": "data/derived/macro_observation_features.json",
        "feature_name": "macro_level",
        "sources": [{"table": "macro_data", "columns": ["series_id", "obs_date", "value"]}],
    },
    {
        "feature_file": "data/derived/macro_observation_features.json",
        "feature_name": "macro_delta_1obs",
        "sources": [{"table": "macro_data", "columns": ["series_id", "obs_date", "value"]}],
    },
    {
        "feature_file": "data/derived/fund_universe_features.json",
        "feature_name": "fund_universe_identity_strategy_benchmark",
        "sources": [
            {"table": "instruments_universe", "columns": ["instrument_id", "ticker", "asset_class", "strategy"]},
            {"table": "instrument_identity", "columns": ["instrument_id", "cik_unpadded", "sec_series_id"]},
            {
                "table": "fund_strategy_benchmark_proxy_map",
                "columns": ["strategy_label", "benchmark_ticker"],
            },
            {
                "table": "strategy_reclassification_stage",
                "columns": ["instrument_id", "strategy_label", "effective_date"],
            },
        ],
    },
    {
        "feature_file": "data/derived/holdings_summary_features.json",
        "feature_name": "holdings_summary_inputs",
        "sources": [
            {"table": "sec_nport_holdings", "columns": ["series_id", "report_date", "holding_key", "pct_of_nav"]}
        ],
    },
    {
        "feature_file": "data/derived/flow_momentum_features.json",
        "feature_name": "flow_momentum_window_input",
        "sources": [
            {
                "table": "sec_nport_fund_monthly_flows",
                "columns": ["series_id", "month_end", "total_net_assets", "net_flow"],
            }
        ],
    },
)


def grouped(rows: Iterable[Mapping[str, Any]], key: str) -> dict[str, list[Mapping[str, Any]]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row[key]), []).append(row)
    return groups


def round_feature(value: float) -> float:
    return round(value, 12)


def derive_nav_features(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for instrument_id, items in grouped(rows, "instrument_id").items():
        ordered = sorted(items, key=lambda row: row["nav_date"])
        previous: float | None = None
        for row in ordered:
            nav = float(row["nav"])
            if previous and previous > 0:
                features.append(
                    {
                        "feature_name": "fund_nav_return_1d",
                        "instrument_id": instrument_id,
                        "observation_date": row["nav_date"],
                        "value": round_feature(nav / previous - 1.0),
                    }
                )
            previous = nav
    return sorted(features, key=lambda row: (row["instrument_id"], row["observation_date"]))


def derive_price_features(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for ticker, items in grouped(rows, "ticker").items():
        ordered = sorted(items, key=lambda row: row["date"])
        previous: float | None = None
        for row in ordered:
            price_value = row.get("adjusted_close") if row.get("adjusted_close") is not None else row.get("close")
            price = float(price_value)
            if previous and previous > 0:
                features.append(
                    {
                        "feature_name": "market_price_return_1d",
                        "observation_date": row["date"],
                        "ticker": ticker,
                        "value": round_feature(price / previous - 1.0),
                    }
                )
            previous = price
    return sorted(features, key=lambda row: (row["ticker"], row["observation_date"]))


def derive_macro_features(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for series_id, items in grouped(rows, "series_id").items():
        ordered = sorted(items, key=lambda row: row["obs_date"])
        previous: float | None = None
        for row in ordered:
            value = float(row["value"])
            features.append(
                {
                    "feature_name": "macro_level",
                    "observation_date": row["obs_date"],
                    "series_id": series_id,
                    "value": round_feature(value),
                }
            )
            if previous is not None:
                features.append(
                    {
                        "feature_name": "macro_delta_1obs",
                        "observation_date": row["obs_date"],
                        "series_id": series_id,
                        "value": round_feature(value - previous),
                    }
                )
            previous = value
    return sorted(features, key=lambda row: (row["series_id"], row["observation_date"], row["feature_name"]))


def latest_strategy_by_instrument(rows: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    selected: dict[str, Mapping[str, Any]] = {}
    for row in sorted(rows, key=lambda item: (item["instrument_id"], item["effective_date"], item["strategy_label"])):
        selected[str(row["instrument_id"])] = row
    return selected


def derive_universe_features(canonical: Mapping[str, Sequence[Mapping[str, Any]]]) -> list[dict[str, Any]]:
    identities = {str(row["instrument_id"]): row for row in canonical["instrument_identity"]}
    strategy_rows = latest_strategy_by_instrument(canonical["strategy_reclassification_stage"])
    proxies = {str(row["strategy_label"]): row for row in canonical["fund_strategy_benchmark_proxy_map"]}

    features: list[dict[str, Any]] = []
    for row in canonical["instruments_universe"]:
        instrument_id = str(row["instrument_id"])
        strategy_label = str(
            (strategy_rows.get(instrument_id) or {}).get("strategy_label")
            or row.get("strategy")
            or "unclassified"
        )
        identity = identities.get(instrument_id, {})
        proxy = proxies.get(strategy_label, {})
        features.append(
            {
                "asset_class": row.get("asset_class"),
                "benchmark_ticker": proxy.get("benchmark_ticker"),
                "cik_unpadded": identity.get("cik_unpadded"),
                "feature_name": "fund_universe_identity_strategy_benchmark",
                "instrument_id": instrument_id,
                "is_active": row.get("is_active"),
                "sec_series_id": identity.get("sec_series_id"),
                "strategy_label": strategy_label,
                "ticker": row.get("ticker"),
            }
        )
    return sorted(features, key=lambda item: item["instrument_id"])


def derive_holdings_features(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_series_date: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        by_series_date.setdefault((str(row["series_id"]), str(row["report_date"])), []).append(row)

    latest_by_series: dict[str, tuple[str, list[Mapping[str, Any]]]] = {}
    for (series_id, report_date), items in sorted(by_series_date.items()):
        latest_by_series[series_id] = (report_date, items)

    features: list[dict[str, Any]] = []
    for series_id, (report_date, items) in sorted(latest_by_series.items()):
        pct_values = [float(item["pct_of_nav"]) for item in items if item.get("pct_of_nav") is not None]
        features.append(
            {
                "feature_name": "holdings_summary_inputs",
                "holdings_count": len(items),
                "largest_holding_pct": round_feature(max(pct_values) if pct_values else 0.0),
                "pct_nav_covered": round_feature(sum(pct_values)),
                "report_date": report_date,
                "series_id": series_id,
            }
        )
    return features


def derive_flow_features(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for series_id, items in grouped(rows, "series_id").items():
        ordered = sorted(items, key=lambda row: row["month_end"])
        trailing = ordered[-3:]
        assets = [float(row["total_net_assets"]) for row in trailing if row.get("total_net_assets")]
        denominator = sum(assets) / len(assets) if assets else 0.0
        flow = sum(float(row["net_flow"]) for row in trailing if row.get("net_flow") is not None)
        features.append(
            {
                "as_of_month_end": trailing[-1]["month_end"] if trailing else None,
                "feature_name": "flow_momentum_window_input",
                "series_id": series_id,
                "value": round_feature(flow / denominator) if denominator else None,
                "window_months": len(trailing),
            }
        )
    return sorted(features, key=lambda row: row["series_id"])

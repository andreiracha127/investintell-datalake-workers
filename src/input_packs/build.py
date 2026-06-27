"""P0 Certified Input Pack builder.

The builder consumes local raw snapshot files and writes an offline pack that
the quant-engine can verify without database or network access.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .hashing import canonical_json_sha256, file_sha256
from .manifest import build_manifest, write_manifest
from .verifier import verify_pack

PROFILE_OPEN_MACRO_V03 = "open_macro_v03"
P0_INPUT_PACK_ID = "open_macro_v03_certified_input_pack_001"
SOURCE_REPO = "investintell-datalake-workers"
INPUT_PACK_VERSION = "v1"
IMAGE_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class TableSpec:
    name: str
    key_columns: tuple[str, ...]
    columns: tuple[str, ...]
    numeric_columns: frozenset[str] = frozenset()
    boolean_columns: frozenset[str] = frozenset()
    date_columns: frozenset[str] = frozenset()
    as_of_column: str | None = None


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
        "feature_name": "macro_level_and_delta",
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


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_source_dir() -> Path:
    return repo_root() / "fixtures" / "input_packs" / "p0_sources" / PROFILE_OPEN_MACRO_V03


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_as_of(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"--as-of must be YYYY-MM-DD, got {value!r}") from exc


def normalize_date(value: Any) -> str:
    if isinstance(value, dt.date):
        return value.isoformat()
    if not isinstance(value, str):
        raise ValueError(f"date value must be a string, got {type(value).__name__}")
    return dt.date.fromisoformat(value[:10]).isoformat()


def normalize_number(value: Any) -> float | int | None:
    if value is None:
        return None
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


def require_key_columns(row: Mapping[str, Any], spec: TableSpec, source_path: Path) -> None:
    missing = [column for column in spec.key_columns if column not in row or row[column] is None]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"{source_path.name}: {spec.name} row missing required key columns: {joined}")


def row_sort_key(row: Mapping[str, Any], spec: TableSpec) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in spec.key_columns)


def load_source_table(source_dir: Path, spec: TableSpec, as_of: dt.date) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    path = source_dir / f"{spec.name}.json"
    if not path.is_file():
        raise FileNotFoundError(f"missing P0 source snapshot: {path}")
    payload = read_json(path)
    if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
        raise ValueError(f"{path} must contain a JSON array of objects")

    raw_rows: list[dict[str, Any]] = []
    canonical_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for raw in payload:
        require_key_columns(raw, spec, path)
        if spec.as_of_column:
            observed = dt.date.fromisoformat(normalize_date(raw.get(spec.as_of_column)))
            if observed > as_of:
                continue
        canonical = normalize_row(raw, spec)
        key = row_sort_key(canonical, spec)
        if key in seen_keys:
            joined = ", ".join(f"{column}={value!r}" for column, value in zip(spec.key_columns, key))
            raise ValueError(f"{path.name}: {spec.name} duplicate natural key after as_of filter: {joined}")
        seen_keys.add(key)
        raw_rows.append({str(key): normalize_value(raw[key]) for key in sorted(raw)})
        canonical_rows.append(canonical)
    return (
        sorted(raw_rows, key=lambda row: row_sort_key(row, spec)),
        sorted(canonical_rows, key=lambda row: row_sort_key(row, spec)),
    )


def table_columns(rows: Sequence[Mapping[str, Any]], fallback: Sequence[str]) -> list[str]:
    if not rows:
        return list(fallback)
    columns: set[str] = set()
    for row in rows:
        columns.update(row.keys())
    return sorted(columns)


def artifact_entry(root: Path, rel_path: str, dataset_name: str, rows: int, columns: Sequence[str]) -> dict[str, Any]:
    return {
        "dataset_name": dataset_name,
        "path": rel_path,
        "sha256": file_sha256(root / rel_path),
        "rows": rows,
        "columns": list(columns),
    }


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


def copy_pack_schemas(output_dir: Path) -> list[dict[str, Any]]:
    schema_entries: list[dict[str, Any]] = []
    schema_dir = repo_root() / "schemas" / "input_packs"
    for source in sorted(schema_dir.glob("*.json")):
        target = output_dir / "schemas" / source.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        rel_path = target.relative_to(output_dir).as_posix()
        schema_entries.append(
            {
                "name": f"schema:{source.stem}",
                "path": rel_path,
                "rows": 0,
                "sha256": file_sha256(target),
            }
        )
    return schema_entries


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root(),
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "0" * 40


def contract_bundle_sha256() -> str:
    manifest = read_json(repo_root() / "contracts" / "quant-engine" / "v1" / "manifest.json")
    value = str(manifest["bundle_sha256"])
    return value.removeprefix("sha256:")


def builder_code_sha256() -> str:
    root = repo_root()
    files = [
        *sorted((root / "src" / "input_packs").glob("*.py")),
        *sorted((root / "schemas" / "input_packs").glob("*.json")),
    ]
    return canonical_json_sha256(
        {
            "builder_name": "certified-input-pack-builder-p0",
            "files": [
                {
                    "path": path.relative_to(root).as_posix(),
                    "sha256": file_sha256(path),
                }
                for path in files
            ],
        }
    )


def normalize_builder_image_digest(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    if not IMAGE_DIGEST_PATTERN.fullmatch(value):
        raise ValueError("builder image digest must match sha256:<64 lowercase hex chars>")
    return value


def reset_output_dir(output_dir: Path, *, force: bool) -> None:
    if output_dir.exists():
        if not force:
            raise FileExistsError(f"output directory already exists: {output_dir}; pass --force to overwrite")
        resolved = output_dir.resolve()
        root = repo_root().resolve()
        artifact_root = (root / "artifacts" / "input_packs").resolve()
        temp_root = Path(tempfile.gettempdir()).resolve()
        if _is_child_or_self(resolved, root):
            is_safe = _is_strict_child(resolved, artifact_root)
        else:
            is_safe = _is_strict_child(resolved, temp_root)
        if not is_safe:
            raise ValueError(
                "--force output must target a child of a safe artifact subtree "
                f"(artifacts/input_packs or system temp), got: {resolved}"
            )
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)


def _is_strict_child(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return path != root


def _is_child_or_self(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def write_snapshot_exports(
    output_dir: Path,
    source_dir: Path,
    as_of: dt.date,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    canonical: dict[str, list[dict[str, Any]]] = {}
    raw_artifacts: list[dict[str, Any]] = []
    canonical_artifacts: list[dict[str, Any]] = []

    for spec in P0_TABLE_SPECS:
        raw_rows, canonical_rows = load_source_table(source_dir, spec, as_of)
        canonical[spec.name] = canonical_rows

        raw_rel = f"data/raw/{spec.name}.json"
        canonical_rel = f"data/canonical/{spec.name}.json"
        write_json(output_dir / raw_rel, raw_rows)
        write_json(output_dir / canonical_rel, canonical_rows)
        raw_artifacts.append(
            artifact_entry(
                output_dir,
                raw_rel,
                f"raw_{spec.name}",
                len(raw_rows),
                table_columns(raw_rows, spec.columns),
            )
        )
        canonical_artifacts.append(
            artifact_entry(
                output_dir,
                canonical_rel,
                f"canonical_{spec.name}",
                len(canonical_rows),
                list(spec.columns),
            )
        )
    return canonical, raw_artifacts, canonical_artifacts


def write_derived_features(output_dir: Path, canonical: Mapping[str, Sequence[Mapping[str, Any]]]) -> list[dict[str, Any]]:
    outputs: tuple[tuple[str, str, list[dict[str, Any]]], ...] = (
        (
            "data/derived/fund_nav_return_features.json",
            "derived_fund_nav_return_features",
            derive_nav_features(canonical["nav_timeseries"]),
        ),
        (
            "data/derived/market_price_return_features.json",
            "derived_market_price_return_features",
            derive_price_features(canonical["eod_prices"]),
        ),
        (
            "data/derived/macro_observation_features.json",
            "derived_macro_observation_features",
            derive_macro_features(canonical["macro_data"]),
        ),
        (
            "data/derived/fund_universe_features.json",
            "derived_fund_universe_features",
            derive_universe_features(canonical),
        ),
        (
            "data/derived/holdings_summary_features.json",
            "derived_holdings_summary_features",
            derive_holdings_features(canonical["sec_nport_holdings"]),
        ),
        (
            "data/derived/flow_momentum_features.json",
            "derived_flow_momentum_features",
            derive_flow_features(canonical["sec_nport_fund_monthly_flows"]),
        ),
    )

    artifacts: list[dict[str, Any]] = []
    for rel_path, dataset_name, rows in outputs:
        write_json(output_dir / rel_path, rows)
        artifacts.append(
            artifact_entry(
                output_dir,
                rel_path,
                dataset_name,
                len(rows),
                table_columns(rows, ()),
            )
        )

    lineage_path = "data/derived/feature_lineage.json"
    write_json(output_dir / lineage_path, list(DERIVED_FEATURE_LINEAGE))
    artifacts.append(
        artifact_entry(
            output_dir,
            lineage_path,
            "derived_feature_lineage",
            len(DERIVED_FEATURE_LINEAGE),
            ("feature_file", "feature_name", "sources"),
        )
    )
    return artifacts


def write_component_manifests(
    output_dir: Path,
    *,
    as_of: dt.date,
    raw_artifacts: list[dict[str, Any]],
    canonical_artifacts: list[dict[str, Any]],
    derived_artifacts: list[dict[str, Any]],
) -> None:
    base = {"schema_version": INPUT_PACK_VERSION, "as_of": as_of.isoformat()}
    write_json(output_dir / "raw_snapshot_manifest.json", {**base, "snapshot_kind": "raw", "artifacts": raw_artifacts})
    write_json(
        output_dir / "canonical_snapshot_manifest.json",
        {**base, "snapshot_kind": "canonical", "artifacts": canonical_artifacts},
    )
    write_json(
        output_dir / "derived_feature_manifest.json",
        {
            **base,
            "snapshot_kind": "derived_feature",
            "artifacts": derived_artifacts,
            "lineage": list(DERIVED_FEATURE_LINEAGE),
        },
    )


def write_source_and_provenance(
    output_dir: Path,
    *,
    as_of: dt.date,
    source_commit: str,
    code_sha256: str,
    image_digest: str | None,
) -> None:
    source_payload: dict[str, Any] = {
        "builder_code_sha256": code_sha256,
        "builder_commit": source_commit,
        "builder_name": "certified-input-pack-builder-p0",
        "source_commit": source_commit,
        "source_repo": SOURCE_REPO,
    }
    if image_digest is not None:
        source_payload["builder_image_digest"] = image_digest
    write_json(
        output_dir / "SOURCE.json",
        source_payload,
    )
    snapshot_suffix = as_of.strftime("%Y%m%d")
    write_json(
        output_dir / "provenance.json",
        {
            "schema_version": INPUT_PACK_VERSION,
            "datasets": [
                {
                    "dataset_namespace": "lake://certified-input-packs/open_macro_v03/p0",
                    "dataset_name": spec.name,
                    "snapshot_id": f"{spec.name}_{snapshot_suffix}",
                }
                for spec in P0_TABLE_SPECS
            ],
            "jobs": [
                {
                    "job_namespace": SOURCE_REPO,
                    "job_name": "certified-input-pack-builder-p0",
                }
            ],
            "runs": [
                {
                    "job_name": "certified-input-pack-builder-p0",
                    "run_id": f"{P0_INPUT_PACK_ID}_{snapshot_suffix}",
                }
            ],
            "sources": [{"source_repo": SOURCE_REPO, "source_commit": source_commit}],
        },
    )


def table_hash_entries(output_dir: Path, artifacts: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for artifact in artifacts:
        rel_path = str(artifact["path"])
        entries.append(
            {
                "name": str(artifact.get("dataset_name") or artifact.get("name") or rel_path),
                "path": rel_path,
                "rows": int(artifact.get("rows", 0)),
                "sha256": file_sha256(output_dir / rel_path),
            }
        )
    return sorted(entries, key=lambda item: item["path"])


def write_report(output_dir: Path, *, as_of: dt.date, table_names: Sequence[str]) -> dict[str, Any]:
    summary = {
        "schema_version": INPUT_PACK_VERSION,
        "input_pack_id": P0_INPUT_PACK_ID,
        "profile": PROFILE_OPEN_MACRO_V03,
        "as_of": as_of.isoformat(),
        "runtime_activation": False,
        "engine_network_required": False,
        "official_source_tables": list(table_names),
        "excluded_as_official_inputs": [
            "fund_risk_metrics",
            "fund_risk_latest_mv",
            "funds_list_mv",
            "factor_model_fits",
            "fund_factor_exposures",
            "regime_composite_daily",
            "credit_regime_daily",
            "screener_equity_snapshot_mv",
        ],
    }
    write_json(output_dir / "reports" / "certification_summary.json", summary)
    return summary


def build_pack(
    *,
    profile: str,
    as_of: str,
    output: str | Path,
    source_dir: str | Path | None = None,
    builder_image_digest: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    if profile != PROFILE_OPEN_MACRO_V03:
        raise ValueError(f"unsupported input pack profile: {profile}")

    as_of_date = parse_as_of(as_of)
    output_dir = Path(output)
    source_root = Path(source_dir) if source_dir is not None else default_source_dir()
    reset_output_dir(output_dir, force=force)

    canonical, raw_artifacts, canonical_artifacts = write_snapshot_exports(output_dir, source_root, as_of_date)
    derived_artifacts = write_derived_features(output_dir, canonical)
    schema_entries = copy_pack_schemas(output_dir)
    report = write_report(output_dir, as_of=as_of_date, table_names=[spec.name for spec in P0_TABLE_SPECS])
    report_entry = {
        "name": "report:certification_summary",
        "path": "reports/certification_summary.json",
        "rows": 1,
        "sha256": file_sha256(output_dir / "reports" / "certification_summary.json"),
    }

    write_component_manifests(
        output_dir,
        as_of=as_of_date,
        raw_artifacts=raw_artifacts,
        canonical_artifacts=canonical_artifacts,
        derived_artifacts=derived_artifacts,
    )

    source_commit = git_commit()
    code_sha256 = builder_code_sha256()
    image_digest = normalize_builder_image_digest(
        builder_image_digest if builder_image_digest is not None else os.environ.get("INPUT_PACK_BUILDER_IMAGE_DIGEST")
    )
    write_source_and_provenance(
        output_dir,
        as_of=as_of_date,
        source_commit=source_commit,
        code_sha256=code_sha256,
        image_digest=image_digest,
    )
    all_artifacts = [
        *raw_artifacts,
        *canonical_artifacts,
        *derived_artifacts,
        *schema_entries,
        report_entry,
    ]
    write_json(
        output_dir / "table_hashes.json",
        {"schema_version": INPUT_PACK_VERSION, "tables": table_hash_entries(output_dir, all_artifacts)},
    )

    base_manifest = {
        "input_pack_id": P0_INPUT_PACK_ID,
        "input_pack_version": INPUT_PACK_VERSION,
        "as_of": as_of_date.isoformat(),
        "contract_bundle_sha256": contract_bundle_sha256(),
        "source_repo": SOURCE_REPO,
        "source_commit": source_commit,
        "builder_commit": source_commit,
        "builder_code_sha256": code_sha256,
        "raw_snapshot_sha256": "",
        "canonical_snapshot_sha256": "",
        "derived_feature_sha256": "",
        "input_pack_sha256": "",
        "runtime_activation": False,
    }
    if image_digest is not None:
        base_manifest["builder_image_digest"] = image_digest
    manifest_path = write_manifest(output_dir, base_manifest)
    manifest = read_json(manifest_path)
    verification = verify_pack(output_dir)
    if not verification["ok"]:
        raise ValueError(f"built input pack failed verification: {json.dumps(verification, sort_keys=True)}")
    return {
        "input_pack_id": manifest["input_pack_id"],
        "input_pack_sha256": manifest["input_pack_sha256"],
        "as_of": manifest["as_of"],
        "contract_bundle_sha256": manifest["contract_bundle_sha256"],
        "builder_commit": manifest["builder_commit"],
        "source_snapshot_sha256": canonical_json_sha256(
            {
                "raw_snapshot_sha256": manifest["raw_snapshot_sha256"],
                "canonical_snapshot_sha256": manifest["canonical_snapshot_sha256"],
            }
        ),
        "verified": True,
        "output": str(output_dir),
        "report": report,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Certified Input Packs")
    parser.add_argument("--profile", required=True, choices=[PROFILE_OPEN_MACRO_V03])
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--source-dir",
        default=None,
        help="Directory containing P0 source snapshot JSON files. Defaults to checked-in P0 fixtures.",
    )
    parser.add_argument(
        "--builder-image-digest",
        default=None,
        help=(
            "Optional real builder container digest, sha256:<64 hex>. "
            "If omitted, the pack records builder_code_sha256 instead of claiming an image digest."
        ),
    )
    parser.add_argument("--force", action="store_true", help="Overwrite an existing output directory inside this repo.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_pack(
        profile=args.profile,
        as_of=args.as_of,
        output=args.output,
        source_dir=args.source_dir,
        builder_image_digest=args.builder_image_digest,
        force=args.force,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""P0 Certified Input Pack builder.

The builder consumes local raw snapshot files and writes an offline pack that
the quant-engine can verify without database or network access.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .p0_derived import (
    DERIVED_FEATURE_LINEAGE,
    derive_flow_features,
    derive_holdings_features,
    derive_macro_features,
    derive_nav_features,
    derive_price_features,
    derive_universe_features,
)
from .hashing import canonical_json_sha256, file_sha256
from .manifest import build_manifest, write_manifest
from .p0_contract import (
    P0_TABLE_SPECS,
    TableSpec,
    normalize_date,
    normalize_row,
    normalize_value,
    row_sort_key,
)
from .verifier import verify_pack

PROFILE_OPEN_MACRO_V03 = "open_macro_v03"
P0_INPUT_PACK_ID = "open_macro_v03_certified_input_pack_001"
SOURCE_REPO = "investintell-datalake-workers"
INPUT_PACK_VERSION = "v1"
IMAGE_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


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


def require_key_columns(row: Mapping[str, Any], spec: TableSpec, source_path: Path) -> None:
    missing = [column for column in spec.key_columns if column not in row or row[column] is None]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"{source_path.name}: {spec.name} row missing required key columns: {joined}")

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


def canonical_text_file_sha256(path: Path) -> str:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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
                    "sha256": canonical_text_file_sha256(path),
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

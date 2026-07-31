"""P1 Certified Input Pack builder (``open_macro_v03_certified_input_pack_002``).

Pure file transformation over the committed P1 source snapshots. Reuses the
immutable P0 helpers (``src/input_packs/p0_contract.py``, ``hashing.py``,
``manifest.py``) by import. No database or network access.

Layout (P0-shaped, minus the derived layer which P1 does not have):

* ``manifest.json``           aggregate manifest + governance pins + sha tree
* ``SOURCE.json``             P1 export provenance carried through + builder provenance
* ``provenance.json``         datasets / jobs / runs / sources
* ``raw_snapshot_manifest.json`` / ``canonical_snapshot_manifest.json``
* ``table_hashes.json``       sha256 + rowcount per artifact
* ``data/raw/*.json`` and ``data/canonical/*.json``
* ``schemas/*.json``          embedded pack schemas (P1 variants)
* ``reports/certification_summary.json``

For P1, the canonical rows ARE the normalized raw rows: both ``data/raw`` and
``data/canonical`` hold the identical normalized, key-sorted rows. This is
recorded in the manifest (``raw_equals_canonical: true``) and enforced by the
verifier. There is no derived feature layer.

CLI::

    python -m harness.p1_pack.build \
        --sources fixtures/p1_sources/open_macro_v03 \
        --out fixtures/p1_packs/open_macro_v03_certified_input_pack_002
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from src.input_packs.hashing import canonical_json_sha256, file_sha256
from src.input_packs.manifest import build_manifest
from src.input_packs.p0_contract import (
    TableSpec,
    normalize_date,
    normalize_row,
    normalize_value,
    row_sort_key,
)
from src.input_packs.registry import P1_PROFILE, load_registry

from .contract import P1_TABLE_SPECS

PROFILE = "open_macro_v03"
SCHEMA_VERSION = "v2"
SOURCE_REPO = "investintell-datalake-workers"
BUILDER_NAME = "certified-input-pack-builder-p1"
DATASET_NAMESPACE = "lake://certified-input-packs/open_macro_v03/p1"

# Identity, contract binding, source export and governance stance come from the
# single certified-pack registry (contracts/input-packs/registry.json). The
# module constants below are the CURRENT entry's values — a default, not a pin
# that has to be retyped to renew a pack. Building a NEW pack passes the new
# identity on the CLI (--pack-id / --pack-version / --source-export-id /
# --db-source); `python -m src.input_packs.registry publish` then records what
# the builder stamped, and `... promote` makes it current. No source file is
# edited to renew a certification.
_REGISTRY = load_registry()
_CURRENT = _REGISTRY.current(P1_PROFILE)

INPUT_PACK_ID = _CURRENT.pack_id
INPUT_PACK_VERSION = _CURRENT.pack_version
REQUIRED_SOURCE_EXPORT_ID = _CURRENT.source_export_id
REQUIRED_DB_SOURCE = _CURRENT.db_source

# The contract bundle sha the current pack is certified under, recomputed live at
# build time from the bundle the registry names and cross-checked against it.
CONTRACT_BUNDLE_DIR = _CURRENT.contract_dir
CONTRACT_BUNDLE_SHA256 = _CURRENT.contract_bundle_sha256

# The corrected snapshot and pack-003 builder live at separate reachable
# commits. Keep their identities distinct instead of presenting the snapshot
# fallback as the builder commit.
SNAPSHOT_SOURCE_COMMIT = "e76d4822f09e8780eafed838bfdaf51c0b9750fc"
BUILDER_COMMIT = "3eab2bc6c10d6bf9d09a4028d203edd41f4de58d"

GOVERNANCE_PINS: dict[str, Any] = {
    **(_CURRENT.governance or {}),
    "status": "candidate_not_approved",
}

VERIFIER_DELTA_VS_P0 = (
    "The shared P0 verifier (src/input_packs/verifier.py verify_pack) is hard-wired "
    "to the nine P0 tables, input_pack_id open_macro_v03_certified_input_pack_001, "
    "P0 derived-feature recomputation and P0-specific provenance dataset naming, so "
    "it cannot validate this P1 pack. This pack is validated by "
    "harness/p1_pack/verifier.py verify_pack, which reuses the P0 hash-tree and "
    "row-normalization helpers unchanged and encodes the P1 table + governance "
    "contract. P1 has no derived feature layer (raw == canonical)."
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    # newline="" disables OS newline translation so the pack is byte-deterministic
    # (LF) on Windows and Linux alike; the aggregate/tree hashes re-parse JSON so
    # they are line-ending independent regardless.
    path.write_text(text, encoding="utf-8", newline="")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def require_key_columns(row: Mapping[str, Any], spec: TableSpec, source_path: Path) -> None:
    missing = [column for column in spec.key_columns if column not in row or row[column] is None]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"{source_path.name}: {spec.name} row missing required key columns: {joined}")


def load_source_table(source_dir: Path, spec: TableSpec, as_of: dt.date) -> list[dict[str, Any]]:
    """Load one P1 source snapshot, normalize, as_of-filter and key-sort.

    For P1 the raw and canonical rows are identical: both are the normalized rows.
    """
    path = source_dir / f"{spec.name}.json"
    if not path.is_file():
        raise FileNotFoundError(f"missing P1 source snapshot: {path}")
    payload = read_json(path)
    if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
        raise ValueError(f"{path} must contain a JSON array of objects")

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
        canonical_rows.append(canonical)
    return sorted(canonical_rows, key=lambda row: row_sort_key(row, spec))


def artifact_entry(root: Path, rel_path: str, dataset_name: str, rows: int, columns: Sequence[str]) -> dict[str, Any]:
    return {
        "dataset_name": dataset_name,
        "path": rel_path,
        "sha256": file_sha256(root / rel_path),
        "rows": rows,
        "columns": list(columns),
    }


def canonical_text_file_sha256(path: Path) -> str:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def builder_code_sha256(commit: str = BUILDER_COMMIT) -> str:
    """Hash the committed P1 builder package identified by ``commit``."""
    root = repo_root()
    paths = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", commit, "--", "harness/p1_pack"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    files = sorted(
        path
        for path in paths
        if Path(path).parent.as_posix() == "harness/p1_pack" and path.endswith(".py")
    )
    if not files:
        raise ValueError(f"builder commit {commit!r} contains no P1 builder package files")
    return canonical_json_sha256(
        {
            "builder_name": BUILDER_NAME,
            "files": [
                {
                    "path": path,
                    "sha256": hashlib.sha256(
                        subprocess.run(
                            ["git", "show", f"{commit}:{path}"],
                            cwd=root,
                            check=True,
                            capture_output=True,
                        ).stdout.decode("utf-8").replace("\r\n", "\n").encode("utf-8")
                    ).hexdigest(),
                }
                for path in files
            ],
        }
    )


def pack_schemas_dir() -> Path:
    return Path(__file__).resolve().parent / "schemas"


def contract_bundle_sha256(
    contract_dir: str | None = None, *, expected: str | None = None
) -> str:
    """Recompute the bundle sha from the committed manifest and cross-check it.

    The bundle DIRECTORY and the expected sha both come from the pack registry
    entry, so certifying a pack against a newer contract is a registry
    publication rather than an edit here. The cross-check itself is unchanged:
    the value the pack records must be the value the committed bundle hashes to.
    """
    directory = contract_dir or CONTRACT_BUNDLE_DIR
    expected = expected if expected is not None else CONTRACT_BUNDLE_SHA256
    manifest = read_json(repo_root() / directory / "manifest.json")
    value = str(manifest["bundle_sha256"]).removeprefix("sha256:")
    if value != expected:
        raise ValueError(
            f"contract bundle sha mismatch for {directory}: manifest {value!r} != "
            f"registry {expected!r}"
        )
    return value


def copy_pack_schemas(output_dir: Path) -> list[dict[str, Any]]:
    schema_entries: list[dict[str, Any]] = []
    for source in sorted(pack_schemas_dir().glob("*.json")):
        target = output_dir / "schemas" / source.name
        target.parent.mkdir(parents=True, exist_ok=True)
        # Normalize line endings so the committed pack is deterministic across OSes.
        text = source.read_text(encoding="utf-8").replace("\r\n", "\n")
        target.write_text(text, encoding="utf-8", newline="")
        rel_path = target.relative_to(output_dir).as_posix()
        schema_entries.append(
            {"name": f"schema:{source.stem}", "path": rel_path, "rows": 0, "sha256": file_sha256(target)}
        )
    return schema_entries


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


def _source_export(source_dir: Path) -> dict[str, Any]:
    export = read_json(source_dir / "SOURCE.json")
    if not isinstance(export, dict):
        raise ValueError("P1 source SOURCE.json must be a JSON object")
    return export


def _validate_source_export_identity(
    export: Mapping[str, Any],
    *,
    required_export_id: str | None = None,
    required_db_source: str | None = None,
) -> None:
    """The export snapshot must be the one the target pack entry declares."""
    required_export_id = required_export_id or REQUIRED_SOURCE_EXPORT_ID
    required_db_source = required_db_source or REQUIRED_DB_SOURCE
    export_id = str(export.get("export_id") or "")
    if export_id != required_export_id:
        raise ValueError(
            f"this pack requires source export {required_export_id!r}; got {export_id!r}"
        )
    db_source = str(export.get("db_source") or "")
    if db_source != required_db_source:
        raise ValueError(
            f"this pack requires the source {required_db_source!r}; got {db_source!r}"
        )


def _validate_source_export_tables(source_dir: Path, export: Mapping[str, Any]) -> None:
    tables = export.get("tables")
    if not isinstance(tables, list):
        raise ValueError("P1 source SOURCE.json tables must be a JSON array")

    entries: dict[str, Mapping[str, Any]] = {}
    for index, entry in enumerate(tables):
        if not isinstance(entry, Mapping):
            raise ValueError(f"P1 source SOURCE.json tables[{index}] must be a JSON object")
        table = entry.get("table")
        if not isinstance(table, str) or not table:
            raise ValueError(f"P1 source SOURCE.json tables[{index}] must name a table")
        if table in entries:
            raise ValueError(f"P1 source SOURCE.json has duplicate table provenance for {table}")
        entries[table] = entry

    for spec in P1_TABLE_SPECS:
        entry = entries.get(spec.name)
        if entry is None:
            raise ValueError(f"P1 source SOURCE.json is missing table provenance for {spec.name}")

        path = source_dir / f"{spec.name}.json"
        if not path.is_file():
            raise FileNotFoundError(f"missing P1 source snapshot: {path}")
        file_bytes = path.read_bytes()
        # The exporter hashes its canonical LF bytes. Git may materialize tracked
        # JSON with CRLF under core.autocrlf on Windows, so restore those canonical
        # bytes without otherwise reserializing or forgiving byte-level edits.
        canonical_file_bytes = file_bytes.replace(b"\r\n", b"\n")
        actual_sha256 = hashlib.sha256(canonical_file_bytes).hexdigest()
        expected_sha256 = entry.get("sha256")
        if expected_sha256 != actual_sha256:
            raise ValueError(
                f"{spec.name}: SOURCE.json sha256 {expected_sha256!r} "
                f"does not match source file sha256 {actual_sha256!r}"
            )

        payload = json.loads(file_bytes.decode("utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"{path} must contain a JSON array")
        actual_row_count = len(payload)
        expected_row_count = entry.get("row_count")
        if expected_row_count != actual_row_count:
            raise ValueError(
                f"{spec.name}: SOURCE.json row_count {expected_row_count!r} "
                f"does not match source file row_count {actual_row_count}"
            )


def build_pack(
    *,
    sources: str | Path,
    out: str | Path,
    pack_id: str | None = None,
    pack_version: int | None = None,
    source_export_id: str | None = None,
    db_source: str | None = None,
    contract_dir: str | None = None,
    governance: dict[str, Any] | None = None,
    builder_commit: str | None = None,
) -> dict[str, Any]:
    """Build a P1 pack.

    Every identity argument defaults to the registry CURRENT entry, so rebuilding
    the committed pack reproduces it byte-for-byte. Building a NEW pack passes the
    new identity here instead of editing this module; the pack is then published
    and promoted through ``src.input_packs.registry``.
    """
    pack_id = pack_id or INPUT_PACK_ID
    pack_version = pack_version if pack_version is not None else INPUT_PACK_VERSION
    required_export_id = source_export_id or REQUIRED_SOURCE_EXPORT_ID
    required_db_source = db_source or REQUIRED_DB_SOURCE
    contract_dir = contract_dir or CONTRACT_BUNDLE_DIR
    governance_pins = dict(governance) if governance is not None else dict(GOVERNANCE_PINS)
    builder_commit = builder_commit or BUILDER_COMMIT

    source_dir = Path(sources)
    output_dir = Path(out)

    export = _source_export(source_dir)
    _validate_source_export_identity(
        export, required_export_id=required_export_id, required_db_source=required_db_source
    )
    _validate_source_export_tables(source_dir, export)
    as_of_str = str(export["as_of"])
    as_of_date = dt.date.fromisoformat(as_of_str)
    export_id = str(export["export_id"])
    snapshot_suffix = as_of_date.strftime("%Y%m%d")

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)

    # --- data + snapshot manifests ---------------------------------------
    raw_artifacts: list[dict[str, Any]] = []
    canonical_artifacts: list[dict[str, Any]] = []
    for spec in P1_TABLE_SPECS:
        rows = load_source_table(source_dir, spec, as_of_date)
        raw_rel = f"data/raw/{spec.name}.json"
        canonical_rel = f"data/canonical/{spec.name}.json"
        # raw == canonical for P1: identical normalized, key-sorted rows.
        write_json(output_dir / raw_rel, rows)
        write_json(output_dir / canonical_rel, rows)
        raw_artifacts.append(
            artifact_entry(output_dir, raw_rel, f"raw_{spec.name}", len(rows), list(spec.columns))
        )
        canonical_artifacts.append(
            artifact_entry(output_dir, canonical_rel, f"canonical_{spec.name}", len(rows), list(spec.columns))
        )

    base = {"schema_version": SCHEMA_VERSION, "as_of": as_of_str}
    write_json(
        output_dir / "raw_snapshot_manifest.json",
        {**base, "snapshot_kind": "raw", "artifacts": raw_artifacts},
    )
    write_json(
        output_dir / "canonical_snapshot_manifest.json",
        {**base, "snapshot_kind": "canonical", "artifacts": canonical_artifacts},
    )

    # --- schemas + report ------------------------------------------------
    schema_entries = copy_pack_schemas(output_dir)
    report = {
        "schema_version": SCHEMA_VERSION,
        "input_pack_id": pack_id,
        "profile": PROFILE,
        "as_of": as_of_str,
        "engine_network_required": False,
        "official_source_tables": [spec.name for spec in P1_TABLE_SPECS],
        "raw_equals_canonical": True,
        "has_derived_layer": False,
        **governance_pins,
    }
    write_json(output_dir / "reports" / "certification_summary.json", report)
    report_entry = {
        "name": "report:certification_summary",
        "path": "reports/certification_summary.json",
        "rows": 1,
        "sha256": file_sha256(output_dir / "reports" / "certification_summary.json"),
    }

    # --- SOURCE.json: carry P1 export provenance through + builder provenance ---
    code_sha256 = builder_code_sha256()
    source_commit = str(export.get("source_commit") or SNAPSHOT_SOURCE_COMMIT)
    source_payload = {
        "builder_code_sha256": code_sha256,
        "builder_commit": builder_commit,
        "builder_name": BUILDER_NAME,
        "source_repo": SOURCE_REPO,
        "source_commit": source_commit,
        "p1_export": export,
    }
    write_json(output_dir / "SOURCE.json", source_payload)

    # --- provenance ------------------------------------------------------
    write_json(
        output_dir / "provenance.json",
        {
            "schema_version": SCHEMA_VERSION,
            "datasets": [
                {
                    "dataset_namespace": DATASET_NAMESPACE,
                    "dataset_name": spec.name,
                    "snapshot_id": f"{spec.name}_{snapshot_suffix}",
                }
                for spec in P1_TABLE_SPECS
            ],
            "jobs": [{"job_namespace": SOURCE_REPO, "job_name": BUILDER_NAME}],
            "runs": [
                {
                    "job_name": BUILDER_NAME,
                    "run_id": f"{pack_id}_{snapshot_suffix}",
                    "export_id": export_id,
                }
            ],
            "sources": [{"source_repo": SOURCE_REPO, "source_commit": source_commit}],
        },
    )

    # --- table_hashes ----------------------------------------------------
    all_artifacts = [*raw_artifacts, *canonical_artifacts, *schema_entries, report_entry]
    write_json(
        output_dir / "table_hashes.json",
        {"schema_version": SCHEMA_VERSION, "tables": table_hash_entries(output_dir, all_artifacts)},
    )

    # --- manifest --------------------------------------------------------
    base_manifest = {
        "input_pack_id": pack_id,
        "input_pack_version": pack_version,
        "as_of": as_of_str,
        "contract_bundle_sha256": contract_bundle_sha256(contract_dir),
        "source_repo": SOURCE_REPO,
        "source_commit": source_commit,
        "builder_commit": builder_commit,
        "builder_code_sha256": code_sha256,
        "source_export_id": export_id,
        "raw_snapshot_sha256": "",
        "canonical_snapshot_sha256": "",
        "raw_equals_canonical": True,
        "has_derived_layer": False,
        "verifier": "harness.p1_pack.verifier",
        "verifier_delta_vs_p0": VERIFIER_DELTA_VS_P0,
        "input_pack_sha256": "",
        **governance_pins,
    }
    # build_manifest fills raw/canonical/derived component sha fields (only the
    # ones whose files exist), forces runtime_activation False, and computes the
    # aggregate input_pack_sha256. P1 has no derived_feature_manifest.json, so
    # derived_feature_sha256 is intentionally absent.
    manifest = build_manifest(output_dir, base_manifest)
    write_json(output_dir / "manifest.json", manifest)

    return {
        "input_pack_id": manifest["input_pack_id"],
        "input_pack_version": manifest["input_pack_version"],
        "input_pack_sha256": manifest["input_pack_sha256"],
        "as_of": manifest["as_of"],
        "contract_bundle_sha256": manifest["contract_bundle_sha256"],
        "raw_snapshot_sha256": manifest["raw_snapshot_sha256"],
        "canonical_snapshot_sha256": manifest["canonical_snapshot_sha256"],
        "builder_commit": manifest["builder_commit"],
        "builder_code_sha256": manifest["builder_code_sha256"],
        "source_export_id": manifest["source_export_id"],
        "output": str(output_dir),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a P1 certified input pack. Identity defaults to the registry "
            "current entry; pass the overrides to build a renewal, then publish and "
            "promote it with `python -m src.input_packs.registry`."
        )
    )
    parser.add_argument("--sources", required=True, help="Directory with P1 source snapshot JSON files.")
    parser.add_argument("--out", required=True, help="Output pack directory.")
    parser.add_argument("--pack-id", default=None, help=f"Pack id (default: {INPUT_PACK_ID}).")
    parser.add_argument("--pack-version", type=int, default=None, help="Pack version integer.")
    parser.add_argument("--source-export-id", default=None, help="Required export_id of the source snapshot.")
    parser.add_argument("--db-source", default=None, help="Required db_source of the source snapshot.")
    parser.add_argument(
        "--contract-dir",
        default=None,
        help=f"Contract bundle directory to certify against (default: {CONTRACT_BUNDLE_DIR}).",
    )
    parser.add_argument("--builder-commit", default=None, help="Commit whose builder source is hashed.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_pack(
        sources=args.sources,
        out=args.out,
        pack_id=args.pack_id,
        pack_version=args.pack_version,
        source_export_id=args.source_export_id,
        db_source=args.db_source,
        contract_dir=args.contract_dir,
        builder_commit=args.builder_commit,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Fast, restartable two-stage loader for the Phase 4 SEC corpus.

``prepare`` is intentionally file-only: ten CPU processes parse frozen SEC
packages into immutable, compressed JSONL payloads.  ``load`` is the only
command that opens a database connection; it creates the normal manifest rows,
COPYs the prepared raw rows, and invokes the existing reconciliation contract.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

DEFAULT_WORKERS = 10
RAW_TABLES = {"nport": "nport_raw_rows", "ncen": "ncen_raw_v2_rows", "rr1": "rr1_raw_v2_rows"}
PARSER_VERSIONS = {"nport": "nport-v1", "ncen": "ncen-v2", "rr1": "rr1-v2"}
ROOT_NAMES = {"nport": "nport", "ncen": "ncen", "rr1": "RR1"}


class FastBackfillError(RuntimeError):
    pass


@dataclass(frozen=True)
class ParseTask:
    form: str
    root: str
    package: str
    relative_package_path: str
    output: str
    inventory_files: tuple[tuple[str, str, int], ...] = ()
    expected_package_sha256: str | None = None


@dataclass(frozen=True)
class PreparedPackage:
    identity: str
    form: str
    quarter: str
    relative_package_path: str
    package_sha256: str
    metadata_sha256: str
    readme_sha256: str
    parser_version: str
    raw_table: str
    files: tuple[dict[str, Any], ...]
    rows: tuple[dict[str, Any], ...]
    explicit_zero_tables: tuple[str, ...] = ()
    metadata_filename: str = ""
    metadata_byte_size: int = 0


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_name(identity: str) -> str:
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _open_payload(path: Path) -> Iterator[str]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            yield from handle
        return
    zstd = shutil.which("zstd")
    if not zstd:
        raise FastBackfillError("prepared zstd payload requires external zstd on PATH")
    process = subprocess.Popen([zstd, "-q", "-d", "-c", str(path)], stdout=subprocess.PIPE, text=True, encoding="utf-8")
    assert process.stdout is not None
    try:
        yield from process.stdout
    finally:
        process.stdout.close()
        if process.wait() != 0:
            raise FastBackfillError(f"unable to decompress prepared payload: {path}")


def _write_json(path: Path, value: object) -> None:
    data = (_canonical(value) + "\n").encode("utf-8")
    if path.exists():
        if path.read_bytes() != data:
            raise FastBackfillError(f"immutable artifact conflicts: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_bytes(data)
    try:
        temporary.replace(path)
    except FileExistsError:
        temporary.unlink(missing_ok=True)
        if path.read_bytes() != data:
            raise FastBackfillError(f"immutable artifact conflicts: {path}")


def _payload_record(row: dict[str, Any]) -> bytes:
    return (_canonical(row) + "\n").encode("utf-8")


def _write_payload(directory: Path, rows: Iterable[dict[str, Any]]) -> tuple[Path, int]:
    """Compress directly from parser output; never materialize a raw payload on disk."""
    zstd = shutil.which("zstd")
    payload = directory / ("payload.jsonl.zst" if zstd else "payload.jsonl.gz")
    temporary = payload.with_name(payload.name + f".{os.getpid()}.tmp")
    count = 0
    try:
        if zstd:
            process = subprocess.Popen(
                [zstd, "-q", "-1", "-T1", "-f", "-o", str(temporary), "-"], stdin=subprocess.PIPE,
            )
            assert process.stdin is not None
            try:
                for row in rows:
                    process.stdin.write(_payload_record(row))
                    count += 1
            finally:
                process.stdin.close()
            if process.wait() != 0:
                raise FastBackfillError("zstd failed while preparing payload")
        else:
            with temporary.open("wb") as binary_handle:
                with gzip.GzipFile(filename="", mode="wb", fileobj=binary_handle, mtime=0) as handle:
                    for row in rows:
                        handle.write(_payload_record(row))
                        count += 1
        if payload.exists():
            if _sha256_file(payload) != _sha256_file(temporary):
                raise FastBackfillError(f"immutable payload conflicts: {payload}")
            temporary.unlink()
        else:
            temporary.replace(payload)
    finally:
        temporary.unlink(missing_ok=True)
    return payload, count


def _manifest_entry(prepared: PreparedPackage, payload: Path, output: Path) -> dict[str, Any]:
    relative_payload = payload.relative_to(output).as_posix()
    return {
        "identity": prepared.identity,
        "form": prepared.form,
        "quarter": prepared.quarter,
        "relative_package_path": prepared.relative_package_path,
        "package_sha256": prepared.package_sha256,
        "metadata_sha256": prepared.metadata_sha256,
        "readme_sha256": prepared.readme_sha256,
        "metadata_filename": prepared.metadata_filename,
        "metadata_byte_size": prepared.metadata_byte_size,
        "parser_version": prepared.parser_version,
        "raw_table": prepared.raw_table,
        "payload_path": relative_payload,
        "payload_sha256": _sha256_file(payload),
        "payload_byte_size": payload.stat().st_size,
        "row_count": len(prepared.rows),
        "files": list(prepared.files),
        "explicit_zero_tables": list(prepared.explicit_zero_tables),
    }


def _write_prepared_package(prepared: PreparedPackage, output: Path) -> dict[str, Any]:
    """Write an immutable prepared package and refresh its canonical root manifest."""
    directory = output / "packages" / _safe_name(prepared.identity)
    directory.mkdir(parents=True, exist_ok=True)
    payload, _ = _write_payload(directory, prepared.rows)
    entry = _manifest_entry(prepared, payload, output)
    _write_json(directory / "package.json", entry)
    manifest_path = output / "prepared-manifest.json"
    current = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {"schema_version": 1, "packages": []}
    entries = {item["identity"]: item for item in current["packages"]}
    entries[prepared.identity] = entry
    manifest = {"schema_version": 1, "packages": [entries[key] for key in sorted(entries)]}
    _write_json(manifest_path, manifest)
    return entry


def _adapter(
    form: str,
    package: Path,
    governed_file_hashes: Mapping[str, str] | None = None,
) -> tuple[Any, Any, Any, Any, str, tuple[str, ...], str, int]:
    """Return verified tables plus the family parser primitives without DB access."""
    if form == "nport":
        from src.nport.ingestion import source_quarter_from_package
        from src.nport.schema import json_typed_projection, load_nport_contract, parse_row, verify_package

        contract = load_nport_contract()
        verified = verify_package(package, contract, governed_file_hashes=governed_file_hashes)
        zeros = tuple(table.source_file for table in contract.tables if table.source_file not in verified.file_hashes)
        return verified, contract.tables, parse_row, json_typed_projection, source_quarter_from_package(package), zeros, "nport_metadata.json", (package / "nport_metadata.json").stat().st_size
    if form == "ncen":
        from src.ncen.ingestion import source_quarter_from_package
        from src.ncen.schema import json_typed_projection, parse_row, verify_package
        verified = verify_package(package)
        return verified, verified.contract.tables, parse_row, json_typed_projection, source_quarter_from_package(package), verified.explicit_zero_tables, "ncen_metadata.json", (package / "ncen_metadata.json").stat().st_size
    if form == "rr1":
        from src.rr1.ingestion import source_quarter_from_package
        from src.rr1.schema import (
            DATA_FILES,
            VerifiedPackage,
            json_typed_projection,
            load_rr1_contract,
            parse_row,
            verify_package,
        )

        if governed_file_hashes is None:
            verified = verify_package(package)
        else:
            metadata_names = sorted(name for name in governed_file_hashes if name.endswith("-metadata.json"))
            if len(metadata_names) != 1 or "readme.htm" not in governed_file_hashes:
                raise FastBackfillError(f"RR1 inventory metadata is incomplete: {package}")
            metadata_filename = metadata_names[0]
            metadata_hash = governed_file_hashes[metadata_filename]
            contract = load_rr1_contract(metadata_hash)
            actual = {path.name for path in package.iterdir() if path.is_file() and path.suffix.lower() == ".tsv"}
            if actual != DATA_FILES or actual != contract.required_filenames:
                raise FastBackfillError(f"RR1 physical file set changed: {package}")
            file_hashes: dict[str, str] = {}
            from src.rr1.tsv import stream_tsv

            for table in contract.tables:
                expected_sha256 = governed_file_hashes.get(table.source_file)
                if not expected_sha256:
                    raise FastBackfillError(f"RR1 inventory hash missing: {package / table.source_file}")
                header, rows = stream_tsv(package / table.source_file)
                try:
                    if header != table.headers:
                        raise FastBackfillError(f"RR1 header changed: {package / table.source_file}")
                finally:
                    rows.close()
                file_hashes[table.source_file] = expected_sha256
            verified = VerifiedPackage(
                contract,
                file_hashes,
                metadata_hash,
                governed_file_hashes["readme.htm"],
                metadata_filename,
            )
        return verified, verified.contract.tables, parse_row, json_typed_projection, source_quarter_from_package(package), (), verified.metadata_filename, (package / verified.metadata_filename).stat().st_size
    raise FastBackfillError(f"unsupported SEC form: {form}")


def _package_digest(form: str, verified: Any) -> str:
    if form == "nport":
        from src.nport.schema import package_sha256
        return package_sha256(verified.file_hashes, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256)
    if form == "ncen":
        return verified.package_sha256
    from src.rr1.schema import package_sha256
    return package_sha256(verified.file_hashes, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256, metadata_filename=verified.metadata_filename)


def _row_extras(form: str, table: Any, lexical: dict[str, str]) -> dict[str, Any]:
    if form == "nport":
        return {"accession_number": lexical.get("ACCESSION_NUMBER") or None, "holding_id": lexical.get("HOLDING_ID") or None}
    if form == "ncen":
        from src.ncen.ingestion import _relationship_keys
        entity_key, parent_key = _relationship_keys(table, lexical)
        return {
            "accession_number": lexical.get("ACCESSION_NUMBER") or None, "fund_id": lexical.get("FUND_ID") or None,
            "director_seqnum": lexical.get("DIRECTOR_SEQNUM") or None, "line_of_credit_seqnum": lexical.get("LINE_OF_CREDIT_SEQNUM") or None,
            "valuation_method_change_seqnum": lexical.get("VALUATION_METHOD_CHANGE_SEQNUM") or None,
            "entity_key": entity_key, "parent_key": parent_key,
        }
    return {name: lexical.get(name) or None for name in ("adsh", "tag", "version", "ddate", "series", "class", "measure", "document", "otherdims", "iprx")}


def _parse_task(task: ParseTask) -> dict[str, Any]:
    package = Path(task.package)
    output = Path(task.output)
    match = re.search(r"(20\d{2})q([1-4])", Path(task.relative_package_path).name, re.IGNORECASE)
    if match is not None:
        identity = f"{task.form}:{match.group(1)}Q{match.group(2)}:{task.relative_package_path}"
        completed = output / "packages" / _safe_name(identity) / "package.json"
        if completed.is_file():
            entry = json.loads(completed.read_text(encoding="utf-8"))
            payload = output / entry.get("payload_path", "")
            if entry.get("identity") == identity and payload.is_file() and _sha256_file(payload) == entry.get("payload_sha256"):
                return entry
    governed_file_hashes = {name: sha256 for name, sha256, _byte_size in task.inventory_files} or None
    verified, tables, parse_row, typed_projection, quarter, explicit_zero_tables, metadata_filename, metadata_size = _adapter(
        task.form,
        package,
        governed_file_hashes,
    )
    package_sha = _package_digest(task.form, verified)
    if task.expected_package_sha256 is not None and package_sha != task.expected_package_sha256:
        raise FastBackfillError(f"inventory package digest mismatch: {task.relative_package_path}")
    identity = f"{task.form}:{quarter}:{task.relative_package_path}"
    directory = output / "packages" / _safe_name(identity)
    directory.mkdir(parents=True, exist_ok=True)
    files: list[dict[str, Any]] = []

    def rows() -> Iterator[dict[str, Any]]:
        for table in sorted(tables, key=lambda item: item.source_file):
            if table.source_file not in verified.file_hashes:
                continue
            from src.rr1.tsv import stream_tsv as rr1_stream_tsv
            from src.sec_regulatory.tsv import stream_tsv as generic_stream_tsv
            stream = rr1_stream_tsv if task.form == "rr1" else generic_stream_tsv
            path = package / table.source_file
            header, values = stream(path, expected_sha256=verified.file_hashes[table.source_file])
            if header != table.headers:
                raise FastBackfillError(f"header changed after package verification: {path}")
            lexical_count = typed_count = quarantine_count = 0
            try:
                for row_number, lexical_values in values:
                    parsed = parse_row(table.columns, lexical_values)
                    lexical_count += 1
                    typed_count += parsed.parse_status == "typed"
                    quarantine_count += parsed.parse_status == "quarantined"
                    yield {
                        "source_table": table.source_file, "source_sha256": verified.file_hashes[table.source_file],
                        "source_row_number": row_number, "original_lexical_row": _canonical(parsed.lexical),
                        "typed_projection": _canonical(typed_projection(parsed.typed)), "parse_status": parsed.parse_status,
                        "parse_errors": _canonical([asdict(issue) for issue in parsed.issues]),
                        "candidate_key_evidence": _canonical(parsed.candidate_key_evidence), **_row_extras(task.form, table, parsed.lexical),
                    }
            finally:
                close = getattr(values, "close", None)
                if close:
                    close()
            files.append({"source_table": table.source_file, "source_sha256": verified.file_hashes[table.source_file],
                          "byte_size": path.stat().st_size, "headers": list(table.headers), "expected_count": lexical_count,
                          "data_count": lexical_count, "lexical_count": lexical_count, "typed_success_count": typed_count,
                          "quarantine_count": quarantine_count, "reject_count": 0})

    payload, row_count = _write_payload(directory, rows())
    entry = {
        "identity": identity, "form": task.form, "quarter": quarter, "relative_package_path": task.relative_package_path,
        "package_sha256": package_sha, "metadata_sha256": verified.metadata_sha256, "readme_sha256": verified.readme_sha256,
        "metadata_filename": metadata_filename, "metadata_byte_size": metadata_size, "parser_version": PARSER_VERSIONS[task.form],
        "raw_table": RAW_TABLES[task.form], "payload_path": payload.relative_to(output).as_posix(),
        "payload_sha256": _sha256_file(payload), "payload_byte_size": payload.stat().st_size, "row_count": row_count,
        "files": files, "explicit_zero_tables": list(explicit_zero_tables),
    }
    _write_json(directory / "package.json", entry)
    return entry


def _discover_tasks(root: Path, inventory: Path | None, forms: Sequence[str], identities: Sequence[str], output: Path) -> list[ParseTask]:
    selected_forms = set(forms)
    selected_identities = set(identities)
    tasks: list[ParseTask] = []
    if inventory:
        data = json.loads(inventory.read_text(encoding="utf-8"))
        inventory_data = data.get("inventory", data)
        if not isinstance(inventory_data, dict):
            raise FastBackfillError("inventory must be an object")
        packages = inventory_data.get("packages", [])
        roots = data.get("roots", inventory_data.get("roots", {}))
        root_by_form = roots if isinstance(roots, dict) else {}
        if isinstance(roots, list):
            root_by_form = {
                item["form"]: item["root"] for item in roots
                if isinstance(item, dict) and isinstance(item.get("form"), str) and isinstance(item.get("root"), str)
            }
        if not isinstance(packages, list):
            raise FastBackfillError("inventory packages must be a list")
        for item in packages:
            form = item.get("form")
            identity = item.get("identity")
            relative = item.get("relative_package_path")
            if form not in selected_forms or (selected_identities and identity not in selected_identities):
                continue
            if not isinstance(form, str) or not isinstance(relative, str):
                raise FastBackfillError("inventory package missing form or relative_package_path")
            configured_root = root_by_form.get(form)
            source_root = Path(configured_root) if isinstance(configured_root, str) else root / ROOT_NAMES[form]
            package_path = item.get("package_path") or item.get("absolute_package_path")
            package = Path(package_path) if isinstance(package_path, str) else source_root / relative
            inventory_files = tuple(
                (file_entry["relative_path"], file_entry["sha256"], file_entry["byte_count"])
                for file_entry in item.get("files", [])
                if isinstance(file_entry, dict)
                and isinstance(file_entry.get("relative_path"), str)
                and isinstance(file_entry.get("sha256"), str)
                and isinstance(file_entry.get("byte_count"), int)
            )
            expected_package_sha256 = item.get("package_sha256")
            tasks.append(ParseTask(
                form,
                str(source_root),
                str(package),
                relative,
                str(output),
                inventory_files,
                expected_package_sha256 if isinstance(expected_package_sha256, str) else None,
            ))
    else:
        for form in sorted(selected_forms):
            source_root = root / ROOT_NAMES[form]
            if not source_root.is_dir():
                raise FastBackfillError(f"source root unavailable: {source_root}")
            for package in sorted((path for path in source_root.iterdir() if path.is_dir()), key=lambda path: path.name.casefold()):
                relative = package.relative_to(source_root).as_posix()
                tasks.append(ParseTask(form, str(source_root), str(package), relative, str(output)))
    return tasks


def prepare(*, root: Path, output: Path, workers: int = DEFAULT_WORKERS, forms: Sequence[str] = tuple(RAW_TABLES), identities: Sequence[str] = (), inventory: Path | None = None) -> dict[str, Any]:
    if workers < 1:
        raise FastBackfillError("workers must be positive")
    tasks = _discover_tasks(root, inventory, forms, identities, output)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        entries = list(pool.map(_parse_task, tasks))
    manifest = {"schema_version": 1, "packages": sorted(entries, key=lambda item: item["identity"])}
    _write_json(output / "prepared-manifest.json", manifest)
    return {"state": "prepared", "packages": len(entries), "manifest": str(output / "prepared-manifest.json")}


def _connect(dsn: str):
    import psycopg
    return psycopg.connect(dsn)


def _raw_columns(form: str) -> tuple[str, ...]:
    common = ("ingestion_run_id", "source_file_id", "source_row_number", "source_sha256", "parser_version", "source_table", "original_lexical_row", "typed_projection", "parse_status", "parse_errors", "candidate_key_evidence")
    return common + ({"nport": ("accession_number", "holding_id"), "ncen": ("accession_number", "fund_id", "director_seqnum", "line_of_credit_seqnum", "valuation_method_change_seqnum", "entity_key", "parent_key"), "rr1": ("adsh", "tag", "version", "ddate", "series", "class", "measure", "document", "otherdims", "iprx")}[form])


def _prepared_json(value: object) -> str:
    """Accept legacy dict payload rows while loading new pre-canonicalized rows."""
    return value if isinstance(value, str) else _canonical(value)


def _load_package(conn: Any, output: Path, entry: dict[str, Any]) -> str:
    from src.sec_regulatory.manifests import create_or_resume_run, register_file, register_package_discovery, register_table_reconciliation, transition_run, validate_raw_run
    form = entry["form"]
    register_package_discovery(conn, source_family=form, source_quarter=entry["quarter"], package_relative_path=entry["relative_package_path"], package_sha256=entry["package_sha256"], metadata_sha256=entry["metadata_sha256"], readme_sha256=entry["readme_sha256"], package_state="discovered")
    run = create_or_resume_run(conn, source_family=form, package_sha256=entry["package_sha256"], parser_version=entry["parser_version"], source_quarter=entry["quarter"], package_relative_path=entry["relative_package_path"])
    if run.current_state == "raw_validated":
        return "raw_validated"
    if run.current_state == "discovered":
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
    if run.current_state != "loading":
        raise FastBackfillError(f"cannot load prepared package in {run.current_state}")
    file_ids: dict[str, Any] = {}
    files_to_copy: set[str] = set()
    for file_entry in entry["files"]:
        source_table = file_entry["source_table"]
        with conn.cursor() as cursor:
            cursor.execute("SELECT source_file_id,state FROM sec_source_files WHERE run_id=%s AND relative_path=%s", (run.run_id, source_table))
            existing = cursor.fetchone()
        if existing and existing[1] == "accounted":
            file_ids[source_table] = existing[0]
            continue
        if existing:
            raise FastBackfillError(f"existing non-accounted file requires explicit recovery: {entry['identity']}:{source_table}")
        file_ids[source_table] = register_file(conn, run_id=run.run_id, relative_path=source_table, sha256=file_entry["source_sha256"], byte_size=file_entry["byte_size"], schema_metadata={"headers": file_entry["headers"]}, state="loading")
        files_to_copy.add(source_table)
    columns = _raw_columns(form)
    if files_to_copy:
        with conn.cursor() as cursor:
            with cursor.copy(f"COPY {entry['raw_table']} ({','.join(columns)}) FROM STDIN") as copy:
                for line in _open_payload(output / entry["payload_path"]):
                    row = json.loads(line)
                    if row["source_table"] not in files_to_copy:
                        continue
                    copy.write_row(tuple([run.run_id, file_ids[row["source_table"]], row["source_row_number"], row["source_sha256"], entry["parser_version"], row["source_table"], _prepared_json(row["original_lexical_row"]), _prepared_json(row["typed_projection"]), row["parse_status"], _prepared_json(row["parse_errors"]), _prepared_json(row["candidate_key_evidence"])] + [row.get(name) for name in columns[11:]]))
        for source_table in files_to_copy:
            if form == "nport":
                from src.nport.ingestion import _insert_issues_from_raw
                _insert_issues_from_raw(conn, run_id=run.run_id, source_file_id=file_ids[source_table], source_table=source_table)
            else:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""INSERT INTO sec_row_issues (source_file_id,source_row_number,issue_sequence,table_name,column_name,raw_lexical_value,typed_error_code,typed_error_detail,status)
                    SELECT r.source_file_id,r.source_row_number,issue.ordinality::integer,r.source_table,issue.value->>'column_name',issue.value->>'raw_value',issue.value->>'code',issue.value->>'detail','quarantined'
                    FROM {entry['raw_table']} r CROSS JOIN LATERAL jsonb_array_elements(r.parse_errors) WITH ORDINALITY AS issue(value,ordinality)
                    WHERE r.ingestion_run_id=%s AND r.source_file_id=%s AND jsonb_array_length(r.parse_errors)>0""",
                        (run.run_id, file_ids[source_table]),
                    )
    for file_entry in entry["files"]:
        source_table = file_entry["source_table"]
        file_id = file_ids[source_table]
        counts = {key: file_entry[key] for key in ("expected_count", "data_count", "lexical_count", "typed_success_count", "quarantine_count", "reject_count")}
        if source_table in files_to_copy:
            register_file(conn, run_id=run.run_id, source_file_id=file_id, relative_path=source_table, sha256=file_entry["source_sha256"], byte_size=file_entry["byte_size"], schema_metadata={"headers": file_entry["headers"]}, state="accounted", **counts)
            register_table_reconciliation(conn, run_id=run.run_id, source_file_id=file_id, table_name=source_table, state="accounted", **counts)
    if entry["explicit_zero_tables"]:
        metadata_file_id = register_file(conn, run_id=run.run_id, relative_path=entry["metadata_filename"], sha256=entry["metadata_sha256"], byte_size=entry["metadata_byte_size"], schema_metadata={"metadata_for_absent_declared_tables": True}, state="accounted")
        for source_table in entry["explicit_zero_tables"]:
            register_table_reconciliation(conn, run_id=run.run_id, source_file_id=metadata_file_id, table_name=source_table, state="accounted")
    if form == "nport":
        from src.nport.ingestion import _resolve_holding_parents, _validate_candidate_keys
        from src.nport.schema import load_nport_contract
        _resolve_holding_parents(conn, run_id=run.run_id, contract=load_nport_contract())
        _validate_candidate_keys(conn, run_id=run.run_id)
    elif form == "ncen":
        from src.ncen.ingestion import _resolve_accession_parents
        from src.ncen.schema import load_ncen_contract
        _resolve_accession_parents(conn, run.run_id, load_ncen_contract(entry["metadata_sha256"]))
    validated = validate_raw_run(conn, run_id=run.run_id)
    register_package_discovery(conn, source_family=form, source_quarter=entry["quarter"], package_relative_path=entry["relative_package_path"], package_sha256=entry["package_sha256"], metadata_sha256=entry["metadata_sha256"], readme_sha256=entry["readme_sha256"], package_state="loaded", run_id=run.run_id)
    conn.commit()
    return validated.current_state


def load(*, output: Path, dsn: str, forms: Sequence[str] = tuple(RAW_TABLES), identities: Sequence[str] = (), delete_loaded: bool = False) -> dict[str, Any]:
    manifest = json.loads((output / "prepared-manifest.json").read_text(encoding="utf-8"))
    selected_forms, selected_identities = set(forms), set(identities)
    entries = [item for item in manifest["packages"] if item["form"] in selected_forms and (not selected_identities or item["identity"] in selected_identities)]
    with _connect(dsn) as conn:
        states = {entry["identity"]: _load_package(conn, output, entry) for entry in entries}
    deleted: list[str] = []
    if delete_loaded:
        for entry in entries:
            if states[entry["identity"]] != "raw_validated":
                continue
            payload = output / entry["payload_path"]
            if payload.exists() and _sha256_file(payload) == entry["payload_sha256"]:
                payload.unlink()
                deleted.append(entry["identity"])
        _write_json(output / "load-status.json", {"deleted_payloads": sorted(deleted), "package_states": states})
    return {"state": "loaded", "packages": len(entries), "package_states": states, "deleted_payloads": deleted}


def status(*, output: Path) -> dict[str, Any]:
    manifest = json.loads((output / "prepared-manifest.json").read_text(encoding="utf-8"))
    return {"state": "prepared", "packages": len(manifest["packages"]), "by_form": {form: sum(item["form"] == form for item in manifest["packages"]) for form in RAW_TABLES}}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="phase4_fast_backfill")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare", "load", "status"):
        item = sub.add_parser(command)
        item.add_argument("--output", type=Path, required=True)
        item.add_argument("--forms", nargs="+", choices=tuple(RAW_TABLES), default=list(RAW_TABLES))
        item.add_argument("--identities", nargs="*", default=[])
        if command == "prepare":
            item.add_argument("--root", type=Path, required=True)
            item.add_argument("--inventory", type=Path)
            item.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
        if command == "load":
            item.add_argument("--dsn-env", default="DATABASE_URL")
            item.add_argument("--delete-loaded", action="store_true", help="delete a payload only after raw validation commits")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "prepare":
        result = prepare(root=args.root, output=args.output, workers=args.workers, forms=args.forms, identities=args.identities, inventory=args.inventory)
    elif args.command == "load":
        dsn = os.environ.get(args.dsn_env)
        if not dsn:
            raise FastBackfillError(f"missing database DSN environment variable: {args.dsn_env}")
        result = load(output=args.output, dsn=dsn, forms=args.forms, identities=args.identities, delete_loaded=args.delete_loaded)
    else:
        result = status(output=args.output)
    print(_canonical(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

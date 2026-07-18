"""Transactional, descriptor-bound streaming landing for frozen RR1 packages."""
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

import psycopg

from src.sec_regulatory.contracts import ContractError
from src.sec_regulatory.manifests import (create_or_resume_run, fail_run, register_file,
    register_package_discovery, register_table_reconciliation, record_issue, retry_package_discovery,
    retry_run, transition_run, validate_raw_run)
from .tsv import stream_tsv
from .schema import json_typed_projection, package_sha256, parse_row, verify_package

BATCH_SIZE = 1000


class Rr1IngestionError(RuntimeError):
    pass


def source_quarter_from_package(package: Path) -> str:
    match = re.fullmatch(r"(20\d{2})q([1-4])_rr1", package.name.lower())
    if not match:
        raise Rr1IngestionError(f"nome de pacote RR1 inválido: {package.name}")
    return f"{match.group(1)}Q{match.group(2)}"


def ingest_package(conn: psycopg.Connection, *, package: Path, source_root: Path, parser_version: str = "rr1-v2",
                   _locked_package_digest: str | None = None) -> dict[str, object]:
    relative_path = package.relative_to(source_root).as_posix()
    quarter = source_quarter_from_package(package)
    try:
        verified = verify_package(package)
    except (ContractError, ValueError) as error:
        _discover_failure(conn, relative_path, quarter, str(error))
        return {"package": relative_path, "state": "failed", "reason": str(error)}
    digest = package_sha256(verified.file_hashes, metadata_sha256=verified.metadata_sha256,
        readme_sha256=verified.readme_sha256, metadata_filename=verified.metadata_filename)
    # File-level checkpoints commit by design.  Keep a package-specific session
    # lock across those commits so two sessions cannot independently resume the
    # same descriptor between checkpoints.
    if _locked_package_digest is None:
        with _package_advisory_lock(conn, digest):
            return ingest_package(conn, package=package, source_root=source_root, parser_version=parser_version,
                                  _locked_package_digest=digest)
    if digest != _locked_package_digest:
        return {"package": relative_path, "state": "failed", "reason": "bytes do pacote mudaram durante aquisição do lock"}
    existing = _package_status(conn, relative_path)
    if existing:
        known, state, run_id = existing
        if state == "loaded":
            return {"package": relative_path, "run_id": str(run_id), "state": "raw_validated", "resumed": True} if known == digest else {"package": relative_path, "state": "failed", "reason": "bytes mudaram em caminho loaded"}
        if state == "duplicate":
            return {"package": relative_path, "state": "duplicate", "rows": 0} if known == digest else {
                "package": relative_path, "state": "failed", "reason": "bytes mudaram em caminho duplicate"
            }
        if state in {"failed", "quarantined", "unsupported"}:
            if known and known != digest:
                return {"package": relative_path, "state": "failed", "reason": "bytes mudaram após falha inventariada"}
            retry_package_discovery(conn, source_family="rr1", package_relative_path=relative_path)
    else:
        _discover(conn, relative_path, quarter, digest, verified, "discovered")
    canonical = _canonical_duplicate(conn, relative_path, digest)
    if canonical:
        _discover(conn, relative_path, quarter, digest, verified, "duplicate", duplicate_of_package_id=canonical, reason="bytes idênticos a pacote canônico")
        return {"package": relative_path, "state": "duplicate", "rows": 0}
    run = create_or_resume_run(conn, source_family="rr1", package_sha256=digest, parser_version=parser_version, source_quarter=quarter, package_relative_path=relative_path)
    if run.current_state == "raw_validated":
        return {"package": relative_path, "run_id": str(run.run_id), "state": "raw_validated", "resumed": True}
    if run.current_state == "failed":
        run = retry_run(conn, run_id=run.run_id, detail="retomada RR1 exata")
    if run.current_state == "discovered":
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
    if run.current_state != "loading":
        return {"package": relative_path, "run_id": str(run.run_id), "state": run.current_state}
    _discover(conn, relative_path, quarter, digest, verified, "discovered", run_id=run.run_id)
    try:
        total = 0
        for table in sorted(verified.contract.tables, key=lambda value: (value.source_file != "sub.tsv", value.source_file)):
            path = package / table.source_file
            if not _file_is_complete(conn, run.run_id, table.source_file, verified.file_hashes[table.source_file], path.stat().st_size):
                total += _load_file(conn, run.run_id, parser_version, path, table, verified.file_hashes[table.source_file])
            conn.commit()
        with conn.transaction():
            # Re-verify headers and all package hashes after the same-descriptor loads.
            if verify_package(package) != verified:
                raise Rr1IngestionError("bytes do pacote mudaram durante o carregamento")
            validated = validate_raw_run(conn, run_id=run.run_id)
            _discover(conn, relative_path, quarter, digest, verified, "loaded", run_id=run.run_id)
    except Exception as error:
        failed = fail_run(conn, run_id=run.run_id, expected_state="loading", failure_code="rr1_raw_load_failed", failure_detail=str(error))
        _discover(conn, relative_path, quarter, digest, verified, "failed", run_id=failed.run_id, reason=str(error))
        return {"package": relative_path, "run_id": str(run.run_id), "state": "failed", "reason": str(error)}
    return {"package": relative_path, "run_id": str(run.run_id), "state": validated.current_state, "rows": total}


def _load_file(conn: psycopg.Connection, run_id: UUID, parser_version: str, path: Path, table, source_sha256: str) -> int:
    header, rows = stream_tsv(path, expected_sha256=source_sha256)
    if header != table.headers:
        rows.close()
        raise Rr1IngestionError(f"cabeçalho ou ordem divergente: {table.source_file}")
    lexical = typed = quarantined = 0
    batch: list[tuple[object, ...]] = []
    with conn.transaction():
        file_id = register_file(conn, run_id=run_id, relative_path=table.source_file, sha256=source_sha256,
            byte_size=path.stat().st_size, schema_metadata={"headers": list(table.headers)}, state="loading")
        for row_number, values in rows:
            parsed = parse_row(table.columns, values)
            lexical += 1
            typed += parsed.parse_status == "typed"
            quarantined += parsed.parse_status == "quarantined"
            batch.append((run_id, file_id, row_number, source_sha256, parser_version, table.source_file,
                json.dumps(parsed.lexical, sort_keys=True), json.dumps(json_typed_projection(parsed.typed), sort_keys=True),
                parsed.parse_status, json.dumps([issue.__dict__ for issue in parsed.issues], sort_keys=True),
                json.dumps(parsed.candidate_key_evidence, sort_keys=True), *(parsed.lexical.get(name) or None for name in ("adsh", "tag", "version", "ddate", "series", "class", "measure", "document", "otherdims", "iprx"))))
            if len(batch) >= BATCH_SIZE:
                _insert_rows(conn, batch)
                batch.clear()
            for sequence, issue in enumerate(parsed.issues, 1):
                record_issue(conn, source_file_id=file_id, source_row_number=row_number, issue_sequence=sequence,
                    table_name=table.source_file, column_name=issue.column_name, raw_lexical_value=issue.raw_value,
                    typed_error_code=issue.code, typed_error_detail=issue.detail, status="quarantined")
        if batch:
            _insert_rows(conn, batch)
        register_file(conn, run_id=run_id, source_file_id=file_id, relative_path=table.source_file, sha256=source_sha256,
            byte_size=path.stat().st_size, schema_metadata={"headers": list(table.headers)}, expected_count=lexical,
            data_count=lexical, lexical_count=lexical, typed_success_count=typed, quarantine_count=quarantined,
            reject_count=0, state="accounted")
        register_table_reconciliation(conn, run_id=run_id, source_file_id=file_id, table_name=table.source_file,
            expected_count=lexical, source_count=lexical, lexical_count=lexical, typed_success_count=typed,
            quarantine_count=quarantined, reject_count=0, state="accounted")
    return lexical


def _insert_rows(conn: psycopg.Connection, rows: Iterable[tuple[object, ...]]) -> None:
    with conn.cursor() as cur:
        with cur.copy("""COPY rr1_raw_v2_rows (ingestion_run_id,source_file_id,source_row_number,source_sha256,parser_version,source_table,original_lexical_row,typed_projection,parse_status,parse_errors,candidate_key_evidence,adsh,tag,version,ddate,series,class,measure,document,otherdims,iprx) FROM STDIN""") as copy:
            for row in rows:
                copy.write_row(row)


def _file_is_complete(conn, run_id, source_file, sha, byte_size) -> bool:
    with conn.cursor() as cur:
        cur.execute("""SELECT f.expected_count,f.data_count,f.lexical_count,f.typed_success_count,f.quarantine_count,f.reject_count,count(r.raw_row_id),t.expected_count,t.source_count,t.lexical_count,t.typed_success_count,t.quarantine_count,t.reject_count,t.state FROM sec_source_files f LEFT JOIN rr1_raw_v2_rows r ON r.source_file_id=f.source_file_id LEFT JOIN sec_table_reconciliations t ON t.source_file_id=f.source_file_id AND t.table_name=%s WHERE f.run_id=%s AND f.relative_path=%s AND f.sha256=%s AND f.byte_size=%s AND f.state='accounted' GROUP BY f.source_file_id,t.reconciliation_id""", (source_file,run_id,source_file,sha,byte_size))
        row = cur.fetchone()
    return bool(row and row[-1] == "accounted" and row[0] == row[1] == row[2] == row[6] and row[2] == row[3] + row[4] + row[5] and tuple(row[7:13]) == tuple(row[:6]))


def _package_status(conn, relative_path):
    with conn.cursor() as cur:
        cur.execute("SELECT package_sha256,package_state,run_id FROM sec_source_packages WHERE source_family='rr1' AND package_relative_path=%s", (relative_path,))
        return cur.fetchone()


def _canonical_duplicate(conn, relative_path, digest):
    with conn.cursor() as cur:
        cur.execute("SELECT package_id FROM sec_source_packages WHERE source_family='rr1' AND package_sha256=%s AND package_relative_path<>%s AND package_state='loaded' ORDER BY created_at LIMIT 1", (digest, relative_path))
        row = cur.fetchone()
    return row[0] if row else None


def _discover(conn, path, quarter, digest, verified, state, **extra):
    return register_package_discovery(conn, source_family="rr1", source_quarter=quarter, package_relative_path=path,
        package_sha256=digest, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256,
        package_state=state, **extra)


def _discover_failure(conn, path, quarter, reason):
    register_package_discovery(conn, source_family="rr1", source_quarter=quarter, package_relative_path=path, package_state="discovered")
    register_package_discovery(conn, source_family="rr1", source_quarter=quarter, package_relative_path=path, package_state="failed", reason=reason)


@contextmanager
def _package_advisory_lock(conn: psycopg.Connection, package_digest: str):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (f"rr1:{package_digest}",))
    try:
        yield
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (f"rr1:{package_digest}",))

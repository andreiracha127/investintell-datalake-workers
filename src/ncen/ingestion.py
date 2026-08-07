"""Transactional streaming landing for frozen N-CEN packages."""
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from uuid import UUID
import psycopg

from src.sec_regulatory.contracts import ContractError
from src.sec_regulatory.manifests import (create_or_resume_run, fail_run, register_file,
    register_package_discovery, register_table_reconciliation, record_issue, retry_package_discovery,
    retry_run, transition_run, validate_raw_run)
from src.sec_regulatory.tsv import stream_tsv
from .inventory import PackageInventory
from .schema import json_typed_projection, package_sha256, parse_row, verify_package

BATCH_SIZE = 1000
class NcenIngestionError(RuntimeError): pass

def source_quarter_from_package(package: Path) -> str:
    match = re.fullmatch(r"(20\d{2})q([1-4])_ncen(?:_\d+)?", package.name.lower())
    if not match: raise NcenIngestionError(f"nome de pacote N-CEN inválido: {package.name}")
    return f"{match.group(1)}Q{match.group(2)}"

def ingest_package(conn: psycopg.Connection, *, package: Path, source_root: Path, parser_version: str = "ncen-v2", inventory: PackageInventory | None = None) -> dict[str, object]:
    relative_path = package.relative_to(source_root).as_posix(); quarter = source_quarter_from_package(package)
    try: verified = verify_package(package, inventory=inventory)
    except (ContractError, ValueError) as error:
        _discover_failure(conn, relative_path, quarter, str(error)); return {"package": relative_path, "state":"failed", "reason":str(error)}
    digest = package_sha256(verified.file_hashes, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256)
    _acquire_digest_lock(conn, digest)
    pending_exception = False
    try:
        # Session advisory locks survive the loader's bounded transactions.  The
        # terminal-state read must happen after acquisition so a waiter resumes.
        return _ingest_locked(conn, package, relative_path, quarter, digest, verified, parser_version, inventory)
    except BaseException:
        pending_exception = True
        raise
    finally:
        try:
            _release_digest_lock(conn, digest)
        except BaseException:
            if not pending_exception:
                raise


def _ingest_locked(conn: psycopg.Connection, package: Path, relative_path: str, quarter: str, digest: str, verified, parser_version: str, inventory: PackageInventory | None) -> dict[str, object]:
    contract = verified.contract
    existing = _package_status(conn, relative_path)
    if existing:
        known, state, run_id = existing
        if state == "loaded":
            return {"package":relative_path,"run_id":str(run_id),"state":"raw_validated","resumed":True} if known == digest else {"package":relative_path,"state":"failed","reason":"bytes mudaram em caminho loaded"}
        if state == "duplicate": return {"package":relative_path,"state":"duplicate","rows":0}
        if state in {"failed","quarantined","unsupported"}:
            if known and known != digest: return {"package":relative_path,"state":"failed","reason":"bytes mudaram após falha inventariada"}
            retry_package_discovery(conn, source_family="ncen", package_relative_path=relative_path)
    else: _discover(conn, relative_path, quarter, digest, verified, "discovered")
    canonical = _canonical_duplicate(conn, relative_path, digest)
    if canonical:
        _discover(conn, relative_path, quarter, digest, verified, "duplicate", duplicate_of_package_id=canonical, reason="bytes idênticos a pacote canônico")
        return {"package":relative_path,"state":"duplicate","rows":0}
    run = create_or_resume_run(conn, source_family="ncen", package_sha256=digest, parser_version=parser_version, source_quarter=quarter, package_relative_path=relative_path)
    if run.current_state == "raw_validated": return {"package":relative_path,"run_id":str(run.run_id),"state":"raw_validated","resumed":True}
    if run.current_state == "failed": run = retry_run(conn, run_id=run.run_id, detail="retomada N-CEN exata")
    if run.current_state == "discovered": run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
    if run.current_state != "loading": return {"package":relative_path,"run_id":str(run.run_id),"state":run.current_state}
    # The database statement guard verifies raw provenance against the package
    # inventory, so bind the exact selected metadata variant before COPY.
    _discover(conn, relative_path, quarter, digest, verified, "discovered", run_id=run.run_id)
    try:
        total = 0
        for table in sorted(contract.tables, key=lambda t: (t.source_file != "SUBMISSION.tsv", t.source_file)):
            if table.source_file in verified.file_hashes:
                path = package / table.source_file
                if not _file_is_complete(conn, run.run_id, table.source_file, verified.file_hashes[table.source_file], path.stat().st_size):
                    total += _load_file(conn, run.run_id, parser_version, path, table, verified.file_hashes[table.source_file], verified.file_inventory[table.source_file].row_count)
            else: _register_absent_table(conn, run.run_id, package, table.source_file, verified.metadata_sha256)
            conn.commit()
        with conn.transaction():
            if verify_package(package, inventory=inventory) != verified: raise NcenIngestionError("bytes do pacote mudaram durante o carregamento")
            _resolve_accession_parents(conn, run.run_id, contract)
            validated = validate_raw_run(conn, run_id=run.run_id)
            _discover(conn, relative_path, quarter, digest, verified, "loaded", run_id=run.run_id)
    except Exception as error:
        failed = fail_run(conn, run_id=run.run_id, expected_state="loading", failure_code="ncen_raw_load_failed", failure_detail=str(error))
        _discover(conn, relative_path, quarter, digest, verified, "failed", run_id=failed.run_id, reason=str(error))
        return {"package":relative_path,"run_id":str(run.run_id),"state":"failed","reason":str(error)}
    return {"package":relative_path,"run_id":str(run.run_id),"state":validated.current_state,"rows":total}


def _acquire_digest_lock(conn: psycopg.Connection, digest: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (f"ncen:{digest}",))


def _release_digest_lock(conn: psycopg.Connection, digest: str) -> None:
    # A failed statement leaves the transaction unusable; session locks are
    # independent of transaction state, so recover the connection before the
    # unlock query.  This is deliberately safe for callers that are handling
    # an exception: the original exception is preserved by ingest_package.
    if conn.info.transaction_status.name == "INERROR":
        conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (f"ncen:{digest}",))
        if cur.fetchone() != (True,):
            raise NcenIngestionError("lock consultivo N-CEN não estava retido pela sessão")

def _load_file(conn, run_id: UUID, parser_version: str, path: Path, table, source_sha256: str, expected_rows: int) -> int:
    header, rows = stream_tsv(path, expected_sha256=source_sha256)
    if header != table.headers: raise NcenIngestionError(f"cabeçalho ou ordem divergente: {table.source_file}")
    lexical = typed = quarantined = 0; batch=[]
    with conn.transaction():
        file_id = register_file(conn, run_id=run_id, relative_path=table.source_file, sha256=source_sha256, byte_size=path.stat().st_size, schema_metadata={"headers":list(table.headers)}, state="loading")
        for row_number, values in rows:
            parsed = parse_row(table.columns, values); lexical += 1; typed += parsed.parse_status == "typed"; quarantined += parsed.parse_status == "quarantined"
            entity_key, parent_key = _relationship_keys(table, parsed.lexical)
            batch.append((run_id,file_id,row_number,source_sha256,parser_version,table.source_file,json.dumps(parsed.lexical,sort_keys=True),json.dumps(json_typed_projection(parsed.typed),sort_keys=True),parsed.parse_status,json.dumps([x.__dict__ for x in parsed.issues],sort_keys=True),json.dumps(parsed.candidate_key_evidence,sort_keys=True),parsed.lexical.get("ACCESSION_NUMBER") or None,parsed.lexical.get("FUND_ID") or None,parsed.lexical.get("DIRECTOR_SEQNUM") or None,parsed.lexical.get("LINE_OF_CREDIT_SEQNUM") or None,parsed.lexical.get("VALUATION_METHOD_CHANGE_SEQNUM") or None,entity_key,parent_key))
            if len(batch) >= BATCH_SIZE: _insert_rows(conn,batch); batch.clear()
            for seq, issue in enumerate(parsed.issues, 1): record_issue(conn, source_file_id=file_id, source_row_number=row_number, issue_sequence=seq, table_name=table.source_file, column_name=issue.column_name, raw_lexical_value=issue.raw_value, typed_error_code=issue.code, typed_error_detail=issue.detail, status="quarantined")
        if batch: _insert_rows(conn,batch)
        # Two independent readers must agree on how many physical records the
        # delivery has: the inventory's csv.reader pass and the loader's own
        # streaming parser.  This is the cheap counting scaffold that survives
        # the retirement of the pre-committed canonical inventory.
        if lexical != expected_rows: raise NcenIngestionError(f"contagem física divergente em {table.source_file}: inventário={expected_rows}, carregado={lexical}")
        register_file(conn, run_id=run_id, source_file_id=file_id, relative_path=table.source_file, sha256=source_sha256, byte_size=path.stat().st_size, schema_metadata={"headers":list(table.headers)}, expected_count=lexical,data_count=lexical,lexical_count=lexical,typed_success_count=typed,quarantine_count=quarantined,reject_count=0,state="accounted")
        register_table_reconciliation(conn,run_id=run_id,source_file_id=file_id,table_name=table.source_file,expected_count=lexical,source_count=lexical,lexical_count=lexical,typed_success_count=typed,quarantine_count=quarantined,reject_count=0,state="accounted")
    return lexical

def _insert_rows(conn, rows: Iterable[tuple[object,...]]) -> None:
    with conn.cursor() as cur:
        with cur.copy("""COPY ncen_raw_v2_rows (ingestion_run_id,source_file_id,source_row_number,source_sha256,parser_version,source_table,original_lexical_row,typed_projection,parse_status,parse_errors,candidate_key_evidence,accession_number,fund_id,director_seqnum,line_of_credit_seqnum,valuation_method_change_seqnum,entity_key,parent_key) FROM STDIN""") as copy:
            for row in rows: copy.write_row(row)

def _register_absent_table(conn, run_id, package, source_file, metadata_sha256):
    file_id=register_file(conn,run_id=run_id,relative_path="ncen_metadata.json",sha256=metadata_sha256,byte_size=(package/"ncen_metadata.json").stat().st_size,schema_metadata={"metadata_for_absent_declared_tables":True},state="accounted")
    register_table_reconciliation(conn,run_id=run_id,source_file_id=file_id,table_name=source_file,state="accounted")

def _file_is_complete(conn, run_id, source_file, sha, byte_size):
    with conn.cursor() as cur:
        cur.execute("""SELECT f.expected_count,f.data_count,f.lexical_count,f.typed_success_count,f.quarantine_count,f.reject_count,count(r.raw_row_id),t.expected_count,t.source_count,t.lexical_count,t.typed_success_count,t.quarantine_count,t.reject_count,t.state FROM sec_source_files f LEFT JOIN ncen_raw_v2_rows r ON r.source_file_id=f.source_file_id LEFT JOIN sec_table_reconciliations t ON t.source_file_id=f.source_file_id AND t.table_name=%s WHERE f.run_id=%s AND f.relative_path=%s AND f.sha256=%s AND f.byte_size=%s AND f.state='accounted' GROUP BY f.source_file_id,t.reconciliation_id""",(source_file,run_id,source_file,sha,byte_size)); row=cur.fetchone()
    return bool(row and row[-1]=="accounted" and row[0]==row[1]==row[2]==row[6] and row[2]==row[3]+row[4]+row[5] and tuple(row[7:13])==tuple(row[:6]))

def _resolve_accession_parents(conn, run_id, contract):
    with conn.cursor() as cur:
        cur.execute("""SELECT r.source_table FROM ncen_raw_v2_rows r JOIN ncen_contract_tables c ON c.source_table=r.source_table LEFT JOIN (SELECT accession_number,count(*) n FROM ncen_raw_v2_rows WHERE ingestion_run_id=%s AND source_table='SUBMISSION.tsv' AND typed_projection->>'ACCESSION_NUMBER' IS NOT NULL GROUP BY accession_number) s ON s.accession_number=r.accession_number WHERE r.ingestion_run_id=%s AND 'SUBMISSION.tsv'=ANY(c.logical_parents) AND (r.accession_number IS NULL OR s.n<>1) LIMIT 1""",(run_id,run_id))
        if cur.fetchone(): raise NcenIngestionError("ACCESSION_NUMBER órfão ou ambíguo em tabela filha N-CEN")

def _relationship_keys(table, row: dict[str, str]) -> tuple[str | None, str | None]:
    """Stable child-to-parent identities for the frozen N-CEN relationship graph."""
    def key(kind: str, *names: str) -> str | None:
        values = [row.get(name) or "" for name in names]
        return f"{kind}:" + ":".join(values) if all(values) else None
    own = {
        "FUND_REPORTED_INFO.tsv": key("fund", "FUND_ID"),
        "DIRECTOR.tsv": key("director", "ACCESSION_NUMBER", "DIRECTOR_SEQNUM"),
        "LINE_OF_CREDIT_DETAIL.tsv": key("credit", "FUND_ID", "LINE_OF_CREDIT_SEQNUM"),
        "VALUATION_METHOD_CHANGE.tsv": key("valuation", "ACCESSION_NUMBER", "VALUATION_METHOD_CHANGE_SEQNUM"),
    }.get(table.source_file, key("accession", "ACCESSION_NUMBER"))
    if not table.logical_parents: return own, None
    parent = table.logical_parents[0]
    parent_key = {
        "FUND_REPORTED_INFO.tsv": key("fund", "FUND_ID"),
        "DIRECTOR.tsv": key("director", "ACCESSION_NUMBER", "DIRECTOR_SEQNUM"),
        "LINE_OF_CREDIT_DETAIL.tsv": key("credit", "FUND_ID", "LINE_OF_CREDIT_SEQNUM"),
        "VALUATION_METHOD_CHANGE.tsv": key("valuation", "ACCESSION_NUMBER", "VALUATION_METHOD_CHANGE_SEQNUM"),
    }.get(parent, key("accession", "ACCESSION_NUMBER"))
    return own, parent_key

def _package_status(conn, relative_path):
    with conn.cursor() as cur: cur.execute("SELECT package_sha256,package_state,run_id FROM sec_source_packages WHERE source_family='ncen' AND package_relative_path=%s",(relative_path,)); return cur.fetchone()
def _canonical_duplicate(conn, relative_path, digest):
    with conn.cursor() as cur: cur.execute("SELECT package_id FROM sec_source_packages WHERE source_family='ncen' AND package_sha256=%s AND package_relative_path<>%s AND package_state='loaded' ORDER BY created_at LIMIT 1",(digest,relative_path)); row=cur.fetchone(); return row[0] if row else None
def _discover(conn, path, quarter, digest, verified, state, **extra):
    return register_package_discovery(conn,source_family="ncen",source_quarter=quarter,package_relative_path=path,package_sha256=digest,metadata_sha256=verified.metadata_sha256,readme_sha256=verified.readme_sha256,package_state=state,**extra)
def _discover_failure(conn,path,quarter,reason):
    register_package_discovery(conn,source_family="ncen",source_quarter=quarter,package_relative_path=path,package_state="discovered")
    register_package_discovery(conn,source_family="ncen",source_quarter=quarter,package_relative_path=path,package_state="failed",reason=reason)

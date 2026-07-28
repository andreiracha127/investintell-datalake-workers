"""Landing transacional e streaming de um pacote N-PORT governado."""

from __future__ import annotations

import json
from contextlib import contextmanager
from collections.abc import Iterable
from pathlib import Path
from uuid import UUID

import psycopg

from src.sec_regulatory.contracts import ContractError
from src.sec_regulatory.manifests import (
    create_or_resume_run,
    fail_run,
    register_file,
    register_table_reconciliation,
    record_issue,
    register_package_discovery,
    retry_package_discovery,
    retry_run,
    transition_run,
    validate_raw_run,
)
from src.sec_regulatory.tsv import stream_tsv

from .schema import json_typed_projection, load_nport_contract, package_sha256, parse_row, verify_package
from .storage import install_schema


BATCH_SIZE = 1_000


class NportIngestionError(RuntimeError):
    """O pacote N-PORT falhou numa regra fechada de landing bruta."""


def source_quarter_from_package(package: Path) -> str:
    """Converte diretório DERA ``2026q1_nport`` para a identidade governada."""
    name = package.name.lower()
    import re

    matched = re.fullmatch(r"(20\d{2})q([1-4])_nport", name)
    if not matched:
        raise NportIngestionError(f"nome de pacote N-PORT inválido: {package.name}")
    return f"{matched.group(1)}Q{matched.group(2)}"


def ingest_package(
    conn: psycopg.Connection,
    *,
    package: Path,
    source_root: Path,
    parser_version: str = "nport-v1",
    _locked_package_digest: str | None = None,
) -> dict[str, object]:
    """Carrega um pacote numa transação; linhas só aparecem após raw validation."""
    contract = load_nport_contract()
    relative_path = package.relative_to(source_root).as_posix()
    quarter = source_quarter_from_package(package)
    try:
        verified = verify_package(package, contract)
    except (ContractError, ValueError) as error:
        existing = _package_status(conn, relative_path=relative_path)
        if existing is not None and existing[1] == "loaded":
            return {"package": relative_path, "state": "failed", "reason": f"pacote loaded mudou: {error}"}
        if existing is None:
            register_package_discovery(
                conn, source_family="nport", source_quarter=quarter, package_relative_path=relative_path,
                package_state="discovered",
            )
        register_package_discovery(
            conn, source_family="nport", source_quarter=quarter, package_relative_path=relative_path,
            package_state="failed", reason=str(error),
        )
        return {"package": relative_path, "state": "failed", "reason": str(error)}
    digest = package_sha256(
        verified.file_hashes, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256,
    )
    if _locked_package_digest is None:
        # Session scope deliberately survives file checkpoint commits. The
        # digest makes contention package-specific rather than global.
        with _package_advisory_lock(conn, digest):
            return ingest_package(
                conn, package=package, source_root=source_root, parser_version=parser_version,
                _locked_package_digest=digest,
            )
    if digest != _locked_package_digest:
        return {
            "package": relative_path,
            "state": "failed",
            "reason": "bytes do pacote mudaram durante aquisição do lock",
        }
    existing = _package_status(conn, relative_path=relative_path)
    if existing is not None:
        known_hash, package_state, known_run_id = existing
        if package_state == "loaded":
            if known_hash != digest:
                return {"package": relative_path, "state": "failed", "reason": "bytes mudaram em caminho loaded"}
            return {"package": relative_path, "run_id": str(known_run_id), "state": "raw_validated", "resumed": True}
        if package_state == "duplicate":
            return {"package": relative_path, "state": "duplicate", "rows": 0}
        if package_state in {"failed", "quarantined", "unsupported"}:
            if known_hash is not None and known_hash != digest:
                return {"package": relative_path, "state": "failed", "reason": "bytes mudaram após falha inventariada"}
            retry_package_discovery(conn, source_family="nport", package_relative_path=relative_path)
    else:
        register_package_discovery(
            conn, source_family="nport", source_quarter=quarter, package_relative_path=relative_path,
            package_sha256=digest, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256,
            package_state="discovered",
        )
    duplicate_id = _canonical_duplicate(conn, relative_path=relative_path, package_sha256=digest)
    if duplicate_id is not None:
        register_package_discovery(
            conn, source_family="nport", source_quarter=quarter, package_relative_path=relative_path,
            package_sha256=digest, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256,
            package_state="duplicate", reason="bytes idênticos a pacote canônico",
            duplicate_of_package_id=duplicate_id,
        )
        return {"package": relative_path, "state": "duplicate", "rows": 0}
    run = create_or_resume_run(
        conn, source_family="nport", package_sha256=digest, parser_version=parser_version,
        source_quarter=quarter, package_relative_path=relative_path,
    )
    if run.current_state == "raw_validated":
        return {"package": relative_path, "run_id": str(run.run_id), "state": "raw_validated", "resumed": True}
    if run.current_state == "failed":
        run = retry_run(conn, run_id=run.run_id, detail="retomada N-PORT exata")
    if run.current_state == "discovered":
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
    if run.current_state != "loading":
        return {"package": relative_path, "run_id": str(run.run_id), "state": run.current_state, "reason": "estado não retomável"}
    try:
        total_rows = 0
        for table in sorted(
            contract.tables,
            key=lambda item: 0 if item.source_file == "SUBMISSION.tsv" else 1 if item.source_file == "FUND_REPORTED_HOLDING.tsv" else 2,
        ):
            if table.source_file in verified.file_hashes:
                if _file_is_complete(conn, run_id=run.run_id, source_file=table.source_file,
                                     source_sha256=verified.file_hashes[table.source_file],
                                     byte_size=(package / table.source_file).stat().st_size):
                    continue
                total_rows += _load_file(
                    conn, run_id=run.run_id, parser_version=parser_version, package=package,
                    table=table, source_sha256=verified.file_hashes[table.source_file],
                )
            else:
                _register_absent_table(conn, run_id=run.run_id, package=package, source_file=table.source_file,
                                       metadata_sha256=verified.metadata_sha256)
            # Checkpoint por arquivo lógico: uma falha tardia conserva apenas
            # arquivos completamente reconciliados, nunca lotes parciais.
            conn.commit()
        with conn.transaction():
            # Re-check the exact governed package after every physical stream.
            # This also catches metadata/readme or filename-set swaps that do
            # not affect the TSV currently being consumed.
            if verify_package(package, contract) != verified:
                raise NportIngestionError("bytes do pacote mudaram durante o carregamento")
            _resolve_holding_parents(conn, run_id=run.run_id, contract=contract)
            _validate_candidate_keys(conn, run_id=run.run_id)
            validated = validate_raw_run(conn, run_id=run.run_id)
            # A visibilidade validada e o inventário loaded são um único ponto
            # de commit: não pode sobreviver um raw_validated descoberto.
            register_package_discovery(
                conn, source_family="nport", source_quarter=quarter, package_relative_path=relative_path,
                package_sha256=digest, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256,
                package_state="loaded", run_id=run.run_id,
            )
    except Exception as error:
        failed = fail_run(conn, run_id=run.run_id, expected_state="loading", failure_code="nport_raw_load_failed",
                          failure_detail=str(error))
        register_package_discovery(
            conn, source_family="nport", source_quarter=quarter, package_relative_path=relative_path,
            package_sha256=digest, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256,
            package_state="failed", reason=str(error), run_id=failed.run_id,
        )
        return {"package": relative_path, "run_id": str(run.run_id), "state": "failed", "reason": str(error)}
    return {"package": relative_path, "run_id": str(run.run_id), "state": validated.current_state, "rows": total_rows}


@contextmanager
def _package_advisory_lock(conn: psycopg.Connection, package_digest: str):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (f"nport:{package_digest}",))
    try:
        yield
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (f"nport:{package_digest}",))


def _load_file(conn: psycopg.Connection, *, run_id: UUID, parser_version: str, package: Path, table, source_sha256: str) -> int:
    path = package / table.source_file
    header, rows = stream_tsv(path, expected_sha256=source_sha256)
    if header != table.headers:
        raise NportIngestionError(f"cabeçalho ou ordem divergente: {table.source_file}")
    lexical = typed = quarantined = 0
    batch: list[tuple[object, ...]] = []
    with conn.transaction():
        source_file_id = register_file(
            conn, run_id=run_id, relative_path=table.source_file, sha256=source_sha256,
            byte_size=path.stat().st_size, schema_metadata={"headers": list(table.headers)},
            expected_count=0, data_count=0, lexical_count=0, typed_success_count=0,
            quarantine_count=0, reject_count=0, state="loading",
        )
        for row_number, values in rows:
            parsed = parse_row(table.columns, values)
            lexical += 1
            typed += int(parsed.parse_status == "typed")
            quarantined += int(parsed.parse_status == "quarantined")
            batch.append((
                run_id, source_file_id, row_number, source_sha256, parser_version, table.source_file,
                json.dumps(parsed.lexical, sort_keys=True), json.dumps(json_typed_projection(parsed.typed), sort_keys=True),
                parsed.parse_status,
                json.dumps([issue.__dict__ for issue in parsed.issues], sort_keys=True),
                json.dumps(parsed.candidate_key_evidence, sort_keys=True),
                parsed.lexical.get("ACCESSION_NUMBER") or None, parsed.lexical.get("HOLDING_ID") or None,
            ))
            if len(batch) >= BATCH_SIZE:
                _insert_rows(conn, batch)
                batch.clear()
            for sequence, issue in enumerate(parsed.issues, start=1):
                record_issue(
                    conn, source_file_id=source_file_id, source_row_number=row_number,
                    issue_sequence=sequence, table_name=table.source_file, column_name=issue.column_name,
                    raw_lexical_value=issue.raw_value, typed_error_code=issue.code,
                    typed_error_detail=issue.detail, status="quarantined",
                )
        if batch:
            _insert_rows(conn, batch)
        register_file(
            conn, run_id=run_id, relative_path=table.source_file, sha256=source_sha256,
            byte_size=path.stat().st_size, schema_metadata={"headers": list(table.headers)},
            expected_count=lexical, data_count=lexical, lexical_count=lexical, typed_success_count=typed,
            quarantine_count=quarantined, reject_count=0, state="accounted", source_file_id=source_file_id,
        )
        register_table_reconciliation(
            conn, run_id=run_id, source_file_id=source_file_id, table_name=table.source_file,
            expected_count=lexical, source_count=lexical, lexical_count=lexical, typed_success_count=typed,
            quarantine_count=quarantined, reject_count=0, state="accounted",
        )
    return lexical


def _insert_rows(conn: psycopg.Connection, rows: Iterable[tuple[object, ...]]) -> None:
    with conn.cursor() as cur:
        with cur.copy(
            """COPY nport_raw_rows (
                ingestion_run_id, source_file_id, source_row_number, source_sha256, parser_version,
                source_table, original_lexical_row, typed_projection, parse_status, parse_errors,
                candidate_key_evidence, accession_number, holding_id
            ) FROM STDIN"""
        ) as copy:
            for row in rows:
                copy.write_row(row)


def _file_is_complete(
    conn: psycopg.Connection, *, run_id: UUID, source_file: str, source_sha256: str, byte_size: int,
) -> bool:
    """Só salta arquivo cuja manifestação e linhas físicas fecham exatamente."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT f.expected_count, f.data_count, f.lexical_count, f.typed_success_count,
                      f.quarantine_count, f.reject_count, count(r.raw_row_id),
                      t.expected_count, t.source_count, t.lexical_count, t.typed_success_count,
                      t.quarantine_count, t.reject_count, t.state
               FROM sec_source_files f LEFT JOIN nport_raw_rows r ON r.source_file_id = f.source_file_id
               LEFT JOIN sec_table_reconciliations t ON t.source_file_id = f.source_file_id AND t.table_name = %s
               WHERE f.run_id = %s AND f.relative_path = %s AND f.sha256 = %s AND f.byte_size = %s
                 AND f.state = 'accounted'
               GROUP BY f.source_file_id, t.reconciliation_id""",
            (source_file, run_id, source_file, source_sha256, byte_size),
        )
        row = cur.fetchone()
    if row is None:
        return False
    expected, data, lexical, typed, quarantine, rejected, raw_count, t_expected, t_source, t_lexical, t_typed, t_quarantine, t_rejected, t_state = row
    return bool(
        t_state == "accounted"
        and expected == data == lexical == raw_count
        and lexical == typed + quarantine + rejected
        and (t_expected, t_source, t_lexical, t_typed, t_quarantine, t_rejected)
        == (expected, data, lexical, typed, quarantine, rejected)
    )


def _register_absent_table(
    conn: psycopg.Connection, *, run_id: UUID, package: Path, source_file: str, metadata_sha256: str,
) -> None:
    """Materializa tabela declarada mas fisicamente ausente como zero-row auditável."""
    # A reconciliação aponta ao metadata real; a ausência declarada jamais vira
    # um arquivo sintético (nem recebe uma SHA fingida de TSV).
    source_file_id = register_file(
        conn, run_id=run_id, relative_path="nport_metadata.json", sha256=metadata_sha256,
        byte_size=(package / "nport_metadata.json").stat().st_size,
        schema_metadata={"metadata_for_absent_declared_tables": True}, expected_count=0, data_count=0,
        lexical_count=0, typed_success_count=0, quarantine_count=0, reject_count=0, state="accounted",
    )
    register_table_reconciliation(
        conn, run_id=run_id, source_file_id=source_file_id, table_name=source_file,
        expected_count=0, source_count=0, lexical_count=0, typed_success_count=0,
        quarantine_count=0, reject_count=0, state="accounted",
    )


def _canonical_duplicate(conn: psycopg.Connection, *, relative_path: str, package_sha256: str) -> UUID | None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT package_id FROM sec_source_packages
               WHERE source_family = 'nport' AND package_sha256 = %s
                 AND package_relative_path <> %s AND package_state = 'loaded'
               ORDER BY created_at LIMIT 1""",
            (package_sha256, relative_path),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _package_status(conn: psycopg.Connection, *, relative_path: str) -> tuple[str | None, str, UUID | None] | None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT package_sha256, package_state, run_id FROM sec_source_packages
               WHERE source_family = 'nport' AND package_relative_path = %s""",
            (relative_path,),
        )
        row = cur.fetchone()
    return row if row is not None else None


def _resolve_holding_parents(conn: psycopg.Connection, *, run_id: UUID, contract) -> None:
    """Resolve filhos apenas por mapa one-to-one validado em SQL."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM nport_holding_accession_map WHERE ingestion_run_id = %s", (run_id,))
        cur.execute(
            """SELECT holding_id FROM nport_raw_rows
               WHERE ingestion_run_id = %s AND source_table = 'FUND_REPORTED_HOLDING.tsv'
                 AND typed_projection->>'HOLDING_ID' IS NOT NULL AND typed_projection->>'ACCESSION_NUMBER' IS NOT NULL
               GROUP BY holding_id HAVING count(DISTINCT accession_number) <> 1 LIMIT 1""",
            (run_id,),
        )
        if cur.fetchone():
            raise NportIngestionError("HOLDING_ID ambíguo em FUND_REPORTED_HOLDING.tsv")
        cur.execute(
            """INSERT INTO nport_holding_accession_map
                    (ingestion_run_id, holding_id, accession_number, source_file_id, source_row_number)
                SELECT %s, s.holding_id, s.accession_number, s.source_file_id, s.source_row_number
                FROM (
                    SELECT DISTINCT ON (holding_id) holding_id, accession_number, source_file_id, source_row_number
                    FROM nport_raw_rows
                    WHERE ingestion_run_id = %s AND source_table = 'FUND_REPORTED_HOLDING.tsv'
                      AND typed_projection->>'HOLDING_ID' IS NOT NULL AND typed_projection->>'ACCESSION_NUMBER' IS NOT NULL
                    ORDER BY holding_id, source_file_id, source_row_number
                ) s""",
            (run_id, run_id),
        )
        # O mapa acabou de ser populado dentro desta transação e não tem
        # estatística nenhuma, então o planejador o trata como vazio e escolhe um
        # plano que derrama dezenas de GB ao varrer as dezenas de milhões de
        # linhas brutas abaixo. Analisar aqui custa segundos e evita isso.
        cur.execute("ANALYZE nport_holding_accession_map")
        cur.execute(
            """SELECT 1 FROM nport_raw_rows r
               LEFT JOIN nport_holding_accession_map m
                 ON m.ingestion_run_id = r.ingestion_run_id AND m.holding_id = r.holding_id
               WHERE r.ingestion_run_id = %s AND r.source_table <> 'FUND_REPORTED_HOLDING.tsv'
                 AND r.typed_projection->>'HOLDING_ID' IS NOT NULL AND m.holding_id IS NULL LIMIT 1""",
            (run_id,),
        )
        if cur.fetchone():
            raise NportIngestionError("HOLDING_ID órfão em tabela filha N-PORT")
        cur.execute(
            """UPDATE nport_raw_rows r SET accession_number = m.accession_number
               FROM nport_holding_accession_map m
               WHERE r.ingestion_run_id = %s AND r.source_table <> 'FUND_REPORTED_HOLDING.tsv'
                 AND r.holding_id = m.holding_id AND m.ingestion_run_id = r.ingestion_run_id
                 AND r.accession_number IS NULL""",
            (run_id,),
        )
        submission_children = [table.source_file for table in contract.tables if "SUBMISSION.tsv" in table.logical_parents]
        holding_children = [table.source_file for table in contract.tables if "FUND_REPORTED_HOLDING.tsv" in table.logical_parents]
        if holding_children:
            cur.execute(
                """SELECT source_table, source_row_number FROM nport_raw_rows
                   WHERE ingestion_run_id = %s AND source_table = ANY(%s)
                     AND (holding_id IS NULL OR typed_projection->>'HOLDING_ID' IS NULL)
                   LIMIT 1""",
                (run_id, holding_children),
            )
            if cur.fetchone():
                raise NportIngestionError("HOLDING_ID filho em branco ou inválido")
        if submission_children:
            cur.execute(
                """SELECT r.source_table, r.source_row_number
                   FROM nport_raw_rows r
                   LEFT JOIN (
                       SELECT accession_number, count(*) AS n
                       FROM nport_raw_rows
                       WHERE ingestion_run_id = %s AND source_table = 'SUBMISSION.tsv'
                         AND typed_projection->>'ACCESSION_NUMBER' IS NOT NULL
                       GROUP BY accession_number
                   ) s ON s.accession_number = r.accession_number
                   WHERE r.ingestion_run_id = %s AND r.source_table = ANY(%s)
                     AND (r.accession_number IS NULL OR s.n IS DISTINCT FROM 1)
                   LIMIT 1""",
                (run_id, run_id, submission_children),
            )
            if cur.fetchone():
                raise NportIngestionError("ACCESSION_NUMBER órfão, ambíguo ou em branco")


def _validate_candidate_keys(conn: psycopg.Connection, *, run_id: UUID) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT source_table, candidate_key_evidence->'values'
               FROM nport_raw_rows WHERE ingestion_run_id = %s
                 AND (candidate_key_evidence->>'complete')::boolean
               GROUP BY source_table, candidate_key_evidence->'values' HAVING count(*) > 1 LIMIT 1""",
            (run_id,),
        )
        if cur.fetchone():
            raise NportIngestionError("candidate key N-PORT ambígua")

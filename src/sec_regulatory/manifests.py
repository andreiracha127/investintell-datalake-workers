"""Estado transacional e reconciliação das fontes regulatórias SEC."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg

from src.db import connect


ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = ROOT / "schemas" / "sec_source_manifests.sql"


class ManifestStateError(RuntimeError):
    """Indica transição concorrente ou incompatível com o estado persistido."""


class RawValidationError(RuntimeError):
    """Indica que as contagens brutas não fecham exatamente."""


@dataclass(frozen=True)
class PackageStatus:
    """Inventário de um pacote SEC observado, sem alterar a identidade da execução."""

    package_id: UUID
    source_family: str
    source_quarter: str
    package_relative_path: str
    package_sha256: str | None
    package_state: str
    reason: str | None
    run_id: UUID | None
    duplicate_of_package_id: UUID | None


@dataclass(frozen=True)
class RunStatus:
    """Resumo imutável do estado atual de uma execução de fonte."""

    run_id: UUID
    source_family: str
    package_sha256: str
    parser_version: str
    source_quarter: str
    package_relative_path: str
    current_state: str
    raw_validated_at: datetime | None
    published_at: datetime | None
    retry_count: int


_NEXT_STATES = {
    "discovered": {"loading"},
    "loading": set(),  # raw_validated exige validate_raw_run().
    "raw_validated": {"derived_building"},
    "derived_building": {"derived_validated"},
    "derived_validated": {"published"},
}


def _status(row: tuple[Any, ...]) -> RunStatus:
    return RunStatus(*row)


def install_schema(conn: psycopg.Connection | None = None, *, dsn: str | None = None) -> None:
    """Instala o DDL idempotente; uma conexão recebida permanece sob controle do chamador."""
    if conn is None:
        with connect(dsn) as owned_conn:
            install_schema(owned_conn)
            owned_conn.commit()
        return
    with conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text(encoding="utf-8"))


def create_or_resume_run(
    conn: psycopg.Connection,
    *,
    source_family: str,
    package_sha256: str,
    parser_version: str,
    source_quarter: str,
    package_relative_path: str,
    run_id: UUID | None = None,
) -> RunStatus:
    """Cria ou retoma deterministicamente a execução pela chave de negócio."""
    candidate_id = run_id or uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_ingestion_runs
                (run_id, source_family, package_sha256, parser_version, source_quarter, package_relative_path)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_family, package_sha256, parser_version) DO NOTHING
            """,
            (candidate_id, source_family, package_sha256, parser_version, source_quarter, package_relative_path),
        )
        cur.execute(
            """
            SELECT run_id, source_family, package_sha256, parser_version, source_quarter,
                   package_relative_path, current_state, raw_validated_at, published_at, retry_count
            FROM sec_ingestion_runs
            WHERE source_family = %s AND package_sha256 = %s AND parser_version = %s
            FOR UPDATE
            """,
            (source_family, package_sha256, parser_version),
        )
        row = cur.fetchone()
    if row is None:
        raise ManifestStateError("não foi possível localizar a execução idempotente")
    status = _status(row)
    if (
        status.source_quarter != source_quarter
        or status.package_relative_path != package_relative_path
    ):
        raise ManifestStateError("metadados conflitantes para a mesma chave de execução")
    return status


def register_package_discovery(
    conn: psycopg.Connection,
    *,
    source_family: str,
    source_quarter: str,
    package_relative_path: str,
    package_state: str,
    package_sha256: str | None = None,
    metadata_sha256: str | None = None,
    readme_sha256: str | None = None,
    reason: str | None = None,
    run_id: UUID | None = None,
    duplicate_of_package_id: UUID | None = None,
    package_id: UUID | None = None,
) -> PackageStatus:
    """Registra cada pasta descoberta, inclusive duplicatas e pacotes não suportados."""
    candidate_id = package_id or uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_source_packages (
                package_id, source_family, source_quarter, package_relative_path, package_sha256,
                metadata_sha256, readme_sha256, package_state, reason, run_id, duplicate_of_package_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_family, package_relative_path) DO UPDATE SET
                source_quarter = EXCLUDED.source_quarter,
                package_sha256 = COALESCE(EXCLUDED.package_sha256, sec_source_packages.package_sha256),
                metadata_sha256 = COALESCE(EXCLUDED.metadata_sha256, sec_source_packages.metadata_sha256),
                readme_sha256 = COALESCE(EXCLUDED.readme_sha256, sec_source_packages.readme_sha256),
                package_state = EXCLUDED.package_state,
                reason = COALESCE(EXCLUDED.reason, sec_source_packages.reason),
                run_id = COALESCE(EXCLUDED.run_id, sec_source_packages.run_id),
                duplicate_of_package_id = COALESCE(EXCLUDED.duplicate_of_package_id, sec_source_packages.duplicate_of_package_id),
                updated_at = now()
            RETURNING package_id, source_family, source_quarter, package_relative_path, package_sha256,
                      package_state, reason, run_id, duplicate_of_package_id
            """,
            (candidate_id, source_family, source_quarter, package_relative_path, package_sha256,
             metadata_sha256, readme_sha256, package_state, reason, run_id, duplicate_of_package_id),
        )
        row = cur.fetchone()
    return PackageStatus(*row)


def register_file(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    relative_path: str,
    sha256: str,
    byte_size: int,
    schema_metadata: dict[str, Any] | None = None,
    readme_metadata: dict[str, Any] | None = None,
    expected_count: int = 0,
    data_count: int = 0,
    lexical_count: int = 0,
    typed_success_count: int = 0,
    quarantine_count: int = 0,
    reject_count: int = 0,
    state: str = "accounted",
    source_file_id: UUID | None = None,
) -> UUID:
    """Registra ou atualiza um manifesto de arquivo antes da validação bruta."""
    candidate_id = source_file_id or uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_source_files (
                source_file_id, run_id, relative_path, sha256, byte_size, schema_metadata, readme_metadata,
                expected_count, data_count, lexical_count, typed_success_count, quarantine_count, reject_count, state
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, relative_path) DO UPDATE SET
                sha256 = EXCLUDED.sha256, byte_size = EXCLUDED.byte_size,
                schema_metadata = EXCLUDED.schema_metadata, readme_metadata = EXCLUDED.readme_metadata,
                expected_count = EXCLUDED.expected_count, data_count = EXCLUDED.data_count,
                lexical_count = EXCLUDED.lexical_count, typed_success_count = EXCLUDED.typed_success_count,
                quarantine_count = EXCLUDED.quarantine_count, reject_count = EXCLUDED.reject_count,
                state = EXCLUDED.state, updated_at = now()
            WHERE sec_source_files.sha256 = EXCLUDED.sha256
              AND sec_source_files.byte_size = EXCLUDED.byte_size
            RETURNING source_file_id
            """,
            (candidate_id, run_id, relative_path, sha256, byte_size,
             _json(schema_metadata), _json(readme_metadata), expected_count, data_count,
             lexical_count, typed_success_count, quarantine_count, reject_count, state),
        )
        row = cur.fetchone()
    if row is None:
        raise ManifestStateError("conteúdo conflitante para o mesmo arquivo de origem")
    return row[0]


def register_table_reconciliation(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    source_file_id: UUID,
    table_name: str,
    expected_count: int = 0,
    source_count: int = 0,
    lexical_count: int = 0,
    typed_success_count: int = 0,
    quarantine_count: int = 0,
    reject_count: int = 0,
    state: str = "accounted",
) -> None:
    """Registra a reconciliação de uma tabela declarada em um arquivo SEC."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT run_id FROM sec_source_files WHERE source_file_id = %s FOR UPDATE",
            (source_file_id,),
        )
        file_row = cur.fetchone()
        if file_row is None or file_row[0] != run_id:
            raise ManifestStateError("arquivo de origem não pertence à execução informada")
        cur.execute(
            """SELECT run_id FROM sec_table_reconciliations
               WHERE source_file_id = %s AND table_name = %s FOR UPDATE""",
            (source_file_id, table_name),
        )
        existing = cur.fetchone()
        if existing is not None and existing[0] != run_id:
            raise ManifestStateError("reconciliação de tabela pertence a outra execução")
        cur.execute(
            """
            INSERT INTO sec_table_reconciliations
                (run_id, source_file_id, table_name, expected_count, source_count, lexical_count,
                 typed_success_count, quarantine_count, reject_count, state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_file_id, table_name) DO UPDATE SET
                expected_count = EXCLUDED.expected_count, source_count = EXCLUDED.source_count,
                lexical_count = EXCLUDED.lexical_count, typed_success_count = EXCLUDED.typed_success_count,
                quarantine_count = EXCLUDED.quarantine_count, reject_count = EXCLUDED.reject_count,
                state = EXCLUDED.state, updated_at = now()
            """,
            (run_id, source_file_id, table_name, expected_count, source_count, lexical_count,
             typed_success_count, quarantine_count, reject_count, state),
        )


def record_issue(
    conn: psycopg.Connection,
    *,
    source_file_id: UUID,
    source_row_number: int,
    typed_error_code: str,
    status: str,
    issue_sequence: int = 1,
    table_name: str | None = None,
    column_name: str | None = None,
    raw_lexical_value: str | None = None,
    typed_error_detail: str | None = None,
) -> None:
    """Acrescenta uma evidência de erro tipado sem apagar a evidência lexical."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_row_issues
                (source_file_id, source_row_number, issue_sequence, table_name, column_name,
                 raw_lexical_value, typed_error_code, typed_error_detail, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (source_file_id, source_row_number, issue_sequence, table_name, column_name,
             raw_lexical_value, typed_error_code, typed_error_detail, status),
        )


def transition_run(
    conn: psycopg.Connection, *, run_id: UUID, expected_state: str, target_state: str, detail: str | None = None
) -> RunStatus:
    """Avança uma execução com guarda otimista do estado esperado."""
    if target_state not in _NEXT_STATES.get(expected_state, set()):
        raise ManifestStateError(f"transição inválida: {expected_state} -> {target_state}")
    published = "now()" if target_state == "published" else "NULL"
    with conn.cursor() as cur:
        cur.execute("SELECT set_config('sec.lifecycle_detail', %s, true)", (detail or "",))
        cur.execute(
            f"""
            UPDATE sec_ingestion_runs
            SET current_state = %s, published_at = {published}, updated_at = now()
            WHERE run_id = %s AND current_state = %s
            RETURNING run_id, source_family, package_sha256, parser_version, source_quarter,
                      package_relative_path, current_state, raw_validated_at, published_at, retry_count
            """,
            (target_state, run_id, expected_state),
        )
        row = cur.fetchone()
        if row is None:
            raise ManifestStateError("estado esperado não corresponde à execução persistida")
    return _status(row)


def fail_run(
    conn: psycopg.Connection, *, run_id: UUID, expected_state: str, failure_code: str, failure_detail: str | None = None
) -> RunStatus:
    """Marca falha auditável, preservando o ponto determinístico de retomada."""
    if expected_state == "published":
        raise ManifestStateError("uma execução publicada é terminal")
    with conn.cursor() as cur:
        audit_detail = failure_code if not failure_detail else f"{failure_code}: {failure_detail}"
        cur.execute("SELECT set_config('sec.lifecycle_detail', %s, true)", (audit_detail,))
        cur.execute(
            """
            UPDATE sec_ingestion_runs
            SET current_state = 'failed', retry_state = current_state, failure_code = %s,
                failure_detail = %s, updated_at = now()
            WHERE run_id = %s AND current_state = %s
            RETURNING run_id, source_family, package_sha256, parser_version, source_quarter,
                      package_relative_path, current_state, raw_validated_at, published_at, retry_count
            """,
            (failure_code, failure_detail, run_id, expected_state),
        )
        row = cur.fetchone()
        if row is None:
            raise ManifestStateError("estado esperado não corresponde à execução persistida")
    return _status(row)


def retry_run(conn: psycopg.Connection, *, run_id: UUID, detail: str | None = None) -> RunStatus:
    """Retoma uma falha exatamente no estado que a precedeu."""
    with conn.cursor() as cur:
        cur.execute("SELECT set_config('sec.lifecycle_detail', %s, true)", (detail or "",))
        cur.execute(
            """
            UPDATE sec_ingestion_runs
            SET current_state = retry_state, retry_state = NULL, failure_code = NULL, failure_detail = NULL,
                retry_count = retry_count + 1, updated_at = now()
            WHERE run_id = %s AND current_state = 'failed'
            RETURNING run_id, source_family, package_sha256, parser_version, source_quarter,
                      package_relative_path, current_state, raw_validated_at, published_at, retry_count
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise ManifestStateError("somente uma execução falha pode ser retomada")
    return _status(row)


def validate_raw_run(conn: psycopg.Connection, *, run_id: UUID) -> RunStatus:
    """Fecha as contas brutas e cria a visibilidade validada na mesma transação."""
    with conn.cursor() as cur:
        cur.execute("SELECT current_state FROM sec_ingestion_runs WHERE run_id = %s FOR UPDATE", (run_id,))
        state_row = cur.fetchone()
        if state_row is None or state_row[0] != "loading":
            raise ManifestStateError("a validação bruta exige uma execução em loading")
        cur.execute(
            """
            SELECT
                EXISTS (SELECT 1 FROM sec_source_files WHERE run_id = %s),
                EXISTS (SELECT 1 FROM sec_source_files WHERE run_id = %s AND state <> 'accounted'),
                EXISTS (SELECT 1 FROM sec_table_reconciliations WHERE run_id = %s AND state <> 'accounted'),
                EXISTS (
                    SELECT 1 FROM sec_source_files AS f
                    LEFT JOIN LATERAL (
                        SELECT count(DISTINCT source_row_number) FILTER (WHERE status = 'quarantined') AS quarantined,
                               count(DISTINCT source_row_number) FILTER (WHERE status = 'rejected') AS rejected
                        FROM sec_row_issues WHERE source_file_id = f.source_file_id
                    ) AS i ON TRUE
                    WHERE f.run_id = %s
                      AND (f.quarantine_count <> i.quarantined OR f.reject_count <> i.rejected)
                ),
                EXISTS (
                    SELECT 1 FROM sec_source_files
                    WHERE run_id = %s AND (
                        expected_count <> data_count OR data_count <> lexical_count OR
                        lexical_count <> typed_success_count + quarantine_count + reject_count
                    )
                    UNION ALL
                    SELECT 1 FROM sec_table_reconciliations
                    WHERE run_id = %s AND (
                        expected_count <> source_count OR source_count <> lexical_count OR
                        lexical_count <> typed_success_count + quarantine_count + reject_count
                    )
                )
            """,
            (run_id, run_id, run_id, run_id, run_id, run_id),
        )
        has_files, bad_file_state, bad_table_state, bad_issues, bad_counts = cur.fetchone()
        if not has_files:
            raise RawValidationError("a validação bruta exige ao menos um arquivo registrado")
        if bad_file_state or bad_table_state:
            raise RawValidationError("todos os manifestos de arquivo e tabela devem estar accounted")
        if bad_issues:
            raise RawValidationError("as contagens de issues quarentenadas/rejeitadas não reconciliam")
        if bad_counts:
            raise RawValidationError("as contagens source/lexical/tipadas não reconciliam exatamente")
        cur.execute("SELECT sec_validate_raw_run(%s, %s)", (run_id, "reconciliação exata"))
        if cur.fetchone() is None:
            raise ManifestStateError("a execução mudou durante a validação bruta")
    status = get_run_status(conn, run_id=run_id)
    if status is None:
        raise ManifestStateError("a execução não existe após a validação")
    return status


def get_run_status(conn: psycopg.Connection, *, run_id: UUID) -> RunStatus | None:
    """Consulta o estado de uma execução sem alterar seu ciclo de vida."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT run_id, source_family, package_sha256, parser_version, source_quarter,
                      package_relative_path, current_state, raw_validated_at, published_at, retry_count
               FROM sec_ingestion_runs WHERE run_id = %s""",
            (run_id,),
        )
        row = cur.fetchone()
    return _status(row) if row is not None else None


def is_raw_visible(conn: psycopg.Connection, *, run_id: UUID) -> bool:
    """Informa se a execução aparece na superfície de dados brutos validados."""
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM sec_validated_raw_runs WHERE run_id = %s)", (run_id,))
        return bool(cur.fetchone()[0])


def _json(value: dict[str, Any] | None) -> str:
    """Serializa metadados opcionais sem interpolar SQL."""
    import json

    return json.dumps(value or {}, sort_keys=True)

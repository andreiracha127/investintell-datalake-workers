"""Leitura estrita dos contratos de tabelas regulatórias congelados."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ContractError(ValueError):
    """O pacote não corresponde ao contrato SEC versionado."""


ALLOWED_PARSING_POLICIES = frozenset({
    "date_field_specific_fail_preserve_lexical",
    "decimal_preserve_lexical",
    "text_preserve_lexical",
})
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class ContractColumn:
    name: str
    parsing_policy: str
    required: bool
    candidate_key: bool
    datatype: dict[str, Any]


@dataclass(frozen=True)
class ContractTable:
    source_file: str
    raw_target: str
    columns: tuple[ContractColumn, ...]
    candidate_primary_key: tuple[str, ...] = ()
    logical_parents: tuple[str, ...] = ()

    @property
    def headers(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def candidate_key(self) -> tuple[str, ...]:
        return self.candidate_primary_key

    @property
    def required_table(self) -> bool:
        """Somente a fundação de submissões é física e incondicionalmente obrigatória."""
        return self.source_file == "SUBMISSION.tsv"


@dataclass(frozen=True)
class SourceTableContract:
    family: str
    contract_version: str
    metadata_sha256: str
    tables: tuple[ContractTable, ...]

    @property
    def required_filenames(self) -> frozenset[str]:
        return frozenset(table.source_file for table in self.tables)

    def table_for_filename(self, filename: str) -> ContractTable:
        for table in self.tables:
            if table.source_file == filename:
                return table
        raise ContractError(f"arquivo TSV fora do contrato: {filename}")


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Calcula SHA-256 sem materializar o arquivo de origem."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def load_source_table_contract(
    path: Path, *, family: str, metadata_sha256: str | None = None,
) -> SourceTableContract:
    """Seleciona fail-closed a variante governada pela SHA de metadata."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("family") != family:
        raise ContractError(f"família contratada inválida: {document.get('family')!r}")
    contract_version = document.get("contract_version")
    if not isinstance(contract_version, str) or not contract_version:
        raise ContractError("contract_version ausente ou inválido")
    variants = document.get("schema_variants")
    if not isinstance(variants, list) or not variants:
        raise ContractError("contrato sem variantes de schema")
    variant_hashes = [variant.get("metadata_sha256") for variant in variants if isinstance(variant, dict)]
    if len(variant_hashes) != len(variants) or any(
        not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None for value in variant_hashes
    ):
        raise ContractError("variante com metadata_sha256 ausente ou inválido")
    if len(set(variant_hashes)) != len(variant_hashes):
        raise ContractError("metadata_sha256 duplicado entre variantes")
    candidates = [variant for variant in variants if metadata_sha256 is None or variant.get("metadata_sha256") == metadata_sha256]
    if len(candidates) != 1:
        raise ContractError("metadata não seleciona exatamente uma variante governada")
    variant = candidates[0]
    metadata_sha256 = variant.get("metadata_sha256")
    tables_data = variant.get("tables")
    if not isinstance(metadata_sha256, str) or _SHA256_RE.fullmatch(metadata_sha256) is None:
        raise ContractError("metadata_sha256 ausente ou inválido")
    if not isinstance(tables_data, list) or not tables_data:
        raise ContractError("contrato sem tabelas")
    tables: list[ContractTable] = []
    seen_files: set[str] = set()
    seen_targets: set[str] = set()
    for item in tables_data:
        if not isinstance(item, dict):
            raise ContractError("definição de tabela inválida")
        source_file = item.get("source_file")
        columns_data = item.get("columns")
        if not isinstance(source_file, str) or not source_file.endswith(".tsv"):
            raise ContractError("nome de arquivo do contrato inválido")
        if source_file in seen_files or not isinstance(columns_data, list) or not columns_data:
            raise ContractError(f"definição duplicada ou sem colunas: {source_file}")
        columns: list[ContractColumn] = []
        for column in columns_data:
            if not isinstance(column, dict):
                raise ContractError(f"coluna inválida no contrato: {source_file}")
            name = column.get("name")
            policy = column.get("parsing_policy")
            datatype = column.get("datatype")
            if not isinstance(name, str) or not name:
                raise ContractError(f"nome de coluna inválido: {source_file}")
            if policy not in ALLOWED_PARSING_POLICIES:
                raise ContractError(f"política de parsing inválida em {source_file}.{name}: {policy!r}")
            if not isinstance(datatype, dict) or not isinstance(datatype.get("base"), str):
                raise ContractError(f"datatype inválido em {source_file}.{name}")
            if not isinstance(column.get("required"), bool) or not isinstance(column.get("candidate_key"), bool):
                raise ContractError(f"flags required/candidate_key inválidos em {source_file}.{name}")
            columns.append(ContractColumn(
                name=name, parsing_policy=policy, required=column["required"],
                candidate_key=column["candidate_key"], datatype=dict(datatype),
            ))
        columns_tuple = tuple(columns)
        target = item.get("raw_target") or columns_data[0].get("raw_target")
        if not isinstance(target, str) or not target.endswith("_raw"):
            raise ContractError(f"raw_target inválido para {source_file}")
        column_targets = {column.get("raw_target") for column in columns_data}
        if column_targets != {target}:
            raise ContractError(f"raw_target divergente entre colunas de {source_file}")
        if target in seen_targets:
            raise ContractError(f"raw_target duplicado no contrato: {target}")
        if len({column.name for column in columns_tuple}) != len(columns_tuple):
            raise ContractError(f"colunas duplicadas no contrato: {source_file}")
        candidate_primary_key = item.get("candidate_primary_key")
        if not isinstance(candidate_primary_key, list) or any(
            not isinstance(name, str) for name in candidate_primary_key
        ):
            raise ContractError(f"candidate_primary_key inválida: {source_file}")
        known_columns = {column.name for column in columns_tuple}
        flagged_key = {column.name for column in columns_tuple if column.candidate_key}
        if (
            len(set(candidate_primary_key)) != len(candidate_primary_key)
            or any(name not in known_columns for name in candidate_primary_key)
            or flagged_key != set(candidate_primary_key)
        ):
            raise ContractError(f"candidate_primary_key diverge das colunas: {source_file}")
        logical_parents = item.get("logical_parents")
        if not isinstance(logical_parents, list) or any(
            not isinstance(parent, str) or not parent.endswith(".tsv") for parent in logical_parents
        ):
            raise ContractError(f"logical_parents inválido: {source_file}")
        if source_file in logical_parents or len(set(logical_parents)) != len(logical_parents):
            raise ContractError(f"logical_parents cíclico ou duplicado: {source_file}")
        seen_files.add(source_file)
        seen_targets.add(target)
        tables.append(ContractTable(
            source_file=source_file, raw_target=target, columns=columns_tuple,
            candidate_primary_key=tuple(candidate_primary_key),
            logical_parents=tuple(logical_parents),
        ))
    unknown_parents = {
        parent for table in tables for parent in table.logical_parents if parent not in seen_files
    }
    if unknown_parents:
        raise ContractError(f"logical_parents fora do contrato: {sorted(unknown_parents)}")
    return SourceTableContract(
        family=family, contract_version=contract_version,
        metadata_sha256=metadata_sha256, tables=tuple(tables),
    )

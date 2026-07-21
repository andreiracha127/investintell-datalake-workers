"""Contrato e parsing tipado do pacote N-PORT."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
from typing import Any

from src.sec_regulatory.contracts import ContractColumn, ContractError, SourceTableContract, load_source_table_contract, sha256_file


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts" / "sec-regulatory" / "v1" / "source-tables" / "nport.json"
MAX_DECIMAL_INTEGRAL_DIGITS = 131_072
MAX_DECIMAL_FRACTIONAL_DIGITS = 16_383
_DECIMAL_RE = re.compile(r"^[+-]?(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+))?$")
_SEC_DATE_RE = re.compile(r"^\d{2}-[A-Z]{3}-\d{4}$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SEC_MONTHS = {name: month for month, name in enumerate(
    ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), start=1,
)}


@dataclass(frozen=True)
class RowIssue:
    column_name: str
    code: str
    raw_value: str
    detail: str


@dataclass(frozen=True)
class ParsedRow:
    lexical: dict[str, str]
    typed: dict[str, Any]
    issues: tuple[RowIssue, ...]
    candidate_key_evidence: dict[str, Any]

    @property
    def parse_status(self) -> str:
        return "quarantined" if self.issues else "typed"


def load_nport_contract() -> SourceTableContract:
    """Carrega o contrato N-PORT V1 congelado no repositório."""
    return load_source_table_contract(CONTRACT_PATH, family="nport")


def contract_catalog_payload(contract: SourceTableContract | None = None) -> list[dict[str, Any]]:
    """Canonical DB catalog projection; PostgreSQL pins its JSONB SHA-256."""
    selected = contract or load_nport_contract()
    return [
        {
            "table_ordinal": ordinal,
            "source_table": table.source_file,
            "raw_target": table.raw_target,
            "logical_parents": list(table.logical_parents),
            "candidate_key": list(table.candidate_key),
            "columns": [column.name for column in table.columns],
            "required_columns": [column.name for column in table.columns if column.required],
            "column_specs": [
                {
                    "name": column.name,
                    "parsing_policy": column.parsing_policy,
                    "required": column.required,
                    "datatype": column.datatype,
                }
                for column in table.columns
            ],
        }
        for ordinal, table in enumerate(selected.tables, start=1)
    ]


@dataclass(frozen=True)
class VerifiedPackage:
    file_hashes: dict[str, str]
    metadata_sha256: str
    readme_sha256: str


def verify_package(package: Path, contract: SourceTableContract) -> VerifiedPackage:
    """Valida a família fechada antes de qualquer parsing de linha."""
    metadata = package / "nport_metadata.json"
    if not metadata.is_file():
        raise ContractError("nport_metadata.json ausente")
    metadata_hash = sha256_file(metadata)
    if metadata_hash != contract.metadata_sha256:
        raise ContractError("SHA-256 de nport_metadata.json diverge do contrato")
    readme = package / "nport_readme.htm"
    if not readme.is_file():
        raise ContractError("nport_readme.htm ausente")
    actual = {path.name for path in package.iterdir() if path.is_file() and path.suffix.lower() == ".tsv"}
    required_filenames = {table.source_file for table in contract.tables if table.required_table}
    # FUND_REPORTED_HOLDING pode faltar apenas em pacote sem qualquer filho
    # HOLDING_ID; o 2019Q4 prova que uma tabela declarada pode ser zero-row
    # inclusive quando metadata enumera todas as 30 tabelas.
    holding_children = [
        table for table in contract.tables
        if table.source_file != "FUND_REPORTED_HOLDING.tsv" and "HOLDING_ID" in table.headers
        and table.source_file in actual
    ]
    if holding_children:
        required_filenames.add("FUND_REPORTED_HOLDING.tsv")
    missing = required_filenames - actual
    unknown = actual - contract.required_filenames
    if missing or unknown:
        raise ContractError(f"conjunto TSV fechado inválido; missing={sorted(missing)}, unknown={sorted(unknown)}")
    hashes: dict[str, str] = {}
    from src.sec_regulatory.tsv import stream_tsv
    for table in contract.tables:
        if table.source_file not in actual:
            # A ausência de tabela declarada inteiramente opcional é evidência
            # explícita de zero linhas, materializada pelo worker no manifesto.
            continue
        header, rows = stream_tsv(package / table.source_file)
        if header != table.headers:
            raise ContractError(f"cabeçalho ou ordem divergente: {table.source_file}")
        # Fecha o handle do iterador sem iterar os dados: apenas a validação de cabeçalho.
        close = getattr(rows, "close", None)
        if close:
            close()
        hashes[table.source_file] = sha256_file(package / table.source_file)
    return VerifiedPackage(file_hashes=hashes, metadata_sha256=metadata_hash, readme_sha256=sha256_file(readme))


def package_sha256(file_hashes: dict[str, str], *, metadata_sha256: str, readme_sha256: str) -> str:
    """Hash determinístico de nomes relativos governados e hashes de arquivos."""
    import hashlib

    digest = hashlib.sha256()
    governed = {"nport_metadata.json": metadata_sha256, "nport_readme.htm": readme_sha256, **file_hashes}
    for relative_name in sorted(governed):
        digest.update(relative_name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(governed[relative_name].encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def parse_row(columns: tuple[ContractColumn, ...], values: tuple[str, ...]) -> ParsedRow:
    """Preserva cada valor lexical e produz a projeção tipada ou issues estáveis."""
    lexical = dict(zip((column.name for column in columns), values, strict=True))
    typed: dict[str, Any] = {}
    issues: list[RowIssue] = []
    for column in columns:
        raw = lexical[column.name]
        if not raw:
            typed[column.name] = None
            if column.required:
                issues.append(RowIssue(column.name, "required_blank", raw, "campo obrigatório vazio"))
            continue
        try:
            typed[column.name] = _parse_value(column, raw)
        except ValueError as error:
            typed[column.name] = None
            code = {
                "decimal_preserve_lexical": "invalid_decimal",
                "date_field_specific_fail_preserve_lexical": "invalid_date",
                "text_preserve_lexical": "invalid_text",
            }.get(column.parsing_policy, "invalid_value")
            if column.parsing_policy == "decimal_preserve_lexical" and str(error) == "decimal fora do domínio governado":
                code = "decimal_out_of_domain"
            issues.append(RowIssue(column.name, code, raw, str(error)))
    key = tuple(column.name for column in columns if column.candidate_key)
    complete = bool(key) and all(lexical[name] != "" for name in key) and not any(issue.column_name in key for issue in issues)
    # Evidence is always a positional projection of the immutable lexical row;
    # ``complete`` says whether those values may participate in uniqueness.
    evidence = {"columns": list(key), "complete": complete, "values": [lexical[name] for name in key]}
    return ParsedRow(lexical=lexical, typed=typed, issues=tuple(issues), candidate_key_evidence=evidence)


def json_typed_projection(typed: dict[str, Any]) -> dict[str, Any]:
    """Serializa tipos sem perder a evidência lexical armazenada separadamente."""
    return {name: value.isoformat() if hasattr(value, "isoformat") else format(value, "f") if isinstance(value, Decimal) else value
            for name, value in typed.items()}


def _parse_value(column: ContractColumn, raw: str) -> Any:
    if column.parsing_policy == "text_preserve_lexical":
        max_length = column.datatype.get("maxLength")
        if isinstance(max_length, int) and len(raw) > max_length:
            raise ValueError(f"texto excede maxLength={max_length}")
        return raw
    if column.parsing_policy == "decimal_preserve_lexical":
        matched = _DECIMAL_RE.fullmatch(raw)
        if matched is None:
            raise ValueError("decimal inválido")
        integer = matched.group(1) or ""
        fraction = (matched.group(2) or "") if matched.group(1) is not None else (matched.group(3) or "")
        exponent_text = matched.group(4) or "0"
        # Avoid parsing/expanding an attacker-controlled exponent before the
        # PostgreSQL NUMERIC storage domain is known to contain it.
        unsigned_exponent = exponent_text.lstrip("+-0")
        if len(unsigned_exponent) > 6:
            raise ValueError("decimal fora do domínio governado")
        exponent = int(exponent_text)
        coefficient = integer + fraction
        first_nonzero = next((index for index, digit in enumerate(coefficient) if digit != "0"), len(coefficient))
        integral_digits = max(len(integer) + exponent - first_nonzero, 0)
        fractional_digits = max(len(fraction) - exponent, 0)
        if integral_digits > MAX_DECIMAL_INTEGRAL_DIGITS or fractional_digits > MAX_DECIMAL_FRACTIONAL_DIGITS:
            raise ValueError("decimal fora do domínio governado")
        try:
            value = Decimal(raw)
        except InvalidOperation as error:
            raise ValueError("decimal inválido") from error
        if not value.is_finite():
            raise ValueError("decimal não finito")
        return value
    if column.parsing_policy == "date_field_specific_fail_preserve_lexical":
        try:
            if _SEC_DATE_RE.fullmatch(raw):
                return date(int(raw[7:11]), _SEC_MONTHS[raw[3:6]], int(raw[0:2]))
            if _ISO_DATE_RE.fullmatch(raw):
                return date.fromisoformat(raw)
        except (KeyError, ValueError):
            pass
        raise ValueError("data inválida para o campo")
    raise ValueError(f"política de parsing não reconhecida: {column.parsing_policy}")

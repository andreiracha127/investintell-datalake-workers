"""Frozen RR1 contract selection, physical closure, and lossless typing."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from src.sec_regulatory.contracts import ContractColumn, ContractError, SourceTableContract, load_source_table_contract, sha256_file
from .tsv import stream_tsv

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts" / "sec-regulatory" / "v1" / "source-tables" / "rr1.json"
DATA_FILES = frozenset(("sub.tsv", "tag.tsv", "lab.tsv", "cal.tsv", "num.tsv", "txt.tsv"))
_DECIMAL_LEXICAL_RE = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


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


@dataclass(frozen=True)
class VerifiedPackage:
    contract: SourceTableContract
    file_hashes: dict[str, str]
    metadata_sha256: str
    readme_sha256: str
    metadata_filename: str


def load_rr1_contract(metadata_sha256: str) -> SourceTableContract:
    return load_source_table_contract(CONTRACT_PATH, family="rr1", metadata_sha256=metadata_sha256)


def _metadata_file(package: Path) -> Path:
    matches = [path for path in package.iterdir() if path.is_file() and path.name.endswith("-metadata.json")]
    if len(matches) != 1:
        raise ContractError("RR1 exige exatamente um arquivo *-metadata.json")
    return matches[0]


def verify_package(package: Path) -> VerifiedPackage:
    """Bind metadata SHA before accepting the exact six physical RR1 TSVs."""
    metadata = _metadata_file(package)
    readme = package / "readme.htm"
    if not readme.is_file():
        raise ContractError("readme.htm ausente")
    metadata_hash = sha256_file(metadata)
    contract = load_rr1_contract(metadata_hash)
    actual = {path.name for path in package.iterdir() if path.is_file() and path.suffix.lower() == ".tsv"}
    if actual != DATA_FILES or actual != contract.required_filenames:
        raise ContractError(f"conjunto TSV RR1 fechado inválido; missing={sorted(DATA_FILES - actual)}, extra={sorted(actual - DATA_FILES)}")
    hashes: dict[str, str] = {}
    for table in contract.tables:
        header, rows = stream_tsv(package / table.source_file)
        if header != table.headers:
            close = getattr(rows, "close", None)
            if close:
                close()
            raise ContractError(f"cabeçalho ou ordem divergente: {table.source_file}")
        rows.close()
        hashes[table.source_file] = sha256_file(package / table.source_file)
    return VerifiedPackage(contract, hashes, metadata_hash, sha256_file(readme), metadata.name)


def package_sha256(file_hashes: dict[str, str], *, metadata_sha256: str, readme_sha256: str, metadata_filename: str) -> str:
    digest = hashlib.sha256()
    for name, sha in sorted({metadata_filename: metadata_sha256, "readme.htm": readme_sha256, **file_hashes}.items()):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(sha.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def parse_row(columns: tuple[ContractColumn, ...], values: tuple[str, ...]) -> ParsedRow:
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
            code = {"decimal_preserve_lexical": "invalid_decimal", "date_field_specific_fail_preserve_lexical": "invalid_date", "text_preserve_lexical": "invalid_text"}.get(column.parsing_policy, "invalid_value")
            issues.append(RowIssue(column.name, code, raw, str(error)))
    key = tuple(column.name for column in columns if column.candidate_key)
    complete = bool(key) and all(lexical[name] for name in key) and not any(issue.column_name in key for issue in issues)
    return ParsedRow(lexical, typed, tuple(issues), {"columns": list(key), "complete": complete, "values": [lexical[name] for name in key] if complete else None})


def json_typed_projection(typed: dict[str, Any]) -> dict[str, Any]:
    return {name: value.isoformat() if hasattr(value, "isoformat") else str(value) if isinstance(value, Decimal) else value for name, value in typed.items()}


def _parse_value(column: ContractColumn, raw: str) -> Any:
    _require_lexical_length(column, raw)
    if column.parsing_policy == "text_preserve_lexical":
        return raw
    if column.parsing_policy == "decimal_preserve_lexical":
        if not _DECIMAL_LEXICAL_RE.fullmatch(raw):
            raise ValueError("decimal com léxico inválido")
        try:
            value = Decimal(raw)
        except InvalidOperation as error:
            raise ValueError("decimal inválido") from error
        if not value.is_finite():
            raise ValueError("decimal não finito")
        _require_decimal_bounds(column, value)
        return value
    if column.parsing_policy == "date_field_specific_fail_preserve_lexical":
        if column.name == "accepted":
            try:
                value = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError as error:
                raise ValueError("timestamp accepted inválido") from error
            _require_reasonable_year(value.date())
            return value
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%d-%b-%Y", "%d%m%Y"):
            try:
                value = datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
            _require_reasonable_year(value)
            # Only fact ddate is constrained to a month end.  Submission dates
            # (pdate, effdate, filed) are ordinary date-only fields.
            if column.name == "ddate" and (value + timedelta(days=1)).month == value.month:
                raise ValueError("data RR1 não é fim de mês")
            return value
        raise ValueError("data inválida para o campo")
    raise ValueError(f"política de parsing não reconhecida: {column.parsing_policy}")


def _require_reasonable_year(value: date) -> None:
    if not 1900 <= value.year <= 2027:
        raise ValueError("ano RR1 fora da faixa aceita")


def _require_lexical_length(column: ContractColumn, raw: str) -> None:
    minimum = column.datatype.get("minLength")
    maximum = column.datatype.get("maxLength")
    if isinstance(minimum, int) and len(raw) < minimum:
        raise ValueError(f"valor abaixo de minLength={minimum}")
    if isinstance(maximum, int) and len(raw) > maximum:
        raise ValueError(f"valor excede maxLength={maximum}")


def _require_decimal_bounds(column: ContractColumn, value: Decimal) -> None:
    minimum = column.datatype.get("minInclusive")
    maximum = column.datatype.get("maxInclusive")
    if minimum is not None and value < Decimal(str(minimum)):
        raise ValueError(f"decimal abaixo de minInclusive={minimum}")
    if maximum is not None and value > Decimal(str(maximum)):
        raise ValueError(f"decimal acima de maxInclusive={maximum}")

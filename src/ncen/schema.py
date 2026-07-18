"""Frozen N-CEN contract selection, package verification, and lossless typing."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from src.sec_regulatory.contracts import ContractColumn, ContractError, SourceTableContract, load_source_table_contract, sha256_file
from .inventory import CANONICAL_PACKAGE_INVENTORIES, PackageInventory, count_tsv_rows, inventory_digest

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts" / "sec-regulatory" / "v1" / "source-tables" / "ncen.json"

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
    file_inventory: dict[str, object]
    explicit_zero_tables: tuple[str, ...]
    package_sha256: str
    inventory_digest: str

def load_ncen_contract(metadata_sha256: str) -> SourceTableContract:
    return load_source_table_contract(CONTRACT_PATH, family="ncen", metadata_sha256=metadata_sha256)

def verify_package(package: Path, *, inventory: PackageInventory | None = None) -> VerifiedPackage:
    """Select exactly one metadata variant, then close the physical TSV set."""
    metadata = package / "ncen_metadata.json"
    readme = package / "ncen_readme.htm"
    if not metadata.is_file() or not readme.is_file():
        raise ContractError("ncen_metadata.json ou ncen_readme.htm ausente")
    metadata_hash = sha256_file(metadata)
    contract = load_ncen_contract(metadata_hash)
    expected = inventory or CANONICAL_PACKAGE_INVENTORIES.get(package.name)
    if expected is None:
        raise ContractError("pacote N-CEN fora do corpus canônico exige inventário explícito")
    if expected.metadata_sha256 != metadata_hash:
        raise ContractError("metadata SHA-256 diverge do inventário N-CEN imutável")
    actual = {path.name for path in package.iterdir() if path.is_file()}
    if actual != set(expected.files):
        raise ContractError(f"conjunto físico fechado inválido; missing={sorted(set(expected.files) - actual)}, unknown={sorted(actual - set(expected.files))}")
    physical_tsvs = {name for name in actual if name.lower().endswith(".tsv")}
    if physical_tsvs | set(expected.explicit_zero_tables) != contract.required_filenames or physical_tsvs & set(expected.explicit_zero_tables):
        raise ContractError("inventário físico e tabelas zero não fecham o contrato N-CEN")
    from src.sec_regulatory.tsv import stream_tsv
    hashes: dict[str, str] = {}
    for table in contract.tables:
        if table.source_file not in physical_tsvs:
            continue
        path = package / table.source_file
        evidence = expected.files[table.source_file]
        actual_hash = sha256_file(path)
        if actual_hash != evidence.sha256 or path.stat().st_size != evidence.byte_size or count_tsv_rows(path) != evidence.row_count:
            raise ContractError(f"evidência física divergente: {table.source_file}")
        header, rows = stream_tsv(path)
        if header != table.headers:
            raise ContractError(f"cabeçalho ou ordem divergente: {table.source_file}")
        close = getattr(rows, "close", None)
        if close:
            close()
        hashes[table.source_file] = actual_hash
    for name in ("ncen_metadata.json", "ncen_readme.htm"):
        path = package / name
        evidence = expected.files[name]
        if sha256_file(path) != evidence.sha256 or path.stat().st_size != evidence.byte_size:
            raise ContractError(f"evidência física divergente: {name}")
    if tuple(sorted(table.source_file for table in contract.tables if table.source_file not in physical_tsvs)) != expected.explicit_zero_tables:
        raise ContractError("tabelas zero explícitas divergem do inventário imutável")
    calculated_package_sha = package_sha256(hashes, metadata_sha256=metadata_hash, readme_sha256=sha256_file(readme))
    material = {"metadata_sha256": metadata_hash, "files": {name: {"sha256": item.sha256, "byte_size": item.byte_size, "row_count": item.row_count} for name, item in sorted(expected.files.items())}, "explicit_zero_tables": list(expected.explicit_zero_tables), "package_sha256": calculated_package_sha}
    if calculated_package_sha != expected.package_sha256 or inventory_digest(material) != expected.inventory_digest:
        raise ContractError("digest agregado do inventário N-CEN diverge")
    return VerifiedPackage(contract, hashes, metadata_hash, sha256_file(readme), dict(expected.files), expected.explicit_zero_tables, calculated_package_sha, expected.inventory_digest)

def package_sha256(file_hashes: dict[str, str], *, metadata_sha256: str, readme_sha256: str) -> str:
    import hashlib
    digest = hashlib.sha256()
    for name, sha in sorted({"ncen_metadata.json": metadata_sha256, "ncen_readme.htm": readme_sha256, **file_hashes}.items()):
        digest.update(name.encode()); digest.update(b"\0"); digest.update(sha.encode("ascii")); digest.update(b"\n")
    return digest.hexdigest()

def parse_row(columns: tuple[ContractColumn, ...], values: tuple[str, ...]) -> ParsedRow:
    lexical = dict(zip((column.name for column in columns), values, strict=True))
    typed: dict[str, Any] = {}; issues: list[RowIssue] = []
    for column in columns:
        raw = lexical[column.name]
        if not raw:
            typed[column.name] = None
            if column.required: issues.append(RowIssue(column.name, "required_blank", raw, "campo obrigatório vazio"))
            continue
        try: typed[column.name] = _parse_value(column, raw)
        except ValueError as error:
            typed[column.name] = None
            issues.append(RowIssue(column.name, {"decimal_preserve_lexical":"invalid_decimal", "date_field_specific_fail_preserve_lexical":"invalid_date", "text_preserve_lexical":"invalid_text"}.get(column.parsing_policy, "invalid_value"), raw, str(error)))
    key = tuple(column.name for column in columns if column.candidate_key)
    complete = bool(key) and all(lexical[name] for name in key) and not any(issue.column_name in key for issue in issues)
    return ParsedRow(lexical, typed, tuple(issues), {"columns": list(key), "complete": complete, "values": [lexical[name] for name in key] if complete else None})

def json_typed_projection(typed: dict[str, Any]) -> dict[str, Any]:
    # Fixed-point Decimal text is the canonical wire representation shared
    # with the DB-side numeric::text derivation (including exponent inputs).
    return {name: value.isoformat() if hasattr(value, "isoformat") else format(value, "f") if isinstance(value, Decimal) else value for name, value in typed.items()}

def _parse_value(column: ContractColumn, raw: str) -> Any:
    if column.parsing_policy == "text_preserve_lexical":
        maximum = column.datatype.get("maxLength")
        if isinstance(maximum, int) and len(raw) > maximum: raise ValueError(f"texto excede maxLength={maximum}")
        return raw
    if column.parsing_policy == "decimal_preserve_lexical":
        try: value = Decimal(raw)
        except InvalidOperation as error: raise ValueError("decimal inválido") from error
        if not value.is_finite(): raise ValueError("decimal não finito")
        return value
    if column.parsing_policy == "date_field_specific_fail_preserve_lexical":
        for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
            try: return datetime.strptime(raw, fmt).date()
            except ValueError: pass
        raise ValueError("data inválida para o campo")
    raise ValueError(f"política de parsing não reconhecida: {column.parsing_policy}")

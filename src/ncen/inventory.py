"""Immutable physical inventories for the frozen N-CEN delivery corpus."""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from src.sec_regulatory.contracts import ContractError, SourceTableContract, sha256_file

INVENTORY_DIR = Path(__file__).with_name("canonical_inventory")


@dataclass(frozen=True)
class FileEvidence:
    sha256: str
    byte_size: int
    row_count: int


@dataclass(frozen=True)
class PackageInventory:
    metadata_sha256: str
    files: Mapping[str, FileEvidence]
    explicit_zero_tables: tuple[str, ...]
    package_sha256: str
    inventory_digest: str

    def material(self) -> dict[str, object]:
        return {
            "metadata_sha256": self.metadata_sha256,
            "files": {name: {"sha256": item.sha256, "byte_size": item.byte_size, "row_count": item.row_count}
                      for name, item in sorted(self.files.items())},
            "explicit_zero_tables": list(self.explicit_zero_tables),
            "package_sha256": self.package_sha256,
        }


def inventory_digest(material: dict[str, object]) -> str:
    return hashlib.sha256(json.dumps(material, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def count_tsv_rows(path: Path) -> int:
    """Count physical records independently from the loader's streaming parser."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        try:
            next(reader)
        except StopIteration:
            raise ContractError(f"TSV sem cabeçalho: {path.name}")
        return sum(1 for _ in reader)


def build_package_inventory(package: Path, contract: SourceTableContract) -> PackageInventory:
    """Create an explicit inventory for a deliberately synthetic test package."""
    files: dict[str, FileEvidence] = {}
    for path in sorted(path for path in package.iterdir() if path.is_file()):
        files[path.name] = FileEvidence(sha256_file(path), path.stat().st_size, count_tsv_rows(path) if path.suffix.lower() == ".tsv" else 0)
    zeroes = tuple(sorted(table.source_file for table in contract.tables if table.source_file not in files))
    package_hash = hashlib.sha256(b"".join(name.encode() + b"\0" + files[name].sha256.encode("ascii") + b"\n" for name in sorted(files))).hexdigest()
    material = {"metadata_sha256": sha256_file(package / "ncen_metadata.json"), "files": {name: {"sha256": item.sha256, "byte_size": item.byte_size, "row_count": item.row_count} for name, item in sorted(files.items())}, "explicit_zero_tables": list(zeroes), "package_sha256": package_hash}
    return PackageInventory(material["metadata_sha256"], MappingProxyType(files), zeroes, package_hash, inventory_digest(material))


def _load() -> Mapping[str, PackageInventory]:
    inventories: dict[str, PackageInventory] = {}
    for inventory_path in sorted(INVENTORY_DIR.glob("*.json")):
        package = inventory_path.stem
        record = json.loads(inventory_path.read_text(encoding="utf-8"))
        files = MappingProxyType({name: FileEvidence(**evidence) for name, evidence in record["files"].items()})
        inventory = PackageInventory(record["metadata_sha256"], files, tuple(record["explicit_zero_tables"]), record["package_sha256"], record["inventory_digest"])
        if inventory_digest(inventory.material()) != inventory.inventory_digest:
            raise RuntimeError(f"inventário N-CEN corrompido: {package}")
        inventories[package] = inventory
    return MappingProxyType(inventories)


CANONICAL_PACKAGE_INVENTORIES = _load()

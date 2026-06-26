from __future__ import annotations

import json
from pathlib import Path

from src.input_packs.hashing import canonical_json_sha256, file_sha256


def test_canonical_json_hash_is_key_order_independent() -> None:
    first = {"b": 2, "a": [{"z": 3, "y": 4}]}
    second = {"a": [{"y": 4, "z": 3}], "b": 2}
    assert canonical_json_sha256(first) == canonical_json_sha256(second)


def test_canonical_json_hash_keeps_list_order_material() -> None:
    assert canonical_json_sha256([1, 2]) != canonical_json_sha256([2, 1])


def test_json_file_hash_ignores_pretty_printing(tmp_path: Path) -> None:
    compact = tmp_path / "compact.json"
    pretty = tmp_path / "pretty.json"
    compact.write_text('{"b":2,"a":1}', encoding="utf-8")
    pretty.write_text(json.dumps({"a": 1, "b": 2}, indent=2), encoding="utf-8")
    assert file_sha256(compact) == file_sha256(pretty)


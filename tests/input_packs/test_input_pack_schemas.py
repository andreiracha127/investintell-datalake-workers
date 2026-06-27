from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS = ROOT / "schemas" / "input_packs"
GOLDEN = ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack"
INVALID = ROOT / "fixtures" / "input_packs" / "invalid"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_golden_manifest_satisfies_v1_schema() -> None:
    jsonschema.validate(_json(GOLDEN / "manifest.json"), _json(SCHEMAS / "input_pack_manifest.schema.json"))


@pytest.mark.parametrize("fixture", sorted(INVALID.glob("*.json")), ids=lambda p: p.name)
def test_invalid_manifest_fixtures_fail_v1_schema(fixture: Path) -> None:
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(_json(fixture), _json(SCHEMAS / "input_pack_manifest.schema.json"))


@pytest.mark.parametrize(
    ("fixture", "schema"),
    [
        ("SOURCE.json", "source.schema.json"),
        ("raw_snapshot_manifest.json", "snapshot_manifest.schema.json"),
        ("canonical_snapshot_manifest.json", "snapshot_manifest.schema.json"),
        ("derived_feature_manifest.json", "snapshot_manifest.schema.json"),
        ("table_hashes.json", "table_hashes.schema.json"),
        ("provenance.json", "provenance.schema.json"),
    ],
)
def test_golden_component_manifests_satisfy_their_schemas(fixture: str, schema: str) -> None:
    jsonschema.validate(_json(GOLDEN / fixture), _json(SCHEMAS / schema))


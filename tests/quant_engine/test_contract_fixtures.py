from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[2]
V1 = ROOT / "contracts" / "quant-engine" / "v1"

SCHEMA_FOR_PREFIX = {
    "job-request": "job-request.schema.json",
    "job-result": "job-result.schema.json",
    "engine-manifest": "engine-manifest.schema.json",
}

VALID = sorted((V1 / "fixtures" / "valid").glob("*.json"))
INVALID = sorted((V1 / "fixtures" / "invalid").glob("*.json"))


def _schema_for(fixture: Path) -> dict:
    prefix = fixture.name.split(".")[0]
    name = SCHEMA_FOR_PREFIX[prefix]
    return json.loads((V1 / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("fixture", VALID, ids=lambda p: p.name)
def test_valid_fixtures_pass_schema(fixture):
    instance = json.loads(fixture.read_text(encoding="utf-8"))
    jsonschema.validate(instance, _schema_for(fixture))


@pytest.mark.parametrize("fixture", INVALID, ids=lambda p: p.name)
def test_invalid_fixtures_fail_schema(fixture):
    instance = json.loads(fixture.read_text(encoding="utf-8"))
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance, _schema_for(fixture))


def test_each_schema_has_positive_and_negative_coverage():
    valid_prefixes = {p.name.split(".")[0] for p in VALID}
    invalid_prefixes = {p.name.split(".")[0] for p in INVALID}
    assert valid_prefixes == set(SCHEMA_FOR_PREFIX)
    assert invalid_prefixes == set(SCHEMA_FOR_PREFIX)

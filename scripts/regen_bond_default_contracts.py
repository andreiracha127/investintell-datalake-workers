"""Regenerate (or check) every derived artifact of the bond default-event contract.

Derived artifacts, in dependency order:

1. the generated frame-spec and contract-pin blocks in ``schemas/bond_credit_publications_v1.sql``;
2. the embedded digest of ``contracts/bonds/default_event_policy_v1.json``;
3. the rendered ``contracts/bonds/default_event_bundle_v2.schema.json`` and its digest;
4. the ``POLICY_DIGEST``/``SCHEMA_DIGEST`` pins in ``src/bonds/default_events/contracts.py``;
5. the synthetic fixtures ``tests/fixtures/bond_default_events/bundle_*.json`` (v2) and the
   retained v1 rejection fixture.

The retained ``default_event_bundle_v1.schema.json`` is never rewritten: it is checked
against its pinned digest (``LEGACY_V1_SCHEMA_DIGEST``) and any drift fails both modes.

Later artifacts depend on constants that earlier ones rewrite (the schema embeds the
policy digest, the SQL pins and fixtures embed the digests), so write mode repeats
fresh-interpreter passes until nothing changes. ``--check`` writes nothing and exits 1
when any artifact is stale; it passes exactly when the tree is at that fixed point.

Usage::

    .venv/Scripts/python.exe scripts/regen_bond_default_contracts.py          # regenerate
    .venv/Scripts/python.exe scripts/regen_bond_default_contracts.py --check  # verify only
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_PY = ROOT / "src" / "bonds" / "default_events" / "contracts.py"
PUBLICATIONS_SQL = ROOT / "schemas" / "bond_credit_publications_v1.sql"
SYNTHETIC_PY = ROOT / "tests" / "fixtures" / "bond_default_events" / "synthetic.py"
MAX_WRITE_PASSES = 6
CHANGED_EXIT = 10


def _contracts() -> ModuleType:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from src.bonds.default_events import contracts

    return contracts


def _synthetic() -> ModuleType:
    spec = importlib.util.spec_from_file_location("bond_default_synthetic_regen", SYNTHETIC_PY)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SYNTHETIC_PY}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _json_document(document: dict) -> bytes:
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _pin(text: str, name: str, digest: str) -> str:
    pattern = rf'^{name} = "sha256:[0-9a-f]{{64}}"'
    updated, count = re.subn(pattern, f'{name} = "{digest}"', text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(f"{name} pin not found in {CONTRACTS_PY}")
    return updated


def expected_outputs() -> dict[Path, bytes]:
    """Expected bytes of every derived artifact, computed from the current tree."""
    c = _contracts()
    outputs: dict[Path, bytes] = {}

    c.load_legacy_v1_schema()  # retained v1 schema must be byte-for-byte digest-pinned

    sql_text = PUBLICATIONS_SQL.read_bytes().decode("utf-8").replace("\r\n", "\n")
    for begin, end_marker, render in (
        (c.SQL_FRAME_SPEC_BEGIN, c.SQL_FRAME_SPEC_END, c.render_sql_frame_specs),
        (c.SQL_PINS_BEGIN, c.SQL_PINS_END, c.render_sql_pins),
    ):
        start = sql_text.index(begin)
        end = sql_text.index(end_marker) + len(end_marker)
        sql_text = sql_text[:start] + render() + sql_text[end:]
    outputs[PUBLICATIONS_SQL] = sql_text.encode("utf-8")

    policy = c.load_json_strict(c.POLICY_PATH.read_bytes())
    policy.pop("digest", None)
    policy_digest = c.digest_of(policy)
    policy["digest"] = policy_digest
    outputs[c.POLICY_PATH] = _json_document(policy)

    schema = c.render_bundle_schema()
    schema.pop("x-digest", None)
    schema["x-digest"] = c.document_digest(schema, "x-digest")
    outputs[c.SCHEMA_PATH] = _json_document(schema)

    contracts_text = CONTRACTS_PY.read_bytes().decode("utf-8").replace("\r\n", "\n")
    contracts_text = _pin(contracts_text, "POLICY_DIGEST", policy_digest)
    contracts_text = _pin(contracts_text, "SCHEMA_DIGEST", schema["x-digest"])
    outputs[CONTRACTS_PY] = contracts_text.encode("utf-8")

    synthetic = _synthetic()
    for name, factory in synthetic.FIXTURES.items():
        outputs[synthetic.HERE / name] = synthetic.render(factory())
    # Retained v1 rejection fixture: never regenerated, only verified against its pin.
    legacy = synthetic.HERE / synthetic.LEGACY_V1_FIXTURE
    if c.sha256_hex(_normalized(legacy.read_bytes())) != synthetic.LEGACY_V1_FIXTURE_SHA256:
        raise RuntimeError(f"retained v1 rejection fixture drifted: {legacy.name}")
    return outputs


def _normalized(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


def stale_paths(outputs: dict[Path, bytes]) -> list[Path]:
    """Artifacts whose on-disk bytes (line endings normalized) differ from ``outputs``."""
    return [
        path for path, expected in outputs.items()
        if not path.is_file() or _normalized(path.read_bytes()) != _normalized(expected)
    ]


def _write_pass() -> int:
    outputs = expected_outputs()
    stale = stale_paths(outputs)
    for path in stale:
        before = int(path.stat().st_mtime) if path.is_file() else None
        path.write_bytes(outputs[path])
        if path.suffix == ".py" and before is not None and int(path.stat().st_mtime) <= before:
            # A same-size rewrite in the same second would leave any cached .pyc (validated by
            # mtime and size only) looking fresh: move the source mtime past the old value.
            os.utime(path, (before + 1, before + 1))
        print(f"wrote {path.relative_to(ROOT).as_posix()}")
    return CHANGED_EXIT if stale else 0


def _regenerate() -> int:
    for _ in range(MAX_WRITE_PASSES):
        # Each pass runs in a fresh interpreter (-B: no bytecode) so rewritten module constants are
        # re-imported from source; a same-size, same-second rewrite must never hit a stale .pyc.
        result = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--write-pass"], check=False)
        if result.returncode == 0:
            print("bond default-event contract artifacts are up to date")
            return 0
        if result.returncode != CHANGED_EXIT:
            return result.returncode
    print(f"no fixed point after {MAX_WRITE_PASSES} passes", file=sys.stderr)
    return 1


def _check() -> int:
    stale = stale_paths(expected_outputs())
    for path in stale:
        print(f"stale: {path.relative_to(ROOT).as_posix()}", file=sys.stderr)
    if stale:
        print("run scripts/regen_bond_default_contracts.py to regenerate", file=sys.stderr)
        return 1
    print("bond default-event contract artifacts are up to date")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="write nothing; exit 1 if any artifact is stale")
    mode.add_argument("--write-pass", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.check:
        return _check()
    if args.write_pass:
        return _write_pass()
    return _regenerate()


if __name__ == "__main__":
    sys.exit(main())

"""Finding F1: catalog hash must not depend on the absolute input path.

See docs/architecture/quant-engine-determinism-findings.md. The A31 catalog hash
(and, transitively, the bundle evaluation_hash and the object-store prefix) must
be reproducible regardless of where the catalog file is mounted (host `E:\\...`
vs container `/input/combo/...`).
"""

from __future__ import annotations

from pathlib import Path

from src import calibration_harness as ch


def _payload() -> dict:
    return {"configs": [{"name": "TEST-A31-REF"}]}


def test_catalog_hash_is_independent_of_source_path():
    _, host_hash = ch.normalize_a31_catalog(
        _payload(),
        l2_macro_logical_hash="L2-FIXED",
        source_path=Path(r"E:\investintell-datalake-workers-combo\catalog.json"),
    )
    _, container_hash = ch.normalize_a31_catalog(
        _payload(),
        l2_macro_logical_hash="L2-FIXED",
        source_path=Path("/input/combo/catalog.json"),
    )
    assert host_hash == container_hash


def test_normalized_catalog_still_exposes_source_path_for_diagnostics():
    source = Path("/input/combo/catalog.json")
    normalized, _ = ch.normalize_a31_catalog(
        _payload(), l2_macro_logical_hash="L2-FIXED", source_path=source
    )
    # source_path remains as out-of-band metadata, just not part of the hash.
    assert normalized["source_path"] == str(source)

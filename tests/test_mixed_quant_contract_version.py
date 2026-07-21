"""The workers ``mixed_quant_v1`` contract declaration stays pinned (Task 6, Step 4).

Pure (no DB): asserts the publisher declares ``mixed_quant_v1`` and that the
frozen surface digest matches the recomputed surface, so a one-sided change to the
product/instrument/factor/income vocabulary fails until re-synced with the app
reader's ``app.contracts.mixed_quant_v1.SURFACE_DIGEST``.
"""

from __future__ import annotations

from src.quant_data import mixed_quant_contract as mq
from src.quant_data import publication as pub


def test_workers_declare_mixed_quant_v1() -> None:
    assert pub.PRODUCT == "mixed_quant_v1"
    assert mq.CONTRACT_VERSION == "mixed_quant_v1"


def test_frozen_digest_matches_declared_surface() -> None:
    assert mq.compute_surface_digest() == mq.SURFACE_DIGEST


def test_contract_verifies_clean() -> None:
    verdict = mq.verify_contract()
    assert verdict["ok"] is True, verdict["mismatches"]

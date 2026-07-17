"""Exact repository files that can affect the Stage A measured decision path."""

from __future__ import annotations


# ``measure_stage_a`` reaches the live-validation child through ordinary imports.
# ``repeatability_matrix`` is loaded dynamically by the shared measurement helper,
# so it is an explicit root of the import-closure contract.
STAGE_A_ENTRYPOINTS = (
    "harness/direct_activation/measure_stage_a.py",
    "scripts/repeatability_matrix.py",
)


STAGE_A_COMPUTE_PATHS = tuple(
    sorted(
        (
            "harness/__init__.py",
            "harness/dark_launch/__init__.py",
            "harness/dark_launch/measure_observability.py",
            "harness/direct_activation/__init__.py",
            "harness/direct_activation/build_stage_a_amendment.py",
            "harness/direct_activation/compute_manifest.py",
            "harness/direct_activation/live_validation.py",
            "harness/direct_activation/measure_stage_a.py",
            "harness/direct_activation/measure_stage_a_child.py",
            "harness/phase0q/__init__.py",
            "harness/phase0q/decision.py",
            # macro_quadrant_us_v3 (owner-approved switch 2026-07-16): the fused
            # decision path live_validation now runs is measured compute surface.
            "harness/phase0q/decision_v3.py",
            "harness/phase0q/pit.py",
            "harness/phase0q/sleeve.py",
            "scripts/p1_export/__init__.py",
            "scripts/p1_export/export_p1_sources.py",
            "scripts/repeatability_matrix.py",
            "scripts/__init__.py",
            "services/quant_engine/src/investintell_quant_engine/__init__.py",
            "services/quant_engine/src/investintell_quant_engine/comparator.py",
            "services/quant_engine/src/investintell_quant_engine/outputs_manifest.py",
            "services/quant_engine/src/investintell_quant_engine/repeatability.py",
            "services/quant_engine/src/investintell_quant_engine/version.py",
            "src/__init__.py",
            "src/db.py",
            "src/input_packs/__init__.py",
            "src/input_packs/hashing.py",
            "src/input_packs/manifest.py",
            "src/input_packs/p0_contract.py",
            "src/input_packs/p0_derived.py",
            "src/input_packs/verifier.py",
            "src/macro_sources.py",
            "src/macro_transforms.py",
            "src/quadrant_assemble.py",
            "src/quadrant_assemble_v2.py",
            "src/quadrant_confidence.py",
            "src/quadrant_confidence_v2.py",
            "src/quadrant_hysteresis.py",
            "src/quadrant_market_observation.py",
            "src/quadrant_score.py",
            "src/quadrant_snapshot.py",
            "src/quadrant_staleness.py",
        )
    )
)

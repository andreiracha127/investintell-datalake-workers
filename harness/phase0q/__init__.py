"""Phase 0Q metric parity harness (PR-C).

Pure-Python, filesystem-in / filesystem-out, network-free reimplementation of the
open_macro_v03 decision path plus the reference-sleeve metric extractor. Reuses the
frozen decision-formula modules (``src.quadrant_score``, ``src.quadrant_hysteresis``,
``src.quadrant_confidence``, ``src.macro_transforms``, ``src.macro_sources``) by
import only; it never modifies guarded ``src/`` code.

Modules
-------
``pit``       in-memory ``latest_vintage_as_of`` over pack-v2 vintage rows.
``decision``  monthly quadrant classification (hysteresis / latch / coverage).
``sleeve``    reference-sleeve simulator (rebalance, drift band, costs).
``metrics``   exact ``metric_definitions.json`` formulas.
``runner``    grid orchestration + canonical contract-shaped outputs.
"""

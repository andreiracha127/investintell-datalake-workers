"""P1 certified input pack builder (``open_macro_v03_certified_input_pack_002``).

The P1 pack carries the point-in-time (PIT) macro vintage table
(``macro_observation_vintage``) and the sleeve price table (``eod_prices``) that
feed the open_macro_v03 metric harness. It is a pure file transformation over the
committed P1 source snapshots under ``fixtures/p1_sources/open_macro_v03/`` and
reuses the immutable P0 builder helpers (``src/input_packs/p0_contract.py``,
``hashing.py``, ``manifest.py``) unchanged.

Because the P0 offline verifier (``src/input_packs/verifier.py`` ``verify_pack``)
is hard-wired to the nine P0 tables, the P0 ``input_pack_id`` and the P0 derived
feature recomputation, it cannot validate a P1 pack. This package therefore ships
its own :func:`harness.p1_pack.verifier.verify_pack` that reuses the generic P0
hash-tree / normalization helpers and encodes the P1-specific table + governance
contract. The delta is documented in the pack manifest under
``verifier_delta_vs_p0``.
"""

from .contract import P1_TABLE_SPECS, P1_TABLES_BY_NAME

__all__ = ["P1_TABLE_SPECS", "P1_TABLES_BY_NAME"]

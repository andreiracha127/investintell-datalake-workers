"""Top-level harness package (guard-free) for open_macro_v03 metric harness work.

This package lives outside the preflight-guarded surfaces (``src/input_packs/``,
``services/``, ``packages/``). It imports shared, immutable builder code from
``src/input_packs`` read-only and never mutates it.
"""

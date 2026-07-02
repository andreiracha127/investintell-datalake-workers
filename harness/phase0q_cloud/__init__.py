"""Phase 0Q cloud-leg PREPARATION package (build-only, network-free).

This package prepares — but never executes — the ``qc_research_object_store`` leg of
the open_macro_v03 phase0q reproducibility matrix
(``local_python_pure`` x ``qc_research_object_store``). It mirrors the proven
``qc-a3-parity`` pattern (immutable Object Store prefix + per-object sha256 +
drift refusal + fail-loud ``src/db.py`` stub), but for the phase0q harness.

Modules
-------
``bundle``        deterministic LOCAL bundle builder + CLI (ZERO network / ZERO uploads).
``upload_plan``   emits (does NOT run) the ordered ``lean cloud object-store set`` plan.
``fetch_results`` validates a fetched cloud verdict JSON + completes the consolidated report.

Nothing in this package performs any network call, any ``lean`` invocation, or any
Object Store upload. The orchestrator runs the reviewed upload / push / fetch
commands separately in the main session. Governance stays pinned: A5 blocked;
runtime_activation / activation_allowed / allocator_publish / official_result all
false; db_write_mode none; status candidate_not_approved; approved false.
"""

from __future__ import annotations

BUNDLE_SCHEMA_VERSION = 1
OBJECT_STORE_BASE_PREFIX = "investintell/open_macro_v03/phase0q"
QC_PROJECT_ID = 33679769
QC_PROJECT_NAME = "open_macro_v03_phase0q_harness"

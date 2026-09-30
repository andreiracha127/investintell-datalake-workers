"""Public-source bond default evidence (bond_credit_evidence_v1).

Stage 2 (W0) owns the frozen contracts (:mod:`.contracts`) and the immutable
publication ledger (:mod:`.publication`). Source adapters (``sec_acquisition``,
``nport``, ``edgar``), resolvers (``resolve``, ``public_ratings``) and the worker
entrypoint consume these interfaces and never redefine them.
"""

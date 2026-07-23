"""Owner-authorized source-qualification worker for the Phase-10 bond gate.

This is the single authorized INSERT that lifts a Phase-10 metric out of the
standing ``no_qualified_source`` dark state: it records, in the minimal
``bond_source_qualification`` registry the gate reads (SELECT-only), that an
AUTHORIZED source contract now backs a metric's inputs. Nothing else in the
codebase writes that registry, and the gate machine itself never does — so this
worker is the ONLY way the gate can begin to pass, by owner decision.

Fail-closed by construction:
  * an empty/absent metric set refuses — never "qualify everything";
  * an absent (or blank) source-contract reference refuses;
  * any metric outside the exact Phase-10 vocabulary refuses, writing nothing;
  * ``security_oas`` is DENYLISTED in code (owner decision 2026-07-23): even
    though the engine gate would also block it (its engine is
    ``model_validation_incomplete``), the worker refuses LOUDLY with
    ``oas_deliberately_excluded`` rather than insert a dead qualification row.

Every gate check happens BEFORE a database connection is opened, so a refusal
trivially writes nothing. The authorized INSERT is idempotent
(``ON CONFLICT (metric_id, source_contract_ref) DO NOTHING``): a re-run inserts
nothing new and reports the already-present rows as ``already_active``.

Source identity is confidential (Global Constraint): this module carries no
vendor identity. The metric ids are gate vocabulary; the source reference is the
OPAQUE internal token Task 1 registered (``bond_price_source_v1@<prefix>``),
never a vendor name.

Contract: ``run(dsn=None) -> dict`` (standard worker envelope).
  ok:      ``{"state": "ok", "qualified": [...], "already_active": [...],
             "source_contract_ref": "<token>"}``
  refused: ``{"state": "refused", "reason": "no_metrics_requested" |
             "no_source_ref" | "unknown_metric" | "oas_deliberately_excluded",
             ...}``
"""
from __future__ import annotations

import os
from typing import Any

from src.bonds.phase10_gate import REQUIREMENTS, install_gate_schema
from src.db import connect, resolve_dsn

# Environment contract (fail-closed; both are mandatory).
ENV_METRICS = "QUALIFY_METRICS"  # csv of Phase-10 metric ids
ENV_SOURCE_REF = "QUALIFY_SOURCE_REF"  # opaque internal token from Task 1

# Owner decision 2026-07-23: security_oas is DELIBERATELY EXCLUDED from Wave 1.
# Its engine is model_validation_incomplete, so the worker refuses loudly rather
# than insert a dead qualification row the gate could never honestly pass.
DENYLISTED_METRICS: frozenset[str] = frozenset({"security_oas"})

# Typed refusal vocabulary (closed).
REFUSAL_NO_METRICS = "no_metrics_requested"
REFUSAL_NO_SOURCE_REF = "no_source_ref"
REFUSAL_UNKNOWN_METRIC = "unknown_metric"
REFUSAL_OAS_EXCLUDED = "oas_deliberately_excluded"


def _refused(reason: str, **detail: Any) -> dict[str, Any]:
    envelope: dict[str, Any] = {"state": "refused", "reason": reason}
    if detail:
        envelope["detail"] = detail
    return envelope


def _parse_metrics(raw: str | None) -> list[str]:
    """Split the csv contract into an ordered, de-duplicated metric list."""
    if not raw:
        return []
    ordered: dict[str, None] = {}
    for token in raw.split(","):
        metric = token.strip()
        if metric:
            ordered.setdefault(metric, None)
    return list(ordered)


def run(dsn: str | None = None) -> dict[str, Any]:
    # Fail-closed env gates BEFORE any database connection: a refusal here
    # trivially writes nothing.
    metrics = _parse_metrics(os.getenv(ENV_METRICS))
    if not metrics:
        return _refused(REFUSAL_NO_METRICS)

    source_ref = (os.getenv(ENV_SOURCE_REF) or "").strip()
    if not source_ref:
        return _refused(REFUSAL_NO_SOURCE_REF)

    # Req 1 — vocabulary: every metric must be a known Phase-10 metric id.
    unknown = [metric for metric in metrics if metric not in REQUIREMENTS]
    if unknown:
        return _refused(REFUSAL_UNKNOWN_METRIC, unknown=unknown)

    # Req 2 — denylist: security_oas is an owner-excluded metric; refuse loudly.
    excluded = [metric for metric in metrics if metric in DENYLISTED_METRICS]
    if excluded:
        return _refused(REFUSAL_OAS_EXCLUDED, excluded=excluded)

    qualified: list[str] = []
    already_active: list[str] = []
    with connect(resolve_dsn(dsn)) as conn:
        install_gate_schema(conn)  # Req 5 — self-installing DDL, first.
        for metric in metrics:
            # Req 3 — the authorized, idempotent INSERT. RETURNING yields a row
            # only when this run actually inserted it; an ON CONFLICT skip yields
            # None, so the metric is reported as already_active instead.
            row = conn.execute(
                "INSERT INTO bond_source_qualification "
                "(metric_id, source_contract_ref, qualified_from, qualified_to) "
                "VALUES (%s, %s, now(), NULL) "
                "ON CONFLICT (metric_id, source_contract_ref) DO NOTHING "
                "RETURNING metric_id",
                (metric, source_ref),
            ).fetchone()
            (qualified if row is not None else already_active).append(metric)
        conn.commit()

    return {
        "state": "ok",
        "qualified": qualified,
        "already_active": already_active,
        "source_contract_ref": source_ref,
    }

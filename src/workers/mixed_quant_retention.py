"""Prune superseded ``mixed_quant_v1`` publications beyond the kept generations.

Each publication is a FULL immutable snapshot — ~2.4 GB for the current universe
(7.1M returns plus instruments, aliases, exposures and links) — and nothing
pruned them, so the datalake grew by that much on every publish. That is the
reason the publish cron is monthly instead of weekly; with retention in place the
steady state is bounded and the cadence stops being a disk decision.

Runs as its OWN step, never as a side effect of publishing or promoting:

* not in the publisher, because pruning must be decided against the ACTIVE
  pointer, and the publisher deliberately does not promote;
* not in ``promote``, because a rollback repoints to an OLDER publication — a
  promotion that pruned "everything older than active" would delete the very
  snapshot being rolled back onto.

Two safety properties are enforced by the schema rather than here: the active
publication is undeletable (``active_quant_publication_v1`` FKs with ON DELETE
RESTRICT), and a publication whose ``as_of`` is far in the past is a deliberate
point-in-time build, so recency never prunes it.

Contract: ``run(dsn) -> dict`` (see src/run.py).
"""
from __future__ import annotations

import os

from src.db import connect, resolve_dsn
from src.quant_data import publication as pub

# Active + two rollback generations. A publication older than that is stale by
# construction — its three-year return window ends a cadence-period earlier, so
# rolling back onto it would feed the optimizer outdated history.
DEFAULT_KEEP_GENERATIONS = 3
# An as_of older than this is a historical point-in-time build, kept on purpose.
DEFAULT_HISTORICAL_BEFORE = "1 year"


def run(dsn: str | None = None) -> dict:
    keep = int(os.getenv("MIXED_QUANT_KEEP_GENERATIONS", DEFAULT_KEEP_GENERATIONS))
    historical_before = os.getenv(
        "MIXED_QUANT_HISTORICAL_BEFORE", DEFAULT_HISTORICAL_BEFORE
    )
    with connect(resolve_dsn(dsn)) as conn:
        pub.install_schema(conn)
        active = pub.active_publication_id(conn, pub.PRODUCT)
        if active is None:
            # Never prune while no pointer is published: without an active
            # publication there is nothing to measure "generations" against, and
            # the schema's RESTRICT has nothing to protect.
            return {
                "product": pub.PRODUCT,
                "status": "skipped",
                "reason": "no_active_publication",
                "pruned": [],
            }
        pruned = pub.prune(
            conn,
            pub.PRODUCT,
            keep_generations=keep,
            historical_before=historical_before,
        )
        conn.commit()
    return {
        "product": pub.PRODUCT,
        "status": "ok",
        "active_publication_id": str(active),
        "keep_generations": keep,
        "historical_before": historical_before,
        "pruned_count": len(pruned),
        "pruned": [
            {"publication_id": str(pid), "as_of": str(as_of), "status": status}
            for pid, as_of, status in pruned
        ],
    }

"""Fail-closed freshness probe for the N-PORT identity matview.

``nport_holdings_snapshot_identity_v1`` is a MATERIALIZED VIEW this repository
neither creates nor refreshes: production owns it as ``postgres`` and refreshes
it OUT OF BAND.  The app reads it through
``sec_current_nport_holdings_snapshot_identity_v1`` and uses ``MAX(report_date)``
per series as the ANCHOR of the fixed-income dossier -- so when the matview is
behind, the app does not fail, it serves an OLDER report_date labelled with a
current publication id.

WHY A PROBE AND NOT A REFRESH
-----------------------------
Bringing the refresh in-band (``src/workers/matview_refresh.py`` already refreshes
a datalake list, and the matview does carry the UNIQUE index CONCURRENTLY needs)
is a production change whose cost was not measured here: the matview's defining
join costs ~15 s just to answer a ``max()`` on the mirror.  Detection is what was
asked for and what can be proven from this repository; the in-band refresh is
recorded as a proposal in ``docs/runbooks/nport-identity-matview-freshness.md``.

The detection itself is cheap: four index-backed ``max()`` reads, 24 ms cold and
3 ms warm on the mirror's 625 508-row publication.  The exact, bridge-filtered
answer costs ~15 s and is paid only when the cheap comparison already looks wrong.

WHAT "FAIL-CLOSED" MEANS HERE
-----------------------------
The probe never reports ``fresh`` unless it PROVED it.  Absence of any input --
matview, pointer, holdings surface -- resolves to ``unavailable``, never to
``fresh``.  ``run()`` (the dispatchable job) raises on a proven divergence so the
job exits non-zero; ``probe()`` returns the same verdict without raising, so a
caller that merely wants to observe can embed it.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.db import connect, resolve_dsn

LOGGER = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FILE = ROOT / "schemas" / "nport_holdings_snapshot_identity_freshness.sql"

MATVIEW = "nport_holdings_snapshot_identity_v1"
CONSUMER_VIEW = "sec_current_nport_holdings_snapshot_identity_v1"

#: Verdicts that mean the served anchor cannot be trusted.
DIVERGENT_STATES = frozenset({"stale", "behind_pointer"})


class IdentityMatviewStale(RuntimeError):
    """The identity matview is behind the pinned holdings publication."""

    def __init__(self, verdict: dict[str, Any]) -> None:
        super().__init__(
            f"{MATVIEW} is not fresh for the current sec_nport_holdings_v2 publication: {verdict}"
        )
        self.verdict = verdict


def install_schema(conn: Any) -> None:
    """Install the freshness functions. Idempotent, and installs no matview.

    The matview itself is deliberately NOT created here: production owns it and
    the runtime role is not its owner (the same constraint
    ``src/sec_effective_matviews.py`` documents for the SEC effective views).
    A probe that created its own subject would be probing itself.
    """
    with conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))


def probe(conn: Any, *, install: bool = True) -> dict[str, Any]:
    """The structured freshness verdict. Never raises on a divergence.

    A divergence is logged at WARNING with the identifiers that make it
    actionable -- the same pattern ``nport_fixed_income_serving`` uses for its
    pruned-evidence standstill, and for the same reason: the state can PERSIST,
    so it has to be legible in the logs rather than only in a return value.
    """
    if install:
        install_schema(conn)
    verdict: dict[str, Any] = conn.execute(
        "SELECT nport_holdings_snapshot_identity_freshness()"
    ).fetchone()[0]

    state = verdict.get("state")
    if state in DIVERGENT_STATES:
        LOGGER.warning(
            "nport_holdings_identity_freshness: %s is behind the pinned holdings publication "
            "(state=%s reason=%s publication_id=%s matview_max_report_date=%s "
            "source_max_report_date=%s bridge_resolved_max_report_date=%s); the app anchors the "
            "fixed-income dossier on MAX(report_date) of %s, so it is labelling an older "
            "report_date with a current publication id. Refresh the matview and re-run -- see "
            "docs/runbooks/nport-identity-matview-freshness.md",
            MATVIEW, state, verdict.get("reason"), verdict.get("publication_id"),
            verdict.get("matview_max_report_date"), verdict.get("source_max_report_date"),
            verdict.get("bridge_resolved_max_report_date"), CONSUMER_VIEW,
        )
    elif state != "fresh":
        LOGGER.warning(
            "nport_holdings_identity_freshness: freshness could not be asserted "
            "(state=%s reason=%s matview=%s); this is NOT a proof of freshness",
            state, verdict.get("reason"), MATVIEW,
        )
    return verdict


def run(
    dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None
) -> dict[str, Any]:
    """Assert the identity matview is fresh for the pinned holdings publication.

    ``calc_date``/``limit`` are accepted for dispatcher-contract parity and
    ignored: there is exactly one current publication to check and the check is
    two ``max()`` reads, so there is nothing to slice.

    Raises ``IdentityMatviewStale`` on a proven divergence -- the job must go red,
    because the app is serving a stale anchor under a current label.
    """
    del calc_date, limit
    with connect(resolve_dsn(dsn), autocommit=True) as conn:
        verdict = probe(conn)
    if verdict.get("state") in DIVERGENT_STATES:
        raise IdentityMatviewStale(verdict)
    return {"rows": 0, **verdict}

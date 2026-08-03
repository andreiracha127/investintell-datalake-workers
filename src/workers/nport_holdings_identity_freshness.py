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
The probe never reports ``fresh`` unless it PROVED it, and it distinguishes two
kinds of not-proven:

* ``unreadable`` -- the anchor relation is ABSENT or was never refreshed.  The
  app's read path is broken, not merely behind, so this is HARD: ``run()`` raises
  ``IdentityMatviewUnreadable``.  Answering a caller that has just run REFRESH
  with a silent pass would be telling it the opposite of the truth.
* ``unavailable`` -- no pointer, no holdings surface, or a pinned publication with
  no holdings.  Genuinely undecidable UPSTREAM and not the matview's fault, so it
  stays soft: WARNING, no raise.

``stale`` and ``behind_pointer`` raise ``IdentityMatviewStale``.  ``probe()``
returns every verdict without raising, so a caller that merely wants to observe
can embed it.

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

#: Verdicts that mean the served anchor is behind what it labels itself as.
DIVERGENT_STATES = frozenset({"stale", "behind_pointer"})
#: Verdicts that mean the served anchor cannot be read at all.
UNREADABLE_STATES = frozenset({"unreadable"})
#: Every verdict that must stop the job.
HARD_STATES = DIVERGENT_STATES | UNREADABLE_STATES


class IdentityMatviewError(RuntimeError):
    """Base for the verdicts that must stop the job."""

    def __init__(self, message: str, verdict: dict[str, Any]) -> None:
        super().__init__(f"{message}: {verdict}")
        self.verdict = verdict


class IdentityMatviewStale(IdentityMatviewError):
    """The identity matview is behind the pinned holdings publication."""

    def __init__(self, verdict: dict[str, Any]) -> None:
        super().__init__(
            f"{MATVIEW} is not fresh for the current sec_nport_holdings_v2 publication",
            verdict,
        )


class IdentityMatviewUnreadable(IdentityMatviewError):
    """The identity matview is absent, or exists but was never refreshed.

    Deliberately a DIFFERENT exception from ``IdentityMatviewStale``: "behind" and
    "not there" call for different repairs, and only one of them is fixed by a
    refresh.
    """

    def __init__(self, verdict: dict[str, Any]) -> None:
        super().__init__(f"{MATVIEW} cannot be read", verdict)


def install_schema(conn: Any) -> bool:
    """Install the freshness functions. Idempotent, and installs no matview.

    The matview itself is deliberately NOT created here: production owns it and
    the runtime role is not its owner (the same constraint
    ``src/sec_effective_matviews.py`` documents for the SEC effective views).
    A probe that created its own subject would be probing itself.

    Returns whether the install ran.  ``CREATE OR REPLACE FUNCTION`` requires
    ownership, and the runbook encourages an operator to install these functions
    once as ``postgres`` -- after which this call would fail with *must be owner
    of function* and turn the job red for a reason that has nothing to do with
    freshness.  So a privilege error is tolerated ONLY when the functions are
    already there; if they are not, it is re-raised, because then there is nothing
    to probe with.

    The install runs inside ``conn.transaction()`` so that recovery works on a
    connection that is NOT in autocommit: without it the privilege error would
    leave an aborted transaction, the existence probe below would raise
    ``InFailedSqlTransaction``, and the very error this tolerance exists for would
    be masked by a different one.  ``conn.transaction()`` opens a real transaction
    under autocommit and a SAVEPOINT when one is already open, so both callers are
    left with a usable connection.
    """
    import psycopg

    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))
        return True
    except psycopg.errors.InsufficientPrivilege:
        installed = conn.execute(
            "SELECT to_regprocedure('nport_holdings_snapshot_identity_freshness()') IS NOT NULL"
        ).fetchone()[0]
        if not installed:
            raise
        LOGGER.warning(
            "nport_holdings_identity_freshness: not the owner of the freshness functions, "
            "using the already-installed ones. Whoever owns them must re-apply "
            "schemas/%s when it changes -- see docs/runbooks/nport-identity-matview-freshness.md",
            SCHEMA_FILE.name,
        )
        return False


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
    if state in UNREADABLE_STATES:
        LOGGER.warning(
            "nport_holdings_identity_freshness: %s cannot be read (state=%s reason=%s); the app "
            "reads it through %s to anchor the fixed-income dossier, so that read path is broken "
            "-- this is NOT a freshness gap and a REFRESH alone may not fix it. See "
            "docs/runbooks/nport-identity-matview-freshness.md",
            MATVIEW, state, verdict.get("reason"), CONSUMER_VIEW,
        )
    elif state in DIVERGENT_STATES:
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
    ignored: there is exactly one current publication to check and the check is a
    handful of ``max()`` reads, so there is nothing to slice.

    Raises ``IdentityMatviewStale`` on a proven divergence and
    ``IdentityMatviewUnreadable`` when the anchor relation is absent or was never
    refreshed.  Both go red: in the first case the app serves a stale anchor under
    a current label, in the second its read path does not resolve at all.
    """
    del calc_date, limit
    with connect(resolve_dsn(dsn), autocommit=True) as conn:
        verdict = probe(conn)
    state = verdict.get("state")
    if state in HARD_STATES:
        raise (
            IdentityMatviewUnreadable if state in UNREADABLE_STATES else IdentityMatviewStale
        )(verdict)
    return {"rows": 0, **verdict}

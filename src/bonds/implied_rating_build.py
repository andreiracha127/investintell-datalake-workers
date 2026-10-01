"""The one pure build of a ``bond_market_implied_rating_v1`` publication payload.

Extracted verbatim from the worker's ``_build_payload`` so that the daily
worker, the backfill CLI and the determinism replay (two fresh processes
loading the same exported snapshot) run the SAME function on the same inputs:
anchor validation under the fixed policy, the pinned-anchor compatibility
gate, the state machine, ``rows_digest``, the closed-window check and the
publication identity. No database, no clock, no environment.

The only impure input a publication build needs -- the ``l_anchor`` of the
publication the pointer currently names -- is supplied either as a value
(``pinned_anchor``, the replay path) or as a zero-argument callable
(``resolve_pinned_anchor``, the worker path), invoked at exactly the point
``_build_payload`` used to read it: after the anchor diagnostics, before the
state machine. Supplying neither means "no current publication".

A typed refusal is returned as ``{"refusal": {"reason": <input reason>,
**extra}}``; the worker maps it onto ``implied_rating_gate_failed`` with the
same ``input_reasons`` and extra fields it reported before this extraction.
"""
from __future__ import annotations

import logging
import math
from collections.abc import Callable
from datetime import date
from typing import Any

import pandas as pd

from src.bonds import implied_rating as policy
from src.bonds.implied_rating import ANCHOR_DRIFT_ABS_TOL
from src.bonds.implied_rating_materializer import (
    ImpliedRatingPublication,
    publication_id_for,
)

LOGGER = logging.getLogger(__name__)


def _refusal(reason: str, **extra: Any) -> dict[str, Any]:
    return {"refusal": {"reason": reason, **extra}}


def latest_month_witnessed_count(rows: pd.DataFrame, *, month: date) -> int:
    """Rows of ``month`` the state machine published as WITNESSED (not carried).

    A closed month the producer served dark (every row carried / NOT_RATED)
    yields 0: the publication would re-date every bucket to a month the
    market never saw. Reported by plan() and the determinism receipt; run()
    refuses to publish on 0 (``implied_rating_latest_month_unwitnessed``).
    Pure reporting: it changes no row, policy or digest.
    """
    if rows.empty:
        return 0
    on_month = pd.to_datetime(rows["month"]).dt.normalize() == pd.Timestamp(month).normalize()
    return int(rows.loc[on_month, "witnessed"].fillna(False).astype(bool).sum())


def build_payload_from_snapshot(
    snapshot: pd.DataFrame,
    *,
    last_closed_month: date,
    revision: str,
    panel_publication_id: str,
    input_fingerprint: str,
    pinned_anchor: float | None = None,
    resolve_pinned_anchor: Callable[[], float | None] | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Build the publication payload from an in-memory closed snapshot (pure).

    Returns ``{"refusal": {...}}`` or ``{"publication", "rows",
    "last_closed_month", "input_fingerprint", "rows_digest", "l_anchor",
    "resolved_l_anchor", "pinned_l_anchor", "policy_l_anchor", "anchor_source",
    "anchor_diagnostic_reason", "anchor_diagnostic_drift", "d_confirmed_count",
    "d_candidate_count", "latest_month_witnessed_count", "bucket_counts"}``.
    """
    log = LOGGER if logger is None else logger
    try:
        diagnostics = policy.market_anchor_diagnostics_for_snapshot(
            snapshot, last_closed_month=last_closed_month
        )
        policy_anchor = policy.policy_l_anchor()
        resolved_l_anchor = (
            None if diagnostics.resolved_l_anchor is None else policy.finite_anchor(
                diagnostics.resolved_l_anchor, field="resolved_l_anchor"
            )
        )
        l_anchor = policy.finite_anchor(diagnostics.l_anchor, field="chosen_l_anchor")
        if policy_anchor is not None and l_anchor != policy_anchor:
            raise policy.InvalidAnchor("chosen_l_anchor does not match the official policy pin")
    except policy.AnchorWindowEmpty:
        # No genuine observed market level globally (or no window median under
        # an explicitly unpinned policy). A fixed pin never invents witnesses.
        return _refusal("no_market_level_observation", panel_publication_id=panel_publication_id)
    except policy.InvalidAnchor:
        return _refusal("invalid_anchor", panel_publication_id=panel_publication_id)
    previous_anchor = (
        resolve_pinned_anchor() if resolve_pinned_anchor is not None else pinned_anchor
    )
    try:
        if previous_anchor is not None:
            previous_anchor = policy.finite_anchor(previous_anchor, field="pinned_l_anchor")
    except policy.InvalidAnchor:
        return _refusal("invalid_anchor", panel_publication_id=panel_publication_id)
    if policy_anchor is not None:
        if previous_anchor is not None and not math.isclose(
            previous_anchor, policy_anchor, rel_tol=0.0, abs_tol=ANCHOR_DRIFT_ABS_TOL
        ):
            # A foreign current pin is not a corrected window median. Do not
            # silently adopt it or treat a diagnostic drift as its authority.
            return _refusal(
                "anchor_policy_mismatch",
                pinned_l_anchor=previous_anchor, policy_l_anchor=policy_anchor,
                resolved_l_anchor=resolved_l_anchor,
            )
        l_anchor = policy_anchor
    elif previous_anchor is not None and resolved_l_anchor is not None and math.isclose(
        previous_anchor, resolved_l_anchor, rel_tol=0.0, abs_tol=ANCHOR_DRIFT_ABS_TOL
    ):
        # Historical unpinned policy: bind a bitwise-different inherited value.
        l_anchor = previous_anchor
    diagnostic_drift = (
        None if resolved_l_anchor is None else not math.isclose(
            l_anchor, resolved_l_anchor, rel_tol=0.0, abs_tol=ANCHOR_DRIFT_ABS_TOL
        )
    )
    log.info(
        "bond_market_implied_rating_v1 anchor: l_anchor=%s pinned_l_anchor=%s "
        "resolved_l_anchor=%s anchor_source=%s diagnostic_reason=%s",
        l_anchor, previous_anchor, resolved_l_anchor,
        diagnostics.anchor_source, diagnostics.diagnostic_reason,
    )
    rows = policy.build_publication_rows(
        snapshot, last_closed_month=last_closed_month, l_anchor=l_anchor
    )
    rows_digest = policy.rows_digest(rows)
    d_confirmed_count, d_candidate_count = policy.default_counts(rows)
    months = pd.to_datetime(rows["month"])
    if months.max().date() != last_closed_month:
        return _refusal(
            "snapshot_window_incomplete",
            panel_publication_id=panel_publication_id,
            panel_last_closed_month=last_closed_month.isoformat(),
            published_last_month=months.max().date().isoformat(),
        )
    identity_kwargs: dict[str, float] = {}
    if policy_anchor is None and resolved_l_anchor is not None and l_anchor.hex() != resolved_l_anchor.hex():
        # A fixed anchor is already bound canonically by POLICY_DIGEST. The
        # diagnostic must not introduce a second, data-dependent identity pin.
        identity_kwargs["inherited_l_anchor"] = l_anchor
    publication = ImpliedRatingPublication(
        publication_id=publication_id_for(
            policy.POLICY_DIGEST, revision, input_fingerprint, **identity_kwargs
        ),
        panel_publication_id=panel_publication_id,
        policy_version=policy.POLICY_VERSION,
        policy_digest=policy.POLICY_DIGEST,
        code_revision=revision,
        panel_last_closed_month=last_closed_month,
        first_month=months.min().date(),
        last_month=last_closed_month,
        input_fingerprint=input_fingerprint,
        l_anchor=l_anchor,
        rows_digest=rows_digest,
        d_confirmed_count=d_confirmed_count,
        d_candidate_count=d_candidate_count,
        row_count=len(rows),
    )
    return {
        "publication": publication,
        "rows": rows,
        "last_closed_month": last_closed_month,
        "input_fingerprint": input_fingerprint,
        "rows_digest": rows_digest,
        "l_anchor": l_anchor,
        "resolved_l_anchor": resolved_l_anchor,
        "pinned_l_anchor": previous_anchor,
        "policy_l_anchor": policy_anchor,
        "anchor_source": diagnostics.anchor_source,
        "anchor_diagnostic_reason": diagnostics.diagnostic_reason,
        "anchor_diagnostic_drift": diagnostic_drift,
        "d_confirmed_count": d_confirmed_count,
        "d_candidate_count": d_candidate_count,
        "latest_month_witnessed_count": latest_month_witnessed_count(
            rows, month=last_closed_month
        ),
        "bucket_counts": {
            str(bucket): int(count)
            for bucket, count in rows["implied_bucket"].value_counts().items()
        },
    }

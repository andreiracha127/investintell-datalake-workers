"""Monthly market-implied bond rating publication (``bond_market_implied_rating_v1``).

The producer half of the market-implied rating front: it derives the full
point-in-time state series -- market level, bucket, hysteresis, carry-forward,
spells, D candidate/confirmation and cure -- from the served panel snapshot
(``bond_panel_current_snapshot_v1_mat``) and publishes it as its own product.

WHAT IT READS (all candidates, months <= the panel's last closed month):
``cusip_id, month, price, spread_final_bps, mod_dur, trade_count,
dollar_volume, maturity_date``. The pure state machine lives in
``src.bonds.implied_rating``; this module only reads the database, pins the
publication and classifies failures.

FULL REBUILD. The machine needs the whole history (the level chain is
cumulative, carry-forward and hysteresis are path-dependent), so every
publication rewrites every closed month and the pointer flip is the only
"delta". ``BOND_IMPLIED_RATING_FORCE_REPUBLISH=1`` bypasses the
exact panel, policy and code identity short-circuit; the pointer CAS
still refuses a stale predecessor.

IDENTITY. ``uuid5(product | policy_version | policy_digest | code_revision |
input_fingerprint)`` -- the same closed panel rows under the same code and
policy replay the SAME publication; a changed policy or code mints a new one.
The code revision comes from the shared ladder (``CODE_REVISION``, ``GIT_SHA``,
``SOURCE_COMMIT``, ``RAILWAY_GIT_COMMIT_SHA``, then git). An unresolvable
revision refuses to publish rather than stamping "unknown".

The service is exercised by the daily hook in ``bond_live_daily`` (after the
panel publishes and its matviews refresh). This module does not create the
service and does not apply the DDL anywhere but its own connection: an operator
runbook owns production operations.

OBSERVABILITY. Before Phase 3 the daily hook reports this stage verdict-neutrally,
so every typed refusal (``anchor_drift``, ``publish_failed``, ``no_market_level_observation``,
...) is logged at WARNING level and carried in the day JSON: pre-Phase-3 alerting
must match on ``bond_market_implied_rating_v1`` warnings or on
``implied_rating.state``.
"""
from __future__ import annotations

import logging
import math
import os
import subprocess
import time
from datetime import date
from typing import Any

import pandas as pd
import psycopg

from src.bonds import implied_rating as policy
from src.bonds.build_manifest import collect_build_manifest, manifest_summary
from src.bonds.errors import BondError
from src.bonds.implied_rating import ANCHOR_DRIFT_ABS_TOL
from src.bonds.implied_rating_build import build_payload_from_snapshot
from src.bonds.implied_rating_materializer import (
    PRODUCT,
    ImpliedRatingPublication,
    build_fingerprint,
    current_pinned_anchor,
    install_schema,
    materialize,
)
from src.db import connect, resolve_dsn

LOGGER = logging.getLogger(__name__)

SNAPSHOT_MATVIEW = "bond_panel_current_snapshot_v1_mat"
STAGE_COLUMNS: tuple[str, ...] = (
    "cusip_id", "month", "price", "spread_final_bps", "mod_dur",
    "trade_count", "dollar_volume", "maturity_date",
)

# Build stamps a deploy may inject (the container image carries no ``.git``).
# Verbatim from src/workers/bond_metrics.py: one ladder, one fleet.
_REVISION_ENV_VARS = ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA")


def _code_revision() -> str:
    for var in _REVISION_ENV_VARS:
        value = os.getenv(var)
        if value:
            return value.strip()
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        stamped = out.stdout.strip()
        if stamped:
            return stamped
    except Exception:
        pass
    return "unknown"


def _force_republish_requested() -> bool:
    value = (os.getenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH") or "").strip().lower()
    return value in {"1", "true"}


def _relation_exists(conn: psycopg.Connection, name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()
    return bool(row and row[0] is not None)


def _current_panel(conn: psycopg.Connection) -> dict[str, Any] | None:
    """The panel's validated current publication (same CTE as the panel worker)."""
    row = conn.execute(
        "SELECT p.publication_id::text, p.first_month, p.last_closed_month, p.open_month "
        "FROM bond_panel_app_pointer pointer "
        "JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id "
        "WHERE pointer.product = 'bond_panel_v1' "
        "AND p.publication_status = 'validated'"
    ).fetchone()
    if row is None:
        return None
    return {
        "publication_id": str(row[0]),
        "first_month": row[1],
        "last_closed_month": row[2],
        "open_month": row[3],
    }


def _current_pointer(conn: psycopg.Connection) -> str | None:
    # A ledger this database never got is not an error: the pointer is simply
    # absent, and install_schema creates the tables right after the gates.
    if not _relation_exists(conn, "sec_derived_current_pointers"):
        return None
    row = conn.execute(
        "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product = %s",
        (PRODUCT,),
    ).fetchone()
    return None if row is None else str(row[0])


def _pointer_build(conn: psycopg.Connection, pointer: str) -> dict[str, Any] | None:
    """Read only the build owned by the product's current pointer."""
    if not _relation_exists(conn, f"{PRODUCT}_builds"):
        return None
    row = conn.execute(
        "SELECT b.publication_id::text, b.panel_publication_id::text, b.policy_digest, "
        "b.code_revision, b.panel_last_closed_month, s.lifecycle_state, b.input_fingerprint "
        f"FROM {PRODUCT}_builds b "
        "JOIN sec_derived_publications s USING (publication_id) "
        "WHERE b.publication_id = %s",
        (pointer,),
    ).fetchone()
    if row is None:
        return None
    return {
        "publication_id": str(row[0]),
        "panel_publication_id": str(row[1]),
        "policy_digest": row[2],
        "code_revision": row[3],
        "panel_last_closed_month": row[4],
        "lifecycle_state": row[5],
        "input_fingerprint": row[6],
    }


def _currentness_mismatch(
    build: dict[str, Any] | None, *, parent: dict[str, Any], revision: str
) -> list[str]:
    """Identity inputs available before reading the closed snapshot, in gate order."""
    if build is None:
        return ["pointer_build_absent"]
    return [
        reason
        for mismatch, reason in (
            (build["lifecycle_state"] != "validated", "pointer_not_validated"),
            (build["panel_publication_id"] != parent["publication_id"], "panel_publication_changed"),
            (build["panel_last_closed_month"] != parent["last_closed_month"], "panel_month_changed"),
            (build["policy_digest"] != policy.POLICY_DIGEST, "policy_digest_changed"),
            (build["code_revision"] != revision, "code_revision_changed"),
        )
        if mismatch
    ]


def _currentness(
    conn: psycopg.Connection, *, parent: dict[str, Any], revision: str, pointer: str | None
) -> tuple[str | None, list[str]]:
    if pointer is None:
        return None, []
    reasons = _currentness_mismatch(_pointer_build(conn, pointer), parent=parent, revision=revision)
    if reasons:
        LOGGER.info("bond_market_implied_rating_v1 pointer %s not current: %s", pointer, reasons)
        return None, reasons
    return pointer, []


def _already_current(
    conn: psycopg.Connection, *, parent: dict[str, Any], revision: str, pointer: str | None
) -> str | None:
    """The pointer's own validated build, only if all pre-read identity inputs match."""
    return _currentness(conn, parent=parent, revision=revision, pointer=pointer)[0]


def _mirror_serves_panel(conn: psycopg.Connection, *, parent: dict[str, Any]) -> bool:
    row = conn.execute(
        f"SELECT EXISTS (SELECT 1 FROM {SNAPSHOT_MATVIEW} "
        "WHERE month >= %s AND publication_id = %s)",
        (parent["last_closed_month"], parent["publication_id"]),
    ).fetchone()
    return bool(row and row[0])


def _failure(
    reason: str, *, elapsed: float, input_reasons: list[str] | None = None, **extra: Any
) -> dict[str, Any]:
    # Every typed refusal is WARNING-visible: before Phase 3 the daily hook
    # reports this stage verdict-neutrally, so the alerting surface is the log
    # line (and the day JSON), not a red run.
    LOGGER.warning(
        "bond_market_implied_rating_v1 %s: %s",
        reason,
        ", ".join(input_reasons) if input_reasons else "no input reason",
    )
    return {
        "state": reason.removeprefix("implied_rating_"),
        "reason": reason,
        "aborted": True,
        "elapsed_seconds": round(elapsed, 3),
        "input_reasons": input_reasons or [],
        **extra,
    }


def _read_snapshot(
    conn: psycopg.Connection, *, last_closed_month: date
) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(STAGE_COLUMNS)} FROM {SNAPSHOT_MATVIEW} "
            "WHERE month <= %s",
            (last_closed_month,),
        )
        columns = [column.name for column in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def _read_snapshot_inputs(
    conn: psycopg.Connection, *, parent: dict[str, Any], started: float
) -> dict[str, Any]:
    """One read and one canonical fingerprint for both convergence and rebuild."""
    try:
        snapshot = _read_snapshot(conn, last_closed_month=parent["last_closed_month"])
    except psycopg.Error as exc:
        return {"failure": _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=[f"snapshot_unreadable:{type(exc).__name__}"],
            panel_publication_id=parent["publication_id"],
        )}
    # The matview read is a single-statement snapshot. The panel commits its
    # forward-only pointer CAS before refreshing the mirror, so the pre-read
    # mirror probe plus an unchanged post-read pointer proves the rows belong
    # to the captured generation. A newer mirror requires a pointer move;
    # another mirror probe would add no protection against this race.
    current_parent = _current_panel(conn)
    if (
        current_parent is None
        or current_parent["publication_id"] != parent["publication_id"]
        or current_parent["last_closed_month"] != parent["last_closed_month"]
    ):
        return {"failure": _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=["panel_pointer_moved"],
            panel_publication_id=parent["publication_id"],
            current_panel_publication_id=(
                current_parent["publication_id"] if current_parent is not None else None
            ),
        )}
    if snapshot.empty:
        return {"failure": _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=["snapshot_empty"],
            panel_publication_id=parent["publication_id"],
        )}
    # The rows are in memory and their generation is proven: end the read
    # transaction before the full-frame fingerprint, so its ledger and mirror
    # AccessShare locks cannot queue another worker's DDL for minutes.
    conn.commit()
    return {"snapshot": snapshot, "input_fingerprint": policy.snapshot_fingerprint(snapshot)}


def _panel_inputs_current(
    conn: psycopg.Connection, *, parent: dict[str, Any], revision: str,
    pointer: str, started: float
) -> dict[str, Any]:
    """A new panel pointer can serve the same immutable implied publication."""
    inputs = _read_snapshot_inputs(conn, parent=parent, started=started)
    if "failure" in inputs:
        return inputs
    build = _pointer_build(conn, pointer)
    if build is not None and inputs["input_fingerprint"] == build["input_fingerprint"]:
        return {"current": {
            "state": "current",
            "aborted": False,
            "reason": "panel_inputs_unchanged",
            "publication_id": pointer,
            "panel_publication_id": parent["publication_id"],
            "build_panel_publication_id": build["panel_publication_id"],
            "panel_last_closed_month": parent["last_closed_month"].isoformat(),
            "input_fingerprint": inputs["input_fingerprint"],
            "code_revision": revision,
            "policy_digest": policy.POLICY_DIGEST,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }}
    return inputs


def _build_payload(
    conn: psycopg.Connection, *, parent: dict[str, Any], revision: str, started: float,
    snapshot_inputs: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Read the closed snapshot and build the publication payload (no writes).

    Returns ``{"failure": <typed dict>}`` when the read cannot produce a build,
    or ``{"publication", "rows", "last_closed_month", "input_fingerprint",
    "rows_digest", "d_confirmed_count", "d_candidate_count", "l_anchor",
    "resolved_l_anchor", "pinned_l_anchor", "bucket_counts"}``.
    """
    inputs = (
        _read_snapshot_inputs(conn, parent=parent, started=started)
        if snapshot_inputs is None else snapshot_inputs
    )
    if "failure" in inputs:
        return inputs

    def resolve_pinned_anchor() -> float | None:
        previous_anchor = (
            current_pinned_anchor(conn)
            if _relation_exists(conn, f"{PRODUCT}_builds")
            else None
        )
        # End the read transaction before the full-history build (tens of
        # minutes in production): an open transaction keeps AccessShare locks
        # on the shared ledger, which would queue any other derived worker's
        # DDL behind this build and every ledger reader behind that queued
        # DDL. Nothing read so far needs to stay locked -- materialize
        # re-checks the pointer by CAS.
        conn.commit()
        return previous_anchor

    # The build itself is the pure, process-independent function the
    # determinism replay runs in fresh subprocesses (src.bonds.implied_rating_build).
    built = build_payload_from_snapshot(
        inputs["snapshot"],
        last_closed_month=parent["last_closed_month"],
        revision=revision,
        panel_publication_id=parent["publication_id"],
        input_fingerprint=inputs["input_fingerprint"],
        resolve_pinned_anchor=resolve_pinned_anchor,
        logger=LOGGER,
    )
    if "refusal" in built:
        refusal = dict(built["refusal"])
        reason = refusal.pop("reason")
        return {"failure": _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=[reason],
            **refusal,
        )}
    return built


def _build_manifest() -> dict[str, Any]:
    """The runtime stack evidence every plan/run result carries (never an identity input).

    Collected once per invocation and logged in one line, so a production log
    shows WHICH interpreter / numpy / lock digest / SIMD baseline computed the
    digests it reports. See ``src.bonds.build_manifest``.
    """
    manifest = collect_build_manifest()
    LOGGER.info("bond_market_implied_rating_v1 build manifest: %s", manifest_summary(manifest))
    return manifest


def _revision_or_failure(started: float) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve the code revision before any connection: identity needs it."""
    revision = _code_revision()
    if revision == "unknown":
        return None, _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=["code_revision_absent"],
        )
    return revision, None


def _gates(conn: psycopg.Connection, *, started: float) -> dict[str, Any]:
    """Panel relations and the panel's current publication."""
    if not _relation_exists(conn, "bond_panel_publications"):
        return {"failure": _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=["relation_absent:bond_panel_publications"],
        )}
    if not _relation_exists(conn, SNAPSHOT_MATVIEW):
        return {"failure": _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=[f"relation_absent:{SNAPSHOT_MATVIEW}"],
        )}
    parent = _current_panel(conn)
    if parent is None:
        return {"failure": _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=["panel_no_parent"],
        )}
    if not _mirror_serves_panel(conn, parent=parent):
        return {"failure": _failure(
            "implied_rating_gate_failed",
            elapsed=time.monotonic() - started,
            input_reasons=["snapshot_mirror_stale"],
            panel_publication_id=parent["publication_id"],
        )}
    return {"parent": parent, "pointer": _current_pointer(conn)}


def plan(dsn: str | None = None) -> dict[str, Any]:
    """Read-only: exactly what ``run`` would publish right now, without writing.

    The backfill CLI's dry-run path. It never installs the schema, never pins a
    build and never moves the pointer; it reports the identity, digests, counts
    and bucket histogram the next publication would carry.
    """
    started = time.monotonic()
    revision, refusal = _revision_or_failure(started)
    if refusal is not None:
        return refusal
    manifest = _build_manifest()
    try:
        with connect(resolve_dsn(dsn)) as conn:
            gates = _gates(conn, started=started)
            if "failure" in gates:
                return gates["failure"]
            parent = gates["parent"]
            pointer = gates["pointer"]
            _, rebuild_reasons = _currentness(
                conn, parent=parent, revision=revision, pointer=pointer
            )
            payload_kwargs: dict[str, Any] = {}
            if not _force_republish_requested() and rebuild_reasons == ["panel_publication_changed"]:
                inputs = _panel_inputs_current(
                    conn, parent=parent, revision=revision, pointer=pointer, started=started
                )
                if "failure" in inputs:
                    return inputs["failure"]
                if "current" in inputs:
                    return {**inputs["current"], "build_manifest": manifest}
                payload_kwargs["snapshot_inputs"] = inputs
            if "snapshot_inputs" not in payload_kwargs:
                inputs = _read_snapshot_inputs(conn, parent=parent, started=started)
                if "failure" in inputs:
                    return inputs["failure"]
                payload_kwargs["snapshot_inputs"] = inputs
            prepared = _build_payload(
                conn, parent=parent, revision=revision, started=started, **payload_kwargs
            )
            if "failure" in prepared:
                return prepared["failure"]
            publication: ImpliedRatingPublication = prepared["publication"]
            previous_anchor = prepared["pinned_l_anchor"]
            drift = (
                policy.policy_l_anchor() is None
                and previous_anchor is not None
                and not math.isclose(
                    previous_anchor, prepared["resolved_l_anchor"],
                    rel_tol=0.0, abs_tol=ANCHOR_DRIFT_ABS_TOL,
                )
            )
            if drift:
                LOGGER.warning(
                    "bond_market_implied_rating_v1 anchor drift in plan: pinned=%s "
                    "resolved=%s; a publication under this anchor is refused",
                    previous_anchor,
                    prepared["resolved_l_anchor"],
                )
            return {
                "state": "anchor_drift" if drift else "planned",
                "aborted": False,
                "publication_id": publication.publication_id,
                "panel_publication_id": publication.panel_publication_id,
                "panel_last_closed_month": prepared["last_closed_month"].isoformat(),
                "first_month": publication.first_month.isoformat(),
                "last_month": publication.last_month.isoformat(),
                "policy_version": policy.POLICY_VERSION,
                "policy_digest": policy.POLICY_DIGEST,
                "code_revision": publication.code_revision,
                "input_fingerprint": prepared["input_fingerprint"],
                "rows_digest": prepared["rows_digest"],
                "row_count": publication.row_count,
                "d_confirmed_count": prepared["d_confirmed_count"],
                "d_candidate_count": prepared["d_candidate_count"],
                "l_anchor": prepared["l_anchor"],
                "resolved_l_anchor": prepared["resolved_l_anchor"],
                "pinned_l_anchor": previous_anchor,
                "policy_l_anchor": prepared.get("policy_l_anchor"),
                "anchor_source": prepared.get("anchor_source"),
                "anchor_diagnostic_reason": prepared.get("anchor_diagnostic_reason"),
                "anchor_diagnostic_drift": prepared.get("anchor_diagnostic_drift"),
                "anchor_drift": drift,
                "current_pointer": pointer,
                "bucket_counts": prepared["bucket_counts"],
                "build_manifest": manifest,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
    except BondError as exc:
        return _failure("implied_rating_gate_failed", elapsed=time.monotonic() - started,
                        input_reasons=[exc.code])
    except Exception as exc:  # a broken build is a typed state, never a crash
        LOGGER.exception("bond_market_implied_rating_v1 plan failed")
        return _failure("implied_rating_publish_failed", elapsed=time.monotonic() - started,
                        input_reasons=[f"{type(exc).__name__}"])


def run(dsn: str | None = None, *, as_of: date | None = None) -> dict[str, Any]:
    """Publish the implied-rating product, or return a typed refusal.

    ``as_of`` is accepted for programmatic callers and ignored: the product's
    data date is the PANEL's last closed month, which is what the app gates on.
    A replay is the rebuild itself -- ``BOND_IMPLIED_RATING_FORCE_REPUBLISH``
    (or the backfill CLI) is how one is asked for. ``WORKER_CALC_DATE`` is
    deliberately refused by ``run_worker`` (this ``run`` takes no ``calc_date``):
    a date the worker never reads must not be accepted from config.
    """
    started = time.monotonic()
    revision, refusal = _revision_or_failure(started)
    if refusal is not None:
        return refusal
    force_republish = _force_republish_requested()
    manifest = _build_manifest()
    try:
        with connect(resolve_dsn(dsn)) as conn:
            gates = _gates(conn, started=started)
            if "failure" in gates:
                return gates["failure"]
            parent = gates["parent"]
            pointer = gates["pointer"]
            current, rebuild_reasons = _currentness(
                conn, parent=parent, revision=revision, pointer=pointer
            )
            # The short-circuit runs BEFORE the rebuild: a daily hook that
            # sees the same panel, policy and code must not rebuild 3M rows.
            if not force_republish and current is not None:
                return {
                    "state": "current",
                    "aborted": False,
                    "reason": "implied_rating_already_current",
                    "publication_id": current,
                    "panel_publication_id": parent["publication_id"],
                    "panel_last_closed_month": parent["last_closed_month"].isoformat(),
                    "policy_digest": policy.POLICY_DIGEST,
                    "code_revision": revision,
                    "build_manifest": manifest,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                }
            payload_kwargs: dict[str, Any] = {}
            if not force_republish and rebuild_reasons == ["panel_publication_changed"]:
                inputs = _panel_inputs_current(
                    conn, parent=parent, revision=revision, pointer=pointer, started=started
                )
                if "failure" in inputs:
                    return inputs["failure"]
                if "current" in inputs:
                    return {**inputs["current"], "build_manifest": manifest}
                payload_kwargs["snapshot_inputs"] = inputs
            if "snapshot_inputs" not in payload_kwargs:
                inputs = _read_snapshot_inputs(conn, parent=parent, started=started)
                if "failure" in inputs:
                    return inputs["failure"]
                payload_kwargs["snapshot_inputs"] = inputs
            install_schema(conn)
            # Commit the DDL at once: it takes AccessExclusiveLock on the shared
            # ledger (sec_derived_publications / sec_derived_current_pointers),
            # and holding it through the build blocked every ledger reader for
            # ~55 minutes on the first production rebuild (2026-09-30).
            conn.commit()
            prepared = _build_payload(
                conn, parent=parent, revision=revision, started=started, **payload_kwargs
            )
            if "failure" in prepared:
                return prepared["failure"]
            publication: ImpliedRatingPublication = prepared["publication"]
            previous_anchor = prepared["pinned_l_anchor"]
            if policy.policy_l_anchor() is None and previous_anchor is not None and not math.isclose(
                previous_anchor, prepared["resolved_l_anchor"],
                rel_tol=0.0, abs_tol=ANCHOR_DRIFT_ABS_TOL,
            ):
                # The closed window no longer resolves the anchor the current
                # publication was built with: publishing would silently rewrite
                # every historical bucket. Stop and let the calibration round
                # decide (a policy anchor bump, digest and all).
                return _failure(
                    "implied_rating_gate_failed",
                    elapsed=time.monotonic() - started,
                    input_reasons=["anchor_drift"],
                    pinned_l_anchor=previous_anchor,
                    resolved_l_anchor=prepared["resolved_l_anchor"],
                )
            result = materialize(
                conn, publication, prepared["rows"], expected_pointer=pointer
            )
            build_parent_id = publication.panel_publication_id
            if result.reused:
                persisted_build = _pointer_build(conn, result.publication_id)
                if persisted_build is None:
                    raise BondError("build_pin_mismatch", {"publication_id": result.publication_id})
                build_parent_id = persisted_build["panel_publication_id"]
            if prepared["d_confirmed_count"] == 0:
                # Publish anyway (a young or quiet history is not an error), but
                # the caller must be able to see that the producer's positive
                # control is not met -- the app's §8 gate decides what to do.
                LOGGER.warning(
                    "bond_market_implied_rating_v1 publication %s carries zero confirmed "
                    "defaults; the app's default-capacity gate will judge it",
                    publication.publication_id,
                )
            return {
                "state": "published_no_defaults" if prepared["d_confirmed_count"] == 0 else "published",
                "aborted": False,
                "publication_id": result.publication_id,
                "panel_publication_id": build_parent_id,
                "panel_last_closed_month": publication.panel_last_closed_month.isoformat(),
                "policy_version": policy.POLICY_VERSION,
                "policy_digest": policy.POLICY_DIGEST,
                "code_revision": publication.code_revision,
                "input_fingerprint": prepared["input_fingerprint"],
                "rows_digest": prepared["rows_digest"],
                "row_count": result.row_count,
                "d_confirmed_count": prepared["d_confirmed_count"],
                "d_candidate_count": prepared["d_candidate_count"],
                "l_anchor": prepared["l_anchor"],
                "resolved_l_anchor": prepared["resolved_l_anchor"],
                "pinned_l_anchor": previous_anchor,
                "policy_l_anchor": prepared.get("policy_l_anchor"),
                "anchor_source": prepared.get("anchor_source"),
                "anchor_diagnostic_reason": prepared.get("anchor_diagnostic_reason"),
                "anchor_diagnostic_drift": prepared.get("anchor_diagnostic_drift"),
                "bucket_counts": prepared["bucket_counts"],
                "reused_publication": result.reused,
                "rebuild_reasons": rebuild_reasons,
                "build_fingerprint": build_fingerprint(
                    policy.POLICY_DIGEST, publication.code_revision,
                    prepared["input_fingerprint"],
                ),
                "build_manifest": manifest,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
    except BondError as exc:
        return _failure(
            "implied_rating_gate_failed" if exc.code == "pointer_moved"
            else "implied_rating_publish_failed",
            elapsed=time.monotonic() - started,
            input_reasons=[exc.code],
        )
    except Exception as exc:  # a broken build is a typed state, never a crash
        LOGGER.exception("bond_market_implied_rating_v1 publication failed")
        return _failure(
            "implied_rating_publish_failed",
            elapsed=time.monotonic() - started,
            input_reasons=[f"{type(exc).__name__}"],
        )

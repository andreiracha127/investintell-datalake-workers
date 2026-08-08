"""Registered materializer for the public ``bond_serving_v1`` product.

Projects the current bond security / price-lane / N-PORT reverse-lookup snapshots
into the public-only ``bond_serving_facts`` surface across the four bond serving
surfaces (catalog / detail / observations / fund_exposure) and promotes one
complete serving version via the shared derived-publication current pointer. A
SIBLING product to ``sec_regulatory_serving_v1`` -- bonds publish on their own
cadence/lifecycle so fund dossier freshness never couples to bond freshness. The
app pins an exact publication of this product.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint: ships without running any production backfill or authorizing
any production price/holdings source (the 144A pricing pilot authorizes none). When
no current bond snapshot / validated source anchor exists the worker is a no-op.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from datetime import date
from typing import Any

from src.bonds import serving_contract as contract
from src.bonds import serving_materializer as materializer
from src.db import LOCK_BOND_SERVING, advisory_lock, connect, resolve_dsn


def _code_revision() -> str:
    """The revision the publication identity is derived from.

    ``CODE_REVISION`` first, exactly like ``bond_security_master`` and
    ``mixed_quant_publication``: the deployed image carries no ``.git``, so the
    git fallback returns "unknown" there and every build of a given ``as_of``
    collapses onto ONE ``publication_id``. ``materialize`` treats an existing id
    as already built and only re-points, so a code change would silently re-serve
    the previous payload instead of rebuilding -- which is exactly what a Wave 1b
    republication hit on 2026-07-30. The dl-bond-chain job already sets the env
    var; this makes the worker honour it.
    """
    configured = os.getenv("CODE_REVISION")
    if configured:
        return configured
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    """The day this serving snapshot speaks for: the freshest input it carries.

    It used to be the security master's ``max(measured_at)`` alone, and that is
    what made a DAILY refresh structurally impossible (measured 2026-08-07): the
    publication identity is ``uuid5(product | as_of | code_revision)``, the
    master's measured_at sat at 2026-07-23, and ``CODE_REVISION`` only moves on a
    deploy -- so every run of an undeployed day resolved to the SAME
    publication_id, which ``materialize`` treats as already built and merely
    re-points. Fresh prices could land forever and the served payload would never
    change.

    Taking the greatest of the master's date and the dense daily series' last day
    fixes that at the honest end: the snapshot's as_of now follows its data. What
    was rejected here was a fingerprint used INSTEAD of an honest as_of, which
    would mint a new publication every day while the payload still CLAIMED
    2026-07-23 -- a fresh price stamped with a stale date is the one outcome worse
    than a stale price. ``_input_digest`` below is the other thing: as_of stays
    the data date and the digest only splits identity WITHIN one as_of, for the
    case this function cannot see (same day, changed values).

    The free property survives: on a day nothing advances (a weekend), as_of does
    not move, the digest is identical, the identity replays, and the build is a
    cheap re-point instead of a needless 2M-row rewrite.
    """
    if calc_date:
        return date.fromisoformat(calc_date)
    anchors: list[date] = []
    row = conn.execute("SELECT max(measured_at) FROM sec_current_bond_security_v1").fetchone()
    if row and row[0]:
        anchors.append(row[0])
    if conn.execute("SELECT to_regclass('bond_observation_daily')").fetchone()[0]:
        row = conn.execute("SELECT max(day) FROM bond_observation_daily").fetchone()
        if row and row[0]:
            anchors.append(row[0])
    return max(anchors) if anchors else None


# --------------------------------------------------------------------------- #
# Same-day content revisions
# --------------------------------------------------------------------------- #
# The publication identity is ``uuid5(product | as_of | code_revision)`` and
# ``materialize`` treats an existing id as already built -- it only re-points. So
# an as_of that follows the data is necessary but NOT sufficient: the daily candle
# loader deliberately re-reads the watermark day to pick up revised closes, and a
# governed source (metrics, prices, the security master) can republish for the
# same day. Both change what the serving surface should say WITHOUT moving as_of,
# and both would replay onto the same publication_id -- a green run that serves
# yesterday's price and yesterday's metric rows. That is the exact trap this
# program has already paid for once (2026-07-30, Wave 1b: no discriminant -> the
# id collided -> the publication was already validated -> materialize only
# re-pointed).
#
# The discriminant must be CONTENT, and it must be deterministic: running twice
# over unchanged inputs has to produce the same id, or replay and idempotence are
# gone (a clock or a random salt would mint a 1.2 GB publication per run). So the
# digest is a pure function of the inputs:
#   * the current publication of every source product the serving contract
#     consumes -- this is what catches a same-day metric/price/master
#     republication;
#   * the dense daily series' watermark slice AT or BEFORE as_of: its day, its row
#     count and a checksum of exactly the columns the build reads -- this is what
#     catches a revised close.
#
# Bounded by ``as_of`` on purpose: a ``calc_date`` replay of an older day must
# digest THAT day's slice, never today's, or two identical replays would mint two
# publications.
#
# Numeric values are rounded to a fixed scale before hashing: numeric carries its
# scale into ``::text``, so 98.6 reloaded as 98.600 would otherwise read as a
# revision and rebuild 2M rows for nothing.
#
# HONEST RESIDUAL: only the watermark day is checksummed. A revision to an OLDER
# day inside the dense series does not move the digest. That is scoped to what the
# loader actually does (it re-reads the watermark day); a deliberate history
# reload is a deploy-shaped event and moves ``CODE_REVISION``.
#
# The watermark day is resolved in its OWN statement and then bound as a constant,
# which is not a style choice: measured against production 2026-08-07, folding it
# into the checksum query as a CTE makes the day a runtime value, TimescaleDB
# cannot exclude chunks while planning, and the plan opens an index scan on every
# chunk of the series -- 6.5 s planning + 7.8 s execution warm (36 s cold), growing
# with every year of history. With the day bound as a constant the plan touches
# exactly ONE chunk: 0.6 ms planning, 7.6 ms execution, for the identical result.
_DIGEST_POINTERS_SQL = """
SELECT product, publication_id FROM sec_derived_current_pointers
WHERE product = ANY(%s) ORDER BY product
"""

_DIGEST_WATERMARK_SQL = "SELECT max(day) FROM bond_observation_daily WHERE day <= %s"

_DIGEST_DENSE_SQL = """
SELECT count(*),
       md5(coalesce(string_agg(
           o.cusip9 || '|' || round(o.price, 10)::text
                    || '|' || coalesce(round(o.ytm, 10)::text, '')
                    || '|' || coalesce(o.price_type, '')
                    || '|' || coalesce(o.accrued, '')
                    || '|' || o.source_rank::text,
           E'\\n' ORDER BY o.cusip9, o.source_rank), ''))
FROM bond_observation_daily o
WHERE o.day = %s AND o.price IS NOT NULL AND o.price > 0
"""


def _input_digest(conn: Any, as_of: date) -> str:
    """A deterministic fingerprint of everything the build projects for ``as_of``.

    Same inputs -> same digest -> same publication_id -> cheap re-point. Changed
    inputs on the SAME as_of -> new digest -> a new publication that actually gets
    built. An environment without the dense series (every unit database) digests a
    fixed marker, so its identities stay exactly as reproducible as before.
    """
    parts: list[str] = [as_of.isoformat()]
    products = sorted({surface["source_product"] for surface in contract.SURFACES})
    for product, publication_id in conn.execute(_DIGEST_POINTERS_SQL, (products,)).fetchall():
        parts.append(f"{product}={publication_id}")
    if conn.execute("SELECT to_regclass('bond_observation_daily')").fetchone()[0]:
        watermark = conn.execute(_DIGEST_WATERMARK_SQL, (as_of,)).fetchone()[0]
        if watermark is None:
            parts.append("dense=none")
        else:
            rows, checksum = conn.execute(_DIGEST_DENSE_SQL, (watermark,)).fetchone()
            parts.append(f"dense={watermark.isoformat()}:{rows}:{checksum}")
    else:
        parts.append("dense=absent")
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:12]


def _advance_app_pin(conn: Any, worker_publication_id: str) -> dict[str, Any]:
    """Advance the app-side serving pin to the just-validated worker publication.

    The app reads through ``bond_serving_publications`` (its own prepared ->
    validated -> current protocol) and pins an EXACT worker publication_id, so a
    fresh serving build is invisible until this pin advances. Doing it here —
    only AFTER the worker publication is validated and current — removes the
    manual re-pin step without weakening the guarantee the pin exists for (the
    app never reads a half-built serving).

    Honest no-ops: the app protocol absent (``app_protocol_missing``, e.g. unit
    schemas that never install the app DDL) or the publication already pinned
    (``already_pinned``). A failure to advance raises — the operator must see
    it, because the app would silently keep serving the previous payload.
    """
    if not conn.execute(
        "SELECT to_regclass('bond_serving_publications') IS NOT NULL"
    ).fetchone()[0]:
        return {"app_pin": "app_protocol_missing"}
    worker = conn.execute(
        "SELECT publication_id, publication_version FROM sec_derived_publications "
        "WHERE publication_id=%s AND lifecycle_state='validated'",
        (worker_publication_id,),
    ).fetchone()
    if worker is None:
        return {"app_pin": "worker_publication_not_validated"}
    already = conn.execute(
        "SELECT app_publication_id FROM bond_serving_publications "
        "WHERE worker_publication_id=%s AND lifecycle_state='validated'",
        (worker_publication_id,),
    ).fetchone()
    if already is not None:
        return {"app_pin": "already_pinned", "app_publication_id": str(already[0])}
    app_id, next_version = conn.execute(
        "SELECT gen_random_uuid(), COALESCE(max(app_publication_version),0)+1 "
        "FROM bond_serving_publications"
    ).fetchone()
    conn.execute(
        "INSERT INTO bond_serving_publications "
        "(app_publication_id, app_publication_version, worker_publication_id, "
        " worker_publication_version, lifecycle_state) "
        "VALUES (%s,%s,%s,%s,'prepared')",
        (app_id, next_version, worker[0], worker[1]),
    )
    conn.execute("SELECT bond_validate_serving_publication(%s)", (app_id,))
    conn.execute("SELECT bond_set_current_serving_publication(%s)", (app_id,))
    return {
        "app_pin": "advanced",
        "app_publication_id": str(app_id),
        "app_publication_version": next_version,
    }


# --------------------------------------------------------------------------- #
# Retention
# --------------------------------------------------------------------------- #
# A complete serving publication is ~2.0M facts / ~1.2 GB (measured 2026-08-07).
# That was harmless while a rebuild needed a deploy; now that ``as_of`` follows
# the daily series a rebuild happens most days, and unbounded retention would add
# ~1.2 GB/day forever. So the worker prunes what nothing can reach.
#
# The keep-set is a UNION of three, and all three are load-bearing:
#   * the worker's own current pointer -- what the next build compares against;
#   * whatever the APP's current pin references -- the pin advance can honestly
#     fail (``app_protocol_missing``, ``worker_publication_not_validated``) and
#     leave the app on an older publication; deleting that one breaks the
#     product silently, which is the worst failure available here;
#   * the immediately-prior worker publication -- ``daily_chain`` compensation
#     restores the PRE-RUN pointer on a failed run, and restoring a pointer to a
#     publication whose facts were deleted is an empty product.
#
# HOW THE ROWS ACTUALLY GO. ``bond_serving_facts`` is immutable by construction:
# ``bond_serving_facts_write_guard`` is a BEFORE INSERT OR UPDATE OR DELETE row
# trigger that rejects every non-INSERT, the publication_id FK is ON DELETE
# RESTRICT and ``sec_derived_publication_delete_guard`` refuses to delete a
# validated publication -- so deleting the parent is not a way around it either.
# (``prune_quant_publications``, the other retention routine in this repo, does not
# transfer: it deletes the PARENT and rides ON DELETE CASCADE. Both halves are the
# opposite of this schema.) That immutability is worth keeping, so the schema now
# carries ONE sanctioned exception instead of a relaxed guard, in the protocol's
# own idiom (``sec_derived_publication_tokens`` / ``sec_derived_pointer_tokens``):
#   * ``bond_serving_purge_tokens(publication_id, backend_pid)``, revoked from
#     PUBLIC, says "backend N is purging this publication right now";
#   * the write guard returns OLD for a DELETE on the FACTS table when THIS
#     backend holds that token, and raises the identical
#     'bond serving row is immutable' for everything else -- every UPDATE, every
#     untokened DELETE, and any DELETE against ``bond_serving_builds``;
#   * ``bond_purge_serving_publication(uuid, batch int)`` takes the token, deletes
#     ONE bounded batch and drops the token, all inside the caller's transaction.
# So the loop below commits per batch: one 2M-row DELETE would be a long
# transaction, and a long transaction holds back VACUUM for the WHOLE database (a
# trap this repo has already paid for).
#
# The build row STAYS. ``sec_derived_publication_as_of`` reads
# ``bond_serving_builds`` and that is what feeds the current-pointer as_of
# regression guard; purging it would break the guard, not merely lose metadata. A
# purged publication therefore stays in the ledger, visibly, with its build
# metadata, holding no facts.
#
# WHAT THIS DOES AND DOES NOT RECLAIM (measured, production, 2026-08-07):
# ``bond_serving_facts`` is a PLAIN table, not a hypertable -- there are no chunks
# to drop. A DELETE marks tuples dead; VACUUM then makes that space reusable BY
# THIS TABLE, but does not return it to the operating system (only VACUUM FULL
# would, and it takes an ACCESS EXCLUSIVE lock on a table the app reads). The
# effect is therefore a PLATEAU, not a shrink: the first production purge freed
# 1,945,768 rows across 5 unreachable publications (5,988,124 -> 4,042,356 rows,
# 40 batches, 107.6 s, no transaction longer than one batch) and the table stayed
# at 3,446 MB -- roughly a third of it turns into free space the next publication
# is written INTO instead of extending the file. That is the point: steady state
# is a handful of publications instead of +1.2 GB per changed day. The number that
# says retention is working is count(DISTINCT publication_id), not the table size.
#
# And VACUUM only reclaims once no snapshot older than the delete survives: the
# first vacuum after that purge reported all 1,945,768 tuples "dead but not yet
# removable" because an unrelated hour-old transaction elsewhere in the database
# was pinning the global xmin horizon. Twenty minutes later, with the pin gone,
# one vacuum took n_dead_tup to 0 in 1.6 s and the table was still 3,446 MB.
# Nothing to do with the purge and nothing to do about it but wait -- but do not
# read it as a failure.
#
# ``blocked_by_write_guard`` below is what remains for a database whose installed
# schema predates the purge routine: the guard exists, nothing can free the rows,
# and retention says so ONCE with the numbers instead of firing a DELETE that is
# certain to raise and reporting an anonymous ``retention.failed`` every run.
_KEEP_WORKER_CURRENT = (
    "SELECT publication_id FROM sec_derived_current_pointers "
    "WHERE product = 'bond_serving_v1'"
)
_KEEP_APP_PINNED = (
    "SELECT s.worker_publication_id FROM bond_serving_app_current_pointer p "
    "JOIN bond_serving_publications s ON s.app_publication_id = p.app_publication_id"
)
_KEEP_TWO_MOST_RECENT = (
    "SELECT publication_id FROM (SELECT publication_id FROM sec_derived_publications "
    "WHERE product = 'bond_serving_v1' ORDER BY publication_version DESC LIMIT 2) recent"
)

_STALE_CANDIDATES_SQL = (
    "SELECT publication_id FROM sec_derived_publications "
    "WHERE product = 'bond_serving_v1' AND NOT (publication_id = ANY(%s))"
)
_STALE_ROWS_SQL = (
    "SELECT publication_id, count(*) FROM bond_serving_facts "
    "WHERE publication_id = ANY(%s) GROUP BY 1"
)

_PRUNE_BATCH_SQL = """
DELETE FROM bond_serving_facts
WHERE ctid = ANY (ARRAY(
    SELECT ctid FROM bond_serving_facts
    WHERE publication_id = ANY(%s) LIMIT %s
))
"""

PRUNE_BATCH_ROWS = 50_000

# Can this database PURGE? The question is deliberately not "is the guard gone":
# the guard trigger survives the purge routine (that is the whole design -- the
# rows stay immutable except through the token), so probing for its ABSENCE would
# report ``blocked`` forever on a database that can purge perfectly well. What is
# probed is the CAPABILITY itself: the token table plus the exact routine
# signature the loop calls.
_PURGE_CAPABILITY_SQL = """
SELECT to_regclass('bond_serving_purge_tokens') IS NOT NULL
   AND to_regprocedure('bond_purge_serving_publication(uuid,integer)') IS NOT NULL
"""

_PURGE_BATCH_SQL = "SELECT bond_purge_serving_publication(%s, %s)"

# Is a row-level DELETE trigger installed on the facts table? tgtype bit 3 (8) is
# DELETE. Only consulted when the purge routine is ABSENT, to tell the two
# remaining worlds apart: a schema that predates the routine (guarded -> nothing
# can free the rows -> say so with the numbers) from one that never guarded the
# table at all (a test schema that drops the trigger -> a plain batched DELETE
# works). The probe asks the catalog rather than hardcoding a belief.
_DELETE_GUARD_SQL = """
SELECT tgname FROM pg_trigger
WHERE tgrelid = 'bond_serving_facts'::regclass AND NOT tgisinternal
  AND (tgtype & 8) <> 0
ORDER BY tgname LIMIT 1
"""

RETENTION_BLOCKED_ACTION = (
    "bond_serving_facts is delete-guarded and this database has no purge routine: "
    "the write guard raises 'bond serving row is immutable' on any non-INSERT, the "
    "publication_id FK is ON DELETE RESTRICT and sec_derived_publication_delete_guard "
    "forbids deleting a validated publication, so nothing here frees these rows. "
    "Re-apply schemas/bond_serving_v1.sql: it installs bond_serving_purge_tokens and "
    "bond_purge_serving_publication(uuid, integer), the token-gated path this worker "
    "purges through (facts only -- the bond_serving_builds row is kept because "
    "sec_derived_publication_as_of reads it). Until then bond_serving_facts grows by "
    "one full publication (~1.2 GB) per content change."
)


def _prune_superseded_facts(dsn: str, *, batch: int = PRUNE_BATCH_ROWS) -> dict[str, Any]:
    """Free the facts of serving publications nothing can reach any more.

    Purges through the token-gated ``bond_purge_serving_publication`` routine, one
    bounded batch per transaction. Still refuses in a TYPED way
    (``blocked_by_write_guard``) on a database whose installed schema predates that
    routine, instead of firing a DELETE that is certain to raise and reporting an
    anonymous ``retention.failed`` on every run.
    """
    with connect(dsn) as conn:
        if not conn.execute(
            "SELECT to_regclass('bond_serving_facts') IS NOT NULL"
        ).fetchone()[0]:
            conn.commit()
            return {"pruned_publications": 0, "pruned_rows": 0, "state": "no_facts_table"}
        arms = [_KEEP_WORKER_CURRENT, _KEEP_TWO_MOST_RECENT]
        if conn.execute(
            "SELECT to_regclass('bond_serving_app_current_pointer') IS NOT NULL"
        ).fetchone()[0]:
            arms.append(_KEEP_APP_PINNED)
        # An absent app protocol drops that ARM. Dropping a keep arm can only
        # WIDEN the stale set, so it is gated on the pointer relation and not on
        # the pin table: where the app protocol is not installed nothing is pinned
        # through it, and there is no keep to lose.
        keep = [
            row[0]
            for row in conn.execute("\nUNION\n".join(arms)).fetchall()
            if row[0]
        ]
        # Which publications are stale is answered by the PUBLICATIONS table (nine
        # rows in production), and only then are their rows counted through the
        # index. Asking the 3.4 GB facts table which publication_ids it holds --
        # ``DISTINCT``, or a bare ``GROUP BY`` -- reads the whole relation: 4m28s
        # measured against production 2026-08-07, against 2.1 s for the index-only
        # count below. A daily worker cannot pay four minutes to discover it has
        # nothing to do. Publications holding no rows simply do not come back from
        # the count, which is the same stale set the facts scan produced.
        candidates = [
            row[0] for row in conn.execute(_STALE_CANDIDATES_SQL, (keep,)).fetchall()
        ]
        by_publication = (
            conn.execute(_STALE_ROWS_SQL, (candidates,)).fetchall() if candidates else []
        )
        stale = [pub for pub, _ in by_publication]
        stale_rows = sum(rows for _, rows in by_publication)
        can_purge = conn.execute(_PURGE_CAPABILITY_SQL).fetchone()[0]
        guard = conn.execute(_DELETE_GUARD_SQL).fetchone() if not can_purge else None
        conn.commit()
        if not stale:
            # Nothing to free: never raise a retention alarm on a healthy steady
            # state just because the guard exists.
            return {"pruned_publications": 0, "pruned_rows": 0, "kept": len(keep)}
        if can_purge:
            # One publication at a time, one COMMITTED batch at a time. The
            # routine's own refusals (worker-current / app-pinned) are the net
            # under the keep-set computed above: if they ever disagree the purge
            # raises and the operator sees it, rather than a live publication
            # quietly losing its rows.
            purged: dict[str, int] = {}
            removed = 0
            for publication in stale:
                freed = 0
                while True:
                    deleted = conn.execute(
                        _PURGE_BATCH_SQL, (publication, batch)
                    ).fetchone()[0]
                    conn.commit()
                    freed += deleted
                    if deleted < batch:
                        break
                purged[str(publication)] = freed
                removed += freed
            return {
                "state": "purged",
                "pruned_publications": len(stale),
                "pruned_rows": removed,
                "kept": len(keep),
                "purged": purged,
            }
        if guard is not None:
            return {
                "state": "blocked_by_write_guard",
                "guard_trigger": guard[0],
                "blocked_publications": len(stale),
                "blocked_rows": stale_rows,
                "kept": len(keep),
                "action": RETENTION_BLOCKED_ACTION,
            }
        removed = 0
        while True:
            deleted = conn.execute(_PRUNE_BATCH_SQL, (stale, batch)).rowcount
            conn.commit()
            removed += deleted
            if deleted < batch:
                break
    return {"pruned_publications": len(stale), "pruned_rows": removed, "kept": len(keep)}


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_SERVING) as acquired:
        if not acquired:
            return {"state": "locked", "rows": 0}
        materializer.install_schema(conn)
        # No current bond security snapshot yet -> nothing to serve (dark until backfill).
        if not conn.execute(
            "SELECT to_regclass('sec_current_bond_security_v1') IS NOT NULL"
        ).fetchone()[0]:
            conn.commit()
            return {"state": "no_source", "rows": 0}
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_securities", "rows": 0}
        # Identity = code revision + a content digest of the inputs. The digest is
        # what makes a same-day revision a NEW publication instead of a re-point of
        # the stale one; it is deterministic, so an unchanged replay still lands on
        # the same publication_id.
        revision = f"{_code_revision()}+{_input_digest(conn, as_of)}"
        try:
            result = materializer.materialize(conn, as_of=as_of, code_revision=revision)
        except (
            materializer.BondFundExposureMultiplicationError,
            materializer.BondServingSurfaceCoverageError,
        ):
            # Integrity/coverage violations are ACTIONABLE failures (spec §5: never a
            # silent success). Roll back the partial build but let the typed error
            # PROPAGATE -- it must never be laundered into the empty-source dark state
            # below, which is indistinguishable from a genuinely absent source.
            conn.rollback()
            raise
        except RuntimeError:
            # No validated source run/package anchor yet -> dark until backfill.
            conn.rollback()
            return {"state": "no_source", "rows": 0}
        conn.commit()
        # Worker publication validated + current: advance the app pin in its own
        # transaction (a pin failure must not roll back the worker publication).
        pin = _advance_app_pin(conn, result["publication_id"])
        conn.commit()
    # Retention runs LAST, on its own connection, outside the serving lock's
    # transaction: it must never be able to roll back a promotion, and it must
    # see the pin this run just advanced (which is what keeps the app's
    # publication in the keep-set).
    try:
        retention = _prune_superseded_facts(resolve_dsn(dsn))
    except Exception as exc:  # reported, never fatal: the publication is current
        retention = {"state": "failed", "error": f"{type(exc).__name__}: {exc}"}
    # ``code_revision`` rides along so an operator can tell WHY a run rebuilt (a
    # deploy moved the revision, or the digest moved because the inputs did)
    # without re-deriving the identity by hand.
    return {"state": "ok", **result, "code_revision": revision, **pin, "retention": retention}

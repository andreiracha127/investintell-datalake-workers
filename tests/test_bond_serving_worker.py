"""Worker-level tests for ``src.workers.bond_serving.run`` error routing.

Regression guard for the review Important: an integrity/coverage violation
(``BondFundExposureMultiplicationError`` / ``BondServingSurfaceCoverageError`` --
both ``RuntimeError`` subclasses) must be an ACTIONABLE signal that PROPAGATES out
of ``run()`` (spec §5: never a silent success), never laundered into the
``no_source`` dark state that a genuinely absent source produces. The worker opens
its own connection, so these tests commit an isolated schema and point ``run()`` at
it via a ``search_path`` DSN.

DSN-agnostic (Global Constraint): reads ``SEC_TEST_DATABASE_URL``.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import pytest

from src.bonds import serving_materializer as materializer
from src.workers import bond_serving

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_serving_fixtures import (  # noqa: E402
    SEC1,
    SEC2,
    connect,
    protocol_only_schema,
    setup,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)


def _search_path_dsn(schema: str) -> str:
    # The worker opens its OWN connection; route it into the isolated test schema.
    base = os.environ["SEC_TEST_DATABASE_URL"]
    if base.startswith("postgres"):  # URL form
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}options=-c%20search_path%3D{schema}"
    return f"{base} options='-c search_path={schema}'"  # keyword form


def test_run_propagates_integrity_error_and_never_reports_no_source() -> None:
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        # Force the multiplication: SEC2 now shares SEC1's CUSIP -> a holding lot
        # maps to two securities -> the fund_exposure guard must hard-fail.
        cur.execute(
            "UPDATE sec_current_bond_security_alias_v1 SET alias_value='037833100' WHERE security_id=%s",
            (SEC2,),
        )
        admin.commit()

        with pytest.raises(materializer.BondFundExposureMultiplicationError):
            bond_serving.run(_search_path_dsn(schema))
        # nothing promoted (the actionable failure never became a serving version).
        pointer = admin.execute(
            f'SELECT publication_id FROM "{schema}".sec_derived_current_pointers '
            "WHERE product='bond_serving_v1'"
        ).fetchone()
        assert pointer is None
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_run_reports_no_source_when_snapshot_is_genuinely_absent() -> None:
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = protocol_only_schema(cur)  # no sec_current_bond_security_v1
        admin.commit()
        result = bond_serving.run(_search_path_dsn(schema))
        assert result == {"state": "no_source", "rows": 0}
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_run_reports_app_protocol_missing_when_the_pin_table_is_absent() -> None:
    """A successful worker publication with NO app-side protocol installed is an
    honest no-op on the pin lane (unit schemas never install the app DDL)."""
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        admin.commit()
        result = bond_serving.run(_search_path_dsn(schema))
        assert result["state"] == "current"
        assert result["app_pin"] == "app_protocol_missing"
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def _install_app_pin_protocol(admin, schema: str) -> None:
    """A minimal replica of the app-side pin protocol (table + functions).

    The real DDL lives in the app repo; this replica keeps the SAME call
    signature (``bond_validate_serving_publication(uuid)`` ->
    ``bond_set_current_serving_publication(uuid)``) so the worker's pin lane is
    exercised end-to-end.
    """
    # The sql-language bodies reference unqualified relations: point the creating
    # session's search_path at the schema so check_function_bodies passes.
    admin.execute(f'SET search_path TO "{schema}"')
    admin.execute(f"""
        CREATE TABLE "{schema}".bond_serving_publications(
            app_publication_id uuid PRIMARY KEY,
            app_publication_version integer NOT NULL,
            worker_publication_id uuid NOT NULL,
            worker_publication_version integer NOT NULL,
            lifecycle_state text NOT NULL,
            prepared_at timestamptz NOT NULL DEFAULT now(),
            validated_at timestamptz)
    """)
    admin.execute(f"""
        CREATE FUNCTION "{schema}".bond_validate_serving_publication(pub uuid)
        RETURNS void LANGUAGE sql AS
        'UPDATE bond_serving_publications
         SET lifecycle_state=''validated'', validated_at=now()
         WHERE app_publication_id=pub'
    """)
    admin.execute(f"""
        CREATE TABLE "{schema}".bond_serving_current_pointer(
            singleton boolean PRIMARY KEY DEFAULT true,
            app_publication_id uuid NOT NULL)
    """)
    admin.execute(f"""
        CREATE FUNCTION "{schema}".bond_set_current_serving_publication(pub uuid)
        RETURNS void LANGUAGE sql AS
        'INSERT INTO bond_serving_current_pointer(singleton, app_publication_id)
         VALUES (true, pub)
         ON CONFLICT (singleton) DO UPDATE SET app_publication_id=EXCLUDED.app_publication_id'
    """)
    admin.execute("SET search_path TO public")


def test_run_advances_the_app_pin_after_a_validated_publication() -> None:
    """The pin advances AUTOMATICALLY after the worker publication validates —
    the manual re-pin step is retired — and a replay reports ``already_pinned``
    instead of minting a duplicate app version."""
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        admin.commit()
        _install_app_pin_protocol(admin, schema)
        admin.commit()

        result = bond_serving.run(_search_path_dsn(schema))
        assert result["state"] == "current"
        assert result["app_pin"] == "advanced"
        assert result["app_publication_version"] == 1
        pinned = admin.execute(
            f'SELECT worker_publication_id::text, lifecycle_state '
            f'FROM "{schema}".bond_serving_publications'
        ).fetchall()
        assert pinned == [(result["publication_id"], "validated")]
        current = admin.execute(
            f'SELECT app_publication_id::text FROM "{schema}".bond_serving_current_pointer'
        ).fetchone()[0]
        assert current == result["app_publication_id"]

        replay = bond_serving.run(_search_path_dsn(schema))
        assert replay["state"] == "current"
        assert replay["app_pin"] == "already_pinned"
        count = admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_publications'
        ).fetchone()[0]
        assert count == 1
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_code_revision_prefers_the_configured_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deployed image has no ``.git``, so the git fallback returns "unknown".

    Every build of one ``as_of`` would then collapse onto a single
    ``publication_id``, and ``materialize`` treats an existing id as already
    built -- it only re-points. A code change would silently re-serve the previous
    payload instead of rebuilding, which is what a Wave 1b republication hit on
    2026-07-30. Honouring ``CODE_REVISION`` (which the dl-bond-chain job already
    sets, and which ``bond_security_master`` already reads) keeps publication
    identity tracking the code that produced it.
    """
    monkeypatch.setenv("CODE_REVISION", "deadbee")
    assert bond_serving._code_revision() == "deadbee"

    # Distinct revisions must yield distinct publication identities for one as_of,
    # otherwise the rebuild is a no-op.
    as_of = date(2025, 3, 31)
    assert materializer.publication_id_for(
        as_of, "deadbee"
    ) != materializer.publication_id_for(as_of, "unknown")


def test_code_revision_falls_back_when_the_env_var_is_absent_or_blank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A blank env var is absence, not a revision named "" (it would hash)."""
    monkeypatch.setenv("CODE_REVISION", "")
    assert bond_serving._code_revision() != ""
    monkeypatch.delenv("CODE_REVISION", raising=False)
    assert bond_serving._code_revision() != ""


# --------------------------------------------------------------------------- #
# Same-day revisions (review P2: the serving anchor only followed max(day))
# --------------------------------------------------------------------------- #
# The dense daily series is what makes as_of move; a revised close on the day it
# ALREADY sits on is what as_of cannot see. These tests pin both halves: content
# change on one as_of -> a new publication that is actually built; no change ->
# the SAME publication_id, so replay stays idempotent and a weekend rerun is a
# cheap re-point rather than a 1.2 GB rewrite.
DENSE_DAY = date(2026, 1, 5)  # strictly after the fixtures' AS_OF (2025-12-31)


def _install_dense_series(admin, schema: str, *, sec1_price: str = "101.5") -> None:
    """The dense daily serving series, as a plain table (the worker only reads it).

    Shaped exactly like the production hypertable's read columns, seeded on ONE
    day that is later than the governed lane's, so the live lane wins and the
    served price is the one under test.
    """
    admin.execute(f"""
        CREATE TABLE "{schema}".bond_observation_daily(
            cusip9 text NOT NULL, day date NOT NULL, price numeric, ytm numeric,
            volume numeric, price_type text, accrued text, source text NOT NULL,
            source_rank smallint NOT NULL, ytm_basis text,
            PRIMARY KEY (cusip9, day))
    """)
    admin.execute(
        f'INSERT INTO "{schema}".bond_observation_daily VALUES '
        "(%s,%s,%s,0.0491,NULL,'evaluated','clean','live',9,'reported'),"
        "(%s,%s,100.75,0.0325,NULL,'evaluated','clean','live',9,'reported')",
        ("037833100", DENSE_DAY, sec1_price, "459200101", DENSE_DAY),
    )


def _served_latest_price(admin, schema: str, publication_id: str, security_id=SEC1):
    row = admin.execute(
        f"SELECT payload->>'price' FROM \"{schema}\".bond_serving_facts "
        "WHERE publication_id=%s AND surface='observations' AND lane='latest' "
        "AND security_id=%s",
        (publication_id, security_id),
    ).fetchone()
    return None if row is None else row[0]


def _build_as_of(admin, schema: str, publication_id: str):
    return admin.execute(
        f'SELECT as_of_date FROM "{schema}".bond_serving_builds WHERE publication_id=%s',
        (publication_id,),
    ).fetchone()[0]


def test_a_same_day_close_revision_mints_a_new_serving_publication() -> None:
    """The candle loader re-reads the watermark day for revised closes.

    Before the input digest, that rerun resolved to the SAME
    ``publication_id_for(as_of, code_revision)``; ``materialize`` treats an
    existing id as already built and only re-points, so the catalog/detail/latest
    facts kept the OLD price until a later date or a deploy changed the identity.
    A green run serving yesterday's price is exactly the silent failure this
    product cannot afford.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        dsn = _search_path_dsn(schema)

        first = bond_serving.run(dsn)
        assert first["state"] == "current"
        assert _build_as_of(admin, schema, first["publication_id"]) == DENSE_DAY
        assert _served_latest_price(admin, schema, first["publication_id"]) == "101.5"

        # Revise the close ON THE DAY THE SNAPSHOT ALREADY SPEAKS FOR: as_of cannot
        # move, so identity has to move for another reason or nothing rebuilds.
        admin.execute(
            f'UPDATE "{schema}".bond_observation_daily SET price=103.25 '
            "WHERE cusip9='037833100' AND day=%s",
            (DENSE_DAY,),
        )
        admin.commit()

        second = bond_serving.run(dsn)
        assert second["state"] == "current"
        assert second["publication_id"] != first["publication_id"]
        # ... for the SAME data date (a fresh price must never be stamped with a
        # different day just to force a new identity).
        assert _build_as_of(admin, schema, second["publication_id"]) == DENSE_DAY
        # ... and the new publication actually carries the revised price.
        assert _served_latest_price(admin, schema, second["publication_id"]) == "103.25"
        # ... and it is the one being served.
        current = admin.execute(
            f'SELECT publication_id::text FROM "{schema}".sec_derived_current_pointers '
            "WHERE product='bond_serving_v1'"
        ).fetchone()[0]
        assert current == second["publication_id"]
        # The digest is the discriminant, not the revision: same CODE_REVISION.
        assert first["code_revision"].split("+")[0] == second["code_revision"].split("+")[0]
        assert first["code_revision"] != second["code_revision"]
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_an_unchanged_rerun_replays_onto_the_same_publication() -> None:
    """Idempotence: the digest is CONTENT, never a clock.

    Two runs over identical inputs must land on ONE publication -- a timestamp or
    a random salt would mint a full ~1.2 GB publication on every run and break
    replay. A numeric rewritten at a different SCALE (101.500 for 101.5) is the
    same value and must not count as a revision either.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        dsn = _search_path_dsn(schema)

        first = bond_serving.run(dsn)
        second = bond_serving.run(dsn)
        assert second["publication_id"] == first["publication_id"]
        assert second["code_revision"] == first["code_revision"]

        admin.execute(
            f'UPDATE "{schema}".bond_observation_daily SET price=101.500 '
            "WHERE cusip9='037833100' AND day=%s",
            (DENSE_DAY,),
        )
        admin.commit()
        third = bond_serving.run(dsn)
        assert third["publication_id"] == first["publication_id"]

        publications = admin.execute(
            f'SELECT count(*) FROM "{schema}".sec_derived_publications '
            "WHERE product='bond_serving_v1'"
        ).fetchone()[0]
        assert publications == 1
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


# --------------------------------------------------------------------------- #
# Retention (review P2: the prune used a delete path the guard rejects)
# --------------------------------------------------------------------------- #
def _three_publications(admin, schema: str) -> list[str]:
    """Three serving publications on ONE as_of, via two same-day revisions."""
    dsn = _search_path_dsn(schema)
    ids = [bond_serving.run(dsn)["publication_id"]]
    for price in ("102.25", "104.75"):
        admin.execute(
            f'UPDATE "{schema}".bond_observation_daily SET price=%s '
            "WHERE cusip9='037833100' AND day=%s",
            (price, DENSE_DAY),
        )
        admin.commit()
        ids.append(bond_serving.run(dsn)["publication_id"])
    assert len(set(ids)) == 3
    return ids


def test_deleting_a_serving_fact_raises_the_immutability_error() -> None:
    """Immutability is unchanged for everyone who holds no purge token.

    ``bond_serving_facts_write_guard`` rejects every non-INSERT except a DELETE on
    the FACTS table by a backend that holds a ``bond_serving_purge_tokens`` row --
    so an ordinary DELETE still raises, and no lifecycle state makes a fact
    deletable. Deleting the PARENT is closed by the ON DELETE RESTRICT FK plus
    ``sec_derived_publication_delete_guard``; the purge routine does not try.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        published = bond_serving.run(_search_path_dsn(schema))["publication_id"]

        with pytest.raises(Exception) as deletion:
            admin.execute(
                f'DELETE FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
                (published,),
            )
        assert "bond serving row is immutable" in str(deletion.value)
        admin.rollback()

        with pytest.raises(Exception) as parent:
            admin.execute(
                f'DELETE FROM "{schema}".sec_derived_publications WHERE publication_id=%s',
                (published,),
            )
        assert "validated derived publication cannot be deleted" in str(parent.value)
        admin.rollback()
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def _without_purge_routine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Emulate a database whose installed schema predates the purge routine.

    Dropping the function is not enough to reproduce that world: ``run()`` calls
    ``install_schema`` first thing on EVERY run, so the DDL comes straight back.
    The capability probe is the seam, and it is exactly what the branch turns on,
    so the probe is what the test flips -- with
    ``test_the_installed_schema_exposes_the_purge_capability`` covering the other
    half (that the real DDL is what makes the probe answer true).
    """
    monkeypatch.setattr(bond_serving, "_PURGE_CAPABILITY_SQL", "SELECT false")


def test_the_installed_schema_exposes_the_purge_capability() -> None:
    """The probe asks for the capability, never for the guard's absence.

    The guard trigger SURVIVES the purge routine -- that is the design, the rows
    stay immutable except through a token -- so probing for a missing trigger
    would report ``blocked`` forever on a database that purges perfectly well.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        admin.commit()
        assert admin.execute(bond_serving._PURGE_CAPABILITY_SQL).fetchone()[0] is True
        # ... and the guard is still there.
        assert admin.execute(bond_serving._DELETE_GUARD_SQL).fetchone()[0] == (
            "bond_serving_facts_write_guard"
        )
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_retention_refuses_in_a_typed_way_when_the_purge_routine_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit and EARLY, with the numbers -- never an attempt that always raises.

    An older prune fired a DELETE the write guard rejects, so every run with a
    stale publication reported an anonymous ``retention.failed``: indistinguishable
    from a transient database error, and no operator could tell that the ~1.2 GB
    per publication the daily rebuild path creates was unreclaimable. On a database
    that has not yet installed ``bond_purge_serving_publication`` the refusal is
    still the honest answer -- and it now names the DDL that fixes it.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        _without_purge_routine(monkeypatch)
        published = _three_publications(admin, schema)

        retention = bond_serving._prune_superseded_facts(_search_path_dsn(schema))
        assert retention["state"] == "blocked_by_write_guard"
        assert retention["guard_trigger"] == "bond_serving_facts_write_guard"
        # keep-set = worker current + the two most recent -> exactly one is stale.
        assert retention["blocked_publications"] == 1
        assert retention["blocked_rows"] > 0
        assert "bond_purge_serving_publication" in retention["action"]
        # Never reported as a generic failure, and nothing was deleted.
        assert "error" not in retention
        surviving = {
            row[0] for row in admin.execute(
                f'SELECT DISTINCT publication_id::text FROM "{schema}".bond_serving_facts'
            ).fetchall()
        }
        assert surviving == set(published)
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_retention_reports_no_alarm_when_nothing_is_stale() -> None:
    """A healthy steady state must not raise a retention alarm just because the
    guard exists: the refusal is about rows that CANNOT be freed, and with two
    publications every one of them is still in the keep-set."""
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        result = bond_serving.run(_search_path_dsn(schema))
        assert result["retention"] == {
            "pruned_publications": 0, "pruned_rows": 0, "kept": 1,
        }
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_the_digest_binds_the_watermark_day_instead_of_resolving_it_inline() -> None:
    """A measured plan property, pinned so it cannot regress silently.

    Resolving the watermark inside the checksum query (a CTE) makes the day a
    runtime value, TimescaleDB cannot exclude chunks while planning, and the plan
    opens an index scan on EVERY chunk of the series: 6.5 s planning + 7.8 s
    execution warm, 36 s cold, growing with every year of history (measured
    against production 2026-08-07). Bound as a constant the same result costs
    0.6 ms planning + 7.6 ms execution over one chunk.
    """
    assert "max(day)" in bond_serving._DIGEST_WATERMARK_SQL
    assert "max(day)" not in bond_serving._DIGEST_DENSE_SQL
    assert "o.day = %s" in bond_serving._DIGEST_DENSE_SQL


def test_retention_finds_stale_publications_without_scanning_the_facts_table() -> None:
    """Same reason, other end: asking the 3.4 GB facts table which publication_ids
    it holds reads the whole relation (4m28s measured against production
    2026-08-07). The stale set comes from the nine-row publications table and only
    the candidates' rows are counted, through the index (2.1 s)."""
    assert "sec_derived_publications" in bond_serving._STALE_CANDIDATES_SQL
    assert "publication_id = ANY(%s)" in bond_serving._STALE_ROWS_SQL
    assert "FROM bond_serving_facts" not in bond_serving._STALE_CANDIDATES_SQL


def test_retention_falls_back_to_a_plain_delete_where_nothing_guards_the_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both refusals are PROBES, not hardcoded beliefs.

    With neither the purge routine nor a DELETE trigger -- a schema that never
    guarded the surface at all -- the batched plain DELETE still frees exactly the
    unreachable publication instead of refusing on a guard that is not there.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        _without_purge_routine(monkeypatch)
        published = _three_publications(admin, schema)
        stale = published[0]
        stale_rows = admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
            (stale,),
        ).fetchone()[0]
        assert stale_rows > 0
        admin.execute(
            f'DROP TRIGGER bond_serving_facts_write_guard ON "{schema}".bond_serving_facts'
        )
        admin.commit()

        # batch=1 so the bounded loop (a commit per batch, never one long
        # transaction holding back VACUUM) is exercised end to end.
        retention = bond_serving._prune_superseded_facts(_search_path_dsn(schema), batch=1)
        assert retention == {
            "pruned_publications": 1, "pruned_rows": stale_rows, "kept": 2,
        }
        surviving = {
            row[0] for row in admin.execute(
                f'SELECT DISTINCT publication_id::text FROM "{schema}".bond_serving_facts'
            ).fetchall()
        }
        assert surviving == set(published[1:])
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_retention_purges_the_unreachable_publication_through_the_token_routine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole point: the rows that nothing can reach actually go.

    The three publications are accumulated with the routine out of reach (so a
    stale one survives to be purged deliberately), then retention runs for real
    with ``batch=1`` -- which exercises the bounded loop, a COMMIT per batch and
    never one long transaction holding back VACUUM, end to end.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        with monkeypatch.context() as accumulate:
            _without_purge_routine(accumulate)
            published = _three_publications(admin, schema)
        stale = published[0]
        stale_rows = admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
            (stale,),
        ).fetchone()[0]
        assert stale_rows > 0
        admin.commit()

        retention = bond_serving._prune_superseded_facts(_search_path_dsn(schema), batch=1)
        assert retention == {
            "state": "purged",
            "pruned_publications": 1,
            "pruned_rows": stale_rows,
            "kept": 2,
            "purged": {stale: stale_rows},
        }
        assert admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
            (stale,),
        ).fetchone()[0] == 0
        # The keep-set is untouched, and the purged publication's build row stays:
        # sec_derived_publication_as_of reads it, and that is what feeds the
        # current-pointer as_of regression guard.
        surviving = {
            row[0] for row in admin.execute(
                f'SELECT DISTINCT publication_id::text FROM "{schema}".bond_serving_facts'
            ).fetchall()
        }
        assert surviving == set(published[1:])
        assert admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_builds WHERE publication_id=%s',
            (stale,),
        ).fetchone()[0] == 1
        # ... and the publication itself survives in the ledger, holding no facts.
        assert admin.execute(
            f'SELECT count(*) FROM "{schema}".sec_derived_publications WHERE publication_id=%s',
            (stale,),
        ).fetchone()[0] == 1
        # The purge leaves NO token behind, and the surface is immutable again.
        assert admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_purge_tokens'
        ).fetchone()[0] == 0
        with pytest.raises(Exception) as still_guarded:
            admin.execute(
                f'DELETE FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
                (published[1],),
            )
        assert "bond serving row is immutable" in str(still_guarded.value)
        admin.rollback()
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_a_run_frees_the_publication_it_supersedes() -> None:
    """End to end: retention is part of the daily run, not a manual chore.

    Three publications on one as_of -> the third run's retention purges the first,
    which nothing can reach any more. This is what keeps the surface at a plateau
    instead of +1 publication (~1.2 GB in production) per changed day.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        published = _three_publications(admin, schema)

        surviving = {
            row[0] for row in admin.execute(
                f'SELECT DISTINCT publication_id::text FROM "{schema}".bond_serving_facts'
            ).fetchall()
        }
        assert surviving == set(published[1:])
        assert admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_builds'
        ).fetchone()[0] == 3
        # A read left open would hold AccessShareLock and block the next run's
        # install_schema on its DROP TRIGGER.
        admin.commit()
        # ... and a further run finds nothing left to free.
        final = bond_serving.run(_search_path_dsn(schema))
        assert final["retention"]["pruned_publications"] == 0
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_a_fact_delete_needs_a_purge_token_belonging_to_this_backend() -> None:
    """The token is the ONLY key, and it is per-backend.

    Mirrors ``sec_derived_publication_tokens``: the guard authorises a DELETE only
    when a token row exists for THIS ``pg_backend_pid()``. A token minted by
    another session authorises nothing -- otherwise one purge would open the whole
    surface to every concurrent connection.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        published = bond_serving.run(_search_path_dsn(schema))["publication_id"]

        # (a) no token at all
        with pytest.raises(Exception) as untokened:
            admin.execute(
                f'DELETE FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
                (published,),
            )
        assert "bond serving row is immutable" in str(untokened.value)
        admin.rollback()

        # (b) a token belonging to ANOTHER backend
        admin.execute(
            f'INSERT INTO "{schema}".bond_serving_purge_tokens VALUES (%s, pg_backend_pid() + 1)',
            (published,),
        )
        admin.commit()
        with pytest.raises(Exception) as foreign:
            admin.execute(
                f'DELETE FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
                (published,),
            )
        assert "bond serving row is immutable" in str(foreign.value)
        admin.rollback()

        # (c) this backend's own token authorises the FACTS table and nothing else:
        # the build row must survive a purge (sec_derived_publication_as_of reads
        # it), and an UPDATE stays forbidden token or no token.
        admin.execute(
            f'UPDATE "{schema}".bond_serving_purge_tokens SET backend_pid = pg_backend_pid() '
            "WHERE publication_id=%s",
            (published,),
        )
        admin.commit()
        with pytest.raises(Exception) as builds_row:
            admin.execute(
                f'DELETE FROM "{schema}".bond_serving_builds WHERE publication_id=%s',
                (published,),
            )
        assert "bond serving row is immutable" in str(builds_row.value)
        admin.rollback()

        with pytest.raises(Exception) as updated:
            admin.execute(
                f'UPDATE "{schema}".bond_serving_facts SET state=\'degraded\' '
                "WHERE publication_id=%s",
                (published,),
            )
        assert "bond serving row is immutable" in str(updated.value)
        admin.rollback()

        deleted = admin.execute(
            f'DELETE FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
            (published,),
        ).rowcount
        assert deleted > 0
        admin.rollback()
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_the_purge_routine_refuses_the_publications_a_reader_still_needs() -> None:
    """Invariants that hold whatever the CALLER believes its keep-set is.

    The worker computes a wider keep margin (it also spares the immediately-prior
    publication, which daily_chain compensation can restore the pointer onto), but
    the two that would break a LIVE reader are refused by the routine itself, so a
    hand-run purge cannot empty the served surface either.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        _install_app_pin_protocol(admin, schema)
        admin.commit()
        dsn = _search_path_dsn(schema)
        left_behind = bond_serving.run(dsn)["publication_id"]
        admin.execute(
            f'UPDATE "{schema}".bond_observation_daily SET price=107.5 '
            "WHERE cusip9='037833100' AND day=%s",
            (DENSE_DAY,),
        )
        admin.commit()
        current = bond_serving.run(dsn)["publication_id"]
        assert current != left_behind
        # The app's pointer relation carries the PRODUCTION name; the replica the
        # pin lane is exercised against uses the app repo's own. Alias it, then
        # leave the app behind on the PREVIOUS publication -- exactly the state a
        # failed pin advance produces, and the one where purging "everything but
        # the worker's current" would empty the served surface.
        admin.execute(
            f'CREATE VIEW "{schema}".bond_serving_app_current_pointer AS '
            f'SELECT app_publication_id FROM "{schema}".bond_serving_current_pointer'
        )
        admin.execute(
            f'UPDATE "{schema}".bond_serving_current_pointer SET app_publication_id = '
            f'(SELECT app_publication_id FROM "{schema}".bond_serving_publications '
            " WHERE worker_publication_id=%s)",
            (left_behind,),
        )
        admin.commit()

        admin.execute(f'SET search_path TO "{schema}"')
        with pytest.raises(Exception) as serving:
            admin.execute("SELECT bond_purge_serving_publication(%s, 10)", (current,))
        assert "current bond serving publication cannot be purged" in str(serving.value)
        admin.rollback()

        admin.execute(f'SET search_path TO "{schema}"')
        with pytest.raises(Exception) as pinned:
            admin.execute("SELECT bond_purge_serving_publication(%s, 10)", (left_behind,))
        assert "app-pinned bond serving publication cannot be purged" in str(pinned.value)
        admin.rollback()

        admin.execute(f'SET search_path TO "{schema}"')
        with pytest.raises(Exception) as foreign_product:
            admin.execute(
                "SELECT bond_purge_serving_publication(gen_random_uuid(), 10)"
            )
        assert "not a bond_serving_v1 publication" in str(foreign_product.value)
        admin.rollback()

        admin.execute(f'SET search_path TO "{schema}"')
        with pytest.raises(Exception) as bad_batch:
            admin.execute("SELECT bond_purge_serving_publication(%s, 0)", (current,))
        assert "batch must be at least 1" in str(bad_batch.value)
        admin.rollback()

        # Nothing was freed by any of the refusals.
        admin.execute('SET search_path TO public')
        for publication in (current, left_behind):
            assert admin.execute(
                f'SELECT count(*) FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
                (publication,),
            ).fetchone()[0] > 0
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_reapplying_the_schema_is_a_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    """install_schema runs on EVERY worker run, so the purge DDL must be replayable.

    Re-applying must not drop the token table, reset the guard, break the routine
    or touch a single served row.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        admin.commit()
        with monkeypatch.context() as accumulate:
            _without_purge_routine(accumulate)
            published = _three_publications(admin, schema)
        before = admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_facts'
        ).fetchone()[0]

        admin.execute(f'SET search_path TO "{schema}"')
        materializer.install_schema(admin)
        materializer.install_schema(admin)
        admin.commit()

        assert admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_facts'
        ).fetchone()[0] == before
        assert admin.execute(bond_serving._PURGE_CAPABILITY_SQL).fetchone()[0] is True
        assert admin.execute(bond_serving._DELETE_GUARD_SQL).fetchone()[0] == (
            "bond_serving_facts_write_guard"
        )
        admin.execute('SET search_path TO public')
        admin.commit()
        # ... and the purge still works after the replay.
        retention = bond_serving._prune_superseded_facts(_search_path_dsn(schema))
        assert retention["state"] == "purged"
        assert retention["pruned_publications"] == 1
        assert admin.execute(
            f'SELECT count(*) FROM "{schema}".bond_serving_facts WHERE publication_id=%s',
            (published[0],),
        ).fetchone()[0] == 0
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()

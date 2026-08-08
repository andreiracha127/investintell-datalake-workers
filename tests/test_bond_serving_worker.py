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

import hashlib
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from src.bonds import serving_materializer as materializer
from src.workers import bond_serving

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_serving_fixtures import (  # noqa: E402
    AS_OF,
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


def _clear_revision_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every rung starts absent: the host's own CI env must not leak into a rung."""
    for name in bond_serving._REVISION_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_code_revision_prefers_the_configured_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deployed image has no ``.git``, so the git rung is silent in production.

    Every build of one ``as_of`` would then collapse onto a single
    ``publication_id``, and ``materialize`` treats an existing id as already
    built -- it only re-points. A code change would silently re-serve the previous
    payload instead of rebuilding, which is what a Wave 1b republication hit on
    2026-07-30. Honouring an explicit revision keeps publication identity tracking
    the code that produced it.
    """
    _clear_revision_env(monkeypatch)
    monkeypatch.setenv("CODE_REVISION", "deadbee")
    assert bond_serving._code_revision() == "deadbee"

    # Distinct revisions must yield distinct publication identities for one as_of,
    # otherwise the rebuild is a no-op.
    as_of = date(2025, 3, 31)
    assert materializer.publication_id_for(
        as_of, "deadbee"
    ) != materializer.publication_id_for(as_of, "unknown")


def test_code_revision_ladder_prefers_explicit_pins_over_the_deploy_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Precedence, weakest rung first: an explicit pin outranks the injected sha.

    ``CODE_REVISION`` wins because it is a DELIBERATE choice (a replay that must
    land on a known publication). Railway's per-deploy sha is what moves the
    identity with no operator action -- which is why a permanently-set
    ``CODE_REVISION`` is the trap: it shadows the sha and freezes the identity.
    Same ladder, same order, as the sibling publication workers.
    """
    _clear_revision_env(monkeypatch)
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "railway-sha")
    assert bond_serving._code_revision() == "railway-sha"
    monkeypatch.setenv("SOURCE_COMMIT", "source-sha")
    assert bond_serving._code_revision() == "source-sha"
    monkeypatch.setenv("GIT_SHA", "git-sha")
    assert bond_serving._code_revision() == "git-sha"
    monkeypatch.setenv("CODE_REVISION", "code-revision")
    assert bond_serving._code_revision() == "code-revision"


def test_code_revision_uses_the_railway_deploy_sha_when_nothing_is_pinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production case: no pin, no ``.git``, and the identity still moves.

    Measured on production 2026-08-08 -- ``bond-chain`` sets none of the explicit
    vars, yet its unattended cron runs recorded the full 40-hex sha of the commit
    deployed at that moment. A code-only change therefore rebuilds on its own.
    """
    _clear_revision_env(monkeypatch)
    sha = "cfa628e552a96eedc142e4e24e19410e56317f8a"
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", sha)
    # git must not be consulted at all once a rung resolved.
    monkeypatch.setattr(
        bond_serving.subprocess, "run",
        lambda *a, **k: pytest.fail("git consulted while an env rung was set"),
    )
    assert bond_serving._code_revision() == sha

    as_of = date(2025, 3, 31)
    assert materializer.publication_id_for(
        as_of, sha
    ) != materializer.publication_id_for(as_of, "e36213c335db0472268cbd9df0d7e3b977562602")


def test_code_revision_falls_back_when_the_env_vars_are_absent_or_blank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A blank env var is absence, not a revision named "" (it would hash).

    With every rung blank the git fallback answers -- this suite runs from a
    checkout, so that rung is live here even though it is silent in a deployed
    image.
    """
    for name in bond_serving._REVISION_ENV_VARS:
        monkeypatch.setenv(name, "")
    assert bond_serving._code_revision() != ""
    _clear_revision_env(monkeypatch)
    assert bond_serving._code_revision() != ""


def test_code_revision_refuses_to_publish_under_an_unresolvable_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No rung answered ⇒ raise, because "unknown" is a SILENT defect.

    A revision that cannot see the code collapses every build of one ``as_of``
    onto one publication id, which ``materialize`` re-points instead of
    rebuilding -- a green run serving stale payload, invisible for weeks. It can
    only happen on a workload detached from the GitHub source AND unpinned, so
    failing the first run is the cheap outcome.

    The error must NOT be a ``RuntimeError``: ``run`` launders those into the
    ``no_source`` dark state, which is the one verdict this must never become.
    """
    _clear_revision_env(monkeypatch)
    monkeypatch.setattr(
        bond_serving.subprocess, "run",
        lambda *a, **k: (_ for _ in ()).throw(OSError("git not on PATH")),
    )
    with pytest.raises(bond_serving.BondServingRevisionUnresolved) as excinfo:
        bond_serving._code_revision()
    # The message has to carry the fix, not just the complaint.
    assert "RAILWAY_GIT_COMMIT_SHA" in str(excinfo.value)
    assert "CODE_REVISION" in str(excinfo.value)
    assert not isinstance(excinfo.value, RuntimeError)


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
# The digest's REACH (review round 3, two P2s on the same identity)
# --------------------------------------------------------------------------- #
# Round 2's digest was right in kind and too narrow in two directions, and both
# narrownesses end the same way: a green run that serves stale data.
#   * it checksummed the table-wide watermark slice, while the candle loader
#     watermarks and re-reads EACH CUSIP independently -- so a revision to a
#     less-liquid bond, whose latest row sits BEFORE the table-wide max, left the
#     digest unmoved;
#   * it derived its product set from ``contract.SURFACES``, which never names
#     ``bond_metric_v1`` -- yet catalog/detail read the promoted current metric
#     view for ytm/ytw/current_yield/wal, so a metric republication on the same
#     as_of was invisible too.
QUIET_DAY = DENSE_DAY - timedelta(days=3)  # SEC2's last dense day: BEFORE the max


def _install_staggered_dense_series(
    admin, schema: str, *, sec2_price: str = "99.25",
) -> None:
    """A LIQUID and a LESS-LIQUID bond, exactly as the live feed leaves them.

    SEC1's CUSIP trades on ``DENSE_DAY`` (the table-wide max, and therefore the
    resolved as_of); SEC2's stops three days earlier. Both are still strictly
    newer than the governed lane's AS_OF, so both are SERVED from the dense
    series -- which is the whole point: staleness of a row has nothing to do with
    whether the product is showing it.
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
        "(%s,%s,101.5,0.0491,NULL,'evaluated','clean','live',9,'reported'),"
        "(%s,%s,%s,0.0325,NULL,'evaluated','clean','live',9,'reported')",
        ("037833100", DENSE_DAY, "459200101", QUIET_DAY, sec2_price),
    )


def test_a_revision_to_a_less_liquid_bond_mints_a_new_serving_publication() -> None:
    """The digest must cover the latest SERVED row per CUSIP, not the max slice.

    The candle loader re-reads every CUSIP from its OWN watermark, so a revised
    close lands on whatever day that bond last traded. Checksumming only the
    table-wide max day leaves that revision invisible: the identity replays,
    ``materialize`` treats the publication as already built and only re-points,
    and catalog/detail/latest keep serving the superseded price until the bond
    reaches the global max day or a deploy moves ``CODE_REVISION``.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_staggered_dense_series(admin, schema)
        admin.commit()
        dsn = _search_path_dsn(schema)

        first = bond_serving.run(dsn)
        assert first["state"] == "current"
        # as_of follows the TABLE-WIDE max -- SEC2 is served from an older day.
        assert _build_as_of(admin, schema, first["publication_id"]) == DENSE_DAY
        assert _served_latest_price(admin, schema, first["publication_id"], SEC2) == "99.25"
        admin.commit()  # never hold a read snapshot across a run(): install_schema takes DDL locks

        # Replay with nothing changed: still ONE publication (no clock, no salt).
        assert bond_serving.run(dsn)["publication_id"] == first["publication_id"]

        # Revise the less-liquid bond's close ON ITS OWN latest day, which is
        # STRICTLY BEFORE the table-wide max the old digest checksummed.
        admin.execute(
            f'UPDATE "{schema}".bond_observation_daily SET price=97.5 '
            "WHERE cusip9='459200101' AND day=%s",
            (QUIET_DAY,),
        )
        admin.commit()

        second = bond_serving.run(dsn)
        assert second["publication_id"] != first["publication_id"]
        # ... for the SAME data date: the revision moved identity, never as_of.
        assert _build_as_of(admin, schema, second["publication_id"]) == DENSE_DAY
        # ... and the new publication actually carries the revised price.
        assert _served_latest_price(admin, schema, second["publication_id"], SEC2) == "97.5"
        # ... and it is the one being served.
        current = admin.execute(
            f'SELECT publication_id::text FROM "{schema}".sec_derived_current_pointers '
            "WHERE product='bond_serving_v1'"
        ).fetchone()[0]
        assert current == second["publication_id"]
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def _promote_metric_publication(admin, schema: str) -> str:
    """Prepare, validate and promote one more ``bond_metric_v1`` publication.

    The real ``sec_current_bond_metric_v1`` is a view gated on this pointer, so
    advancing it is exactly how a same-day metric rebuild reaches the serving
    build -- without touching as_of and without a deploy.
    """
    run_id, package_id = admin.execute(
        f'SELECT r.run_id, p.package_id FROM "{schema}".sec_ingestion_runs r '
        f'JOIN "{schema}".sec_source_packages p ON p.run_id = r.run_id LIMIT 1'
    ).fetchone()
    publication = uuid4()
    version = admin.execute(
        f'SELECT COALESCE(max(publication_version),0)+1 FROM "{schema}".sec_derived_publications '
        "WHERE product='bond_metric_v1'"
    ).fetchone()[0]
    admin.execute(
        f'INSERT INTO "{schema}".sec_derived_publications'
        "(publication_id,product,publication_version,source_run_id,source_package_id,"
        " build_fingerprint) VALUES(%s,'bond_metric_v1',%s,%s,%s,%s)",
        (publication, version, run_id, package_id,
         hashlib.sha256(publication.bytes).hexdigest()),
    )
    admin.execute(f'SELECT "{schema}".sec_validate_derived_publication(%s)', (publication,))
    admin.execute(
        f"SELECT \"{schema}\".sec_set_current_derived_publication('bond_metric_v1', %s)",
        (publication,),
    )
    admin.commit()
    return str(publication)


def _served_catalog_ytm(admin, schema: str, publication_id: str, security_id=SEC1):
    row = admin.execute(
        f"SELECT payload->>'security_ytm' FROM \"{schema}\".bond_serving_facts "
        "WHERE publication_id=%s AND surface='catalog' AND security_id=%s",
        (publication_id, security_id),
    ).fetchone()
    return None if row is None else row[0]


def test_advancing_the_metric_publication_mints_a_new_serving_publication() -> None:
    """The metric product is a serving INPUT, so its pointer belongs in the digest.

    ``contract.SURFACES`` names the security, price-observation and N-PORT
    products; it does not name ``bond_metric_v1``. But the materializer REQUIRES
    ``sec_current_bond_metric_v1`` for catalog and detail (ytm / ytw /
    current_yield / wal), so a metric rebuild promoted on the same as_of changes
    what the product should say while leaving as_of and CODE_REVISION alone. With
    the metric pointer outside the digest the serving identity replayed and the
    app kept serving the previous metric values.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        _promote_metric_publication(admin, schema)
        admin.commit()
        dsn = _search_path_dsn(schema)

        first = bond_serving.run(dsn)
        assert first["state"] == "current"
        assert _served_catalog_ytm(admin, schema, first["publication_id"]) == "0.0525"

        # A metric rebuild for the SAME as_of: new values, promoted by advancing
        # the current pointer. Nothing else about the day moves.
        admin.execute(
            f'UPDATE "{schema}".sec_current_bond_metric_v1 SET value=0.0611 '
            "WHERE security_id=%s AND metric_id='security_ytm' AND status='available'",
            (SEC1,),
        )
        _promote_metric_publication(admin, schema)

        second = bond_serving.run(dsn)
        assert second["publication_id"] != first["publication_id"]
        assert _build_as_of(admin, schema, second["publication_id"]) == DENSE_DAY
        assert _served_catalog_ytm(admin, schema, second["publication_id"]) == "0.0611"
        current = admin.execute(
            f'SELECT publication_id::text FROM "{schema}".sec_derived_current_pointers '
            "WHERE product='bond_serving_v1'"
        ).fetchone()[0]
        assert current == second["publication_id"]
        admin.commit()  # never hold a read snapshot across a run(): install_schema takes DDL locks

        # ... and the enlarged digest is still pure content: a third run over
        # unchanged inputs replays onto the SAME publication.
        assert bond_serving.run(dsn)["publication_id"] == second["publication_id"]
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


def test_the_digest_reads_the_latest_row_per_cusip_and_carries_no_clock() -> None:
    """Two measured properties of the checksum query, pinned against regression.

    SHAPE: the digest resolves the latest priced row PER CUSIP through the PIT
    alias set (a LATERAL ordered by ``day DESC LIMIT 1``, one chunk-walk per
    CUSIP that stops at the first hit). Collapsing it back to a table-wide
    ``max(day)`` slice is the round-2 blind spot -- a less-liquid bond's revision
    goes undetected -- and a ``DISTINCT ON (cusip9)`` over the whole series was
    measured at 16.60 s against 6.15 s for this form (production, 2026-08-08).

    PURITY: no clock anywhere in the digest SQL. ``now()``/``CURRENT_DATE`` would
    mint a full ~1.2 GB publication on every run and destroy replay.
    """
    assert not hasattr(bond_serving, "_DIGEST_WATERMARK_SQL")
    assert "max(day)" not in bond_serving._DIGEST_DENSE_SQL
    assert "DISTINCT ON" not in bond_serving._DIGEST_DENSE_SQL
    assert "CROSS JOIN LATERAL" in bond_serving._DIGEST_DENSE_SQL
    assert "ORDER BY o.day DESC" in bond_serving._DIGEST_DENSE_SQL
    assert "LIMIT 1" in bond_serving._DIGEST_DENSE_SQL
    # The alias window is the build's own: valid_to EXCLUSIVE, valid_from inclusive.
    assert "a.valid_from <= %(as_of)s" in bond_serving._DIGEST_DENSE_SQL
    assert "a.valid_to IS NULL OR a.valid_to > %(as_of)s" in bond_serving._DIGEST_DENSE_SQL
    # ... and it is the SAME template the anchor renders at the observation's own
    # day (2026-08-08). Two hand-written copies of a half-open window is where a
    # slipped ``>`` for ``>=`` hides: it would change which rows the digest sees and
    # which day the anchor picks, and no replay test can see it, because a replay is
    # self-consistent under either reading.
    assert bond_serving._alias_pit_window("%(as_of)s") in bond_serving._DIGEST_DENSE_SQL
    assert bond_serving._alias_pit_window("o.day") in bond_serving._LIVE_ANCHOR_SQL
    for sql in (bond_serving._DIGEST_DENSE_SQL, bond_serving._DIGEST_POINTERS_SQL):
        for clock in ("now()", "current_date", "clock_timestamp", "random("):
            assert clock not in sql.lower()


def test_the_digest_covers_every_publication_product_the_build_reads() -> None:
    """The audit, pinned: the metric product is a serving INPUT that no surface names.

    ``contract.SURFACES`` declares the security, price-observation and N-PORT
    products; the materializer additionally REQUIRES ``sec_current_bond_metric_v1``
    (catalog: security_ytm/security_ytw; detail: + current_yield/wal), whose view
    is gated on the ``bond_metric_v1`` current pointer. Leaving it out let a
    same-as_of metric republication replay onto the stale serving publication.
    """
    from src.bonds import serving_contract as contract

    declared = {surface["source_product"] for surface in contract.SURFACES}
    assert set(bond_serving._DIGEST_PRODUCTS) == declared | {"bond_metric_v1"}
    # The claim this rests on, read from the materializer rather than believed.
    required = {
        rel
        for rels in materializer._SURFACE_REQUIRED_RELATIONS.values()
        for rel in rels
    }
    assert "sec_current_bond_metric_v1" in required


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


# --------------------------------------------------------------------------- #
# The serving anchor is resolved through the BUILD's own read rule (2026-08-08)
# --------------------------------------------------------------------------- #
# ``as_of`` used to take the dense series' bare ``max(day)``, while the build's
# dense lane (``_LATEST_OBSERVATION_LIVE``, src/bonds/serving_materializer.py)
# only ever reads a PRICED row whose CUSIP9 a published alias holds AT ``as_of``.
# A day contributed only by rows the lane refuses therefore let the publication
# claim a date not one of its dense inputs carries -- and here that is worse than
# in bond_metrics: ``as_of`` is half of the publication IDENTITY
# (``uuid5(product | as_of | code_revision)``) and it is what
# ``sec_derived_publication_as_of`` feeds the current-pointer regression guard.
#
# The circularity (the lane's alias filter is evaluated AT ``as_of``, and
# ``as_of`` is what the anchor computes) is closed by asking the alias question at
# the observation's OWN day, which yields the GREATEST fixed point of the lane's
# reading rule -- the argument is written out at ``_LIVE_ANCHOR_SQL``.
#
# The value predicate is the SERVING's and not the metric worker's, which is the
# one place the two anchors legitimately differ: bond_metrics reads two cohorts
# (``price > 0`` and ``ytm IS NOT NULL``) and its anchor carries an OR arm; this
# build starts from ``price IS NOT NULL AND price > 0`` and lets ``ytm`` ride
# along on a row that already cleared it. ``ytm_only`` below is the shape that
# tells the two rules apart -- copying the twin's OR would pass every other test
# here.
#
# Measured in production 2026-08-08: the series holds 149,762 distinct CUSIP9s
# against 194,521 published cusip9 aliases, and 41 of the 7,716 rows on the
# freshest day (2026-08-06) carry a CUSIP9 with no alias valid that day. Both
# anchors answer 2026-08-06 today, so this is a structural guarantee rather than a
# live correction.
READABLE_DAY = DENSE_DAY                         # a day the build can actually read
UNREADABLE_DAY = DENSE_DAY + timedelta(days=10)  # later, and refused by the lane


def _install_anchor_series(admin, schema: str, row_shape: str) -> None:
    """One readable day, plus ONE later row whose readability is the parameter.

    The readable day is always present so the anchor has a truthful place to land
    and the shape is the only thing deciding the outcome.
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
        "(%s,%s,104.50,0.0491,NULL,'evaluated','clean','live',9,'reported'),"
        "(%s,%s,100.75,0.0325,NULL,'evaluated','clean','live',9,'reported')",
        ("037833100", READABLE_DAY, "459200101", READABLE_DAY),
    )
    if row_shape == "retired_alias":
        # A CUSIP9 SEC1 wore and no longer holds: the window CLOSES on the later
        # day (valid_to is EXCLUSIVE), so the lane skips its row there.
        admin.execute(
            f'INSERT INTO "{schema}".sec_current_bond_security_alias_v1 VALUES '
            "(%s,'cusip9','037833199',%s,%s)",
            (SEC1, date(2020, 1, 1), UNREADABLE_DAY),
        )
    fresh = {
        # held by SEC1 on that day, and priced -> the lane reads it
        "held_alias": ("037833100", "95.10", "0.0512"),
        # the retired identifier: joinable nowhere on that day
        "retired_alias": ("037833199", "95.10", "0.0512"),
        # a CUSIP9 the published universe has never heard of
        "unknown_cusip": ("ZZZZZZZZZ", "95.10", "0.0512"),
        # held and joinable, but carrying no price: this build has no yield-only
        # cohort, so there is nothing here it can read
        "ytm_only": ("037833100", None, "0.0512"),
    }[row_shape]
    admin.execute(
        f'INSERT INTO "{schema}".bond_observation_daily VALUES '
        "(%s,%s,%s,%s,NULL,'evaluated','clean','live',9,'reported')",
        (fresh[0], UNREADABLE_DAY, fresh[1], fresh[2]),
    )


@pytest.mark.parametrize(
    ("row_shape", "anchor_follows_the_later_row"),
    [
        ("held_alias", True),      # the build reads it, so it may speak for it
        ("retired_alias", False),  # closed window: the lane skips the row
        ("unknown_cusip", False),  # no alias at all: unjoinable
        ("ytm_only", False),       # joinable, but this build reads no unpriced row
    ],
)
def test_the_serving_anchor_follows_only_dense_rows_the_build_can_read(
    row_shape, anchor_follows_the_later_row,
) -> None:
    """A later dense day only dates the publication when the build can read it."""
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_anchor_series(admin, schema, row_shape)
        admin.commit()

        result = bond_serving.run(_search_path_dsn(schema))

        assert result["state"] == "current"
        expected_day = UNREADABLE_DAY if anchor_follows_the_later_row else READABLE_DAY
        # The date the publication CLAIMS -- read from the very row
        # ``sec_derived_publication_as_of`` feeds the pointer regression guard from ...
        assert _build_as_of(admin, schema, result["publication_id"]) == expected_day
        # ... agrees with the row the build actually served: the day it claims is a
        # day its own dense inputs carry, not one it merely borrowed a date from.
        assert _served_latest_price(admin, schema, result["publication_id"]) == (
            "95.10" if anchor_follows_the_later_row else "104.50"
        )
        # The bare max(day) -- the anchor this replaces -- is the later day in
        # EVERY shape, which is exactly why it could not be trusted.
        assert admin.execute(
            f'SELECT max(day) FROM "{schema}".bond_observation_daily'
        ).fetchone()[0] == UNREADABLE_DAY
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_an_unreadable_dense_row_moves_neither_the_anchor_nor_the_identity() -> None:
    """Anchor and digest have to agree, or the fix breaks replay on its way past.

    The digest is bounded by ``as_of`` and asks the same alias/price question, so a
    row no lane reads must leave BOTH untouched: same date, same publication_id,
    still one publication. If the anchor moved and the digest did not (or the
    reverse) the product would either claim a date it cannot serve or mint a 1.2 GB
    publication for a row that changes nothing.
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
        assert _build_as_of(admin, schema, first["publication_id"]) == DENSE_DAY

        # A later row on a CUSIP9 no published alias holds: nothing reads it.
        admin.execute(
            f'INSERT INTO "{schema}".bond_observation_daily VALUES '
            "('ZZZZZZZZZ',%s,95.10,0.0512,NULL,'evaluated','clean','live',9,'reported')",
            (UNREADABLE_DAY,),
        )
        admin.commit()

        second = bond_serving.run(dsn)
        assert _build_as_of(admin, schema, second["publication_id"]) == DENSE_DAY
        assert second["publication_id"] == first["publication_id"]
        assert second["code_revision"] == first["code_revision"]
        assert admin.execute(
            f'SELECT count(*) FROM "{schema}".sec_derived_publications '
            "WHERE product='bond_serving_v1'"
        ).fetchone()[0] == 1
    finally:
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


@pytest.mark.parametrize("alias_state", ["dropped", "empty"])
def test_the_dense_anchor_is_silent_without_a_published_alias(alias_state) -> None:
    """No way to key the series -> no dense anchor, whatever ``max(day)`` says.

    The build gates its dense merge on BOTH relations, so with the alias view
    absent the dense lane contributes nothing at all -- yet the old anchor read the
    SERIES alone and dated the publication from a lane the build never opened.
    That second hole is what this pins; the EMPTY view is the same question in its
    degenerate form, and answering it first is also what keeps the newest-first
    anchor scan from walking the whole series for a match that cannot exist.

    Asserted on ``_resolve_as_of`` rather than through ``run()`` on purpose: with
    no alias the fund_exposure surface projects zero rows and the coverage gate
    fails the build first, so a full run would never reach the question.
    """
    admin = connect()
    schema = None
    try:
        cur = admin.cursor()
        schema = setup(cur)
        _install_dense_series(admin, schema)
        if alias_state == "dropped":
            admin.execute(f'DROP TABLE "{schema}".sec_current_bond_security_alias_v1')
        else:
            admin.execute(f'DELETE FROM "{schema}".sec_current_bond_security_alias_v1')
        admin.execute(f'SET search_path TO "{schema}"')
        admin.commit()

        # The series is there and its max(day) is the later one ...
        assert admin.execute(
            "SELECT max(day) FROM bond_observation_daily"
        ).fetchone()[0] == DENSE_DAY
        # ... and the anchor still falls back to the security master's own day.
        assert bond_serving._live_anchor(admin) is None
        assert bond_serving._resolve_as_of(admin, None) == AS_OF
    finally:
        admin.execute('SET search_path TO public')
        if schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()

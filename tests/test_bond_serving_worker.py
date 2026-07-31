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

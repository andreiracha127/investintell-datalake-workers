"""Disposable-PostgreSQL tests for ``docs/bond_default_events/admin_roles_v1.sql``.

The file is the one-time superuser prerequisite that creates the four NOLOGIN group roles the
pinned bond_credit SQL files and the diagnostic release file expect and grants exactly two
memberships. It is a psql script written for the production database ``market``; here its
statements run through psycopg (autocommit, so the script's own ``BEGIN``/``COMMIT`` apply) after
two mechanical edits: the psql-only ``\\set`` line is dropped and the literal ``'market'`` in the
database-name guard is replaced by the throwaway database's name. A separate test runs the
UNEDITED guard against the throwaway database and requires it to refuse.

Safety: the DSN guard of ``test_bond_default_publication_db.py`` is reused verbatim (loopback,
database name containing ``disposable``, explicit user and password; the module is skipped
without ``BOND_DEFAULT_TEST_DATABASE_URL``). Every scenario runs in its own throwaway database
named ``bde_admin_roles_disposable_<hex>``, dropped afterwards.

CLUSTER-GLOBAL ROLES: the script uses fixed role names (``worker_writer``, ``app_runtime`` and the
four group roles). This module therefore MUST NOT share a cluster with other role-sensitive suites
running concurrently (run it serially, ideally first in a fresh disposable cluster, as CI does).
Roles it creates (``worker_writer``, ``app_runtime``, ``bde_stranger``) are dropped afterwards. The
group roles are dropped too; if that is impossible because an earlier suite left privileges for
them in another database of the cluster, they are neutralised instead (memberships revoked,
attributes reset) and the "roles were created by the script" assertion is skipped.
"""

from __future__ import annotations

import contextlib
import importlib.util
import secrets
import sys
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import errors, sql

HERE = Path(__file__).resolve().parent
SCRIPT = HERE.parent / "docs" / "bond_default_events" / "admin_roles_v1.sql"


def _load(alias: str, filename: str):
    spec = importlib.util.spec_from_file_location(alias, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


guard = _load("_bde_admin_roles_db_guard", "test_bond_default_publication_db.py")
connect_disposable = guard.connect_disposable

GROUP_ROLES = ("bond_credit_reader", "bond_credit_writer", "bond_credit_auditor", "bond_default_diagnostic_reader")
EXPECTED_MEMBERS = {
    "bond_credit_reader": [],
    "bond_credit_writer": ["worker_writer"],
    "bond_credit_auditor": [],
    "bond_default_diagnostic_reader": ["app_runtime"],
}
TEST_ROLES = ("worker_writer", "app_runtime", "bde_stranger")
GUARD_LITERAL = "current_database() <> 'market'"


def script_text(dbname: str | None) -> str:
    """The SQL file's statements: psql ``\\set`` dropped, the database-name guard pointed at ``dbname``."""
    text = SCRIPT.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if not line.startswith("\\set ")]
    assert len(lines) == len(text.splitlines()) - 1, "expected exactly one psql \\set line"
    body = "\n".join(lines) + "\n"
    if dbname is None:
        return body
    assert body.count(GUARD_LITERAL) == 1, "the database-name guard changed; update this test"
    assert dbname.replace("_", "").isalnum()
    return body.replace(GUARD_LITERAL, f"current_database() <> '{dbname}'")


@pytest.fixture(scope="module")
def dsn():
    import os

    raw = os.environ.get(guard.ENV_VAR)
    if raw is None:
        pytest.skip(f"{guard.ENV_VAR} not set; the disposable DB suite refuses any ambient DSN")
    try:
        target = guard.check_test_dsn(raw)
    except guard.UnsafeTestDsn as exc:
        pytest.fail(f"refusing {guard.ENV_VAR}: {exc}")
    with connect_disposable(target) as conn:
        assert conn.execute("SELECT current_setting('server_version_num')::int").fetchone()[0] >= 160000
        assert conn.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()[0], (
            "the rehearsal DSN user plays the administrator and must be a superuser"
        )
    return target


def _role_exists(conn, name: str) -> bool:
    return conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", [name]).fetchone() is not None


def _reset_roles(dsn) -> bool:
    """Remove every role this module touches. Returns True when a group role had to be kept."""
    kept = False
    with connect_disposable(dsn, autocommit=True) as conn:
        for name in (*TEST_ROLES, *GROUP_ROLES):
            if not _role_exists(conn, name):
                continue
            # Memberships in and out first (roles created here have no other dependencies).
            for row in conn.execute(
                "SELECT g.rolname, u.rolname FROM pg_auth_members m "
                "JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member "
                "WHERE g.rolname = %s OR u.rolname = %s", [name, name]).fetchall():
                conn.execute(sql.SQL("REVOKE {} FROM {}").format(sql.Identifier(row[0]), sql.Identifier(row[1])))
            try:
                conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))
            except errors.DependentObjectsStillExist:
                assert name in GROUP_ROLES, f"{name} has dependent objects in the cluster"
                conn.execute(sql.SQL(
                    "ALTER ROLE {} NOLOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(name)))
                kept = True
    return kept


class Scenario:
    """One throwaway database plus a clean role slate."""

    def __init__(self, dsn, dbname: str, groups_preexisting: bool) -> None:
        self.dsn, self.dbname, self.groups_preexisting = dsn, dbname, groups_preexisting
        self.target = dsn.derive(dbname=dbname)

    def admin(self):
        return connect_disposable(self.target, autocommit=True)

    def cluster(self):
        return connect_disposable(self.dsn, autocommit=True)

    def make_principals(self, *, writer_create: bool = True) -> None:
        with self.cluster() as conn:
            for role in ("worker_writer", "app_runtime"):
                conn.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(secrets.token_hex(16))))
        if writer_create:
            with self.admin() as conn:
                conn.execute("GRANT CREATE ON SCHEMA public TO worker_writer")

    def run(self, *, edited: bool = True) -> None:
        text = script_text(self.dbname if edited else None)
        with self.admin() as conn:
            try:
                conn.execute(text)
            except psycopg.Error:
                with contextlib.suppress(psycopg.Error):
                    conn.execute("ROLLBACK")
                raise

    def state(self) -> dict[str, object]:
        with self.admin() as conn:
            roles = {
                row[0]: row[1:] for row in conn.execute(
                    "SELECT rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb, rolreplication, "
                    "rolbypassrls FROM pg_roles WHERE rolname = ANY(%s)", [list(GROUP_ROLES)]).fetchall()
            }
            members = {
                group: [row[0] for row in conn.execute(
                    "SELECT u.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid "
                    "JOIN pg_roles u ON u.oid = m.member WHERE g.rolname = %s ORDER BY u.rolname", [group]).fetchall()]
                for group in GROUP_ROLES if group in roles
            }
            broad = {
                group: conn.execute("SELECT pg_has_role('app_runtime', %s, 'MEMBER')", [group]).fetchone()[0]
                for group in ("bond_credit_reader", "bond_credit_auditor", "bond_credit_writer")
                if group in roles
            }
        return {"roles": roles, "members": members, "app_runtime_broad": broad}


@pytest.fixture
def scenario(dsn):
    groups_preexisting = _reset_roles(dsn)
    dbname = f"bde_admin_roles_disposable_{uuid.uuid4().hex[:10]}"
    with connect_disposable(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    try:
        yield Scenario(dsn, dbname, groups_preexisting)
    finally:
        with connect_disposable(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(dbname)))
        _reset_roles(dsn)


def _assert_intended(state: dict[str, object]) -> None:
    assert sorted(state["roles"]) == sorted(GROUP_ROLES)
    # rolcanlogin, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls: all off.
    assert all(attrs == (False,) * 6 for attrs in state["roles"].values())
    assert state["members"] == EXPECTED_MEMBERS
    assert state["app_runtime_broad"] == {
        "bond_credit_reader": False, "bond_credit_auditor": False, "bond_credit_writer": False}


def test_fresh_install_creates_four_nologin_roles_with_exact_memberships(scenario):
    scenario.make_principals()
    scenario.run()
    _assert_intended(scenario.state())
    with scenario.admin() as conn:
        assert conn.execute("SELECT has_schema_privilege('worker_writer', 'public', 'CREATE')").fetchone()[0]
        for group in GROUP_ROLES:  # USAGE on public for every group role
            assert conn.execute("SELECT has_schema_privilege(%s, 'public', 'USAGE')", [group]).fetchone()[0]
    if scenario.groups_preexisting:
        pytest.skip("group roles pre-existed in this cluster: creation from scratch was not exercised")


def test_rerun_is_idempotent(scenario):
    scenario.make_principals()
    scenario.run()
    first = scenario.state()
    scenario.run()
    scenario.run()
    assert scenario.state() == first
    _assert_intended(first)


def test_unedited_database_guard_refuses_any_other_database(scenario):
    scenario.make_principals()
    with pytest.raises(errors.RaiseException, match="expected database market"):
        scenario.run(edited=False)
    assert all(members == [] for members in scenario.state()["members"].values())


def test_missing_principals_are_refused(scenario):
    with pytest.raises(errors.RaiseException, match="worker_writer and app_runtime must already exist"):
        scenario.run()


def test_preexisting_unexpected_member_is_refused_without_partial_grants(scenario):
    scenario.make_principals()
    with scenario.cluster() as conn:
        conn.execute("CREATE ROLE bond_credit_reader NOLOGIN")
        conn.execute("CREATE ROLE bde_stranger NOLOGIN")
        conn.execute("GRANT bond_credit_reader TO bde_stranger")
    with pytest.raises(errors.RaiseException, match="bond_credit_reader has an unexpected member"):
        scenario.run()
    state = scenario.state()
    assert state["members"]["bond_credit_reader"] == ["bde_stranger"]  # untouched, not reconciled
    assert state["members"].get("bond_credit_writer", []) == []  # the transaction rolled back
    assert state["members"].get("bond_default_diagnostic_reader", []) == []


@pytest.mark.parametrize("attribute", ["LOGIN", "SUPERUSER", "CREATEROLE", "CREATEDB", "REPLICATION", "BYPASSRLS"])
def test_existing_role_with_unexpected_attributes_is_refused(scenario, attribute):
    scenario.make_principals()
    with scenario.cluster() as conn:
        conn.execute(sql.SQL("CREATE ROLE bond_credit_auditor {}").format(sql.SQL(attribute)))
    with pytest.raises(errors.RaiseException, match="bond_credit_auditor has unexpected attributes"):
        scenario.run()
    with scenario.admin() as conn:  # the script never alters an existing role
        row = conn.execute(
            "SELECT rolcanlogin, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls "
            "FROM pg_roles WHERE rolname = 'bond_credit_auditor'").fetchone()
    assert sum(row) == 1


@pytest.mark.parametrize("broad", ["bond_credit_reader", "bond_credit_auditor", "bond_credit_writer"])
def test_app_runtime_with_a_broad_bond_credit_membership_is_refused(scenario, broad):
    scenario.make_principals()
    with scenario.cluster() as conn:
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(broad)))
        conn.execute(sql.SQL("GRANT {} TO app_runtime").format(sql.Identifier(broad)))
    with pytest.raises(errors.RaiseException, match="unexpected member|inherits a broad"):
        scenario.run()
    state = scenario.state()
    assert sum(state["app_runtime_broad"].values()) == 1  # left for manual reconciliation
    assert state["members"].get("bond_default_diagnostic_reader", []) == []


def test_existing_role_that_is_itself_a_member_is_refused(scenario):
    scenario.make_principals()
    with scenario.cluster() as conn:
        conn.execute("CREATE ROLE bde_stranger NOLOGIN")
        conn.execute("CREATE ROLE bond_credit_reader NOLOGIN")
        conn.execute("GRANT bde_stranger TO bond_credit_reader")
    with pytest.raises(errors.RaiseException, match="bond_credit_reader is a member of another role"):
        scenario.run()


def test_worker_writer_without_create_on_public_is_refused_and_rolled_back(scenario):
    scenario.make_principals(writer_create=False)
    with pytest.raises(errors.RaiseException, match="worker_writer lacks CREATE on schema public"):
        scenario.run()
    state = scenario.state()
    assert state["members"].get("bond_credit_writer", []) == []
    assert state["members"].get("bond_default_diagnostic_reader", []) == []
    if not scenario.groups_preexisting:
        assert state["roles"] == {}  # even the role creations were rolled back

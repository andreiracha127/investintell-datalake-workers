"""Role-faithful disposable-PostgreSQL rehearsal of the diagnostic release layer.

Runs only against ``BOND_DEFAULT_TEST_DATABASE_URL`` (loopback, database name containing
``disposable``, the same guard as ``test_bond_default_publication_db.py``); each test module fixture
creates its own throwaway *databases* on that server and never touches the database named by the DSN.
Production is mirrored role by role:

* ``postgres`` (the DSN user) plays the administrator and owns schema ``public``;
* ``worker_writer`` LOGIN without CREATEROLE/CREATEDB/SUPERUSER holds CREATE on ``public`` and
  installs the four pinned SQL files unchanged plus the diagnostic file (autocommit);
* ``app_runtime`` LOGIN with USAGE on ``public`` is only a member of ``bond_default_diagnostic_reader``;
* the production ``ALTER DEFAULT PRIVILEGES`` (``app_runtime`` DML, ``app_analytics_ro`` SELECT on every
  new public table) are reproduced, plus unrelated pre-existing ``public`` objects.

A skipped DB suite is not acceptance evidence.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import importlib.util
import os
import re
import secrets
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from src.bonds.default_events import contracts as c
from src.bonds.default_events import diagnostic_publication as d
from src.bonds.default_events import publication as p

psycopg = pytest.importorskip("psycopg")
from psycopg import errors, sql  # noqa: E402

HERE = Path(__file__).resolve().parent
UTC = dt.timezone.utc


def _load(alias: str, filename: str):
    spec = importlib.util.spec_from_file_location(alias, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[alias] = module  # dataclasses resolve their module through sys.modules
    spec.loader.exec_module(module)
    return module


guard = _load(
    "_bde_publication_db_guard", "test_bond_default_publication_db.py"
)  # DSN guard + connector
unit = _load(
    "_bde_diagnostic_unit", "test_bond_default_diagnostic_publication.py"
)  # bundle builder
connect_disposable = guard.connect_disposable
K0, T0 = unit.K0, unit.T0
GROUP_ROLES = (
    "bond_credit_reader",
    "bond_credit_writer",
    "bond_credit_auditor",
    "bond_default_diagnostic_reader",
)
RAW_READERS = (
    "bond_credit_current_publication(NULL, NULL)",
    "bond_credit_read_publication(NULL, true)",
    "bond_credit_read_coverage(NULL, true)",
    "bond_credit_read_ratings(NULL, true)",
    "bond_credit_read_events(NULL, true)",
    "bond_credit_read_publication_sources(NULL, true)",
    "bond_credit_read_source_packages(NULL, true)",
    "bond_credit_read_observations(NULL, true)",
    "bond_credit_read_adjudications(NULL, true)",
    "bond_credit_read_event_links(NULL, true)",
    "bond_credit_read_ncen_filings(NULL, true)",
)
#: Cluster-wide LOGIN roles are shared by every environment of this module, so their passwords are too.
ROLE_PASSWORDS = {
    name: secrets.token_hex(16)
    for name in ("worker_writer", "app_runtime", "app_analytics_ro")
}
DIAG_TABLES = (
    "bond_default_diagnostic_releases",
    "bond_default_diagnostic_pointer",
    "bond_default_diagnostic_revocations",
    "bond_default_diagnostic_installations",
)
DIAG_WRITER_FUNCTIONS = (
    "bond_default_prepare_diagnostic(NULL::uuid, NULL::jsonb, NULL::text, NULL::text)",
    "bond_default_promote_diagnostic(NULL::uuid, NULL::uuid)",
    "bond_default_verify_diagnostic(NULL::uuid)",
    "bond_default_revoke_diagnostic(NULL::uuid, NULL::text)",
)


# ---------------------------------------------------------------------------
# Environment: a throwaway production-shaped database
# ---------------------------------------------------------------------------
@dataclass
class Env:
    dbname: str
    admin_target: object
    writer_target: object
    app_target: object
    strict: bool
    four_file_notices: list[str] = field(default_factory=list)
    diagnostic_notices: list[str] = field(default_factory=list)
    first_install_error: str | None = None
    leak_before_hardening: dict[str, bool] = field(default_factory=dict)
    function_leak_before_hardening: dict[str, bool] = field(default_factory=dict)
    unrelated_before: object = None
    created_tables: list[str] = field(default_factory=list)
    harden_report: dict | None = None
    verify_report: dict | None = None

    def admin(self, **kw):
        return connect_disposable(self.admin_target, **kw)

    def writer(self, **kw):
        return connect_disposable(self.writer_target, **kw)

    def app(self, **kw):
        return connect_disposable(self.app_target, **kw)


def _ensure_role(conn, name: str, *, login: bool, password: str | None) -> None:
    row = conn.execute(
        "SELECT rolcanlogin, rolsuper, rolcreaterole, rolcreatedb FROM pg_roles WHERE rolname = %s",
        [name],
    ).fetchone()
    if row is None:
        options = (
            "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}"
            if login
            else "NOLOGIN"
        )
        conn.execute(
            sql.SQL("CREATE ROLE {} " + options).format(
                sql.Identifier(name), *([sql.Literal(password)] if login else [])
            )
        )
        return
    assert (row[0], row[1], row[2], row[3]) == (login, False, False, False), (
        f"role {name} exists with other attributes"
    )
    if login:
        conn.execute(
            sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                sql.Identifier(name), sql.Literal(password)
            )
        )


def _public_objects_fingerprint(conn):
    """Structure + ACL + data fingerprint of the unrelated pre-existing public objects."""
    rows = conn.execute(
        """
        SELECT c.relname, c.relkind, c.relacl::text, c.relowner::regrole::text,
               (SELECT string_agg(a.attname || ':' || format_type(a.atttypid, a.atttypmod), ',' ORDER BY a.attnum)
                FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND (c.relname LIKE 'unrelated\\_%' OR c.relname LIKE '%lookalike%')
        ORDER BY c.relname
        """
    ).fetchall()
    functions = conn.execute(
        """SELECT p.proname, p.proacl::text, p.proowner::regrole::text, md5(p.prosrc)
           FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE n.nspname = 'public' AND (p.proname LIKE 'unrelated\\_%' OR p.proname LIKE '%lookalike%')
           ORDER BY p.proname"""
    ).fetchall()
    data = conn.execute(
        "SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM public.unrelated_ledger t"
    ).fetchone()
    return rows, functions, data


def build_env(dsn, *, strict: bool, harden: bool = True) -> Env:
    tag = "strict" if strict else ("prod" if harden else "leaky")
    dbname = f"bdefault_disposable_rel_{tag}_{uuid.uuid4().hex[:8]}"
    passwords = ROLE_PASSWORDS
    with connect_disposable(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        for role in ("worker_writer", "app_runtime", "app_analytics_ro"):
            _ensure_role(conn, role, login=True, password=passwords[role])
        for role in GROUP_ROLES:
            _ensure_role(conn, role, login=False, password=None)
    admin_target = dsn.derive(dbname=dbname)
    env = Env(
        dbname=dbname,
        admin_target=admin_target,
        writer_target=admin_target.derive(
            user="worker_writer", password=passwords["worker_writer"]
        ),
        app_target=admin_target.derive(
            user="app_runtime", password=passwords["app_runtime"]
        ),
        strict=strict,
    )
    with connect_disposable(admin_target, autocommit=True) as conn:
        # Production-like unrelated public objects owned by the administrator.
        conn.execute("""
            CREATE TABLE public.unrelated_ledger (id integer PRIMARY KEY, note text NOT NULL);
            INSERT INTO public.unrelated_ledger VALUES (1, 'alpha'), (2, 'beta');
            GRANT SELECT ON public.unrelated_ledger TO app_runtime;
            CREATE VIEW public.unrelated_view AS SELECT id FROM public.unrelated_ledger;
            CREATE FUNCTION public.unrelated_add(a integer, b integer) RETURNS integer LANGUAGE sql IMMUTABLE
                AS 'SELECT a + b';
        """)
        if strict:
            conn.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
        conn.execute("GRANT USAGE, CREATE ON SCHEMA public TO worker_writer")
        conn.execute(
            "GRANT USAGE ON SCHEMA public TO bond_credit_reader, bond_credit_writer, bond_credit_auditor, "
            "bond_default_diagnostic_reader, app_runtime"
        )
        conn.execute("GRANT bond_credit_writer TO worker_writer")
        conn.execute("GRANT bond_default_diagnostic_reader TO app_runtime")
        if not strict:
            conn.execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE worker_writer IN SCHEMA public "
                "GRANT INSERT, SELECT, UPDATE, DELETE ON TABLES TO app_runtime"
            )
            conn.execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE worker_writer IN SCHEMA public "
                "GRANT SELECT ON TABLES TO app_analytics_ro"
            )
            # Adversarial extension beyond the measured table defaults: new functions are executable by the API too.
            conn.execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE worker_writer IN SCHEMA public "
                "GRANT EXECUTE ON FUNCTIONS TO app_runtime"
            )
    # Pre-existing objects OWNED BY THE INSTALLING ROLE (so default privileges leak onto them too), some
    # with names that a `bond_%` pattern would match: hardening must leave their ACLs exactly as they are.
    with env.writer(autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE public.unrelated_writer_table (id integer PRIMARY KEY);
            CREATE TABLE public.bond_credit_lookalike_table (id integer PRIMARY KEY);
            CREATE FUNCTION public.bond_credit_lookalike_fn(integer) RETURNS integer LANGUAGE sql IMMUTABLE
                AS 'SELECT 1';
            """
        )
    with connect_disposable(admin_target, autocommit=True) as conn:
        before_tables = {
            r[0]
            for r in conn.execute(
                "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND relkind = 'r'"
            ).fetchall()
        }
        env.unrelated_before = _public_objects_fingerprint(conn)

    def install(writer_conn, notices):
        writer_conn.add_notice_handler(
            lambda diag: notices.append(f"{diag.severity}:{diag.message_primary}")
        )
        p.install_schema(writer_conn, "public")
        d.install_diagnostic_schema(writer_conn, schema="public")

    with env.writer(autocommit=True) as conn:
        try:
            install(conn, env.four_file_notices)
        except psycopg.errors.InsufficientPrivilege as exc:
            env.first_install_error = f"{type(exc).__name__}: {exc}"
    if env.first_install_error is not None:
        # Documented narrow fallback: a temporary USAGE WITH GRANT OPTION for worker_writer, removed afterwards.
        with env.admin(autocommit=True) as conn:
            conn.execute(
                "GRANT USAGE ON SCHEMA public TO worker_writer WITH GRANT OPTION"
            )
        try:
            with env.writer(autocommit=True) as conn:
                install(conn, env.four_file_notices)
        finally:
            with env.admin(autocommit=True) as conn:
                conn.execute(
                    "REVOKE GRANT OPTION FOR USAGE ON SCHEMA public FROM worker_writer"
                )
    with env.admin(autocommit=True) as conn:
        env.created_tables = sorted(
            r[0]
            for r in conn.execute(
                "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND relkind = 'r'"
            ).fetchall()
            if r[0] not in before_tables
        )
        for table in env.created_tables:
            env.leak_before_hardening[table] = bool(
                conn.execute(
                    "SELECT has_table_privilege('app_runtime', %s, 'SELECT')",
                    [f"public.{table}"],
                ).fetchone()[0]
            )
        for oid_text, name in conn.execute(
            "SELECT p.oid::regprocedure::text, p.proname FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace "
            "AND p.proowner = 'worker_writer'::regrole"
        ).fetchall():
            env.function_leak_before_hardening[oid_text] = bool(
                conn.execute(
                    "SELECT has_function_privilege('app_runtime', %s::regprocedure, 'EXECUTE')",
                    [oid_text],
                ).fetchone()[0]
            )
    if harden:
        with env.writer(autocommit=True) as conn:
            env.harden_report = d.harden_installed_privileges(conn, schema="public")
            env.verify_report = d.verify_installed_privileges(conn, schema="public")
    return env


def drop_env(dsn, env: Env) -> None:
    with connect_disposable(dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(env.dbname)
            )
        )


@pytest.fixture(scope="module")
def dsn():
    raw = os.environ.get(guard.ENV_VAR)
    if raw is None:
        pytest.skip(
            f"{guard.ENV_VAR} not set; the disposable DB suite refuses any ambient DSN"
        )
    try:
        target = guard.check_test_dsn(raw)
    except guard.UnsafeTestDsn as exc:
        pytest.fail(f"refusing {guard.ENV_VAR}: {exc}")
    with connect_disposable(target) as conn:
        version = conn.execute(
            "SELECT current_setting('server_version_num')::int"
        ).fetchone()[0]
        assert version >= 160000
        is_super = conn.execute(
            "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
        ).fetchone()[0]
        assert is_super, (
            "the rehearsal DSN user plays the administrator and must be a superuser"
        )
    return target


@pytest.fixture(scope="module")
def env(dsn):
    environment = build_env(dsn, strict=False)
    try:
        yield environment
    finally:
        drop_env(dsn, environment)
        with connect_disposable(dsn, autocommit=True) as conn:
            for role in ("worker_writer", "app_runtime", "app_analytics_ro"):
                with contextlib.suppress(psycopg.Error):
                    conn.execute(
                        sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role))
                    )


@pytest.fixture
def clean(env):
    """Empty every table created by the installers (superuser, triggers bypassed); pointers start absent."""
    with env.admin(autocommit=True) as conn:
        conn.execute("SET session_replication_role = replica")
        # The installation marker is install state, not scenario data: it survives the reset.
        tables = ", ".join(
            f"public.{t}"
            for t in env.created_tables
            if t != "bond_default_diagnostic_installations"
        )
        conn.execute(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
    yield env


# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------
def publish(env, bundle) -> uuid.UUID:
    with env.writer() as conn:
        store = p.PostgresPublicationStore(conn, "public")
        p.prepare_bundle(store, bundle)
        p.validate_bundle(store, bundle.publication_id)
    return bundle.publication_id


def diagnostic_release(env, bundle) -> uuid.UUID:
    publication = publish(env, bundle)
    with env.writer() as conn, conn.transaction():
        return d.prepare_diagnostic(
            conn,
            schema="public",
            publication_id=publication,
            projection=unit.projection_of(bundle),
            source_frontier_manifest_digest=unit.frontier_digest_of(bundle),
        )


def promote(env, release_id, expected=None):
    with env.writer() as conn, conn.transaction():
        d.promote_diagnostic(
            conn, schema="public", release_id=release_id, expected_release_id=expected
        )


def read_current(env) -> dict:
    with env.app() as conn:
        return d.read_current_diagnostic(conn, schema="public")


def reason_of(info) -> str:
    return info.value.reason


def count_of(admin, table: str) -> int:
    return admin.execute(
        sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("public", table))
    ).fetchone()[0]


# ---------------------------------------------------------------------------
# Install, idempotency and catalog
# ---------------------------------------------------------------------------
def test_environment_is_production_shaped_and_install_warns_but_does_not_fail(env):
    assert env.first_install_error is None, env.first_install_error
    with env.admin() as conn:
        roles = {
            r[0]: r[1:]
            for r in conn.execute(
                "SELECT rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb FROM pg_roles "
                "WHERE rolname IN ('worker_writer', 'app_runtime') OR rolname = ANY(%s)",
                [list(GROUP_ROLES)],
            ).fetchall()
        }
        assert roles["worker_writer"] == (True, False, False, False) and roles[
            "app_runtime"
        ] == (True, False, False, False)
        assert all(roles[g] == (False, False, False, False) for g in GROUP_ROLES)
        owner = conn.execute(
            "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = 'public'"
        ).fetchone()[0]
        assert owner in ("pg_database_owner", "postgres")
        assert conn.execute(
            "SELECT has_schema_privilege('worker_writer', 'public', 'CREATE'), "
            "has_schema_privilege('app_runtime', 'public', 'CREATE')"
        ).fetchone() == (True, False)
        table_owners = {
            r[0]
            for r in conn.execute(
                "SELECT relowner::regrole::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND c.relname = ANY(%s)",
                [list(DIAG_TABLES) + ["bond_credit_publications"]],
            ).fetchall()
        }
        assert table_owners == {"worker_writer"}
    # GRANT USAGE ON SCHEMA public by a non-owner that still holds privileges on it is a WARNING, never an error.
    assert any("no privileges were granted" in n for n in env.four_file_notices), (
        env.four_file_notices
    )


def test_reinstall_is_idempotent_and_leaves_unrelated_objects_and_acls_unchanged(env):
    with env.admin() as conn:
        first = guard._catalog(conn, "public", sorted(env.created_tables))
        acl_before = conn.execute(
            "SELECT relname, relacl::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND relkind = 'r' ORDER BY 1"
        ).fetchall()
        proc_before = conn.execute(
            "SELECT proname, pg_get_function_identity_arguments(oid), proacl::text, md5(prosrc), prosecdef, proconfig::text "
            "FROM pg_proc WHERE pronamespace = 'public'::regnamespace ORDER BY 1, 2"
        ).fetchall()
    with env.writer(autocommit=True) as conn:
        for _ in range(2):
            p.install_schema(conn, "public")
            d.install_diagnostic_schema(conn, schema="public")
    with env.admin() as conn:
        assert guard._catalog(conn, "public", sorted(env.created_tables)) == first
        assert (
            conn.execute(
                "SELECT relname, relacl::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND relkind = 'r' ORDER BY 1"
            ).fetchall()
            == acl_before
        )
        assert (
            conn.execute(
                "SELECT proname, pg_get_function_identity_arguments(oid), proacl::text, md5(prosrc), prosecdef, proconfig::text "
                "FROM pg_proc WHERE pronamespace = 'public'::regnamespace ORDER BY 1, 2"
            ).fetchall()
            == proc_before
        )
        assert _public_objects_fingerprint(conn) == env.unrelated_before


def test_diagnostic_functions_are_definer_owned_by_the_writer_with_a_captured_search_path_and_no_public_grant(
    env,
):
    with env.admin() as conn:
        rows = conn.execute(
            """SELECT p.proname, p.prosecdef, p.proowner::regrole::text, p.proconfig::text,
                      coalesce((SELECT array_agg(a.grantee::regrole::text ORDER BY a.grantee::regrole::text)
                                FROM aclexplode(p.proacl) a WHERE a.grantee <> p.proowner), ARRAY[]::text[]),
                      EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
                              WHERE a.grantee = 0)
               FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace AND p.proname LIKE 'bond\\_default\\_%'
               ORDER BY 1"""
        ).fetchall()
    by_name = {r[0]: r for r in rows}
    public_api = {
        "bond_default_prepare_diagnostic": ["bond_credit_writer"],
        "bond_default_promote_diagnostic": ["bond_credit_writer"],
        "bond_default_revoke_diagnostic": ["bond_credit_writer"],
        "bond_default_verify_diagnostic": ["bond_credit_auditor", "bond_credit_writer"],
        "bond_default_current_diagnostic_release": ["bond_default_diagnostic_reader"],
    }
    for name, grantees in public_api.items():
        row = by_name[name]
        assert (
            row[1] is True
            and row[2] == "worker_writer"
            and row[3] == '{"search_path=public, pg_temp"}'
        ), name
        assert row[4] == grantees and row[5] is False, (name, row)
    assert set(by_name) >= set(public_api)
    for name, row in by_name.items():
        assert (
            row[2] == "worker_writer" and row[3] == '{"search_path=public, pg_temp"}'
        ), name
        assert row[5] is False, f"{name} is executable by PUBLIC"
        if name not in public_api:
            assert row[4] == [], f"{name} has extra grantees {row[4]}"
        if name.startswith("bond_default_diag_"):
            assert row[1] is False or name in public_api
    with (
        env.admin() as conn
    ):  # tables: PUBLIC and the default-privilege grantees hold nothing on the new tables
        for table in DIAG_TABLES:
            acl = conn.execute(
                "SELECT coalesce(array_agg(a.grantee::regrole::text || ':' || a.privilege_type ORDER BY 1), ARRAY[]::text[]) "
                "FROM pg_class c, aclexplode(c.relacl) a WHERE c.oid = %s::regclass AND a.grantee <> c.relowner",
                [f"public.{table}"],
            ).fetchone()[0]
            assert acl == ["bond_credit_auditor:SELECT", "bond_credit_writer:SELECT"], (
                table,
                acl,
            )
            assert conn.execute(
                "SELECT has_table_privilege('app_runtime', %s, 'SELECT'), "
                "has_table_privilege('app_analytics_ro', %s, 'SELECT')",
                [f"public.{table}"] * 2,
            ).fetchone() == (False, False)


def test_production_default_privileges_leak_dml_on_pinned_tables_until_the_owner_revokes(
    env,
):
    """Measured production behavior: ALTER DEFAULT PRIVILEGES grant app_runtime DML on every new public table.

    The pinned files only revoke from PUBLIC and bond_credit_reader, so the leak on their tables is real;
    it is closed by ``harden_installed_privileges`` (run by ``build_env``).
    """
    pinned = [t for t in env.created_tables if t not in DIAG_TABLES]
    assert env.leak_before_hardening and all(
        env.leak_before_hardening[t] for t in pinned
    )
    assert not any(
        env.leak_before_hardening[t] for t in DIAG_TABLES
    )  # the diagnostic file sweeps its own objects
    pinned_functions = [
        f for f in env.function_leak_before_hardening if f.startswith("bond_credit_")
    ]
    diagnostic_functions = {
        f: v
        for f, v in env.function_leak_before_hardening.items()
        if f.startswith("bond_default_")
    }
    assert pinned_functions and any(
        env.function_leak_before_hardening[f] for f in pinned_functions
    )
    # The diagnostic file sweeps default EXECUTE grants: only the runtime reader stays reachable by app_runtime.
    assert {f for f, v in diagnostic_functions.items() if v} == {
        "bond_default_current_diagnostic_release()"
    }
    with env.admin() as conn:
        for table in pinned:
            assert conn.execute(
                "SELECT has_table_privilege('app_runtime', %s, 'SELECT'), "
                "has_table_privilege('app_runtime', %s, 'INSERT'), "
                "has_table_privilege('app_analytics_ro', %s, 'SELECT')",
                [f"public.{table}"] * 3,
            ).fetchone() == (False, False, False), table
        leaks = conn.execute(
            "SELECT p.oid::regprocedure::text FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace "
            "AND p.proowner = 'worker_writer'::regrole AND p.proname LIKE 'bond\\_credit\\_%%' "
            "AND p.proname NOT LIKE '%%lookalike%%' AND has_function_privilege('app_runtime', p.oid, 'EXECUTE')"
        ).fetchall()
        assert leaks == []


@pytest.fixture(scope="module")
def leaky(dsn, env):
    """A production-shaped database installed but NOT hardened (default privileges leak onto new objects)."""
    environment = build_env(dsn, strict=False, harden=False)
    try:
        yield environment
    finally:
        drop_env(dsn, environment)


def _acl_snapshot(env, names: tuple[str, ...]):
    with env.admin() as conn:
        rels = conn.execute(
            "SELECT relname, relacl::text, relowner::regrole::text FROM pg_class "
            "WHERE relnamespace = 'public'::regnamespace AND relname = ANY(%s) ORDER BY 1",
            [list(names)],
        ).fetchall()
        funcs = conn.execute(
            "SELECT oid::regprocedure::text, proacl::text, proowner::regrole::text FROM pg_proc "
            "WHERE pronamespace = 'public'::regnamespace AND proname = ANY(%s) ORDER BY 1",
            [list(names)],
        ).fetchall()
    return rels, funcs


UNRELATED_NAMES = (
    "unrelated_ledger",
    "unrelated_view",
    "unrelated_add",
    "unrelated_writer_table",
    "bond_credit_lookalike_table",
    "bond_credit_lookalike_fn",
)


def test_verify_fails_on_the_default_privilege_leak_until_hardening_then_passes(leaky):
    assert all(
        leaky.leak_before_hardening[t]
        for t in leaky.created_tables
        if t not in DIAG_TABLES
    )
    with leaky.writer(autocommit=True) as conn:
        with pytest.raises(d.DiagnosticError) as info:
            d.verify_installed_privileges(conn, schema="public")
        assert info.value.reason == "privileges_unverified"
        report = info.value.report
        assert report["ok"] is False
        failed = {k for k, v in report["checks"].items() if not v["ok"]}
        assert {"no_public_or_unintended_grants", "runtime_privilege_matrix"} <= failed
        assert report["checks"]["group_roles_nologin"]["ok"] is True
        assert report["checks"]["runtime_membership"]["ok"] is True
        assert report["checks"]["contract_pins_match"]["ok"] is True
        assert any(
            item.startswith("app_runtime:bond_")
            for item in report["checks"]["runtime_privilege_matrix"]["detail"]
        )
        # The unrelated writer-owned objects are outside the manifest: the leak on them is not reported.
        assert not any(
            "lookalike" in str(v["detail"]) or "unrelated_writer" in str(v["detail"])
            for v in report["checks"].values()
        )
        before = _acl_snapshot(leaky, UNRELATED_NAMES)
        fingerprint = None
        with leaky.admin() as admin:
            fingerprint = _public_objects_fingerprint(admin)
        harden = d.harden_installed_privileges(conn, schema="public")
        assert harden["owner"] == "worker_writer" and harden["schema"] == "public"
        assert (
            harden["tables"] == 24
            and harden["sequences"] == 0
            and harden["functions"] >= 121
        )
        revoked = {tuple(x) for x in harden["revoked"]}
        pinned_tables = [t for t in leaky.created_tables if t not in DIAG_TABLES]
        for table in pinned_tables:
            assert (table, "app_runtime", "SELECT") in revoked and (
                table,
                "app_analytics_ro",
                "SELECT",
            ) in revoked
            assert (table, "app_runtime", "INSERT") in revoked
        assert any(
            o.startswith("bond_credit_read_coverage(") and g == "app_runtime"
            for o, g, _p in revoked
        )
        assert not any(
            o in UNRELATED_NAMES or "lookalike" in o for o, _g, _p in revoked
        )
        assert not any(g in d.INTENDED_ROLES for _o, g, _p in revoked)
        assert harden["intended_grants_added"] == []
        assert {g for _o, g, _p in revoked} == {"app_runtime", "app_analytics_ro"}, {
            g for _o, g, _p in revoked
        }
        report = d.verify_installed_privileges(conn, schema="public")
        assert report["ok"] is True and all(v["ok"] for v in report["checks"].values())
        assert set(report["checks"]) >= {
            "objects_present",
            "single_owner_not_runtime",
            "group_roles_nologin",
            "runtime_membership",
            "no_public_or_unintended_grants",
            "intended_grants_exact",
            "api_function_grants_exact",
            "diagnostic_tables_select_only",
            "runtime_privilege_matrix",
            "definer_set_exact",
            "search_path_pinned_all_functions",
            "owner_not_privileged",
            "schema_create_restricted",
            "contract_pins_match",
            "diagnostic_sql_digest_pinned",
            "pointers_empty",
        }
        # Idempotent: nothing left to revoke, nothing added; unrelated ACLs (incl. writer-owned lookalikes
        # that DO carry the leaked default privileges) are byte-identical; no default privileges were touched.
        again = d.harden_installed_privileges(conn, schema="public")
        assert again["revoked"] == [] and again["intended_grants_added"] == []
        assert _acl_snapshot(leaky, UNRELATED_NAMES) == before
        with leaky.admin() as admin:
            assert _public_objects_fingerprint(admin) == fingerprint
            defaults = admin.execute(
                "SELECT defaclrole::regrole::text, defaclobjtype, defaclacl::text FROM pg_default_acl ORDER BY 1, 2"
            ).fetchall()
            assert len(defaults) == 2 and {d_[1] for d_ in defaults} == {"r", "f"}
    rels, funcs = before
    assert any(
        "app_runtime=arwd/worker_writer" in (acl or "")
        for _n, acl, _o in rels
        if _n == "unrelated_writer_table"
    )
    assert any(
        a and "app_runtime=X/worker_writer" in a for _n, a, _o in funcs
    )  # still leaking: not in scope


def test_reinstall_then_harden_is_idempotent_and_the_end_state_is_stable(env):
    with env.admin() as admin:
        state = admin.execute(
            "SELECT c.relname, c.relacl::text FROM pg_class c WHERE relnamespace = 'public'::regnamespace "
            "AND relkind = 'r' ORDER BY 1"
        ).fetchall()
        procs = admin.execute(
            "SELECT oid::regprocedure::text, proacl::text FROM pg_proc WHERE pronamespace = 'public'::regnamespace ORDER BY 1"
        ).fetchall()
    with env.writer(autocommit=True) as conn:
        for _ in range(2):
            p.install_schema(conn, "public")
            d.install_diagnostic_schema(conn, schema="public")
            report = d.harden_installed_privileges(conn, schema="public")
            assert report["revoked"] == [] or all(
                # reinstalling re-applies the default privileges to nothing new: only DDL-created objects leak
                g in ("app_runtime", "app_analytics_ro", "PUBLIC")
                for _o, g, _p in report["revoked"]
            )
            assert d.verify_installed_privileges(conn, schema="public")["ok"] is True
        assert d.harden_installed_privileges(conn, schema="public")["revoked"] == []
    with env.admin() as admin:
        assert (
            admin.execute(
                "SELECT c.relname, c.relacl::text FROM pg_class c WHERE relnamespace = 'public'::regnamespace "
                "AND relkind = 'r' ORDER BY 1"
            ).fetchall()
            == state
        )
        assert (
            admin.execute(
                "SELECT oid::regprocedure::text, proacl::text FROM pg_proc WHERE pronamespace = 'public'::regnamespace ORDER BY 1"
            ).fetchall()
            == procs
        )


def test_harden_fails_loudly_unless_the_current_user_owns_every_target(env):
    for target in (env.admin, env.app):
        with target(autocommit=True) as conn:
            with pytest.raises(d.DiagnosticError) as info:
                d.harden_installed_privileges(conn, schema="public")
            assert info.value.reason == "privileges_not_owner"
            assert info.value.detail.startswith(
                conn.execute("SELECT current_user").fetchone()[0]
            )
    with (
        env.writer(autocommit=True) as conn,
        pytest.raises(ValueError, match="privileges_schema_must_be_public"),
    ):
        d.harden_installed_privileges(conn, schema="other")
    with (
        env.writer() as conn,  # not autocommit
        pytest.raises(ValueError, match="autocommit"),
    ):
        d.harden_installed_privileges(conn, schema="public")


def test_harden_reports_a_missing_object_and_refuses_to_run_on_an_uninstalled_schema(
    dsn, env
):
    name = f"bdefault_disposable_rel_empty_{uuid.uuid4().hex[:8]}"
    with connect_disposable(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        with connect_disposable(
            env.admin_target.derive(dbname=name), autocommit=True
        ) as conn:
            with pytest.raises(d.DiagnosticError) as info:
                d.harden_installed_privileges(conn, schema="public")
            assert info.value.reason == "privileges_target_missing"
            with pytest.raises(d.DiagnosticError) as info:
                d.verify_installed_privileges(conn, schema="public")
            assert info.value.report["checks"]["objects_present"]["ok"] is False
    finally:
        with connect_disposable(dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )


def test_verify_detects_each_regression_after_a_clean_install(env):
    """Grant something wrong as the owner and the read-back names it; revoke it and verify passes again."""
    with env.writer(autocommit=True) as conn:
        assert d.verify_installed_privileges(conn, schema="public")["ok"] is True
        mutations = [
            (
                "GRANT EXECUTE ON FUNCTION public.bond_credit_read_coverage(uuid, boolean) TO app_runtime",
                "REVOKE ALL ON FUNCTION public.bond_credit_read_coverage(uuid, boolean) FROM app_runtime",
                "runtime_privilege_matrix",
            ),
            (
                "GRANT SELECT ON public.bond_credit_publications TO PUBLIC",
                "REVOKE ALL ON public.bond_credit_publications FROM PUBLIC",
                "no_public_or_unintended_grants",
            ),
            (
                "GRANT SELECT (publication_id) ON public.bond_credit_publications TO app_analytics_ro",
                "REVOKE ALL ON public.bond_credit_publications FROM app_analytics_ro",
                "runtime_privilege_matrix",
            ),
            (
                "GRANT UPDATE ON public.bond_default_diagnostic_pointer TO bond_credit_writer",
                "REVOKE UPDATE ON public.bond_default_diagnostic_pointer FROM bond_credit_writer",
                "intended_grants_exact",
            ),
            (
                "GRANT EXECUTE ON FUNCTION public.bond_default_current_diagnostic_release() TO bond_credit_reader",
                "REVOKE ALL ON FUNCTION public.bond_default_current_diagnostic_release() FROM bond_credit_reader",
                "api_function_grants_exact",
            ),
            (
                "ALTER FUNCTION public.bond_default_diag_guard(uuid, boolean) SET search_path = pg_temp, public",
                "ALTER FUNCTION public.bond_default_diag_guard(uuid, boolean) SET search_path = public, pg_temp",
                "search_path_pinned_all_functions",
            ),
            (
                "ALTER FUNCTION public.bond_default_verify_diagnostic(uuid) SECURITY INVOKER",
                "ALTER FUNCTION public.bond_default_verify_diagnostic(uuid) SECURITY DEFINER",
                "definer_set_exact",
            ),
            (  # M1: a NON-diagnostic (pinned-file) function with a wrong search_path
                "ALTER FUNCTION public.bond_credit_cusip9_valid(text) SET search_path = pg_temp, public",
                "ALTER FUNCTION public.bond_credit_cusip9_valid(text) SET search_path = public, pg_temp",
                "search_path_pinned_all_functions",
            ),
            (  # M1: ... or with no captured search_path at all
                "ALTER FUNCTION public.bond_credit_cusip9_valid(text) RESET search_path",
                "ALTER FUNCTION public.bond_credit_cusip9_valid(text) SET search_path = public, pg_temp",
                "search_path_pinned_all_functions",
            ),
            (  # L2: a missing intended grant is now an exact-set failure too
                "REVOKE SELECT ON public.bond_default_diagnostic_releases FROM bond_credit_auditor",
                "GRANT SELECT ON public.bond_default_diagnostic_releases TO bond_credit_auditor",
                "intended_grants_exact",
            ),
            (  # L2: EXECUTE for an intended role that the SQL files do not grant
                "GRANT EXECUTE ON FUNCTION public.bond_credit_cusip9_valid(text) TO bond_default_diagnostic_reader",
                "REVOKE ALL ON FUNCTION public.bond_credit_cusip9_valid(text) FROM bond_default_diagnostic_reader",
                "intended_grants_exact",
            ),
        ]
        for wrong, restore, check in mutations:
            conn.execute(wrong)
            try:
                with pytest.raises(d.DiagnosticError) as info:
                    d.verify_installed_privileges(conn, schema="public")
                assert not info.value.report["checks"][check]["ok"], wrong
            finally:
                conn.execute(restore)
            assert d.verify_installed_privileges(conn, schema="public")["ok"] is True, (
                restore
            )
    with env.admin(autocommit=True) as admin:  # membership regressions
        admin.execute("GRANT bond_credit_reader TO app_runtime")
        try:
            with pytest.raises(d.DiagnosticError) as info:
                d.verify_installed_privileges(admin, schema="public")
            assert not info.value.report["checks"]["runtime_membership"]["ok"]
        finally:
            admin.execute("REVOKE bond_credit_reader FROM app_runtime")
        d.verify_installed_privileges(admin, schema="public")


def test_verify_restricts_who_can_create_in_the_schema_and_who_owns_the_objects(env):
    """M2: CREATE on schema public only for the object owner / schema owner / superusers; owner not privileged."""
    with env.admin(autocommit=True) as admin:
        report = d.verify_installed_privileges(admin, schema="public")
        assert report["checks"]["schema_create_restricted"]["ok"] is True
        assert report["checks"]["owner_not_privileged"]["detail"] == {
            "worker_writer": (False, False, False)
        }
        cases = [
            (
                "GRANT CREATE ON SCHEMA public TO app_runtime",
                "REVOKE CREATE ON SCHEMA public FROM app_runtime",
                "schema_create_restricted",
            ),
            (
                "GRANT CREATE ON SCHEMA public TO PUBLIC",
                "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
                "schema_create_restricted",
            ),
            (
                "GRANT CREATE ON SCHEMA public TO bond_credit_writer",
                "REVOKE CREATE ON SCHEMA public FROM bond_credit_writer",
                "schema_create_restricted",
            ),
            (
                "ALTER ROLE worker_writer CREATEROLE",
                "ALTER ROLE worker_writer NOCREATEROLE",
                "owner_not_privileged",
            ),
            (
                "ALTER ROLE worker_writer CREATEDB",
                "ALTER ROLE worker_writer NOCREATEDB",
                "owner_not_privileged",
            ),
        ]
        for wrong, restore, check in cases:
            admin.execute(wrong)
            try:
                with pytest.raises(d.DiagnosticError) as info:
                    d.verify_installed_privileges(admin, schema="public")
                assert not info.value.report["checks"][check]["ok"], wrong
            finally:
                admin.execute(restore)
            assert (
                d.verify_installed_privileges(admin, schema="public")["ok"] is True
            ), restore
        # worker_writer (the object owner) legitimately holds CREATE by design.
        assert (
            admin.execute(
                "SELECT has_schema_privilege('worker_writer', 'public', 'CREATE')"
            ).fetchone()[0]
            is True
        )


def test_null_acls_are_treated_as_their_default_privileges(env):
    """L1: a NULL proacl means PUBLIC EXECUTE; a NULL relacl means owner-only. Both are seen, never skipped."""
    with env.admin(autocommit=True) as admin:
        admin.execute(
            "UPDATE pg_proc SET proacl = NULL WHERE oid = 'public.bond_credit_cusip9_valid(text)'::regprocedure"
        )
        try:
            with pytest.raises(d.DiagnosticError) as info:
                d.verify_installed_privileges(admin, schema="public")
            checks = info.value.report["checks"]
            assert not checks["no_public_or_unintended_grants"]["ok"]
            assert any(
                "PUBLIC" in x
                for x in checks["no_public_or_unintended_grants"]["detail"]
            )
            assert not checks["intended_grants_exact"][
                "ok"
            ]  # the intended reader/writer EXECUTE grants vanished
        finally:
            admin.execute(
                "UPDATE pg_proc SET proacl = NULL WHERE false"
            )  # no-op guard; the restore is the owner-side harden below
    with env.writer(autocommit=True) as conn:
        report = d.harden_installed_privileges(conn, schema="public")
        assert any(
            o.startswith("bond_credit_cusip9_valid(") and g == "PUBLIC"
            for o, g, _p in report["revoked"]
        )
        assert d.verify_installed_privileges(conn, schema="public")["ok"] is True
    with env.admin(autocommit=True) as admin:
        admin.execute(
            "UPDATE pg_class SET relacl = NULL WHERE oid = 'public.bond_default_diagnostic_pointer'::regclass"
        )
        try:
            with pytest.raises(d.DiagnosticError) as info:
                d.verify_installed_privileges(admin, schema="public")
            assert not info.value.report["checks"]["intended_grants_exact"]["ok"]
        finally:
            pass
    with env.writer(autocommit=True) as conn:
        d.harden_installed_privileges(conn, schema="public")
        assert d.verify_installed_privileges(conn, schema="public")["ok"] is True


def test_verify_derives_the_roles_it_checks_from_default_privileges(env):
    role = f"bde_default_grantee_{uuid.uuid4().hex[:6]}"
    with env.admin(autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        admin.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE worker_writer IN SCHEMA public GRANT SELECT ON TABLES TO {}"
            ).format(sql.Identifier(role))
        )
        admin.execute(
            sql.SQL("GRANT SELECT ON public.bond_credit_publications TO {}").format(
                sql.Identifier(role)
            )
        )
        try:
            report = None
            with pytest.raises(d.DiagnosticError) as info:
                d.verify_installed_privileges(admin, schema="public")
            report = info.value.report
            assert (
                role in report["checked_roles"]
                and "app_analytics_ro" in report["checked_roles"]
            )
            assert any(
                role in x
                for x in report["checks"]["runtime_privilege_matrix"]["detail"]
            )
            with pytest.raises(d.DiagnosticError) as info:
                d.verify_installed_privileges(
                    admin, schema="public", other_roles=("nonexistent_role_xyz",)
                )
            assert "nonexistent_role_xyz" in info.value.report["checked_roles"]
        finally:
            admin.execute(
                sql.SQL("REVOKE ALL ON public.bond_credit_publications FROM {}").format(
                    sql.Identifier(role)
                )
            )
            admin.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE worker_writer IN SCHEMA public REVOKE SELECT ON TABLES FROM {}"
                ).format(sql.Identifier(role))
            )
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
        assert d.verify_installed_privileges(admin, schema="public")["ok"] is True


def test_verify_compares_all_five_contract_pins_and_reports_definition_evidence(
    env, monkeypatch
):
    with env.writer(autocommit=True) as conn:
        report = d.verify_installed_privileges(conn, schema="public")
        evidence = report["evidence"]["function_definition_sha256"]
        assert len(evidence) == 25
        assert all(re.fullmatch(r"[0-9a-f]{64}", v) for v in evidence.values())
        assert evidence["bond_default_current_diagnostic_release()"] == c.sha256_hex(
            conn.execute(
                "SELECT pg_get_functiondef('public.bond_default_current_diagnostic_release()'::regprocedure)"
            )
            .fetchone()[0]
            .encode()
        )
        assert report == d.verify_installed_privileges(
            conn, schema="public"
        )  # deterministic
        for name in (
            "FAMILY_RULE_VERSION",
            "RATING_RESOLVER_ID",
            "CONTRACT_VERSION",
            "POLICY_DIGEST",
            "SCHEMA_DIGEST",
        ):
            monkeypatch.setattr(c, name, "tampered")
            with pytest.raises(d.DiagnosticError) as info:
                d.verify_installed_privileges(conn, schema="public")
            assert info.value.report["checks"]["contract_pins_match"] == {
                "ok": False,
                "detail": "mismatch",
            }, name
            monkeypatch.undo()
        assert d.verify_installed_privileges(conn, schema="public")["ok"] is True


def test_harden_refuses_an_installation_with_an_uncaptured_search_path(env):
    with env.writer(autocommit=True) as conn:
        conn.execute(
            "ALTER FUNCTION public.bond_credit_cusip9_valid(text) RESET search_path"
        )
        try:
            with pytest.raises(d.DiagnosticError) as info:
                d.harden_installed_privileges(conn, schema="public")
            assert info.value.reason == "privileges_search_path"
            assert "bond_credit_cusip9_valid(" in info.value.detail
        finally:
            conn.execute(
                "ALTER FUNCTION public.bond_credit_cusip9_valid(text) SET search_path = public, pg_temp"
            )
        assert d.harden_installed_privileges(conn, schema="public")["revoked"] == []
        # The install helper pins the session search_path explicitly and verifies it.
        conn.execute("SET search_path TO other_schema_xyz, pg_temp")
        d.install_diagnostic_schema(conn, schema="public")
        assert conn.execute("SHOW search_path").fetchone()[0] == "public, pg_temp"
        conn.execute("SET search_path TO DEFAULT")


def test_verify_requires_empty_pointers_unless_told_otherwise(clean):
    release = diagnostic_release(clean, unit.coverage_only_bundle())
    promote(clean, release)
    with clean.writer(autocommit=True) as conn:
        with pytest.raises(d.DiagnosticError) as info:
            d.verify_installed_privileges(conn, schema="public")
        assert (
            info.value.report["checks"]["pointers_empty"]["detail"][
                "bond_default_diagnostic_pointer"
            ]
            == 1
        )
        assert (
            d.verify_installed_privileges(
                conn, schema="public", require_empty_pointers=False
            )["ok"]
            is True
        )


def test_strict_schema_privileges_variant_installs_with_warnings_or_the_documented_fallback(
    dsn,
):
    """PUBLIC without USAGE on public and no default privileges: the pinned installer still works as worker_writer."""
    strict = build_env(dsn, strict=True)
    try:
        # Whether the repeated GRANT USAGE errors is recorded; either way the end state must be correct.
        assert strict.first_install_error is None, strict.first_install_error
        assert any(
            "no privileges were granted" in n for n in strict.four_file_notices
        ), strict.four_file_notices
        with strict.admin() as conn:
            assert conn.execute(
                "SELECT has_schema_privilege('worker_writer', 'public', 'USAGE WITH GRANT OPTION'), "
                "has_schema_privilege('app_runtime', 'public', 'USAGE'), "
                "has_schema_privilege('app_runtime', 'public', 'CREATE'), "
                "has_function_privilege('app_runtime', 'public.bond_default_current_diagnostic_release()', 'EXECUTE'), "
                "has_function_privilege('app_runtime', 'public.bond_credit_read_coverage(uuid, boolean)', 'EXECUTE'), "
                "has_table_privilege('app_runtime', 'public.bond_credit_publications', 'SELECT')"
            ).fetchone() == (False, True, False, True, False, False)
        with strict.app() as conn, pytest.raises(d.DiagnosticError) as info:
            d.read_current_diagnostic(conn, schema="public")
        assert (
            info.value.reason == "diagnostic_not_published"
            and info.value.sqlstate == "P0002"
        )
    finally:
        drop_env(dsn, strict)


# ---------------------------------------------------------------------------
# Publication, verification, election and the runtime reader
# ---------------------------------------------------------------------------
def test_no_pointer_yields_diagnostic_not_published_for_the_runtime_role(clean):
    with clean.app() as conn:
        assert conn.execute(
            "SELECT pg_has_role('app_runtime', 'bond_default_diagnostic_reader', 'member'), "
            "pg_has_role('app_runtime', 'bond_credit_reader', 'member'), "
            "pg_has_role('app_runtime', 'bond_credit_writer', 'member'), "
            "pg_has_role('app_runtime', 'bond_credit_auditor', 'member')"
        ).fetchone() == (True, False, False, False)
        with pytest.raises(d.DiagnosticError) as info:
            d.read_current_diagnostic(conn, schema="public")
    assert (info.value.reason, info.value.sqlstate) == (
        "diagnostic_not_published",
        "P0002",
    )
    # The raw exception is a stable, bounded SQLSTATE / message pair.
    with (
        clean.app(autocommit=True) as conn,
        pytest.raises(errors.lookup("P0002")) as raw,
    ):
        conn.execute("SELECT public.bond_default_current_diagnostic_release()")
    assert (
        raw.value.diag.message_primary
        == "bond_default_diagnostic:diagnostic_not_published"
    )


def test_prepare_verify_promote_and_the_runtime_reads_the_complete_allowlisted_response(
    clean,
):
    bundle = unit.coverage_only_bundle()
    projection = unit.projection_of(bundle)
    publication = publish(clean, bundle)
    with clean.writer() as conn, conn.transaction():
        release = d.prepare_diagnostic(
            conn,
            schema="public",
            publication_id=publication,
            projection=projection,
            source_frontier_manifest_digest=unit.frontier_digest_of(bundle),
        )
        assert (
            d.verify_diagnostic(conn, schema="public", release_id=release) == projection
        )  # allowed before election
        assert (
            d.verify_diagnostic_report(conn, schema="public", release_id=release)[
                "is_current"
            ]
            is False
        )
    with clean.admin() as admin:
        assert (
            count_of(admin, "bond_default_diagnostic_pointer") == 0
            and count_of(admin, "bond_credit_current_pointer") == 0
        )
    with (
        pytest.raises(d.DiagnosticError, match="diagnostic_not_published"),
        clean.app() as conn,
    ):
        d.read_current_diagnostic(conn, schema="public")  # prepare never elects

    promote(clean, release)
    served = read_current(clean)
    assert set(served) == {
        "schema_version",
        "product",
        "release_id",
        "publication_id",
        "panel_publication_id",
        "tier",
        "display_mode",
        "quality_state",
        "build_scope",
        "target_month",
        "knowledge_cutoff",
        "built_at",
        "validated_at",
        "promoted_at",
        "recommendation_eligible",
        "source_frontiers",
        "coverage",
        "accepted_events",
        "counts",
        "limitations",
    }
    with clean.admin() as admin:
        built, validated, promoted, created_by = admin.execute(
            "SELECT bond_credit_ts_text(p.prepared_at), bond_credit_ts_text(p.validated_at), "
            "bond_credit_ts_text(d.promoted_at), r.created_by "
            "FROM bond_credit_publications p, bond_default_diagnostic_pointer d, bond_default_diagnostic_releases r "
            "WHERE p.publication_id = %s AND r.release_id = d.release_id",
            [publication],
        ).fetchone()
    assert created_by == "worker_writer"
    expected = projection.to_json_obj() | {
        "release_id": str(release),
        "publication_id": str(publication),
        "panel_publication_id": str(bundle.manifest["panel_publication_id"]),
        "target_month": "2026-08-01",
        "knowledge_cutoff": c.ts_text(K0),
        "built_at": built,
        "validated_at": validated,
        "promoted_at": promoted,
    }
    assert served == expected
    assert built < validated < promoted and built != c.ts_text(
        K0
    )  # persisted times, not K or the request time
    assert (
        served["accepted_events"] == [] and served["recommendation_eligible"] is False
    )
    assert (
        served["counts"]["unresolved_events"] is None
        and served["counts"]["censored_issue_months"] is None
    )
    assert (
        d.DiagnosticProjection.from_json_obj(
            {k: served[k] for k in projection.to_json_obj()}
        )
        == projection
    )
    # The qualified pointer is untouched.
    with clean.admin() as admin:
        assert count_of(admin, "bond_credit_current_pointer") == 0
    # Reinstalling keeps every row (CREATE IF NOT EXISTS) and the reader unchanged.
    with clean.writer(autocommit=True) as conn:
        d.install_diagnostic_schema(conn, schema="public")
    assert read_current(clean) == served


def test_sql_and_python_agree_on_projection_frontier_digest_identity_and_release_id(
    clean,
):
    bundle = unit.coverage_only_bundle()
    projection = unit.projection_of(bundle)
    release = diagnostic_release(clean, bundle)
    publication = bundle.publication_id
    with clean.writer() as conn:
        derived, digest, frontier_digest = conn.execute(
            "SELECT public.bond_default_diag_derive(%s), public.bond_credit_json_digest(%s::jsonb), "
            "public.bond_default_diag_frontier_digest(public.bond_default_diag_frontier_records(%s))",
            [
                publication,
                psycopg.types.json.Jsonb(projection.to_json_obj()),
                publication,
            ],
        ).fetchone()
        assert derived == projection.to_json_obj()
        assert digest == projection.digest
        assert frontier_digest == unit.frontier_digest_of(bundle)
        row = conn.execute(
            "SELECT release_id, projection_digest, source_frontier_manifest_digest, diagnostic_sql_digest, "
            "publication_fingerprint, policy_digest, contract_digest, product, tier, display_projection_version "
            "FROM public.bond_default_diagnostic_releases"
        ).fetchone()
        assert (
            row[0] == release
            and row[1] == projection.digest
            and row[2] == frontier_digest
        )
        assert row[3] == d.DIAGNOSTIC_SQL_DIGEST == d.diagnostic_sql_digest()
        assert (
            row[4] == bundle.manifest["fingerprint_digest"]
            and row[5] == c.POLICY_DIGEST
            and row[6] == c.SCHEMA_DIGEST
        )
        assert row[7:] == (d.PRODUCT, d.TIER, d.DISPLAY_VERSION)
        identity = d.release_identity(
            publication_id=publication,
            publication_fingerprint=row[4],
            target_month=T0,
            knowledge_cutoff=K0,
            policy_digest=row[5],
            contract_digest=row[6],
            source_frontier_manifest_digest=row[2],
            projection_digest=row[1],
            diagnostic_sql_digest=row[3],
        )
        assert d.release_id_for(identity) == release
        sql_identity = conn.execute(
            "SELECT public.bond_default_diag_identity(%s, %s, %s::date, %s::timestamptz, %s, %s, %s, %s, %s)",
            [publication, row[4], T0, K0, row[5], row[6], row[2], row[1], row[3]],
        ).fetchone()[0]
        assert sql_identity == identity
        golden = unit.IDENTITY
        assert (
            str(
                conn.execute(
                    "SELECT public.bond_default_diag_release_id_for(public.bond_default_diag_identity(%s, %s, %s::date, "
                    "%s::timestamptz, %s, %s, %s, %s, %s))",
                    [
                        golden["publication_id"],
                        golden["publication_fingerprint"],
                        golden["target_month"],
                        golden["knowledge_cutoff"],
                        golden["policy_digest"],
                        golden["contract_digest"],
                        golden["source_frontier_manifest_digest"],
                        golden["projection_digest"],
                        golden["diagnostic_sql_digest"],
                    ],
                ).fetchone()[0]
            )
            == unit.GOLDEN_RELEASE_ID
        )


def test_prepare_is_idempotent_replays_by_content_and_never_replaces_or_commits(clean):
    bundle = unit.coverage_only_bundle()
    publication = publish(clean, bundle)
    projection = unit.projection_of(bundle)
    frontier = unit.frontier_digest_of(bundle)
    with clean.writer() as conn, conn.transaction():
        first = d.prepare_diagnostic(
            conn,
            schema="public",
            publication_id=publication,
            projection=projection,
            source_frontier_manifest_digest=frontier,
        )
    with clean.admin() as admin:
        created = admin.execute(
            "SELECT created_at FROM bond_default_diagnostic_releases"
        ).fetchone()[0]
    with clean.writer() as conn, conn.transaction():
        second = d.prepare_diagnostic(
            conn,
            schema="public",
            publication_id=publication,
            projection=projection,
            source_frontier_manifest_digest=frontier,
        )
    assert first == second
    with clean.admin() as admin:
        assert count_of(admin, "bond_default_diagnostic_releases") == 1
        assert (
            admin.execute(
                "SELECT created_at FROM bond_default_diagnostic_releases"
            ).fetchone()[0]
            == created
        )
    # No hidden commit: a rolled back transaction leaves nothing behind.
    other = unit.coverage_only_bundle(label="rolled_back")
    other_publication = publish(clean, other)
    with (
        clean.writer() as conn,
        pytest.raises(RuntimeError, match="rollback"),
        conn.transaction(),
    ):
        d.prepare_diagnostic(
            conn,
            schema="public",
            publication_id=other_publication,
            projection=unit.projection_of(other),
            source_frontier_manifest_digest=unit.frontier_digest_of(other),
        )
        assert count_of(conn, "bond_default_diagnostic_releases") == 2
        raise RuntimeError("rollback")
    with clean.admin() as admin:
        assert count_of(admin, "bond_default_diagnostic_releases") == 1


def test_concurrent_identical_prepares_produce_one_release(clean):
    bundle = unit.coverage_only_bundle()
    publication = publish(clean, bundle)
    projection, frontier = unit.projection_of(bundle), unit.frontier_digest_of(bundle)
    results: list[uuid.UUID] = []
    barrier = threading.Barrier(4)

    def worker():
        with clean.writer() as conn:
            barrier.wait()
            with conn.transaction():
                results.append(
                    d.prepare_diagnostic(
                        conn,
                        schema="public",
                        publication_id=publication,
                        projection=projection,
                        source_frontier_manifest_digest=frontier,
                    )
                )

    threads = [threading.Thread(target=worker) for _ in range(4)]
    [t.start() for t in threads]
    [t.join(60) for t in threads]
    assert len(results) == 4 and len(set(results)) == 1
    with clean.admin() as admin:
        assert count_of(admin, "bond_default_diagnostic_releases") == 1


# ---------------------------------------------------------------------------
# Least privilege: what app_runtime can and cannot do
# ---------------------------------------------------------------------------
def test_app_runtime_reads_only_through_the_atomic_reader_and_cannot_touch_anything_else(
    clean,
):
    release = diagnostic_release(clean, unit.coverage_only_bundle())
    promote(clean, release)
    served = read_current(clean)
    assert served["release_id"] == str(release)
    denied_sql = [
        f"SELECT public.{call}" for call in RAW_READERS + DIAG_WRITER_FUNCTIONS
    ]
    denied_sql += [
        f"SELECT count(*) FROM public.{t}"
        for t in (
            *DIAG_TABLES,
            "bond_credit_publications",
            "bond_default_coverage_v1",
            "bond_rating_history_public_v1",
            "bond_credit_current_pointer",
            "bond_credit_publication_revocations",
            "bond_default_source_package",
        )
    ]
    denied_sql += [
        f"INSERT INTO public.bond_default_diagnostic_pointer (product, release_id) VALUES ('bond_default_events_diagnostic_v1', '{release}')",
        "UPDATE public.bond_default_diagnostic_pointer SET release_id = release_id",
        "DELETE FROM public.bond_default_diagnostic_revocations",
        "TRUNCATE public.bond_default_diagnostic_releases",
        "SET ROLE bond_credit_writer",
        "SET ROLE bond_credit_reader",
        "SET ROLE worker_writer",
        "CREATE TABLE public.app_runtime_probe (id integer)",
        "CREATE OR REPLACE FUNCTION public.bond_default_current_diagnostic_release() RETURNS jsonb LANGUAGE sql AS $$ SELECT '{}'::jsonb $$",
    ]
    with clean.app(autocommit=True) as conn:
        for statement in denied_sql:
            with pytest.raises(
                (errors.InsufficientPrivilege, errors.InvalidGrantOperation)
            ) as info:
                conn.execute(statement)
            assert info.value.sqlstate in ("42501", "0LP01"), statement
        assert (
            d.read_current_diagnostic(conn, schema="public") == served
        )  # still readable afterwards
    with clean.admin() as admin:
        privileges = admin.execute(
            """SELECT has_function_privilege('app_runtime', 'public.bond_default_current_diagnostic_release()', 'EXECUTE'),
                      has_function_privilege('app_runtime', 'public.bond_credit_read_coverage(uuid, boolean)', 'EXECUTE'),
                      has_function_privilege('app_runtime', 'public.bond_credit_current_publication(date, uuid)', 'EXECUTE'),
                      has_function_privilege('app_runtime', 'public.bond_default_verify_diagnostic(uuid)', 'EXECUTE'),
                      has_table_privilege('app_runtime', 'public.bond_credit_publications', 'SELECT'),
                      has_table_privilege('app_analytics_ro', 'public.bond_default_diagnostic_releases', 'SELECT'),
                      has_table_privilege('app_runtime', 'public.unrelated_ledger', 'SELECT')"""
        ).fetchone()
        assert privileges == (
            True,
            False,
            False,
            False,
            False,
            False,
            True,
        )  # unrelated grants are untouched
    # Negative control: the broad reader role WOULD expose raw frames, which is why it is not granted.
    role = f"bde_control_{uuid.uuid4().hex[:8]}"
    with clean.admin(autocommit=True) as ctl:
        ctl.execute(
            sql.SQL("CREATE ROLE {} NOLOGIN IN ROLE bond_credit_reader").format(
                sql.Identifier(role)
            )
        )
        try:
            assert (
                ctl.execute(
                    "SELECT has_function_privilege(%s, 'public.bond_credit_read_coverage(uuid, boolean)', "
                    "'EXECUTE')",
                    [role],
                ).fetchone()[0]
                is True
            )
        finally:
            ctl.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_writer_and_auditor_group_members_use_the_definer_functions_without_direct_table_rights(
    clean,
):
    password = secrets.token_hex(12)
    names = {
        "writer": f"bde_member_writer_{uuid.uuid4().hex[:6]}",
        "auditor": f"bde_member_auditor_{uuid.uuid4().hex[:6]}",
    }
    with clean.admin(autocommit=True) as admin:
        for kind, role in names.items():
            admin.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {} IN ROLE {}"
                ).format(
                    sql.Identifier(role),
                    sql.Literal(password),
                    sql.Identifier(f"bond_credit_{kind}"),
                )
            )
    try:
        bundle = unit.coverage_only_bundle()
        publication = publish(clean, bundle)
        member_writer = clean.admin_target.derive(
            user=names["writer"], password=password
        )
        member_auditor = clean.admin_target.derive(
            user=names["auditor"], password=password
        )
        with connect_disposable(member_writer) as conn:
            with conn.transaction():
                release = d.prepare_diagnostic(
                    conn,
                    schema="public",
                    publication_id=publication,
                    projection=unit.projection_of(bundle),
                    source_frontier_manifest_digest=unit.frontier_digest_of(bundle),
                )
            with conn.transaction():
                d.promote_diagnostic(
                    conn, schema="public", release_id=release, expected_release_id=None
                )
            conn.rollback()
            for statement in (
                "INSERT INTO public.bond_default_diagnostic_releases (release_id) VALUES (gen_random_uuid())",
                "UPDATE public.bond_default_diagnostic_pointer SET promoted_by = 'x'",
                "DELETE FROM public.bond_default_diagnostic_revocations",
            ):
                with pytest.raises(errors.InsufficientPrivilege), conn.transaction():
                    conn.execute(statement)
            created_by = conn.execute(
                "SELECT created_by FROM public.bond_default_diagnostic_releases"
            ).fetchone()[0]
            assert (
                created_by == names["writer"]
            )  # session_user, never caller-supplied input
        with connect_disposable(member_auditor) as conn:
            assert d.verify_diagnostic(
                conn, schema="public", release_id=release
            ) == unit.projection_of(bundle)
            assert (
                conn.execute(
                    "SELECT count(*) FROM public.bond_default_diagnostic_pointer"
                ).fetchone()[0]
                == 1
            )
            conn.rollback()
            for call in (
                lambda: d.promote_diagnostic(
                    conn,
                    schema="public",
                    release_id=release,
                    expected_release_id=release,
                ),
                lambda: d.revoke_diagnostic(
                    conn, schema="public", release_id=release, reason_code="not_allowed"
                ),
                lambda: d.prepare_diagnostic(
                    conn,
                    schema="public",
                    publication_id=publication,
                    projection=unit.projection_of(bundle),
                    source_frontier_manifest_digest=unit.frontier_digest_of(bundle),
                ),
                lambda: d.read_current_diagnostic(conn, schema="public"),
            ):
                with pytest.raises(errors.InsufficientPrivilege), conn.transaction():
                    call()
    finally:
        with clean.admin(autocommit=True) as admin:
            for role in names.values():
                admin.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_public_and_temp_schema_shadowing_cannot_redirect_the_definer_reader(clean):
    release = diagnostic_release(clean, unit.coverage_only_bundle())
    promote(clean, release)
    served = read_current(clean)
    forged = uuid.uuid4()
    with clean.app(autocommit=True) as conn:
        conn.execute(
            "CREATE TEMP TABLE bond_default_diagnostic_pointer (product text, release_id uuid, promoted_at timestamptz, promoted_by text)"
        )
        conn.execute(
            "CREATE TEMP TABLE bond_default_diagnostic_releases (release_id uuid, display_projection jsonb)"
        )
        conn.execute("CREATE TEMP TABLE bond_credit_publications (publication_id uuid)")
        conn.execute(
            "INSERT INTO pg_temp.bond_default_diagnostic_pointer VALUES ('bond_default_events_diagnostic_v1', %s, now(), 'x')",
            [forged],
        )
        conn.execute(
            "CREATE FUNCTION pg_temp.bond_credit_is_revoked(uuid) RETURNS boolean LANGUAGE sql AS 'SELECT true'"
        )
        conn.execute(
            "CREATE FUNCTION pg_temp.bond_credit_ts_text(timestamptz) RETURNS text LANGUAGE sql AS $$ SELECT 'forged' $$"
        )
        conn.execute(
            "CREATE FUNCTION pg_temp.bond_default_diag_guard(uuid, boolean) RETURNS jsonb LANGUAGE sql AS $$ SELECT '{}'::jsonb $$"
        )
        for path in (
            "pg_temp, public",
            "pg_temp",
            "public, pg_temp",
            "pg_catalog, pg_temp",
        ):
            conn.execute(sql.SQL("SET search_path = {}").format(sql.SQL(path)))
            assert d.read_current_diagnostic(conn, schema="public") == served, path
        conn.execute("SET search_path = pg_temp")
        assert d.read_current_diagnostic(conn, schema="public") == served
    with clean.app(
        autocommit=True
    ) as conn:  # nothing can be created in public by the runtime role
        with pytest.raises(errors.InsufficientPrivilege):
            conn.execute(
                "CREATE FUNCTION public.bond_credit_is_revoked(uuid) RETURNS boolean LANGUAGE sql AS 'SELECT true'"
            )
        with pytest.raises(errors.InsufficientPrivilege):
            conn.execute(
                "CREATE TABLE public.bond_default_diagnostic_pointer_shadow (x integer)"
            )
    with (
        clean.writer(autocommit=True) as conn
    ):  # the trusted owner's temp objects cannot redirect the writer path either
        conn.execute(
            "CREATE FUNCTION pg_temp.bond_credit_is_revoked(uuid) RETURNS boolean LANGUAGE sql AS 'SELECT true'"
        )
        conn.execute("SET search_path = pg_temp, public")
        assert (
            d.verify_diagnostic(
                conn, schema="public", release_id=release
            ).counts.accepted_events
            == 0
        )


# ---------------------------------------------------------------------------
# Election: CAS, races, non-regression
# ---------------------------------------------------------------------------
def _wait_for_advisory_waiter(env, timeout: float = 15.0) -> None:
    with env.admin(autocommit=True) as admin:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if admin.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
                "AND wait_event = 'advisory' AND datname = current_database()"
            ).fetchone()[0]:
                return
            time.sleep(0.05)
    raise AssertionError("second session never waited for the pointer lock")


def _race(
    env,
    first: tuple[uuid.UUID, uuid.UUID | None],
    second: tuple[uuid.UUID, uuid.UUID | None],
):
    """First session holds the pointer lock in an open transaction; second must wait, then lose the CAS."""
    outcome: dict[str, object] = {}

    def loser():
        with env.writer() as conn:
            try:
                with conn.transaction():
                    d.promote_diagnostic(
                        conn,
                        schema="public",
                        release_id=second[0],
                        expected_release_id=second[1],
                    )
                outcome["second"] = "ok"
            except d.DiagnosticError as exc:
                outcome["second"] = exc.reason

    with env.writer() as conn:
        with conn.transaction():
            d.promote_diagnostic(
                conn, schema="public", release_id=first[0], expected_release_id=first[1]
            )
            thread = threading.Thread(target=loser)
            thread.start()
            _wait_for_advisory_waiter(env)
            assert (
                "second" not in outcome
            )  # blocked on the lock taken BEFORE any lookup
        thread.join(30)
    return outcome


def test_cas_from_an_absent_pointer_serializes_two_sessions(clean):
    r1 = diagnostic_release(clean, unit.coverage_only_bundle(label="r1"))
    r2 = diagnostic_release(clean, unit.coverage_only_bundle(label="r2"))
    assert _race(clean, (r1, None), (r2, None)) == {"second": "cas_mismatch"}
    assert read_current(clean)["release_id"] == str(r1)
    with clean.admin() as admin:
        assert count_of(admin, "bond_default_diagnostic_pointer") == 1


def test_cas_from_an_existing_pointer_serializes_two_sessions(clean):
    base = diagnostic_release(clean, unit.coverage_only_bundle(label="base"))
    r2 = diagnostic_release(
        clean,
        unit.coverage_only_bundle(
            label="r2", knowledge_cutoff=K0 + dt.timedelta(days=1)
        ),
    )
    r3 = diagnostic_release(
        clean,
        unit.coverage_only_bundle(
            label="r3", knowledge_cutoff=K0 + dt.timedelta(days=2)
        ),
    )
    promote(clean, base)
    assert _race(clean, (r2, base), (r3, base)) == {"second": "cas_mismatch"}
    assert read_current(clean)["release_id"] == str(r2)
    promote(clean, r3, expected=r2)  # the loser retries with the fresh pointer
    assert read_current(clean)["release_id"] == str(r3)


def test_cas_rejects_wrong_expectations_and_identical_promotion_is_a_noop(clean):
    r1 = diagnostic_release(clean, unit.coverage_only_bundle(label="r1"))
    r2 = diagnostic_release(
        clean,
        unit.coverage_only_bundle(
            label="r2", knowledge_cutoff=K0 + dt.timedelta(days=1)
        ),
    )
    with pytest.raises(d.DiagnosticError) as info:
        promote(clean, r1, expected=r2)  # pointer is absent, a release was expected
    assert info.value.reason == "cas_mismatch"
    promote(clean, r1)
    with clean.admin() as admin:
        first = admin.execute(
            "SELECT promoted_at, promoted_by FROM bond_default_diagnostic_pointer"
        ).fetchone()
    for expected in (None, r2):
        with pytest.raises(d.DiagnosticError) as info:
            promote(clean, r2, expected=expected)
        assert info.value.reason == "cas_mismatch"
    time.sleep(0.02)
    promote(clean, r1, expected=r1)  # identical retry with the matching expectation
    with clean.admin() as admin:
        assert (
            admin.execute(
                "SELECT promoted_at, promoted_by FROM bond_default_diagnostic_pointer"
            ).fetchone()
            == first
        )
    promote(clean, r2, expected=r1)
    with clean.admin() as admin:
        assert (
            admin.execute(
                "SELECT promoted_at FROM bond_default_diagnostic_pointer"
            ).fetchone()[0]
            > first[0]
        )
    assert read_current(clean)["release_id"] == str(r2)


def test_target_month_and_knowledge_cutoff_may_not_regress_separately(clean):
    base = diagnostic_release(clean, unit.coverage_only_bundle(label="base"))
    promote(clean, base)
    later = K0 + dt.timedelta(days=3)
    earlier_month = dt.date(2026, 7, 1)
    later_month = dt.date(2026, 9, 1)
    t_lower_k_higher = diagnostic_release(
        clean,
        unit.coverage_only_bundle(
            label="t_lower", target_month=earlier_month, knowledge_cutoff=later
        ),
    )
    t_higher_k_lower = diagnostic_release(
        clean,
        unit.coverage_only_bundle(
            label="k_lower",
            target_month=later_month,
            knowledge_cutoff=K0 - dt.timedelta(days=1),
            records=unit.frontier_records(observed_at=K0 - dt.timedelta(days=2)),
        ),
    )
    both_advance = diagnostic_release(
        clean,
        unit.coverage_only_bundle(
            label="advance", target_month=later_month, knowledge_cutoff=later
        ),
    )
    with pytest.raises(d.DiagnosticError) as info:
        promote(clean, t_lower_k_higher, expected=base)
    assert info.value.reason == "t_regression"
    with pytest.raises(d.DiagnosticError) as info:
        promote(clean, t_higher_k_lower, expected=base)
    assert info.value.reason == "k_regression"
    assert read_current(clean)["release_id"] == str(base)
    promote(clean, both_advance, expected=base)
    served = read_current(clean)
    assert (served["target_month"], served["knowledge_cutoff"]) == (
        "2026-09-01",
        c.ts_text(later),
    )
    same_tk = diagnostic_release(
        clean,
        unit.coverage_only_bundle(
            label="same_tk", target_month=later_month, knowledge_cutoff=later
        ),
    )
    promote(clean, same_tk, expected=both_advance)  # equal T and K is not a regression
    with pytest.raises(d.DiagnosticError) as info:
        promote(clean, base, expected=same_tk)
    assert info.value.reason == "t_regression"


def test_the_qualified_pointer_and_the_diagnostic_pointer_are_independent(clean):
    qualified = guard.syn.build_bundle()  # synthetic qualified, complete bundle
    publication = publish(clean, qualified)
    with clean.writer() as conn:
        store = p.PostgresPublicationStore(conn, "public")
        assert (
            p.promote_bundle(store, publication, expected_pointer=None) == publication
        )
    diagnostic = diagnostic_release(clean, unit.coverage_only_bundle())
    promote(clean, diagnostic)
    with clean.admin() as admin:
        assert (
            admin.execute(
                "SELECT publication_id FROM bond_credit_current_pointer"
            ).fetchone()[0]
            == publication
        )
        assert (
            admin.execute(
                "SELECT release_id FROM bond_default_diagnostic_pointer"
            ).fetchone()[0]
            == diagnostic
        )
    assert read_current(clean)["publication_id"] != str(publication)
    # A diagnostic (partial / limited) build can never be promoted through the qualified path.
    with clean.writer() as conn:
        store = p.PostgresPublicationStore(conn, "public")
        with pytest.raises(p.PublicationError, match="not_qualified_complete"):
            p.promote_bundle(
                store,
                unit.coverage_only_bundle().publication_id,
                expected_pointer=publication,
            )
    # ... and a qualified build never becomes a diagnostic release (tier confusion).
    with (
        clean.writer() as conn,
        pytest.raises(d.DiagnosticError) as info,
        conn.transaction(),
    ):
        d.prepare_diagnostic(
            conn,
            schema="public",
            publication_id=publication,
            projection=unit.projection_of(unit.coverage_only_bundle()),
            source_frontier_manifest_digest=unit.frontier_digest_of(
                unit.coverage_only_bundle()
            ),
        )
    assert info.value.reason == "publication_not_partial_limited"


# ---------------------------------------------------------------------------
# Revocation
# ---------------------------------------------------------------------------
def test_revoking_the_release_is_refused_on_the_next_read_and_is_idempotent(clean):
    release = diagnostic_release(clean, unit.coverage_only_bundle())
    promote(clean, release)
    assert read_current(clean)["release_id"] == str(release)
    with clean.writer() as conn, conn.transaction():
        d.revoke_diagnostic(
            conn, schema="public", release_id=release, reason_code="operator_rollback"
        )
        d.revoke_diagnostic(
            conn, schema="public", release_id=release, reason_code="operator_rollback"
        )  # replay
    with pytest.raises(d.DiagnosticError) as info:
        read_current(clean)
    assert (info.value.reason, info.value.sqlstate) == (
        "diagnostic_release_revoked",
        "P0001",
    )
    with clean.writer() as conn:
        with pytest.raises(d.DiagnosticError) as info, conn.transaction():
            d.revoke_diagnostic(
                conn, schema="public", release_id=release, reason_code="other_reason"
            )
        assert info.value.reason == "revocation_conflict"
        for call in (
            lambda: d.verify_diagnostic(conn, schema="public", release_id=release),
            lambda: d.promote_diagnostic(
                conn, schema="public", release_id=release, expected_release_id=release
            ),
        ):
            with pytest.raises(d.DiagnosticError) as info, conn.transaction():
                call()
            assert info.value.reason == "diagnostic_release_revoked"
        with pytest.raises(d.DiagnosticError) as info, conn.transaction():
            d.revoke_diagnostic(
                conn,
                schema="public",
                release_id=uuid.uuid4(),
                reason_code="operator_rollback",
            )
        assert info.value.reason == "unknown_release"
    with (
        clean.admin() as admin
    ):  # append-only audit row, no mutable flag on the release
        row = admin.execute(
            "SELECT reason_code, revoked_by FROM bond_default_diagnostic_revocations"
        ).fetchone()
        assert (
            row == ("operator_rollback", "worker_writer")
            and count_of(admin, "bond_default_diagnostic_releases") == 1
        )


def test_revoking_the_underlying_build_is_refused_on_the_next_read_and_blocks_new_releases(
    clean,
):
    bundle = unit.coverage_only_bundle()
    release = diagnostic_release(clean, bundle)
    promote(clean, release)
    with clean.writer() as conn, conn.transaction():
        conn.execute(
            "SELECT public.bond_credit_revoke(%s, 'underlying build withdrawn', %s)",
            [bundle.publication_id, "sha256:" + "a" * 64],
        )
    with pytest.raises(d.DiagnosticError) as info:
        read_current(clean)
    assert (info.value.reason, info.value.sqlstate) == (
        "credit_publication_revoked",
        "P0001",
    )
    with clean.writer() as conn:
        for call in (
            lambda: d.verify_diagnostic(conn, schema="public", release_id=release),
            lambda: d.promote_diagnostic(
                conn, schema="public", release_id=release, expected_release_id=release
            ),
            lambda: d.prepare_diagnostic(
                conn,
                schema="public",
                publication_id=bundle.publication_id,
                projection=unit.projection_of(bundle),
                source_frontier_manifest_digest=unit.frontier_digest_of(bundle),
            ),
        ):
            with pytest.raises(d.DiagnosticError) as info, conn.transaction():
                call()
            assert info.value.reason == "credit_publication_revoked"


def _wait_for_waiter_or_fail(env):
    _wait_for_advisory_waiter(env)


def test_revocation_committing_while_promotion_waits_on_the_lock_refuses_the_promotion(
    clean,
):
    release = diagnostic_release(clean, unit.coverage_only_bundle())
    outcome: dict[str, object] = {}

    def promoter():
        with clean.writer() as conn:
            try:
                with conn.transaction():
                    d.promote_diagnostic(
                        conn,
                        schema="public",
                        release_id=release,
                        expected_release_id=None,
                    )
                outcome["promote"] = "ok"
            except d.DiagnosticError as exc:
                outcome["promote"] = exc.reason

    with clean.writer() as conn:
        with conn.transaction():
            d.revoke_diagnostic(
                conn,
                schema="public",
                release_id=release,
                reason_code="operator_rollback",
            )
            thread = threading.Thread(target=promoter)
            thread.start()
            _wait_for_waiter_or_fail(clean)  # promotion is blocked on the SAME lock
            assert "promote" not in outcome
        thread.join(30)
    assert outcome == {"promote": "diagnostic_release_revoked"}
    with clean.admin() as admin:
        assert count_of(admin, "bond_default_diagnostic_pointer") == 0
    with pytest.raises(d.DiagnosticError, match="diagnostic_not_published"):
        read_current(clean)


def test_promotion_committing_first_lets_a_waiting_revocation_succeed_and_the_next_read_refuses(
    clean,
):
    release = diagnostic_release(clean, unit.coverage_only_bundle())
    outcome: dict[str, object] = {}

    def revoker():
        with clean.writer() as conn:
            try:
                with conn.transaction():
                    d.revoke_diagnostic(
                        conn,
                        schema="public",
                        release_id=release,
                        reason_code="operator_rollback",
                    )
                outcome["revoke"] = "ok"
            except d.DiagnosticError as exc:
                outcome["revoke"] = exc.reason

    with clean.writer() as conn:
        with conn.transaction():
            d.promote_diagnostic(
                conn, schema="public", release_id=release, expected_release_id=None
            )
            thread = threading.Thread(target=revoker)
            thread.start()
            _wait_for_waiter_or_fail(
                clean
            )  # revocation is blocked on the promotion's lock
            assert "revoke" not in outcome
        thread.join(30)
    assert outcome == {"revoke": "ok"}
    with pytest.raises(d.DiagnosticError) as info:
        read_current(clean)
    assert (info.value.reason, info.value.sqlstate) == (
        "diagnostic_release_revoked",
        "P0001",
    )


def test_release_revocation_takes_precedence_and_other_releases_stay_electable(clean):
    r1 = diagnostic_release(clean, unit.coverage_only_bundle(label="r1"))
    r2 = diagnostic_release(
        clean,
        unit.coverage_only_bundle(
            label="r2", knowledge_cutoff=K0 + dt.timedelta(days=1)
        ),
    )
    promote(clean, r1)
    with clean.writer() as conn, conn.transaction():
        d.revoke_diagnostic(
            conn, schema="public", release_id=r1, reason_code="superseded_by_r2"
        )
    promote(
        clean, r2, expected=r1
    )  # replacing a revoked pointer target is allowed (T and K still advance)
    assert read_current(clean)["release_id"] == str(r2)


# ---------------------------------------------------------------------------
# Mixed identifiers, tampering, unsupported input
# ---------------------------------------------------------------------------
def test_mixed_identifiers_are_refused(clean):
    other_records = unit.frontier_records()
    other_records[4] = dict(other_records[4], filing_count=496)
    bundle_a = unit.coverage_only_bundle(label="a")
    bundle_b = unit.coverage_only_bundle(
        label="b", start_counts=(5, 4, 3, 2, 2, 1), records=other_records
    )
    assert unit.projection_of(bundle_a) != unit.projection_of(bundle_b)
    assert unit.frontier_digest_of(bundle_a) != unit.frontier_digest_of(bundle_b)
    release_a = diagnostic_release(clean, bundle_a)
    publication_b = publish(clean, bundle_b)
    with clean.writer() as conn:
        checks = [
            (
                lambda: d.verify_diagnostic(
                    conn, schema="public", release_id=bundle_a.publication_id
                ),
                "unknown_release",
            ),
            (
                lambda: d.promote_diagnostic(
                    conn,
                    schema="public",
                    release_id=bundle_a.publication_id,
                    expected_release_id=None,
                ),
                "unknown_release",
            ),
            (
                lambda: d.prepare_diagnostic(
                    conn,
                    schema="public",
                    publication_id=release_a,
                    projection=unit.projection_of(bundle_a),
                    source_frontier_manifest_digest=unit.frontier_digest_of(bundle_a),
                ),
                "unknown_publication",
            ),
            (
                lambda: d.prepare_diagnostic(
                    conn,
                    schema="public",
                    publication_id=uuid.uuid4(),
                    projection=unit.projection_of(bundle_a),
                    source_frontier_manifest_digest=unit.frontier_digest_of(bundle_a),
                ),
                "unknown_publication",
            ),
            (
                lambda: d.prepare_diagnostic(
                    conn,
                    schema="public",
                    publication_id=publication_b,
                    projection=unit.projection_of(bundle_a),
                    source_frontier_manifest_digest=unit.frontier_digest_of(bundle_b),
                ),
                "projection_mismatch",
            ),
            (
                lambda: d.prepare_diagnostic(
                    conn,
                    schema="public",
                    publication_id=publication_b,
                    projection=unit.projection_of(bundle_b),
                    source_frontier_manifest_digest=unit.frontier_digest_of(bundle_a),
                ),
                "frontier_digest_mismatch",
            ),
            (
                lambda: d.prepare_diagnostic(
                    conn,
                    schema="public",
                    publication_id=publication_b,
                    projection=unit.projection_of(bundle_b),
                    source_frontier_manifest_digest="sha256:" + "0" * 64,
                ),
                "frontier_digest_mismatch",
            ),
        ]
        for call, reason in checks:
            with pytest.raises(d.DiagnosticError) as info, conn.transaction():
                call()
            assert info.value.reason == reason, reason
        with pytest.raises(d.DiagnosticError) as info, conn.transaction():
            d.promote_diagnostic(  # wrong expected id
                conn,
                schema="public",
                release_id=release_a,
                expected_release_id=uuid.uuid4(),
            )
        assert info.value.reason == "cas_mismatch"
    with clean.admin() as admin:
        assert (
            count_of(admin, "bond_default_diagnostic_releases") == 1
            and count_of(admin, "bond_default_diagnostic_pointer") == 0
        )


def _prepare_raw(env, publication, projection_obj, frontier, sql_digest=None):
    with env.writer() as conn:
        try:
            with conn.transaction():
                return d._call(
                    conn,
                    "public",
                    "bond_default_prepare_diagnostic",
                    [
                        publication,
                        psycopg.types.json.Jsonb(projection_obj),
                        frontier,
                        sql_digest or d.DIAGNOSTIC_SQL_DIGEST,
                    ],
                    ["uuid", "jsonb", "text", "text"],
                )
        except d.DiagnosticError as exc:
            return exc


@pytest.mark.parametrize(
    "name,change,reason",
    [
        (
            "extra_key",
            lambda o: o.update(private_id=str(uuid.uuid4())),
            "projection_invalid",
        ),
        ("missing_key", lambda o: o.pop("limitations"), "projection_invalid"),
        (
            "events_accepted",
            lambda o: o.update(accepted_events=[{"cusip9": "ZZ#DIA019"}]),
            "projection_invalid",
        ),
        (
            "version_v2",
            lambda o: o.update(schema_version="bond_default_events_display_v2"),
            "projection_invalid",
        ),
        ("tier_qualified", lambda o: o.update(tier="qualified"), "projection_invalid"),
        (
            "mode_events",
            lambda o: o.update(display_mode="events"),
            "projection_invalid",
        ),
        (
            "recommendation",
            lambda o: o.update(recommendation_eligible=True),
            "projection_invalid",
        ),
        (
            "raw_path_in_cell",
            lambda o: o["coverage"][0].update(period_label="C:\\data\\raw"),
            "projection_invalid",
        ),
        (
            "unix_path",
            lambda o: o["source_frontiers"][0].update(inventory_kind="/etc/passwd"),
            "projection_invalid",
        ),
        (
            "float_count",
            lambda o: o["counts"].update(panel_grid_keys=1.5),
            "projection_invalid",
        ),
        (
            "extra_cell_key",
            lambda o: o["coverage"][0].update(adjudication_id="x"),
            "projection_invalid",
        ),
        (
            "extra_frontier_key",
            lambda o: o["source_frontiers"][0].update(local_path="x"),
            "projection_invalid",
        ),
        (
            "unknown_limitation",
            lambda o: o.update(limitations=["private_note"]),
            "projection_invalid",
        ),
        (
            "candidate_count",
            lambda o: o["counts"].update(
                candidate_issue_months=o["counts"]["candidate_issue_months"] + 1
            ),
            "projection_mismatch",
        ),
        (
            "grid_count",
            lambda o: o["counts"].update(panel_grid_keys=1),
            "projection_mismatch",
        ),
        (
            "cell_exposure",
            lambda o: o["coverage"][0].update(exposed_issue_months=99),
            "projection_mismatch",
        ),
        ("dropped_cell", lambda o: o["coverage"].pop(), "projection_mismatch"),
        (
            "frontier_date",
            lambda o: o["source_frontiers"][0].update(frontier="2026-01-01"),
            "projection_mismatch",
        ),
        ("reordered", lambda o: o["coverage"].reverse(), "projection_mismatch"),
        ("limitation_dropped", lambda o: o["limitations"].pop(), "projection_mismatch"),
    ],
)
def test_projection_tampering_is_rejected_by_the_database(clean, name, change, reason):
    bundle = unit.coverage_only_bundle()
    publication = publish(clean, bundle)
    obj = unit.projection_of(bundle).to_json_obj()
    change(obj)
    result = _prepare_raw(clean, publication, obj, unit.frontier_digest_of(bundle))
    assert isinstance(result, d.DiagnosticError) and result.reason == reason, (
        name,
        result,
    )
    with clean.admin() as admin:
        assert count_of(admin, "bond_default_diagnostic_releases") == 0


def test_digest_arguments_are_validated_and_nonfinite_json_cannot_be_sent(clean):
    bundle = unit.coverage_only_bundle()
    publication = publish(clean, bundle)
    good = unit.projection_of(bundle).to_json_obj()
    frontier = unit.frontier_digest_of(bundle)
    for bad_frontier, bad_sql in (
        ("sha256:abc", None),
        ("plain", None),
        (frontier, "sha256:XYZ"),
        (frontier, "md5:1"),
    ):
        result = _prepare_raw(clean, publication, good, bad_frontier, bad_sql)
        assert (
            isinstance(result, d.DiagnosticError)
            and result.reason == "invalid_argument"
        )
    nan_projection = dict(
        good, counts=dict(good["counts"], panel_grid_keys=float("nan"))
    )
    with (
        clean.writer() as conn,
        pytest.raises((errors.InvalidTextRepresentation, errors.DataError)),
        conn.transaction(),
    ):
        conn.execute(
            "SELECT public.bond_default_prepare_diagnostic(%s, %s::jsonb, %s, %s)",
            [
                publication,
                psycopg.types.json.Jsonb(nan_projection),
                frontier,
                d.DIAGNOSTIC_SQL_DIGEST,
            ],
        )
    with (
        clean.writer() as conn,
        pytest.raises(d.DiagnosticError) as info,
        conn.transaction(),
    ):
        d._call(
            conn,
            "public",
            "bond_default_prepare_diagnostic",
            [None, None, None, None],
            ["uuid", "jsonb", "text", "text"],
        )
    assert info.value.reason == "invalid_argument"
    with clean.admin() as admin:
        assert count_of(admin, "bond_default_diagnostic_releases") == 0


def test_a_publication_that_is_not_validated_cannot_be_released(clean):
    bundle = unit.coverage_only_bundle()
    with clean.writer() as conn:
        p.prepare_bundle(
            p.PostgresPublicationStore(conn, "public"), bundle
        )  # prepared only
    result = _prepare_raw(
        clean,
        bundle.publication_id,
        unit.projection_of(bundle).to_json_obj(),
        unit.frontier_digest_of(bundle),
    )
    assert (
        isinstance(result, d.DiagnosticError)
        and result.reason == "publication_not_validated"
    )


@pytest.mark.parametrize(
    "name,mutate,reason_fragment",
    [
        (
            "cell_state_partial",
            lambda cells: [unit._swap(cells[0], state="partial")] + cells[1:],
            "cell_shape",
        ),
        (
            "exposure_not_panel_count",
            lambda cells: (
                [
                    unit._swap(
                        cells[0],
                        exposed_issue_months=9,
                        denominator_count=9,
                        unknown_outcome_issue_months=9,
                    )
                ]
                + cells[1:]
            ),
            "exposure_not_panel_start_count",
        ),
        (
            "plain_text_rationale",
            lambda cells: (
                [unit._swap(cells[0], rationale="SYNTHETIC coverage")] + cells[1:]
            ),
            "rationale_not_json_object",
        ),
        (
            "period_gap",
            lambda cells: [x for x in cells if x.period_label != "2026-04"],
            "periods_not_contiguous",
        ),
        ("missing_cell", lambda cells: cells[1:], "cells_per_period"),
    ],
)
def test_sql_refuses_validated_builds_that_do_not_fit_the_coverage_only_contract(
    clean, name, mutate, reason_fragment
):
    good = unit.coverage_only_bundle()
    bundle = unit.coverage_only_bundle(mutate_cells=mutate)
    publication = publish(
        clean, bundle
    )  # structurally valid for the ledger, not for the diagnostic contract
    result = _prepare_raw(
        clean,
        publication,
        unit.projection_of(good).to_json_obj(),
        unit.frontier_digest_of(good),
    )
    assert (
        isinstance(result, d.DiagnosticError) and result.reason == "coverage_invalid"
    ), (name, result)
    assert reason_fragment in result.detail, (name, result.detail)
    with pytest.raises(d.DiagnosticError, match=reason_fragment):
        d.derive_projection_from_bundle(
            bundle
        )  # Python refuses the same shape for the same reason


def test_inconsistent_or_late_frontier_records_are_refused_by_sql(clean):
    records = unit.frontier_records()
    inconsistent = unit.coverage_only_bundle(
        records=records + [dict(records[0], frontier="2026-06-29")], label="inc"
    )
    late = unit.coverage_only_bundle(
        records=unit.frontier_records(observed_at=K0 + dt.timedelta(days=1)),
        label="late",
    )
    for bundle, fragment in (
        (inconsistent, "frontier_records_inconsistent"),
        (late, "frontier_observed_after_cutoff"),
    ):
        publication = publish(clean, bundle)
        good = unit.coverage_only_bundle()
        result = _prepare_raw(
            clean,
            publication,
            unit.projection_of(good).to_json_obj(),
            unit.frontier_digest_of(good),
        )
        assert (
            isinstance(result, d.DiagnosticError)
            and result.reason == "coverage_invalid"
            and fragment in result.detail
        )


@pytest.mark.parametrize(
    "name,mutate,fragment",
    [
        (
            "all_all_carries_frontier",
            lambda cells: (
                [unit._swap(cells[0], source_frontier=dt.date(2026, 9, 24))] + cells[1:]
            ),
            "cell_frontier",
        ),
        (
            "source_cell_foreign_record",
            lambda cells: [
                unit._swap(
                    x,
                    rationale=d.coverage_rationale(
                        frontiers=unit.frontier_records(), reason_codes=[]
                    ),
                )
                if (x.source, x.event_type) == ("sec_nport", "default_state")
                else x
                for x in cells
            ],
            "cell_frontier",
        ),
        (
            "source_cell_without_records",
            lambda cells: [
                unit._swap(
                    x, rationale=d.coverage_rationale(frontiers=[], reason_codes=[])
                )
                if x.source == "agency_rocr"
                else x
                for x in cells
            ],
            "cell_frontier",
        ),
        (
            "frontier_not_in_records",
            lambda cells: [
                unit._swap(x, source_frontier=dt.date(2026, 1, 1))
                if x.source == "sec_nport"
                else x
                for x in cells
            ],
            "cell_frontier",
        ),
    ],
)
def test_sql_requires_source_cells_to_carry_their_own_frontier_records(
    clean, name, mutate, fragment
):
    good = unit.coverage_only_bundle()
    publication = publish(clean, unit.coverage_only_bundle(mutate_cells=mutate))
    result = _prepare_raw(
        clean,
        publication,
        unit.projection_of(good).to_json_obj(),
        unit.frontier_digest_of(good),
    )
    assert (
        isinstance(result, d.DiagnosticError) and result.reason == "coverage_invalid"
    ), (name, result)
    assert result.detail.startswith(fragment), (name, result.detail)


def test_sql_requires_a_single_manifest_hash_across_all_frontier_records(clean):
    records = unit.frontier_records()
    records[3] = dict(
        records[3], manifest_sha256=hashlib.sha256(b"another manifest").hexdigest()
    )
    bundle = unit.coverage_only_bundle(records=records)
    publication = publish(clean, bundle)
    good = unit.coverage_only_bundle()
    result = _prepare_raw(
        clean,
        publication,
        unit.projection_of(good).to_json_obj(),
        unit.frontier_digest_of(good),
    )
    assert isinstance(result, d.DiagnosticError) and result.reason == "coverage_invalid"
    assert result.detail == "manifest_sha256_not_single"
    with pytest.raises(d.DiagnosticError, match="manifest_sha256_not_single"):
        d.derive_projection_from_bundle(bundle)


def test_frames_beyond_coverage_are_refused_by_sql(clean):
    """A validated build that carries ratings other than 'missing' (or events) is not a coverage-only build."""
    bundle = guard.syn.build_bundle(
        quality_state="partial", with_receipt=False, with_events=False
    )
    publication = publish(clean, bundle)
    good = unit.coverage_only_bundle()
    result = _prepare_raw(
        clean,
        publication,
        unit.projection_of(good).to_json_obj(),
        unit.frontier_digest_of(good),
    )
    assert isinstance(result, d.DiagnosticError) and result.reason == "coverage_invalid"
    assert result.detail in (
        "frames_not_empty",
        "ratings_not_all_missing",
        "cell_shape:2026/all/all",
    )


def test_immutability_of_releases_revocations_and_the_pointer_even_for_the_owner(clean):
    release = diagnostic_release(clean, unit.coverage_only_bundle())
    promote(clean, release)
    with clean.writer() as conn:
        conn.execute(
            "SELECT public.bond_default_revoke_diagnostic(%s, 'operator_audit')",
            [release],
        )
        conn.commit()
        statements = [
            "UPDATE public.bond_default_diagnostic_releases SET display_projection = '{}'::jsonb",
            "DELETE FROM public.bond_default_diagnostic_releases",
            (
                "TRUNCATE public.bond_default_diagnostic_releases, "
                "public.bond_default_diagnostic_pointer, public.bond_default_diagnostic_revocations"
            ),
            "UPDATE public.bond_default_diagnostic_revocations SET reason_code = 'changed_reason'",
            "DELETE FROM public.bond_default_diagnostic_revocations",
            f"INSERT INTO public.bond_default_diagnostic_pointer (product, release_id) VALUES ('bond_default_events_diagnostic_v1', '{release}') ON CONFLICT (product) DO NOTHING",
            f"UPDATE public.bond_default_diagnostic_pointer SET release_id = '{release}'",
            "DELETE FROM public.bond_default_diagnostic_pointer",
            "UPDATE public.bond_credit_publications SET quality_state = 'qualified'",
            "DELETE FROM public.bond_default_coverage_v1",
        ]
        for statement in statements:
            with pytest.raises(errors.RaiseException), conn.transaction():
                conn.execute(statement)
        conn.execute(
            "SELECT set_config('bond_default_diagnostic.pointer_token', 'forged', false)"
        )
        with pytest.raises(errors.RaiseException), conn.transaction():
            conn.execute(
                f"UPDATE public.bond_default_diagnostic_pointer SET release_id = '{release}'"
            )
    with clean.admin() as admin:
        assert (
            count_of(admin, "bond_default_diagnostic_releases") == 1
            and count_of(admin, "bond_default_diagnostic_pointer") == 1
        )


def test_forged_rows_written_around_the_triggers_are_caught_by_the_next_guarded_read(
    clean,
):
    bundle = unit.coverage_only_bundle()
    release = diagnostic_release(clean, bundle)
    promote(clean, release)
    good = read_current(clean)

    def superuser(statement, params=()):
        with clean.admin(autocommit=True) as admin:
            admin.execute("SET session_replication_role = replica")
            admin.execute(statement, params)

    forgeries = [
        (
            (
                "UPDATE public.bond_default_diagnostic_releases SET display_projection = "
                "jsonb_set(display_projection, '{counts,candidate_issue_months}', '999')"
            ),
            "diagnostic_invalid",
            "projection_digest_mismatch",
        ),
        (
            "UPDATE public.bond_default_diagnostic_releases SET source_frontier_manifest_digest = 'sha256:' || repeat('1', 64)",
            "diagnostic_invalid",
            "release_id_mismatch",
        ),
        (
            "UPDATE public.bond_default_diagnostic_releases SET publication_fingerprint = 'sha256:' || repeat('2', 64)",
            "diagnostic_invalid",
            "release_publication_mismatch",
        ),
        (
            "UPDATE public.bond_credit_publications SET policy_digest = 'sha256:' || repeat('3', 64)",
            "diagnostic_contract_unsupported",
            "publication_pins",
        ),
        (
            "UPDATE public.bond_credit_publications SET quality_state = 'unavailable', build_scope = 'complete'",
            "diagnostic_invalid",
            "publication_not_partial_limited",
        ),
        (
            "UPDATE public.bond_credit_publications SET lifecycle_state = 'prepared', validated_at = NULL",
            "diagnostic_invalid",
            "publication_not_validated",
        ),
    ]
    with clean.admin() as admin:
        stored = admin.execute(
            "SELECT display_projection FROM public.bond_default_diagnostic_releases"
        ).fetchone()[0]
        publication_row = admin.execute(
            "SELECT policy_digest, quality_state, build_scope, lifecycle_state, validated_at "
            "FROM public.bond_credit_publications"
        ).fetchone()
        release_row = admin.execute(
            "SELECT publication_fingerprint, projection_digest, source_frontier_manifest_digest "
            "FROM public.bond_default_diagnostic_releases"
        ).fetchone()
    for forge, reason, detail in forgeries:
        superuser(forge)
        try:
            with pytest.raises(d.DiagnosticError) as info:
                read_current(clean)
            assert (info.value.reason, info.value.detail) == (reason, detail), forge
        finally:
            superuser(
                "UPDATE public.bond_default_diagnostic_releases SET display_projection = %s::jsonb, "
                "publication_fingerprint = %s, projection_digest = %s, source_frontier_manifest_digest = %s",
                [
                    psycopg.types.json.Jsonb(stored),
                    release_row[0],
                    release_row[1],
                    release_row[2],
                ],
            )
            superuser(
                "UPDATE public.bond_credit_publications SET policy_digest = %s, quality_state = %s, "
                "build_scope = %s, lifecycle_state = %s, validated_at = %s",
                list(publication_row),
            )
        assert read_current(clean) == good
    # Tampering with the persisted frames is caught by the deep (writer / auditor) verification.
    superuser(
        "UPDATE public.bond_default_coverage_v1 SET exposed_issue_months = exposed_issue_months + 1 "
        "WHERE period_label = '2026-05' AND source = 'all'"
    )
    with (
        clean.writer() as conn,
        pytest.raises(d.DiagnosticError) as info,
        conn.transaction(),
    ):
        d.verify_diagnostic(conn, schema="public", release_id=release)
    assert info.value.reason == "coverage_invalid"
    superuser(
        "UPDATE public.bond_default_coverage_v1 SET exposed_issue_months = exposed_issue_months - 1 "
        "WHERE period_label = '2026-05' AND source = 'all'"
    )
    with clean.writer() as conn, conn.transaction():
        assert d.verify_diagnostic(
            conn, schema="public", release_id=release
        ) == unit.projection_of(bundle)


# ---------------------------------------------------------------------------
# Schema-owned installation marker: the SQL pin is verified inside the database
# ---------------------------------------------------------------------------
def _marker_rows(env):
    with env.admin() as admin:
        return admin.execute(
            "SELECT sql_digest FROM public.bond_default_diagnostic_installations "
            "ORDER BY installed_at, installation_id"
        ).fetchall()


def test_installation_marker_is_recorded_once_and_reruns_are_idempotent(env):
    before = _marker_rows(env)
    assert before and before[-1] == (d.DIAGNOSTIC_SQL_DIGEST,)
    with env.writer(autocommit=True) as conn:
        for _ in range(3):
            d.install_diagnostic_schema(conn, schema="public")
        assert d._record_installation(conn, "public") is False
    assert _marker_rows(env) == before
    with env.admin() as admin:
        assert (
            admin.execute(
                "SELECT public.bond_default_diag_installed_sql_digest()"
            ).fetchone()[0]
            == d.DIAGNOSTIC_SQL_DIGEST
        )
        row = admin.execute(
            "SELECT installed_by FROM public.bond_default_diagnostic_installations LIMIT 1"
        ).fetchone()
        assert row == ("worker_writer",)


def test_installation_marker_is_append_only_and_has_no_dml_grants(env):
    with env.writer(autocommit=True) as conn:
        for statement in (
            "UPDATE public.bond_default_diagnostic_installations SET sql_digest = 'sha256:' || repeat('1', 64)",
            "DELETE FROM public.bond_default_diagnostic_installations",
            "TRUNCATE public.bond_default_diagnostic_installations",
        ):
            with pytest.raises(errors.RaiseException):
                conn.execute(statement)
    with env.app(autocommit=True) as conn:
        for statement in (
            "SELECT count(*) FROM public.bond_default_diagnostic_installations",
            "INSERT INTO public.bond_default_diagnostic_installations (sql_digest) VALUES ('sha256:' || repeat('1', 64))",
            "SELECT public.bond_default_diag_installed_sql_digest()",
        ):
            with pytest.raises(errors.InsufficientPrivilege):
                conn.execute(statement)
    with env.admin() as admin:
        assert admin.execute(
            "SELECT has_table_privilege('bond_credit_writer', 'public.bond_default_diagnostic_installations', 'SELECT'), "
            "has_table_privilege('bond_credit_auditor', 'public.bond_default_diagnostic_installations', 'SELECT'), "
            "has_table_privilege('bond_credit_writer', 'public.bond_default_diagnostic_installations', 'INSERT'), "
            "has_table_privilege('bond_credit_reader', 'public.bond_default_diagnostic_installations', 'SELECT'), "
            "has_table_privilege('app_runtime', 'public.bond_default_diagnostic_installations', 'SELECT'), "
            "has_table_privilege('app_analytics_ro', 'public.bond_default_diagnostic_installations', 'SELECT')"
        ).fetchone() == (True, True, False, False, False, False)
    with env.writer(autocommit=True) as conn:
        report = d.verify_installed_privileges(
            conn, schema="public", require_empty_pointers=False
        )
        assert report["checks"]["installation_marker_current"]["ok"] is True
        assert report["checks"]["diagnostic_tables_select_only"]["ok"] is True
        assert report["checks"]["contract_pins_match"]["ok"] is True
        assert d.harden_installed_privileges(conn, schema="public")["revoked"] == []


def test_prepare_refuses_a_digest_that_is_not_the_installed_pin(clean):
    bundle = unit.coverage_only_bundle()
    publication = publish(clean, bundle)
    other = "sha256:" + "1" * 64
    result = _prepare_raw(
        clean,
        publication,
        unit.projection_of(bundle).to_json_obj(),
        unit.frontier_digest_of(bundle),
        sql_digest=other,
    )
    assert isinstance(result, d.DiagnosticError)
    assert (result.reason, result.sqlstate) == ("diagnostic_sql_pin_mismatch", "P0001")
    with clean.admin() as admin:
        assert count_of(admin, "bond_default_diagnostic_releases") == 0


def test_stale_install_and_new_worker_mismatch_is_refused_at_prepare_verify_promote_and_read(
    clean,
):
    bundle = unit.coverage_only_bundle()
    release = diagnostic_release(clean, bundle)
    promote(clean, release)
    served = read_current(clean)
    stale = "sha256:" + "2" * 64
    with (
        clean.writer() as conn
    ):  # a different pinned file installed later becomes the marker
        conn.execute(
            "INSERT INTO public.bond_default_diagnostic_installations (sql_digest) VALUES (%s)",
            [stale],
        )
        conn.commit()
    try:
        with pytest.raises(d.DiagnosticError) as info:
            read_current(clean)  # the runtime guard
        assert (info.value.reason, info.value.sqlstate) == (
            "diagnostic_sql_pin_mismatch",
            "P0001",
        )
        with clean.writer() as conn:
            for call in (
                lambda: d.verify_diagnostic(conn, schema="public", release_id=release),
                lambda: d.promote_diagnostic(
                    conn,
                    schema="public",
                    release_id=release,
                    expected_release_id=release,
                ),
                lambda: d.prepare_diagnostic(
                    conn,
                    schema="public",
                    publication_id=bundle.publication_id,
                    projection=unit.projection_of(bundle),
                    source_frontier_manifest_digest=unit.frontier_digest_of(bundle),
                ),
            ):
                with pytest.raises(d.DiagnosticError) as info, conn.transaction():
                    call()
                assert info.value.reason == "diagnostic_sql_pin_mismatch"
        with clean.writer(autocommit=True) as conn:
            report_error = None
            try:
                d.verify_installed_privileges(
                    conn, schema="public", require_empty_pointers=False
                )
            except d.DiagnosticError as exc:
                report_error = exc
            assert report_error is not None
            assert not report_error.report["checks"]["installation_marker_current"][
                "ok"
            ]
    finally:
        with clean.writer(autocommit=True) as conn:
            d.install_diagnostic_schema(
                conn, schema="public"
            )  # re-records the pinned digest as current
    assert read_current(clean) == served
    assert _marker_rows(clean)[-2:] == [(stale,), (d.DIAGNOSTIC_SQL_DIGEST,)]
    with clean.writer(autocommit=True) as conn:
        assert (
            d.verify_installed_privileges(
                conn, schema="public", require_empty_pointers=False
            )["ok"]
            is True
        )


def test_a_database_without_any_marker_refuses_every_release(clean):
    bundle = unit.coverage_only_bundle()
    release = diagnostic_release(clean, bundle)
    promote(clean, release)
    with clean.admin(autocommit=True) as admin:
        admin.execute("SET session_replication_role = replica")
        admin.execute(
            "CREATE TEMP TABLE marker_backup AS SELECT * FROM public.bond_default_diagnostic_installations"
        )
        admin.execute("DELETE FROM public.bond_default_diagnostic_installations")
        try:
            with pytest.raises(d.DiagnosticError) as info:
                read_current(clean)
            assert info.value.reason == "diagnostic_sql_pin_mismatch"
            with pytest.raises(d.DiagnosticError) as info:
                d.verify_installed_privileges(
                    admin, schema="public", require_empty_pointers=False
                )
            assert (
                info.value.report["checks"]["installation_marker_current"]["detail"][
                    "recorded"
                ]
                == "none"
            )
        finally:
            admin.execute(
                "INSERT INTO public.bond_default_diagnostic_installations SELECT * FROM marker_backup"
            )
    assert read_current(clean)["release_id"] == str(release)

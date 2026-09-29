"""Disposable-PostgreSQL contract suite for the bond-credit publication ledger.

Runs only against an explicit ``BOND_DEFAULT_TEST_DATABASE_URL`` on a loopback address
whose database name matches ``^[a-z0-9_]*disposable[a-z0-9_]*$``. It never reads ``.env``
or ``DATABASE_URL``; any Railway/``*.railway.internal``/proxy host is refused. The DSN is
parsed and validated once; every connection is then opened only from explicit sanitized
keyword parameters (including a loopback ``hostaddr``) while every ``PG*`` libpq
environment variable is removed, so nothing unspecified can be inherited from the
environment. Without the variable the DB tests are skipped (the DSN guard tests always
run) — a skipped DB suite is not acceptance evidence.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import importlib.util
import json
import os
import re
import secrets
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from src.bonds.default_events import contracts as c
from src.bonds.default_events import publication as p

psycopg = pytest.importorskip("psycopg")
from psycopg import errors, sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb

ENV_VAR = "BOND_DEFAULT_TEST_DATABASE_URL"
# Accepted ``host`` values and the loopback ``hostaddr`` each one pins when none is given.
ALLOWED_HOSTS = {"127.0.0.1": "127.0.0.1", "localhost": "127.0.0.1", "::1": "::1"}
LOOPBACK_ADDRS = frozenset({"127.0.0.1", "::1"})
ALLOWED_DSN_KEYS = frozenset({"host", "hostaddr", "port", "dbname", "user", "password"})
DBNAME_RE = re.compile(r"^[a-z0-9_]*disposable[a-z0-9_]*$")
USER_RE = re.compile(r"^[A-Za-z0-9_]+$")
SEARCH_PATH_OPTION_RE = re.compile(r"^-csearch_path=[a-z0-9_]+$")
FORBIDDEN_FRAGMENTS = ("railway", "rlwy", "proxy")
CONNECT_KWARGS = frozenset({"autocommit", "connect_timeout"})
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "bond_default_events"
UTC = dt.timezone.utc
_LIBPQ_ENV_LOCK = threading.Lock()


class UnsafeTestDsn(ValueError):
    """The DSN is not a disposable local database."""


@dataclass(frozen=True)
class DisposableTarget:
    """A validated loopback disposable database; the only thing ``connect_disposable`` accepts."""

    host: str
    hostaddr: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)

    def params(self) -> dict[str, str]:
        return {"host": self.host, "hostaddr": self.hostaddr, "port": str(self.port),
                "dbname": self.dbname, "user": self.user, "password": self.password}

    def derive(self, **changes: str) -> DisposableTarget:
        """Re-validated copy with some parameters replaced (e.g. another disposable dbname)."""
        return _validate_params({**self.params(), **changes})


def _validate_params(params: Mapping[str, str | None]) -> DisposableTarget:
    unexpected = sorted(key for key, value in params.items() if value is not None and key not in ALLOWED_DSN_KEYS)
    if unexpected:
        raise UnsafeTestDsn(f"DSN parameters not allowed: {', '.join(unexpected)}")
    values = {key: str(params.get(key) or "") for key in ALLOWED_DSN_KEYS}
    for key in ("host", "hostaddr", "dbname"):
        if any(fragment in values[key].lower() for fragment in FORBIDDEN_FRAGMENTS):
            raise UnsafeTestDsn("railway/proxy hosts are refused")
    for key in ("host", "hostaddr", "port"):
        if "," in values[key]:
            raise UnsafeTestDsn("multi-host DSNs are refused")
    host = values["host"]
    if host not in ALLOWED_HOSTS:
        raise UnsafeTestDsn(f"host {host!r} is not local")
    hostaddr = values["hostaddr"] or ALLOWED_HOSTS[host]
    if hostaddr not in LOOPBACK_ADDRS:
        raise UnsafeTestDsn("hostaddr is not a loopback address")
    port_text = values["port"] or "5432"
    if not port_text.isdigit() or not 1 <= int(port_text) <= 65535:
        raise UnsafeTestDsn("port must be a single explicit TCP port")
    if not DBNAME_RE.fullmatch(values["dbname"]):
        raise UnsafeTestDsn("database name must match ^[a-z0-9_]*disposable[a-z0-9_]*$")
    if not USER_RE.fullmatch(values["user"]):
        raise UnsafeTestDsn("an explicit user is required")
    if not values["password"]:
        raise UnsafeTestDsn("an explicit password is required")
    return DisposableTarget(host=host, hostaddr=hostaddr, port=int(port_text), dbname=values["dbname"],
                            user=values["user"], password=values["password"])


def check_test_dsn(dsn: str | None) -> DisposableTarget:
    """Accept only an explicit local disposable database; never an ambient DSN."""
    if not dsn:
        raise UnsafeTestDsn(f"{ENV_VAR} is absent")
    try:
        params = conninfo_to_dict(dsn)  # pure parse: libpq defaults/environment are not applied
    except psycopg.ProgrammingError as exc:
        raise UnsafeTestDsn("unparseable DSN") from exc
    return _validate_params(params)


@contextlib.contextmanager
def _libpq_environment_removed() -> Iterator[None]:
    """Remove every ``PG*`` variable (PGHOST, PGHOSTADDR, PGSERVICE, PGOPTIONS, ...) while connecting."""
    with _LIBPQ_ENV_LOCK:
        saved = {key: value for key, value in os.environ.items() if key.upper().startswith("PG")}
        for key in saved:
            del os.environ[key]
        try:
            yield
        finally:
            os.environ.update(saved)


def connect_disposable(target: DisposableTarget, *, options: str | None = None, **kwargs):
    """Open a connection only from explicit sanitized keyword parameters of a validated target."""
    if not isinstance(target, DisposableTarget):
        raise UnsafeTestDsn("connections require a validated DisposableTarget, not a raw DSN")
    unexpected = set(kwargs) - CONNECT_KWARGS
    if unexpected:
        raise UnsafeTestDsn(f"connect parameters not allowed: {', '.join(sorted(unexpected))}")
    params: dict[str, object] = {**target.params(), "sslmode": "disable",
                                 "connect_timeout": kwargs.pop("connect_timeout", 10)}
    if options is not None:
        if not SEARCH_PATH_OPTION_RE.fullmatch(options):
            raise UnsafeTestDsn("only a search_path option is allowed")
        params["options"] = options
    with _libpq_environment_removed():
        return psycopg.connect("", **params, **kwargs)


def _load_synthetic():
    spec = importlib.util.spec_from_file_location("bond_default_synthetic", FIXTURES / "synthetic.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


syn = _load_synthetic()
K0 = syn.KNOWLEDGE_CUTOFF


# ---------------------------------------------------------------------------
# DSN guard (always runs)
# ---------------------------------------------------------------------------
SAFE_RAW = "host=127.0.0.1 port=55433 dbname=bdefault_disposable user=u password=pw"
HOSTILE_LIBPQ_ENV = {
    "PGHOST": "db.railway.internal", "PGHOSTADDR": "203.0.113.42", "PGPORT": "6543",
    "PGDATABASE": "railway", "PGUSER": "postgres", "PGPASSWORD": "prod-secret", "PGSERVICE": "prod",
    "PGSERVICEFILE": "C:/prod/pg_service.conf", "PGSYSCONFDIR": "C:/prod", "PGPASSFILE": "C:/prod/pgpass",
    "PGOPTIONS": "-csearch_path=public", "PGSSLMODE": "require", "PGAPPNAME": "inherited",
    "PGTARGETSESSIONATTRS": "read-write",
}
UNSAFE_DSNS = [
    None,
    "",
    "host=db.railway.internal port=5432 dbname=bdefault_disposable user=u password=pw",
    "postgresql://u:p@monorail.proxy.rlwy.net:12345/bdefault_disposable",
    "host=10.0.0.5 dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 dbname=postgres user=u password=pw",
    "dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 hostaddr=10.1.1.1 dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 hostaddr=203.0.113.42 dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 service=prod dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 servicefile=/etc/pg_service.conf dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 passfile=/tmp/pgpass dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 options=-csearch_path=public dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 sslmode=require dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1,10.0.0.5 dbname=bdefault_disposable user=u password=pw",
    "host=127.0.0.1 port=5432,6543 dbname=bdefault_disposable user=u password=pw",
    "postgresql://u:p@127.0.0.1:5432/bdefault_disposable?hostaddr=203.0.113.42",
    "postgresql://u:p@127.0.0.1:5432/bdefault_disposable?host=db.railway.internal",
    "postgresql://u:p@127.0.0.1:5432/bdefault_disposable?service=prod",
    "postgresql://u:p@127.0.0.1:5432/bdefault_disposable?options=-cdefault_transaction_read_only%3Doff",
    "host=localhost dbname=railway_disposable user=u password=pw",
    "host=127.0.0.1 dbname=Bdefault_Disposable user=u password=pw",
    "host=127.0.0.1 dbname=bdefault-disposable user=u password=pw",
    "host=127.0.0.1 dbname=bdefault_disposable password=pw",
    "host=127.0.0.1 dbname=bdefault_disposable user=u",
    "host=127.0.0.1 port=0 dbname=bdefault_disposable user=u password=pw",
]


@pytest.mark.parametrize("dsn", UNSAFE_DSNS)
def test_dsn_guard_refuses_unsafe_targets(dsn):
    with pytest.raises(UnsafeTestDsn):
        check_test_dsn(dsn)


def test_dsn_guard_accepts_local_disposable_and_ignores_ambient_env(monkeypatch):
    for key, value in HOSTILE_LIBPQ_ENV.items():
        monkeypatch.setenv(key, value)
    target = check_test_dsn(SAFE_RAW)
    assert (target.host, target.hostaddr, target.port, target.dbname, target.user) == (
        "127.0.0.1", "127.0.0.1", 55433, "bdefault_disposable", "u")
    uri = check_test_dsn("postgresql://u:p@localhost/x_disposable_y")
    assert (uri.hostaddr, uri.port, uri.dbname) == ("127.0.0.1", 5432, "x_disposable_y")  # not PGPORT/PGDATABASE
    assert check_test_dsn("host=::1 dbname=bdefault_disposable user=u password=pw").hostaddr == "::1"
    assert "pw" not in repr(target)
    monkeypatch.delenv(ENV_VAR, raising=False)
    monkeypatch.setenv("DATABASE_URL", SAFE_RAW)
    with pytest.raises(UnsafeTestDsn, match="absent"):
        check_test_dsn(os.environ.get(ENV_VAR))


def test_derived_targets_are_revalidated():
    target = check_test_dsn(SAFE_RAW)
    assert target.derive(dbname="bdefault_disposable_empty_1").dbname == "bdefault_disposable_empty_1"
    for changes in ({"dbname": "postgres"}, {"host": "db.railway.internal"}, {"hostaddr": "203.0.113.42"},
                    {"service": "prod"}, {"options": "-cfoo=bar"}, {"user": ""}):
        with pytest.raises(UnsafeTestDsn):
            target.derive(**changes)


def test_connect_uses_only_explicit_sanitized_parameters(monkeypatch):
    for key, value in HOSTILE_LIBPQ_ENV.items():
        monkeypatch.setenv(key, value)
    calls = []

    def fake_connect(*args, **kwargs):
        calls.append((args, kwargs, {k: v for k, v in os.environ.items() if k.upper().startswith("PG")}))
        return "connection"

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    target = check_test_dsn(SAFE_RAW)
    assert connect_disposable(target, options="-csearch_path=bdt_x", autocommit=True) == "connection"
    assert connect_disposable(target.derive(user="bdt_reader_1", password="rpw")) == "connection"
    assert calls[0][0] == ("",)
    assert calls[0][1] == {"host": "127.0.0.1", "hostaddr": "127.0.0.1", "port": "55433",
                           "dbname": "bdefault_disposable", "user": "u", "password": "pw", "sslmode": "disable",
                           "connect_timeout": 10, "options": "-csearch_path=bdt_x", "autocommit": True}
    assert calls[1][1] == {"host": "127.0.0.1", "hostaddr": "127.0.0.1", "port": "55433",
                           "dbname": "bdefault_disposable", "user": "bdt_reader_1", "password": "rpw",
                           "sslmode": "disable", "connect_timeout": 10}
    assert [env for (_args, _kwargs, env) in calls] == [{}, {}]  # no PG* variable visible to libpq
    assert {key: os.environ[key] for key in HOSTILE_LIBPQ_ENV} == HOSTILE_LIBPQ_ENV  # restored afterwards


@pytest.mark.parametrize("dsn", UNSAFE_DSNS)
def test_unsafe_dsn_never_attempts_a_connection(monkeypatch, dsn):
    for key, value in HOSTILE_LIBPQ_ENV.items():
        monkeypatch.setenv(key, value)

    def forbidden_connect(*args, **kwargs):
        raise AssertionError("a connection was attempted")

    monkeypatch.setattr(psycopg, "connect", forbidden_connect)
    with pytest.raises(UnsafeTestDsn):
        connect_disposable(check_test_dsn(dsn))


@pytest.mark.parametrize(("target", "kwargs"), [
    (SAFE_RAW, {}),
    (None, {}),
    ("valid", {"options": "-csearch_path=public -cfoo=bar"}),
    ("valid", {"options": "-cdefault_transaction_read_only=off"}),
    ("valid", {"hostaddr": "203.0.113.42"}),
    ("valid", {"service": "prod"}),
])
def test_connect_refuses_raw_dsns_and_extra_parameters(monkeypatch, target, kwargs):
    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: pytest.fail("a connection was attempted"))
    resolved = check_test_dsn(SAFE_RAW) if target == "valid" else target
    with pytest.raises(UnsafeTestDsn):
        connect_disposable(resolved, **kwargs)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def dsn():
    raw = os.environ.get(ENV_VAR)
    if raw is None:
        pytest.skip(f"{ENV_VAR} not set; the disposable DB suite refuses any ambient DSN")
    try:
        target = check_test_dsn(raw)
    except UnsafeTestDsn as exc:
        pytest.fail(f"refusing {ENV_VAR}: {exc}")
    with connect_disposable(target) as conn:
        version, database = conn.execute(
            "SELECT current_setting('server_version_num')::int, current_database()").fetchone()
        assert version >= 160000
        assert database == target.dbname
    return target


@pytest.fixture
def schema(dsn):
    name = "bdt_" + uuid.uuid4().hex[:12]
    with connect_disposable(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
        p.install_schema(conn, name)
    try:
        yield name
    finally:
        with connect_disposable(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))


@pytest.fixture
def admin(dsn, schema):
    with connect_disposable(dsn, options=f"-csearch_path={schema}") as conn:
        yield conn


@pytest.fixture
def store(admin, schema):
    return p.PostgresPublicationStore(admin, schema)


@pytest.fixture(scope="module")
def login_roles(dsn):
    """Non-superuser LOGIN roles that are members of the reader/writer/auditor group roles."""
    suffix = uuid.uuid4().hex[:8]
    roles = {"reader": f"bdt_reader_{suffix}", "writer": f"bdt_writer_{suffix}", "auditor": f"bdt_auditor_{suffix}"}
    password = secrets.token_hex(16)
    with connect_disposable(dsn, autocommit=True) as conn:
        # Group roles are created by the first install; ensure they exist for membership.
        with conn.transaction():
            for group in ("bond_credit_reader", "bond_credit_writer", "bond_credit_auditor"):
                if conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", [group]).fetchone() is None:
                    conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(group)))
        for kind, role in roles.items():
            conn.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password)))
            conn.execute(sql.SQL("GRANT {} TO {}").format(
                sql.Identifier(f"bond_credit_{kind}"), sql.Identifier(role)))
    try:
        yield {kind: dsn.derive(user=role, password=password) for kind, role in roles.items()}
    finally:
        with connect_disposable(dsn, autocommit=True) as conn:
            for role in roles.values():
                conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


def _connect_as(target, schema, **kwargs):
    return connect_disposable(target, options=f"-csearch_path={schema}", **kwargs)


def test_hostile_libpq_environment_is_not_inherited_by_a_real_connection(dsn, monkeypatch):
    for key, value in HOSTILE_LIBPQ_ENV.items():
        monkeypatch.setenv(key, value)
    with connect_disposable(dsn) as conn:
        application, database, user, search_path = conn.execute(
            "SELECT current_setting('application_name'), current_database(), current_user, "
            "current_setting('search_path')").fetchone()
    assert application != HOSTILE_LIBPQ_ENV["PGAPPNAME"]  # PGAPPNAME not inherited
    assert search_path != "public"  # PGOPTIONS not inherited
    assert (database, user) == (dsn.dbname, dsn.user)
    assert os.environ["PGHOSTADDR"] == HOSTILE_LIBPQ_ENV["PGHOSTADDR"]  # restored after connecting


def _promoted(store, bundle, expected=None):
    p.prepare_bundle(store, bundle)
    p.validate_bundle(store, bundle.publication_id)
    return p.promote_bundle(store, bundle.publication_id, expected_pointer=expected)


def _pointer(conn):
    row = conn.execute("SELECT publication_id FROM bond_credit_current_pointer").fetchone()
    conn.rollback()
    return None if row is None else row[0]


def _catalog(conn, schema, relations=None):
    """Structural + data fingerprint of relations in ``schema``."""
    rows = conn.execute(
        """
        SELECT c.relname, c.relkind, c.relacl::text,
               (SELECT string_agg(a.attname || ':' || format_type(a.atttypid, a.atttypmod) || ':' || a.attnotnull,
                                  ',' ORDER BY a.attnum)
                FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
               (SELECT string_agg(pg_get_constraintdef(k.oid), ';' ORDER BY k.conname)
                FROM pg_constraint k WHERE k.conrelid = c.oid),
               (SELECT string_agg(t.tgname || ':' || pg_get_triggerdef(t.oid), ';' ORDER BY t.tgname)
                FROM pg_trigger t WHERE t.tgrelid = c.oid AND NOT t.tgisinternal),
               (SELECT string_agg(pg_get_indexdef(i.indexrelid), ';' ORDER BY i.indexrelid::regclass::text)
                FROM pg_index i WHERE i.indrelid = c.oid)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'v', 'm', 'p')
          AND (%s::text[] IS NULL OR c.relname = ANY(%s::text[]))
        ORDER BY c.relname
        """,
        [schema, relations, relations],
    ).fetchall()
    data = {}
    for (name, *_rest) in rows:
        data[name] = conn.execute(
            sql.SQL("SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) FROM {} t").format(
                sql.Identifier(schema, name))
        ).fetchone()[0]
    return rows, data


EXPECTED_TABLES = {
    "bond_default_source_package", "bond_credit_observation", "bond_default_event_link",
    "bond_default_adjudication", "bond_credit_publications", "bond_credit_publication_sources",
    "bond_credit_current_pointer", "bond_credit_publication_revocations", "bond_credit_lifecycle_tokens",
    "bond_credit_pointer_tokens", "bond_default_event_v1", "bond_default_followup_v1",
    "bond_default_exit_evidence_v1", "bond_default_coverage_v1", "bond_rating_history_public_v1",
    # W0 amendment 1 (bundle v2): N-CEN ledger and the persisted dependency closure.
    "bond_default_ncen_filing", "bond_default_family_context_v2", "bond_default_family_evidence_v2",
    "bond_default_proposal_evidence_v2", "bond_default_exchange_relation_v2",
}


# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
def test_install_on_empty_database(dsn):
    name = f"bdefault_disposable_empty_{uuid.uuid4().hex[:8]}"
    with connect_disposable(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        empty_target = dsn.derive(dbname=name)  # re-validated: still a loopback disposable target
        with connect_disposable(empty_target, autocommit=True) as conn:
            before = conn.execute(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')").fetchone()[0]
            assert before == 0
            conn.execute("CREATE SCHEMA credit")
            p.install_schema(conn, "credit")
            tables = {r[0] for r in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'credit'").fetchall()}
            assert tables == EXPECTED_TABLES
            functions = {r[0] for r in conn.execute(
                "SELECT proname FROM pg_proc WHERE pronamespace = 'credit'::regnamespace").fetchall()}
            assert {"bond_credit_validate", "bond_credit_promote", "bond_credit_revoke",
                    "bond_credit_current_publication", "bond_credit_read_publication"} <= functions
            # Every function pins its search_path; nothing lands outside the target schema.
            unpinned = conn.execute(
                "SELECT proname FROM pg_proc WHERE pronamespace = 'credit'::regnamespace "
                "AND NOT EXISTS (SELECT 1 FROM unnest(proconfig) s WHERE s LIKE 'search_path=%%')").fetchall()
            assert unpinned == []
            outside = conn.execute(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public'").fetchone()[0]
            assert outside == 0
    finally:
        with connect_disposable(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def test_install_leaves_preexisting_unrelated_tables_unchanged(dsn):
    name = "bdt_pre_" + uuid.uuid4().hex[:10]
    with connect_disposable(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
        try:
            conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(name)))
            conn.execute("""
                CREATE TABLE sec_derived_publications (publication_id uuid PRIMARY KEY, product text NOT NULL,
                    sec_run_id uuid NOT NULL);
                CREATE TABLE bond_rating_history_v1 (cusip char(9), month date, bucket text,
                    PRIMARY KEY (cusip, month));
                CREATE INDEX bond_rating_history_v1_bucket_idx ON bond_rating_history_v1 (bucket);
                CREATE TABLE unrelated_worker_state (id serial PRIMARY KEY, payload jsonb);
                CREATE FUNCTION unrelated_touch() RETURNS trigger LANGUAGE plpgsql AS
                    $$ BEGIN NEW.payload := NEW.payload; RETURN NEW; END $$;
                CREATE TRIGGER unrelated_touch BEFORE INSERT ON unrelated_worker_state
                    FOR EACH ROW EXECUTE FUNCTION unrelated_touch();
                INSERT INTO sec_derived_publications VALUES (gen_random_uuid(), 'bond_curve_v1', gen_random_uuid());
                INSERT INTO bond_rating_history_v1 VALUES ('ZZ#SYN014', '2026-04-01', 'BB');
                INSERT INTO unrelated_worker_state (payload) VALUES ('{"k": 1}'), ('{"k": 2}');
            """)
            unrelated = ["sec_derived_publications", "bond_rating_history_v1", "unrelated_worker_state"]
            before = _catalog(conn, name, unrelated)
            p.install_schema(conn, name)
            first = _catalog(conn, name, sorted(EXPECTED_TABLES))
            p.install_schema(conn, name)  # additive and rerunnable
            assert _catalog(conn, name, unrelated) == before
            assert _catalog(conn, name, sorted(EXPECTED_TABLES)) == first
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))


def test_install_refuses_an_installed_v1_schema(dsn):
    """W0 amendment 1 stop gate: v2 is never installed over v1 shapes (no in-place migration)."""
    name = "bdt_v1_" + uuid.uuid4().hex[:10]
    with connect_disposable(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
        try:
            conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(name)))
            # Minimal v1 shape: the adjudication ledger without the v2 proposal edge column.
            conn.execute("CREATE TABLE bond_default_adjudication (adjudication_id uuid PRIMARY KEY)")
            with pytest.raises(errors.RaiseException, match="in-place v1->v2 migration is not supported"):
                p.install_schema(conn, name)
            conn.execute("ROLLBACK")  # the SQL file's explicit transaction is aborted, never committed
            assert conn.execute(
                "SELECT to_regclass(%s)", [f"{name}.bond_credit_publications"]).fetchone()[0] is None
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))


# ---------------------------------------------------------------------------
# Lifecycle, replay, collision
# ---------------------------------------------------------------------------
def test_full_lifecycle_roundtrip(store, admin):
    q = syn.build_bundle()
    assert p.prepare_bundle(store, q) == "inserted"
    state = store.state(q.publication_id)
    assert state.lifecycle_state == "prepared" and state.publication_version >= 1
    assert p.validate_bundle(store, q.publication_id).lifecycle_state == "validated"
    assert p.validate_bundle(store, q.publication_id).lifecycle_state == "validated"
    assert p.promote_bundle(store, q.publication_id, expected_pointer=None) == q.publication_id
    assert store.current(expected_target_month=syn.TARGET_MONTH)["publication_id"] == q.publication_id
    assert store.read(q.publication_id).canonical_bytes() == q.canonical_bytes()
    assert _pointer(admin) == q.publication_id


def test_exact_replay_noop_and_collision_with_different_bytes(store, admin):
    q = syn.build_bundle()
    assert p.prepare_bundle(store, q) == "inserted"
    counts = admin.execute(
        "SELECT (SELECT count(*) FROM bond_credit_observation), (SELECT count(*) FROM bond_default_coverage_v1)"
    ).fetchone()
    admin.rollback()
    assert p.prepare_bundle(store, q) == "replayed"
    altered = syn.build_bundle(alter_output=True)
    assert altered.publication_id == q.publication_id
    with pytest.raises(p.PublicationCollision, match="publication_collision"):
        p.prepare_bundle(store, altered)
    edgar = next(o for o in q.frames["observations"] if o.observation_kind == "edgar_passage")
    changed = syn.replace_row(edgar, document_quote="SYNTHETIC: other bytes")
    other = syn.reassemble(q, frames={"observations": tuple(
        changed if o is edgar else o for o in q.frames["observations"])})
    with pytest.raises(p.PublicationCollision, match="ledger_row_collision"):
        p.prepare_bundle(store, other)
    after = admin.execute(
        "SELECT (SELECT count(*) FROM bond_credit_observation), (SELECT count(*) FROM bond_default_coverage_v1),"
        " (SELECT count(*) FROM bond_credit_publications)"
    ).fetchone()
    admin.rollback()
    assert after == (*counts, 1)


def test_child_rows_only_while_prepared(store, admin):
    q = syn.build_bundle()
    p.prepare_bundle(store, q)
    cell = next(x for x in q.frames["coverage"] if x.state == "not_applicable")
    extra = syn.replace_row(cell, period_label="2025")
    insert = sql.SQL(
        "INSERT INTO bond_default_coverage_v1 (publication_id, {}, row_sha256) VALUES (%s, {}, %s)"
    ).format(
        sql.SQL(", ").join(sql.Identifier(n) for n, _ in c.CoverageCell.SPEC),
        sql.SQL(", ").join(sql.Placeholder() for _ in c.CoverageCell.SPEC),
    )
    params = [q.publication_id, *[getattr(extra, n) for n, _ in c.CoverageCell.SPEC], extra.row_sha256()]
    with admin.transaction():
        admin.execute(insert, params)  # allowed while prepared ...
        with pytest.raises(errors.RaiseException, match="frame_mismatch:coverage"):
            admin.execute("SELECT bond_credit_validate(%s)", [q.publication_id])  # ... but breaks the digest
        raise psycopg.Rollback()
    p.validate_bundle(store, q.publication_id)
    with pytest.raises(errors.RaiseException, match="only be inserted while publication"), admin.transaction():
        admin.execute(insert, params)


@pytest.mark.parametrize(("table", "frame"), [
    ("bond_default_event_v1", "events"),
    ("bond_default_coverage_v1", "coverage"),
    ("bond_credit_publication_sources", "publication_sources"),
])
def test_promotion_reverifies_stored_frames_against_the_manifest(store, admin, table, frame):
    q = syn.build_bundle()
    p.prepare_bundle(store, q)
    p.validate_bundle(store, q.publication_id)
    with admin.transaction():
        # Superuser-only bypass of the immutability triggers, simulating out-of-band tampering.
        admin.execute("SET LOCAL session_replication_role = replica")
        deleted = admin.execute(
            sql.SQL("DELETE FROM {} WHERE ctid = (SELECT ctid FROM {} WHERE publication_id = %s LIMIT 1)").format(
                sql.Identifier(table), sql.Identifier(table)), [q.publication_id]).rowcount
        assert deleted == 1
        admin.execute("SET LOCAL session_replication_role = origin")
        with pytest.raises(errors.RaiseException, match=f"bond_credit_promote:frame_mismatch:{frame}"):
            admin.execute("SELECT bond_credit_promote(%s, NULL)", [q.publication_id])
        raise psycopg.Rollback()
    assert _pointer(admin) is None
    # The untampered build still promotes through the same function.
    assert p.promote_bundle(store, q.publication_id, expected_pointer=None) == q.publication_id


def test_updates_and_deletes_refused_on_inputs_and_builds(store, admin):
    q = syn.build_bundle()
    _promoted(store, q)
    partial = syn.build_bundle(quality_state="partial", with_receipt=False)
    p.prepare_bundle(store, partial)  # stays prepared
    statements = [
        "UPDATE bond_default_source_package SET parser_version = 'x'",
        "DELETE FROM bond_credit_observation",
        "UPDATE bond_default_event_link SET rationale = 'x'",
        "DELETE FROM bond_default_adjudication",
        "UPDATE bond_credit_publications SET quality_state = 'partial'",
        "DELETE FROM bond_credit_publications",
        "UPDATE bond_credit_publications SET lifecycle_state = 'validated', validated_at = now()",
        "UPDATE bond_default_event_v1 SET obligor_id = 'x'",
        "DELETE FROM bond_default_followup_v1",
        "UPDATE bond_default_exit_evidence_v1 SET primary_reason = 'unknown'",
        "DELETE FROM bond_default_coverage_v1",
        "UPDATE bond_rating_history_public_v1 SET coverage_frontier = NULL",
        "DELETE FROM bond_credit_publication_sources",
        "TRUNCATE bond_credit_observation CASCADE",
        "TRUNCATE bond_default_event_v1",
        "DELETE FROM bond_credit_current_pointer",
        "UPDATE bond_credit_current_pointer SET publication_id = publication_id",
        (
            "INSERT INTO bond_credit_current_pointer (product, publication_id) VALUES "
            f"('bond_credit_evidence_v1', '{partial.publication_id}') ON CONFLICT (product) DO UPDATE "
            "SET publication_id = EXCLUDED.publication_id"
        ),
    ]
    for statement in statements:
        with pytest.raises(errors.RaiseException), admin.transaction():
            admin.execute(statement)
    assert store.state(partial.publication_id).lifecycle_state == "prepared"
    assert _pointer(admin) == q.publication_id


# ---------------------------------------------------------------------------
# Least privilege (non-superuser login roles)
# ---------------------------------------------------------------------------
def test_reader_and_writer_roles(dsn, schema, login_roles, admin):
    with connect_disposable(dsn) as conn:
        flags = conn.execute(
            "SELECT rolname, rolsuper FROM pg_roles WHERE rolname LIKE 'bdt\\_%%' OR rolname LIKE 'bond\\_credit\\_%%'"
        ).fetchall()
    assert flags and not any(sup for _name, sup in flags)
    q = syn.build_bundle()
    with _connect_as(login_roles["writer"], schema) as writer:
        assert writer.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()[0] is False
        writer.rollback()
        store = p.PostgresPublicationStore(writer, schema)
        assert _promoted(store, q) == q.publication_id
        for statement in (
            "UPDATE bond_credit_observation SET cusip_raw = 'x'",
            "DELETE FROM bond_default_event_v1",
            "UPDATE bond_credit_publications SET quality_state = 'partial'",
            (
                "INSERT INTO bond_credit_current_pointer (product, publication_id) "
                f"VALUES ('bond_credit_evidence_v1', '{q.publication_id}')"
            ),
            f"INSERT INTO bond_credit_lifecycle_tokens VALUES ('{q.publication_id}', 1, 1)",
            "TRUNCATE bond_rating_history_public_v1",
            "DELETE FROM bond_credit_publication_revocations",
        ):
            with pytest.raises(errors.InsufficientPrivilege), writer.transaction():
                writer.execute(statement)
    with _connect_as(login_roles["reader"], schema) as reader:
        current = reader.execute("SELECT publication_id FROM bond_credit_current_publication()").fetchone()[0]
        assert current == q.publication_id
        reader.rollback()
        for statement, params in (
            ("SELECT * FROM bond_credit_current_pointer", []),
            ("SELECT bond_credit_promote(%s, %s)", [q.publication_id, q.publication_id]),
            ("SELECT bond_credit_validate(%s)", [q.publication_id]),
            ("SELECT bond_credit_revoke(%s, 'x', %s)", [q.publication_id, "sha256:" + "0" * 64]),
            ("INSERT INTO bond_default_adjudication (adjudication_id) VALUES (gen_random_uuid())", []),
        ):
            with pytest.raises(errors.InsufficientPrivilege), reader.transaction():
                reader.execute(statement, params)
        with pytest.raises(p.PublicationError):
            p.PostgresPublicationStore(reader, schema).prepare(syn.build_bundle(with_events=False))


# ---------------------------------------------------------------------------
# Promotion: CAS race, T/K, shadow builds, empty-qualified
# ---------------------------------------------------------------------------
def test_cas_race_from_two_sessions_with_absent_pointer(dsn, schema, store, admin):
    q = syn.build_bundle()
    other = syn.build_bundle(knowledge_cutoff=K0 + dt.timedelta(days=1))
    for bundle in (q, other):
        p.prepare_bundle(store, bundle)
        p.validate_bundle(store, bundle.publication_id)
    outcome: dict[str, object] = {}
    with _connect_as(dsn, schema) as first, _connect_as(dsn, schema) as second:
        first.execute("SELECT bond_credit_promote(%s, NULL)", [q.publication_id])  # holds the product lock

        def contender():
            try:
                p.promote_bundle(p.PostgresPublicationStore(second, schema), other.publication_id,
                                 expected_pointer=None)
                outcome["result"] = "promoted"
            except p.PublicationError as exc:
                outcome["result"] = exc.code

        thread = threading.Thread(target=contender)
        thread.start()
        time.sleep(1.0)
        assert thread.is_alive(), "second session must wait on the product advisory lock"
        first.commit()
        thread.join(timeout=30)
    assert outcome["result"] == "bond_credit_promote:cas_mismatch"
    assert _pointer(admin) == q.publication_id
    # With a present pointer the correct expectation wins and the stale one loses.
    with pytest.raises(p.PublicationError, match="cas_mismatch"):
        p.promote_bundle(store, other.publication_id, expected_pointer=None)
    assert p.promote_bundle(store, other.publication_id, expected_pointer=q.publication_id)


def test_target_month_and_cutoff_never_regress(store, admin):
    q = syn.build_bundle()
    _promoted(store, q)
    older_k = syn.build_bundle(knowledge_cutoff=dt.datetime(2026, 9, 24, 18, 0, tzinfo=UTC))
    older_t = syn.build_bundle(target_month=dt.date(2026, 5, 1), knowledge_cutoff=K0 + dt.timedelta(days=1))
    same_tk = syn.reassemble(q, code_digest="sha256:" + "6" * 64)
    for bundle, code in ((older_k, "tk_regression"), (older_t, "tk_regression"), (same_tk, "tk_not_advanced")):
        p.prepare_bundle(store, bundle)
        p.validate_bundle(store, bundle.publication_id)
        with pytest.raises(p.PublicationError, match=code):
            p.promote_bundle(store, bundle.publication_id, expected_pointer=q.publication_id)
        assert _pointer(admin) == q.publication_id
    newer = syn.build_bundle(knowledge_cutoff=K0 + dt.timedelta(days=1))
    assert _promoted(store, newer, q.publication_id) == newer.publication_id


def test_partial_and_unavailable_are_never_promoted(store, admin):
    q = syn.build_bundle()
    _promoted(store, q)
    for quality, delta in (("partial", 1), ("unavailable", 2)):
        shadow = syn.build_bundle(quality_state=quality, with_receipt=False,
                                  knowledge_cutoff=K0 + dt.timedelta(days=delta))
        p.prepare_bundle(store, shadow)
        assert p.validate_bundle(store, shadow.publication_id).lifecycle_state == "validated"
        with pytest.raises(p.PublicationError, match="not_qualified_complete"):
            p.promote_bundle(store, shadow.publication_id, expected_pointer=q.publication_id)
        with pytest.raises(p.PublicationError, match="shadow_build_requires_allow_shadow"):
            store.read(shadow.publication_id)
        assert store.read(shadow.publication_id, allow_shadow=True).publication_id == shadow.publication_id
    assert _pointer(admin) == q.publication_id


def test_unqualified_coverage_only_build_persists_and_reads_by_id_without_pointer(store, admin, login_roles, schema):
    """Diagnostic-tier feasibility: a limited/partial (or unavailable) build with no events is stored,
    validated and read back by ID with every child frame, yet never becomes the serving pointer."""
    for quality, delta in (("partial", 1), ("unavailable", 2)):
        shadow = syn.build_bundle(quality_state=quality, with_receipt=False, with_events=False,
                                  knowledge_cutoff=K0 + dt.timedelta(days=delta))
        assert shadow.manifest.values["build_scope"] == "limited" and not shadow.frames["events"]
        assert p.prepare_bundle(store, shadow) == "inserted"
        assert p.validate_bundle(store, shadow.publication_id).lifecycle_state == "validated"
        with pytest.raises(p.PublicationError, match="not_qualified_complete"):
            p.promote_bundle(store, shadow.publication_id, expected_pointer=None)
        with pytest.raises(p.PublicationError, match="no_current_publication"):
            store.current()
        with _connect_as(login_roles["reader"], schema) as reader:
            served = p.PostgresPublicationStore(reader, schema).read(shadow.publication_id, allow_shadow=True)
            assert served.canonical_bytes() == shadow.canonical_bytes()
            counts = _read_all(reader, shadow.publication_id, allow_shadow=True)
            for frame in (*c.INPUT_FRAMES, *c.OUTPUT_FRAMES):
                assert counts[f"bond_credit_read_{frame}"] == len(shadow.frames[frame]), frame
            assert counts["bond_credit_read_coverage"] == len(shadow.frames["coverage"]) > 0
            assert counts["bond_credit_read_ratings"] == 2 * len(shadow.panel_grid)
    assert _pointer(admin) is None


def test_empty_qualified_requires_positive_validation_receipt(store, admin):
    empty = syn.build_bundle(with_events=False)
    assert not empty.frames["events"]
    assert _promoted(store, empty) == empty.publication_id
    base = {n: empty.manifest.values[n] for n, _ in c.MANIFEST_SPEC}
    names = list(base)
    insert = sql.SQL("INSERT INTO bond_credit_publications ({}) VALUES ({})").format(
        sql.SQL(", ").join(sql.Identifier(n) for n in names), sql.SQL(", ").join(sql.Placeholder() for _ in names))
    no_receipt = {**base, "publication_id": uuid.uuid4(), "fingerprint_digest": "sha256:" + "a" * 64,
                  **{f: None for f in (*c.VALIDATION_FIELDS, *c.VALIDATION_RATING_FIELDS)},
                  "validation_digest": None}
    zero_evidence = {**base, "publication_id": uuid.uuid4(), "fingerprint_digest": "sha256:" + "b" * 64,
                     "validation_positive_evidence_count": 0}
    partial_verdict = {**base, "publication_id": uuid.uuid4(), "fingerprint_digest": "sha256:" + "c" * 64,
                       "validation_verdict": "partial"}
    # Rating qualification: a half pair, or a pair without any receipt, is refused by the table.
    half_pair = {**base, "publication_id": uuid.uuid4(), "fingerprint_digest": "sha256:" + "e" * 64,
                 "validation_rating_package_digest": None}
    rating_without_receipt = {**no_receipt, "publication_id": uuid.uuid4(), "fingerprint_digest": "sha256:" + "f" * 64,
                              "quality_state": "partial", **{f: base[f] for f in c.VALIDATION_RATING_FIELDS}}
    assert all(base[f] is not None for f in c.VALIDATION_RATING_FIELDS)
    # Surveillance scope: a half window, an empty window or a window without a receipt is refused.
    start, end = (syn.SURVEILLANCE_WINDOW[f.removeprefix("validation_")] for f in c.VALIDATION_SURVEILLANCE_FIELDS)
    half_window = {**base, "publication_id": uuid.uuid4(), "fingerprint_digest": "sha256:" + "1" * 64,
                   "validation_surveillance_start_exclusive": start}
    empty_window = {**base, "publication_id": uuid.uuid4(), "fingerprint_digest": "sha256:" + "2" * 64,
                    "validation_surveillance_start_exclusive": end, "validation_surveillance_end_inclusive": end}
    window_without_receipt = {**no_receipt, "publication_id": uuid.uuid4(), "fingerprint_digest": "sha256:" + "3" * 64,
                              "quality_state": "partial", "validation_surveillance_start_exclusive": start,
                              "validation_surveillance_end_inclusive": end}
    for values in (no_receipt, zero_evidence, partial_verdict, half_pair, rating_without_receipt,
                   half_window, empty_window, window_without_receipt):
        with pytest.raises(errors.CheckViolation), admin.transaction():
            admin.execute(insert, [p._db_value(kind, values[name]) for name, kind in c.MANIFEST_SPEC])


# ---------------------------------------------------------------------------
# SQL validation parity (semantic rules enforced by bond_credit_validate itself)
# ---------------------------------------------------------------------------
def _sql_validate_code(store, admin, bundle):
    p.prepare_bundle(store, bundle)  # prepare verifies derivation only
    with pytest.raises(errors.RaiseException) as info, admin.transaction():
        admin.execute("SELECT bond_credit_validate(%s)", [bundle.publication_id])
    with pytest.raises(p.PublicationError) as py_info:
        p.validate_bundle(store, bundle.publication_id)
    sql_code = str(info.value.diag.message_primary).split(":")[1]
    assert py_info.value.code == f"bond_credit_validate:{sql_code}"
    assert store.state(bundle.publication_id).lifecycle_state == "prepared"
    return sql_code


def test_sql_validate_enforces_semantic_rules(store, admin):
    q = syn.build_bundle()
    event = next(e for e in q.frames["events"] if e.primary_type == "payment_default")
    late = syn.replace_row(event, evidence_known_at=event.evidence_known_at + dt.timedelta(hours=1))
    bad_event = syn.reassemble(q, frames={"events": tuple(late if e is event else e for e in q.frames["events"])},
                               code_digest="sha256:" + "d1" * 32)
    assert _sql_validate_code(store, admin, bad_event) == "event_evidence_invalid"

    missing_event = syn.reassemble(
        q, frames={"events": tuple(e for e in q.frames["events"] if e is not event)},
        code_digest="sha256:" + "d2" * 32)
    assert _sql_validate_code(store, admin, missing_event) == "admitted_adjudication_without_event"

    short_grid = syn.reassemble(q, frames={"ratings": q.frames["ratings"][1:]}, code_digest="sha256:" + "d3" * 32)
    assert _sql_validate_code(store, admin, short_grid) == "rating_grid_mismatch"
    assert p.prepare_bundle(store, short_grid) == "replayed"  # exact replay of an invalid build is still a no-op

    disputed = next(a for a in q.frames["adjudications"] if a.status == "disputed")
    second_batch = syn.new_package("adjudication_batch", "SYNTHETIC-ADJ-2")
    dangling = c.Adjudication.create(**{
        **{n: getattr(disputed, n) for n, _ in c.Adjudication.SPEC if n != "adjudication_id"},
        "package_id": second_batch.package_id,
        "evidence_observation_ids": c.sorted_uuids([*disputed.evidence_observation_ids, uuid.uuid4()]),
    })
    outside = syn.reassemble(q, frames={
        "source_packages": (*q.frames["source_packages"], second_batch),
        "adjudications": (*q.frames["adjudications"], dangling),
    }, code_digest="sha256:" + "d6" * 32)
    assert _sql_validate_code(store, admin, outside) == "adjudication_evidence_outside_inventory"


RATING_REGRESSIONS = syn.rating_regressions()


@pytest.mark.parametrize("name", sorted(RATING_REGRESSIONS))
def test_sql_rating_input_qualification_and_public_pit_binding(store, admin, name):
    """bond_credit_validate itself refuses unqualified rating input and unbound PIT rows."""
    bundle, reason = RATING_REGRESSIONS[name]
    if reason is None:
        assert _promoted(store, bundle) == bundle.publication_id
        assert _pointer(admin) == bundle.publication_id
        return
    assert _sql_validate_code(store, admin, bundle) == reason
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)
    assert _pointer(admin) is None
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)
    assert _pointer(admin) is None


CLOSURE_REGRESSIONS = syn.evidence_closure_regressions()


@pytest.mark.parametrize("name", sorted(CLOSURE_REGRESSIONS))
def test_sql_evidence_closure_and_onset_lower_provenance(store, admin, name):
    """bond_credit_validate itself enforces dependency closure, row known-times and onset provenance."""
    bundle, reason = CLOSURE_REGRESSIONS[name]
    if reason is None:
        assert _promoted(store, bundle) == bundle.publication_id
        assert _pointer(admin) == bundle.publication_id
        return
    assert _sql_validate_code(store, admin, bundle) == reason
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)
    assert _pointer(admin) is None


V2_REGRESSIONS = syn.v2_regressions()
V2_DB_SEEDS = syn.v2_db_seeds()


@pytest.mark.parametrize("name", sorted(V2_REGRESSIONS))
def test_sql_v2_dependency_closure(store, admin, name):
    """bond_credit_validate itself enforces W0 amendment 1 section 9 items 1-3 (SQL-only path:
    prepare verifies derivation only; the reason must equal Python's check_bundle)."""
    bundle, reason = V2_REGRESSIONS[name]
    if name in syn.V2_DB_INSERT_REFUSED:
        # A stricter ledger guard (link -> observation FK, family routing trigger) refuses the row at insert.
        with pytest.raises(p.PublicationError, match=syn.V2_DB_INSERT_REFUSED[name]):
            p.prepare_bundle(store, bundle)
        return
    seed = V2_DB_SEEDS.get(name)
    if seed is not None:
        # The omitted rows already exist in the append-only ledger (a prior valid prepare), so the
        # defective publication is inserted and bond_credit_validate itself must refuse it.
        p.prepare_bundle(store, seed)
    if reason is None:
        assert _promoted(store, bundle) == bundle.publication_id
        assert _pointer(admin) == bundle.publication_id
        # Round trip through the guarded readers (N-CEN ledger and v2 frames included).
        assert store.read(bundle.publication_id).canonical_bytes() == bundle.canonical_bytes()
        return
    assert _sql_validate_code(store, admin, bundle) == reason


def test_sql_group_c_pairing_head_is_scoped_to_publication_inventory(store, admin):
    target = syn.exchange_bundle(syn.build_bundle())
    p.prepare_bundle(store, target)
    pair = syn._exchange_inputs(syn.build_bundle())["pair"]
    base_package = next(row for row in target.frames["source_packages"]  # type: ignore[attr-defined]
                        if row.source_family == "adjudication_batch")
    external_package = syn._package(
        "adjudication_batch", "SYNTHETIC-GROUP-C-EXTERNAL-PAIR-REVISION", base_package.rights_state,
        first_public=dt.datetime(2026, 9, 20, tzinfo=UTC), basis=base_package.public_time_basis,
    )
    external_retraction = syn._readjudicate(
        pair, package_id=external_package.package_id, status="retracted",
        supersedes_adjudication_id=pair.adjudication_id,
        adjudicated_at=dt.datetime(2026, 9, 24, 9, 0, tzinfo=UTC),
        support_valid_from=None, support_valid_to=None,
    )
    seeded = syn.reassemble(target, frames={
        "source_packages": (*target.frames["source_packages"], external_package),
        "adjudications": (*target.frames["adjudications"], external_retraction),
    })
    p.prepare_bundle(store, seeded)

    # The excluded package cannot alter the original publication's K-scoped head.
    assert _promoted(store, target) == target.publication_id
    # Once the package is part of the publication inventory, the retraction is effective.
    assert _sql_validate_code(store, admin, seeded) == "exchange_pairing_invalid"


GROUP_D_REGRESSIONS = syn.group_d_regressions()


@pytest.mark.parametrize("name", sorted(GROUP_D_REGRESSIONS))
def test_sql_group_d_admission_and_cure_authority(store, admin, name):
    bundle, expected = GROUP_D_REGRESSIONS[name]
    if expected is None:
        assert _promoted(store, bundle) == bundle.publication_id
        assert _pointer(admin) == bundle.publication_id
        return
    assert _sql_validate_code(store, admin, bundle) == expected
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)
    assert _pointer(admin) is None


REVISION_REGRESSIONS = syn.revision_regressions()


@pytest.mark.parametrize("name", sorted(REVISION_REGRESSIONS))
def test_sql_revision_chains_resolved_as_of_cutoff(store, admin, name):
    """bond_credit_validate itself refuses outputs relying on inputs superseded/retracted by K."""
    bundle, reason = REVISION_REGRESSIONS[name]
    if reason is None:
        assert _promoted(store, bundle) == bundle.publication_id
        assert _pointer(admin) == bundle.publication_id
        return
    assert _sql_validate_code(store, admin, bundle) == reason
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)
    assert _pointer(admin) is None


def test_sql_revision_cycle_detection(store, admin):
    """The store refuses to persist a revision cycle; the SQL cycle finder detects any cycle."""
    with pytest.raises(p.PublicationError, match="bond_credit_db:contract_violation:supersession_cycle"):
        p.prepare_bundle(store, syn.observation_cycle_bundle())
    a, b, d, e = sorted(uuid.UUID(int=n) for n in (1, 2, 3, 4))
    cases = (
        ([a, b, d], [b, a, None], a),  # two-cycle
        ([a, b, d, e], [b, d, e, d], d),  # cycle not containing the first record
        ([a, b, d], [b, d, None], None),  # acyclic chain
        ([], [], None),
    )
    for ids, parents, expected in cases:
        found = admin.execute("SELECT bond_credit_revision_cycle(%s::uuid[], %s::uuid[])", [ids, parents]).fetchone()[0]
        assert found == expected
    admin.rollback()


# ---------------------------------------------------------------------------
# Row encoding parity (bond_credit_row_v1) and recomputed identity (UUIDv8)
# ---------------------------------------------------------------------------
_PARITY_IDS = sorted(uuid.UUID(int=n * 0x1111_1111_1111_1111_1111_1111_1111_1111) for n in (3, 1, 2))
_PARITY_RECORD = {
    "n": None,
    "u": "é中\U0001d11e \"q\" \\ \n\t|x",
    "e": "",
    "i": 42,
    "z": 0,
    "b": True,
    "f": False,
    "a": ["b|c", "é", ""],
    "ae": [],
    "ids": [str(item) for item in _PARITY_IDS],
    "m": {"b": "1", "aa": "é", "Z": "x", "中": "y", "é": "z|w"},
    "me": {},
    "t": "2026-05-20T10:15:00.000001Z",
    "tz": "2026-01-01T00:00:00.000000Z",
    "tn": None,
    "d": "2026-05-01",
}
_PARITY_SQL = """
SELECT bond_credit_row_sha256(pg_catalog.jsonb_build_object(
    'n', NULL::text, 'u', %(u)s::text, 'e', ''::text, 'i', 42, 'z', 0, 'b', true, 'f', false,
    'a', %(a)s::text[], 'ae', ARRAY[]::uuid[], 'ids', %(ids)s::uuid[], 'm', %(m)s::jsonb,
    'me', '{}'::jsonb, 't', '2026-05-20 06:15:00.000001-04'::timestamptz,
    'tz', '2026-01-01 00:00:00+00'::timestamptz, 'tn', NULL::timestamptz, 'd', '2026-05-01'::date),
    %(spec)s::text[])
"""


def test_sql_row_encoding_is_byte_identical_to_python(store, admin):
    for frame, spec in c.sql_frame_specs().items():
        assert admin.execute("SELECT bond_credit_frame_spec(%s)", [frame]).fetchone()[0] == list(spec)
    admin.rollback()
    names = list(_PARITY_RECORD)
    spec = [("t:" if name in ("t", "tz", "tn") else "j:") + name for name in names]
    expected = c.row_encoding_sha256(_PARITY_RECORD, names)
    params = {"u": _PARITY_RECORD["u"], "a": _PARITY_RECORD["a"], "ids": _PARITY_IDS, "spec": spec,
              "m": json.dumps(_PARITY_RECORD["m"], ensure_ascii=False)}
    q = syn.build_bundle()
    p.prepare_bundle(store, q)
    for zone, style in (("UTC", "ISO, MDY"), ("America/New_York", "SQL, DMY"), ("Asia/Kolkata", "German")):
        admin.execute("SELECT set_config('TimeZone', %s, false), set_config('DateStyle', %s, false)", [zone, style])
        assert admin.execute(_PARITY_SQL, params).fetchone()[0] == expected
        # Every persisted row, the fingerprint, the receipt digest and the UUIDv8 id recompute in SQL.
        persisted = 0
        for frame, table in p.TABLES.items():
            rows = admin.execute(sql.SQL(
                "SELECT row_sha256::text, bond_credit_row_sha256(pg_catalog.to_jsonb(t), bond_credit_frame_spec(%s)) "
                "FROM {} t").format(sql.Identifier(table)), [frame]).fetchall()
            assert sorted(stored for stored, _ in rows) == sorted(r.row_sha256() for r in q.frames[frame])
            assert all(stored == computed for stored, computed in rows), frame
            persisted += len(rows)
        assert persisted > 0
        identity = admin.execute(
            "SELECT bond_credit_fingerprint_digest(pg_catalog.to_jsonb(x)), "
            "bond_credit_validation_digest(pg_catalog.to_jsonb(x)), bond_credit_publication_id_for(x.fingerprint_digest) "
            "FROM bond_credit_publications x WHERE x.publication_id = %s", [q.publication_id]).fetchone()
        assert identity == (q.manifest["fingerprint_digest"], q.manifest["validation_digest"], q.publication_id)
        admin.rollback()
    with admin.transaction():
        admin.execute("SELECT set_config('TimeZone', 'Pacific/Chatham', false), "
                      "set_config('DateStyle', 'Postgres, DMY', false)")
        admin.execute("SELECT bond_credit_validate(%s)", [q.publication_id])
        # psycopg's binary loaders need ISO output; the SQL checks above ran under the odd settings.
        admin.execute("SELECT set_config('TimeZone', 'UTC', false), set_config('DateStyle', 'ISO, MDY', false)")
    assert store.state(q.publication_id).lifecycle_state == "validated"


def test_sql_sha1_uuid5_and_edgar_time_match_python(admin):
    messages = (b"", b"abc", b"a" * 56, b"multi-block:" + b"z" * 80, "café中".encode())
    for message in messages:
        observed = admin.execute("SELECT bond_credit_sha1(%s)", [message]).fetchone()[0]
        assert bytes(observed) == hashlib.sha1(message).digest()
    names = ("", "abc", "a" * 56, "multi-block:" + "z" * 80, "café中")
    for name in names:
        observed = admin.execute("SELECT bond_credit_uuid5(%s, %s)", [c.NAMESPACE, name]).fetchone()[0]
        assert observed == uuid.uuid5(c.NAMESPACE, name)

    bundle = syn.build_bundle()
    filings = bundle.frames["ncen_filings"]
    for filing in filings:
        parts = [str(filing.package_id), filing.accession_number, filing.row_locator]
        identity_name, filing_id = admin.execute(
            "SELECT bond_credit_identity_name('ncen_filing', %s), "
            "bond_credit_uuid5(%s, bond_credit_identity_name('ncen_filing', %s))",
            [parts, c.NAMESPACE, parts],
        ).fetchone()
        assert identity_name == c.identity_name("ncen_filing", *parts)
        assert filing_id == filing.filing_evidence_id

    valid = ("20260518163015", "20251102013000", "20261101013000")
    for raw in valid:
        observed = admin.execute("SELECT bond_credit_edgar_acceptance_to_utc(%s)", [raw]).fetchone()[0]
        assert observed == c.edgar_acceptance_to_utc(raw)
    for raw in ("20250309023000", "20260308023000"):
        assert admin.execute("SELECT bond_credit_edgar_acceptance_to_utc(%s)", [raw]).fetchone()[0] is None
        with pytest.raises(c.ContractError, match="nonexistent"):
            c.edgar_acceptance_to_utc(raw)
    for raw in ("20260518240000", "20260518163060"):
        assert admin.execute("SELECT bond_credit_edgar_acceptance_to_utc(%s)", [raw]).fetchone()[0] is None
        with pytest.raises(c.ContractError, match="invalid"):
            c.edgar_acceptance_to_utc(raw)
    # PostgreSQL's native ambiguous-wall-time conversion is the later UTC instant.
    assert admin.execute(
        "SELECT '2026-11-01 01:30:00'::timestamp AT TIME ZONE 'America/New_York'"
    ).fetchone()[0] == c.edgar_acceptance_to_utc("20261101013000")
    admin.rollback()


def test_sql_rating_declaration_and_input_digests_match_python(admin):
    bundle = syn.build_bundle()
    declarations = bundle.manifest.to_record()["rating_declarations"]
    package_digest = c.rating_package_digest(bundle.frames["source_packages"])
    unicode_declarations = syn.pr.rating_declarations_record(
        [syn.pr.RatingScope('AG "é中\\\n', None, "")],
        [syn.pr.UnclearedRatingSource('MIRROR "é中\\\n', "unverified")],
    )
    for record in (declarations, unicode_declarations):
        canonical, declarations_digest, input_digest, valid = admin.execute(
            "SELECT bond_credit_canonical_json(%s::jsonb), bond_credit_json_digest(%s::jsonb), "
            "bond_credit_rating_input_digest(%s::jsonb, %s), bond_credit_rating_declarations_valid(%s::jsonb)",
            [json.dumps(record), json.dumps(record), json.dumps(record), package_digest, json.dumps(record)],
        ).fetchone()
        assert canonical.encode() == c.canonical_json_bytes(record)
        assert declarations_digest == c.digest_of(record)
        assert input_digest == c.rating_input_manifest_digest(record, bundle.frames["source_packages"])
        assert valid
    assert c.digest_of(declarations) == bundle.manifest["rating_declarations_digest"]
    assert c.rating_input_manifest_digest(
        declarations, bundle.frames["source_packages"]
    ) == bundle.manifest["rating_input_digest"]
    assert not admin.execute(
        "SELECT bond_credit_rating_declarations_valid(%s::jsonb)",
        [json.dumps({**declarations, "version": "unknown"})],
    ).fetchone()[0]
    admin.rollback()


def _raw_ncen_copy(admin, source_id, **overrides):
    columns = [name for name, _ in c.NcenFilingEvidence.SPEC] + ["row_sha256"]
    expressions = [sql.Placeholder(name) if name in overrides else sql.Identifier(name) for name in columns]
    admin.execute(
        sql.SQL("INSERT INTO bond_default_ncen_filing ({}) SELECT {} "
                "FROM bond_default_ncen_filing WHERE filing_evidence_id = %(source_id)s").format(
            sql.SQL(", ").join(sql.Identifier(name) for name in columns),
            sql.SQL(", ").join(expressions),
        ),
        {"source_id": source_id, **overrides},
    )


def test_sql_ncen_row_contract_matches_python(store, admin):
    accession = "9999999901-26-009901"
    accepted_raw = "20260518163015"
    header = syn.ncen_header(accession, accepted_raw)
    ncen_package = syn._package(
        "sec_ncen_public_xml", "SYNTHETIC-SQL-NCEN-CONTRACT", "public_government_record",
        first_public=c.edgar_acceptance_to_utc(accepted_raw), basis="edgar_acceptance_datetime",
    )
    filing = syn.ncen_filing(
        ncen_package, header, "9999999911", ("S999999001",), "801-99911", accepted_raw,
        locator="sql-ncen-base",
    )
    store._run(lambda: store._insert("source_packages", [header, ncen_package]))
    store._run(lambda: store._insert("ncen_filings", [filing]))

    valid_locator = "sql-ncen-raw-valid"
    with admin.transaction():
        _raw_ncen_copy(
            admin, filing.filing_evidence_id,
            filing_evidence_id=c.NcenFilingEvidence.derive_id(
                ncen_package.package_id, accession, valid_locator),
            row_locator=valid_locator,
        )

    cases = (
        {
            "filing_evidence_id": uuid.uuid4(),
            "row_locator": "sql-ncen-arbitrary-id",
        },
        {
            "filing_evidence_id": c.NcenFilingEvidence.derive_id(
                ncen_package.package_id, accession, "sql-ncen-wrong-acceptance"),
            "row_locator": "sql-ncen-wrong-acceptance",
            "acceptance_raw": "20261101013000",
        },
        {
            "filing_evidence_id": c.NcenFilingEvidence.derive_id(
                ncen_package.package_id, accession, "sql-ncen-false-date-boundary"),
            "row_locator": "sql-ncen-false-date-boundary",
            "filing_date": dt.date(2026, 7, 2),
            "public_time_basis": "date_only_next_day_boundary",
            "public_available_at": dt.datetime(2026, 7, 3, 5, 0, tzinfo=UTC),
        },
        {
            "filing_evidence_id": c.NcenFilingEvidence.derive_id(
                ncen_package.package_id, accession, "sql-ncen-invalid-wall-time"),
            "row_locator": "sql-ncen-invalid-wall-time",
            "acceptance_raw": "20260518240000",
        },
    )
    for changes in cases:
        with pytest.raises(errors.CheckViolation), admin.transaction():
            _raw_ncen_copy(admin, filing.filing_evidence_id, **changes)


def test_sql_historical_ncen_selection_refuses_future_acceptance(store, admin):
    bundle, expected = V2_REGRESSIONS["historical_filing_accepted_after_cutoff"]
    assert expected == "ncen_provenance_invalid"
    p.prepare_bundle(store, bundle)
    selected = admin.execute(
        "SELECT bond_credit_ncen_selection(%s::uuid[], '{}'::uuid[], %s, %s, %s)",
        [[row.package_id for row in bundle.frames["source_packages"]], bundle.manifest["knowledge_cutoff"],
         "9999999911", syn.REPORT_DATE],
    ).fetchone()[0]
    assert selected["selected"] is not None
    assert selected["reason"] == "filing_acceptance_unattested"
    admin.rollback()
    assert _sql_validate_code(store, admin, bundle) == "ncen_provenance_invalid"


def _forge_copy(conn, source_id, *, publication_id, fingerprint_digest, code_digest,
                tamper_rationale=False, **manifest_overrides):
    """Copy a prepared publication under a new id, as the writer role could do directly in SQL."""
    names = [n for n, _ in c.MANIFEST_SPEC]
    overrides = {"publication_id": publication_id, "fingerprint_digest": fingerprint_digest,
                 "code_digest": code_digest, **manifest_overrides}
    kinds = dict(c.MANIFEST_SPEC)
    conn.execute(
        sql.SQL("INSERT INTO bond_credit_publications ({}) SELECT {} FROM bond_credit_publications "
                "WHERE publication_id = %s").format(
            sql.SQL(", ").join(sql.Identifier(n) for n in names),
            sql.SQL(", ").join(sql.Placeholder() if n in overrides else sql.Identifier(n) for n in names)),
        [*(overrides[n] if isinstance(overrides[n], Jsonb) else p._db_value(kinds[n], overrides[n])
           for n in names if n in overrides), source_id])
    for frame in c.OUTPUT_FRAMES:
        columns = [n for n, _ in c.FRAME_TYPES[frame].SPEC] + ["row_sha256"]
        exprs = [
            sql.SQL("CASE WHEN rationale IS NULL THEN NULL ELSE rationale || ' (tampered)' END")
            if tamper_rationale and frame == "coverage" and n == "rationale" else sql.Identifier(n)
            for n in columns
        ]
        conn.execute(
            sql.SQL("INSERT INTO {t} (publication_id, {cols}) SELECT %s, {exprs} FROM {t} "
                    "WHERE publication_id = %s").format(
                t=sql.Identifier(p.TABLES[frame]),
                cols=sql.SQL(", ").join(sql.Identifier(n) for n in columns),
                exprs=sql.SQL(", ").join(exprs)),
            [publication_id, source_id])


@pytest.mark.parametrize("malformation", ["invalid_date", "null_version", "null_rights_state"])
def test_sql_publication_rejects_malformed_rating_declaration_record(store, admin, malformation):
    q = syn.build_bundle()
    p.prepare_bundle(store, q)
    declarations = q.manifest.to_record()["rating_declarations"]
    source = {
        "source_ref": "SYNTHETIC-MIRROR-1",
        "rights_state": None if malformation == "null_rights_state" else "unverified",
        "coverage_start": "2026-02-30" if malformation == "invalid_date" else None,
        "coverage_end": "2026-03-31" if malformation == "invalid_date" else None,
    }
    malformed = {**declarations, "uncleared_rating_sources": [source]}
    if malformation == "null_version":
        malformed["version"] = None
    declaration_digest = c.digest_of(malformed)
    rating_input_digest = c.digest_of({
        "version": c.RATING_INPUT_MANIFEST_VERSION,
        "resolver_id": c.RATING_RESOLVER_ID,
        "rating_declarations": malformed,
        "rating_declarations_digest": declaration_digest,
        "rating_package_digest": c.rating_package_digest(q.frames["source_packages"]),
    })
    values = {
        **q.manifest.values,
        "rating_declarations": malformed,
        "rating_declarations_digest": declaration_digest,
        "rating_input_digest": rating_input_digest,
    }
    fingerprint = c.fingerprint_digest(values)
    publication_id = c.publication_id_for(fingerprint)
    with admin.transaction():
        _forge_copy(
            admin,
            q.publication_id,
            publication_id=publication_id,
            fingerprint_digest=fingerprint,
            code_digest=q.manifest["code_digest"],
            rating_declarations=Jsonb(malformed),
            rating_declarations_digest=declaration_digest,
            rating_input_digest=rating_input_digest,
        )
    with pytest.raises(errors.RaiseException, match="bond_credit_validate:rating_declarations_invalid"), \
            admin.transaction():
        admin.execute("SELECT bond_credit_validate(%s)", [publication_id])


def test_sql_validate_recomputes_hashes_and_identity_for_the_writer_role(schema, login_roles):
    q = syn.build_bundle()
    assert any(row.rationale is not None for row in q.frames["coverage"])
    with _connect_as(login_roles["writer"], schema) as writer:
        assert writer.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()[0] is False
        writer.rollback()
        store = p.PostgresPublicationStore(writer, schema)
        p.prepare_bundle(store, q)

        def consistent(code_hex):
            code = "sha256:" + code_hex * 64
            fingerprint = c.fingerprint_digest({**q.manifest.values, "code_digest": code})
            return code, fingerprint, c.publication_id_for(fingerprint)

        wrong_fp = "sha256:" + "e" * 64
        control = consistent("1")
        cases = {
            "control": (control, False, None),
            "arbitrary_uuid": ((*consistent("2")[:2], uuid.uuid4()), False, "publication_id_mismatch"),
            "wrong_fingerprint": (("sha256:" + "3" * 64, wrong_fp, c.publication_id_for(wrong_fp)), False,
                                  "fingerprint_mismatch"),
            "tampered_column_original_hash": (consistent("4"), True, "row_hash_mismatch:coverage"),
            "review_reproduction": (("sha256:" + "5" * 64, "sha256:" + "d" * 64, uuid.uuid4()), True,
                                    "fingerprint_mismatch"),
        }
        for label, ((code, fingerprint, pid), tamper, reason) in cases.items():
            with writer.transaction():
                _forge_copy(writer, q.publication_id, publication_id=pid, fingerprint_digest=fingerprint,
                            code_digest=code, tamper_rationale=tamper)
            if reason is None:
                with writer.transaction():
                    writer.execute("SELECT bond_credit_validate(%s)", [pid])
                assert store.state(pid).lifecycle_state == "validated", label
                continue
            with pytest.raises(errors.RaiseException, match=f"bond_credit_validate:{reason}"), writer.transaction():
                writer.execute("SELECT bond_credit_validate(%s)", [pid])
            with pytest.raises(errors.RaiseException, match="bond_credit_promote:not_validated"), \
                    writer.transaction():
                writer.execute("SELECT bond_credit_promote(%s, NULL)", [pid])
            assert store.state(pid).lifecycle_state == "prepared", label
        assert writer.execute("SELECT count(*) FROM bond_credit_current_pointer").fetchone()[0] == 0
        writer.rollback()


def test_consumed_packages_are_sealed(store, admin):
    q = syn.build_bundle()
    p.prepare_bundle(store, q)
    adj_pkg = next(x for x in q.frames["source_packages"] if x.source_family == "adjudication_batch")
    disputed = next(a for a in q.frames["adjudications"] if a.status == "disputed")
    extra = c.Adjudication.create(**{
        **{n: getattr(disputed, n) for n, _ in c.Adjudication.SPEC if n != "adjudication_id"},
        "rationale": "SYNTHETIC late addition to a consumed package",
    })
    grown = syn.reassemble(q, frames={"adjudications": (*q.frames["adjudications"], extra)},
                           code_digest="sha256:" + "e1" * 32)
    with pytest.raises(p.PublicationCollision, match="package_inventory_collision"):
        p.prepare_bundle(store, grown)
    names = [n for n, _ in c.Adjudication.SPEC]
    insert = sql.SQL("INSERT INTO bond_default_adjudication ({}, row_sha256) VALUES ({}, %s)").format(
        sql.SQL(", ").join(sql.Identifier(n) for n in names), sql.SQL(", ").join(sql.Placeholder() for _ in names))
    params = [list(v) if isinstance(v, tuple) else v for v in (getattr(extra, n) for n in names)]
    with pytest.raises(errors.RaiseException, match="is sealed by a publication"), admin.transaction():
        admin.execute(insert, [*params, extra.row_sha256()])
    # The original build is untouched and still validates.
    assert extra.package_id == adj_pkg.package_id
    assert p.validate_bundle(store, q.publication_id).lifecycle_state == "validated"

    receipt = q.manifest.receipt()
    weak = c.ValidationReceipt(**{**{n: getattr(receipt, n) for n, _ in receipt.SPEC}, "verdict": "partial"})
    cell = next(x for x in q.frames["coverage"] if x.state == "qualified")
    weak_cell = syn.replace_row(cell, validation_receipt_digest=weak.digest())
    rights = tuple(
        syn.replace_row(r, state="rights_unverified", bucket=None, action_date=None, public_known_at=None,
                        agency_source_ids=(), binding_link_ids=(), coverage_frontier=None,
                        action_input_digest=None) if r.state in ("missing", "observed", "carried_verified")
        else r for r in q.frames["ratings"]
    )
    declarations = syn.pr.rating_declarations_record(
        syn.RATING_SCOPES,
        syn.UNCLEARED_RATING_SOURCES,
    )
    unverified = syn.reassemble(
        q,
        frames={**syn._without_agency(q), "ratings": rights},
        rating_declarations=declarations,
        code_digest="sha256:" + "d4" * 32,
    )
    assert _sql_validate_code(store, admin, unverified) == "qualified_state_unsupported"
    follow = next(f for f in q.frames["followups"] if f.status == "nondefault_continuous")
    nport_only = tuple(oid for oid in follow.evidence_observation_ids
                       if next(o for o in q.frames["observations"] if o.observation_id == oid)
                       .observation_kind == "nport_holding")
    points = syn.reassemble(q, frames={"followups": tuple(
        syn.replace_row(f, evidence_observation_ids=nport_only) if f is follow else f
        for f in q.frames["followups"])}, code_digest="sha256:" + "d5" * 32)
    assert _sql_validate_code(store, admin, points) == "followup_invalid"
    # Partial receipts cannot back a qualified publication row at all (table CHECK).
    with pytest.raises(p.PublicationError):
        p.prepare_bundle(store, syn.reassemble(
            q, validation_receipt=weak,
            frames={"coverage": tuple(weak_cell if x is cell else x for x in q.frames["coverage"])}))


# ---------------------------------------------------------------------------
# Revocation, crash safety, malformed rows
# ---------------------------------------------------------------------------
def test_revocation_makes_readers_refuse(store, admin, login_roles, schema):
    q = syn.build_bundle()
    _promoted(store, q)
    store.revoke(q.publication_id, "SYNTHETIC corrected evidence", "sha256:" + "7" * 64)
    with pytest.raises(p.PublicationError, match="bond_credit_current_publication:revoked"):
        store.current()
    with pytest.raises(p.PublicationError, match="bond_credit_read_publication:revoked"):
        store.read(q.publication_id, allow_shadow=True)
    with (
        _connect_as(login_roles["reader"], schema) as reader,
        pytest.raises(errors.RaiseException, match="revoked"),
    ):
        reader.execute("SELECT * FROM bond_credit_current_publication()")
    with pytest.raises(p.PublicationError):
        store.revoke(q.publication_id, "again", "sha256:" + "7" * 64)
    with pytest.raises(errors.RaiseException), admin.transaction():
        admin.execute("DELETE FROM bond_credit_publication_revocations")
    replacement = syn.reassemble(q, code_digest="sha256:" + "8" * 64)
    assert _promoted(store, replacement, q.publication_id) == replacement.publication_id
    assert store.current()["publication_id"] == replacement.publication_id


BASE_TABLES = (*p.TABLES.values(), "bond_credit_publications", "bond_credit_current_pointer",
               "bond_credit_publication_revocations")
READ_FUNCTIONS = tuple(f"bond_credit_read_{frame}" for frame in (*c.INPUT_FRAMES, *c.OUTPUT_FRAMES))


def _read_all(conn, publication_id, allow_shadow=False):
    """Row counts of every guarded frame function (raises on the first refusal)."""
    return {
        fn: conn.execute(sql.SQL("SELECT count(*) FROM {}(%s, %s)").format(sql.Identifier(fn)),
                         [publication_id, allow_shadow]).fetchone()[0]
        for fn in READ_FUNCTIONS
    }


def test_serving_reader_reads_only_through_guarded_functions(store, admin, login_roles, schema):
    """Finding 3: no base-table SELECT; revoked/unvalidated/shadow refused by every frame function."""
    q = syn.build_bundle()
    partial = syn.build_bundle(quality_state="partial", with_receipt=False)
    prepared_only = syn.build_bundle(knowledge_cutoff=K0 + dt.timedelta(days=1))
    _promoted(store, q)
    p.prepare_bundle(store, partial)
    p.validate_bundle(store, partial.publication_id)
    p.prepare_bundle(store, prepared_only)
    functions = admin.execute(
        "SELECT p.proname, p.prosecdef, p.proconfig::text, p.proacl IS NOT NULL, "
        "       coalesce(bool_or(a.grantee = 0), false) "
        "FROM pg_proc p LEFT JOIN LATERAL aclexplode(p.proacl) a ON true "
        "WHERE p.pronamespace = %s::regnamespace AND p.proname = ANY(%s) GROUP BY 1, 2, 3, 4",
        [schema, list(READ_FUNCTIONS)]).fetchall()
    admin.rollback()
    assert len(functions) == len(READ_FUNCTIONS)
    for name, definer, config, has_acl, public_execute in functions:
        assert definer and "search_path=" in config and has_acl and not public_execute, name
    with _connect_as(login_roles["reader"], schema) as reader:
        for table in BASE_TABLES:
            with pytest.raises(errors.InsufficientPrivilege), reader.transaction():
                reader.execute(sql.SQL("SELECT 1 FROM {} LIMIT 1").format(sql.Identifier(table)))
        served = p.PostgresPublicationStore(reader, schema).read(q.publication_id)
        assert served.canonical_bytes() == q.canonical_bytes()
        assert _read_all(reader, q.publication_id)["bond_credit_read_ratings"] == 2 * len(q.panel_grid)
        reader.rollback()
        for fn in READ_FUNCTIONS:  # shadow needs the explicit flag; unvalidated is never served
            for pid, allow, match in ((partial.publication_id, False, "shadow_build_requires_allow_shadow"),
                                      (prepared_only.publication_id, True, "not_validated")):
                with pytest.raises(errors.RaiseException, match=match), reader.transaction():
                    reader.execute(sql.SQL("SELECT * FROM {}(%s, %s)").format(sql.Identifier(fn)), [pid, allow])
        shadow = p.PostgresPublicationStore(reader, schema).read(partial.publication_id, allow_shadow=True)
        assert shadow.canonical_bytes() == partial.canonical_bytes()
        reader.rollback()
    store.revoke(q.publication_id, "SYNTHETIC corrected evidence", "sha256:" + "7" * 64)
    with _connect_as(login_roles["reader"], schema) as reader:
        for fn in READ_FUNCTIONS:
            for allow in (False, True):
                with pytest.raises(errors.RaiseException, match="revoked"), reader.transaction():
                    reader.execute(sql.SQL("SELECT * FROM {}(%s, %s)").format(sql.Identifier(fn)),
                                   [q.publication_id, allow])
        with pytest.raises(p.PublicationError, match="bond_credit_read_publication:revoked"):
            p.PostgresPublicationStore(reader, schema).read(q.publication_id, allow_shadow=True)
    with _connect_as(login_roles["auditor"], schema) as auditor:
        for table in BASE_TABLES:
            auditor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))).fetchone()
        revoked = auditor.execute(
            "SELECT count(*) FROM bond_default_event_v1 WHERE publication_id = %s", [q.publication_id]).fetchone()[0]
        assert revoked == len(q.frames["events"])
        with pytest.raises(errors.InsufficientPrivilege), auditor.transaction():
            auditor.execute("INSERT INTO bond_credit_publication_revocations (publication_id) VALUES (%s)",
                            [partial.publication_id])


V2_OUTPUT_TABLES = {
    "family_contexts": "bond_default_family_context_v2",
    "family_evidence": "bond_default_family_evidence_v2",
    "proposal_evidence": "bond_default_proposal_evidence_v2",
    "exchange_relations": "bond_default_exchange_relation_v2",
}


def test_v2_tables_immutable_and_served_only_through_guarded_readers(store, admin, login_roles, schema):
    """§9 item 6: the four new publication tables (and the N-CEN ledger) are append-only and
    sealed; writers cannot rewrite them; the serving reader has no raw SELECT and every new
    guarded reader refuses revoked, unvalidated and unflagged shadow publications."""
    ex = syn.exchange_bundle()
    _promoted(store, ex)
    partial = syn.exchange_bundle(syn.build_bundle(quality_state="partial", with_receipt=False))
    p.prepare_bundle(store, partial)
    p.validate_bundle(store, partial.publication_id)
    prepared_only = syn.reassemble(ex, code_digest="sha256:" + "b6" * 32)
    p.prepare_bundle(store, prepared_only)
    tables = {**V2_OUTPUT_TABLES, "ncen_filings": "bond_default_ncen_filing"}
    for frame, table in tables.items():
        assert len(ex.frames[frame]) > 0, frame
        ident = sql.Identifier(table)
        for statement in (sql.SQL("UPDATE {} SET row_sha256 = row_sha256").format(ident),
                          sql.SQL("DELETE FROM {}").format(ident), sql.SQL("TRUNCATE {} CASCADE").format(ident)):
            with pytest.raises(errors.RaiseException), admin.transaction():
                admin.execute(statement)
    for frame, table in V2_OUTPUT_TABLES.items():  # no child row once the publication left 'prepared'
        with pytest.raises(errors.RaiseException), admin.transaction():
            admin.execute(sql.SQL("INSERT INTO {t} SELECT * FROM {t} WHERE publication_id = %s LIMIT 1").format(
                t=sql.Identifier(table)), [ex.publication_id])
    with _connect_as(login_roles["writer"], schema) as writer:
        for table in tables.values():
            with pytest.raises(errors.InsufficientPrivilege), writer.transaction():
                writer.execute(sql.SQL("UPDATE {} SET row_sha256 = row_sha256").format(sql.Identifier(table)))
    readers = {frame: f"bond_credit_read_{frame}" for frame in tables}
    with _connect_as(login_roles["reader"], schema) as reader:
        for table in tables.values():
            with pytest.raises(errors.InsufficientPrivilege), reader.transaction():
                reader.execute(sql.SQL("SELECT 1 FROM {} LIMIT 1").format(sql.Identifier(table)))
        for frame, fn in readers.items():
            count = reader.execute(sql.SQL("SELECT count(*) FROM {}(%s, %s)").format(sql.Identifier(fn)),
                                   [ex.publication_id, False]).fetchone()[0]
            assert count == len(ex.frames[frame]), frame
        reader.rollback()
        for fn in readers.values():
            for pid, allow, match in ((partial.publication_id, False, "shadow_build_requires_allow_shadow"),
                                      (prepared_only.publication_id, True, "not_validated")):
                with pytest.raises(errors.RaiseException, match=match), reader.transaction():
                    reader.execute(sql.SQL("SELECT * FROM {}(%s, %s)").format(sql.Identifier(fn)), [pid, allow])
    store.revoke(ex.publication_id, "SYNTHETIC corrected exchange evidence", "sha256:" + "6" * 64)
    with _connect_as(login_roles["reader"], schema) as reader:
        for fn in readers.values():
            for allow in (False, True):
                with pytest.raises(errors.RaiseException, match="revoked"), reader.transaction():
                    reader.execute(sql.SQL("SELECT * FROM {}(%s, %s)").format(sql.Identifier(fn)),
                                   [ex.publication_id, allow])


def test_transaction_crash_leaves_pointer_unchanged(dsn, schema, store, admin):
    q = syn.build_bundle()
    newer = syn.build_bundle(knowledge_cutoff=K0 + dt.timedelta(days=1))
    _promoted(store, q)
    p.prepare_bundle(store, newer)
    p.validate_bundle(store, newer.publication_id)
    victim = _connect_as(dsn, schema)
    try:
        victim.execute("SELECT bond_credit_promote(%s, %s)", [newer.publication_id, q.publication_id])
        pid = victim.info.backend_pid
        with connect_disposable(dsn, autocommit=True) as killer:
            assert killer.execute("SELECT pg_terminate_backend(%s)", [pid]).fetchone()[0]
        with pytest.raises(psycopg.OperationalError):
            victim.commit()
    finally:
        victim.close()
    assert _pointer(admin) == q.publication_id
    # A failure inside prepare leaves no rows either.
    bad = syn.build_bundle(alter_output=True)
    with pytest.raises(p.PublicationCollision):
        p.prepare_bundle(store, bad)
    assert p.promote_bundle(store, newer.publication_id, expected_pointer=q.publication_id)


def _insert(table: str, **columns: str) -> str:
    """INSERT with raw SQL value fragments (test-only; no user input)."""
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(columns.values())})"


def test_malformed_rows_rejected(store, admin):
    partial = syn.build_bundle(quality_state="partial", with_receipt=False)
    p.prepare_bundle(store, partial)
    # Fresh, unsealed packages (consumed packages refuse rows before any CHECK runs).
    edgar_pkg = syn.new_package("sec_edgar_document", "SYNTHETIC-MALFORMED-EDGAR", "public_government_record")
    adj_pkg = syn.new_package("adjudication_batch", "SYNTHETIC-MALFORMED-ADJ")
    store._run(lambda: store._insert("source_packages", [edgar_pkg, adj_pkg]))
    pid = f"'{partial.publication_id}'"
    h = "'" + "e" * 64 + "'"
    digest = "'sha256:" + "e" * 64 + "'"
    new_id = "gen_random_uuid()"
    ids = "ARRAY[gen_random_uuid()]"
    package = {
        "package_id": new_id, "source_family": "'sec_edgar_index'", "external_id": "'x'", "content_sha256": h, "raw_sha256": h,
        "member_sha256s": "'{}'", "rights_state": "'public_government_record'", "parser_version": "'p'",
        "schema_version": "'s'", "retrieved_at": "now()", "first_verified_public_at": "now()",
        "public_time_basis": "'internal_record'", "public_time_evidence": "'e'", "raw_locator": "'a/b'", "row_sha256": h,
    }
    observation = {
        "observation_id": new_id, "package_id": f"'{edgar_pkg.package_id}'", "member_name": "'m'", "row_locator": "'r'",
        "observation_kind": "'edgar_passage'", "semantic_key": "'k'", "date_precision": "'unknown'",
        "public_available_at": "now()", "public_time_basis": "'internal_record'", "first_seen_at": "now()",
        "field_presence": "'{}'", "revision_kind": "'original'", "document_location": "'loc'", "document_sha256": h,
        "row_sha256": h,
    }
    presence = "'{\"nport_is_default\": \"null\", \"nport_arrears_or_deferral\": \"null\", \"nport_paid_in_kind\": \"null\"}'"
    event = {
        "publication_id": pid, "security_id": new_id, "episode_id": new_id, "cusip9": "'ZZ#SYN014'", "obligor_id": "'o'",
        "issuer_episode_id": new_id, "primary_type": "'payment_default'", "corroboration_flags": "'{}'",
        "admission_status": "'accepted_event'", "timing_class": "'incident'", "onset_lower_exclusive": "'2026-03-31'",
        "onset_upper_inclusive": "'2026-06-30'", "onset_lower_evidence_ids": ids,
        "evidence_known_at": "now()", "link_known_at": "now()",
        "evidence_observation_ids": ids, "link_ids": ids, "adjudication_ids": ids, "resolution_refs": "'{}'",
        "proposal_evidence_ids": "'{}'", "exchange_relation_ids": "'{}'", "dependency_digest": digest,
        "event_input_digest": digest, "row_sha256": h,
    }
    # A fully valid event row built from one set of ids (e/l/a); variants change one field.
    valid_event = {
        **event, "timing_class": "'interval_uncertain'", "onset_lower_evidence_ids": "ARRAY[e]",
        "evidence_observation_ids": "ARRAY[e]", "link_ids": "ARRAY[l]", "adjudication_ids": "ARRAY[a]",
        "event_input_digest": ("bond_credit_event_input_digest(ARRAY[e], ARRAY[l], ARRAY[a], '{}'::uuid[], "
                               f"'{{}}'::uuid[], {digest})"),
    }

    def insert_event(**changes: str) -> str:
        values = {**valid_event, **changes}
        return (
            f"INSERT INTO bond_default_event_v1 ({', '.join(values)}) SELECT {', '.join(values.values())} "
            "FROM (SELECT gen_random_uuid() AS e, gen_random_uuid() AS l, gen_random_uuid() AS a) AS ids"
        )

    coverage = {
        "publication_id": pid, "period_label": "'2025'", "source": "'all'", "event_type": "'all'", "rating_stratum": "'all'",
        "exposure_cohort": "'all'", "state": "'partial'", "denominator_basis": "'none'", "exposed_issue_months": "0",
        "event_count": "0", "unlinked_count": "0", "date_uncertain_count": "0", "unknown_outcome_issue_months": "0",
        "rationale": "'x'", "row_sha256": h,
    }
    bad = [
        ("agency package with unverified rights", errors.CheckViolation, _insert(
            "bond_default_source_package",
            **{**package, "source_family": "'agency_rocr_xbrl'", "rights_state": "'unverified'"})),
        ("path traversal locator", errors.CheckViolation, _insert(
            "bond_default_source_package", **{**package, "raw_locator": "'../etc/passwd'"})),
        ("invalid CUSIP checksum", errors.CheckViolation, _insert(
            "bond_credit_observation", **{**observation, "cusip9": "'ZZ#SYN010'"})),
        ("nport row in an EDGAR package", errors.RaiseException, _insert(
            "bond_credit_observation",
            **{**observation, "row_locator": "'r2'", "observation_kind": "'nport_holding'",
               "accession_number": "'a'", "holding_id": "'h'", "report_date": "'2026-03-31'",
               "field_presence": presence, "document_location": "NULL", "document_sha256": "NULL"})),
        ("unsorted uuid array", errors.CheckViolation, _insert(
            "bond_default_adjudication", adjudication_id=new_id, package_id=f"'{adj_pkg.package_id}'",
            subject_kind="'candidate'", subject_id=new_id, status="'candidate'", policy_digest=digest,
            reviewer_id="'r'", reviewer_role="'human_reviewer'", adjudicated_at="now()", rationale="'x'",
            evidence_observation_ids=(
                "ARRAY['ffffffff-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000']::uuid[]"
            ),
            link_ids="'{}'", proposal_evidence_ids="'{}'", row_sha256=h)),
        # W0 amendment 1 (bundle v2) table-level refusals (the SQL-only path; no Python validation).
        ("distressed exchange flag without a persisted relation", errors.CheckViolation, insert_event(
            corroboration_flags="ARRAY['distressed_exchange']")),
        ("alias spell without a relation", errors.CheckViolation, insert_event(alias_spell_id="gen_random_uuid()")),
        ("consensus flag without persisted proposal evidence", errors.CheckViolation, insert_event(
            corroboration_flags="ARRAY['nport_consensus_state']")),
        ("accepted evidence by the policy engine", errors.CheckViolation, _insert(
            "bond_default_adjudication", adjudication_id=new_id, package_id=f"'{adj_pkg.package_id}'",
            subject_kind="'corroboration'", subject_id=new_id, status="'accepted_evidence'", policy_digest=digest,
            reviewer_id="'r'", reviewer_role="'policy_rule_engine'", adjudicated_at="now()", rationale="'x'",
            evidence_observation_ids="ARRAY[gen_random_uuid()]", link_ids="ARRAY[gen_random_uuid()]",
            proposal_evidence_ids="'{}'", support_valid_from="'2026-05-15'", row_sha256=h)),
        ("issue scope decision not citing its link", errors.CheckViolation, _insert(
            "bond_default_adjudication", adjudication_id=new_id, package_id=f"'{adj_pkg.package_id}'",
            subject_kind="'issue_scope'", subject_id=new_id, status="'accepted_evidence'", policy_digest=digest,
            reviewer_id="'r'", reviewer_role="'human_reviewer'", adjudicated_at="now()", rationale="'x'",
            evidence_observation_ids="ARRAY[gen_random_uuid()]", link_ids="ARRAY[gen_random_uuid()]",
            proposal_evidence_ids="'{}'", row_sha256=h)),
        # §9 item 4: a corroboration decision citing a proposal (cyclic proposal/corroboration dependency).
        ("corroboration citing a proposal", errors.CheckViolation, _insert(
            "bond_default_adjudication", adjudication_id=new_id, package_id=f"'{adj_pkg.package_id}'",
            subject_kind="'corroboration'", subject_id=new_id, status="'accepted_evidence'", policy_digest=digest,
            reviewer_id="'r'", reviewer_role="'human_reviewer'", adjudicated_at="now()", rationale="'x'",
            evidence_observation_ids="ARRAY[gen_random_uuid()]", link_ids="ARRAY[gen_random_uuid()]",
            proposal_evidence_ids="ARRAY[gen_random_uuid()]", support_valid_from="'2026-03-01'", row_sha256=h)),
        ("exchange relation onto itself", errors.CheckViolation, _insert(
            "bond_default_exchange_relation_v2", publication_id=pid, relation_id=new_id, old_security_id=new_id,
            old_cusip9="'ZZ#SYN014'", new_security_id=new_id, new_cusip9="'ZZ#SYN014'", episode_id=new_id,
            alias_spell_id=new_id, old_link_id=new_id, new_link_id=new_id,
            exchange_document_observation_ids="ARRAY[gen_random_uuid()]", pairing_adjudication_id=new_id,
            exchange_effective_date="'2026-06-10'", valid_from="'2026-06-10'", evidence_known_at="now()",
            row_sha256=h)),
        ("event timing class not derived", errors.CheckViolation, _insert("bond_default_event_v1", **event)),
        ("exact onset date with NULL lower bound", errors.CheckViolation, _insert(
            "bond_default_event_v1",
            **{**event, "timing_class": "'prevalent'", "onset_date": "'2026-05-15'",
               "onset_lower_exclusive": "NULL", "onset_upper_inclusive": "'2026-05-15'",
               "onset_lower_evidence_ids": "'{}'"})),
        # Finding 7: a lower bound is never invented, and its provenance is event evidence.
        ("lower bound without provenance", errors.CheckViolation, insert_event(onset_lower_evidence_ids="'{}'")),
        ("lower-bound provenance outside event evidence", errors.CheckViolation, insert_event(
            onset_lower_evidence_ids="ARRAY[l]")),
        ("provenance without a lower bound", errors.CheckViolation, insert_event(
            timing_class="'prevalent'", onset_lower_exclusive="NULL")),
        ("resolution without its own knowledge time", errors.CheckViolation, insert_event(
            resolution_date="'2026-07-31'", resolution_refs="ARRAY[e]")),
        ("gap reentry without gap months", errors.CheckViolation, _insert(
            "bond_default_exit_evidence_v1", publication_id=pid, security_id=new_id,
            last_panel_month="'2026-01-01'", cusip9="'ZZ#SYN014'", primary_reason="'unknown'",
            flags="ARRAY['observed_gap_reentry','unknown']", next_observed_month="'2026-04-01'",
            gap_months="NULL", evidence_observation_ids="'{}'", known_at="now()", row_sha256=h)),
        ("rating bucket without verified action", errors.CheckViolation, _insert(
            "bond_rating_history_public_v1", publication_id=pid, cusip_id="'ZZ#SYN014'", month="'2025-01-01'",
            view_kind="'public_pit'", bucket="'D'", state="'missing'", agency_source_ids="'{}'",
            binding_link_ids="'{}'", row_sha256=h)),
        ("binding links without relied actions", errors.CheckViolation, _insert(
            "bond_rating_history_public_v1", publication_id=pid, cusip_id="'ZZ#SYN014'", month="'2025-01-01'",
            view_kind="'public_pit'", state="'missing'", agency_source_ids="'{}'",
            binding_link_ids="ARRAY[gen_random_uuid()]", row_sha256=h)),
        ("qualified coverage without receipt", errors.CheckViolation, _insert(
            "bond_default_coverage_v1",
            **{**coverage, "state": "'qualified'", "denominator_basis": "'panel_exposure'",
               "denominator_count": "1"})),
        ("NULL coverage dimension", errors.NotNullViolation, _insert(
            "bond_default_coverage_v1", **{**coverage, "source": "NULL"})),
        ("bad digest format", errors.CheckViolation, _insert(
            "bond_credit_publication_revocations", publication_id=pid, reason="'x'", evidence_digest="'md5:abc'")),
    ]
    # Controls: the base rows are valid, so each failure is caused by the single malformed field.
    with admin.transaction():
        admin.execute(_insert("bond_default_source_package", **package))
        admin.execute(_insert("bond_credit_observation", **observation))
        admin.execute(insert_event())
        admin.execute(insert_event(  # a resolved event with its own knowledge time is also valid
            resolution_date="'2026-07-31'", resolution_refs="ARRAY[e]", resolution_known_at="now()"))
        admin.execute(_insert("bond_default_coverage_v1", **coverage))
        raise psycopg.Rollback()
    for label, error, statement in bad:
        with pytest.raises(error), admin.transaction():
            admin.execute(statement)
        assert admin.info.transaction_status == psycopg.pq.TransactionStatus.IDLE, label

"""Opt-in ACL/trigger regressions against ONE authorized synthetic PG16 fixture.

Run with DSH_OWNER_EVIDENCE_PG_TEST=1. No DSN/env credential lookup, production
roles, public objects, other test schemas, container lifecycle or live data.
Operational role names in the DDL are mapped to unique disposable NOLOGIN roles.
"""

from __future__ import annotations

import json
import os
import subprocess
from contextlib import contextmanager
from uuid import uuid4

import pytest
from test_bond_default_owner_evidence import (
    OWNER,
    ROOT,
    decision,
    export,
    oracle_publication_id,
    producer_source_sha,
    proposal,
    publication_pins,
    rehash,
)

from src.bonds.default_owner_evidence import (
    PostgresOwnerEvidenceStore,
    build_bundle,
    canonical_json,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("DSH_OWNER_EVIDENCE_PG_TEST") != "1",
    reason="explicit opt-in required for the literal disposable PostgreSQL fixture",
)


def _connect():
    import psycopg

    connection = psycopg.connect(
        host="127.0.0.1", port=54339, dbname="bond_default_review_test", user="postgres",
        password="dsh-local-review-test", connect_timeout=5, autocommit=True,
        options="-c lock_timeout=5000 -c statement_timeout=60000",
    )
    assert connection.execute("SELECT current_database(), session_user, current_user").fetchone() == (
        "bond_default_review_test", "postgres", "postgres",
    )
    assert connection.execute("SHOW server_version").fetchone()[0] == "16.15"
    return connection


@pytest.fixture(scope="module")
def pg():
    import psycopg
    from psycopg import sql

    inspected = subprocess.run(
        ["docker", "inspect", "dsh-bde-el-pg-20260930"], check=True,
        capture_output=True, text=True, timeout=10,
    )
    container = json.loads(inspected.stdout)[0]
    assert container["Config"]["Labels"]["dsh.test"] == "bde-el-20260930"
    assert container["NetworkSettings"]["Ports"]["5432/tcp"] == [
        {"HostIp": "127.0.0.1", "HostPort": "54339"},
    ]
    suffix = uuid4().hex
    schema = "correction_evidence_test_" + suffix
    roles = {kind: "ce_test_" + suffix + "_" + kind for kind in ("owner", "writer", "reader")}
    admin = _connect()
    connections = []
    created_roles = []
    created_schema = False
    try:
        for role in roles.values():
            admin.execute(sql.SQL(
                "CREATE ROLE {} NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION"
            ).format(sql.Identifier(role)))
            created_roles.append(role)
        admin.execute(sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
            sql.Identifier(schema), sql.Identifier(roles["owner"]),
        ))
        created_schema = True
        for kind in ("writer", "reader"):
            admin.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(roles[kind]),
            ))
        for kind in ("owner", "writer", "reader"):
            connection = _connect()
            connections.append(connection)
            connection.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(roles[kind])))
            assert connection.execute("SELECT current_user").fetchone()[0] == roles[kind]
        owner, writer, reader = connections
        ddl = (ROOT / "schemas/bond_default_owner_evidence_v1.sql").read_text(encoding="utf-8")
        # Substitute ONLY operational grant recipients; never touch their real roles.
        ddl = ddl.replace("worker_writer", roles["writer"])
        ddl = ddl.replace("app_runtime", roles["reader"]).replace("app_analytics_ro", roles["reader"])
        owner.execute(sql.SQL("SET search_path TO {}, pg_catalog").format(sql.Identifier(schema)))
        with owner.transaction():
            owner.execute(ddl)
        table = lambda suffix: sql.Identifier(schema, "bond_default_owner_evidence_v1_" + suffix)
        # Recreate the old unsafe ACL locally, then prove reapply removes it.
        owner.execute(sql.SQL("GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON {},{} TO {}").format(
            table("pointer"), table("pointer_tokens"), sql.Identifier(roles["writer"]),
        ))
        owner.execute(sql.SQL("GRANT SELECT,INSERT,UPDATE,DELETE ON {},{} TO {}").format(
            table("builds"), table("events"), sql.Identifier(roles["reader"]),
        ))
        owner.execute(sql.SQL("GRANT EXECUTE ON FUNCTION {}(uuid,uuid) TO {}").format(
            sql.Identifier(schema, "bond_default_owner_evidence_v1_point"), sql.Identifier(roles["reader"]),
        ))
        assert admin.execute("SELECT has_table_privilege(%s,%s,'UPDATE')", (
            roles["writer"], schema + ".bond_default_owner_evidence_v1_pointer",
        )).fetchone()[0]
        with owner.transaction():
            owner.execute(ddl)
        writer.execute("SET search_path TO public")
        reader.execute("SET search_path TO public")
        assert writer.execute("SHOW search_path").fetchone()[0] == "public"
        assert writer.execute("SELECT current_user <> %s", (roles["owner"],)).fetchone()[0]
        for role in (roles["writer"], roles["reader"]):
            assert not admin.execute("SELECT pg_has_role(%s,%s,'MEMBER')", (
                role, roles["owner"],
            )).fetchone()[0]
            assert not admin.execute("SELECT has_schema_privilege(%s,%s,'CREATE')", (
                role, schema,
            )).fetchone()[0]
        yield {"schema": schema, "roles": roles, "admin": admin, "owner": owner,
               "writer": writer, "reader": reader, "table": table, "psycopg": psycopg, "sql": sql}
    finally:
        for connection in connections:
            connection.close()  # Also drops this session's temporary shadow relations.
        if created_schema:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        for role in reversed(created_roles):
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
        assert not admin.execute("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=%s)", (
            schema,
        )).fetchone()[0]
        assert admin.execute("SELECT count(*) FROM pg_roles WHERE rolname = ANY(%s)", (
            created_roles,
        )).fetchone()[0] == 0
        admin.close()
        print("PG16.15 fixture cleanup verified: " + json.dumps({"schema": schema, "roles": created_roles}))


def _bundle(value, cutoff):
    bundle = build_bundle(value, expected_owner_sub=OWNER, code_revision="local-pg-correction-synthetic",
                          knowledge_cutoff=cutoff)
    assert bundle["code_digest"] == producer_source_sha()  # Fresh runtime bytes, not historical receipts.
    assert bundle["publication_id"] == oracle_publication_id(publication_pins(bundle))
    return bundle


@pytest.fixture(scope="module")
def prepared(pg):
    writer, table, sql = pg["writer"], pg["table"], pg["sql"]
    # A malicious caller's implicit temp path must not shadow any guarded relation.
    for suffix in ("builds", "events", "validations", "pointer", "pointer_tokens"):
        writer.execute(sql.SQL("CREATE TEMP TABLE {} (shadow_only integer)").format(
            sql.Identifier("bond_default_owner_evidence_v1_" + suffix),
        ))
    zero_export = export(zero=True)
    zero_export["proposals"][0]["source_obligation_id"] = None
    zero_export["proposals"][0]["citations"] = []
    zero = _bundle(rehash(zero_export), "2020-12-01T00:00:00Z")
    two_export = export()
    second = proposal("pg-second")
    second["proposed_cusips"] = ["594918104"]
    accepted = decision(second, label="pg-second", cusip="594918104")
    two_export["proposals"].append(second)
    two_export["decisions"].append(accepted)
    two_export["accepted_events"].append(accepted)
    two = _bundle(rehash(two_export), "2021-01-01T00:00:00Z")
    store = PostgresOwnerEvidenceStore(writer, expected_owner_sub=OWNER, schema=pg["schema"])
    for bundle, count in ((zero, 0), (two, 2)):
        assert store.prepare(bundle)["accepted_event_count"] == count
        receipt = store.validate(bundle["publication_id"])
        assert receipt["accepted_event_count"] == count and receipt["state"] == "validated"
        assert store.prepare(bundle)["accepted_event_count"] == count  # Sealed replay is idempotent.
        assert store.validate(bundle["publication_id"]) == receipt
        assert writer.execute(sql.SQL("SELECT count(*) FROM {} WHERE publication_id=%s").format(
            table("events")), (bundle["publication_id"],),
        ).fetchone()[0] == count
    assert writer.execute(sql.SQL("SELECT count(*) FROM {}").format(table("pointer"))).fetchone()[0] == 0
    return {"zero": zero, "two": two, "store": store}


@contextmanager
def _refused(pg, connection, states, text=None):
    with pytest.raises(pg["psycopg"].Error) as caught, connection.transaction():
        yield
    assert caught.value.sqlstate in states, (caught.value.sqlstate, str(caught.value))
    if text is not None:
        assert text in str(caught.value)


def _point(pg, target, prior, connection=None):
    return (connection or pg["writer"]).execute(pg["sql"].SQL("SELECT {}(%s::uuid,%s::uuid)").format(
        pg["sql"].Identifier(pg["schema"], "bond_default_owner_evidence_v1_point"),
    ), (target, prior))


def test_real_writer_private_schema_replay_and_safe_cas(pg, prepared):
    zero, two = prepared["zero"], prepared["two"]
    unvalidated = _bundle(export(), "2022-01-01T00:00:00Z")
    prepared["store"].prepare(unvalidated)
    bad_receipt = {"publication_id": unvalidated["publication_id"], "bundle_sha256": "d" * 64,
                   "events_sha256": unvalidated["events_sha256"], "accepted_event_count": 1,
                   "verification": "replayed_export_and_stored_issue_rows"}
    with _refused(pg, pg["writer"], {"P0001"}, "matching persisted"):
        pg["writer"].execute(pg["sql"].SQL(
            "INSERT INTO {} (publication_id,bundle_sha256,events_sha256,accepted_event_count,receipt) "
            "VALUES (%s,%s,%s,%s,%s::jsonb)"
        ).format(pg["table"]("validations")), (
            bad_receipt["publication_id"], bad_receipt["bundle_sha256"], bad_receipt["events_sha256"],
            bad_receipt["accepted_event_count"], canonical_json(bad_receipt),
        ))
    with _refused(pg, pg["writer"], {"P0001"}, "not validated"):
        _point(pg, unvalidated["publication_id"], None)
    _point(pg, zero["publication_id"], None)
    with _refused(pg, pg["writer"], {"P0001"}, "CAS mismatch"):
        _point(pg, two["publication_id"], None)
    with _refused(pg, pg["writer"], {"P0001"}, "not validated"):
        _point(pg, unvalidated["publication_id"], zero["publication_id"])
    _point(pg, two["publication_id"], zero["publication_id"])
    with _refused(pg, pg["writer"], {"P0001"}, "cutoff regression"):
        _point(pg, zero["publication_id"], two["publication_id"])
    with _refused(pg, pg["writer"], {"P0001"}, "CAS mismatch"):
        _point(pg, zero["publication_id"], zero["publication_id"])
    with _refused(pg, pg["writer"], {"P0001"}, "not validated"):
        _point(pg, None, two["publication_id"])
    current = pg["reader"].execute(pg["sql"].SQL("SELECT cusip9,economic_authority FROM {} ORDER BY cusip9").format(
        pg["sql"].Identifier(pg["schema"], "bond_default_owner_events_v1_current"),
    )).fetchall()
    assert current == [("037833100", False), ("594918104", False)]
    assert pg["owner"].execute(pg["sql"].SQL("SELECT count(*) FROM {}").format(
        pg["table"]("pointer_tokens"),
    )).fetchone()[0] == 0
    for suffix in ("builds", "events", "validations", "pointer", "pointer_tokens"):
        assert pg["writer"].execute(pg["sql"].SQL("SELECT count(*) FROM pg_temp.{}").format(
            pg["sql"].Identifier("bond_default_owner_evidence_v1_" + suffix),
        )).fetchone()[0] == 0
    print("Real non-owner store: prepare/validate/replay 0 and 2; private schema/temp-shadow safe; CAS advance/refusals pass")


def test_real_writer_minimal_row_lock_and_seal(pg, prepared):
    writer, sql, table = pg["writer"], pg["sql"], pg["table"]
    build_name = pg["schema"] + ".bond_default_owner_evidence_v1_builds"
    role = pg["roles"]["writer"]
    assert not pg["admin"].execute("SELECT has_table_privilege(%s,%s,'UPDATE')", (role, build_name)).fetchone()[0]
    assert pg["admin"].execute("SELECT has_column_privilege(%s,%s,'publication_id','UPDATE')", (
        role, build_name,
    )).fetchone()[0]
    assert not pg["admin"].execute("SELECT has_column_privilege(%s,%s,'payload','UPDATE')", (
        role, build_name,
    )).fetchone()[0]
    with writer.transaction():
        assert writer.execute(sql.SQL("SELECT publication_id FROM {} WHERE publication_id=%s FOR UPDATE").format(
            table("builds")), (prepared["two"]["publication_id"],),
        ).fetchone() is not None
    with _refused(pg, writer, {"P0001"}, "append-only"):
        writer.execute(sql.SQL("UPDATE {} SET publication_id=publication_id").format(table("builds")))
    with _refused(pg, writer, {"42501"}):
        writer.execute(sql.SQL("UPDATE {} SET payload=payload").format(table("builds")))
    event = prepared["two"]["events"][0]
    with _refused(pg, writer, {"P0001"}, "sealed"):
        writer.execute(sql.SQL(
            "INSERT INTO {} (publication_id,event_id,decision_id,proposal_id,cusip9,event_date,event_type,link_sha256,payload) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING"
        ).format(table("events")), (prepared["two"]["publication_id"],) + tuple(
            event[key] for key in ("event_id", "decision_id", "proposal_id", "cusip9", "event_date", "event_type", "link_sha256")
        ) + (canonical_json(event),))
    for suffix in ("builds", "events", "validations"):
        for operation in ("DELETE FROM {}", "TRUNCATE {} CASCADE"):
            with _refused(pg, writer, {"42501"}):
                writer.execute(sql.SQL(operation).format(table(suffix)))
    print("P1: narrow UPDATE(publication_id) permits real FOR UPDATE; rewrites/deletes/truncates and sealed inserts refused")


def test_real_roles_cannot_mint_tokens_or_mutate_pointer_and_readers_get_views_only(pg, prepared):
    sql, table = pg["sql"], pg["table"]
    with _refused(pg, pg["writer"], {"42501"}):
        pg["writer"].execute(sql.SQL("INSERT INTO {} VALUES ('bond_default_owner_evidence_v1',pg_backend_pid())").format(
            table("pointer_tokens"),
        ))
    for suffix in ("pointer_tokens", "pointer"):
        for operation in ("UPDATE {} SET product=product", "DELETE FROM {}", "TRUNCATE {}"):
            with _refused(pg, pg["writer"], {"42501"}):
                pg["writer"].execute(sql.SQL(operation).format(table(suffix)))
    with _refused(pg, pg["writer"], {"42501"}):
        pg["writer"].execute(sql.SQL("UPDATE {} SET publication_id=%s").format(
            table("pointer")), (prepared["zero"]["publication_id"],),
        )
    with _refused(pg, pg["writer"], {"42501"}):
        pg["writer"].execute(sql.SQL("INSERT INTO {} (product,publication_id) VALUES (%s,%s)").format(
            table("pointer")), ("bond_default_owner_evidence_v1", prepared["zero"]["publication_id"]),
        )
    with _refused(pg, pg["writer"], {"42501"}):
        pg["writer"].execute(sql.SQL("SELECT * FROM {}").format(table("pointer_tokens")))
    for suffix in ("builds", "events", "validations", "pointer", "pointer_tokens"):
        for operation in ("SELECT * FROM {}", "INSERT INTO {} DEFAULT VALUES", "DELETE FROM {}", "TRUNCATE {}"):
            with _refused(pg, pg["reader"], {"42501"}):
                pg["reader"].execute(sql.SQL(operation).format(table(suffix)))
    for view in ("bond_default_owner_evidence_v1_publications", "bond_default_owner_events_v1_current"):
        relation = sql.Identifier(pg["schema"], view)
        pg["reader"].execute(sql.SQL("SELECT * FROM {}").format(relation)).fetchall()
        with _refused(pg, pg["reader"], {"42501", "55000"}):
            pg["reader"].execute(sql.SQL("DELETE FROM {}").format(relation))
    with _refused(pg, pg["reader"], {"42501"}):
        _point(pg, prepared["two"]["publication_id"], None, pg["reader"])
    print("P2: stale grants revoked on reapply; direct token/pointer DML refused; reader is view-only, not CAS/raw history")


def test_function_paths_and_owner_appendonly_guards(pg, prepared):
    functions = pg["admin"].execute(
        "SELECT p.proname,p.prosecdef,p.proconfig,r.rolname FROM pg_proc p "
        "JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles r ON r.oid=p.proowner WHERE n.nspname=%s",
        (pg["schema"],),
    ).fetchall()
    assert len(functions) == 6
    for name, definer, config, owner in functions:
        assert definer is (name == "bond_default_owner_evidence_v1_point")
        assert owner == pg["roles"]["owner"]
        assert config == ["search_path=pg_catalog, " + pg["schema"] + ", pg_temp"]
    for suffix in ("builds", "events", "validations"):
        for operation in ("UPDATE {} SET publication_id=publication_id", "DELETE FROM {}", "TRUNCATE {} CASCADE"):
            with _refused(pg, pg["owner"], {"P0001"}, "append-only"):
                pg["owner"].execute(pg["sql"].SQL(operation).format(pg["table"](suffix)))
    with _refused(pg, pg["owner"], {"P0001"}, "explicit compare-and-set"):
        pg["owner"].execute(pg["sql"].SQL("INSERT INTO {} (product,publication_id) VALUES (%s,%s)").format(
            pg["table"]("pointer")), ("bond_default_owner_evidence_v1", prepared["two"]["publication_id"]),
        )
    with _refused(pg, pg["owner"], {"P0001"}, "append-only"):
        pg["owner"].execute(pg["sql"].SQL("TRUNCATE {}").format(pg["table"]("pointer")))

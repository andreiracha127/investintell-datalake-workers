"""Real PostgreSQL 18 transaction tests for the frozen-artifact loader."""
from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Self
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from src.bonds import implied_rating_artifact_loader as loader
from src.bonds.implied_rating_materializer import materialize
from tests.test_bond_market_implied_rating_artifact_loader import (
    _approve_release_context,
    _fixture,
    _write_release_context,
)

# SEC_TEST_DATABASE_URL is the disposable-cluster administrator: fixture setup,
# teardown and fault injection only.  Every loader connection uses
# SEC_TEST_WORKER_DATABASE_URL, a genuine LOGIN worker_writer on the same database.
# A missing worker DSN is an unmet PG18 gate, never a silently green run: when the
# admin DSN is configured, the worker DSN is required.
pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL unavailable"
)


def _worker_dsn() -> str:
    dsn = os.getenv("SEC_TEST_WORKER_DATABASE_URL")
    if not dsn:
        pytest.fail(
            "SEC_TEST_WORKER_DATABASE_URL (genuine LOGIN worker_writer) is required "
            "whenever SEC_TEST_DATABASE_URL is set"
        )
    return dsn


def _same_isolated_database(admin_dsn: str, worker_dsn: str) -> None:
    with (
        psycopg.connect(admin_dsn, autocommit=True) as admin,
        psycopg.connect(worker_dsn, autocommit=True) as worker,
    ):
        identity = "SELECT current_database(), inet_server_port(), pg_postmaster_start_time()"
        assert admin.execute(identity).fetchone() == worker.execute(identity).fetchone()
        assert worker.execute("SELECT current_user, session_user").fetchone() == (
            "worker_writer", "worker_writer"
        )


@contextmanager
def _database(
    tmp_path: Path,
) -> Iterator[
    tuple[
        loader.FrozenArtifactContract,
        loader.VerifiedArtifact,
        Callable[[], psycopg.Connection],
        str,
        Path,
    ]
]:
    dsn = os.environ["SEC_TEST_DATABASE_URL"]
    worker_dsn = _worker_dsn()
    _same_isolated_database(dsn, worker_dsn)
    schema = f"test_artifact_loader_{uuid4().hex}"
    artifact_root, raw, original_contract = _fixture(tmp_path)
    (tmp_path / "evidence").mkdir()
    with psycopg.connect(dsn, autocommit=True) as admin:
        version = int(admin.execute("SHOW server_version_num").fetchone()[0])
        assert 180000 <= version < 190000
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        admin.execute(sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO worker_writer").format(
            sql.Identifier(schema)
        ))
        admin.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        admin.execute(
            "CREATE TABLE sec_ingestion_runs (run_id uuid PRIMARY KEY, "
            "raw_validated_at timestamptz)"
        )
        admin.execute(
            "CREATE TABLE sec_source_packages (package_id uuid PRIMARY KEY, "
            "run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id))"
        )
        admin.execute(
            "CREATE VIEW sec_validated_raw_runs AS SELECT run_id, raw_validated_at "
            "FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
        )
        admin.execute(
            "CREATE TABLE bond_panel_publications ("
            "publication_id uuid PRIMARY KEY, product text NOT NULL, "
            "parent_publication_id uuid, publication_status text NOT NULL, failure_reason text, "
            "config_hash char(16) NOT NULL, input_fingerprint char(64) NOT NULL, "
            "code_revision text NOT NULL, first_month date NOT NULL, "
            "last_closed_month date NOT NULL, open_month date, snapshot_rows integer NOT NULL, "
            "rv_signal_rows integer NOT NULL, returns_rows integer NOT NULL, "
            "ratings_pit_rows integer NOT NULL, source_lineage jsonb NOT NULL, "
            "gate_evidence jsonb NOT NULL, built_at timestamptz NOT NULL DEFAULT now(), "
            "computed_at timestamptz NOT NULL DEFAULT now(), validated_at timestamptz)"
        )
        admin.execute(
            "CREATE TABLE bond_panel_app_pointer (product text PRIMARY KEY, "
            "publication_id uuid NOT NULL REFERENCES bond_panel_publications(publication_id), "
            "changed_at timestamptz NOT NULL DEFAULT now())"
        )
        run_id, package_id = uuid4(), uuid4()
        admin.execute("INSERT INTO sec_ingestion_runs VALUES (%s, now())", (run_id,))
        admin.execute("INSERT INTO sec_source_packages VALUES (%s, %s)", (package_id, run_id))

        pins = original_contract.parent
        source_shas = {key: "d" * 64 for key in pins.source_sha256_keys}
        source_shas["bond_panel_live.parquet"] = pins.snapshot_source_sha256
        child_lineage = {
            "unit_repair": {"contract": pins.repair_contract},
            "source_sha256": source_shas,
        }
        child_gates = {"unit_repair": {"contract": pins.repair_contract}}
        admin.execute(
            "INSERT INTO bond_panel_publications "
            "(publication_id,product,parent_publication_id,publication_status,failure_reason,"
            "config_hash,input_fingerprint,code_revision,first_month,last_closed_month,open_month,"
            "snapshot_rows,rv_signal_rows,returns_rows,ratings_pit_rows,source_lineage,gate_evidence,"
            "validated_at) VALUES (%s,%s,NULL,'validated',NULL,%s,%s,%s,%s,%s,NULL,1,1,1,1,%s,%s,now())",
            (
                pins.parent_publication_id, loader.PANEL_PRODUCT, pins.config_hash, "e" * 64,
                "fixture-parent", pins.first_month, pins.last_closed_month,
                Jsonb({"source_sha256": source_shas}), Jsonb({"fixture": True}),
            ),
        )
        admin.execute(
            "INSERT INTO bond_panel_publications "
            "(publication_id,product,parent_publication_id,publication_status,failure_reason,"
            "config_hash,input_fingerprint,code_revision,first_month,last_closed_month,open_month,"
            "snapshot_rows,rv_signal_rows,returns_rows,ratings_pit_rows,source_lineage,gate_evidence,"
            "validated_at) VALUES (%s,%s,%s,'validated',NULL,%s,%s,%s,%s,%s,%s,1,1,1,1,%s,%s,now())",
            (
                pins.publication_id, loader.PANEL_PRODUCT, pins.parent_publication_id,
                pins.config_hash, "f" * 64, pins.code_revision, pins.first_month,
                pins.last_closed_month, pins.open_month, Jsonb(child_lineage), Jsonb(child_gates),
            ),
        )
        admin.execute(
            "INSERT INTO bond_panel_app_pointer(product,publication_id) VALUES (%s,%s)",
            (loader.PANEL_PRODUCT, pins.publication_id),
        )
        child = loader._parent_projection(
            admin.execute(loader._PARENT_SQL, (pins.publication_id,)).fetchone()
        )
        parent = loader._parent_projection(
            admin.execute(loader._PARENT_SQL, (pins.parent_publication_id,)).fetchone()
        )
        header_sha = hashlib.sha256(
            loader._canonical_json_bytes({"child": child, "parent": parent})
        ).hexdigest()
        contract = replace(
            original_contract,
            parent=replace(original_contract.parent, header_sha256=header_sha),
        )
        artifact = loader._load_verified_artifact(
            artifact_root,
            contract=contract,
            contract_sha256=hashlib.sha256(raw).hexdigest(),
        )
        admin.execute(sql.SQL(
            "GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES ON ALL TABLES "
            "IN SCHEMA {} TO worker_writer"
        ).format(sql.Identifier(schema)))

        def connection_factory() -> psycopg.Connection:
            # Genuine LOGIN worker_writer session; the search_path is set through the
            # connection options so the session carries no role switch at all.
            return psycopg.connect(
                worker_dsn,
                autocommit=True,
                connect_timeout=5,
                options=f"-c search_path={schema},public",
            )

        try:
            yield contract, artifact, connection_factory, schema, tmp_path / "evidence"
        finally:
            admin.execute("SET search_path TO public")
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def _install_shared_only(factory: Callable[[], psycopg.Connection]) -> None:
    ddl = (loader.ROOT / "schemas" / "sec_derived_publications.sql").read_text(
        encoding="utf-8"
    )
    with factory() as conn, conn.transaction():
        conn.execute(ddl)
        assert loader._schema_state(conn) == "shared_only"


def _admin_in_schema(schema: str, *statements: str | sql.Composable) -> None:
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"], autocommit=True) as admin:
        admin.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        for statement in statements:
            admin.execute(statement)


# The captured production worker_writer default privileges (read-only capture
# receipt 148e6b2a...): new tables arwd for app_runtime and r for app_analytics_ro,
# new functions X for app_runtime.
_CAPTURED_DEFAULT_PRIVILEGES = (
    "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_runtime",
    "GRANT SELECT ON TABLES TO app_analytics_ro",
    "GRANT EXECUTE ON FUNCTIONS TO app_runtime",
)


def _set_default_privileges(schema: str, *grants: str) -> None:
    # Schema-scoped default ACLs depend on the schema and drop with it.
    _admin_in_schema(schema, *(
        sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE worker_writer IN SCHEMA {} ").format(
            sql.Identifier(schema)
        ) + sql.SQL(grant)
        for grant in grants
    ))


def _install_production_ledger(
    factory: Callable[[], psycopg.Connection],
    schema: str,
    *,
    captured_defaults: bool = False,
    extra_defaults: tuple[str, ...] = (),
    check: bool = True,
) -> None:
    """Shared ledger plus the authentic, unmodified RR1 SQL, installed as worker_writer."""
    _admin_in_schema(
        schema,
        "CREATE TABLE rr1_effective_facts(raw_row_id bigint, ingestion_run_id uuid, "
        "source_table text, accession_number text, tag text, version text, data_date date, "
        "series_id text, class_id text, measure_id text, document_id text, dimensions text, "
        "occurrence text, fact_typed_projection jsonb, effective_date date, "
        "accepted_at timestamptz, filed_date date, form text)",
        "GRANT SELECT ON rr1_effective_facts TO worker_writer",
    )
    if captured_defaults or extra_defaults:
        _set_default_privileges(
            schema, *(_CAPTURED_DEFAULT_PRIVILEGES if captured_defaults else ()), *extra_defaults
        )
    with factory() as conn, conn.transaction():
        for name in ("sec_derived_publications.sql", "rr1_fee_profiles.sql"):
            conn.execute((loader.ROOT / "schemas" / name).read_text(encoding="utf-8"))
        if check:
            assert loader._schema_state_and_profile(conn) == ("shared_only", loader.PROFILE_RR1)


def test_pg18_fresh_publish_full_readback_and_exact_replay(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        first = loader._publish_verified_artifact(
            artifact,
            contract=contract,
            connection_factory=factory,
            evidence_dir=evidence_dir,
        )
        assert first.outcome == "published_verified"
        assert first.schema_installed
        assert first.stored is not None
        assert first.stored.summary.rows_digest == artifact.summary.rows_digest
        assert len(first.receipts) == 3
        second = loader._publish_verified_artifact(
            artifact,
            contract=contract,
            connection_factory=factory,
            evidence_dir=evidence_dir,
        )
        assert second.outcome == "already_published_verified"
        assert not second.schema_installed
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1 WHERE publication_id=%s",
                (artifact.publication.publication_id,),
            ).fetchone()[0] == 13
            assert str(conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0]) == artifact.publication.publication_id


def test_outer_transaction_rolls_back_rows_build_ledger_and_pointer(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        loader._persist_receipt(
            evidence_dir, phase="pre-apply",
            payload=loader._receipt_payload(
                phase="pre-apply", artifact=artifact, outcome="verified", stored=None
            ),
        )
        assert loader._ensure_schema(contract, factory)
        payload = loader._artifact_payload(artifact, contract)
        with factory() as conn, pytest.raises(RuntimeError), conn.transaction():
            loader._set_local_timeouts(conn)
            loader._acquire_product_lock(conn)
            loader._verify_parent(conn, contract=contract, lock_pointer=True)
            materialize(conn, artifact.publication, payload, expected_pointer=None)
            loader._verify_stored_publication(
                conn, artifact.publication, contract=contract, require_current=True
            )
            raise RuntimeError("interrupt before commit")
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1"
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_current_pointers WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0


def test_two_connections_serialize_on_product_advisory_lock(tmp_path: Path) -> None:
    with (
        _database(tmp_path) as (_, _, factory, _, _),
        factory() as first,
        factory() as second,
        first.transaction(),
    ):
        loader._acquire_product_lock(first)
        with second.transaction():
            second.execute("SET LOCAL lock_timeout='200ms'")
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._acquire_product_lock(second)
            assert exc.value.code == loader.ErrorCode.LOCK_TIMEOUT.value


def test_panel_pointer_row_is_held_for_share_until_apply_commit(tmp_path: Path) -> None:
    with (
        _database(tmp_path) as (contract, _, factory, _, _),
        factory() as first,
        factory() as second,
        first.transaction(),
    ):
        loader._verify_parent(first, contract=contract, lock_pointer=True)
        with second.transaction():
            second.execute("SET LOCAL lock_timeout='200ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                second.execute(
                    "UPDATE bond_panel_app_pointer SET changed_at=now() WHERE product=%s",
                    (loader.PANEL_PRODUCT,),
                )


def test_dry_run_does_not_install_schema_or_write_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, _):
        _install_production_ledger(factory, schema)
        monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
        result = loader.dry_run_verified_artifact(
            artifact,
            evidence_dir=tmp_path / "dry-run-evidence",
            connection_factory=factory,
        )
        assert result.outcome == "dry_run_verified_schema_install_required"
        assert len(result.receipts) == 1
        assert result.receipts[0].phase == "dry-run"
        with factory() as conn:
            assert conn.execute(
                "SELECT to_regclass('bond_market_implied_rating_v1')"
            ).fetchone()[0] is None


def test_equal_count_stored_row_corruption_is_detected_by_full_digest(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        loader._publish_verified_artifact(
            artifact,
            contract=contract,
            connection_factory=factory,
            evidence_dir=evidence_dir,
        )
        with factory() as conn, conn.transaction():
            conn.execute("ALTER TABLE bond_market_implied_rating_v1 DISABLE TRIGGER USER")
            conn.execute(
                "UPDATE bond_market_implied_rating_v1 SET neutralized_score=neutralized_score+1 "
                "WHERE publication_id=%s AND cusip_id='000000000'",
                (artifact.publication.publication_id,),
            )
            conn.execute("ALTER TABLE bond_market_implied_rating_v1 ENABLE TRIGGER USER")
        with (
            factory() as conn,
            conn.transaction(),
            pytest.raises(loader.ArtifactLoaderError) as exc,
        ):
            loader._verify_stored_publication(
                conn, artifact.publication, contract=contract, require_current=True
            )
        assert exc.value.code == loader.ErrorCode.ROW_DIGEST_MISMATCH.value


def test_sql_nan_cannot_masquerade_as_nullable_value(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        loader._publish_verified_artifact(
            artifact,
            contract=contract,
            connection_factory=factory,
            evidence_dir=evidence_dir,
        )
        with factory() as conn, conn.transaction():
            conn.execute("ALTER TABLE bond_market_implied_rating_v1 DISABLE TRIGGER USER")
            conn.execute(
                "UPDATE bond_market_implied_rating_v1 SET market_level_l='NaN'::float8 "
                "WHERE publication_id=%s AND cusip_id='000000000'",
                (artifact.publication.publication_id,),
            )
            conn.execute("ALTER TABLE bond_market_implied_rating_v1 ENABLE TRIGGER USER")
        with (
            factory() as conn,
            conn.transaction(),
            pytest.raises(loader.ArtifactLoaderError) as exc,
        ):
            loader._verify_stored_publication(
                conn, artifact.publication, contract=contract, require_current=True
            )
        assert exc.value.code == loader.ErrorCode.STORED_MISMATCH.value
        assert exc.value.details == {"field": "stored.nonfinite.market_level_l"}


def test_commit_visible_identity_reconciles_after_missing_postcommit_step(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, _):
        loader._ensure_schema(contract, factory)
        payload = loader._artifact_payload(artifact, contract)
        with factory() as conn, conn.transaction():
            loader._set_local_timeouts(conn)
            loader._acquire_product_lock(conn)
            loader._verify_parent(conn, contract=contract, lock_pointer=True)
            materialize(conn, artifact.publication, payload, expected_pointer=None)
            loader._verify_stored_publication(
                conn, artifact.publication, contract=contract, require_current=True
            )
        recovered = loader._read_only_operation(
            artifact,
            contract=contract,
            connection_factory=factory,
            require_published=True,
        )
        assert recovered.outcome == "already_published_verified"
        assert recovered.stored is not None


def test_foreign_partial_identity_is_refused_without_pointer_move(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, _):
        loader._ensure_schema(contract, factory)
        run_id, package_id, foreign_id = uuid4(), uuid4(), uuid4()
        with factory() as conn, conn.transaction():
            conn.execute("INSERT INTO sec_ingestion_runs VALUES (%s, now())", (run_id,))
            conn.execute("INSERT INTO sec_source_packages VALUES (%s, %s)", (package_id, run_id))
            conn.execute(
                "INSERT INTO sec_derived_publications "
                "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) "
                "VALUES (%s,%s,99,%s,%s,%s)",
                (foreign_id, loader.PRODUCT, run_id, package_id, "0" * 64),
            )
        with (
            factory() as conn,
            conn.transaction(),
            pytest.raises(loader.ArtifactLoaderError) as exc,
        ):
            loader._publication_state(conn, artifact.publication)
        assert exc.value.code == loader.ErrorCode.PUBLICATION_CONFLICT.value


def test_partial_schema_drift_is_refused_instead_of_reinstalled(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            conn.execute("DROP VIEW bond_market_implied_rating_v1_current")
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_preserved_name_serving_view_drift_is_refused(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            conn.execute(
                "CREATE OR REPLACE VIEW bond_market_implied_rating_v1_current AS "
                "SELECT r.* FROM sec_derived_current_pointers pointer "
                "JOIN bond_market_implied_rating_v1 r "
                "ON r.publication_id=pointer.publication_id "
                "WHERE pointer.product='bond_market_implied_rating_v1' AND false"
            )
            conn.execute(
                "ALTER VIEW bond_market_implied_rating_v1_current OWNER TO worker_writer"
            )
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_preserved_name_noop_write_guard_is_refused(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            conn.execute(
                "CREATE OR REPLACE FUNCTION bond_market_implied_rating_v1_write_guard() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$"
            )
            conn.execute(
                "ALTER FUNCTION bond_market_implied_rating_v1_write_guard() OWNER TO worker_writer"
            )
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_exact_reviewed_pg18_function_and_trigger_contract_admits(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            assert loader._schema_state(conn) == "compatible"


def test_weakened_lifecycle_predicate_is_refused_before_publication(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            conn.execute(
                "CREATE OR REPLACE FUNCTION sec_derived_publication_immutable() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                "IF TG_OP='UPDATE' AND OLD.lifecycle_state='prepared' "
                "AND NEW.lifecycle_state='validated' THEN RETURN NEW; END IF; "
                "RAISE EXCEPTION 'derived publication is immutable'; END $$"
            )
            conn.execute(
                "ALTER FUNCTION sec_derived_publication_immutable() OWNER TO worker_writer"
            )
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0


def _replace_pointer_function_body(
    conn: psycopg.Connection,
    body: str,
    *,
    default: str | None = "false",
) -> None:
    conn.execute(
        "DROP FUNCTION sec_set_current_derived_publication(text,uuid,boolean)"
    )
    default_clause = "" if default is None else f" DEFAULT {default}"
    conn.execute(sql.SQL(
        "CREATE FUNCTION sec_set_current_derived_publication("
        "target_product text,target_publication_id uuid,"
        f"allow_as_of_regression boolean{default_clause}) RETURNS void "
        "LANGUAGE plpgsql AS {}"
    ).format(sql.Literal(body)))
    conn.execute(
        "ALTER FUNCTION sec_set_current_derived_publication(text,uuid,boolean) "
        "OWNER TO worker_writer"
    )


def test_joined_comment_newline_cannot_disable_monotonic_guard(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            source = conn.execute(
                "SELECT prosrc FROM pg_proc WHERE oid="
                "'sec_set_current_derived_publication(text,uuid,boolean)'::regprocedure"
            ).fetchone()[0]
            weakened = source.replace(
                "explicitly.\n    SELECT publication_id INTO current_publication_id\n"
                "    FROM sec_derived_current_pointers WHERE product=target_product;",
                "explicitly.    SELECT publication_id INTO current_publication_id "
                "FROM sec_derived_current_pointers WHERE product=target_product;",
            )
            assert weakened != source
            _replace_pointer_function_body(conn, weakened)
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_case_sensitive_function_literal_change_is_refused(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            source = conn.execute(
                "SELECT prosrc FROM pg_proc WHERE oid="
                "'sec_set_current_derived_publication(text,uuid,boolean)'::regprocedure"
            ).fetchone()[0]
            changed = source.replace(
                "'current pointer requires a validated publication for its product'",
                "'Current Pointer Requires A Validated Publication For Its Product'",
            )
            assert changed != source
            _replace_pointer_function_body(conn, changed)
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_pointer_regression_default_true_is_refused(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            source = conn.execute(
                "SELECT prosrc FROM pg_proc WHERE oid="
                "'sec_set_current_derived_publication(text,uuid,boolean)'::regprocedure"
            ).fetchone()[0]
            _replace_pointer_function_body(conn, source, default="true")
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_pointer_regression_missing_default_is_refused(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            source = conn.execute(
                "SELECT prosrc FROM pg_proc WHERE oid="
                "'sec_set_current_derived_publication(text,uuid,boolean)'::regprocedure"
            ).fetchone()[0]
            _replace_pointer_function_body(conn, source, default=None)
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_named_trigger_attached_to_wrong_relation_is_refused(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            conn.execute(
                "DROP TRIGGER bond_market_implied_rating_v1_rows_write_guard "
                "ON bond_market_implied_rating_v1"
            )
            conn.execute(
                "CREATE TRIGGER bond_market_implied_rating_v1_rows_write_guard "
                "BEFORE INSERT OR UPDATE OR DELETE ON bond_market_implied_rating_v1_builds "
                "FOR EACH ROW EXECUTE FUNCTION bond_market_implied_rating_v1_write_guard()"
            )
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_missing_pinned_function_is_typed_schema_mismatch(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            conn.execute("DROP FUNCTION sec_validate_derived_publication(uuid)")
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


def test_empty_shared_only_ledger_retains_clean_absence(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, _):
        _install_shared_only(factory)
        result = loader._read_only_operation(
            artifact,
            contract=contract,
            connection_factory=factory,
            require_published=False,
        )
        assert result.outcome == "dry_run_verified_schema_install_required"
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            assert loader._schema_state(conn) == "compatible"


def _seed_shared_partial_state(
    factory: Callable[[], psycopg.Connection],
    contract: loader.FrozenArtifactContract,
    state: str,
) -> None:
    assert contract.identity is not None
    with factory() as conn, conn.transaction():
        run_id = conn.execute("SELECT run_id FROM sec_ingestion_runs").fetchone()[0]
        package_id = conn.execute("SELECT package_id FROM sec_source_packages").fetchone()[0]
        publication_id = (
            uuid4() if state == "foreign" else UUID(contract.identity.publication_id)
        )
        conn.execute(
            "INSERT INTO sec_derived_publications "
            "(publication_id,product,publication_version,source_run_id,source_package_id,"
            "build_fingerprint) VALUES (%s,%s,1,%s,%s,%s)",
            (publication_id, loader.PRODUCT, run_id, package_id, "0" * 64),
        )
        if state in {"validated", "pointer"}:
            conn.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        if state == "pointer":
            conn.execute(
                "SELECT sec_set_current_derived_publication(%s,%s)",
                (loader.PRODUCT, publication_id),
            )


@pytest.mark.parametrize("state", ["prepared", "validated", "foreign", "pointer"])
@pytest.mark.parametrize("operation", ["read", "install"])
def test_shared_only_partial_product_state_is_typed_conflict(
    tmp_path: Path, state: str, operation: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, _):
        _install_shared_only(factory)
        _seed_shared_partial_state(factory, contract, state)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            if operation == "read":
                loader._read_only_operation(
                    artifact,
                    contract=contract,
                    connection_factory=factory,
                    require_published=False,
                )
            else:
                loader._ensure_schema(contract, factory)
        assert exc.value.code == loader.ErrorCode.PUBLICATION_CONFLICT.value
        with factory() as conn:
            assert conn.execute(
                "SELECT to_regclass('bond_market_implied_rating_v1')"
            ).fetchone()[0] is None


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE bond_panel_app_pointer SET publication_id='22222222-2222-4222-8222-222222222222'",
        (
            "UPDATE bond_panel_publications SET publication_status='prepared' "
            "WHERE publication_id='11111111-1111-4111-8111-111111111111'"
        ),
        (
            "UPDATE bond_panel_publications SET config_hash='0000000000000000' "
            "WHERE publication_id='11111111-1111-4111-8111-111111111111'"
        ),
        (
            "UPDATE bond_panel_publications SET source_lineage=jsonb_set("
            "source_lineage,'{unit_repair,contract}','\"wrong\"') "
            "WHERE publication_id='11111111-1111-4111-8111-111111111111'"
        ),
    ],
)
def test_parent_pointer_header_and_lineage_mismatches_are_refused(
    tmp_path: Path, mutation: str
) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        with factory() as conn:
            conn.execute(mutation)
        with factory() as conn, conn.transaction(), pytest.raises(
            loader.ArtifactLoaderError
        ) as exc:
            loader._verify_parent(conn, contract=contract, lock_pointer=False)
        assert exc.value.code == loader.ErrorCode.PARENT_MISMATCH.value


def test_full_build_field_mismatch_is_refused_on_replay(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        loader._publish_verified_artifact(
            artifact,
            contract=contract,
            connection_factory=factory,
            evidence_dir=evidence_dir,
        )
        with factory() as conn, conn.transaction():
            conn.execute("ALTER TABLE bond_market_implied_rating_v1_builds DISABLE TRIGGER USER")
            conn.execute(
                "UPDATE bond_market_implied_rating_v1_builds "
                "SET d_candidate_count=d_candidate_count+1 WHERE publication_id=%s",
                (artifact.publication.publication_id,),
            )
            conn.execute("ALTER TABLE bond_market_implied_rating_v1_builds ENABLE TRIGGER USER")
        with (
            factory() as conn,
            conn.transaction(),
            pytest.raises(loader.ArtifactLoaderError) as exc,
        ):
            loader._verify_stored_publication(
                conn, artifact.publication, contract=contract, require_current=True
            )
        assert exc.value.code == loader.ErrorCode.STORED_MISMATCH.value
        assert exc.value.details == {"field": "stored.build_fields"}


def test_materializer_bond_error_is_typed_and_receipted(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        with factory() as conn:
            conn.execute("UPDATE sec_ingestion_runs SET raw_validated_at=NULL")
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.MATERIALIZER_REFUSAL.value
        receipts = [
            path.read_text(encoding="utf-8")
            for path in evidence_dir.glob("*-failure-*.json")
        ]
        assert any('"code":"materializer_refusal"' in value for value in receipts)
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications"
            ).fetchone()[0] == 0


def _counting(
    factory: Callable[[], psycopg.Connection],
) -> tuple[Callable[[], psycopg.Connection], list[int]]:
    calls: list[int] = []

    def counted() -> psycopg.Connection:
        calls.append(1)
        return factory()

    return counted, calls


def test_payload_refusal_precedes_schema_install_and_success_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        def refuse_payload(*_: object, **__: object) -> list[tuple[object, ...]]:
            raise loader.ArtifactLoaderError(
                loader.ErrorCode.ROW_INVALID, field="row.injected_refusal"
            )

        monkeypatch.setattr(loader, "_artifact_payload", refuse_payload)
        counted, calls = _counting(factory)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=counted,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.ROW_INVALID.value
        assert calls == []
        receipts = list(evidence_dir.glob("*.json"))
        assert len(receipts) == 1 and "-failure-" in receipts[0].name
        receipt = json.loads(receipts[0].read_text(encoding="utf-8"))
        assert receipt["failure_phase"] == "payload"
        assert receipt["schema_installed"] is False
        assert receipt["transaction_outcome"] == "not_started"
        with factory() as conn:
            assert loader._schema_state(conn) == "absent"


def test_post_schema_refusal_receipt_records_committed_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        def refuse_state(*_: object, **__: object) -> object:
            raise loader.ArtifactLoaderError(
                loader.ErrorCode.ROW_INVALID, field="row.injected_refusal"
            )

        monkeypatch.setattr(loader, "_publication_state", refuse_state)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.ROW_INVALID.value
        receipt_path = next(evidence_dir.glob("*-failure-*.json"))
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        assert receipt["failure_phase"] == "publication_state"
        assert receipt["schema_installed"] is True
        assert receipt["transaction_outcome"] == "not_committed"
        with factory() as conn:
            assert loader._schema_state(conn) == "compatible"
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0


def test_final_receipt_failure_requires_recovery_without_compensating_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        original = loader._persist_receipt

        def fail_final_receipt(
            path: Path, *, phase: str, payload: bytes
        ) -> loader.ReceiptRef:
            if phase == "readback":
                raise loader.ArtifactLoaderError(
                    loader.ErrorCode.RECEIPT_FAILURE, field="receipt.injected_final"
                )
            return original(path, phase=phase, payload=payload)

        monkeypatch.setattr(loader, "_persist_receipt", fail_final_receipt)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.COMMITTED_EVIDENCE_INCOMPLETE.value
        assert exc.value.phase == "final_receipt"
        assert exc.value.schema_installed is True
        assert exc.value.transaction_outcome == "committed"
        assert exc.value.outcome == "recovery_required"
        failure_path = next(evidence_dir.glob("*-failure-*.json"))
        failure = json.loads(failure_path.read_text(encoding="utf-8"))
        assert failure["failure_phase"] == "final_receipt"
        assert failure["schema_installed"] is True
        assert failure["transaction_outcome"] == "committed"
        assert failure["outcome"] == "recovery_required"
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1"
            ).fetchone()[0] == artifact.summary.row_count
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 1
            assert str(conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0]) == artifact.publication.publication_id


def test_committed_classification_survives_all_receipt_writes_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        original = loader._persist_receipt

        def fail_final_and_failure_receipts(
            path: Path, *, phase: str, payload: bytes
        ) -> loader.ReceiptRef:
            if phase in {"readback", "failure"}:
                raise loader.ArtifactLoaderError(
                    loader.ErrorCode.RECEIPT_FAILURE, field="receipt.unavailable"
                )
            return original(path, phase=phase, payload=payload)

        monkeypatch.setattr(loader, "_persist_receipt", fail_final_and_failure_receipts)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        error = exc.value
        assert error.code == loader.ErrorCode.COMMITTED_EVIDENCE_INCOMPLETE.value
        assert error.phase == "final_receipt"
        assert error.schema_installed is True
        assert error.transaction_outcome == "committed"
        assert error.outcome == "recovery_required"
        assert error.receipt_written is False
        assert not list(evidence_dir.glob("*-failure-*.json"))
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1"
            ).fetchone()[0] == artifact.summary.row_count
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 1


def test_insufficient_privilege_during_materialization_is_not_committed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        def deny_materialization(*_: object, **__: object) -> None:
            raise psycopg.errors.InsufficientPrivilege("injected privilege refusal")

        monkeypatch.setattr(loader, "materialize", deny_materialization)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        error = exc.value
        assert error.code == loader.ErrorCode.DB_FAILURE.value
        assert error.phase == "materialize"
        assert error.schema_installed is True
        assert error.transaction_outcome == "not_committed"
        receipt = json.loads(next(evidence_dir.glob("*-failure-*.json")).read_text())
        assert receipt["failure_phase"] == "materialize"
        assert receipt["schema_installed"] is True
        assert receipt["transaction_outcome"] == "not_committed"
        assert "injected privilege refusal" not in json.dumps(receipt)
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_current_pointers WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0


def test_public_apply_enforces_complete_release_context_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        release_document = _write_release_context(evidence_dir, contract)
        _approve_release_context(evidence_dir, monkeypatch)
        monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
        result = loader.publish_verified_artifact(
            artifact,
            evidence_dir=evidence_dir,
            connection_factory=factory,
        )
        assert result.outcome == "published_verified"
        assert result.schema_installed is True
        preapply = json.loads(
            (evidence_dir / result.receipts[0].basename).read_text(encoding="utf-8")
        )
        assert preapply["release"]["target"] == "production"
        assert preapply["release"]["railway_toml_sha256"] == (
            release_document["railway_toml_sha256"]
        )
        assert preapply["release"]["release_context_sha256"] == hashlib.sha256(
            (evidence_dir / "release-context.json").read_bytes()
        ).hexdigest()


class _AmbiguousCommitConnection:
    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn
        self._transaction_depth = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self._conn.close()

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._transaction_depth += 1
        transaction = self._conn.transaction()
        transaction.__enter__()
        try:
            yield
        except BaseException as exc:
            transaction.__exit__(type(exc), exc, exc.__traceback__)
            raise
        else:
            transaction.__exit__(None, None, None)
            if self._transaction_depth == 1:
                raise psycopg.OperationalError("commit acknowledgement lost")
        finally:
            self._transaction_depth -= 1


def test_commit_acknowledgement_loss_persists_unknown_and_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        calls = 0

        def ambiguous_factory() -> psycopg.Connection | _AmbiguousCommitConnection:
            nonlocal calls
            calls += 1
            conn = factory()
            return conn if calls == 1 else _AmbiguousCommitConnection(conn)

        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=ambiguous_factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.COMMIT_UNKNOWN.value
        failure_receipts = [
            path.read_text(encoding="utf-8")
            for path in evidence_dir.glob("*-failure-*.json")
        ]
        assert any('"code":"commit_unknown"' in value for value in failure_receipts)
        monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
        recovered = loader.recover_published_artifact(
            artifact,
            evidence_dir=evidence_dir,
            connection_factory=factory,
        )
        assert recovered.outcome == "already_published_verified"
        assert recovered.receipts[0].phase == "recovery-readback"


def test_ambiguous_commit_classification_survives_receipt_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        calls = 0
        original = loader._persist_receipt

        def ambiguous_factory() -> psycopg.Connection | _AmbiguousCommitConnection:
            nonlocal calls
            calls += 1
            conn = factory()
            return conn if calls == 1 else _AmbiguousCommitConnection(conn)

        def fail_failure_receipt(
            path: Path, *, phase: str, payload: bytes
        ) -> loader.ReceiptRef:
            if phase == "failure":
                raise loader.ArtifactLoaderError(
                    loader.ErrorCode.RECEIPT_FAILURE, field="receipt.unavailable"
                )
            return original(path, phase=phase, payload=payload)

        monkeypatch.setattr(loader, "_persist_receipt", fail_failure_receipt)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=ambiguous_factory,
                evidence_dir=evidence_dir,
            )
        error = exc.value
        assert error.code == loader.ErrorCode.COMMIT_UNKNOWN.value
        assert error.phase == "transaction_commit"
        assert error.schema_installed is True
        assert error.transaction_outcome == "unknown"
        assert error.outcome == "recovery_required"
        assert error.receipt_written is False
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1"
            ).fetchone()[0] == artifact.summary.row_count


def test_postcommit_readback_failure_is_classified_as_commit_unknown(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        calls = 0

        def postcommit_failure_factory() -> psycopg.Connection:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise psycopg.OperationalError("readback connection unavailable")
            return factory()

        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact,
                contract=contract,
                connection_factory=postcommit_failure_factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.COMMIT_UNKNOWN.value
        receipts = [
            path.read_text(encoding="utf-8")
            for path in evidence_dir.glob("*-failure-*.json")
        ]
        assert any('"transaction_outcome":"committed"' in value for value in receipts)
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1"
            ).fetchone()[0] == artifact.summary.row_count


# --- r4074042091: exact, case-sensitive PG18 view definitions -----------------------


def test_clean_install_views_render_exact_pg18_definitions(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            for view_name, expected in loader._EXPECTED_VIEW_DEFINITIONS.items():
                observed = conn.execute(
                    "SELECT pg_get_viewdef(%s::regclass, false)", (view_name,)
                ).fetchone()[0]
                assert observed == expected
                assert "'bond_market_implied_rating_v1'::text" in observed


@pytest.mark.parametrize("view_name", sorted(loader._EXPECTED_VIEW_DEFINITIONS))
def test_case_only_view_literal_drift_is_refused_before_materialization(
    tmp_path: Path, view_name: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            definition = conn.execute(
                "SELECT pg_get_viewdef(%s::regclass, false)", (view_name,)
            ).fetchone()[0]
            drifted = definition.replace(
                "'bond_market_implied_rating_v1'::text", "'BOND_MARKET_IMPLIED_RATING_V1'::text"
            )
            assert drifted != definition and drifted.lower() == definition.lower()
            conn.execute(sql.SQL("CREATE OR REPLACE VIEW {} AS {}").format(
                sql.Identifier(view_name), sql.SQL(drifted.rstrip(";"))
            ))
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.details == {"field": f"schema.view.{view_name}"}
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory, evidence_dir=evidence_dir
            )
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.phase == "schema_install"
        with factory() as conn:
            assert conn.execute("SELECT count(*) FROM sec_derived_publications").fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1"
            ).fetchone()[0] == 0


# --- r4078254935: relation/column ACL semantic allowlist ----------------------------

_ALL_RELATIONS = (*loader._SHARED_SCHEMA_OBJECTS, *loader._PRODUCT_SCHEMA_OBJECTS)
_REFUSED_TABLE_GRANTS = (
    "GRANT INSERT ON {relation} TO app_runtime",
    "GRANT SELECT ON {relation} TO app_runtime WITH GRANT OPTION",
    "GRANT SELECT ON {relation} TO PUBLIC",
    "GRANT TRUNCATE ON {relation} TO app_analytics_ro",
    "GRANT MAINTAIN ON {relation} TO app_analytics_ro",
    "GRANT TRIGGER ON {relation} TO app_runtime",
)


def _relation_acls(conn: psycopg.Connection) -> list[tuple[str, str | None]]:
    return conn.execute(
        "SELECT c.relname, c.relacl::text FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) ORDER BY c.relname",
        (list(_ALL_RELATIONS),),
    ).fetchall()


def _admin_execute(statement: sql.Composable | str) -> None:
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"], autocommit=True) as admin:
        admin.execute(statement)


@pytest.mark.parametrize("grant", _REFUSED_TABLE_GRANTS)
@pytest.mark.parametrize("relation", _ALL_RELATIONS)
def test_unsafe_relation_grant_is_refused(tmp_path: Path, relation: str, grant: str) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            conn.execute(sql.SQL(grant).format(relation=sql.Identifier(relation)))
            before = _relation_acls(conn)
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.details == {"field": f"schema.acl.{relation}"}
        with factory() as conn:
            assert _relation_acls(conn) == before


@pytest.mark.parametrize("privilege", ["UPDATE", "INSERT", "REFERENCES"])
@pytest.mark.parametrize("relation", _ALL_RELATIONS)
def test_unsafe_column_grant_is_refused(tmp_path: Path, relation: str, privilege: str) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            column = conn.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum=1",
                (relation,),
            ).fetchone()[0]
            conn.execute(sql.SQL("GRANT {} ({}) ON {} TO app_runtime").format(
                sql.SQL(privilege), sql.Identifier(column), sql.Identifier(relation)
            ))
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state(conn)
        assert exc.value.details == {"field": f"schema.column_acl.{relation}"}


def test_read_only_reader_grants_are_admitted_and_never_rewritten(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            for relation in _ALL_RELATIONS:
                conn.execute(sql.SQL("GRANT SELECT ON {} TO app_analytics_ro, app_runtime").format(
                    sql.Identifier(relation)
                ))
            conn.execute(
                "GRANT SELECT (publication_id) ON bond_market_implied_rating_v1 TO app_analytics_ro"
            )
            before = _relation_acls(conn)
            assert loader._schema_state(conn) == "compatible"
        first = loader._publish_verified_artifact(
            artifact, contract=contract, connection_factory=factory, evidence_dir=evidence_dir
        )
        assert first.outcome == "published_verified"
        replay = loader._publish_verified_artifact(
            artifact, contract=contract, connection_factory=factory, evidence_dir=evidence_dir
        )
        assert replay.outcome == "already_published_verified"
        with factory() as conn:
            assert _relation_acls(conn) == before


def test_unknown_grantee_even_read_only_is_refused(tmp_path: Path) -> None:
    role = f"acl_probe_{uuid4().hex[:12]}"
    _admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
    try:
        with _database(tmp_path) as (contract, _, factory, _, _):
            assert loader._ensure_schema(contract, factory)
            with factory() as conn:
                conn.execute(sql.SQL("GRANT SELECT ON sec_derived_pointer_tokens TO {}").format(
                    sql.Identifier(role)
                ))
            with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(conn)
            assert exc.value.details == {"field": "schema.acl.sec_derived_pointer_tokens"}
    finally:
        _admin_execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


@pytest.mark.parametrize("relation", _ALL_RELATIONS)
def test_group_role_truncate_is_refused_even_without_current_members(
    tmp_path: Path, relation: str
) -> None:
    group = f"acl_group_{uuid4().hex[:12]}"
    _admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(group)))
    try:
        with _database(tmp_path) as (contract, _, factory, _, _):
            assert loader._ensure_schema(contract, factory)
            with factory() as conn:
                conn.execute(sql.SQL("GRANT TRUNCATE ON {} TO {}").format(
                    sql.Identifier(relation), sql.Identifier(group)
                ))
                before = _relation_acls(conn)
            with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(conn)
            assert exc.value.details == {"field": f"schema.acl.{relation}"}
            with factory() as conn:
                assert _relation_acls(conn) == before
    finally:
        _admin_execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(group)))
        _admin_execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(group)))


def test_runtime_inheriting_reader_only_role_is_admitted(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            for relation in _ALL_RELATIONS:
                conn.execute(sql.SQL("GRANT SELECT ON {} TO app_analytics_ro").format(
                    sql.Identifier(relation)
                ))
        _admin_execute("GRANT app_analytics_ro TO app_runtime")
        try:
            with factory() as conn:
                assert loader._schema_state(conn) == "compatible"
        finally:
            _admin_execute("REVOKE app_analytics_ro FROM app_runtime")


@pytest.mark.parametrize(
    ("grant", "revoke", "field"),
    [
        ("GRANT worker_writer TO app_runtime", "REVOKE worker_writer FROM app_runtime",
         "schema.runtime_create"),
        (
            "GRANT worker_writer TO app_analytics_ro WITH INHERIT FALSE, SET TRUE",
            "REVOKE worker_writer FROM app_analytics_ro",
            "schema.reader_role.app_analytics_ro",
        ),
        (
            "GRANT pg_write_all_data TO app_runtime",
            "REVOKE pg_write_all_data FROM app_runtime",
            "schema.reader_role.app_runtime",
        ),
        (
            "GRANT pg_maintain TO app_analytics_ro",
            "REVOKE pg_maintain FROM app_analytics_ro",
            "schema.reader_role.app_analytics_ro",
        ),
        ("ALTER ROLE app_runtime SUPERUSER", "ALTER ROLE app_runtime NOSUPERUSER",
         "schema.runtime_create"),
    ],
)
def test_reader_role_inheritance_or_owner_reach_is_refused(
    tmp_path: Path, grant: str, revoke: str, field: str
) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            assert loader._schema_state(conn) == "compatible"
            before = _relation_acls(conn)
        _admin_execute(grant)
        try:
            with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(conn)
            assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
            assert exc.value.details == {"field": field}
        finally:
            _admin_execute(revoke)
        with factory() as conn:
            assert _relation_acls(conn) == before
            assert loader._schema_state(conn) == "compatible"


@pytest.mark.parametrize("reader", ["app_runtime", "app_analytics_ro"])
@pytest.mark.parametrize(
    ("privileged_role", "privilege"),
    [("pg_write_all_data", "INSERT"), ("pg_maintain", "MAINTAIN")],
)
@pytest.mark.parametrize("indirect", [False, True], ids=["direct", "indirect"])
def test_set_only_predefined_write_roles_are_refused(
    tmp_path: Path, reader: str, privileged_role: str, privilege: str, indirect: bool
) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        intermediate = f"acl_set_{uuid4().hex[:12]}"
        if indirect:
            _admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(intermediate)))
        try:
            if indirect:
                _admin_execute(sql.SQL(
                    "GRANT {} TO {} WITH INHERIT FALSE, SET TRUE"
                ).format(sql.Identifier(privileged_role), sql.Identifier(intermediate)))
                _admin_execute(sql.SQL(
                    "GRANT {} TO {} WITH INHERIT FALSE, SET TRUE"
                ).format(sql.Identifier(intermediate), sql.Identifier(reader)))
            else:
                _admin_execute(sql.SQL(
                    "GRANT {} TO {} WITH INHERIT FALSE, SET TRUE"
                ).format(sql.Identifier(privileged_role), sql.Identifier(reader)))
            with factory() as conn:
                assert conn.execute(
                    "SELECT pg_has_role(%s, %s, 'SET')", (reader, privileged_role)
                ).fetchone()[0] is True
                assert conn.execute(
                    "SELECT has_table_privilege(%s, %s::regclass, %s)",
                    (reader, "sec_derived_publications", privilege),
                ).fetchone()[0] is False
                assert conn.execute(
                    "SELECT has_table_privilege(%s, %s::regclass, %s)",
                    (privileged_role, "sec_derived_publications", privilege),
                ).fetchone()[0] is True
                assert conn.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles r CROSS JOIN pg_roles a "
                    "CROSS JOIN pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE r.rolname=%s AND a.rolname=%s "
                    "AND pg_has_role(r.oid,a.oid,'SET') "
                    "AND n.nspname=current_schema() AND c.relname='sec_derived_publications' "
                    "AND has_table_privilege(a.oid,c.oid,%s))",
                    (reader, privileged_role, loader._READER_FORBIDDEN_TABLE_PRIVILEGES),
                ).fetchone()[0] is True
                before = _relation_acls(conn)
                with pytest.raises(loader.ArtifactLoaderError) as exc:
                    loader._schema_state(conn)
                assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
                assert exc.value.details == {"field": f"schema.reader_role.{reader}"}
                assert _relation_acls(conn) == before
        finally:
            if indirect:
                _admin_execute(sql.SQL("REVOKE {} FROM {}").format(
                    sql.Identifier(intermediate), sql.Identifier(reader)
                ))
                _admin_execute(sql.SQL("REVOKE {} FROM {}").format(
                    sql.Identifier(privileged_role), sql.Identifier(intermediate)
                ))
                _admin_execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(intermediate)))
            else:
                _admin_execute(sql.SQL("REVOKE {} FROM {}").format(
                    sql.Identifier(privileged_role), sql.Identifier(reader)
                ))
        with factory() as conn:
            assert loader._schema_state(conn) == "compatible"


def test_set_only_read_only_membership_remains_admitted(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            for relation in _ALL_RELATIONS:
                conn.execute(sql.SQL("GRANT SELECT ON {} TO app_analytics_ro").format(
                    sql.Identifier(relation)
                ))
        _admin_execute("GRANT app_analytics_ro TO app_runtime WITH INHERIT FALSE, SET TRUE")
        try:
            with factory() as conn:
                assert conn.execute(
                    "SELECT pg_has_role('app_runtime', 'app_analytics_ro', 'SET')"
                ).fetchone()[0] is True
                assert loader._schema_state(conn) == "compatible"
        finally:
            _admin_execute("REVOKE app_analytics_ro FROM app_runtime")


def test_live_shared_write_acl_posture_is_refused_before_install(tmp_path: Path) -> None:
    # Mirrors the read-only production capture: app_runtime=arwdm and
    # app_analytics_ro=r on the four shared relations owned by worker_writer.
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        _install_shared_only(factory)
        with factory() as conn:
            for relation in loader._SHARED_SCHEMA_OBJECTS:
                conn.execute(sql.SQL(
                    "GRANT SELECT, INSERT, UPDATE, DELETE, MAINTAIN ON {} TO app_runtime"
                ).format(sql.Identifier(relation)))
                conn.execute(sql.SQL("GRANT SELECT ON {} TO app_analytics_ro").format(
                    sql.Identifier(relation)
                ))
            before = _relation_acls(conn)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._read_only_operation(
                artifact, contract=contract, connection_factory=factory, require_published=False
            )
        # Relations are checked in name order; the first shared relation refuses.
        assert exc.value.details == {"field": "schema.acl.sec_derived_current_pointers"}
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory, evidence_dir=evidence_dir
            )
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.phase == "schema_install"
        with factory() as conn:
            assert _relation_acls(conn) == before
            assert conn.execute(
                "SELECT to_regclass('bond_market_implied_rating_v1')"
            ).fetchone()[0] is None


_WRITE_PRIVILEGES = ("INSERT", "UPDATE", "DELETE", "MAINTAIN", "TRUNCATE")


def _runtime_table_privileges(conn: psycopg.Connection) -> dict[str, dict[str, bool]]:
    return {
        relation: {
            privilege: conn.execute(
                "SELECT has_table_privilege('app_runtime', %s::regclass, %s)",
                (relation, privilege),
            ).fetchone()[0]
            for privilege in ("SELECT", *_WRITE_PRIVILEGES)
        }
        for relation in _ALL_RELATIONS
    }


def test_captured_default_privileges_fresh_install_is_read_only_for_app_runtime(
    tmp_path: Path,
) -> None:
    # Reproduces the captured worker_writer defaults: the reviewed shared and product
    # SQL remove exactly the inherited DML/MAINTAIN on the nine relations they create,
    # including the automatically updatable app_pointer view, and keep SELECT.
    with _database(tmp_path) as (contract, _, factory, schema, _):
        _install_production_ledger(factory, schema, captured_defaults=True)
        installed, profile = loader._ensure_schema_profile(
            contract, factory, required_profile=loader.PROFILE_RR1,
            required_roles=loader._PRODUCTION_REQUIRED_ROLES,
        )
        assert (installed, profile) == (True, loader.PROFILE_RR1)
        with factory() as conn:
            assert loader._schema_state_and_profile(conn) == ("compatible", loader.PROFILE_RR1)
            privileges = _runtime_table_privileges(conn)
            for relation, held in privileges.items():
                assert held == {
                    "SELECT": True, **{privilege: False for privilege in _WRITE_PRIVILEGES}
                }, relation
            assert conn.execute(
                "SELECT pg_relation_is_updatable('bond_market_implied_rating_app_pointer'::regclass,"
                " false)"
            ).fetchone()[0] != 0
            # Captured function defaults (explicit redundant app_runtime EXECUTE) admit.
            acl = conn.execute(
                "SELECT proacl::text FROM pg_proc WHERE oid="
                "'sec_derived_pointer_guard()'::regprocedure"
            ).fetchone()[0]
            assert "app_runtime=X/worker_writer" in acl


@pytest.mark.parametrize("scope", ["shared", "product"])
def test_unknown_extra_default_grant_aborts_schema_transaction(
    tmp_path: Path, scope: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        if scope == "shared":
            _install_production_ledger(
                factory, schema, captured_defaults=True,
                extra_defaults=("GRANT TRUNCATE ON TABLES TO app_runtime",), check=False,
            )
        else:
            _install_production_ledger(factory, schema, captured_defaults=True)
            _set_default_privileges(schema, "GRANT TRUNCATE ON TABLES TO app_runtime")
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.details["field"].startswith("schema.acl.")
        assert exc.value.phase == "schema_install"
        with factory() as conn:
            assert conn.execute(
                "SELECT to_regclass('bond_market_implied_rating_v1')"
            ).fetchone()[0] is None
            assert conn.execute(
                "SELECT has_table_privilege('app_runtime', 'sec_derived_publications', "
                "'TRUNCATE')"
            ).fetchone()[0] is (scope == "shared")


# --- r4078254941: lock timeouts during schema setup ---------------------------------


def _assert_schema_lock_timeout(error: loader.ArtifactLoaderError, evidence_dir: Path) -> None:
    assert error.code == loader.ErrorCode.LOCK_TIMEOUT.value
    assert error.details == {"field": "schema.install"}
    assert error.phase == "schema_install"
    assert error.schema_installed is None
    assert error.transaction_outcome == "not_started"
    failures = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in evidence_dir.glob("*-failure-*.json")
    ]
    assert len(failures) == 1
    assert failures[0]["code"] == "lock_timeout"
    assert failures[0]["failure_phase"] == "schema_install"
    assert failures[0]["transaction_outcome"] == "not_started"


def test_schema_setup_parent_pointer_lock_timeout_is_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(loader, "LOCK_TIMEOUT_MS", 300)
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        with factory() as holder, holder.transaction():
            holder.execute(
                "SELECT 1 FROM bond_panel_app_pointer WHERE product=%s FOR UPDATE",
                (loader.PANEL_PRODUCT,),
            )
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._publish_verified_artifact(
                    artifact, contract=contract, connection_factory=factory,
                    evidence_dir=evidence_dir,
                )
        _assert_schema_lock_timeout(exc.value, evidence_dir)
        with factory() as conn:
            assert loader._schema_state(conn) == "absent"


def test_schema_setup_shared_relation_lock_timeout_is_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(loader, "LOCK_TIMEOUT_MS", 300)
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        _install_shared_only(factory)
        with factory() as holder, holder.transaction():
            holder.execute("LOCK TABLE sec_derived_publications IN ACCESS EXCLUSIVE MODE")
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._publish_verified_artifact(
                    artifact, contract=contract, connection_factory=factory,
                    evidence_dir=evidence_dir,
                )
        _assert_schema_lock_timeout(exc.value, evidence_dir)
        with factory() as conn:
            assert loader._schema_state(conn) == "shared_only"


# --- r4078254947: the parent header digest is always enforced -----------------------


@pytest.mark.parametrize("pin", [None, "0" * 64])
def test_parent_header_pin_absence_or_drift_is_refused(tmp_path: Path, pin: str | None) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        drifted = replace(contract, parent=replace(contract.parent, header_sha256=pin))
        with factory() as conn, conn.transaction():
            evidence = loader._verify_parent(conn, contract=contract, lock_pointer=False)
            assert evidence.header_sha256 == contract.parent.header_sha256
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._verify_parent(conn, contract=drifted, lock_pointer=False)
        assert exc.value.code == loader.ErrorCode.PARENT_MISMATCH.value
        assert exc.value.details == {"field": "parent.header_sha256"}


@pytest.mark.parametrize(
    "mutation",
    [
        (
            "UPDATE bond_panel_publications SET gate_evidence=gate_evidence || "
            "'{\"late_note\": 1}'::jsonb "
            "WHERE publication_id='11111111-1111-4111-8111-111111111111'"
        ),
        (
            "UPDATE bond_panel_publications SET source_lineage=source_lineage || "
            "'{\"late_note\": 1}'::jsonb "
            "WHERE publication_id='11111111-1111-4111-8111-111111111111'"
        ),
        (
            "UPDATE bond_panel_publications SET code_revision='fixture-parent-rewritten' "
            "WHERE publication_id='22222222-2222-4222-8222-222222222222'"
        ),
        (
            "UPDATE bond_panel_publications SET input_fingerprint=repeat('0', 64) "
            "WHERE publication_id='22222222-2222-4222-8222-222222222222'"
        ),
    ],
)
def test_non_core_header_drift_is_refused_by_pinned_digest(tmp_path: Path, mutation: str) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        with factory() as conn:
            conn.execute(mutation)
        with factory() as conn, conn.transaction(), pytest.raises(
            loader.ArtifactLoaderError
        ) as exc:
            loader._verify_parent(conn, contract=contract, lock_pointer=False)
        assert exc.value.code == loader.ErrorCode.PARENT_MISMATCH.value
        assert exc.value.details == {"field": "parent.header_sha256"}


def test_unpinned_ready_contract_is_refused_before_database(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        unpinned = replace(contract, parent=replace(contract.parent, header_sha256=None))
        counted, calls = _counting(factory)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=unpinned, connection_factory=counted, evidence_dir=evidence_dir
            )
        assert exc.value.code == loader.ErrorCode.CONTRACT_INVALID.value
        assert calls == []
        with factory() as conn:
            assert loader._schema_state(conn) == "absent"


# --- r4074042094: forged VerifiedArtifact metadata never reaches the database -------


def test_forged_metadata_is_refused_without_connecting_and_control_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        _write_release_context(evidence_dir, contract)
        _approve_release_context(evidence_dir, monkeypatch)
        monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
        counted, calls = _counting(factory)
        publication, summary = artifact.publication, artifact.summary
        forgeries = [
            replace(artifact, publication=replace(publication, publication_id=str(uuid4()))),
            replace(artifact, publication=replace(
                publication, panel_publication_id=contract.parent.parent_publication_id
            )),
            replace(artifact, publication=replace(publication, code_revision="forged")),
            replace(artifact, publication=replace(publication, input_fingerprint="0" * 64)),
            replace(artifact, publication=replace(publication, l_anchor=float("nan"))),
            replace(artifact, publication=replace(publication, row_count=True)),
            replace(artifact, summary=replace(summary, witnessed_count=summary.witnessed_count - 1)),
            replace(artifact, summary=replace(summary, rows_digest="0" * 64)),
        ]
        for forged in forgeries:
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader.publish_verified_artifact(
                    forged, evidence_dir=evidence_dir, connection_factory=counted
                )
            assert exc.value.code == loader.ErrorCode.IDENTITY_MISMATCH.value
            with pytest.raises(loader.ArtifactLoaderError):
                loader._publish_verified_artifact(
                    forged, contract=contract, connection_factory=counted,
                    evidence_dir=evidence_dir,
                )
        assert calls == []
        assert not list(evidence_dir.glob("*pre-apply*"))
        with factory() as conn:
            assert loader._schema_state(conn) == "shared_only"
        result = loader.publish_verified_artifact(
            artifact, evidence_dir=evidence_dir, connection_factory=counted
        )
        assert result.outcome == "published_verified"
        calls.clear()
        for forged in forgeries:
            for operation in (
                loader.dry_run_verified_artifact, loader.recover_published_artifact
            ):
                with pytest.raises(loader.ArtifactLoaderError):
                    operation(forged, evidence_dir=evidence_dir, connection_factory=counted)
        assert calls == []
        with factory() as conn:
            assert str(conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0]) == contract.identity.publication_id
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1"
            ).fetchone()[0] == artifact.summary.row_count


# --- RR1 shared-ledger extension profile ---------------------------------------------


def _assert_nothing_published(factory: Callable[[], psycopg.Connection]) -> None:
    with factory() as conn:
        assert conn.execute(
            "SELECT to_regclass('bond_market_implied_rating_v1')"
        ).fetchone()[0] is None
        assert conn.execute(
            "SELECT count(*) FROM sec_derived_publications WHERE product=%s", (loader.PRODUCT,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM sec_derived_current_pointers WHERE product=%s",
            (loader.PRODUCT,),
        ).fetchone()[0] == 0


def test_baseline_profile_is_admitted_shared_only_and_complete(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        _install_shared_only(factory)
        with factory() as conn:
            assert loader._schema_state_and_profile(conn) == (
                "shared_only", loader.PROFILE_BASELINE
            )
        assert loader._ensure_schema_profile(contract, factory) == (
            True, loader.PROFILE_BASELINE
        )
        with factory() as conn:
            assert loader._schema_state_and_profile(conn) == (
                "compatible", loader.PROFILE_BASELINE
            )
            # A clean install leaves every protected function ACL NULL (owner/PUBLIC).
            assert conn.execute(
                "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname=current_schema() AND p.proacl IS NOT NULL "
                "AND p.proname=ANY(%s)",
                ([s.split("(")[0] for s in (
                    *loader._SHARED_FUNCTION_CONTRACTS, *loader._PRODUCT_FUNCTION_CONTRACTS
                )],),
            ).fetchone()[0] == 0


def test_rr1_pair_is_admitted_shared_only_and_complete(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, schema, _):
        _install_production_ledger(factory, schema)
        assert loader._ensure_schema_profile(
            contract, factory, required_profile=loader.PROFILE_RR1
        ) == (True, loader.PROFILE_RR1)
        with factory() as conn:
            assert loader._schema_state_and_profile(conn) == ("compatible", loader.PROFILE_RR1)
            observed = {
                (row[0], row[1]): row[2:]
                for row in conn.execute(
                    "SELECT t.tgname,c.relname,p.oid::regprocedure::text,t.tgtype,t.tgenabled,"
                    "encode(sha256(convert_to(pg_get_triggerdef(t.oid,true),'UTF8')),'hex') "
                    "FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                    "JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname LIKE 'rr1\\_%%' "
                    "AND c.relnamespace=current_schema()::regnamespace AND c.relname=ANY(%s)",
                    (list(loader._SHARED_SCHEMA_OBJECTS),),
                ).fetchall()
            }
            assert observed == {
                key: (value[0], value[1], value[2], value[3])
                for key, value in loader._RR1_TRIGGER_CONTRACTS.items()
            }


def _function_redefinition(signature: str, body_edit: tuple[str, str]) -> str:
    name = signature.split("(")[0]
    source = (loader.ROOT / "schemas" / "rr1_fee_profiles.sql").read_text(encoding="utf-8")
    start = source.index(f"CREATE OR REPLACE FUNCTION {name}()")
    end = source.index("END $$;", start) + len("END $$;")
    definition = source[start:end]
    assert body_edit[0] in definition
    return definition.replace(body_edit[0], body_edit[1], 1)


_POINTER_GUARD = "rr1_fee_profile_current_pointer_guard"
_VALIDATION_GUARD = "rr1_fee_profile_publication_validation_guard"
_RR1_DRIFTS: dict[str, tuple[tuple[str, ...], str]] = {
    "missing_pointer_trigger": (
        (f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",), "schema.triggers",
    ),
    "missing_validation_trigger": (
        (f"DROP TRIGGER {_VALIDATION_GUARD} ON sec_derived_publications",), "schema.triggers",
    ),
    "triggers_gone_functions_linger": (
        (
            f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",
            f"DROP TRIGGER {_VALIDATION_GUARD} ON sec_derived_publications",
        ),
        "schema.functions",
    ),
    "one_function_missing": (
        (
            f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",
            f"DROP TRIGGER {_VALIDATION_GUARD} ON sec_derived_publications",
            f"DROP FUNCTION {_POINTER_GUARD}()",
        ),
        "schema.functions",
    ),
    "extra_rr1_like_trigger": (
        (
            "CREATE TRIGGER rr1_fee_profile_extra_guard BEFORE UPDATE ON "
            f"sec_derived_publications FOR EACH ROW EXECUTE FUNCTION {_VALIDATION_GUARD}()",
        ),
        "schema.triggers",
    ),
    "renamed_trigger": (
        (
            f"ALTER TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers "
            f"RENAME TO {_POINTER_GUARD}_v2",
        ),
        "schema.triggers",
    ),
    "disabled_trigger": (
        (f"ALTER TABLE sec_derived_publications DISABLE TRIGGER {_VALIDATION_GUARD}",),
        "schema.triggers",
    ),
    "replica_only_trigger": (
        (f"ALTER TABLE sec_derived_current_pointers ENABLE REPLICA TRIGGER {_POINTER_GUARD}",),
        "schema.triggers",
    ),
    "event_drift": (
        (
            f"DROP TRIGGER {_VALIDATION_GUARD} ON sec_derived_publications",
            f"CREATE TRIGGER {_VALIDATION_GUARD} BEFORE INSERT OR UPDATE ON "
            f"sec_derived_publications FOR EACH ROW EXECUTE FUNCTION {_VALIDATION_GUARD}()",
        ),
        "schema.triggers",
    ),
    "when_clause_drift": (
        (
            f"DROP TRIGGER {_VALIDATION_GUARD} ON sec_derived_publications",
            f"CREATE TRIGGER {_VALIDATION_GUARD} BEFORE UPDATE ON sec_derived_publications "
            "FOR EACH ROW WHEN (OLD.product = 'rr1_fee_profile_v1') "
            f"EXECUTE FUNCTION {_VALIDATION_GUARD}()",
        ),
        "schema.triggers",
    ),
    "statement_level": (
        (
            f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",
            f"CREATE TRIGGER {_POINTER_GUARD} BEFORE INSERT OR UPDATE OR DELETE ON "
            f"sec_derived_current_pointers FOR EACH STATEMENT EXECUTE FUNCTION {_POINTER_GUARD}()",
        ),
        "schema.triggers",
    ),
    "table_drift": (
        (
            f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",
            f"CREATE TRIGGER {_POINTER_GUARD} BEFORE INSERT OR UPDATE OR DELETE ON "
            f"sec_derived_publications FOR EACH ROW EXECUTE FUNCTION {_POINTER_GUARD}()",
        ),
        "schema.triggers",
    ),
    "function_swap": (
        (
            f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",
            f"CREATE TRIGGER {_POINTER_GUARD} BEFORE INSERT OR UPDATE OR DELETE ON "
            f"sec_derived_current_pointers FOR EACH ROW EXECUTE FUNCTION {_VALIDATION_GUARD}()",
        ),
        "schema.triggers",
    ),
    "predicate_drift": (
        (_function_redefinition(
            f"{_POINTER_GUARD}()", ("NEW.product='rr1_fee_profile_v1'", "NEW.product='rr1_%'")
        ),),
        "schema.functions",
    ),
    "product_literal_drift": (
        (_function_redefinition(
            f"{_VALIDATION_GUARD}()",
            ("OLD.product='rr1_fee_profile_v1'", "OLD.product='bond_market_implied_rating_v1'"),
        ),),
        "schema.functions",
    ),
    "whitespace_only_source_drift": (
        (_function_redefinition(f"{_POINTER_GUARD}()", ("RETURN COALESCE(NEW,OLD);",
                                                          "RETURN COALESCE(NEW, OLD);")),),
        "schema.functions",
    ),
    "config_drift": (
        (f"ALTER FUNCTION {_POINTER_GUARD}() SET search_path = pg_catalog",), "schema.functions",
    ),
    "owner_drift": (
        (f"ALTER FUNCTION {_VALIDATION_GUARD}() OWNER TO app_runtime",), "schema.functions",
    ),
    "security_definer": (
        (f"ALTER FUNCTION {_VALIDATION_GUARD}() SECURITY DEFINER",), "schema.functions",
    ),
    "strict_drift": ((f"ALTER FUNCTION {_POINTER_GUARD}() STRICT",), "schema.functions"),
    "parallel_drift": (
        (f"ALTER FUNCTION {_POINTER_GUARD}() PARALLEL SAFE",), "schema.functions",
    ),
    "leakproof_drift": ((f"ALTER FUNCTION {_POINTER_GUARD}() LEAKPROOF",), "schema.functions"),
    "volatility_drift": ((f"ALTER FUNCTION {_POINTER_GUARD}() STABLE",), "schema.functions"),
    "overload": (
        (
            f"CREATE FUNCTION {_POINTER_GUARD}(x integer) RETURNS integer "
            "LANGUAGE sql AS 'SELECT 1'",
        ),
        "schema.functions",
    ),
    # A defaulted overload makes the zero-argument call ambiguous, so PostgreSQL
    # already renders the trigger definition differently.
    "overload_with_default": (
        (
            f"CREATE FUNCTION {_POINTER_GUARD}(x integer DEFAULT 1) RETURNS integer "
            "LANGUAGE sql AS 'SELECT 1'",
        ),
        "schema.triggers",
    ),
}


def _foreign_schema_drift(schema: str) -> tuple[str, ...]:
    foreign = sql.Identifier(f"{schema}_foreign").as_string(None)
    body = _function_redefinition(f"{_POINTER_GUARD}()", ("BEGIN", "BEGIN"))
    return (
        f"CREATE SCHEMA {foreign}",
        body.replace(
            f"CREATE OR REPLACE FUNCTION {_POINTER_GUARD}()",
            f"CREATE FUNCTION {foreign}.{_POINTER_GUARD}()",
        ),
        f"ALTER FUNCTION {foreign}.{_POINTER_GUARD}() OWNER TO worker_writer",
        f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",
        f"CREATE TRIGGER {_POINTER_GUARD} BEFORE INSERT OR UPDATE OR DELETE ON "
        "sec_derived_current_pointers FOR EACH ROW EXECUTE FUNCTION "
        f"{foreign}.{_POINTER_GUARD}()",
    )


@pytest.mark.parametrize("stage", ["shared_only", "compatible"])
@pytest.mark.parametrize("drift", [*_RR1_DRIFTS, "foreign_schema_function"])
def test_rr1_drift_refuses_before_writes(tmp_path: Path, drift: str, stage: str) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        if stage == "compatible":
            assert loader._ensure_schema_profile(contract, factory)[1] == loader.PROFILE_RR1
        if drift == "foreign_schema_function":
            statements, field = _foreign_schema_drift(schema), "schema.trigger_binding"
        else:
            statements, field = _RR1_DRIFTS[drift]
        try:
            _admin_in_schema(schema, *statements)
            with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state_and_profile(conn)
            assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
            assert exc.value.details == {"field": field}
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._publish_verified_artifact(
                    artifact, contract=contract, connection_factory=factory,
                    evidence_dir=evidence_dir,
                )
            assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
            assert exc.value.phase == "schema_install"
            with factory() as conn:
                assert conn.execute(
                    "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                    (loader.PRODUCT,),
                ).fetchone()[0] == 0
                if stage == "shared_only":
                    assert conn.execute(
                        "SELECT to_regclass('bond_market_implied_rating_v1')"
                    ).fetchone()[0] is None
        finally:
            _admin_execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(f"{schema}_foreign")
            ))


@pytest.mark.parametrize(
    ("statement", "field"),
    [
        ("ALTER TABLE sec_derived_publications DISABLE TRIGGER sec_derived_publications_immutable",
         "schema.triggers"),
        ("DROP TRIGGER sec_derived_publications_delete_guard ON sec_derived_publications",
         "schema.triggers"),
        ("ALTER FUNCTION sec_derived_pointer_guard() SECURITY DEFINER", "schema.functions"),
        ("ALTER FUNCTION sec_set_current_derived_publication(text,uuid,boolean) "
         "SET search_path = pg_catalog", "schema.functions"),
    ],
)
def test_original_sec_contract_drift_still_refuses_with_rr1_present(
    tmp_path: Path, statement: str, field: str
) -> None:
    with _database(tmp_path) as (_, _, factory, schema, _):
        _install_production_ledger(factory, schema)
        _admin_in_schema(schema, statement)
        with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._schema_state_and_profile(conn)
        assert exc.value.details == {"field": field}


@pytest.mark.parametrize("entrypoint", ["dry_run", "recover", "publish"])
def test_production_envelope_refuses_rr1_pair_disappearance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entrypoint: str
) -> None:
    # Absent and baseline-only ledgers are valid for disposable installs but never
    # for the production target, whose envelope pins the RR1-present profile.
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
        _write_release_context(evidence_dir, contract)
        _approve_release_context(evidence_dir, monkeypatch)
        function = {
            "dry_run": loader.dry_run_verified_artifact,
            "recover": loader.recover_published_artifact,
            "publish": loader.publish_verified_artifact,
        }[entrypoint]
        for prepare in (None, _install_shared_only):
            if prepare is not None:
                prepare(factory)
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                function(artifact, evidence_dir=evidence_dir, connection_factory=factory)
            assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
            assert exc.value.details == {"field": "schema.profile"}
        _assert_nothing_published(factory)


def test_production_envelope_refuses_published_baseline_after_rr1_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        loader._publish_verified_artifact(
            artifact, contract=contract, connection_factory=factory, evidence_dir=evidence_dir
        )
        _admin_in_schema(
            schema,
            f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",
            f"DROP TRIGGER {_VALIDATION_GUARD} ON sec_derived_publications",
            f"DROP FUNCTION {_POINTER_GUARD}()",
            f"DROP FUNCTION {_VALIDATION_GUARD}()",
        )
        monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
        with factory() as conn:
            assert loader._schema_state_and_profile(conn) == (
                "compatible", loader.PROFILE_BASELINE
            )
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader.recover_published_artifact(
                artifact, evidence_dir=evidence_dir, connection_factory=factory
            )
        assert exc.value.details == {"field": "schema.profile"}


def test_missing_production_role_blocks_and_ddl_never_creates_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parked = f"app_runtime_parked_{uuid4().hex[:8]}"
    _admin_execute(sql.SQL("ALTER ROLE app_runtime RENAME TO {}").format(sql.Identifier(parked)))
    try:
        with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
            _install_production_ledger(factory, schema)
            assert loader._ensure_schema_profile(contract, factory)[1] == loader.PROFILE_RR1
            with factory() as conn:
                assert conn.execute(
                    "SELECT count(*) FROM pg_roles WHERE rolname='app_runtime'"
                ).fetchone()[0] == 0
            monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader.dry_run_verified_artifact(
                    artifact, evidence_dir=evidence_dir, connection_factory=factory
                )
            assert exc.value.details == {"field": "schema.required_role.app_runtime"}
    finally:
        _admin_execute(sql.SQL("ALTER ROLE {} RENAME TO app_runtime").format(
            sql.Identifier(parked)
        ))


def test_concurrent_profile_drift_fails_schema_recheck_and_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        original = loader._ensure_schema_profile

        def ensure_then_drop_rr1(*args: object, **kwargs: object) -> tuple[bool, str]:
            result = original(*args, **kwargs)  # type: ignore[arg-type]
            _admin_in_schema(
                schema,
                f"DROP TRIGGER {_POINTER_GUARD} ON sec_derived_current_pointers",
                f"DROP TRIGGER {_VALIDATION_GUARD} ON sec_derived_publications",
                f"DROP FUNCTION {_POINTER_GUARD}()",
                f"DROP FUNCTION {_VALIDATION_GUARD}()",
            )
            return result

        monkeypatch.setattr(loader, "_ensure_schema_profile", ensure_then_drop_rr1)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.details == {"field": "schema.profile_changed"}
        assert exc.value.phase == "schema_recheck"
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0


def _flip_profile_after_parent_calls(
    monkeypatch: pytest.MonkeyPatch, calls_before_flip: int
) -> None:
    original_parent = loader._verify_parent
    original_state = loader._schema_state_and_profile
    seen = {"parent": 0}

    def counting_parent(*args: object, **kwargs: object) -> loader.ParentEvidence:
        seen["parent"] += 1
        return original_parent(*args, **kwargs)  # type: ignore[arg-type]

    def flipping_state(conn: psycopg.Connection) -> tuple[str, str | None]:
        state, profile = original_state(conn)
        if seen["parent"] >= calls_before_flip:
            return state, loader.PROFILE_BASELINE
        return state, profile

    monkeypatch.setattr(loader, "_verify_parent", counting_parent)
    monkeypatch.setattr(loader, "_schema_state_and_profile", flipping_state)


def test_profile_drift_at_precommit_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        # Calls: schema setup, parent preflight, parent recheck -> flip at precommit.
        _flip_profile_after_parent_calls(monkeypatch, 3)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.details == {"field": "schema.profile_changed"}
        assert exc.value.phase == "schema_precommit"
        assert not list(evidence_dir.glob("*precommit*"))
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0
            assert conn.execute("SELECT count(*) FROM bond_market_implied_rating_v1").fetchone()[
                0
            ] == 0


def test_read_only_profile_drift_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        loader._publish_verified_artifact(
            artifact, contract=contract, connection_factory=factory, evidence_dir=evidence_dir
        )
        # Calls: first parent read, second parent read -> flip at the final recheck.
        _flip_profile_after_parent_calls(monkeypatch, 2)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._read_only_operation(
                artifact, contract=contract, connection_factory=factory,
                require_published=True, required_profile=loader.PROFILE_RR1,
            )
        assert exc.value.details == {"field": "schema.profile_changed"}


# --- Semantic function EXECUTE admission ----------------------------------------------

_ACL_FUNCTIONS = (
    "sec_set_current_derived_publication(text,uuid,boolean)",
    f"{_POINTER_GUARD}()",
    "bond_market_implied_rating_v1_write_guard()",
)


def _complete_production(
    contract: loader.FrozenArtifactContract,
    factory: Callable[[], psycopg.Connection],
    schema: str,
) -> None:
    _install_production_ledger(factory, schema)
    assert loader._ensure_schema_profile(contract, factory) == (True, loader.PROFILE_RR1)


def _function_acl(conn: psycopg.Connection, signature: str) -> str | None:
    return conn.execute(
        "SELECT proacl::text FROM pg_proc WHERE oid=%s::regprocedure", (signature,)
    ).fetchone()[0]


@pytest.mark.parametrize(
    "statements",
    [
        pytest.param((), id="null_acl"),
        pytest.param(
            ("REVOKE EXECUTE ON FUNCTION {f} FROM PUBLIC",
             "GRANT EXECUTE ON FUNCTION {f} TO PUBLIC"),
            id="explicit_owner_public",
        ),
        pytest.param(
            ("GRANT EXECUTE ON FUNCTION {f} TO app_runtime",),
            id="redundant_app_runtime",
        ),
        pytest.param(
            ("GRANT EXECUTE ON FUNCTION {f} TO app_runtime",
             "REVOKE EXECUTE ON FUNCTION {f} FROM PUBLIC",
             "GRANT EXECUTE ON FUNCTION {f} TO PUBLIC"),
            id="reordered_with_app_runtime",
        ),
    ],
)
@pytest.mark.parametrize("function", _ACL_FUNCTIONS)
def test_semantically_default_function_acls_are_admitted(
    tmp_path: Path, function: str, statements: tuple[str, ...]
) -> None:
    with _database(tmp_path) as (contract, _, factory, schema, _):
        _complete_production(contract, factory, schema)
        with factory() as conn:
            for statement in statements:
                conn.execute(statement.format(f=function))
            acl = _function_acl(conn, function)
            assert (acl is None) is (not statements)
            assert loader._schema_state_and_profile(conn) == ("compatible", loader.PROFILE_RR1)
            assert _function_acl(conn, function) == acl


@pytest.mark.parametrize(
    "statements",
    [
        pytest.param(("GRANT EXECUTE ON FUNCTION {f} TO app_analytics_ro",),
                     id="explicit_app_analytics_ro"),
        pytest.param(("GRANT EXECUTE ON FUNCTION {f} TO {probe}",), id="unknown_role"),
        pytest.param(("GRANT EXECUTE ON FUNCTION {f} TO app_runtime WITH GRANT OPTION",),
                     id="grant_option"),
        pytest.param(("REVOKE EXECUTE ON FUNCTION {f} FROM PUBLIC",), id="public_missing"),
        pytest.param(("REVOKE EXECUTE ON FUNCTION {f} FROM worker_writer",), id="owner_missing"),
    ],
)
@pytest.mark.parametrize("function", _ACL_FUNCTIONS)
def test_unsafe_function_acls_are_refused(
    tmp_path: Path, function: str, statements: tuple[str, ...]
) -> None:
    probe = f"acl_fn_probe_{uuid4().hex[:10]}"
    _admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(probe)))
    try:
        with _database(tmp_path) as (contract, _, factory, schema, _):
            _complete_production(contract, factory, schema)
            with factory() as conn:
                for statement in statements:
                    conn.execute(statement.format(f=function, probe=probe))
                before = _function_acl(conn, function)
            with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state_and_profile(conn)
            assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
            assert exc.value.details == {"field": f"schema.function_acl.{function}"}
            with factory() as conn:
                assert _function_acl(conn, function) == before
    finally:
        _admin_execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(probe)))
        _admin_execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(probe)))


def test_function_grant_issued_by_non_owner_is_refused(tmp_path: Path) -> None:
    function = "sec_derived_publication_as_of(uuid)"
    with _database(tmp_path) as (contract, _, factory, schema, _):
        _complete_production(contract, factory, schema)
        _admin_in_schema(
            schema,
            sql.SQL("GRANT USAGE ON SCHEMA {} TO app_runtime").format(sql.Identifier(schema)),
        )
        try:
            with factory() as conn:
                conn.execute(
                    f"GRANT EXECUTE ON FUNCTION {function} TO app_runtime WITH GRANT OPTION"
                )
            _admin_in_schema(
                schema, "SET ROLE app_runtime",
                f"GRANT EXECUTE ON FUNCTION {function} TO app_analytics_ro",
            )
            with factory() as conn:
                acl = _function_acl(conn, function)
                assert "app_analytics_ro=X/app_runtime" in (acl or "")
            with factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state_and_profile(conn)
            assert exc.value.details == {"field": f"schema.function_acl.{function}"}
        finally:
            _admin_in_schema(
                schema,
                sql.SQL("REVOKE USAGE ON SCHEMA {} FROM app_runtime").format(
                    sql.Identifier(schema)
                ),
            )


# --- Production ACL posture, worker workflow and non-RR1 isolation --------------------


def test_bad_shared_prestate_is_not_repaired_by_worker_reruns_until_approved_revoke(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema, captured_defaults=True)
        with factory() as conn:
            # The captured live posture: app_runtime=arwdm on the four shared tables.
            for relation in loader._SHARED_SCHEMA_OBJECTS:
                conn.execute(sql.SQL(
                    "GRANT INSERT, UPDATE, DELETE, MAINTAIN ON {} TO app_runtime"
                ).format(sql.Identifier(relation)))
            before = _relation_acls(conn)
        # Any derived worker re-running the shared/RR1 protocol must not repair it.
        with factory() as conn, conn.transaction():
            for name in ("sec_derived_publications.sql", "rr1_fee_profiles.sql"):
                conn.execute((loader.ROOT / "schemas" / name).read_text(encoding="utf-8"))
        with factory() as conn:
            assert _relation_acls(conn) == before
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.details == {"field": "schema.acl.sec_derived_current_pointers"}
        _assert_nothing_published(factory)
        with factory() as conn:
            assert _relation_acls(conn) == before
            # The separately approved remediation: exactly this delta, SELECT kept.
            for relation in loader._SHARED_SCHEMA_OBJECTS:
                conn.execute(sql.SQL(
                    "REVOKE INSERT, UPDATE, DELETE, MAINTAIN ON {} FROM app_runtime RESTRICT"
                ).format(sql.Identifier(relation)))
            assert loader._schema_state_and_profile(conn) == ("shared_only", loader.PROFILE_RR1)
        _write_release_context(evidence_dir, contract)
        _approve_release_context(evidence_dir, monkeypatch)
        monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
        result = loader.publish_verified_artifact(
            artifact, evidence_dir=evidence_dir, connection_factory=factory
        )
        assert result.outcome == "published_verified"


def test_production_shaped_workflow_rr1_isolation_and_app_runtime_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema, captured_defaults=True)
        _write_release_context(evidence_dir, contract)
        _approve_release_context(evidence_dir, monkeypatch)
        monkeypatch.setattr(loader, "_contract_from_artifact", lambda _: contract)
        # The legitimate worker pointer workflow passes both RR1 guards untouched.
        result = loader.publish_verified_artifact(
            artifact, evidence_dir=evidence_dir, connection_factory=factory
        )
        assert result.outcome == "published_verified"
        replay = loader.publish_verified_artifact(
            artifact, evidence_dir=evidence_dir, connection_factory=factory
        )
        assert replay.outcome == "already_published_verified"
        dry = loader.dry_run_verified_artifact(
            artifact, evidence_dir=evidence_dir, connection_factory=factory
        )
        assert dry.outcome == "already_published_verified"
        with factory() as conn:
            run_id, package_id = conn.execute(
                "SELECT source_run_id, source_package_id FROM sec_derived_publications "
                "WHERE product=%s", (loader.PRODUCT,),
            ).fetchone()
            # The RR1 guards stay active for their own product only.
            rr1_publication = uuid4()
            conn.execute(
                "INSERT INTO sec_derived_publications (publication_id,product,"
                "publication_version,source_run_id,source_package_id,build_fingerprint) "
                "VALUES (%s,'rr1_fee_profile_v1',1,%s,%s,%s)",
                (rr1_publication, run_id, package_id, "a" * 64),
            )
            with pytest.raises(psycopg.errors.RaiseException, match="RR1 fee-profile validation"):
                conn.execute("SELECT sec_validate_derived_publication(%s)", (rr1_publication,))
            assert loader._schema_state_and_profile(conn) == ("compatible", loader.PROFILE_RR1)
        _admin_in_schema(
            schema,
            sql.SQL("GRANT USAGE ON SCHEMA {} TO app_runtime").format(sql.Identifier(schema)),
        )
        try:
            with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"], autocommit=True) as runtime:
                runtime.execute("SET ROLE app_runtime")
                runtime.execute(
                    sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
                )
                assert runtime.execute(
                    "SELECT count(*) FROM bond_market_implied_rating_v1_current"
                ).fetchone()[0] == artifact.summary.row_count
                assert runtime.execute(
                    "SELECT publication_id FROM bond_market_implied_rating_app_pointer"
                ).fetchone()[0] == UUID(contract.identity.publication_id)
                for statement in (
                    "UPDATE bond_market_implied_rating_app_pointer SET changed_at=now()",
                    "DELETE FROM bond_market_implied_rating_v1_builds",
                    "UPDATE sec_derived_current_pointers SET set_at=now()",
                    "DELETE FROM sec_derived_pointer_tokens",
                    "INSERT INTO sec_derived_publication_tokens VALUES (gen_random_uuid(), 1)",
                    "SELECT sec_set_current_derived_publication('"
                    f"{loader.PRODUCT}', '{contract.identity.publication_id}'::uuid, false)",
                ):
                    with pytest.raises(psycopg.errors.InsufficientPrivilege):
                        runtime.execute(statement)
        finally:
            _admin_in_schema(
                schema,
                sql.SQL("REVOKE USAGE ON SCHEMA {} FROM app_runtime").format(
                    sql.Identifier(schema)
                ),
            )
        with factory() as conn:
            assert str(conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0]) == contract.identity.publication_id


def test_outer_transaction_rollback_with_rr1_present(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, _):
        _install_production_ledger(factory, schema)
        assert loader._ensure_schema_profile(contract, factory) == (True, loader.PROFILE_RR1)
        payload = loader._artifact_payload(artifact, contract)
        with factory() as conn, pytest.raises(RuntimeError), conn.transaction():
            loader._set_local_timeouts(conn)
            loader._acquire_product_lock(conn)
            loader._verify_parent(conn, contract=contract, lock_pointer=True)
            materialize(conn, artifact.publication, payload, expected_pointer=None)
            loader._verify_stored_publication(
                conn, artifact.publication, contract=contract, require_current=True
            )
            raise RuntimeError("interrupt before commit")
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == 0
            assert conn.execute("SELECT count(*) FROM sec_derived_current_pointers").fetchone()[
                0
            ] == 0
            assert loader._schema_state_and_profile(conn) == ("compatible", loader.PROFILE_RR1)


# --- r4081728357: genuine LOGIN worker session on every connection -------------------

_SUBSTITUTE_LOGIN = "loader_substitute_login"


def _admin_factory(schema: str, *setup: str) -> Callable[[], psycopg.Connection]:
    def connect() -> psycopg.Connection:
        conn = psycopg.connect(
            os.environ["SEC_TEST_DATABASE_URL"], autocommit=True, connect_timeout=5,
            options=f"-c search_path={schema},public",
        )
        for statement in setup:
            conn.execute(statement)
        return conn

    return connect


def _worker_factory(schema: str, *setup: str) -> Callable[[], psycopg.Connection]:
    def connect() -> psycopg.Connection:
        conn = psycopg.connect(
            _worker_dsn(), autocommit=True, connect_timeout=5,
            options=f"-c search_path={schema},public",
        )
        for statement in setup:
            conn.execute(statement)
        return conn

    return connect


@contextmanager
def _substitute_login() -> Iterator[Callable[[str], Callable[[], psycopg.Connection]]]:
    """A second disposable LOGIN role that is a SET-enabled member of worker_writer."""
    password = uuid4().hex
    _admin_execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
        sql.Identifier(_SUBSTITUTE_LOGIN), sql.Literal(password)
    ))
    _admin_execute(sql.SQL("GRANT worker_writer TO {} WITH INHERIT FALSE, SET TRUE").format(
        sql.Identifier(_SUBSTITUTE_LOGIN)
    ))
    base = psycopg.conninfo.conninfo_to_dict(_worker_dsn())

    def factory(schema: str) -> Callable[[], psycopg.Connection]:
        def connect() -> psycopg.Connection:
            conn = psycopg.connect(
                **{**base, "user": _SUBSTITUTE_LOGIN, "password": password},
                autocommit=True, connect_timeout=5,
                options=f"-c search_path={schema},public",
            )
            conn.execute("SET ROLE worker_writer")
            return conn

        return connect

    try:
        yield factory
    finally:
        _admin_execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(_SUBSTITUTE_LOGIN)))


def _assert_session_refused(error: loader.ArtifactLoaderError) -> None:
    assert error.code == loader.ErrorCode.IDENTITY_MISMATCH.value
    assert error.details == {"field": "database.session_role"}


def _run_all_connection_paths(
    contract: loader.FrozenArtifactContract,
    artifact: loader.VerifiedArtifact,
    factory: Callable[[], psycopg.Connection],
    evidence_dir: Path,
) -> None:
    for operation in (
        lambda: loader._ensure_schema_profile(contract, factory),
        lambda: loader._read_only_operation(
            artifact, contract=contract, connection_factory=factory, require_published=False
        ),
        lambda: loader._publish_verified_artifact(
            artifact, contract=contract, connection_factory=factory, evidence_dir=evidence_dir
        ),
    ):
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            operation()
        _assert_session_refused(exc.value)


@pytest.mark.parametrize("stage", ["absent", "shared_only", "complete"])
@pytest.mark.parametrize(
    "variant",
    ["admin_login", "admin_set_role_worker", "worker_set_role_self", "member_set_role_worker"],
)
def test_non_genuine_worker_sessions_are_refused_before_any_ddl_or_lock(
    tmp_path: Path, stage: str, variant: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        if stage in {"shared_only", "complete"}:
            _install_production_ledger(factory, schema)
        if stage == "complete":
            assert loader._ensure_schema_profile(contract, factory) == (
                True, loader.PROFILE_RR1
            )
        with factory() as conn:
            before = loader._schema_state(conn)
        with _substitute_login() as substitute:
            bad = {
                "admin_login": _admin_factory(schema),
                "admin_set_role_worker": _admin_factory(schema, "SET ROLE worker_writer"),
                "worker_set_role_self": _worker_factory(schema, "SET ROLE worker_writer"),
                "member_set_role_worker": substitute(schema),
            }[variant]
            _run_all_connection_paths(contract, artifact, bad, evidence_dir)
        with factory() as conn:
            assert loader._schema_state(conn) == before
            if stage != "absent":
                assert conn.execute(
                    "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                    (loader.PRODUCT,),
                ).fetchone()[0] == 0
        # The offline pre-apply receipt precedes every connection by design; no
        # database-phase evidence may exist after a refused session.
        names = [path.name for path in evidence_dir.glob("*.json")]
        assert not [name for name in names if "-precommit-" in name or "-readback-" in name]
        failures = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in evidence_dir.glob("*-failure-*.json")
        ]
        assert [receipt["failure_phase"] for receipt in failures] == ["schema_install"]
        assert failures[0]["code"] == "identity_mismatch"


@pytest.mark.parametrize(
    ("alter", "restore"),
    [
        ("ALTER ROLE worker_writer SUPERUSER", "ALTER ROLE worker_writer NOSUPERUSER"),
        ("ALTER ROLE worker_writer CREATEROLE", "ALTER ROLE worker_writer NOCREATEROLE"),
        ("ALTER ROLE worker_writer CREATEDB", "ALTER ROLE worker_writer NOCREATEDB"),
        ("ALTER ROLE worker_writer REPLICATION", "ALTER ROLE worker_writer NOREPLICATION"),
        ("ALTER ROLE worker_writer BYPASSRLS", "ALTER ROLE worker_writer NOBYPASSRLS"),
        ("GRANT pg_create_subscription TO worker_writer",
         "REVOKE pg_create_subscription FROM worker_writer"),
    ],
)
def test_elevated_worker_role_is_refused(tmp_path: Path, alter: str, restore: str) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        elevated_role = f"loader_elevated_{uuid4().hex[:10]}"
        if alter.startswith("GRANT"):
            # A membership path to an elevated role, rather than a role attribute.
            _admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN CREATEDB").format(
                sql.Identifier(elevated_role)
            ))
            alter = f"GRANT {elevated_role} TO worker_writer WITH INHERIT FALSE, SET TRUE"
            restore = f"REVOKE {elevated_role} FROM worker_writer"
        _admin_execute(alter)
        try:
            _run_all_connection_paths(contract, artifact, factory, evidence_dir)
        finally:
            _admin_execute(restore)
            _admin_execute(sql.SQL("DROP ROLE IF EXISTS {}").format(
                sql.Identifier(elevated_role)
            ))
        with factory() as conn:
            assert loader._schema_state(conn) == "shared_only"


def test_genuine_worker_session_passes_and_innocuous_membership_is_allowed(
    tmp_path: Path,
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        _admin_execute("GRANT app_analytics_ro TO worker_writer WITH INHERIT FALSE, SET TRUE")
        try:
            with factory() as conn:
                loader._verify_worker_session(conn)
                assert conn.execute("SELECT current_setting('role')").fetchone()[0] == "none"
            result = loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
            assert result.outcome == "published_verified"
        finally:
            _admin_execute("REVOKE app_analytics_ro FROM worker_writer")


def _substitute_nth_connection(
    good: Callable[[], psycopg.Connection],
    bad: Callable[[], psycopg.Connection],
    substitute_at: int,
) -> tuple[Callable[[], psycopg.Connection], list[int]]:
    calls: list[int] = []

    def factory() -> psycopg.Connection:
        calls.append(1)
        return bad() if len(calls) == substitute_at else good()

    return factory, calls


def test_substituted_publication_connection_is_refused_before_writes(
    tmp_path: Path,
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        # Connection 1: schema setup (genuine), connection 2: publication (admin).
        substituted, calls = _substitute_nth_connection(
            factory, _admin_factory(schema, "SET ROLE worker_writer"), 2
        )
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=substituted,
                evidence_dir=evidence_dir,
            )
        _assert_session_refused(exc.value)
        assert exc.value.phase == "transaction_setup"
        assert exc.value.transaction_outcome == "not_started"
        assert len(calls) == 2
        failures = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in evidence_dir.glob("*-failure-*.json")
        ]
        assert [receipt["failure_phase"] for receipt in failures] == ["transaction_setup"]
        with factory() as conn:
            # The genuinely committed schema transaction is not undone.
            assert loader._schema_state_and_profile(conn) == ("compatible", loader.PROFILE_RR1)
        _assert_nothing_published_rows(factory)


def _assert_nothing_published_rows(factory: Callable[[], psycopg.Connection]) -> None:
    with factory() as conn:
        for statement in (
            "SELECT count(*) FROM bond_market_implied_rating_v1",
            "SELECT count(*) FROM bond_market_implied_rating_v1_builds",
            "SELECT count(*) FROM sec_derived_current_pointers WHERE product="
            "'bond_market_implied_rating_v1'",
            "SELECT count(*) FROM sec_derived_publications WHERE product="
            "'bond_market_implied_rating_v1'",
        ):
            assert conn.execute(statement).fetchone()[0] == 0, statement


def test_substituted_fresh_readback_yields_recovery_required(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        # Connection 3 is the fresh post-commit readback.
        substituted, _ = _substitute_nth_connection(
            factory, _admin_factory(schema, "SET ROLE worker_writer"), 3
        )
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=substituted,
                evidence_dir=evidence_dir,
            )
        # Existing typed postcommit taxonomy: committed, readback not trusted.
        assert exc.value.code == loader.ErrorCode.COMMIT_UNKNOWN.value
        assert exc.value.details == {"field": "transaction.postcommit_readback"}
        assert exc.value.phase == "postcommit_readback"
        assert exc.value.transaction_outcome == "committed"
        assert exc.value.outcome == "recovery_required"
        assert isinstance(exc.value.__cause__, loader.ArtifactLoaderError)
        assert exc.value.__cause__.details == {"field": "database.session_role"}
        # The committed publication is intact and genuinely recoverable.
        recovered = loader._read_only_operation(
            artifact, contract=contract, connection_factory=factory, require_published=True
        )
        assert recovered.outcome == "already_published_verified"


def test_session_change_inside_publication_is_refused_at_precommit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        original = loader._verify_parent
        seen = {"calls": 0}

        def switch_role_after_parent_recheck(conn, **kwargs):  # type: ignore[no-untyped-def]
            seen["calls"] += 1
            result = original(conn, **kwargs)
            # Calls: schema setup, publication preflight, publication recheck.
            if seen["calls"] == 3:
                conn.execute("SET LOCAL ROLE worker_writer")
            return result

        monkeypatch.setattr(loader, "_verify_parent", switch_role_after_parent_recheck)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        _assert_session_refused(exc.value)
        assert exc.value.phase == "session_precommit"
        assert exc.value.transaction_outcome == "not_committed"
        assert not list(evidence_dir.glob("*-precommit-*"))
        _assert_nothing_published_rows(factory)


# --- r4081728337 / r4081728342: exact column expressions and persistence --------------


def test_clean_install_column_expressions_match_pinned_pg18_catalog(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, schema, _):
        _install_production_ledger(factory, schema)
        assert loader._ensure_schema_profile(contract, factory)[0] is True
        with factory() as conn:
            for name in loader._PHYSICAL_TABLES:
                observed = loader._relation_columns(conn, name)
                assert observed == loader._expected_relation_columns(name), name
            defaults = {
                (row[0], row[1]): row[2]
                for row in conn.execute(
                    "SELECT c.relname, a.attname, pg_get_expr(d.adbin, d.adrelid, false) "
                    "FROM pg_attrdef d JOIN pg_class c ON c.oid=d.adrelid "
                    "JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum "
                    "WHERE c.relnamespace=current_schema()::regnamespace AND c.relname=ANY(%s)",
                    (list(loader._PHYSICAL_TABLES),),
                ).fetchall()
            }
            assert defaults == {
                ("sec_derived_publications", "prepared_at"): "now()",
                ("sec_derived_publications", "lifecycle_state"): "'prepared'::text",
                ("sec_derived_current_pointers", "set_at"): "now()",
                ("bond_market_implied_rating_v1_builds", "created_at"): "now()",
            }


_EXPRESSION_DRIFTS: tuple[tuple[str, str, str], ...] = (
    ("sec_derived_publications", "prepared_at",
     "ALTER TABLE sec_derived_publications ALTER COLUMN prepared_at "
     "SET DEFAULT '2000-01-01 00:00:00+00'::timestamptz"),
    ("sec_derived_publications", "prepared_at",
     "ALTER TABLE sec_derived_publications ALTER COLUMN prepared_at "
     "SET DEFAULT clock_timestamp()"),
    ("sec_derived_publications", "prepared_at",
     "ALTER TABLE sec_derived_publications ALTER COLUMN prepared_at DROP DEFAULT"),
    ("sec_derived_publications", "lifecycle_state",
     "ALTER TABLE sec_derived_publications ALTER COLUMN lifecycle_state "
     "SET DEFAULT 'validated'"),
    ("sec_derived_publications", "lifecycle_state",
     "ALTER TABLE sec_derived_publications ALTER COLUMN lifecycle_state "
     "SET DEFAULT 'PREPARED'"),
    # A genuinely recorded null-producing default on a defaultless column.  (A bare
    # DEFAULT NULL is not recorded by PG18 at all; see the equivalence test below.)
    ("sec_derived_publications", "validated_at",
     "ALTER TABLE sec_derived_publications ALTER COLUMN validated_at "
     "SET DEFAULT nullif(now(), now())"),
    ("sec_derived_current_pointers", "set_at",
     "ALTER TABLE sec_derived_current_pointers ALTER COLUMN set_at "
     "SET DEFAULT clock_timestamp()"),
    ("sec_derived_current_pointers", "set_at",
     "ALTER TABLE sec_derived_current_pointers ALTER COLUMN set_at DROP DEFAULT"),
    ("sec_derived_publication_tokens", "backend_pid",
     "ALTER TABLE sec_derived_publication_tokens ALTER COLUMN backend_pid "
     "SET DEFAULT pg_backend_pid()"),
    ("bond_market_implied_rating_v1_builds", "created_at",
     "ALTER TABLE bond_market_implied_rating_v1_builds ALTER COLUMN created_at "
     "SET DEFAULT '2000-01-01 00:00:00+00'::timestamptz"),
    ("bond_market_implied_rating_v1_builds", "row_count",
     "ALTER TABLE bond_market_implied_rating_v1_builds ALTER COLUMN row_count "
     "ADD GENERATED ALWAYS AS IDENTITY"),
    ("bond_market_implied_rating_v1", "spell_id",
     "ALTER TABLE bond_market_implied_rating_v1 ALTER COLUMN spell_id "
     "ADD GENERATED BY DEFAULT AS IDENTITY"),
    ("bond_market_implied_rating_v1", "cusip_id",
     "ALTER TABLE bond_market_implied_rating_v1 ALTER COLUMN cusip_id SET DEFAULT ''"),
)


@pytest.mark.parametrize(
    ("relation", "column", "statement"), _EXPRESSION_DRIFTS,
    ids=[f"{r}.{c}-{i}" for i, (r, c, _) in enumerate(_EXPRESSION_DRIFTS)],
)
def test_column_expression_drift_is_refused_and_never_repaired(
    tmp_path: Path, relation: str, column: str, statement: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        product = relation.startswith("bond_market_")
        if product:
            assert loader._ensure_schema_profile(contract, factory)[0] is True
        _admin_in_schema(schema, statement)
        with factory() as conn:
            drifted = loader._relation_columns(conn, relation)
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.details == {"field": f"schema.columns.{relation}"}
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.details == {"field": f"schema.columns.{relation}"}
        with factory() as conn:
            assert loader._relation_columns(conn, relation) == drifted
            if not product:
                assert conn.execute(
                    "SELECT to_regclass('bond_market_implied_rating_v1')"
                ).fetchone()[0] is None


def test_bare_default_null_is_not_recorded_by_pg18_and_is_catalog_identical(
    tmp_path: Path,
) -> None:
    # PG18 stores no pg_attrdef entry for a constant NULL default, so it cannot
    # differ from "no default" in any catalog field the loader could compare.
    with _database(tmp_path) as (_, _, factory, schema, _):
        _install_production_ledger(factory, schema)
        with factory() as conn:
            before = loader._relation_columns(conn, "sec_derived_publications")
        _admin_in_schema(
            schema,
            "ALTER TABLE sec_derived_publications ALTER COLUMN validated_at SET DEFAULT NULL",
        )
        with factory() as conn:
            assert loader._relation_columns(conn, "sec_derived_publications") == before
            assert conn.execute(
                "SELECT count(*) FROM pg_attrdef d JOIN pg_attribute a "
                "ON a.attrelid=d.adrelid AND a.attnum=d.adnum "
                "WHERE d.adrelid='sec_derived_publications'::regclass "
                "AND a.attname='validated_at'"
            ).fetchone()[0] == 0
            assert loader._schema_state_and_profile(conn) == ("shared_only", loader.PROFILE_RR1)


@pytest.mark.parametrize("mode", ["stored", "virtual"])
def test_generated_column_replacement_table_is_refused(tmp_path: Path, mode: str) -> None:
    # PG18 cannot convert an existing column to a generated one; replace the token
    # table in the disposable schema, keeping names, types and nullability.
    with _database(tmp_path) as (_, _, factory, schema, _):
        _install_production_ledger(factory, schema)
        _admin_in_schema(
            schema,
            "ALTER TABLE sec_derived_publication_tokens RENAME TO sec_derived_publication_tokens_old",
            "CREATE TABLE sec_derived_publication_tokens ("
            "publication_id uuid NOT NULL, "
            f"backend_pid integer GENERATED ALWAYS AS (1) {mode.upper()})",
            "ALTER TABLE sec_derived_publication_tokens OWNER TO worker_writer",
            "DROP TABLE sec_derived_publication_tokens_old CASCADE",
        )
        with factory() as conn:
            generated = conn.execute(
                "SELECT attgenerated::text FROM pg_attribute WHERE attrelid="
                "'sec_derived_publication_tokens'::regclass AND attname='backend_pid'"
            ).fetchone()[0]
            assert generated == ("s" if mode == "stored" else "v")
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.details["field"].startswith("schema.")


def _set_unlogged_with_referencers(schema: str, relation: str) -> list[str]:
    """SET UNLOGGED on ``relation`` and every table transitively referencing it.

    PostgreSQL forbids a permanent table referencing an unlogged one, so the whole
    foreign-key referencer closure (e.g. the RR1 fee-profile tables referencing the
    ledger) goes UNLOGGED together, deepest referencers first.
    """
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"], autocommit=True) as admin:
        admin.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        closure = [
            row[0] for row in admin.execute(
                "WITH RECURSIVE ref(oid) AS (SELECT %s::regclass::oid UNION "
                "SELECT c.conrelid FROM pg_constraint c JOIN ref ON c.confrelid=ref.oid "
                "WHERE c.contype='f' AND c.conrelid<>c.confrelid) "
                "SELECT DISTINCT cl.relname FROM ref JOIN pg_class cl ON cl.oid=ref.oid",
                (relation,),
            ).fetchall()
        ]
        pending = list(closure)
        for _ in range(len(closure) + 1):
            remaining = []
            for name in pending:
                try:
                    admin.execute(
                        sql.SQL("ALTER TABLE {} SET UNLOGGED").format(sql.Identifier(name))
                    )
                except psycopg.errors.InvalidTableDefinition:
                    remaining.append(name)
            pending = remaining
            if not pending:
                break
        assert not pending, pending
    return closure


@pytest.mark.parametrize("relation", loader._PHYSICAL_TABLES)
def test_unlogged_physical_table_is_refused_and_never_repaired(
    tmp_path: Path, relation: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        product = relation.startswith("bond_market_")
        if product:
            assert loader._ensure_schema_profile(contract, factory)[0] is True
        assert relation in _set_unlogged_with_referencers(schema, relation)
        with factory() as conn:
            persistence = conn.execute(
                "SELECT relpersistence FROM pg_class WHERE oid=%s::regclass", (relation,)
            ).fetchone()[0]
            assert persistence == "u"
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.details["field"].startswith("schema.persistence.")
        for operation in (
            lambda: loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            ),
            lambda: loader._read_only_operation(
                artifact, contract=contract, connection_factory=factory,
                require_published=product,
            ),
        ):
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                operation()
            assert exc.value.details["field"].startswith("schema.persistence.")
        with factory() as conn:
            assert conn.execute(
                "SELECT relpersistence FROM pg_class WHERE oid=%s::regclass", (relation,)
            ).fetchone()[0] == "u"
        _assert_nothing_published_rows_if_present(factory)


def _assert_nothing_published_rows_if_present(
    factory: Callable[[], psycopg.Connection],
) -> None:
    with factory() as conn:
        assert conn.execute(
            "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
            (loader.PRODUCT,),
        ).fetchone()[0] == 0


def test_pristine_schema_security_and_indexes_admit(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, _):
        with factory() as conn:
            assert loader._schema_state(conn) == "absent"
        _install_shared_only(factory)
        with factory() as conn:
            assert loader._schema_state(conn) == "shared_only"
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            assert loader._schema_state(conn) == "compatible"
            assert conn.execute(
                "SELECT count(*) FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid "
                "WHERE idx.relname=ANY(%s) AND i.indisvalid AND i.indisready AND i.indislive",
                (list(loader._SECONDARY_INDEXES),),
            ).fetchone()[0] == 2
            with conn.transaction():
                conn.execute("SET LOCAL enable_seqscan TO off")
                plan = "\n".join(row[0] for row in conn.execute(
                    "EXPLAIN SELECT month FROM bond_market_implied_rating_v1 "
                    "WHERE cusip_id=%s ORDER BY month", ("000000000",)
                ).fetchall())
                assert "bond_market_implied_rating_v1_cusip_month_idx" in plan
        result = loader._read_only_operation(
            artifact, contract=contract, connection_factory=factory, require_published=False
        )
        assert result.outcome == "dry_run_verified"
        _assert_nothing_published_rows(factory)


@pytest.mark.parametrize("relation", loader._PHYSICAL_TABLES)
@pytest.mark.parametrize("mutation", ["enabled", "forced", "policy"])
def test_rls_and_policies_refuse_before_publication(
    tmp_path: Path, relation: str, mutation: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        product = relation not in loader._SHARED_SCHEMA_OBJECTS
        if product:
            assert loader._ensure_schema(contract, factory)
        else:
            _install_shared_only(factory)
        with factory() as conn:
            statement = {
                "enabled": "ALTER TABLE {} ENABLE ROW LEVEL SECURITY",
                "forced": "ALTER TABLE {} FORCE ROW LEVEL SECURITY",
                "policy": "CREATE POLICY admission_probe ON {} USING (true)",
            }[mutation]
            conn.execute(sql.SQL(statement).format(sql.Identifier(relation)))
        for operation in (
            lambda: loader._read_only_operation(
                artifact, contract=contract, connection_factory=factory,
                require_published=False,
            ),
            lambda: loader._ensure_schema(contract, factory),
            lambda: loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            ),
        ):
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                operation()
            assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
            assert exc.value.details == {"field": f"schema.rls.{relation}"}
        _assert_nothing_published_rows_if_present(factory)
        if not product:
            _assert_nothing_published(factory)


@pytest.mark.parametrize("stage", ["absent", "shared_only", "compatible"])
def test_runtime_schema_create_refuses_at_every_admission_stage(
    tmp_path: Path, stage: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        if stage == "shared_only":
            _install_shared_only(factory)
        elif stage == "compatible":
            assert loader._ensure_schema(contract, factory)
        _admin_in_schema(schema, sql.SQL("GRANT CREATE ON SCHEMA {} TO app_runtime").format(
            sql.Identifier(schema)
        ))
        for operation in (
            lambda: loader._read_only_operation(
                artifact, contract=contract, connection_factory=factory,
                require_published=False,
            ),
            lambda: loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            ),
        ):
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                operation()
            assert exc.value.details == {"field": "schema.runtime_create"}
        if stage != "absent":
            _assert_nothing_published_rows_if_present(factory)
        with factory() as conn:
            assert conn.execute(
                "SELECT has_schema_privilege('app_runtime', %s, 'CREATE')", (schema,)
            ).fetchone()[0] is True
        if stage == "shared_only":
            _assert_nothing_published(factory)


@pytest.mark.parametrize(
    ("grant", "field"),
    [
        ("PUBLIC", "schema.reader_role.app_analytics_ro"),
        ("app_runtime", "schema.runtime_create"),
    ],
)
def test_runtime_schema_create_grant_is_not_repaired(
    tmp_path: Path, grant: str, field: str
) -> None:
    with _database(tmp_path) as (contract, _, factory, schema, _):
        _install_shared_only(factory)
        _admin_in_schema(schema, sql.SQL("GRANT CREATE ON SCHEMA {} TO {}").format(
            sql.Identifier(schema), sql.SQL(grant)
        ))
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._ensure_schema(contract, factory)
        assert exc.value.details == {"field": field}
        _assert_nothing_published(factory)


@pytest.mark.parametrize("reader", loader._READER_ROLES)
def test_reader_create_in_later_search_path_schema_is_refused(
    tmp_path: Path, reader: str
) -> None:
    shadow = f"schema_shadow_{uuid4().hex[:12]}"
    with _database(tmp_path) as (contract, _, factory, schema, _):
        _install_shared_only(factory)
        _admin_execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(shadow)))
        try:
            _admin_execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO worker_writer").format(
                sql.Identifier(shadow)
            ))

            def searched_factory() -> psycopg.Connection:
                return psycopg.connect(
                    _worker_dsn(), autocommit=True,
                    options=f"-c search_path={schema},{shadow},public",
                )

            with searched_factory() as conn:
                assert conn.execute("SELECT current_schemas(false)").fetchone()[0] == [
                    schema, shadow, "public"
                ]
                assert loader._schema_state(conn) == "shared_only"
            _admin_execute(sql.SQL("GRANT CREATE ON SCHEMA {} TO {}").format(
                sql.Identifier(shadow), sql.Identifier(reader)
            ))
            with searched_factory() as conn, pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(conn)
            field = (
                "schema.runtime_create" if reader == "app_runtime"
                else "schema.reader_role.app_analytics_ro"
            )
            assert exc.value.details == {"field": field}
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._ensure_schema(contract, searched_factory)
            assert exc.value.details == {"field": field}
            _assert_nothing_published(factory)
        finally:
            _admin_execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(shadow)))


def test_admin_only_membership_to_schema_creator_is_refused(tmp_path: Path) -> None:
    creator = f"schema_admin_{uuid4().hex[:12]}"
    _admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(creator)))
    try:
        with _database(tmp_path) as (contract, _, factory, schema, _):
            _install_shared_only(factory)
            _admin_in_schema(schema, sql.SQL("GRANT CREATE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(creator)
            ))
            _admin_execute(sql.SQL(
                "GRANT {} TO app_runtime WITH INHERIT FALSE, SET FALSE, ADMIN TRUE"
            ).format(sql.Identifier(creator)))
            try:
                with factory() as conn:
                    assert conn.execute(
                        "SELECT has_schema_privilege('app_runtime', %s, 'CREATE'), "
                        "pg_has_role('app_runtime', %s, 'SET'), "
                        "pg_has_role('app_runtime', %s, 'MEMBER WITH ADMIN OPTION')",
                        (schema, creator, creator),
                    ).fetchone() == (False, False, True)
                    with pytest.raises(loader.ArtifactLoaderError) as exc:
                        loader._schema_state(conn)
                assert exc.value.details == {"field": "schema.runtime_create"}
                with pytest.raises(loader.ArtifactLoaderError) as exc:
                    loader._ensure_schema(contract, factory)
                assert exc.value.details == {"field": "schema.runtime_create"}
                _assert_nothing_published(factory)
            finally:
                _admin_execute(sql.SQL("REVOKE {} FROM app_runtime").format(
                    sql.Identifier(creator)
                ))
    finally:
        _admin_execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(creator)))
        _admin_execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(creator)))


def test_runtime_schema_owner_is_refused_without_relation_acl_drift(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_shared_only(factory)
        with factory() as conn:
            owner = conn.execute(
                "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=%s",
                (schema,),
            ).fetchone()[0]
        _admin_in_schema(schema, sql.SQL("ALTER SCHEMA {} OWNER TO app_runtime").format(
            sql.Identifier(schema)
        ))
        try:
            for operation in (
                lambda: loader._read_only_operation(
                    artifact, contract=contract, connection_factory=factory,
                    require_published=False,
                ),
                lambda: loader._publish_verified_artifact(
                    artifact, contract=contract, connection_factory=factory,
                    evidence_dir=evidence_dir,
                ),
            ):
                with pytest.raises(loader.ArtifactLoaderError) as exc:
                    operation()
                assert exc.value.details == {"field": "schema.runtime_create"}
            _assert_nothing_published(factory)
        finally:
            _admin_in_schema(schema, sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                sql.Identifier(schema), sql.Identifier(owner)
            ))


def test_set_only_schema_creator_role_is_refused(tmp_path: Path) -> None:
    creator = f"schema_creator_{uuid4().hex[:12]}"
    _admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(creator)))
    try:
        with _database(tmp_path) as (contract, _, factory, schema, _):
            _install_shared_only(factory)
            _admin_in_schema(schema, sql.SQL("GRANT CREATE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(creator)
            ))
            _admin_execute(sql.SQL(
                "GRANT {} TO app_runtime WITH INHERIT FALSE, SET TRUE"
            ).format(sql.Identifier(creator)))
            try:
                with factory() as conn:
                    assert conn.execute(
                        "SELECT has_schema_privilege('app_runtime', %s, 'CREATE'), "
                        "pg_has_role('app_runtime', %s, 'SET')", (schema, creator)
                    ).fetchone() == (False, True)
                    with pytest.raises(loader.ArtifactLoaderError) as exc:
                        loader._schema_state(conn)
                assert exc.value.details == {"field": "schema.runtime_create"}
                with pytest.raises(loader.ArtifactLoaderError) as exc:
                    loader._ensure_schema(contract, factory)
                assert exc.value.details == {"field": "schema.runtime_create"}
                _assert_nothing_published(factory)
            finally:
                _admin_execute(sql.SQL("REVOKE {} FROM app_runtime").format(
                    sql.Identifier(creator)
                ))
    finally:
        _admin_execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(creator)))
        _admin_execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(creator)))


def test_set_only_schema_owner_role_is_refused(tmp_path: Path) -> None:
    owner_role = f"schema_owner_{uuid4().hex[:12]}"
    _admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(owner_role)))
    try:
        with _database(tmp_path) as (contract, _, factory, schema, _):
            _install_shared_only(factory)
            with factory() as conn:
                prior_owner = conn.execute(
                    "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=%s",
                    (schema,),
                ).fetchone()[0]
                before = _relation_acls(conn)
            _admin_in_schema(schema, sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                sql.Identifier(schema), sql.Identifier(owner_role)
            ))
            try:
                # Ownership can drop a schema even after ordinary CREATE is revoked.
                _admin_in_schema(schema, sql.SQL("REVOKE CREATE ON SCHEMA {} FROM {}").format(
                    sql.Identifier(schema), sql.Identifier(owner_role)
                ))
                _admin_execute(sql.SQL(
                    "GRANT {} TO app_runtime WITH INHERIT FALSE, SET TRUE"
                ).format(sql.Identifier(owner_role)))
                try:
                    with factory() as conn:
                        assert conn.execute(
                            "SELECT has_schema_privilege('app_runtime', %s, 'CREATE'), "
                            "has_schema_privilege(%s, %s, 'CREATE'), "
                            "pg_has_role('app_runtime', %s, 'SET')",
                            (schema, owner_role, schema, owner_role),
                        ).fetchone() == (False, False, True)
                        assert _relation_acls(conn) == before
                        with pytest.raises(loader.ArtifactLoaderError) as exc:
                            loader._schema_state(conn)
                    assert exc.value.details == {"field": "schema.runtime_create"}
                    with pytest.raises(loader.ArtifactLoaderError) as exc:
                        loader._ensure_schema(contract, factory)
                    assert exc.value.details == {"field": "schema.runtime_create"}
                    _assert_nothing_published(factory)
                    with factory() as conn:
                        assert _relation_acls(conn) == before
                finally:
                    _admin_execute(sql.SQL("REVOKE {} FROM app_runtime").format(
                        sql.Identifier(owner_role)
                    ))
            finally:
                _admin_in_schema(schema, sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                    sql.Identifier(schema), sql.Identifier(prior_owner)
                ))
    finally:
        _admin_execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(owner_role)))


@pytest.mark.parametrize(
    "mutation", ["missing", "replaced", "descending", "partial", "included", "unique", "invalid"]
)
@pytest.mark.parametrize("index", tuple(loader._SECONDARY_INDEXES))
def test_secondary_index_drift_refuses_without_a_new_publication(
    tmp_path: Path, index: str, mutation: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        assert loader._ensure_schema(contract, factory)
        if mutation == "invalid":
            # The already-published fixture supplies duplicate month values, so
            # CREATE UNIQUE INDEX CONCURRENTLY leaves an invalid index on failure.
            result = loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
            assert result.outcome == "published_verified"
        with factory() as conn:
            before = conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0]
            pointer = conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()
            conn.execute(sql.SQL("DROP INDEX {}").format(sql.Identifier(index)))
            if mutation in {"replaced", "descending", "partial", "included", "unique"}:
                first, second = loader._SECONDARY_INDEXES[index]
                definition = {
                    "replaced": sql.SQL("(month, publication_id)"),
                    "descending": sql.SQL("({} DESC, {})").format(
                        sql.Identifier(first), sql.Identifier(second)
                    ),
                    "partial": sql.SQL("({}, {}) WHERE witnessed").format(
                        sql.Identifier(first), sql.Identifier(second)
                    ),
                    "included": sql.SQL("({}, {}) INCLUDE (witnessed)").format(
                        sql.Identifier(first), sql.Identifier(second)
                    ),
                    "unique": sql.SQL("({}, {})").format(
                        sql.Identifier(first), sql.Identifier(second)
                    ),
                }[mutation]
                prefix = "CREATE UNIQUE INDEX" if mutation == "unique" else "CREATE INDEX"
                conn.execute(sql.SQL(prefix + " {} ON bond_market_implied_rating_v1 ").format(
                    sql.Identifier(index)
                ) + definition)
        if mutation == "invalid":
            with psycopg.connect(_worker_dsn(), autocommit=True,
                                 options=f"-c search_path={schema},public") as conn:
                with pytest.raises(psycopg.errors.UniqueViolation):
                    conn.execute(sql.SQL(
                        "CREATE UNIQUE INDEX CONCURRENTLY {} "
                        "ON bond_market_implied_rating_v1 (month)"
                    ).format(sql.Identifier(index)))
                assert conn.execute(
                    "SELECT indisvalid, indisready FROM pg_index "
                    "WHERE indexrelid=%s::regclass",
                    (index,),
                ).fetchone() == (False, True)
        for operation in (
            lambda: loader._read_only_operation(
                artifact, contract=contract, connection_factory=factory,
                require_published=False,
            ),
            lambda: loader._ensure_schema(contract, factory),
            lambda: loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            ),
        ):
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                operation()
            assert exc.value.details == {"field": f"schema.index.{index}"}
        with factory() as conn:
            assert conn.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone()[0] == before
            assert conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
                (loader.PRODUCT,),
            ).fetchone() == pointer
        if mutation != "invalid":
            _assert_nothing_published_rows(factory)


@pytest.mark.parametrize(("flag", "column"), [("invalid", 7), ("not_ready", 8)])
def test_mocked_index_catalog_flag_is_refused_with_unchanged_definition(
    tmp_path: Path, flag: str, column: int,
) -> None:
    with _database(tmp_path) as (contract, _, factory, _, _):
        assert loader._ensure_schema(contract, factory)
        with factory() as conn:
            assert loader._schema_state(conn) == "compatible"

            class MutatedIndexCatalog:
                def execute(self, statement: str, params: object = None) -> object:
                    cursor = conn.execute(statement, params)
                    if "FROM pg_catalog.pg_class idx " not in statement:
                        return cursor

                    class MutatedIndexRows:
                        def fetchall(self) -> list[tuple[object, ...]]:
                            rows = cursor.fetchall()
                            return [
                                (*row[:column], False, *row[column + 1:])
                                if row[0] == "bond_market_implied_rating_v1_pub_month_idx" else row
                                for row in rows
                            ]

                    return MutatedIndexRows()

            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(MutatedIndexCatalog())  # type: ignore[arg-type]
            assert exc.value.details == {
                "field": "schema.index.bond_market_implied_rating_v1_pub_month_idx"
            }, flag
        _assert_nothing_published_rows(factory)


@pytest.mark.parametrize("drift", ["rls", "index", "schema_create"])
def test_admission_drift_at_precommit_rolls_back_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        assert loader._ensure_schema(contract, factory)
        original = loader.materialize

        def materialize_then_drift(
            conn: psycopg.Connection, *args: object, **kwargs: object
        ) -> None:
            original(conn, *args, **kwargs)  # type: ignore[arg-type]
            if drift == "rls":
                conn.execute("ALTER TABLE bond_market_implied_rating_v1 ENABLE ROW LEVEL SECURITY")
            elif drift == "index":
                conn.execute("DROP INDEX bond_market_implied_rating_v1_cusip_month_idx")
            else:
                _admin_in_schema(schema, sql.SQL("GRANT CREATE ON SCHEMA {} TO app_runtime").format(
                    sql.Identifier(schema)
                ))

        monkeypatch.setattr(loader, "materialize", materialize_then_drift)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value
        assert exc.value.phase == "schema_precommit"
        assert exc.value.details == {"field": {
            "rls": "schema.rls.bond_market_implied_rating_v1",
            "index": "schema.index.bond_market_implied_rating_v1_cusip_month_idx",
            "schema_create": "schema.runtime_create",
        }[drift]}
        assert not list(evidence_dir.glob("*precommit*"))
        _assert_nothing_published_rows(factory)
        with factory() as conn:
            if drift == "schema_create":
                assert conn.execute(
                    "SELECT has_schema_privilege('app_runtime', %s, 'CREATE')", (schema,)
                ).fetchone()[0] is True
            else:
                assert loader._schema_state(conn) == "compatible"


def test_temporary_shadow_table_is_not_admitted_as_the_ledger(tmp_path: Path) -> None:
    with _database(tmp_path) as (contract, _, factory, schema, _):
        _install_production_ledger(factory, schema)
        _admin_in_schema(schema, "DROP TABLE sec_derived_pointer_tokens")
        with factory() as conn:
            # A session-local shadow with the same name lives in pg_temp, not the
            # ledger schema; the ledger is then partial and refused.
            conn.execute(
                "CREATE TEMPORARY TABLE sec_derived_pointer_tokens "
                "(product text NOT NULL, backend_pid integer NOT NULL)"
            )
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._schema_state(conn)
        assert exc.value.code == loader.ErrorCode.SCHEMA_MISMATCH.value


# --- r4081728318: durable receipts around the publication transaction -----------------


def test_precommit_directory_fsync_failure_rolls_back_everything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        original_persist = loader._persist_receipt
        original_fsync = loader._DurableFs.fsync
        state = {"precommit": False}

        def persist(evidence: Path, *, phase: str, payload: bytes) -> loader.ReceiptRef:
            state["precommit"] = phase == "precommit"
            try:
                return original_persist(evidence, phase=phase, payload=payload)
            finally:
                state["precommit"] = False

        evidence_stat = os.stat(evidence_dir)
        calls = {"parent": 0, "file": 0, "dir": 0}

        def fsync(fd: int) -> None:
            if state["precommit"]:
                status = os.fstat(fd)
                if os.path.samestat(status, evidence_stat):
                    calls["dir"] += 1
                    # The directory fsync that makes the written file's entry durable.
                    raise OSError("injected directory fsync failure")
                calls["file" if stat.S_ISREG(status.st_mode) else "parent"] += 1
            original_fsync(fd)

        monkeypatch.setattr(loader, "_persist_receipt", persist)
        monkeypatch.setattr(loader._DurableFs, "fsync", staticmethod(fsync))
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value
        assert exc.value.phase == "precommit_receipt"
        assert exc.value.transaction_outcome == "not_committed"
        # Local leaf: parent entry synced, then the file, then the failing directory.
        assert calls == {"parent": 1, "file": 1, "dir": 1}
        _assert_nothing_published_rows(factory)


def test_leftover_local_evidence_leaf_refuses_apply_until_parent_sync_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, _):
        _install_production_ledger(factory, schema)
        leaf = tmp_path / "fresh-evidence"
        parent_stat = os.stat(tmp_path)
        original_fsync = loader._DurableFs.fsync
        state = {"fail": True, "parent_syncs": 0}

        def fsync(fd: int) -> None:
            if os.path.samestat(os.fstat(fd), parent_stat):
                state["parent_syncs"] += 1
                if state["fail"]:
                    raise OSError("injected parent directory fsync failure")
            original_fsync(fd)

        monkeypatch.setattr(loader._DurableFs, "fsync", staticmethod(fsync))
        counted, calls = _counting(factory)
        # Attempt 0 creates the leaf (mkdir succeeds, its parent sync fails); attempt 1
        # finds that leftover leaf.  Neither the pre-apply receipt nor the immediate
        # best-effort failure receipt may treat it as durable, so no connection opens.
        for attempt in range(2):
            with pytest.raises(loader.ArtifactLoaderError) as exc:
                loader._publish_verified_artifact(
                    artifact, contract=contract, connection_factory=counted,
                    evidence_dir=leaf,
                )
            assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value
            assert exc.value.details == {"field": "receipt.write"}
            error = loader._best_effort_failure_receipt(
                exc.value,
                leaf,
                operation_id=str(uuid4()),
                operation_started_at_utc=datetime.now(timezone.utc).isoformat(),
                mode="apply",
                publication_id=artifact.publication.publication_id,
            )
            assert error.receipt_written is False
            assert state["parent_syncs"] == 2 * (attempt + 1)
            assert leaf.is_dir() and not list(leaf.iterdir())
            assert calls == []
        _assert_nothing_published_rows_if_present(factory)
        # Once the parent entry is durable, the same path is admitted end to end.
        state["fail"] = False
        result = loader._publish_verified_artifact(
            artifact, contract=contract, connection_factory=counted, evidence_dir=leaf,
        )
        assert result.outcome == "published_verified"
        assert calls
        assert state["parent_syncs"] > 4
        assert sorted(path.name for path in leaf.iterdir()) == sorted(
            receipt.basename for receipt in result.receipts
        )


def test_postcommit_receipt_fsync_failure_reports_evidence_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, schema, evidence_dir):
        _install_production_ledger(factory, schema)
        original_persist = loader._persist_receipt

        def persist(evidence: Path, *, phase: str, payload: bytes) -> loader.ReceiptRef:
            if phase in {"readback", "failure"}:
                # Even the failure receipt cannot be made durable.
                raise loader.ArtifactLoaderError(
                    loader.ErrorCode.RECEIPT_FAILURE, field="receipt.write"
                )
            return original_persist(evidence, phase=phase, payload=payload)

        monkeypatch.setattr(loader, "_persist_receipt", persist)
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._publish_verified_artifact(
                artifact, contract=contract, connection_factory=factory,
                evidence_dir=evidence_dir,
            )
        assert exc.value.code == loader.ErrorCode.COMMITTED_EVIDENCE_INCOMPLETE.value
        assert exc.value.phase == "final_receipt"
        assert exc.value.transaction_outcome == "committed"
        assert exc.value.outcome == "recovery_required"
        recovered = loader._read_only_operation(
            artifact, contract=contract, connection_factory=factory, require_published=True
        )
        assert recovered.outcome == "already_published_verified"

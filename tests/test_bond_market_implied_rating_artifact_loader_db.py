"""Real PostgreSQL 18 transaction tests for the frozen-artifact loader."""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
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
    _fixture,
    _write_release_context,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL unavailable"
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
    schema = f"test_artifact_loader_{uuid4().hex}"
    artifact_root, raw, original_contract = _fixture(tmp_path)
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
            conn = psycopg.connect(dsn, autocommit=True, connect_timeout=5)
            conn.execute("SET ROLE worker_writer")
            conn.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
            )
            return conn

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
    with _database(tmp_path) as (contract, artifact, factory, _, _):
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


def test_post_schema_refusal_receipt_records_committed_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        def refuse_payload(*_: object, **__: object) -> list[tuple[object, ...]]:
            raise loader.ArtifactLoaderError(
                loader.ErrorCode.ROW_INVALID, field="row.injected_refusal"
            )

        monkeypatch.setattr(loader, "_artifact_payload", refuse_payload)
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
        assert receipt["failure_phase"] == "payload"
        assert receipt["schema_installed"] is True
        assert receipt["transaction_outcome"] == "not_started"
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
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
        release_document = _write_release_context(evidence_dir, contract)
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
    with _database(tmp_path) as (contract, artifact, factory, _, evidence_dir):
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

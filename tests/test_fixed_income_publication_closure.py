"""Idempotent replay of a published fixed-income artifact is O(1).

An already-published manifest used to be re-proved by running ``count(*)`` over
each of the eight target relations -- one of them
``nport_fixed_income_metric_coverage_v2``, 45.6M rows in production -- even though
the manifest bytes, the lifecycle and the current pointer had already been
verified above.

The relations are frozen for a validated publication: the row guards reject every
UPDATE/DELETE outright, an INSERT needs the parent ``prepared``, and TRUNCATE is
refused by its own guard. A closure recorded while the publication was already
validated therefore stays true, and re-proving storage is one indexed row.

These tests pin: the closure is written on publish, the replay reads it and
touches no relation, a tampered/mismatched closure still fails closed, a
publication with no closure recount-then-records (so the cost is paid once), and
``verify_storage`` forces the full recount for restores and audits.
"""
from __future__ import annotations

import gzip
from pathlib import Path
from uuid import uuid4

import pytest

from src.nport import fixed_income_local_materializer as materializer

ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"

AS_OF = "2026-06-30"
COVERAGE = "nport_fixed_income_metric_coverage_v2"


class _RecordingCursor:
    """A cursor that records every statement, delegating everything else."""

    def __init__(self, inner, sink):
        self._inner = inner
        self._sink = sink

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, *exc):
        return self._inner.__exit__(*exc)

    def execute(self, statement, params=None):
        self._sink.append(statement)
        return self._inner.execute(statement, params) if params is not None else self._inner.execute(statement)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _RecordingConnection:
    def __init__(self, inner, sink):
        self._inner = inner
        self._sink = sink

    def transaction(self):
        return self._inner.transaction()

    def cursor(self):
        return _RecordingCursor(self._inner.cursor(), self._sink)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _resource_config(tmp_path: Path) -> materializer.ResourceConfig:
    return materializer.ResourceConfig(
        memory_limit="1GB",
        temp_directory=str(tmp_path),
        postgres_image_digest="sha256:" + "a" * 64,
        postgres_server_fingerprint="test-postgres-18",
    )


def _write_payloads(tmp_path: Path, identity, holdings_publication, run_id) -> dict[str, Path]:
    """Nine payloads: the eight contract relations plus the coverage rollup.

    Two carry a row (one per-position coverage fact and its rollup) so the
    counts are not all zero. The rollup is part of every publication since the
    builder stopped materializing absence: it is the only place the counts of
    what was absent exist, so an artifact without it publishes a coverage
    surface that silently claims full coverage.
    """
    columns = materializer._published_columns()
    payloads: dict[str, Path] = {}
    coverage_row = {
        "publication_id": identity.target_publication_id,
        "source_holdings_publication_id": str(holdings_publication),
        "source_run_id": str(run_id),
        "series_id": "S1",
        "report_date": AS_OF,
        "accession_number": "A1",
        "source_raw_row_id": r"\N",
        "source_file_id": r"\N",
        "source_row_number": r"\N",
        "source_identity_key": "K1",
        "metric_family": "duration",
        "metric_key": "effective_duration",
        "numerator": "1",
        "denominator": "2",
        "denominator_unit": "count",
        "coverage_ratio": "0.5",
        "availability_state": "reported_numeric",
        "missing_reason": r"\N",
        "exclusions": "[]",
        "methodology_version": "nport_fixed_income_features_v2",
    }
    rollup_row = {
        "publication_id": identity.target_publication_id,
        "source_holdings_publication_id": str(holdings_publication),
        "source_run_id": str(run_id),
        "series_id": "S1",
        "report_date": AS_OF,
        "accession_number": "A1",
        "metric_family": "duration",
        "metric_key": "effective_duration",
        "numerator": "1",
        "denominator": "2",
        "denominator_unit": "count",
        "coverage_ratio": "0.5",
        "availability_state": "reported_numeric",
        "methodology_version": "nport_fixed_income_features_v2",
        "exclusions": "[]",
        "source_row_count": "2",
        "reported_row_count": "1",
        "missing_reason_counts": "{}",
    }
    for relation in materializer.PUBLISHED_RELATIONS:
        path = tmp_path / f"{relation}.tsv.gz"
        lines = ["\t".join(columns[relation])]
        if relation == COVERAGE:
            lines.append("\t".join(coverage_row[name] for name in columns[relation]))
        if relation == materializer.COVERAGE_ROLLUP_RELATION:
            lines.append("\t".join(rollup_row[name] for name in columns[relation]))
        with gzip.GzipFile(filename="", mode="wb", fileobj=path.open("wb"), mtime=0) as out:
            out.write(("\n".join(lines) + "\n").encode())
        payloads[relation] = path
    return payloads


def _seed(cur) -> tuple[str, object, object, object]:
    schema = f"fi_closure_fixture_{uuid4().hex}"
    run_id, package_id, holdings_publication = uuid4(), uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS SELECT run_id, raw_validated_at "
        "FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    cur.execute(
        """CREATE TABLE nport_raw_rows(
        raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        ingestion_run_id uuid NOT NULL, source_file_id uuid NOT NULL, source_row_number bigint NOT NULL,
        source_table text NOT NULL, accession_number text, holding_id text, typed_projection jsonb NOT NULL,
        UNIQUE(source_file_id,source_row_number))"""
    )
    for ddl_name in ("sec_derived_publications.sql", "nport_holdings_v2.sql",
                     "nport_fixed_income_features.sql"):
        cur.execute((ROOT / "schemas" / ddl_name).read_text(encoding="utf-8"))
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    cur.execute(
        "INSERT INTO sec_derived_publications"
        "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)"
        " VALUES(%s,'sec_nport_holdings_v2',1,%s,%s,%s)",
        (holdings_publication, run_id, package_id, "a" * 64),
    )
    cur.execute("SELECT sec_validate_derived_publication(%s)", (holdings_publication,))
    cur.execute("SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)",
                (holdings_publication,))
    return schema, run_id, package_id, holdings_publication


@pytest.fixture()
def published(tmp_path):
    """A published fixed-income artifact on a scratch schema, plus its handles."""
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as admin, admin.cursor() as cur:
        schema, run_id, package_id, holdings_publication = _seed(cur)
    dsn = f"{DSN} options=-csearch_path={schema}"
    identity = materializer.BuildIdentity(
        source_publication_id=str(holdings_publication),
        source_run_id=str(run_id),
        source_package_id=str(package_id),
        target_publication_id=str(uuid4()),
        as_of_date=AS_OF,
        contract_digest=materializer.CONTRACT_DIGEST,
    )
    config = _resource_config(tmp_path)
    payloads = _write_payloads(tmp_path, identity, holdings_publication, run_id)
    manifest = materializer.build_manifest(
        identity=identity,
        worker_sha="a" * 40,
        source_files={name: payloads[COVERAGE] for name in materializer.SOURCE_RELATIONS},
        output_files=payloads,
        resource_config=config,
        output_counts={COVERAGE: 1, materializer.COVERAGE_ROLLUP_RELATION: 1},
    )
    (tmp_path / "manifest.json").write_text(
        materializer.canonical_json(manifest), encoding="utf-8"
    )
    with psycopg.connect(dsn) as conn:
        materializer.publish_artifact(
            connection=conn, artifact_dir=tmp_path, identity=identity, resource_config=config
        )
        conn.commit()
    try:
        yield dsn, identity, config, tmp_path, manifest
    finally:
        with psycopg.connect(DSN, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_publish_records_the_closure_and_replay_never_scans_a_relation(published) -> None:
    import psycopg

    dsn, identity, config, artifact_dir, manifest = published
    target = identity.target_publication_id

    with psycopg.connect(dsn) as conn:
        closure = conn.execute(
            "SELECT manifest_sha256, relation_counts FROM nport_fixed_income_publication_closures"
            " WHERE publication_id=%s", (target,)
        ).fetchone()
        assert closure is not None
        assert closure[0] == manifest["manifest_sha256"]
        # The closure carries the per-relation counts the manifest attests, so a
        # replay compares numbers it already has instead of producing them.
        assert closure[1][COVERAGE] == 1
        assert closure[1]["nport_fixed_income_features"] == 0
        # The closure covers the rollup too: it is part of the publication, and
        # it is the relation the reader actually consumes -- a re-proof that
        # skipped it would attest everything except what is served.
        assert set(closure[1]) == set(materializer.PUBLISHED_RELATIONS)
        assert closure[1][materializer.COVERAGE_ROLLUP_RELATION] == 1
        # The published rows really are there: the closure is a shortcut for the
        # re-proof, never a substitute for the write.
        assert conn.execute(
            f"SELECT count(*) FROM {COVERAGE} WHERE publication_id=%s", (target,)
        ).fetchone() == (1,)
        conn.commit()

    statements: list[str] = []
    with psycopg.connect(dsn) as conn:
        materializer.publish_artifact(
            connection=_RecordingConnection(conn, statements), artifact_dir=artifact_dir,
            identity=identity, resource_config=config,
        )
        conn.commit()
    # The whole point: not one count over a target relation.
    assert not [s for s in statements if "count(*) FROM nport_fixed_income" in s]
    assert any("nport_fixed_income_publication_closures" in s for s in statements)


def test_verify_storage_forces_the_full_recount(published) -> None:
    import psycopg

    dsn, identity, config, artifact_dir, _ = published
    statements: list[str] = []
    with psycopg.connect(dsn) as conn:
        materializer.publish_artifact(
            connection=_RecordingConnection(conn, statements), artifact_dir=artifact_dir,
            identity=identity, resource_config=config, verify_storage=True,
        )
        conn.commit()
    counted = [s for s in statements if "count(*) FROM nport_fixed_income" in s]
    assert len(counted) == len(materializer.PUBLISHED_RELATIONS)
    assert any(COVERAGE in s for s in counted)
    assert any(materializer.COVERAGE_ROLLUP_RELATION in s for s in counted)


def test_verify_storage_env_switch_forces_the_full_recount(published, monkeypatch) -> None:
    import psycopg

    dsn, identity, config, artifact_dir, _ = published
    monkeypatch.setenv("NPORT_FI_VERIFY_STORAGE", "1")
    statements: list[str] = []
    with psycopg.connect(dsn) as conn:
        materializer.publish_artifact(
            connection=_RecordingConnection(conn, statements), artifact_dir=artifact_dir,
            identity=identity, resource_config=config,
        )
        conn.commit()
    assert len([s for s in statements if "count(*) FROM nport_fixed_income" in s]) == len(
        materializer.PUBLISHED_RELATIONS
    )


def test_missing_closure_recounts_once_and_then_records_one(published) -> None:
    """A publication from before the closure existed pays the counts exactly once."""
    import psycopg

    dsn, identity, config, artifact_dir, _ = published
    target = identity.target_publication_id
    with psycopg.connect(dsn, autocommit=True) as conn:
        # Only a superuser can drop a closure (the guard forbids DELETE); this
        # simulates the pre-existing publications the migration will meet.
        conn.execute("ALTER TABLE nport_fixed_income_publication_closures DISABLE TRIGGER USER")
        conn.execute("DELETE FROM nport_fixed_income_publication_closures WHERE publication_id=%s",
                     (target,))
        conn.execute("ALTER TABLE nport_fixed_income_publication_closures ENABLE TRIGGER USER")

    first: list[str] = []
    with psycopg.connect(dsn) as conn:
        materializer.publish_artifact(
            connection=_RecordingConnection(conn, first), artifact_dir=artifact_dir,
            identity=identity, resource_config=config,
        )
        conn.commit()
    assert len([s for s in first if "count(*) FROM nport_fixed_income" in s]) == len(
        materializer.PUBLISHED_RELATIONS
    )

    second: list[str] = []
    with psycopg.connect(dsn) as conn:
        materializer.publish_artifact(
            connection=_RecordingConnection(conn, second), artifact_dir=artifact_dir,
            identity=identity, resource_config=config,
        )
        conn.commit()
    assert not [s for s in second if "count(*) FROM nport_fixed_income" in s]


def test_a_closure_that_disagrees_with_the_manifest_fails_closed(published) -> None:
    import psycopg

    dsn, identity, config, artifact_dir, _ = published
    target = identity.target_publication_id
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("ALTER TABLE nport_fixed_income_publication_closures DISABLE TRIGGER USER")
        conn.execute(
            "UPDATE nport_fixed_income_publication_closures "
            f"SET relation_counts = jsonb_set(relation_counts,'{{{COVERAGE}}}','999') "
            "WHERE publication_id=%s", (target,)
        )
        conn.execute("ALTER TABLE nport_fixed_income_publication_closures ENABLE TRIGGER USER")
    with psycopg.connect(dsn) as conn:
        with pytest.raises(materializer.PublicationConflictError,
                           match="recorded storage closure diverges"):
            materializer.publish_artifact(
                connection=conn, artifact_dir=artifact_dir, identity=identity,
                resource_config=config,
            )


def test_the_closure_and_its_relations_are_immutable_in_the_database(published) -> None:
    """The O(1) shortcut rests on DB guards, not on the caller's good behaviour."""
    import psycopg

    dsn, identity, _, _, manifest = published
    target = identity.target_publication_id
    with psycopg.connect(dsn, autocommit=True) as conn:
        with pytest.raises(psycopg.Error, match="closure is immutable"):
            conn.execute("DELETE FROM nport_fixed_income_publication_closures WHERE publication_id=%s",
                         (target,))
        with pytest.raises(psycopg.Error, match="row is immutable"):
            conn.execute(f"DELETE FROM {COVERAGE} WHERE publication_id=%s", (target,))
        with pytest.raises(psycopg.Error, match="cannot be truncated"):
            conn.execute(f"TRUNCATE {COVERAGE}")
        # A closure can never be recorded for a publication that is not validated
        # (before validation the row guards still admit inserts, so a count taken
        # then would not be a closure at all).
        with pytest.raises(psycopg.Error, match="requires a validated publication"):
            conn.execute(
                "INSERT INTO nport_fixed_income_publication_closures"
                "(publication_id,manifest_sha256,relation_counts) VALUES(%s,%s,'{}'::jsonb)",
                (uuid4(), "b" * 64),
            )
        assert manifest["manifest_sha256"]

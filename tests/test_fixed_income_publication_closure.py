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
from dataclasses import asdict

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


def _write_payloads(
    tmp_path: Path, identity, holdings_publication, run_id, *, filled: tuple[str, ...] | None = None
) -> dict[str, Path]:
    """Nine payloads: the eight contract relations plus the coverage rollup.

    Two carry a row (one per-position coverage fact and its rollup) so the
    counts are not all zero. The rollup is part of every publication since the
    builder stopped materializing absence: it is the only place the counts of
    what was absent exist, so an artifact without it publishes a coverage
    surface that silently claims full coverage.

    ``filled`` narrows which of those two carry their row, so a test can build an
    artifact that EMPTIES a relation the published one has.
    """
    if filled is None:
        filled = (COVERAGE, materializer.COVERAGE_ROLLUP_RELATION)
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
        if relation == COVERAGE and relation in filled:
            lines.append("\t".join(coverage_row[name] for name in columns[relation]))
        if relation == materializer.COVERAGE_ROLLUP_RELATION and relation in filled:
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


def _next_artifact(dsn, tmp_path: Path, config, *, filled: tuple[str, ...]):
    """A SECOND artifact for the same pinned source, on the published schema.

    Identity and lineage are read back from the database instead of threaded
    through the fixture, so this stays a real artifact of the same product --
    the exact shape a restore or a republication produces.
    """
    import psycopg

    with psycopg.connect(dsn) as conn:
        source_publication, run_id, package_id = conn.execute(
            "SELECT p.publication_id::text,p.source_run_id::text,p.source_package_id::text "
            "FROM sec_derived_current_pointers c "
            "JOIN sec_derived_publications p ON p.publication_id=c.publication_id "
            "WHERE c.product='sec_nport_holdings_v2'"
        ).fetchone()
    identity = materializer.BuildIdentity(
        source_publication_id=source_publication,
        source_run_id=run_id,
        source_package_id=package_id,
        target_publication_id=str(uuid4()),
        as_of_date=AS_OF,
        contract_digest=materializer.CONTRACT_DIGEST,
    )
    artifact_dir = tmp_path / f"artifact_{identity.target_publication_id[:8]}"
    artifact_dir.mkdir()
    payloads = _write_payloads(
        artifact_dir, identity, source_publication, run_id, filled=filled
    )
    manifest = materializer.build_manifest(
        identity=identity,
        worker_sha="b" * 40,
        source_files={name: payloads[COVERAGE] for name in materializer.SOURCE_RELATIONS},
        output_files=payloads,
        resource_config=config,
        output_counts={name: 1 for name in filled},
    )
    (artifact_dir / "manifest.json").write_text(
        materializer.canonical_json(manifest), encoding="utf-8"
    )
    return identity, artifact_dir


def test_the_artifact_route_refuses_an_artifact_that_empties_a_served_relation(
    published, tmp_path
) -> None:
    """The completeness gate is executed by THIS route, not just by the worker.

    The product has two producers. A gate on only one of them is a gate on
    neither: the artifact route is exactly the path an operator reaches for when
    the in-database build is refused, and a truncated bundle restored through it
    would empty a served relation just as silently.
    """
    import psycopg

    dsn, published_identity, config, _artifact_dir, _manifest = published
    # Same publication shape MINUS the per-position coverage relation.
    identity, artifact_dir = _next_artifact(
        dsn, tmp_path, config, filled=(materializer.COVERAGE_ROLLUP_RELATION,)
    )

    with psycopg.connect(dsn) as conn:
        with pytest.raises(psycopg.Error, match="regressed to zero rows"):
            materializer.publish_artifact(
                connection=conn, artifact_dir=artifact_dir, identity=identity,
                resource_config=config,
            )
        conn.rollback()
        # The refused publish left nothing behind and the pointer never moved.
        assert conn.execute(
            "SELECT count(*) FROM sec_derived_publications WHERE publication_id=%s",
            (identity.target_publication_id,),
        ).fetchone() == (0,)
        assert conn.execute(
            "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product=%s",
            ("nport_fixed_income_features_v1",),
        ).fetchone() == (published_identity.target_publication_id,)
        conn.commit()


def test_the_artifact_route_honours_the_explicit_regression_override(
    published, tmp_path
) -> None:
    import psycopg

    dsn, _published_identity, config, _artifact_dir, _manifest = published
    identity, artifact_dir = _next_artifact(
        dsn, tmp_path, config, filled=(materializer.COVERAGE_ROLLUP_RELATION,)
    )

    with psycopg.connect(dsn) as conn:
        materializer.publish_artifact(
            connection=conn, artifact_dir=artifact_dir, identity=identity,
            resource_config=config, allow_relation_regression=True,
        )
        conn.commit()
        assert conn.execute(
            "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
            (identity.target_publication_id,),
        ).fetchone() == ("validated",)
        assert conn.execute(
            "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product=%s",
            ("nport_fixed_income_features_v1",),
        ).fetchone() == (identity.target_publication_id,)
        conn.commit()


def test_the_artifact_route_publishes_a_complete_successor(published, tmp_path) -> None:
    """The gate must not block a legitimate republication of the same shape."""
    import psycopg

    dsn, _published_identity, config, _artifact_dir, _manifest = published
    identity, artifact_dir = _next_artifact(
        dsn, tmp_path, config, filled=(COVERAGE, materializer.COVERAGE_ROLLUP_RELATION)
    )

    with psycopg.connect(dsn) as conn:
        materializer.publish_artifact(
            connection=conn, artifact_dir=artifact_dir, identity=identity,
            resource_config=config,
        )
        conn.commit()
        assert conn.execute(
            "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product=%s",
            ("nport_fixed_income_features_v1",),
        ).fetchone() == (identity.target_publication_id,)
        conn.commit()


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


def _legacy_v2_manifest(manifest: dict, tmp_path) -> dict:
    """Rewrite a current manifest into the frozen v2 shape it superseded.

    A real pre-migration bundle attests eight payloads and the previous builder
    sha; nothing else about it changes.
    """
    import hashlib

    legacy = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    legacy["format"] = materializer.LEGACY_MANIFEST_FORMAT
    legacy["engine"] = {
        **legacy["engine"],
        "oracle_sha256": materializer.LEGACY_ORACLE_SHA256,
    }
    # A frozen bundle carries the contract digest it was BUILT under, not the
    # current one -- validating it against today's digest is what would make
    # every pre-migration artifact unrecoverable.
    legacy["contract_digest"] = materializer.LEGACY_CONTRACT_DIGEST
    legacy["identity"] = {
        **legacy["identity"],
        "contract_digest": materializer.LEGACY_CONTRACT_DIGEST,
    }
    legacy["outputs"] = {
        name: value
        for name, value in legacy["outputs"].items()
        if name != materializer.COVERAGE_ROLLUP_RELATION
    }
    # ...and it DOES carry the two per-position repo/lending payloads the current
    # builder no longer produces: restoring a frozen bundle means restoring what
    # it froze.
    for name, (columns, _keys) in materializer._legacy_relation_shapes().items():
        path = tmp_path / f"{name}.tsv.gz"
        with gzip.GzipFile(filename="", mode="wb", fileobj=path.open("wb"), mtime=0) as out:
            out.write(("\t".join(columns) + "\n").encode())
        legacy["outputs"][name] = {
            "count": 0,
            "sha256": materializer.sha256_file(path),
            "filename": path.name,
            "columns": list(columns),
        }
    legacy["manifest_sha256"] = hashlib.sha256(
        materializer.canonical_json(legacy).encode()
    ).hexdigest()
    return legacy


def test_a_frozen_v2_artifact_still_restores_and_gets_its_rollup(tmp_path) -> None:
    """Recovery from a pre-migration bundle must not be collateral damage.

    A v2 artifact carries eight payloads and no rollup -- but its coverage
    payload holds one row per position INCLUDING the absent ones, which is
    exactly what the current builder stopped writing. So the rollup is derived
    from it at publish time, with the absence counted, and the publication ends
    up serving the same figures a v3 restore would.
    """
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
    legacy = _legacy_v2_manifest(manifest, tmp_path)
    identity = materializer.BuildIdentity(
        **{**asdict(identity), "contract_digest": materializer.LEGACY_CONTRACT_DIGEST}
    )
    (tmp_path / "manifest.json").write_text(
        materializer.canonical_json(legacy), encoding="utf-8"
    )
    (tmp_path / f"{materializer.COVERAGE_ROLLUP_RELATION}.tsv.gz").unlink()

    try:
        with psycopg.connect(dsn) as conn:
            materializer.publish_artifact(
                connection=conn, artifact_dir=tmp_path, identity=identity,
                resource_config=config,
            )
            conn.commit()
        with psycopg.connect(dsn) as conn:
            target = identity.target_publication_id
            assert conn.execute(
                "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
                (target,),
            ).fetchone() == ("validated",)
            # Derived, not carried: the rollup exists and reports the same figure.
            assert conn.execute(
                "SELECT metric_key, reported_row_count, source_row_count "
                f"FROM {materializer.COVERAGE_ROLLUP_RELATION} WHERE publication_id=%s",
                (target,),
            ).fetchall() == [("effective_duration", 1, 1)]
            # The closure attests exactly what the v2 manifest claimed: eight.
            closure = conn.execute(
                "SELECT relation_counts FROM nport_fixed_income_publication_closures "
                "WHERE publication_id=%s", (target,)
            ).fetchone()[0]
            assert set(closure) == set(materializer.LEGACY_RELATIONS)
        # And the replay of a v2 bundle stays idempotent.
        with psycopg.connect(dsn) as conn:
            materializer.publish_artifact(
                connection=conn, artifact_dir=tmp_path, identity=identity,
                resource_config=config,
            )
            conn.commit()
    finally:
        with psycopg.connect(DSN, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_an_unknown_manifest_format_is_refused(published) -> None:
    """Only the two known shapes are restorable; anything else fails closed."""
    _dsn, _identity, _config, _artifact_dir, manifest = published
    with pytest.raises(materializer.ArtifactIntegrityError, match="unexpected manifest format"):
        materializer.manifest_relations({**manifest, "format": "nport-fixed-income/v9"})


def test_a_publication_promoted_before_the_rollup_existed_is_repaired(published) -> None:
    """The idempotency shortcut must not certify a publication serving nothing.

    A publication promoted by the previous builder -- restored from a v2 bundle,
    or short-circuited by the worker because its identity did not change -- has
    no rollup rows, and the dossier reads the rollup. Every check on the replay
    path still passes: the manifest attests eight relations, the closure attests
    eight counts, and both remain true. Detect and repair before accepting it.
    """
    import psycopg

    dsn, identity, config, artifact_dir, _manifest = published
    target = identity.target_publication_id
    rollup = materializer.COVERAGE_ROLLUP_RELATION

    with psycopg.connect(dsn, autocommit=True) as conn:
        # Simulate the pre-migration state: the publication is validated and
        # current, its coverage rows are there, the rollup is not.
        conn.execute(f"ALTER TABLE {rollup} DISABLE TRIGGER USER")
        conn.execute(f"DELETE FROM {rollup} WHERE publication_id=%s", (target,))
        conn.execute(f"ALTER TABLE {rollup} ENABLE TRIGGER USER")
        assert conn.execute(
            f"SELECT count(*) FROM {rollup} WHERE publication_id=%s", (target,)
        ).fetchone() == (0,)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            assert materializer.ensure_coverage_rollup(cur, target) == "backfilled"
        conn.commit()

    with psycopg.connect(dsn) as conn:
        # Derived from the coverage rows, which for such a publication carry one
        # row per position including the absent ones.
        assert conn.execute(
            f"SELECT metric_key, reported_row_count, source_row_count FROM {rollup} "
            "WHERE publication_id=%s", (target,)
        ).fetchall() == [("effective_duration", 1, 1)]
        # Self-limiting: a second call is a no-op, never a rewrite.
        with conn.cursor() as cur:
            assert materializer.ensure_coverage_rollup(cur, target) == "present"


def test_the_backfill_of_a_validated_publication_writes_every_row_it_derives(
    published,
) -> None:
    """The repair path has to survive a rollup with more than one row.

    The backfill is a single INSERT..SELECT, one row per coverage group. The
    "already published" check used to be asked once per ROW, and a row trigger's
    query sees what the same command already inserted -- so row 2 found row 1 and
    raised. The repair the runbook documents could only ever succeed for a
    publication whose coverage collapsed to a single group; the real one
    (f110a2bb, thousands of groups) died on its second row. Only the statement
    knows whether the rollup was empty when it started, so only the statement can
    answer.
    """
    import psycopg

    dsn, identity, _config, _artifact_dir, _manifest = published
    target = identity.target_publication_id
    rollup = materializer.COVERAGE_ROLLUP_RELATION

    with psycopg.connect(dsn, autocommit=True) as conn:
        # Widen the publication's coverage to three groups, then clear the
        # rollup: the pre-migration state, at a grain that is not degenerate.
        conn.execute(f"ALTER TABLE {COVERAGE} DISABLE TRIGGER USER")
        conn.execute(
            f"""INSERT INTO {COVERAGE}
                (publication_id,source_holdings_publication_id,source_run_id,series_id,
                 report_date,accession_number,source_identity_key,metric_family,metric_key,
                 numerator,denominator,denominator_unit,coverage_ratio,availability_state)
                SELECT c.publication_id,c.source_holdings_publication_id,c.source_run_id,
                       c.series_id,c.report_date,c.accession_number,'K'||g,c.metric_family,
                       'derived_metric_'||g,1,2,'count',0.5,'reported_numeric'
                FROM {COVERAGE} c, generate_series(2,3) g
                WHERE c.publication_id=%s""",
            (target,),
        )
        conn.execute(f"ALTER TABLE {COVERAGE} ENABLE TRIGGER USER")
        conn.execute(f"ALTER TABLE {rollup} DISABLE TRIGGER USER")
        conn.execute(f"DELETE FROM {rollup} WHERE publication_id=%s", (target,))
        conn.execute(f"ALTER TABLE {rollup} ENABLE TRIGGER USER")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            assert materializer.ensure_coverage_rollup(cur, target) == "backfilled"
        conn.commit()

    with psycopg.connect(dsn) as conn:
        # Every derived group landed -- the whole rollup, not just its first row.
        assert conn.execute(
            f"SELECT count(*) FROM {rollup} WHERE publication_id=%s", (target,)
        ).fetchone() == (3,)
        assert conn.execute(
            f"SELECT count(DISTINCT metric_key) FROM {rollup} WHERE publication_id=%s",
            (target,),
        ).fetchone() == (3,)
        # Still closed afterwards: the window was this one statement.
        with conn.cursor() as cur:
            assert materializer.ensure_coverage_rollup(cur, target) == "present"


def test_the_backfill_cannot_rewrite_an_existing_rollup(published) -> None:
    """The relaxation is additive only: a validated publication WITH a rollup is closed."""
    import psycopg

    dsn, identity, _config, _artifact_dir, _manifest = published
    target = identity.target_publication_id
    with psycopg.connect(dsn) as conn:
        with pytest.raises(psycopg.errors.RaiseException, match="already published"):
            conn.execute(
                f"INSERT INTO {materializer.COVERAGE_ROLLUP_RELATION} "
                "(publication_id,source_holdings_publication_id,source_run_id,series_id,"
                " report_date,accession_number,metric_family,metric_key,"
                " source_row_count,reported_row_count) "
                "SELECT publication_id,source_holdings_publication_id,source_run_id,series_id,"
                " report_date,accession_number,metric_family,'forged',1,1 "
                f"FROM {materializer.COVERAGE_ROLLUP_RELATION} WHERE publication_id=%s",
                (target,),
            )
        conn.rollback()


def test_a_v2_bundle_presenting_the_wrong_contract_digest_is_refused(tmp_path) -> None:
    """Per-format, not per-either: a v2 artifact may only carry the v2 digest."""
    identity = materializer.BuildIdentity(
        source_publication_id=str(uuid4()),
        source_run_id=str(uuid4()),
        source_package_id=str(uuid4()),
        target_publication_id=str(uuid4()),
        as_of_date=AS_OF,
        contract_digest=materializer.CONTRACT_DIGEST,
    )
    payloads = _write_payloads(tmp_path, identity, uuid4(), uuid4())
    manifest = materializer.build_manifest(
        identity=identity,
        worker_sha="a" * 40,
        source_files={name: payloads[COVERAGE] for name in materializer.SOURCE_RELATIONS},
        output_files=payloads,
        resource_config=_resource_config(tmp_path),
        output_counts={COVERAGE: 1, materializer.COVERAGE_ROLLUP_RELATION: 1},
    )
    legacy = _legacy_v2_manifest(manifest, tmp_path)
    # Claim the v2 format while carrying the CURRENT contract digest.
    import hashlib

    forged = {key: value for key, value in legacy.items() if key != "manifest_sha256"}
    forged["contract_digest"] = materializer.CONTRACT_DIGEST
    forged["manifest_sha256"] = hashlib.sha256(
        materializer.canonical_json(forged).encode()
    ).hexdigest()
    files = {
        name: payloads[name]
        for name in materializer.LEGACY_RELATIONS
        if name in payloads
    }
    with pytest.raises(materializer.ArtifactIntegrityError, match="contract digest"):
        materializer.verify_manifest(forged, files)

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from src.nport import fixed_income_local_materializer as materializer


def test_client_safe_timeouts_do_not_set_local_postgres_resource_limits(
    tmp_path: Path,
) -> None:
    class RecordingCursor:
        def __init__(self) -> None:
            self.settings: list[tuple[str, str]] = []

        def execute(
            self, _query: str, parameters: tuple[str, str]
        ) -> None:
            self.settings.append(parameters)

    cursor = RecordingCursor()
    config = materializer.ResourceConfig(
        memory_limit="1GB",
        temp_directory=str(tmp_path),
        minimum_free_bytes=1,
    )

    materializer._set_client_safe_timeouts(cursor, config)

    assert dict(cursor.settings) == {
        "lock_timeout": f"{config.lock_timeout_ms}ms",
        "statement_timeout": f"{config.statement_timeout_ms}ms",
        "idle_in_transaction_session_timeout": (
            f"{config.idle_transaction_timeout_ms}ms"
        ),
    }


def _e2e_config(
    tmp_path: Path, admin_dsn: str
) -> materializer.ResourceConfig:
    image_digest = os.environ.get("NPORT_FI_E2E_IMAGE_DIGEST")
    if not image_digest:
        pytest.skip("set NPORT_FI_E2E_IMAGE_DIGEST to run PostgreSQL 18 E2E")
    import psycopg

    with psycopg.connect(admin_dsn) as connection:
        server_fingerprint = materializer.postgres_server_fingerprint(
            connection.cursor()
        )
    return materializer.ResourceConfig(
        memory_limit="1GB",
        temp_directory=str(tmp_path / "postgres-tmp"),
        postgres_image_digest=image_digest,
        postgres_server_fingerprint=server_fingerprint,
        max_temp_directory_size="2GB",
        minimum_free_bytes=1,
        statement_timeout_ms=60_000,
        local_build_statement_timeout_ms=60_000,
        client_watchdog_seconds=60,
    )


def _create_database(admin_dsn: str, name: str) -> str:
    import psycopg

    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{name}"')
    return admin_dsn.rsplit("/", 1)[0] + f"/{name}"


def _run_cli(repo: Path, *args: str) -> None:
    subprocess.run(
        [sys.executable, "scripts/materialize_nport_fixed_income_local.py", *args],
        cwd=repo,
        check=True,
        text=True,
    )


def test_postgres18_cli_pipeline_exports_eight_files_and_rolls_back_publish(
    tmp_path: Path,
) -> None:
    """Real PostgreSQL 18 boundary: CLI bootstrap/extract/compute plus atomic publish."""
    admin_dsn = os.environ.get("NPORT_FI_E2E_DSN")
    if not admin_dsn:
        pytest.skip("set NPORT_FI_E2E_DSN to run PostgreSQL 18 E2E")
    import psycopg

    config = _e2e_config(tmp_path, admin_dsn)
    repo = Path(__file__).resolve().parents[1]
    suffix = uuid4().hex
    source_dsn = _create_database(admin_dsn, f"nport_local_source_{suffix}")
    compute_dsn = _create_database(admin_dsn, f"nport_local_compute_{suffix}")
    source_run, source_package, source_publication = (str(uuid4()) for _ in range(3))
    source_bootstrap_target, target_publication, local_run = (
        str(uuid4()),
        str(uuid4()),
        str(uuid4()),
    )
    identity = materializer.BuildIdentity(
        source_publication, source_run, source_package, target_publication,
        "2026-07-24", materializer.CONTRACT_DIGEST,
    )
    common = (
        "--source-publication-id", source_publication,
        "--source-run-id", source_run,
        "--source-package-id", source_package,
        "--as-of", identity.as_of_date,
        "--contract-digest", identity.contract_digest,
        "--memory-limit", config.memory_limit,
        "--temp-directory", config.temp_directory,
        "--postgres-image-digest", config.postgres_image_digest,
        "--postgres-server-fingerprint", config.postgres_server_fingerprint,
        "--max-temp-directory-size", config.max_temp_directory_size,
        "--statement-timeout-ms", str(config.statement_timeout_ms),
        "--local-build-statement-timeout-ms", str(config.local_build_statement_timeout_ms),
        "--client-watchdog-seconds", str(config.client_watchdog_seconds),
    )
    try:
        _run_cli(repo, *common, "--target-publication-id", source_bootstrap_target,
                 "bootstrap", "--local-dsn", source_dsn, "--local-run-uuid", local_run)
        with psycopg.connect(source_dsn) as connection:
            connection.execute(
                """INSERT INTO sec_nport_instrument_class_bridge
                (publication_id,accession_number,holding_id,instrument_id,series_id,class_id,valid_from,resolution_state)
                VALUES(%s,'A1','H1','I-H1','S1','C1','2020-01-01','resolved')""",
                (source_publication,),
            )
            connection.execute(
                """INSERT INTO sec_nport_holdings_v2
                (publication_id,accession_number,holding_id,source_run_id,report_date,filing_date,source_series_id,
                 signed_market_value,signed_pct_of_nav,cusip,source_typed_projection)
                VALUES(%s,'A1','H1',%s,'2026-06-30','2026-06-30','S1',100,10,'111111111',%s::jsonb)""",
                (source_publication, source_run, '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"5.0","MATURITY_DATE":"2029-06-30"}}'),
            )
            raw_source_file = str(uuid4())
            connection.execute(
                """INSERT INTO nport_raw_rows
                (raw_row_id,ingestion_run_id,source_file_id,source_row_number,
                 source_table,typed_projection,accession_number)
                VALUES(777,%s,%s,2,'INTEREST_RATE_RISK.tsv',%s::jsonb,'A1')""",
                (
                    source_run,
                    raw_source_file,
                    '{"INTEREST_RATE_RISK_ID":"RISK-E2E",'
                    '"INTRST_RATE_CHANGE_3MON_DV01":"-9"}',
                ),
            )
            connection.execute(
                "SELECT sec_validate_derived_publication(%s)", (source_publication,)
            )
            connection.execute(
                "SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)",
                (source_publication,),
            )
        extraction = tmp_path / "extract"
        _run_cli(repo, *common, "--target-publication-id", target_publication,
                 "extract", "--dsn", source_dsn, "--output-dir", str(extraction))
        _run_cli(repo, *common, "--target-publication-id", target_publication,
                 "bootstrap", "--local-dsn", compute_dsn, "--local-run-uuid", local_run)
        artifact = tmp_path / "artifact"
        _run_cli(repo, *common, "--target-publication-id", target_publication,
                 "compute", "--local-dsn", compute_dsn, "--local-run-uuid", local_run,
                 "--extraction-dir", str(extraction), "--output-dir", str(artifact),
                 "--worker-sha", "a" * 40)
        assert {path.name for path in artifact.glob("*.tsv.gz")} == {
            f"{name}.tsv.gz" for name in materializer.PUBLISHED_RELATIONS
        }
        key_rate_path = (
            artifact / "nport_fixed_income_key_rate_sensitivities_v2.tsv.gz"
        )
        with gzip.open(key_rate_path, "rt", encoding="utf-8") as payload:
            key_rate_payload = payload.read()
        assert "\t777\t" in key_rate_payload
        artifact_manifest = json.loads(
            (artifact / "manifest.json").read_text(encoding="utf-8")
        )
        assert (
            artifact_manifest["outputs"][
                "nport_fixed_income_key_rate_sensitivities_v2"
            ]["count"]
            == 1
        )
        with psycopg.connect(source_dsn) as connection:
            with pytest.raises(RuntimeError, match="injected publish failure"):
                materializer.publish_artifact(
                    connection=connection, artifact_dir=artifact, identity=identity,
                    resource_config=config,
                    failure_after_relation=materializer.TARGET_RELATIONS[0],
                )
            assert connection.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE publication_id=%s",
                (target_publication,),
            ).fetchone() == (0,)
            materializer.publish_artifact(
                connection=connection, artifact_dir=artifact, identity=identity,
                resource_config=config,
            )
            assert connection.execute(
                "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
                (target_publication,),
            ).fetchone() == ("validated",)
            assert connection.execute(
                "SELECT count(*) FROM nport_fixed_income_publication_manifests WHERE publication_id=%s",
                (target_publication,),
            ).fetchone() == (1,)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE IF EXISTS "nport_local_source_{suffix}" WITH (FORCE)')
            connection.execute(f'DROP DATABASE IF EXISTS "nport_local_compute_{suffix}" WITH (FORCE)')


def test_the_completeness_gate_covers_exactly_the_published_relations() -> None:
    """A seventh relation must not be able to join the product without a gate.

    ``nport_fixed_income_assert_publication_complete`` names its relation set in
    SQL, so nothing in Python fails when a relation is added to
    ``PUBLISHED_RELATIONS`` and not to the array: the new relation would simply
    never be checked, and could regress to zero exactly the way the raw-derived
    four did on 2026-08-01. This pins the two lists to each other.

    The retired repo/securities-lending relations are deliberately outside BOTH
    lists: they are empty by owner decision since 2026-07-31.
    """
    schema = (
        Path(__file__).resolve().parents[1] / "schemas" / "nport_fixed_income_features.sql"
    ).read_text(encoding="utf-8")
    body = schema[schema.index("CREATE OR REPLACE FUNCTION nport_fixed_income_assert_publication_complete"):]
    array = body[body.index("FOREACH relation IN ARRAY ARRAY["):]
    array = array[: array.index("]")]
    gated = set(re.findall(r"'([a-z0-9_]+)'", array))

    assert gated == set(materializer.PUBLISHED_RELATIONS)
    assert set(materializer.TARGET_RELATIONS) < gated
    assert materializer.COVERAGE_ROLLUP_RELATION in gated
    for retired in (
        "nport_fixed_income_repo_lending_primitives_v2",
        "nport_fixed_income_repo_lending_reported_flags_v2",
    ):
        assert retired not in gated


def test_the_publish_cli_applies_the_product_ddl_before_publishing() -> None:
    """The DDL is a prerequisite of publishing, not an ambient assumption.

    ``publish_artifact`` calls a function defined in
    schemas/nport_fixed_income_features.sql, so a restore into a database that
    never had it applied fails with 42883 instead of publishing.
    """
    cli = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "materialize_nport_fixed_income_local.py"
    ).read_text(encoding="utf-8")
    assert cli.index("install_product_schema(cursor)") < cli.index("publish_artifact(")
    assert materializer.PRODUCT_SCHEMA_PATH.is_file()


def test_manifest_is_canonical_and_tamper_evident(tmp_path: Path) -> None:
    payload = tmp_path / "payload.tsv.gz"
    with gzip.GzipFile(
        filename="", mode="wb", fileobj=payload.open("wb"), mtime=0
    ) as out:
        out.write(b"a\tb\n1\t2\n")
    payloads = {}
    for name in materializer.PUBLISHED_RELATIONS:
        path = tmp_path / f"{name}.tsv.gz"
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=path.open("wb"), mtime=0
        ) as out:
            out.write(("\t".join(materializer._published_columns()[name]) + "\n").encode())
        payloads[name] = path
    manifest = materializer.build_manifest(
        identity=materializer.BuildIdentity(
            source_publication_id="62ba191f-5dcf-4e69-b863-3e343db010c2",
            source_run_id="e47ad93a-ac18-467e-b0a5-ee3c39c607c0",
            source_package_id="d5b103ed-72a1-4601-bdcf-0ea0b873787b",
            target_publication_id="4c9f5552-3b57-40cf-882d-e574174fa1c5",
            as_of_date="2026-07-24",
            contract_digest=materializer.CONTRACT_DIGEST,
        ),
        worker_sha="a" * 40,
        source_files={name: payload for name in materializer.SOURCE_RELATIONS},
        output_files=payloads,
        resource_config=materializer.ResourceConfig(
            memory_limit="1GB", temp_directory=str(tmp_path),
            postgres_image_digest="sha256:" + "a" * 64,
            postgres_server_fingerprint="test-postgres-18",
        ),
    )
    assert materializer.canonical_json(manifest) == materializer.canonical_json(
        dict(manifest)
    )
    assert (
        manifest["outputs"]["nport_fixed_income_features"]["sha256"]
        == hashlib.sha256(payloads["nport_fixed_income_features"].read_bytes()).hexdigest()
    )
    materializer.verify_manifest(
        manifest, payloads
    )
    payloads["nport_fixed_income_features"].write_bytes(b"tampered")
    with pytest.raises(materializer.ArtifactIntegrityError, match="sha256"):
        materializer.verify_manifest(
            manifest, payloads
        )


def test_schema_never_defines_the_builder_and_the_pinned_resource_does() -> None:
    """The product must be buildable by a deployed worker, not only by a human.

    The DDL used to define ``build_nport_fixed_income_features`` as a stub that
    raised 0A000, so re-applying it over a live database would replace the real
    builder with a refusal.  The function now has exactly one definition, in the
    sha256-pinned resource the worker installs.
    """
    schema = (
        Path(__file__).resolve().parents[1]
        / "schemas"
        / "nport_fixed_income_features.sql"
    ).read_text(encoding="utf-8")
    assert "SQLSTATE '0A000'" not in schema
    assert "CREATE OR REPLACE FUNCTION build_nport_fixed_income_features" not in schema
    builder = materializer.BUILDER_SQL_PATH.read_text(encoding="utf-8")
    assert builder.count("CREATE OR REPLACE FUNCTION build_nport_fixed_income_features(") == 1
    assert "DROP FUNCTION IF EXISTS build_nport_fixed_income_features(uuid,date,text)" in builder


def test_compute_is_delegated_to_guarded_local_postgres_not_duckdb_or_python_formulas() -> (
    None
):
    source = Path(materializer.__file__).read_text(encoding="utf-8")
    compute = source[
        source.index("def compute_local") : source.index("def build_manifest")
    ]
    assert "psycopg.connect" in compute
    assert "build_nport_fixed_income_features" in compute
    assert "duckdb" not in compute.lower()
    assert "_feature_rows" not in compute


def test_production_schema_carries_no_builder_body() -> (
    None
):
    schema = (
        Path(__file__).resolve().parents[1]
        / "schemas"
        / "nport_fixed_income_features.sql"
    ).read_text(encoding="utf-8")
    assert schema.count("FUNCTION build_nport_fixed_income_features(") == 0
    assert "snapshot_holdings AS" not in schema
    assert "INSERT INTO nport_fixed_income_metric_coverage_v2" not in schema


def test_manifest_declares_every_published_payload_deterministically(
    tmp_path: Path,
) -> None:
    identity = materializer.BuildIdentity(
        source_publication_id="62ba191f-5dcf-4e69-b863-3e343db010c2",
        source_run_id="e47ad93a-ac18-467e-b0a5-ee3c39c607c0",
        source_package_id="d5b103ed-72a1-4601-bdcf-0ea0b873787b",
        target_publication_id="4c9f5552-3b57-40cf-882d-e574174fa1c5",
        as_of_date="2026-07-24",
        contract_digest=materializer.CONTRACT_DIGEST,
    )
    payloads = {}
    for relation in materializer.PUBLISHED_RELATIONS:
        path = tmp_path / f"{relation}.tsv.gz"
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=path.open("wb"), mtime=0
        ) as stream:
            stream.write(b"header\nrow\n")
        payloads[relation] = path
    manifest = materializer.build_manifest(
        identity=identity,
        worker_sha="a" * 40,
        source_files={name: payloads[materializer.PUBLISHED_RELATIONS[0]] for name in materializer.SOURCE_RELATIONS},
        output_files=payloads,
        resource_config=materializer.ResourceConfig(
            memory_limit="1GB", temp_directory=str(tmp_path),
            postgres_image_digest="sha256:" + "a" * 64,
            postgres_server_fingerprint="test-postgres-18",
        ),
        output_counts={name: 1 for name in materializer.PUBLISHED_RELATIONS},
    )
    assert set(manifest["outputs"]) == set(materializer.PUBLISHED_RELATIONS)
    assert manifest["engine"]["kind"] == "postgresql-local"
    assert len(manifest["manifest_sha256"]) == 64


def test_multi_referenced_oracle_ctes_are_materialized() -> None:
    """Every CTE the oracle reads more than once must be AS MATERIALIZED.

    PostgreSQL inlines a single-reference CTE by default, so without this the
    holdings join over 4.1M rows is re-executed once per UNION ALL branch.

    ``snapshot_holdings`` left this list with the repo/securities-lending
    statements it existed for: those were the nine-branch UNION ALL this guard
    was written about, and they are no longer built.
    """
    sql_text = materializer.BUILDER_SQL_PATH.read_text(encoding="utf-8")
    for name in ("snapshot_filings", "values_rows", "coverage_rows",
                 "positions", "source_rows", "debt"):
        reads = sql_text.count(f"FROM {name}") + sql_text.count(f"JOIN {name}")
        assert reads > 1, f"{name} is no longer multi-referenced; revisit this guard"
        assert f"{name} AS (" not in sql_text, (
            f"{name} is read {reads} times but is defined without MATERIALIZED"
        )


def test_approved_oracle_hash_matches_the_shipped_sql() -> None:
    """The pin must track the file, or the runtime attestation fails at build time."""
    digest = hashlib.sha256(
        materializer.LOCAL_ORACLE_PATH.read_bytes()
    ).hexdigest()
    assert digest == materializer.APPROVED_LOCAL_ORACLE_SHA256


def test_local_load_indexes_and_analyze_precede_the_oracle() -> None:
    """COPY leaves no statistics and no source_table index; both are load-time work.

    The seven raw views each filter nport_raw_rows by source_table, so without the
    index every oracle branch sequentially scans ~6M rows.
    """
    executed: list[str] = []

    class _Cursor:
        def execute(self, statement, *args):
            executed.append(str(statement))

    materializer._prepare_local_statistics(_Cursor())

    # ANALYZE is composed with sql.Identifier, so the rendered statement is a
    # Composed repr rather than a bare string; match on content, not prefix.
    index_statements = [s for s in executed if "CREATE INDEX" in s]
    analyze_statements = [s for s in executed if "ANALYZE" in s]
    assert index_statements, "no index is created for the copied snapshots"
    assert any("source_table" in s for s in index_statements)
    for relation in materializer.SOURCE_RELATIONS:
        assert any(relation in s for s in analyze_statements), (
            f"{relation} is loaded by COPY but never analysed"
        )
    assert executed.index(index_statements[0]) < executed.index(analyze_statements[0]), (
        "the index must exist before ANALYZE runs"
    )


def test_coverage_rollup_travels_with_the_artifact() -> None:
    """A pointer that moves without the rollup leaves the reader with no coverage.

    Coverage is written per holding -- roughly 173k rows per snapshot for 46
    figures -- and serving that grain per request cost 36s and ~493MB of I/O on a
    cold cache, past the datalake statement timeout.  The reader consumes the
    rollup instead, so publishing has to leave it current.
    """
    source = Path(materializer.__file__).read_text(encoding="utf-8")

    # The rollup is a WRITTEN publication fact, not a materialized view over the
    # per-position rows: the builder stopped materializing absence, so a view
    # could no longer count it. For the offline route that means the rollup has
    # to TRAVEL IN THE ARTIFACT -- rebuilding it from the published rows would
    # see only the reported ones and report coverage_ratio 1 for partially
    # covered metrics, with metrics that reported nothing missing entirely.
    assert materializer.COVERAGE_ROLLUP_RELATION in materializer.PUBLISHED_RELATIONS
    assert materializer.COVERAGE_ROLLUP_RELATION not in materializer.TARGET_RELATIONS
    assert "REFRESH MATERIALIZED VIEW" not in source
    # The only place the rollup is still DERIVED is the legacy-artifact branch,
    # whose coverage payload does carry the absent rows.
    publish = source[source.index("def publish_artifact"):]
    assert "_derive_rollup_from_coverage" in publish
    assert "INSERT INTO nport_fixed_income_metric_coverage_snapshot_v1" not in publish
    for column in ("source_row_count", "reported_row_count", "missing_reason_counts"):
        assert column in materializer.COVERAGE_ROLLUP_COLUMNS


def test_coverage_rollup_ddl_ships_with_the_guards_and_index_its_reader_needs() -> None:
    schema = (
        Path(__file__).resolve().parents[1]
        / "schemas"
        / "nport_fixed_income_features.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS nport_fixed_income_metric_coverage_snapshot_v1" in schema
    assert "MATERIALIZED VIEW" not in schema
    # Absence is counted, never materialized per position.
    assert "source_row_count" in schema and "reported_row_count" in schema
    assert "missing_reason_counts" in schema
    # The reader pins by source holdings publication, not by the feature publication.
    assert "nport_fi_metric_coverage_snapshot_v1_serving_idx" in schema
    assert "sec_current_nport_fixed_income_metric_coverage_snapshot_v1" in schema
    # Same lifecycle guards as every other publication fact.
    assert schema.count("nport_fixed_income_v2_fact_write_guard ON nport_fixed_income_metric_coverage_snapshot_v1") == 1


def test_artifact_without_the_rollup_payload_is_rejected(tmp_path: Path) -> None:
    """An artifact that drops the rollup cannot be published as one that has it.

    The builder no longer materializes absence per position, so absence counts
    exist only in the rollup.  An artifact carrying just the eight contract
    relations would publish a rollup rebuilt from reported rows alone:
    coverage_ratio 1 for partially covered metrics, empty missing_reason_counts,
    and no row at all for metrics that reported nothing.  The manifest refuses
    that shape instead of silently degrading it.
    """
    payloads = {}
    for name in materializer.PUBLISHED_RELATIONS:
        path = tmp_path / f"{name}.tsv.gz"
        with gzip.GzipFile(filename="", mode="wb", fileobj=path.open("wb"), mtime=0) as out:
            out.write(("\t".join(materializer._published_columns()[name]) + "\n").encode())
        payloads[name] = path
    without_rollup = {
        name: path
        for name, path in payloads.items()
        if name != materializer.COVERAGE_ROLLUP_RELATION
    }
    with pytest.raises(materializer.ArtifactIntegrityError):
        materializer.build_manifest(
            identity=materializer.BuildIdentity(
                source_publication_id="62ba191f-5dcf-4e69-b863-3e343db010c2",
                source_run_id="e47ad93a-ac18-467e-b0a5-ee3c39c607c0",
                source_package_id="d5b103ed-72a1-4601-bdcf-0ea0b873787b",
                target_publication_id="4c9f5552-3b57-40cf-882d-e574174fa1c5",
                as_of_date="2026-07-24",
                contract_digest=materializer.CONTRACT_DIGEST,
            ),
            worker_sha="a" * 40,
            source_files={name: payloads[materializer.PUBLISHED_RELATIONS[0]] for name in materializer.SOURCE_RELATIONS},
            output_files=without_rollup,
            resource_config=materializer.ResourceConfig(
                memory_limit="1GB", temp_directory=str(tmp_path),
                postgres_image_digest="sha256:" + "a" * 64,
                postgres_server_fingerprint="test-postgres-18",
            ),
            output_counts={name: 1 for name in without_rollup},
        )


def test_a_format_label_describes_artifacts_that_exist_and_is_never_reused() -> None:
    """``/v3`` was emitted into production before this branch changed the shape.

    Publication a42e5032 was promoted on 2026-07-31 by the code merged as #77:
    nine payloads (the v2 contract's eight plus the rollup), the v2 contract
    digest, and the wave-3b builder. Re-pointing ``/v3`` at the new shape would
    have made that artifact unrestorable, so the new shape took a new label.
    """
    relations, oracle, contract = materializer._MANIFEST_FORMATS[
        materializer.PRIOR_MANIFEST_FORMAT
    ]
    assert len(relations) == 9
    assert materializer.COVERAGE_ROLLUP_RELATION in relations
    assert "nport_fixed_income_repo_lending_reported_flags_v2" in relations
    assert oracle == materializer.PRIOR_ORACLE_SHA256
    assert contract == materializer.LEGACY_CONTRACT_DIGEST

    # The label this branch produces is a different one.
    assert materializer.MANIFEST_FORMAT.endswith("/v4")
    assert materializer.MANIFEST_FORMAT not in {
        materializer.PRIOR_MANIFEST_FORMAT,
        materializer.LEGACY_MANIFEST_FORMAT,
    }
    # /v3 already ships a rollup payload; only /v2 needs one derived.
    assert materializer._DERIVE_ROLLUP_FORMATS == {materializer.LEGACY_MANIFEST_FORMAT}

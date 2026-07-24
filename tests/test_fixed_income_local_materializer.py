from __future__ import annotations

import gzip
import hashlib
import json
import os
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
            f"{name}.tsv.gz" for name in materializer.TARGET_RELATIONS
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


def test_manifest_is_canonical_and_tamper_evident(tmp_path: Path) -> None:
    payload = tmp_path / "payload.tsv.gz"
    with gzip.GzipFile(
        filename="", mode="wb", fileobj=payload.open("wb"), mtime=0
    ) as out:
        out.write(b"a\tb\n1\t2\n")
    payloads = {}
    for name in materializer.TARGET_RELATIONS:
        path = tmp_path / f"{name}.tsv.gz"
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=path.open("wb"), mtime=0
        ) as out:
            out.write(("\t".join(materializer._contract_columns()[name]) + "\n").encode())
        payloads[name] = path
    manifest = materializer.build_manifest(
        identity=materializer.BuildIdentity(
            source_publication_id="62ba191f-5dcf-4e69-b863-3e343db010c2",
            source_run_id="e47ad93a-ac18-467e-b0a5-ee3c39c607c0",
            source_package_id="d5b103ed-72a1-4601-bdcf-0ea0b873787b",
            target_publication_id="4c9f5552-3b57-40cf-882d-e574174fa1c5",
            as_of_date="2026-07-24",
            contract_digest="sha256:797332a98c62c3843ea1f870a61dca3c67fe5a4bd012aa7d978913ca120be563",
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


def test_installed_builder_is_explicitly_unsupported() -> None:
    schema = (
        Path(__file__).resolve().parents[1]
        / "schemas"
        / "nport_fixed_income_features.sql"
    ).read_text(encoding="utf-8")
    assert "SQLSTATE '0A000'" in schema
    assert "local fixed-income materializer" in schema


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


def test_production_schema_contains_one_fail_fast_builder_and_no_legacy_compute_body() -> (
    None
):
    schema = (
        Path(__file__).resolve().parents[1]
        / "schemas"
        / "nport_fixed_income_features.sql"
    ).read_text(encoding="utf-8")
    assert schema.count("FUNCTION build_nport_fixed_income_features(") == 1
    assert "snapshot_holdings AS" not in schema
    assert "INSERT INTO nport_fixed_income_metric_coverage_v2" not in schema


def test_manifest_declares_all_eight_target_payloads_deterministically(
    tmp_path: Path,
) -> None:
    identity = materializer.BuildIdentity(
        source_publication_id="62ba191f-5dcf-4e69-b863-3e343db010c2",
        source_run_id="e47ad93a-ac18-467e-b0a5-ee3c39c607c0",
        source_package_id="d5b103ed-72a1-4601-bdcf-0ea0b873787b",
        target_publication_id="4c9f5552-3b57-40cf-882d-e574174fa1c5",
        as_of_date="2026-07-24",
        contract_digest="sha256:797332a98c62c3843ea1f870a61dca3c67fe5a4bd012aa7d978913ca120be563",
    )
    payloads = {}
    for relation in materializer.TARGET_RELATIONS:
        path = tmp_path / f"{relation}.tsv.gz"
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=path.open("wb"), mtime=0
        ) as stream:
            stream.write(b"header\nrow\n")
        payloads[relation] = path
    manifest = materializer.build_manifest(
        identity=identity,
        worker_sha="a" * 40,
        source_files={name: payloads[materializer.TARGET_RELATIONS[0]] for name in materializer.SOURCE_RELATIONS},
        output_files=payloads,
        resource_config=materializer.ResourceConfig(
            memory_limit="1GB", temp_directory=str(tmp_path),
            postgres_image_digest="sha256:" + "a" * 64,
            postgres_server_fingerprint="test-postgres-18",
        ),
        output_counts={name: 1 for name in materializer.TARGET_RELATIONS},
    )
    assert set(manifest["outputs"]) == set(materializer.TARGET_RELATIONS)
    assert manifest["engine"]["kind"] == "postgresql-local"
    assert len(manifest["manifest_sha256"]) == 64

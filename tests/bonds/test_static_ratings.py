from __future__ import annotations

import hashlib
import subprocess
import sys
from uuid import uuid4
from datetime import date
from pathlib import Path

import pytest


def _artifact(tmp_path: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "bond_ratings_pit.parquet"
    pq.write_table(
        pa.table(
            {
                "cusip_id": ["037833100", "037833100", "594918104", "BAD", "594918104"],
                "month": [date(2025, 1, 1), date(2025, 2, 1), date(2025, 1, 1), date(2025, 1, 1), date(2024, 12, 1)],
                "rating_bucket": ["A", "NR", "AA", "BBB", "X"],
            }
        ),
        path,
    )
    return path


def test_pinned_static_backfill_keeps_final_nr_and_typed_rejects(tmp_path: Path) -> None:
    from src.bonds.static_ratings import build_static_mapping

    artifact = _artifact(tmp_path)
    result = build_static_mapping(artifact, expected_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())

    assert result.source_rows == 5
    assert result.rejected_rows == 2
    assert result.mapping["037833100"].rating_bucket == "NR"
    assert result.mapping["037833100"].rating_state == "not_rated"
    assert result.mapping["037833100"].rating_as_of_month == date(2025, 2, 1)
    assert result.mapping["594918104"].rating_bucket == "AA"
    assert {reject.reason_code for reject in result.rejects} == {"invalid_cusip", "invalid_rating_bucket"}


def test_resolver_is_generic_and_marks_missing_static_rating() -> None:
    from src.bonds.static_ratings import StaticRating, attach_static_ratings

    mapping = {
        "037833100": StaticRating("037833100", "A", date(2025, 1, 1), "rated", "hash", 1),
    }
    resolved = attach_static_ratings(
        [{"cusip9": "037833100", "month": date(2025, 2, 1)}, {"cusip9": "NONE", "month": date(2025, 2, 1)}],
        mapping,
    )

    assert resolved[0]["rating_bucket"] == "A"
    assert resolved[0]["rating_state"] == "static_carry_forward"
    assert resolved[0]["reason_code"] == "static_rating_carry_forward"
    assert resolved[1] == {
        "cusip9": "NONE", "month": date(2025, 2, 1), "rating_bucket": "NR",
        "rating_as_of_month": None, "rating_state": "missing", "reason_code": "static_rating_absent",
    }


def test_equivalence_and_copy_cursor_are_deterministic_and_stdout_pure(tmp_path: Path) -> None:
    from src.bonds.static_ratings import build_static_mapping, equivalence_report
    from src.workers.bond_rating_static_backfill import render_copy_slice

    artifact = _artifact(tmp_path)
    result = build_static_mapping(artifact, expected_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())
    report = equivalence_report(result.mapping, result.mapping)
    copy = render_copy_slice(tuple(result.mapping.values()), cursor=0, limit=1)

    assert report == {"rows": 2, "matches": 2, "differences": 0}
    assert copy.startswith("\\set ON_ERROR_STOP on\nBEGIN;\nSET LOCAL ROLE worker_writer;")
    assert "COPY _backfill_stage" in copy
    assert "'committed_through', 1" in copy
    assert "'remaining', 1" in copy
    assert "'inserted'" in copy and "'existing'" in copy and "'conflicted'" in copy
    assert "mixed static-rating source_sha256" in copy
    assert "non-contiguous static-rating cursor" in copy
    assert "037833100" in copy
    assert "594918104" not in copy


def test_static_ddl_is_worker_owned_and_immutable() -> None:
    ddl = (Path(__file__).resolve().parents[2] / "schemas" / "bond_rating_static.sql").read_text(encoding="utf-8")
    assert "ALTER TABLE bond_rating_static OWNER TO worker_writer" in ddl
    assert "ALTER FUNCTION bond_rating_static_prevent_mutation() OWNER TO worker_writer" in ddl
    assert "DROP TRIGGER IF EXISTS bond_rating_static_immutable ON bond_rating_static" in ddl
    assert "REVOKE ALL ON bond_rating_static FROM PUBLIC" in ddl


def test_hash_mismatch_refuses_before_parquet_read(tmp_path: Path) -> None:
    from src.bonds.static_ratings import StaticRatingRefusal, build_static_mapping

    artifact = _artifact(tmp_path)
    with pytest.raises(StaticRatingRefusal, match="sha256_mismatch"):
        build_static_mapping(artifact, expected_sha256="0" * 64)


@pytest.mark.parametrize(
    "latest_buckets",
    [
        ["A", "BBB"],
        ["A", "X"],
    ],
)
def test_latest_month_ambiguity_or_invalid_value_refuses_the_mapping(
    tmp_path: Path, latest_buckets: list[str]
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from src.bonds.static_ratings import StaticRatingRefusal, build_static_mapping

    artifact = tmp_path / "ambiguous.parquet"
    pq.write_table(
        pa.table({
            "cusip_id": ["037833100", "037833100", "037833100"],
            "month": [date(2025, 1, 1), date(2025, 2, 1), date(2025, 2, 1)],
            "rating_bucket": ["AA", *latest_buckets],
        }),
        artifact,
    )

    with pytest.raises(StaticRatingRefusal, match="ambiguous_latest_rating"):
        build_static_mapping(artifact, expected_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())


def test_static_backfill_cli_emits_a_stdout_pure_copy_slice(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path)
    result = subprocess.run(
        [
            sys.executable, "scripts/backfill_bond_rating_static.py", "--artifact", str(artifact),
            "--sha256", hashlib.sha256(artifact.read_bytes()).hexdigest(), "--emit-psql", "--cursor", "0", "--limit", "1",
        ],
        cwd=Path(__file__).resolve().parents[2], check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("\\set ON_ERROR_STOP on\nBEGIN;")
    assert result.stderr == ""


def test_copy_cursor_refuses_past_the_mapping_and_schema_emit_is_psql_only(tmp_path: Path) -> None:
    from src.bonds.static_ratings import StaticRatingRefusal, build_static_mapping
    from src.workers.bond_rating_static_backfill import render_copy_slice

    artifact = _artifact(tmp_path)
    result = build_static_mapping(artifact, expected_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())
    with pytest.raises(StaticRatingRefusal, match="cursor_beyond_mapping"):
        render_copy_slice(tuple(result.mapping.values()), cursor=3, limit=1)
    with pytest.raises(StaticRatingRefusal, match="cursor_at_end_requires_no_batch"):
        render_copy_slice(tuple(result.mapping.values()), cursor=2, limit=1)
    schema = subprocess.run(
        [sys.executable, "scripts/backfill_bond_rating_static.py", "--emit-schema"],
        cwd=Path(__file__).resolve().parents[2], check=False, capture_output=True, text=True,
    )
    assert schema.returncode == 0, schema.stderr
    assert schema.stdout.startswith("\\set ON_ERROR_STOP on\nBEGIN;\n")
    assert "SET LOCAL ROLE worker_writer" not in schema.stdout
    assert "ALTER TABLE bond_rating_static OWNER TO worker_writer" in schema.stdout
    assert schema.stderr == ""


def test_coverage_reports_curated_states_buckets_and_unmatched() -> None:
    from src.bonds.static_ratings import StaticRating, coverage_report

    mapping = {
        "037833100": StaticRating("037833100", "A", date(2025, 1, 1), "rated", "a" * 64, 1),
        "594918104": StaticRating("594918104", "NR", date(2025, 2, 1), "not_rated", "a" * 64, 2),
    }
    report = coverage_report(["037833100", "459200101"], mapping)

    assert report["bucket_counts"] == {"A": 1, "missing": 1}
    assert report["state_counts"] == {"rated": 1, "missing": 1}
    assert report["unmatched_mapping_cusips"] == 1


def test_mapping_evidence_reports_last_row_parity_shape(tmp_path: Path) -> None:
    from src.bonds.static_ratings import build_static_mapping, mapping_evidence, verify_mapping_against_artifact

    artifact = _artifact(tmp_path)
    result = build_static_mapping(artifact, expected_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())
    evidence = mapping_evidence(result)

    assert evidence["source_rows"] == 5
    assert evidence["mapping_rows"] == 2
    assert evidence["rating_month_max"] == "2025-02-01"
    assert evidence["bucket_counts"] == {"AA": 1, "NR": 1}
    assert evidence["selection_rule"] == "one_generic_bucket_at_each_cusip_latest_month"
    assert verify_mapping_against_artifact(artifact, expected_sha256=result.source_sha256, mapping=result.mapping) == {
        "rows": 2, "matches": 2, "differences": 0
    }


def test_t4_evidence_records_the_live_snapshot_measurement() -> None:
    report = (Path(__file__).resolve().parents[2] / "docs" / "calibration" / "bond_panel_pack_live_evidence_001.md").read_text(encoding="utf-8")
    assert "ab48d99f466ae3a943ce0a2819175ab6efdd95212b4efc9079151750057b077a" in report
    assert "1,688,652" in report
    assert "31,375" in report
    assert "59 / 31,375" in report


def test_local_postgres_static_mapping_is_immutable_and_conflicts_fail(tmp_path: Path) -> None:
    import psycopg

    from src.bonds.static_ratings import StaticRatingRefusal, build_static_mapping
    from src.workers.bond_rating_static_backfill import install_schema, load_static_mapping

    database = f"static_rating_{uuid4().hex}"
    admin_dsn = "host=127.0.0.1 port=5432 dbname=postgres user=postgres sslmode=disable connect_timeout=5"
    try:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'CREATE DATABASE "{database}"')
    except psycopg.Error as exc:
        pytest.skip(f"local PostgreSQL unavailable: {exc}")
    dsn = f"host=127.0.0.1 port=5432 dbname={database} user=postgres sslmode=disable connect_timeout=5"
    try:
        artifact = _artifact(tmp_path)
        result = build_static_mapping(artifact, expected_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())
        rows = tuple(result.mapping.values())
        with psycopg.connect(dsn) as conn:
            install_schema(conn)
            install_schema(conn)
            conn.commit()
            owner, function_owner, runtime_select = conn.execute(
                """SELECT (SELECT tableowner FROM pg_tables WHERE tablename='bond_rating_static'),
                          (SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE proname='bond_rating_static_prevent_mutation'),
                          has_table_privilege('app_runtime', 'bond_rating_static', 'SELECT')"""
            ).fetchone()
            assert (owner, function_owner, runtime_select) == ("worker_writer", "worker_writer", False)
            conn.commit()
            with conn.transaction():
                conn.execute("SET LOCAL ROLE worker_writer")
                first = load_static_mapping(conn, rows, batch_size=1)
            with conn.transaction():
                conn.execute("SET LOCAL ROLE worker_writer")
                second = load_static_mapping(conn, rows, batch_size=1)
            assert first == {"inserted": 2, "existing": 0, "conflicted": 0, "skipped": 0, "reconciled": 2}
            assert second == {"inserted": 0, "existing": 2, "conflicted": 0, "skipped": 0, "reconciled": 2}
            with pytest.raises(StaticRatingRefusal, match="writer_role_required"):
                load_static_mapping(conn, rows)
            with pytest.raises(psycopg.Error, match="immutable"):
                conn.execute("UPDATE bond_rating_static SET rating_bucket='AAA'")
            conn.rollback()
            altered = list(rows)
            altered[0] = altered[0].__class__(altered[0].cusip9, "AAA", altered[0].rating_as_of_month, "rated", altered[0].source_sha256, altered[0].source_row_number)
            with conn.transaction():
                conn.execute("SET LOCAL ROLE worker_writer")
                with pytest.raises(StaticRatingRefusal, match="immutable_conflict"):
                    load_static_mapping(conn, altered)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s", (database,))
            admin.execute(f'DROP DATABASE IF EXISTS "{database}"')

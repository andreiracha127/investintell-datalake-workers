"""Artifact-pinned ingest worker (``src.workers.bond_price_ingest``).

Covers, per the Wave 1 Task 1 brief:

* fail-closed env contract: absent/malformed pin and absent locator refuse
  BEFORE any database connection is opened (the DSN below is unreachable, so a
  premature connection attempt would error the test);
* typed refusals with ZERO database writes (proven against the disposable PG);
* landing through the EXISTING resolution path (identifier normalization is
  reused, never re-implemented): resolved rows land + are eligibility-capable,
  unresolvable identifiers land ``unresolved`` (landed-but-not-published),
  unparseable dates are quarantined with lexical evidence — never dropped;
* source registration: ONE ``sec_ingestion_runs``/``sec_source_packages`` pair
  per pinned artifact, discoverable by the price materializer's own
  ``_latest_validated_source`` SELECT; ``source_contract_ref`` is an opaque
  token with no vendor identity;
* idempotent re-run: PK collision path (deterministic observation ids), zero
  new rows, ``skipped_existing == first.inserted``;
* leak scan: no vendor identifiers in the new surfaces or in any landed value;
  the env var names never leave the worker's env handling.

DSN-agnostic (Global Constraint): DB cases read ``SEC_TEST_DATABASE_URL`` and
run identically under the keyword and URL DSN conventions.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import os
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.bonds import source_artifact
from src.workers import bond_price_ingest

ROOT = Path(__file__).resolve().parents[1]

# Vendor tokens are composed dynamically so this test file itself stays clean
# under any grep-based leak scan.
_PREFIX = "OS" + "BAP"
VENDOR_TOKENS = (
    _PREFIX,
    "open" + "bond" + "asset" + "pricing",
    "TR" + "ACE",
    "WR" + "DS",
)

# A DSN that fails fast if anything ever tries to connect with it: pre-DB
# refusals must return BEFORE psycopg is asked to dial anywhere.
UNREACHABLE_DSN = "host=127.0.0.1 port=9 connect_timeout=1"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_parquet(path: Path, columns: dict[str, list], *, row_group_size: int | None = None) -> str:
    pq.write_table(pa.table(columns), path, row_group_size=row_group_size)
    return _sha256(path)


def _happy_columns() -> dict[str, list]:
    return {
        "cusip_id": ["037833100", "459200101", "594918104", "BADCUSIP!", "594918104"],
        "trd_exctn_dt": ["2026-06-01", "2026-06-15", "2026-06-30", "2026-06-30", "not-a-date"],
        "pr": [99.5, 100.25, 101.0, 50.0, 77.0],
        "ytm": [4.1, 4.2, None, 9.9, 5.5],
        "db_type": [3, None, 1, None, None],
    }


def _pin_env(monkeypatch: pytest.MonkeyPatch, *, path: Path | None, pin: str | None) -> None:
    for name in (
        bond_price_ingest.ENV_ARTIFACT_PIN,
        bond_price_ingest.ENV_ARTIFACT_PATH,
        bond_price_ingest.ENV_ARTIFACT_URL,
    ):
        monkeypatch.delenv(name, raising=False)
    if pin is not None:
        monkeypatch.setenv(bond_price_ingest.ENV_ARTIFACT_PIN, pin)
    if path is not None:
        monkeypatch.setenv(bond_price_ingest.ENV_ARTIFACT_PATH, str(path))


def _assert_envelope_is_grep_clean(envelope: dict) -> None:
    payload = json.dumps(envelope, default=str).lower()
    for token in VENDOR_TOKENS:
        assert token.lower() not in payload, token


# ---------------------------------------------------------------------------
# Static contract + leak scan (no database).
# ---------------------------------------------------------------------------
def test_worker_uses_a_dedicated_advisory_lock_and_the_existing_seams() -> None:
    source = (ROOT / "src" / "workers" / "bond_price_ingest.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_PRICE_INGEST" in source
    assert "advisory_lock" in source
    db = (ROOT / "src" / "db.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_PRICE_INGEST = 900_348" in db
    # Landing goes through the EXISTING resolution/insert protocol — the worker
    # must not re-implement normalization or the observation INSERT.
    assert "load_price_observations" in source
    assert "normalize_cusip9" not in source
    # Source registration mirrors the shared manifests idiom.
    for seam in ("create_or_resume_run", "register_file", "validate_raw_run", "register_package_discovery"):
        assert seam in source, seam


def test_refusal_vocabulary_is_closed() -> None:
    assert source_artifact.REFUSAL_SHA256_MISMATCH == "sha256_mismatch"
    assert source_artifact.REFUSAL_SCHEMA_CONTRACT_VIOLATION == "schema_contract_violation"
    assert source_artifact.REFUSAL_ARTIFACT_UNAVAILABLE == "artifact_unavailable"


def test_new_surfaces_carry_no_vendor_identity() -> None:
    # source_artifact: zero vendor appearances of any kind.
    artifact_source = inspect.getsource(source_artifact).lower()
    for token in VENDOR_TOKENS:
        assert token.lower() not in artifact_source, token
    # worker: the ONLY allowed appearances are the three env var names, which
    # never leave the env handling.
    worker_source = (ROOT / "src" / "workers" / "bond_price_ingest.py").read_text(encoding="utf-8")
    for allowed in (
        bond_price_ingest.ENV_ARTIFACT_PIN,
        bond_price_ingest.ENV_ARTIFACT_PATH,
        bond_price_ingest.ENV_ARTIFACT_URL,
    ):
        assert worker_source.count(allowed) == 1, allowed  # constant assignment only
        worker_source = worker_source.replace(allowed, "")
    lowered = worker_source.lower()
    for token in VENDOR_TOKENS:
        assert token.lower() not in lowered, token


# ---------------------------------------------------------------------------
# Fail-closed env contract: refusals BEFORE any database connection.
# ---------------------------------------------------------------------------
def test_missing_pin_refuses_fail_closed_before_any_db_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    _pin_env(monkeypatch, path=None, pin=None)
    envelope = bond_price_ingest.run(UNREACHABLE_DSN)
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "sha256_mismatch"
    _assert_envelope_is_grep_clean(envelope)


def test_malformed_pin_refuses_fail_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.parquet"
    _write_parquet(artifact, _happy_columns())
    _pin_env(monkeypatch, path=artifact, pin="not-64-hex")
    envelope = bond_price_ingest.run(UNREACHABLE_DSN)
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "sha256_mismatch"
    _assert_envelope_is_grep_clean(envelope)


def test_missing_locator_refuses_artifact_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    _pin_env(monkeypatch, path=None, pin="0" * 64)
    envelope = bond_price_ingest.run(UNREACHABLE_DSN)
    assert envelope == {"state": "refused", "reason": "artifact_unavailable", "detail": {"detail": "no_artifact_locator"}}


def test_unreachable_remote_locator_refuses_artifact_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    _pin_env(monkeypatch, path=None, pin="0" * 64)
    # Nothing listens on port 9: the streaming fetch fails fast, and neither
    # the locator nor any env var name may surface in the refusal envelope.
    monkeypatch.setenv(bond_price_ingest.ENV_ARTIFACT_URL, "https://127.0.0.1:9/a.bin")
    envelope = bond_price_ingest.run(UNREACHABLE_DSN)
    assert envelope == {"state": "refused", "reason": "artifact_unavailable", "detail": {"detail": "fetch_failed"}}
    _assert_envelope_is_grep_clean(envelope)


def test_absent_artifact_file_refuses_artifact_unavailable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _pin_env(monkeypatch, path=tmp_path / "missing.parquet", pin="0" * 64)
    envelope = bond_price_ingest.run(UNREACHABLE_DSN)
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "artifact_unavailable"
    _assert_envelope_is_grep_clean(envelope)


def test_pin_mismatch_refuses_before_any_db_connection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.parquet"
    _write_parquet(artifact, _happy_columns())
    _pin_env(monkeypatch, path=artifact, pin="0" * 64)
    envelope = bond_price_ingest.run(UNREACHABLE_DSN)
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "sha256_mismatch"
    _assert_envelope_is_grep_clean(envelope)


def test_missing_required_column_refuses_schema_contract_violation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    columns = _happy_columns()
    del columns["pr"]
    artifact = tmp_path / "artifact.parquet"
    sha = _write_parquet(artifact, columns)
    _pin_env(monkeypatch, path=artifact, pin=sha)
    envelope = bond_price_ingest.run(UNREACHABLE_DSN)
    assert envelope["state"] == "refused"
    assert envelope["reason"] == "schema_contract_violation"
    assert envelope["detail"]["missing_columns"] == ["pr"]
    _assert_envelope_is_grep_clean(envelope)


# ---------------------------------------------------------------------------
# Database cases (disposable PG; both DSN conventions via SEC_TEST_DATABASE_URL).
# ---------------------------------------------------------------------------
pytestmark_db = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)


@pytest.fixture(autouse=True)
def _isolated_ingest_database():
    """O banco SEC descartável não retém runs/pacotes/observações entre casos."""
    dsn = os.getenv("SEC_TEST_DATABASE_URL")
    if not dsn:
        yield
        return
    import psycopg

    from src.bonds import price_observations
    from src.sec_regulatory import manifests

    with psycopg.connect(dsn) as conn:
        manifests.install_schema(conn)
        price_observations.install_schema(conn)
        with conn.cursor() as cur:
            cur.execute((ROOT / "schemas" / "bond_price_eligibility_v1.sql").read_text(encoding="utf-8"))
            cur.execute(
                "TRUNCATE bond_price_observation, sec_source_packages, sec_ingestion_runs CASCADE"
            )
        conn.commit()
    yield


def _db_dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


@pytestmark_db
def test_happy_path_lands_registers_and_is_discoverable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import psycopg

    from src.workers.bond_price_observations import _latest_validated_source

    artifact = tmp_path / "artifact.parquet"
    sha = _write_parquet(artifact, _happy_columns())
    _pin_env(monkeypatch, path=artifact, pin=sha)

    envelope = bond_price_ingest.run(_db_dsn())
    assert envelope["state"] == "ok"
    assert envelope["inserted"] == 4          # 3 resolved + 1 unresolved-identity row land
    assert envelope["skipped_existing"] == 0
    assert envelope["quarantined"] == 1       # the unparseable-date row
    token = envelope["source_contract_ref"]
    assert token == f"bond_price_source_v1@{sha[:12]}"
    _assert_envelope_is_grep_clean(envelope)

    with psycopg.connect(_db_dsn()) as conn:
        # The price materializer's OWN discovery SELECT finds the registered pair.
        discovered = _latest_validated_source(conn)
        assert discovered is not None
        assert str(discovered[0]) == envelope["run_id"]
        assert str(discovered[1]) == envelope["package_id"]

        rows = conn.execute(
            "SELECT identity_state, price_type, accrued_treatment, as_of FROM bond_price_observation"
        ).fetchall()
        assert len(rows) == 4
        assert sorted(r[0] for r in rows) == ["resolved", "resolved", "resolved", "unresolved"]
        assert {r[1] for r in rows} == {"trade"} and {r[2] for r in rows} == {"clean"}
        assert {r[3] for r in rows} == {date(2026, 6, 30)}  # as_of pinned to the artifact cutoff

        # ytm passes through raw to the observation surface.
        ytm = conn.execute(
            "SELECT ytm FROM bond_price_observation WHERE normalized_cusip9='037833100'"
        ).fetchone()[0]
        assert float(ytm) == 4.1

        # Landed rows are eligibility-capable (the PIT gate input for Wave 1).
        eligible = conn.execute(
            "SELECT count(*) FROM bond_price_eligibility_v1 WHERE is_eligible"
        ).fetchone()[0]
        assert eligible == 3  # the unresolved-identity row is landed but not eligible

        # Source registration: package loaded + pinned, run raw-validated.
        package = conn.execute(
            "SELECT package_state, package_sha256, package_relative_path, source_family "
            "FROM sec_source_packages"
        ).fetchall()
        assert package == [("loaded", sha, token, "bond_price")]

        # Manifest accounting closes exactly; the bad-date row is quarantined
        # with lexical evidence — never dropped silently.
        accounting = conn.execute(
            "SELECT expected_count, data_count, lexical_count, typed_success_count, "
            "quarantine_count, reject_count, state FROM sec_source_files"
        ).fetchall()
        assert accounting == [(5, 5, 5, 4, 1, 0, "accounted")]
        issue = conn.execute(
            "SELECT typed_error_code, status, raw_lexical_value FROM sec_row_issues"
        ).fetchall()
        assert issue == [("invalid_observation_date", "quarantined", "not-a-date")]

        # Lineage carries ONLY the opaque token (plus artifact coordinates).
        lineage_refs = conn.execute(
            "SELECT DISTINCT source_lineage->>'source_contract_ref' FROM bond_price_observation"
        ).fetchall()
        assert lineage_refs == [(token,)]

        # Data-level scrub: no vendor identity in any landed text value.
        landed_text = json.dumps(
            conn.execute(
                "SELECT cusip9_input, identity_reason_code, source_lineage::text "
                "FROM bond_price_observation"
            ).fetchall()
            + package
            + conn.execute(
                "SELECT source_family, package_relative_path, parser_version FROM sec_ingestion_runs"
            ).fetchall(),
            default=str,
        ).lower()
        for vendor_token in VENDOR_TOKENS:
            assert vendor_token.lower() not in landed_text, vendor_token


@pytestmark_db
def test_second_run_is_idempotent_over_the_pk_collision_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import psycopg

    artifact = tmp_path / "artifact.parquet"
    sha = _write_parquet(artifact, _happy_columns())
    _pin_env(monkeypatch, path=artifact, pin=sha)

    first = bond_price_ingest.run(_db_dsn())
    assert first["state"] == "ok" and first["inserted"] == 4

    second = bond_price_ingest.run(_db_dsn())
    assert second["state"] == "ok"
    assert second["inserted"] == 0
    assert second["skipped_existing"] == first["inserted"]
    assert second["run_id"] == first["run_id"]
    assert second["package_id"] == first["package_id"]

    with psycopg.connect(_db_dsn()) as conn:
        assert conn.execute("SELECT count(*) FROM bond_price_observation").fetchone()[0] == 4
        # ONE run/package pair per pinned artifact — never duplicated.
        assert conn.execute("SELECT count(*) FROM sec_ingestion_runs").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM sec_source_packages").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM sec_validated_raw_runs").fetchone()[0] == 1
        # Quarantine evidence is not re-recorded on the resume path.
        assert conn.execute("SELECT count(*) FROM sec_row_issues").fetchone()[0] == 1


@pytestmark_db
def test_refusals_write_nothing_to_the_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import psycopg

    artifact = tmp_path / "artifact.parquet"
    _write_parquet(artifact, _happy_columns())
    _pin_env(monkeypatch, path=artifact, pin="0" * 64)  # wrong pin

    envelope = bond_price_ingest.run(_db_dsn())
    assert envelope["state"] == "refused" and envelope["reason"] == "sha256_mismatch"

    with psycopg.connect(_db_dsn()) as conn:
        for table in ("bond_price_observation", "sec_ingestion_runs", "sec_source_packages", "sec_source_files"):
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0, table


@pytestmark_db
def test_multi_batch_artifact_streams_and_lands_completely(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import psycopg

    n = 250
    columns = {
        "cusip_id": ["037833100"] * n,
        "trd_exctn_dt": [f"2026-{(i % 12) + 1:02d}-15" for i in range(n)],
        "pr": [90.0 + (i % 50) for i in range(n)],
    }
    artifact = tmp_path / "multi.parquet"
    sha = _write_parquet(artifact, columns, row_group_size=100)
    _pin_env(monkeypatch, path=artifact, pin=sha)
    monkeypatch.setattr(bond_price_ingest, "_LOAD_BATCH_SIZE", 100)  # force multiple load batches

    envelope = bond_price_ingest.run(_db_dsn())
    assert envelope["state"] == "ok"
    assert envelope["inserted"] == n and envelope["quarantined"] == 0

    with psycopg.connect(_db_dsn()) as conn:
        assert conn.execute("SELECT count(*) FROM bond_price_observation").fetchone()[0] == n


@pytestmark_db
def test_zip_wrapped_artifact_lands_identically(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import zipfile

    inner = tmp_path / "inner.parquet"
    _write_parquet(inner, _happy_columns())
    artifact = tmp_path / "artifact.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.write(inner, "source.parquet")
    sha = _sha256(artifact)
    _pin_env(monkeypatch, path=artifact, pin=sha)

    envelope = bond_price_ingest.run(_db_dsn())
    assert envelope["state"] == "ok"
    assert envelope["inserted"] == 4 and envelope["quarantined"] == 1
    assert envelope["source_contract_ref"] == f"bond_price_source_v1@{sha[:12]}"

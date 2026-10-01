"""Build manifest: allowlisted stack evidence, no secrets, never an identity input.

Fake connections and injected environments only; no DSN, no database.
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.bonds import build_manifest as bm
from src.bonds import implied_rating as policy
from src.bonds.implied_rating_materializer import build_fingerprint, publication_id_for
from src.workers import bond_market_implied_rating as worker

SECRET_DSN = "postgresql://worker_writer:hunter2@db.internal:5432/market"
CPUINFO = (
    "processor\t: 0\nmodel name\t: Synthetic CPU 9000\nflags\t\t: fpu sse2 avx2 fma avx512f\n\n"
    "processor\t: 1\nmodel name\t: Synthetic CPU 9000\nflags\t\t: fpu sse2 avx2 fma avx512f\n\n"
)


def _image_root(tmp_path: Path, *, lock_sidecar: str | None = "match") -> Path:
    image_dir = tmp_path / "docker" / "bond-live-daily"
    image_dir.mkdir(parents=True)
    lock = image_dir / "requirements.lock"
    lock.write_bytes(b"numpy==2.5.1 --hash=sha256:abc\n")
    dockerfile = image_dir / "Dockerfile"
    dockerfile.write_bytes(b"FROM python:3.13.12-slim@sha256:deadbeef\n")
    if lock_sidecar is not None:
        digest = hashlib.sha256(lock.read_bytes()).hexdigest()
        recorded = digest if lock_sidecar == "match" else "0" * 64
        (image_dir / "requirements.lock.sha256").write_text(f"{recorded}  requirements.lock\n")
    (image_dir / "Dockerfile.sha256").write_text(
        f"{hashlib.sha256(dockerfile.read_bytes()).hexdigest()}  Dockerfile\n"
    )
    return tmp_path


def _manifest(tmp_path: Path, environ: dict[str, str] | None = None, **kwargs):
    return bm.collect_build_manifest(
        root=_image_root(tmp_path), environ=environ or {}, cpuinfo=CPUINFO, **kwargs
    )


def test_manifest_is_complete_json_and_reflects_the_running_interpreter(tmp_path):
    manifest = _manifest(tmp_path, {"RAILWAY_GIT_COMMIT_SHA": "a" * 40, "OMP_NUM_THREADS": "1"})
    assert manifest["schema"] == bm.MANIFEST_SCHEMA
    assert set(manifest) == {
        "schema", "python", "packages", "platform", "cpu", "numpy_runtime",
        "thread_env", "lock", "dockerfile", "source_revision",
    }
    json.dumps(manifest)  # serializable as-is: it rides every result JSON
    assert manifest["python"]["version_info"] == list(sys.version_info[:5])
    assert manifest["python"]["implementation"] == sys.implementation.name
    assert manifest["packages"]["numpy"] == __import__("numpy").__version__
    assert manifest["packages"]["pandas"] == pd.__version__
    assert set(manifest["packages"]) == set(bm.TRACKED_PACKAGES)
    assert manifest["cpu"] == {
        "source": "/proc/cpuinfo", "model": "Synthetic CPU 9000",
        "flags": ["avx2", "avx512f", "fma", "fpu", "sse2"], "logical_processors": 2,
    }
    assert manifest["numpy_runtime"]["present"] is True
    assert isinstance(manifest["numpy_runtime"]["cpu_features"], dict)
    assert manifest["numpy_runtime"]["cpu_baseline"]
    assert manifest["numpy_runtime"]["blas"] is None or "blas" in manifest["numpy_runtime"]["blas"]
    assert manifest["thread_env"]["OMP_NUM_THREADS"] == "1"
    assert manifest["thread_env"]["MKL_NUM_THREADS"] is None
    assert manifest["source_revision"] == {"revision": "a" * 40, "source": "RAILWAY_GIT_COMMIT_SHA"}
    assert manifest["lock"]["present"] is True
    assert manifest["lock"]["sha256_matches_recorded"] is True
    assert manifest["lock"]["path"] == "docker/bond-live-daily/requirements.lock"
    assert manifest["dockerfile"]["sha256_matches_recorded"] is True
    assert "python=" in bm.manifest_summary(manifest)


def test_absent_package_file_and_cpuinfo_are_explicit_not_omitted(tmp_path, monkeypatch):
    monkeypatch.setattr(bm, "TRACKED_PACKAGES", ("numpy", "definitely-not-installed-pkg"))
    manifest = bm.collect_build_manifest(root=tmp_path, environ={}, cpuinfo=None)
    assert manifest["packages"]["definitely-not-installed-pkg"] is None
    assert manifest["cpu"] == {
        "source": None, "model": None, "flags": None, "logical_processors": None,
    }
    assert manifest["lock"] == {
        "path": "docker/bond-live-daily/requirements.lock", "present": False,
        "sha256": None, "recorded_sha256": None, "sha256_matches_recorded": None,
    }
    assert manifest["source_revision"] == {"revision": None, "source": None}


def test_lock_sidecar_mismatch_is_reported_not_hidden(tmp_path):
    manifest = bm.collect_build_manifest(
        root=_image_root(tmp_path, lock_sidecar="mismatch"), environ={}, cpuinfo=None
    )
    assert manifest["lock"]["present"] is True
    assert manifest["lock"]["recorded_sha256"] == "0" * 64
    assert manifest["lock"]["sha256_matches_recorded"] is False
    without_sidecar = bm.collect_build_manifest(
        root=_image_root(tmp_path / "other", lock_sidecar=None), environ={}, cpuinfo=None
    )
    assert without_sidecar["lock"]["recorded_sha256"] is None
    assert without_sidecar["lock"]["sha256_matches_recorded"] is None


def test_manifest_reads_only_allowlisted_environment_and_leaks_no_secret(tmp_path):
    environ = {
        "DATABASE_URL": SECRET_DSN,
        "FINNHUB_API_KEY": "finnhub-secret-token",
        "DB_TLS_KEY_PEM": "-----BEGIN PRIVATE KEY-----",
        "PATH": "/usr/bin",
        "OPENBLAS_NUM_THREADS": "1",
        "NPY_DISABLE_CPU_FEATURES": "AVX512F",
        "CODE_REVISION": "pinned-revision",
    }
    text = json.dumps(_manifest(tmp_path, environ))
    for secret in (SECRET_DSN, "hunter2", "finnhub-secret-token", "PRIVATE KEY", "/usr/bin"):
        assert secret not in text
    manifest = json.loads(text)
    assert set(manifest["thread_env"]) == set(bm.ALLOWED_ENV_VARS)
    assert manifest["thread_env"]["OPENBLAS_NUM_THREADS"] == "1"
    assert manifest["thread_env"]["NPY_DISABLE_CPU_FEATURES"] == "AVX512F"
    assert manifest["source_revision"] == {"revision": "pinned-revision", "source": "CODE_REVISION"}


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({"CODE_REVISION": "pin", "RAILWAY_GIT_COMMIT_SHA": "deploy"}, ("pin", "CODE_REVISION")),
        ({"GIT_SHA": "ci"}, ("ci", "GIT_SHA")),
        ({"SOURCE_COMMIT": "  src  "}, ("src", "SOURCE_COMMIT")),
        ({"RAILWAY_GIT_COMMIT_SHA": "deploy"}, ("deploy", "RAILWAY_GIT_COMMIT_SHA")),
    ],
)
def test_source_revision_ladder_matches_the_worker_ladder(environ, expected, tmp_path):
    assert bm.resolve_source_revision(environ, cwd=tmp_path) == {
        "revision": expected[0], "source": expected[1],
    }
    assert tuple(bm.REVISION_ENV_VARS) == worker._REVISION_ENV_VARS


# --------------------------------------------------------------------------- #
# The manifest is evidence, never identity
# --------------------------------------------------------------------------- #
PARENT = {
    "publication_id": "panel-current",
    "first_month": date(2026, 6, 1),
    "last_closed_month": date(2026, 8, 1),
    "open_month": date(2026, 9, 1),
}
REVISION = "runtime-revision"


def _snapshot() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "cusip_id": "AAA000001", "month": pd.Timestamp(month), "price": 95.0,
            "spread_final_bps": spread, "mod_dur": 5.0, "trade_count": 10,
            "dollar_volume": 1_000_000.0, "maturity_date": date(2035, 1, 1),
        }
        for month, spread in (("2026-06-01", 80.0), ("2026-07-01", 90.0), ("2026-08-01", 95.0))
    ])


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        pass

    def execute(self, sql, params=None):
        return self

    def fetchone(self):
        return None


def _patch_plan(monkeypatch, manifest):
    monkeypatch.setattr(worker, "connect", lambda _dsn: _FakeConnection())
    monkeypatch.setattr(worker, "resolve_dsn", lambda _dsn: "postgresql://example")
    monkeypatch.setattr(worker, "_code_revision", lambda: REVISION)
    monkeypatch.delenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", raising=False)
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, name: True)
    monkeypatch.setattr(worker, "_current_panel", lambda _conn: dict(PARENT))
    monkeypatch.setattr(worker, "_current_pointer", lambda _conn: None)
    monkeypatch.setattr(worker, "_mirror_serves_panel", lambda _conn, **kwargs: True)
    monkeypatch.setattr(worker, "current_pinned_anchor", lambda _conn: None)
    monkeypatch.setattr(worker, "_read_snapshot", lambda _conn, **kwargs: _snapshot())
    monkeypatch.setattr(worker, "collect_build_manifest", lambda: manifest)


def test_changing_the_manifest_changes_no_identity_fingerprint_or_digest(monkeypatch):
    results = []
    for stamp in ("stack-a", "stack-b"):
        manifest = {"schema": bm.MANIFEST_SCHEMA, "packages": {"numpy": stamp}, "lock": {"sha256": stamp}}
        _patch_plan(monkeypatch, manifest)
        result = worker.plan("postgresql://example")
        assert result["state"] == "planned", result
        assert result["build_manifest"] == manifest
        results.append(result)
    first, second = results
    for key in ("publication_id", "input_fingerprint", "rows_digest", "row_count", "l_anchor"):
        assert first[key] == second[key], key
    # The identity primitives take no manifest at all: same inputs, same answers.
    assert first["publication_id"] == publication_id_for(
        policy.POLICY_DIGEST, REVISION, first["input_fingerprint"]
    )
    assert build_fingerprint(policy.POLICY_DIGEST, REVISION, first["input_fingerprint"]) == (
        build_fingerprint(policy.POLICY_DIGEST, REVISION, second["input_fingerprint"])
    )
    assert first["input_fingerprint"] == policy.snapshot_fingerprint(_snapshot())
    rows = policy.build_publication_rows(
        _snapshot(), last_closed_month=PARENT["last_closed_month"], l_anchor=first["l_anchor"]
    )
    assert policy.rows_digest(rows) == first["rows_digest"]


def test_plan_result_carries_the_real_manifest_and_stays_serializable(monkeypatch):
    _patch_plan(monkeypatch, bm.collect_build_manifest(environ={}, cpuinfo=None))
    result = worker.plan("postgresql://example")
    assert result["state"] == "planned"
    manifest = result["build_manifest"]
    assert manifest["python"]["version_info"] == list(sys.version_info[:5])
    assert manifest["packages"]["numpy"]
    json.dumps(result, default=str)

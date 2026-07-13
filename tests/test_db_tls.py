import os
import stat

import pytest

from src.db import resolve_dsn

CA = "-----BEGIN CERTIFICATE-----\nMIIfakeca\n-----END CERTIFICATE-----\n"
CRT = "-----BEGIN CERTIFICATE-----\nMIIfakecrt\n-----END CERTIFICATE-----\n"
KEY = "-----BEGIN PRIVATE KEY-----\nMIIfakekey\n-----END PRIVATE KEY-----\n"


def test_resolve_dsn_materializes_tls(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://worker_writer:pw@1.2.3.4:5432/market")
    monkeypatch.setenv("DB_TLS_CA_PEM", CA)
    monkeypatch.setenv("DB_TLS_CERT_PEM", CRT)
    monkeypatch.setenv("DB_TLS_KEY_PEM", KEY)
    monkeypatch.setenv("DB_TLS_DIR", str(tmp_path))
    dsn = resolve_dsn()
    assert "sslmode=verify-full" in dsn
    assert f"sslrootcert={tmp_path}/ca.crt" in dsn
    key_mode = stat.S_IMODE(os.stat(tmp_path / "client.key").st_mode)
    assert key_mode == 0o600  # libpq recusa key world-readable


def test_resolve_dsn_unchanged_without_tls_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@tiger:5432/tsdb?sslmode=require")
    for v in ("DB_TLS_CA_PEM", "DB_TLS_CERT_PEM", "DB_TLS_KEY_PEM"):
        monkeypatch.delenv(v, raising=False)
    assert resolve_dsn() == "postgresql://u:p@tiger:5432/tsdb?sslmode=require"


def test_resolve_dsn_strips_sslmode_preserves_other_params(monkeypatch, tmp_path):
    """Pre-existing sslmode/ssl* query params must be stripped, but non-ssl
    params (e.g. connect_timeout) must survive the TLS rewrite untouched."""
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://worker_writer:pw@1.2.3.4:5432/market?sslmode=require&connect_timeout=10",
    )
    monkeypatch.setenv("DB_TLS_CA_PEM", CA)
    monkeypatch.setenv("DB_TLS_CERT_PEM", CRT)
    monkeypatch.setenv("DB_TLS_KEY_PEM", KEY)
    monkeypatch.setenv("DB_TLS_DIR", str(tmp_path))
    dsn = resolve_dsn()
    assert dsn.count("sslmode=") == 1
    assert "sslmode=verify-full" in dsn
    assert "sslmode=require" not in dsn
    assert "connect_timeout=10" in dsn


def test_resolve_dsn_preserves_percent_encoded_userinfo(monkeypatch, tmp_path):
    """A percent-encoded password in DATABASE_URL must round-trip byte-identical
    through the TLS DSN rewrite (no decode/re-encode corruption)."""
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://worker_writer:p%2Fw%40x@1.2.3.4:5432/market",
    )
    monkeypatch.setenv("DB_TLS_CA_PEM", CA)
    monkeypatch.setenv("DB_TLS_CERT_PEM", CRT)
    monkeypatch.setenv("DB_TLS_KEY_PEM", KEY)
    monkeypatch.setenv("DB_TLS_DIR", str(tmp_path))
    dsn = resolve_dsn()
    assert "worker_writer:p%2Fw%40x@1.2.3.4:5432" in dsn
    assert "sslmode=verify-full" in dsn


def test_resolve_dsn_trailing_slash_dir(monkeypatch, tmp_path):
    """DB_TLS_DIR with a trailing slash must not produce a doubled '//' in the
    generated sslrootcert/sslcert/sslkey DSN paths."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://worker_writer:pw@1.2.3.4:5432/market")
    monkeypatch.setenv("DB_TLS_CA_PEM", CA)
    monkeypatch.setenv("DB_TLS_CERT_PEM", CRT)
    monkeypatch.setenv("DB_TLS_KEY_PEM", KEY)
    monkeypatch.setenv("DB_TLS_DIR", str(tmp_path) + "/")
    dsn = resolve_dsn()
    assert f"sslrootcert={tmp_path}/ca.crt" in dsn
    assert "//ca.crt" not in dsn

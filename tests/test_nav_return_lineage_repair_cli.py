"""CLI guards and read-only planning with no database or provider access."""

import datetime as dt
import json
from unittest.mock import Mock

import psycopg
import pytest

from scripts import repair_nav_return_lineage_v1 as cli
from src.workers import nav_return_lineage_repair as repair


PRIVATE_DSN = "postgresql://secret-user:secret-password@private-host/private-db"
IID = "11111111-1111-1111-1111-111111111111"


def _output(capsys, exit_code, expected_code):
    captured = capsys.readouterr()
    assert captured.err == ""
    assert len(captured.out.splitlines()) == 1
    result = json.loads(captured.out)
    assert result["exit_code"] == exit_code
    assert result["code"] == expected_code
    for forbidden in ("postgresql://", "https://", "secret-password",
                      "private-host", "sensitive exception"):
        assert forbidden not in captured.out
    return result


@pytest.fixture
def offline(monkeypatch):
    monkeypatch.delenv("NAV_READINESS_DATABASE_URL", raising=False)
    connect = Mock(side_effect=AssertionError("database access forbidden"))
    monkeypatch.setattr(cli.psycopg, "connect", connect)
    monkeypatch.setattr(cli, "_schema_pins", lambda: {"test_pin": "fixture"})
    provider = Mock(side_effect=AssertionError("provider access forbidden"))
    monkeypatch.setattr(repair, "ProviderClient", provider)
    return connect, provider


def _apply_args(plan_file, digest):
    return ["--mode", "apply", "--plan-file", str(plan_file),
            "--plan-sha256", digest, "--instrument-id", IID,
            "--confirm", repair.CONFIRM_TOKEN]


def test_apply_missing_confirmation_is_sanitized_before_database(offline, capsys):
    args = _apply_args("https://private-host/plan", "a" * 64)[:-2]
    assert cli.main(args) == 2
    _output(capsys, 2, "APPLY_REQUIRES_PLAN_ALLOWLIST_CONFIRM")
    offline[0].assert_not_called()
    offline[1].assert_not_called()


def test_apply_digest_mismatch_is_sanitized_before_database(offline, tmp_path, capsys):
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps({"sensitive": "https://private-host/plan"}), encoding="utf-8")
    assert cli.main(_apply_args(plan_file, "0" * 64)) == 2
    _output(capsys, 2, "PLAN_DIGEST_MISMATCH")
    offline[0].assert_not_called()
    offline[1].assert_not_called()


def test_zero_request_budget_is_refused_before_database(offline, capsys):
    assert cli.main(["--max-requests", "0"]) == 2
    _output(capsys, 2, "MAX_REQUESTS_INVALID")
    offline[0].assert_not_called()
    offline[1].assert_not_called()


def test_missing_dsn_is_sanitized(offline, capsys):
    assert cli.main([]) == 2
    _output(capsys, 2, "DSN_REQUIRED")
    offline[0].assert_not_called()


@pytest.mark.parametrize("failure,expected_code", [
    (psycopg.OperationalError("sensitive exception " + PRIVATE_DSN), "DATABASE_ERROR"),
    (RuntimeError("sensitive exception https://private-host"), "VALIDATION_OR_IO_ERROR"),
])
def test_exceptions_never_expose_connection_or_message(
        offline, monkeypatch, capsys, failure, expected_code):
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", PRIVATE_DSN)
    offline[0].side_effect = failure
    assert cli.main([]) == 2
    _output(capsys, 2, expected_code)


@pytest.fixture
def planning_connection(offline, monkeypatch):
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", PRIVATE_DSN)
    conn = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    queries = []

    def execute(query):
        rendered = query.as_string() if hasattr(query, "as_string") else query
        queries.append(rendered)
        cursor = Mock()
        if rendered.startswith("SELECT (clock_timestamp()"):
            cursor.fetchone.return_value = (dt.date(2026, 10, 10),)
        elif rendered.startswith("SELECT DISTINCT instrument_id"):
            cursor.fetchall.return_value = []
        else:
            assert rendered.startswith("SET "), rendered
        return cursor

    conn.execute.side_effect = execute
    offline[0].side_effect = None
    offline[0].return_value = conn
    monkeypatch.setattr(cli.schema_operator, "_check", lambda *_: {"ready": True})
    return conn, queries, offline[1]


def test_default_plan_is_read_only_without_http_or_ledger(planning_connection, capsys):
    conn, queries, provider = planning_connection
    assert cli.main([]) == 0
    result = _output(capsys, 0, None)
    assert result["status"] == "planned"
    assert result["instruments"] == result["rows"] == 0
    assert len(result["plan_sha256"]) == 64
    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in queries
    assert all(q.startswith(("SET ", "SELECT ")) for q in queries)
    conn.rollback.assert_called_once()
    provider.assert_not_called()


def test_plan_file_is_new_and_cannot_be_overwritten(planning_connection, tmp_path, capsys):
    plan_file = tmp_path / "plan.json"
    assert cli.main(["--plan-file", str(plan_file)]) == 0
    first = _output(capsys, 0, None)
    original = plan_file.read_bytes()
    assert repair.sha256(json.loads(original)) == first["plan_sha256"]
    assert cli.main(["--plan-file", str(plan_file)]) == 2
    _output(capsys, 2, "PLAN_FILE_EXISTS")
    assert plan_file.read_bytes() == original
    planning_connection[2].assert_not_called()

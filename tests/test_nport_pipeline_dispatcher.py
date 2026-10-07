"""A stale or incomplete fund-data job must produce a structured red run."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from src import run_worker


def _run(monkeypatch, capsys, worker, stats=None, error=None, validator=None):
    def run(_dsn):
        if error is not None:
            raise error
        return stats

    monkeypatch.setenv("WORKER", worker)
    monkeypatch.delenv("WORKER_CALC_DATE", raising=False)
    monkeypatch.delenv("WORKER_LIMIT", raising=False)
    monkeypatch.setattr(run_worker, "resolve_dsn", lambda: "unused")
    monkeypatch.setattr(
        run_worker.importlib, "import_module", lambda _name: SimpleNamespace(run=run)
    )
    try:
        run_worker.main(monthly_source_validator=validator)
        code = 0
    except SystemExit as exc:
        code = exc.code
    return code, [json.loads(line) for line in capsys.readouterr().out.splitlines()]


@pytest.mark.parametrize(
    ("worker", "stats"),
    [
        ("characteristics", {"status": "partial", "equity_error": "query failed"}),
        ("characteristics", {"status": "skipped", "reason": "lock_held"}),
        ("nport_lookthrough", {"skipped": "lock_busy", "processed": 0}),
        ("nport_ingestion", {"state": "locked", "packages": 0}),
        ("nport_lookthrough", {"freshness": {"alarm": True, "breaches": ["RAW_STALE"]}}),
        ("nport_lookthrough", {"identifier_coverage": {"state": "degraded"}}),
        ("nport_lookthrough", {"identifier_coverage": {"state": "undecidable"}}),
        ("nport_classification_inputs_chain", {"aborted": True}),
        ("nport_ingestion", {"state": "conflict"}),
        ("characteristics", {}),
    ],
)
def test_incomplete_pipeline_results_emit_alarm_and_exit_one(
    monkeypatch, capsys, worker, stats
):
    code, lines = _run(monkeypatch, capsys, worker, stats)
    assert code == 1
    assert lines[-1]["event"] == "fund_pipeline_alarm"
    assert lines[-1]["worker"] == worker


def test_pipeline_exception_is_a_structured_failure_without_secret_values(
    monkeypatch, capsys
):
    code, lines = _run(
        monkeypatch, capsys, "characteristics", error=RuntimeError("secret-db-password")
    )
    assert code == 1
    assert lines[-1]["event"] == "fund_pipeline_alarm"
    assert lines[-1]["error_type"] == "RuntimeError"
    assert "secret-db-password" not in json.dumps(lines)


def test_verified_current_pipeline_result_stays_green(monkeypatch, capsys):
    stats = {"status": "current", "freshness": {"alarm": False}}
    code, lines = _run(monkeypatch, capsys, "characteristics", stats)
    assert code == 0
    assert lines == [{"worker": "characteristics", **stats}]


def test_optional_absent_verdicts_do_not_crash_the_dispatcher(monkeypatch, capsys):
    stats = {"status": "current", "freshness": None, "identifier_coverage": None}
    code, _lines = _run(monkeypatch, capsys, "nport_lookthrough", stats)
    assert code == 0


def test_empty_landing_scope_is_failed_before_opening_a_database(monkeypatch, tmp_path):
    from src.workers import nport_ingestion

    monkeypatch.setattr(nport_ingestion, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(nport_ingestion, "connect", lambda *_a: pytest.fail("empty scope opened DB"))
    stats = nport_ingestion.run("unused")
    assert stats["state"] == "failed"
    assert stats["packages"] == 0


@pytest.mark.parametrize("state", ["ok", "noop"])
def test_monthly_success_and_noop_cannot_certify_stale_source(monkeypatch, capsys, state):
    from src.workers import _fund_pipeline_freshness as freshness

    def validator(_dsn):
        raise freshness.FundPipelineBlocked({
            "stage": "nport", "alarm": True, "breaches": ["SOURCE_REPORT_STALE"],
        })

    code, lines = _run(monkeypatch, capsys, "nport_secapi_monthly",
                       {"state": state}, validator=validator)
    assert code == 1
    assert lines[-1]["event"] == "fund_pipeline_alarm"
    assert lines[-1]["state"] == "blocked"
    assert lines[-1]["freshness"]["breaches"] == ["SOURCE_REPORT_STALE"]


def test_a_verified_current_monthly_noop_is_green(monkeypatch, capsys):
    seen = []

    def validator(dsn):
        seen.append(dsn)
        return {"stage": "nport", "alarm": False}

    code, lines = _run(monkeypatch, capsys, "nport_secapi_monthly",
                       {"state": "noop"}, validator=validator)
    assert code == 0
    assert seen == ["unused"]
    assert lines[-1]["freshness"]["alarm"] is False

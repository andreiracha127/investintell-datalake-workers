"""Deployment contract for the bounded SEC API recovery worker."""

from __future__ import annotations

import inspect
from pathlib import Path

from src.workers import nport_fixed_income_secapi_recovery as worker


ROOT = Path(__file__).resolve().parents[1]


def test_secapi_recovery_dependency_dispatch_and_dedicated_one_shot_config():
    assert "sec-api==1.0.36" in (ROOT / "requirements.txt").read_text(encoding="utf-8")
    runner = (ROOT / "src" / "run_worker.py").read_text(encoding="utf-8")
    assert "nport_fixed_income_secapi_recovery" in runner
    config = (ROOT / "railway.nport-fixed-income-secapi-recovery.toml").read_text(
        encoding="utf-8"
    )
    assert 'startCommand = "python -m src.run_worker"' in config
    assert 'restartPolicyType = "never"' in config
    assert "healthcheck" not in config.lower()
    assert "cronSchedule" not in config


def test_recovery_service_requires_explicit_budgets_and_never_publishes():
    source = (
        ROOT / "src" / "workers" / "nport_fixed_income_secapi_recovery.py"
    ).read_text(encoding="utf-8")
    for name in (
        "NPORT_SECAPI_SOURCE_HOLDINGS_PUBLICATION_ID",
        "NPORT_SECAPI_SOURCE_RUN_ID",
        "NPORT_SECAPI_MAX_ACCESSIONS",
        "NPORT_SECAPI_MAX_API_CALLS",
        "NPORT_SECAPI_REQUEST_INTERVAL_SECONDS",
    ):
        assert name in source
    assert "sec_set_current_derived_publication" not in source
    assert "_SIDECAR_SCHEMA.read_text" not in source
    assert "schema migration is not installed" in source
    assert "nport_fixed_income_secapi_scope_ready" in (
        ROOT / "docs" / "runbooks" / "fixed-income-publication-closure.md"
    ).read_text(encoding="utf-8")
    # ``python -m src.run`` always passes these generic dispatcher keywords.
    assert {"calc_date", "limit"} <= set(inspect.signature(worker.run).parameters)

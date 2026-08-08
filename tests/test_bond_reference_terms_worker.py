"""Railway dispatch contract for the direct Finnhub terms backfill."""
from __future__ import annotations

import inspect
import json
from datetime import date

import pytest

from scripts import backfill_bond_reference_terms as backfill


def test_worker_adapter_passes_the_resolved_dsn_and_marks_partial_results_aborted(
    monkeypatch,
) -> None:
    """A one-off must not go green after a typed provider-side partial result."""
    from src.workers import bond_reference_terms as worker

    client = object()
    calls = []
    monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
    monkeypatch.setattr(worker._finnhub, "client_from_env", lambda: client)
    monkeypatch.setattr(
        backfill,
        "run",
        lambda received_client, *, dsn, batch_label, limit, stale_after_days: calls.append(
            (received_client, dsn, batch_label, limit, stale_after_days)
        )
        or {
            "attempted": 1,
            "loaded": 0,
            "empty": 0,
            "mismatch": 1,
            "transient": 0,
            "config_error": 0,
        },
    )

    summary = worker.run("postgresql://market-clean-serial.railway.internal:5432/market", limit=7)

    assert calls == [
        (
            client,
            "postgresql://market-clean-serial.railway.internal:5432/market",
            date.today().isoformat(),
            7,
            worker.DEFAULT_STALE_AFTER_DAYS,
        )
    ]
    assert summary["mismatch"] == 1
    assert summary["aborted"] == "bond_reference_terms_incomplete"


@pytest.mark.parametrize("api_key", [None, "   "])
def test_missing_finnhub_key_is_a_typed_nonzero_json_worker_result(
    monkeypatch, capsys, api_key
) -> None:
    """Absent or whitespace-only credentials must fail before any DB write."""
    import src.run_worker as dispatcher

    monkeypatch.setenv("WORKER", "bond_reference_terms")
    if api_key is None:
        monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    else:
        monkeypatch.setenv("FINNHUB_API_KEY", api_key)
    monkeypatch.delenv("WORKER_LIMIT", raising=False)
    monkeypatch.delenv("WORKER_CALC_DATE", raising=False)
    monkeypatch.setattr(dispatcher, "resolve_dsn", lambda: "postgresql://private-market")

    try:
        dispatcher.main()
        code = 0
    except SystemExit as exc:
        code = exc.code

    result = json.loads(capsys.readouterr().out)
    assert code != 0
    assert result["worker"] == "bond_reference_terms"
    assert result["config_error"] == 1
    assert result["reason_counts"] == {"config_error": 1}
    assert result["aborted"] == "bond_reference_terms_config_error"


def test_adapter_is_reachable_by_normal_worker_limit_dispatch(monkeypatch, capsys) -> None:
    """Railway's shared start command passes WORKER_LIMIT to this resumable batch."""
    import src.run_worker as dispatcher
    from src.workers import bond_reference_terms as worker

    calls = []
    monkeypatch.setenv("WORKER", "bond_reference_terms")
    monkeypatch.setenv("WORKER_LIMIT", "11")
    monkeypatch.delenv("WORKER_CALC_DATE", raising=False)
    monkeypatch.setattr(dispatcher, "resolve_dsn", lambda: "postgresql://private-market")
    monkeypatch.setattr(
        worker,
        "run",
        lambda dsn, *, limit=worker.DEFAULT_LIMIT: calls.append((dsn, limit))
        or {"attempted": 0, "loaded": 0},
    )

    dispatcher.main()

    assert calls == [("postgresql://private-market", 11)]
    assert json.loads(capsys.readouterr().out) == {
        "worker": "bond_reference_terms", "attempted": 0, "loaded": 0
    }
    assert "limit" in inspect.signature(worker.run).parameters

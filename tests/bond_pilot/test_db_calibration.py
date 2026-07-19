from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.bond_pilot.contracts import PilotError
from src.bond_pilot.db_calibration import (
    REQUIRED_COLUMNS,
    V2_RELATION,
    budget_for,
    run_v2_calibration,
)


def _evidence(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "phase4_status": "completed",
        "reconciled": True,
        "v2_ready": True,
        "seam": "nport-v2-current",
        "relation": V2_RELATION,
        "required_columns": list(REQUIRED_COLUMNS),
    }
    value.update(changes)
    return value


class _Transaction:
    def __init__(self, connection: "TranscriptConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "_Transaction":
        self.connection.calls.append(("transaction_enter",))
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.connection.calls.append(("transaction_exit", exc_type.__name__ if exc_type else None))


class _Cursor:
    def __init__(self, connection: "TranscriptConnection") -> None:
        self.connection = connection
        self.result: object = None

    def __enter__(self) -> "_Cursor":
        self.connection.calls.append(("cursor_enter",))
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.connection.calls.append(("cursor_exit", exc_type.__name__ if exc_type else None))

    def execute(self, sql: str, params: object = None) -> None:
        self.connection.assert_read_only()
        expected_sql, expected_params, self.result = self.connection.expected.pop(0)
        assert sql == expected_sql
        assert params == expected_params
        self.connection.calls.append(("execute", sql, params))

    def fetchone(self) -> object:
        return self.result

    def fetchall(self) -> object:
        return self.result


class TranscriptConnection:
    def __init__(self, expected: list[tuple[str, object, object]]) -> None:
        self.expected = list(expected)
        self.read_only = False
        self.calls: list[tuple[object, ...]] = []

    def assert_read_only(self) -> None:
        assert self.read_only is True
        self.calls.append(("assert_read_only",))

    def transaction(self) -> _Transaction:
        self.assert_read_only()
        self.calls.append(("transaction",))
        return _Transaction(self)

    def cursor(self) -> _Cursor:
        self.assert_read_only()
        self.calls.append(("cursor",))
        return _Cursor(self)

    def assert_exhausted(self) -> None:
        assert self.expected == []


def _safe_plan() -> list[dict[str, object]]:
    return [{"Plan": {"Node Type": "Index Scan", "Index Cond": "series_id = ANY ($1) AND report_date = ANY ($2) AND publication_id = ANY ($3)"}}]


def test_evidence_missing_or_unreconciled_stops_before_connection(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text('{"prior":true}\n', encoding="utf-8")
    for evidence in ({}, _evidence(phase4_status="in_progress"), _evidence(reconciled=False), _evidence(v2_ready="unknown")):
        with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
            run_v2_calibration(None, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint)
    assert json.loads(checkpoint.read_text(encoding="utf-8")) == {"prior": True}


@pytest.mark.parametrize("change", [
    {"seam": "unknown"},
    {"relation": "public.sec_nport_holdings"},
    {"required_columns": ["publication_id"]},
])
def test_evidence_rejects_unknown_legacy_or_column_mismatch(change: dict[str, object]) -> None:
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_v2_calibration(None, evidence=_evidence(**change), series_ids=("S1",), mode="calibration", checkpoint_path=Path("checkpoint.json"))


def test_budget_contract_is_exact_and_full_requires_separate_authorization() -> None:
    calibration = budget_for("calibration")
    bounded = budget_for("first_bounded")
    assert (calibration.page_size, calibration.max_pages, calibration.max_rows, calibration.wall_seconds) == (1000, 5, 5000, 600)
    assert (bounded.page_size, bounded.max_pages, bounded.max_rows, bounded.wall_seconds) == (2500, 20, 50000, 1800)
    with pytest.raises(PilotError, match="run_budget_required"):
        budget_for("full")
    for series_ids in ((), tuple(f"S{index}" for index in range(6))):
        with pytest.raises(PilotError, match="run_budget_required"):
            run_v2_calibration(None, evidence=_evidence(), series_ids=series_ids, mode="full", checkpoint_path=Path("checkpoint.json"))


def test_keyset_select_qualifies_holding_columns_after_resolved_report_join() -> None:
    from src.bond_pilot import db_calibration as calibration

    assert "SELECT holdings.publication_id" in calibration.PAGE_SQL
    assert "SELECT holdings.publication_id" in calibration.EXPLAIN_SQL


def test_absent_to_regclass_stops_before_schema_or_data_query(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    connection = TranscriptConnection([
        (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None),
        (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, ("on",)),
        (calibration.RELATION_SQL, (V2_RELATION,), (None,)),
    ])
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_v2_calibration(connection, evidence=_evidence(), series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json")
    connection.assert_exhausted()


def test_read_only_transcript_resolves_reports_explains_then_keyset_pages(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    report_rows = [("S1", "2024-03-31", "pub-1", "acc-1")]
    first_page = [
        {"publication_id": "pub-1", "accession_number": "acc-1", "holding_id": "h-1", "series_id": "S1"},
        {"publication_id": "pub-1", "accession_number": "acc-1", "holding_id": "h-2", "series_id": "S1"},
    ]
    connection = TranscriptConnection([
        (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None),
        (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, ("on",)),
        (calibration.RELATION_SQL, (V2_RELATION,), (V2_RELATION,)),
        (calibration.COLUMNS_SQL, None, [(column,) for column in REQUIRED_COLUMNS]),
        (calibration.EXPLAIN_SQL, (("S1",), None, None, None, 1000), _safe_plan()),
        (calibration.LATEST_REPORTS_SQL, (("S1",),), report_rows),
        (calibration.PAGE_SQL, (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), None, None, None, 1000), first_page),
        (calibration.PAGE_SQL, (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), "pub-1", "acc-1", "h-2", 1000), []),
    ])
    checkpoint = tmp_path / "checkpoint.json"
    result = run_v2_calibration(connection, evidence=_evidence(), series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint)
    assert result.rows == tuple(first_page)
    assert result.pages == 1
    assert result.partial is False
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["last_key"] == ["pub-1", "acc-1", "h-2"]
    assert connection.read_only is True
    connection.assert_exhausted()
    assert connection.calls.index(("transaction",)) < connection.calls.index(("transaction_enter",))


@pytest.mark.parametrize("plan", [
    [{"Plan": {"Node Type": "Seq Scan", "Filter": "series_id = ANY ($1) AND report_date = ANY ($2)"}}],
    [{"Plan": {"Node Type": "Index Scan", "Index Cond": "series_id = ANY ($1)"}}],
])
def test_unsafe_explain_stops_before_report_resolution(plan: list[dict[str, object]], tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    connection = TranscriptConnection([
        (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None),
        (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, ("on",)),
        (calibration.RELATION_SQL, (V2_RELATION,), (V2_RELATION,)),
        (calibration.COLUMNS_SQL, None, [(column,) for column in REQUIRED_COLUMNS]),
        (calibration.EXPLAIN_SQL, (("S1",), None, None, None, 1000), plan),
    ])
    with pytest.raises(PilotError, match="unsafe_query_plan"):
        run_v2_calibration(connection, evidence=_evidence(), series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json")
    connection.assert_exhausted()


def test_checkpoint_is_replaced_per_page_and_retained_on_nondeterministic_stop(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    connection = TranscriptConnection([
        (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None),
        (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, ("on",)),
        (calibration.RELATION_SQL, (V2_RELATION,), (V2_RELATION,)),
        (calibration.COLUMNS_SQL, None, [(column,) for column in REQUIRED_COLUMNS]),
        (calibration.EXPLAIN_SQL, (("S1",), None, None, None, 1000), _safe_plan()),
        (calibration.LATEST_REPORTS_SQL, (("S1",),), [("S1", "2024-03-31", "pub-1", "acc-1")]),
        (calibration.PAGE_SQL, (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), None, None, None, 1000), [{"publication_id": "pub-1", "accession_number": "acc-1", "holding_id": "h-2"}]),
        (calibration.PAGE_SQL, (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), "pub-1", "acc-1", "h-2", 1000), [{"publication_id": "pub-1", "accession_number": "acc-1", "holding_id": "h-2"}]),
    ])
    checkpoint = tmp_path / "checkpoint.json"
    with pytest.raises(PilotError, match="nondeterministic_page"):
        run_v2_calibration(connection, evidence=_evidence(), series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint)
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["last_key"] == ["pub-1", "acc-1", "h-2"]
    connection.assert_exhausted()

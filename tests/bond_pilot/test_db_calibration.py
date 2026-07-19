from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from src.bond_pilot.artifacts import canonical_json_bytes
from src.bond_pilot.contracts import PilotError
from src.bond_pilot.db_calibration import (
    FULL_KEY_COLUMNS,
    INITIAL_PAGE_SQL,
    REQUIRED_COLUMNS,
    RESUME_PAGE_SQL,
    V2_RELATION,
    budget_for,
    load_phase4_v2_evidence,
    run_v2_calibration,
)


def _sha(char: str) -> str:
    return char * 64


def _payload(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": "phase4b-v2-evidence-v1", "phase4_status": "completed", "reconciled": True,
        "v2_published": True, "seam": "nport-v2-current", "relation": V2_RELATION,
        "required_columns": list(REQUIRED_COLUMNS), "phase4_run_sha256": _sha("a"),
        "reconciliation_sha256": _sha("b"), "publication_sha256": _sha("c"), "schema_sha256": _sha("d"),
        "approved_series": ["S1"], "lineage_attestation_sha256": _sha("e"),
        "approved_by": "internal-approver", "approved_at": "2026-07-19T12:00:00Z",
    }
    value.update(changes)
    return value


def _evidence(tmp_path: Path, **changes: object):
    path = tmp_path / "evidence.json"
    path.write_bytes(canonical_json_bytes(_payload(**changes)))
    return load_phase4_v2_evidence(path)


class _Transaction:
    def __init__(self, connection: "TranscriptConnection") -> None:
        self.connection = connection

    def __enter__(self):
        self.connection.calls.append(("transaction_enter",))
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.connection.calls.append(("transaction_exit", exc_type.__name__ if exc_type else None))


class _Cursor:
    def __init__(self, connection: "TranscriptConnection") -> None:
        self.connection = connection
        self.result: object = None

    def __enter__(self):
        self.connection.calls.append(("cursor_enter",))
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.connection.calls.append(("cursor_exit", exc_type.__name__ if exc_type else None))

    def execute(self, sql: str, params: object = None) -> None:
        self.connection.assert_read_only()
        expected_sql, expected_params, self.result = self.connection.expected.pop(0)
        assert sql == expected_sql
        assert params == expected_params
        assert sql.count("%s") == (len(params) if params is not None else 0)
        self.connection.calls.append(("execute", sql, params))
        if isinstance(self.result, BaseException):
            raise self.result

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result


class TranscriptConnection:
    def __init__(self, expected: list[tuple[str, object, object]]) -> None:
        self.expected = list(expected)
        self.read_only = False
        self.calls: list[tuple[object, ...]] = []

    def assert_read_only(self) -> None:
        assert self.read_only is True
        self.calls.append(("assert_read_only",))

    def transaction(self):
        self.assert_read_only()
        self.calls.append(("transaction",))
        return _Transaction(self)

    def cursor(self):
        self.assert_read_only()
        self.calls.append(("cursor",))
        return _Cursor(self)

    def assert_exhausted(self) -> None:
        assert self.expected == []


def _safe_plan(*, resume: bool = False, target: bool = True) -> list[dict[str, object]]:
    predicate = "series_id = ANY ($1) AND report_date = ANY ($2) AND publication_id = ANY ($3) AND accession_number = ANY ($4)"
    if resume:
        predicate += " AND (series_id, report_date, publication_id, accession_number, holding_id, source_run_id, instrument_id) > ($5, $6, $7, $8, $9, $10, $11)"
    return [{"Plan": {"Node Type": "Index Scan", "Relation Name": "sec_nport_holdings_v2_current" if target else "other_relation", "Schema": "public", "Plan Rows": 1000, "Index Cond": predicate}}]


def _setup_calls(calibration, *, evidence, reports: object, plan: object, page_sql: str, page_params: tuple[object, ...], page_rows: object) -> list[tuple[str, object, object]]:
    return [
        (calibration.SET_REPEATABLE_READ_ONLY_SQL, None, None),
        (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None),
        (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, ("on",)),
        (calibration.RELATION_SQL, (V2_RELATION,), (V2_RELATION,)),
        (calibration.COLUMNS_SQL, None, [(column,) for column in REQUIRED_COLUMNS]),
        (calibration.LATEST_REPORTS_SQL, (("S1",),), reports),
        (calibration.EXPLAIN_INITIAL_SQL if page_sql == INITIAL_PAGE_SQL else calibration.EXPLAIN_RESUME_SQL, page_params, plan),
        (page_sql, page_params, page_rows),
    ]


def test_strict_evidence_loader_rejects_hand_built_mapping_current_state_tampering_and_substitution(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    assert evidence.artifact_sha256 == hashlib.sha256((tmp_path / "evidence.json").read_bytes()).hexdigest()
    for payload in ({}, _payload(phase4_status="in_progress"), _payload(v2_published=False), _payload(relation="public.sec_nport_holdings")):
        with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
            run_v2_calibration(None, evidence=payload, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json")
    (tmp_path / "evidence.json").write_bytes(canonical_json_bytes(_payload(approved_by="substituted")))
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_v2_calibration(None, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json")
    bad = tmp_path / "bad.json"
    bad.write_text('{"schema_version":"phase4b-v2-evidence-v1","schema_version":"phase4b-v2-evidence-v1"}', encoding="utf-8")
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        load_phase4_v2_evidence(bad)


def test_exact_budgets_and_series_allowlist_fail_before_connection(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    assert (budget_for("calibration").page_size, budget_for("calibration").max_pages, budget_for("calibration").max_rows, budget_for("calibration").wall_seconds) == (1000, 5, 5000, 600)
    assert (budget_for("first_bounded").page_size, budget_for("first_bounded").max_pages, budget_for("first_bounded").max_rows, budget_for("first_bounded").wall_seconds) == (2500, 20, 50000, 1800)
    for mode, series in (("full", ("S1",)), ("calibration", ("S2",)), ("calibration", ("S1", "S1")), ("calibration", tuple(f"S{index}" for index in range(6)))):
        with pytest.raises(PilotError, match="run_budget_required"):
            run_v2_calibration(None, evidence=evidence, series_ids=series, mode=mode, checkpoint_path=tmp_path / "checkpoint.json")


@pytest.mark.parametrize("payload", [_payload(unexpected=True), {"schema_version": "phase4b-v2-evidence-v1"}, _payload(phase4_run_sha256="not-a-hash")])
def test_evidence_loader_rejects_unknown_missing_and_unpinned_fields(payload: dict[str, object], tmp_path: Path) -> None:
    path = tmp_path / "evidence.json"
    path.write_bytes(canonical_json_bytes(payload))
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        load_phase4_v2_evidence(path)


def test_initial_and_resume_templates_have_distinct_arity_and_complete_key() -> None:
    assert INITIAL_PAGE_SQL.count("%s") == 5
    assert RESUME_PAGE_SQL.count("%s") == 12
    assert " OR " not in INITIAL_PAGE_SQL
    assert all(column in RESUME_PAGE_SQL for column in FULL_KEY_COLUMNS)


def test_initial_transcript_resolves_then_explains_exact_page_and_pages_with_full_key(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence = _evidence(tmp_path)
    reports = [{"accession_number": "acc-1", "publication_id": "pub-1", "report_date": "2024-03-31", "series_id": "S1"}]
    params = (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), 1000)
    row = {column: "x" for column in REQUIRED_COLUMNS}
    row.update({"series_id": "S1", "report_date": "2024-03-31", "filing_date": "2024-04-15", "publication_id": "pub-1", "accession_number": "acc-1", "holding_id": "h-1", "source_run_id": "run-1", "instrument_id": "instrument-1"})
    connection = TranscriptConnection(_setup_calls(calibration, evidence=evidence, reports=reports, plan=_safe_plan(), page_sql=INITIAL_PAGE_SQL, page_params=params, page_rows=[row]))
    checkpoint = tmp_path / "checkpoint.json"
    result = run_v2_calibration(connection, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")
    assert result.pages == result.rows_read == 1
    saved = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert saved["last_key"] == [row[column] for column in FULL_KEY_COLUMNS]
    assert saved["resolved_reports"] == [{"series_id": "S1", "report_date": "2024-03-31", "publication_id": "pub-1", "accession_number": "acc-1"}]
    assert connection.read_only is True
    connection.assert_exhausted()


def test_complete_key_preserves_old_triple_ties_across_source_runs(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence = _evidence(tmp_path)
    reports = [("S1", "2024-03-31", "pub-1", "acc-1")]
    params = (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), 1000)
    rows = []
    for source_run_id, instrument_id in (("run-1", "instrument-1"), ("run-2", "instrument-2")):
        row = {column: "x" for column in REQUIRED_COLUMNS}
        row.update({"series_id": "S1", "report_date": "2024-03-31", "filing_date": "2024-04-15", "publication_id": "pub-1", "accession_number": "acc-1", "holding_id": "same-holding", "source_run_id": source_run_id, "instrument_id": instrument_id})
        rows.append(row)
    connection = TranscriptConnection(_setup_calls(calibration, evidence=evidence, reports=reports, plan=_safe_plan(), page_sql=INITIAL_PAGE_SQL, page_params=params, page_rows=rows))
    result = run_v2_calibration(connection, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json", run_id="run-1")
    assert result.rows_read == 2
    assert result.last_key == tuple(rows[-1][column] for column in FULL_KEY_COLUMNS)
    connection.assert_exhausted()


@pytest.mark.parametrize("plan", [_safe_plan(target=False), [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "sec_nport_holdings_v2_current", "Schema": "public", "Plan Rows": 1, "Filter": "series_id AND report_date AND publication_id AND accession_number"}}], [{"Plan": {"Node Type": "Index Scan", "Relation Name": "sec_nport_holdings_v2_current", "Schema": "public", "Plan Rows": 1, "Index Cond": "series_id AND report_date"}}]])
def test_plan_requires_target_index_and_exact_lineage_predicates(plan: list[dict[str, object]], tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence = _evidence(tmp_path)
    reports = [("S1", "2024-03-31", "pub-1", "acc-1")]
    params = (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), 1000)
    connection = TranscriptConnection(_setup_calls(calibration, evidence=evidence, reports=reports, plan=plan, page_sql=INITIAL_PAGE_SQL, page_params=params, page_rows=[] )[:-1])
    with pytest.raises(PilotError, match="unsafe_query_plan"):
        run_v2_calibration(connection, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json", run_id="run-1")
    connection.assert_exhausted()


def test_mapping_decoding_uses_names_and_rejects_incomplete_or_extra_page_rows(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence = _evidence(tmp_path)
    reports = [{"publication_id": "pub-1", "series_id": "S1", "accession_number": "acc-1", "report_date": "2024-03-31"}]
    params = (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), 1000)
    for page_row in ({column: "x" for column in REQUIRED_COLUMNS if column != "holding_id"}, {**{column: "x" for column in REQUIRED_COLUMNS}, "unexpected": "x"}):
        connection = TranscriptConnection(_setup_calls(calibration, evidence=evidence, reports=reports, plan=_safe_plan(), page_sql=INITIAL_PAGE_SQL, page_params=params, page_rows=[page_row]))
        with pytest.raises(PilotError, match="nondeterministic_page"):
            run_v2_calibration(connection, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json", run_id="run-1")
        connection.assert_exhausted()


def test_to_regclass_absence_stops_before_schema_or_data_read(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence = _evidence(tmp_path)
    connection = TranscriptConnection([
        (calibration.SET_REPEATABLE_READ_ONLY_SQL, None, None), (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None), (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, ("on",)), (calibration.RELATION_SQL, (V2_RELATION,), (None,)),
    ])
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_v2_calibration(connection, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json", run_id="run-1")
    connection.assert_exhausted()


def test_resume_checkpoint_is_prevalidated_cumulative_and_uses_resume_explain_and_page(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence = _evidence(tmp_path)
    reports = [("S1", "2024-03-31", "pub-1", "acc-1")]
    first_key = ("S1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1")
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_bytes(canonical_json_bytes(calibration._checkpoint_payload(run_id="run-1", evidence=evidence, mode="calibration", series_ids=("S1",), reports=reports, last_key=first_key, pages=1, rows=1, elapsed_seconds=1.0, output_hash=_sha("f"), output_state="in_progress")))
    with pytest.raises(PilotError, match="run_budget_required"):
        run_v2_calibration(None, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="other")
    resume_params = (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), *first_key, 1000)
    connection = TranscriptConnection(_setup_calls(calibration, evidence=evidence, reports=reports, plan=_safe_plan(resume=True), page_sql=RESUME_PAGE_SQL, page_params=resume_params, page_rows=[]))
    result = run_v2_calibration(connection, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")
    assert result.partial is False
    assert result.rows_read == 1
    connection.assert_exhausted()


def test_exhausted_cumulative_checkpoint_cannot_read_another_page(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence = _evidence(tmp_path)
    reports = [("S1", "2024-03-31", "pub-1", "acc-1")]
    last_key = ("S1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1")
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_bytes(canonical_json_bytes(calibration._checkpoint_payload(run_id="run-1", evidence=evidence, mode="calibration", series_ids=("S1",), reports=reports, last_key=last_key, pages=5, rows=5000, elapsed_seconds=1.0, output_hash=_sha("f"), output_state="in_progress")))
    connection = TranscriptConnection([
        (calibration.SET_REPEATABLE_READ_ONLY_SQL, None, None), (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None), (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, ("on",)), (calibration.RELATION_SQL, (V2_RELATION,), (V2_RELATION,)),
        (calibration.COLUMNS_SQL, None, [(column,) for column in REQUIRED_COLUMNS]), (calibration.LATEST_REPORTS_SQL, (("S1",),), reports),
    ])
    result = run_v2_calibration(connection, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")
    assert result.partial is True and result.rows_read == 5000
    connection.assert_exhausted()


def test_corrupt_checkpoint_schema_or_counters_fail_before_connection(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence = _evidence(tmp_path)
    checkpoint = tmp_path / "checkpoint.json"
    for change in ({"schema_version": "other"}, {"pages": 6, "rows": 5000}, {"pages": True, "rows": 1}):
        payload = calibration._checkpoint_payload(run_id="run-1", evidence=evidence, mode="calibration", series_ids=("S1",), reports=(), last_key=None, pages=0, rows=0, elapsed_seconds=0.0, output_hash=_sha("f"), output_state="new")
        payload.update(change)
        checkpoint.write_bytes(canonical_json_bytes(payload))
        with pytest.raises(PilotError, match="run_budget_required"):
            run_v2_calibration(None, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")
    checkpoint.write_text("{", encoding="utf-8")
    with pytest.raises(PilotError, match="run_budget_required"):
        run_v2_calibration(None, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")


def test_recognized_database_cancel_becomes_timeout_and_checkpoints(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    class QueryCanceled(Exception):
        sqlstate = "57014"

    evidence = _evidence(tmp_path)
    checkpoint = tmp_path / "checkpoint.json"
    connection = TranscriptConnection([
        (calibration.SET_REPEATABLE_READ_ONLY_SQL, None, None), (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None), (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, QueryCanceled("cancelled")),
    ])
    with pytest.raises(PilotError, match="calibration_timeout"):
        run_v2_calibration(connection, evidence=evidence, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["stop_reason"] == "calibration_timeout"
    connection.assert_exhausted()

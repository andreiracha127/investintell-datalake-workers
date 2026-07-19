from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path

import pytest

from src.bond_pilot.artifacts import canonical_json_bytes
from src.bond_pilot.contracts import PilotError
from src.bond_pilot.db_calibration import (
    EXPLAIN_INITIAL_SQL,
    EXPLAIN_RESOLVER_SQL,
    EXPLAIN_RESUME_SQL,
    INITIAL_PAGE_SQL,
    REQUIRED_COLUMNS,
    RESOLVER_SQL,
    RESUME_PAGE_SQL,
    V2_RELATION,
    load_phase4_v2_evidence,
    load_phase4_v2_evidence_approval,
    run_v2_calibration,
)


def _sha(char: str) -> str:
    return char * 64


def _evidence_payload(**changes: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "phase4b-v2-evidence-v1", "phase4_status": "completed", "reconciled": True,
        "v2_published": True, "seam": "nport-v2-current", "relation": V2_RELATION,
        "required_columns": list(REQUIRED_COLUMNS), "phase4_run_sha256": _sha("a"),
        "reconciliation_sha256": _sha("b"), "publication_sha256": _sha("c"), "schema_sha256": _sha("d"),
        "approved_series": ["S1"], "lineage_attestation_sha256": _sha("e"),
        "approved_by": "human-approver", "approved_at": "2026-07-19T12:00:00Z",
    }
    payload.update(changes)
    return payload


def _governance(tmp_path: Path):
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_bytes(canonical_json_bytes(_evidence_payload()))
    evidence_bytes = evidence_path.read_bytes()
    approval = {
        "schema_version": "phase4b-v2-evidence-approval-v1", "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
        "phase4_run_sha256": _sha("a"), "reconciliation_sha256": _sha("b"), "publication_sha256": _sha("c"),
        "schema_sha256": _sha("d"), "lineage_attestation_sha256": _sha("e"), "seam": "nport-v2-current",
        "relation": V2_RELATION, "approved_series": ["S1"], "approved_modes": ["calibration", "first_bounded"],
        "allow_read": True, "approved_by": "human-approver", "approved_at": "2026-07-19T12:01:00Z",
    }
    approval_path = tmp_path / "approval.json"
    approval_path.write_bytes(canonical_json_bytes(approval))
    return load_phase4_v2_evidence(evidence_path), load_phase4_v2_evidence_approval(approval_path), evidence_path, approval_path


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


def _plan(*, resolver: bool = False, resume: bool = False, target: bool = True) -> list[dict[str, object]]:
    predicate = "series_id = ANY ($1) AND report_date = ANY ($2) AND publication_id = ANY ($3) AND accession_number = ANY ($4)"
    if resolver:
        predicate = "series_id = requested.series_id"
    if resume:
        predicate += " AND (series_id, report_date, publication_id, accession_number, holding_id, source_run_id, instrument_id) > ($5, $6, $7, $8, $9, $10, $11)"
    return [{"Plan": {"Node Type": "Index Scan", "Relation Name": "sec_nport_holdings_v2_current" if target else "unrelated", "Schema": "public", "Plan Rows": 1, "Index Cond": predicate, "Sort Key": ["report_date DESC", "publication_id DESC", "accession_number DESC"]}}]


def _row(*, holding_id: str, source_run_id: str = "run-1", instrument_id: str = "instrument-1") -> dict[str, object]:
    row = {column: "x" for column in REQUIRED_COLUMNS}
    row.update({"series_id": "S1", "report_date": date(2024, 3, 31), "filing_date": date(2024, 4, 15), "publication_id": "pub-1", "accession_number": "acc-1", "holding_id": holding_id, "source_run_id": source_run_id, "instrument_id": instrument_id})
    return row


def _prefix(calibration, reports: object) -> list[tuple[str, object, object]]:
    return [
        (calibration.SET_REPEATABLE_READ_ONLY_SQL, None, None), (calibration.SET_STATEMENT_TIMEOUT_SQL, None, None),
        (calibration.SET_LOCK_TIMEOUT_SQL, None, None), (calibration.SET_IDLE_TIMEOUT_SQL, None, None),
        (calibration.SHOW_READ_ONLY_SQL, None, ("on",)), (calibration.RELATION_SQL, (V2_RELATION,), (V2_RELATION,)),
        (calibration.COLUMNS_SQL, None, [(column,) for column in REQUIRED_COLUMNS]),
        (EXPLAIN_RESOLVER_SQL, (("S1",),), _plan(resolver=True)), (RESOLVER_SQL, (("S1",),), reports),
    ]


def test_governance_pair_rejects_self_built_cross_file_mismatch_and_same_size_tamper(tmp_path: Path) -> None:
    evidence, approval, evidence_path, approval_path = _governance(tmp_path)
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_v2_calibration(None, evidence={}, approval=approval, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json")
    mismatched = json.loads(approval_path.read_text(encoding="utf-8"))
    mismatched["publication_sha256"] = _sha("f")
    approval_path.write_bytes(canonical_json_bytes(mismatched))
    bad_approval = load_phase4_v2_evidence_approval(approval_path)
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_v2_calibration(None, evidence=evidence, approval=bad_approval, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json")
    _, approval, evidence_path, _ = _governance(tmp_path)
    original = evidence_path.stat()
    tampered = canonical_json_bytes(_evidence_payload(approved_by="other-approver"))
    assert len(tampered) == original.st_size
    evidence_path.write_bytes(tampered)
    os.utime(evidence_path, ns=(original.st_atime_ns, original.st_mtime_ns))
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_v2_calibration(None, evidence=evidence, approval=approval, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json")


def test_resolver_is_static_lateral_and_plan_must_target_v2_with_series_order(tmp_path: Path) -> None:
    assert "LATERAL" in RESOLVER_SQL and "DISTINCT" not in RESOLVER_SQL and RESOLVER_SQL.count("%s") == 1
    evidence, approval, _, _ = _governance(tmp_path)
    from src.bond_pilot import db_calibration as calibration

    connection = TranscriptConnection(_prefix(calibration, [])[:7] + [(EXPLAIN_RESOLVER_SQL, (("S1",),), _plan(resolver=True, target=False))])
    with pytest.raises(PilotError, match="unsafe_query_plan"):
        run_v2_calibration(connection, evidence=evidence, approval=approval, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json")
    connection.assert_exhausted()


def test_two_pages_run_in_one_snapshot_and_short_page_proves_completion(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence, approval, _, _ = _governance(tmp_path)
    reports = [{"accession_number": "acc-1", "report_date": date(2024, 3, 31), "series_id": "S1", "publication_id": "pub-1"}]
    initial = (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), 1000)
    first = [_row(holding_id=f"h-{index:04}") for index in range(1000)]
    key = tuple(["S1", "2024-03-31", "pub-1", "acc-1", "h-0999", "run-1", "instrument-1"])
    resume = (("S1",), ("2024-03-31",), ("pub-1",), ("acc-1",), *key, 1000)
    expected = _prefix(calibration, reports) + [
        (EXPLAIN_INITIAL_SQL, initial, _plan()), (INITIAL_PAGE_SQL, initial, first),
        (EXPLAIN_RESUME_SQL, resume, _plan(resume=True)), (RESUME_PAGE_SQL, resume, [_row(holding_id="h-1000")]),
    ]
    connection = TranscriptConnection(expected)
    result = run_v2_calibration(connection, evidence=evidence, approval=approval, series_ids=("S1",), mode="calibration", checkpoint_path=tmp_path / "checkpoint.json", run_id="run-1")
    assert result.pages == 2 and result.rows_read == 1001
    assert result.partial is False
    assert connection.calls.count(("transaction",)) == connection.calls.count(("transaction_enter",)) == 1
    connection.assert_exhausted()


def test_budget_reached_checkpoint_is_partial_and_does_not_reopen(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence, approval, _, _ = _governance(tmp_path)
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_bytes(canonical_json_bytes(calibration._checkpoint_payload(run_id="run-1", evidence=evidence, approval=approval, mode="calibration", series_ids=("S1",), reports=[("S1", "2024-03-31", "pub-1", "acc-1")], last_key=("S1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1"), pages=5, rows=5000, elapsed_seconds=1.0, output_hash=_sha("f"), output_state="budget_reached", stop_reason="budget_reached")))
    again = run_v2_calibration(None, evidence=evidence, approval=approval, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")
    assert again.partial is True and again.rows_read == 5000


def test_complete_checkpoint_returns_without_connection_and_date_rows_are_canonical(tmp_path: Path) -> None:
    from src.bond_pilot import db_calibration as calibration

    evidence, approval, _, _ = _governance(tmp_path)
    reports = [("S1", "2024-03-31", "pub-1", "acc-1")]
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_bytes(canonical_json_bytes(calibration._checkpoint_payload(run_id="run-1", evidence=evidence, approval=approval, mode="calibration", series_ids=("S1",), reports=reports, last_key=("S1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1"), pages=1, rows=1, elapsed_seconds=1.0, output_hash=_sha("f"), output_state="complete", stop_reason=None)))
    result = run_v2_calibration(None, evidence=evidence, approval=approval, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")
    assert result.partial is False and result.rows_read == 1


def test_impossible_checkpoint_and_datetime_values_fail_pre_connection(tmp_path: Path) -> None:
    evidence, approval, _, _ = _governance(tmp_path)
    from src.bond_pilot import db_calibration as calibration

    checkpoint = tmp_path / "checkpoint.json"
    impossible = calibration._checkpoint_payload(run_id="run-1", evidence=evidence, approval=approval, mode="calibration", series_ids=("S1",), reports=(), last_key=None, pages=1, rows=0, elapsed_seconds=0.0, output_hash=_sha("f"), output_state="in_progress", stop_reason=None)
    checkpoint.write_bytes(canonical_json_bytes(impossible))
    with pytest.raises(PilotError, match="run_budget_required"):
        run_v2_calibration(None, evidence=evidence, approval=approval, series_ids=("S1",), mode="calibration", checkpoint_path=checkpoint, run_id="run-1")

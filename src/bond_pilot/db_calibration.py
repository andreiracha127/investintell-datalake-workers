"""Fail-closed, read-only calibration against one reviewed V2 N-PORT seam."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import stat
from time import monotonic
from typing import Callable, Mapping, Sequence

from .artifacts import canonical_json_bytes, replace_checkpoint
from .contracts import PilotError


V2_RELATION = "public.sec_nport_holdings_v2_current"
SEAM_NAME = "nport-v2-current"
REQUIRED_COLUMNS = (
    "publication_id", "accession_number", "holding_id", "source_run_id", "report_date", "filing_date",
    "series_id", "class_id", "instrument_id", "issuer_category", "cusip", "signed_market_value",
    "signed_pct_of_nav", "currency",
)
FULL_KEY_COLUMNS = (
    "series_id", "report_date", "publication_id", "accession_number", "holding_id", "source_run_id", "instrument_id",
)
_REPORT_COLUMNS = ("series_id", "report_date", "publication_id", "accession_number")
_EVIDENCE_KEYS = frozenset({
    "schema_version", "phase4_status", "reconciled", "v2_published", "seam", "relation", "required_columns",
    "phase4_run_sha256", "reconciliation_sha256", "publication_sha256", "schema_sha256", "approved_series",
    "lineage_attestation_sha256", "approved_by", "approved_at",
})
_EVIDENCE_MAX_BYTES = 1024 * 1024
_EVIDENCE_TOKEN = object()

SET_REPEATABLE_READ_ONLY_SQL = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
SET_STATEMENT_TIMEOUT_SQL = "SET LOCAL statement_timeout = '20s'"
SET_LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '2s'"
SET_IDLE_TIMEOUT_SQL = "SET LOCAL idle_in_transaction_session_timeout = '60s'"
SHOW_READ_ONLY_SQL = "SHOW transaction_read_only"
RELATION_SQL = "SELECT to_regclass(%s)"
COLUMNS_SQL = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sec_nport_holdings_v2_current' ORDER BY ordinal_position"
LATEST_REPORTS_SQL = "SELECT DISTINCT ON (series_id) series_id, report_date, publication_id, accession_number FROM public.sec_nport_holdings_v2_current WHERE series_id = ANY(%s) ORDER BY series_id, report_date DESC, publication_id DESC, accession_number DESC"
_HOLDINGS_SELECT = ", ".join(f"holdings.{column}" for column in REQUIRED_COLUMNS)
_SELECTED_JOIN = "JOIN unnest(%s::text[], %s::date[], %s::text[], %s::text[]) AS selected(series_id, report_date, publication_id, accession_number) ON (holdings.series_id = selected.series_id AND holdings.report_date = selected.report_date AND holdings.publication_id = selected.publication_id AND holdings.accession_number = selected.accession_number)"
_ORDER_BY = ", ".join(f"holdings.{column}" for column in FULL_KEY_COLUMNS)
INITIAL_PAGE_SQL = f"SELECT {_HOLDINGS_SELECT} FROM {V2_RELATION} AS holdings {_SELECTED_JOIN} ORDER BY {_ORDER_BY} LIMIT %s"
RESUME_PAGE_SQL = f"SELECT {_HOLDINGS_SELECT} FROM {V2_RELATION} AS holdings {_SELECTED_JOIN} WHERE ({_ORDER_BY}) > ({', '.join('%s' for _ in FULL_KEY_COLUMNS)}) ORDER BY {_ORDER_BY} LIMIT %s"
EXPLAIN_INITIAL_SQL = f"EXPLAIN (FORMAT JSON) {INITIAL_PAGE_SQL}"
EXPLAIN_RESUME_SQL = f"EXPLAIN (FORMAT JSON) {RESUME_PAGE_SQL}"

_QUERY_VERSION = "nport-v2-keyset-v2"
_METHOD_VERSION = "bond-pilot-calibration-v2"
_QUERY_SHA256 = hashlib.sha256((INITIAL_PAGE_SQL + "\n" + RESUME_PAGE_SQL).encode("utf-8")).hexdigest()
_METHOD_SHA256 = hashlib.sha256(_METHOD_VERSION.encode("utf-8")).hexdigest()
_CHECKPOINT_KEYS = frozenset({
    "schema_version", "run_id", "evidence_sha256", "mode", "series_ids", "seam", "relation", "query_version",
    "query_sha256", "method_version", "method_sha256", "resolved_reports", "last_key", "pages", "rows",
    "elapsed_seconds", "output_hash", "output_state", "stop_reason",
})


@dataclass(frozen=True, init=False)
class Phase4V2Evidence:
    artifact_sha256: str
    approved_series: tuple[str, ...]
    source_path: Path
    source_identity: tuple[int, int, int, int]
    _provenance_token: object

    def __init__(self, *, token: object, artifact_sha256: str, approved_series: tuple[str, ...], source_path: Path, source_identity: tuple[int, int, int, int]) -> None:
        if token is not _EVIDENCE_TOKEN:
            raise PilotError("phase4b_v2_unavailable")
        object.__setattr__(self, "artifact_sha256", artifact_sha256)
        object.__setattr__(self, "approved_series", approved_series)
        object.__setattr__(self, "source_path", source_path)
        object.__setattr__(self, "source_identity", source_identity)
        object.__setattr__(self, "_provenance_token", token)

    def verify_unchanged(self) -> None:
        try:
            current = os.stat(self.source_path, follow_symlinks=False)
        except OSError as exc:
            raise PilotError("phase4b_v2_unavailable") from exc
        identity = (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
        if not stat.S_ISREG(current.st_mode) or identity != self.source_identity:
            raise PilotError("phase4b_v2_unavailable")


@dataclass(frozen=True)
class CalibrationBudget:
    mode: str
    page_size: int
    max_pages: int
    max_rows: int
    wall_seconds: int
    statement_timeout_seconds: int
    lock_timeout_seconds: int = 2
    idle_transaction_timeout_seconds: int = 60
    max_series: int = 5


@dataclass(frozen=True)
class CalibrationResult:
    rows: tuple[dict[str, object], ...]
    pages: int
    rows_read: int
    partial: bool
    last_key: tuple[str, ...] | None
    mode: str


_BUDGETS = {
    "calibration": CalibrationBudget("calibration", 1000, 5, 5000, 600, 20),
    "first_bounded": CalibrationBudget("first_bounded", 2500, 20, 50000, 1800, 30),
}


def _reject_duplicate(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def _reject_constant(value: str) -> object:
    raise ValueError(f"non-finite {value}")


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _load_json_file(path: str | Path, limit: int) -> tuple[dict[str, object], bytes, Path, tuple[int, int, int, int]]:
    source_path = Path(path)
    try:
        before = os.stat(source_path, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise OSError("not regular")
        with source_path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise OSError("file changed")
            raw = handle.read(limit + 1)
    except OSError as exc:
        raise PilotError("phase4b_v2_unavailable") from exc
    if len(raw) > limit:
        raise PilotError("phase4b_v2_unavailable")
    try:
        parsed = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate, parse_constant=_reject_constant)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise PilotError("phase4b_v2_unavailable") from exc
    if not isinstance(parsed, dict):
        raise PilotError("phase4b_v2_unavailable")
    return parsed, raw, source_path, (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)


def load_phase4_v2_evidence(path: str | Path) -> Phase4V2Evidence:
    """Load one bounded immutable evidence artifact; no mapping is accepted at runtime."""
    value, raw, source_path, identity = _load_json_file(path, _EVIDENCE_MAX_BYTES)
    if set(value) != _EVIDENCE_KEYS or value.get("schema_version") != "phase4b-v2-evidence-v1":
        raise PilotError("phase4b_v2_unavailable")
    if value.get("phase4_status") != "completed" or value.get("reconciled") is not True or value.get("v2_published") is not True:
        raise PilotError("phase4b_v2_unavailable")
    if value.get("seam") != SEAM_NAME or value.get("relation") != V2_RELATION or tuple(value.get("required_columns", ())) != REQUIRED_COLUMNS:
        raise PilotError("phase4b_v2_unavailable")
    if not all(_is_sha256(value.get(field)) for field in ("phase4_run_sha256", "reconciliation_sha256", "publication_sha256", "schema_sha256", "lineage_attestation_sha256")):
        raise PilotError("phase4b_v2_unavailable")
    approved_series = value.get("approved_series")
    if not isinstance(approved_series, list) or not approved_series or len(approved_series) > 5 or len(set(approved_series)) != len(approved_series) or any(not isinstance(item, str) or not item.strip() for item in approved_series):
        raise PilotError("phase4b_v2_unavailable")
    if not isinstance(value.get("approved_by"), str) or not value["approved_by"].strip() or not isinstance(value.get("approved_at"), str):
        raise PilotError("phase4b_v2_unavailable")
    try:
        datetime.strptime(value["approved_at"], "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise PilotError("phase4b_v2_unavailable") from exc
    return Phase4V2Evidence(token=_EVIDENCE_TOKEN, artifact_sha256=hashlib.sha256(raw).hexdigest(), approved_series=tuple(approved_series), source_path=source_path, source_identity=identity)


def budget_for(mode: str) -> CalibrationBudget:
    if mode == "full":
        raise PilotError("run_budget_required")
    try:
        return _BUDGETS[mode]
    except (KeyError, TypeError) as exc:
        raise PilotError("run_budget_required") from exc


def _validate_request(evidence: object, series_ids: Sequence[str], budget: CalibrationBudget) -> tuple[Phase4V2Evidence, tuple[str, ...]]:
    if not isinstance(evidence, Phase4V2Evidence) or getattr(evidence, "_provenance_token", None) is not _EVIDENCE_TOKEN:
        raise PilotError("phase4b_v2_unavailable")
    evidence.verify_unchanged()
    if isinstance(series_ids, str) or not isinstance(series_ids, Sequence):
        raise PilotError("run_budget_required")
    values = tuple(series_ids)
    if not values or len(values) > budget.max_series or len(values) != len(set(values)) or any(not isinstance(item, str) or not item.strip() for item in values):
        raise PilotError("run_budget_required")
    if not set(values).issubset(evidence.approved_series):
        raise PilotError("run_budget_required")
    return evidence, values


def _checkpoint_payload(*, run_id: str, evidence: Phase4V2Evidence, mode: str, series_ids: tuple[str, ...], reports: Sequence[tuple[str, str, str, str]], last_key: tuple[str, ...] | None, pages: int, rows: int, elapsed_seconds: float, output_hash: str, output_state: str, stop_reason: str | None = None) -> dict[str, object]:
    return {
        "schema_version": "bond-pilot-calibration-checkpoint-v1", "run_id": run_id, "evidence_sha256": evidence.artifact_sha256,
        "mode": mode, "series_ids": list(series_ids), "seam": SEAM_NAME, "relation": V2_RELATION,
        "query_version": _QUERY_VERSION, "query_sha256": _QUERY_SHA256, "method_version": _METHOD_VERSION,
        "method_sha256": _METHOD_SHA256, "resolved_reports": [dict(zip(_REPORT_COLUMNS, report, strict=True)) for report in reports],
        "last_key": list(last_key) if last_key is not None else None, "pages": pages, "rows": rows,
        "elapsed_seconds": elapsed_seconds, "output_hash": output_hash, "output_state": output_state, "stop_reason": stop_reason,
    }


def _load_checkpoint(path: Path, *, run_id: str, evidence: Phase4V2Evidence, mode: str, series_ids: tuple[str, ...], budget: CalibrationBudget) -> dict[str, object]:
    if not path.exists():
        return _checkpoint_payload(run_id=run_id, evidence=evidence, mode=mode, series_ids=series_ids, reports=(), last_key=None, pages=0, rows=0, elapsed_seconds=0.0, output_hash=hashlib.sha256(b"").hexdigest(), output_state="new")
    try:
        value, _raw, _source_path, _identity = _load_json_file(path, _EVIDENCE_MAX_BYTES)
    except PilotError as exc:
        raise PilotError("run_budget_required") from exc
    if set(value) != _CHECKPOINT_KEYS:
        raise PilotError("run_budget_required")
    expected = {"schema_version": "bond-pilot-calibration-checkpoint-v1", "run_id": run_id, "evidence_sha256": evidence.artifact_sha256, "mode": mode, "series_ids": list(series_ids), "seam": SEAM_NAME, "relation": V2_RELATION, "query_version": _QUERY_VERSION, "query_sha256": _QUERY_SHA256, "method_version": _METHOD_VERSION, "method_sha256": _METHOD_SHA256}
    if any(value.get(key) != item for key, item in expected.items()):
        raise PilotError("run_budget_required")
    if not isinstance(value["pages"], int) or isinstance(value["pages"], bool) or not isinstance(value["rows"], int) or isinstance(value["rows"], bool) or value["pages"] < 0 or value["pages"] > budget.max_pages or value["rows"] < 0 or value["rows"] > budget.max_rows or not isinstance(value["elapsed_seconds"], (int, float)) or isinstance(value["elapsed_seconds"], bool) or not math.isfinite(value["elapsed_seconds"]) or value["elapsed_seconds"] < 0 or not _is_sha256(value["output_hash"]) or value["output_state"] not in ("new", "in_progress", "complete", "stopped"):
        raise PilotError("run_budget_required")
    reports = _decode_reports(value["resolved_reports"], series_ids) if value["resolved_reports"] else ()
    last_key = _decode_key(value["last_key"]) if value["last_key"] is not None else None
    if (last_key is None) != (value["rows"] == 0):
        raise PilotError("run_budget_required")
    value["resolved_reports"] = reports
    value["last_key"] = last_key
    return value


def _assert_read_only(connection: object) -> None:
    if getattr(connection, "read_only", True) is not True:
        raise PilotError("read_only_required")
    checker = getattr(connection, "assert_read_only", None)
    if callable(checker):
        checker()


def _is_timeout(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) == "57014" or error.__class__.__name__ == "QueryCanceled"


def _statement(cursor: object, connection: object, sql: str, params: tuple[object, ...] | None, remaining: Callable[[], float], required_timeout_seconds: int) -> None:
    _assert_read_only(connection)
    if remaining() < required_timeout_seconds:
        raise PilotError("calibration_timeout")
    try:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
    except BaseException as exc:
        if _is_timeout(exc):
            raise PilotError("calibration_timeout") from exc
        raise
    if remaining() < 0:
        raise PilotError("calibration_timeout")


def _scalar(row: object) -> object:
    if isinstance(row, Mapping):
        return next(iter(row.values()), None)
    if isinstance(row, (tuple, list)):
        return row[0] if row else None
    return row


def _decode_reports(rows: object, requested_series: tuple[str, ...]) -> tuple[tuple[str, str, str, str], ...]:
    if not isinstance(rows, (list, tuple)):
        raise PilotError("phase4b_v2_unavailable")
    selected: dict[str, tuple[str, str, str, str]] = {}
    for row in rows:
        if isinstance(row, Mapping):
            if set(row) != set(_REPORT_COLUMNS):
                raise PilotError("phase4b_v2_unavailable")
            values = tuple(row[column] for column in _REPORT_COLUMNS)
        elif isinstance(row, (tuple, list)) and len(row) == len(_REPORT_COLUMNS):
            values = tuple(row)
        else:
            raise PilotError("phase4b_v2_unavailable")
        if any(not isinstance(item, str) or not item for item in values):
            raise PilotError("phase4b_v2_unavailable")
        if values[0] in selected:
            raise PilotError("phase4b_v2_unavailable")
        selected[values[0]] = values  # type: ignore[assignment]
    if set(selected) != set(requested_series):
        raise PilotError("phase4b_v2_unavailable")
    return tuple(selected[series_id] for series_id in requested_series)


def _decode_key(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) != len(FULL_KEY_COLUMNS) or any(not isinstance(item, str) or not item for item in value):
        raise PilotError("run_budget_required")
    return tuple(value)


def _decode_page(rows: object, last_key: tuple[str, ...] | None) -> tuple[tuple[dict[str, object], ...], tuple[str, ...] | None]:
    if not isinstance(rows, (list, tuple)):
        raise PilotError("nondeterministic_page")
    decoded: list[dict[str, object]] = []
    keys: list[tuple[str, ...]] = []
    for row in rows:
        if isinstance(row, Mapping):
            if set(row) != set(REQUIRED_COLUMNS):
                raise PilotError("nondeterministic_page")
            item = {column: row[column] for column in REQUIRED_COLUMNS}
        elif isinstance(row, (tuple, list)) and len(row) == len(REQUIRED_COLUMNS):
            item = dict(zip(REQUIRED_COLUMNS, row, strict=True))
        else:
            raise PilotError("nondeterministic_page")
        key = tuple(item[column] for column in FULL_KEY_COLUMNS)
        if any(not isinstance(part, str) or not part for part in key):
            raise PilotError("nondeterministic_page")
        for field in ("report_date", "filing_date"):
            if not isinstance(item[field], str):
                raise PilotError("nondeterministic_page")
            try:
                datetime.strptime(item[field], "%Y-%m-%d")
            except ValueError as exc:
                raise PilotError("nondeterministic_page") from exc
        decoded.append(item)
        keys.append(key)  # type: ignore[arg-type]
    if keys != sorted(keys) or len(keys) != len(set(keys)) or (last_key is not None and keys and keys[0] <= last_key):
        raise PilotError("nondeterministic_page")
    return tuple(decoded), keys[-1] if keys else last_key


def _walk_plan(value: object) -> list[Mapping[str, object]]:
    found: list[Mapping[str, object]] = []
    if isinstance(value, Mapping):
        if isinstance(value.get("Node Type"), str):
            found.append(value)
        for child in value.values():
            found.extend(_walk_plan(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_plan(child))
    return found


def _validate_plan(rows: object, *, page_size: int, resume: bool) -> None:
    raw = rows[0] if isinstance(rows, (list, tuple)) and len(rows) == 1 else rows
    if isinstance(raw, (tuple, list)) and len(raw) == 1:
        raw = raw[0]
    nodes = _walk_plan(raw)
    target = [node for node in nodes if node.get("Relation Name") == "sec_nport_holdings_v2_current" and node.get("Schema") == "public"]
    if not nodes or not target or any(node.get("Node Type") == "Seq Scan" for node in nodes):
        raise PilotError("unsafe_query_plan")
    if not any(("Index" in str(node.get("Node Type")) or "Partition" in str(node.get("Node Type"))) for node in target):
        raise PilotError("unsafe_query_plan")
    if any(not isinstance(node.get("Plan Rows"), int) or node["Plan Rows"] > page_size for node in target):
        raise PilotError("unsafe_query_plan")
    predicates = " ".join(str(node.get(field, "")) for node in target for field in ("Index Cond", "Filter", "Recheck Cond", "Join Filter"))
    required = _REPORT_COLUMNS + (FULL_KEY_COLUMNS if resume else ())
    if any(field not in predicates for field in required):
        raise PilotError("unsafe_query_plan")


def _write_checkpoint(path: Path, state: dict[str, object], *, reports: tuple[tuple[str, str, str, str], ...], last_key: tuple[str, ...] | None, pages: int, rows: int, elapsed: float, output_hash: str, output_state: str, stop_reason: str | None) -> None:
    payload = _checkpoint_payload(run_id=state["run_id"], evidence=state["evidence"], mode=state["mode"], series_ids=state["series_ids"], reports=reports, last_key=last_key, pages=pages, rows=rows, elapsed_seconds=elapsed, output_hash=output_hash, output_state=output_state, stop_reason=stop_reason)
    replace_checkpoint(path, canonical_json_bytes(payload))


def _page_hash(previous: str, page: tuple[dict[str, object], ...]) -> str:
    return hashlib.sha256(bytes.fromhex(previous) + canonical_json_bytes(page)).hexdigest()


def run_v2_calibration(connection: object, *, evidence: object, series_ids: Sequence[str], mode: str, checkpoint_path: str | Path, run_id: str = "bond-pilot-calibration", clock: Callable[[], float] = monotonic) -> CalibrationResult:
    """Execute no more than one approved V2 keyset budget with no retries."""
    budget = budget_for(mode)
    evidence, series_ids = _validate_request(evidence, series_ids, budget)
    if not isinstance(run_id, str) or not run_id:
        raise PilotError("run_budget_required")
    checkpoint_path = Path(checkpoint_path)
    checkpoint = _load_checkpoint(checkpoint_path, run_id=run_id, evidence=evidence, mode=mode, series_ids=series_ids, budget=budget)
    started = clock()
    state: dict[str, object] = {"run_id": run_id, "evidence": evidence, "mode": mode, "series_ids": series_ids}
    pages, rows_read = checkpoint["pages"], checkpoint["rows"]
    elapsed_before, output_hash = float(checkpoint["elapsed_seconds"]), checkpoint["output_hash"]
    last_key = checkpoint["last_key"]
    reports = checkpoint["resolved_reports"]
    page_rows: list[dict[str, object]] = []

    def elapsed() -> float:
        return elapsed_before + (clock() - started)

    def remaining() -> float:
        return budget.wall_seconds - elapsed()

    if connection is None:
        raise PilotError("phase4b_v2_unavailable")
    connection.read_only = True
    _assert_read_only(connection)
    try:
        with connection.transaction():
            _assert_read_only(connection)
            with connection.cursor() as cursor:
                for sql in (SET_REPEATABLE_READ_ONLY_SQL, f"SET LOCAL statement_timeout = '{budget.statement_timeout_seconds}s'", SET_LOCK_TIMEOUT_SQL, SET_IDLE_TIMEOUT_SQL):
                    _statement(cursor, connection, sql, None, remaining, budget.statement_timeout_seconds)
                _statement(cursor, connection, SHOW_READ_ONLY_SQL, None, remaining, budget.statement_timeout_seconds)
                if _scalar(cursor.fetchone()) != "on":
                    raise PilotError("read_only_required")
                _statement(cursor, connection, RELATION_SQL, (V2_RELATION,), remaining, budget.statement_timeout_seconds)
                relation = _scalar(cursor.fetchone())
                if relation not in (V2_RELATION, V2_RELATION.rsplit(".", 1)[1]):
                    raise PilotError("phase4b_v2_unavailable")
                _statement(cursor, connection, COLUMNS_SQL, None, remaining, budget.statement_timeout_seconds)
                columns = tuple(str(_scalar(row)) for row in cursor.fetchall())
                if not set(REQUIRED_COLUMNS).issubset(columns):
                    raise PilotError("phase4b_v2_unavailable")
                _statement(cursor, connection, LATEST_REPORTS_SQL, (series_ids,), remaining, budget.statement_timeout_seconds)
                resolved = _decode_reports(cursor.fetchall(), series_ids)
                if reports and resolved != reports:
                    raise PilotError("phase4b_v2_unavailable")
                reports = resolved
                if pages >= budget.max_pages or rows_read >= budget.max_rows:
                    _write_checkpoint(checkpoint_path, state, reports=reports, last_key=last_key, pages=pages, rows=rows_read, elapsed=elapsed(), output_hash=output_hash, output_state="stopped", stop_reason="budget_reached")
                    return CalibrationResult((), pages, rows_read, True, last_key, mode)
                report_dates = tuple(report[1] for report in reports)
                publications = tuple(report[2] for report in reports)
                accessions = tuple(report[3] for report in reports)
                resume = last_key is not None
                sql = RESUME_PAGE_SQL if resume else INITIAL_PAGE_SQL
                explain_sql = EXPLAIN_RESUME_SQL if resume else EXPLAIN_INITIAL_SQL
                params = (series_ids, report_dates, publications, accessions, *(last_key or ()), budget.page_size)
                _statement(cursor, connection, explain_sql, params, remaining, budget.statement_timeout_seconds)
                _validate_plan(cursor.fetchall(), page_size=budget.page_size, resume=resume)
                _statement(cursor, connection, sql, params, remaining, budget.statement_timeout_seconds)
                decoded, next_key = _decode_page(cursor.fetchall(), last_key)
                if len(decoded) > budget.page_size or rows_read + len(decoded) > budget.max_rows:
                    raise PilotError("budget_drift")
                page_rows.extend(decoded)
                if decoded:
                    pages += 1
                    rows_read += len(decoded)
                    last_key = next_key
                    output_hash = _page_hash(output_hash, decoded)
                partial = pages >= budget.max_pages or rows_read >= budget.max_rows
                complete = not decoded or len(decoded) < budget.page_size
                _write_checkpoint(checkpoint_path, state, reports=reports, last_key=last_key, pages=pages, rows=rows_read, elapsed=elapsed(), output_hash=output_hash, output_state="complete" if complete and not partial else "in_progress", stop_reason="budget_reached" if partial else None)
                return CalibrationResult(tuple(page_rows), pages, rows_read, partial, last_key, mode)
    except PilotError as exc:
        _write_checkpoint(checkpoint_path, state, reports=reports, last_key=last_key, pages=pages, rows=rows_read, elapsed=elapsed(), output_hash=output_hash, output_state="stopped", stop_reason=exc.code)
        raise

"""Fail-closed, connection-injected V2 calibration reads for the internal pilot."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Callable, Mapping, Sequence

from .artifacts import canonical_json_bytes, replace_checkpoint
from .contracts import PilotError


V2_RELATION = "public.sec_nport_holdings_v2_current"
SEAM_NAME = "nport-v2-current"
REQUIRED_COLUMNS = (
    "publication_id", "accession_number", "holding_id", "source_run_id",
    "report_date", "filing_date", "series_id", "class_id", "instrument_id",
    "issuer_category", "cusip", "signed_market_value", "signed_pct_of_nav", "currency",
)

SET_STATEMENT_TIMEOUT_SQL = "SET LOCAL statement_timeout = '20s'"
SET_LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '2s'"
SET_IDLE_TIMEOUT_SQL = "SET LOCAL idle_in_transaction_session_timeout = '60s'"
SHOW_READ_ONLY_SQL = "SHOW transaction_read_only"
RELATION_SQL = "SELECT to_regclass(%s)"
COLUMNS_SQL = (
    "SELECT column_name FROM information_schema.columns "
    "WHERE table_schema = 'public' AND table_name = 'sec_nport_holdings_v2_current' "
    "ORDER BY ordinal_position"
)
LATEST_REPORTS_SQL = (
    "SELECT DISTINCT ON (series_id) series_id, report_date, publication_id, accession_number "
    "FROM public.sec_nport_holdings_v2_current "
    "WHERE series_id = ANY(%s) "
    "ORDER BY series_id, report_date DESC, publication_id DESC, accession_number DESC"
)
_HOLDINGS_SELECT = ", ".join(f"holdings.{column}" for column in REQUIRED_COLUMNS)
PAGE_SQL = (
    f"SELECT {_HOLDINGS_SELECT} FROM public.sec_nport_holdings_v2_current AS holdings "
    "JOIN unnest(%s::text[], %s::date[], %s::text[], %s::text[]) "
    "AS selected(series_id, report_date, publication_id, accession_number) "
    "ON (holdings.series_id = selected.series_id AND holdings.report_date = selected.report_date "
    "AND holdings.publication_id = selected.publication_id AND holdings.accession_number = selected.accession_number) "
    "WHERE (%s IS NULL OR (holdings.publication_id, holdings.accession_number, holdings.holding_id) > (%s, %s, %s)) "
    "ORDER BY holdings.publication_id, holdings.accession_number, holdings.holding_id LIMIT %s"
)
EXPLAIN_SQL = (
    "EXPLAIN (FORMAT JSON) "
    "WITH latest_effective_reports AS ("
    "SELECT DISTINCT ON (series_id) series_id, report_date, publication_id, accession_number "
    "FROM public.sec_nport_holdings_v2_current WHERE series_id = ANY(%s) "
    "ORDER BY series_id, report_date DESC, publication_id DESC, accession_number DESC"
    ") "
    f"SELECT {_HOLDINGS_SELECT} FROM public.sec_nport_holdings_v2_current AS holdings "
    "JOIN latest_effective_reports AS reports ON (holdings.series_id = reports.series_id "
    "AND holdings.report_date = reports.report_date "
    "AND holdings.publication_id = reports.publication_id "
    "AND holdings.accession_number = reports.accession_number) "
    "WHERE (%s IS NULL OR (holdings.publication_id, holdings.accession_number, holdings.holding_id) > (%s, %s, %s)) "
    "ORDER BY holdings.publication_id, holdings.accession_number, holdings.holding_id LIMIT %s"
)


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
    retries: int = 0
    concurrency: int = 1


@dataclass(frozen=True)
class CalibrationResult:
    rows: tuple[dict[str, object], ...]
    pages: int
    partial: bool
    last_key: tuple[str, str, str] | None
    mode: str


_BUDGETS = {
    "calibration": CalibrationBudget("calibration", 1000, 5, 5000, 600, 20),
    "first_bounded": CalibrationBudget("first_bounded", 2500, 20, 50000, 1800, 30),
}


def budget_for(mode: str) -> CalibrationBudget:
    """Return one immutable allowlisted budget; full reads require a later approval."""
    if mode == "full":
        raise PilotError("run_budget_required")
    try:
        return _BUDGETS[mode]
    except (KeyError, TypeError) as exc:
        raise PilotError("run_budget_required") from exc


def _unavailable() -> None:
    raise PilotError("phase4b_v2_unavailable")


def _validate_evidence(evidence: Mapping[str, object] | None) -> None:
    if not isinstance(evidence, Mapping):
        _unavailable()
    if evidence.get("phase4_status") != "completed" or evidence.get("reconciled") is not True or evidence.get("v2_ready") is not True:
        _unavailable()
    if evidence.get("seam") != SEAM_NAME or evidence.get("relation") != V2_RELATION:
        _unavailable()
    columns = evidence.get("required_columns")
    if not isinstance(columns, (list, tuple)) or tuple(columns) != REQUIRED_COLUMNS:
        _unavailable()


def _validate_series_ids(series_ids: Sequence[str], budget: CalibrationBudget) -> tuple[str, ...]:
    if isinstance(series_ids, str) or not isinstance(series_ids, Sequence):
        raise PilotError("run_budget_required")
    values = tuple(series_ids)
    if not values or len(values) > budget.max_series or len(set(values)) != len(values):
        raise PilotError("run_budget_required")
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise PilotError("run_budget_required")
    return values


def _assert_read_only(connection: object) -> None:
    if getattr(connection, "read_only", True) is not True:
        raise PilotError("read_only_required")
    checker = getattr(connection, "assert_read_only", None)
    if callable(checker):
        checker()


def _execute(cursor: object, sql: str, params: object = None) -> None:
    _assert_read_only(getattr(cursor, "connection", None) or getattr(cursor, "_connection", None) or _ACTIVE_CONNECTION)
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)


_ACTIVE_CONNECTION: object | None = None


def _one(cursor: object, sql: str, params: object = None) -> object:
    _execute(cursor, sql, params)
    return cursor.fetchone()


def _all(cursor: object, sql: str, params: object = None) -> list[object]:
    _execute(cursor, sql, params)
    return list(cursor.fetchall())


def _timeout_sql(budget: CalibrationBudget) -> tuple[str, str, str]:
    return (
        f"SET LOCAL statement_timeout = '{budget.statement_timeout_seconds}s'",
        f"SET LOCAL lock_timeout = '{budget.lock_timeout_seconds}s'",
        f"SET LOCAL idle_in_transaction_session_timeout = '{budget.idle_transaction_timeout_seconds}s'",
    )


def _scalar(row: object) -> object:
    if isinstance(row, Mapping):
        return next(iter(row.values()), None)
    if isinstance(row, (tuple, list)):
        return row[0] if row else None
    return row


def _report_rows(rows: list[object], requested_series: tuple[str, ...]) -> tuple[tuple[str, str, str, str], ...]:
    selected: dict[str, tuple[str, str, str, str]] = {}
    for row in rows:
        values = tuple(row.values()) if isinstance(row, Mapping) else tuple(row) if isinstance(row, (tuple, list)) else ()
        if len(values) != 4 or not all(isinstance(value, str) and value for value in values):
            _unavailable()
        series_id, report_date, publication_id, accession_number = values
        if series_id in selected:
            _unavailable()
        selected[series_id] = (series_id, report_date, publication_id, accession_number)
    if set(selected) != set(requested_series):
        _unavailable()
    return tuple(selected[series_id] for series_id in requested_series)


def _page_rows(rows: list[object], last_key: tuple[str, str, str] | None) -> tuple[tuple[dict[str, object], ...], tuple[str, str, str] | None]:
    converted: list[dict[str, object]] = []
    keys: list[tuple[str, str, str]] = []
    for row in rows:
        if isinstance(row, Mapping):
            value = dict(row)
        elif isinstance(row, (tuple, list)) and len(row) == len(REQUIRED_COLUMNS):
            value = dict(zip(REQUIRED_COLUMNS, row, strict=True))
        else:
            raise PilotError("nondeterministic_page")
        key = tuple(value.get(field) for field in ("publication_id", "accession_number", "holding_id"))
        if not all(isinstance(part, str) and part for part in key):
            raise PilotError("nondeterministic_page")
        keys.append(key)  # type: ignore[arg-type]
        converted.append(value)
    if keys and (keys != sorted(keys) or len(set(keys)) != len(keys) or (last_key is not None and keys[0] <= last_key)):
        raise PilotError("nondeterministic_page")
    return tuple(converted), keys[-1] if keys else last_key


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


def _validate_explain(plan_rows: list[object]) -> None:
    raw_plan = plan_rows[0] if len(plan_rows) == 1 else plan_rows
    if isinstance(raw_plan, (tuple, list)) and len(raw_plan) == 1 and isinstance(raw_plan[0], (Mapping, list)):
        raw_plan = raw_plan[0]
    nodes = _walk_plan(raw_plan)
    if not nodes or any(node.get("Node Type") == "Seq Scan" for node in nodes):
        raise PilotError("unsafe_query_plan")
    if not any("Index" in str(node.get("Node Type")) or "Partition" in str(node.get("Node Type")) for node in nodes):
        raise PilotError("unsafe_query_plan")
    predicates = " ".join(str(node.get(field, "")) for node in nodes for field in ("Index Cond", "Filter", "Recheck Cond", "Join Filter"))
    if "series_id" not in predicates or "report_date" not in predicates:
        raise PilotError("unsafe_query_plan")


def _checkpoint(path: Path, *, mode: str, pages: int, rows: int, last_key: tuple[str, str, str] | None, stop_reason: str | None = None) -> None:
    value: dict[str, object] = {
        "schema_version": "bond-pilot-calibration-checkpoint-v1", "mode": mode,
        "seam": SEAM_NAME, "relation": V2_RELATION, "pages": pages, "rows": rows,
        "last_key": list(last_key) if last_key is not None else None,
    }
    if stop_reason is not None:
        value["stop_reason"] = stop_reason
    replace_checkpoint(path, canonical_json_bytes(value))


def _assert_static_sql_is_safe() -> None:
    for sql in (LATEST_REPORTS_SQL, PAGE_SQL, EXPLAIN_SQL):
        lowered = sql.lower()
        if "sec_nport_holdings_v2_current" not in lowered or "sec_nport_holdings " in lowered or ";" in sql:
            raise PilotError("unsafe_sql")


def run_v2_calibration(
    connection: object,
    *,
    evidence: Mapping[str, object] | None,
    series_ids: Sequence[str],
    mode: str,
    checkpoint_path: str | Path,
    clock: Callable[[], float] = monotonic,
) -> CalibrationResult:
    """Read a fixed V2 seam under a fixed, cumulative, no-retry budget."""
    budget = budget_for(mode)
    requested_series = _validate_series_ids(series_ids, budget)
    _validate_evidence(evidence)
    _assert_static_sql_is_safe()
    if connection is None:
        _unavailable()
    connection.read_only = True
    _assert_read_only(connection)
    checkpoint = Path(checkpoint_path)
    started = clock()
    rows: list[dict[str, object]] = []
    pages = 0
    last_key: tuple[str, str, str] | None = None
    global _ACTIVE_CONNECTION
    _ACTIVE_CONNECTION = connection
    try:
        _assert_read_only(connection)
        with connection.transaction():
            _assert_read_only(connection)
            with connection.cursor() as cursor:
                for sql in _timeout_sql(budget):
                    _execute(cursor, sql)
                if _scalar(_one(cursor, SHOW_READ_ONLY_SQL)) != "on":
                    raise PilotError("read_only_required")
                relation = _scalar(_one(cursor, RELATION_SQL, (V2_RELATION,)))
                if relation is None or str(relation) not in (V2_RELATION, V2_RELATION.rsplit(".", 1)[-1]):
                    _unavailable()
                columns = tuple(str(_scalar(row)) for row in _all(cursor, COLUMNS_SQL))
                if not set(REQUIRED_COLUMNS).issubset(columns):
                    _unavailable()
                _validate_explain(_all(cursor, EXPLAIN_SQL, (requested_series, None, None, None, budget.page_size)))
                reports = _report_rows(_all(cursor, LATEST_REPORTS_SQL, (requested_series,)), requested_series)
                report_dates = tuple(report[1] for report in reports)
                publication_ids = tuple(report[2] for report in reports)
                accession_numbers = tuple(report[3] for report in reports)
                while pages < budget.max_pages and len(rows) < budget.max_rows:
                    if clock() - started > budget.wall_seconds:
                        _checkpoint(checkpoint, mode=mode, pages=pages, rows=len(rows), last_key=last_key, stop_reason="timeout")
                        raise PilotError("calibration_timeout")
                    params = (requested_series, report_dates, publication_ids, accession_numbers, *(last_key or (None, None, None)), budget.page_size)
                    page_rows, next_key = _page_rows(_all(cursor, PAGE_SQL, params), last_key)
                    if clock() - started > budget.wall_seconds:
                        _checkpoint(checkpoint, mode=mode, pages=pages, rows=len(rows), last_key=last_key, stop_reason="timeout")
                        raise PilotError("calibration_timeout")
                    if len(page_rows) > budget.page_size or len(rows) + len(page_rows) > budget.max_rows:
                        _checkpoint(checkpoint, mode=mode, pages=pages, rows=len(rows), last_key=last_key, stop_reason="budget_drift")
                        raise PilotError("budget_drift")
                    if not page_rows:
                        _checkpoint(checkpoint, mode=mode, pages=pages, rows=len(rows), last_key=last_key)
                        break
                    rows.extend(page_rows)
                    pages += 1
                    last_key = next_key
                    _checkpoint(checkpoint, mode=mode, pages=pages, rows=len(rows), last_key=last_key)
                partial = pages == budget.max_pages or len(rows) == budget.max_rows
                if partial:
                    _checkpoint(checkpoint, mode=mode, pages=pages, rows=len(rows), last_key=last_key, stop_reason="budget_reached")
                return CalibrationResult(tuple(rows), pages, partial, last_key, mode)
    except PilotError:
        raise
    finally:
        _ACTIVE_CONNECTION = None

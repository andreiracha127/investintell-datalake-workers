"""Governed, read-only, bounded V2 calibration for the internal bond pilot."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
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
REQUIRED_COLUMNS = ("publication_id", "accession_number", "holding_id", "source_run_id", "report_date", "filing_date", "series_id", "class_id", "instrument_id", "issuer_category", "cusip", "signed_market_value", "signed_pct_of_nav", "currency")
FULL_KEY_COLUMNS = ("series_id", "report_date", "publication_id", "accession_number", "holding_id", "source_run_id", "instrument_id")
_REPORT_COLUMNS = ("series_id", "report_date", "publication_id", "accession_number")
_HASH_FIELDS = ("phase4_run_sha256", "reconciliation_sha256", "publication_sha256", "schema_sha256", "lineage_attestation_sha256")
_EVIDENCE_KEYS = frozenset({"schema_version", "phase4_status", "reconciled", "v2_published", "seam", "relation", "required_columns", *_HASH_FIELDS, "approved_series", "approved_by", "approved_at"})
_APPROVAL_KEYS = frozenset({"schema_version", "evidence_sha256", *_HASH_FIELDS, "seam", "relation", "approved_series", "approved_modes", "allow_read", "approved_by", "approved_at"})
_TOKEN = object()
_MAX_GOVERNANCE_BYTES = 1024 * 1024

SET_REPEATABLE_READ_ONLY_SQL = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
SET_STATEMENT_TIMEOUT_SQL = "SET LOCAL statement_timeout = '20s'"
SET_LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '2s'"
SET_IDLE_TIMEOUT_SQL = "SET LOCAL idle_in_transaction_session_timeout = '60s'"
SHOW_READ_ONLY_SQL = "SHOW transaction_read_only"
RELATION_SQL = "SELECT to_regclass(%s)"
COLUMNS_SQL = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sec_nport_holdings_v2_current' ORDER BY ordinal_position"
RESOLVER_SQL = "WITH requested(series_id) AS (SELECT unnest(%s::text[])) SELECT requested.series_id, resolved.report_date, resolved.publication_id, resolved.accession_number FROM requested CROSS JOIN LATERAL (SELECT report_date, publication_id, accession_number FROM public.sec_nport_holdings_v2_current WHERE series_id = requested.series_id ORDER BY report_date DESC, publication_id DESC, accession_number DESC LIMIT 1) AS resolved"
EXPLAIN_RESOLVER_SQL = f"EXPLAIN (FORMAT JSON) {RESOLVER_SQL}"
_SELECT = ", ".join(f"holdings.{field}" for field in REQUIRED_COLUMNS)
_JOIN = "JOIN unnest(%s::text[], %s::date[], %s::text[], %s::text[]) AS selected(series_id, report_date, publication_id, accession_number) ON (holdings.series_id = selected.series_id AND holdings.report_date = selected.report_date AND holdings.publication_id = selected.publication_id AND holdings.accession_number = selected.accession_number)"
_ORDER = ", ".join(f"holdings.{field}" for field in FULL_KEY_COLUMNS)
INITIAL_PAGE_SQL = f"SELECT {_SELECT} FROM {V2_RELATION} AS holdings {_JOIN} ORDER BY {_ORDER} LIMIT %s"
RESUME_PAGE_SQL = f"SELECT {_SELECT} FROM {V2_RELATION} AS holdings {_JOIN} WHERE ({_ORDER}) > ({', '.join('%s' for _ in FULL_KEY_COLUMNS)}) ORDER BY {_ORDER} LIMIT %s"
EXPLAIN_INITIAL_SQL = f"EXPLAIN (FORMAT JSON) {INITIAL_PAGE_SQL}"
EXPLAIN_RESUME_SQL = f"EXPLAIN (FORMAT JSON) {RESUME_PAGE_SQL}"

_QUERY_VERSION = "nport-v2-keyset-v3"
_METHOD_VERSION = "bond-pilot-calibration-v3"
_QUERY_SHA256 = hashlib.sha256((RESOLVER_SQL + "\n" + INITIAL_PAGE_SQL + "\n" + RESUME_PAGE_SQL).encode()).hexdigest()
_METHOD_SHA256 = hashlib.sha256(_METHOD_VERSION.encode()).hexdigest()
_EMPTY_HASH = hashlib.sha256(b"").hexdigest()
_CHECKPOINT_KEYS = frozenset({"schema_version", "run_id", "evidence_sha256", "approval_sha256", "approval_authority_sha256", "publication_sha256", "mode", "series_ids", "seam", "relation", "query_version", "query_sha256", "method_version", "method_sha256", "resolved_reports", "last_key", "pages", "rows", "elapsed_seconds", "output_hash", "output_state", "stop_reason"})


@dataclass(frozen=True, init=False)
class Phase4V2Evidence:
    artifact_sha256: str
    path: Path
    values: Mapping[str, object]
    _token: object

    def __init__(self, *, token: object, artifact_sha256: str, path: Path, values: Mapping[str, object]) -> None:
        if token is not _TOKEN:
            raise PilotError("phase4b_v2_unavailable")
        object.__setattr__(self, "artifact_sha256", artifact_sha256)
        object.__setattr__(self, "path", path.resolve())
        object.__setattr__(self, "values", dict(values))
        object.__setattr__(self, "_token", token)


@dataclass(frozen=True, init=False)
class Phase4V2EvidenceApproval:
    artifact_sha256: str
    path: Path
    values: Mapping[str, object]
    _token: object

    def __init__(self, *, token: object, artifact_sha256: str, path: Path, values: Mapping[str, object]) -> None:
        if token is not _TOKEN:
            raise PilotError("phase4b_v2_unavailable")
        object.__setattr__(self, "artifact_sha256", artifact_sha256)
        object.__setattr__(self, "path", path.resolve())
        object.__setattr__(self, "values", dict(values))
        object.__setattr__(self, "_token", token)


@dataclass(frozen=True)
class CalibrationBudget:
    mode: str
    page_size: int
    max_pages: int
    max_rows: int
    wall_seconds: int
    statement_timeout_seconds: int


@dataclass(frozen=True)
class CalibrationResult:
    rows: tuple[dict[str, object], ...]
    pages: int
    rows_read: int
    partial: bool
    last_key: tuple[str, ...] | None
    mode: str


_BUDGETS = {"calibration": CalibrationBudget("calibration", 1000, 5, 5000, 600, 20), "first_bounded": CalibrationBudget("first_bounded", 2500, 20, 50000, 1800, 30)}


def _duplicate(pairs: list[tuple[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("duplicate JSON key")
        output[key] = value
    return output


def _nonfinite(value: str) -> object:
    raise ValueError(f"non-finite {value}")


def _sha(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _read_json(path: str | Path) -> tuple[dict[str, object], bytes, Path]:
    source = Path(path).resolve()
    try:
        before = os.stat(source, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise OSError("not a regular file")
        with source.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise OSError("file changed")
            raw = handle.read(_MAX_GOVERNANCE_BYTES + 1)
    except OSError as exc:
        raise PilotError("phase4b_v2_unavailable") from exc
    if len(raw) > _MAX_GOVERNANCE_BYTES:
        raise PilotError("phase4b_v2_unavailable")
    try:
        parsed = json.loads(raw.decode("utf-8"), object_pairs_hook=_duplicate, parse_constant=_nonfinite)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise PilotError("phase4b_v2_unavailable") from exc
    if not isinstance(parsed, dict):
        raise PilotError("phase4b_v2_unavailable")
    return parsed, raw, source


def _timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value or value.casefold() in {"unknown", "none"}:
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True


def _validate_evidence(value: Mapping[str, object]) -> None:
    if set(value) != _EVIDENCE_KEYS or value.get("schema_version") != "phase4b-v2-evidence-v1" or value.get("phase4_status") != "completed" or value.get("reconciled") is not True or value.get("v2_published") is not True or value.get("seam") != SEAM_NAME or value.get("relation") != V2_RELATION or tuple(value.get("required_columns", ())) != REQUIRED_COLUMNS:
        raise PilotError("phase4b_v2_unavailable")
    series = value.get("approved_series")
    if not isinstance(series, list) or not series or len(series) > 5 or len(series) != len(set(series)) or any(not isinstance(item, str) or not item.strip() for item in series) or not all(_sha(value.get(field)) for field in _HASH_FIELDS) or not _timestamp(value.get("approved_at")) or not isinstance(value.get("approved_by"), str) or not value["approved_by"].strip():
        raise PilotError("phase4b_v2_unavailable")


def _validate_approval(value: Mapping[str, object]) -> None:
    if set(value) != _APPROVAL_KEYS or value.get("schema_version") != "phase4b-v2-evidence-approval-v1" or value.get("seam") != SEAM_NAME or value.get("relation") != V2_RELATION or value.get("allow_read") is not True or not _sha(value.get("evidence_sha256")) or not all(_sha(value.get(field)) for field in _HASH_FIELDS) or not _timestamp(value.get("approved_at")) or not isinstance(value.get("approved_by"), str) or not value["approved_by"].strip():
        raise PilotError("phase4b_v2_unavailable")
    for field in ("approved_series", "approved_modes"):
        items = value.get(field)
        if not isinstance(items, list) or not items or len(items) != len(set(items)) or any(not isinstance(item, str) or not item.strip() for item in items):
            raise PilotError("phase4b_v2_unavailable")
    if not set(value["approved_modes"]).issubset(_BUDGETS):
        raise PilotError("phase4b_v2_unavailable")


def load_phase4_v2_evidence(path: str | Path) -> Phase4V2Evidence:
    value, raw, source = _read_json(path)
    _validate_evidence(value)
    return Phase4V2Evidence(token=_TOKEN, artifact_sha256=hashlib.sha256(raw).hexdigest(), path=source, values=value)


def load_phase4_v2_evidence_approval(path: str | Path) -> Phase4V2EvidenceApproval:
    value, raw, source = _read_json(path)
    _validate_approval(value)
    approval = Phase4V2EvidenceApproval(token=_TOKEN, artifact_sha256=hashlib.sha256(raw).hexdigest(), path=source, values=value)
    _approval_authority_hash(approval)
    return approval


def _approval_authority_hash(approval: Phase4V2EvidenceApproval) -> str:
    pin = os.environ.get("BOND_PILOT_PHASE4_V2_APPROVAL_SHA256")
    authority = os.environ.get("BOND_PILOT_PHASE4_V2_APPROVER_ID")
    if not _sha(pin) or pin != approval.artifact_sha256 or not isinstance(authority, str) or not authority or authority != approval.values.get("approved_by"):
        raise PilotError("phase4b_v2_unavailable")
    return hashlib.sha256(authority.encode("utf-8")).hexdigest()


def _rehash(governance: object, kind: str) -> tuple[dict[str, object], str]:
    expected_type = Phase4V2Evidence if kind == "evidence" else Phase4V2EvidenceApproval
    if not isinstance(governance, expected_type) or getattr(governance, "_token", None) is not _TOKEN:
        raise PilotError("phase4b_v2_unavailable")
    value, raw, source = _read_json(governance.path)
    if source != governance.path or hashlib.sha256(raw).hexdigest() != governance.artifact_sha256:
        raise PilotError("phase4b_v2_unavailable")
    (_validate_evidence if kind == "evidence" else _validate_approval)(value)
    if dict(governance.values) != value:
        raise PilotError("phase4b_v2_unavailable")
    if kind == "approval":
        _approval_authority_hash(governance)
    return value, governance.artifact_sha256


def _governance(evidence: object, approval: object, *, mode: str, series_ids: Sequence[str]) -> tuple[Phase4V2Evidence, Phase4V2EvidenceApproval, tuple[str, ...]]:
    if mode == "full":
        raise PilotError("run_budget_required")
    try:
        _BUDGETS[mode]
    except KeyError as exc:
        raise PilotError("run_budget_required") from exc
    ev, _ = _rehash(evidence, "evidence")
    ap, _ = _rehash(approval, "approval")
    if ap["evidence_sha256"] != evidence.artifact_sha256 or any(ap[field] != ev[field] for field in _HASH_FIELDS) or ap["seam"] != ev["seam"] or ap["relation"] != ev["relation"] or not set(ap["approved_series"]).issubset(ev["approved_series"]) or mode not in ap["approved_modes"]:
        raise PilotError("phase4b_v2_unavailable")
    if isinstance(series_ids, str) or not isinstance(series_ids, Sequence):
        raise PilotError("run_budget_required")
    series = tuple(series_ids)
    if not series or len(series) > 5 or len(series) != len(set(series)) or any(not isinstance(item, str) or not item.strip() for item in series) or not set(series).issubset(ap["approved_series"]):
        raise PilotError("run_budget_required")
    return evidence, approval, series


def validate_v2_request(evidence: object, approval: object, mode: str, series_ids: Sequence[str]) -> tuple[str, ...]:
    """Revalidate every V2 authority pin before a caller resolves or opens a DB connection."""
    _evidence, _approval, series = _governance(evidence, approval, mode=mode, series_ids=series_ids)
    return series


def _date(value: object) -> str:
    if isinstance(value, datetime):
        raise PilotError("nondeterministic_page")
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        raise PilotError("nondeterministic_page")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise PilotError("nondeterministic_page") from exc
    if parsed.isoformat() != value:
        raise PilotError("nondeterministic_page")
    return value


def _checkpoint_payload(*, run_id: str, evidence: Phase4V2Evidence, approval: Phase4V2EvidenceApproval, mode: str, series_ids: tuple[str, ...], reports: Sequence[tuple[str, str, str, str]], last_key: tuple[str, ...] | None, pages: int, rows: int, elapsed_seconds: float, output_hash: str, output_state: str, stop_reason: str | None) -> dict[str, object]:
    return {"schema_version": "bond-pilot-calibration-checkpoint-v2", "run_id": run_id, "evidence_sha256": evidence.artifact_sha256, "approval_sha256": approval.artifact_sha256, "approval_authority_sha256": _approval_authority_hash(approval), "publication_sha256": evidence.values["publication_sha256"], "mode": mode, "series_ids": list(series_ids), "seam": SEAM_NAME, "relation": V2_RELATION, "query_version": _QUERY_VERSION, "query_sha256": _QUERY_SHA256, "method_version": _METHOD_VERSION, "method_sha256": _METHOD_SHA256, "resolved_reports": [dict(zip(_REPORT_COLUMNS, report, strict=True)) for report in reports], "last_key": list(last_key) if last_key else None, "pages": pages, "rows": rows, "elapsed_seconds": elapsed_seconds, "output_hash": output_hash, "output_state": output_state, "stop_reason": stop_reason}


def _decode_reports(rows: object, expected_series: tuple[str, ...]) -> tuple[tuple[str, str, str, str], ...]:
    if not isinstance(rows, (list, tuple)):
        raise PilotError("phase4b_v2_unavailable")
    found: dict[str, tuple[str, str, str, str]] = {}
    for row in rows:
        if isinstance(row, Mapping):
            if set(row) != set(_REPORT_COLUMNS):
                raise PilotError("phase4b_v2_unavailable")
            values = (row["series_id"], _date(row["report_date"]), row["publication_id"], row["accession_number"])
        elif isinstance(row, (tuple, list)) and len(row) == 4:
            values = (row[0], _date(row[1]), row[2], row[3])
        else:
            raise PilotError("phase4b_v2_unavailable")
        if any(not isinstance(item, str) or not item for item in values) or values[0] in found:
            raise PilotError("phase4b_v2_unavailable")
        found[values[0]] = values  # type: ignore[assignment]
    if set(found) != set(expected_series):
        raise PilotError("phase4b_v2_unavailable")
    return tuple(found[item] for item in expected_series)


def _decode_key(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list) or len(value) != len(FULL_KEY_COLUMNS) or any(not isinstance(item, str) or not item for item in value):
        raise PilotError("run_budget_required")
    return tuple(value)


def _load_checkpoint(path: Path, *, evidence: Phase4V2Evidence, approval: Phase4V2EvidenceApproval, mode: str, series: tuple[str, ...], run_id: str, budget: CalibrationBudget) -> dict[str, object]:
    if not path.exists():
        return _checkpoint_payload(run_id=run_id, evidence=evidence, approval=approval, mode=mode, series_ids=series, reports=(), last_key=None, pages=0, rows=0, elapsed_seconds=0.0, output_hash=_EMPTY_HASH, output_state="new", stop_reason=None)
    try:
        value, _raw, _ = _read_json(path)
    except PilotError as exc:
        raise PilotError("run_budget_required") from exc
    expected = {"schema_version": "bond-pilot-calibration-checkpoint-v2", "run_id": run_id, "evidence_sha256": evidence.artifact_sha256, "approval_sha256": approval.artifact_sha256, "approval_authority_sha256": _approval_authority_hash(approval), "publication_sha256": evidence.values["publication_sha256"], "mode": mode, "series_ids": list(series), "seam": SEAM_NAME, "relation": V2_RELATION, "query_version": _QUERY_VERSION, "query_sha256": _QUERY_SHA256, "method_version": _METHOD_VERSION, "method_sha256": _METHOD_SHA256}
    if set(value) != _CHECKPOINT_KEYS or any(value.get(key) != item for key, item in expected.items()):
        raise PilotError("run_budget_required")
    pages, rows, elapsed = value["pages"], value["rows"], value["elapsed_seconds"]
    if not isinstance(pages, int) or isinstance(pages, bool) or not isinstance(rows, int) or isinstance(rows, bool) or pages < 0 or pages > budget.max_pages or rows < 0 or rows > budget.max_rows or not isinstance(elapsed, (int, float)) or isinstance(elapsed, bool) or not math.isfinite(elapsed) or elapsed < 0 or not _sha(value["output_hash"]):
        raise PilotError("run_budget_required")
    try:
        reports = _decode_reports(value["resolved_reports"], series) if value["resolved_reports"] else ()
        key = _decode_key(value["last_key"])
        if key is not None:
            canonical_date = _date(key[1])
            key = (key[0], canonical_date, *key[2:])
    except PilotError as exc:
        raise PilotError("run_budget_required") from exc
    state, reason = value["output_state"], value["stop_reason"]
    if state not in {"new", "in_progress", "complete", "budget_reached", "stopped"} or reason is not None and not isinstance(reason, str):
        raise PilotError("run_budget_required")
    if state == "new" and (pages or rows or reports or key or value["output_hash"] != _EMPTY_HASH or reason is not None):
        raise PilotError("run_budget_required")
    if state in {"in_progress", "budget_reached"} and (not rows or not pages or not reports or key is None or value["output_hash"] == _EMPTY_HASH):
        raise PilotError("run_budget_required")
    if state == "stopped" and (not isinstance(reason, str) or not reason or (rows and (not pages or not reports or key is None or value["output_hash"] == _EMPTY_HASH)) or (not rows and (pages or key is not None or value["output_hash"] != _EMPTY_HASH))):
        raise PilotError("run_budget_required")
    if state == "budget_reached" and (reason != "budget_reached" or (pages < budget.max_pages and rows < budget.max_rows)):
        raise PilotError("run_budget_required")
    if state == "complete" and (not pages or not reports or reason is not None):
        raise PilotError("run_budget_required")
    if state == "complete" and not rows and (pages != 1 or key is not None or value["output_hash"] != _EMPTY_HASH):
        raise PilotError("run_budget_required")
    if state == "complete" and rows and (key is None or value["output_hash"] == _EMPTY_HASH):
        raise PilotError("run_budget_required")
    if state == "in_progress" and rows != pages * budget.page_size:
        raise PilotError("run_budget_required")
    if state == "stopped" and rows and rows != pages * budget.page_size:
        raise PilotError("run_budget_required")
    if state == "budget_reached" and (pages != budget.max_pages or rows != budget.max_rows):
        raise PilotError("run_budget_required")
    if state == "complete" and not ((pages - 1) * budget.page_size <= rows < pages * budget.page_size):
        raise PilotError("run_budget_required")
    if key is not None and tuple(key[:4]) not in reports:
        raise PilotError("run_budget_required")
    value["resolved_reports"], value["last_key"] = reports, key
    return value


def _assert_read_only(connection: object) -> None:
    if getattr(connection, "read_only", True) is not True:
        raise PilotError("read_only_required")
    checker = getattr(connection, "assert_read_only", None)
    if callable(checker):
        checker()


def _timeout(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) == "57014" or error.__class__.__name__ == "QueryCanceled"


def _statement(cursor: object, connection: object, sql: str, params: tuple[object, ...] | None, remaining: Callable[[], float], timeout_seconds: int) -> None:
    _assert_read_only(connection)
    if remaining() < timeout_seconds:
        raise PilotError("calibration_timeout")
    try:
        cursor.execute(sql) if params is None else cursor.execute(sql, params)
    except BaseException as exc:
        if _timeout(exc):
            raise PilotError("calibration_timeout") from exc
        raise
    if remaining() < 0:
        raise PilotError("calibration_timeout")


def _scalar(row: object) -> object:
    if isinstance(row, Mapping):
        return next(iter(row.values()), None)
    return row[0] if isinstance(row, (tuple, list)) and row else row


def _walk(value: object) -> list[Mapping[str, object]]:
    output: list[Mapping[str, object]] = []
    if isinstance(value, Mapping):
        if isinstance(value.get("Node Type"), str):
            output.append(value)
        for child in value.values():
            output.extend(_walk(child))
    elif isinstance(value, list):
        for child in value:
            output.extend(_walk(child))
    return output


def _validate_plan(rows: object, *, resolver: bool, resume: bool, limit: int) -> None:
    raw = rows[0] if isinstance(rows, (tuple, list)) and len(rows) == 1 else rows
    if isinstance(raw, (tuple, list)) and len(raw) == 1:
        raw = raw[0]
    nodes = _walk(raw)
    target = [node for node in nodes if node.get("Relation Name") == "sec_nport_holdings_v2_current" and node.get("Schema") == "public"]
    if not target or any(node.get("Node Type") == "Seq Scan" for node in nodes) or not any("Index" in str(node.get("Node Type")) or "Partition" in str(node.get("Node Type")) for node in target) or any(not isinstance(node.get("Plan Rows"), int) or node["Plan Rows"] > limit for node in target):
        raise PilotError("unsafe_query_plan")
    text = " ".join(str(node.get(field, "")) for node in target for field in ("Index Cond", "Filter", "Recheck Cond", "Join Filter", "Sort Key"))
    required = ("series_id",) if resolver else _REPORT_COLUMNS + (FULL_KEY_COLUMNS if resume else ())
    if any(field not in text for field in required):
        raise PilotError("unsafe_query_plan")
    if resolver and any(field not in text for field in ("report_date", "publication_id", "accession_number")):
        raise PilotError("unsafe_query_plan")


def _decode_page(rows: object, last_key: tuple[str, ...] | None) -> tuple[tuple[dict[str, object], ...], tuple[str, ...] | None]:
    if not isinstance(rows, (tuple, list)):
        raise PilotError("nondeterministic_page")
    decoded: list[dict[str, object]] = []
    keys: list[tuple[str, ...]] = []
    for row in rows:
        if isinstance(row, Mapping):
            if set(row) != set(REQUIRED_COLUMNS):
                raise PilotError("nondeterministic_page")
            item = {field: row[field] for field in REQUIRED_COLUMNS}
        elif isinstance(row, (tuple, list)) and len(row) == len(REQUIRED_COLUMNS):
            item = dict(zip(REQUIRED_COLUMNS, row, strict=True))
        else:
            raise PilotError("nondeterministic_page")
        item["report_date"], item["filing_date"] = _date(item["report_date"]), _date(item["filing_date"])
        key = tuple(item[field] for field in FULL_KEY_COLUMNS)
        if any(not isinstance(value, str) or not value for value in key):
            raise PilotError("nondeterministic_page")
        decoded.append(item)
        keys.append(key)  # type: ignore[arg-type]
    if keys != sorted(keys) or len(keys) != len(set(keys)) or (last_key is not None and keys and keys[0] <= last_key):
        raise PilotError("nondeterministic_page")
    return tuple(decoded), keys[-1] if keys else last_key


def _write(path: Path, *, run_id: str, evidence: Phase4V2Evidence, approval: Phase4V2EvidenceApproval, mode: str, series: tuple[str, ...], reports: tuple[tuple[str, str, str, str], ...], key: tuple[str, ...] | None, pages: int, rows: int, elapsed: float, output_hash: str, state: str, reason: str | None) -> None:
    replace_checkpoint(path, canonical_json_bytes(_checkpoint_payload(run_id=run_id, evidence=evidence, approval=approval, mode=mode, series_ids=series, reports=reports, last_key=key, pages=pages, rows=rows, elapsed_seconds=elapsed, output_hash=output_hash, output_state=state, stop_reason=reason)))


def _page_hash(previous: str, page: tuple[dict[str, object], ...]) -> str:
    return hashlib.sha256(bytes.fromhex(previous) + canonical_json_bytes(page)).hexdigest()


def run_v2_calibration(connection: object, *, evidence: object, approval: object, series_ids: Sequence[str], mode: str, checkpoint_path: str | Path, run_id: str = "bond-pilot-calibration", clock: Callable[[], float] = monotonic) -> CalibrationResult:
    """Read the human-pinned immutable V2 publication under one bounded snapshot."""
    evidence, approval, series = _governance(evidence, approval, mode=mode, series_ids=series_ids)
    if not isinstance(run_id, str) or not run_id:
        raise PilotError("run_budget_required")
    budget = _BUDGETS[mode]
    path = Path(checkpoint_path)
    checkpoint = _load_checkpoint(path, evidence=evidence, approval=approval, mode=mode, series=series, run_id=run_id, budget=budget)
    pages, rows_read, elapsed_before, output_hash, reports, last_key = checkpoint["pages"], checkpoint["rows"], float(checkpoint["elapsed_seconds"]), checkpoint["output_hash"], checkpoint["resolved_reports"], checkpoint["last_key"]
    if checkpoint["output_state"] == "complete":
        _governance(evidence, approval, mode=mode, series_ids=series)
        return CalibrationResult((), pages, rows_read, False, last_key, mode)
    if checkpoint["output_state"] == "budget_reached":
        _governance(evidence, approval, mode=mode, series_ids=series)
        return CalibrationResult((), pages, rows_read, True, last_key, mode)
    started = clock()
    result_rows: list[dict[str, object]] = []

    def elapsed() -> float:
        return elapsed_before + clock() - started

    def remaining() -> float:
        return budget.wall_seconds - elapsed()

    _governance(evidence, approval, mode=mode, series_ids=series)  # immediate pre-connection rehash
    if connection is None:
        raise PilotError("phase4b_v2_unavailable")
    connection.read_only = True
    _assert_read_only(connection)
    terminal_checkpoint_written = False
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
                if _scalar(cursor.fetchone()) not in (V2_RELATION, V2_RELATION.rsplit(".", 1)[1]):
                    raise PilotError("phase4b_v2_unavailable")
                _statement(cursor, connection, COLUMNS_SQL, None, remaining, budget.statement_timeout_seconds)
                if not set(REQUIRED_COLUMNS).issubset(tuple(str(_scalar(row)) for row in cursor.fetchall())):
                    raise PilotError("phase4b_v2_unavailable")
                _statement(cursor, connection, EXPLAIN_RESOLVER_SQL, (series,), remaining, budget.statement_timeout_seconds)
                _validate_plan(cursor.fetchall(), resolver=True, resume=False, limit=len(series))
                _statement(cursor, connection, RESOLVER_SQL, (series,), remaining, budget.statement_timeout_seconds)
                resolved = _decode_reports(cursor.fetchall(), series)
                if reports and reports != resolved:
                    raise PilotError("phase4b_v2_unavailable")
                reports = resolved
                explained_initial = last_key is not None
                explained_resume = False
                while pages < budget.max_pages and rows_read < budget.max_rows:
                    limit = min(budget.page_size, budget.max_rows - rows_read)
                    dates, publications, accessions = tuple(item[1] for item in reports), tuple(item[2] for item in reports), tuple(item[3] for item in reports)
                    resume = last_key is not None
                    sql, explain = (RESUME_PAGE_SQL, EXPLAIN_RESUME_SQL) if resume else (INITIAL_PAGE_SQL, EXPLAIN_INITIAL_SQL)
                    params = (series, dates, publications, accessions, *(last_key or ()), limit)
                    if (resume and not explained_resume) or (not resume and not explained_initial):
                        _statement(cursor, connection, explain, params, remaining, budget.statement_timeout_seconds)
                        _validate_plan(cursor.fetchall(), resolver=False, resume=resume, limit=limit)
                        explained_initial = explained_initial or not resume
                        explained_resume = explained_resume or resume
                    _statement(cursor, connection, sql, params, remaining, budget.statement_timeout_seconds)
                    page, key = _decode_page(cursor.fetchall(), last_key)
                    if len(page) > limit:
                        raise PilotError("budget_drift")
                    pages += 1
                    if page:
                        rows_read += len(page)
                        last_key = key
                        output_hash = _page_hash(output_hash, page)
                        result_rows.extend(page)
                    complete = not page or len(page) < limit
                    state = "complete" if complete else "in_progress"
                    reason = None
                    if not complete and (pages >= budget.max_pages or rows_read >= budget.max_rows):
                        state, reason = "budget_reached", "budget_reached"
                    _write(path, run_id=run_id, evidence=evidence, approval=approval, mode=mode, series=series, reports=reports, key=last_key, pages=pages, rows=rows_read, elapsed=elapsed(), output_hash=output_hash, state=state, reason=reason)
                    if complete or state == "budget_reached":
                        terminal_checkpoint_written = True
                        _governance(evidence, approval, mode=mode, series_ids=series)  # rehash before success
                        return CalibrationResult(tuple(result_rows), pages, rows_read, state != "complete", last_key, mode)
                _write(path, run_id=run_id, evidence=evidence, approval=approval, mode=mode, series=series, reports=reports, key=last_key, pages=pages, rows=rows_read, elapsed=elapsed(), output_hash=output_hash, state="budget_reached", reason="budget_reached")
                terminal_checkpoint_written = True
                _governance(evidence, approval, mode=mode, series_ids=series)
                return CalibrationResult(tuple(result_rows), pages, rows_read, True, last_key, mode)
    except PilotError as exc:
        if terminal_checkpoint_written:
            raise
        _write(path, run_id=run_id, evidence=evidence, approval=approval, mode=mode, series=series, reports=reports, key=last_key, pages=pages, rows=rows_read, elapsed=elapsed(), output_hash=output_hash, state="stopped", reason=exc.code)
        raise

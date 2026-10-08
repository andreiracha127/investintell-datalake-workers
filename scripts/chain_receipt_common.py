"""One stable row projection and digest definition for both chain receipts.

No repository compute modules are imported here: callers supply the worker and
canonical hash function from the revision being measured.
"""

from __future__ import annotations

import datetime as dt

STABLE_COLUMNS = (
    "as_of", "quadrant", "candidate_quadrant", "status", "candidate_confidence",
    "growth_score", "inflation_score", "coverage_quality", "transition_pending",
    "basis", "pack_sha256", "chain_start",
)
EXCLUDED_COLUMNS = ("code_commit", "loaded_at")


def projection_metadata(worker) -> dict:
    columns = tuple(c for c in worker.ROW_COLUMNS if c not in EXCLUDED_COLUMNS)
    if columns != STABLE_COLUMNS:
        raise RuntimeError("worker row contract changed; review the receipt projection")
    return {
        "source": "worker.build_row",
        "columns": list(STABLE_COLUMNS),
        "excluded_columns": list(EXCLUDED_COLUMNS),
        "exclusion_reason": "commit and ingestion-time provenance vary by run",
        "dates": "ISO 8601 date strings",
        "floats": "Python JSON float representation without rounding or tolerance",
    }


def project_rows(worker, series, pack_sha256: str, revision: str) -> list[dict]:
    projection_metadata(worker)
    rows = []
    for decision in series:
        row = worker.build_row(
            decision, pack_sha256, revision,
            dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
        )
        if set(row) != set(worker.ROW_COLUMNS):
            raise RuntimeError("build_row and ROW_COLUMNS disagree")
        rows.append({
            column: value.isoformat() if isinstance(value, dt.date) else value
            for column, value in row.items() if column in STABLE_COLUMNS
        })
    return rows


def row_digests(rows: list[dict], canonical_json_sha256) -> dict:
    if not rows:
        raise RuntimeError("refusing an empty chain replay")
    return {
        "row_count": len(rows),
        "first_month": rows[0]["as_of"],
        "last_month": rows[-1]["as_of"],
        "all_months_sha256": canonical_json_sha256(rows),
        "latest_month_sha256": canonical_json_sha256(rows[-1:]),
        "latest_row": rows[-1],
    }


def hash_definition() -> dict:
    return {
        "function": "src.input_packs.hashing.canonical_json_sha256",
        "algorithm": "SHA-256",
        "encoding": "UTF-8 sorted-key compact JSON; no newline; NaN/Inf rejected",
        "all_months_payload": "ordered list of all projected rows",
        "latest_month_payload": "one-element list containing the final projected row",
    }

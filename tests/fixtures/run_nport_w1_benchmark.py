"""Reproducible W1 N-PORT semantic-validation benchmark.

This is deliberately an evidence harness, not a pytest test.  It compares the
exact governed derivation with an explicitly unsafe comparator that checks JSON
shape only and therefore disables semantic re-derivation.  Both predicates run
against the same repeatable-read snapshot and row-prefix boundaries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import statistics
import time
from typing import Any

import psycopg


UNSAFE_SQL = """
SELECT count(*)
FROM nport_raw_rows r
JOIN nport_contract_tables c ON c.source_table = r.source_table
WHERE r.ingestion_run_id = %(run_id)s
  AND r.raw_row_id <= %(cutoff)s
  AND (
    jsonb_typeof(r.original_lexical_row) <> 'object'
    OR jsonb_typeof(r.typed_projection) <> 'object'
    OR EXISTS (
      SELECT 1 FROM jsonb_object_keys(r.original_lexical_row) key
      WHERE NOT key = ANY(c.columns)
    )
    OR EXISTS (
      SELECT 1 FROM unnest(c.columns) key
      WHERE NOT r.original_lexical_row ? key OR NOT r.typed_projection ? key
    )
  )
""".strip()

SAFE_SQL = """
SELECT count(*)
FROM nport_raw_rows r
JOIN nport_contract_tables c ON c.source_table = r.source_table
CROSS JOIN LATERAL (
  SELECT nport_expected_row(r.original_lexical_row, c.column_specs) AS row
) expected
WHERE r.ingestion_run_id = %(run_id)s
  AND r.raw_row_id <= %(cutoff)s
  AND (
    jsonb_typeof(r.original_lexical_row) <> 'object'
    OR jsonb_typeof(r.typed_projection) <> 'object'
    OR expected.row IS NULL
    OR r.typed_projection IS DISTINCT FROM expected.row->'typed_projection'
    OR r.parse_errors IS DISTINCT FROM expected.row->'parse_errors'
    OR r.parse_status IS DISTINCT FROM expected.row->>'parse_status'
  )
""".strip()


def _timed(cur: psycopg.Cursor[Any], sql: str, params: dict[str, Any]) -> tuple[float, int]:
    started = time.perf_counter()
    cur.execute(sql, params)
    count = int(cur.fetchone()[0])
    return time.perf_counter() - started, count


def _relation_scans(plan: Any, relation: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("Relation Name") == relation:
                found.append({
                    key: node[key]
                    for key in ("Node Type", "Relation Name", "Alias", "Plan Rows")
                    if key in node
                })
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    visit(plan)
    return found


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--statement-timeout-ms", type=int, default=1_800_000)
    args = parser.parse_args()

    evidence: dict[str, Any] = {
        "methodology": {
            "snapshot": "one REPEATABLE READ, READ ONLY transaction",
            "fractions": [25, 50, 100],
            "runs": "one warmup plus three measured executions per predicate/fraction",
            "ordering": "unsafe warmup, safe warmup, then alternating unsafe/safe trials",
            "unsafe_provenance": (
                "reconstructed semantic-check-disabled comparator; no historical HEAD exists "
                "because the W1 implementation is untracked"
            ),
            "acceptance": {"T50/T25_max": 2.5, "T100/T50_max": 2.5, "safe100/unsafe100_max": 2.0},
        },
        "sql_sha256": {
            "unsafe": hashlib.sha256(UNSAFE_SQL.encode()).hexdigest(),
            "safe": hashlib.sha256(SAFE_SQL.encode()).hexdigest(),
        },
        "sql": {"unsafe": UNSAFE_SQL, "safe": SAFE_SQL},
        "host": {"python": platform.python_version(), "platform": platform.platform()},
        "run_id": args.run_id,
        "statement_timeout_ms": args.statement_timeout_ms,
    }

    with psycopg.connect(args.database_url) as conn:
        conn.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, true)", (str(args.statement_timeout_ms),))
            cur.execute("SELECT version(), current_setting('max_locks_per_transaction'), current_setting('max_connections')")
            version, locks, connections = cur.fetchone()
            evidence["database"] = {
                "version": version,
                "max_locks_per_transaction": locks,
                "max_connections": connections,
            }
            cur.execute(
                """SELECT count(*), min(raw_row_id), max(raw_row_id)
                   FROM nport_raw_rows WHERE ingestion_run_id=%s""",
                (args.run_id,),
            )
            total, minimum, maximum = map(int, cur.fetchone())
            evidence["snapshot"] = {"rows": total, "min_raw_row_id": minimum, "max_raw_row_id": maximum}
            expected_total = 40_276_320
            if total != expected_total:
                raise RuntimeError(f"expected {expected_total} rows, found {total}")

            sizes = {25: total // 4, 50: total // 2, 100: total}
            boundaries: dict[int, int] = {}
            for fraction, count in sizes.items():
                cur.execute(
                    """SELECT raw_row_id FROM nport_raw_rows
                       WHERE ingestion_run_id=%s ORDER BY raw_row_id OFFSET %s LIMIT 1""",
                    (args.run_id, count - 1),
                )
                boundaries[fraction] = int(cur.fetchone()[0])
                cur.execute(
                    """SELECT count(*) FROM nport_raw_rows
                       WHERE ingestion_run_id=%s AND raw_row_id<=%s""",
                    (args.run_id, boundaries[fraction]),
                )
                actual = int(cur.fetchone()[0])
                if actual != count:
                    raise RuntimeError(f"{fraction}% boundary contains {actual}, expected {count}")
            evidence["boundaries"] = {
                str(fraction): {"rows": sizes[fraction], "raw_row_id_lte": boundaries[fraction]}
                for fraction in sizes
            }

            params_100 = {"run_id": args.run_id, "cutoff": boundaries[100]}
            cur.execute("EXPLAIN (FORMAT JSON, COSTS true) " + SAFE_SQL, params_100)
            plan = cur.fetchone()[0][0]
            evidence["safe_explain"] = {
                "plan": plan,
                "nport_raw_rows_scan_nodes": _relation_scans(plan, "nport_raw_rows"),
            }

            results: dict[str, Any] = {}
            try:
                for fraction in (25, 50, 100):
                    params = {"run_id": args.run_id, "cutoff": boundaries[fraction]}
                    unsafe_warmup, unsafe_count = _timed(cur, UNSAFE_SQL, params)
                    safe_warmup, safe_count = _timed(cur, SAFE_SQL, params)
                    unsafe_trials: list[float] = []
                    safe_trials: list[float] = []
                    for _ in range(3):
                        elapsed, count = _timed(cur, UNSAFE_SQL, params)
                        if count != unsafe_count:
                            raise RuntimeError("unsafe result changed inside repeatable-read snapshot")
                        unsafe_trials.append(elapsed)
                        elapsed, count = _timed(cur, SAFE_SQL, params)
                        if count != safe_count:
                            raise RuntimeError("safe result changed inside repeatable-read snapshot")
                        safe_trials.append(elapsed)
                    results[str(fraction)] = {
                        "rows": sizes[fraction],
                        "unsafe": {
                            "warmup_seconds": unsafe_warmup,
                            "trials_seconds": unsafe_trials,
                            "median_seconds": statistics.median(unsafe_trials),
                            "mismatches": unsafe_count,
                        },
                        "safe": {
                            "warmup_seconds": safe_warmup,
                            "trials_seconds": safe_trials,
                            "median_seconds": statistics.median(safe_trials),
                            "mismatches": safe_count,
                        },
                    }
            except psycopg.errors.QueryCanceled as error:
                evidence["status"] = "blocked_timeout"
                evidence["timeout_error"] = str(error)
                evidence["results"] = results
            else:
                evidence["status"] = "measured"
                evidence["results"] = results
                safe25 = results["25"]["safe"]["median_seconds"]
                safe50 = results["50"]["safe"]["median_seconds"]
                safe100 = results["100"]["safe"]["median_seconds"]
                unsafe100 = results["100"]["unsafe"]["median_seconds"]
                ratios = {
                    "T50_over_T25": safe50 / safe25,
                    "T100_over_T50": safe100 / safe50,
                    "safe100_over_unsafe100": safe100 / unsafe100,
                }
                evidence["ratios"] = ratios
                evidence["gates"] = {
                    "T50_over_T25": ratios["T50_over_T25"] <= 2.5,
                    "T100_over_T50": ratios["T100_over_T50"] <= 2.5,
                    "safe100_over_unsafe100": ratios["safe100_over_unsafe100"] <= 2.0,
                }
    print(json.dumps(evidence, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()

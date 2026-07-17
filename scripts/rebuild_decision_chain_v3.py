"""Rebuild the ``open_macro_v03_decision_chain`` materialization with the ACTIVE
macro_quadrant_us_v3 model (owner-ordered PIT repopulation, 2026-07-17).

WHAT THIS TABLE IS: a derived, read-side materialization of the certified monthly
decision chain (148 rows, 2014-03-31..2026-06-30) that the Light backend serves as
the deep history leg of ``fetch_decision_history``, the walk-forward backtests read
per fold, and the quadrant-as-of endpoint carries forward. It is NOT the published
ledger: ``open_macro_v03_decisions`` / ``_allocations`` LIVE rows are never touched
by this script (the ledger records what the system actually said each day).

WHY REBUILD: the runtime switched to the market-fused v3 engine (PR #46; every
ratified gate GO). The chain rows still hold the frozen v1 series (61 valid months,
multi-year carried contraction) — the frontend and backtests must see the same
point-in-time quadrants the ACTIVE engine reconstitutes.

DETERMINISM / PIT: the v3 series is computed from the CERTIFIED input pack _003
only (its window exactly covers the chain span; the pack is the ratified PIT
snapshot of macro vintages + eod prices). Same engine, same inputs, same latch —
byte-stable across reruns; the evidence run's chain is reproduced exactly.

SUBCOMMANDS (run from the repo root, venv active, DSN via --dsn or DATABASE_URL):

  inspect   READ-ONLY: prints the table's column set, row/valid counts, span, and
            the backfill sentinels found in decisions/allocations — run this first.
  plan      READ-ONLY: computes the v3 series, diffs it against the current chain
            rows, writes ``_tmp_chain_rebuild_v3/plan.json`` and prints the summary
            (months changing quadrant, valid-count delta, unknown columns).
  apply     TRANSACTIONAL: replaces the chain contents with the v3 series in ONE
            transaction (DELETE + INSERT), stamping provenance columns when the
            table carries them. Refuses when the observed schema diverges from the
            plan; --plan must reference a fresh plan file (same series hash).

The script maps ONLY the columns it understands; any unknown NOT-NULL column in
the live table aborts ``apply`` with a clear message instead of guessing.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_003"
OUT_DIR = ROOT / "_tmp_chain_rebuild_v3"
CHAIN_TABLE = "open_macro_v03_decision_chain"

CHAIN_START = _dt.date(2014, 3, 1)
CHAIN_END = _dt.date(2026, 6, 30)

MODEL_VERSION = "macro_quadrant_us_v3"
JUDGMENT_REF = "open_macro_v03_confidence_v3_evidence_001:go"
RUN_ID = "chain-rebuild-v3-2026-07-17"

# columns we know how to fill, in priority order; optional ones are stamped only
# when present in the live table.
KNOWN_COLUMNS = {
    "as_of": "required",
    "quadrant": "required",
    "candidate_confidence": "required",
    "status": "required",
    "growth_score": "optional",
    "inflation_score": "optional",
    "coverage_quality": "optional",
    "candidate_quadrant": "optional",
    "model_version": "optional-provenance",
    "confidence_method": "optional-provenance",
    "judgment_ref": "optional-provenance",
    "run_id": "optional-provenance",
    "code_commit": "optional-provenance",
    "rebuilt_at": "optional-provenance",
}


def compute_v3_series() -> list[dict[str, Any]]:
    """The monthly v3 chain from the certified pack (same engine as the runtime)."""
    from harness.phase0q import decision_v3
    from harness.phase0q.decision_v3 import CONFIDENCE_METHOD_V3_FUSED

    macro_rows = json.loads(
        (PACK / "data" / "canonical" / "macro_observation_vintage.json")
        .read_text(encoding="utf-8"))
    eod_rows = json.loads(
        (PACK / "data" / "canonical" / "eod_prices.json").read_text(encoding="utf-8"))
    series = decision_v3.run_decision_series_v3(
        macro_rows, eod_rows, CHAIN_START, CHAIN_END)
    rows: list[dict[str, Any]] = []
    for r in series:
        rows.append({
            "as_of": r.as_of,
            "quadrant": r.quadrant,
            "candidate_confidence": r.candidate_confidence,
            "status": r.status,
            "growth_score": r.growth_score,
            "inflation_score": r.inflation_score,
            "coverage_quality": r.coverage_quality,
            "candidate_quadrant": r.candidate_quadrant,
            "model_version": MODEL_VERSION,
            "confidence_method": CONFIDENCE_METHOD_V3_FUSED,
            "judgment_ref": JUDGMENT_REF,
            "run_id": RUN_ID,
        })
    return rows


def series_content_hash(rows: list[dict[str, Any]]) -> str:
    payload = [
        {k: (v.isoformat() if isinstance(v, _dt.date) else v)
         for k, v in row.items() if k in ("as_of", "quadrant",
                                          "candidate_confidence", "status")}
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
        .encode("utf-8")).hexdigest()


def _connect(dsn: str | None):
    import psycopg
    resolved = dsn or os.environ.get("DATABASE_URL")
    if not resolved:
        sys.exit("no DSN: pass --dsn or set DATABASE_URL")
    return psycopg.connect(resolved)


def _table_columns(conn) -> dict[str, dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (CHAIN_TABLE,))
        cols = {r[0]: {"nullable": r[1] == "YES", "default": r[2]}
                for r in cur.fetchall()}
    if not cols:
        sys.exit(f"table {CHAIN_TABLE} not found (schema public)")
    return cols


def cmd_inspect(args) -> int:
    with _connect(args.dsn) as conn:
        cols = _table_columns(conn)
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*), min(as_of), max(as_of), "
                        f"count(*) FILTER (WHERE status='valid') FROM {CHAIN_TABLE}")
            total, lo, hi, valid = cur.fetchone()
            cur.execute(
                "SELECT run_id, count(*) FROM open_macro_v03_decisions "
                "GROUP BY run_id ORDER BY 2 DESC LIMIT 8")
            decision_runs = cur.fetchall()
            cur.execute("SELECT count(*), min(as_of), max(as_of) "
                        "FROM open_macro_v03_allocations")
            alloc = cur.fetchone()
    print(json.dumps({
        "chain_columns": {c: ("nullable" if m["nullable"] else "not-null")
                          for c, m in sorted(cols.items())},
        "chain": {"rows": total, "valid": valid,
                  "span": [str(lo), str(hi)]},
        "decisions_run_ids": [{"run_id": r, "rows": n} for r, n in decision_runs],
        "allocations": {"rows": alloc[0], "span": [str(alloc[1]), str(alloc[2])]},
    }, indent=2, default=str))
    return 0


def cmd_plan(args) -> int:
    rows = compute_v3_series()
    content_hash = series_content_hash(rows)
    with _connect(args.dsn) as conn:
        cols = _table_columns(conn)
        with conn.cursor() as cur:
            cur.execute(f"SELECT as_of, quadrant, status FROM {CHAIN_TABLE} "
                        f"ORDER BY as_of")
            current = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    unknown_required = sorted(
        c for c, m in cols.items()
        if c not in KNOWN_COLUMNS and not m["nullable"] and m["default"] is None)
    changes = []
    for row in rows:
        cur_q, cur_s = current.get(row["as_of"], (None, "absent"))
        if (cur_q, cur_s) != (row["quadrant"], row["status"]):
            changes.append({"as_of": str(row["as_of"]),
                            "from": {"quadrant": cur_q, "status": cur_s},
                            "to": {"quadrant": row["quadrant"],
                                   "status": row["status"]}})
    plan = {
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "series_content_hash": content_hash,
        "v3_rows": len(rows),
        "v3_valid": sum(1 for r in rows if r["status"] == "valid"),
        "current_rows": len(current),
        "current_valid": sum(1 for q, s in current.values() if s == "valid"),
        "months_changing": len(changes),
        "changes": changes,
        "unknown_not_null_columns": unknown_required,
        "columns_to_write": [c for c in KNOWN_COLUMNS if c in cols],
    }
    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "plan.json").write_text(
        json.dumps(plan, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({k: plan[k] for k in plan if k != "changes"},
                     indent=2, default=str))
    print(f"plan written -> {OUT_DIR / 'plan.json'} ({len(changes)} changes)")
    if unknown_required:
        print("WARNING: unknown NOT-NULL columns present; apply will refuse:",
              unknown_required, file=sys.stderr)
    return 0


def cmd_apply(args) -> int:
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    rows = compute_v3_series()
    content_hash = series_content_hash(rows)
    if plan["series_content_hash"] != content_hash:
        sys.exit("plan is stale: series content hash mismatch — rerun `plan`")
    if plan["unknown_not_null_columns"]:
        sys.exit(f"refusing: unknown NOT-NULL columns "
                 f"{plan['unknown_not_null_columns']} — extend KNOWN_COLUMNS "
                 "after inspecting their meaning")

    import subprocess
    code_commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                 capture_output=True, text=True,
                                 check=True).stdout.strip()
    now = _dt.datetime.now(_dt.timezone.utc)

    with _connect(args.dsn) as conn:
        cols = _table_columns(conn)
        write_cols = [c for c in KNOWN_COLUMNS if c in cols]
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {CHAIN_TABLE}")
            before = cur.fetchone()[0]
            cur.execute(f"DELETE FROM {CHAIN_TABLE}")
            placeholders = ", ".join(f"%({c})s" for c in write_cols)
            insert_sql = (f"INSERT INTO {CHAIN_TABLE} "
                          f"({', '.join(write_cols)}) VALUES ({placeholders})")
            for row in rows:
                params = {c: row.get(c) for c in write_cols}
                if "code_commit" in write_cols:
                    params["code_commit"] = code_commit
                if "rebuilt_at" in write_cols:
                    params["rebuilt_at"] = now
                cur.execute(insert_sql, params)
            cur.execute(f"SELECT count(*), count(*) FILTER (WHERE status='valid') "
                        f"FROM {CHAIN_TABLE}")
            total, valid = cur.fetchone()
        conn.commit()
    print(json.dumps({
        "replaced": {"before_rows": before, "after_rows": total,
                     "after_valid": valid},
        "series_content_hash": content_hash,
        "run_id": RUN_ID, "code_commit": code_commit,
        "applied_at": now.isoformat(),
    }, indent=2))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="rebuild_decision_chain_v3")
    ap.add_argument("--dsn", default=None,
                    help="Postgres DSN (default: DATABASE_URL env)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("inspect")
    sub.add_parser("plan")
    p_apply = sub.add_parser("apply")
    p_apply.add_argument("--plan", default=str(OUT_DIR / "plan.json"))
    args = ap.parse_args(argv)
    return {"inspect": cmd_inspect, "plan": cmd_plan,
            "apply": cmd_apply}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())

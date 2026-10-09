"""Reproduce W1b coverage using a local load and bounded read-only production exports.

Production exports use psql/mcp_ro and never read universe_constituents.cik.
Run --export-only once, then replay --local-dsn against the saved snapshots.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess

YEARS = (2007, 2008, 2010, 2012, 2013, 2014, 2015, 2016, 2017, 2018)
READ_OPTIONS = "-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=5000"


def _check_query_window() -> None:
    now = dt.datetime.now(dt.timezone.utc)
    if dt.time(6) <= now.time().replace(tzinfo=None) < dt.time(8, 30):
        raise RuntimeError("Production exports are outside the allowed query window")


def _complete_csv(text: str, header: str) -> bool:
    """psql writes the header line first and ends every row with a newline."""
    return text.startswith(header + "\n") and text.endswith("\n")


def _reusable_export(path: Path, header: str) -> bool:
    try:
        return _complete_csv(path.read_text(encoding="utf-8"), header)
    except (OSError, UnicodeDecodeError):
        return False


def _write_atomic(path: Path, text: str) -> None:
    """An interrupted write leaves only the staged file, never a partial export."""
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(text, encoding="utf-8", newline="\n")
    os.replace(staged, path)


def export_production(psql: str, target: Path) -> None:
    """Save price eligibility and cover states, without a present-day CIK prior."""
    _check_query_window()
    target.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "PGOPTIONS": READ_OPTIONS, "PGCONNECT_TIMEOUT": "15", "PGSSLMODE": "disable"}
    base = [psql, "-X", "-q", "-w", "-h", "127.0.0.1", "-p", "65432", "-U", "mcp_ro",
            "-d", "market", "-v", "ON_ERROR_STOP=1"]
    identity = subprocess.run(base + ["-At", "-c", "SELECT current_user || ',' || "
        "current_setting('transaction_read_only') || ',' || current_setting('statement_timeout')"],
        env=env, check=True, capture_output=True, text=True).stdout.strip()
    if identity != "mcp_ro,on,30s":
        raise RuntimeError("Unexpected production identity/read-only/timeout settings")
    queries = {"first_prices.csv": "SELECT u.ticker, min(p.bucket)::date AS first_price "
        "FROM universe_constituents u LEFT JOIN cagg_eod_monthly p ON p.ticker=u.ticker "
        "GROUP BY u.ticker ORDER BY u.ticker"}
    queries.update({f"cover_{y}.csv": "SELECT u.ticker, f.status, f.cik AS cover_cik "
        "FROM universe_constituents u CROSS JOIN LATERAL "
        f"sec_ticker_issuer_at(u.ticker, DATE '{y}-12-31') f ORDER BY u.ticker" for y in YEARS})
    for name, query in queries.items():
        _check_query_window()
        header = "ticker,first_price" if name == "first_prices.csv" else "ticker,status,cover_cik"
        # Only a complete earlier export is resumed; anything else is exported again.
        if _reusable_export(target / name, header):
            print(json.dumps({"reused_export": name}), flush=True)
            continue
        result = subprocess.run(base + ["-c", f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"],
            env=env, check=False, capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(f"Production psql export {name} failed: {result.stderr.strip()}")
        if not _complete_csv(result.stdout, header):
            raise RuntimeError(f"Production psql export {name} is not a complete CSV")
        _write_atomic(target / name, result.stdout)
        print(json.dumps({"export": name, "rows": len(result.stdout.splitlines()) - 1}), flush=True)
    files = [target / name for name in queries]
    _write_atomic(target / "production_snapshot.json", json.dumps({
        "captured_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "first_exported_at": dt.datetime.fromtimestamp(min(p.stat().st_mtime for p in files), dt.timezone.utc).isoformat(),
        "identity": identity, "pgoptions": READ_OPTIONS,
        "universe_columns": ["ticker"], "years": YEARS,
        "sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}}, indent=2) + "\n")


def validate(local_dsn: str, target: Path) -> dict:
    import psycopg
    with (target / "first_prices.csv").open(encoding="utf-8", newline="") as fh:
        prices = {r["ticker"]: r["first_price"] or None for r in csv.DictReader(fh)}
    cover = []
    for y in YEARS:
        with (target / f"cover_{y}.csv").open(encoding="utf-8", newline="") as fh:
            cover.extend((y, r["ticker"], r["status"], r["cover_cik"] or None,
                          prices[r["ticker"]]) for r in csv.DictReader(fh))
    with psycopg.connect(local_dsn, autocommit=True) as conn:
        conn.execute("CREATE TEMP TABLE validation_cover (year int, ticker text, "
                     "status text, cover_cik bigint, first_price date)")
        with conn.cursor().copy("COPY validation_cover FROM STDIN") as copy:
            for row in cover:
                copy.write_row(row)
        conn.execute("ANALYZE validation_cover")
        coverage = []
        for y in (2007, 2008, 2010, 2013, 2016):
            d = dt.date(y, 12, 31)
            row = conn.execute("""SELECT count(*) eligible,
                count(*) FILTER (WHERE f.status='resolved') resolved,
                count(*) FILTER (WHERE f.status='ambiguous') ambiguous,
                count(*) FILTER (WHERE f.status='none') none
                FROM validation_cover c CROSS JOIN LATERAL
                  sec_insider_ticker_issuer_at(c.ticker, %s) f
                WHERE c.year=%s AND c.status='missing' AND c.first_price <= %s""",
                (d, y, d)).fetchone()
            coverage.append(dict(zip(("year", "eligible", "resolved", "ambiguous", "none"),
                                     (y, *row))))
            coverage[-1]["resolved_pct"] = round(100 * row[1] / row[0], 2) if row[0] else None
            print(json.dumps(coverage[-1]), flush=True)
        agreement = []
        conflicts = []
        for y in range(2012, 2019):
            d = dt.date(y, 12, 31)
            row = conn.execute("""SELECT count(*) cover_resolved,
                count(*) FILTER (WHERE f.status='resolved') insider_resolved,
                count(*) FILTER (WHERE f.status='resolved' AND f.cik=c.cover_cik) agrees,
                count(*) FILTER (WHERE f.status='resolved' AND f.cik<>c.cover_cik) disagrees
                FROM validation_cover c CROSS JOIN LATERAL
                  sec_insider_ticker_issuer_at(c.ticker, %s) f
                WHERE c.year=%s AND c.status='resolved'""", (d, y)).fetchone()
            agreement.append(dict(zip(("year", "cover_resolved", "insider_resolved", "agrees", "disagrees"),
                                      (y, *row))))
            # Validate clean, non-overlapping CIK runs against cover evidence.
            # Select content versions before the final symbol check, as in the resolver.
            grouped = defaultdict(list)
            rows = conn.execute("""WITH targets AS (
                SELECT ticker, cover_cik FROM validation_cover WHERE year=%s AND status='resolved'
            ), matching AS MATERIALIZED (
                SELECT DISTINCT c.ticker, c.cover_cik, f.accession
                FROM targets c CROSS JOIN LATERAL (
                    WITH key_versions AS MATERIALIZED (
                        SELECT accession, filed, available_on, retired_on FROM sec_insider_filings
                        WHERE ticker_keys @> ARRAY[regexp_replace(upper(c.ticker), '[^A-Z0-9]', '', 'g')]
                    ) SELECT accession FROM key_versions
                    WHERE filed >= %s::date - 365 AND filed < %s
                      AND available_on <= %s AND (retired_on IS NULL OR retired_on > %s)
                ) f
            ), visible AS (
                SELECT c.ticker, c.cover_cik, f.accession, f.cik, f.filed
                FROM matching c CROSS JOIN LATERAL (
                    SELECT v.* FROM sec_insider_filings v WHERE v.accession=c.accession
                      AND v.available_on <= %s AND (v.retired_on IS NULL OR v.retired_on > %s)
                    ORDER BY v.available_on DESC, v.loaded_on DESC, v.id DESC LIMIT 1
                ) f WHERE f.ticker_keys @> ARRAY[regexp_replace(upper(c.ticker), '[^A-Z0-9]', '', 'g')]
                  AND f.filed >= %s::date - 365 AND f.filed < %s
            ) SELECT ticker, cover_cik, cik, count(*) n, count(DISTINCT filed) nd,
                     min(filed), max(filed) FROM visible GROUP BY 1,2,3 ORDER BY 1,3""",
                (y, d, d, d, d, d, d, d, d)).fetchall()
            for ticker, cover_cik, cik, n, nd, first, last in rows:
                grouped[(ticker, cover_cik)].append((cik, n, nd, first, last))
            for (ticker, cover_cik), candidates in grouped.items():
                if len(candidates) < 2:
                    continue
                ranked = sorted(candidates, key=lambda c: (-c[1], c[0]))
                dominant = (ranked[0][0] if ranked[0][1] >= 3 * ranked[1][1]
                            and ranked[0][1] >= 2 and ranked[0][2] >= 2 else None)
                runs = sorted(candidates, key=lambda c: (c[3], c[0]))
                separated = all(a[4] < b[3] for a, b in zip(runs, runs[1:]))
                handover = runs[-1][0] if separated and runs[-1][1] >= 2 and runs[-1][2] >= 2 else None
                conflicts.append({"year": y, "ticker": ticker, "cover_cik": cover_cik,
                    "candidates": candidates, "dominant_cik": dominant,
                    "handover_cik": handover, "handover_fallback": handover if dominant is None else None})
        def policy_counts(key):
            fires = [r for r in conflicts if r[key] is not None]
            agrees = sum(r[key] == r["cover_cik"] for r in fires)
            return {"fires": len(fires), "agrees": agrees, "disagrees": len(fires)-agrees}
        result = {"coverage": coverage, "cover_agreement": agreement,
                  "cover_conflicts": {"ticker_years": len(conflicts),
                      "dominance": policy_counts("dominant_cik"),
                      "handover_all": policy_counts("handover_cik"),
                      "handover_fallback": policy_counts("handover_fallback")},
                  "conflict_details": conflicts}
        print(json.dumps(result["cover_conflicts"]), flush=True)
        (target / "coverage.json").write_text(json.dumps(result, indent=2, default=str) + "\n",
                                               encoding="utf-8", newline="\n")
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshots", type=Path, required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--export-only", action="store_true")
    parser.add_argument("--local-dsn")
    args = parser.parse_args()
    if args.export_only:
        export_production(args.psql, args.snapshots)
    else:
        if not args.local_dsn:
            parser.error("--local-dsn is required to validate saved snapshots")
        validate(args.local_dsn, args.snapshots)


if __name__ == "__main__":
    main()

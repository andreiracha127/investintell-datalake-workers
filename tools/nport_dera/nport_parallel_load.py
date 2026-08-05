"""Parallel bulk-load of parsed N-PORT CSVs into ``sec_nport_holdings``.

PROVENANCE
----------
Rescued verbatim from ``E:\\investintell-allocation\\scripts\\nport_parallel_load.py``
(mtime 2026-06-11 20:11), an untracked operator script. Every one of the
96,398,851 rows in ``sec_nport_holdings`` was written by it: the table's
``created_at`` holds exactly three timestamps, all on 2026-06-11, one per run.

THE TRAP, WHICH IS THE WHOLE REASON THIS FILE IS NOW UNDER GIT
--------------------------------------------------------------
The insert is ``ON CONFLICT (report_date, series_id, cusip) DO NOTHING``. That
makes a re-run idempotent, which is what it was designed for — and it also makes
a re-run **incapable of repairing anything**. When a package loads with a short
``HOLDING_ID -> ISIN`` map, its rows arrive keyed on ``LE:<lei>`` instead of
``IS:<isin>``. Those wrong rows now OWN the primary key. Re-parsing the package
correctly and re-running this loader inserts zero rows, logs zero errors, exits
zero, and changes nothing. The job is green and the data is still broken. (Same
shape as ``bond-serving-republication`` and ``railway-redeploy-nao-executa-worker``.)

A repair is therefore always: DELETE the affected report_dates → reload every
package that contributes rows to them → verify the ISIN fill. ``--delete-first``
below encodes that, and refuses to run unscoped.

``LE:<lei>`` is also not unique per security, so the collapse is lossy on top of
being wrong: 270,020 rows of 2023q4/2025q1 never made it into the table at all
because same-issuer holdings inside one series folded onto one key.

WHAT WAS ADDED to the rescued original, and nothing else:

* ``--only-report-dates`` — a scope guard at the DB boundary, independent of the
  one in the parser. The INSERT will not touch a report_date outside the list.
* ``--delete-first`` — the mandatory DELETE, allowed only together with an
  explicit report_date list.
* ``prep()`` decompresses **only the chunks that overlap the target dates**
  instead of the whole hypertable. The original decompressed all 27 chunks of a
  96M-row table; on a volume with 240 GB free that is a real hazard, and it is
  unnecessary when the write is scoped to four chunks.
* ``finalize()`` re-compresses exactly the chunks ``prep()`` opened, rather than
  leaving the table uncompressed until a background policy job gets to it.
* ``verify_isin_fill()`` — a post-load check, with a configurable floor. Its
  absence is why two bad quarters went unnoticed for one and two years.

Lifecycle:
  1. prep()      — drop compression policy + decompress the affected chunks so
                   inserts land in the row store.
  2. delete      — only with --delete-first; without it a repair is a no-op.
  3. workers     — N threads, each: own connection + own session-local TEMP stage,
                   COPY one CSV, INSERT ... SELECT ON CONFLICT DO NOTHING.
  4. finalize()  — re-compress the opened chunks, restore the compression policy.
  5. verify      — ISIN fill per report_date against the floor.

COPY releases the GIL on socket I/O, so threads give real network parallelism.
No advisory lock (it would serialize the workers); rely on idempotency for safety.

Usage:
  python -m tools.nport_dera.nport_parallel_load --seed-dir DIR --dsn DSN --workers 8 \
      --only-report-dates 2023-09-30,2023-10-31 --delete-first --skip-matview
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import glob
import os
import sys
import threading

import psycopg

TABLE = "sec_nport_holdings"

CSV_COLS = [
    "report_date", "cik", "cusip", "isin", "issuer_name", "asset_class",
    "sector", "market_value", "quantity", "currency", "pct_of_nav",
    "is_restricted", "fair_value_level", "series_id",
]
PLACEHOLDER_CUSIP = "000000000"

#: See ``tools.nport_dera.nport_bulk_parse.DEFAULT_FILL_FLOOR`` and
#: ``src/workers/nport_identifier_coverage.DEFAULT_FLOOR`` — same number, same
#: 28 pp of daylight between the worst clean and the best degraded reading.
DEFAULT_FILL_FLOOR = 0.90

STAGE_DDL = """
CREATE TEMP TABLE _nport_stage (
  report_date date, cik text, cusip text, isin text, issuer_name text,
  asset_class text, sector text, market_value bigint, quantity numeric,
  currency text, pct_of_nav numeric, is_restricted boolean,
  fair_value_level text, series_id text
) ON COMMIT DROP
"""

INSERT_SQL = f"""
INSERT INTO {TABLE} ({', '.join(CSV_COLS)}, cik_padded, created_at)
SELECT {', '.join(CSV_COLS)}, lpad(cik, 10, '0'), %(ts)s
FROM _nport_stage
WHERE report_date <= current_date AND series_id IS NOT NULL
ON CONFLICT (report_date, series_id, cusip) DO NOTHING
"""

INSERT_SCOPED_SQL = f"""
INSERT INTO {TABLE} ({', '.join(CSV_COLS)}, cik_padded, created_at)
SELECT {', '.join(CSV_COLS)}, lpad(cik, 10, '0'), %(ts)s
FROM _nport_stage
WHERE report_date <= current_date AND series_id IS NOT NULL
  AND report_date = ANY(%(dates)s::date[])
ON CONFLICT (report_date, series_id, cusip) DO NOTHING
"""

AFFECTED_CHUNKS_SQL = """
SELECT format('%%I.%%I', chunk_schema, chunk_name)
FROM timescaledb_information.chunks
WHERE hypertable_name = %(table)s
  AND is_compressed
  AND range_end > %(lo)s::timestamptz
  AND range_start <= %(hi)s::timestamptz
ORDER BY range_start
"""

COVERAGE_SQL = f"""
SELECT report_date,
       count(*)                                                AS n_rows,
       count(*) FILTER (WHERE isin IS NOT NULL AND isin <> '')  AS n_isin
FROM {TABLE}
WHERE report_date = ANY(%(dates)s::date[])
GROUP BY 1 ORDER BY 1
"""

_print_lock = threading.Lock()


def _log(msg: str) -> None:
    with _print_lock:
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()


def affected_chunks(cur, report_dates: list[str]) -> list[str]:
    """Compressed chunks whose time range overlaps ``report_dates``."""
    cur.execute(AFFECTED_CHUNKS_SQL, {
        "table": TABLE, "lo": min(report_dates), "hi": max(report_dates),
    })
    return [r[0] for r in cur.fetchall()]


def prep(dsn: str, report_dates: list[str] | None = None) -> list[str]:
    """Open the write path. Returns the chunks that were decompressed.

    With ``report_dates`` only the overlapping chunks are decompressed; without
    it, the whole hypertable is, which is the rescued original's behaviour and
    should be reserved for a full reload.
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT remove_compression_policy('{TABLE}', if_exists => true)")
        if report_dates:
            chunks = affected_chunks(cur, report_dates)
            for chunk in chunks:
                cur.execute("SELECT decompress_chunk(%s::regclass, if_compressed => true)", (chunk,))
        else:
            cur.execute(
                "SELECT format('%I.%I', chunk_schema, chunk_name) "
                "FROM timescaledb_information.chunks "
                f"WHERE hypertable_name = '{TABLE}' AND is_compressed = true"
            )
            chunks = [r[0] for r in cur.fetchall()]
            for chunk in chunks:
                cur.execute("SELECT decompress_chunk(%s::regclass, if_compressed => true)", (chunk,))
    _log(f"prep done: compression policy removed, {len(chunks)} chunk(s) decompressed: {chunks}")
    return chunks


def delete_report_dates(dsn: str, report_dates: list[str]) -> int:
    """Remove every row on ``report_dates``. Mandatory before a repair reload.

    Without this the reload is a guaranteed no-op — see the module docstring.
    """
    if not report_dates:
        raise ValueError("refusing to DELETE without an explicit report_date list")
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {TABLE} WHERE report_date = ANY(%s::date[])", (report_dates,))
        deleted = cur.rowcount
    _log(f"deleted {deleted:,} rows on {report_dates}")
    return deleted


def load_one(dsn: str, path: str, ts: dt.datetime, report_dates: list[str] | None = None) -> tuple[str, int]:
    name = os.path.basename(path)
    sql = INSERT_SCOPED_SQL if report_dates else INSERT_SQL
    params: dict = {"ts": ts}
    if report_dates:
        params["dates"] = report_dates
    # Each worker uses ON COMMIT DROP temp table; one transaction per CSV.
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(STAGE_DDL)
        with open(path, encoding="utf-8", newline="") as fh:
            with cur.copy(
                "COPY _nport_stage (" + ", ".join(CSV_COLS)
                + ") FROM STDIN WITH (FORMAT csv, HEADER true)"
            ) as cp:
                while chunk := fh.read(1 << 20):
                    cp.write(chunk)
        cur.execute(sql, params)
        inserted = cur.rowcount
        conn.commit()
    _log(f"  {name:24s} inserted={inserted:>9}")
    return name, inserted


def finalize(dsn: str, cleanup: bool, skip_matview: bool, recompress: list[str] | None = None) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        if cleanup:
            cur.execute(f"DELETE FROM {TABLE} WHERE cusip = %s", (PLACEHOLDER_CUSIP,))
            _log(f"cleanup: deleted {cur.rowcount} rows cusip='{PLACEHOLDER_CUSIP}'")
        for chunk in recompress or []:
            cur.execute("SELECT compress_chunk(%s::regclass, if_not_compressed => true)", (chunk,))
        if recompress:
            _log(f"re-compressed {len(recompress)} chunk(s)")
        if not skip_matview:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_nport_sector_attribution")
            _log("matview refreshed")
        cur.execute(f"SELECT add_compression_policy('{TABLE}', INTERVAL '3 months', if_not_exists => true)")
    _log("finalize done: compression policy restored")


def verify_isin_fill(
    dsn: str,
    report_dates: list[str],
    floor: float = DEFAULT_FILL_FLOOR,
) -> tuple[list[dict], list[dict]]:
    """Post-load check. Returns (readings, readings_below_floor).

    The check the original loader did not have. A package that lost its ISIN side
    is invisible in row counts, in error logs and in the exit code; it is obvious
    here and nowhere else.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(COVERAGE_SQL, {"dates": report_dates})
        rows = cur.fetchall()
    readings = [
        {"report_date": str(rd), "rows": n, "isin": n_isin,
         "isin_fill": round(n_isin / n, 4) if n else 0.0}
        for rd, n, n_isin in rows
    ]
    missing = sorted(set(report_dates) - {r["report_date"] for r in readings})
    for rd in missing:
        readings.append({"report_date": rd, "rows": 0, "isin": 0, "isin_fill": 0.0})
    readings.sort(key=lambda r: r["report_date"])
    return readings, [r for r in readings if r["isin_fill"] < floor]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed-dir", required=True)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--skip-matview", action="store_true")
    ap.add_argument("--cleanup-placeholders", action="store_true")
    ap.add_argument("--only", default="", help="comma-separated substrings; load only matching CSVs")
    ap.add_argument(
        "--only-report-dates", default="",
        help="comma-separated ISO dates; the INSERT will not touch any other report_date",
    )
    ap.add_argument(
        "--delete-first", action="store_true",
        help="DELETE the --only-report-dates before loading. Required for a repair: "
             "ON CONFLICT DO NOTHING means a plain reload cannot displace bad rows.",
    )
    ap.add_argument("--verify-floor", type=float, default=DEFAULT_FILL_FLOOR)
    ap.add_argument("--no-verify", action="store_true", help="skip the post-load ISIN fill check")
    args = ap.parse_args(argv)

    report_dates = [d.strip() for d in args.only_report_dates.split(",") if d.strip()]
    if args.delete_first and not report_dates:
        ap.error("--delete-first requires --only-report-dates; refusing an unscoped DELETE")

    ts = dt.datetime.now(dt.UTC).replace(microsecond=0)
    files = sorted(glob.glob(os.path.join(args.seed_dir, "*.csv")))
    if args.only:
        subs = [s.strip() for s in args.only.split(",") if s.strip()]
        files = [f for f in files if any(s in os.path.basename(f) for s in subs)]
    _log(f"parallel load: {len(files)} files, {args.workers} workers, ts={ts.isoformat()}")
    if report_dates:
        _log(f"scope: report_date IN {report_dates}")
    _log(f"ROLLBACK handle: DELETE FROM {TABLE} WHERE created_at = '{ts.isoformat()}';")

    chunks = prep(args.dsn, report_dates or None)
    if args.delete_first:
        delete_report_dates(args.dsn, report_dates)

    total = 0
    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(load_one, args.dsn, p, ts, report_dates or None): p for p in files}
        for fut in concurrent.futures.as_completed(futs):
            try:
                _, n = fut.result()
                total += n
            except Exception as exc:
                failures += 1
                _log(f"  FAIL {os.path.basename(futs[fut])}: {exc}")

    finalize(args.dsn, args.cleanup_placeholders, args.skip_matview, chunks)
    _log(f"TOTAL inserted={total}")
    if failures:
        _log(f"REFUSING to pass: {failures} CSV(s) failed to load")
        return 1

    if report_dates and not args.no_verify:
        readings, bad = verify_isin_fill(args.dsn, report_dates, args.verify_floor)
        for r in readings:
            _log(f"  verify {r['report_date']}  rows={r['rows']:>10,}  isin_fill={r['isin_fill']:.4f}")
        if bad:
            _log(
                f"REFUSING to pass: {len(bad)} report_date(s) below the {args.verify_floor:.2f} "
                f"ISIN fill floor: {[(r['report_date'], r['isin_fill']) for r in bad]}. "
                "The load left the table in the 2023q4/2025q1 state. Do not re-run this loader "
                "expecting a fix: ON CONFLICT DO NOTHING makes a plain re-run a no-op."
            )
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

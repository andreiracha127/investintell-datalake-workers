"""Bulk-load parsed N-PORT CSVs into ``sec_nport_holdings``.

The conflict key is ``(report_date, series_id, cusip)``. ``ON CONFLICT DO
NOTHING`` preserves existing rows; it cannot repair a bad filing. Monthly
``--new-series-only`` loads preserve each existing series wholesale.

A real load runs the same read-only preflight as ``--dry-run``. CSV/header/type
checks and exact conflict-key indexes predict COPY and insertion; the indexes
use bounded temporary disk storage. A sec-api converter manifest automatically
enables the shared quality contract in ``tools.nport_secapi.contract``.

COPY, scoped replacement/cleanup, insertion, and actual-row verification occur
in one transaction. CSVs sharing any report_date form one transaction group, so
a rejected date leaves no committed inserts or deletes, even when split across
files. Disjoint date groups COPY in parallel. ISIN verification includes the
whole target date; sec-api value and malformed-source checks judge only rows
actually inserted. Verification precedes the commit and includes trigger changes.

The lifecycle holds LOAD_LOCK across preparation, workers and restoration.
Preparation pauses existing compression jobs and opens only overlapping chunks.
Finalization restores those chunks and the original job IDs/configuration and
scheduled state, including on failure; it never invents a missing policy. Each
new-series transaction also holds NEW_SERIES_LOCK from its post-wait snapshot
through verification and commit. Lock order: monthly worker, lifecycle, insert.

The original operator script was rescued from
``E:\\investintell-allocation\\scripts\\nport_parallel_load.py``. Its key and field
mapping remain unchanged. Repairs use ``--delete-first`` with explicit dates;
the replacement and verification now roll back together if rejected.

Usage:
  python -m tools.nport_dera.nport_parallel_load --seed-dir DIR --dsn DSN \
      --only-report-dates 2023-09-30,2023-10-31 --delete-first --skip-matview
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import datetime as dt
import glob
import json
import math
import os
import re
import sys
import threading
from dataclasses import dataclass, field
from contextlib import ExitStack
from collections.abc import MutableSet
from typing import Iterator

import psycopg

from tools.nport_secapi.contract import (
    ValidationAccumulator, filing_reference_sums, isin_present, judge_isin_fill, malformed_verdict,
    profile_series, quality_verdict,
)
from tools.nport_dera.key_index import KeyIndex

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

#: ``src.db.LOCK_NPORT_NEW_SERIES_INSERT`` (this tool does not import ``src``).
#: Transaction-level: held from just before a --new-series-only INSERT to its commit.
NEW_SERIES_LOCK = 900_364

# Session-level lifecycle mutex; COPY workers still run concurrently inside it.
# See src.db.LOCK_NPORT_LOAD for the registry and lock order.
LOAD_LOCK = 900_365

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

_QUALIFIED = ", ".join(f"s.{c}" for c in CSV_COLS)

INSERT_NEW_SERIES_SQL = f"""
INSERT INTO {TABLE} ({', '.join(CSV_COLS)}, cik_padded, created_at)
SELECT {_QUALIFIED}, lpad(s.cik, 10, '0'), %(ts)s
FROM _nport_stage s
WHERE s.report_date <= current_date AND s.series_id IS NOT NULL
  AND s.report_date = ANY(%(dates)s::date[])
  AND NOT EXISTS (
    SELECT 1 FROM {TABLE} h
    WHERE h.report_date = s.report_date AND h.series_id = s.series_id
  )
ON CONFLICT (report_date, series_id, cusip) DO NOTHING
"""

AFFECTED_CHUNKS_SQL = """
SELECT format('%%I.%%I', chunk_schema, chunk_name)
FROM timescaledb_information.chunks
WHERE hypertable_name = %(table)s
  AND hypertable_schema = current_schema()
  AND is_compressed
  AND range_end > %(lo)s::timestamptz
  AND range_start <= %(hi)s::timestamptz
ORDER BY range_start
"""

#: The ISIN the post-load verdict counts. ``_isin_present`` is the same test in Python.
_ISIN_PRESENT = "(isin IS NOT NULL AND isin <> '')"

COVERAGE_SQL = f"""
SELECT report_date,
       count(*)                                  AS n_rows,
       count(*) FILTER (WHERE {_ISIN_PRESENT})   AS n_isin
FROM {TABLE}
WHERE report_date = ANY(%(dates)s::date[])
GROUP BY 1 ORDER BY 1
"""

#: What the dry run needs from the table: per (report_date, series), rows split
#: by placeholder cusip (``--cleanup-placeholders`` deletes those) and by ISIN.
TABLE_STATE_SQL = f"""
SELECT report_date, series_id, cusip = %(placeholder)s, {_ISIN_PRESENT}, count(*)
FROM {TABLE}
WHERE report_date = ANY(%(dates)s::date[])
GROUP BY 1, 2, 3, 4
"""

#: The keys a plain (not new-series-only) load's ON CONFLICT will skip.
TABLE_KEYS_SQL = f"""
SELECT report_date, series_id, cusip FROM {TABLE} WHERE report_date = ANY(%(dates)s::date[])
"""

MATVIEW = "mv_nport_sector_attribution"

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


@dataclass
class Maintenance:
    """Restoration state, including progress if preparation fails partway."""

    chunks: list[str] = field(default_factory=list)
    policies: list[tuple[int, bool]] = field(default_factory=list)


def prep(dsn: str, report_dates: list[str] | None = None, state: Maintenance | None = None) -> Maintenance:
    """Pause existing policies and decompress the scope, preserving their configuration."""
    state = state if state is not None else Maintenance()
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT job_id, scheduled FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = current_schema() AND hypertable_name = %s "
            "AND proc_name = 'policy_compression'", (TABLE,),
        )
        state.policies = cur.fetchall()
        _log(f'maintenance policy snapshot (job_id, scheduled): {state.policies}')
        for job_id, scheduled in state.policies:
            if scheduled:
                cur.execute("SELECT alter_job(%s, scheduled => false)", (job_id,))
        if report_dates:
            chunks = affected_chunks(cur, report_dates)
        else:
            cur.execute(
                "SELECT format('%I.%I', chunk_schema, chunk_name) "
                "FROM timescaledb_information.chunks "
                f"WHERE hypertable_schema = current_schema() AND hypertable_name = '{TABLE}' AND is_compressed = true"
            )
            chunks = [r[0] for r in cur.fetchall()]
        for chunk in chunks:
            cur.execute("SELECT decompress_chunk(%s::regclass, if_compressed => true)", (chunk,))
            state.chunks.append(chunk)
    _log(f"prep done: policies paused, {len(state.chunks)} chunk(s) decompressed")
    return state


def load_one(
    dsn: str,
    path: str,
    ts: dt.datetime,
    report_dates: list[str] | None = None,
    new_series_only: bool = False,
    **options,
) -> tuple[str, int]:
    """Load a CSV and verify on its own transaction before committing."""
    return load_batch(dsn, [path], ts, report_dates, new_series_only, **options)


class VerificationError(ValueError):
    """A rejected transaction; none of its inserts or deletes were committed."""


def load_batch(
    dsn: str,
    paths: list[str],
    ts: dt.datetime,
    report_dates: list[str] | None = None,
    new_series_only: bool = False,
    *,
    verify: bool = True,
    floor: float = DEFAULT_FILL_FLOOR,
    delete_first: bool = False,
    cleanup: bool = False,
    quality_manifest: dict | None = None,
) -> tuple[str, int]:
    """COPY, insert/delete and verify atomically across every CSV sharing a date."""
    name = ','.join(os.path.basename(p) for p in paths)
    if new_series_only and not report_dates:
        raise ValueError("new_series_only requires an explicit report_date list")
    if new_series_only and (delete_first or cleanup):
        raise ValueError("new_series_only cannot be combined with deletion or placeholder cleanup")
    if delete_first and not report_dates:
        raise ValueError("delete_first requires an explicit report_date list")
    if not math.isfinite(floor) or not 0 <= floor <= 1:
        raise ValueError("verification floor must be finite and between 0 and 1")
    sql = INSERT_NEW_SERIES_SQL if new_series_only else (INSERT_SCOPED_SQL if report_dates else INSERT_SQL)
    params: dict = {"ts": ts}
    if report_dates:
        params["dates"] = report_dates
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        if new_series_only:
            # The INSERT's snapshot must postdate the lock wait (see below).
            conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
        cur.execute(STAGE_DDL)
        for path in paths:
            with open(path, encoding="utf-8", newline="") as fh:
                header = next(copy_csv_records(fh), (1, None))[1]
                if header != CSV_COLS:
                    raise VerificationError(f"{os.path.basename(path)}: unexpected CSV header")
                fh.seek(0)
                with cur.copy(
                    "COPY _nport_stage (" + ", ".join(CSV_COLS)
                    + ") FROM STDIN WITH (FORMAT csv, HEADER true)"
                ) as cp:
                    while chunk := fh.read(1 << 20):
                        cp.write(chunk)
        if new_series_only:
            # Serialize the NOT EXISTS check with its own commit, across every
            # loader process: two concurrent loads of one absent series would
            # otherwise both find it absent and graft two filings together.
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (NEW_SERIES_LOCK,))
        if delete_first:
            cur.execute(f"DELETE FROM {TABLE} WHERE report_date = ANY(%s::date[])", (report_dates,))
        if quality_manifest is not None:
            cur.execute("CREATE TEMP TABLE _nport_inserted (report_date date, series_id text, cusip text) ON COMMIT DROP")
            # Store only inserted keys in PostgreSQL, never millions of rows in Python.
            sql = ("WITH inserted AS (" + sql.rstrip().rstrip(';')
                   + " RETURNING report_date, series_id, cusip) "
                   "INSERT INTO _nport_inserted SELECT * FROM inserted")
        cur.execute(sql, params)
        inserted = cur.rowcount
        if cleanup:
            where = " AND report_date = ANY(%(dates)s::date[])" if report_dates else ""
            cur.execute(f"DELETE FROM {TABLE} WHERE cusip = %(placeholder)s" + where,
                        {"placeholder": PLACEHOLDER_CUSIP, "dates": report_dates})
        if quality_manifest is not None:
            # The actual target values include changes made by database triggers.
            _verify_quality(cur, quality_manifest, report_dates or [])
        if report_dates and verify:
            cur.execute(COVERAGE_SQL, {"dates": report_dates})
            readings, bad = judge_isin_fill(
                {str(rd): (n, n_isin) for rd, n, n_isin in cur.fetchall()}, report_dates, floor,
            )
            for r in readings:
                _log(f"  verify {r['report_date']} rows={r['rows']:,} isin_fill={r['isin_fill']:.4f}")
            if bad:
                raise VerificationError(f"ISIN fill below {floor:.2f}: "
                                        f"{[(r['report_date'], r['isin_fill']) for r in bad]}")
        conn.commit()
    _log(f"  {name:24s} inserted={inserted:>9}")
    return name, inserted


def _verify_quality(cur, manifest: dict, report_dates: list[str]) -> None:
    cur.execute(
        "SELECT h.report_date,h.series_id,count(*), "
        "count(*) FILTER(WHERE h.isin IS NOT NULL AND h.isin<>''), "
        "COALESCE(sum(h.pct_of_nav),0),count(*) FILTER(WHERE h.pct_of_nav IS NULL), "
        "count(*) FILTER(WHERE h.market_value IS NULL),COALESCE(sum(h.market_value),0), "
        "count(*) FILTER(WHERE h.currency='USD') "
        f"FROM {TABLE} h JOIN _nport_inserted i USING(report_date,series_id,cusip) GROUP BY 1,2",
    )
    by_date: dict[str, list[dict]] = collections.defaultdict(list)
    cols = ['series_id', 'rows', 'isin', 'pct_sum', 'pct_missing', 'mv_missing',
            'market_value_usd_total', 'usd_rows']
    for rd, *values in cur.fetchall():
        by_date[str(rd)].append(dict(zip(cols, values)))
    for rd, aggregates in by_date.items():
        profile = profile_series(rd, aggregates, reference_sums=filing_reference_sums(manifest['report_dates'][rd]))
        problems = quality_verdict(profile, include_isin=False)
        series = {r['series_id'] for r in aggregates}
        problems += malformed_verdict(manifest['report_dates'][rd], only_series=series)
        if problems:
            raise VerificationError(f"{rd}: " + '; '.join(problems))


def finalize(dsn: str, skip_matview: bool, recompress: Maintenance | None = None) -> None:
    """Restore maintenance even when recompression or a matview refresh fails."""
    state = recompress if recompress is not None else Maintenance()
    failures = []
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for chunk in state.chunks:
            try:
                cur.execute("SELECT compress_chunk(%s::regclass, if_not_compressed => true)", (chunk,))
            except Exception as exc:
                failures.append(f"recompression {chunk}: {type(exc).__name__}")
        for job_id, scheduled in state.policies:
            try:
                cur.execute("SELECT alter_job(%s, scheduled => %s)", (job_id, scheduled))
            except Exception as exc:
                failures.append(f"policy {job_id}: {type(exc).__name__}")
        if not skip_matview:
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {MATVIEW}")
            except Exception as exc:
                failures.append(f"matview: {type(exc).__name__}")
    if failures:
        raise RuntimeError('; '.join(failures))
    _log("finalize done: original compression policy state restored")


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
    return judge_isin_fill({str(rd): (n, n_isin) for rd, n, n_isin in rows}, report_dates, floor)


_BOOL_TOKENS = {"t", "true", "y", "yes", "on", "1", "f", "false", "n", "no", "off", "0"}
_BIGINT = (-(2**63), 2**63 - 1)
#: ASCII only: ``int()`` and ``Decimal()`` also read Unicode digits and ``1_000``,
#: which PostgreSQL rejects (before 16, for the underscore).
#: ``re.ASCII`` and ``_ASCII_SPACE``: PostgreSQL trims C-locale whitespace only.
_INT_RE = re.compile(r"\s*[+-]?[0-9]+\s*", re.ASCII)
_NUMERIC_RE = re.compile(r"\s*[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*", re.ASCII)
_NONFINITE_RE = re.compile(r"\s*[+-]?(?:nan|inf|infinity)\s*", re.ASCII | re.IGNORECASE)
_ASCII_SPACE = " \t\n\r\f\v"
#: STAGE_DDL's non-text columns, and the target's NOT NULL columns the INSERT
#: does not filter on (a NULL there fails the whole CSV's transaction).
_DATE_COLS = ("report_date",)
_BIGINT_COLS = ("market_value",)
_NUMERIC_COLS = ("quantity", "pct_of_nav")
_BOOL_COLS = ("is_restricted",)
_NOT_NULL_INSERTED = ("cik", "cusip")
#: One CSV field as COPY splits it: a quoted section (``""`` escapes a quote), an
#: unquoted run, or the delimiter. A quote may open anywhere inside a field.
_CSV_SEGMENT = re.compile(r'"((?:[^"]|"")*)"|([^,"]+)|(,)')


def iso_date(value: str) -> str | None:
    """``YYYY-MM-DD`` for a value COPY reads as that date; None when the dry run cannot vouch for it.

    ``date.fromisoformat`` also reads ISO week dates (``2026-W22-1``), which
    PostgreSQL does not. Normalizing matters: the conflict key and the scope
    compare dates, not spellings (``20260531`` is ``2026-05-31`` to the table).
    """
    if "W" in value.upper():
        return None
    try:
        return dt.date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def _isin_present(isin: str | None) -> bool:
    """``_ISIN_PRESENT`` in Python."""
    return isin_present(isin)


def _copy_fields(record: str) -> list[str | None] | None:
    """Fields of one record; None for an unquoted empty field (COPY's NULL), None overall if unparseable."""
    if "\x00" in record:
        return None  # PostgreSQL text cannot hold NUL: COPY fails the CSV
    if '"' not in record:
        return [field or None for field in record.split(",")]
    fields: list[str | None] = []
    parts: list[str] = []
    quoted = False
    pos = 0
    for match in _CSV_SEGMENT.finditer(record):
        if match.start() != pos:
            return None  # a quote that never closes
        pos = match.end()
        inner, plain, comma = match.groups()
        if comma is not None:
            fields.append("".join(parts) if parts or quoted else None)
            parts, quoted = [], False
        elif inner is not None:
            parts.append(inner.replace('""', '"'))
            quoted = True
        else:
            parts.append(plain)
    if pos != len(record):
        return None
    fields.append("".join(parts) if parts or quoted else None)
    return fields


def copy_csv_records(fh) -> Iterator[tuple[int, list[str | None] | None]]:
    """``(first line number, fields)`` per record, split the way ``COPY ... (FORMAT csv)`` splits it.

    ``csv.reader`` returns ``""`` for both an unquoted empty field and a quoted
    ``""``; COPY loads the first as NULL and the second as an empty string, which
    a typed column rejects and a NOT NULL text column accepts. A record whose
    quotes do not close yields None. ``fh`` must be opened with ``newline=""``.
    """
    pending: str | None = None
    delimiter: str | None = None
    start = line_no = 0
    for line in fh:
        line_no += 1
        if pending is None:
            pending, start = line, line_no
        else:
            pending += line
        if pending.count('"') % 2:
            continue  # inside a quoted field: the newline is data
        record, pending = pending, None
        ending = '\r\n' if record.endswith('\r\n') else (record[-1:] if record.endswith(('\r', '\n')) else '')
        if ending and delimiter is not None and ending != delimiter:
            yield start, None  # COPY refuses a change in unquoted record delimiters.
            continue
        if ending:
            delimiter = ending
            record = record[:-len(ending)]
        yield start, _copy_fields(record)
    if pending is not None:
        yield start, None


def _coerce_errors(rec: dict) -> list[tuple[str, str]]:
    """Type errors COPY into ``STAGE_DDL`` would raise for one row. NULL (None) always passes."""
    errors = []
    for col in _DATE_COLS:
        value = rec[col]
        if value is not None and iso_date(value) is None:
            errors.append((f"type:{col}", value))
    for col in _BIGINT_COLS:
        value = rec[col]
        if value is None:
            continue
        if not _INT_RE.fullmatch(value):
            errors.append((f"type:{col}", value))
        elif not _BIGINT[0] <= int(value) <= _BIGINT[1]:
            errors.append((f"range:{col}", value))
    for col in _NUMERIC_COLS:
        value = rec[col]
        if value is None:
            continue
        if _NONFINITE_RE.fullmatch(value):
            errors.append((f"nonfinite:{col}", value))  # loads; refused: a NaN weight poisons every sum
        elif not _NUMERIC_RE.fullmatch(value):
            errors.append((f"type:{col}", value))
    for col in _BOOL_COLS:
        value = rec[col]
        if value is not None and value.strip(_ASCII_SPACE).lower() not in _BOOL_TOKENS:
            errors.append((f"type:{col}", value))
    return errors


@dataclass
class TableState:
    """The target as the load will find it on the scope dates, before its first INSERT."""

    today: dt.date
    #: True / False when read through a DSN; None offline (not checked).
    matview_exists: bool | None = None
    #: report_date -> series the table holds (``--new-series-only`` skips them).
    series: dict[str, set[str]] = field(default_factory=dict)
    #: report_date -> [rows, rows with an ISIN] that survive the load (the verify counts them).
    counts: dict[str, list[int]] = field(default_factory=dict)
    #: Conflict keys a plain load's ON CONFLICT will skip.
    keys: MutableSet[tuple[str, str, str]] = field(default_factory=KeyIndex)
    source: str = "offline: models no rows on the scope dates; pass --dsn to read the table"


def read_table_state(
    dsn: str,
    report_dates: list[str],
    *,
    rows: bool,
    keys: bool,
    cleanup: bool,
) -> TableState:
    """Read, never write, what ``dry_run`` needs. ``rows=False`` when nothing will be there (``--delete-first``)."""
    with ExitStack() as resources:
        with psycopg.connect(dsn) as conn:
            conn.read_only = True
            with conn.cursor() as cur:
                cur.execute("SELECT current_date, to_regclass(%s) IS NOT NULL", (MATVIEW,))
                today, matview = cur.fetchone()
                state = TableState(today=today, matview_exists=matview, source=(
                    "read through --dsn" if rows else "not read: no scope dates (current_date and matview read)"))
                if isinstance(state.keys, KeyIndex):
                    resources.callback(state.keys.close)
                if rows:
                    cur.execute(TABLE_STATE_SQL, {"dates": report_dates, "placeholder": PLACEHOLDER_CUSIP})
                    for rd, series, placeholder, has_isin, n in cur.fetchall():
                        rd = str(rd)
                        state.series.setdefault(rd, set()).add(series)
                        if cleanup and placeholder:
                            continue  # transactional cleanup deletes it before verification
                        count = state.counts.setdefault(rd, [0, 0])
                        count[0] += n
                        count[1] += n if has_isin else 0
            if rows and keys:
                with conn.cursor(name="nport_dry_run_keys") as cur:
                    cur.itersize = 100_000
                    cur.execute(TABLE_KEYS_SQL, {"dates": report_dates})
                    state.keys.update((str(rd), series, cusip) for rd, series, cusip in cur)
        resources.pop_all()  # hand ownership off only after the connection exits
    return state


@dataclass
class _Plan:
    """What one dry run carries from CSV to CSV."""

    table: TableState
    new_series_only: bool = False
    cleanup: bool = False
    earlier_keys: MutableSet[tuple[str, str, str]] = field(default_factory=KeyIndex)
    earlier_series: set[tuple[str, str]] = field(default_factory=set)
    quality: dict[str, ValidationAccumulator] = field(default_factory=dict)


def check_csv(
    path: str,
    report_dates: list[str] | None = None,
    today: dt.date | None = None,
    max_examples: int = 5,
    plan: _Plan | None = None,
) -> dict:
    """Plan one CSV using a bounded exact conflict-key index."""
    owns_plan = plan is None
    plan = plan or _Plan(TableState(today=today or dt.datetime.now(dt.UTC).date()))
    with ExitStack() as resources:
        seen = resources.enter_context(KeyIndex())
        if owns_plan:
            if isinstance(plan.table.keys, KeyIndex):
                resources.callback(plan.table.keys.close)
            if isinstance(plan.earlier_keys, KeyIndex):
                resources.callback(plan.earlier_keys.close)
        return _check_csv(path, report_dates, today, max_examples, plan, seen=seen)


def _check_csv(
    path: str,
    report_dates: list[str] | None,
    today: dt.date | None,
    max_examples: int,
    plan: _Plan,
    *,
    seen: KeyIndex,
) -> dict:
    """Offline plan for one CSV: what COPY + INSERT would do to the table ``plan`` describes.

    Mirrors the write path in its order: COPY (FORMAT csv) splits the records
    and turns an unquoted empty field into NULL; ``STAGE_DDL`` coerces types; the
    INSERT's WHERE drops future or NULL dates, NULL series, (scoped) other
    report_dates and (``--new-series-only``) series the table holds; the
    target's NOT NULLs reject the whole CSV; ``ON CONFLICT DO NOTHING`` keeps the
    first row per key and skips keys the table holds; ``--cleanup-placeholders``
    deletes placeholder rows afterwards. Keys and series of earlier CSVs in
    ``plan`` are refusals (``cross_file_dupes`` / ``split_series``); this CSV's
    are added to it.
    """
    plan = plan or _Plan(TableState(today=today or dt.datetime.now(dt.UTC).date()))
    today = today or plan.table.today
    scope = {iso_date(d) or d for d in report_dates or ()}
    out: dict = {
        "file": os.path.basename(path), "rows": 0, "would_insert": 0,
        "header_ok": False, "errors": collections.Counter(), "examples": [],
        "dropped": collections.Counter(), "conflict_key_dupes": 0, "cross_file_dupes": 0,
        "split_series": 0, "per_report_date": {},
        "source_dates": {},
    }
    per: dict = collections.defaultdict(lambda: {"rows": 0, "isin": 0, "series": set()})
    series_here: set[tuple[str, str]] = set()

    def bad(kind: str, line: int, detail: str) -> None:
        out["errors"][kind] += 1
        if len(out["examples"]) < max_examples:
            out["examples"].append(f"line {line}: {kind}: {detail[:120]}")

    with open(path, encoding="utf-8", newline="") as fh:
        records = copy_csv_records(fh)
        _, header = next(records, (1, None))
        out["header_ok"] = header == CSV_COLS
        if not out["header_ok"]:
            bad("header", 1, f"{header!r} != {CSV_COLS!r}")
            return _finish_check(out, per)
        for line, row in records:
            out["rows"] += 1
            if row is None:
                bad("unparseable", line, "unterminated quoted field or NUL byte")
                continue
            if len(row) != len(CSV_COLS):
                bad("width", line, f"{len(row)} fields")
                continue
            rec = dict(zip(CSV_COLS, row))
            errors = _coerce_errors(rec)
            for kind, value in errors:
                bad(kind, line, value)
            if errors:
                continue
            rd = iso_date(rec["report_date"]) if rec["report_date"] is not None else None
            series = rec["series_id"]
            if rd is None or dt.date.fromisoformat(rd) > today:
                out["dropped"]["future_or_null_report_date"] += 1
                continue
            if series is None:
                out["dropped"]["null_series_id"] += 1
                continue
            if scope and rd not in scope:
                out["dropped"]["outside_only_report_dates"] += 1
                continue
            source = out['source_dates'].setdefault(rd, {'rows': 0, 'series': set()})
            source['rows'] += 1
            source['series'].add(series)
            if plan.new_series_only:
                if series in plan.table.series.get(rd, ()):
                    out["dropped"]["series_already_loaded"] += 1
                    continue
                if (rd, series) in plan.earlier_series:
                    out["split_series"] += 1
                    continue
            missing = [c for c in _NOT_NULL_INSERTED if rec[c] is None]
            if missing:
                bad("not_null:" + ",".join(missing), line, repr(row[:4]))
                continue
            key = (rd, series, rec["cusip"])
            if key in seen:
                out["conflict_key_dupes"] += 1
                continue
            if key in plan.earlier_keys:
                out["cross_file_dupes"] += 1
                continue
            if key in plan.table.keys:
                out["dropped"]["key_already_loaded"] += 1
                continue
            seen.add(key)
            series_here.add((rd, series))
            out["would_insert"] += 1
            if plan.cleanup and rec["cusip"] == PLACEHOLDER_CUSIP:
                out["dropped"]["deleted_by_cleanup_placeholders"] += 1
                continue
            bucket = per[rd]
            bucket["rows"] += 1
            bucket["series"].add(series)
            if _isin_present(rec["isin"]):
                bucket["isin"] += 1
            if rd in plan.quality:
                rec['report_date'] = rd
                plan.quality[rd].add(rec)
    plan.earlier_keys |= seen
    plan.earlier_series |= series_here
    return _finish_check(out, per)


def _finish_check(out: dict, per: dict) -> dict:
    out['source_dates'] = {
        rd: {'rows': v['rows'], 'series': sorted(v['series'])}
        for rd, v in out['source_dates'].items()
    }
    out["per_report_date"] = {
        rd: {"rows": b["rows"], "isin": b["isin"], "series": len(b["series"]),
             "isin_fill": round(b["isin"] / b["rows"], 4) if b["rows"] else 0.0}
        for rd, b in sorted(per.items())
    }
    out["errors"] = dict(out["errors"])
    out["dropped"] = dict(out["dropped"])
    return out


def dry_run(
    files: list[str],
    report_dates: list[str],
    floor: float,
    *,
    dsn: str | None = None,
    new_series_only: bool = False,
    delete_first: bool = False,
    cleanup: bool = False,
    skip_matview: bool = False,
    verify: bool | None = None,
    today: dt.date | None = None,
    quality_manifest: dict | None = None,
    file_dates: dict[str, set[str]] | None = None,
) -> int:
    """Read-only preflight with deterministic cleanup of disk-backed keys."""
    with ExitStack() as resources:
        return _dry_run(
            files, report_dates, floor, dsn=dsn, new_series_only=new_series_only,
            delete_first=delete_first, cleanup=cleanup, skip_matview=skip_matview,
            verify=verify, today=today, quality_manifest=quality_manifest,
            file_dates=file_dates, resources=resources,
        )


def _dry_run(
    files: list[str],
    report_dates: list[str],
    floor: float,
    *,
    dsn: str | None,
    new_series_only: bool,
    delete_first: bool,
    cleanup: bool,
    skip_matview: bool,
    verify: bool | None,
    today: dt.date | None,
    quality_manifest: dict | None,
    file_dates: dict[str, set[str]] | None,
    resources: ExitStack,
) -> int:
    """The load's plan over ``files``: 0 when the load and its verify would pass, 2 otherwise.

    Takes the load's own options. The ISIN verdict is ``judge_isin_fill``, the
    one ``verify_isin_fill`` applies after the load, over the same dates (the
    scope; none when unscoped or ``verify`` is off, as in the load) and the same
    rows: what the table keeps on them plus what the INSERT would add. With
    ``dsn`` the table is read once, read-only, at the start; a writer that lands
    between this and the load is not modeled (the monthly lane holds an advisory
    lock for that). Without ``dsn`` the table is modeled as holding nothing on
    the scope dates, which is exact for ``--delete-first`` and for a date never
    loaded; ``--new-series-only`` has no meaning without the table and needs it.
    """
    report_dates = [iso_date(d) or d for d in report_dates]
    if not math.isfinite(floor) or not 0 <= floor <= 1:
        raise ValueError('verification floor must be finite and between 0 and 1')
    verify = bool(report_dates) if verify is None else verify
    if new_series_only and not dsn:
        raise ValueError("a --new-series-only plan needs --dsn: it depends on the series the table holds")
    if new_series_only and cleanup:
        raise ValueError("--new-series-only and --cleanup-placeholders cannot be combined")
    if dsn:
        state = read_table_state(dsn, report_dates, rows=bool(report_dates) and not delete_first,
                                 keys=not new_series_only, cleanup=cleanup)
    else:
        state = TableState(today=today or dt.datetime.now(dt.UTC).date())
    if delete_first:
        state.source = "emptied on the scope dates by --delete-first"
    _log(f"plan: table {state.source}")
    plan = _Plan(state, new_series_only=new_series_only, cleanup=cleanup)
    if isinstance(state.keys, KeyIndex):
        resources.callback(state.keys.close)
    if isinstance(plan.earlier_keys, KeyIndex):
        resources.callback(plan.earlier_keys.close)
    if quality_manifest is not None:
        plan.quality = {rd: ValidationAccumulator() for rd in report_dates}
    failing = cross_file = split = 0
    planned: dict[str, dict] = {}
    sources: dict[str, dict] = {}
    for path in files:
        result = check_csv(path, report_dates or None, today=today, plan=plan)
        if file_dates is not None:
            file_dates[path] = set(result['source_dates'])
        for rd, source in result['source_dates'].items():
            total = sources.setdefault(rd, {'rows': 0, 'series': set()})
            total['rows'] += source['rows']
            total['series'].update(source['series'])
        cross_file += result["cross_file_dupes"]
        split += result["split_series"]
        _log(f"  {result['file']:<24} rows={result['rows']:>10,} would_insert={result['would_insert']:>10,} "
             f"dupes={result['conflict_key_dupes']:,} cross_file_dupes={result['cross_file_dupes']:,} "
             f"split_series={result['split_series']:,} dropped={result['dropped']} errors={result['errors']}")
        for example in result["examples"]:
            _log(f"      {example}")
        if result["errors"]:
            failing += 1
        for rd, r in result["per_report_date"].items():
            m = planned.setdefault(rd, {"rows": 0, "isin": 0, "series": 0})
            m["rows"] += r["rows"]
            m["isin"] += r["isin"]
            m["series"] += r["series"]
    after = {
        rd: (state.counts.get(rd, [0, 0])[0] + planned.get(rd, {}).get("rows", 0),
             state.counts.get(rd, [0, 0])[1] + planned.get(rd, {}).get("isin", 0))
        for rd in set(planned) | set(state.counts)
    }
    readings, below = judge_isin_fill(after, report_dates if verify else sorted(after), floor)
    for r in readings:
        added = planned.get(r["report_date"], {})
        _log(f"  plan {r['report_date']}  rows after={r['rows']:>10,} (+{added.get('rows', 0):,} from "
             f"{added.get('series', 0):,} series)  isin_fill={r['isin_fill']:.4f}"
             f"{'' if verify else '  (not verified by this load)'}")
    if not verify:
        below = []  # the load runs no verify: unscoped (its ON CONFLICT against the table is not read) or --no-verify
    quality_problems = []
    if quality_manifest is not None:
        for rd in report_dates:
            entry = quality_manifest['report_dates'].get(rd)
            source = sources.get(rd, {'rows': 0, 'series': set()})
            if not entry:
                quality_problems.append(f'{rd}: missing manifest date')
                continue
            if entry.get('partial'):
                quality_problems.append(f'{rd}: manifest date is partial')
            expected_series = {f['series_id'] for f in entry.get('filing_quality', [])}
            if source['rows'] != entry['rows'] or source['series'] != expected_series:
                quality_problems.append(f'{rd}: CSV rows/series do not match the manifest')
            accumulator = plan.quality[rd]
            # An idempotent revisit with no inserts has no new quality cohort.
            if accumulator.series:
                quality_problems += [f'{rd}: {p}' for p in quality_verdict(
                    accumulator.profile(rd, reference_sums=filing_reference_sums(entry)), include_isin=False,
                )]
                quality_problems += [f'{rd}: {p}' for p in malformed_verdict(
                    entry, only_series=set(accumulator.series),
                )]
    matview_missing = not skip_matview and state.matview_exists is False
    if failing:
        _log(f"DRY RUN REFUSES: {failing} CSV(s) would fail COPY/INSERT")
    if below:
        _log(f"DRY RUN REFUSES: the post-load verify would fail: report_date(s) below the {floor:.2f} "
             f"ISIN fill floor: {[(r['report_date'], r['isin_fill']) for r in below]}")
    if cross_file:
        _log(f"DRY RUN REFUSES: {cross_file:,} conflict key(s) repeat across CSVs; the parallel load would keep "
             "whichever copy commits first. Merge each report_date into one CSV (tools.nport_dera.nport_merge).")
    if split:
        _log(f"DRY RUN REFUSES: {split:,} row(s) of series already planned from another CSV; --new-series-only "
             "would load whichever CSV commits first. Put each series in one CSV.")
    if matview_missing:
        _log(f"DRY RUN REFUSES: {MATVIEW} does not exist (use --skip-matview)")
    for problem in quality_problems:
        _log(f'DRY RUN REFUSES: {problem}')
    if failing or below or cross_file or split or matview_missing or quality_problems:
        return 2
    checked = "" if state.matview_exists is not None or skip_matview else f"; {MATVIEW} not checked"
    _log(f"dry run clean ({state.source}{checked}): nothing was written")
    return 0


def transaction_groups(
    files: list[str], file_dates: dict[str, set[str]], report_dates: list[str],
) -> list[tuple[list[str], list[str]]]:
    """Connected CSV/date groups: a date never commits in two transactions."""
    groups: list[tuple[list[str], set[str]]] = []
    for path in files:
        dates = set(file_dates.get(path, ()))
        if report_dates and not dates:
            continue
        paths = [path]
        separate = []
        for existing_paths, existing_dates in groups:
            if dates & existing_dates:
                paths += existing_paths
                dates |= existing_dates
            else:
                separate.append((existing_paths, existing_dates))
        groups = [*separate, (paths, dates)]
    missing = set(report_dates) - set().union(*(dates for _, dates in groups))
    if missing:
        groups.append(([], missing))
    return [(sorted(paths), sorted(dates)) for paths, dates in groups]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed-dir", required=True)
    ap.add_argument("--dsn", default=None,
                    help="target DSN; with --dry-run it is only read (optional, except with --new-series-only)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument('--secapi', action='store_true',
                    help='require the sec-api converter manifest and its shared quality contract')
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
    ap.add_argument(
        "--new-series-only", action="store_true",
        help="insert a (report_date, series_id) only if the table has none of it yet; requires "
             "--only-report-dates and one CSV per series (monthly top-up mode)",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="plan the load with the same options and exit; writes nothing (reads the table with --dsn)",
    )
    ap.add_argument("--verify-floor", type=float, default=DEFAULT_FILL_FLOOR)
    ap.add_argument("--no-verify", action="store_true", help="skip the post-load ISIN fill check")
    args = ap.parse_args(argv)
    if args.workers < 1:
        ap.error('--workers must be positive')
    if not math.isfinite(args.verify_floor) or not 0 <= args.verify_floor <= 1:
        ap.error('--verify-floor must be finite and between 0 and 1')

    report_dates = []
    for raw in (d.strip() for d in args.only_report_dates.split(",")):
        if raw:
            report_dates.append(iso_date(raw) or ap.error(f"--only-report-dates: {raw!r} is not a date"))
    if args.delete_first and not report_dates:
        ap.error("--delete-first requires --only-report-dates; refusing an unscoped DELETE")
    if args.new_series_only and not report_dates:
        ap.error("--new-series-only requires --only-report-dates")
    if args.delete_first and args.new_series_only:
        ap.error("--delete-first and --new-series-only contradict each other")
    if args.new_series_only and args.cleanup_placeholders:
        ap.error("--new-series-only and --cleanup-placeholders cannot be combined: a series held only by "
                 "placeholder rows counts as present, is skipped, then deleted")
    if not args.dsn and not args.dry_run:
        ap.error("--dsn is required unless --dry-run")
    if args.dry_run and args.new_series_only and not args.dsn:
        ap.error("--dry-run --new-series-only needs --dsn: the plan depends on the series the table holds")
    if args.secapi and not os.path.isfile(os.path.join(args.seed_dir, 'manifest.json')):
        ap.error('--secapi requires a sec-api converter manifest; reconvert the containers')

    ts = dt.datetime.now(dt.UTC).replace(microsecond=0)
    files = sorted(glob.glob(os.path.join(args.seed_dir, "*.csv")))
    if args.only:
        subs = [s.strip() for s in args.only.split(",") if s.strip()]
        files = [f for f in files if any(s in os.path.basename(f) for s in subs)]
    _log(f"parallel load: {len(files)} files, {args.workers} workers, ts={ts.isoformat()}")
    if report_dates:
        _log(f"scope: report_date IN {report_dates}")
    if not files:
        _log(f"REFUSING: no CSV matched in {args.seed_dir} (--only={args.only!r})")
        return 1
    manifest_path = os.path.join(args.seed_dir, 'manifest.json')
    quality_manifest = None
    if os.path.isfile(manifest_path):
        with open(manifest_path, encoding='utf-8') as fh:
            manifest = json.load(fh)
        if isinstance(manifest, dict) and manifest.get('generated_by') == 'tools.nport_secapi.convert':
            quality_manifest = manifest
            if not report_dates:
                ap.error('sec-api seed loads require --only-report-dates')
            if args.no_verify:
                ap.error('--no-verify cannot bypass the sec-api validation contract')
    if args.secapi and quality_manifest is None:
        ap.error('--secapi requires a sec-api converter manifest; reconvert the containers')
    if args.dry_run:
        return dry_run(
            files, report_dates, args.verify_floor, dsn=args.dsn, new_series_only=args.new_series_only,
            delete_first=args.delete_first, cleanup=args.cleanup_placeholders, skip_matview=args.skip_matview,
            verify=bool(report_dates) and not args.no_verify,
            quality_manifest=quality_manifest,
        )
    total = failures = rejected = 0
    with psycopg.connect(args.dsn, autocommit=True) as lifecycle_conn:
        lifecycle_conn.execute('SELECT pg_advisory_lock(%s)', (LOAD_LOCK,))
        state = Maintenance()
        restoration_attempted = False
        try:
            # Also enforce the plan on direct CLI invocations, before maintenance
            # or deletes. The transaction repeats verification on actual rows.
            file_dates: dict[str, set[str]] = {}
            rc = dry_run(
                files, report_dates, args.verify_floor, dsn=args.dsn,
                new_series_only=args.new_series_only, delete_first=args.delete_first,
                cleanup=args.cleanup_placeholders, skip_matview=args.skip_matview,
                verify=bool(report_dates) and not args.no_verify,
                quality_manifest=quality_manifest, file_dates=file_dates,
            )
            if rc:
                return rc
            groups = transaction_groups(files, file_dates, report_dates)
            prep(args.dsn, report_dates or None, state)
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
                    futures = {
                        ex.submit(
                            load_batch, args.dsn, paths, ts, dates or None, args.new_series_only,
                            verify=bool(report_dates) and not args.no_verify, floor=args.verify_floor,
                            delete_first=args.delete_first, cleanup=args.cleanup_placeholders,
                            quality_manifest=quality_manifest,
                        ): dates for paths, dates in groups
                    }
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            _, n = future.result()
                            total += n
                        except VerificationError as exc:
                            rejected += 1
                            _log(f'  REJECTED {futures[future]} (rolled back): {exc}')
                        except Exception as exc:
                            failures += 1
                            _log(f'  FAIL {futures[future]} (rolled back): {type(exc).__name__}')
            finally:
                restoration_attempted = True
                finalize(args.dsn, args.skip_matview, state)
        except Exception as exc:
            failures += 1
            _log(f'load/maintenance failed: {type(exc).__name__}: {exc}')
            # prep() may have failed partway, before the inner finally existed.
            if not restoration_attempted and (state.policies or state.chunks):
                try:
                    finalize(args.dsn, True, state)
                except Exception as restore_exc:
                    _log(f'maintenance restoration failed: {type(restore_exc).__name__}: {restore_exc}')
        finally:
            lifecycle_conn.execute('SELECT pg_advisory_unlock(%s)', (LOAD_LOCK,))
    _log(f'TOTAL committed inserts={total}')
    return 1 if failures else (2 if rejected else 0)


if __name__ == "__main__":
    raise SystemExit(main())

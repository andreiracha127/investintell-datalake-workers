"""Export P1 historical sources as canonical JSON snapshots (read-only).

CLI::

    python -m scripts.p1_export.export_p1_sources --out <dir> [--as-of 2026-06-30]

Connects via ``src.db.resolve_dsn()`` + ``src.db.connect()`` and issues
read-only SELECTs only — this module must never write to the database.
Output mirrors the P0 certified-source conventions (sorted keys, 2-space
indent, trailing LF newline, ``p0_contract`` numeric/date normalization) so
the files are deterministic byte-for-byte given the same rows and ``--now``.

Exports into the output directory:

- ``macro_observation_vintage.json`` — all vintages for the 8 SEED_SOURCES
  series, filtered to ``available_at <= as_of`` end-of-day UTC.
- ``eod_prices.json`` — the Phase 0Q reference sleeve tickers
  (artifacts/quant/open_macro_v03_phase0q_002/reference_sleeve_proposal.json),
  ``1998-01-01 <= date <= as_of``.
- ``SOURCE.json`` — provenance (sha256 of file bytes, row counts, min/max
  dates, exact SQL + params) plus export identity fields.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.input_packs.p0_contract import normalize_date, normalize_number
from src.macro_sources import SEED_SOURCES

EXPORT_ID = "open_macro_v03_p1_sources_001"
DB_SOURCE = "tiger_t83f4np6x4"
SCHEMA_VERSION = 1

# Reference sleeve tickers pinned by
# artifacts/quant/open_macro_v03_phase0q_002/reference_sleeve_proposal.json.
SLEEVE_TICKERS: tuple[str, ...] = ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")
EOD_MIN_DATE = "1998-01-01"

MACRO_VINTAGE_COLUMNS: tuple[str, ...] = (
    "series_id", "observation_period", "vintage_date", "value",
    "available_at", "revision_number", "source", "source_spec_version",
)
EOD_PRICES_COLUMNS: tuple[str, ...] = (
    "ticker", "date", "close", "adjusted_close", "volume",
)

MACRO_VINTAGE_SQL = (
    "SELECT series_id, observation_period, vintage_date, value, available_at,\n"
    "       revision_number, source, source_spec_version\n"
    "FROM macro_observation_vintage\n"
    "WHERE series_id = ANY(%(series_ids)s)\n"
    "  AND available_at <= %(as_of_end)s\n"
    "ORDER BY series_id, observation_period, vintage_date"
)

EOD_PRICES_SQL = (
    "SELECT ticker, date, close, adj_close AS adjusted_close, volume\n"
    "FROM eod_prices\n"
    "WHERE ticker = ANY(%(tickers)s)\n"
    "  AND date >= %(min_date)s\n"
    "  AND date <= %(as_of)s\n"
    "ORDER BY ticker, date"
)


def seed_series_ids() -> tuple[str, ...]:
    """The 8 macro series ids, imported from SEED_SOURCES at runtime."""
    return tuple(sorted(spec.series_id for spec in SEED_SOURCES))


def _utc_iso(value: Any) -> str:
    """Normalize a timestamp to a UTC ISO string with a +00:00 offset."""
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, dt.datetime):
        raise ValueError(f"timestamp value is invalid: {value!r}")
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat()


def _execute_select(conn: Any, sql: str, params: Mapping[str, Any]) -> list[tuple[Any, ...]]:
    """Issue one SQL statement, enforcing that it is a read-only SELECT."""
    if not sql.lstrip().upper().startswith("SELECT"):
        raise ValueError(f"read-only export: refusing non-SELECT SQL: {sql!r}")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _as_of_end_utc(as_of: dt.date) -> str:
    return dt.datetime.combine(
        as_of, dt.time(23, 59, 59, 999999), tzinfo=dt.timezone.utc
    ).isoformat()


def format_macro_vintage_rows(rows: Sequence[Sequence[Any]]) -> list[dict[str, Any]]:
    formatted = []
    for row in rows:
        record = dict(zip(MACRO_VINTAGE_COLUMNS, row, strict=True))
        formatted.append({
            "series_id": str(record["series_id"]),
            "observation_period": normalize_date(record["observation_period"]),
            "vintage_date": normalize_date(record["vintage_date"]),
            "value": normalize_number(record["value"]),
            "available_at": _utc_iso(record["available_at"]),
            "revision_number": int(record["revision_number"]),
            "source": str(record["source"]),
            "source_spec_version": str(record["source_spec_version"]),
        })
    formatted.sort(key=lambda r: (r["series_id"], r["observation_period"], r["vintage_date"]))
    return formatted


def format_eod_price_rows(rows: Sequence[Sequence[Any]]) -> list[dict[str, Any]]:
    formatted = []
    for row in rows:
        record = dict(zip(EOD_PRICES_COLUMNS, row, strict=True))
        formatted.append({
            "ticker": str(record["ticker"]),
            "date": normalize_date(record["date"]),
            "close": normalize_number(record["close"]),
            "adjusted_close": normalize_number(record["adjusted_close"]),
            "volume": normalize_number(record["volume"]),
        })
    formatted.sort(key=lambda r: (r["ticker"], r["date"]))
    return formatted


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _write_canonical(path: Path, payload: Any) -> bytes:
    data = _canonical_json(payload).encode("utf-8")
    path.write_bytes(data)
    return data


def _table_provenance(*, table: str, file_bytes: bytes, rows: Sequence[Mapping[str, Any]],
                      date_column: str, sql: str, params: Mapping[str, Any]) -> dict[str, Any]:
    dates = [row[date_column] for row in rows]
    return {
        "table": table,
        "sha256": hashlib.sha256(file_bytes).hexdigest(),
        "row_count": len(rows),
        "min_date": min(dates) if dates else None,
        "max_date": max(dates) if dates else None,
        "sql": sql,
        "params": dict(params),
    }


def export_p1_sources(conn: Any, out_dir: Path | str, *, as_of: dt.date,
                      now: dt.datetime) -> dict[str, Any]:
    """Export the P1 source snapshots into ``out_dir`` and return SOURCE.json.

    ``conn`` is any connection-like object exposing ``cursor()`` (psycopg in
    the CLI path, a fake in tests). Only SELECT statements are ever issued.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    macro_params = {
        "series_ids": list(seed_series_ids()),
        "as_of_end": _as_of_end_utc(as_of),
    }
    macro_rows = format_macro_vintage_rows(
        _execute_select(conn, MACRO_VINTAGE_SQL, macro_params))
    macro_bytes = _write_canonical(out_dir / "macro_observation_vintage.json", macro_rows)

    eod_params = {
        "tickers": list(SLEEVE_TICKERS),
        "min_date": EOD_MIN_DATE,
        "as_of": as_of.isoformat(),
    }
    eod_rows = format_eod_price_rows(
        _execute_select(conn, EOD_PRICES_SQL, eod_params))
    eod_bytes = _write_canonical(out_dir / "eod_prices.json", eod_rows)

    source = {
        "export_id": EXPORT_ID,
        "exported_at": _utc_iso(now),
        "db_source": DB_SOURCE,
        "as_of": as_of.isoformat(),
        "runtime_activation": False,
        "A5": "blocked",
        "schema_version": SCHEMA_VERSION,
        "tables": [
            _table_provenance(
                table="macro_observation_vintage", file_bytes=macro_bytes,
                rows=macro_rows, date_column="observation_period",
                sql=MACRO_VINTAGE_SQL, params=macro_params),
            _table_provenance(
                table="eod_prices", file_bytes=eod_bytes,
                rows=eod_rows, date_column="date",
                sql=EOD_PRICES_SQL, params=eod_params),
        ],
    }
    _write_canonical(out_dir / "SOURCE.json", source)
    return source


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="export_p1_sources",
        description="Export P1 historical sources as canonical JSON (read-only SELECTs).",
    )
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument("--as-of", dest="as_of", default=None,
                        help="as-of date (YYYY-MM-DD); defaults to today UTC")
    parser.add_argument("--now", dest="now", default=None,
                        help="UTC ISO timestamp for exported_at (deterministic runs); "
                             "defaults to the real current time")
    args = parser.parse_args(argv)

    as_of = (dt.date.fromisoformat(args.as_of) if args.as_of
             else dt.datetime.now(dt.timezone.utc).date())
    now = (dt.datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now
           else dt.datetime.now(dt.timezone.utc))

    from src import db

    conn = db.connect(db.resolve_dsn())
    try:
        source = export_p1_sources(conn, Path(args.out), as_of=as_of, now=now)
    finally:
        conn.close()

    print(json.dumps(
        {
            "export_id": source["export_id"],
            "out": str(Path(args.out)),
            "tables": {entry["table"]: entry["row_count"] for entry in source["tables"]},
        },
        indent=2, sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

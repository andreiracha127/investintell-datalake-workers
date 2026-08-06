"""Collapse several package CSVs for ONE report_date into one deterministic CSV.

WHY THIS EXISTS
---------------
``nport_parallel_load`` runs its CSVs in parallel and inserts them
``ON CONFLICT (report_date, series_id, cusip) DO NOTHING``. When more than one
DERA package contributes rows to the same report_date -- which is the normal case,
not the exception -- **which package owns a contested key is decided by whichever
thread commits first**. The loader is idempotent, but it is not deterministic, and
nobody had noticed because nothing had ever compared two loads of the same input.

It is not a theoretical hazard. Measured over the eight report_dates of the
2026-08 repair, 56,782 keys are carried by two or more packages and **1,205 of
them carry different content depending on who wins**:

===============  ======  ===========================================
key kind          count   what a divergence means
===============  ======  ===========================================
real CUSIP-9         966  the same security, restated by a later
                          amendment -- the newer filing is correct
degenerate           221  ``LE:N/A`` / ``IS:N/A``: distinct securities
                          colliding on a placeholder identifier, so
                          neither version is more correct
valid ``IS:``         18  same as real CUSIP-9
===============  ======  ===========================================

Loading the same package set into two databases could therefore leave them
disagreeing on ~1,200 rows, with no error anywhere. The authoritative datalake
and the local research replica must not drift like that.

THE RULE
--------
Merge before loading, in DESCENDING package order, first-wins, so the emitted CSV
has no duplicate key at all and the loader has nothing left to decide. Newest
package wins because an N-PORT/A amendment restates the portfolio: for the 984
non-degenerate keys the later filing is the corrected one. For the degenerate keys
the choice is arbitrary and the only thing that matters is that it is fixed.

Package order is lexicographic on the ``<yyyy>q<n>`` stem, which is also
chronological.

Usage:
  python -m tools.nport_dera.nport_merge --out merged/2023-09-30.csv \
      by_date/2023-09-30/2023q4.csv by_date/2023-09-30/2024q1.csv ...
"""

from __future__ import annotations

import argparse
import csv
import os
import sys

from tools.nport_dera.nport_bulk_parse import CSV_COLS

csv.field_size_limit(2**31 - 1)

CONFLICT_KEY = ("report_date", "series_id", "cusip")


def merge_csvs(paths: list[str], out_path: str) -> dict:
    """Merge ``paths`` newest-package-first, first row per conflict key wins."""
    ordered = sorted(paths, key=lambda p: os.path.basename(p), reverse=True)
    seen: set = set()
    stats: dict = {"kept_by_source": {}, "rows": 0, "isin": 0, "dropped": 0,
                   "order": [os.path.basename(p) for p in ordered]}
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as out_fh:
        writer = csv.writer(out_fh)
        writer.writerow(CSV_COLS)
        for path in ordered:
            name = os.path.basename(path)
            kept = 0
            with open(path, encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                if reader.fieldnames != CSV_COLS:
                    raise SystemExit(f"{name}: unexpected header {reader.fieldnames}")
                for row in reader:
                    key = tuple(row[c] for c in CONFLICT_KEY)
                    if key in seen:
                        stats["dropped"] += 1
                        continue
                    seen.add(key)
                    writer.writerow([row[c] for c in CSV_COLS])
                    kept += 1
                    stats["rows"] += 1
                    if row["isin"]:
                        stats["isin"] += 1
            stats["kept_by_source"][name] = kept
    stats["isin_fill"] = stats["isin"] / stats["rows"] if stats["rows"] else 0.0
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("csvs", nargs="+")
    args = ap.parse_args(argv)
    stats = merge_csvs(args.csvs, args.out)
    sys.stderr.write(
        f"merge order (newest first): {stats['order']}\n"
        + "".join(f"  {name:<16} kept {n:,}\n" for name, n in stats["kept_by_source"].items())
        + f"  dropped as duplicate keys: {stats['dropped']:,}\n"
        f"  -> {args.out}: {stats['rows']:,} rows, isin_fill {stats['isin_fill']:.4f}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

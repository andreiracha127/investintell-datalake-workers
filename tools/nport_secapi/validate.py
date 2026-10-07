r"""Offline sanity over a seed directory produced by ``tools.nport_secapi.convert``.

Reads the CSVs back (independently of the converter's own counters) and reports,
per report_date: rows, series, ISIN fill, the per-series ``pct_of_nav`` sum
distribution, USD market value, and the identifier-key mix. It complements the
loader's ``--dry-run`` (which proves the CSV will COPY and INSERT) with checks
on the values themselves.

``pct_of_nav`` is a percentage. Monthly seeds compare emitted sums against the
selected filing's independently counted source sum: cash, leverage and net
derivative values can make that sum differ from 100. Bands around 100 remain
descriptive. All blocking predicates live in ``tools.nport_secapi.contract``.

Usage:
  python -m tools.nport_secapi.validate E:\tmp-deploy\nport-q3-seed [--json out.json]
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys

from tools.nport_dera.nport_bulk_parse import CSV_COLS
from tools.nport_secapi.contract import (
    ISIN_FLOOR,
    ValidationAccumulator,
    filing_reference_sums,
    malformed_verdict,
    quality_verdict,
)


def profile_csv(path: str, only_series: set[str] | None = None) -> dict:
    """Profile ``path``; with ``only_series``, only those series' rows (what a ``--new-series-only`` load inserts)."""
    csv.field_size_limit(2**31 - 1)
    expected_date = os.path.basename(path)[:10]
    accumulator = ValidationAccumulator()
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != CSV_COLS:
            raise SystemExit(f"{path}: header {reader.fieldnames} != {CSV_COLS}")
        for row in reader:
            if only_series is not None and row["series_id"] not in only_series:
                continue
            accumulator.add(row)
    manifest_path = os.path.join(os.path.dirname(path), "manifest.json")
    date_manifest = None
    if os.path.isfile(manifest_path):
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        if manifest.get("generated_by") == "tools.nport_secapi.convert":
            date_manifest = manifest.get("report_dates", {}).get(expected_date, {})
    profile = accumulator.profile(
        expected_date, reference_sums=filing_reference_sums(date_manifest) if date_manifest is not None else None,
    )
    profile["file"] = os.path.basename(path)
    if date_manifest is not None:
        profile["malformed_problems"] = malformed_verdict(
            date_manifest, only_series if only_series is not None else set(accumulator.series),
        )
    return profile


def verdict(profile: dict, *, include_isin: bool = True, isin_floor=ISIN_FLOOR) -> list[str]:
    """The shared contract; a lane precheck may defer full-date ISIN to the loader."""
    return quality_verdict(profile, include_isin=include_isin, isin_floor=isin_floor)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("seed_dir")
    ap.add_argument("--json", default=None, help="also write the profiles here")
    args = ap.parse_args(argv)
    files = sorted(glob.glob(os.path.join(args.seed_dir, "*.csv")))
    if not files:
        raise SystemExit(f"no CSV in {args.seed_dir}")
    profiles, failing = [], 0
    w = sys.stdout.write
    w(f"{'report_date':<12} {'rows':>10} {'series':>7} {'isin':>7} {'ref+/-5':>8} {'+/-10':>6} "
      f"{'median':>7} {'USD mv ($bn)':>13} {'usd_rows':>8}\n")
    for path in files:
        p = profile_csv(path)
        p["problems"] = verdict(p)
        failing += bool(p["problems"])
        profiles.append(p)
        w(f"{p['report_date']:<12} {p['rows']:>10,} {p['series']:>7,} {p['isin_fill']:>7.4f} "
          f"{p['pct_reference_within_5']:>8.2%} {p['pct_reference_within_10']:>6.0%} {p['pct_sum_median'] or 0:>7.2f} "
          f"{p['market_value_usd_total'] / 1e9:>13,.1f} {p['usd_row_share']:>8.2%}"
          f"{'  ' + '; '.join(p['problems']) if p['problems'] else ''}\n")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(profiles, fh, indent=1, default=str)
    return 2 if failing else 0


if __name__ == "__main__":
    raise SystemExit(main())

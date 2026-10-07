r"""Offline sanity over a seed directory produced by ``tools.nport_secapi.convert``.

Reads the CSVs back (independently of the converter's own counters) and reports,
per report_date: rows, series, ISIN fill, the per-series ``pct_of_nav`` sum
distribution, USD market value, and the identifier-key mix. It complements the
loader's ``--dry-run`` (which proves the CSV will COPY and INSERT) with checks
on the values themselves.

``pct_of_nav`` is a percentage, so a series' holdings sum to ~100. Production's
2026-04-30 (4,132 series) reads median 99.88 with 72% of series inside
[95, 105] and 79% inside [90, 110]; anything far below that means a unit or a
supersession bug, not a market move.

Usage:
  python -m tools.nport_secapi.validate E:\tmp-deploy\nport-q3-seed [--json out.json]
"""

from __future__ import annotations

import argparse
import collections
import csv
import glob
import json
import os
import statistics
import sys
from decimal import Decimal

from tools.nport_dera.nport_bulk_parse import CSV_COLS

#: Floors below production's measured 2026-04-30 reading (0.72 / 0.79), loose
#: enough for a quieter month, tight enough to catch a x100 or a half-loaded series.
PCT_WITHIN_5_FLOOR = 0.60
PCT_WITHIN_10_FLOOR = 0.70
ISIN_FLOOR = 0.90
MIN_JUDGED_ROWS = 1000


def profile_csv(path: str, only_series: set[str] | None = None) -> dict:
    """Profile ``path``; with ``only_series``, only those series' rows (what a ``--new-series-only`` load inserts)."""
    csv.field_size_limit(2**31 - 1)
    expected_date = os.path.basename(path)[:10]
    pct = collections.defaultdict(Decimal)
    mv = collections.defaultdict(int)
    keys = collections.Counter()
    out = collections.Counter()
    dates = collections.Counter()
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != CSV_COLS:
            raise SystemExit(f"{path}: header {reader.fieldnames} != {CSV_COLS}")
        for row in reader:
            if only_series is not None and row["series_id"] not in only_series:
                continue
            out["rows"] += 1
            dates[row["report_date"]] += 1
            series = row["series_id"]
            # Every observed series is judged, so one with no percentages at
            # all sums to 0 and fails the band instead of leaving the sample.
            pct.setdefault(series, Decimal(0))
            if row["pct_of_nav"]:
                pct[series] += Decimal(row["pct_of_nav"])
            else:
                out["pct_missing"] += 1
            if row["market_value"]:
                mv[series] += int(row["market_value"])
            else:
                out["mv_missing"] += 1
            if row["isin"]:
                out["isin"] += 1
            if row["currency"] == "USD":
                out["usd_rows"] += 1
            cusip = row["cusip"]
            keys[cusip[:3] if cusip[:3] in ("IS:", "LE:") else ("H:" if cusip.startswith("H:") else "real")] += 1
    sums = sorted(float(v) for v in pct.values())
    n_series = len(set(pct) | set(mv))
    within = lambda tol: sum(1 for v in sums if abs(v - 100) <= tol) / len(sums) if sums else 0.0  # noqa: E731
    return {
        "file": os.path.basename(path),
        "report_date": expected_date,
        "rows": out["rows"],
        "series": n_series,
        "foreign_report_dates": {d: n for d, n in dates.items() if d != expected_date},
        "isin_fill": round(out["isin"] / out["rows"], 4) if out["rows"] else 0.0,
        "usd_row_share": round(out["usd_rows"] / out["rows"], 4) if out["rows"] else 0.0,
        "pct_missing": out["pct_missing"],
        "mv_missing": out["mv_missing"],
        "pct_sum_median": round(statistics.median(sums), 3) if sums else None,
        "pct_sum_within_5": round(within(5), 4),
        "pct_sum_within_10": round(within(10), 4),
        "pct_sum_over_1000": sum(1 for v in sums if v > 1000),
        "market_value_usd_total": sum(mv.values()),
        "market_value_usd_median_series": statistics.median(sorted(mv.values())) if mv else None,
        "keys": dict(keys),
    }


def verdict(profile: dict) -> list[str]:
    """Problems that should stop a load; empty when the CSV looks like production."""
    problems = []
    if profile["foreign_report_dates"]:
        problems.append(f"rows for other report_dates: {profile['foreign_report_dates']}")
    if profile["rows"] >= MIN_JUDGED_ROWS:
        if profile["isin_fill"] < ISIN_FLOOR:
            problems.append(f"isin_fill {profile['isin_fill']} < {ISIN_FLOOR}")
        if profile["pct_sum_within_5"] < PCT_WITHIN_5_FLOOR:
            problems.append(f"only {profile['pct_sum_within_5']:.0%} of series sum to 100 +/- 5")
        if profile["pct_sum_within_10"] < PCT_WITHIN_10_FLOOR:
            problems.append(f"only {profile['pct_sum_within_10']:.0%} of series sum to 100 +/- 10")
    if profile["pct_sum_over_1000"]:
        problems.append(f"{profile['pct_sum_over_1000']} series sum above 1000% (unit bug?)")
    return problems


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
    w(f"{'report_date':<12} {'rows':>10} {'series':>7} {'isin':>7} {'pct~100':>8} {'+/-10':>6} "
      f"{'median':>7} {'USD mv ($bn)':>13} {'usd_rows':>8}\n")
    for path in files:
        p = profile_csv(path)
        p["problems"] = verdict(p)
        failing += bool(p["problems"])
        profiles.append(p)
        w(f"{p['report_date']:<12} {p['rows']:>10,} {p['series']:>7,} {p['isin_fill']:>7.4f} "
          f"{p['pct_sum_within_5']:>8.2%} {p['pct_sum_within_10']:>6.0%} {p['pct_sum_median'] or 0:>7.2f} "
          f"{p['market_value_usd_total'] / 1e9:>13,.1f} {p['usd_row_share']:>8.2%}"
          f"{'  ' + '; '.join(p['problems']) if p['problems'] else ''}\n")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(profiles, fh, indent=1, default=str)
    return 2 if failing else 0


if __name__ == "__main__":
    raise SystemExit(main())

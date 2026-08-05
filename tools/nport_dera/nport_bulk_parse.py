r"""Parse a SEC N-PORT DERA bulk dataset into the ``sec_nport_holdings`` CSV schema.

PROVENANCE
----------
Rescued verbatim from ``E:\investintell-allocation\scripts\nport_bulk_parse.py``
(mtime 2026-06-11 18:44), an untracked operator script that never existed in any
repository. It is the parser that produced 100% of ``sec_nport_holdings``:
``created_at`` on that table holds exactly three timestamps, all on 2026-06-11,
one per invocation of its companion ``nport_parallel_load.py``.

The parsing logic below is unchanged from the rescued original — deliberately,
because the table in production is its output and a "cleanup" here would make the
repair unverifiable against the numbers already measured. What was ADDED, and
only added, is what the 2026-08 repair needs and what would have caught the
defect in the first place:

* ``--only-report-dates`` — emit only the named report_dates. The repair must
  not write a single row outside the eight report_dates it is chartered to fix,
  and a DERA package carries the report_dates of its neighbours too.
* per-report_date ISIN statistics in the returned stats, so a parse can be
  judged BEFORE it is loaded. The historical defect was a silently
  under-populated ``HOLDING_ID -> ISIN`` map; that is visible in these counters
  and invisible everywhere else.
* ``--dedupe-conflict-key`` — apply the loader's ``(report_date, series_id,
  cusip)`` conflict key while streaming, keeping the first row per key. Off by
  default, so the rescued output is unchanged. It matters because the raw CSV
  and the table it produces are NOT the same population: ``LE:<lei>`` keys
  collide across every holding of one issuer inside a series, so the loader
  discards a large, ISIN-poor slice of the rows on its way in. Judging the raw
  CSV therefore mis-reads a good parse as a bad one — 2023-08-31 reads 0.586
  raw and 0.991 as the table will hold it. Only the deduped reading is
  comparable to what ``verify_isin_fill`` will see afterwards.

THE DEFECT THIS TOOL CAUSED, STATED PLAINLY
-------------------------------------------
``_load_isin_map`` is the whole game. When it comes back short, every holding
whose ``ISSUER_CUSIP`` is a placeholder silently walks past the ``IS:<isin>``
branch of ``_synthetic_cusip`` and lands on ``LE:<lei>``. Two consequences, both
of which hid the damage:

1. ``isin`` is written empty, so the row stops joining downstream — but nothing
   errors, because ``cusip`` is still populated with *something*.
2. ``LE:<lei>`` is NOT unique per security. Every holding of the same issuer
   inside one series collapses onto one ``(report_date, series_id, cusip)`` key,
   and the loader's ``ON CONFLICT DO NOTHING`` eats the rest. The 2023q4 and
   2025q1 loads lost 270,020 rows this way, on top of 5.34M missing ISINs.

Measured on the packages still on disk: the map is fine today (2023q4 yields
3,890,882 HOLDING_IDs with ISIN, 2025q1 yields 3,901,270) and re-parsing
recovers 92.5%–94.2% ISIN fill on the report_dates that sit at 15%–28% in
production. The files were never the problem.

Key differences from the legacy ``nport_parsed/*.csv`` (truncated at 200/filing,
and CUSIP-less holdings collapsed onto cusip='000000000'):

- NO per-filing truncation gate — every ``FUND_REPORTED_HOLDING`` row is emitted.
- Synthetic key for CUSIP-less holdings so they no longer collapse on the
  (report_date, series_id, cusip) PK. ~45% of rows carry cusip='000000000'
  (foreign issuers/ADRs without a US CUSIP, plus derivatives). We backfill the
  ``cusip`` column with, in priority order:
    1. the real ISSUER_CUSIP when present and not a placeholder,
    2. ``IS:<isin>``  (from IDENTIFIERS.tsv, by HOLDING_ID),
    3. ``LE:<lei>``   (ISSUER_LEI),
    4. ``H:<holding_id>`` (always unique within a filing — guarantees no collapse).
  The real ISIN is still stored separately in the ``isin`` column for look-through
  matching; synthetic ``cusip`` values are prefixed so they never false-match a
  real 9-char CUSIP downstream.

Source tables joined (all keyed by ACCESSION_NUMBER unless noted):
  FUND_REPORTED_HOLDING  — base rows (HOLDING_ID, ISSUER_*, BALANCE, PERCENTAGE, ...)
  REGISTRANT             — CIK
  FUND_REPORTED_INFO     — SERIES_ID (rows without series_id fall back to CIK:<cik>)
  SUBMISSION             — REPORT_DATE  (DD-MON-YYYY)
  IDENTIFIERS            — ISIN by HOLDING_ID (first ISIN wins)

Usage:
  python -m tools.nport_dera.nport_bulk_parse "E:\Edgard\nport\2023q4_nport" \
      -o out\2023q4.csv --only-report-dates 2023-09-30,2023-10-31
"""

from __future__ import annotations

import argparse
import collections
import csv
import os
import sys

CSV_COLS = [
    "report_date", "cik", "cusip", "isin", "issuer_name", "asset_class",
    "sector", "market_value", "quantity", "currency", "pct_of_nav",
    "is_restricted", "fair_value_level", "series_id",
]

_MONTHS = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
    "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}
_CUSIP_PLACEHOLDERS = {"", "N/A", "NA", "NONE", "000000000", "0", "XXXXXXXXX"}

#: ISIN fill rate a freshly parsed report_date has to clear before it is worth
#: loading. Matches ``src/workers/nport_identifier_coverage.DEFAULT_FLOOR``: the
#: worst clean report_date ever observed in production is 0.9445 and the *best*
#: degraded one is 0.6198, so 0.90 sits in 28 pp of daylight.
DEFAULT_FILL_FLOOR = 0.90


def _iso_date(raw: str) -> str | None:
    """Convert SEC 'DD-MON-YYYY' to ISO 'YYYY-MM-DD'."""
    parts = raw.strip().split("-")
    if len(parts) != 3:
        return None
    dd, mon, yyyy = parts
    mm = _MONTHS.get(mon.upper())
    if mm is None or not dd.isdigit() or not yyyy.isdigit():
        return None
    return f"{yyyy}-{mm}-{int(dd):02d}"


def _to_bigint(raw: str) -> str:
    """CURRENCY_VALUE -> bigint string (truncate decimals), '' on failure."""
    raw = raw.strip()
    if not raw:
        return ""
    try:
        return str(int(float(raw)))
    except ValueError:
        return ""


def _clean(raw: str) -> str:
    """Trim; map placeholder NULL-ish tokens to empty."""
    raw = raw.strip()
    return "" if raw.upper() in {"N/A", "NA", "NONE"} else raw


def _read_tsv_header(path: str) -> tuple[list[str], "csv.reader"]:
    fh = open(path, encoding="utf-8", newline="")
    reader = csv.reader(fh, delimiter="\t")
    header = next(reader)
    return header, reader


def _col_index(header: list[str], name: str) -> int:
    try:
        return header.index(name)
    except ValueError:
        sys.exit(f"missing column {name!r} in header {header}")


def _load_map(path: str, key_col: str, val_cols: list[str]) -> dict[str, tuple[str, ...]]:
    """Load a small dim TSV into {key: (val1, val2, ...)}; last row wins."""
    header, reader = _read_tsv_header(path)
    ki = _col_index(header, key_col)
    vis = [_col_index(header, c) for c in val_cols]
    out: dict[str, tuple[str, ...]] = {}
    for row in reader:
        if len(row) <= max(ki, *vis):
            continue
        out[row[ki]] = tuple(row[vi] for vi in vis)
    return out


def _load_isin_map(path: str) -> dict[str, str]:
    """HOLDING_ID -> first non-empty ISIN.

    The single most load-bearing function in this file: everything the 2023q4 and
    2025q1 loads got wrong is the difference between what this returns and what
    it returned then. Its size is logged and asserted on by the caller.
    """
    header, reader = _read_tsv_header(path)
    hi = _col_index(header, "HOLDING_ID")
    ii = _col_index(header, "IDENTIFIER_ISIN")
    out: dict[str, str] = {}
    for row in reader:
        if len(row) <= max(hi, ii):
            continue
        isin = row[ii].strip()
        if isin and row[hi] not in out:
            out[row[hi]] = isin
    return out


def _synthetic_cusip(raw_cusip: str, isin: str, lei: str, holding_id: str) -> tuple[str, bool]:
    """Return (cusip_value, is_synthetic)."""
    c = raw_cusip.strip()
    if c.upper() not in _CUSIP_PLACEHOLDERS:
        return c, False
    if isin:
        return f"IS:{isin}", True
    lei = lei.strip()
    if lei:
        return f"LE:{lei}", True
    return f"H:{holding_id}", True


def isin_fill_by_report_date(stats: dict) -> dict[str, float]:
    """{report_date: ISIN fill rate} from a ``parse_dataset`` stats dict."""
    per = stats["per_report_date"]
    return {rd: (c["isin"] / c["rows"] if c["rows"] else 0.0) for rd, c in sorted(per.items())}


def report_dates_below_floor(stats: dict, floor: float, min_rows: int = 1000) -> list[tuple[str, float]]:
    """Report_dates whose parsed ISIN fill is under ``floor``.

    A non-empty return means the parse is NOT fit to load: the ISIN map came back
    short, exactly as it did for 2023q4 and 2025q1. Loading it anyway is how the
    defect was created, and ``ON CONFLICT DO NOTHING`` makes it unfixable by a
    plain re-run afterwards.
    """
    per = stats["per_report_date"]
    return [
        (rd, fill)
        for rd, fill in isin_fill_by_report_date(stats).items()
        if per[rd]["rows"] >= min_rows and fill < floor
    ]


def parse_dataset(
    src_dir: str,
    out_path: str,
    only_report_dates: "set[str] | None" = None,
    dedupe_conflict_key: bool = False,
) -> dict:
    """Stream one unzipped DERA package into a load-ready CSV.

    ``only_report_dates`` (ISO ``YYYY-MM-DD``) restricts what is emitted. A
    quarterly package carries filings for report_dates outside the quarter it is
    named after, so a repair that targets specific report_dates MUST pass this
    or it will write rows it was not chartered to touch.

    ``dedupe_conflict_key`` applies the loader's ``(report_date, series_id,
    cusip)`` key here, first row wins — the same row the database would keep,
    since ``ON CONFLICT DO NOTHING`` resolves within-statement duplicates in
    scan order and the stage table is COPY'd in CSV order. Costs one set of
    keys in memory (~1 GB for a full quarter) and makes the emitted statistics
    predict the table instead of describing the file.
    """
    p = lambda name: os.path.join(src_dir, name)  # noqa: E731

    sys.stderr.write("loading dim tables (REGISTRANT, FUND_REPORTED_INFO, SUBMISSION)...\n")
    cik_by_acc = {k: v[0] for k, v in _load_map(p("REGISTRANT.tsv"), "ACCESSION_NUMBER", ["CIK"]).items()}
    series_by_acc = {k: v[0] for k, v in _load_map(p("FUND_REPORTED_INFO.tsv"), "ACCESSION_NUMBER", ["SERIES_ID"]).items()}
    sub = _load_map(p("SUBMISSION.tsv"), "ACCESSION_NUMBER", ["REPORT_DATE"])
    date_by_acc = {k: _iso_date(v[0]) for k, v in sub.items()}

    sys.stderr.write("loading IDENTIFIERS (ISIN by HOLDING_ID)...\n")
    isin_by_hid = _load_isin_map(p("IDENTIFIERS.tsv"))
    sys.stderr.write(f"  ISIN map: {len(isin_by_hid):,} holdings with ISIN\n")

    h_header, h_reader = _read_tsv_header(p("FUND_REPORTED_HOLDING.tsv"))
    idx = {c: _col_index(h_header, c) for c in (
        "ACCESSION_NUMBER", "HOLDING_ID", "ISSUER_NAME", "ISSUER_LEI", "ISSUER_CUSIP",
        "BALANCE", "CURRENCY_CODE", "CURRENCY_VALUE", "PERCENTAGE", "ASSET_CAT",
        "ISSUER_TYPE", "IS_RESTRICTED_SECURITY", "FAIR_VALUE_LEVEL",
    )}
    maxi = max(idx.values())

    stats: dict = {
        "rows": 0, "written": 0, "no_series": 0, "no_date": 0, "synthetic": 0,
        "series_from_cik": 0, "filtered_report_date": 0, "conflict_key_dupes": 0,
        "isin_map_size": len(isin_by_hid),
        "deduped": dedupe_conflict_key,
        "per_report_date": collections.defaultdict(collections.Counter),
    }
    seen: set[str] = set()
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    sys.stderr.write("streaming FUND_REPORTED_HOLDING...\n")
    with open(out_path, "w", encoding="utf-8", newline="") as out_fh:
        w = csv.writer(out_fh)
        w.writerow(CSV_COLS)
        for row in h_reader:
            stats["rows"] += 1
            if len(row) <= maxi:
                continue
            acc = row[idx["ACCESSION_NUMBER"]]
            cik = cik_by_acc.get(acc, "")
            series_id = series_by_acc.get(acc, "").strip()
            if not series_id:
                # Funds without a series structure (closed-end/BDC/large bond &
                # money-market filings). Keep 100% of holdings by synthesizing a
                # per-fund series key from the CIK rather than dropping them.
                if not cik:
                    stats["no_series"] += 1
                    continue
                series_id = f"CIK:{cik}"
                stats["series_from_cik"] += 1
            report_date = date_by_acc.get(acc)
            if not report_date:
                stats["no_date"] += 1
                continue
            if only_report_dates is not None and report_date not in only_report_dates:
                stats["filtered_report_date"] += 1
                continue
            hid = row[idx["HOLDING_ID"]]
            isin = isin_by_hid.get(hid, "")
            cusip, synth = _synthetic_cusip(
                row[idx["ISSUER_CUSIP"]], isin, row[idx["ISSUER_LEI"]], hid,
            )
            if dedupe_conflict_key:
                key = f"{report_date}\x00{series_id}\x00{cusip}"
                if key in seen:
                    stats["conflict_key_dupes"] += 1
                    continue
                seen.add(key)
            if synth:
                stats["synthetic"] += 1
            is_restricted = "true" if row[idx["IS_RESTRICTED_SECURITY"]].strip().upper() == "Y" else "false"
            w.writerow([
                report_date,
                cik_by_acc.get(acc, ""),
                cusip,
                isin,
                _clean(row[idx["ISSUER_NAME"]]),
                _clean(row[idx["ASSET_CAT"]]),
                _clean(row[idx["ISSUER_TYPE"]]),
                _to_bigint(row[idx["CURRENCY_VALUE"]]),
                _clean(row[idx["BALANCE"]]),
                _clean(row[idx["CURRENCY_CODE"]]),
                _clean(row[idx["PERCENTAGE"]]),
                is_restricted,
                _clean(row[idx["FAIR_VALUE_LEVEL"]]),
                series_id,
            ])
            stats["written"] += 1
            counter = stats["per_report_date"][report_date]
            counter["rows"] += 1
            if isin:
                counter["isin"] += 1
            if not synth:
                counter["real_cusip"] += 1
            elif cusip.startswith("IS:"):
                counter["synthetic_is"] += 1
            elif cusip.startswith("LE:"):
                counter["synthetic_le"] += 1
            else:
                counter["synthetic_h"] += 1
            if stats["rows"] % 1_000_000 == 0:
                sys.stderr.write(f"  {stats['rows']:,} read, {stats['written']:,} written\n")
    stats["per_report_date"] = dict(stats["per_report_date"])
    return stats


def _parse_dates(raw: str) -> "set[str] | None":
    if not raw.strip():
        return None
    return {d.strip() for d in raw.split(",") if d.strip()}


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("src_dir", help=r"unzipped bulk dataset dir, e.g. 'E:\Edgard\nport\2023q4_nport'")
    ap.add_argument("-o", "--out", required=True, help="output CSV path")
    ap.add_argument(
        "--only-report-dates", default="",
        help="comma-separated ISO report_dates; emit ONLY these (repair scope guard)",
    )
    ap.add_argument(
        "--dedupe-conflict-key", action="store_true",
        help="drop rows the loader's ON CONFLICT would discard anyway, so the "
             "reported ISIN fill predicts the table instead of the file",
    )
    ap.add_argument(
        "--fill-floor", type=float, default=DEFAULT_FILL_FLOOR,
        help="fail if any emitted report_date parses below this ISIN fill rate",
    )
    ap.add_argument(
        "--fill-floor-min-rows", type=int, default=1000,
        help="report_dates thinner than this are reported but not judged",
    )
    args = ap.parse_args(argv)

    stats = parse_dataset(
        args.src_dir, args.out, _parse_dates(args.only_report_dates), args.dedupe_conflict_key,
    )
    sys.stderr.write(
        f"\nDONE {args.src_dir}\n"
        f"  ISIN map size     : {stats['isin_map_size']:,}\n"
        f"  holding rows read : {stats['rows']:,}\n"
        f"  written           : {stats['written']:,}\n"
        f"  series from CIK   : {stats['series_from_cik']:,}\n"
        f"  dropped no_series : {stats['no_series']:,} (no CIK either)\n"
        f"  dropped no_date   : {stats['no_date']:,}\n"
        f"  out-of-scope date : {stats['filtered_report_date']:,}\n"
        f"  conflict-key dupes: {stats['conflict_key_dupes']:,}"
        f"{'' if stats['deduped'] else ' (not deduped -- readings below describe the FILE, not the table)'}\n"
        f"  synthetic cusip   : {stats['synthetic']:,}\n"
        f"  -> {args.out}\n\n"
    )
    sys.stderr.write(f"{'report_date':<12} {'rows':>10} {'isin_fill':>10} {'real':>9} {'IS:':>9} {'LE:':>9} {'H:':>9}\n")
    for rd, c in sorted(stats["per_report_date"].items()):
        n = c["rows"]
        sys.stderr.write(
            f"{rd:<12} {n:>10,} {c['isin'] / n:>10.4f} {c['real_cusip']:>9,} "
            f"{c['synthetic_is']:>9,} {c['synthetic_le']:>9,} {c['synthetic_h']:>9,}\n"
        )

    if not args.dedupe_conflict_key:
        sys.stderr.write(
            f"\nfill floor {args.fill_floor:.2f} NOT applied: without --dedupe-conflict-key the "
            "readings above are the file's, and the loader will discard a large, ISIN-poor slice "
            "of it on the conflict key. Re-run with --dedupe-conflict-key to judge the parse, or "
            "rely on the loader's post-load verify.\n"
        )
        return 0

    if args.only_report_dates.strip():
        # A scoped parse cannot judge a package. Outside its own quarter a package
        # carries only late and amended filings for a report_date -- a few thousand
        # rows from a handful of filers, whose ISIN fill is a property of those
        # filers, not of the ISIN map. 2024q1 reads 0.733 over its 6,743 rows on
        # 2023-09-30 and is a perfectly healthy package. The reading that decides a
        # repair is the loader's post-load verify over the MERGED report_date, which
        # is also the population the weekly monitor measures.
        sys.stderr.write(
            f"\nfill floor {args.fill_floor:.2f} NOT applied: --only-report-dates subsamples the "
            "package, so a per-report_date reading here is not a verdict on its ISIN map. The "
            "gate for a scoped repair is nport_parallel_load's post-load verify over the merged "
            "report_date.\n"
        )
        return 0

    bad = report_dates_below_floor(stats, args.fill_floor, args.fill_floor_min_rows)
    if bad:
        sys.stderr.write(
            f"\nREFUSING: {len(bad)} report_date(s) parsed below the {args.fill_floor:.2f} "
            f"ISIN fill floor: {bad}.\nThis is the 2023q4/2025q1 failure mode. Do NOT load this "
            "CSV: the loader is ON CONFLICT DO NOTHING, so the bad rows would own the primary "
            "key and no later re-run could displace them without a DELETE first.\n"
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

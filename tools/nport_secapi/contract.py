"""The measured monthly N-PORT quality contract, shared by every load phase.

The Q3 2026 DERA-keyed seed has no malformed source holdings or missing USD
market values. Complete dates retain 82.52–90.86% of selected source percentage
sums within five points, 87.72–93.40% within ten points, and 93.92–99.36% ISIN
fill. Source references preserve legitimate cash, leverage and derivative
weights that differ from 100. The small loss allowances below tolerate isolated
upstream defects without accepting a broken filing. Ratios are rounded only
for display, never before a blocking decision.
"""
from __future__ import annotations

import collections
import statistics
from collections.abc import Iterable, Mapping
from decimal import Decimal, InvalidOperation, getcontext, localcontext
from typing import Any

CONTRACT_VERSION = 1
PCT_WITHIN_5_FLOOR = Decimal("0.60")
PCT_WITHIN_10_FLOOR = Decimal("0.70")
ISIN_FLOOR = Decimal("0.90")
MAX_MISSING_MARKET_VALUE_SHARE = Decimal("0.01")
MAX_MALFORMED_HOLDINGS_SHARE = Decimal("0.01")


def isin_present(value: str | None) -> bool:
    """The SQL predicate ``isin IS NOT NULL AND isin <> ''``."""
    return value is not None and value != ""


def _below(numerator: int, denominator: int, floor: Decimal | float) -> bool:
    threshold = Decimal(str(floor))
    return threshold > 0 if denominator == 0 else Decimal(numerator) < threshold * denominator


def _above(numerator: int, denominator: int, ceiling: Decimal) -> bool:
    return denominator > 0 and Decimal(numerator) > ceiling * denominator


def exact_sum(left: Decimal, right: Decimal) -> Decimal:
    """Add filing digits without Python's default 28-digit context rounding."""
    if not left.is_finite() or not right.is_finite():
        return Decimal("NaN")
    precision = max(left.adjusted(), right.adjusted()) - min(left.as_tuple().exponent, right.as_tuple().exponent) + 2
    if precision <= getcontext().prec:
        return left + right
    with localcontext() as context:
        context.prec = precision
        return left + right


def _within(actual: Decimal, reference: Decimal, tolerance: int) -> bool:
    return exact_sum(actual, reference.copy_negate()).copy_abs() <= tolerance


def filing_reference_sums(date_manifest: Mapping[str, Any]) -> dict[str, Decimal]:
    """Independently recorded source percentages, keyed by selected series."""
    references = {}
    filings = date_manifest.get("filing_quality")
    if not isinstance(filings, list):
        return references
    for filing in filings:
        if not isinstance(filing, dict):
            continue
        try:
            value = Decimal(str(filing["source_pct_sum"]))
        except (InvalidOperation, KeyError, TypeError):
            continue  # missing reference is a blocking reading, not a 100 fallback
        if value.is_finite():
            references[filing["series_id"]] = value
    return references


def judge_isin_fill(
    counts: Mapping[str, tuple[int, int] | list[int]], report_dates: list[str] | None,
    floor: Decimal | float = ISIN_FLOOR,
) -> tuple[list[dict], list[dict]]:
    """Judge the full target date, including existing rows, from exact counts."""
    readings, below = [], []
    for rd in report_dates if report_dates is not None else sorted(counts):
        n, n_isin = counts.get(rd, (0, 0))
        reading = {"report_date": rd, "rows": n, "isin": n_isin,
                   "isin_fill": round(n_isin / n, 4) if n else 0.0}
        readings.append(reading)
        if _below(n_isin, n, floor):
            below.append(reading)
    return readings, below


def profile_series(
    report_date: str, aggregates: Iterable[Mapping[str, Any]], *,
    reference_sums: Mapping[str, Decimal | str] | None = None,
) -> dict:
    """Build one profile from per-series counts/sums, supplied by CSV or SQL.

    Each mapping contains ``series_id``, ``rows``, ``isin``, ``pct_sum``,
    ``pct_missing``, ``mv_missing``, ``market_value_usd_total``, and ``usd_rows``.
    SQL callers must coalesce a missing percentage sum to zero so that every
    observed series belongs to the denominator, even with no percentages.
    Monthly seeds compare against all holdings of their selected source filing
    before deliberate key deduplication. A seed without a monthly manifest uses
    100 as its reference. The 100-centered bands are also retained as reports.
    """
    totals: collections.Counter = collections.Counter()
    sums, values = [], []
    reference5 = reference10 = without_values = 0
    missing_references = []
    for aggregate in aggregates:
        for field in ("rows", "isin", "pct_missing", "mv_missing", "usd_rows"):
            totals[field] += int(aggregate.get(field, 0))
        pct = Decimal(str(aggregate.get("pct_sum") or 0))
        if not pct.is_finite():
            totals["nonfinite_percentages"] += 1
            pct = Decimal(0)
        sums.append(pct)
        without_values += int(aggregate["rows"]) == int(aggregate.get("pct_missing", 0))
        series_id = aggregate["series_id"]
        if reference_sums is None:
            reference = Decimal(100)
        elif series_id not in reference_sums:
            missing_references.append(series_id)
            reference = None
        else:
            reference = Decimal(str(reference_sums[series_id]))
            if not reference.is_finite():
                missing_references.append(series_id)
                reference = None
        if reference is not None:
            reference5 += _within(pct, reference, 5)
            reference10 += _within(pct, reference, 10)
        values.append(int(aggregate.get("market_value_usd_total") or 0))
    series = len(sums)
    within5 = sum(_within(v, Decimal(100), 5) for v in sums)
    within10 = sum(_within(v, Decimal(100), 10) for v in sums)
    rows = totals["rows"]
    return {
        "report_date": report_date, "rows": rows, "series": series, "isin": totals["isin"],
        "foreign_report_dates": {},
        "isin_fill": round(totals["isin"] / rows, 4) if rows else 0.0,
        "usd_row_share": round(totals["usd_rows"] / rows, 4) if rows else 0.0,
        "pct_missing": totals["pct_missing"], "mv_missing": totals["mv_missing"],
        "mv_missing_share": round(totals["mv_missing"] / rows, 6) if rows else 0.0,
        "pct_sum_median": round(float(statistics.median(sums)), 3) if sums else None,
        "pct_within_5_count": within5, "pct_within_10_count": within10,
        "pct_sum_within_5": round(within5 / series, 4) if series else 0.0,
        "pct_sum_within_10": round(within10 / series, 4) if series else 0.0,
        "pct_reference_basis": "source" if reference_sums is not None else "100",
        "pct_reference_within_5_count": reference5, "pct_reference_within_10_count": reference10,
        "pct_reference_within_5": round(reference5 / series, 4) if series else 0.0,
        "pct_reference_within_10": round(reference10 / series, 4) if series else 0.0,
        "pct_series_without_values": without_values,
        "pct_missing_references": missing_references,
        "pct_sum_over_1000": sum(v > 1000 for v in sums),
        "nonfinite_percentages": totals["nonfinite_percentages"],
        "market_value_usd_total": sum(values),
        "market_value_usd_median_series": statistics.median(values) if values else None,
        "keys": {},
    }


class ValidationAccumulator:
    """Stream rows while retaining only per-series aggregates, not holdings."""

    def __init__(self) -> None:
        self.series: dict[str, dict] = {}
        self.dates: collections.Counter = collections.Counter()
        self.keys: collections.Counter = collections.Counter()

    def add(self, row: Mapping[str, Any]) -> None:
        series = row["series_id"]
        bucket = self.series.setdefault(series, {
            "series_id": series, "rows": 0, "isin": 0, "pct_sum": Decimal(0),
            "pct_missing": 0, "mv_missing": 0, "market_value_usd_total": 0, "usd_rows": 0,
        })
        bucket["rows"] += 1
        self.dates[row["report_date"]] += 1
        if row["pct_of_nav"] is not None and row["pct_of_nav"] != "":
            bucket["pct_sum"] = exact_sum(bucket["pct_sum"], Decimal(str(row["pct_of_nav"])))
        else:
            bucket["pct_missing"] += 1
        if row["market_value"] is not None and row["market_value"] != "":
            bucket["market_value_usd_total"] += int(row["market_value"])
        else:
            bucket["mv_missing"] += 1
        bucket["isin"] += isin_present(row["isin"])
        bucket["usd_rows"] += row["currency"] == "USD"
        key = row["cusip"] or ""
        kind = key[:3] if key[:3] in ("IS:", "LE:") else ("H:" if key.startswith("H:") else "real")
        self.keys[kind] += 1

    def profile(self, report_date: str, *, reference_sums: Mapping[str, Decimal | str] | None = None) -> dict:
        profile = profile_series(report_date, self.series.values(), reference_sums=reference_sums)
        profile["foreign_report_dates"] = {d: n for d, n in self.dates.items() if d != report_date}
        profile["keys"] = dict(self.keys)
        return profile


def malformed_verdict(date_manifest: Mapping[str, Any], only_series: set[str] | None = None) -> list[str]:
    """Judge source losses only for the selected winning filings; dedupes are reported."""
    filings = date_manifest.get("filing_quality")
    if not isinstance(filings, list):
        return ["manifest lacks per-filing malformed-holding accounting; reconvert the containers"]
    required = {"accession", "series_id", "source_holdings", "malformed_holdings", "rows", "conflict_key_dupes",
                "source_pct_sum", "source_pct_present"}
    for filing in filings:
        if not isinstance(filing, dict) or not required <= filing.keys():
            return ["manifest has invalid per-filing malformed-holding accounting; reconvert the containers"]
        counts = [filing[k] for k in ("source_holdings", "malformed_holdings", "rows", "conflict_key_dupes")]
        if any(type(n) is not int or n < 0 for n in counts) or counts[0] != sum(counts[1:]):
            return ["manifest has inconsistent per-filing holding counts; reconvert the containers"]
        present = filing["source_pct_present"]
        try:
            reference = Decimal(str(filing["source_pct_sum"]))
        except (InvalidOperation, ValueError):
            reference = Decimal("NaN")
        if type(present) is not int or not 0 <= present <= counts[0] or not reference.is_finite():
            return ["manifest has invalid source percentage accounting; reconvert the containers"]
        if not present and reference != 0:
            return ["manifest has inconsistent source percentage accounting; reconvert the containers"]
    available = {f["series_id"] for f in filings}
    if len(available) != len(filings):
        return ["manifest repeats a series' selected filing; reconvert the containers"]
    if only_series is not None and (missing := only_series - available):
        return [f"manifest lacks selected series: {sorted(missing)}; reconvert the containers"]
    selected = [f for f in filings if only_series is None or f["series_id"] in only_series]
    problems, total, malformed = [], 0, 0
    for filing in selected:
        n, bad = filing["source_holdings"], filing["malformed_holdings"]
        total += n
        malformed += bad
        if _above(bad, n, MAX_MALFORMED_HOLDINGS_SHARE):
            problems.append(
                f"filing {filing['accession']} ({filing['series_id']}): malformed holdings "
                f"{bad}/{n} exceeds {MAX_MALFORMED_HOLDINGS_SHARE:.0%}"
            )
    if _above(malformed, total, MAX_MALFORMED_HOLDINGS_SHARE):
        problems.append(
            f"date malformed holdings {malformed}/{total} exceeds {MAX_MALFORMED_HOLDINGS_SHARE:.0%}"
        )
    return problems


def quality_verdict(
    profile: Mapping[str, Any], *, isin_floor: Decimal | float = ISIN_FLOOR, include_isin: bool = True,
) -> list[str]:
    """The blocking contract. Descriptive readings do not introduce extra gates."""
    problems = list(profile.get("malformed_problems", ()))
    if profile.get("foreign_report_dates"):
        problems.append(f"rows for other report_dates: {profile['foreign_report_dates']}")
    rows, series = profile["rows"], profile["series"]
    if not rows or not series:
        problems.append("no holdings in the selected series")
        return problems
    if profile.get("nonfinite_percentages"):
        problems.append("nonfinite percentage values")
    if profile.get("pct_series_without_values"):
        problems.append(f"{profile['pct_series_without_values']} series have no percentage values")
    if profile.get("pct_missing_references"):
        missing = profile["pct_missing_references"]
        problems.append(f"missing source percentage references for {len(missing)} series (e.g. {missing[:5]})")
    if include_isin and _below(profile["isin"], rows, isin_floor):
        problems.append(f"isin_fill {profile['isin_fill']} < {isin_floor}")
    if _above(profile["mv_missing"], rows, MAX_MISSING_MARKET_VALUE_SHARE):
        problems.append(
            f"missing market value {profile['mv_missing']}/{rows} exceeds {MAX_MISSING_MARKET_VALUE_SHARE:.0%}"
        )
    for band, floor in ((5, PCT_WITHIN_5_FLOOR), (10, PCT_WITHIN_10_FLOOR)):
        count = profile[f"pct_reference_within_{band}_count"]
        if _below(count, series, floor):
            expectation = "agree with source" if profile["pct_reference_basis"] == "source" else "sum to 100"
            problems.append(f"only {count}/{series} of series {expectation} +/- {band} (floor {floor:.0%})")
    return problems

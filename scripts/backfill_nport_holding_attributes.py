"""Backfill exact N-PORT equity classification inputs from local DERA bundles.

The cloud holdings primary key collapses some distinct DERA holding lots. This
loader therefore leaves ``sec_nport_holdings`` untouched and materializes exact,
compact rollups by series/report date for the look-through producer.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Iterator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_nport_fund_flows import (  # noqa: E402
    DEFAULT_NPORT_DIRS,
    parse_decimal,
    parse_sec_date,
)
from src.db import connect  # noqa: E402
from src.workers.nport_lookthrough import ISO_3166_ALPHA2  # noqa: E402

DEFAULT_MINIMUM_MATCH_RATE = 0.99


@dataclass(frozen=True)
class EquitySummary:
    report_date: dt.date
    series_id: str
    gross_equity_pct: float
    net_equity_pct: float
    source_quarter: str

    def as_tuple(self) -> tuple[dt.date, str, float, float, str]:
        return (
            self.report_date,
            self.series_id,
            self.gross_equity_pct,
            self.net_equity_pct,
            self.source_quarter,
        )


@dataclass(frozen=True)
class CountryExposure:
    report_date: dt.date
    series_id: str
    country: str
    direct_pct: float
    source_quarter: str

    def as_tuple(self) -> tuple[dt.date, str, str, float, str]:
        return (
            self.report_date,
            self.series_id,
            self.country,
            self.direct_pct,
            self.source_quarter,
        )


@dataclass(frozen=True)
class HoldingWeight:
    report_date: dt.date
    series_id: str
    cusip: str
    signed_pct_of_nav: float
    source_quarter: str

    def as_tuple(self) -> tuple[dt.date, str, str, float, str]:
        return (
            self.report_date,
            self.series_id,
            self.cusip,
            self.signed_pct_of_nav,
            self.source_quarter,
        )


def _value(value: str | None) -> str | None:
    normalized = (value or "").strip().upper()
    if normalized in {"", "N/A", "NA", "NULL", "NONE"}:
        return None
    return normalized


def _rows(path: Path) -> Iterator[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        yield from csv.DictReader(handle, delimiter="\t")


def _accession_map(path: Path, value_column: str) -> dict[str, str]:
    return {
        accession: value
        for row in _rows(path)
        if (accession := _value(row.get("ACCESSION_NUMBER")))
        and (value := _value(row.get(value_column)))
    }


def _identifier_isins(dataset_dir: Path) -> dict[str, str]:
    path = dataset_dir / "IDENTIFIERS.tsv"
    if not path.exists():
        return {}
    return {
        holding_id: isin
        for row in _rows(path)
        if (holding_id := _value(row.get("HOLDING_ID")))
        and (isin := _value(row.get("IDENTIFIER_ISIN")))
    }


def _holding_key(row: dict[str, str], identifier_isins: dict[str, str]) -> str | None:
    cusip = _value(row.get("ISSUER_CUSIP"))
    if (
        cusip is not None
        and len(cusip) == 9
        and cusip.isalnum()
        and cusip != "000000000"
    ):
        return cusip
    holding_id = _value(row.get("HOLDING_ID"))
    isin = identifier_isins.get(holding_id or "")
    if isin:
        return f"IS:{isin}"
    issuer_lei = _value(row.get("ISSUER_LEI"))
    if issuer_lei:
        return f"LE:{issuer_lei}"
    return f"H:{holding_id}" if holding_id else None


def _selected_series_reports(dataset_dir: Path) -> dict[str, tuple[dt.date, str]]:
    series_ids = _accession_map(dataset_dir / "FUND_REPORTED_INFO.tsv", "SERIES_ID")
    selected: dict[
        tuple[dt.date, str], tuple[tuple[bool, dt.date, str], str]
    ] = {}
    for row in _rows(dataset_dir / "SUBMISSION.tsv"):
        accession = _value(row.get("ACCESSION_NUMBER"))
        if not accession or accession not in series_ids:
            continue
        report_date = parse_sec_date(row.get("REPORT_DATE"))
        if report_date is None:
            continue
        filing_date = parse_sec_date(row.get("FILING_DATE")) or dt.date.min
        priority = (_value(row.get("IS_LAST_FILING")) == "Y", filing_date, accession)
        key = (report_date, series_ids[accession])
        if key not in selected or priority > selected[key][0]:
            selected[key] = (priority, accession)
    return {accession: key for key, (_priority, accession) in selected.items()}


def build_equity_rollups(
    dataset_dir: Path,
) -> tuple[list[EquitySummary], list[CountryExposure], list[HoldingWeight]]:
    dataset_dir = dataset_dir.resolve()
    selected = _selected_series_reports(dataset_dir)
    identifier_isins = _identifier_isins(dataset_dir)
    totals: dict[tuple[dt.date, str], list[Decimal]] = {
        key: [Decimal(0), Decimal(0)] for key in selected.values()
    }
    country_totals: dict[tuple[dt.date, str, str], Decimal] = {}
    holding_totals: dict[tuple[dt.date, str, str], Decimal] = {}

    for row in _rows(dataset_dir / "FUND_REPORTED_HOLDING.tsv"):
        accession = _value(row.get("ACCESSION_NUMBER"))
        if not accession or accession not in selected:
            continue
        if _value(row.get("ASSET_CAT")) not in {"EC", "EP"}:
            continue
        pct = parse_decimal(row.get("PERCENTAGE"))
        if pct is None:
            continue
        payoff = _value(row.get("PAYOFF_PROFILE"))
        signed_pct = -abs(pct) if payoff == "SHORT" else abs(pct) if payoff == "LONG" else pct
        report_date, series_id = selected[accession]
        summary = totals.setdefault((report_date, series_id), [Decimal(0), Decimal(0)])
        summary[0] += abs(signed_pct)
        summary[1] += signed_pct
        country = _value(row.get("INVESTMENT_COUNTRY"))
        if country not in ISO_3166_ALPHA2:
            country = "UNKNOWN"
        key = (report_date, series_id, country)
        country_totals[key] = country_totals.get(key, Decimal(0)) + signed_pct
        cusip = _holding_key(row, identifier_isins)
        if cusip is not None:
            holding_key = (report_date, series_id, cusip)
            holding_totals[holding_key] = (
                holding_totals.get(holding_key, Decimal(0)) + signed_pct
            )

    source_quarter = dataset_dir.name
    summaries = [
        EquitySummary(report_date, series_id, float(values[0]), float(values[1]), source_quarter)
        for (report_date, series_id), values in sorted(totals.items())
    ]
    countries = [
        CountryExposure(report_date, series_id, country, float(value), source_quarter)
        for (report_date, series_id, country), value in sorted(country_totals.items())
    ]
    weights = [
        HoldingWeight(report_date, series_id, cusip, float(value), source_quarter)
        for (report_date, series_id, cusip), value in sorted(holding_totals.items())
    ]
    return summaries, countries, weights


def copy_rollups(
    cur,
    summaries: Iterable[EquitySummary],
    countries: Iterable[CountryExposure],
    weights: Iterable[HoldingWeight],
) -> tuple[int, int, int]:
    summary_count = 0
    with cur.copy(
        "COPY tmp_nport_equity_exposure_summary "
        "(report_date, series_id, gross_equity_pct, net_equity_pct, source_quarter) "
        "FROM STDIN"
    ) as stream:
        for row in summaries:
            stream.write_row(row.as_tuple())
            summary_count += 1
    country_count = 0
    with cur.copy(
        "COPY tmp_nport_equity_country_exposures "
        "(report_date, series_id, country, direct_pct, source_quarter) FROM STDIN"
    ) as stream:
        for row in countries:
            stream.write_row(row.as_tuple())
            country_count += 1
    weight_count = 0
    with cur.copy(
        "COPY tmp_nport_equity_holding_weights "
        "(report_date, series_id, cusip, signed_pct_of_nav, source_quarter) FROM STDIN"
    ) as stream:
        for row in weights:
            stream.write_row(row.as_tuple())
            weight_count += 1
    return summary_count, country_count, weight_count


def apply_schema(conn) -> None:
    schema = ROOT / "schemas" / "nport_equity_classification_inputs.sql"
    with conn.cursor() as cur:
        cur.execute(schema.read_text(encoding="utf-8"))
    conn.commit()


def _prepare_stage(cur) -> None:
    cur.execute(
        """
        CREATE TEMP TABLE tmp_nport_equity_exposure_summary (
            report_date date NOT NULL,
            series_id text NOT NULL,
            gross_equity_pct numeric(14,6) NOT NULL,
            net_equity_pct numeric(14,6) NOT NULL,
            source_quarter text NOT NULL
        ) ON COMMIT DROP;
        CREATE TEMP TABLE tmp_nport_equity_country_exposures (
            report_date date NOT NULL,
            series_id text NOT NULL,
            country text NOT NULL,
            direct_pct numeric(14,6) NOT NULL,
            source_quarter text NOT NULL
        ) ON COMMIT DROP;
        CREATE TEMP TABLE tmp_nport_equity_holding_weights (
            report_date date NOT NULL,
            series_id text NOT NULL,
            cusip text NOT NULL,
            signed_pct_of_nav numeric(14,6) NOT NULL,
            source_quarter text NOT NULL
        ) ON COMMIT DROP;
        """
    )


def _match_stats(cur) -> tuple[int, int, float]:
    cur.execute(
        """
        SELECT count(*) AS summary_rows,
               count(*) FILTER (
                   WHERE EXISTS (
                       SELECT 1 FROM sec_nport_holdings AS holdings
                       WHERE holdings.report_date = source.report_date
                         AND holdings.series_id = source.series_id
                   )
               ) AS matched_rows
        FROM tmp_nport_equity_exposure_summary AS source
        """
    )
    summary_rows, matched_rows = (int(value) for value in cur.fetchone())
    match_rate = matched_rows / summary_rows if summary_rows else 1.0
    return summary_rows, matched_rows, match_rate


def _upsert_rollups(cur) -> tuple[int, int, int]:
    cur.execute(
        """
        INSERT INTO nport_equity_exposure_summary
            (report_date, series_id, gross_equity_pct, net_equity_pct, source_quarter)
        SELECT report_date, series_id, gross_equity_pct, net_equity_pct, source_quarter
        FROM tmp_nport_equity_exposure_summary
        ON CONFLICT (report_date, series_id) DO UPDATE SET
            gross_equity_pct = EXCLUDED.gross_equity_pct,
            net_equity_pct = EXCLUDED.net_equity_pct,
            source_quarter = EXCLUDED.source_quarter,
            computed_at = now()
        """
    )
    summary_changes = cur.rowcount
    cur.execute(
        """
        DELETE FROM nport_equity_country_exposures AS target
        USING tmp_nport_equity_exposure_summary AS source
        WHERE target.report_date = source.report_date
          AND target.series_id = source.series_id
        """
    )
    cur.execute(
        """
        INSERT INTO nport_equity_country_exposures
            (report_date, series_id, country, direct_pct, source_quarter)
        SELECT report_date, series_id, country, direct_pct, source_quarter
        FROM tmp_nport_equity_country_exposures
        """
    )
    country_changes = cur.rowcount
    cur.execute(
        """
        DELETE FROM nport_equity_holding_weights AS target
        USING tmp_nport_equity_exposure_summary AS source
        WHERE target.report_date = source.report_date
          AND target.series_id = source.series_id
        """
    )
    cur.execute(
        """
        INSERT INTO nport_equity_holding_weights
            (report_date, series_id, cusip, signed_pct_of_nav, source_quarter)
        SELECT report_date, series_id, cusip, signed_pct_of_nav, source_quarter
        FROM tmp_nport_equity_holding_weights
        """
    )
    return summary_changes, country_changes, cur.rowcount


def load_directory(
    conn,
    dataset_dir: Path,
    *,
    apply: bool,
    minimum_match_rate: float = DEFAULT_MINIMUM_MATCH_RATE,
) -> dict[str, int | float | str | bool]:
    summaries, countries, weights = build_equity_rollups(dataset_dir)
    try:
        with conn.cursor() as cur:
            _prepare_stage(cur)
            summary_rows, country_rows, weight_rows = copy_rollups(
                cur, summaries, countries, weights
            )
            matched_rows, _matched_again, match_rate = _match_stats(cur)
            if matched_rows != summary_rows:
                raise RuntimeError("staging row count changed unexpectedly")
            if match_rate < minimum_match_rate:
                raise ValueError(
                    f"series/report match rate {match_rate:.6f} is below "
                    f"{minimum_match_rate:.6f}"
                )
            summary_changes, country_changes, weight_changes = (
                _upsert_rollups(cur) if apply else (0, 0, 0)
            )
        conn.commit() if apply else conn.rollback()
    except Exception:
        conn.rollback()
        raise
    return {
        "directory": str(dataset_dir.resolve()),
        "summary_rows": summary_rows,
        "country_rows": country_rows,
        "weight_rows": weight_rows,
        "matched_rows": _matched_again,
        "match_rate": match_rate,
        "summary_changes": summary_changes,
        "country_changes": country_changes,
        "weight_changes": weight_changes,
        "applied": apply,
    }


def load_directories(
    dsn: str | None,
    dirs: Iterable[Path],
    *,
    apply: bool,
    schema: bool,
    minimum_match_rate: float,
) -> list[dict[str, int | float | str | bool]]:
    results = []
    with connect(dsn) as conn:
        if apply and schema:
            apply_schema(conn)
        for dataset_dir in dirs:
            result = load_directory(
                conn,
                dataset_dir,
                apply=apply,
                minimum_match_rate=minimum_match_rate,
            )
            print(json.dumps(result, sort_keys=True))
            results.append(result)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dirs", nargs="*", type=Path)
    parser.add_argument("--dsn", default=None, help="Database DSN; defaults to DATABASE_URL")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply rollups; without this flag the script performs a rolled-back dry run",
    )
    parser.add_argument("--no-schema", action="store_true")
    parser.add_argument(
        "--minimum-match-rate", type=float, default=DEFAULT_MINIMUM_MATCH_RATE
    )
    args = parser.parse_args()
    dirs = args.dirs or list(DEFAULT_NPORT_DIRS)
    results = load_directories(
        args.dsn,
        dirs,
        apply=args.apply,
        schema=not args.no_schema,
        minimum_match_rate=args.minimum_match_rate,
    )
    print(
        json.dumps(
            {
                "directories": len(results),
                "summary_rows": sum(int(item["summary_rows"]) for item in results),
                "country_rows": sum(int(item["country_rows"]) for item in results),
                "weight_rows": sum(int(item["weight_rows"]) for item in results),
                "summary_changes": sum(int(item["summary_changes"]) for item in results),
                "country_changes": sum(int(item["country_changes"]) for item in results),
                "weight_changes": sum(int(item["weight_changes"]) for item in results),
                "applied": args.apply,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

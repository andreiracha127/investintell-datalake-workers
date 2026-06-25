"""Load N-PORT fund-level sales/redemption flows from DERA TSV datasets."""

from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from src.db import connect

DEFAULT_NPORT_DIRS = tuple(
    Path("E:/Edgard") / name
    for name in (
        "2019q4_nport",
        "2020q1_nport",
        "2020q2_nport",
        "2020q3_nport",
        "2020q4_nport",
        "2021q1_nport",
        "2021q2_nport",
        "2021q3_nport",
        "2021q4_nport",
        "2022q1_nport",
        "2022q2_nport",
        "2022q3_nport",
        "2022q4_nport",
        "2023q1_nport",
        "2023q2_nport",
        "2023q3_nport",
        "2023q4_nport",
        "2024q1_nport",
        "2024q2_nport",
        "2024q3_nport",
        "2024q4_nport",
        "2025q1_nport",
        "2025q2_nport",
        "2025q3_nport",
        "2025q4_nport",
        "2026q1_nport",
    )
)

REPORTED_INFO_COLUMNS = (
    "accession_number",
    "series_id",
    "series_name",
    "series_lei",
    "report_date",
    "filing_date",
    "total_assets",
    "total_liabilities",
    "net_assets",
    "sales_flow_mon1",
    "reinvestment_flow_mon1",
    "redemption_flow_mon1",
    "sales_flow_mon2",
    "reinvestment_flow_mon2",
    "redemption_flow_mon2",
    "sales_flow_mon3",
    "reinvestment_flow_mon3",
    "redemption_flow_mon3",
    "source_quarter",
    "source_file",
)

MONTHLY_FLOW_COLUMNS = (
    "accession_number",
    "series_id",
    "series_name",
    "report_date",
    "filing_date",
    "flow_month_end",
    "month_ordinal",
    "total_assets",
    "net_assets",
    "sales_flow",
    "reinvestment_flow",
    "redemption_flow",
    "gross_subscription_flow",
    "gross_redemption_flow",
    "net_flow",
    "net_flow_pct_assets",
    "source_quarter",
    "source_file",
)


@dataclass(frozen=True)
class Submission:
    report_date: dt.date
    filing_date: dt.date | None


@dataclass(frozen=True)
class ReportedInfo:
    accession_number: str
    series_id: str | None
    series_name: str | None
    series_lei: str | None
    report_date: dt.date
    filing_date: dt.date | None
    total_assets: Decimal | None
    total_liabilities: Decimal | None
    net_assets: Decimal | None
    sales_flow_mon1: Decimal | None
    reinvestment_flow_mon1: Decimal | None
    redemption_flow_mon1: Decimal | None
    sales_flow_mon2: Decimal | None
    reinvestment_flow_mon2: Decimal | None
    redemption_flow_mon2: Decimal | None
    sales_flow_mon3: Decimal | None
    reinvestment_flow_mon3: Decimal | None
    redemption_flow_mon3: Decimal | None
    source_quarter: str
    source_file: str

    def as_tuple(self) -> tuple:
        return tuple(getattr(self, c) for c in REPORTED_INFO_COLUMNS)


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def parse_sec_date(value: str | None) -> dt.date | None:
    value = _blank_to_none(value)
    if value is None:
        return None
    return dt.datetime.strptime(value.upper(), "%d-%b-%Y").date()


def parse_decimal(value: str | None) -> Decimal | None:
    value = _blank_to_none(value)
    if value is None:
        return None
    try:
        return Decimal(value.replace(",", ""))
    except InvalidOperation:
        return None


def add_months(value: dt.date, months: int) -> dt.date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return dt.date(year, month, day)


def month_end(value: dt.date) -> dt.date:
    return dt.date(value.year, value.month, calendar.monthrange(value.year, value.month)[1])


def flow_month_end(report_date: dt.date, month_ordinal: int) -> dt.date:
    # N-PORT reports the first, second, and third months in the three-month
    # period ending at REPORT_DATE. For a 2024-03-31 report: Jan, Feb, Mar.
    return month_end(add_months(report_date, month_ordinal - 3))


def _read_submissions(dataset_dir: Path) -> dict[str, Submission]:
    path = dataset_dir / "SUBMISSION.tsv"
    if not path.exists():
        raise FileNotFoundError(path)
    submissions: dict[str, Submission] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            accession = _blank_to_none(row.get("ACCESSION_NUMBER"))
            report_date = parse_sec_date(row.get("REPORT_DATE"))
            if accession and report_date:
                submissions[accession] = Submission(
                    report_date=report_date,
                    filing_date=parse_sec_date(row.get("FILING_DATE")),
                )
    return submissions


def iter_reported_info(dataset_dir: Path) -> Iterable[ReportedInfo]:
    path = dataset_dir / "FUND_REPORTED_INFO.tsv"
    if not path.exists():
        raise FileNotFoundError(path)
    submissions = _read_submissions(dataset_dir)
    source_quarter = dataset_dir.name
    source_file = str(path)
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            accession = _blank_to_none(row.get("ACCESSION_NUMBER"))
            if not accession:
                continue
            submission = submissions.get(accession)
            if submission is None:
                continue
            yield ReportedInfo(
                accession_number=accession,
                series_id=_blank_to_none(row.get("SERIES_ID")),
                series_name=_blank_to_none(row.get("SERIES_NAME")),
                series_lei=_blank_to_none(row.get("SERIES_LEI")),
                report_date=submission.report_date,
                filing_date=submission.filing_date,
                total_assets=parse_decimal(row.get("TOTAL_ASSETS")),
                total_liabilities=parse_decimal(row.get("TOTAL_LIABILITIES")),
                net_assets=parse_decimal(row.get("NET_ASSETS")),
                sales_flow_mon1=parse_decimal(row.get("SALES_FLOW_MON1")),
                reinvestment_flow_mon1=parse_decimal(row.get("REINVESTMENT_FLOW_MON1")),
                redemption_flow_mon1=parse_decimal(row.get("REDEMPTION_FLOW_MON1")),
                sales_flow_mon2=parse_decimal(row.get("SALES_FLOW_MON2")),
                reinvestment_flow_mon2=parse_decimal(row.get("REINVESTMENT_FLOW_MON2")),
                redemption_flow_mon2=parse_decimal(row.get("REDEMPTION_FLOW_MON2")),
                sales_flow_mon3=parse_decimal(row.get("SALES_FLOW_MON3")),
                reinvestment_flow_mon3=parse_decimal(row.get("REINVESTMENT_FLOW_MON3")),
                redemption_flow_mon3=parse_decimal(row.get("REDEMPTION_FLOW_MON3")),
                source_quarter=source_quarter,
                source_file=source_file,
            )


def _zero(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def iter_monthly_flows(info: ReportedInfo) -> Iterable[tuple]:
    if not info.series_id:
        return
    for ordinal in (1, 2, 3):
        sales = getattr(info, f"sales_flow_mon{ordinal}")
        reinvestment = getattr(info, f"reinvestment_flow_mon{ordinal}")
        redemption = getattr(info, f"redemption_flow_mon{ordinal}")
        gross_subscription = _zero(sales) + _zero(reinvestment)
        gross_redemption = abs(_zero(redemption))
        net_flow = gross_subscription - gross_redemption
        net_flow_pct = None
        if info.net_assets is not None and info.net_assets > 0:
            net_flow_pct = net_flow / info.net_assets
        yield (
            info.accession_number,
            info.series_id,
            info.series_name,
            info.report_date,
            info.filing_date,
            flow_month_end(info.report_date, ordinal),
            ordinal,
            info.total_assets,
            info.net_assets,
            sales,
            reinvestment,
            redemption,
            gross_subscription,
            gross_redemption,
            net_flow,
            net_flow_pct,
            info.source_quarter,
            info.source_file,
        )


def apply_schema(conn) -> None:
    schema = Path(__file__).resolve().parents[1] / "schemas" / "nport_fund_flows.sql"
    with conn.cursor() as cur:
        cur.execute(schema.read_text(encoding="utf-8"))
    conn.commit()


def _copy_rows(cur, table: str, columns: tuple[str, ...], rows: Iterable[tuple]) -> int:
    count = 0
    col_sql = ", ".join(columns)
    with cur.copy(f"COPY {table} ({col_sql}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
            count += 1
    return count


def load_directory(conn, dataset_dir: Path) -> dict[str, int | str]:
    dataset_dir = dataset_dir.resolve()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tmp_sec_nport_fund_reported_info")
        cur.execute("DROP TABLE IF EXISTS tmp_sec_nport_fund_monthly_flows")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_sec_nport_fund_reported_info (
                accession_number text,
                series_id text,
                series_name text,
                series_lei text,
                report_date date,
                filing_date date,
                total_assets numeric,
                total_liabilities numeric,
                net_assets numeric,
                sales_flow_mon1 numeric,
                reinvestment_flow_mon1 numeric,
                redemption_flow_mon1 numeric,
                sales_flow_mon2 numeric,
                reinvestment_flow_mon2 numeric,
                redemption_flow_mon2 numeric,
                sales_flow_mon3 numeric,
                reinvestment_flow_mon3 numeric,
                redemption_flow_mon3 numeric,
                source_quarter text,
                source_file text
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            """
            CREATE TEMP TABLE tmp_sec_nport_fund_monthly_flows (
                accession_number text,
                series_id text,
                series_name text,
                report_date date,
                filing_date date,
                flow_month_end date,
                month_ordinal smallint,
                total_assets numeric,
                net_assets numeric,
                sales_flow numeric,
                reinvestment_flow numeric,
                redemption_flow numeric,
                gross_subscription_flow numeric,
                gross_redemption_flow numeric,
                net_flow numeric,
                net_flow_pct_assets numeric,
                source_quarter text,
                source_file text
            ) ON COMMIT DROP
            """
        )

        reported_rows = list(iter_reported_info(dataset_dir))
        monthly_rows = [row for info in reported_rows for row in iter_monthly_flows(info)]
        reported_count = _copy_rows(
            cur,
            "tmp_sec_nport_fund_reported_info",
            REPORTED_INFO_COLUMNS,
            (row.as_tuple() for row in reported_rows),
        )
        monthly_count = _copy_rows(
            cur,
            "tmp_sec_nport_fund_monthly_flows",
            MONTHLY_FLOW_COLUMNS,
            monthly_rows,
        )
        cur.execute(
            f"""
            INSERT INTO sec_nport_fund_reported_info
                ({", ".join(REPORTED_INFO_COLUMNS)})
            SELECT {", ".join(REPORTED_INFO_COLUMNS)}
            FROM tmp_sec_nport_fund_reported_info
            ON CONFLICT (accession_number) DO UPDATE SET
                series_id = EXCLUDED.series_id,
                series_name = EXCLUDED.series_name,
                series_lei = EXCLUDED.series_lei,
                report_date = EXCLUDED.report_date,
                filing_date = EXCLUDED.filing_date,
                total_assets = EXCLUDED.total_assets,
                total_liabilities = EXCLUDED.total_liabilities,
                net_assets = EXCLUDED.net_assets,
                sales_flow_mon1 = EXCLUDED.sales_flow_mon1,
                reinvestment_flow_mon1 = EXCLUDED.reinvestment_flow_mon1,
                redemption_flow_mon1 = EXCLUDED.redemption_flow_mon1,
                sales_flow_mon2 = EXCLUDED.sales_flow_mon2,
                reinvestment_flow_mon2 = EXCLUDED.reinvestment_flow_mon2,
                redemption_flow_mon2 = EXCLUDED.redemption_flow_mon2,
                sales_flow_mon3 = EXCLUDED.sales_flow_mon3,
                reinvestment_flow_mon3 = EXCLUDED.reinvestment_flow_mon3,
                redemption_flow_mon3 = EXCLUDED.redemption_flow_mon3,
                source_quarter = EXCLUDED.source_quarter,
                source_file = EXCLUDED.source_file,
                updated_at = now()
            """
        )
        cur.execute(
            f"""
            INSERT INTO sec_nport_fund_monthly_flows
                ({", ".join(MONTHLY_FLOW_COLUMNS)})
            SELECT {", ".join(MONTHLY_FLOW_COLUMNS)}
            FROM tmp_sec_nport_fund_monthly_flows
            ON CONFLICT (accession_number, month_ordinal) DO UPDATE SET
                series_id = EXCLUDED.series_id,
                series_name = EXCLUDED.series_name,
                report_date = EXCLUDED.report_date,
                filing_date = EXCLUDED.filing_date,
                flow_month_end = EXCLUDED.flow_month_end,
                total_assets = EXCLUDED.total_assets,
                net_assets = EXCLUDED.net_assets,
                sales_flow = EXCLUDED.sales_flow,
                reinvestment_flow = EXCLUDED.reinvestment_flow,
                redemption_flow = EXCLUDED.redemption_flow,
                gross_subscription_flow = EXCLUDED.gross_subscription_flow,
                gross_redemption_flow = EXCLUDED.gross_redemption_flow,
                net_flow = EXCLUDED.net_flow,
                net_flow_pct_assets = EXCLUDED.net_flow_pct_assets,
                source_quarter = EXCLUDED.source_quarter,
                source_file = EXCLUDED.source_file,
                updated_at = now()
            """
        )
    conn.commit()
    return {
        "directory": str(dataset_dir),
        "reported_rows": reported_count,
        "monthly_rows": monthly_count,
    }


def load_directories(dsn: str | None, dirs: Iterable[Path], *, schema: bool) -> list[dict]:
    stats = []
    with connect(dsn) as conn:
        if schema:
            apply_schema(conn)
        for dataset_dir in dirs:
            stats.append(load_directory(conn, dataset_dir))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dirs", nargs="*", type=Path, help="N-PORT quarterly dataset directories")
    parser.add_argument("--dsn", default=None, help="Database DSN; defaults to DATABASE_URL")
    parser.add_argument("--no-schema", action="store_true", help="Do not apply schemas/nport_fund_flows.sql first")
    args = parser.parse_args()

    dirs = args.dirs or list(DEFAULT_NPORT_DIRS)
    stats = load_directories(args.dsn, dirs, schema=not args.no_schema)
    total_reported = sum(int(s["reported_rows"]) for s in stats)
    total_monthly = sum(int(s["monthly_rows"]) for s in stats)
    for item in stats:
        print(item)
    print({
        "directories": len(stats),
        "reported_rows": total_reported,
        "monthly_rows": total_monthly,
    })


if __name__ == "__main__":
    main()

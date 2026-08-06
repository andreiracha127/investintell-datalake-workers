"""Backfill exact N-PORT equity classification inputs from local DERA bundles.

The cloud holdings primary key collapses some distinct DERA holding lots. This
loader therefore leaves ``sec_nport_holdings`` untouched and materializes exact,
compact rollups by series/report date for the look-through producer.

Three guard rails, all three paid for in production:

* **``IDENTIFIERS.tsv`` is mandatory.** ``_identifier_isins`` used to return an
  empty map when the file was absent, and every CUSIP-less holding then fell one
  rung down ``_holding_key`` onto ``LE:<issuer_lei>`` — which is not unique per
  security, so distinct lots of one issuer collapsed onto a single
  ``(report_date, series_id, cusip)``. That is the same defect the 2026-08-05
  ``sec_nport_holdings`` repair removed from the base, and on 2026-08-06 the
  sidecar was found still carrying it on eight report_dates (25k-185k ``LE:``
  keys per date, against ~1k-4k on the untouched control dates). Nothing errored
  at the time, because a populated-but-wrong key looks exactly like a right one.
  Missing identifiers now fail the run instead of silently degrading it.
* **``--only-report-dates`` scopes the write.** ``load_directory`` otherwise
  processes a whole quarterly bundle, so rebuilding one report_date rewrites
  every neighbour that shares the bundle — including the dates being held aside
  as untouched controls. Declarations for a given period arrive in bundles up to
  three years later, so a faithful rebuild has to read many bundles while
  writing only the target dates.
* **``--min-isin-fill`` is a quality floor, not a presence check.** The bullet
  above only refuses a bundle whose ``IDENTIFIERS.tsv`` is *absent*. A file that
  is present but short reads exactly like a good one, and that is not
  hypothetical: the 2026-07-16 sidecar build had the file on every bundle and
  still left ~10k keys one rung down the ladder, which cost two repair waves to
  pay back. This floor makes a short map fail the run. See
  ``isin_key_fill_by_report_date`` for what is measured and why it is measured
  on the emitted rollup rather than on the parsed input.
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
from src.workers.nport_lookthrough import (  # noqa: E402
    ISO_3166_ALPHA2,
    equity_country_key,
)

DEFAULT_MINIMUM_MATCH_RATE = 0.99

#: Share of the identifier-map-dependent keys of one ``report_date`` that must
#: resolve to ``IS:<isin>``. Derived, not chosen: measured over the 102
#: report_dates of ``nport_equity_holding_weights`` after the 2026-08-06 repair
#: waves, the 100 judged dates run 0.8296 (2023-04-28, the same date that is the
#: worst clean reading of the base table's own probe) to 0.9942, median 0.9796.
#: The eight dates the sidecar was rotten on read 0.1347 to 0.7015. 0.75 sits
#: 7.96 pp below the worst healthy date ever observed and 4.85 pp above the best
#: degraded one, and catches all eight.
DEFAULT_MIN_ISIN_FILL = 0.75

#: report_dates with fewer identifier-dependent keys than this are not judged.
#: Off-cycle fiscal month-ends are thin (2020-08-30 emits 7 keys and none of
#: them synthetic, 2022-09-28 emits 291) and one filer's junk would otherwise
#: read as a failed bundle. This is also what makes a scoped ``--only-report-
#: dates`` run over a neighbouring bundle safe: outside its own quarter a bundle
#: carries only late and amended declarations, whose fill is the filer's
#: property and not the ISIN map's.
DEFAULT_MIN_ISIN_FILL_KEYS = 1000


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
    gross_pct_of_nav: float | None = None

    def as_tuple(self) -> tuple[dt.date, str, str, float, float | None, str]:
        return (
            self.report_date,
            self.series_id,
            self.cusip,
            self.signed_pct_of_nav,
            self.gross_pct_of_nav,
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
        # Fail closed: without this map every CUSIP-less holding lands on
        # ``LE:<issuer_lei>``, which collapses distinct securities of one issuer
        # onto a single key. See the module docstring.
        raise FileNotFoundError(
            f"{path} is missing; without it every CUSIP-less holding falls back "
            "to a non-unique LE:<issuer_lei> key and lots collapse silently"
        )
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


def _selected_series_reports(
    dataset_dir: Path,
    only_report_dates: frozenset[dt.date] | None = None,
) -> dict[str, tuple[dt.date, str]]:
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
        if only_report_dates is not None and report_date not in only_report_dates:
            continue
        filing_date = parse_sec_date(row.get("FILING_DATE")) or dt.date.min
        priority = (_value(row.get("IS_LAST_FILING")) == "Y", filing_date, accession)
        key = (report_date, series_ids[accession])
        if key not in selected or priority > selected[key][0]:
            selected[key] = (priority, accession)
    return {accession: key for key, (_priority, accession) in selected.items()}


def build_equity_rollups(
    dataset_dir: Path,
    only_report_dates: frozenset[dt.date] | None = None,
) -> tuple[list[EquitySummary], list[CountryExposure], list[HoldingWeight]]:
    dataset_dir = dataset_dir.resolve()
    selected = _selected_series_reports(dataset_dir, only_report_dates)
    identifier_isins = _identifier_isins(dataset_dir)
    totals: dict[tuple[dt.date, str], list[Decimal]] = {
        key: [Decimal(0), Decimal(0)] for key in selected.values()
    }
    country_totals: dict[tuple[dt.date, str, str], Decimal] = {}
    holding_totals: dict[tuple[dt.date, str, str], list[Decimal]] = {}

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
        if country is None:
            holding_id = _value(row.get("HOLDING_ID"))
            country = equity_country_key(
                identifier_isins.get(holding_id or ""),
                _value(row.get("ASSET_CAT")),
                _holding_key(row, identifier_isins),
            )
        elif country not in ISO_3166_ALPHA2:
            country = "UNKNOWN"
        country = country or "UNKNOWN"
        key = (report_date, series_id, country)
        country_totals[key] = country_totals.get(key, Decimal(0)) + signed_pct
        cusip = _holding_key(row, identifier_isins)
        if cusip is not None:
            holding_key = (report_date, series_id, cusip)
            holding_values = holding_totals.setdefault(
                holding_key, [Decimal(0), Decimal(0)]
            )
            holding_values[0] += signed_pct
            holding_values[1] += abs(signed_pct)

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
        HoldingWeight(
            report_date,
            series_id,
            cusip,
            float(values[0]),
            source_quarter,
            float(values[1]),
        )
        for (report_date, series_id, cusip), values in sorted(holding_totals.items())
    ]
    return summaries, countries, weights


def isin_key_fill_by_report_date(
    weights: Iterable[HoldingWeight],
) -> dict[dt.date, dict[str, int | float]]:
    """Per ``report_date`` ISIN fill of the keys ``build_equity_rollups`` emits.

    MEASURED ON THE OUTPUT, ON PURPOSE
    ----------------------------------
    The first ISIN floor written in this codebase measured the parsed input, and
    it refused the very re-parse that fixed the defect: 2023-08-31 read 0.5858 as
    a file and 0.9912 as the table it produced. The distortion has one cause in
    both places — ``LE:<lei>`` is not unique per security, so many input rows
    land on one key. Downstream of that collapse the ISIN-poor rows are
    over-represented in the input and under-represented in the output, and only
    the output is the population the table will hold. So the reading is taken
    over ``weights``, after aggregation, one row per
    ``(report_date, series_id, key)`` — the same rows ``copy_rollups`` is about
    to stage.

    WHY THE DENOMINATOR EXCLUDES REAL CUSIPs
    ----------------------------------------
    A holding with a valid CUSIP-9 never consults the ISIN map, so counting it
    only dilutes. Over the same 102 report_dates the two candidate readings
    separate healthy from rotten by 12.81 pp (this one) and 6.09 pp (the share
    of *all* keys that are a CUSIP-9 or an ``IS:``). That is the same argument
    ``src/workers/nport_identifier_coverage`` makes for gating on the ISIN
    column instead of on "is the row identifiable at all". Both readings are
    returned; only ``isin_key_fill`` is gated on.
    """
    per: dict[dt.date, dict[str, int | float]] = {}
    for weight in weights:
        counts = per.setdefault(
            weight.report_date,
            {"keys": 0, "real": 0, "isin": 0, "lei": 0, "holding": 0},
        )
        counts["keys"] = int(counts["keys"]) + 1
        if weight.cusip.startswith("IS:"):
            bucket = "isin"
        elif weight.cusip.startswith("LE:"):
            bucket = "lei"
        elif weight.cusip.startswith("H:"):
            bucket = "holding"
        else:
            bucket = "real"
        counts[bucket] = int(counts[bucket]) + 1
    for counts in per.values():
        synthetic = int(counts["isin"]) + int(counts["lei"]) + int(counts["holding"])
        counts["synthetic_keys"] = synthetic
        counts["isin_key_fill"] = (
            round(int(counts["isin"]) / synthetic, 4) if synthetic else None
        )
        counts["identifiable_share"] = (
            round((int(counts["real"]) + int(counts["isin"])) / int(counts["keys"]), 4)
            if counts["keys"]
            else None
        )
    return dict(sorted(per.items()))


def report_dates_below_isin_floor(
    weights: Iterable[HoldingWeight],
    floor: float,
    min_keys: int = DEFAULT_MIN_ISIN_FILL_KEYS,
) -> list[tuple[dt.date, float]]:
    """report_dates whose emitted ISIN fill is under ``floor``.

    A non-empty return means the bundle's ``HOLDING_ID -> ISIN`` map came back
    short and the rollup being staged carries the collapsed ``LE:<lei>`` keys
    that the 2026-08-05 and 2026-08-06 repairs removed. Writing it anyway is how
    the defect was created the first time.
    """
    return [
        (report_date, float(counts["isin_key_fill"]))
        for report_date, counts in isin_key_fill_by_report_date(weights).items()
        if int(counts["synthetic_keys"]) >= min_keys
        and float(counts["isin_key_fill"]) < floor
    ]


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
        "(report_date, series_id, cusip, signed_pct_of_nav, gross_pct_of_nav, "
        "source_quarter) FROM STDIN"
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
            gross_pct_of_nav numeric(14,6) NOT NULL,
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
            (report_date, series_id, cusip, signed_pct_of_nav, gross_pct_of_nav,
             source_quarter)
        SELECT report_date, series_id, cusip, signed_pct_of_nav, gross_pct_of_nav,
               source_quarter
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
    minimum_isin_fill: float = DEFAULT_MIN_ISIN_FILL,
    only_report_dates: frozenset[dt.date] | None = None,
) -> dict[str, int | float | str | bool | None]:
    summaries, countries, weights = build_equity_rollups(dataset_dir, only_report_dates)
    fills = isin_key_fill_by_report_date(weights)
    judged = [
        counts
        for counts in fills.values()
        if int(counts["synthetic_keys"]) >= DEFAULT_MIN_ISIN_FILL_KEYS
    ]
    # Before the connection is opened, let alone before anything is staged: a
    # short ISIN map is a property of the bundle on disk, and nothing about the
    # database can change the verdict.
    if minimum_isin_fill > 0:
        below = report_dates_below_isin_floor(weights, minimum_isin_fill)
        if below:
            raise ValueError(
                f"{dataset_dir} has an ISIN map too short to load: "
                + ", ".join(f"{d.isoformat()} {fill:.4f}" for d, fill in below)
                + f" is under --min-isin-fill {minimum_isin_fill:.4f}. Every "
                "CUSIP-less holding the map misses falls onto LE:<issuer_lei>, "
                "which is not unique per security, so distinct lots of one "
                "issuer collapse onto one (report_date, series_id, cusip). Pass "
                "--min-isin-fill 0 to write it anyway."
            )
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
        "only_report_dates": (
            ",".join(sorted(d.isoformat() for d in only_report_dates))
            if only_report_dates is not None
            else ""
        ),
        "summary_rows": summary_rows,
        "country_rows": country_rows,
        "weight_rows": weight_rows,
        "matched_rows": _matched_again,
        "match_rate": match_rate,
        "min_isin_fill": minimum_isin_fill,
        "isin_fill_judged_report_dates": len(judged),
        "worst_isin_key_fill": min(
            (float(counts["isin_key_fill"]) for counts in judged), default=None
        ),
        "worst_identifiable_share": min(
            (float(counts["identifiable_share"]) for counts in judged), default=None
        ),
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
    minimum_isin_fill: float = DEFAULT_MIN_ISIN_FILL,
    only_report_dates: frozenset[dt.date] | None = None,
) -> list[dict[str, int | float | str | bool | None]]:
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
                minimum_isin_fill=minimum_isin_fill,
                only_report_dates=only_report_dates,
            )
            print(json.dumps(result, sort_keys=True))
            results.append(result)
    return results


def _parse_only_report_dates(raw: str | None) -> frozenset[dt.date] | None:
    if raw is None:
        return None
    parsed = set()
    for token in (part.strip() for part in raw.split(",")):
        if not token:
            continue
        try:
            parsed.add(dt.date.fromisoformat(token))
        except ValueError:
            raise SystemExit(f"--only-report-dates: {token!r} is not YYYY-MM-DD")
    if not parsed:
        # An empty scope would silently mean "write nothing", which reads as a
        # successful no-op run. Refuse it rather than paint that green.
        raise SystemExit("--only-report-dates was given but lists no date")
    return frozenset(parsed)


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
    parser.add_argument(
        "--min-isin-fill",
        type=float,
        default=DEFAULT_MIN_ISIN_FILL,
        metavar="FLOAT",
        help=(
            "Refuse a bundle whose emitted rollup carries fewer than this share "
            f"of identifier-dependent keys resolved to IS:<isin> (default "
            f"{DEFAULT_MIN_ISIN_FILL}, judged per report_date over "
            f"{DEFAULT_MIN_ISIN_FILL_KEYS}+ such keys). Measured on the rollup "
            "being staged, never on the parsed input. Pass 0 to disable, "
            "explicitly and on the record."
        ),
    )
    parser.add_argument(
        "--only-report-dates",
        default=None,
        metavar="YYYY-MM-DD[,YYYY-MM-DD...]",
        help=(
            "Restrict every read and write to these report_dates. Without it a "
            "run rewrites every report_date the given bundles carry, including "
            "neighbours being held aside as controls."
        ),
    )
    args = parser.parse_args()
    only_report_dates = _parse_only_report_dates(args.only_report_dates)
    dirs = args.dirs or list(DEFAULT_NPORT_DIRS)
    results = load_directories(
        args.dsn,
        dirs,
        apply=args.apply,
        schema=not args.no_schema,
        minimum_match_rate=args.minimum_match_rate,
        minimum_isin_fill=args.min_isin_fill,
        only_report_dates=only_report_dates,
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
                "min_isin_fill": args.min_isin_fill,
                "worst_isin_key_fill": min(
                    (
                        float(item["worst_isin_key_fill"])
                        for item in results
                        if item["worst_isin_key_fill"] is not None
                    ),
                    default=None,
                ),
                "applied": args.apply,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

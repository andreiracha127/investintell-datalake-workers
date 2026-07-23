"""Deterministic, local-only N-PORT V2 COPY payload generator."""

from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import shutil
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

import duckdb


SOURCE_FILES = (
    "SUBMISSION.tsv",
    "FUND_REPORTED_INFO.tsv",
    "FUND_REPORTED_HOLDING.tsv",
    "IDENTIFIERS.tsv",
    "DEBT_SECURITY.tsv",
)
BRIDGE_COLUMNS = (
    "publication_id", "accession_number", "holding_id", "instrument_id", "series_id",
    "class_id", "valid_from", "valid_to", "resolution_state", "source_candidate_key_evidence",
)
HOLDINGS_COLUMNS = (
    "publication_id", "accession_number", "holding_id", "source_run_id", "report_date",
    "filing_date", "source_series_id", "issuer_name", "issuer_category", "cusip", "isin",
    "issuer_lei", "signed_market_value", "signed_pct_of_nav", "payoff_profile",
    "source_typed_projection",
)
_BATCH_SIZE = 50_000


class SourceHashMismatch(ValueError):
    """A required input does not match its pinned SHA-256."""


class DuplicatePrimaryKeyError(ValueError):
    """The resulting publication would contain a duplicate COPY primary key."""


@dataclass(frozen=True)
class GenerationResult:
    bridge_path: Path
    holdings_path: Path
    manifest_path: Path


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_date(value: str) -> dt.date:
    for pattern in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(value.strip().upper(), pattern).date()
        except ValueError:
            pass
    raise ValueError(f"unsupported N-PORT date: {value!r}")


def _decimal_text(value: str | None, payoff: str | None) -> str | None:
    if not value or value.strip().upper() in {"N/A", "NA", "NULL"}:
        return None
    try:
        parsed = Decimal(value.replace(",", "").strip())
    except InvalidOperation as error:
        raise ValueError(f"invalid decimal value: {value!r}") from error
    if (payoff or "").strip().lower() in {"short", "short position"}:
        parsed = -abs(parsed)
    return format(parsed.normalize(), "f")


def normalize_cusip(value: str | None) -> str | None:
    candidate = "".join((value or "").upper().split())
    if (
        len(candidate) != 9
        or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789*@#" for char in candidate)
        or not candidate[8].isdigit()
    ):
        return None

    def digit(char: str) -> int:
        if char.isdigit():
            return int(char)
        return {"*": 36, "@": 37, "#": 38}.get(char, ord(char) - 55)

    total = 0
    for index, char in enumerate(candidate[:8]):
        value_digit = digit(char) * (2 if index % 2 else 1)
        total += value_digit // 10 + value_digit % 10
    return candidate if (10 - total % 10) % 10 == int(candidate[8]) else None


def normalize_isin(value: str | None) -> str | None:
    candidate = "".join((value or "").upper().split())
    if len(candidate) != 12 or not candidate.isalnum():
        return None
    expanded = "".join(char if char.isdigit() else str(ord(char) - 55) for char in candidate)
    total = 0
    for index, char in enumerate(reversed(expanded)):
        number = int(char) * (2 if index % 2 else 1)
        total += number // 10 + number % 10
    return candidate if total % 10 == 0 else None


def normalize_lei(value: str | None) -> str | None:
    candidate = "".join((value or "").upper().split())
    if len(candidate) != 20 or not candidate.isalnum():
        return None
    remainder = 0
    for char in "".join(char if char.isdigit() else str(ord(char) - 55) for char in candidate):
        remainder = (remainder * 10 + int(char)) % 97
    return candidate if remainder == 1 else None


def _hashes(source_dir: Path, expected_hashes: dict[str, str]) -> dict[str, str]:
    if set(expected_hashes) != set(SOURCE_FILES):
        raise ValueError("expected hashes must name exactly the five required N-PORT TSV inputs")
    actual: dict[str, str] = {}
    for filename in SOURCE_FILES:
        path = source_dir / filename
        if not path.is_file():
            raise FileNotFoundError(path)
        actual[filename] = _sha256_file(path)
        if actual[filename].lower() != expected_hashes[filename].lower():
            raise SourceHashMismatch(f"SHA-256 mismatch for {filename}")
    return actual


def _install_views(connection: duckdb.DuckDBPyConnection, source_dir: Path) -> None:
    connection.create_function("nport_norm_cusip", normalize_cusip, ["VARCHAR"], "VARCHAR", null_handling="special")
    connection.create_function("nport_norm_isin", normalize_isin, ["VARCHAR"], "VARCHAR", null_handling="special")
    for filename, name in (("SUBMISSION.tsv", "submission"), ("FUND_REPORTED_INFO.tsv", "info"),
                           ("FUND_REPORTED_HOLDING.tsv", "holding"), ("IDENTIFIERS.tsv", "identifiers"),
                           ("DEBT_SECURITY.tsv", "debt")):
        sql_path = str(source_dir / filename).replace("'", "''")
        connection.execute(
            f"CREATE TEMP VIEW {name} AS SELECT * FROM read_csv('{sql_path}', header=true, delim='\\t', all_varchar=true)",
        )
    connection.execute(
        """
        CREATE TEMP TABLE filing_candidates AS
        SELECT trim(i.SERIES_ID) AS SERIES_ID,
               s.ACCESSION_NUMBER,
               s.REPORT_DATE,
               s.FILING_DATE,
               s.SUB_TYPE,
               s.source_ordinal,
               nullif(trim(i.SERIES_ID), '') IS NOT NULL
                 AND upper(trim(s.SUB_TYPE)) IN ('NPORT-P', 'NPORT-P/A')
                 AND try_strptime(s.FILING_DATE, '%d-%b-%Y') IS NOT NULL
                 AND try_strptime(s.REPORT_DATE, '%d-%b-%Y') IS NOT NULL AS eligible
        FROM (SELECT *, row_number() OVER () AS source_ordinal FROM submission) s
        JOIN info i USING (ACCESSION_NUMBER)
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE selected_filings AS
        SELECT SERIES_ID, ACCESSION_NUMBER, REPORT_DATE, FILING_DATE
        FROM (
          SELECT *,
                 row_number() OVER (
                   PARTITION BY SERIES_ID, REPORT_DATE
                   ORDER BY CASE WHEN upper(SUB_TYPE) LIKE '%/A' THEN 1 ELSE 0 END DESC,
                            try_strptime(FILING_DATE, '%d-%b-%Y') DESC,
                            ACCESSION_NUMBER DESC,
                            source_ordinal DESC
                 ) AS selection_rank
          FROM filing_candidates
          WHERE eligible
        ) ranked
        WHERE selection_rank = 1
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE selected_holding_keys AS
        SELECT h.ACCESSION_NUMBER, h.HOLDING_ID
        FROM holding h
        JOIN selected_filings USING (ACCESSION_NUMBER)
        """
    )


def _assert_unambiguous(connection: duckdb.DuckDBPyConnection) -> None:
    duplicate = connection.execute(
        """
        SELECT HOLDING_ID, count(DISTINCT ACCESSION_NUMBER) FROM selected_holding_keys
        GROUP BY HOLDING_ID HAVING count(DISTINCT ACCESSION_NUMBER)>1 LIMIT 1
        """
    ).fetchone()
    if duplicate:
        raise ValueError(f"ambiguous selected parent for child HOLDING_ID {duplicate[0]}")
    duplicate_pk = connection.execute(
        """
        SELECT ACCESSION_NUMBER, HOLDING_ID FROM selected_holding_keys
        GROUP BY ACCESSION_NUMBER, HOLDING_ID HAVING count(*)>1 LIMIT 1
        """
    ).fetchone()
    if duplicate_pk:
        raise DuplicatePrimaryKeyError(f"duplicate V2 publication primary key for {duplicate_pk[0]}/{duplicate_pk[1]}")
    duplicate_debt = connection.execute(
        """
        SELECT d.HOLDING_ID FROM debt d JOIN selected_holding_keys USING (HOLDING_ID)
        GROUP BY d.HOLDING_ID HAVING count(*)>1 LIMIT 1
        """
    ).fetchone()
    if duplicate_debt:
        raise ValueError(f"ambiguous DEBT_SECURITY child rows for HOLDING_ID {duplicate_debt[0]}")


def _selected_rows(connection: duckdb.DuckDBPyConnection) -> Iterator[dict[str, Any]]:
    cursor = connection.execute(
        """
        WITH isin_sets AS (
          SELECT HOLDING_ID,list_sort(list(DISTINCT nport_norm_isin(IDENTIFIER_ISIN))
                   FILTER (WHERE nport_norm_isin(IDENTIFIER_ISIN) IS NOT NULL)) AS values
          FROM identifiers GROUP BY HOLDING_ID
        ), debt_rows AS (SELECT HOLDING_ID,to_json(debt) AS debt_json FROM debt)
        SELECT selected.SERIES_ID,selected.ACCESSION_NUMBER,selected.REPORT_DATE,selected.FILING_DATE,
               h.HOLDING_ID,h.ISSUER_NAME,h.ISSUER_TYPE,h.ISSUER_CUSIP,h.ISSUER_LEI,h.ASSET_CAT,
               h.PAYOFF_PROFILE,h.PERCENTAGE,h.CURRENCY_VALUE,to_json(h) AS holding_json,
               isin_sets.values AS isin_values,debt_rows.debt_json
        FROM selected_filings selected JOIN holding h USING (ACCESSION_NUMBER)
        LEFT JOIN isin_sets USING (HOLDING_ID) LEFT JOIN debt_rows USING (HOLDING_ID)
        ORDER BY selected.SERIES_ID,selected.REPORT_DATE,selected.ACCESSION_NUMBER,h.HOLDING_ID
        """
    )
    reader = cursor.to_arrow_reader(batch_size=_BATCH_SIZE)
    for batch in reader:
        yield from batch.to_pylist()


def _copy_field(value: str | None) -> str:
    if value is None:
        return "\\N"
    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def _open_tsv(path: Path, columns: tuple[str, ...]):
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("w", encoding="utf-8", newline="")
    stream.write("\t".join(columns) + "\n")
    return stream


def _write_row(stream: Any, columns: tuple[str, ...], row: dict[str, str | None]) -> None:
    stream.write("\t".join(_copy_field(row.get(column)) for column in columns) + "\n")


def _gzip_payload(source: Path, destination: Path) -> str:
    with source.open("rb") as raw, destination.open("wb") as output:
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as zipped:
            for chunk in iter(lambda: raw.read(1024 * 1024), b""):
                zipped.write(chunk)
    source.unlink()
    return _sha256_file(destination)


def generate(
    *, source_dir: Path, source_run_id: str, package_id: str, package_sha256: str,
    parser_version: str, publication_id: str, expected_hashes: dict[str, str], output_dir: Path,
    generator_version: str, config_version: str,
) -> GenerationResult:
    """Build pinned V2 payloads using local DuckDB and bounded record batches only."""
    source_dir, output_dir = Path(source_dir), Path(output_dir)
    if output_dir.exists():
        raise FileExistsError(f"output directory already exists: {output_dir}")
    source_hashes = _hashes(source_dir, expected_hashes)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = output_dir.parent / f".{output_dir.name}.tmp-{uuid.uuid4().hex}"
    staging_dir.mkdir()
    try:
        connection = duckdb.connect(":memory:")
        try:
            _install_views(connection, source_dir)
            _assert_unambiguous(connection)
            selected_holdings = connection.execute("SELECT count(*) FROM selected_holding_keys").fetchone()[0]
            if not selected_holdings:
                raise ValueError("no eligible N-PORT holdings found")
            candidate_counts = connection.execute(
                """
                SELECT count(*) AS candidates,
                       count(*) FILTER (WHERE eligible) AS eligible,
                       count(*) FILTER (WHERE NOT eligible) AS excluded,
                       (SELECT count(*) FROM selected_filings) AS selected
                FROM filing_candidates
                """
            ).fetchone()
            bridge_plain = staging_dir / "sec_nport_instrument_class_bridge.tsv"
            holdings_plain = staging_dir / "sec_nport_holdings_v2.tsv"
            counts = {
                "bridge_rows": 0,
                "holdings_rows": 0,
                "identified_rows": 0,
                "unidentified_rows": 0,
                "resolved_rows": 0,
                "duplicate_primary_keys": 0,
                "filing_candidates": candidate_counts[0],
                "eligible_filing_candidates": candidate_counts[1],
                "excluded_ineligible_filings": candidate_counts[2],
                "selected_filings": candidate_counts[3],
            }
            series: set[str] = set()
            report_dates: set[str] = set()
            with (
                _open_tsv(bridge_plain, BRIDGE_COLUMNS) as bridge,
                _open_tsv(holdings_plain, HOLDINGS_COLUMNS) as holdings,
            ):
                for raw in _selected_rows(connection):
                    accession, holding_id = raw["ACCESSION_NUMBER"], raw["HOLDING_ID"]
                    report_date = _parse_date(raw["REPORT_DATE"]).isoformat()
                    filing_date = _parse_date(raw["FILING_DATE"]).isoformat()
                    cusip = normalize_cusip(raw["ISSUER_CUSIP"])
                    isins = sorted(set(raw["isin_values"] or []))
                    unique_isin = isins[0] if len(isins) == 1 else None
                    instrument_id = f"CUSIP:{cusip}" if cusip else (f"ISIN:{unique_isin}" if unique_isin else None)
                    projection = json.loads(raw["holding_json"])
                    if raw["debt_json"]:
                        debt_projection = json.loads(raw["debt_json"])
                        debt_projection.pop("HOLDING_ID", None)
                        maturity_date = debt_projection.get("MATURITY_DATE")
                        if maturity_date:
                            try:
                                debt_projection["MATURITY_DATE"] = _parse_date(maturity_date).isoformat()
                            except ValueError:
                                debt_projection.pop("MATURITY_DATE")
                        projection["DEBT_SECURITY"] = debt_projection
                    evidence = {
                        "accession_number": accession,
                        "holding_id": holding_id,
                        "identifier_candidates": {"cusip": cusip, "isin": isins},
                        "security_identity": instrument_id,
                    }
                    _write_row(bridge, BRIDGE_COLUMNS, {
                        "publication_id": publication_id, "accession_number": accession, "holding_id": holding_id,
                        "instrument_id": instrument_id, "series_id": raw["SERIES_ID"], "class_id": None,
                        "valid_from": report_date, "valid_to": None, "resolution_state": "resolved",
                        "source_candidate_key_evidence": _canonical_json(evidence),
                    })
                    _write_row(holdings, HOLDINGS_COLUMNS, {
                        "publication_id": publication_id, "accession_number": accession, "holding_id": holding_id,
                        "source_run_id": source_run_id, "report_date": report_date, "filing_date": filing_date,
                        "source_series_id": raw["SERIES_ID"], "issuer_name": raw["ISSUER_NAME"] or None,
                        "issuer_category": raw["ISSUER_TYPE"] or None, "cusip": cusip, "isin": unique_isin,
                        "issuer_lei": normalize_lei(raw["ISSUER_LEI"]),
                        "signed_market_value": _decimal_text(raw["CURRENCY_VALUE"], raw["PAYOFF_PROFILE"]),
                        "signed_pct_of_nav": _decimal_text(raw["PERCENTAGE"], raw["PAYOFF_PROFILE"]),
                        "payoff_profile": raw["PAYOFF_PROFILE"] or None,
                        "source_typed_projection": _canonical_json(projection),
                    })
                    counts["bridge_rows"] += 1
                    counts["holdings_rows"] += 1
                    counts["resolved_rows"] += 1
                    counts["identified_rows" if instrument_id else "unidentified_rows"] += 1
                    series.add(raw["SERIES_ID"])
                    report_dates.add(report_date)
        finally:
            connection.close()

        bridge_staged = staging_dir / "sec_nport_instrument_class_bridge.tsv.gz"
        holdings_staged = staging_dir / "sec_nport_holdings_v2.tsv.gz"
        payload_hashes = {
            "bridge": _gzip_payload(bridge_plain, bridge_staged),
            "holdings": _gzip_payload(holdings_plain, holdings_staged),
        }
        values = sorted(report_dates)
        counts.update({"series": len(series), "report_dates": len(values)})
        manifest = {
            "generator_version": generator_version, "config_version": config_version,
            "fingerprints": {"generator_code_sha256": _sha256_file(Path(__file__)),
                             "config_sha256": hashlib.sha256(_canonical_json({"config_version": config_version}).encode()).hexdigest()},
            "lineage": {"source_run_id": source_run_id, "package_id": package_id, "package_sha256": package_sha256,
                        "parser_version": parser_version, "publication_id": publication_id},
            "source_hashes": source_hashes, "payload_hashes": payload_hashes,
            "report_dates": {"values": values, "min": values[0] if values else None, "max": values[-1] if values else None},
            "counts": counts,
        }
        (staging_dir / "manifest.json").write_text(_canonical_json(manifest) + "\n", encoding="utf-8")
        staging_dir.rename(output_dir)
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    bridge_path = output_dir / "sec_nport_instrument_class_bridge.tsv.gz"
    holdings_path = output_dir / "sec_nport_holdings_v2.tsv.gz"
    manifest_path = output_dir / "manifest.json"
    return GenerationResult(bridge_path=bridge_path, holdings_path=holdings_path, manifest_path=manifest_path)

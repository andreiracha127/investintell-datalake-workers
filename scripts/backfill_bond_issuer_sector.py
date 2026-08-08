"""One-time, resumable immutable load for ``bond_issuer_sector``.

The panel artifact is an operator-selected backfill input.  The service code
never reads it: this CLI hashes and streams only ``cusip_id`` and ``ff17num``,
then records enough provenance to refuse a different artifact or sector later.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Sequence

import pyarrow.parquet as pq
import psycopg
from psycopg.types.json import Jsonb

from src.bonds.errors import BondError
from src.bonds.panel_sources import (
    FF17_SOURCE_URL,
    FF17_SOURCE_VERSION,
    normalize_cusip9,
    resolve_modal_ff17,
    resolve_sic_to_ff17,
)
from src.db import resolve_dsn

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schemas" / "bond_panel_sources.sql"
REQUIRED_PANEL_COLUMNS = ("cusip_id", "ff17num")


class PanelArtifactError(ValueError):
    pass


class BackfillConflictError(RuntimeError):
    """Existing immutable evidence differs from the operator-pinned input."""

    def __init__(self, summary: dict[str, Any]) -> None:
        self.summary = summary
        super().__init__("bond_issuer_sector immutable evidence conflict")


@dataclass(frozen=True)
class SectorRow:
    cusip9: str
    ff17num: int
    source: str
    disagreement_count: int
    source_provenance: dict[str, Any]

    @classmethod
    def osbap(cls, cusip9: str, ff17num: int, disagreement_count: int, artifact_sha256: str) -> "SectorRow":
        return cls(cusip9, ff17num, "osbap", disagreement_count, {
            "artifact_sha256": artifact_sha256,
            "columns": list(REQUIRED_PANEL_COLUMNS),
            "modal_tie_break": "lowest_ff17num",
        })

    @classmethod
    def sic_map(cls, cusip9: str, ff17num: int, sic_code: int) -> "SectorRow":
        return cls(cusip9, ff17num, "sic_map", 0, {
            "sic_code": sic_code,
            "sic_source_surface": "sec_cusip_ticker_map.sic_code",
            "ff17_source_url": FF17_SOURCE_URL,
            "ff17_source_version": FF17_SOURCE_VERSION,
        })


@dataclass(frozen=True)
class PanelLoad:
    artifact_sha256: str
    rows: tuple[SectorRow, ...]
    reason_counts: dict[str, int]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_osbap_panel(path: Path, *, batch_size: int = 10_000) -> PanelLoad:
    """Hash then stream the two required columns; malformed data is counted, not repaired."""
    if not path.is_file():
        raise PanelArtifactError("artifact_unavailable")
    artifact_sha256 = _sha256(path)
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:
        raise PanelArtifactError("unreadable_parquet") from exc
    missing = [column for column in REQUIRED_PANEL_COLUMNS if column not in parquet.schema_arrow.names]
    if missing:
        raise PanelArtifactError(f"missing_required_columns:{','.join(missing)}")

    values_by_cusip: dict[str, list[object]] = defaultdict(list)
    reasons: Counter[str] = Counter()
    for batch in parquet.iter_batches(columns=list(REQUIRED_PANEL_COLUMNS), batch_size=batch_size):
        values = batch.to_pydict()
        for raw_cusip, raw_ff17 in zip(values["cusip_id"], values["ff17num"], strict=True):
            try:
                cusip9 = normalize_cusip9(raw_cusip)
            except BondError as exc:
                reasons[exc.code] += 1
                continue
            values_by_cusip[cusip9].append(raw_ff17)

    rows: list[SectorRow] = []
    for cusip9 in sorted(values_by_cusip):
        resolution = resolve_modal_ff17(values_by_cusip[cusip9])
        if resolution.ff17num is None:
            reasons[resolution.reason or "no_valid_ff17num"] += 1
            continue
        rows.append(SectorRow.osbap(cusip9, resolution.ff17num, resolution.disagreement_count, artifact_sha256))
    return PanelLoad(artifact_sha256, tuple(rows), dict(sorted(reasons.items())))


def sic_rows_from_exact_matches(matches: Iterable[tuple[object, object]]) -> tuple[tuple[SectorRow, ...], dict[str, int]]:
    """Resolve only the database's exact CUSIP9/SIC pairs; no issuer/name fallback exists."""
    rows: list[SectorRow] = []
    reasons: Counter[str] = Counter()
    for raw_cusip, raw_sic in matches:
        try:
            cusip9 = normalize_cusip9(raw_cusip)
        except BondError as exc:
            reasons[exc.code] += 1
            continue
        resolution = resolve_sic_to_ff17(raw_sic)
        if resolution.ff17num is None:
            reasons[resolution.reason or "sic_not_in_ff17_definition"] += 1
            continue
        rows.append(SectorRow.sic_map(cusip9, resolution.ff17num, int(str(raw_sic).strip())))
    return tuple(rows), dict(sorted(reasons.items()))


def classify_immutable_row(existing: SectorRow, incoming: SectorRow) -> str:
    """Only byte-for-byte compatible evidence is a resumable existing row."""
    return "existing" if existing == incoming else "conflicted"


def install_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def _existing_row(conn: psycopg.Connection, cusip9: str) -> SectorRow | None:
    row = conn.execute(
        "SELECT cusip9, ff17num, source, disagreement_count, source_provenance "
        "FROM bond_issuer_sector WHERE cusip9=%s", (cusip9,)
    ).fetchone()
    return None if row is None else SectorRow(row[0], row[1], row[2], row[3], dict(row[4]))


def persist_rows(conn: psycopg.Connection, rows: Sequence[SectorRow]) -> tuple[int, int, int]:
    inserted = existing = conflicted = 0
    for item in rows:
        result = conn.execute(
            "INSERT INTO bond_issuer_sector "
            "(cusip9, ff17num, source, disagreement_count, source_provenance) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (cusip9) DO NOTHING RETURNING cusip9",
            (item.cusip9, item.ff17num, item.source, item.disagreement_count, Jsonb(item.source_provenance)),
        ).fetchone()
        if result is not None:
            inserted += 1
            continue
        current = _existing_row(conn, item.cusip9)
        if current is not None and classify_immutable_row(current, item) == "existing":
            existing += 1
        else:
            conflicted += 1
    return inserted, existing, conflicted


def _sic_candidates(conn: psycopg.Connection) -> list[tuple[object, object]]:
    # Strictly exact CUSIP9 equality.  Do not substitute CUSIP6, ticker, or name.
    return conn.execute(
        "SELECT c.cusip9, m.sic_code "
        "FROM bond_curated_universe c "
        "JOIN sec_cusip_ticker_map m ON m.cusip = c.cusip9 "
        "LEFT JOIN bond_issuer_sector s ON s.cusip9 = c.cusip9 "
        "WHERE s.cusip9 IS NULL AND m.sic_code IS NOT NULL "
        "ORDER BY c.cusip9"
    ).fetchall()


def _coverage(conn: psycopg.Connection) -> tuple[dict[str, int], int]:
    counts = {"osbap": 0, "sic_map": 0}
    for source, count in conn.execute(
        "SELECT s.source, count(*) FROM bond_issuer_sector s "
        "JOIN bond_curated_universe c ON c.cusip9=s.cusip9 GROUP BY s.source"
    ).fetchall():
        counts[source] = count
    no_sector = conn.execute(
        "SELECT count(*) FROM bond_curated_universe c "
        "WHERE NOT EXISTS (SELECT 1 FROM bond_issuer_sector s WHERE s.cusip9=c.cusip9)"
    ).fetchone()[0]
    return counts, no_sector


def build_summary(*, artifact_sha256: str, attempted: int, inserted: int, existing: int,
                  conflicted: int, skipped: int, source_coverage: dict[str, int],
                  no_sector: int, reason_counts: dict[str, int]) -> dict[str, Any]:
    coverage = {"osbap": source_coverage.get("osbap", 0), "sic_map": source_coverage.get("sic_map", 0), "no_sector": no_sector}
    return {
        "artifact_sha256": artifact_sha256,
        "attempted": attempted,
        "inserted": inserted,
        "existing": existing,
        "conflicted": conflicted,
        "skipped": skipped,
        "source_coverage": coverage,
        "reason_counts": dict(sorted(reason_counts.items())),
    }


def require_no_conflicts(summary: dict[str, Any]) -> None:
    """Fail closed so a drifted artifact can never leave a partial load committed."""
    if summary["conflicted"]:
        raise BackfillConflictError(summary)


def run(dsn: str, panel: Path) -> dict[str, Any]:
    panel_load = load_osbap_panel(panel)
    reason_counts: Counter[str] = Counter(panel_load.reason_counts)
    with psycopg.connect(dsn) as conn:
        install_schema(conn)
        panel_inserted, panel_existing, panel_conflicted = persist_rows(conn, panel_load.rows)
        sic_rows, sic_reasons = sic_rows_from_exact_matches(_sic_candidates(conn))
        reason_counts.update(sic_reasons)
        sic_inserted, sic_existing, sic_conflicted = persist_rows(conn, sic_rows)
        coverage, no_sector = _coverage(conn)
        summary = build_summary(
            artifact_sha256=panel_load.artifact_sha256,
            attempted=len(panel_load.rows) + len(sic_rows),
            inserted=panel_inserted + sic_inserted,
            existing=panel_existing + sic_existing,
            conflicted=panel_conflicted + sic_conflicted,
            skipped=sum(reason_counts.values()),
            source_coverage=coverage,
            no_sector=no_sector,
            reason_counts=dict(reason_counts),
        )
        require_no_conflicts(summary)
        return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn", default=None, help="target datalake DSN; defaults to DATABASE_URL"
    )
    parser.add_argument("--panel", required=True, type=Path, help="explicit OSBAP parquet artifact")
    args = parser.parse_args(argv)
    try:
        summary = run(resolve_dsn(args.dsn), args.panel)
    except BackfillConflictError as exc:
        print(json.dumps(exc.summary, sort_keys=True))
        return 2
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

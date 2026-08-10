"""Offline-only, resumable T3 base publication emitter for the frozen OSBAP panel.

This program deliberately has no DSN option and never connects to a database.
It verifies the five local artifacts before emitting either read-only planning
evidence or a single stdin-safe ``psql`` transaction.  Production workers must
not import this module: it is a one-time historical transport only.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import sys
import tempfile
import uuid
from typing import Any, Literal

import duckdb
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""} and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_psql_transport import render_immutable_batch, render_schema  # noqa: E402
DEFAULT_ARTIFACT_DIRECTORY = Path(
    r"C:\Users\andre\Downloads\stage1_osbap_0k_volume_2025\bond_panel_monthly"
)
DEFAULT_CUTOFF = "2026-06-01"
CONFIG_HASH = "0c0d78a866bc1090"
PRODUCT = "bond_panel_v1"
CODE_REVISION = "t3_historical_base_001"
REPAIR_CODE_REVISION = "t3_historical_base_return_coverage_repair_v1"
LEGACY_REPAIR_FROM_PUBLICATION_ID = "92740098-1571-559d-9fb3-119de8321754"
LEGACY_REPAIR_FROM_INPUT_FINGERPRINT = "5a7af9e1adaed315e9940293cf3e9e789ca6350993688d58ab3e759cee37a3cb"
LEGACY_REPAIR_ARTIFACT_FINGERPRINT = "e963304af08c1f513d048e1e7eee9fbe334fc3fe01b1c80f3cd5b7f8acb19581"
REPAIR_CONTRACT = "legacy_parentless_return_coverage_repair_v1"
REPAIR_HISTORY_CUTOFF = "2025-03-01"
REPAIR_TAIL_FIRST_MONTH = "2025-04-01"
REPAIR_TAIL_MONTH_COUNTS = (13641, 14542, 14288, 14178, 13956, 13812, 13660, 13331, 13195, 13229, 12899, 12734, 12610, 12476, 12404)
REPAIR_EXPECTED_TAIL_ROWS = 200955
REPAIR_EXPECTED_TAIL_CUSIPS = 17494
REPAIR_EXPECTED_TAIL_SUSPECT = 47
REPAIR_EXPECTED_TOTAL_RETURNS = 2801208
SURFACES = ("snapshot", "rv_signal", "returns", "rating_pit")
Surface = Literal["snapshot", "rv_signal", "returns", "rating_pit"]
SCHEMA_PATH = ROOT / "schemas" / "bond_panel_v1.sql"
HISTORICAL_RATING_CUTOFF = "2025-03-01"
GENERIC_RATING_BUCKETS = ("AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D", "NR")

EXPECTED_SHA256 = {
    "bond_panel_live.parquet": "3e4d451faa05bcedefa086903325e93842a59e31368c7e12aaa5a4972214e210",
    "universe_snapshots_live.parquet": "ab48d99f466ae3a943ce0a2819175ab6efdd95212b4efc9079151750057b077a",
    "rv_signal_live.parquet": "b6afc8bc44dd11563b794b2c11a9d13eb9a882af4d364a728e87a34258c90e6e",
    "bond_monthly_returns.parquet": "d0c8827437d6a49c4481ead71eac69097d00db11a19d91e2b58dc3d714ae8179",
    "bond_ratings_pit.parquet": "97c645ce7d98ad945288369e20ed40abe2d7d1590b4953f7a983bc6e719efcb4",
}

REQUIRED_COLUMNS = {
    "bond_panel_live.parquet": {"cusip_id", "month", "pr", "ytm", "mod_dur", "bond_maturity", "credit_spread", "trade_count", "dollar_volume", "traded_days", "prc_bid", "prc_ask", "rel_bid_ask_bps", "quoted_days", "amt_outstanding_k", "ff17num", "db_type", "price_source"},
    "universe_snapshots_live.parquet": {"cusip_id", "month", "spread_final", "rating_bucket"},
    "rv_signal_live.parquet": {"cusip_id", "month", "spread_bps", "fitted_bps", "residual_bps", "rv_signal"},
    "bond_monthly_returns.parquet": {"cusip_id", "month", "total_return", "price_return", "carry_return", "suspect"},
    "bond_ratings_pit.parquet": {"cusip_id", "month", "rating_bucket"},
}


class ArtifactPinError(ValueError):
    pass


class PlanError(ValueError):
    pass


class CursorError(ValueError):
    pass


@dataclass(frozen=True)
class ArtifactSet:
    directory: Path
    paths: dict[str, Path]
    sha256: dict[str, str]

    @classmethod
    def open(cls, directory: Path, *, expected_hashes: dict[str, str] | None = None) -> "ArtifactSet":
        """Pin every required input before allowing any output or query."""
        expected = EXPECTED_SHA256 if expected_hashes is None else expected_hashes
        paths: dict[str, Path] = {}
        actual: dict[str, str] = {}
        for filename in EXPECTED_SHA256:
            path = directory / filename
            if not path.is_file():
                raise ArtifactPinError(f"artifact_unavailable:{filename}")
            digest = _sha256(path)
            if digest != expected.get(filename):
                raise ArtifactPinError(f"artifact_sha256_mismatch:{filename}")
            try:
                columns = set(pq.ParquetFile(path).schema_arrow.names)
            except Exception as exc:  # pragma: no cover - pyarrow gives format-specific detail
                raise ArtifactPinError(f"unreadable_parquet:{filename}") from exc
            missing = sorted(REQUIRED_COLUMNS[filename] - columns)
            if missing:
                raise ArtifactPinError(f"missing_required_columns:{filename}:{','.join(missing)}")
            paths[filename] = path
            actual[filename] = digest
        return cls(directory=directory, paths=paths, sha256=actual)

    def path(self, filename: str) -> str:
        return self.paths[filename].as_posix()


@dataclass(frozen=True)
class BackfillPlan:
    publication_id: str
    input_fingerprint: str
    cutoff: str
    first_month: str
    last_closed_month: str
    returns_last_month: str
    counts: dict[str, int]
    source_sha256: dict[str, str]
    panel_without_rating_pit: int
    config_hash: str = CONFIG_HASH
    base_repair: dict[str, Any] | None = None
    returns_first_month: str | None = None

    @property
    def is_repair(self) -> bool:
        return self.base_repair is not None

    @property
    def code_revision(self) -> str:
        return REPAIR_CODE_REVISION if self.is_repair else CODE_REVISION

    def evidence(self) -> dict[str, Any]:
        evidence = {
            "publication_id": self.publication_id,
            "input_fingerprint": self.input_fingerprint,
            "config_hash": self.config_hash,
            "first_month": self.first_month,
            "last_closed_month": self.last_closed_month,
            "open_month": None,
            "returns_last_month": self.returns_last_month,
            "returns_first_month": self.returns_first_month,
            "counts": self.counts,
            "source_sha256": self.source_sha256,
            "rating_pit_coverage": {
                "through": HISTORICAL_RATING_CUTOFF,
                "panel_without_rating_pit": self.panel_without_rating_pit,
                "missing_state": "historical_missing",
                "missing_reason": "historical_rating_absent",
            },
            "artifact_scope": "frozen_osbap_trace_panel_local_backfill_only",
        }
        if self.base_repair is not None:
            evidence["base_repair"] = self.base_repair
        return evidence


@dataclass(frozen=True)
class SurfaceRows:
    surface: Surface
    rows: tuple[dict[str, Any], ...]
    start_after: int
    committed_through: int
    total: int


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _connect() -> tuple[duckdb.DuckDBPyConnection, tempfile.TemporaryDirectory[str]]:
    state = tempfile.TemporaryDirectory(prefix="bond-panel-history-")
    conn = duckdb.connect(":memory:")
    conn.execute("SET temp_directory = ?", [state.name])
    return conn, state


def _one(conn: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> Any:
    return conn.execute(sql, params).fetchone()[0]


def _scalar_month(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _require_zero(conn: duckdb.DuckDBPyConnection, *, reason: str, sql: str, params: list[Any]) -> None:
    if int(_one(conn, sql, params)):
        raise PlanError(reason)


def _gate_unique_and_valid_keys(
    conn: duckdb.DuckDBPyConnection, *, label: str, path: str, cutoff: str,
) -> None:
    _require_zero(
        conn,
        reason=f"duplicate_month_cusip:{label}",
        sql="SELECT count(*) FROM (SELECT CAST(month AS DATE), cusip_id FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) GROUP BY 1,2 HAVING count(*) > 1)",
        params=[path, cutoff],
    )
    _require_zero(
        conn,
        reason=f"invalid_month_cusip:{label}",
        sql="SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) AND (cusip_id IS NULL OR NOT regexp_full_match(CAST(cusip_id AS VARCHAR), '[0-9A-Z]{9}') OR month IS NULL OR EXTRACT(DAY FROM CAST(month AS DATE)) <> 1)",
        params=[path, cutoff],
    )


def build_plan(artifacts: ArtifactSet, *, cutoff: str = DEFAULT_CUTOFF) -> BackfillPlan:
    """Scan counts only; reject non-closed-month data and bad frozen joins."""
    try:
        cutoff_date = date.fromisoformat(cutoff)
    except ValueError as exc:
        raise PlanError("invalid_cutoff") from exc
    if cutoff_date != date(2026, 6, 1):
        raise PlanError("base_cutoff_must_be_2026-06-01")
    conn, state = _connect()
    try:
        panel = artifacts.path("bond_panel_live.parquet")
        universe = artifacts.path("universe_snapshots_live.parquet")
        rv = artifacts.path("rv_signal_live.parquet")
        returns = artifacts.path("bond_monthly_returns.parquet")
        ratings = artifacts.path("bond_ratings_pit.parquet")
        _gate_unique_and_valid_keys(conn, label="panel", path=panel, cutoff=cutoff)
        _gate_unique_and_valid_keys(conn, label="universe", path=universe, cutoff=cutoff)
        _gate_unique_and_valid_keys(conn, label="rv_signal", path=rv, cutoff=cutoff)
        _gate_unique_and_valid_keys(conn, label="returns", path=returns, cutoff=cutoff)
        _gate_unique_and_valid_keys(conn, label="rating_pit", path=ratings, cutoff=HISTORICAL_RATING_CUTOFF)
        for label, path, source_cutoff in (
            ("panel", panel, cutoff),
            ("universe", universe, cutoff),
            ("rv_signal", rv, cutoff),
            ("returns", returns, cutoff),
            ("rating_pit", ratings, HISTORICAL_RATING_CUTOFF),
        ):
            if int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE)", [path, source_cutoff])) == 0:
                raise PlanError(f"source_empty:{label}")
        _require_zero(
            conn,
            reason="included_universe_missing_panel",
            sql="SELECT count(*) FROM read_parquet(?) u LEFT JOIN read_parquet(?) p ON u.cusip_id=p.cusip_id AND CAST(u.month AS DATE)=CAST(p.month AS DATE) WHERE CAST(u.month AS DATE) <= CAST(? AS DATE) AND p.cusip_id IS NULL",
            params=[universe, panel, cutoff],
        )
        _require_zero(
            conn,
            reason="universe_rating_invalid",
            sql="SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) AND (rating_bucket IS NULL OR rating_bucket NOT IN (" + ",".join(repr(bucket) for bucket in GENERIC_RATING_BUCKETS) + "))",
            params=[universe, cutoff],
        )
        _require_zero(
            conn,
            reason="rating_pit_value_invalid",
            sql="SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) AND rating_bucket IS NOT NULL AND rating_bucket NOT IN (" + ",".join(repr(bucket) for bucket in GENERIC_RATING_BUCKETS) + ")",
            params=[ratings, HISTORICAL_RATING_CUTOFF],
        )
        _require_zero(
            conn,
            reason="historical_return_value_missing",
            sql="SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) AND (total_return IS NULL OR NOT isfinite(total_return))",
            params=[returns, cutoff],
        )
        _require_zero(
            conn,
            reason="returns_not_subset_of_panel",
            sql="SELECT count(*) FROM (SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) EXCEPT SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE))",
            params=[returns, cutoff, panel, cutoff],
        )
        _require_zero(
            conn,
            reason="rv_key_set_mismatch",
            sql="SELECT count(*) FROM ((SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) EXCEPT SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE)) UNION ALL (SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) EXCEPT SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE)))",
            params=[universe, cutoff, rv, cutoff, rv, cutoff, universe, cutoff],
        )
        _require_zero(
            conn,
            reason="rating_pit_not_subset_of_panel",
            sql="SELECT count(*) FROM (SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= DATE '2025-03-01' EXCEPT SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= DATE '2025-03-01')",
            params=[ratings, panel],
        )
        panel_without_rating_pit = int(_one(conn, "SELECT count(*) FROM (SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= DATE '2025-03-01' EXCEPT SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= DATE '2025-03-01')", [panel, ratings]))
        counts = {
            "snapshot": int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [panel, cutoff])),
            "rv_signal": int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [rv, cutoff])),
            "returns": int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [returns, cutoff])),
            "rating_pit": int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [panel, cutoff])),
        }
        if any(value <= 0 for value in counts.values()):
            raise PlanError("empty_historical_surface")
        first_month = _one(conn, "SELECT min(CAST(month AS DATE)) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [panel, cutoff])
        returns_last = _one(conn, "SELECT max(CAST(month AS DATE)) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [returns, cutoff])
        if returns_last is None:
            raise PlanError("returns_history_absent")
        if _scalar_month(returns_last) != cutoff:
            raise PlanError("returns_history_must_reach_cutoff")
        _require_zero(
            conn,
            reason="returns_history_must_be_contiguous_through_cutoff",
            sql="SELECT count(*) FROM generate_series(CAST(? AS DATE), CAST(? AS DATE), INTERVAL '1 month') AS expected(month) LEFT JOIN (SELECT DISTINCT CAST(month AS DATE) AS month FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE)) actual ON actual.month=CAST(expected.month AS DATE) WHERE actual.month IS NULL",
            params=[first_month, cutoff, returns, cutoff],
        )
        future_returns = _one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) > ?", [returns, cutoff])
        if future_returns:
            raise PlanError("returns_artifact_extends_past_base_cutoff")
        # The target has one rating row for every snapshot candidate.  The exact
        # PIT value is copied wherever the frozen grid carries it; an absent
        # value is represented explicitly as NR/historical_missing during row
        # generation rather than silently dropping the candidate.
        source_sha256 = dict(sorted(artifacts.sha256.items()))
        fingerprint = hashlib.sha256(json.dumps({"config_hash": CONFIG_HASH, "cutoff": cutoff, "counts": counts, "source_sha256": source_sha256}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        publication_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCT}:t3-base:{fingerprint}"))
        if counts["rating_pit"] != counts["snapshot"]:
            raise PlanError("rating_rows_do_not_cover_snapshot")
        return BackfillPlan(publication_id, fingerprint, cutoff, _scalar_month(first_month), cutoff, _scalar_month(returns_last), counts, source_sha256, panel_without_rating_pit)
    finally:
        conn.close()
        state.cleanup()


def _canonical_digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _month_starts(first: str, last: str) -> list[str]:
    current = date.fromisoformat(first)
    end = date.fromisoformat(last)
    values: list[str] = []
    while current <= end:
        values.append(current.isoformat())
        current = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
    return values


def _repair_return_tail_rows(
    artifacts: ArtifactSet, *, start_after: int = 0, limit: int | None = None
) -> list[dict[str, Any]]:
    """Build only the requested observed-return tail from frozen artifacts."""
    if start_after < 0 or (limit is not None and limit <= 0):
        raise CursorError("invalid_cursor_or_limit")
    conn, state = _connect()
    try:
        bounded = "" if limit is None else " LIMIT ? OFFSET ?"
        params: list[Any] = [
            artifacts.path("bond_panel_live.parquet"),
            artifacts.path("bond_monthly_returns.parquet"),
        ]
        if limit is not None:
            params.extend([limit, start_after])
        raw = conn.execute(
            """WITH panel AS MATERIALIZED (
                SELECT CAST(month AS DATE) AS month, cusip_id,
                       CAST(pr AS DOUBLE) AS price, CAST(ytm AS DOUBLE) AS ytm,
                       CAST(bond_maturity AS DOUBLE) AS maturity
                FROM read_parquet(?)
                WHERE CAST(month AS DATE) <= DATE '2026-06-01'
            ), lagged AS MATERIALIZED (
                SELECT *, lag(month) OVER w AS previous_month,
                          lag(price) OVER w AS previous_price
                FROM panel
                WINDOW w AS (PARTITION BY cusip_id ORDER BY month)
            ), historical_coupon AS (
                SELECT r.cusip_id,
                       median(12 * CAST(r.carry_return AS DOUBLE) * p.previous_price) AS coupon
                FROM read_parquet(?) r
                JOIN lagged p ON p.cusip_id=r.cusip_id AND p.month=CAST(r.month AS DATE)
                WHERE CAST(r.month AS DATE) <= DATE '2025-03-01'
                  AND isfinite(CAST(r.carry_return AS DOUBLE))
                  AND isfinite(p.previous_price) AND p.previous_price <> 0
                  AND date_diff('day', p.previous_month, p.month) BETWEEN 28 AND 31
                GROUP BY r.cusip_id
            ), implied_parts AS (
                SELECT cusip_id, price, ytm, ytm / 2 AS y,
                       greatest(CAST(round(2 * maturity) AS INTEGER), 1) AS periods
                FROM panel
                WHERE month <= DATE '2025-03-01'
                  AND isfinite(price) AND isfinite(ytm) AND isfinite(maturity)
            ), implied_math AS (
                SELECT *, pow(1 + y, -periods) AS discount FROM implied_parts
            ), implied_coupon AS (
                SELECT cusip_id, median(greatest(0.0, least(20.0,
                    CASE WHEN abs(y) > 0 AND (1 - discount) / y > 1e-9
                         THEN (price / 100 - discount) / ((1 - discount) / y) * 200
                         ELSE ytm * 100 END))) AS coupon
                FROM implied_math GROUP BY cusip_id
            ), coupons AS (
                SELECT i.cusip_id, coalesce(h.coupon, i.coupon) AS coupon
                FROM implied_coupon i LEFT JOIN historical_coupon h USING (cusip_id)
                UNION ALL
                SELECT h.cusip_id, h.coupon
                FROM historical_coupon h LEFT JOIN implied_coupon i USING (cusip_id)
                WHERE i.cusip_id IS NULL
            ), tail AS (
                SELECT l.month, l.cusip_id,
                       (l.price - l.previous_price) / l.previous_price AS price_return,
                       (c.coupon / 12) / l.previous_price AS carry_return
                FROM lagged l JOIN coupons c USING (cusip_id)
                WHERE l.month BETWEEN DATE '2025-04-01' AND DATE '2026-06-01'
                  AND date_diff('day', l.previous_month, l.month) BETWEEN 28 AND 31
                  AND isfinite(l.price) AND isfinite(l.previous_price)
                  AND l.previous_price <> 0 AND isfinite(c.coupon)
            )
            SELECT month, cusip_id, price_return, carry_return,
                   price_return + carry_return AS total_return
            FROM tail ORDER BY month, cusip_id""" + bounded,
            params,
        ).fetchall()
    finally:
        conn.close()
        state.cleanup()
    lineage = _lineage(artifacts, "bond_panel_live.parquet", "bond_monthly_returns.parquet")
    original_artifact_fingerprint = _canonical_digest({"source_sha256": dict(sorted(artifacts.sha256.items()))})
    rows: list[dict[str, Any]] = []
    for month, cusip, price_return, carry_return, total_return in raw:
        total = float(total_return)
        rows.append({
            "month": _scalar_month(month), "cusip_id": cusip, "total_return": total,
            "price_return": float(price_return), "carry_return": float(carry_return),
            "suspect": abs(total) > .5, "exit_basis": "observed", "exit_reason": None,
            "payload": {"base_repair": {"contract": REPAIR_CONTRACT, "from_publication_id": LEGACY_REPAIR_FROM_PUBLICATION_ID, "from_artifact_fingerprint": original_artifact_fingerprint}, "source_lineage": lineage},
        })
    return rows


def build_repair_plan(artifacts: ArtifactSet, *, from_publication_id: str) -> BackfillPlan:
    """Plan the one authorized immutable root replacement; normal mode remains fail-closed."""
    if from_publication_id != LEGACY_REPAIR_FROM_PUBLICATION_ID:
        raise PlanError("repair_from_publication_id_not_authorized")
    conn, state = _connect()
    try:
        panel = artifacts.path("bond_panel_live.parquet")
        universe = artifacts.path("universe_snapshots_live.parquet")
        rv = artifacts.path("rv_signal_live.parquet")
        returns = artifacts.path("bond_monthly_returns.parquet")
        ratings = artifacts.path("bond_ratings_pit.parquet")
        _gate_unique_and_valid_keys(conn, label="panel", path=panel, cutoff=DEFAULT_CUTOFF)
        _gate_unique_and_valid_keys(conn, label="universe", path=universe, cutoff=DEFAULT_CUTOFF)
        _gate_unique_and_valid_keys(conn, label="rv_signal", path=rv, cutoff=DEFAULT_CUTOFF)
        _gate_unique_and_valid_keys(conn, label="returns", path=returns, cutoff=REPAIR_HISTORY_CUTOFF)
        _gate_unique_and_valid_keys(conn, label="rating_pit", path=ratings, cutoff=HISTORICAL_RATING_CUTOFF)
        for label, path, source_cutoff in (("panel", panel, DEFAULT_CUTOFF), ("universe", universe, DEFAULT_CUTOFF), ("rv_signal", rv, DEFAULT_CUTOFF), ("returns", returns, REPAIR_HISTORY_CUTOFF), ("rating_pit", ratings, HISTORICAL_RATING_CUTOFF)):
            if not int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE)", [path, source_cutoff])):
                raise PlanError(f"source_empty:{label}")
        _require_zero(conn, reason="included_universe_missing_panel", sql="SELECT count(*) FROM read_parquet(?) u LEFT JOIN read_parquet(?) p ON u.cusip_id=p.cusip_id AND CAST(u.month AS DATE)=CAST(p.month AS DATE) WHERE CAST(u.month AS DATE) <= CAST(? AS DATE) AND p.cusip_id IS NULL", params=[universe, panel, DEFAULT_CUTOFF])
        _require_zero(conn, reason="repair_historical_return_value_missing", sql="SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) AND (total_return IS NULL OR NOT isfinite(total_return) OR carry_return IS NULL OR NOT isfinite(carry_return))", params=[returns, REPAIR_HISTORY_CUTOFF])
        _require_zero(conn, reason="repair_returns_not_subset_of_panel", sql="SELECT count(*) FROM (SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) EXCEPT SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE))", params=[returns, REPAIR_HISTORY_CUTOFF, panel, DEFAULT_CUTOFF])
        if _scalar_month(_one(conn, "SELECT max(CAST(month AS DATE)) FROM read_parquet(?)", [returns])) != REPAIR_HISTORY_CUTOFF:
            raise PlanError("repair_returns_must_end_at_2025-03-01")
        first_month = _one(conn, "SELECT min(CAST(month AS DATE)) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [panel, DEFAULT_CUTOFF])
        returns_first_month = _one(conn, "SELECT min(CAST(month AS DATE)) FROM read_parquet(?)", [returns])
        expected_returns_first = _one(conn, "SELECT (CAST(? AS DATE) + INTERVAL '1 month')::date", [first_month])
        if returns_first_month != expected_returns_first:
            raise PlanError("repair_returns_must_start_one_month_after_snapshot")
        _require_zero(conn, reason="repair_historical_returns_must_be_contiguous", sql="SELECT count(*) FROM generate_series(CAST(? AS DATE), CAST(? AS DATE), INTERVAL '1 month') expected(month) LEFT JOIN (SELECT DISTINCT CAST(month AS DATE) AS return_month FROM read_parquet(?)) actual ON actual.return_month=expected.month::date WHERE actual.return_month IS NULL", params=[returns_first_month, REPAIR_HISTORY_CUTOFF, returns])
        historical_return_count = int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE)", [returns, REPAIR_HISTORY_CUTOFF]))
        counts = {"snapshot": int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [panel, DEFAULT_CUTOFF])), "rv_signal": int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [rv, DEFAULT_CUTOFF])), "rating_pit": int(_one(conn, "SELECT count(*) FROM read_parquet(?) WHERE CAST(month AS DATE) <= ?", [panel, DEFAULT_CUTOFF]))}
        panel_without_rating_pit = int(_one(conn, "SELECT count(*) FROM (SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= DATE '2025-03-01' EXCEPT SELECT cusip_id, CAST(month AS DATE) FROM read_parquet(?) WHERE CAST(month AS DATE) <= DATE '2025-03-01')", [panel, ratings]))
    finally:
        conn.close()
        state.cleanup()
    tail = _repair_return_tail_rows(artifacts)
    if not tail:
        raise PlanError("repair_tail_empty")
    month_counts = [sum(row["month"] == month for row in tail) for month in _month_starts(REPAIR_TAIL_FIRST_MONTH, DEFAULT_CUTOFF)]
    tail_digest = _canonical_digest(tail)
    if artifacts.sha256 == EXPECTED_SHA256 and (len(tail) != REPAIR_EXPECTED_TAIL_ROWS or len({row["cusip_id"] for row in tail}) != REPAIR_EXPECTED_TAIL_CUSIPS or sum(bool(row["suspect"]) for row in tail) != REPAIR_EXPECTED_TAIL_SUSPECT or tuple(month_counts) != REPAIR_TAIL_MONTH_COUNTS):
        raise PlanError("repair_tail_does_not_match_authorized_frozen_contract")
    counts["returns"] = historical_return_count + len(tail)
    if artifacts.sha256 == EXPECTED_SHA256 and counts["returns"] != REPAIR_EXPECTED_TOTAL_RETURNS:
        raise PlanError("repair_total_returns_does_not_match_authorized_frozen_contract")
    source_sha256 = dict(sorted(artifacts.sha256.items()))
    original_artifact_fingerprint = _canonical_digest({"source_sha256": source_sha256})
    if artifacts.sha256 == EXPECTED_SHA256 and original_artifact_fingerprint != LEGACY_REPAIR_ARTIFACT_FINGERPRINT:  # pragma: no cover - protects future digest refactors
        raise PlanError("repair_artifact_fingerprint_drift")
    base_repair = {"contract": REPAIR_CONTRACT, "from_publication_id": from_publication_id, "from_config_hash": CONFIG_HASH, "from_input_fingerprint": LEGACY_REPAIR_FROM_INPUT_FINGERPRINT, "from_artifact_fingerprint": original_artifact_fingerprint, "first_month": _scalar_month(first_month), "last_closed_month": DEFAULT_CUTOFF, "reconstruction": "median_coupon_from_historical_carry_then_price_ytm_fallback", "tail_rows": len(tail), "tail_months": len(month_counts), "tail_month_counts": month_counts, "tail_cusips": len({row["cusip_id"] for row in tail}), "tail_suspect": sum(bool(row["suspect"]) for row in tail), "tail_digest": tail_digest, "authorized_code_revision": REPAIR_CODE_REVISION}
    fingerprint = _canonical_digest({"config_hash": CONFIG_HASH, "source_sha256": source_sha256, "base_repair": base_repair})
    publication_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCT}:legacy-base-repair:{fingerprint}"))
    return BackfillPlan(publication_id, fingerprint, DEFAULT_CUTOFF, _scalar_month(first_month), DEFAULT_CUTOFF, DEFAULT_CUTOFF, counts, source_sha256, panel_without_rating_pit, base_repair=base_repair, returns_first_month=_scalar_month(returns_first_month))


def _lineage(artifacts: ArtifactSet, *names: str) -> dict[str, Any]:
    return {
        "scope": "frozen_osbap_trace_panel_scope",
        "local_parquet_use": "one_time_historical_backfill_only",
        "source_sha256": {name: artifacts.sha256[name] for name in names},
    }


def _clean(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _record(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    integer_columns = {"ff17num", "db_type", "traded_days", "trade_count", "quoted_days"}
    result: dict[str, Any] = {}
    for name, value in zip(columns, row, strict=True):
        cleaned = _clean(value)
        result[name] = int(cleaned) if name in integer_columns and cleaned is not None else cleaned
    return result


def _snapshot_sql(artifacts: ArtifactSet) -> tuple[str, list[str]]:
    columns = ["month", "cusip_id", "ff17num", "eligibility_state", "eligibility_reason", "amount_outstanding_k", "maturity_years", "price", "price_source", "db_type", "ytm", "mod_dur", "spread_final", "spread_final_bps", "rating_bucket", "rating_state", "traded_days", "trade_count", "dollar_volume", "rel_bid_ask_bps", "quoted_days"]
    sql = """SELECT CAST(p.month AS DATE), p.cusip_id, p.ff17num,
      CASE WHEN u.cusip_id IS NOT NULL THEN 'included' ELSE 'excluded' END,
      CASE WHEN u.cusip_id IS NOT NULL THEN 'eligible'
           WHEN p.ytm IS NULL OR NOT isfinite(p.ytm) OR p.mod_dur IS NULL OR NOT isfinite(p.mod_dur) OR p.pr IS NULL OR NOT isfinite(p.pr) OR p.pr < 1 OR p.pr > 300 OR p.ytm < -.02 OR p.ytm > .60 OR p.mod_dur < .05 OR p.mod_dur > 40 THEN 'missing_fields'
           WHEN p.amt_outstanding_k IS NULL OR p.amt_outstanding_k < 250000 THEN 'too_small'
           WHEN p.bond_maturity IS NULL OR p.bond_maturity < 1 THEN 'matured_or_short'
           WHEN p.traded_days IS NULL OR p.traded_days < 5 THEN 'illiquid'
           ELSE 'missing_fields' END,
      p.amt_outstanding_k, p.bond_maturity, p.pr, p.price_source, p.db_type, p.ytm, p.mod_dur,
      u.spread_final, CASE WHEN u.spread_final IS NULL THEN NULL ELSE u.spread_final * 10000 END,
      COALESCE(r.rating_bucket, 'NR'),
      CASE WHEN r.rating_bucket IS NULL THEN 'historical_missing'
           WHEN CAST(r.month AS DATE) = CAST(p.month AS DATE) THEN 'historical_pit'
           ELSE 'static_carry_forward' END,
      p.traded_days, p.trade_count, p.dollar_volume, p.rel_bid_ask_bps, p.quoted_days
      FROM read_parquet(?) p LEFT JOIN read_parquet(?) u
        ON p.cusip_id=u.cusip_id AND CAST(p.month AS DATE)=CAST(u.month AS DATE)
      ASOF LEFT JOIN (SELECT * FROM read_parquet(?) WHERE CAST(month AS DATE) <= DATE '2025-03-01') r
        ON p.cusip_id=r.cusip_id AND CAST(p.month AS DATE) >= CAST(r.month AS DATE)
      WHERE CAST(p.month AS DATE) <= ? ORDER BY CAST(p.month AS DATE), p.cusip_id LIMIT ? OFFSET ?"""
    return sql, columns


def _surface_query(artifacts: ArtifactSet, surface: Surface, cutoff: str, limit: int, offset: int) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    conn, state = _connect()
    try:
        if surface == "snapshot":
            sql, columns = _snapshot_sql(artifacts)
            raw = conn.execute(sql, [artifacts.path("bond_panel_live.parquet"), artifacts.path("universe_snapshots_live.parquet"), artifacts.path("bond_ratings_pit.parquet"), cutoff, limit, offset]).fetchall()
            lineage = _lineage(artifacts, "bond_panel_live.parquet", "universe_snapshots_live.parquet", "bond_ratings_pit.parquet")
            rows = []
            for item in (_record(columns, row) for row in raw):
                item.update({"issuer_id": None, "issuer_identity_state": "historical_identity_absent", "currency": "USD", "asset_class": "corporate", "maturity_date": None, "coupon_pct": None, "ytm_basis": "frozen_osbap_trace_monthly", "mod_dur_source": "frozen_osbap_trace_monthly", "spread_definition": "ytm_minus_interpolated_dgs", "spread_source": "frozen_computed_spread_final" if item["spread_final"] is not None else None, "terms_source": "historical_osbap_trace_panel", "source_lineage": lineage})
                item["payload"] = dict(item)
                rows.append(item)
            return rows, ("bond_panel_live.parquet", "universe_snapshots_live.parquet", "bond_ratings_pit.parquet")
        if surface == "rv_signal":
            columns = ["month", "cusip_id", "spread_final_bps", "residual_bps", "rv_signal", "price", "amount_outstanding_k", "maturity_years", "traded_days", "trade_count", "dollar_volume", "rel_bid_ask_bps", "quoted_days", "ytm", "mod_dur", "price_source", "ff17num"]
            raw = conn.execute("""SELECT CAST(r.month AS DATE), r.cusip_id, r.spread_bps, r.residual_bps, r.rv_signal,
                u.pr, u.amt_outstanding_k, u.bond_maturity, u.traded_days, u.trade_count, u.dollar_volume, u.rel_bid_ask_bps, u.quoted_days, u.ytm, u.mod_dur, u.price_source, u.ff17num
                FROM read_parquet(?) r JOIN read_parquet(?) u ON r.cusip_id=u.cusip_id AND CAST(r.month AS DATE)=CAST(u.month AS DATE)
                WHERE CAST(r.month AS DATE) <= ? ORDER BY CAST(r.month AS DATE), r.cusip_id LIMIT ? OFFSET ?""", [artifacts.path("rv_signal_live.parquet"), artifacts.path("universe_snapshots_live.parquet"), cutoff, limit, offset]).fetchall()
            lineage = _lineage(artifacts, "rv_signal_live.parquet", "universe_snapshots_live.parquet")
            rows = []
            for item in (_record(columns, row) for row in raw):
                item.update({"issuer_id": None, "eligibility_state": "included", "eligibility_reason": "eligible", "ytm_basis": "frozen_osbap_trace_monthly", "mod_dur_source": "frozen_osbap_trace_monthly", "spread_definition": "ytm_minus_interpolated_dgs", "flags": {"fitted_bps_not_published": True}, "source_lineage": lineage})
                item["payload"] = dict(item)
                rows.append(item)
            return rows, ("rv_signal_live.parquet", "universe_snapshots_live.parquet")
        if surface == "returns":
            columns = ["month", "cusip_id", "total_return", "price_return", "carry_return", "suspect"]
            raw = conn.execute("SELECT CAST(month AS DATE), cusip_id, total_return, price_return, carry_return, suspect FROM read_parquet(?) WHERE CAST(month AS DATE) <= ? ORDER BY CAST(month AS DATE), cusip_id LIMIT ? OFFSET ?", [artifacts.path("bond_monthly_returns.parquet"), cutoff, limit, offset]).fetchall()
            lineage = _lineage(artifacts, "bond_monthly_returns.parquet")
            rows = []
            for item in (_record(columns, row) for row in raw):
                item.update({"exit_basis": "observed", "exit_reason": None, "payload": {"historical_return_coverage_through": cutoff, "source_lineage": lineage}})
                rows.append(item)
            return rows, ("bond_monthly_returns.parquet",)
        if surface == "rating_pit":
            columns = ["month", "cusip_id", "rating_bucket", "rating_as_of_month", "rating_state", "rating_reason"]
            raw = conn.execute("""SELECT CAST(p.month AS DATE), p.cusip_id,
                COALESCE(r.rating_bucket, 'NR'),
                CASE WHEN r.rating_bucket IS NOT NULL THEN CAST(r.month AS DATE) ELSE NULL END,
                CASE WHEN r.rating_bucket IS NULL THEN 'historical_missing'
                     WHEN CAST(r.month AS DATE) = CAST(p.month AS DATE) THEN 'historical_pit'
                     ELSE 'static_carry_forward' END,
                CASE WHEN r.rating_bucket IS NULL THEN 'historical_rating_absent'
                     WHEN CAST(r.month AS DATE) = CAST(p.month AS DATE) THEN 'historical_pit'
                     ELSE 'historical_pit_carry_forward' END,
                CASE WHEN r.rating_bucket IS NULL THEN NULL ELSE date_diff('month', CAST(r.month AS DATE), CAST(p.month AS DATE)) END
                FROM read_parquet(?) p
                ASOF LEFT JOIN (SELECT * FROM read_parquet(?) WHERE CAST(month AS DATE) <= DATE '2025-03-01') r
                  ON p.cusip_id=r.cusip_id AND CAST(p.month AS DATE) >= CAST(r.month AS DATE)
                WHERE CAST(p.month AS DATE) <= ? ORDER BY CAST(p.month AS DATE), p.cusip_id LIMIT ? OFFSET ?""", [artifacts.path("bond_panel_live.parquet"), artifacts.path("bond_ratings_pit.parquet"), cutoff, limit, offset]).fetchall()
            lineage = _lineage(artifacts, "bond_panel_live.parquet", "bond_ratings_pit.parquet")
            rows = []
            columns.append("rating_staleness_months")
            for item in (_record(columns, row) for row in raw):
                item.update({"source_lineage": lineage})
                item["payload"] = dict(item)
                rows.append(item)
            return rows, ("bond_panel_live.parquet", "bond_ratings_pit.parquet")
        raise ValueError(f"unknown_surface:{surface}")
    finally:
        conn.close()
        state.cleanup()


def rows_for_surface(artifacts: ArtifactSet, plan: BackfillPlan, surface: Surface, *, start_after: int, limit: int) -> SurfaceRows:
    if surface not in SURFACES:
        raise ValueError(f"unknown_surface:{surface}")
    if start_after < 0 or limit <= 0:
        raise CursorError("invalid_cursor_or_limit")
    total = int(plan.base_repair["tail_rows"]) if plan.is_repair and surface == "returns" else plan.counts[surface]
    if start_after > total:
        raise CursorError("start_after_exceeds_surface")
    if plan.is_repair and surface == "returns":
        rows = _repair_return_tail_rows(artifacts, start_after=start_after, limit=limit)
    else:
        rows, _sources = _surface_query(artifacts, surface, plan.cutoff, limit, start_after)
    return SurfaceRows(surface, tuple(rows), start_after, start_after + len(rows), total)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render_prepare_sql(plan: BackfillPlan) -> str:
    """Create or attest the deterministic prepared publication; never move pointer."""
    evidence = json.dumps(plan.evidence(), sort_keys=True, separators=(",", ":"))
    hashes = json.dumps(plan.source_sha256, sort_keys=True, separators=(",", ":"))
    repair_evidence_check = f" AND p.gate_evidence @> {_sql_string(evidence)}::jsonb" if plan.is_repair else ""
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
INSERT INTO bond_panel_publications (publication_id, publication_status, config_hash, input_fingerprint, code_revision, first_month, last_closed_month, open_month, snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows, source_lineage, gate_evidence)
VALUES ({_sql_string(plan.publication_id)}::uuid, 'prepared', {_sql_string(plan.config_hash)}, {_sql_string(plan.input_fingerprint)}, {_sql_string(plan.code_revision)}, {_sql_string(plan.first_month)}::date, {_sql_string(plan.last_closed_month)}::date, NULL, {plan.counts['snapshot']}, {plan.counts['rv_signal']}, {plan.counts['returns']}, {plan.counts['rating_pit']}, jsonb_build_object('scope','frozen_osbap_trace_panel_scope','source_sha256',{_sql_string(hashes)}::jsonb), {_sql_string(evidence)}::jsonb)
ON CONFLICT (publication_id) DO NOTHING;
DO $prepared_backfill$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM bond_panel_publications p WHERE p.publication_id={_sql_string(plan.publication_id)}::uuid AND p.publication_status IN ('prepared','validated') AND p.config_hash={_sql_string(plan.config_hash)} AND p.input_fingerprint={_sql_string(plan.input_fingerprint)} AND p.code_revision={_sql_string(plan.code_revision)} AND p.first_month={_sql_string(plan.first_month)}::date AND p.last_closed_month={_sql_string(plan.last_closed_month)}::date AND p.open_month IS NULL AND p.snapshot_rows={plan.counts['snapshot']} AND p.rv_signal_rows={plan.counts['rv_signal']} AND p.returns_rows={plan.counts['returns']} AND p.ratings_pit_rows={plan.counts['rating_pit']} AND p.source_lineage @> jsonb_build_object('source_sha256',{_sql_string(hashes)}::jsonb){repair_evidence_check}) THEN
        RAISE EXCEPTION 'non-identical or non-resumable bond panel base publication';
    END IF;
END
$prepared_backfill$;
COMMIT;
SELECT jsonb_build_object('publication_id',{_sql_string(plan.publication_id)},'phase','prepared','config_hash',{_sql_string(plan.config_hash)},'source_sha256',{_sql_string(hashes)}::jsonb,'counts',{_sql_string(json.dumps(plan.counts, sort_keys=True))}::jsonb) AS backfill_evidence;
"""


def render_schema_sql() -> str:
    """Emit the worker-owned panel DDL in a role-scoped psql transaction."""
    return render_schema(SCHEMA_PATH.read_text(encoding="utf-8"))


_TABLES = {"snapshot": "bond_panel_snapshot", "rv_signal": "bond_panel_rv_signal", "returns": "bond_panel_returns", "rating_pit": "bond_panel_rating_pit"}
_COLUMNS: dict[Surface, tuple[str, ...]] = {
    "snapshot": ("publication_id", "month", "cusip_id", "issuer_id", "issuer_identity_state", "ff17num", "eligibility_state", "eligibility_reason", "currency", "asset_class", "amount_outstanding_k", "maturity_date", "maturity_years", "coupon_pct", "price", "price_source", "db_type", "ytm", "ytm_basis", "mod_dur", "mod_dur_source", "spread_final", "spread_final_bps", "spread_definition", "spread_source", "rating_bucket", "rating_state", "traded_days", "trade_count", "dollar_volume", "rel_bid_ask_bps", "quoted_days", "terms_source", "source_lineage", "payload"),
    "rv_signal": ("publication_id", "month", "cusip_id", "issuer_id", "ff17num", "eligibility_state", "eligibility_reason", "price", "amount_outstanding_k", "maturity_years", "traded_days", "trade_count", "dollar_volume", "rel_bid_ask_bps", "quoted_days", "ytm", "ytm_basis", "mod_dur", "mod_dur_source", "spread_final_bps", "spread_definition", "residual_bps", "rv_signal", "price_source", "flags", "source_lineage", "payload"),
    "returns": ("publication_id", "month", "cusip_id", "total_return", "price_return", "carry_return", "exit_basis", "exit_reason", "suspect", "payload"),
    "rating_pit": ("publication_id", "month", "cusip_id", "rating_bucket", "rating_as_of_month", "rating_state", "rating_reason", "rating_staleness_months", "source_lineage", "payload"),
}
_TYPES: dict[Surface, tuple[str, ...]] = {
    "snapshot": ("uuid", "date", "text", "text", "text", "integer", "text", "text", "text", "text", "numeric", "date", "numeric", "numeric", "numeric", "text", "integer", "numeric", "text", "numeric", "text", "numeric", "numeric", "text", "text", "text", "text", "integer", "integer", "numeric", "numeric", "integer", "text", "jsonb", "jsonb"),
    "rv_signal": ("uuid", "date", "text", "text", "integer", "text", "text", "numeric", "numeric", "numeric", "integer", "integer", "numeric", "numeric", "integer", "numeric", "text", "numeric", "text", "numeric", "text", "numeric", "numeric", "text", "jsonb", "jsonb", "jsonb"),
    "returns": ("uuid", "date", "text", "numeric", "numeric", "numeric", "text", "text", "boolean", "jsonb"),
    "rating_pit": ("uuid", "date", "text", "text", "date", "text", "text", "integer", "jsonb", "jsonb"),
}
_NULLABLE: dict[Surface, tuple[str, ...]] = {
    "snapshot": ("issuer_id", "ff17num", "amount_outstanding_k", "maturity_date", "maturity_years", "coupon_pct", "price", "price_source", "db_type", "ytm", "ytm_basis", "mod_dur", "mod_dur_source", "spread_final", "spread_final_bps", "spread_source", "traded_days", "trade_count", "dollar_volume", "rel_bid_ask_bps", "quoted_days", "terms_source"),
    "rv_signal": ("issuer_id", "ff17num", "price", "amount_outstanding_k", "maturity_years", "traded_days", "trade_count", "dollar_volume", "rel_bid_ask_bps", "quoted_days", "ytm", "ytm_basis", "mod_dur", "mod_dur_source", "spread_final_bps", "residual_bps", "rv_signal", "price_source"),
    "returns": ("price_return", "carry_return", "exit_reason"),
    "rating_pit": ("rating_as_of_month", "rating_staleness_months"),
}


def render_batch_sql(artifacts: ArtifactSet, plan: BackfillPlan, surface: Surface, *, start_after: int, limit: int) -> str:
    if plan.is_repair:
        if surface != "returns":
            raise PlanError("repair_surface_requires_database_copy")
        return _render_repair_tail_batch(artifacts, plan, start_after=start_after, limit=limit)
    selected = rows_for_surface(artifacts, plan, surface, start_after=start_after, limit=limit)
    columns = _COLUMNS[surface]
    values = []
    for row in selected.rows:
        values.append(tuple([plan.publication_id] + [row[column] for column in columns[1:]]))
    source_names = _surface_query(artifacts, surface, plan.cutoff, 0, 0)[1]
    evidence = "jsonb_build_object(" + ",".join((
        "'publication_id'," + _sql_string(plan.publication_id), "'surface'," + _sql_string(surface),
        "'source_sha256'," + _sql_string(json.dumps({name: artifacts.sha256[name] for name in source_names}, sort_keys=True)) + "::jsonb",
        "'cursor'," + str(selected.start_after), "'selected'," + str(len(selected.rows)),
        "'committed_through'," + str(selected.committed_through), "'remaining'," + str(selected.total - selected.committed_through),
        "'done'," + ("true" if selected.committed_through == selected.total else "false"), "'config_hash'," + _sql_string(plan.config_hash),
    )) + ")"
    emitted = render_immutable_batch(target=_TABLES[surface], columns=columns, column_types=_TYPES[surface], key_columns=("publication_id", "month", "cusip_id"), rows=values, artifact_sha256=artifacts.sha256[source_names[0]], start_after=selected.start_after, committed_through=selected.committed_through, skipped=0, target_evidence_sql=evidence, nullable_columns=_NULLABLE[surface])
    select_values = ", ".join(f's."{column}"' for column in columns)
    insert_select = f"SELECT {select_values} FROM _backfill_stage s"
    # On an exact post-finalize replay facts must remain untouched.  Existing
    # evidence was reconciled above; this condition turns its INSERT into a
    # no-op instead of firing the prepared-only immutable trigger.
    replay_safe_select = insert_select + f" WHERE EXISTS (SELECT 1 FROM bond_panel_publications p WHERE p.publication_id={_sql_string(plan.publication_id)}::uuid AND p.publication_status='prepared')"
    if insert_select not in emitted:  # pragma: no cover - guards transport drift
        raise RuntimeError("psql_transport_insert_shape_changed")
    return emitted.replace(insert_select, replay_safe_select, 1)


def _render_repair_tail_batch(artifacts: ArtifactSet, plan: BackfillPlan, *, start_after: int, limit: int) -> str:
    if start_after < 0 or limit <= 0:
        raise CursorError("invalid_cursor_or_limit")
    tail_total = int(plan.base_repair["tail_rows"])
    if start_after > tail_total:
        raise CursorError("start_after_exceeds_surface")
    selected = _repair_return_tail_rows(artifacts, start_after=start_after, limit=limit)
    columns = _COLUMNS["returns"]
    values = [tuple([plan.publication_id] + [row[column] for column in columns[1:]]) for row in selected]
    evidence = "jsonb_build_object('publication_id'," + _sql_string(plan.publication_id) + ",'surface','returns_repair_tail','source_sha256'," + _sql_string(json.dumps({name: artifacts.sha256[name] for name in ("bond_panel_live.parquet", "bond_monthly_returns.parquet")}, sort_keys=True)) + "::jsonb,'cursor'," + str(start_after) + ",'selected'," + str(len(selected)) + ",'committed_through'," + str(start_after + len(selected)) + ",'remaining'," + str(tail_total - start_after - len(selected)) + ",'done'," + ("true" if start_after + len(selected) == tail_total else "false") + ",'config_hash'," + _sql_string(plan.config_hash) + ",'base_repair'," + _sql_string(json.dumps(plan.base_repair, sort_keys=True)) + "::jsonb)"
    emitted = render_immutable_batch(target=_TABLES["returns"], columns=columns, column_types=_TYPES["returns"], key_columns=("publication_id", "month", "cusip_id"), rows=values, artifact_sha256=artifacts.sha256["bond_panel_live.parquet"], start_after=start_after, committed_through=start_after + len(selected), skipped=0, target_evidence_sql=evidence, nullable_columns=_NULLABLE["returns"])
    insert_select = "SELECT " + ", ".join(f's."{column}"' for column in columns) + " FROM _backfill_stage s"
    replay_safe = insert_select + f" WHERE EXISTS (SELECT 1 FROM bond_panel_publications p WHERE p.publication_id={_sql_string(plan.publication_id)}::uuid AND p.publication_status='prepared')"
    return emitted.replace(insert_select, replay_safe, 1)


def render_repair_copy_sql(plan: BackfillPlan, surface: Surface) -> str:
    """Copy immutable old facts inside PostgreSQL; only repaired return tail uses local transport."""
    if not plan.is_repair:
        raise PlanError("repair_copy_requires_repair_plan")
    source_id = plan.base_repair["from_publication_id"]
    table = _TABLES[surface]
    columns = _COLUMNS[surface]
    source_columns = ", ".join(f"source.{column}" for column in columns[1:])
    target_columns = ", ".join(columns)
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
DO $repair_source$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications prior
        JOIN bond_panel_app_pointer pointer ON pointer.product={_sql_string(PRODUCT)} AND pointer.publication_id=prior.publication_id
        JOIN bond_panel_publications candidate ON candidate.publication_id={_sql_string(plan.publication_id)}::uuid
        WHERE prior.publication_id={_sql_string(source_id)}::uuid
          AND prior.publication_status='validated' AND prior.parent_publication_id IS NULL
          AND prior.config_hash={_sql_string(CONFIG_HASH)}
          AND candidate.publication_status='prepared' AND candidate.parent_publication_id IS NULL
          AND candidate.gate_evidence @> jsonb_build_object('base_repair',{_sql_string(json.dumps(plan.base_repair, sort_keys=True))}::jsonb)
    ) THEN RAISE EXCEPTION 'repair copy requires exact evidence-bound current legacy source'; END IF;
END
$repair_source$;
INSERT INTO {table} ({target_columns})
SELECT {_sql_string(plan.publication_id)}::uuid, {source_columns}
FROM {table} source
WHERE source.publication_id={_sql_string(source_id)}::uuid
ON CONFLICT (publication_id, month, cusip_id) DO NOTHING;
DO $repair_copy_count$
BEGIN
    IF (SELECT count(*) FROM {table} WHERE publication_id={_sql_string(plan.publication_id)}::uuid) <> (SELECT count(*) FROM {table} WHERE publication_id={_sql_string(source_id)}::uuid) THEN
        RAISE EXCEPTION 'repair copy count mismatch:{surface}';
    END IF;
END
$repair_copy_count$;
COMMIT;
"""


def render_finalize_sql(plan: BackfillPlan) -> str:
    """Validate exact loaded counts then atomically validate and point exactly once."""
    hashes = json.dumps(plan.source_sha256, sort_keys=True, separators=(",", ":"))
    evidence = json.dumps(plan.evidence(), sort_keys=True, separators=(",", ":"))
    repair_evidence_check = f" AND gate_evidence @> {_sql_string(evidence)}::jsonb" if plan.is_repair else ""
    checks = "\n".join(f"    IF (SELECT count(*) FROM {_TABLES[surface]} WHERE publication_id={_sql_string(plan.publication_id)}::uuid) <> {plan.counts[surface]} THEN RAISE EXCEPTION 'partial {surface} surface'; END IF;" for surface in SURFACES)
    coverage_first = plan.returns_first_month if plan.is_repair else plan.first_month
    return_coverage_check = f"    IF EXISTS (SELECT 1 FROM generate_series({_sql_string(coverage_first)}::date, {_sql_string(plan.last_closed_month)}::date, INTERVAL '1 month') AS expected(month) LEFT JOIN bond_panel_returns r ON r.publication_id={_sql_string(plan.publication_id)}::uuid AND r.month=expected.month::date WHERE r.month IS NULL) THEN RAISE EXCEPTION 'returns history is not contiguous through closed-month cutoff'; END IF;"
    repair_cas = ""
    pointer_statement = f"INSERT INTO bond_panel_app_pointer (product, publication_id) VALUES ({_sql_string(PRODUCT)}, {_sql_string(plan.publication_id)}::uuid) ON CONFLICT (product) DO UPDATE SET publication_id=EXCLUDED.publication_id, changed_at=now() WHERE bond_panel_app_pointer.publication_id IS DISTINCT FROM EXCLUDED.publication_id;"
    if plan.is_repair:
        source_id = plan.base_repair["from_publication_id"]
        repair_cas = f"""DO $repair_finalize$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM bond_panel_publications prior WHERE prior.publication_id={_sql_string(source_id)}::uuid AND prior.publication_status='validated' AND prior.parent_publication_id IS NULL AND prior.config_hash={_sql_string(CONFIG_HASH)}) THEN RAISE EXCEPTION 'repair source no longer matches authorized legacy base'; END IF;
END
$repair_finalize$;
"""
        pointer_statement = f"""DO $repair_cas$
BEGIN
    UPDATE bond_panel_app_pointer
    SET publication_id={_sql_string(plan.publication_id)}::uuid, changed_at=now()
    WHERE product={_sql_string(PRODUCT)} AND publication_id={_sql_string(source_id)}::uuid;
    IF NOT FOUND AND NOT EXISTS (
        SELECT 1 FROM bond_panel_app_pointer
        WHERE product={_sql_string(PRODUCT)} AND publication_id={_sql_string(plan.publication_id)}::uuid
    ) THEN RAISE EXCEPTION 'repair pointer compare-and-swap lost'; END IF;
END
$repair_cas$;"""
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
DO $finalize_backfill$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM bond_panel_publications WHERE publication_id={_sql_string(plan.publication_id)}::uuid AND publication_status IN ('prepared','validated') AND config_hash={_sql_string(plan.config_hash)} AND input_fingerprint={_sql_string(plan.input_fingerprint)} AND code_revision={_sql_string(plan.code_revision)} AND first_month={_sql_string(plan.first_month)}::date AND last_closed_month={_sql_string(plan.last_closed_month)}::date AND open_month IS NULL AND source_lineage @> jsonb_build_object('source_sha256',{_sql_string(hashes)}::jsonb){repair_evidence_check}) THEN RAISE EXCEPTION 'non-identical base publication finalization'; END IF;
{checks}
{return_coverage_check}
END
$finalize_backfill$;
UPDATE bond_panel_publications SET publication_status='validated', validated_at=COALESCE(validated_at, now()), gate_evidence=gate_evidence || jsonb_build_object('validated_counts',{_sql_string(json.dumps(plan.counts, sort_keys=True))}::jsonb,'source_sha256',{_sql_string(hashes)}::jsonb,'historical_return_coverage_through',{_sql_string(plan.returns_last_month)}) WHERE publication_id={_sql_string(plan.publication_id)}::uuid AND publication_status='prepared';
{repair_cas}{pointer_statement}
COMMIT;
SELECT jsonb_build_object('publication_id',{_sql_string(plan.publication_id)},'phase','validated_and_pointed','config_hash',{_sql_string(plan.config_hash)},'source_sha256',{_sql_string(hashes)}::jsonb,'counts',{_sql_string(json.dumps(plan.counts, sort_keys=True))}::jsonb,'historical_return_coverage_through',{_sql_string(plan.returns_last_month)}) AS backfill_evidence;
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIRECTORY)
    parser.add_argument("--cutoff", default=DEFAULT_CUTOFF)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="emit verified read-only planning evidence as JSON")
    mode.add_argument("--evidence", action="store_true", help="alias for --plan")
    mode.add_argument(
        "--emit-schema",
        action="store_true",
        help="emit the panel DDL as an administrative install that transfers ownership to worker_writer",
    )
    mode.add_argument("--emit-prepare", action="store_true")
    mode.add_argument("--emit-batch", choices=SURFACES)
    mode.add_argument("--emit-repair-copy", choices=SURFACES, help="repair-only database-to-database copy of the exact current legacy publication")
    mode.add_argument("--emit-finalize", action="store_true")
    parser.add_argument("--repair-from-publication-id", help="enable the one evidence-bound legacy root replacement; ordinary mode never repairs return coverage")
    parser.add_argument("--start-after", type=int, default=0)
    parser.add_argument("--limit", type=int, help="bounded row count for one --emit-batch transaction; choose an operator-safe size")
    args = parser.parse_args(argv)
    if args.emit_batch and args.limit is None:
        parser.error("--limit is required with --emit-batch")
    if args.emit_schema:
        print(render_schema_sql(), end="")
        return 0
    try:
        artifacts = ArtifactSet.open(args.artifact_dir)
        plan = build_repair_plan(artifacts, from_publication_id=args.repair_from_publication_id) if args.repair_from_publication_id else build_plan(artifacts, cutoff=args.cutoff)
        if args.plan or args.evidence:
            print(json.dumps(plan.evidence(), sort_keys=True))
        elif args.emit_prepare:
            print(render_prepare_sql(plan), end="")
        elif args.emit_batch:
            print(render_batch_sql(artifacts, plan, args.emit_batch, start_after=args.start_after, limit=args.limit), end="")
        elif args.emit_repair_copy:
            print(render_repair_copy_sql(plan, args.emit_repair_copy), end="")
        else:
            print(render_finalize_sql(plan), end="")
    except (ArtifactPinError, PlanError, CursorError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

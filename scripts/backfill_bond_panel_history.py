"""Offline-only, resumable T3 base publication emitter for the frozen OSBAP panel.

This program deliberately has no DSN option and never connects to a database.
It verifies the five local artifacts before emitting either read-only planning
evidence or a single stdin-safe ``psql`` transaction.  Production workers must
not import this module: it is a one-time historical transport only.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import sys
import tempfile
import uuid
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

import duckdb
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""} and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_psql_transport import (
    render_immutable_batch,
    render_schema,
)

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
REPAIR_EXPECTED_TAIL_DIGEST = "e6f2911143d01b1417973714a7d35f0040af90b0747917d326c5d055c29c9663"
REPAIR_EXPECTED_PUBLICATION_ID = "b3c92982-d82f-5a76-bb51-a4c980d21b25"
REPAIR_EXPECTED_INPUT_FINGERPRINT = "6e00313b5f2774dbd71e4c6f96f8c628e3a19015e9a1775b0dac986c5fdf1e7e"
REPAIR_EXPECTED_FIRST_MONTH = "2002-07-01"
REPAIR_EXPECTED_RETURNS_FIRST_MONTH = "2002-08-01"
REPAIR_EXPECTED_PANEL_WITHOUT_RATING_PIT = 1347344
REPAIR_EXPECTED_COUNTS = {
    "snapshot": 3417683,
    "rv_signal": 1687524,
    "returns": 2801208,
    "rating_pit": 3417683,
}
REPAIR_EXPECTED_TOTAL_RETURNS = REPAIR_EXPECTED_COUNTS["returns"]
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

# --- Unit-repair child (T2.1) -------------------------------------------------
# ``C`` extends the current validated head ``H`` with H's exact window and
# copies the four ``bond_panel_current_*_v1`` views inside PostgreSQL.  The
# declared unit predicate is applied ONLY to the two volume surfaces; returns
# and rating facts are copied verbatim with the identity fill the dual-series
# trigger requires.  Inputs are the frozen v2 parquet set derived from the
# T0.2v read-only export; like the rest of this program it never opens a
# database connection.
UNIT_REPAIR_CONTRACT = "dollar_volume_unit_repair_v1"
UNIT_REPAIR_CODE_REVISION = "t3_dollar_volume_unit_repair_v1"
UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID = "71b672c8-239c-55bc-bccb-ed39960c0fd2"
UNIT_REPAIR_EXPECTED_PUBLICATION_ID = "65156481-8cb4-52b5-8676-cf77edc5644f"
UNIT_REPAIR_EXPECTED_INPUT_FINGERPRINT = (
    "7063a271999f3861b24fdba0063e3ede0eb382cad053c2b64fefa8c416c01e8e"
)
UNIT_REPAIR_EXPECTED_PER_YEAR_DIGEST = (
    "b3b66e57f0d612e6d2a471543484c82450d09dcb0bf782ea756954517bc14b62"
)
UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID = "b3c92982-d82f-5a76-bb51-a4c980d21b25"
UNIT_REPAIR_CONFIG_HASH = "1863d3d5fa3a0edf"
UNIT_REPAIR_SCALE = 1_000_000
UNIT_REPAIR_PREDICATE = "price_source = 'osbap'"
UNIT_REPAIR_AFFECTED_SURFACES: tuple[Surface, ...] = ("snapshot", "rv_signal")
UNIT_REPAIR_ARTIFACT_CUTOFF = "2026-06-01"
# The v2 artifacts store ``dollar_volume`` as float64 while PostgreSQL stores
# ``numeric``.  The artifact side sums the per-row DECIMAL(38,6) casts, which is
# exact and order-independent (so the plan fingerprint is reproducible across
# runs); the measured worst-case difference from the exact numeric sums is
# 0.0012 USD per year.  The gate keeps exact row/NULL equality and compares the
# DECIMAL(38,6)-cast sums within this declared absolute tolerance.
UNIT_REPAIR_SUM_TOLERANCE_USD = 1
# §14 execution shape: the whole validation + status + CAS runs in one timed
# transaction; the lock timeout must fail instead of queueing an incident, and
# the refresh phase after COMMIT gets its own explicit session-level timeout.
UNIT_REPAIR_FINALIZE_LOCK_TIMEOUT = "5s"
UNIT_REPAIR_FINALIZE_STATEMENT_TIMEOUT = "55min"
UNIT_REPAIR_FINALIZE_REFRESH_STATEMENT_TIMEOUT = "20min"
UNIT_REPAIR_EXPECTED_WINDOW = {
    "first_month": "2002-07-01",
    "last_closed_month": "2026-08-01",
    "open_month": "2026-09-01",
}
UNIT_REPAIR_EXPECTED_COUNTS = {
    "snapshot": 3448307,
    "rv_signal": 1689773,
    "returns": 2810912,
    "rating_pit": 3448307,
}
EXPECTED_SHA256_UNIT_REPAIR_V2 = {
    "bond_panel_live.parquet": "2bc85774f608aebc57e8e345e49113938549f8f89ae87dee509cdcae8758aba2",
    "universe_snapshots_live.parquet": "eaad18121d48d885fc23b6a28a96f33d00f444a7479296997520dc9e782c07a8",
    "bond_monthly_returns.parquet": "a2778b5c723f1d4c1c91e31319dfe2618589a30ce56c60a9d7d5008482f39007",
    "bond_ratings_pit.parquet": "309b405b6de34dae51cf8bb702ad3e4b78123316de23a8f44225c200bf2e96a3",
}
UNIT_REPAIR_ARTIFACT_SURFACES = {
    "bond_panel_live.parquet": "snapshot",
    "universe_snapshots_live.parquet": "rv_signal",
    "bond_monthly_returns.parquet": "returns",
    "bond_ratings_pit.parquet": "rating_pit",
}
UNIT_REPAIR_REQUIRED_COLUMNS = {
    "bond_panel_live.parquet": {"month", "cusip_id", "price_source", "dollar_volume"},
    "universe_snapshots_live.parquet": {"month", "cusip_id", "price_source", "dollar_volume"},
    "bond_monthly_returns.parquet": {"month", "cusip_id"},
    "bond_ratings_pit.parquet": {"month", "cusip_id"},
}
UNIT_REPAIR_USD_BAND = (10_000.0, 1_000_000_000.0)
EXPECTED_EXPORT_PROVENANCE = {
    "source_pointer": UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID,
    "export_manifest_sha256": "8afb23a0ff0616a8256f292aa5b6748a7fffa552705b18521874a1d11be6ff4f",
    "export_datetime_utc": "2026-09-18T21:32:07Z",
    "export_files": {
        "snapshot": "55963cc686cf054f6f2b23835fe051018f5891e71d2b7658a86e7edc283faebc",
        "rv_signal": "378567e56ebc7a602367e75080db367eee42d68e3b65f620583c0badb0f57024",
        "returns": "3fd16c0c9300dee19a3f14fa11470ed82822c9da36ee603fbacd397bc28a0cec",
        "rating_pit": "8452ced775290911e8a669de952b36cc497f8ab5a81f00b79daed151f6bf7940",
    },
}
UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY = Path(
    r"C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\unit_repair_v2"
)

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
    def open(cls, directory: Path, *, expected_hashes: dict[str, str] | None = None) -> ArtifactSet:
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


def _authorized_repair_base_evidence() -> dict[str, Any]:
    """Return the one evidence record permitted for the frozen root repair."""
    return {
        "contract": REPAIR_CONTRACT,
        "from_publication_id": LEGACY_REPAIR_FROM_PUBLICATION_ID,
        "from_config_hash": CONFIG_HASH,
        "from_input_fingerprint": LEGACY_REPAIR_FROM_INPUT_FINGERPRINT,
        "from_artifact_fingerprint": LEGACY_REPAIR_ARTIFACT_FINGERPRINT,
        "first_month": REPAIR_EXPECTED_FIRST_MONTH,
        "last_closed_month": DEFAULT_CUTOFF,
        "reconstruction": "median_coupon_from_historical_carry_then_price_ytm_fallback",
        "tail_rows": REPAIR_EXPECTED_TAIL_ROWS,
        "tail_months": len(REPAIR_TAIL_MONTH_COUNTS),
        "tail_month_counts": list(REPAIR_TAIL_MONTH_COUNTS),
        "tail_cusips": REPAIR_EXPECTED_TAIL_CUSIPS,
        "tail_suspect": REPAIR_EXPECTED_TAIL_SUSPECT,
        "tail_digest": REPAIR_EXPECTED_TAIL_DIGEST,
        "authorized_code_revision": REPAIR_CODE_REVISION,
    }


def _validate_authorized_repair_plan(plan: BackfillPlan) -> None:
    """Fail closed before emission when the pinned frozen repair drifts."""
    if not plan.is_repair or plan.source_sha256 != EXPECTED_SHA256:
        return
    if (
        plan.publication_id != REPAIR_EXPECTED_PUBLICATION_ID
        or plan.input_fingerprint != REPAIR_EXPECTED_INPUT_FINGERPRINT
        or plan.first_month != REPAIR_EXPECTED_FIRST_MONTH
        or plan.last_closed_month != DEFAULT_CUTOFF
        or plan.returns_first_month != REPAIR_EXPECTED_RETURNS_FIRST_MONTH
        or plan.returns_last_month != DEFAULT_CUTOFF
        or plan.counts != REPAIR_EXPECTED_COUNTS
        or plan.panel_without_rating_pit != REPAIR_EXPECTED_PANEL_WITHOUT_RATING_PIT
        or plan.base_repair != _authorized_repair_base_evidence()
    ):
        raise PlanError("repair_plan_not_authorized")


def _repair_tail_attestation_evidence(
    plan: BackfillPlan, *, cursor: int, committed_through: int,
) -> dict[str, Any]:
    return {
        "publication_id": plan.publication_id,
        "surface": "returns_repair_tail",
        "cursor": cursor,
        "committed_through": committed_through,
        "source_sha256": plan.source_sha256,
        "tail_digest": plan.base_repair["tail_digest"],
        "base_repair": plan.base_repair,
    }


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
                SELECT month, cusip_id, price, ytm, ytm / 2 AS y,
                        greatest(CAST(round(2 * maturity) AS INTEGER), 1) AS periods
                FROM panel
                WHERE isfinite(price) AND isfinite(ytm) AND isfinite(maturity)
            ), implied_math AS (
                SELECT *, pow(1 + y, -periods) AS discount FROM implied_parts
            ), implied_coupon_points AS (
                SELECT month, cusip_id, greatest(0.0, least(20.0,
                    CASE WHEN abs(y) > 0 AND (1 - discount) / y > 1e-9
                         THEN (price / 100 - discount) / ((1 - discount) / y) * 200
                         ELSE ytm * 100 END)) AS coupon
                FROM implied_math
            ), implied_coupon AS (
                SELECT cusip_id, median(greatest(0.0, least(20.0,
                    CASE WHEN abs(y) > 0 AND (1 - discount) / y > 1e-9
                         THEN (price / 100 - discount) / ((1 - discount) / y) * 200
                         ELSE ytm * 100 END))) AS coupon
                FROM implied_math WHERE month <= DATE '2025-03-01' GROUP BY cusip_id
            ), entrant_implied_coupon AS (
                SELECT cusip_id, coupon
                FROM (
                    SELECT cusip_id, coupon,
                           row_number() OVER (PARTITION BY cusip_id ORDER BY month) AS ordinal
                    FROM implied_coupon_points
                    WHERE month > DATE '2025-03-01'
                ) ranked
                WHERE ordinal = 1
            ), coupons AS (
                SELECT ids.cusip_id, coalesce(h.coupon, i.coupon, entrant.coupon) AS coupon
                FROM (
                    SELECT cusip_id FROM historical_coupon
                    UNION SELECT cusip_id FROM implied_coupon
                    UNION SELECT cusip_id FROM entrant_implied_coupon
                ) ids
                LEFT JOIN historical_coupon h USING (cusip_id)
                LEFT JOIN implied_coupon i USING (cusip_id)
                LEFT JOIN entrant_implied_coupon entrant USING (cusip_id)
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
    counts["returns"] = historical_return_count + len(tail)
    source_sha256 = dict(sorted(artifacts.sha256.items()))
    original_artifact_fingerprint = _canonical_digest({"source_sha256": source_sha256})
    base_repair = {"contract": REPAIR_CONTRACT, "from_publication_id": from_publication_id, "from_config_hash": CONFIG_HASH, "from_input_fingerprint": LEGACY_REPAIR_FROM_INPUT_FINGERPRINT, "from_artifact_fingerprint": original_artifact_fingerprint, "first_month": _scalar_month(first_month), "last_closed_month": DEFAULT_CUTOFF, "reconstruction": "median_coupon_from_historical_carry_then_price_ytm_fallback", "tail_rows": len(tail), "tail_months": len(month_counts), "tail_month_counts": month_counts, "tail_cusips": len({row["cusip_id"] for row in tail}), "tail_suspect": sum(bool(row["suspect"]) for row in tail), "tail_digest": tail_digest, "authorized_code_revision": REPAIR_CODE_REVISION}
    fingerprint = _canonical_digest({"config_hash": CONFIG_HASH, "source_sha256": source_sha256, "base_repair": base_repair})
    publication_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCT}:legacy-base-repair:{fingerprint}"))
    plan = BackfillPlan(publication_id, fingerprint, DEFAULT_CUTOFF, _scalar_month(first_month), DEFAULT_CUTOFF, DEFAULT_CUTOFF, counts, source_sha256, panel_without_rating_pit, base_repair=base_repair, returns_first_month=_scalar_month(returns_first_month))
    _validate_authorized_repair_plan(plan)
    return plan


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
    _validate_authorized_repair_plan(plan)
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
_DISTRIBUTION_COLUMNS = ("distribution_rule", "reference_cusip9", "distribution_decision_id")
_COPY_COLUMNS: dict[Surface, tuple[str, ...]] = {
    surface: columns[:3] + _DISTRIBUTION_COLUMNS + columns[3:]
    for surface, columns in _COLUMNS.items()
}
_TYPES: dict[Surface, tuple[str, ...]] = {
    "snapshot": ("uuid", "date", "text", "text", "text", "integer", "text", "text", "text", "text", "numeric", "date", "numeric", "numeric", "numeric", "text", "integer", "numeric", "text", "numeric", "text", "numeric", "numeric", "text", "text", "text", "text", "integer", "integer", "numeric", "numeric", "integer", "text", "jsonb", "jsonb"),
    "rv_signal": ("uuid", "date", "text", "text", "integer", "text", "text", "numeric", "numeric", "numeric", "integer", "integer", "numeric", "numeric", "integer", "numeric", "text", "numeric", "text", "numeric", "text", "numeric", "numeric", "text", "jsonb", "jsonb", "jsonb"),
    "returns": ("uuid", "date", "text", "numeric", "numeric", "numeric", "text", "text", "boolean", "jsonb"),
    "rating_pit": ("uuid", "date", "text", "text", "date", "text", "text", "integer", "jsonb", "jsonb"),
}
_COPY_TYPES: dict[Surface, tuple[str, ...]] = {
    surface: types[:3] + ("text", "text", "text") + types[3:]
    for surface, types in _TYPES.items()
}
_NULLABLE: dict[Surface, tuple[str, ...]] = {
    "snapshot": ("issuer_id", "ff17num", "amount_outstanding_k", "maturity_date", "maturity_years", "coupon_pct", "price", "price_source", "db_type", "ytm", "ytm_basis", "mod_dur", "mod_dur_source", "spread_final", "spread_final_bps", "spread_source", "traded_days", "trade_count", "dollar_volume", "rel_bid_ask_bps", "quoted_days", "terms_source"),
    "rv_signal": ("issuer_id", "ff17num", "price", "amount_outstanding_k", "maturity_years", "traded_days", "trade_count", "dollar_volume", "rel_bid_ask_bps", "quoted_days", "ytm", "ytm_basis", "mod_dur", "mod_dur_source", "spread_final_bps", "residual_bps", "rv_signal", "price_source"),
    "returns": ("price_return", "carry_return", "exit_reason"),
    "rating_pit": ("rating_as_of_month", "rating_staleness_months"),
}
_COPY_NULLABLE: dict[Surface, tuple[str, ...]] = {
    surface: _DISTRIBUTION_COLUMNS + nullable
    for surface, nullable in _NULLABLE.items()
}


def render_batch_sql(artifacts: ArtifactSet, plan: BackfillPlan, surface: Surface, *, start_after: int, limit: int) -> str:
    if plan.is_repair:
        _validate_authorized_repair_plan(plan)
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
    if start_after >= tail_total:
        raise CursorError("start_after_exceeds_surface")
    selected = _repair_return_tail_rows(artifacts, start_after=start_after, limit=limit)
    if not selected:  # pragma: no cover - guarded by the frozen tail contract
        raise PlanError("repair_tail_slice_empty")
    columns = _COPY_COLUMNS["returns"]
    column_types = _COPY_TYPES["returns"]
    nullable_columns = _COPY_NULLABLE["returns"]
    values = [tuple([plan.publication_id] + [row.get(column) for column in columns[1:]]) for row in selected]
    committed_through = start_after + len(selected)
    tail_sources = {name: artifacts.sha256[name] for name in ("bond_panel_live.parquet", "bond_monthly_returns.parquet")}
    evidence = "jsonb_build_object('publication_id'," + _sql_string(plan.publication_id) + ",'surface','returns_repair_tail','source_sha256'," + _sql_string(json.dumps(tail_sources, sort_keys=True)) + "::jsonb,'cursor'," + str(start_after) + ",'selected'," + str(len(selected)) + ",'committed_through'," + str(committed_through) + ",'remaining'," + str(tail_total - committed_through) + ",'done'," + ("true" if committed_through == tail_total else "false") + ",'config_hash'," + _sql_string(plan.config_hash) + ",'base_repair'," + _sql_string(json.dumps(plan.base_repair, sort_keys=True)) + "::jsonb)"
    emitted = render_immutable_batch(target=_TABLES["returns"], columns=columns, column_types=column_types, key_columns=("publication_id", "month", "cusip_id"), rows=values, artifact_sha256=artifacts.sha256["bond_panel_live.parquet"], start_after=start_after, committed_through=committed_through, skipped=0, target_evidence_sql=evidence, nullable_columns=nullable_columns)
    source_id = plan.base_repair["from_publication_id"]
    expected_before = f"(SELECT count(*) FROM bond_panel_returns WHERE publication_id={_sql_string(source_id)}::uuid) + {start_after}"
    expected_after = f"(SELECT count(*) FROM bond_panel_returns WHERE publication_id={_sql_string(source_id)}::uuid) + {committed_through}"
    terminal = committed_through == tail_total
    attestation = _repair_tail_attestation_evidence(
        plan, cursor=start_after, committed_through=committed_through,
    )
    attestation_json = _sql_string(json.dumps(attestation, sort_keys=True, separators=(",", ":")))
    predecessor_evidence = dict(attestation)
    predecessor_evidence.pop("cursor")
    predecessor_evidence["committed_through"] = start_after
    predecessor_json = _sql_string(json.dumps(predecessor_evidence, sort_keys=True, separators=(",", ":")))
    prepared_current = f"""EXISTS (
        SELECT 1
        FROM bond_panel_publications candidate
        JOIN bond_panel_app_pointer pointer ON pointer.product={_sql_string(PRODUCT)}
        WHERE candidate.publication_id={_sql_string(plan.publication_id)}::uuid
          AND candidate.publication_status='prepared'
          AND pointer.publication_id={_sql_string(source_id)}::uuid
    )"""
    validated_terminal = f"""EXISTS (
        SELECT 1
        FROM bond_panel_publications candidate
        JOIN bond_panel_app_pointer pointer ON pointer.product={_sql_string(PRODUCT)}
        WHERE candidate.publication_id={_sql_string(plan.publication_id)}::uuid
          AND candidate.publication_status='validated'
          AND pointer.publication_id={_sql_string(plan.publication_id)}::uuid
    )"""
    repair_order_check = f"""DO $repair_tail_order$
BEGIN
    IF NOT ({prepared_current}) THEN
        IF NOT ({'TRUE' if terminal else 'FALSE'} AND {validated_terminal}) THEN
            RAISE EXCEPTION '{'repair terminal tail replay requires validated pointed candidate' if terminal else 'repair tail requires prepared candidate and current legacy pointer'}';
        END IF;
    END IF;
    IF {start_after} > 0 AND NOT EXISTS (
        SELECT 1
        FROM bond_panel_repair_tail_batch_attestation prior_batch
        WHERE prior_batch.publication_id={_sql_string(plan.publication_id)}::uuid
          AND prior_batch.committed_through={start_after}
          AND prior_batch.evidence @> {predecessor_json}::jsonb
    ) THEN RAISE EXCEPTION 'repair tail requires contiguous attested prefix'; END IF;
    IF (SELECT count(*) FROM bond_panel_returns WHERE publication_id={_sql_string(plan.publication_id)}::uuid) NOT IN ({expected_before}, {expected_after}) THEN
        RAISE EXCEPTION 'repair tail requires exact copied historical returns before tail';
    END IF;
END
$repair_tail_order$;
"""
    lock_statement = 'LOCK TABLE "bond_panel_returns" IN SHARE ROW EXCLUSIVE MODE;\n'
    if lock_statement not in emitted:  # pragma: no cover - guards transport drift
        raise RuntimeError("psql_transport_lock_shape_changed")
    emitted = emitted.replace(lock_statement, lock_statement + repair_order_check, 1)
    insert_select = "SELECT " + ", ".join(f's."{column}"' for column in columns) + " FROM _backfill_stage s"
    replay_safe = insert_select + f" WHERE EXISTS (SELECT 1 FROM bond_panel_publications p WHERE p.publication_id={_sql_string(plan.publication_id)}::uuid AND p.publication_status='prepared')"
    emitted = emitted.replace(insert_select, replay_safe, 1)
    final_count_check = f"""DO $repair_tail_final_count$
BEGIN
    IF (SELECT count(*) FROM bond_panel_returns WHERE publication_id={_sql_string(plan.publication_id)}::uuid) <> {expected_after} THEN
        RAISE EXCEPTION 'repair tail final count mismatch';
    END IF;
END
$repair_tail_final_count$;
"""
    attestation_check = f"""DO $repair_tail_attestation$
BEGIN
    IF {prepared_current} THEN
        INSERT INTO bond_panel_repair_tail_batch_attestation (
            publication_id, cursor, committed_through, predecessor_committed_through, evidence
        ) VALUES (
            {_sql_string(plan.publication_id)}::uuid, {start_after}, {committed_through},
            {'NULL' if start_after == 0 else str(start_after)}, {attestation_json}::jsonb
        ) ON CONFLICT (publication_id, cursor) DO NOTHING;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM bond_panel_repair_tail_batch_attestation current_batch
        WHERE current_batch.publication_id={_sql_string(plan.publication_id)}::uuid
          AND current_batch.cursor={start_after}
          AND current_batch.committed_through={committed_through}
          AND current_batch.evidence = {attestation_json}::jsonb
    ) THEN RAISE EXCEPTION 'repair tail attestation conflict'; END IF;
END
$repair_tail_attestation$;
"""
    commit_statement = "COMMIT;\nSELECT jsonb_build_object("
    if commit_statement not in emitted:  # pragma: no cover - guards transport drift
        raise RuntimeError("psql_transport_commit_shape_changed")
    return emitted.replace(commit_statement, final_count_check + attestation_check + commit_statement, 1)


def render_repair_copy_sql(plan: BackfillPlan, surface: Surface) -> str:
    """Copy immutable old facts inside PostgreSQL; only repaired return tail uses local transport."""
    if not plan.is_repair:
        raise PlanError("repair_copy_requires_repair_plan")
    _validate_authorized_repair_plan(plan)
    source_id = plan.base_repair["from_publication_id"]
    table = _TABLES[surface]
    columns = _COPY_COLUMNS[surface]
    source_columns = ", ".join(f"source.{column}" for column in columns[1:])
    target_columns = ", ".join(columns)
    candidate_facts = ", ".join(f"candidate.{column}" for column in columns[1:])
    source_facts = ", ".join(f"source.{column}" for column in columns[1:])
    if surface == "returns":
        historical_max_month = (
            f"(SELECT max(legacy.month) FROM {table} legacy "
            f"WHERE legacy.publication_id={_sql_string(source_id)}::uuid)"
        )
        source_range = f" AND source.month <= {historical_max_month}"
        candidate_range = f" AND candidate.month <= {historical_max_month}"
        copy_count_check = f"""DO $repair_copy_count$
BEGIN
    IF (SELECT count(*) FROM {table} candidate WHERE candidate.publication_id={_sql_string(plan.publication_id)}::uuid{candidate_range}) <> (SELECT count(*) FROM {table} source WHERE source.publication_id={_sql_string(source_id)}::uuid{source_range}) THEN
        RAISE EXCEPTION 'repair historical copy count mismatch:{surface}';
    END IF;
END
$repair_copy_count$;"""
    else:
        source_range = ""
        candidate_range = ""
        copy_count_check = f"""DO $repair_copy_count$
BEGIN
    IF (SELECT count(*) FROM {table} WHERE publication_id={_sql_string(plan.publication_id)}::uuid) <> (SELECT count(*) FROM {table} WHERE publication_id={_sql_string(source_id)}::uuid) THEN
        RAISE EXCEPTION 'repair copy count mismatch:{surface}';
    END IF;
END
$repair_copy_count$;"""
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
DO $repair_source$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications prior
        JOIN bond_panel_app_pointer pointer ON pointer.product={_sql_string(PRODUCT)}
        JOIN bond_panel_publications candidate ON candidate.publication_id={_sql_string(plan.publication_id)}::uuid
        WHERE prior.publication_id={_sql_string(source_id)}::uuid
          AND prior.publication_status='validated' AND prior.parent_publication_id IS NULL
          AND prior.config_hash={_sql_string(CONFIG_HASH)}
          AND candidate.publication_status IN ('prepared','validated') AND candidate.parent_publication_id IS NULL
          AND candidate.gate_evidence @> jsonb_build_object('base_repair',{_sql_string(json.dumps(plan.base_repair, sort_keys=True))}::jsonb)
          AND (
              (candidate.publication_status='prepared' AND pointer.publication_id=prior.publication_id)
              OR (candidate.publication_status='validated' AND pointer.publication_id=candidate.publication_id)
          )
    ) THEN RAISE EXCEPTION 'repair copy requires exact evidence-bound current legacy source'; END IF;
END
$repair_source$;
LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE;
DO $repair_copy_conflict$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM {table} source
        JOIN {table} candidate
          ON candidate.publication_id={_sql_string(plan.publication_id)}::uuid
         AND source.publication_id={_sql_string(source_id)}::uuid
         AND candidate.month=source.month
         AND candidate.cusip_id=source.cusip_id
        WHERE ROW({candidate_facts}) IS DISTINCT FROM ROW({source_facts}){source_range}{candidate_range}
    ) THEN RAISE EXCEPTION 'repair copy immutable evidence conflict:{surface}'; END IF;
END
$repair_copy_conflict$;
INSERT INTO {table} ({target_columns})
SELECT {_sql_string(plan.publication_id)}::uuid, {source_columns}
FROM {table} source
WHERE source.publication_id={_sql_string(source_id)}::uuid{source_range}
  AND EXISTS (
      SELECT 1 FROM bond_panel_publications candidate
      WHERE candidate.publication_id={_sql_string(plan.publication_id)}::uuid
        AND candidate.publication_status='prepared'
  )
ON CONFLICT (publication_id, month, cusip_id) DO NOTHING;
{copy_count_check}
COMMIT;
"""


def render_finalize_sql(plan: BackfillPlan) -> str:
    """Validate exact loaded counts then atomically validate and point exactly once."""
    _validate_authorized_repair_plan(plan)
    hashes = json.dumps(plan.source_sha256, sort_keys=True, separators=(",", ":"))
    evidence = json.dumps(plan.evidence(), sort_keys=True, separators=(",", ":"))
    repair_evidence_check = f" AND gate_evidence @> {_sql_string(evidence)}::jsonb" if plan.is_repair else ""
    checks = "\n".join(f"    IF (SELECT count(*) FROM {_TABLES[surface]} WHERE publication_id={_sql_string(plan.publication_id)}::uuid) <> {plan.counts[surface]} THEN RAISE EXCEPTION 'partial {surface} surface'; END IF;" for surface in SURFACES)
    coverage_first = plan.returns_first_month if plan.is_repair else plan.first_month
    return_coverage_check = f"    IF EXISTS (SELECT 1 FROM generate_series({_sql_string(coverage_first)}::date, {_sql_string(plan.last_closed_month)}::date, INTERVAL '1 month') AS expected(month) LEFT JOIN bond_panel_returns r ON r.publication_id={_sql_string(plan.publication_id)}::uuid AND r.month=expected.month::date WHERE r.month IS NULL) THEN RAISE EXCEPTION 'returns history is not contiguous through closed-month cutoff'; END IF;"
    terminal_attestation_check = ""
    if plan.is_repair:
        terminal_attestation = _repair_tail_attestation_evidence(
            plan,
            cursor=0,
            committed_through=int(plan.base_repair["tail_rows"]),
        )
        terminal_attestation.pop("cursor")
        terminal_attestation_json = _sql_string(
            json.dumps(terminal_attestation, sort_keys=True, separators=(",", ":")),
        )
        terminal_attestation_check = f"""    IF NOT EXISTS (
        SELECT 1
        FROM bond_panel_repair_tail_batch_attestation terminal_batch
        WHERE terminal_batch.publication_id={_sql_string(plan.publication_id)}::uuid
          AND terminal_batch.committed_through={plan.base_repair['tail_rows']}
          AND terminal_batch.evidence @> {terminal_attestation_json}::jsonb
    ) THEN RAISE EXCEPTION 'repair terminal tail attestation missing or non-identical'; END IF;"""
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
{terminal_attestation_check}
END
$finalize_backfill$;
UPDATE bond_panel_publications SET publication_status='validated', validated_at=COALESCE(validated_at, now()), gate_evidence=gate_evidence || jsonb_build_object('validated_counts',{_sql_string(json.dumps(plan.counts, sort_keys=True))}::jsonb,'source_sha256',{_sql_string(hashes)}::jsonb,'historical_return_coverage_through',{_sql_string(plan.returns_last_month)}) WHERE publication_id={_sql_string(plan.publication_id)}::uuid AND publication_status='prepared';
{repair_cas}{pointer_statement}
COMMIT;
SELECT jsonb_build_object('publication_id',{_sql_string(plan.publication_id)},'phase','validated_and_pointed','config_hash',{_sql_string(plan.config_hash)},'source_sha256',{_sql_string(hashes)}::jsonb,'counts',{_sql_string(json.dumps(plan.counts, sort_keys=True))}::jsonb,'historical_return_coverage_through',{_sql_string(plan.returns_last_month)}) AS backfill_evidence;
-- Pointer moved: refresh the *_mat mirrors the Light app reads. Deliberately
-- AFTER the COMMIT and the evidence row (CONCURRENTLY refuses transaction
-- blocks, and a refresh failure must not mask a finalized backfill). Snapshot
-- last: the app's solvability probe needs all legs of a month, so out-of-order
-- mirrors can only under-report the newest month, never serve a partial one.
SET ROLE worker_writer;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rv_signal_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_returns_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rating_pit_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;
RESET ROLE;
"""


def _sql_json(value: Any) -> str:
    return _sql_string(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))


@dataclass(frozen=True)
class UnitRepairArtifacts:
    """Pinned v2 artifacts plus the provenance manifest that declares them."""

    directory: Path
    paths: dict[str, Path]
    sha256: dict[str, str]
    rows: dict[str, int]
    manifest: dict[str, Any]
    provenance: dict[str, Any]

    @classmethod
    def open(
        cls,
        directory: Path,
        *,
        expected_hashes: dict[str, str] | None = None,
        expected_counts: dict[str, int] | None = None,
        expected_provenance: dict[str, Any] | None = None,
    ) -> UnitRepairArtifacts:
        """Pin the four v2 parquets and the manifest before any plan or SQL."""
        expected = EXPECTED_SHA256_UNIT_REPAIR_V2 if expected_hashes is None else expected_hashes
        counts_expected = UNIT_REPAIR_EXPECTED_COUNTS if expected_counts is None else expected_counts
        provenance = copy.deepcopy(
            EXPECTED_EXPORT_PROVENANCE
            if expected_provenance is None
            else expected_provenance
        )
        if set(expected) != set(UNIT_REPAIR_ARTIFACT_SURFACES):
            raise ArtifactPinError("unit_repair_artifact_map_not_four_surface")
        manifest_path = directory / "manifest.json"
        if not manifest_path.is_file():
            raise ArtifactPinError("unit_repair_manifest_unavailable")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ArtifactPinError("unit_repair_manifest_unreadable") from exc
        if not isinstance(manifest, dict):
            raise ArtifactPinError("unit_repair_manifest_unreadable")
        if manifest.get("unit_repair_contract") != UNIT_REPAIR_CONTRACT:
            raise PlanError("unit_repair_contract_mismatch")
        if manifest.get("unit") != "usd":
            raise PlanError("unit_repair_unit_mismatch")
        if manifest.get("scale") != UNIT_REPAIR_SCALE:
            raise PlanError("unit_repair_scale_mismatch")
        if manifest.get("source_pointer") != provenance["source_pointer"]:
            raise PlanError("unit_repair_source_pointer_mismatch")
        predicate = str(manifest.get("predicate", "")).replace(" ", "")
        if predicate not in {"price_source='osbap'", "price_source=='osbap'"}:
            raise PlanError("unit_repair_predicate_mismatch")
        input_state = manifest.get("input_state")
        if not isinstance(input_state, dict) or input_state.get("unit_repair_applied") is not True:
            raise PlanError("unit_repair_artifacts_not_repaired")
        manifest_map = manifest.get("artifact_sha256")
        if not isinstance(manifest_map, dict):
            raise ArtifactPinError("unit_repair_manifest_artifact_sha256_absent")
        if set(manifest_map) != set(expected):
            raise ArtifactPinError("unit_repair_manifest_artifact_sha256_key_mismatch")
        export = manifest.get("export")
        if not isinstance(export, dict):
            raise ArtifactPinError("unit_repair_manifest_export_absent")
        if export.get("manifest_sha256") != provenance["export_manifest_sha256"]:
            raise PlanError("unit_repair_export_manifest_sha256_mismatch")
        if export.get("export_datetime_utc") != provenance["export_datetime_utc"]:
            raise PlanError("unit_repair_export_datetime_mismatch")
        export_files = {
            entry.get("surface"): entry
            for entry in export.get("files", [])
            if isinstance(entry, dict)
        }
        for surface, digest in provenance["export_files"].items():
            if (export_files.get(surface) or {}).get("sha256") != digest:
                raise PlanError(f"unit_repair_export_file_sha256_mismatch:{surface}")
        artifacts_block = manifest.get("artifacts")
        paths: dict[str, Path] = {}
        actual: dict[str, str] = {}
        rows: dict[str, int] = {}
        for filename, expected_digest in expected.items():
            path = directory / filename
            if not path.is_file():
                raise ArtifactPinError(f"artifact_unavailable:{filename}")
            digest = _sha256(path)
            if digest != expected_digest:
                raise ArtifactPinError(f"artifact_sha256_mismatch:{filename}")
            if manifest_map.get(filename) != digest:
                raise ArtifactPinError(f"unit_repair_manifest_artifact_sha256_mismatch:{filename}")
            try:
                parquet = pq.ParquetFile(path)
                columns = set(parquet.schema_arrow.names)
                row_count = int(parquet.metadata.num_rows)
            except Exception as exc:  # pragma: no cover - pyarrow gives format-specific detail
                raise ArtifactPinError(f"unreadable_parquet:{filename}") from exc
            missing = sorted(UNIT_REPAIR_REQUIRED_COLUMNS[filename] - columns)
            if missing:
                raise ArtifactPinError(f"missing_required_columns:{filename}:{','.join(missing)}")
            surface = UNIT_REPAIR_ARTIFACT_SURFACES[filename]
            manifest_rows = None
            if isinstance(artifacts_block, dict) and isinstance(artifacts_block.get(filename), dict):
                manifest_rows = artifacts_block[filename].get("rows")
            if manifest_rows is not None and int(manifest_rows) != row_count:
                raise ArtifactPinError(f"unit_repair_manifest_rows_mismatch:{filename}")
            if row_count != counts_expected[surface]:
                raise ArtifactPinError(f"unit_repair_rows_mismatch:{filename}")
            paths[filename] = path
            actual[filename] = digest
            rows[surface] = row_count
        return cls(
            directory=directory,
            paths=paths,
            sha256=actual,
            rows=rows,
            manifest=manifest,
            provenance=provenance,
        )

    def path(self, filename: str) -> str:
        return self.paths[filename].as_posix()


@dataclass(frozen=True)
class UnitRepairPlan:
    publication_id: str
    input_fingerprint: str
    from_head_publication_id: str
    root_base_publication_id: str
    config_hash: str
    first_month: str
    last_closed_month: str
    open_month: str
    counts: dict[str, int]
    source_sha256: dict[str, str]
    export_provenance: dict[str, Any]
    per_year: tuple[dict[str, Any], ...]
    per_year_digest: str
    contract: str = UNIT_REPAIR_CONTRACT
    code_revision: str = UNIT_REPAIR_CODE_REVISION
    scale: int = UNIT_REPAIR_SCALE
    predicate: str = UNIT_REPAIR_PREDICATE

    def evidence(self) -> dict[str, Any]:
        return {
            "publication_id": self.publication_id,
            "input_fingerprint": self.input_fingerprint,
            "contract": self.contract,
            "code_revision": self.code_revision,
            "from_head_publication_id": self.from_head_publication_id,
            "root_base_publication_id": self.root_base_publication_id,
            "config_hash": self.config_hash,
            "first_month": self.first_month,
            "last_closed_month": self.last_closed_month,
            "open_month": self.open_month,
            "scale": self.scale,
            "predicate": self.predicate,
            "affected_surfaces": list(UNIT_REPAIR_AFFECTED_SURFACES),
            "counts": dict(sorted(self.counts.items())),
            "artifact_sha256": dict(sorted(self.source_sha256.items())),
            "export_provenance": copy.deepcopy(self.export_provenance),
            "per_year": [dict(item) for item in self.per_year],
            "per_year_volume_digest": self.per_year_digest,
        }


def _unit_repair_artifact_year_aggregates(artifacts: UnitRepairArtifacts) -> list[dict[str, Any]]:
    """Per-year artifact aggregates for months <= the repair cutoff.

    Volume surfaces carry row/NULL/sum aggregates (the sum cast to
    DECIMAL(38,6) in DuckDB); the other surfaces carry row counts only.
    """
    conn, state = _connect()
    try:
        aggregates: list[dict[str, Any]] = []
        for filename, surface in sorted(UNIT_REPAIR_ARTIFACT_SURFACES.items()):
            path = artifacts.path(filename)
            if surface in UNIT_REPAIR_AFFECTED_SURFACES:
                raw = conn.execute(
                    "SELECT extract(year FROM CAST(month AS DATE))::int, count(*), "
                    "count(*) FILTER (WHERE dollar_volume IS NULL), "
                    "CAST(SUM(CAST(dollar_volume AS DECIMAL(38,6))) AS DECIMAL(38,6)) "
                    "FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) "
                    "GROUP BY 1 ORDER BY 1",
                    [path, UNIT_REPAIR_ARTIFACT_CUTOFF],
                ).fetchall()
                for year, row_count, null_count, total in raw:
                    aggregates.append({
                        "surface": surface,
                        "year": int(year),
                        "rows": int(row_count),
                        "nulls": int(null_count),
                        "sum_dollar_volume": None if total is None else str(total),
                    })
            else:
                raw = conn.execute(
                    "SELECT extract(year FROM CAST(month AS DATE))::int, count(*) "
                    "FROM read_parquet(?) WHERE CAST(month AS DATE) <= CAST(? AS DATE) "
                    "GROUP BY 1 ORDER BY 1",
                    [path, UNIT_REPAIR_ARTIFACT_CUTOFF],
                ).fetchall()
                for year, row_count in raw:
                    aggregates.append({
                        "surface": surface,
                        "year": int(year),
                        "rows": int(row_count),
                        "nulls": None,
                        "sum_dollar_volume": None,
                    })
        return aggregates
    finally:
        conn.close()
        state.cleanup()


def _gate_unit_repair_artifact_scale(artifacts: UnitRepairArtifacts) -> None:
    """Refuse artifacts that are not the declared post-repair unit basis.

    The lower band catches a pre-repair input; the upper band catches an input
    that already received the predicate a second time (double conversion).
    """
    conn, state = _connect()
    try:
        row = conn.execute(
            "SELECT count(*), quantile_cont(dollar_volume, 0.5) FROM read_parquet(?) "
            "WHERE price_source = 'osbap' AND dollar_volume IS NOT NULL AND dollar_volume > 0",
            [artifacts.path("bond_panel_live.parquet")],
        ).fetchone()
    finally:
        conn.close()
        state.cleanup()
    observed = int(row[0])
    median = None if row[1] is None else float(row[1])
    if observed == 0:
        raise PlanError("unit_repair_artifact_osbap_rows_absent")
    low, high = UNIT_REPAIR_USD_BAND
    if median is None or not low <= median <= high:
        raise PlanError("unit_repair_artifact_scale_out_of_band")


def _unit_repair_fingerprint_payload(plan: UnitRepairPlan) -> dict[str, Any]:
    return {
        "contract": plan.contract,
        "from_head_publication_id": plan.from_head_publication_id,
        "root_base_publication_id": plan.root_base_publication_id,
        "config_hash": plan.config_hash,
        "scale": plan.scale,
        "predicate": plan.predicate,
        "affected_surfaces": list(UNIT_REPAIR_AFFECTED_SURFACES),
        "artifact_sha256": dict(sorted(plan.source_sha256.items())),
        "expected_counts": dict(sorted(plan.counts.items())),
        "per_year_volume_digest": plan.per_year_digest,
        "export_provenance": plan.export_provenance,
    }


def _validate_unit_repair_plan(plan: UnitRepairPlan) -> None:
    """Fail closed unless the plan matches the frozen repair authorization."""
    fingerprint = _canonical_digest(_unit_repair_fingerprint_payload(plan))
    publication_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCT}:unit-repair:{fingerprint}"))
    per_year_digest = _canonical_digest({"per_year_volume": list(plan.per_year)})
    if (
        plan.input_fingerprint != fingerprint
        or plan.input_fingerprint != UNIT_REPAIR_EXPECTED_INPUT_FINGERPRINT
        or plan.publication_id != publication_id
        or plan.publication_id != UNIT_REPAIR_EXPECTED_PUBLICATION_ID
        or plan.per_year_digest != per_year_digest
        or plan.per_year_digest != UNIT_REPAIR_EXPECTED_PER_YEAR_DIGEST
        or plan.contract != UNIT_REPAIR_CONTRACT
        or plan.code_revision != UNIT_REPAIR_CODE_REVISION
        or plan.from_head_publication_id != UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID
        or plan.root_base_publication_id != UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID
        or plan.config_hash != UNIT_REPAIR_CONFIG_HASH
        or plan.first_month != UNIT_REPAIR_EXPECTED_WINDOW["first_month"]
        or plan.last_closed_month != UNIT_REPAIR_EXPECTED_WINDOW["last_closed_month"]
        or plan.open_month != UNIT_REPAIR_EXPECTED_WINDOW["open_month"]
        or plan.counts != UNIT_REPAIR_EXPECTED_COUNTS
        or plan.source_sha256 != EXPECTED_SHA256_UNIT_REPAIR_V2
        or plan.export_provenance != EXPECTED_EXPORT_PROVENANCE
        or plan.scale != UNIT_REPAIR_SCALE
        or plan.predicate != UNIT_REPAIR_PREDICATE
    ):
        raise PlanError("unit_repair_plan_not_authorized")


def build_unit_repair_plan(
    artifacts: UnitRepairArtifacts, *, from_head_publication_id: str,
) -> UnitRepairPlan:
    """Plan the unit-repair child of the pinned validated head."""
    if from_head_publication_id != UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID:
        raise PlanError("unit_repair_from_head_not_authorized")
    _gate_unit_repair_artifact_scale(artifacts)
    per_year = tuple(_unit_repair_artifact_year_aggregates(artifacts))
    per_year_digest = _canonical_digest({"per_year_volume": list(per_year)})
    counts = dict(sorted(artifacts.rows.items()))
    source_sha256 = dict(sorted(artifacts.sha256.items()))
    plan = UnitRepairPlan(
        publication_id="",
        input_fingerprint="",
        from_head_publication_id=from_head_publication_id,
        root_base_publication_id=UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID,
        config_hash=UNIT_REPAIR_CONFIG_HASH,
        first_month=UNIT_REPAIR_EXPECTED_WINDOW["first_month"],
        last_closed_month=UNIT_REPAIR_EXPECTED_WINDOW["last_closed_month"],
        open_month=UNIT_REPAIR_EXPECTED_WINDOW["open_month"],
        counts=counts,
        source_sha256=source_sha256,
        export_provenance=copy.deepcopy(artifacts.provenance),
        per_year=per_year,
        per_year_digest=per_year_digest,
    )
    fingerprint = _canonical_digest(_unit_repair_fingerprint_payload(plan))
    publication_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCT}:unit-repair:{fingerprint}"))
    resolved = replace(plan, input_fingerprint=fingerprint, publication_id=publication_id)
    _validate_unit_repair_plan(resolved)
    return resolved


def _unit_repair_marker(plan: UnitRepairPlan) -> dict[str, Any]:
    return {
        "contract": plan.contract,
        "from_head_publication_id": plan.from_head_publication_id,
        "root_base_publication_id": plan.root_base_publication_id,
        "scale": plan.scale,
        "predicate": plan.predicate,
        "affected_surfaces": list(UNIT_REPAIR_AFFECTED_SURFACES),
        "authorized_code_revision": plan.code_revision,
        "expected_counts": dict(sorted(plan.counts.items())),
        "artifact_sha256_v2": dict(sorted(plan.source_sha256.items())),
        "export_provenance": plan.export_provenance,
        "per_year_volume_digest": plan.per_year_digest,
    }


_UNIT_REPAIR_IDENTITY_COLUMNS = ("distribution_rule", "reference_cusip9", "distribution_decision_id")
_UNIT_REPAIR_MARKER_COLUMNS = ("source_lineage", "payload")


def _unit_repair_expected_volume_expression(alias: str = "source") -> str:
    return (
        f"CASE WHEN {alias}.price_source = 'osbap' "
        f"THEN {alias}.dollar_volume * {UNIT_REPAIR_SCALE} "
        f"ELSE {alias}.dollar_volume END"
    )


def _unit_repair_copy_expressions(plan: UnitRepairPlan, surface: Surface) -> list[tuple[str, str]]:
    marker = _sql_json(_unit_repair_marker(plan))
    items: list[tuple[str, str]] = []
    for column in _COPY_COLUMNS[surface]:
        if column == "publication_id":
            items.append((column, f"{_sql_string(plan.publication_id)}::uuid"))
        elif column == "distribution_rule":
            items.append((column, "COALESCE(source.distribution_rule, 'rule_144a')"))
        elif column == "reference_cusip9":
            items.append((column, "COALESCE(source.reference_cusip9, source.cusip_id)"))
        elif column == "distribution_decision_id":
            items.append((
                column,
                "CASE WHEN source.distribution_rule IS NULL THEN NULL ELSE source.distribution_decision_id END",
            ))
        elif column == "dollar_volume" and surface in UNIT_REPAIR_AFFECTED_SURFACES:
            items.append((
                column,
                _unit_repair_expected_volume_expression(),
            ))
        elif column in _UNIT_REPAIR_MARKER_COLUMNS and surface in UNIT_REPAIR_AFFECTED_SURFACES:
            items.append((
                column,
                f"source.{column} || jsonb_build_object('unit_repair', {marker}::jsonb)",
            ))
        else:
            items.append((column, f"source.{column}"))
    return items


def _unit_repair_verbatim_columns(surface: Surface) -> tuple[str, ...]:
    """Data columns that must equal the current view row exactly.

    Exclusions from the row-wise comparison (each is different by design):
    - ``publication_id``: C's identity column; the view row carries H's id, so
      comparing it would trip the gate on every row.  It is pinned separately by
      the prepared-child check and the INSERT expression.
    - ``distribution_rule`` / ``reference_cusip9`` / ``distribution_decision_id``:
      rewritten by the dual-series identity fill and validated by the finalize
      identity gates.
    - volume surfaces only: ``dollar_volume`` (scaled by the declared predicate),
      ``source_lineage`` and ``payload`` (unioned with the unit-repair marker;
      the finalize containment gate checks the marker instead).
    ``month`` and ``cusip_id`` stay in the comparison: they are the join keys,
    so they are equal by construction and the duplicate check is harmless.
    """
    excluded = {"publication_id", *_UNIT_REPAIR_IDENTITY_COLUMNS}
    if surface in UNIT_REPAIR_AFFECTED_SURFACES:
        excluded |= {"dollar_volume", *_UNIT_REPAIR_MARKER_COLUMNS}
    return tuple(column for column in _COPY_COLUMNS[surface] if column not in excluded)


def _unit_repair_child_identity_check(plan: UnitRepairPlan) -> str:
    head = _sql_string(plan.from_head_publication_id)
    child = _sql_string(plan.publication_id)
    return f"""EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = {child}::uuid
          AND candidate.publication_status IN ('prepared', 'validated')
          AND candidate.parent_publication_id = {head}::uuid
          AND candidate.config_hash = {_sql_string(plan.config_hash)}
          AND candidate.input_fingerprint = {_sql_string(plan.input_fingerprint)}
          AND candidate.code_revision = {_sql_string(plan.code_revision)}
          AND candidate.gate_evidence @> jsonb_build_object(
              'unit_repair',
              jsonb_build_object(
                  'contract', {_sql_string(plan.contract)},
                  'from_head_publication_id', {head}
              )
          )
    )"""


def render_unit_repair_prepare_sql(plan: UnitRepairPlan) -> str:
    """Create or attest the deterministic prepared unit-repair child; never move the pointer."""
    _validate_unit_repair_plan(plan)
    head = _sql_string(plan.from_head_publication_id)
    child = _sql_string(plan.publication_id)
    marker = _sql_json(_unit_repair_marker(plan))
    hashes = _sql_json(dict(sorted(plan.source_sha256.items())))
    counts = plan.counts
    window = UNIT_REPAIR_EXPECTED_WINDOW
    frozen_root = _sql_json(dict(sorted(EXPECTED_SHA256.items())))
    count_checks = "\n".join(
        f"        IF (SELECT count(*) FROM bond_panel_current_{surface}_v1) <> {counts[surface]} THEN RAISE EXCEPTION 'unit repair prepare requires the pinned {surface} count'; END IF;"
        for surface in SURFACES
    )
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
DO $unit_repair_prepare$
BEGIN
    IF (SELECT publication_id FROM bond_panel_app_pointer WHERE product = {_sql_string(PRODUCT)}) IS DISTINCT FROM {head}::uuid THEN
        RAISE EXCEPTION 'unit repair prepare requires the expected head pointer';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications head
        WHERE head.publication_id = {head}::uuid
          AND head.publication_status = 'validated'
          AND head.config_hash = {_sql_string(UNIT_REPAIR_CONFIG_HASH)}
          AND head.first_month = {_sql_string(window['first_month'])}::date
          AND head.last_closed_month = {_sql_string(window['last_closed_month'])}::date
          AND head.open_month = {_sql_string(window['open_month'])}::date
    ) THEN RAISE EXCEPTION 'unit repair prepare requires the pinned head window'; END IF;
    IF NOT EXISTS (
        WITH RECURSIVE ancestry(publication_id, parent_publication_id, path) AS (
            SELECT p.publication_id, p.parent_publication_id, ARRAY[p.publication_id]
            FROM bond_panel_publications p
            WHERE p.publication_id = {head}::uuid
            UNION ALL
            SELECT p.publication_id, p.parent_publication_id, a.path || p.publication_id
            FROM bond_panel_publications p JOIN ancestry a ON p.publication_id = a.parent_publication_id
            WHERE NOT p.publication_id = ANY(a.path)
        )
        SELECT 1
        FROM ancestry a JOIN bond_panel_publications root ON root.publication_id = a.publication_id
        WHERE a.publication_id = {_sql_string(plan.root_base_publication_id)}::uuid
          AND a.parent_publication_id IS NULL
          AND root.publication_status = 'validated'
          AND root.config_hash = {_sql_string(CONFIG_HASH)}
          AND root.code_revision = {_sql_string(REPAIR_CODE_REVISION)}
          AND root.source_lineage->'source_sha256' = {frozen_root}::jsonb
    ) THEN RAISE EXCEPTION 'unit repair prepare requires the frozen root provenance'; END IF;
    IF EXISTS (
        SELECT 1 FROM bond_panel_publications prior
        WHERE prior.publication_status = 'validated'
          AND prior.gate_evidence @> jsonb_build_object(
              'unit_repair',
              jsonb_build_object(
                  'contract', {_sql_string(plan.contract)},
                  'from_head_publication_id', {head}
              )
          )
    ) THEN RAISE EXCEPTION 'unit repair child already validated for this head'; END IF;
{count_checks}
END
$unit_repair_prepare$;
INSERT INTO bond_panel_publications (publication_id, parent_publication_id, publication_status, config_hash, input_fingerprint, code_revision, first_month, last_closed_month, open_month, snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows, source_lineage, gate_evidence)
SELECT {child}::uuid, {head}::uuid, 'prepared', {_sql_string(plan.config_hash)}, {_sql_string(plan.input_fingerprint)}, {_sql_string(plan.code_revision)}, {_sql_string(window['first_month'])}::date, {_sql_string(window['last_closed_month'])}::date, {_sql_string(window['open_month'])}::date, {counts['snapshot']}, {counts['rv_signal']}, {counts['returns']}, {counts['rating_pit']},
       head.source_lineage || jsonb_build_object('source_sha256', {hashes}::jsonb, 'unit_repair', {marker}::jsonb),
       jsonb_build_object('unit_repair', {marker}::jsonb)
FROM bond_panel_publications head
WHERE head.publication_id = {head}::uuid
ON CONFLICT (publication_id) DO NOTHING;
DO $unit_repair_prepared$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = {child}::uuid
          AND candidate.parent_publication_id = {head}::uuid
          AND candidate.publication_status IN ('prepared', 'validated')
          AND candidate.config_hash = {_sql_string(plan.config_hash)}
          AND candidate.input_fingerprint = {_sql_string(plan.input_fingerprint)}
          AND candidate.code_revision = {_sql_string(plan.code_revision)}
          AND candidate.first_month = {_sql_string(window['first_month'])}::date
          AND candidate.last_closed_month = {_sql_string(window['last_closed_month'])}::date
          AND candidate.open_month = {_sql_string(window['open_month'])}::date
          AND candidate.snapshot_rows = {counts['snapshot']}
          AND candidate.rv_signal_rows = {counts['rv_signal']}
          AND candidate.returns_rows = {counts['returns']}
          AND candidate.ratings_pit_rows = {counts['rating_pit']}
          AND candidate.source_lineage @> jsonb_build_object('source_sha256', {hashes}::jsonb, 'unit_repair', {marker}::jsonb)
          AND candidate.gate_evidence @> jsonb_build_object('unit_repair', {marker}::jsonb)
    ) THEN RAISE EXCEPTION 'non-identical or non-resumable unit-repair publication'; END IF;
END
$unit_repair_prepared$;
COMMIT;
SELECT jsonb_build_object('publication_id', {child}, 'phase', 'prepared', 'contract', {_sql_string(plan.contract)}, 'from_head_publication_id', {head}, 'input_fingerprint', {_sql_string(plan.input_fingerprint)}, 'counts', {_sql_string(json.dumps(counts, sort_keys=True))}::jsonb) AS unit_repair_evidence;
"""


def render_unit_repair_copy_sql(plan: UnitRepairPlan, surface: Surface) -> str:
    """Copy one current view into the prepared child inside PostgreSQL."""
    if surface not in SURFACES:
        raise ValueError(f"unknown_surface:{surface}")
    _validate_unit_repair_plan(plan)
    head = _sql_string(plan.from_head_publication_id)
    child = _sql_string(plan.publication_id)
    marker = _sql_json(_unit_repair_marker(plan))
    table = _TABLES[surface]
    view = f"bond_panel_current_{surface}_v1"
    items = _unit_repair_copy_expressions(plan, surface)
    target_columns = ", ".join(column for column, _expression in items)
    source_expressions = ", ".join(expression for _column, expression in items)
    verbatim_columns = _unit_repair_verbatim_columns(surface)
    candidate_row = ", ".join(f"candidate.{column}" for column in verbatim_columns)
    source_row = ", ".join(f"source.{column}" for column in verbatim_columns)
    gates = [
        f"""    IF (SELECT count(*) FROM {table} WHERE publication_id = {child}::uuid) <> {plan.counts[surface]} THEN
        RAISE EXCEPTION 'unit repair copy count mismatch:{surface}';
    END IF;""",
    ]
    if verbatim_columns:
        gates.append(
            f"""    IF EXISTS (
        SELECT 1 FROM {table} candidate
        JOIN {view} source USING (month, cusip_id)
        WHERE candidate.publication_id = {child}::uuid
          AND ROW({candidate_row}) IS DISTINCT FROM ROW({source_row})
    ) THEN RAISE EXCEPTION 'unit repair verbatim conflict:{surface}'; END IF;"""
        )
    if surface in UNIT_REPAIR_AFFECTED_SURFACES:
        expected_volume = _unit_repair_expected_volume_expression()
        gates.append(
            f"""    IF EXISTS (
        SELECT 1 FROM {table} candidate
        JOIN {view} source USING (month, cusip_id)
        WHERE candidate.publication_id = {child}::uuid
          AND (
              NOT candidate.source_lineage @> source.source_lineage
              OR NOT candidate.payload @> source.payload
              OR NOT candidate.source_lineage @> jsonb_build_object('unit_repair', {marker}::jsonb)
              OR NOT candidate.payload @> jsonb_build_object('unit_repair', {marker}::jsonb)
          )
    ) THEN RAISE EXCEPTION 'unit repair marker conflict:{surface}'; END IF;"""
        )
        gates.append(
            f"""    IF EXISTS (
        SELECT 1
        FROM (
            SELECT month, cusip_id, dollar_volume
            FROM {table} WHERE publication_id = {child}::uuid
        ) candidate
        FULL JOIN (
            SELECT month, cusip_id, price_source, dollar_volume FROM {view}
        ) source USING (month, cusip_id)
        WHERE candidate.month IS NULL OR source.month IS NULL
           OR candidate.dollar_volume IS DISTINCT FROM {expected_volume}
        LIMIT 1
    ) THEN RAISE EXCEPTION 'unit repair per-row volume mismatch:{surface}'; END IF;"""
        )
        gates.append(
            f"""    IF (SELECT count(*) FROM {table} candidate WHERE candidate.publication_id = {child}::uuid AND candidate.dollar_volume IS NULL) <> (SELECT count(*) FROM {view} source WHERE source.dollar_volume IS NULL) THEN
        RAISE EXCEPTION 'unit repair null volume mismatch:{surface}';
    END IF;"""
        )
        gates.append(
            f"""    IF EXISTS (
        SELECT 1
        FROM (
            SELECT extract(year FROM candidate.month)::int AS yr, sum(candidate.dollar_volume) AS osbap_sum
            FROM {table} candidate
            WHERE candidate.publication_id = {child}::uuid AND candidate.price_source = 'osbap'
            GROUP BY 1
        ) candidate_years
        FULL JOIN (
            SELECT extract(year FROM source.month)::int AS yr, sum(source.dollar_volume) AS osbap_sum
            FROM {view} source
            WHERE source.price_source = 'osbap'
            GROUP BY 1
        ) source_years USING (yr)
        WHERE candidate_years.osbap_sum IS DISTINCT FROM source_years.osbap_sum * {UNIT_REPAIR_SCALE}
    ) THEN RAISE EXCEPTION 'unit repair per-year osbap sum mismatch:{surface}'; END IF;"""
        )
    gate_block = "\n".join(gates)
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
DO $unit_repair_copy_order$
BEGIN
    IF (SELECT publication_id FROM bond_panel_app_pointer WHERE product = {_sql_string(PRODUCT)}) IS DISTINCT FROM {head}::uuid THEN
        RAISE EXCEPTION 'unit repair copy requires the expected head pointer';
    END IF;
    IF NOT {_unit_repair_child_identity_check(plan)} THEN
        RAISE EXCEPTION 'unit repair copy requires the prepared unit-repair child';
    END IF;
END
$unit_repair_copy_order$;
LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO {table} ({target_columns})
SELECT {source_expressions}
FROM {view} source
WHERE EXISTS (
    SELECT 1 FROM bond_panel_publications candidate
    WHERE candidate.publication_id = {child}::uuid
      AND candidate.publication_status = 'prepared'
)
ON CONFLICT (publication_id, month, cusip_id) DO NOTHING;
DO $unit_repair_copy_gates$
BEGIN
{gate_block}
END
$unit_repair_copy_gates$;
COMMIT;
"""


def _unit_repair_expected_aggregates_json(plan: UnitRepairPlan) -> str:
    payload = [
        {
            "surface": item["surface"],
            "year": item["year"],
            "rows": item["rows"],
            "nulls": item["nulls"],
            "sum_dollar_volume": item["sum_dollar_volume"],
        }
        for item in plan.per_year
    ]
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _unit_repair_returns_first_month() -> str:
    first = date.fromisoformat(UNIT_REPAIR_EXPECTED_WINDOW["first_month"])
    return date(first.year + (first.month == 12), first.month % 12 + 1, 1).isoformat()


def render_unit_repair_finalize_sql(plan: UnitRepairPlan) -> str:
    """Gate the loaded child against the artifact aggregates, then validate and CAS.

    Performance shape (plan amendment §14): one timed transaction whose single DO
    fills small per-surface monthly summaries, runs every gate from those
    summaries or from month-bounded, publication-pinned anti-joins, then performs
    the prepared->validated transition and the pointer CAS.  No EXCEPT set
    operations, no repeated all-history scans and no trusted fast path: the gates
    are equivalent to the pre-amendment SQL, and the pointer trigger keeps
    re-checking identity with the same rules after the CAS.
    """
    _validate_unit_repair_plan(plan)
    head = _sql_string(plan.from_head_publication_id)
    child = _sql_string(plan.publication_id)
    marker = _sql_json(_unit_repair_marker(plan))
    hashes = _sql_json(dict(sorted(plan.source_sha256.items())))
    counts = plan.counts
    window = UNIT_REPAIR_EXPECTED_WINDOW
    aggregates = _sql_string(_unit_repair_expected_aggregates_json(plan))
    cutoff = _sql_string(UNIT_REPAIR_ARTIFACT_CUTOFF)
    returns_first = _unit_repair_returns_first_month()
    closed_months = len(_month_starts(returns_first, window["last_closed_month"]))
    counts_json = _sql_string(json.dumps(counts, sort_keys=True))
    invalid_identity = (
        "f.distribution_rule IS NULL"
        " OR f.reference_cusip9 IS NULL OR btrim(f.reference_cusip9) = ''"
        " OR (f.distribution_rule = 'rule_144a' AND"
        " (f.cusip_id <> f.reference_cusip9 OR f.distribution_decision_id IS NOT NULL))"
        " OR (f.distribution_rule = 'reg_s' AND nullif(f.distribution_decision_id, '') IS NULL)"
    )
    bootstrap_identity = (
        "f.distribution_rule = 'rule_144a' AND f.cusip_id = f.reference_cusip9"
        " AND f.distribution_decision_id IS NULL"
    )
    metadata = f"""          AND candidate.config_hash = {_sql_string(plan.config_hash)}
          AND candidate.input_fingerprint = {_sql_string(plan.input_fingerprint)}
          AND candidate.code_revision = {_sql_string(plan.code_revision)}
          AND candidate.first_month = {_sql_string(window['first_month'])}::date
          AND candidate.last_closed_month = {_sql_string(window['last_closed_month'])}::date
          AND candidate.open_month = {_sql_string(window['open_month'])}::date
          AND candidate.snapshot_rows = {counts['snapshot']}
          AND candidate.rv_signal_rows = {counts['rv_signal']}
          AND candidate.returns_rows = {counts['returns']}
          AND candidate.ratings_pit_rows = {counts['rating_pit']}
          AND candidate.source_lineage @> jsonb_build_object('source_sha256', {hashes}::jsonb, 'unit_repair', {marker}::jsonb)"""
    summaries: list[str] = []
    for surface in SURFACES:
        if surface in UNIT_REPAIR_AFFECTED_SURFACES:
            nulls_expr = "count(*) FILTER (WHERE f.dollar_volume IS NULL)"
            sum_expr = "sum(f.dollar_volume)"
        else:
            nulls_expr = "NULL::bigint"
            sum_expr = "NULL::numeric"
        summaries.append(
            f"""INSERT INTO pg_temp.unit_repair_month_stats (surface, month, rows, nulls, volume_sum, bad_identity, bootstrap)
SELECT {_sql_string(surface)}, f.month, count(*), {nulls_expr}, {sum_expr},
       bool_or({invalid_identity}),
       bool_or({bootstrap_identity})
FROM {_TABLES[surface]} f
WHERE f.publication_id = {child}::uuid
GROUP BY f.month;"""
        )
    summary_block = "\n".join(summaries)
    counts_blocks: list[str] = []
    for surface in SURFACES:
        returns_capture = (
            "\n    v_returns_min := v_min;\n    v_returns_max := v_max;"
            if surface == "returns"
            else ""
        )
        counts_blocks.append(
            f"""    SELECT coalesce(sum(rows), 0), min(month), max(month) INTO v_rows, v_min, v_max
    FROM pg_temp.unit_repair_month_stats WHERE surface = {_sql_string(surface)};
    IF v_rows <> {counts[surface]} THEN
        RAISE EXCEPTION 'unit repair final count mismatch:{surface}';
    END IF;{returns_capture}
    RAISE NOTICE 'unit repair finalize: {surface} rows=% min=% max=% elapsed_ms=%', v_rows, v_min, v_max, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);"""
        )
    counts_block = "\n".join(counts_blocks)
    identity_blocks: list[str] = []
    for surface in SURFACES:
        identity_blocks.append(
            f"""    IF EXISTS (
        SELECT 1 FROM pg_temp.unit_repair_month_stats
        WHERE surface = {_sql_string(surface)} AND bad_identity IS TRUE LIMIT 1
    ) THEN RAISE EXCEPTION 'unit repair identity coverage invalid'; END IF;"""
        )
    identity_block = "\n".join(identity_blocks)
    coverage_specs = (
        ("rv_signal", "snapshot", " AND s.eligibility_state = 'included'", "unit repair rv_signal coverage mismatch"),
        ("returns", "snapshot", "", "unit repair returns coverage mismatch"),
        ("snapshot", "rating_pit", "", "unit repair rating coverage mismatch"),
        ("rating_pit", "snapshot", "", "unit repair rating coverage mismatch"),
    )
    coverage_blocks: list[str] = []
    for forward_surface, probe_surface, probe_extra, message in coverage_specs:
        coverage_blocks.append(
            f"""        IF EXISTS (
            SELECT 1 FROM bond_panel_{forward_surface} f
            WHERE f.publication_id = {child}::uuid
              AND f.month = v_month
              AND NOT EXISTS (
                  SELECT 1 FROM bond_panel_{probe_surface} s
                  WHERE s.publication_id = {child}::uuid
                    AND s.month = v_month AND s.month = f.month AND s.cusip_id = f.cusip_id{probe_extra}
              ) LIMIT 1
        ) THEN RAISE EXCEPTION '{message}'; END IF;"""
        )
    for surface in ("rv_signal", "returns", "rating_pit"):
        coverage_blocks.append(
            f"""        IF EXISTS (
            SELECT 1 FROM bond_panel_{surface} f
            JOIN bond_panel_snapshot s
              ON s.month = f.month AND s.cusip_id = f.cusip_id
             AND s.publication_id = {child}::uuid AND s.month = v_month
            WHERE f.publication_id = {child}::uuid
              AND f.month = v_month
              AND (f.distribution_rule, f.reference_cusip9, f.distribution_decision_id)
                  IS DISTINCT FROM (s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)
            LIMIT 1
        ) THEN RAISE EXCEPTION 'unit repair cross-surface identity mismatch:{surface}'; END IF;"""
        )
    for surface in UNIT_REPAIR_AFFECTED_SURFACES:
        expected_volume = _unit_repair_expected_volume_expression()
        source_projection = f"""SELECT DISTINCT ON (source_fact.month, source_fact.cusip_id)
                       source_fact.month, source_fact.cusip_id,
                       source_fact.price_source, source_fact.dollar_volume
                FROM {_TABLES[surface]} source_fact
                JOIN pg_temp.unit_repair_source_ancestry ancestry
                  USING (publication_id)
                WHERE source_fact.month = v_month
                ORDER BY source_fact.month, source_fact.cusip_id, ancestry.depth"""
        coverage_blocks.append(
            f"""        IF EXISTS (
            SELECT 1
            FROM (
                SELECT month, cusip_id, dollar_volume
                FROM {_TABLES[surface]}
                WHERE publication_id = {child}::uuid AND month = v_month
            ) candidate
            FULL JOIN (
                {source_projection}
            ) source USING (month, cusip_id)
            WHERE candidate.month IS NULL OR source.month IS NULL
               OR candidate.dollar_volume IS DISTINCT FROM {expected_volume}
            LIMIT 1
        ) THEN RAISE EXCEPTION 'unit repair per-row volume mismatch:{surface}'; END IF;"""
        )
    coverage_block = "\n".join(coverage_blocks)
    source_ancestry_block = f"""    CREATE TEMP TABLE unit_repair_source_ancestry (
        publication_id uuid PRIMARY KEY,
        depth integer NOT NULL
    ) ON COMMIT DROP;
    WITH RECURSIVE source_ancestry(publication_id, parent_publication_id, config_hash, depth, path) AS (
        SELECT p.publication_id, p.parent_publication_id, p.config_hash, 0,
               ARRAY[p.publication_id]
        FROM bond_panel_publications p
        WHERE p.publication_id = {head}::uuid
        UNION ALL
        SELECT p.publication_id, p.parent_publication_id, p.config_hash,
               ancestry.depth + 1, ancestry.path || p.publication_id
        FROM bond_panel_publications p
        JOIN source_ancestry ancestry
          ON p.publication_id = ancestry.parent_publication_id
        WHERE NOT p.publication_id = ANY(ancestry.path)
          AND (
              p.config_hash = ancestry.config_hash
              OR (
                  btrim(ancestry.config_hash::text) = {_sql_string(UNIT_REPAIR_CONFIG_HASH)}
                  AND btrim(p.config_hash::text) = {_sql_string(CONFIG_HASH)}
              )
          )
    )
    INSERT INTO pg_temp.unit_repair_source_ancestry (publication_id, depth)
    SELECT publication_id, depth FROM source_ancestry;
    IF NOT EXISTS (
        SELECT 1 FROM pg_temp.unit_repair_source_ancestry ancestry
        WHERE ancestry.publication_id = {head}::uuid AND ancestry.depth = 0
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_temp.unit_repair_source_ancestry ancestry
        JOIN bond_panel_publications root USING (publication_id)
        WHERE ancestry.publication_id = {_sql_string(plan.root_base_publication_id)}::uuid
          AND root.parent_publication_id IS NULL
          AND root.publication_status = 'validated'
          AND root.config_hash = {_sql_string(CONFIG_HASH)}
          AND root.code_revision = {_sql_string(REPAIR_CODE_REVISION)}
    ) THEN RAISE EXCEPTION 'unit repair source ancestry does not match the pinned H projection'; END IF;"""
    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
SET LOCAL lock_timeout = {_sql_string(UNIT_REPAIR_FINALIZE_LOCK_TIMEOUT)};
SET LOCAL statement_timeout = {_sql_string(UNIT_REPAIR_FINALIZE_STATEMENT_TIMEOUT)};
-- Freeze the four fact tables for the whole validation window: SHARE blocks
-- writers (INSERT/UPDATE/DELETE) on these tables and allows readers.  Fixed
-- order.  Requires an operator-confirmed quiet panel-write window; a 5s lock
-- timeout fails instead of queueing an extended incident.
LOCK TABLE bond_panel_snapshot, bond_panel_rv_signal, bond_panel_returns, bond_panel_rating_pit IN SHARE MODE;
DO $unit_repair_finalize$
DECLARE
    v_month date;
    v_rows bigint;
    v_min date;
    v_max date;
    v_returns_min date;
    v_returns_max date;
    v_months integer;
    v_checked integer := 0;
    v_mismatches integer;
    v_status_rows integer;
    v_cas_rows integer;
    v_started timestamptz := clock_timestamp();
BEGIN
    -- Row locks: the child publication row first, then the pointer row, so a
    -- concurrent status/CAS writer is serialized behind this transaction.
    PERFORM 1 FROM bond_panel_publications WHERE publication_id = {child}::uuid FOR UPDATE;
    PERFORM 1 FROM bond_panel_app_pointer WHERE product = {_sql_string(PRODUCT)} FOR UPDATE;
    -- Null-safe pointer preflight: missing or unrelated pointer fails immediately.
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_app_pointer
        WHERE product = {_sql_string(PRODUCT)} AND publication_id IN ({head}::uuid, {child}::uuid)
    ) THEN RAISE EXCEPTION 'unit repair finalize requires the expected head pointer or this child'; END IF;
    IF EXISTS (
        SELECT 1 FROM bond_panel_app_pointer
        WHERE product = {_sql_string(PRODUCT)} AND publication_id = {child}::uuid
    ) AND NOT EXISTS (
        SELECT 1 FROM bond_panel_publications
        WHERE publication_id = {child}::uuid AND publication_status = 'validated'
    ) THEN RAISE EXCEPTION 'unit repair finalize cannot point at an unvalidated child'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = {child}::uuid
          AND candidate.parent_publication_id = {head}::uuid
          AND candidate.publication_status IN ('prepared', 'validated')
{metadata}
    ) THEN RAISE EXCEPTION 'non-identical unit-repair publication finalization'; END IF;
{source_ancestry_block}
    -- Small monthly summaries: one grouped scan per surface, C only, actual
    -- dates only (unexpected dates included).  No payload JSON is stored.
    CREATE TEMP TABLE unit_repair_month_stats (
        surface text NOT NULL,
        month date NOT NULL,
        rows bigint NOT NULL,
        nulls bigint,
        volume_sum numeric,
        bad_identity boolean,
        bootstrap boolean,
        PRIMARY KEY (surface, month)
    ) ON COMMIT DROP;
{summary_block}
    RAISE NOTICE 'unit repair finalize: summaries filled months=% elapsed_ms=%', (SELECT count(*) FROM pg_temp.unit_repair_month_stats), round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
{counts_block}
    IF v_returns_min IS DISTINCT FROM {_sql_string(returns_first)}::date THEN
        RAISE EXCEPTION 'unit repair returns must start one month after the first snapshot month';
    END IF;
    IF v_returns_max IS DISTINCT FROM {_sql_string(window['last_closed_month'])}::date THEN
        RAISE EXCEPTION 'unit repair returns must end at the declared closed-month cutoff';
    END IF;
    -- Continuity equivalence: PK months are distinct first-of-month dates, so
    -- this count plus the pinned minimum is exactly the generate_series coverage.
    SELECT count(*) INTO v_months
    FROM pg_temp.unit_repair_month_stats
    WHERE surface = 'returns'
      AND month BETWEEN {_sql_string(returns_first)}::date AND {_sql_string(window['last_closed_month'])}::date
      AND month = date_trunc('month', month)::date;
    IF v_months <> {closed_months} THEN
        RAISE EXCEPTION 'unit repair returns history is not contiguous through the closed-month cutoff';
    END IF;
    RAISE NOTICE 'unit repair finalize: counts/continuity months=% elapsed_ms=%', v_months, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
{identity_block}
    IF NOT EXISTS (
        SELECT 1 FROM pg_temp.unit_repair_month_stats
        WHERE surface = 'snapshot' AND bootstrap IS TRUE LIMIT 1
    ) THEN RAISE EXCEPTION 'unit repair identity bootstrap missing'; END IF;
    RAISE NOTICE 'unit repair finalize: identity gates elapsed_ms=%', round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
    -- Coverage/cross-surface identity stays within C; volume equality compares
    -- C to the H-anchored served projection reconstructed above.
    FOR v_month IN SELECT DISTINCT month FROM pg_temp.unit_repair_month_stats ORDER BY month LOOP
{coverage_block}
        v_checked := v_checked + 1;
        IF v_checked % 25 = 0 THEN
            RAISE NOTICE 'unit repair finalize: coverage months=% elapsed_ms=%', v_checked, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
        END IF;
    END LOOP;
    RAISE NOTICE 'unit repair finalize: coverage months=% elapsed_ms=%', v_checked, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
    -- Artifact/DB aggregate gate: expected JSON comes unchanged from
    -- _unit_repair_expected_aggregates_json(plan) (duplicate keys fail on the PK).
    -- Actual annual totals sum the UNROUNDED monthly numeric partials and cast
    -- once; that is algebraically identical to the original direct annual cast.
    -- The artifact stores dollar_volume as float64 (per-row DECIMAL(38,6) cast +
    -- exact sum; measured worst-case 0.0012 USD/year), so sums are compared
    -- within {UNIT_REPAIR_SUM_TOLERANCE_USD} USD; row and NULL counts are exact.
    CREATE TEMP TABLE unit_repair_expected_year (
        surface text NOT NULL,
        year integer NOT NULL,
        rows bigint,
        nulls bigint,
        sum_dollar_volume numeric,
        PRIMARY KEY (surface, year)
    ) ON COMMIT DROP;
    INSERT INTO pg_temp.unit_repair_expected_year (surface, year, rows, nulls, sum_dollar_volume)
    SELECT surface, year, rows, nulls, sum_dollar_volume
    FROM jsonb_to_recordset({aggregates}::jsonb) AS expected(surface text, "year" int, rows bigint, "nulls" bigint, sum_dollar_volume numeric);
    CREATE TEMP TABLE unit_repair_actual_year (
        surface text NOT NULL,
        year integer NOT NULL,
        rows bigint,
        nulls bigint,
        sum_dollar_volume numeric,
        PRIMARY KEY (surface, year)
    ) ON COMMIT DROP;
    INSERT INTO pg_temp.unit_repair_actual_year (surface, year, rows, nulls, sum_dollar_volume)
    SELECT surface, extract(year FROM month)::int, sum(rows)::bigint, sum(nulls)::bigint,
           CAST(sum(volume_sum) AS numeric(38,6))
    FROM pg_temp.unit_repair_month_stats
    WHERE month <= {cutoff}::date
    GROUP BY surface, extract(year FROM month)::int;
    SELECT count(*) INTO v_mismatches
    FROM pg_temp.unit_repair_expected_year e
    FULL JOIN pg_temp.unit_repair_actual_year a USING (surface, year)
    WHERE e.rows IS DISTINCT FROM a.rows
       OR e.nulls IS DISTINCT FROM a.nulls
       OR (e.sum_dollar_volume IS NULL) <> (a.sum_dollar_volume IS NULL)
       OR (e.sum_dollar_volume IS NOT NULL AND abs(e.sum_dollar_volume - a.sum_dollar_volume) > {UNIT_REPAIR_SUM_TOLERANCE_USD});
    IF v_mismatches > 0 THEN RAISE EXCEPTION 'unit repair artifact/DB aggregate mismatch'; END IF;
    RAISE NOTICE 'unit repair finalize: annual artifact gate mismatches=% elapsed_ms=%', v_mismatches, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
    -- Status transition and CAS inside the same timed DO, after every gate.
    -- Replay: either exactly one prepared->validated transition, or an already
    -- validated identical child (no C->C update, no timestamp change).
    UPDATE bond_panel_publications
    SET publication_status = 'validated',
        validated_at = COALESCE(validated_at, now()),
        gate_evidence = gate_evidence || jsonb_build_object('validated_counts', {counts_json}::jsonb, 'unit_repair_validated', {marker}::jsonb)
    WHERE publication_id = {child}::uuid AND publication_status = 'prepared';
    GET DIAGNOSTICS v_status_rows = ROW_COUNT;
    IF v_status_rows > 1 THEN
        RAISE EXCEPTION 'unit repair finalize updated more than one publication row';
    END IF;
    IF v_status_rows = 0 AND NOT EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = {child}::uuid
          AND candidate.parent_publication_id = {head}::uuid
          AND candidate.publication_status = 'validated'
{metadata}
    ) THEN RAISE EXCEPTION 'unit repair finalize requires one prepared-to-validated transition or an identical validated child'; END IF;
    UPDATE bond_panel_app_pointer
    SET publication_id = {child}::uuid, changed_at = now()
    WHERE product = {_sql_string(PRODUCT)} AND publication_id = {head}::uuid;
    GET DIAGNOSTICS v_cas_rows = ROW_COUNT;
    IF v_cas_rows > 1 THEN
        RAISE EXCEPTION 'unit repair pointer compare-and-swap updated more than one row';
    END IF;
    IF v_cas_rows = 0 AND NOT EXISTS (
        SELECT 1
        FROM bond_panel_app_pointer pointer
        JOIN bond_panel_publications candidate ON candidate.publication_id = pointer.publication_id
        WHERE pointer.product = {_sql_string(PRODUCT)}
          AND pointer.publication_id = {child}::uuid
          AND candidate.publication_status = 'validated'
    ) THEN RAISE EXCEPTION 'unit repair pointer compare-and-swap lost'; END IF;
    RAISE NOTICE 'unit repair finalize: status_rows=% cas_rows=% elapsed_ms=%', v_status_rows, v_cas_rows, round(extract(epoch FROM clock_timestamp() - v_started) * 1000);
END
$unit_repair_finalize$;
COMMIT;
SELECT jsonb_build_object('publication_id', {child}, 'phase', 'validated_and_pointed', 'contract', {_sql_string(plan.contract)}, 'from_head_publication_id', {head}, 'counts', {counts_json}::jsonb) AS unit_repair_evidence;
-- Pointer moved: refresh the *_mat mirrors the Light app reads, deliberately
-- AFTER the COMMIT and in the same order as the frozen base finalize.  The
-- refresh phase gets its own explicit session-level timeout (SET LOCAL has
-- expired at COMMIT) and its failure cannot undo the CAS.
\\echo unit_repair_finalize_phase=refresh_start
SET ROLE worker_writer;
SET statement_timeout = {_sql_string(UNIT_REPAIR_FINALIZE_REFRESH_STATEMENT_TIMEOUT)};
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rv_signal_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_returns_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rating_pit_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;
RESET statement_timeout;
RESET ROLE;
\\echo unit_repair_finalize_phase=refresh_done
"""


def _artifact_directory_for_mode(
    artifact_dir: Path | None, *, unit_repair: bool
) -> Path:
    """Resolve the mode-specific artifact root without cross-mode fallback."""
    if not unit_repair:
        return DEFAULT_ARTIFACT_DIRECTORY if artifact_dir is None else artifact_dir
    selected = (
        UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY if artifact_dir is None else artifact_dir
    )
    if selected.resolve() != UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY.resolve():
        raise PlanError("unit_repair_artifact_directory_not_authorized")
    return UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path)
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
    mode.add_argument("--emit-unit-repair-copy", choices=SURFACES, help="unit-repair-only database-to-database copy of one current surface")
    mode.add_argument("--emit-finalize", action="store_true")
    parser.add_argument("--repair-from-publication-id", help="enable the one evidence-bound legacy root replacement; ordinary mode never repairs return coverage")
    parser.add_argument("--unit-repair-from-head", help="enable the T2.1 unit-repair child of the given validated head publication; artifacts come from --artifact-dir")
    parser.add_argument("--start-after", type=int, default=0)
    parser.add_argument("--limit", type=int, help="bounded row count for one --emit-batch transaction; choose an operator-safe size")
    args = parser.parse_args(argv)
    if args.emit_batch and args.limit is None and not args.unit_repair_from_head:
        parser.error("--limit is required with --emit-batch")
    if args.emit_schema:
        print(render_schema_sql(), end="")
        return 0
    try:
        if args.unit_repair_from_head:
            if args.repair_from_publication_id:
                raise PlanError("unit_repair_does_not_accept_repair_from_publication_id")
            if args.emit_batch:
                raise PlanError("unit_repair_does_not_accept_emit_batch")
            if args.cutoff != DEFAULT_CUTOFF:
                raise PlanError("unit_repair_does_not_accept_cutoff")
            if args.start_after != 0:
                raise PlanError("unit_repair_does_not_accept_start_after")
            artifact_dir = _artifact_directory_for_mode(
                args.artifact_dir, unit_repair=True
            )
            artifacts = UnitRepairArtifacts.open(artifact_dir)
            unit_plan = build_unit_repair_plan(artifacts, from_head_publication_id=args.unit_repair_from_head)
            if args.plan or args.evidence:
                print(json.dumps(unit_plan.evidence(), sort_keys=True))
            elif args.emit_prepare:
                print(render_unit_repair_prepare_sql(unit_plan), end="")
            elif args.emit_unit_repair_copy:
                print(render_unit_repair_copy_sql(unit_plan, args.emit_unit_repair_copy), end="")
            elif args.emit_finalize:
                print(render_unit_repair_finalize_sql(unit_plan), end="")
            else:
                raise PlanError("unit_repair_requires_plan_prepare_copy_or_finalize")
            return 0
        if args.emit_unit_repair_copy:
            raise PlanError("unit_repair_from_head_required")
        artifact_dir = _artifact_directory_for_mode(
            args.artifact_dir, unit_repair=False
        )
        artifacts = ArtifactSet.open(artifact_dir)
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

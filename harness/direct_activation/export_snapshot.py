"""Stage A executor: export the live delta snapshot for pre-activation validation.

Usage (from the repo root, DSN injected out-of-band, e.g. ``railway run``):

    python -m harness.direct_activation.export_snapshot

Exports, from the production datalake (read-only SELECTs only), the DELTA of the
signed candidate's inputs SINCE the certified pack v2 cut, as-of the pinned
validation date, and writes three files into ``input_snapshot/``:

- ``delta_eod_prices.json``     — sleeve prices from PRICE_OVERLAP_START..VALIDATION_AS_OF
  (the tail overlaps the pack so that composition re-validates prod has not
  restated recent bars);
- ``delta_macro_vintages.json`` — SEED-basket PIT vintages whose ``available_at``
  falls strictly after the pack cut and no later than VALIDATION_AS_OF + 1 day
  (legitimately EMPTY today: the basket's newest vintage predates the cut);
- ``snapshot_manifest.json``    — hash-pinned provenance of the two deltas.

Determinism: every parameter is pinned as a module constant, row content and
normalization are the certified pack conventions (``format_*_rows`` +
``normalize_number``/``normalize_date``), and no volatile timestamp is written —
re-exporting at the same as-of against unchanged prod yields byte-identical files.

Fail-loud gate at export time: the Phase 1 staleness criteria are recomputed over
the composed pack+delta (reusing ``live_validation``); a stale basket or missing
sleeve price raises and NOTHING is written.

Governance: Stage A exports and validates only — read-only over production; no DB
write of any kind; A5 stays blocked; governance untouched.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from harness.direct_activation import live_validation as _lv
from scripts.p1_export.export_p1_sources import (
    _as_of_end_utc,
    _execute_select,
    format_eod_price_rows,
    format_macro_vintage_rows,
    seed_series_ids,
)

# --- pinned parameters (deterministic re-export) ---------------------------
VALIDATION_AS_OF = _dt.date(2026, 7, 3)
PRICE_OVERLAP_START = _dt.date(2026, 6, 26)
PACK_CUT = _dt.date(2026, 6, 30)            # pack v2 manifest as_of
DIRECT_ACTIVATION_ID = "open_macro_v03_direct_activation_001"
PINNED_DB_SERVICE_ID = "t83f4np6x4"
SLEEVE_TICKERS = _lv.sleeve_mod.SLEEVE_TICKERS  # ("SPY","TLT","TIP","GLD","DBC","SHY")

SNAPSHOT = _lv.SNAPSHOT
PACK = _lv.PACK
PACK_SHA256_PIN = _lv.PACK_SHA256_PIN

SCHEMA_VERSION = 1

_EOD_PRICES_SQL = (
    "SELECT ticker, date, close, adj_close AS adjusted_close, volume\n"
    "FROM eod_prices\n"
    "WHERE ticker = ANY(%(tickers)s)\n"
    "  AND date >= %(overlap_start)s\n"
    "  AND date <= %(as_of)s\n"
    "ORDER BY ticker, date"
)

_MACRO_VINTAGE_SQL = (
    "SELECT series_id, observation_period, vintage_date, value, available_at,\n"
    "       revision_number, source, source_spec_version\n"
    "FROM macro_observation_vintage\n"
    "WHERE series_id = ANY(%(series_ids)s)\n"
    "  AND available_at > %(cut_exclusive)s\n"
    "  AND available_at <= %(as_of_upper)s\n"
    "ORDER BY series_id, observation_period, revision_number, available_at"
)


class SnapshotExportError(AssertionError):
    """An export invariant did not hold; no snapshot file may be written."""


def _require(condition: bool, note: str) -> None:
    if not condition:
        raise SnapshotExportError(note)


def _vintage_upper_bound_utc() -> str:
    """``available_at`` upper bound: VALIDATION_AS_OF + 1 day at UTC midnight."""
    return _dt.datetime.combine(
        VALIDATION_AS_OF + _dt.timedelta(days=1), _dt.time(0, 0),
        tzinfo=_dt.timezone.utc,
    ).isoformat()


def _delta_bytes(rows: list[dict[str, Any]]) -> bytes:
    """Serialize a delta exactly as the committed snapshot files: sorted keys,
    1-space indent, UTF-8, LF newline, trailing newline (``live_validation`` style)."""
    return (json.dumps(rows, sort_keys=True, indent=1, ensure_ascii=False)
            + "\n").encode("utf-8")


def _query_parameters() -> dict[str, Any]:
    return {
        "validation_as_of": VALIDATION_AS_OF.isoformat(),
        "price_overlap_start": PRICE_OVERLAP_START.isoformat(),
        "pack_cut": PACK_CUT.isoformat(),
        "vintage_available_at_lower_exclusive": _as_of_end_utc(PACK_CUT),
        "vintage_available_at_upper_inclusive": _vintage_upper_bound_utc(),
        "series_ids": list(seed_series_ids()),
        "tickers": list(SLEEVE_TICKERS),
    }


def fetch_delta(conn: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(price_rows, vintage_rows)`` deltas from production (read-only).

    ``conn`` is any connection exposing ``cursor()`` (psycopg in the CLI path, a
    fake in tests). Only SELECT statements are ever issued; the database is never
    written. Rows are normalized with the certified pack conventions.
    """
    price_params = {
        "tickers": list(SLEEVE_TICKERS),
        "overlap_start": PRICE_OVERLAP_START.isoformat(),
        "as_of": VALIDATION_AS_OF.isoformat(),
    }
    price_rows = format_eod_price_rows(
        _execute_select(conn, _EOD_PRICES_SQL, price_params))

    vintage_params = {
        "series_ids": list(seed_series_ids()),
        "cut_exclusive": _as_of_end_utc(PACK_CUT),
        "as_of_upper": _vintage_upper_bound_utc(),
    }
    vintage_rows = format_macro_vintage_rows(
        _execute_select(conn, _MACRO_VINTAGE_SQL, vintage_params))
    return price_rows, vintage_rows


def build_manifest(price_bytes: bytes, price_rows: list[dict[str, Any]],
                   vintage_bytes: bytes, vintage_rows: list[dict[str, Any]]
                   ) -> dict[str, Any]:
    return {
        "artifact_type": "direct_activation_input_snapshot_manifest",
        "schema_version": SCHEMA_VERSION,
        "stage": "A",
        "direct_activation_id": DIRECT_ACTIVATION_ID,
        "validation_as_of": VALIDATION_AS_OF.isoformat(),
        "pack_v2_sha256": PACK_SHA256_PIN,
        "query_parameters": _query_parameters(),
        "files": {
            "delta_eod_prices.json": {
                "sha256": hashlib.sha256(price_bytes).hexdigest(),
                "rows": len(price_rows),
            },
            "delta_macro_vintages.json": {
                "sha256": hashlib.sha256(vintage_bytes).hexdigest(),
                "rows": len(vintage_rows),
            },
        },
        "staleness_gate_at_export": "pass",
        "provenance": {
            "source": "production datalake (read-only)",
            "db_service_id": PINNED_DB_SERVICE_ID,
            "tables": ["eod_prices", "macro_observation_vintage"],
        },
    }


def export(conn: Any, out_dir: Path | str | None = None) -> dict[str, Any]:
    """Export the delta snapshot into ``out_dir`` (defaults to the committed
    ``input_snapshot/``) and return the manifest. Read-only over prod.

    Order of operations is fail-loud-before-write: the deltas are fetched and the
    Phase 1 staleness gate is recomputed over the composed pack+delta BEFORE any
    file is written, so a stale/degraded export leaves no side effects on disk.
    """
    out = Path(out_dir) if out_dir is not None else SNAPSHOT

    pack_manifest = _lv._load_json(PACK / "manifest.json")
    _require(pack_manifest["input_pack_sha256"] == PACK_SHA256_PIN,
             "pack v2 sha diverged from the signed pin")

    price_rows, vintage_rows = fetch_delta(conn)

    # fail-loud staleness gate over the COMPOSED pack + freshly-exported delta,
    # reusing the live_validation gate (Phase 1 criteria) — no duplication.
    pack_vintages = _lv._load_json(
        PACK / "data" / "canonical" / "macro_observation_vintage.json")
    pack_prices = _lv._load_json(
        PACK / "data" / "canonical" / "eod_prices.json")
    composed_vintages = _lv.compose_rows(
        pack_vintages, vintage_rows, _lv._VINTAGE_KEY, what="vintages")
    composed_prices = _lv.compose_rows(
        pack_prices, price_rows, ("ticker", "date"), what="prices")
    _lv.staleness_gate(composed_vintages, composed_prices)

    # all gates held -> write the three files (deltas byte-identical on re-export)
    out.mkdir(parents=True, exist_ok=True)
    price_bytes = _delta_bytes(price_rows)
    vintage_bytes = _delta_bytes(vintage_rows)
    (out / "delta_eod_prices.json").write_bytes(price_bytes)
    (out / "delta_macro_vintages.json").write_bytes(vintage_bytes)

    manifest = build_manifest(price_bytes, price_rows, vintage_bytes, vintage_rows)
    manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=1,
                                 ensure_ascii=False) + "\n").encode("utf-8")
    (out / "snapshot_manifest.json").write_bytes(manifest_bytes)
    return manifest


def _assert_pinned_db_source(dsn: str) -> None:
    """Refuse to export unless the DSN references the pinned Tiger service, so a
    staging/local DB can never masquerade as prod in the snapshot provenance."""
    _require(PINNED_DB_SERVICE_ID in dsn,
             f"refusing export: DSN does not reference pinned Tiger service "
             f"{PINNED_DB_SERVICE_ID}; snapshot provenance would be false")


def main() -> int:
    from src import db

    dsn = db.resolve_dsn()
    _assert_pinned_db_source(dsn)
    conn = db.connect(dsn)
    try:
        manifest = export(conn)
    finally:
        conn.close()

    files = manifest["files"]
    print(f"exported delta snapshot as-of {manifest['validation_as_of']} "
          f"(pack {PACK_SHA256_PIN[:12]}...)")
    for name in ("delta_eod_prices.json", "delta_macro_vintages.json"):
        info = files[name]
        print(f"  {name}: {info['rows']} rows sha256={info['sha256'][:12]}...")
    print("staleness gate: pass; snapshot_manifest.json written "
          "(read-only; A5 blocked; no DB write)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Offline builder for the coupon-PIT returns artifact (``bond_panel_v1``, audit A2-01).

Reads the pinned v2 artifact set (the read-only T0.2v export under pointer H,
as re-derived by the unit repair) and, optionally, the owner's read-only
``bond_reference_terms`` coupon export, and re-prices every OBSERVED historical
return row (months <= ``FALLBACK_CUTOFF``) with the one coupon convention
:func:`src.bonds.panel_resolvers.bond_coupons` implements: the contractual
coupon where the terms carry one, else the point-in-time expanding median of
the price/YTM inversion.  Everything else is kept verbatim: the key set, every
``price_return``, every typed-exit row, the distribution identity columns, the
payload, and every row after the cutoff (those were priced by the live worker
from ``bond_reference_terms`` already).

The stored history is reconciled BEFORE it is replaced: for each observed row
the coupon the stored carry implies (``12 * carry_return * previous_price``)
is compared with the full-history median that produced it (the T3 base used
Light's pre-BOND-01 ``bond_coupons``; the return-coverage repair tail reused
that coupon).  A reconciliation failure means the inputs are not the ones the
history was built from, and the build refuses.

File I/O only: no database, no network, no wall-clock in any identity.  Inputs
are refused unless their sha256 matches the pinned v2 digests.  The output
directory receives ``bond_monthly_returns.parquet`` (v3, same 13-column schema
and row order as v2), ``coupon_basis.parquet`` (the per-row audit trail) and
``manifest.json`` (digests, counts, reconciliation and delta evidence).

A bond in default trades flat (owner decision, 2026-10-07): inside a confirmed
market-implied default window (``--default-events``, rows of
``bond_market_implied_rating_v1``; :func:`src.bonds.panel_resolvers.default_flat_windows`)
the carry is 0 and the total return is the price return, from the episode's
``d_event_month`` (the REALIZED basis: the historical rebuild knows the event
even though the market confirmed it 0-3 months later) until the cure.  Flat
rows carry ``carry_basis = default_flat`` in ``coupon_basis.parquet`` and in
their returns payload.

Usage:
    python scripts/build_bond_panel_coupon_pit_returns.py \\
        --artifact-dir <unit_repair_v2> --out <unit_repair_v2>/coupon_pit_v3 \\
        [--terms <bond_reference_terms_coupons.csv>] [--default-events <implied_rows.parquet>]

Without ``--terms`` the output is a PIT-only PREVIEW (manifest ``mode`` =
``pit_only_preview``); without ``--default-events`` the mode has no
``_default_flat`` suffix.  Only ``contractual_then_pit_default_flat`` is an
input to the republication; every other mode is a preview.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import json
import platform
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""} and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_bond_panel_history import (  # noqa: E402
    EXPECTED_SHA256_UNIT_REPAIR_V2,
    REPAIR_HISTORY_CUTOFF,
    UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY,
    UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID,
)
from src.bonds.panel_resolvers import (  # noqa: E402
    bond_coupons,
    coupon_from_price_ytm,
    default_flat,
    default_flat_windows,
)

# v2: the default-flat rule changes the carry of every row inside a confirmed
# default window, so it is part of the artifact identity (contract, manifest
# version, the emitter's code revision and fingerprint).
CONTRACT = "returns_coupon_pit_repair_v2"
MANIFEST_VERSION = "bond_panel_coupon_pit_v3_manifest_v2"
COUPON_CONVENTION = (
    "contractual coupon_pct (bond_reference_terms.coupon_rate) where finite, else the "
    "expanding (months <= t) median of coupon_from_price_ytm per CUSIP in month order; "
    "src.bonds.panel_resolvers.bond_coupons, one convention with Light "
    "app.bond_optimizer.returns.bond_coupons; carry 0 inside a confirmed default window"
)
DEFAULT_FLAT_RULE = (
    "owner decision 2026-10-07: a bond in default trades flat and pays no coupon. Inside a "
    "confirmed bond_market_implied_rating_v1 D episode (d_confirmed) carry = 0 and total = price "
    "return, from the episode's d_event_month (first stored D row only where d_event_month is "
    "null) until the first witnessed rated row after its last D row; candidates never flatten; "
    "src.bonds.panel_resolvers.default_flat_windows / default_flat"
)
DEFAULT_FLAT_BASIS = "realized"
DEFAULT_EVENT_COLUMNS = ("cusip_id", "month", "implied_bucket", "witnessed", "spell_id", "d_confirmed", "d_event_month")
DEFAULT_EVENT_IDENTITY_COLUMNS = ("policy_version", "policy_digest", "publication_id")
#: Parquet key-value metadata written by scripts/export_bond_market_implied_default_events.py.
EXPORT_MANIFEST_KEY = b"investintell.export_manifest"
EXPORT_SCHEMA = "bond_market_implied_default_events_export_v1"
EXPORT_MANIFEST_PINS = (
    "schema", "purpose", "publication_id", "rows_digest", "policy_version", "policy_digest", "code_revision",
    "panel_publication_id", "as_of", "cure_witnesses", "pointer_start", "pointer_end", "counts",
)
STORED_BASIS = (
    "months <= 2025-03-01: per-CUSIP median of coupon_from_price_ytm over ALL snapshot "
    "months <= 2025-03-01 (Light pre-BOND-01 full-history median, T3 base); "
    "2025-04-01..2026-06-01: the same coupon reused by the return-coverage repair tail, "
    "or the first inversion after 2025-03-01 for entrants"
)
FALLBACK_CUTOFF = "2026-06-01"
SUSPECT_ABS_RETURN = 0.5
# The frozen OSBAP pack stored price, YTM and maturity as float32 and Light
# computed the stored price_return / coupon / carry_return from those; the v2
# export carries the values upcast to double.  Reproduction is therefore exact
# up to float32 rounding: 1.1e-7 relative on price_return (observed maximum on
# 2.8M rows), and an ABSOLUTE floor of about 6e-6 coupon points on the
# inversion, where the cancellation in ``price / 100 - discount`` turns the
# input rounding into a constant absolute error (observed maximum 5.9e-6; the
# relative error is large only on low-coupon names).  The gates admit exactly
# that noise and refuse anything a different definition or input would produce
# (a different per-CUSIP basis moves the coupon by 1e-3 or more).
PRICE_RETURN_TOLERANCE = (1e-9, 1e-6)  # (absolute, relative)
RECONCILIATION_TOLERANCE = (1e-5, 1e-6)  # (absolute coupon points, relative)
CHANGED_ROW_THRESHOLD_BP = 1e-4
INPUT_FILES = ("bond_panel_live.parquet", "bond_monthly_returns.parquet")
PANEL_COLUMNS = ["month", "cusip_id", "price", "ytm", "maturity_years", "coupon_pct"]
RETURNS_COLUMNS = ["month", "cusip_id", "total_return", "price_return", "carry_return", "exit_basis", "suspect"]
OUTPUT_RETURNS = "bond_monthly_returns.parquet"
OUTPUT_BASIS = "coupon_basis.parquet"
OUTPUT_MANIFEST = "manifest.json"
BATCH_ROWS = 200_000
PARQUET_COMPRESSION = "snappy"


class BuildError(ValueError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class PinnedInputs:
    directory: Path
    paths: dict[str, Path]
    sha256: dict[str, str]
    rows: dict[str, int]
    manifest_sha256: str
    source_pointer: str


def pin_inputs(directory: Path, *, expected_hashes: dict[str, str] | None = None) -> PinnedInputs:
    """Refuse any input whose bytes are not the pinned v2 artifact."""
    expected = EXPECTED_SHA256_UNIT_REPAIR_V2 if expected_hashes is None else expected_hashes
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise BuildError("v2_manifest_unavailable")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paths: dict[str, Path] = {}
    digests: dict[str, str] = {}
    rows: dict[str, int] = {}
    for name in INPUT_FILES:
        path = directory / name
        if not path.is_file():
            raise BuildError(f"artifact_unavailable:{name}")
        digest = _sha256(path)
        if digest != expected[name]:
            raise BuildError(f"artifact_sha256_mismatch:{name}")
        declared = (manifest.get("artifact_sha256") or {}).get(name)
        if declared != digest:
            raise BuildError(f"v2_manifest_artifact_sha256_mismatch:{name}")
        paths[name] = path
        digests[name] = digest
        rows[name] = int(pq.ParquetFile(path).metadata.num_rows)
    return PinnedInputs(
        directory=directory, paths=paths, sha256=digests, rows=rows,
        manifest_sha256=_sha256(manifest_path), source_pointer=str(manifest.get("source_pointer")),
    )


def load_terms(path: Path) -> tuple[pd.Series, dict[str, Any]]:
    """The owner's read-only ``bond_reference_terms`` export: ``cusip9 -> coupon_rate`` (percent of par)."""
    if path.suffix.lower() == ".parquet":
        frame = pq.read_table(path).to_pandas()
    else:
        frame = pd.read_csv(path, dtype={"cusip9": "string"})
    missing = {"cusip9", "coupon_rate"} - set(frame.columns)
    if missing:
        raise BuildError(f"terms_export_missing_columns:{','.join(sorted(missing))}")
    frame = frame.copy()
    frame["cusip9"] = frame["cusip9"].astype("string").str.strip()
    if frame["cusip9"].duplicated().any():
        raise BuildError("terms_export_duplicate_cusip9")
    coupon = pd.to_numeric(frame["coupon_rate"], errors="coerce").astype(float)
    finite = coupon[np.isfinite(coupon)]
    if len(finite) and (finite.min() < 0 or finite.max() > 30):
        raise BuildError("terms_export_coupon_rate_out_of_percent_range")
    evidence: dict[str, Any] = {
        "path": path.name,
        "sha256": _sha256(path),
        "rows": int(len(frame)),
        "with_coupon": int(np.isfinite(coupon).sum()),
    }
    if "coupon_type" in frame.columns:
        counts = frame.loc[np.isfinite(coupon), "coupon_type"].fillna("<null>").astype(str).str.lower().str.strip().value_counts()
        evidence["coupon_type_distribution_with_coupon"] = {str(k): int(v) for k, v in counts.items()}
    for column in ("max_loaded_at", "exported_at_utc"):
        if column in frame.columns and len(frame):
            evidence[column] = str(frame[column].iloc[0])
    return pd.Series(coupon.to_numpy(), index=pd.Index(frame["cusip9"].astype(str)), name="coupon_rate"), evidence


def _read_default_event_rows(path: Path) -> pd.DataFrame:
    """The D-state columns of the implied-rating rows, for CUSIPs with a D row only."""
    wanted = [*DEFAULT_EVENT_COLUMNS, *DEFAULT_EVENT_IDENTITY_COLUMNS]
    if path.suffix.lower() == ".parquet":
        names = set(pq.ParquetFile(path).schema_arrow.names)
        missing = [column for column in DEFAULT_EVENT_COLUMNS if column not in names]
        if missing:
            raise BuildError(f"default_events_missing_columns:{','.join(missing)}")
        table = pq.read_table(path, columns=[column for column in wanted if column in names])
        in_default = pc.equal(table["implied_bucket"], "D")
        defaulted = pc.unique(pc.filter(table["cusip_id"], in_default))
        table = table.filter(pc.is_in(table["cusip_id"], value_set=defaulted))
        frame = table.to_pandas()
    else:
        frame = pd.read_csv(path, dtype={"cusip_id": "string", "implied_bucket": "string"})
        missing = [column for column in DEFAULT_EVENT_COLUMNS if column not in frame.columns]
        if missing:
            raise BuildError(f"default_events_missing_columns:{','.join(missing)}")
        for column in ("witnessed", "d_confirmed"):
            frame[column] = frame[column].map(lambda value: str(value).strip().lower() in {"true", "t", "1"})
        frame = frame[frame["cusip_id"].isin(frame.loc[frame["implied_bucket"].eq("D"), "cusip_id"].unique())]
        frame = frame.loc[:, [column for column in wanted if column in frame.columns]]
    return frame.reset_index(drop=True)


def _export_manifest(path: Path) -> dict[str, Any] | None:
    """The pins the default-events export embedded in its parquet metadata, if any."""
    if path.suffix.lower() != ".parquet":
        return None
    raw = (pq.ParquetFile(path).schema_arrow.metadata or {}).get(EXPORT_MANIFEST_KEY)
    if raw is None:
        return None
    manifest = json.loads(raw.decode("utf-8"))
    return {key: manifest.get(key) for key in EXPORT_MANIFEST_PINS}


def require_publishable_default_events(evidence: dict[str, Any]) -> None:
    """A publishable build needs the pinned export WITH its cure witnesses.

    Without the rows that close each cured episode every default window stays open,
    so every return after a real cure would trade flat. Only a file written by
    ``scripts/export_bond_market_implied_default_events.py`` (its manifest embedded)
    declaring ``cure_witnesses`` may feed the ``contractual_then_pit_default_flat``
    mode the republication emitter accepts; a preview (no ``--terms``) may use any
    publication rows.
    """
    manifest = evidence.get("export_manifest") or {}
    if manifest.get("schema") != EXPORT_SCHEMA:
        raise BuildError("default_events_not_a_pinned_export")
    if manifest.get("cure_witnesses") is not True:
        raise BuildError("default_events_without_cure_witnesses")


def load_default_events(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    """The market-implied default windows from ``bond_market_implied_rating_v1`` rows.

    The file is one publication's rows (parquet or CSV with the publication
    columns); only the D-state columns are read. A file mixing policy digests
    is refused: the windows would silently blend two producers.
    """
    rows = _read_default_event_rows(path)
    try:
        windows = default_flat_windows(rows)
    except ValueError as exc:
        raise BuildError(f"default_events_invalid:{exc}") from exc
    identity: dict[str, Any] = {}
    for column in DEFAULT_EVENT_IDENTITY_COLUMNS:
        values = sorted({str(value) for value in rows[column].dropna()}) if column in rows else []
        if len(values) > 1:
            raise BuildError(f"default_events_mixed_{column}")
        identity[column] = values[0] if values else None
    lag = (
        (windows["confirmation_month"].dt.year - windows["event_month"].dt.year) * 12
        + windows["confirmation_month"].dt.month - windows["event_month"].dt.month
    )
    export_manifest = _export_manifest(path)
    if export_manifest is not None:
        for column in ("policy_digest", "publication_id"):
            if identity.get(column) is not None and export_manifest.get(column) != identity[column]:
                raise BuildError(f"default_events_export_manifest_{column}_mismatch")
    evidence: dict[str, Any] = {
        "path": path.name,
        "sha256": _sha256(path),
        **identity,
        "export_manifest": export_manifest,
        "rows_of_defaulted_cusips": int(len(rows)),
        "d_rows": int(rows["implied_bucket"].eq("D").sum()),
        "episodes": int(len(windows)),
        "cusips": int(windows["cusip_id"].nunique()),
        "episodes_cured": int(windows["cure_month"].notna().sum()),
        "episodes_open": int(windows["cure_month"].isna().sum()),
        "event_month_source": {str(k): int(v) for k, v in windows["event_month_source"].value_counts().sort_index().items()},
        "confirmation_lag_months": {str(int(k)): int(v) for k, v in lag.value_counts().sort_index().items()},
        "event_month_min": windows["event_month"].min().strftime("%Y-%m-%d") if len(windows) else None,
        "event_month_max": windows["event_month"].max().strftime("%Y-%m-%d") if len(windows) else None,
    }
    return windows, evidence


def _load_panel(path: Path, cutoff: pd.Timestamp) -> pd.DataFrame:
    table = pq.read_table(path, columns=PANEL_COLUMNS)
    panel = table.to_pandas()
    panel["month"] = pd.to_datetime(panel["month"])
    panel = panel[panel["month"] <= cutoff].reset_index(drop=True)
    panel["cusip_id"] = panel["cusip_id"].astype(str)
    if panel.duplicated(["cusip_id", "month"]).any():
        raise BuildError("panel_duplicate_cusip_months")
    return panel.sort_values(["cusip_id", "month"], kind="mergesort").reset_index(drop=True)


def _load_returns(path: Path) -> pd.DataFrame:
    returns = pq.read_table(path, columns=RETURNS_COLUMNS).to_pandas()
    returns["month"] = pd.to_datetime(returns["month"])
    returns["cusip_id"] = returns["cusip_id"].astype(str)
    if returns.duplicated(["cusip_id", "month"]).any():
        raise BuildError("returns_duplicate_cusip_months")
    return returns


def _stored_basis_coupon(panel: pd.DataFrame, inversion: pd.Series) -> pd.Series:
    """Per-CUSIP coupon the stored history was priced with (see ``STORED_BASIS``)."""
    history = panel["month"] <= pd.Timestamp(REPAIR_HISTORY_CUTOFF)
    base = inversion[history].groupby(panel.loc[history, "cusip_id"], observed=True).median()
    later = inversion[~history & inversion.notna()]
    entrant = later.groupby(panel.loc[later.index, "cusip_id"], observed=True).first()
    combined = base.dropna()
    missing = entrant.index.difference(combined.index)
    return pd.concat([combined, entrant.loc[missing]])


def _no_row(repriced: pd.DataFrame) -> pd.Series:
    """No coupon basis at or before the month and not flat: the resolver produces no return row."""
    return repriced["basis"].eq("none") & ~repriced["carry_basis"].eq("default_flat")


def reprice(
    panel: pd.DataFrame,
    returns: pd.DataFrame,
    terms: pd.Series | None,
    *,
    cutoff: pd.Timestamp,
    default_windows: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Re-price the observed rows at or before ``cutoff``; keep every other row verbatim.

    Rows inside a confirmed default window (``default_windows``, realized basis)
    trade flat: carry 0, whatever the coupon basis, so a flat row is never dropped.
    """
    scope = returns[(returns["month"] <= cutoff) & returns["exit_basis"].eq("observed")].copy()
    scope = scope.set_index(["cusip_id", "month"]).sort_index()
    keyed = panel.set_index(["cusip_id", "month"])
    previous_month = panel.groupby("cusip_id", observed=True)["month"].shift()
    consecutive = (panel["month"] - previous_month).dt.days.between(28, 31)
    previous_price = panel.groupby("cusip_id", observed=True)["price"].shift().where(consecutive)
    keyed["previous_price"] = previous_price.to_numpy()
    if not scope.index.isin(keyed.index).all():
        raise BuildError("returns_keys_absent_from_panel")
    rows = keyed.loc[scope.index]
    price_return = (rows["price"] - rows["previous_price"]) / rows["previous_price"]
    stored_price_return = scope["price_return"].to_numpy(dtype=float)
    price_diff = np.abs(price_return.to_numpy() - stored_price_return)
    price_allowed = PRICE_RETURN_TOLERANCE[0] + PRICE_RETURN_TOLERANCE[1] * np.abs(stored_price_return)
    price_mismatch = ~np.isfinite(price_return.to_numpy()) | (price_diff > price_allowed)
    if price_mismatch.any():
        raise BuildError(f"price_return_not_reproduced:{int(price_mismatch.sum())}")

    inversion = coupon_from_price_ytm(panel["price"], panel["ytm"], panel["maturity_years"])
    stored_coupon = _stored_basis_coupon(panel, inversion)
    implied_by_carry = 12.0 * scope["carry_return"].to_numpy() * rows["previous_price"].to_numpy()
    stored_expected = stored_coupon.reindex(scope.index.get_level_values("cusip_id")).to_numpy()
    reconciliation = np.abs(implied_by_carry - stored_expected)
    reconciliation_allowed = RECONCILIATION_TOLERANCE[0] + RECONCILIATION_TOLERANCE[1] * np.abs(stored_expected)
    reconciled = np.isfinite(reconciliation) & (reconciliation <= reconciliation_allowed)
    if not reconciled.all():
        raise BuildError(f"stored_carry_not_reproduced:{int((~reconciled).sum())}")

    snapshot_coupon = pd.to_numeric(panel["coupon_pct"], errors="coerce").astype(float)
    terms_coupon = (
        terms.reindex(panel["cusip_id"]).to_numpy(dtype=float) if terms is not None else np.full(len(panel), np.nan)
    )
    effective = snapshot_coupon.where(np.isfinite(snapshot_coupon), pd.Series(terms_coupon, index=panel.index))
    resolver_input = pd.DataFrame({
        "cusip_id": panel["cusip_id"], "month": panel["month"], "pr": panel["price"], "ytm": panel["ytm"],
        "bond_maturity": panel["maturity_years"], "coupon_pct": effective,
    })
    new_coupon_all = bond_coupons(resolver_input)
    new_coupon = pd.Series(new_coupon_all.to_numpy(), index=keyed.index).loc[scope.index].to_numpy()
    basis_all = np.where(np.isfinite(effective.to_numpy()), "contractual", np.where(np.isfinite(new_coupon_all.to_numpy()), "pit", "none"))
    basis = pd.Series(basis_all, index=keyed.index).loc[scope.index].to_numpy()

    keys = pd.DataFrame({"cusip_id": scope.index.get_level_values("cusip_id"), "month": scope.index.get_level_values("month")})
    realized = default_flat(keys, default_windows, basis=DEFAULT_FLAT_BASIS)
    flat = realized["default_flat"].to_numpy(dtype=bool)
    confirmed_by_then = default_flat(keys, default_windows, basis="point_in_time")["default_flat"].to_numpy(dtype=bool)
    carry_before = scope["carry_return"].to_numpy(dtype=float)
    carry_coupon = new_coupon / 12.0 / rows["previous_price"].to_numpy()
    carry_after = np.where(flat, 0.0, carry_coupon)
    total_after = scope["price_return"].to_numpy(dtype=float) + carry_after
    repriced = pd.DataFrame({
        "cusip_id": keys["cusip_id"].to_numpy(),
        "month": keys["month"].to_numpy(),
        "previous_price": rows["previous_price"].to_numpy(),
        "coupon_stored": stored_expected,
        "coupon_new": new_coupon,
        "basis": basis,
        "carry_basis": np.where(flat, "default_flat", "coupon"),
        "default_event_month": realized["default_event_month"].to_numpy(),
        "carry_coupon": carry_coupon,
        "carry_before": carry_before,
        "carry_after": carry_after,
        "total_before": scope["total_return"].to_numpy(dtype=float),
        "total_after": total_after,
        "suspect_before": scope["suspect"].to_numpy(dtype=bool),
        "suspect_after": np.abs(total_after) > SUSPECT_ABS_RETURN,
    })
    repriced["delta_bp"] = (repriced["carry_after"] - repriced["carry_before"]) * 1e4
    no_row = _no_row(repriced)
    dropped = repriced[no_row]
    kept = repriced[~no_row]
    kept_flat = kept["carry_basis"].eq("default_flat")
    changed = kept["delta_bp"].abs() > CHANGED_ROW_THRESHOLD_BP
    per_year: list[dict[str, Any]] = []
    for year, group in kept.groupby(kept["month"].dt.year):
        abs_delta = group["delta_bp"].abs()
        per_year.append({
            "year": int(year),
            "rows": int(len(group)),
            "contractual_rows": int(group["basis"].eq("contractual").sum()),
            "pit_rows": int(group["basis"].eq("pit").sum()),
            "default_flat_rows": int(group["carry_basis"].eq("default_flat").sum()),
            "changed_rows": int((abs_delta > CHANGED_ROW_THRESHOLD_BP).sum()),
            "sum_carry_before": float(group["carry_before"].sum()),
            "sum_carry_after": float(group["carry_after"].sum()),
            "mean_delta_bp": float(group["delta_bp"].mean()),
            "p50_abs_delta_bp": float(abs_delta.median()),
            "p90_abs_delta_bp": float(abs_delta.quantile(0.9)),
            "p99_abs_delta_bp": float(abs_delta.quantile(0.99)),
            "max_abs_delta_bp": float(abs_delta.max()),
            "suspect_before": int(group["suspect_before"].sum()),
            "suspect_after": int(group["suspect_after"].sum()),
        })
    abs_all = kept["delta_bp"].abs()
    at_or_before = returns["month"] <= cutoff
    evidence = {
        "rows_at_or_before_cutoff": int(at_or_before.sum()),
        "rows_after_cutoff": int((~at_or_before).sum()),
        "exit_rows_at_or_before_cutoff": int((at_or_before & ~returns["exit_basis"].eq("observed")).sum()),
        "scope_rows": int(len(scope)),
        "repriced_rows": int(len(kept)),
        "dropped_rows_no_pit_basis": int(len(dropped)),
        # The resolver's own outcome where no inversion exists at or before t: no
        # return row.  The keys are listed so the republication can pin exactly
        # these absences; a key set this small is evidence, not data.
        "dropped_keys": [{"cusip_id": str(c), "month": m.strftime("%Y-%m-%d")} for c, m in zip(dropped["cusip_id"], dropped["month"])],
        "dropped_keys_digest": _canonical_digest([{"cusip_id": str(c), "month": m.strftime("%Y-%m-%d")} for c, m in zip(dropped["cusip_id"], dropped["month"])]),
        "contractual_rows": int(kept["basis"].eq("contractual").sum()),
        "pit_rows": int(kept["basis"].eq("pit").sum()),
        "cusips_in_scope": int(scope.index.get_level_values("cusip_id").nunique()),
        "cusips_with_contractual_coupon": int(kept.loc[kept["basis"].eq("contractual"), "cusip_id"].nunique()),
        "snapshot_rows_with_own_coupon_pct": int(np.isfinite(snapshot_coupon.to_numpy()).sum()),
        "changed_rows": int(changed.sum()),
        "delta_bp": {
            "mean": float(kept["delta_bp"].mean()), "p50_abs": float(abs_all.median()), "p90_abs": float(abs_all.quantile(0.9)),
            "p99_abs": float(abs_all.quantile(0.99)), "max_abs": float(abs_all.max()), "min": float(kept["delta_bp"].min()), "max": float(kept["delta_bp"].max()),
        },
        "suspect_before": int(kept["suspect_before"].sum()),
        "suspect_after": int(kept["suspect_after"].sum()),
        "default_flat": {
            "rule": DEFAULT_FLAT_RULE,
            "basis": DEFAULT_FLAT_BASIS,
            "date_field": "d_event_month",
            "date_field_fallback": "first stored D row (confirmation month)",
            "applied": default_windows is not None,
            "rows": int(kept_flat.sum()),
            "cusips": int(kept.loc[kept_flat, "cusip_id"].nunique()),
            "rows_without_coupon_basis": int((kept_flat & kept["basis"].eq("none")).sum()),
            # Realized-basis rows the market had not confirmed yet at that month
            # (event month <= t < confirmation month): what a point-in-time basis
            # would still have priced with the coupon.
            "rows_before_confirmation": int((flat & ~confirmed_by_then).sum()),
            "carry_removed_bp_sum": float((kept.loc[kept_flat, "carry_coupon"].fillna(0.0) * 1e4).sum()),
        },
        "reconciliation": {
            "basis": STORED_BASIS,
            "tolerance": {"absolute_coupon_points": RECONCILIATION_TOLERANCE[0], "relative": RECONCILIATION_TOLERANCE[1]},
            "rows": int(len(scope)),
            "max_abs_diff": float(np.nanmax(reconciliation)) if len(reconciliation) else 0.0,
            "max_relative_diff": float(np.nanmax(reconciliation / np.maximum(np.abs(stored_expected), 1e-12))) if len(reconciliation) else 0.0,
        },
        "price_return_reproduction": {
            "tolerance": {"absolute": PRICE_RETURN_TOLERANCE[0], "relative": PRICE_RETURN_TOLERANCE[1]},
            "rows": int(len(scope)),
            "max_abs_diff": float(np.nanmax(price_diff)) if len(price_diff) else 0.0,
            "max_relative_diff": float(np.nanmax(price_diff / np.maximum(np.abs(stored_price_return), 1e-12))) if len(price_diff) else 0.0,
        },
        "per_year": per_year,
    }
    return repriced, evidence


def _flat_payload(payload: Any, event_month: Any) -> str:
    try:
        document = json.loads(payload) if isinstance(payload, str) and payload else {}
    except ValueError:
        document = {"raw_payload": payload}
    if not isinstance(document, dict):
        document = {"raw_payload": document}
    document["carry_basis"] = "default_flat"
    document["default_event_month"] = pd.Timestamp(event_month).strftime("%Y-%m-%d")
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


_WRITER_COLUMNS = ("carry_after", "total_after", "suspect_after", "carry_basis", "default_event_month")


def _write_returns(source: Path, repriced: pd.DataFrame, destination: Path) -> dict[str, Any]:
    """Stream the v2 returns row groups, swapping only carry/total/suspect on repriced keys."""
    no_row = _no_row(repriced)
    kept = repriced.loc[~no_row, ["cusip_id", "month", *_WRITER_COLUMNS]].set_index(["cusip_id", "month"])
    dropped = set(zip(repriced.loc[no_row, "cusip_id"], repriced.loc[no_row, "month"]))
    reader = pq.ParquetFile(source)
    written = 0
    replaced = 0
    with pq.ParquetWriter(destination, reader.schema_arrow, compression=PARQUET_COMPRESSION) as writer:
        for batch in reader.iter_batches(batch_size=BATCH_ROWS):
            frame = batch.to_pandas()
            keys = pd.MultiIndex.from_arrays([frame["cusip_id"].astype(str), pd.to_datetime(frame["month"])])
            if dropped:
                keep_mask = np.array([(c, m) not in dropped for c, m in zip(keys.get_level_values(0), keys.get_level_values(1))])
                frame = frame[keep_mask].reset_index(drop=True)
                keys = keys[keep_mask]
            match = kept.reindex(keys)
            hit = match["carry_after"].notna().to_numpy()
            frame.loc[hit, "carry_return"] = match.loc[hit, "carry_after"].to_numpy()
            frame.loc[hit, "total_return"] = match.loc[hit, "total_after"].to_numpy()
            frame.loc[hit, "suspect"] = match.loc[hit, "suspect_after"].to_numpy(dtype=bool)
            # Flat rows say so in their payload, with the keys the live worker writes.
            flat = hit & match["carry_basis"].eq("default_flat").to_numpy()
            for position in np.flatnonzero(flat):
                frame.at[position, "payload"] = _flat_payload(frame.at[position, "payload"], match["default_event_month"].iloc[position])
            replaced += int(hit.sum())
            written += int(len(frame))
            writer.write_table(pa.Table.from_pandas(frame, schema=reader.schema_arrow, preserve_index=False))
    return {"rows": written, "replaced_rows": replaced, "sha256": _sha256(destination), "compression": PARQUET_COMPRESSION, "batch_rows": BATCH_ROWS}


def _tool_versions() -> dict[str, str]:
    versions = {"python": platform.python_version()}
    for name in ("pandas", "numpy", "pyarrow"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:  # pragma: no cover
            versions[name] = "unknown"
    return versions


def build(
    artifact_dir: Path,
    out_dir: Path,
    terms_path: Path | None = None,
    *,
    default_events_path: Path | None = None,
    expected_hashes: dict[str, str] | None = None,
    cutoff: str = FALLBACK_CUTOFF,
) -> dict[str, Any]:
    inputs = pin_inputs(artifact_dir, expected_hashes=expected_hashes)
    terms: pd.Series | None = None
    terms_evidence: dict[str, Any] | None = None
    if terms_path is not None:
        terms, terms_evidence = load_terms(terms_path)
    default_windows: pd.DataFrame | None = None
    default_events_evidence: dict[str, Any] | None = None
    if default_events_path is not None:
        default_windows, default_events_evidence = load_default_events(default_events_path)
        if terms is not None:
            require_publishable_default_events(default_events_evidence)
    cutoff_ts = pd.Timestamp(cutoff)
    panel = _load_panel(inputs.paths["bond_panel_live.parquet"], cutoff_ts)
    returns = _load_returns(inputs.paths["bond_monthly_returns.parquet"])
    repriced, evidence = reprice(panel, returns, terms, cutoff=cutoff_ts, default_windows=default_windows)
    # The writer streams the source parquet: release the full panel and returns
    # frames before it runs (the build's peak memory is otherwise both plus the
    # write buffers).
    del panel, returns
    gc.collect()
    out_dir.mkdir(parents=True, exist_ok=True)
    returns_out = _write_returns(inputs.paths["bond_monthly_returns.parquet"], repriced, out_dir / OUTPUT_RETURNS)
    basis_path = out_dir / OUTPUT_BASIS
    basis_table = pa.Table.from_pandas(repriced.sort_values(["month", "cusip_id"], kind="mergesort").reset_index(drop=True), preserve_index=False)
    pq.write_table(basis_table, basis_path, compression=PARQUET_COMPRESSION)
    expected_rows = inputs.rows["bond_monthly_returns.parquet"] - evidence["dropped_rows_no_pit_basis"]
    if returns_out["rows"] != expected_rows or returns_out["replaced_rows"] != evidence["repriced_rows"]:
        raise BuildError("output_row_accounting_mismatch")
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "contract": CONTRACT,
        "mode": ("contractual_then_pit" if terms is not None else "pit_only_preview") + ("_default_flat" if default_windows is not None else ""),
        "coupon_convention": COUPON_CONVENTION,
        "default_flat": evidence["default_flat"],
        "fallback_cutoff": cutoff,
        "source_pointer": inputs.source_pointer,
        "expected_head_publication_id": UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID,
        "inputs": {
            "directory": str(inputs.directory),
            "artifact_sha256": dict(sorted(inputs.sha256.items())),
            "rows": dict(sorted(inputs.rows.items())),
            "v2_manifest_sha256": inputs.manifest_sha256,
            "terms_export": terms_evidence,
            "default_events": default_events_evidence,
            "resolver_sha256": _sha256(ROOT / "src" / "bonds" / "panel_resolvers.py"),
            "builder_sha256": _sha256(Path(__file__).resolve()),
        },
        "outputs": {
            OUTPUT_RETURNS: returns_out,
            OUTPUT_BASIS: {"rows": int(len(repriced)), "sha256": _sha256(basis_path), "compression": PARQUET_COMPRESSION},
        },
        "counts": {
            "returns_rows_in": inputs.rows["bond_monthly_returns.parquet"],
            "returns_rows_out": returns_out["rows"],
            "rows_at_or_before_cutoff": evidence["rows_at_or_before_cutoff"],
            "rows_after_cutoff": evidence["rows_after_cutoff"],
            "exit_rows_at_or_before_cutoff": evidence["exit_rows_at_or_before_cutoff"],
            "scope_rows": evidence["scope_rows"],
            "repriced_rows": evidence["repriced_rows"],
            "dropped_rows_no_pit_basis": evidence["dropped_rows_no_pit_basis"],
            "verbatim_rows": returns_out["rows"] - evidence["repriced_rows"],
            "contractual_rows": evidence["contractual_rows"],
            "pit_rows": evidence["pit_rows"],
            "default_flat_rows": evidence["default_flat"]["rows"],
            "default_flat_cusips": evidence["default_flat"]["cusips"],
            "changed_rows": evidence["changed_rows"],
            "cusips_in_scope": evidence["cusips_in_scope"],
            "cusips_with_contractual_coupon": evidence["cusips_with_contractual_coupon"],
            "snapshot_rows_with_own_coupon_pct": evidence["snapshot_rows_with_own_coupon_pct"],
            "suspect_before": evidence["suspect_before"],
            "suspect_after": evidence["suspect_after"],
        },
        "dropped_keys": evidence["dropped_keys"],
        "dropped_keys_digest": evidence["dropped_keys_digest"],
        "reconciliation": evidence["reconciliation"],
        "price_return_reproduction": evidence["price_return_reproduction"],
        "delta_bp": evidence["delta_bp"],
        "per_year": evidence["per_year"],
        "per_year_digest": _canonical_digest({"per_year": evidence["per_year"]}),
        "tool_versions": _tool_versions(),
    }
    (out_dir / OUTPUT_MANIFEST).write_text(json.dumps(manifest, indent=1, sort_keys=True), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--artifact-dir", type=Path, default=UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--terms", type=Path, help="read-only bond_reference_terms export (cusip9, coupon_rate[, coupon_type, ...]); omit for a PIT-only preview")
    parser.add_argument("--default-events", type=Path, help="bond_market_implied_rating_v1 rows (parquet or CSV) whose confirmed D episodes trade flat; omit for a preview without the default-flat rule")
    args = parser.parse_args(argv)
    try:
        manifest = build(args.artifact_dir, args.out, args.terms, default_events_path=args.default_events)
    except (BuildError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps({k: manifest[k] for k in ("mode", "counts", "default_flat", "delta_bp", "reconciliation", "outputs")}, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

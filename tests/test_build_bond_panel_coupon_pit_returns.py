"""The coupon-PIT returns artifact builder: pinned inputs, reconciliation-then-replace, verbatim rest."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import build_bond_panel_coupon_pit_returns as builder
from src.bonds.panel_resolvers import coupon_from_price_ytm

RETURNS_SCHEMA = pa.schema([
    ("publication_id", pa.string()), ("month", pa.date32()), ("cusip_id", pa.string()),
    ("total_return", pa.float64()), ("price_return", pa.float64()), ("carry_return", pa.float64()),
    ("exit_basis", pa.string()), ("exit_reason", pa.string()), ("suspect", pa.bool_()), ("payload", pa.string()),
    ("distribution_rule", pa.string()), ("reference_cusip9", pa.string()), ("distribution_decision_id", pa.string()),
])


def _panel_rows() -> list[dict[str, object]]:
    """Two bonds: AAA par/5% for 30 months then 70/18% (the audit synthetic), BBB steady at par."""
    rows: list[dict[str, object]] = []
    # 2020-01..2024-12: both regimes sit inside the T3 base window (<= 2025-03), so the
    # stored basis (full-history median) is the inflated (5.0 + distressed) / 2.
    months = pd.date_range("2020-01-01", periods=60, freq="MS")
    for index, month in enumerate(months):
        distressed = index >= 30
        rows.append({"month": month.date(), "cusip_id": "AAA000001", "price": 70.0 if distressed else 100.0, "ytm": 0.18 if distressed else 0.05, "maturity_years": 8.0, "coupon_pct": None})
        rows.append({"month": month.date(), "cusip_id": "BBB000002", "price": 100.0, "ytm": 0.05, "maturity_years": 8.0, "coupon_pct": None})
    # A row the live worker priced from terms (after the cutoff) and one without a maturity.
    rows.append({"month": pd.Timestamp("2026-07-01").date(), "cusip_id": "AAA000001", "price": 71.0, "ytm": 0.18, "maturity_years": 7.7, "coupon_pct": 5.0})
    rows.append({"month": pd.Timestamp("2023-01-01").date(), "cusip_id": "CCC000003", "price": 99.0, "ytm": 0.04, "maturity_years": None, "coupon_pct": None})
    rows.append({"month": pd.Timestamp("2023-02-01").date(), "cusip_id": "CCC000003", "price": 99.5, "ytm": 0.04, "maturity_years": None, "coupon_pct": None})
    # A bond whose first three months carry no YTM: the stored basis took its coupon
    # from the later months (look-ahead); under PIT the first two return months have
    # no basis and get no row.
    for index, month in enumerate(pd.date_range("2021-01-01", periods=6, freq="MS")):
        rows.append({"month": month.date(), "cusip_id": "DDD000004", "price": 100.0 + index, "ytm": None if index < 3 else 0.05, "maturity_years": 5.0, "coupon_pct": None})
    return rows


DROPPED_KEYS = [{"cusip_id": "DDD000004", "month": "2021-02-01"}, {"cusip_id": "DDD000004", "month": "2021-03-01"}]


def _stored_returns(panel: pd.DataFrame) -> pd.DataFrame:
    """The stored history: full-history median coupon over months <= 2025-03 (the T3 base basis)."""
    panel = panel.sort_values(["cusip_id", "month"]).reset_index(drop=True)
    inversion = coupon_from_price_ytm(panel["price"], panel["ytm"], panel["maturity_years"])
    history = panel["month"] <= pd.Timestamp(builder.REPAIR_HISTORY_CUTOFF)
    coupon = inversion[history].groupby(panel.loc[history, "cusip_id"]).median()
    out = []
    for cusip, group in panel.groupby("cusip_id"):
        group = group.reset_index(drop=True)
        for index in range(1, len(group)):
            previous, current = group.loc[index - 1], group.loc[index]
            if not 28 <= (current["month"] - previous["month"]).days <= 31:
                continue
            if current["month"] > pd.Timestamp("2026-06-01"):
                continue
            stored = coupon.get(cusip, np.nan)
            if not np.isfinite(stored):
                continue
            price_return = (current["price"] - previous["price"]) / previous["price"]
            carry = stored / 12 / previous["price"]
            out.append({"publication_id": "head", "month": current["month"].date(), "cusip_id": cusip, "total_return": price_return + carry, "price_return": price_return, "carry_return": carry, "exit_basis": "observed", "exit_reason": None, "suspect": abs(price_return + carry) > 0.5, "payload": "{}", "distribution_rule": "rule_144a", "reference_cusip9": cusip, "distribution_decision_id": None})
    # A typed exit row and a live-worker row after the cutoff: both verbatim.
    out.append({"publication_id": "head", "month": pd.Timestamp("2025-09-01").date(), "cusip_id": "DDD000004", "total_return": -0.4, "price_return": None, "carry_return": None, "exit_basis": "distressed", "exit_reason": "distressed", "suspect": False, "payload": "{}", "distribution_rule": "rule_144a", "reference_cusip9": "DDD000004", "distribution_decision_id": None})
    out.append({"publication_id": "head", "month": pd.Timestamp("2026-07-01").date(), "cusip_id": "AAA000001", "total_return": 0.02, "price_return": 0.0142857, "carry_return": 5.0 / 12 / 70.0, "exit_basis": "observed", "exit_reason": None, "suspect": False, "payload": "{}", "distribution_rule": "rule_144a", "reference_cusip9": "AAA000001", "distribution_decision_id": None})
    return pd.DataFrame(out)


def _artifact_dir(tmp_path: Path) -> tuple[Path, dict[str, str], pd.DataFrame, pd.DataFrame]:
    directory = tmp_path / "unit_repair_v2"
    directory.mkdir()
    panel = pd.DataFrame(_panel_rows())
    panel["month"] = pd.to_datetime(panel["month"])
    returns = _stored_returns(panel)
    panel_out = panel.copy()
    panel_out["month"] = panel_out["month"].dt.date
    pq.write_table(pa.Table.from_pandas(panel_out, preserve_index=False), directory / "bond_panel_live.parquet")
    pq.write_table(pa.Table.from_pandas(returns, schema=RETURNS_SCHEMA, preserve_index=False), directory / "bond_monthly_returns.parquet")
    hashes = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in builder.INPUT_FILES}
    (directory / "manifest.json").write_text(json.dumps({"artifact_sha256": hashes, "source_pointer": "head"}), encoding="utf-8")
    return directory, hashes, panel, returns


def test_pinned_inputs_refuse_a_drifted_artifact(tmp_path: Path) -> None:
    directory, hashes, _panel, _returns = _artifact_dir(tmp_path)
    with pytest.raises(builder.BuildError, match="artifact_sha256_mismatch:bond_panel_live.parquet"):
        builder.pin_inputs(directory, expected_hashes={**hashes, "bond_panel_live.parquet": "0" * 64})
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    manifest["artifact_sha256"]["bond_monthly_returns.parquet"] = "0" * 64
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(builder.BuildError, match="v2_manifest_artifact_sha256_mismatch"):
        builder.pin_inputs(directory, expected_hashes=hashes)


def test_pit_only_preview_reprices_only_the_fallback_carry(tmp_path: Path) -> None:
    directory, hashes, _panel, stored = _artifact_dir(tmp_path)
    out = tmp_path / "coupon_pit_v3"

    manifest = builder.build(directory, out, None, expected_hashes=hashes)

    produced = pq.read_table(out / builder.OUTPUT_RETURNS).to_pandas()
    assert manifest["mode"] == "pit_only_preview"
    assert produced.columns.tolist() == stored.columns.tolist()
    # The two DDD months without a PIT basis get no row; everything else keeps its order.
    assert manifest["counts"]["dropped_rows_no_pit_basis"] == 2
    assert manifest["dropped_keys"] == DROPPED_KEYS
    assert manifest["dropped_keys_digest"] == builder._canonical_digest(DROPPED_KEYS)
    dropped = {(key["cusip_id"], pd.Timestamp(key["month"])) for key in DROPPED_KEYS}
    stored_kept = stored[[(c, pd.Timestamp(m)) not in dropped for c, m in zip(stored["cusip_id"], stored["month"])]].reset_index(drop=True)
    assert len(produced) == len(stored_kept) == len(stored) - 2 == manifest["counts"]["returns_rows_out"]
    assert manifest["counts"]["contractual_rows"] == 0
    assert manifest["counts"]["pit_rows"] == manifest["counts"]["repriced_rows"]
    # Keys, price_return, typed exits, identity and post-cutoff rows: verbatim, same order.
    for column in ("month", "cusip_id", "price_return", "exit_basis", "exit_reason", "distribution_rule", "reference_cusip9", "payload", "publication_id"):
        pd.testing.assert_series_equal(produced[column], stored_kept[column], check_names=False)
    stored = stored_kept
    produced["month"] = pd.to_datetime(produced["month"])
    stored["month"] = pd.to_datetime(stored["month"])
    aaa = produced[produced["cusip_id"].eq("AAA000001")].set_index("month")
    aaa_stored = stored[stored["cusip_id"].eq("AAA000001")].set_index("month")
    # Healthy months keep the par coupon (5.0/12/100); the stored basis had the inflated one.
    assert aaa.loc[pd.Timestamp("2021-01-01"), "carry_return"] == pytest.approx(5.0 / 12 / 100.0, abs=1e-12)
    assert aaa_stored.loc[pd.Timestamp("2021-01-01"), "carry_return"] > 5.5 / 12 / 100.0
    # First month priced off the distressed previous price (month index 31, prev 70): still 5.0.
    assert aaa.loc[pd.Timestamp("2022-08-01"), "carry_return"] == pytest.approx(5.0 / 12 / 70.0, abs=1e-12)
    assert aaa.loc[pd.Timestamp("2022-08-01"), "total_return"] == pytest.approx(aaa_stored.loc[pd.Timestamp("2022-08-01"), "price_return"] + 5.0 / 12 / 70.0, abs=1e-12)
    # After the cutoff and the typed exit: untouched.
    assert aaa.loc[pd.Timestamp("2026-07-01"), "carry_return"] == pytest.approx(5.0 / 12 / 70.0)
    exit_row = produced[produced["exit_basis"].eq("distressed")].iloc[0]
    assert exit_row["total_return"] == pytest.approx(-0.4) and pd.isna(exit_row["carry_return"])
    # BBB (steady at par) is unchanged by construction.
    bbb = produced[produced["cusip_id"].eq("BBB000002")]
    bbb_stored = stored[stored["cusip_id"].eq("BBB000002")]
    assert bbb["carry_return"].to_numpy() == pytest.approx(bbb_stored["carry_return"].to_numpy(), abs=1e-12)
    basis = pq.read_table(out / builder.OUTPUT_BASIS).to_pandas()
    assert set(basis["basis"]) == {"pit", "none"}
    assert sorted(basis.loc[basis["basis"].eq("none"), "month"].astype(str)) == ["2021-02-01", "2021-03-01"]
    ddd = produced[produced["cusip_id"].eq("DDD000004")]
    assert pd.to_datetime(ddd["month"]).min() == pd.Timestamp("2021-04-01")
    assert manifest["outputs"][builder.OUTPUT_RETURNS]["sha256"] == hashlib.sha256((out / builder.OUTPUT_RETURNS).read_bytes()).hexdigest()
    assert manifest["reconciliation"]["max_abs_diff"] <= builder.RECONCILIATION_TOLERANCE[0]
    assert manifest["price_return_reproduction"]["max_abs_diff"] <= builder.PRICE_RETURN_TOLERANCE[0]
    assert manifest["counts"]["rows_after_cutoff"] == 1
    assert manifest["counts"]["exit_rows_at_or_before_cutoff"] == 1
    assert manifest["counts"]["rows_at_or_before_cutoff"] + manifest["counts"]["rows_after_cutoff"] == manifest["counts"]["returns_rows_in"]
    assert manifest["inputs"]["artifact_sha256"] == hashes
    assert len(manifest["inputs"]["resolver_sha256"]) == 64


def test_contractual_terms_take_precedence_and_are_pinned_in_the_manifest(tmp_path: Path) -> None:
    directory, hashes, _panel, stored = _artifact_dir(tmp_path)
    terms = tmp_path / "bond_reference_terms_coupons.csv"
    terms.write_text("cusip9,coupon_rate,coupon_type\nAAA000001,6.5,Fixed\nZZZ000009,4.0,Fixed\n", encoding="utf-8")
    out = tmp_path / "coupon_pit_v3"

    manifest = builder.build(directory, out, terms, expected_hashes=hashes)

    produced = pq.read_table(out / builder.OUTPUT_RETURNS).to_pandas()
    produced["month"] = pd.to_datetime(produced["month"])
    aaa = produced[produced["cusip_id"].eq("AAA000001")].set_index("month")
    assert manifest["mode"] == "contractual_then_pit"
    assert manifest["inputs"]["terms_export"]["sha256"] == hashlib.sha256(terms.read_bytes()).hexdigest()
    assert manifest["inputs"]["terms_export"]["coupon_type_distribution_with_coupon"] == {"fixed": 2}
    assert manifest["counts"]["cusips_with_contractual_coupon"] == 1
    assert manifest["counts"]["contractual_rows"] == int(stored["cusip_id"].eq("AAA000001").sum()) - 1  # minus the post-cutoff row
    assert aaa.loc[pd.Timestamp("2021-01-01"), "carry_return"] == pytest.approx(6.5 / 12 / 100.0, abs=1e-12)
    assert aaa.loc[pd.Timestamp("2022-08-01"), "carry_return"] == pytest.approx(6.5 / 12 / 70.0, abs=1e-12)
    bbb = produced[produced["cusip_id"].eq("BBB000002")]
    assert bbb["carry_return"].to_numpy() == pytest.approx(stored[stored["cusip_id"].eq("BBB000002")]["carry_return"].to_numpy(), abs=1e-12)
    basis = pq.read_table(out / builder.OUTPUT_BASIS).to_pandas()
    assert set(basis.loc[basis["cusip_id"].eq("AAA000001"), "basis"]) == {"contractual"}
    assert set(basis.loc[basis["cusip_id"].eq("BBB000002"), "basis"]) == {"pit"}


def test_terms_export_is_validated(tmp_path: Path) -> None:
    terms = tmp_path / "terms.csv"
    terms.write_text("cusip9,coupon_rate\nAAA000001,6.5\nAAA000001,6.5\n", encoding="utf-8")
    with pytest.raises(builder.BuildError, match="terms_export_duplicate_cusip9"):
        builder.load_terms(terms)
    terms.write_text("cusip9,coupon_rate\nAAA000001,650\n", encoding="utf-8")
    with pytest.raises(builder.BuildError, match="terms_export_coupon_rate_out_of_percent_range"):
        builder.load_terms(terms)
    terms.write_text("cusip9,other\nAAA000001,1\n", encoding="utf-8")
    with pytest.raises(builder.BuildError, match="terms_export_missing_columns:coupon_rate"):
        builder.load_terms(terms)


def test_build_refuses_a_history_it_cannot_reproduce(tmp_path: Path) -> None:
    directory, hashes, _panel, stored = _artifact_dir(tmp_path)
    drifted = stored.copy()
    drifted.loc[drifted.index[0], "carry_return"] = float(drifted.loc[drifted.index[0], "carry_return"]) * 1.5
    pq.write_table(pa.Table.from_pandas(drifted, schema=RETURNS_SCHEMA, preserve_index=False), directory / "bond_monthly_returns.parquet")
    hashes["bond_monthly_returns.parquet"] = hashlib.sha256((directory / "bond_monthly_returns.parquet").read_bytes()).hexdigest()
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    manifest["artifact_sha256"] = hashes
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(builder.BuildError, match="stored_carry_not_reproduced:1"):
        builder.build(directory, tmp_path / "out", None, expected_hashes=hashes)

    drifted = stored.copy()
    drifted.loc[drifted.index[0], "price_return"] = float(drifted.loc[drifted.index[0], "price_return"]) + 1e-3
    pq.write_table(pa.Table.from_pandas(drifted, schema=RETURNS_SCHEMA, preserve_index=False), directory / "bond_monthly_returns.parquet")
    hashes["bond_monthly_returns.parquet"] = hashlib.sha256((directory / "bond_monthly_returns.parquet").read_bytes()).hexdigest()
    manifest["artifact_sha256"] = hashes
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(builder.BuildError, match="price_return_not_reproduced:1"):
        builder.build(directory, tmp_path / "out", None, expected_hashes=hashes)


def test_cli_refuses_unpinned_inputs_without_touching_the_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    directory, _hashes, _panel, _returns = _artifact_dir(tmp_path)
    assert builder.main(["--artifact-dir", str(directory), "--out", str(tmp_path / "out")]) == 2
    assert "artifact_sha256_mismatch" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()

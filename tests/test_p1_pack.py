"""Tests for the P1 certified input pack builder (open_macro_v03 pack v2).

Strict-TDD layout: tiny-fixture builder unit tests first, then the committed
real-pack coverage / governance / hash-tree / determinism / verify tests.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from harness.p1_pack import build as p1_build
from harness.p1_pack import verifier as p1_verifier
from harness.p1_pack.contract import P1_TABLE_SPECS, P1_TABLES_BY_NAME

ROOT = Path(__file__).resolve().parents[1]
P1_SOURCES = ROOT / "fixtures" / "p1_sources" / "open_macro_v03"
REAL_PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"
CONTRACT_BUNDLE_SHA256 = "db85c58968becd890d49d0a022b54b9493449e8c9ff444c88da10678c5d6f53b"


def _read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Tiny-fixture builder unit tests
# ---------------------------------------------------------------------------

def _tiny_sources(tmp_path: Path) -> Path:
    src = tmp_path / "sources"
    src.mkdir()
    macro = [
        # unsorted + a post-as_of vintage that must be filtered out
        {
            "series_id": "PPIFIS", "observation_period": "2026-04-01", "vintage_date": "2026-05-15",
            "value": 100.0, "available_at": "2026-05-15T00:00:00+00:00", "revision_number": 0,
            "source": "alfred", "source_spec_version": "macro_quadrant_us_v1.0",
        },
        {
            "series_id": "INDPRO", "observation_period": "2014-01-01", "vintage_date": "2014-02-19",
            "value": 99, "available_at": "2014-02-19T00:00:00+00:00", "revision_number": 0,
            "source": "alfred", "source_spec_version": "macro_quadrant_us_v1.0",
        },
        {
            "series_id": "INDPRO", "observation_period": "2026-05-01", "vintage_date": "2026-07-15",
            "value": 105, "available_at": "2026-07-15T00:00:00+00:00", "revision_number": 1,
            "source": "alfred", "source_spec_version": "macro_quadrant_us_v1.0",
        },
    ]
    eod = [
        {"ticker": "SPY", "date": "2006-01-03", "close": 100.0, "adjusted_close": 80.0, "volume": 1000},
        {"ticker": "DBC", "date": "2006-02-06", "close": 24.2, "adjusted_close": 19.1, "volume": 500},
        {"ticker": "SPY", "date": "2026-07-15", "close": 500.0, "adjusted_close": 500.0, "volume": 900},
    ]
    (src / "macro_observation_vintage.json").write_text(json.dumps(macro), encoding="utf-8")
    (src / "eod_prices.json").write_text(json.dumps(eod), encoding="utf-8")
    (src / "SOURCE.json").write_text(
        json.dumps(
            {
                "as_of": "2026-06-30",
                "export_id": "tiny_export_001",
                "source_commit": "abcdef1234567890abcdef1234567890abcdef12",
                "db_source": "tiger_test",
                "schema_version": 1,
                "tables": [],
            }
        ),
        encoding="utf-8",
    )
    return src


def test_contract_declares_two_p1_tables():
    names = [spec.name for spec in P1_TABLE_SPECS]
    assert names == ["macro_observation_vintage", "eod_prices"]
    mov = P1_TABLES_BY_NAME["macro_observation_vintage"]
    assert mov.key_columns == ("series_id", "observation_period", "vintage_date")
    assert P1_TABLES_BY_NAME["eod_prices"].key_columns == ("ticker", "date")


def test_builder_filters_post_as_of_and_sorts(tmp_path):
    src = _tiny_sources(tmp_path)
    out = tmp_path / "pack"
    p1_build.build_pack(sources=src, out=out)

    macro = _read(out / "data" / "canonical" / "macro_observation_vintage.json")
    # the 2026-07-15 vintage is > as_of 2026-06-30 and must be dropped
    assert len(macro) == 2
    keys = [(r["series_id"], r["observation_period"], r["vintage_date"]) for r in macro]
    assert keys == sorted(keys)
    assert ("INDPRO", "2026-05-01", "2026-07-15") not in keys

    eod = _read(out / "data" / "canonical" / "eod_prices.json")
    assert len(eod) == 2  # SPY 2026-07-15 dropped (> as_of)


def test_builder_raw_equals_canonical(tmp_path):
    src = _tiny_sources(tmp_path)
    out = tmp_path / "pack"
    p1_build.build_pack(sources=src, out=out)
    for spec in P1_TABLE_SPECS:
        raw = _read(out / "data" / "raw" / f"{spec.name}.json")
        canonical = _read(out / "data" / "canonical" / f"{spec.name}.json")
        assert raw == canonical


def test_builder_no_derived_layer(tmp_path):
    src = _tiny_sources(tmp_path)
    out = tmp_path / "pack"
    p1_build.build_pack(sources=src, out=out)
    assert not (out / "data" / "derived").exists()
    assert not (out / "derived_feature_manifest.json").exists()


def test_builder_pins_v2_bundle_and_governance(tmp_path):
    src = _tiny_sources(tmp_path)
    out = tmp_path / "pack"
    result = p1_build.build_pack(sources=src, out=out)
    manifest = _read(out / "manifest.json")
    assert manifest["contract_bundle_sha256"] == CONTRACT_BUNDLE_SHA256
    assert manifest["input_pack_version"] == 2
    assert manifest["input_pack_id"] == "open_macro_v03_certified_input_pack_002"
    assert manifest["A5"] == "blocked"
    assert manifest["runtime_activation"] is False
    assert manifest["activation_allowed"] is False
    assert manifest["official_result"] is False
    assert manifest["allocator_publish"] is False
    assert manifest["db_write_mode"] == "none"
    assert manifest["classification"] == "metric_evidence_only"
    assert manifest["source_export_id"] == "tiny_export_001"
    assert result["input_pack_sha256"] == manifest["input_pack_sha256"]


def test_builder_carries_p1_export_provenance(tmp_path):
    src = _tiny_sources(tmp_path)
    out = tmp_path / "pack"
    p1_build.build_pack(sources=src, out=out)
    source = _read(out / "SOURCE.json")
    assert source["p1_export"]["export_id"] == "tiny_export_001"
    assert source["builder_name"] == "certified-input-pack-builder-p1"


def test_builder_output_verifies(tmp_path):
    src = _tiny_sources(tmp_path)
    out = tmp_path / "pack"
    p1_build.build_pack(sources=src, out=out)
    result = p1_verifier.verify_pack(out)
    assert result["ok"], json.dumps(result, indent=2)


def test_builder_deterministic_byte_identical(tmp_path):
    src = _tiny_sources(tmp_path)
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    p1_build.build_pack(sources=src, out=out_a)
    p1_build.build_pack(sources=src, out=out_b)
    files_a = sorted(p.relative_to(out_a).as_posix() for p in out_a.rglob("*") if p.is_file())
    files_b = sorted(p.relative_to(out_b).as_posix() for p in out_b.rglob("*") if p.is_file())
    assert files_a == files_b
    for rel in files_a:
        assert (out_a / rel).read_bytes() == (out_b / rel).read_bytes(), rel


def test_verifier_rejects_runtime_activation_true(tmp_path):
    src = _tiny_sources(tmp_path)
    out = tmp_path / "pack"
    p1_build.build_pack(sources=src, out=out)
    manifest = _read(out / "manifest.json")
    manifest["runtime_activation"] = True
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    result = p1_verifier.verify_pack(out)
    assert not result["ok"]
    assert result["runtime_activation_ok"] is False


def test_verifier_rejects_tampered_data(tmp_path):
    src = _tiny_sources(tmp_path)
    out = tmp_path / "pack"
    p1_build.build_pack(sources=src, out=out)
    eod_path = out / "data" / "canonical" / "eod_prices.json"
    rows = _read(eod_path)
    rows[0]["close"] = 123456.0
    eod_path.write_text(json.dumps(rows), encoding="utf-8")
    result = p1_verifier.verify_pack(out)
    assert not result["ok"]


# ---------------------------------------------------------------------------
# Committed real-pack tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def real_manifest():
    return _read(REAL_PACK / "manifest.json")


def test_real_pack_exists():
    assert REAL_PACK.is_dir(), f"committed pack missing: {REAL_PACK}"
    assert (REAL_PACK / "manifest.json").is_file()


def test_real_pack_verifies():
    result = p1_verifier.verify_pack(REAL_PACK)
    assert result["ok"], json.dumps(result, indent=2)
    assert result["input_pack_sha256_match"] is True


def test_real_pack_governance_pins(real_manifest):
    assert real_manifest["input_pack_id"] == "open_macro_v03_certified_input_pack_002"
    assert real_manifest["input_pack_version"] == 2
    assert real_manifest["contract_bundle_sha256"] == CONTRACT_BUNDLE_SHA256
    assert real_manifest["A5"] == "blocked"
    assert real_manifest["runtime_activation"] is False
    assert real_manifest["activation_allowed"] is False
    assert real_manifest["official_result"] is False
    assert real_manifest["allocator_publish"] is False
    assert real_manifest["db_write_mode"] == "none"
    assert real_manifest["classification"] == "metric_evidence_only"
    assert real_manifest["has_derived_layer"] is False
    assert real_manifest["raw_equals_canonical"] is True


def test_real_pack_as_of_matches_source_export(real_manifest):
    source = _read(REAL_PACK / "SOURCE.json")
    assert real_manifest["as_of"] == "2026-06-30"
    assert source["p1_export"]["as_of"] == "2026-06-30"
    assert real_manifest["source_export_id"] == source["p1_export"]["export_id"]


def test_real_pack_rebuild_is_byte_identical(tmp_path):
    """Determinism regression: rebuild from committed snapshots == committed bytes."""
    out = tmp_path / "rebuild"
    p1_build.build_pack(sources=P1_SOURCES, out=out)
    committed = sorted(p.relative_to(REAL_PACK).as_posix() for p in REAL_PACK.rglob("*") if p.is_file())
    rebuilt = sorted(p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file())
    assert committed == rebuilt
    for rel in committed:
        assert (REAL_PACK / rel).read_bytes() == (out / rel).read_bytes(), f"byte mismatch in {rel}"


# --- coverage gates --------------------------------------------------------

def _macro_rows():
    return _read(REAL_PACK / "data" / "canonical" / "macro_observation_vintage.json")


def _eod_rows():
    return _read(REAL_PACK / "data" / "canonical" / "eod_prices.json")


SEED_SOURCES = ("ACOGNO", "AHETPI", "CPILFESL", "INDPRO", "MICH", "PAYEMS", "PCEC96", "PPIFIS")
SLEEVE_TICKERS = ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")


def test_coverage_vintage_all_eight_seed_sources_present():
    series = {r["series_id"] for r in _macro_rows()}
    assert series == set(SEED_SOURCES)


def test_coverage_vintage_ppifis_first_vintage_on_or_before_2014_03_01():
    ppifis = [r["vintage_date"] for r in _macro_rows() if r["series_id"] == "PPIFIS"]
    assert ppifis, "PPIFIS vintages missing"
    assert min(ppifis) <= "2014-03-01"
    # exact measured first vintage from harness_window_policy.json
    assert min(ppifis) == "2014-02-19"


def test_coverage_vintage_max_observation_period_on_or_after_2026_04_01():
    max_obs = max(r["observation_period"] for r in _macro_rows())
    assert max_obs >= "2026-04-01"


def test_coverage_vintage_per_series_first_vintage_matches_reference():
    policy = _read(
        ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_002" / "harness_window_policy.json"
    )
    expected = policy["measured_vintage_coverage"]["per_series_first_vintage"]
    rows = _macro_rows()
    for series, first in expected.items():
        firsts = [r["vintage_date"] for r in rows if r["series_id"] == series]
        assert firsts, f"{series} missing"
        assert min(firsts) == first, f"{series} first vintage {min(firsts)} != reference {first}"


def test_coverage_eod_all_six_sleeve_tickers_present():
    tickers = {r["ticker"] for r in _eod_rows()}
    assert tickers == set(SLEEVE_TICKERS)


def test_coverage_eod_full_sleeve_start_2006_02_06():
    """The full sleeve is available from DBC inception 2006-02-06 (window policy)."""
    rows = _eod_rows()
    per_ticker_min = {}
    for r in rows:
        t = r["ticker"]
        per_ticker_min[t] = min(per_ticker_min.get(t, "9999"), r["date"])
    latest_start = max(per_ticker_min.values())
    assert latest_start == "2006-02-06"  # DBC is the constraining ticker


def test_coverage_eod_per_ticker_matches_snapshot_min():
    """Per-ticker coverage min matches the committed snapshot (export min-date clamped at 1998)."""
    rows = _eod_rows()
    per_ticker = {}
    for r in rows:
        t = r["ticker"]
        cur = per_ticker.setdefault(t, {"min": r["date"], "max": r["date"]})
        cur["min"] = min(cur["min"], r["date"])
        cur["max"] = max(cur["max"], r["date"])
    expected_min = {
        "SPY": "1998-01-02",  # export clamped at min_date 1998-01-01 (DB min is 1993-01-29)
        "TLT": "2002-07-26",
        "SHY": "2002-07-26",
        "TIP": "2003-12-05",
        "GLD": "2004-11-18",
        "DBC": "2006-02-06",
    }
    for ticker, mn in expected_min.items():
        assert per_ticker[ticker]["min"] == mn, ticker
        assert per_ticker[ticker]["max"] == "2026-06-30"  # as_of clamp


def test_coverage_eod_max_date_matches_as_of(real_manifest):
    rows = _eod_rows()
    assert max(r["date"] for r in rows) == real_manifest["as_of"]


# --- hash-tree integrity ---------------------------------------------------

def test_real_pack_table_hashes_cover_all_data_and_match(real_manifest):
    table_hashes = _read(REAL_PACK / "table_hashes.json")
    from src.input_packs.hashing import file_sha256

    for table in table_hashes["tables"]:
        path = REAL_PACK / table["path"]
        assert path.exists(), table["path"]
        assert file_sha256(path) == table["sha256"], table["path"]
    data_paths = {t["path"] for t in table_hashes["tables"] if t["path"].startswith("data/")}
    assert "data/raw/macro_observation_vintage.json" in data_paths
    assert "data/canonical/eod_prices.json" in data_paths
    assert not any(p.startswith("data/derived/") for p in data_paths)


def test_real_pack_component_snapshot_hashes(real_manifest):
    from src.input_packs.hashing import file_sha256

    assert real_manifest["raw_snapshot_sha256"] == file_sha256(REAL_PACK / "raw_snapshot_manifest.json")
    assert real_manifest["canonical_snapshot_sha256"] == file_sha256(REAL_PACK / "canonical_snapshot_manifest.json")


# --- CLI -------------------------------------------------------------------

def test_cli_builds_pack(tmp_path):
    out = tmp_path / "cli_pack"
    proc = subprocess.run(
        [
            sys.executable, "-m", "harness.p1_pack.build",
            "--sources", str(P1_SOURCES),
            "--out", str(out),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["input_pack_id"] == "open_macro_v03_certified_input_pack_002"
    assert p1_verifier.verify_pack(out)["ok"]

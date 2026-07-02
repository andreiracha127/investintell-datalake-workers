"""TDD suite for the compression mini-grid measurement package (compression_grid_001).

Locks the two quant_owner decisions (Andrei Rachadel, 2026-07-02):

  DECISION A - compression mini-grid: measure sleeve variants baseline_100 /
    compressed_75 / compressed_50 / compressed_25 (compressed_N = N% of the original
    inter-quadrant distance RETAINED; compression factor = 1 - N/100). Same decision
    chain, same pack v2, same policy (carry semantics), local leg. compressed_50 is
    the leading alternative candidate and MUST numerically equal evidence_002's
    sleeve_compressed_50 cells (consistency test). Nothing replaces the baseline.
  DECISION B - full per-fold OOS report: for EVERY fold and EVERY variant at 5bps,
    the 11-field table. Bounds stay untouched; OOS verdict stays
    no_go_bounds_under_review.

Governance: evidence_001 AND evidence_002 are IMMUTABLE (git-blob hash-pinned); this
package writes only the new compression_grid_001 dir. A5 stays blocked; no artifact
carries an activation/approval marker.

Network-free, DB-free, deterministic.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import subprocess
from pathlib import Path

import pytest

from harness.phase0q import grid, runner, sleeve

ROOT = Path(__file__).resolve().parents[1]
PACK_DIR = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"
EVIDENCE_001 = ROOT / "artifacts" / "quant" / "open_macro_v03_metric_evidence_001"
EVIDENCE_002 = ROOT / "artifacts" / "quant" / "open_macro_v03_metric_evidence_002"
GRID = ROOT / "artifacts" / "quant" / "open_macro_v03_compression_grid_001"

EVIDENCE_001_GIT_PREFIX = "artifacts/quant/open_macro_v03_metric_evidence_001"
EVIDENCE_002_GIT_PREFIX = "artifacts/quant/open_macro_v03_metric_evidence_002"


# --------------------------------------------------------------------------- #
# Shared measurement fixture (compute the decision chain + prices ONCE)         #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def measured():
    pack = runner.load_and_verify_pack(PACK_DIR)
    prices = sleeve.PriceFrame(pack.eod_rows)
    all_windows = [runner.PRIMARY_WINDOW]
    all_windows += [(w["start"], w["end"]) for w in runner.STRESS_WINDOWS]
    decisions = runner.build_decision_series(pack, all_windows)
    params = sleeve.SleeveParams("baseline_current", 0.5, 0.5, 0.0, 0.0, 0.0)
    grid_results = grid.measure_grid_results(prices, decisions, params)
    oos = {vid: grid.measure_oos_fold_report(prices, decisions, params, vid)
           for vid in grid.VARIANT_FACTORS}
    return {"prices": prices, "decisions": decisions, "params": params,
            "grid": grid_results, "oos": oos}


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git(*args: str) -> bytes:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True,
                          check=True).stdout


# --------------------------------------------------------------------------- #
# Naming convention                                                            #
# --------------------------------------------------------------------------- #

def test_naming_convention_factor_is_one_minus_retained_fraction():
    """compressed_N means N% of the inter-quadrant distance RETAINED, so the
    compression factor (fraction moved toward the mean) is 1 - N/100."""
    for vid, factor in grid.VARIANT_FACTORS.items():
        retained = grid.VARIANT_RETAINED_PCT[vid]
        assert factor == pytest.approx(1.0 - retained / 100.0)
    assert grid.VARIANT_FACTORS["baseline_100"] == 0.0
    assert grid.VARIANT_FACTORS["compressed_50"] == 0.5
    assert grid.VARIANT_FACTORS["compressed_25"] == 0.75  # most compressed


def test_baseline_100_book_is_identity_of_baseline_weights():
    """baseline_100 applies no compression: its book is the untouched
    PER_QUADRANT_BASELINE_WEIGHTS."""
    book = grid.variant_book("baseline_100")
    assert book == {k: dict(v) for k, v in sleeve.PER_QUADRANT_BASELINE_WEIGHTS.items()}


def test_compressed_50_book_equals_sleeve_compressed_50():
    """compressed_50 == sleeve_compressed_50 (factor 0.5) at the book level."""
    assert grid.variant_book("compressed_50") == sleeve.compressed_quadrant_weights(0.5)


# --------------------------------------------------------------------------- #
# DECISION A - consistency: compressed_50 == evidence_002 sleeve_compressed_50 #
# --------------------------------------------------------------------------- #

def test_compressed_50_cells_numerically_equal_evidence_002(measured):
    """The freshly measured compressed_50 primary cost grid must equal evidence_002's
    sleeve_compressed_50 cells byte-for-metric (naming-convention consistency)."""
    ev002 = _json(EVIDENCE_002 / "compressed_sleeve_alternative.json")
    ref = ev002["sleeve_compressed_50"]["by_cost_bps"]
    got = measured["grid"]["compressed_50"]["cost_grid"]["by_cost_bps"]
    for cb in ("0", "5", "10", "25"):
        for key in ("annualized_turnover", "annualized_volatility", "max_drawdown",
                    "window_return", "total_one_way_turnover", "worst_5d_return"):
            assert got[cb][key] == pytest.approx(ref[cb][key], abs=1e-9), (
                f"compressed_50 {cb}bps {key} diverges from evidence_002")


def test_baseline_100_cells_numerically_equal_evidence_002_baseline(measured):
    """baseline_100 (no compression) must equal evidence_002's baseline_sleeve cells:
    same decision chain, same pack, same policy."""
    ev002 = _json(EVIDENCE_002 / "compressed_sleeve_alternative.json")
    ref = ev002["baseline_sleeve"]["by_cost_bps"]
    got = measured["grid"]["baseline_100"]["cost_grid"]["by_cost_bps"]
    for cb in ("0", "5", "10", "25"):
        for key in ("annualized_turnover", "annualized_volatility", "max_drawdown",
                    "window_return"):
            assert got[cb][key] == pytest.approx(ref[cb][key], abs=1e-9)


def test_grid_results_artifact_compressed_50_matches_evidence_002():
    """The COMMITTED grid_results.json compressed_50 cells equal evidence_002."""
    results = _json(GRID / "grid_results.json")
    ev002 = _json(EVIDENCE_002 / "compressed_sleeve_alternative.json")
    ref = ev002["sleeve_compressed_50"]["by_cost_bps"]
    got = results["variants"]["compressed_50"]["cost_grid"]["by_cost_bps"]
    for cb in ("0", "5", "10", "25"):
        assert got[cb]["annualized_turnover"] == pytest.approx(
            ref[cb]["annualized_turnover"], abs=1e-9)
        assert got[cb]["annualized_volatility"] == pytest.approx(
            ref[cb]["annualized_volatility"], abs=1e-9)


def test_grid_turnover_is_monotone_decreasing_in_compression(measured):
    """More compression -> strictly less annualized turnover at 5bps
    (baseline_100 > compressed_75 > compressed_50 > compressed_25)."""
    g = measured["grid"]
    order = ["baseline_100", "compressed_75", "compressed_50", "compressed_25"]
    turns = [g[v]["cost_grid"]["by_cost_bps"]["5"]["annualized_turnover"] for v in order]
    assert turns == sorted(turns, reverse=True)
    assert all(a > b for a, b in zip(turns, turns[1:]))


# --------------------------------------------------------------------------- #
# DECISION A - constraint re-checks per variant                                #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("variant_id", list(grid.VARIANT_FACTORS))
def test_variant_constraints_weights_sum_one_and_bounds(variant_id):
    """Every quadrant target of every variant sums to 1 and satisfies risk_cap 0.65 /
    defensive_floor 0.20 after enforcement (post-compression re-check)."""
    params = sleeve.SleeveParams("baseline_current", 0.5, 0.5, 0.0, 0.0, 0.0)
    check = grid.verify_variant_constraints(variant_id, params)
    assert check["all_constraints_satisfied"] is True
    for quadrant, q in check["per_quadrant"].items():
        assert q["weights_sum_to_one"], f"{variant_id}/{quadrant} sum != 1"
        assert q["risk_cap_satisfied"], f"{variant_id}/{quadrant} risk_cap"
        assert q["defensive_floor_satisfied"], f"{variant_id}/{quadrant} defensive_floor"


def test_constraint_check_records_renormalization_flag(measured):
    """If a variant would violate a bound the enforcement renormalizes and the check
    RECORDS it (never silently passes). Compression moves toward the centroid so no
    bound binds here, and the recorded flag is False — but the field must exist for
    every quadrant so a real violation would be visible."""
    for variant_id in grid.VARIANT_FACTORS:
        check = measured["grid"][variant_id]["constraints"]
        assert "any_renormalization_applied" in check
        for q in check["per_quadrant"].values():
            assert "renormalization_applied" in q


def test_grid_results_artifact_constraints_all_satisfied():
    """The COMMITTED grid_results.json records constraint verification per variant."""
    results = _json(GRID / "grid_results.json")
    for variant_id in grid.VARIANT_FACTORS:
        con = results["variants"][variant_id]["constraints"]
        assert con["all_constraints_satisfied"] is True
        assert con["risk_cap"] == pytest.approx(0.65)
        assert con["defensive_floor"] == pytest.approx(0.20)


# --------------------------------------------------------------------------- #
# DECISION B - OOS fold report field completeness (11 fields, every fold)       #
# --------------------------------------------------------------------------- #

REQUIRED_FOLD_FIELDS = (
    "initial_carried_position_quadrant",
    "last_valid_decision_before_fold_start",
    "fresh_decisions_in_fold",
    "carry_pct_of_scheduled_dates",
    "economic_turnover_excl_initial_acquisition",
    "MDD",
    "volatility",
    "return_annualized",
    "dominant_regime",
    "absolute_bounds",
    "stability_bounds",
)


def test_oos_fold_report_has_all_11_fields_every_fold_every_variant(measured):
    """DECISION B: for EVERY variant and EVERY fold, all 11 fields are present."""
    for variant_id in grid.VARIANT_FACTORS:
        report = measured["oos"][variant_id]
        assert report["folds"], f"{variant_id} has no folds"
        for fold in report["folds"]:
            for field in REQUIRED_FOLD_FIELDS:
                assert field in fold, f"{variant_id} fold {fold['fold_index']} missing {field}"


def test_oos_fold_seed_precedes_fold_start_or_explicitly_unseeded(measured):
    """Each fold's seeding decision date is strictly BEFORE the fold test start
    (carry of the last valid pre-fold position); a fold with no prior valid decision
    is explicitly unseeded_no_prior_valid_position (never a future seed)."""
    for variant_id in grid.VARIANT_FACTORS:
        for fold in measured["oos"][variant_id]["folds"]:
            seed = fold["last_valid_decision_before_fold_start"]
            if fold["initial_carried_position_source"] == "unseeded_no_prior_valid_position":
                continue
            assert seed is not None
            assert dt.date.fromisoformat(seed) < dt.date.fromisoformat(fold["test_start"]), (
                f"{variant_id} fold {fold['fold_index']} seed {seed} not before start")


def test_oos_fold_report_excludes_initial_acquisition_and_no_lookahead(measured):
    """Fold economic turnover excludes the initial empty->position acquisition and
    there is no lookahead; no empty-portfolio transition is counted."""
    for variant_id in grid.VARIANT_FACTORS:
        report = measured["oos"][variant_id]
        assert report["initial_acquisition_excluded_from_fold_turnover"] is True
        assert report["no_lookahead"] is True
        assert report["empty_portfolio_transition_counted"] is False


def test_oos_verdict_stays_no_go_bounds_under_review():
    """Bounds are untouched and the OOS verdict label stays
    no_go_bounds_under_review in the committed report."""
    report = _json(GRID / "oos_fold_report.json")
    assert report["oos_verdict"] == "no_go_bounds_under_review"
    assert report["bounds_unchanged"] is True


def test_committed_oos_fold_report_field_completeness():
    """The COMMITTED oos_fold_report.json carries all 11 fields for all 4 variants,
    every fold."""
    report = _json(GRID / "oos_fold_report.json")
    for variant_id in grid.VARIANT_FACTORS:
        folds = report["variants"][variant_id]["folds"]
        assert folds
        for fold in folds:
            for field in REQUIRED_FOLD_FIELDS:
                assert field in fold, f"{variant_id} fold missing {field}"


def test_dominant_regime_is_a_real_quadrant(measured):
    """dominant_regime (most-held quadrant by days) is one of the four quadrant
    labels (or None only for an empty fold, which does not occur here)."""
    labels = {"recovery", "expansion", "slowdown", "contraction"}
    for variant_id in grid.VARIANT_FACTORS:
        for fold in measured["oos"][variant_id]["folds"]:
            assert fold["dominant_regime"] in labels


# --------------------------------------------------------------------------- #
# Determinism - regeneration is byte-identical (git blob comparison)           #
# --------------------------------------------------------------------------- #

def test_grid_results_regeneration_is_byte_identical(measured):
    """Re-building the grid_results payload from the SAME measured grid yields JSON
    byte-identical to the committed blob (deterministic; no wall-clock, no RNG)."""
    committed = _git("cat-file", "blob",
                     f"HEAD:artifacts/quant/open_macro_v03_compression_grid_001/grid_results.json")
    manifest = _json(GRID / "compression_grid_manifest.json")
    harness_commit = manifest["provenance"]["harness_commit"]
    payload = grid.build_grid_results_payload(measured["grid"], harness_commit)
    regenerated = runner.canonical_json(payload).encode("utf-8")
    assert regenerated == committed, "grid_results.json is not deterministic"


def test_oos_fold_report_regeneration_is_byte_identical(measured):
    """Re-building the oos_fold_report payload is byte-identical to the committed blob."""
    committed = _git("cat-file", "blob",
                     f"HEAD:artifacts/quant/open_macro_v03_compression_grid_001/oos_fold_report.json")
    manifest = _json(GRID / "compression_grid_manifest.json")
    harness_commit = manifest["provenance"]["harness_commit"]
    payload = grid.build_oos_fold_report_payload(measured["oos"], harness_commit)
    regenerated = runner.canonical_json(payload).encode("utf-8")
    assert regenerated == committed, "oos_fold_report.json is not deterministic"


# --------------------------------------------------------------------------- #
# Immutability pins - evidence_001 AND evidence_002 (git blob hashes)          #
# --------------------------------------------------------------------------- #

def test_evidence_001_is_immutable_git_blob_pinned():
    """evidence_001 stays byte-immutable: reuse the amendments blob-pin mechanism."""
    from harness.phase0q import amendments
    tracked = _git("ls-tree", "-r", "--name-only", "HEAD",
                   EVIDENCE_001_GIT_PREFIX).decode("utf-8").split()
    rel = {t.replace(EVIDENCE_001_GIT_PREFIX + "/", "", 1) for t in tracked}
    assert rel == set(amendments.EVIDENCE_001_SHA256)
    for key, pin in amendments.EVIDENCE_001_SHA256.items():
        blob = _git("cat-file", "blob", f"HEAD:{EVIDENCE_001_GIT_PREFIX}/{key}")
        assert hashlib.sha256(blob).hexdigest() == pin, f"evidence_001 mutated: {key}"


def test_evidence_002_is_immutable_git_blob_pinned():
    """evidence_002 is now IMMUTABLE too: every committed file matches its pinned
    sha256 over the git blob bytes (checkout-independent; same mechanism as
    evidence_001). This package must never edit evidence_002."""
    tracked = _git("ls-tree", "-r", "--name-only", "HEAD",
                   EVIDENCE_002_GIT_PREFIX).decode("utf-8").split()
    rel = {t.replace(EVIDENCE_002_GIT_PREFIX + "/", "", 1) for t in tracked}
    assert rel == set(grid.EVIDENCE_002_SHA256), (
        "evidence_002 committed file set changed vs the immutability pin")
    assert len(grid.EVIDENCE_002_SHA256) >= 4
    for key, pin in grid.EVIDENCE_002_SHA256.items():
        blob = _git("cat-file", "blob", f"HEAD:{EVIDENCE_002_GIT_PREFIX}/{key}")
        assert hashlib.sha256(blob).hexdigest() == pin, f"evidence_002 mutated: {key}"
    # working copy still parses to the same JSON as the committed blob.
    for key in grid.EVIDENCE_002_SHA256:
        blob = _git("cat-file", "blob", f"HEAD:{EVIDENCE_002_GIT_PREFIX}/{key}")
        disk = (EVIDENCE_002 / key).read_text(encoding="utf-8")
        assert json.loads(disk) == json.loads(blob.decode("utf-8")), (
            f"evidence_002 working copy diverges: {key}")


# --------------------------------------------------------------------------- #
# Governance markers (whitespace-tolerant regex + recursive JSON walk)          #
# --------------------------------------------------------------------------- #

FORBIDDEN_MARKER_PATTERNS = (
    r'"runtime_activation"\s*:\s*true',
    r'"activation_allowed"\s*:\s*true',
    r'"allocator_publish"\s*:\s*true',
    r'"official_result"\s*:\s*true',
    r'"freeze_ready"\s*:\s*true',
    r'"approved"\s*:\s*true',
    r'"replaces_baseline"\s*:\s*true',
    r'"A5"\s*:\s*"unblocked"',
    r'"db_write_mode"\s*:\s*"write',
    r"A5=unblocked",
)

_FORBIDDEN_TRUE_FIELDS = frozenset({
    "runtime_activation", "activation_allowed", "allocator_publish",
    "official_result", "freeze_ready", "approved", "replaces_baseline",
})


def _walk_forbidden(node, path=""):
    if isinstance(node, dict):
        for key, value in node.items():
            where = f"{path}.{key}" if path else key
            if key in _FORBIDDEN_TRUE_FIELDS:
                assert value is not True, f"{where} is true"
            if key == "A5":
                assert value == "blocked", f"{where} = {value!r}"
            if key == "db_write_mode":
                assert value == "none", f"{where} = {value!r}"
            _walk_forbidden(value, where)
    elif isinstance(node, list):
        for i, value in enumerate(node):
            _walk_forbidden(value, f"{path}[{i}]")


def test_forbidden_marker_regex_matches_minified_and_pretty():
    """Self-check: the whitespace-tolerant patterns catch both minified and pretty
    activation flags."""
    pat = FORBIDDEN_MARKER_PATTERNS[0]
    assert re.search(pat, '{"runtime_activation":true}')
    assert re.search(pat, '{"runtime_activation": true}')
    assert not re.search(pat, '{"runtime_activation":false}')


def test_grid_artifacts_have_no_activation_or_approval_markers():
    """No compression_grid_001 artifact may carry an activation/approval marker in
    either minified or pretty form; every JSON file also passes a recursive
    field-value check. The scan must cover >= the expected file count."""
    json_scanned = 0
    total_scanned = 0
    for path in sorted(GRID.rglob("*")):
        if not path.is_file():
            continue
        total_scanned += 1
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_MARKER_PATTERNS:
            assert not re.search(pattern, text), f"{path.name} matches {pattern}"
        if path.suffix == ".json":
            _walk_forbidden(json.loads(text))
            json_scanned += 1
    # 3 json deliverables (manifest, grid_results, oos_fold_report) + 1 md.
    assert json_scanned >= 3, f"expected >= 3 json files, got {json_scanned}"
    assert total_scanned >= 4, f"expected >= 4 files (incl .md), got {total_scanned}"


def test_manifest_governance_pins_and_provenance():
    """The manifest pins governance (A5 blocked, activation/approval false,
    replaces_baseline false, compressed_50 leading candidate) and the real
    provenance (pack sha, bundle v2 sha, a real harness commit)."""
    manifest = _json(GRID / "compression_grid_manifest.json")
    gov = manifest["governance"]
    assert gov["A5"] == "blocked"
    assert gov["runtime_activation"] is False
    assert gov["activation_allowed"] is False
    assert gov["allocator_publish"] is False
    assert gov["official_result"] is False
    assert gov["db_write_mode"] == "none"
    assert gov["approved"] is False
    assert gov["replaces_baseline"] is False
    assert gov["status"] == "candidate_not_approved"
    assert manifest["leading_alternative_candidate"] == "compressed_50"
    prov = manifest["provenance"]
    assert prov["input_pack_sha256"] == grid.PACK_SHA256
    assert prov["contract_bundle_sha256"] == grid.CONTRACT_BUNDLE_V2_SHA256
    assert re.fullmatch(r"[0-9a-f]{7,40}", prov["harness_commit"])


def test_manifest_harness_commit_is_ancestor_of_head():
    """The manifest harness_commit must be a real commit that is an ancestor of HEAD
    (two-step: commit code, regenerate with that SHA, commit evidence)."""
    manifest = _json(GRID / "compression_grid_manifest.json")
    sha = manifest["provenance"]["harness_commit"]
    full = _git("rev-parse", "--verify", f"{sha}^{{commit}}").decode().strip()
    subprocess.run(["git", "merge-base", "--is-ancestor", full, "HEAD"],
                   cwd=ROOT, check=True)


def test_no_forbidden_records_created():
    """This package must NOT create a review_closure_record.json or unblock Task 2."""
    for path in GRID.rglob("*"):
        if path.is_file():
            assert path.name != "review_closure_record.json"


def test_expected_deliverable_files_present():
    """The compression_grid_001 dir contains at least the four deliverables."""
    assert (GRID / "compression_grid_manifest.json").exists()
    assert (GRID / "grid_results.json").exists()
    assert (GRID / "oos_fold_report.json").exists()
    assert (GRID / "compression_tradeoff_summary.md").exists()
    files = {p.name for p in GRID.rglob("*") if p.is_file()}
    assert len(files) >= 4

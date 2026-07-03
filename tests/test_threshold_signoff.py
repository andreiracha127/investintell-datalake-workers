"""Pins for artifacts/quant/open_macro_v03_threshold_signoff_001.

The threshold sign-off is the quant_owner's human act (2026-07-03) freezing the
base-profile envelope — as amended by phase0q_003 (carry/turnover) and phase0q_004
(OOS jackknife) — as the OFFICIAL candidate thresholds for the dark launch path.
It approves thresholds ONLY: activation stays blocked, and the record must say so.
The envelope and measured values must equal the committed phase0q_004 judgment
(the sign-off freezes that judgment; any divergence is a fabrication signal), and
the judgment blob it cites must be the exact committed bytes (git-blob pin).
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SIGNOFF = (ROOT / "artifacts" / "quant" / "open_macro_v03_threshold_signoff_001" /
           "threshold_signoff_record.json")
JUDGMENT = (ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_004" /
            "quantitative_gate_judgment.phase0q_004.json")


def _signoff() -> dict[str, Any]:
    payload = json.loads(SIGNOFF.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_signoff_is_the_quant_owner_act_with_threshold_only_scope() -> None:
    record = _signoff()

    assert record["signed_off_by"] == "Andrei Rachadel"
    assert record["signed_off_by_role"] == "quant_owner"
    assert record["signoff_date"] == "2026-07-03"
    assert record["threshold_envelope_approved"] is True
    # thresholds ONLY — activation approval must be explicitly false
    assert record["activation_approved"] is False
    assert record["institutional_activation_approval"] is False
    assert "THRESHOLDS ONLY" in record["scope"]
    assert "does NOT close the review_closure_record" in record["effect"]

    gov = record["governance"]
    assert gov["A5"] == "blocked"
    assert gov["runtime_activation"] is False
    assert gov["activation_allowed"] is False
    assert gov["allocator_publish"] is False
    assert gov["official_result"] is False
    assert gov["freeze_ready"] is False
    assert gov["db_write_mode"] == "none"


def test_signoff_envelope_equals_the_committed_phase0q_004_judgment() -> None:
    record = _signoff()
    judgment = json.loads(JUDGMENT.read_text(encoding="utf-8"))

    assert record["judged_sleeve"] == judgment["judged_sleeve"] == "compressed_50"

    absolute = record["envelope"]["absolute"]
    base = judgment["base_profile"]
    assert absolute["max_drawdown"] == base["max_drawdown"]
    assert absolute["max_annualized_volatility"] == base["max_annualized_volatility"]
    assert absolute["signal_design_turnover_bound"] == base["max_one_way_turnover_annualized"]
    assert absolute["min_worst_5d_return"] == base["min_worst_5d_return"]
    assert absolute["reference_sleeve_turnover_candidate_bound"] == (
        judgment["gates"]["turnover"]["reference_sleeve_turnover_candidate_bound"])

    oos = record["envelope"]["oos_jackknife_semantics"]
    assert oos["max_fold_mdd_deviation"] == base["max_fold_mdd_deviation"]
    assert oos["max_fold_volatility_deviation"] == base["max_fold_volatility_deviation"]

    measured = record["measured_at_signoff"]
    assert measured["turnover"] == judgment["gates"]["turnover"]["measured"]
    assert measured["max_drawdown"] == judgment["gates"]["drawdown"]["measured"]
    assert measured["annualized_volatility"] == judgment["gates"]["volatility"]["measured"]
    assert measured["overall_recommendation"] == judgment["overall_recommendation"]
    oos_measured = judgment["gates"]["out_of_sample"]["measured"]
    assert measured["oos_mdd_max_dev_eligible"] == (
        oos_measured["mdd_stability"]["eligible_folds"]["max_dev_from_median"])
    assert measured["oos_volatility_max_dev_eligible"] == (
        oos_measured["volatility_stability"]["eligible_folds"]["max_dev_from_median"])
    assert measured["stress_windows_go"] == sum(
        1 for w in judgment["gates"]["stress"]["windows"].values() if w["go"])


def test_signoff_git_blob_pins_match_the_committed_bytes() -> None:
    record = _signoff()
    pinned = {ref["path"]: ref["git_blob_sha"] for ref in record["evidence_refs"]
              if "git_blob_sha" in ref}
    assert pinned, "at least the judgment blob must be git-pinned"
    for path, expected in pinned.items():
        actual = subprocess.run(
            ["git", "rev-parse", f"HEAD:{path}"],
            cwd=ROOT, capture_output=True, text=True)
        assert actual.returncode == 0, path
        assert actual.stdout.strip() == expected, f"{path} blob diverges from sign-off pin"

"""Builder for the Stage A latency-SLO signed threshold amendment record.

Usage (normally invoked by ``measure_stage_a.py --amend-latency``, but runnable
standalone for inspection):

    python -m harness.direct_activation.build_stage_a_amendment \
        --p95-ms 83720.102 --runs-total 16 --reproducibility-sha256 <sha> [--out FILE]

Why this record exists (workload identity, NOT a regression): the Phase 1
``monitoring_thresholds_record.json`` latency SLO (37826 ms) was derived from the
quant-engine ``a3_qc_parity`` job (measured p95 25217.177 ms) — a DIFFERENT workload
from the Stage A executor, which reconstitutes the full 148-month latched decision
chain (~83 s) in ``live_validation.compute()``. The amendment reapplies the SAME
Phase 1 Task 4 derivation rule — ``ceil(1.5 x measured p95)`` — to the correct
workload; it is never an ad hoc bump to "make it pass". The memory / error-rate /
retry-rate SLOs are inherited from Phase 1 UNCHANGED and stay fail-loud.

The builder is deterministic given its inputs (measured p95, runs_total, and the
sha256 of the reproducibility record that pins the measuring round), so the committed
artifact is reproducible from the round it cites. Governance: the amendment flips
NOTHING — A5 stays blocked; ratification is the Stage A PR merge per the plan's
signed-amendment rule.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
STAGE_A = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_a_001"
THRESHOLDS = (ROOT / "artifacts" / "a5" / "open_macro_v03_dark_launch_001"
              / "monitoring_thresholds_record.json")
LIVE_VALIDATION_RECORD = STAGE_A / "live_validation_record.json"
DIRECT_ACTIVATION_ID = "open_macro_v03_direct_activation_001"

# The six owner roles with their EXACT pinned holders, per
# artifacts/a5/open_macro_v03_dark_launch_001/owners_assignment_record.json
# (all six roles held by a single owner; single-operator governance, recorded there).
OWNER_ROLES = ("technical_owner", "quant_owner", "risk_owner", "operations_owner",
               "product_portfolio_owner", "final_approver")
OWNER_HOLDER = "Andrei Rachadel"

INHERITED_SLO_IDS = ("memory_slo", "error_rate_slo", "retry_rate_slo")


def _sha256_file(path: Path) -> str:
    # CRLF-normalized so the pin is checkout-independent (the canonical bytes are
    # the LF git blob; a Windows autocrlf checkout must hash identically).
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def build(p95_ms: float, runs_total: int,
          reproducibility_record_sha256: str) -> dict[str, Any]:
    """Deterministic amendment record for the given measured round."""
    thresholds_doc = json.loads(THRESHOLDS.read_text(encoding="utf-8"))
    slos = {slo["id"]: slo for slo in thresholds_doc["slos"]}
    phase1_latency_threshold = slos["latency_slo"]["threshold"]
    amended_threshold = int(math.ceil(1.5 * p95_ms))

    governance = json.loads(
        LIVE_VALIDATION_RECORD.read_text(encoding="utf-8"))["governance"]

    return {
        "artifact_type": "stage_a_slo_threshold_amendment_record",
        "schema_version": 1,
        "stage": "A",
        "direct_activation_id": DIRECT_ACTIVATION_ID,
        "amended_slo": {
            "id": "latency_slo",
            "metric": "candidate_compute_latency_p95_ms",
            "scope": "stage_a_live_validation + open_macro_v03 runtime worker (B5)",
            "threshold": amended_threshold,
            "derivation": f"ceil(1.5 x measured p95 {p95_ms} ms) — the SAME Phase 1 "
                           f"Task 4 rule reapplied to the Stage A workload",
        },
        "rationale": (
            "Workload identity, not a regression: the Phase 1 latency threshold "
            f"({phase1_latency_threshold} ms) was derived from the quant-engine "
            "a3_qc_parity job (measured p95 25217.177 ms), a DIFFERENT workload from "
            "the Stage A executor, which reconstitutes the full 148-month latched "
            "decision chain (~83 s) in live_validation.compute(). This amendment "
            "reapplies the SAME derivation rule — ceil(1.5 x measured p95) — to the "
            "correct workload; it is never an ad hoc bump to make a round pass."
        ),
        "inherited_unchanged": {
            "source": {
                "path": THRESHOLDS.relative_to(ROOT).as_posix(),
                "sha256": _sha256_file(THRESHOLDS),
            },
            "slos": [{"id": sid, "metric": slos[sid]["metric"],
                      "threshold": slos[sid]["threshold"]}
                     for sid in INHERITED_SLO_IDS],
        },
        "measured_round": {
            "runs_total": runs_total,
            "p95_ms": p95_ms,
            "reproducibility_record_sha256": reproducibility_record_sha256,
        },
        "approval_matrix": {
            "approvals": [{"role": role, "owner": OWNER_HOLDER}
                          for role in OWNER_ROLES],
            "approval_matrix_complete": True,
            "note": "ratified by the Stage A PR merge per the plan's "
                    "signed-amendment rule",
        },
        "governance": governance,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--p95-ms", type=float, required=True)
    parser.add_argument("--runs-total", type=int, required=True)
    parser.add_argument("--reproducibility-sha256", required=True)
    parser.add_argument("--out", default=None,
                        help="write here instead of printing to stdout")
    args = parser.parse_args(argv)

    record = build(args.p95_ms, args.runs_total, args.reproducibility_sha256)
    payload = json.dumps(record, sort_keys=True, indent=1, ensure_ascii=False) + "\n"
    if args.out:
        Path(args.out).write_text(payload, encoding="utf-8", newline="\n")
        print(f"amendment record -> {args.out}")
    else:
        print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

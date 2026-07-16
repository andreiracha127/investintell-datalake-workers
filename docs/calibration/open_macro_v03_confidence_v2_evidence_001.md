# open_macro_v03 — Confidence v2 gate evidence 001 + activation decision

Status: measured evidence + quant_owner adjudication · Date: 2026-07-16
Authority: quant_owner (Andrei Rachadel), execution delegated in-session 2026-07-16.
Evidence: `artifacts/quant/open_macro_v03_confidence_v2_evidence_001/` (harness run
`runner.run_harness`, `decision_model="v2"`, pack `_003`, policy phase0q_006 GATING).
Model under judgment: `macro_quadrant_us_v2` / `confidence_v2.0`
(`src/quadrant_confidence_v2.py` + `src/quadrant_assemble_v2.py` +
`src/workers/quadrant_macro_v2.py`; harness parity `harness/phase0q/decision_v2.py`).

## Timeline gates (phase0q_006, amended same-quadrant semantics)

The harness's blocking judgment runs over the full union chain (2007-10..2026-06).
2007-2013 is `unavailable` for BOTH v1 and v2 (pack coverage < 0.80 — the chain-head
abstention streak measures the pack's data horizon, not the model). Windowed addenda
are recorded in the evidence summary; the precedent basis (the certified 2021-2026
window on which the frozen v1 was judged and failed 22/66, 18/18/38) reads:

| gate | bound | frozen v1 (2021-26) | **v2 (2021-26)** | v2 (runtime chain 2014-03+) |
|---|---|---|---|---|
| fresh_valid_36m | >= 0.40 | 0.1667 ✗ | **0.8333 ✓** | 0.8333 ✓ |
| abstention streak | <= 6 | 18 ✗ | **3 ✓** | 5 ✓ |
| carry age | <= 3 | 18 ✗ | **3 ✓** | 5 ✗ (2016-08→2017-02, filter warmup era) |
| same-quadrant run (low fresh-density) | <= 12 | 38 ✗ | **0 ✓** (raw 16, density 0.88) | 0 ✓ (raw 19) |

v2 publishes 58/66 months on the precedent window (88%) and 122/148 on the runtime
chain (82%), with a balanced quadrant mix (C38/E37/R29/S19 full-window fresh mix).

## Strategy gates (raw runner values, adjudicated under the ratified amendments)

| gate | raw measured (base 5bps) | ruling semantics | adjudication |
|---|---|---|---|
| drawdown | 0.158 <= 0.25 | base profile | **go** |
| volatility | 0.083 <= 0.12 | base profile | **go** |
| turnover | 1.689 (signal-design bound 0.60) | phase0q_003: reference-sleeve candidate bound 2.00 (v1 measured 1.027) | **go under candidate bound** — higher than v1 (the price of coverage), inside the ratified bound |
| stress windows | INFLATION_SHOCK_2022 fails `decision_coverage` only (window MDD ✓, worst-5d ✓; COVID/Q4-2018/SVB all ✓) | phase0q_003: blocking metric is consumable_position_coverage (carry fills the two abstained months, carry age <= 3 there) | **go under phase0q_003 semantics** |
| out_of_sample | MDD dev 0.099 > 0.08; sigma dev 0.070 > 0.05 | phase0q_004: stress-overlap jackknife (v1: raw 0.107 → eligible 0.072 go) | **pending jackknife adjudication** — plausible-go, NOT yet demonstrated |
| **upside capture (bull years)** | 2017 0.57 ✓ · 2019 0.75 ✓ · 2020 1.05 ✓ · 2021 0.65 ✓ · 2023 0.60 ✓ · 2025 0.81 ✓ · **2024 0.226 ✗** (bound 0.35) | phase0q_006 (unchanged from 005) | **no_go — blocking** |

## Quant-owner decision (2026-07-16)

1. **The confidence recalibration did its job.** Every failure mode the frozen v1
   was blocked on (multi-year abstention, stale carry anchors, single-quadrant
   occupancy) is resolved on the same evidence basis, at the same flip rate.
2. **The 2024 upside-capture failure is a signal-content problem, not a
   confidence problem**: the macro-release axes read contraction through most of
   2024 while SPY rose 25.6%. Slow releases cannot see an expectations-driven
   bull. This is precisely the case for blending the `MarketImpliedAxisModel`
   (already emitting the same QuadrantSnapshot in shadow since A2) into the
   growth axis — the planned next calibration phase. Publishing more months of a
   defensively-wrong 2024 book is not activation-worthy behaviour, and the gate
   caught it exactly as designed.
3. **Therefore: A5 allocator activation stays NO-GO.** No envelope boolean flips,
   no runtime feature flag, no allocator publish.
4. **Dark-launch of the v2 SNAPSHOT STREAM is approved**: the additive
   `quadrant_macro_v2` worker may publish `macro_quadrant_us_v2` rows to
   `regime_quadrant_snapshot` (new model_version stream; v1 rows and the v1
   worker untouched; no allocator consumption). Purpose: accumulate live,
   PIT-honest v2 snapshots and monitoring history ahead of the market-implied
   blend recalibration.
5. Remaining work, in order: (a) OOS jackknife adjudication for v2 under
   phase0q_004; (b) market-implied growth-axis blend experiment (offline, same
   harness, upside gate as the target criterion is PERMITTED — it is a ratified
   strategy gate, not a classifier-calibration metric, but the blend weights
   themselves must still be calibrated on abstention/stability only);
   (c) carry-5 (2016) resolves itself if the blend brings the 2016H2 axis back —
   re-measure then; (d) pack `_004` recertification + full harness rerun +
   Stage B envelope review for the blended candidate.

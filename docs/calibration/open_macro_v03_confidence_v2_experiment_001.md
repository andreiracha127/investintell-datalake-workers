# open_macro_v03 — Confidence v2 experiment 001 (NOT ratified)

Status: `experiment_not_ratified` · Date: 2026-07-16 · Owner review pending
Script: `scripts/regime_confidence_v2_experiment.py` (offline, read-only, NO DB,
not wired into CI) · Pack: `open_macro_v03_certified_input_pack_003`
Gates judged: ratified `artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.json`

## Why

The frozen v1 model fails all four ratified timeline gates on the certified
2021–2026 timeline (fresh_valid_36m 0.1667 vs ≥ 0.40, abstention streak 18 vs
≤ 6, carry age 18 vs ≤ 3, same-quadrant run 38 vs ≤ 12). The harness replay of
pack _003 reproduced the engine byte-for-byte, so the no-go is the frozen
POLICY's behaviour, not a code defect. Root cause, confirmed by this experiment:

1. **The confidence denominator measures the wrong thing.** Frozen v1 uses
   `u_raw = 1.4826·MAD(score over its own trailing 36 vintages)` — the score's
   own variability, not the uncertainty of the estimate. `Φ(|s|/u_adj)` is then
   a self-referential outlier test: it abstains when the score is near zero
   (mid-cycle "neutral" — a legitimate, well-measured macro state) and when
   recent dispersion spikes (drawdown onsets — exactly when the signal matters).
2. **`min()` across axes compounds per-axis abstention.** One weak axis vetoes
   the whole quadrant even when the joint quadrant posterior is decisive.
3. **Carry-forward turns each abstention into multi-month staleness** (the
   2023-02 → 2026 contraction anchor).

## Candidates

* **V1 — joint quadrant posterior on the FROZEN u_adj** (decision-rule fix
  only): `p_axis = Φ(s/u_adj)` signed; quadrant posterior = product across axes;
  publish argmax iff max ≥ τ; sticky hysteresis (challenger must beat the
  incumbent by δ = 0.10).
* **V2 — Kalman local-level filter per axis** (statistic fix):
  `confidence = Φ(|m_t|/√P_t)` on the filtered state; measurement noise from
  robust `MAD(diff(score))` (method of moments), quality-inflated like u_adj;
  `Q = λ·R`. Combined with the V1 publish rule, and also with the FROZEN
  min-rule shape (`min axis confidence ≥ 0.70`) to isolate the statistic fix.

Hard gates (coverage ≥ 0.80, score presence, ≥ 24 distinct vintages) preserved
in every cell. The frozen baseline cell must reproduce the certified pack _003
timeline exactly (22/66 valid, 0.1667/18/18/38) — asserted at run time.

## Results (metrics window 2021-01..2026-06; gates PPPF order: fresh36m/abst/carry/run)

| cell | n_valid | fresh_36m | abst | carry | run | flips/y | rev1m | gates |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| frozen_v1_baseline | 22 | 0.1667 | 18 | 18 | 38 | 1.27 | 2 | FFFF |
| v1_joint_posterior_tau_0.40 | 55 | 0.7778 | 3 | 3 | 15 | 2.91 | 1 | PPPF |
| **v2_kalman_lam_0.10_tau_0.60** | 56 | **0.8056** | **3** | **3** | 17 | **1.27** | **1** | PPPF |
| v2_kalman_lam_0.10_min_rule_0.70 | 46 | 0.6111 | 3 | 3 | 19 | 0.91 | 0 | PPPF |
| v2_kalman_lam_0.25_tau_0.55 | 55 | 0.7222 | 3 | 3 | 16 | 1.82 | 1 | PPPF |

Full sweep (τ × λ grid, 29 cells): `_tmp_confidence_v2_experiment/` after
running the script. Highlights:

* **Coverage is rescued without buying instability.** The recommended cell
  (λ=0.10, τ=0.60) moves fresh_valid_36m 0.1667 → 0.8056 and streak/carry
  18 → 3 at the SAME flip rate as frozen v1 (1.27/y) with fewer one-month
  reversals. The gain is signal the old policy discarded, not noise published.
* **The statistic fix alone (min-rule kept, λ=0.10) already passes 3/4 gates**
  (0.6111 / 3 / 3) with the lowest flip rate of the sweep (0.91/y, 0
  reversals) — confirming the denominator was the dominant defect. The
  decision-rule fix alone (V1 τ=0.40–0.45) also rescues coverage but flips
  more (2.7–2.9/y): the two fixes are complementary, V2 preferred.
* **June/2026** (frozen: candidate confidence 0.7565 killed by the hysteresis
  deadband) publishes under every recommended cell.
* **2022–2023 drawdown behaviour is corrected**: the recalibrated timeline
  publishes the 2022 expansion→recovery transition and the 2023 contraction
  fresh month-by-month, instead of carrying one 2023-02 seed for years.

## Finding: the same-quadrant gate binds on FRESH persistence, not carry

Every candidate cell fails `max_same_quadrant_run_months ≤ 12` at 15–19. The
remaining run is **2021-01 → 2022-05 expansion: 17 consecutive months, all but one
fresh-valid (carry ≤ 1)** — genuine macro persistence in 2021, not staleness.
The ratified policy's own rationale scopes this gate to "the 2023-02 → 2026
contraction anchor", i.e. a CARRY artifact; with `max_carry_age_months ≤ 3`
enforced independently, a bound of 12 on the carry-filled run now punishes real
signal — no honest model should flip artificially to break a regime that lasted
17 months.

**Proposed semantics amendment** (for owner ratification, in the spirit of the
phase0q_003 stress amendment): EITHER (a) judge the same-quadrant run only over
runs whose fresh-valid density is below a floor (e.g. < 0.50 — a carry/latch
anchor), OR (b) raise the bound to ≥ 18 while keeping carry age ≤ 3 as the
staleness bound. Option (a) preserves the gate's original intent exactly.

## Out of scope / limits

* `min_upside_capture_bull_year` needs the LEAN backtest — next step is a
  harness run with the candidate policy compiled in.
* Axis independence in the quadrant posterior (Gaussian-copula ρ refinement
  pending); heuristic method-of-moments R/λ, not MLE.
* Nothing here is ratified and nothing changes the frozen model or defaults.
  Path to adoption: owner review → A3-style recalibration freeze
  (`confidence_model_version` v2) → recertification (pack `_004`) → harness
  rerun → gate judgment.

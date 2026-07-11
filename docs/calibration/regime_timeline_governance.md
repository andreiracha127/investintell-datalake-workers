# Regime timeline governance (open_macro_v03)

Status: **proposed_not_ratified** — this note and the artifacts it references are
instrumentation. Nothing here changes a frozen model default, activates anything, or
ratifies itself. `A5: blocked`, `runtime_activation: false`, `db_write: none`.

This note records the Tranche W hardening of the open_macro_v03 regime system: what the
regime audit found, the `carry_decay_v1` carry policy, the proposed blocking timeline
gates, the ratification process, and the momentum-semantics disclosure that must
accompany every investor-facing regime label.

## 1. The gap the audit found

The model works exactly as coded and its materialization is faithful; the defect is in
what the acceptance machinery rewarded.

- **Abstention became non-gating.** `artifacts/quant/open_macro_v03_phase0q_003/stress_gate_semantics_amendment.json`
  (ratified 2026-07-02) correctly stopped penalizing *intended* per-window abstention by
  making `consumable_position_coverage` the blocking stress metric — carry-forward of the
  last valid decision counts as covered. But it reclassified `fresh_decision_rate` /
  `abstention_rate` (and, implicitly, carry age and single-quadrant occupancy) as
  `diagnostics_not_gating`.
- **The envelope is risk-only.** `BASE_ENVELOPE` in `harness/phase0q/runner.py` judges
  turnover / MDD / vol / worst-5d / fold stability; `no_return_target: true`. A defensive
  book carried for years has low vol, low MDD, ~0 turnover and perfect fold stability.
- **Joint effect: the pathology is the optimum of the envelope.** A chain that abstained
  43 of 66 months (2021-2026) and carried a single **2023-02 contraction** book forward
  indefinitely reported 100% consumable coverage and passed every risk gate. The regime
  the strategy was actually positioned in was invisible to every reported number.

### Measured evidence (this tranche)

Replaying the certified input pack v2 through the frozen decision engine
(`tests/test_regime_timeline_golden.py`, 2021-01..2026-06) and the recalibration smoke
cell (`scripts/regime_recalibration_experiment.py`, default cell 10y / 0.70):

| metric | value |
|---|---|
| valid / low_confidence months (66 total) | 23 / 43 |
| fresh-valid rate, global | 0.348 |
| fresh-valid rate, trailing 36m | **0.194** |
| max abstention streak | **18 months** (2023-03..2024-08) |
| max carry age | **18 months** |
| max same-quadrant run (consumable) | **38 months** (the 2023→2026 contraction anchor) |
| quadrant mix (valid) | recovery 4, expansion 12, slowdown 1, contraction 6 |

## 2. `carry_decay_v1` — bounded carry

`harness/direct_activation/carry_decay.py` (pure, non-pinned, default-OFF).

- A seed book is consumable for at most **`MAX_CARRY_MONTHS = 3`** monthly decision
  points. `carry_age_months` is the **calendar-month** distance from `carry_seed_as_of`
  to the as-of (not a row count), so a chain gap ages the carry naturally.
- Ages 1..3 carry the seed `compressed_50` book. Age **> 3** degrades to the mandate-
  tilted **CENTER book** — the cross-quadrant mean of the four `compressed_50` books, run
  through the sleeve risk-cap / defensive-floor machinery — with `carry_expired = true`.
  Degraded months keep being re-evaluated monthly; a **fresh valid decision resets the age
  to 0**.
- This mirrors the Light-repo backtest (`carry_decay_v1`) **exactly** — cross-repo
  fidelity is a hard requirement, the two repos must consume the same policy.

## 3. Proposed blocking gates (phase0q_005)

`artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.proposed.json`
(`status: proposed_not_ratified`, `ratified_by: null`).

| gate | bound | measured (default cell) |
|---|---|---|
| `min_fresh_valid_rate_36m` | ≥ 0.40 | 0.194 → **no_go** |
| `max_abstention_streak_months` | ≤ 6 | 18 → **no_go** |
| `max_carry_age_months` | ≤ 3 | 18 → **no_go** |
| `max_same_quadrant_run_months` | ≤ 12 | 38 → **no_go** |
| `min_upside_capture_bull_year` | ≥ 0.35 (SPY > +15% years) | reported per year |

`runner.judge_timeline_gates()` computes these on every run and attaches them to the gate
report under `timeline.gate_judgment`. Until the policy is ratified the judgment is
**advisory** (`mode: advisory`, `gates_enforced: false`) and never enters
`gates_overall_base_cost`; only `status == "ratified"` makes it gating. When ratified it
re-elevates `fresh_decision_rate` / `abstention_rate` / carry-age / occupancy from
`diagnostics_not_gating` back to blocking at the run level (an amendment of phase0q_003;
the per-window `consumable_position_coverage` gate stays).

## 4. Ratification process (self-ratification is prohibited)

This tranche delivers instrumentation only. Before `carry_decay_v1` can change any
published behaviour, ALL of the following must happen through the sanctioned governance
path — none of them is done here:

1. **quant_owner ratifies** `timeline_gate_policy.proposed.json` (set `status: ratified`,
   name `ratified_by`, set `decision_date`) with the bounds reviewed against
   abstention / flip / duration behaviour — **never** against CAGR/Sharpe (the freeze
   rule; see the confidence docstring in `src/quadrant_confidence.py`).
2. **DB schema evolution.** The `open_macro_v03_decisions` / `open_macro_v03_allocations`
   CHECK constraints admit only the four quadrant labels, `fresh`/`carried`, and the
   `compressed_50` book. Persisting a `carry_expired` / center-book allocation needs new
   columns/labels, and those DDLs are frozen by the Stage B `immutability_constraint`.
3. **Re-pin the decision-chain closure.** Flipping `consumable_today` itself (rather than
   the additive advisory computation in the worker) would change a hash-pinned module and
   require regenerating `module_pins.json` — a governance-sanctioned re-pin, not a silent
   edit. Regenerating it to bless an unratified change would be self-ratification of the
   activation bundle and is forbidden.

Until then, the runtime **computes and reports** carry provenance (advisory) and the
harness/backtest legs may exercise the degradation directly via
`carry_decay.evaluate(..., active=True)`.

## 5. Momentum-semantics disclosure (labels)

The investor-facing labels stay (Recovery / Expansion / Slowdown / Contraction), but
every surface that exposes a regime must disclose:

- **The axes are momentum, not level.** Growth = annualized log 3m/3m impulse; inflation
  = 3m−YoY impulse; each a robust z-score vs a 10-year baseline. "Contraction" means
  growth *and* inflation are **decelerating**, which is economically a disinflationary
  soft-landing — historically *bullish* for equities, not a recession call. The model
  mapped the most bullish disinflationary stretch of 2023-26 to its most defensive book.
- **Confidence is an abstention proxy, not a probability.** `candidate_confidence` is
  `Φ(|score| / u_adj)` — the statistical separation of the axis scores from zero, not
  "P(regime)". Calibrate it against abstention/flip/vintage-stability, never CAGR.
- No market feature (trend, credit, earnings) enters the classifier; it is a pure
  macro-momentum abstention engine.

## 6. What this tranche delivered

| item | where |
|---|---|
| W1 timeline metrics (always reported) | `harness/phase0q/metrics.py`, `harness/phase0q/runner.py` (`timeline` block) |
| W2 proposed gate policy + advisory judge | `artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.proposed.json`, `runner.judge_timeline_gates` |
| W3 `carry_decay_v1` (default-OFF) + advisory worker provenance | `harness/direct_activation/carry_decay.py`, `src/workers/open_macro_v03.py` |
| W4 golden timeline replay | `tests/test_regime_timeline_golden.py` |
| W5 baseline-window regression + recalibration experiment | `tests/test_baseline_window_regression.py`, `scripts/regime_recalibration_experiment.py` |
| W6 this note | `docs/calibration/regime_timeline_governance.md` |

Frozen model parameters (confidence floor 0.70, axis weights, hysteresis, 10y robust
baseline) are unchanged. Recalibration is a future, separately-ratified experiment.
